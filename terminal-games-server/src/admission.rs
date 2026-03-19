// Admission control is split into two layers:
// 1. Classic queueing and per-IP caps decide which sessions are allowed to start.
// 2. A passive cluster detector continuously fingerprints every allowed session using only
//    host-visible terminal I/O. Each session is modeled as a stochastic control process over
//    several rolling windows. The model combines event-rate and entropy measures, burst and
//    periodicity statistics, byte-class distributions, sequence n-gram simhashes, short-range
//    autocorrelation, and output-conditioned response timing. Those independent views are fused
//    into per-window similarities, then into pairwise evidence between sessions. We also archive
//    recent window signatures from prior sessions so the detector can score replay/template reuse
//    across time rather than only comparing currently active sessions. Pairwise evidence induces a
//    correlation graph; connected components become suspicious clusters when their aggregate
//    likelihood remains strong under current server pressure. As pressure rises, the allowed size
//    of a suspicious cluster shrinks from observation-only to soft capping and finally targeted
//    eviction of the lowest-retention sessions in that cluster. Isolated sessions are never
//    evicted by this logic; enforcement only applies to correlated groups.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use smallvec::SmallVec;
use tokio::sync::watch;

use crate::metrics::{ServerMetrics, SessionGuard, Transport};
use terminal_games::app::{SessionControl, SessionEndReason};

const INPUT_WINDOW_MS: u64 = 8 * 60_000;
const MAX_INPUT_SAMPLES: usize = 96;
const MAX_OUTPUT_SAMPLES: usize = 64;
const MAX_SAMPLE_BYTES: usize = 24;
const OUTPUT_FLUSH_MS: u64 = 250;
const OUTPUT_FLUSH_BYTES: usize = 4096;
const CLUSTER_REEVALUATION_INTERVAL_MS: u64 = 250;
const IDLE_TIMEOUT_MS: u64 = 60_000;
const SHORT_WINDOW_MS: u64 = 60_000;
const MEDIUM_WINDOW_MS: u64 = 3 * 60_000;
const MIN_INPUTS_FOR_FINGERPRINT: usize = 3;
const MIN_CLUSTER_SIZE: usize = 2;
const LOW_PRESSURE_FLOOR: f64 = 0.35;
const SIGNATURE_RETENTION_MS: u64 = 12 * 60 * 60 * 1000;
const MAX_RECENT_SIGNATURES: usize = 16_384;
const GAP_BUCKETS: usize = 11;
const RESPONSE_BUCKETS: usize = 9;
const CLASS_BUCKETS: usize = 6;
const BURST_BUCKET_MS: u64 = 5_000;
const MOUSE_MOTION_COALESCE_MS: u64 = 40;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionState {
    Allowed,
    Queued(usize),
    Rejected(SessionEndReason),
}

#[derive(Debug, Clone)]
pub struct AdmissionConfig {
    pub max_running: usize,
    pub max_running_per_ip: usize,
    pub max_queued_per_ip: usize,
    pub ssh_captcha_threshold: Option<f64>,
}

#[derive(Clone)]
pub struct AdmissionController {
    inner: Arc<Inner>,
}

struct Inner {
    config: AdmissionConfig,
    banned_ips: Mutex<HashMap<IpAddr, Option<i64>>>,
    ban_changes: watch::Sender<()>,
    metrics: Arc<ServerMetrics>,
    next_id: AtomicU64,
    state: Mutex<ControllerState>,
}

#[derive(Default)]
struct ControllerState {
    running: usize,
    running_by_ip: HashMap<IpAddr, usize>,
    queue: VecDeque<QueuedTicket>,
    queued_by_ip: HashMap<IpAddr, usize>,
    live_sessions: HashMap<u64, LiveSessionRecord>,
    recent_signatures: VecDeque<HistoricalSignature>,
    cluster_dirty: bool,
    cluster_revision: u64,
    cluster_eval_in_progress: bool,
    next_cluster_eval_at_ms: u64,
}

impl ControllerState {
    fn mark_cluster_dirty(&mut self) {
        self.cluster_dirty = true;
        self.cluster_revision = self.cluster_revision.wrapping_add(1);
    }

    fn enqueue(
        &mut self,
        id: u64,
        client_ip: IpAddr,
        transport: Transport,
        tx: watch::Sender<AdmissionState>,
    ) {
        let _ = tx.send(AdmissionState::Queued(self.queue.len() + 1));
        *self.queued_by_ip.entry(client_ip).or_default() += 1;
        self.queue.push_back(QueuedTicket {
            id,
            client_ip,
            transport,
            tx,
        });
    }

    fn dequeue_admissible(&mut self, max_running_per_ip: usize) -> Option<QueuedTicket> {
        let idx = self
            .queue
            .iter()
            .position(|ticket| self.running_for_ip(ticket.client_ip) < max_running_per_ip)?;
        let ticket = self.queue.remove(idx)?;
        decrement_ip_count(&mut self.queued_by_ip, ticket.client_ip);
        Some(ticket)
    }

    fn remove_queued(&mut self, id: u64) -> Option<(usize, Transport)> {
        let idx = self.queue.iter().position(|queued| queued.id == id)?;
        if let Some(ticket) = self.queue.remove(idx) {
            decrement_ip_count(&mut self.queued_by_ip, ticket.client_ip);
            return Some((idx, ticket.transport));
        }
        None
    }

    fn refresh_queue_positions(&mut self, start: usize) {
        for (idx, queued) in self.queue.iter_mut().enumerate().skip(start) {
            let _ = queued.tx.send(AdmissionState::Queued(idx + 1));
        }
    }

    fn record_metrics(&self, metrics: &ServerMetrics) {
        let (ssh, web) =
            self.queue
                .iter()
                .fold((0, 0), |(ssh, web), ticket| match ticket.transport {
                    Transport::Ssh => (ssh + 1, web),
                    Transport::Web => (ssh, web + 1),
                });
        metrics.record_admission_state(ssh, web);
    }

    fn running_for_ip(&self, ip: IpAddr) -> usize {
        self.running_by_ip.get(&ip).copied().unwrap_or(0)
    }

    fn queued_for_ip(&self, ip: IpAddr) -> usize {
        self.queued_by_ip.get(&ip).copied().unwrap_or(0)
    }
}

struct QueuedTicket {
    id: u64,
    client_ip: IpAddr,
    transport: Transport,
    tx: watch::Sender<AdmissionState>,
}

pub struct AdmissionTicket {
    id: u64,
    client_ip: IpAddr,
    transport: Transport,
    rx: watch::Receiver<AdmissionState>,
    controller: Arc<Inner>,
}

pub struct AdmittedSession {
    live_session: LiveSessionHandle,
    _admission_ticket: AdmissionTicket,
    _session_guard: SessionGuard,
}

struct LiveSessionHandle {
    id: u64,
    controller: Arc<Inner>,
    control_rx: watch::Receiver<SessionControl>,
    pending_output_bytes: usize,
    pending_output_started_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct BanUpdateSummary {
    pub activated: usize,
    pub deactivated: usize,
    pub evicted_from_queue: usize,
    pub active_ban_count: usize,
}

#[derive(Clone)]
struct LiveSessionRecord {
    id: u64,
    client_ip: IpAddr,
    ip_prefix: NetworkPrefix,
    transport: Transport,
    started_at_ms: u64,
    inputs: VecDeque<InputSample>,
    outputs: VecDeque<OutputSample>,
    control: SessionControl,
    control_tx: watch::Sender<SessionControl>,
}

#[derive(Clone)]
struct InputSample {
    at_ms: u64,
    // We intentionally retain only a short owned prefix of each input event.
    // Using Bytes here would keep the full upstream allocation alive even though
    // the classifier only consumes a tiny bounded sample.
    bytes: SmallVec<[u8; MAX_SAMPLE_BYTES]>,
}

#[derive(Clone, Copy)]
struct OutputSample {
    at_ms: u64,
    bytes: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum NetworkPrefix {
    V4(u32),
    V6(u64),
}

#[derive(Clone)]
struct SessionAssessment {
    session_id: u64,
    client_ip: IpAddr,
    ip_prefix: NetworkPrefix,
    started_at_ms: u64,
    retention_score: f64,
    low_engagement: f64,
    replay_score: f64,
    replay_match_count: usize,
    replay_density: f64,
    coupling_deficit: f64,
    mouse_motion_ratio: f64,
    mouse_coord_entropy: f64,
    spectral_peakiness: f64,
    spectral_entropy_norm: f64,
    windows: Vec<WindowFingerprint>,
}

#[derive(Clone)]
struct WindowFingerprint {
    span_ms: u64,
    confidence: f64,
    event_rate_norm: f64,
    entropy_norm: f64,
    transition_entropy_norm: f64,
    ngram_innovation: f64,
    burstiness: f64,
    periodicity_60s: f64,
    spectral_peakiness: f64,
    spectral_entropy_norm: f64,
    input_output_balance: f64,
    output_coupling: f64,
    autocorr1: f64,
    autocorr2: f64,
    mouse_ratio: f64,
    mouse_motion_ratio: f64,
    mouse_coord_entropy: f64,
    mouse_flood_score: f64,
    gap_hist: [f64; GAP_BUCKETS],
    response_hist: [f64; RESPONSE_BUCKETS],
    class_hist: [f64; CLASS_BUCKETS],
    unigram_hash: u64,
    bigram_hash: u64,
    trigram_hash: u64,
    transition_hash: u64,
}

#[derive(Clone, Copy)]
struct PairEvidence {
    a: u64,
    b: u64,
    score: f64,
}

struct ClusterEvaluation {
    suspicious_cluster_count: usize,
    suspicious_session_ids: HashSet<u64>,
    evicted_session_ids: HashSet<u64>,
    max_cluster_score: f64,
}

#[derive(Clone)]
struct ClusterEvaluationJob {
    revision: u64,
    pressure: f64,
    now_ms: u64,
    live_sessions: HashMap<u64, LiveSessionRecord>,
    recent_signatures: VecDeque<HistoricalSignature>,
}

#[derive(Clone)]
struct HistoricalSignature {
    source_session_id: u64,
    recorded_at_ms: u64,
    span_ms: u64,
    fingerprint: WindowFingerprint,
}

#[derive(Clone)]
struct DerivedInputEvent {
    at_ms: u64,
    bytes: SmallVec<[u8; MAX_SAMPLE_BYTES]>,
    mouse_motion: bool,
    raw_count: u32,
}

#[derive(Clone, Copy)]
struct ParsedMouseEvent {
    motion: bool,
    coord_bucket: u16,
}

impl AdmissionController {
    pub fn new(
        config: AdmissionConfig,
        initial_banned_ips: HashMap<IpAddr, Option<i64>>,
        metrics: Arc<ServerMetrics>,
    ) -> Self {
        let (ban_changes, _) = watch::channel(());
        let controller = Self {
            inner: Arc::new(Inner {
                config,
                banned_ips: Mutex::new(initial_banned_ips),
                ban_changes,
                metrics,
                next_id: AtomicU64::new(1),
                state: Mutex::new(ControllerState::default()),
            }),
        };
        let active_bans = {
            let guard = match controller.inner.banned_ips.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.len()
        };
        controller
            .inner
            .metrics
            .record_ip_ban_update(0, 0, 0, active_bans);
        controller
            .inner
            .metrics
            .record_cluster_snapshot(0, 0, 0, 0.0);
        controller
    }

    pub fn issue_ticket(&self, transport: Transport, client_ip: IpAddr) -> AdmissionTicket {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = watch::channel(AdmissionState::Queued(1));
        let is_banned = self.is_ip_banned(client_ip);

        let job = {
            let mut state = match self.inner.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            if is_banned {
                tracing::warn!(
                    client_ip = %client_ip,
                    transport = transport.as_str(),
                    running = state.running,
                    running_for_ip = state.running_for_ip(client_ip),
                    queued_for_ip = state.queued_for_ip(client_ip),
                    "Rejected client from active IP ban"
                );
                let _ = tx.send(AdmissionState::Rejected(SessionEndReason::BannedIp));
            } else if state.running < self.inner.config.max_running
                && state.running_for_ip(client_ip) < self.inner.config.max_running_per_ip
            {
                bump_count(&mut state.running_by_ip, client_ip, 1);
                state.running += 1;
                let _ = tx.send(AdmissionState::Allowed);
            } else if state.queued_for_ip(client_ip) < self.inner.config.max_queued_per_ip {
                state.enqueue(id, client_ip, transport, tx);
            } else {
                tracing::debug!(
                    client_ip = %client_ip,
                    transport = transport.as_str(),
                    running = state.running,
                    max_running = self.inner.config.max_running,
                    running_for_ip = state.running_for_ip(client_ip),
                    max_running_per_ip = self.inner.config.max_running_per_ip,
                    queued_for_ip = state.queued_for_ip(client_ip),
                    max_queued_per_ip = self.inner.config.max_queued_per_ip,
                    "Rejected client due to per-IP admission limit"
                );
                let _ = tx.send(AdmissionState::Rejected(
                    SessionEndReason::TooManyConnectionsFromIp,
                ));
            }
            state.record_metrics(&self.inner.metrics);
            state.mark_cluster_dirty();
            maybe_run_cluster_reevaluation(&self.inner, &mut state, monotonic_millis(), true)
        };
        if let Some(job) = job {
            execute_cluster_reevaluation(&self.inner, job);
        }

        AdmissionTicket {
            id,
            client_ip,
            transport,
            rx,
            controller: self.inner.clone(),
        }
    }

    pub fn should_require_captcha(&self) -> bool {
        let Some(threshold) = self.inner.config.ssh_captcha_threshold else {
            return false;
        };
        let state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        compute_pressure(state.running, self.inner.config.max_running) >= threshold
    }

    pub fn subscribe_ban_changes(&self) -> watch::Receiver<()> {
        self.inner.ban_changes.subscribe()
    }

    pub fn is_ip_banned(&self, client_ip: IpAddr) -> bool {
        let mut banned_ips = match self.inner.banned_ips.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        match banned_ips.get(&client_ip).copied() {
            Some(expires_at) if is_ban_active(expires_at, current_unix_seconds()) => true,
            Some(_) => {
                banned_ips.remove(&client_ip);
                false
            }
            None => false,
        }
    }

    pub fn apply_ban_updates(&self, updates: Vec<(IpAddr, Option<i64>)>) -> BanUpdateSummary {
        let now = current_unix_seconds();
        let mut newly_banned = HashSet::new();
        let mut newly_banned_entries = Vec::new();
        let mut deactivated_ips = Vec::new();
        let mut deactivated = 0usize;
        let mut activated = 0usize;
        let active_ban_count;
        {
            let mut current = match self.inner.banned_ips.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            for (ip, expires_at) in updates {
                let was_active = current
                    .get(&ip)
                    .copied()
                    .is_some_and(|expires_at| is_ban_active(expires_at, now));
                if is_ban_active(expires_at, now) {
                    current.insert(ip, expires_at);
                    if !was_active {
                        newly_banned.insert(ip);
                        newly_banned_entries.push((ip, expires_at));
                        activated += 1;
                    }
                } else {
                    if current.remove(&ip).is_some() {
                        deactivated += 1;
                        deactivated_ips.push(ip);
                    }
                }
            }
            active_ban_count = current.len();
        }

        if newly_banned.is_empty() {
            let summary = BanUpdateSummary {
                activated,
                deactivated,
                evicted_from_queue: 0,
                active_ban_count,
            };
            for ip in deactivated_ips {
                tracing::info!(
                    client_ip = %ip,
                    active_ban_count = summary.active_ban_count,
                    "Deactivated IP ban"
                );
            }
            self.inner.metrics.record_ip_ban_update(
                summary.activated,
                summary.deactivated,
                summary.evicted_from_queue,
                summary.active_ban_count,
            );
            return summary;
        }
        let _ = self.inner.ban_changes.send(());
        let mut state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        let mut idx = 0usize;
        let mut removed_any = false;
        let mut evicted_from_queue = 0usize;
        while idx < state.queue.len() {
            if newly_banned.contains(&state.queue[idx].client_ip) {
                let ticket = state.queue.remove(idx).expect("index checked against len");
                decrement_ip_count(&mut state.queued_by_ip, ticket.client_ip);
                let _ = ticket
                    .tx
                    .send(AdmissionState::Rejected(SessionEndReason::BannedIp));
                removed_any = true;
                evicted_from_queue += 1;
                continue;
            }
            idx += 1;
        }
        if removed_any {
            state.refresh_queue_positions(0);
            state.record_metrics(&self.inner.metrics);
        }
        let summary = BanUpdateSummary {
            activated,
            deactivated,
            evicted_from_queue,
            active_ban_count,
        };
        for (ip, expires_at) in newly_banned_entries {
            tracing::warn!(
                client_ip = %ip,
                expires_at,
                evicted_from_queue,
                active_ban_count = summary.active_ban_count,
                "Activated IP ban"
            );
        }
        for ip in deactivated_ips {
            tracing::info!(
                client_ip = %ip,
                active_ban_count = summary.active_ban_count,
                "Deactivated IP ban"
            );
        }
        self.inner.metrics.record_ip_ban_update(
            summary.activated,
            summary.deactivated,
            summary.evicted_from_queue,
            summary.active_ban_count,
        );
        summary
    }
}

impl AdmissionTicket {
    pub fn subscribe(&self) -> watch::Receiver<AdmissionState> {
        self.rx.clone()
    }

    pub fn start_session(self, session_guard: SessionGuard) -> AdmittedSession {
        let live_session = self.start_live_session();
        AdmittedSession {
            live_session,
            _admission_ticket: self,
            _session_guard: session_guard,
        }
    }

    fn start_live_session(&self) -> LiveSessionHandle {
        let now_ms = monotonic_millis();
        let (control_tx, control_rx) = watch::channel(SessionControl::Active);
        let job = {
            let mut state = match self.controller.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            insert_live_session(
                &mut state,
                self.id,
                self.client_ip,
                self.transport,
                now_ms,
                control_tx,
            );
            state.mark_cluster_dirty();
            maybe_run_cluster_reevaluation(&self.controller, &mut state, now_ms, true)
        };
        if let Some(job) = job {
            execute_cluster_reevaluation(&self.controller, job);
        }

        LiveSessionHandle {
            id: self.id,
            controller: self.controller.clone(),
            control_rx,
            pending_output_bytes: 0,
            pending_output_started_at_ms: None,
        }
    }
}

impl Drop for AdmissionTicket {
    fn drop(&mut self) {
        let job = {
            let mut state = match self.controller.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            match *self.rx.borrow() {
                AdmissionState::Allowed => {
                    state.running = state.running.saturating_sub(1);
                    decrement_ip_count(&mut state.running_by_ip, self.client_ip);
                    if let Some(next) =
                        state.dequeue_admissible(self.controller.config.max_running_per_ip)
                    {
                        state.running += 1;
                        bump_count(&mut state.running_by_ip, next.client_ip, 1);
                        let _ = next.tx.send(AdmissionState::Allowed);
                        self.controller
                            .metrics
                            .record_admission_joined_from_queue(next.transport);
                        state.refresh_queue_positions(0);
                    }
                }
                AdmissionState::Queued(_) => {
                    if let Some((idx, transport)) = state.remove_queued(self.id) {
                        self.controller
                            .metrics
                            .record_admission_abandoned_queue(transport);
                        state.refresh_queue_positions(idx);
                    }
                }
                AdmissionState::Rejected(reason) => {
                    if reason == SessionEndReason::ClusterLimited {
                        tracing::warn!(
                            ticket_id = self.id,
                            client_ip = %self.client_ip,
                            transport = self.transport.as_str(),
                            "Rejected session due to suspected bot cluster"
                        );
                        self.controller
                            .metrics
                            .record_cluster_enforcement(self.transport, "rejected");
                    }
                }
            }
            state.record_metrics(&self.controller.metrics);
            state.mark_cluster_dirty();
            maybe_run_cluster_reevaluation(&self.controller, &mut state, monotonic_millis(), true)
        };
        if let Some(job) = job {
            execute_cluster_reevaluation(&self.controller, job);
        }
    }
}

impl AdmittedSession {
    pub fn subscribe_control(&self) -> watch::Receiver<SessionControl> {
        self.live_session.subscribe_control()
    }

    pub fn record_input(&mut self, bytes: &[u8]) {
        self.live_session.record_input(bytes);
    }

    pub fn record_output(&mut self, bytes: usize) {
        self.live_session.record_output(bytes);
    }
}

impl Drop for AdmittedSession {
    fn drop(&mut self) {
        // Fields are declared in teardown order and Rust drops struct fields in
        // declaration order after Drop::drop returns, so cleanup is:
        // live_session -> admission_ticket -> session_guard.
        self.live_session.flush();
    }
}

impl LiveSessionHandle {
    pub fn subscribe_control(&self) -> watch::Receiver<SessionControl> {
        self.control_rx.clone()
    }

    pub fn record_input(&mut self, bytes: &[u8]) {
        self.flush_output_batch();
        let now_ms = monotonic_millis();
        let sample = InputSample {
            at_ms: now_ms,
            bytes: bytes
                .iter()
                .copied()
                .take(MAX_SAMPLE_BYTES)
                .collect::<SmallVec<[u8; MAX_SAMPLE_BYTES]>>(),
        };
        let job = {
            let mut state = match self.controller.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            let Some(record) = state.live_sessions.get_mut(&self.id) else {
                return;
            };
            record.inputs.push_back(sample);
            while record.inputs.len() > MAX_INPUT_SAMPLES {
                record.inputs.pop_front();
            }
            trim_old_inputs(&mut record.inputs, now_ms);
            state.mark_cluster_dirty();
            maybe_run_cluster_reevaluation(&self.controller, &mut state, now_ms, false)
        };
        if let Some(job) = job {
            execute_cluster_reevaluation(&self.controller, job);
        }
    }

    pub fn record_output(&mut self, bytes: usize) {
        self.pending_output_bytes = self.pending_output_bytes.saturating_add(bytes);
        let now_ms = monotonic_millis();
        let started_at = self.pending_output_started_at_ms.get_or_insert(now_ms);
        if now_ms.saturating_sub(*started_at) >= OUTPUT_FLUSH_MS
            || self.pending_output_bytes >= OUTPUT_FLUSH_BYTES
        {
            self.flush_output_batch();
        }
    }

    pub fn flush(&mut self) {
        self.flush_output_batch();
    }

    fn flush_output_batch(&mut self) {
        if self.pending_output_bytes == 0 {
            self.pending_output_started_at_ms = None;
            return;
        }
        let now_ms = monotonic_millis();
        let at_ms = self.pending_output_started_at_ms.unwrap_or(now_ms);
        let bytes = self.pending_output_bytes.min(u32::MAX as usize) as u32;
        self.pending_output_bytes = 0;
        self.pending_output_started_at_ms = None;

        let job = {
            let mut state = match self.controller.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            let Some(record) = state.live_sessions.get_mut(&self.id) else {
                return;
            };
            record.outputs.push_back(OutputSample { at_ms, bytes });
            while record.outputs.len() > MAX_OUTPUT_SAMPLES {
                record.outputs.pop_front();
            }
            trim_old_outputs(&mut record.outputs, now_ms);
            state.mark_cluster_dirty();
            maybe_run_cluster_reevaluation(&self.controller, &mut state, now_ms, false)
        };
        if let Some(job) = job {
            execute_cluster_reevaluation(&self.controller, job);
        }
    }
}

impl Drop for LiveSessionHandle {
    fn drop(&mut self) {
        self.flush_output_batch();
        let now_ms = monotonic_millis();
        let job = {
            let mut state = match self.controller.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            if let Some(record) = state.live_sessions.remove(&self.id) {
                archive_signatures(&record, now_ms, &mut state.recent_signatures);
            }
            trim_recent_signatures(&mut state.recent_signatures, now_ms);
            state.mark_cluster_dirty();
            maybe_run_cluster_reevaluation(&self.controller, &mut state, now_ms, true)
        };
        if let Some(job) = job {
            execute_cluster_reevaluation(&self.controller, job);
        }
    }
}

fn decrement_ip_count(counts: &mut HashMap<IpAddr, usize>, ip: IpAddr) {
    let Some(count) = counts.get_mut(&ip) else {
        return;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        counts.remove(&ip);
    }
}

fn bump_count(counts: &mut HashMap<IpAddr, usize>, ip: IpAddr, delta: usize) {
    *counts.entry(ip).or_default() += delta;
}

fn insert_live_session(
    state: &mut ControllerState,
    id: u64,
    client_ip: IpAddr,
    transport: Transport,
    started_at_ms: u64,
    control_tx: watch::Sender<SessionControl>,
) {
    if state.live_sessions.contains_key(&id) {
        panic!("live session already exists for admission ticket {id}");
    }
    state.live_sessions.insert(
        id,
        LiveSessionRecord {
            id,
            client_ip,
            ip_prefix: NetworkPrefix::from_ip(client_ip),
            transport,
            started_at_ms,
            inputs: VecDeque::new(),
            outputs: VecDeque::new(),
            control: SessionControl::Active,
            control_tx,
        },
    );
}

fn is_ban_active(expires_at: Option<i64>, now: i64) -> bool {
    expires_at.is_none_or(|expires_at| expires_at > now)
}

fn current_unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn monotonic_millis() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn trim_old_inputs(inputs: &mut VecDeque<InputSample>, now_ms: u64) {
    while inputs
        .front()
        .is_some_and(|sample| now_ms.saturating_sub(sample.at_ms) > INPUT_WINDOW_MS)
    {
        inputs.pop_front();
    }
}

fn trim_old_outputs(outputs: &mut VecDeque<OutputSample>, now_ms: u64) {
    while outputs
        .front()
        .is_some_and(|sample| now_ms.saturating_sub(sample.at_ms) > INPUT_WINDOW_MS)
    {
        outputs.pop_front();
    }
}

fn maybe_run_cluster_reevaluation(
    inner: &Arc<Inner>,
    state: &mut ControllerState,
    now_ms: u64,
    force: bool,
) -> Option<ClusterEvaluationJob> {
    trim_recent_signatures(&mut state.recent_signatures, now_ms);
    let pressure = compute_pressure(state.running, inner.config.max_running);
    if pressure < LOW_PRESSURE_FLOOR || state.live_sessions.len() < MIN_CLUSTER_SIZE {
        state.cluster_dirty = false;
        state.next_cluster_eval_at_ms = now_ms.saturating_add(CLUSTER_REEVALUATION_INTERVAL_MS);
        clear_cluster_controls(inner, state);
        return None;
    }
    if state.cluster_eval_in_progress {
        state.cluster_dirty = true;
        return None;
    }
    if !force && (!state.cluster_dirty || now_ms < state.next_cluster_eval_at_ms) {
        return None;
    }
    state.cluster_eval_in_progress = true;
    state.cluster_dirty = false;
    state.next_cluster_eval_at_ms = now_ms.saturating_add(CLUSTER_REEVALUATION_INTERVAL_MS);
    Some(ClusterEvaluationJob {
        revision: state.cluster_revision,
        pressure,
        now_ms,
        live_sessions: state.live_sessions.clone(),
        recent_signatures: state.recent_signatures.clone(),
    })
}

fn execute_cluster_reevaluation(inner: &Arc<Inner>, job: ClusterEvaluationJob) {
    let evaluation = evaluate_clusters(
        &job.live_sessions,
        &job.recent_signatures,
        job.pressure,
        job.now_ms,
    );
    let mut state = match inner.state.lock() {
        Ok(state) => state,
        Err(poisoned) => poisoned.into_inner(),
    };
    state.cluster_eval_in_progress = false;
    if state.cluster_revision != job.revision {
        state.cluster_dirty = true;
        return;
    }
    apply_cluster_evaluation(inner, &mut state, evaluation, job.pressure);
}

fn clear_cluster_controls(inner: &Arc<Inner>, state: &mut ControllerState) {
    for session in state.live_sessions.values_mut() {
        if session.control != SessionControl::Active {
            let _ = session.control_tx.send(SessionControl::Active);
            session.control = SessionControl::Active;
        }
    }
    inner.metrics.record_cluster_snapshot(0, 0, 0, 0.0);
}

fn apply_cluster_evaluation(
    inner: &Arc<Inner>,
    state: &mut ControllerState,
    evaluation: ClusterEvaluation,
    pressure: f64,
) {
    let mut suspicious_ssh = 0usize;
    let mut suspicious_web = 0usize;

    for session in state.live_sessions.values_mut() {
        let suspicious = evaluation.suspicious_session_ids.contains(&session.id);
        if suspicious {
            match session.transport {
                Transport::Ssh => suspicious_ssh += 1,
                Transport::Web => suspicious_web += 1,
            }
        }
        let next = if evaluation.evicted_session_ids.contains(&session.id) {
            SessionControl::Close(SessionEndReason::ClusterLimited)
        } else {
            SessionControl::Active
        };
        if session.control != next {
            if matches!(next, SessionControl::Close(_)) {
                tracing::warn!(
                    session_id = session.id,
                    client_ip = %session.client_ip,
                    transport = session.transport.as_str(),
                    pressure,
                    suspicious_cluster_count = evaluation.suspicious_cluster_count,
                    max_cluster_score = evaluation.max_cluster_score,
                    "Evicting session due to suspected bot cluster"
                );
                inner
                    .metrics
                    .record_cluster_enforcement(session.transport, "evicted");
            }
            let _ = session.control_tx.send(next);
            session.control = next;
        }
    }

    inner.metrics.record_cluster_snapshot(
        evaluation.suspicious_cluster_count,
        suspicious_ssh,
        suspicious_web,
        evaluation.max_cluster_score,
    );
}

fn compute_pressure(running: usize, max_running: usize) -> f64 {
    if max_running == 0 || max_running == usize::MAX {
        0.0
    } else {
        (running as f64 / max_running as f64).clamp(0.0, 1.0)
    }
}

fn evaluate_clusters(
    live_sessions: &HashMap<u64, LiveSessionRecord>,
    recent_signatures: &VecDeque<HistoricalSignature>,
    pressure: f64,
    now_ms: u64,
) -> ClusterEvaluation {
    let mut sessions: Vec<SessionAssessment> = live_sessions
        .values()
        .filter_map(|session| SessionAssessment::from_record(session, now_ms))
        .collect();
    apply_replay_evidence(&mut sessions, recent_signatures, now_ms);
    if sessions.len() < MIN_CLUSTER_SIZE || pressure < LOW_PRESSURE_FLOOR {
        return ClusterEvaluation {
            suspicious_cluster_count: 0,
            suspicious_session_ids: HashSet::new(),
            evicted_session_ids: HashSet::new(),
            max_cluster_score: 0.0,
        };
    }

    let mut edges = Vec::new();
    let mut dsu = DisjointSet::new(sessions.len());
    for i in 0..sessions.len() {
        for j in (i + 1)..sessions.len() {
            if let Some(edge) = pair_evidence(&sessions[i], &sessions[j], pressure) {
                dsu.union(i, j);
                edges.push(edge);
            }
        }
    }
    materialize_replay_cohort(&sessions, pressure, &mut dsu, &mut edges);

    if edges.is_empty() {
        return ClusterEvaluation {
            suspicious_cluster_count: 0,
            suspicious_session_ids: HashSet::new(),
            evicted_session_ids: HashSet::new(),
            max_cluster_score: 0.0,
        };
    }

    let mut components: HashMap<usize, Vec<usize>> = HashMap::new();
    for idx in 0..sessions.len() {
        let root = dsu.find(idx);
        components.entry(root).or_default().push(idx);
    }

    let mut edge_scores: HashMap<(u64, u64), f64> = HashMap::new();
    for edge in &edges {
        edge_scores.insert(edge_key(edge.a, edge.b), edge.score);
    }

    let mut suspicious_cluster_count = 0usize;
    let mut suspicious_session_ids = HashSet::new();
    let mut evicted_session_ids = HashSet::new();
    let mut max_cluster_score: f64 = 0.0;

    for component in components.into_values() {
        if component.len() < MIN_CLUSTER_SIZE {
            continue;
        }
        let stats = cluster_stats(&sessions, &component, &edge_scores);
        let score = cluster_score(&stats, pressure);
        if score < required_cluster_score(pressure) {
            continue;
        }
        max_cluster_score = max_cluster_score.max(score);
        suspicious_cluster_count += 1;
        for session_id in component.iter().map(|idx| sessions[*idx].session_id) {
            suspicious_session_ids.insert(session_id);
        }

        if let Some(cap) = cluster_cap(&stats, pressure, score) {
            if component.len() > cap {
                let mut ranked = component.clone();
                ranked.sort_by(|left, right| {
                    let left_priority =
                        sessions[*left].retention_score - 0.65 * sessions[*left].replay_score;
                    let right_priority =
                        sessions[*right].retention_score - 0.65 * sessions[*right].replay_score;
                    right_priority
                        .partial_cmp(&left_priority)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| {
                            sessions[*left]
                                .started_at_ms
                                .cmp(&sessions[*right].started_at_ms)
                        })
                });
                for idx in ranked.into_iter().skip(cap) {
                    evicted_session_ids.insert(sessions[idx].session_id);
                }
            }
        }
    }

    ClusterEvaluation {
        suspicious_cluster_count,
        suspicious_session_ids,
        evicted_session_ids,
        max_cluster_score,
    }
}

fn materialize_replay_cohort(
    sessions: &[SessionAssessment],
    pressure: f64,
    dsu: &mut DisjointSet,
    edges: &mut Vec<PairEvidence>,
) {
    if pressure < 0.60 {
        return;
    }
    let threshold = replay_session_threshold(pressure);
    let cohort = sessions
        .iter()
        .enumerate()
        .filter(|(_, session)| {
            session.replay_score >= threshold
                && session.replay_match_count > 0
                && session.coupling_deficit >= 0.18
        })
        .map(|(idx, _)| idx)
        .collect::<Vec<_>>();
    if cohort.len() < MIN_CLUSTER_SIZE {
        return;
    }
    let affinity = replay_cohort_affinity(sessions, &cohort);
    if affinity < (0.78 - 0.12 * pressure).clamp(0.60, 0.78) {
        return;
    }
    for i in 0..cohort.len() {
        for j in (i + 1)..cohort.len() {
            let left = cohort[i];
            let right = cohort[j];
            dsu.union(left, right);
            edges.push(PairEvidence {
                a: sessions[left].session_id,
                b: sessions[right].session_id,
                score: affinity,
            });
        }
    }
}

struct ComponentStats {
    avg_edge: f64,
    avg_low_engagement: f64,
    avg_replay: f64,
    avg_replay_density: f64,
    avg_coupling_deficit: f64,
    network_cohesion: f64,
    start_sync: f64,
    size_factor: f64,
}

fn cluster_stats(
    sessions: &[SessionAssessment],
    component: &[usize],
    edge_scores: &HashMap<(u64, u64), f64>,
) -> ComponentStats {
    let mut edge_sum = 0.0;
    let mut edge_count = 0usize;
    let mut low_engagement_sum = 0.0;
    let mut replay_sum = 0.0;
    let mut replay_density_sum = 0.0;
    let mut coupling_deficit_sum = 0.0;
    let mut prefix_counts: HashMap<NetworkPrefix, usize> = HashMap::new();
    let mut oldest = u64::MAX;
    let mut newest = 0u64;

    for idx in component {
        low_engagement_sum += sessions[*idx].low_engagement;
        replay_sum += sessions[*idx].replay_score;
        replay_density_sum += sessions[*idx].replay_density;
        coupling_deficit_sum += sessions[*idx].coupling_deficit;
        *prefix_counts.entry(sessions[*idx].ip_prefix).or_default() += 1;
        oldest = oldest.min(sessions[*idx].started_at_ms);
        newest = newest.max(sessions[*idx].started_at_ms);
    }
    for i in 0..component.len() {
        for j in (i + 1)..component.len() {
            let a = sessions[component[i]].session_id;
            let b = sessions[component[j]].session_id;
            if let Some(score) = edge_scores.get(&edge_key(a, b)) {
                edge_sum += *score;
                edge_count += 1;
            }
        }
    }
    let network_cohesion = prefix_counts
        .values()
        .copied()
        .max()
        .map(|count| count as f64 / component.len() as f64)
        .unwrap_or(0.0);
    let start_spread_ms = newest.saturating_sub(oldest);
    ComponentStats {
        avg_edge: if edge_count == 0 {
            0.0
        } else {
            edge_sum / edge_count as f64
        },
        avg_low_engagement: low_engagement_sum / component.len() as f64,
        avg_replay: replay_sum / component.len() as f64,
        avg_replay_density: replay_density_sum / component.len() as f64,
        avg_coupling_deficit: coupling_deficit_sum / component.len() as f64,
        network_cohesion,
        start_sync: (1.0 - start_spread_ms as f64 / (5.0 * 60_000.0)).clamp(0.0, 1.0),
        size_factor: ((component.len() as f64 - 1.0) / 6.0).clamp(0.0, 1.0),
    }
}

fn cluster_cap(stats: &ComponentStats, pressure: f64, score: f64) -> Option<usize> {
    if pressure < LOW_PRESSURE_FLOOR || score < required_cluster_score(pressure) {
        return None;
    }
    if pressure >= 0.90 && stats.avg_replay_density >= 0.75 && stats.avg_edge >= 0.80 {
        return Some(0);
    }
    if pressure >= 0.85 && stats.avg_replay >= 0.40 {
        return Some(1);
    }

    Some(2)
}

fn required_cluster_score(pressure: f64) -> f64 {
    (0.82 - (pressure - LOW_PRESSURE_FLOOR).max(0.0) * 0.42).clamp(0.56, 0.82)
}

fn cluster_score(stats: &ComponentStats, pressure: f64) -> f64 {
    if stats.size_factor <= 0.34
        && stats.avg_low_engagement < 0.48
        && stats.avg_replay < 0.84
        && stats.avg_edge < 0.73
    {
        return 0.0;
    }
    if stats.avg_replay < 0.55 && stats.avg_low_engagement < 0.70 && stats.avg_edge < 0.73 {
        return 0.0;
    }
    if stats.network_cohesion < 0.4 && stats.avg_low_engagement < 0.44 && stats.avg_replay < 0.88 {
        return 0.0;
    }
    let base = 0.48 * stats.avg_edge
        + 0.20 * stats.avg_replay
        + 0.09 * stats.avg_replay_density
        + 0.09 * stats.avg_low_engagement
        + 0.05 * stats.avg_coupling_deficit
        + 0.04 * stats.network_cohesion
        + 0.03 * stats.start_sync;
    (base
        + 0.07 * stats.size_factor
        + 0.04 * pressure
        + 0.08 * (stats.avg_edge * stats.avg_low_engagement))
        .clamp(0.0, 1.0)
}

fn pair_evidence(
    left: &SessionAssessment,
    right: &SessionAssessment,
    pressure: f64,
) -> Option<PairEvidence> {
    let window_similarity = session_window_similarity(left, right);
    let session_confidence = min_session_confidence(left, right);
    if session_confidence < 0.24 {
        return None;
    }
    let low_engagement_affinity = similarity(left.low_engagement, right.low_engagement);
    let low_engagement_level = left.low_engagement.min(right.low_engagement);
    let replay_affinity = (left.replay_score.min(right.replay_score)
        * (0.5 + 0.5 * similarity(left.replay_density, right.replay_density)))
    .clamp(0.0, 1.0);
    let replay_match_affinity = similarity(
        (left.replay_match_count as f64 / 4.0).clamp(0.0, 1.0),
        (right.replay_match_count as f64 / 4.0).clamp(0.0, 1.0),
    );
    let coupling_affinity = similarity(left.coupling_deficit, right.coupling_deficit);
    let spectral_affinity = average_similarity(&[
        similarity(left.spectral_peakiness, right.spectral_peakiness),
        similarity(left.spectral_entropy_norm, right.spectral_entropy_norm),
    ]);
    let start_similarity = 1.0
        - (left.started_at_ms.abs_diff(right.started_at_ms) as f64 / (5.0 * 60_000.0))
            .clamp(0.0, 1.0);
    let network_hint = if left.client_ip == right.client_ip {
        1.0
    } else if left.ip_prefix == right.ip_prefix {
        0.65
    } else {
        0.0
    };
    let hover_relief = left.mouse_motion_ratio.min(right.mouse_motion_ratio)
        * left.mouse_coord_entropy.min(right.mouse_coord_entropy)
        * left.spectral_entropy_norm.min(right.spectral_entropy_norm);
    let score = 0.62 * window_similarity
        + 0.10 * low_engagement_affinity
        + 0.08 * low_engagement_level
        + 0.10 * replay_affinity
        + 0.04 * replay_match_affinity
        + 0.03 * coupling_affinity
        + 0.01 * spectral_affinity
        + 0.01 * start_similarity
        + 0.01 * network_hint;
    let adjusted = (0.86 * score + 0.14 * session_confidence).clamp(0.0, 1.0);
    let dynamic_threshold = if pressure < 0.60 {
        0.78
    } else if pressure < 0.80 {
        0.72
    } else {
        0.67
    };
    let mut threshold: f64 = dynamic_threshold;
    if replay_affinity > 0.78 || network_hint > 0.0 {
        threshold -= 0.03;
    }
    if low_engagement_level > 0.68 && left.coupling_deficit.min(right.coupling_deficit) > 0.80 {
        threshold -= 0.04;
    }
    if hover_relief > 0.18 && replay_affinity < 0.45 {
        threshold += 0.06;
    }
    if left.spectral_peakiness.min(right.spectral_peakiness) > 0.62
        && left.mouse_motion_ratio.min(right.mouse_motion_ratio) > 0.35
    {
        threshold -= 0.03;
    }
    threshold = threshold.clamp(0.56, 0.84);
    if adjusted < threshold {
        return None;
    }
    Some(PairEvidence {
        a: left.session_id,
        b: right.session_id,
        score: adjusted.clamp(0.0, 1.0),
    })
}

impl SessionAssessment {
    fn from_record(record: &LiveSessionRecord, now_ms: u64) -> Option<Self> {
        let windows = build_window_fingerprints(record, now_ms);
        if windows.is_empty() {
            return None;
        }
        let avg_event_rate = weighted_window_average(&windows, |window| window.event_rate_norm);
        let avg_entropy = weighted_window_average(&windows, |window| window.entropy_norm);
        let avg_innovation = weighted_window_average(&windows, |window| window.ngram_innovation);
        let avg_coupling = weighted_window_average(&windows, |window| window.output_coupling);
        let avg_balance = weighted_window_average(&windows, |window| window.input_output_balance);
        let avg_mouse_motion =
            weighted_window_average(&windows, |window| window.mouse_motion_ratio);
        let avg_mouse_coord_entropy =
            weighted_window_average(&windows, |window| window.mouse_coord_entropy);
        let avg_spectral_peak =
            weighted_window_average(&windows, |window| window.spectral_peakiness);
        let avg_spectral_entropy =
            weighted_window_average(&windows, |window| window.spectral_entropy_norm);
        let coupling_deficit = (1.0 - avg_coupling).clamp(0.0, 1.0);
        let mouse_liveness = (avg_mouse_motion
            * (0.35 + 0.65 * avg_mouse_coord_entropy)
            * (0.55 + 0.45 * avg_spectral_entropy))
            .clamp(0.0, 1.0);
        let mechanicality = (0.60 * avg_spectral_peak
            + 0.40 * avg_mouse_motion * (1.0 - avg_mouse_coord_entropy))
            .clamp(0.0, 1.0);
        let interactivity = 0.24 * avg_event_rate
            + 0.18 * avg_entropy
            + 0.14 * avg_innovation
            + 0.14 * avg_coupling
            + 0.10 * avg_balance
            + 0.10 * avg_spectral_entropy
            + 0.10 * mouse_liveness;
        let low_engagement = (1.0 - interactivity + 0.30 * mechanicality).clamp(0.0, 1.0);
        let age_norm = ((now_ms.saturating_sub(record.started_at_ms)) as f64
            / INPUT_WINDOW_MS as f64)
            .clamp(0.0, 1.0);
        let retention_score = 0.44 * age_norm
            + 0.34 * (1.0 - low_engagement)
            + 0.12 * min_window_confidence(&windows)
            + 0.10 * (avg_entropy + avg_innovation) * 0.5;
        Some(Self {
            session_id: record.id,
            client_ip: record.client_ip,
            ip_prefix: record.ip_prefix,
            started_at_ms: record.started_at_ms,
            retention_score,
            low_engagement,
            replay_score: 0.0,
            replay_match_count: 0,
            replay_density: 0.0,
            coupling_deficit,
            mouse_motion_ratio: avg_mouse_motion,
            mouse_coord_entropy: avg_mouse_coord_entropy,
            spectral_peakiness: avg_spectral_peak,
            spectral_entropy_norm: avg_spectral_entropy,
            windows,
        })
    }
}

fn apply_replay_evidence(
    sessions: &mut [SessionAssessment],
    recent_signatures: &VecDeque<HistoricalSignature>,
    now_ms: u64,
) {
    if recent_signatures.is_empty() {
        return;
    }
    for session in sessions {
        let mut best_by_span: HashMap<u64, f64> = HashMap::new();
        let mut distinct_sources: HashMap<u64, f64> = HashMap::new();
        for window in &session.windows {
            for signature in recent_signatures.iter().filter(|signature| {
                signature.span_ms == window.span_ms
                    && signature.recorded_at_ms <= now_ms
                    && now_ms.saturating_sub(signature.recorded_at_ms) <= SIGNATURE_RETENTION_MS
            }) {
                let score = window_similarity(window, &signature.fingerprint);
                best_by_span
                    .entry(window.span_ms)
                    .and_modify(|current| *current = current.max(score))
                    .or_insert(score);
                if score >= 0.78 {
                    distinct_sources
                        .entry(signature.source_session_id)
                        .and_modify(|current| *current = current.max(score))
                        .or_insert(score);
                }
            }
        }
        let weighted_best = session
            .windows
            .iter()
            .map(|window| {
                window_weight(window.span_ms)
                    * best_by_span.get(&window.span_ms).copied().unwrap_or(0.0)
            })
            .sum::<f64>();
        let mut strongest = distinct_sources.values().copied().collect::<Vec<_>>();
        strongest
            .sort_by(|left, right| right.partial_cmp(left).unwrap_or(std::cmp::Ordering::Equal));
        let replay_density = if strongest.is_empty() {
            0.0
        } else {
            strongest.iter().take(3).sum::<f64>() / strongest.len().min(3) as f64
        };
        session.replay_match_count = distinct_sources.len();
        session.replay_density = replay_density;
        session.replay_score = (0.82 * weighted_best
            + 0.12 * replay_density
            + 0.06 * (session.replay_match_count as f64 / 3.0).clamp(0.0, 1.0))
        .clamp(0.0, 1.0);
    }
}

fn replay_session_threshold(pressure: f64) -> f64 {
    if pressure < 0.70 {
        0.86
    } else if pressure < 0.85 {
        0.80
    } else {
        0.72
    }
}

fn replay_cohort_affinity(sessions: &[SessionAssessment], indices: &[usize]) -> f64 {
    let avg_replay = indices
        .iter()
        .map(|idx| sessions[*idx].replay_score)
        .sum::<f64>()
        / indices.len() as f64;
    let avg_density = indices
        .iter()
        .map(|idx| sessions[*idx].replay_density)
        .sum::<f64>()
        / indices.len() as f64;
    let avg_coupling_deficit = indices
        .iter()
        .map(|idx| sessions[*idx].coupling_deficit)
        .sum::<f64>()
        / indices.len() as f64;
    (0.62 * avg_replay + 0.22 * avg_density + 0.16 * avg_coupling_deficit).clamp(0.0, 1.0)
}

fn build_window_fingerprints(record: &LiveSessionRecord, now_ms: u64) -> Vec<WindowFingerprint> {
    [SHORT_WINDOW_MS, MEDIUM_WINDOW_MS, INPUT_WINDOW_MS]
        .into_iter()
        .filter_map(|span_ms| build_window_fingerprint(record, now_ms, span_ms))
        .collect()
}

fn build_window_fingerprint(
    record: &LiveSessionRecord,
    now_ms: u64,
    span_ms: u64,
) -> Option<WindowFingerprint> {
    let window_start = now_ms.saturating_sub(span_ms);
    let inputs = record
        .inputs
        .iter()
        .filter(|sample| sample.at_ms >= window_start)
        .collect::<Vec<_>>();
    if inputs.len() < MIN_INPUTS_FOR_FINGERPRINT {
        return None;
    }
    let outputs = record
        .outputs
        .iter()
        .copied()
        .filter(|sample| sample.at_ms >= window_start)
        .collect::<Vec<_>>();
    let window_anchor = record.started_at_ms.max(window_start);
    let window_ms = now_ms.saturating_sub(window_anchor).max(1);
    let total_output_bytes = outputs
        .iter()
        .map(|sample| sample.bytes as usize)
        .sum::<usize>();
    let mut derived_inputs: Vec<DerivedInputEvent> = Vec::with_capacity(inputs.len());
    let mut byte_counts = [0u32; 256];
    let mut class_counts = [0u32; CLASS_BUCKETS];
    let mut gap_hist = [0.0; GAP_BUCKETS];
    let mut response_hist = [0.0; RESPONSE_BUCKETS];
    let mut bucket_counts = vec![0u32; (window_ms / BURST_BUCKET_MS).max(1) as usize + 1];
    let mut token_counts: HashMap<u64, u32> = HashMap::new();
    let mut transition_counts: HashMap<(u64, u64), u32> = HashMap::new();
    let mut source_counts: HashMap<u64, u32> = HashMap::new();
    let mut unigram_weights = [0i32; 64];
    let mut bigram_weights = [0i32; 64];
    let mut trigram_weights = [0i32; 64];
    let mut transition_weights = [0i32; 64];
    let mut tokens = Vec::with_capacity(inputs.len());
    let mut distinct_bigrams = HashSet::new();
    let mut distinct_trigrams = HashSet::new();
    let mut total_input_bytes = 0usize;
    let mut output_cursor = 0usize;
    let mut last_output_at = None;
    let mut gaps = Vec::with_capacity(inputs.len().saturating_sub(1));
    let mut mouse_events = 0usize;
    let mut mouse_motion_events = 0usize;
    let mut mouse_coord_counts: HashMap<u16, u32> = HashMap::new();

    for sample in &inputs {
        let parsed_mouse = parse_terminal_mouse_event(&sample.bytes);
        if let Some(mouse) = parsed_mouse {
            mouse_events += 1;
            if mouse.motion {
                mouse_motion_events += 1;
                *mouse_coord_counts.entry(mouse.coord_bucket).or_default() += 1;
            }
        }
        if let Some(_mouse) = parsed_mouse.filter(|mouse| mouse.motion) {
            if let Some(last) = derived_inputs.last_mut() {
                if last.mouse_motion
                    && sample.at_ms.saturating_sub(last.at_ms) <= MOUSE_MOTION_COALESCE_MS
                {
                    last.at_ms = sample.at_ms;
                    last.bytes = sample.bytes.clone();
                    last.raw_count = last.raw_count.saturating_add(1);
                    continue;
                }
            }
        }
        derived_inputs.push(DerivedInputEvent {
            at_ms: sample.at_ms,
            bytes: sample.bytes.clone(),
            mouse_motion: parsed_mouse.is_some_and(|mouse| mouse.motion),
            raw_count: 1,
        });
    }

    if derived_inputs.len() < MIN_INPUTS_FOR_FINGERPRINT && mouse_motion_events < 12 {
        return None;
    }

    for (index, event) in derived_inputs.iter().enumerate() {
        while output_cursor < outputs.len() && outputs[output_cursor].at_ms <= event.at_ms {
            last_output_at = Some(outputs[output_cursor].at_ms);
            output_cursor += 1;
        }
        let gap = if index == 0 {
            event.at_ms.saturating_sub(window_anchor)
        } else {
            event.at_ms.saturating_sub(derived_inputs[index - 1].at_ms)
        };
        if index > 0 {
            gaps.push(gap);
        }
        let gap_bucket = quantize_gap(gap) as usize;
        let response_bucket =
            quantize_response_lag(last_output_at.map(|at| event.at_ms.saturating_sub(at)));
        gap_hist[gap_bucket] += 1.0;
        response_hist[response_bucket] += 1.0;
        let len_bucket = quantize_len(event.bytes.len());
        let mask = class_mask(&event.bytes);
        let phase_bucket = (((event.at_ms % IDLE_TIMEOUT_MS) * 8) / IDLE_TIMEOUT_MS).min(7);
        let mouse_token = parse_terminal_mouse_event(&event.bytes)
            .map(|mouse| {
                ((u64::from(mouse.coord_bucket) & 0xff) << 8)
                    | if mouse.motion { 1u64 } else { 2u64 }
            })
            .unwrap_or(0);
        let token = splitmix64(
            ((gap_bucket as u64) << 24)
                | ((response_bucket as u64) << 20)
                | ((len_bucket & 0x0f) << 12)
                | ((mask as u64) << 4)
                | phase_bucket
                | (mouse_token << 28),
        );
        tokens.push(token);
        *token_counts.entry(token).or_default() += 1;
        accumulate_hash(&mut unigram_weights, token);
        if index > 0 {
            let pair_hash = splitmix64(token ^ tokens[index - 1].rotate_left(7));
            distinct_bigrams.insert(pair_hash);
            accumulate_hash(&mut bigram_weights, pair_hash);
            *transition_counts
                .entry((tokens[index - 1], token))
                .or_default() += 1;
            *source_counts.entry(tokens[index - 1]).or_default() += 1;
            accumulate_hash(&mut transition_weights, pair_hash.rotate_left(11));
        }
        if index > 1 {
            let trigram_hash = splitmix64(
                token ^ tokens[index - 1].rotate_left(11) ^ tokens[index - 2].rotate_left(23),
            );
            distinct_trigrams.insert(trigram_hash);
            accumulate_hash(&mut trigram_weights, trigram_hash);
        }
        let bucket = ((event.at_ms.saturating_sub(window_anchor)) / BURST_BUCKET_MS) as usize;
        if let Some(count) = bucket_counts.get_mut(bucket) {
            *count = count.saturating_add(event.raw_count.min(4));
        }
        total_input_bytes += if event.mouse_motion {
            3 * event.raw_count.min(4) as usize
        } else {
            event.bytes.len()
        };
        for byte in &event.bytes {
            byte_counts[*byte as usize] += 1;
            for (class_idx, present) in class_presence(*byte).into_iter().enumerate() {
                if present {
                    class_counts[class_idx] += 1;
                }
            }
        }
    }

    normalize_bins(&mut gap_hist);
    normalize_bins(&mut response_hist);
    let class_total = class_counts.iter().sum::<u32>().max(1) as f64;
    let mut class_hist = [0.0; CLASS_BUCKETS];
    for (idx, count) in class_counts.into_iter().enumerate() {
        class_hist[idx] = count as f64 / class_total;
    }
    let (spectral_peakiness, spectral_entropy_norm) = spectral_features(&bucket_counts);
    let mouse_coord_entropy = normalized_bucket_entropy(mouse_coord_counts.values().copied());
    let mouse_ratio = (mouse_events as f64 / inputs.len() as f64).clamp(0.0, 1.0);
    let mouse_motion_ratio = (mouse_motion_events as f64 / inputs.len() as f64).clamp(0.0, 1.0);
    let mouse_flood_score = if derived_inputs.is_empty() {
        0.0
    } else {
        ((mouse_motion_events as f64 / derived_inputs.len() as f64) / 6.0).clamp(0.0, 1.0)
    };

    let confidence = ((derived_inputs.len() as f64 / 12.0).min(1.0)
        * (0.35 + 0.65 * (window_ms as f64 / span_ms as f64).clamp(0.0, 1.0)))
    .clamp(0.0, 1.0);

    Some(WindowFingerprint {
        span_ms,
        confidence,
        event_rate_norm: (derived_inputs.len() as f64 * 60_000.0 / window_ms as f64 / 24.0)
            .clamp(0.0, 1.0),
        entropy_norm: normalized_entropy(&byte_counts, total_input_bytes.max(1) as f64),
        transition_entropy_norm: normalized_transition_entropy(&transition_counts, &source_counts),
        ngram_innovation: average_similarity(&[
            (token_counts.len() as f64 / derived_inputs.len().max(1) as f64).clamp(0.0, 1.0),
            (distinct_bigrams.len() as f64 / derived_inputs.len().saturating_sub(1).max(1) as f64)
                .clamp(0.0, 1.0),
            (distinct_trigrams.len() as f64 / derived_inputs.len().saturating_sub(2).max(1) as f64)
                .clamp(0.0, 1.0),
        ]),
        burstiness: fano_norm(&bucket_counts),
        periodicity_60s: periodicity_60s(&derived_inputs),
        spectral_peakiness,
        spectral_entropy_norm,
        input_output_balance: ((total_input_bytes as f64 * 24.0)
            / total_output_bytes.max(1) as f64)
            .clamp(0.0, 1.0),
        output_coupling: output_coupling_score(&response_hist),
        autocorr1: autocorrelation_norm(&gaps, 1),
        autocorr2: autocorrelation_norm(&gaps, 2),
        mouse_ratio,
        mouse_motion_ratio,
        mouse_coord_entropy,
        mouse_flood_score,
        gap_hist,
        response_hist,
        class_hist,
        unigram_hash: collapse_simhash(&unigram_weights),
        bigram_hash: collapse_simhash(&bigram_weights),
        trigram_hash: collapse_simhash(&trigram_weights),
        transition_hash: collapse_simhash(&transition_weights),
    })
}

fn archive_signatures(
    record: &LiveSessionRecord,
    now_ms: u64,
    recent_signatures: &mut VecDeque<HistoricalSignature>,
) {
    for fingerprint in build_window_fingerprints(record, now_ms) {
        recent_signatures.push_back(HistoricalSignature {
            source_session_id: record.id,
            recorded_at_ms: now_ms,
            span_ms: fingerprint.span_ms,
            fingerprint,
        });
    }
    trim_recent_signatures(recent_signatures, now_ms);
}

fn trim_recent_signatures(recent_signatures: &mut VecDeque<HistoricalSignature>, now_ms: u64) {
    while recent_signatures.front().is_some_and(|signature| {
        now_ms.saturating_sub(signature.recorded_at_ms) > SIGNATURE_RETENTION_MS
    }) {
        recent_signatures.pop_front();
    }
    while recent_signatures.len() > MAX_RECENT_SIGNATURES {
        recent_signatures.pop_front();
    }
}

impl NetworkPrefix {
    fn from_ip(ip: IpAddr) -> Self {
        match ip {
            IpAddr::V4(addr) => Self::V4(prefix_v4(addr)),
            IpAddr::V6(addr) => Self::V6(prefix_v6(addr)),
        }
    }
}

fn prefix_v4(addr: Ipv4Addr) -> u32 {
    u32::from(addr) & 0xFFFF_FF00
}

fn prefix_v6(addr: Ipv6Addr) -> u64 {
    let octets = addr.octets();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&octets[..8]);
    u64::from_be_bytes(bytes)
}

fn min_session_confidence(left: &SessionAssessment, right: &SessionAssessment) -> f64 {
    let mut confidence = 0.0;
    let mut total_weight = 0.0;
    for left_window in &left.windows {
        if let Some(right_window) = right
            .windows
            .iter()
            .find(|window| window.span_ms == left_window.span_ms)
        {
            let weight = window_weight(left_window.span_ms);
            confidence += weight * (left_window.confidence * right_window.confidence).sqrt();
            total_weight += weight;
        }
    }
    if total_weight == 0.0 {
        0.0
    } else {
        confidence / total_weight
    }
}

fn min_window_confidence(windows: &[WindowFingerprint]) -> f64 {
    windows
        .iter()
        .map(|window| window.confidence)
        .fold(1.0, f64::min)
}

fn weighted_window_average(
    windows: &[WindowFingerprint],
    f: impl Fn(&WindowFingerprint) -> f64,
) -> f64 {
    let mut total = 0.0;
    let mut total_weight = 0.0;
    for window in windows {
        let weight = window_weight(window.span_ms) * window.confidence.max(0.1);
        total += weight * f(window);
        total_weight += weight;
    }
    if total_weight == 0.0 {
        0.0
    } else {
        total / total_weight
    }
}

fn session_window_similarity(left: &SessionAssessment, right: &SessionAssessment) -> f64 {
    let mut total = 0.0;
    let mut total_weight = 0.0;
    for left_window in &left.windows {
        if let Some(right_window) = right
            .windows
            .iter()
            .find(|window| window.span_ms == left_window.span_ms)
        {
            let weight = window_weight(left_window.span_ms)
                * left_window.confidence
                * right_window.confidence;
            total += weight * window_similarity(left_window, right_window);
            total_weight += weight;
        }
    }
    if total_weight == 0.0 {
        0.0
    } else {
        total / total_weight
    }
}

fn window_similarity(left: &WindowFingerprint, right: &WindowFingerprint) -> f64 {
    let hash_similarity = average_similarity(&[
        hamming_similarity(left.unigram_hash, right.unigram_hash),
        hamming_similarity(left.bigram_hash, right.bigram_hash),
        hamming_similarity(left.trigram_hash, right.trigram_hash),
        hamming_similarity(left.transition_hash, right.transition_hash),
    ]);
    let histogram_similarity = average_similarity(&[
        cosine_similarity(&left.gap_hist, &right.gap_hist),
        cosine_similarity(&left.response_hist, &right.response_hist),
        cosine_similarity(&left.class_hist, &right.class_hist),
    ]);
    let scalar_similarity = average_similarity(&[
        similarity(left.event_rate_norm, right.event_rate_norm),
        similarity(left.entropy_norm, right.entropy_norm),
        similarity(left.transition_entropy_norm, right.transition_entropy_norm),
        similarity(left.ngram_innovation, right.ngram_innovation),
        similarity(left.burstiness, right.burstiness),
        similarity(left.periodicity_60s, right.periodicity_60s),
        similarity(left.spectral_peakiness, right.spectral_peakiness),
        similarity(left.spectral_entropy_norm, right.spectral_entropy_norm),
        similarity(left.input_output_balance, right.input_output_balance),
        similarity(left.output_coupling, right.output_coupling),
        similarity(left.autocorr1, right.autocorr1),
        similarity(left.autocorr2, right.autocorr2),
        similarity(left.mouse_ratio, right.mouse_ratio),
        similarity(left.mouse_motion_ratio, right.mouse_motion_ratio),
        similarity(left.mouse_coord_entropy, right.mouse_coord_entropy),
        similarity(left.mouse_flood_score, right.mouse_flood_score),
    ]);
    (0.42 * hash_similarity
        + 0.28 * histogram_similarity
        + 0.22 * scalar_similarity
        + 0.08 * left.confidence.min(right.confidence))
    .clamp(0.0, 1.0)
}

fn average_similarity(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn similarity(left: f64, right: f64) -> f64 {
    1.0 - (left - right).abs().clamp(0.0, 1.0)
}

fn cosine_similarity<const N: usize>(left: &[f64; N], right: &[f64; N]) -> f64 {
    let mut dot = 0.0;
    let mut left_norm = 0.0;
    let mut right_norm = 0.0;
    for idx in 0..N {
        dot += left[idx] * right[idx];
        left_norm += left[idx] * left[idx];
        right_norm += right[idx] * right[idx];
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        0.0
    } else {
        (dot / (left_norm.sqrt() * right_norm.sqrt())).clamp(0.0, 1.0)
    }
}

fn hamming_similarity(left: u64, right: u64) -> f64 {
    1.0 - (left ^ right).count_ones() as f64 / 64.0
}

fn window_weight(span_ms: u64) -> f64 {
    match span_ms {
        SHORT_WINDOW_MS => 0.26,
        MEDIUM_WINDOW_MS => 0.34,
        _ => 0.40,
    }
}

fn normalized_entropy(counts: &[u32; 256], total: f64) -> f64 {
    if total <= 0.0 {
        return 0.0;
    }
    let mut entropy = 0.0;
    for count in counts {
        if *count == 0 {
            continue;
        }
        let p = *count as f64 / total;
        entropy -= p * p.log2();
    }
    (entropy / 8.0).clamp(0.0, 1.0)
}

fn normalized_transition_entropy(
    transition_counts: &HashMap<(u64, u64), u32>,
    source_counts: &HashMap<u64, u32>,
) -> f64 {
    if transition_counts.is_empty() || source_counts.is_empty() {
        return 0.0;
    }
    let total = source_counts.values().sum::<u32>().max(1) as f64;
    let mut entropy = 0.0;
    for (source, source_count) in source_counts {
        let mut conditional = 0.0;
        for ((edge_source, _), count) in transition_counts {
            if edge_source != source {
                continue;
            }
            let p = *count as f64 / *source_count as f64;
            conditional -= p * p.log2();
        }
        entropy += (*source_count as f64 / total) * conditional;
    }
    (entropy / 6.0).clamp(0.0, 1.0)
}

fn normalize_bins<const N: usize>(bins: &mut [f64; N]) {
    let total = bins.iter().sum::<f64>();
    if total <= 0.0 {
        return;
    }
    for bin in bins.iter_mut() {
        *bin /= total;
    }
}

fn normalized_bucket_entropy(values: impl IntoIterator<Item = u32>) -> f64 {
    let counts = values
        .into_iter()
        .filter(|count| *count > 0)
        .collect::<Vec<_>>();
    let total = counts.iter().sum::<u32>() as f64;
    if total <= 0.0 {
        return 0.0;
    }
    let mut entropy = 0.0;
    for count in counts {
        let p = count as f64 / total;
        entropy -= p * p.log2();
    }
    let max_entropy = (total.max(2.0)).log2();
    if max_entropy <= 0.0 {
        0.0
    } else {
        (entropy / max_entropy).clamp(0.0, 1.0)
    }
}

fn spectral_features(counts: &[u32]) -> (f64, f64) {
    if counts.len() < 4 {
        return (0.0, 0.0);
    }
    let n = counts.len();
    let mean = counts.iter().sum::<u32>() as f64 / n as f64;
    let mut magnitudes = Vec::with_capacity(n / 2);
    for harmonic in 1..=(n / 2) {
        let mut real = 0.0;
        let mut imag = 0.0;
        for (idx, count) in counts.iter().enumerate() {
            let centered = *count as f64 - mean;
            let angle = std::f64::consts::TAU * harmonic as f64 * idx as f64 / n as f64;
            real += centered * angle.cos();
            imag -= centered * angle.sin();
        }
        magnitudes.push((real * real + imag * imag).sqrt());
    }
    let total = magnitudes.iter().sum::<f64>();
    if total <= 0.0 {
        return (0.0, 0.0);
    }
    let peak = magnitudes.iter().copied().fold(0.0_f64, f64::max) / total;
    let mut entropy = 0.0;
    for magnitude in &magnitudes {
        if *magnitude <= 0.0 {
            continue;
        }
        let p = *magnitude / total;
        entropy -= p * p.log2();
    }
    let max_entropy = (magnitudes.len().max(2) as f64).log2();
    let entropy_norm = if max_entropy <= 0.0 {
        0.0
    } else {
        (entropy / max_entropy).clamp(0.0, 1.0)
    };
    (peak.clamp(0.0, 1.0), entropy_norm)
}

fn parse_terminal_mouse_event(bytes: &[u8]) -> Option<ParsedMouseEvent> {
    parse_sgr_mouse_event(bytes).or_else(|| parse_legacy_mouse_event(bytes))
}

fn parse_sgr_mouse_event(bytes: &[u8]) -> Option<ParsedMouseEvent> {
    if bytes.len() < 6 || !bytes.starts_with(b"\x1b[<") {
        return None;
    }
    let mut idx = 3usize;
    let cb = parse_mouse_number(bytes, &mut idx)?;
    if *bytes.get(idx)? != b';' {
        return None;
    }
    idx += 1;
    let x = parse_mouse_number(bytes, &mut idx)?;
    if *bytes.get(idx)? != b';' {
        return None;
    }
    idx += 1;
    let y = parse_mouse_number(bytes, &mut idx)?;
    let terminator = *bytes.get(idx)?;
    if terminator != b'M' && terminator != b'm' {
        return None;
    }
    Some(ParsedMouseEvent {
        motion: (cb & 32) != 0,
        coord_bucket: quantize_mouse_coord(x, y),
    })
}

fn parse_legacy_mouse_event(bytes: &[u8]) -> Option<ParsedMouseEvent> {
    if bytes.len() < 6 || !bytes.starts_with(b"\x1b[M") {
        return None;
    }
    let cb = bytes[3].saturating_sub(32);
    let x = bytes[4].saturating_sub(32) as u16;
    let y = bytes[5].saturating_sub(32) as u16;
    Some(ParsedMouseEvent {
        motion: (cb & 32) != 0,
        coord_bucket: quantize_mouse_coord(x, y),
    })
}

fn parse_mouse_number(bytes: &[u8], idx: &mut usize) -> Option<u16> {
    let mut value = 0u16;
    let mut seen = false;
    while let Some(byte) = bytes.get(*idx) {
        if !byte.is_ascii_digit() {
            break;
        }
        seen = true;
        value = value
            .saturating_mul(10)
            .saturating_add(u16::from(byte.saturating_sub(b'0')));
        *idx += 1;
    }
    seen.then_some(value)
}

fn quantize_mouse_coord(x: u16, y: u16) -> u16 {
    let x_bucket = (x.min(255) / 16) & 0x0f;
    let y_bucket = (y.min(255) / 16) & 0x0f;
    (x_bucket << 4) | y_bucket
}

fn quantize_gap(gap_ms: u64) -> u64 {
    match gap_ms {
        0..=40 => 0,
        41..=120 => 1,
        121..=300 => 2,
        301..=700 => 3,
        701..=1_500 => 4,
        1_501..=3_500 => 5,
        3_501..=8_000 => 6,
        8_001..=20_000 => 7,
        20_001..=40_000 => 8,
        40_001..=75_000 => 9,
        _ => 10,
    }
}

fn quantize_response_lag(response_lag: Option<u64>) -> usize {
    match response_lag.unwrap_or(u64::MAX) {
        0..=60 => 0,
        61..=200 => 1,
        201..=500 => 2,
        501..=1_000 => 3,
        1_001..=2_000 => 4,
        2_001..=5_000 => 5,
        5_001..=15_000 => 6,
        15_001..=60_000 => 7,
        _ => 8,
    }
}

fn class_presence(byte: u8) -> [bool; CLASS_BUCKETS] {
    [
        byte == 0x1b,
        byte.is_ascii_control(),
        byte.is_ascii_whitespace(),
        byte.is_ascii_alphabetic(),
        byte.is_ascii_digit(),
        byte.is_ascii_punctuation(),
    ]
}

fn fano_norm(counts: &[u32]) -> f64 {
    if counts.len() < 2 {
        return 0.0;
    }
    let mean = counts.iter().sum::<u32>() as f64 / counts.len() as f64;
    if mean <= 0.0 {
        return 0.0;
    }
    let variance = counts
        .iter()
        .map(|count| {
            let delta = *count as f64 - mean;
            delta * delta
        })
        .sum::<f64>()
        / counts.len() as f64;
    (variance / mean / 8.0).clamp(0.0, 1.0)
}

fn periodicity_60s(inputs: &[DerivedInputEvent]) -> f64 {
    if inputs.is_empty() {
        return 0.0;
    }
    let mut sum_cos = 0.0;
    let mut sum_sin = 0.0;
    for sample in inputs {
        let phase = (sample.at_ms % IDLE_TIMEOUT_MS) as f64 / IDLE_TIMEOUT_MS as f64;
        let angle = phase * std::f64::consts::TAU;
        sum_cos += angle.cos();
        sum_sin += angle.sin();
    }
    ((sum_cos.powi(2) + sum_sin.powi(2)).sqrt() / inputs.len() as f64).clamp(0.0, 1.0)
}

fn output_coupling_score(response_hist: &[f64; RESPONSE_BUCKETS]) -> f64 {
    let near = response_hist[0] + response_hist[1] + response_hist[2] + response_hist[3];
    let mid = response_hist[4] + response_hist[5];
    let far = response_hist[6] + response_hist[7] + response_hist[8];
    (0.75 * near + 0.25 * mid - 0.35 * far).clamp(0.0, 1.0)
}

fn autocorrelation_norm(values: &[u64], lag: usize) -> f64 {
    if values.len() <= lag + 1 {
        return 0.0;
    }
    let mean = values.iter().sum::<u64>() as f64 / values.len() as f64;
    let mut numerator = 0.0;
    let mut denominator = 0.0;
    for value in values {
        let centered = *value as f64 - mean;
        denominator += centered * centered;
    }
    if denominator <= 0.0 {
        return 0.0;
    }
    for idx in 0..(values.len() - lag) {
        numerator += (values[idx] as f64 - mean) * (values[idx + lag] as f64 - mean);
    }
    ((numerator / denominator) + 1.0)
        .mul_add(0.5, 0.0)
        .clamp(0.0, 1.0)
}

fn quantize_len(len: usize) -> u64 {
    match len {
        0 => 0,
        1 => 1,
        2 => 2,
        3..=4 => 3,
        5..=8 => 4,
        9..=16 => 5,
        _ => 6,
    }
}

fn class_mask(bytes: &[u8]) -> u8 {
    let mut mask = 0u8;
    for byte in bytes {
        if *byte == 0x1b {
            mask |= 1 << 0;
        }
        if byte.is_ascii_control() {
            mask |= 1 << 1;
        }
        if byte.is_ascii_whitespace() {
            mask |= 1 << 2;
        }
        if byte.is_ascii_alphabetic() {
            mask |= 1 << 3;
        }
        if byte.is_ascii_digit() {
            mask |= 1 << 4;
        }
        if byte.is_ascii_punctuation() {
            mask |= 1 << 5;
        }
    }
    mask
}

fn accumulate_hash(weights: &mut [i32; 64], hash: u64) {
    for (idx, weight) in weights.iter_mut().enumerate() {
        if (hash >> idx) & 1 == 1 {
            *weight += 1;
        } else {
            *weight -= 1;
        }
    }
}

fn collapse_simhash(weights: &[i32; 64]) -> u64 {
    let mut hash = 0u64;
    for (idx, weight) in weights.iter().enumerate() {
        if *weight >= 0 {
            hash |= 1u64 << idx;
        }
    }
    hash
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn edge_key(a: u64, b: u64) -> (u64, u64) {
    if a < b { (a, b) } else { (b, a) }
}

struct DisjointSet {
    parent: Vec<usize>,
    rank: Vec<u8>,
}

impl DisjointSet {
    fn new(size: usize) -> Self {
        Self {
            parent: (0..size).collect(),
            rank: vec![0; size],
        }
    }

    fn find(&mut self, x: usize) -> usize {
        if self.parent[x] != x {
            let root = self.find(self.parent[x]);
            self.parent[x] = root;
        }
        self.parent[x]
    }

    fn union(&mut self, a: usize, b: usize) {
        let mut root_a = self.find(a);
        let mut root_b = self.find(b);
        if root_a == root_b {
            return;
        }
        if self.rank[root_a] < self.rank[root_b] {
            std::mem::swap(&mut root_a, &mut root_b);
        }
        self.parent[root_b] = root_a;
        if self.rank[root_a] == self.rank[root_b] {
            self.rank[root_a] += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ipv4(value: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, value))
    }

    fn ipv4_in(subnet: u8, host: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(198, 51, subnet, host))
    }

    fn build_session(
        id: u64,
        ip: IpAddr,
        transport: Transport,
        started_at_ms: u64,
        input_events: &[(u64, &'static [u8])],
        output_offsets: &[u64],
    ) -> LiveSessionRecord {
        let (tx, _) = watch::channel(SessionControl::Active);
        let inputs = input_events
            .iter()
            .map(|(at_ms, bytes)| InputSample {
                at_ms: *at_ms,
                bytes: bytes
                    .iter()
                    .copied()
                    .take(MAX_SAMPLE_BYTES)
                    .collect::<SmallVec<[u8; MAX_SAMPLE_BYTES]>>(),
            })
            .collect::<VecDeque<_>>();
        let outputs = output_offsets
            .iter()
            .map(|offset| OutputSample {
                at_ms: started_at_ms + *offset,
                bytes: 6_000,
            })
            .collect::<VecDeque<_>>();
        LiveSessionRecord {
            id,
            client_ip: ip,
            ip_prefix: NetworkPrefix::from_ip(ip),
            transport,
            started_at_ms,
            inputs,
            outputs,
            control: SessionControl::Active,
            control_tx: tx,
        }
    }

    fn build_session_owned(
        id: u64,
        ip: IpAddr,
        transport: Transport,
        started_at_ms: u64,
        input_events: Vec<(u64, Vec<u8>)>,
        output_offsets: &[u64],
    ) -> LiveSessionRecord {
        let (tx, _) = watch::channel(SessionControl::Active);
        let inputs = input_events
            .into_iter()
            .map(|(at_ms, bytes)| InputSample {
                at_ms,
                bytes: bytes
                    .into_iter()
                    .take(MAX_SAMPLE_BYTES)
                    .collect::<SmallVec<[u8; MAX_SAMPLE_BYTES]>>(),
            })
            .collect::<VecDeque<_>>();
        let outputs = output_offsets
            .iter()
            .map(|offset| OutputSample {
                at_ms: started_at_ms + *offset,
                bytes: 6_000,
            })
            .collect::<VecDeque<_>>();
        LiveSessionRecord {
            id,
            client_ip: ip,
            ip_prefix: NetworkPrefix::from_ip(ip),
            transport,
            started_at_ms,
            inputs,
            outputs,
            control: SessionControl::Active,
            control_tx: tx,
        }
    }

    fn evaluate(
        records: Vec<LiveSessionRecord>,
        recent_signatures: VecDeque<HistoricalSignature>,
        pressure: f64,
        now_ms: u64,
    ) -> ClusterEvaluation {
        let map = records
            .into_iter()
            .map(|record| (record.id, record))
            .collect();
        evaluate_clusters(&map, &recent_signatures, pressure, now_ms)
    }

    fn debug_summary(
        records: &[LiveSessionRecord],
        recent_signatures: &VecDeque<HistoricalSignature>,
        pressure: f64,
        now_ms: u64,
    ) -> String {
        let mut sessions = records
            .iter()
            .filter_map(|record| SessionAssessment::from_record(record, now_ms))
            .collect::<Vec<_>>();
        apply_replay_evidence(&mut sessions, recent_signatures, now_ms);
        let mut lines = Vec::new();
        for session in &sessions {
            lines.push(format!(
                "session={} retain={:.3} low={:.3} replay={:.3} matches={} density={:.3} coupling_deficit={:.3} mouse={:.3}/{:.3} mouse_entropy={:.3} spectral={:.3}/{:.3}",
                session.session_id,
                session.retention_score,
                session.low_engagement,
                session.replay_score,
                session.replay_match_count,
                session.replay_density,
                session.coupling_deficit,
                session.mouse_motion_ratio,
                session.mouse_coord_entropy,
                session.mouse_coord_entropy,
                session.spectral_peakiness,
                session.spectral_entropy_norm,
            ));
        }
        for i in 0..sessions.len() {
            for j in (i + 1)..sessions.len() {
                let similarity = session_window_similarity(&sessions[i], &sessions[j]);
                let min_confidence = min_session_confidence(&sessions[i], &sessions[j]);
                let edge = pair_evidence(&sessions[i], &sessions[j], pressure)
                    .map(|edge| edge.score)
                    .unwrap_or(0.0);
                lines.push(format!(
                    "pair={}-{} sim={:.3} conf={:.3} edge={:.3}",
                    sessions[i].session_id,
                    sessions[j].session_id,
                    similarity,
                    min_confidence,
                    edge,
                ));
            }
        }
        lines.join("\n")
    }

    fn archive(records: &[LiveSessionRecord], now_ms: u64) -> VecDeque<HistoricalSignature> {
        let mut signatures = VecDeque::new();
        for record in records {
            archive_signatures(record, now_ms, &mut signatures);
        }
        signatures
    }

    fn replay_trace(now_ms: u64, seed: u64) -> Vec<(u64, &'static [u8])> {
        vec![
            (now_ms - 220_000 + seed * 3, b"\x1b[A"),
            (now_ms - 205_000 + seed * 5, b"d"),
            (now_ms - 191_000 + seed * 2, b" "),
            (now_ms - 170_000 + seed * 4, b"\x1b[C"),
            (now_ms - 151_000 + seed * 3, b"w"),
            (now_ms - 122_000 + seed * 4, b"\r"),
            (now_ms - 96_000 + seed * 2, b"a"),
            (now_ms - 72_000 + seed * 3, b"s"),
            (now_ms - 44_000 + seed * 2, b"\x1b[B"),
            (now_ms - 19_000 + seed * 4, b"1"),
        ]
    }

    fn jittered_random_trace(now_ms: u64, family: u64, variant: u64) -> Vec<(u64, &'static [u8])> {
        let alphabet: [&'static [u8]; 10] =
            [b"a", b"s", b"d", b"f", b"j", b"k", b"l", b";", b"1", b"2"];
        let mut at_ms = now_ms - 250_000;
        let mut events = Vec::new();
        for step in 0..8 {
            let noise = splitmix64(family ^ ((variant + 1) << 8) ^ step as u64);
            let gap = 18_000 + family * 1_300 + (noise % 24_000) + variant * 90;
            at_ms += gap;
            let bytes = alphabet[((noise >> 8) as usize + step) % alphabet.len()];
            events.push((at_ms, bytes));
        }
        events
    }

    fn burst_macro_trace(now_ms: u64, family: u64, variant: u64) -> Vec<(u64, &'static [u8])> {
        let bursts: [[&'static [u8]; 3]; 3] = [
            [b"\x1b[A", b" ", b"\r"],
            [b"\x1b[C", b"\x1b[C", b"z"],
            [b"\x1b[B", b"x", b"1"],
        ];
        let mut events = Vec::new();
        let mut anchor = now_ms - 210_000;
        for (burst_idx, burst) in bursts.into_iter().enumerate() {
            let base_gap = 52_000 + family * 1_400 + variant * 110;
            anchor += base_gap + burst_idx as u64 * 1_700;
            for (event_idx, bytes) in burst.into_iter().enumerate() {
                events.push((anchor + event_idx as u64 * (90 + variant * 3), bytes));
            }
        }
        events
    }

    fn noise_injected_replay_trace(now_ms: u64, seed: u64) -> Vec<(u64, &'static [u8])> {
        let payload_variants: [[&'static [u8]; 2]; 10] = [
            [b"\x1b[A", b"w"],
            [b"d", b"\x1b[C"],
            [b" ", b"\r"],
            [b"\x1b[C", b"e"],
            [b"w", b"\x1b[A"],
            [b"\r", b" "],
            [b"a", b"\x1b[D"],
            [b"s", b"x"],
            [b"\x1b[B", b"q"],
            [b"1", b"2"],
        ];
        replay_trace(now_ms, seed)
            .into_iter()
            .enumerate()
            .map(|(idx, (at_ms, _))| {
                let noise = splitmix64(seed ^ (idx as u64).wrapping_mul(0x9E37_79B9));
                let jitter = (noise % 1_400) as i64 - 700;
                let payload = payload_variants[idx][((noise >> 8) & 1) as usize];
                (((at_ms as i64) + jitter).max(0) as u64, payload)
            })
            .collect()
    }

    fn human_random_trace(now_ms: u64, seed: u64) -> Vec<(u64, &'static [u8])> {
        let palette: [&'static [u8]; 18] = [
            b"w", b"a", b"s", b"d", b"q", b"e", b"r", b" ", b"\r", b"\x1b[A", b"\x1b[B", b"\x1b[C",
            b"\x1b[D", b"1", b"2", b"3", b"z", b"x",
        ];
        let mut at_ms = now_ms - 210_000 - seed * 1_500;
        let mut events = Vec::new();
        for step in 0..14 {
            let noise = splitmix64(seed.rotate_left(9) ^ (step as u64).wrapping_mul(0xBF58_476D));
            let gap = 3_500 + (noise % 19_000) + (step as u64 % 3) * 1_700;
            at_ms += gap;
            let bytes = palette[((noise >> 11) as usize + step * 3) % palette.len()];
            events.push((at_ms, bytes));
        }
        events
    }

    fn rich_token(seed: u64) -> Vec<u8> {
        let bucket = (seed % 82) as u8;
        match bucket {
            0..=25 => vec![b'a' + bucket],
            26..=51 => vec![b'A' + (bucket - 26)],
            52..=61 => vec![b'0' + (bucket - 52)],
            62 => b" ".to_vec(),
            63 => b"\r".to_vec(),
            64 => b"\t".to_vec(),
            65 => b"\x1b[A".to_vec(),
            66 => b"\x1b[B".to_vec(),
            67 => b"\x1b[C".to_vec(),
            68 => b"\x1b[D".to_vec(),
            69 => b"!".to_vec(),
            70 => b"?".to_vec(),
            71 => b"#".to_vec(),
            72 => b"@".to_vec(),
            73 => b"%".to_vec(),
            74 => b"&".to_vec(),
            75 => b"*".to_vec(),
            76 => b"(".to_vec(),
            77 => b")".to_vec(),
            78 => b"=".to_vec(),
            79 => b"+".to_vec(),
            80 => b"/".to_vec(),
            _ => b"-".to_vec(),
        }
    }

    fn full_alphabet_bot_trace(now_ms: u64, family: u64, variant: u64) -> Vec<(u64, Vec<u8>)> {
        let mut at_ms = now_ms - 320_000 + variant * 37;
        let mut events = Vec::new();
        for step in 0..24 {
            let noise = splitmix64(
                family.rotate_left(7)
                    ^ (variant + 1).wrapping_mul(0x9E37_79B9)
                    ^ (step as u64).wrapping_mul(0xBF58_476D),
            );
            let lane = (step % 4) as u64;
            let gap = 4_800 + family * 420 + lane * 1_350 + (noise % 4_600);
            at_ms += gap;
            let token_seed =
                family * 113 + lane * 29 + (step / 3) as u64 * 41 + ((noise >> 12) & 0x0f);
            events.push((at_ms, rich_token(token_seed)));
            if step % 6 == 5 {
                events.push((at_ms + 18 + (noise % 27), rich_token(token_seed ^ 0x55)));
            }
        }
        events
    }

    fn full_alphabet_human_trace(now_ms: u64, seed: u64) -> Vec<(u64, Vec<u8>)> {
        let mut at_ms = now_ms - 260_000 - seed * 1_200;
        let mut events = Vec::new();
        for step in 0..30 {
            let noise = splitmix64(seed.rotate_left(11) ^ (step as u64).wrapping_mul(0x94D0_49BB));
            let base_gap = match step % 5 {
                0 => 1_300,
                1 => 4_800,
                2 => 9_200,
                3 => 2_200,
                _ => 14_500,
            };
            at_ms += base_gap + (noise % 8_200);
            events.push((at_ms, rich_token(seed ^ noise ^ ((step as u64) << 6))));
            if step % 7 == 2 {
                events.push((
                    at_ms + 120 + (noise % 480),
                    rich_token(seed ^ noise.rotate_left(9) ^ 0xA5A5),
                ));
            }
            if step % 9 == 4 {
                events.push((at_ms + 35 + (noise % 90), b"\x08".to_vec()));
            }
        }
        events
    }

    fn full_alphabet_sparse_keepalive_trace(
        now_ms: u64,
        family: u64,
        variant: u64,
    ) -> Vec<(u64, Vec<u8>)> {
        let mut at_ms = now_ms - 310_000 + variant * 33;
        let mut events = Vec::new();
        for step in 0..6 {
            let noise = splitmix64(family.rotate_left(5) ^ variant.rotate_left(17) ^ step as u64);
            let gap = 58_000 + family * 190 + (noise % 2_700);
            at_ms += gap;
            events.push((
                at_ms,
                rich_token(family * 31 + variant * 7 + step as u64 * 13),
            ));
        }
        events
    }

    fn mouse_sgr_motion(x: u16, y: u16) -> Vec<u8> {
        format!("\x1b[<35;{};{}M", x, y).into_bytes()
    }

    fn human_mouse_hover_trace(now_ms: u64, seed: u64) -> Vec<(u64, Vec<u8>)> {
        let mut at_ms = now_ms - 110_000 - seed * 800;
        let mut x = 18 + (seed as u16 % 9);
        let mut y = 10 + (seed as u16 % 7);
        let mut events = Vec::new();
        for step in 0..56 {
            let noise = splitmix64(seed.rotate_left(5) ^ (step as u64).wrapping_mul(0x94D0_49BB));
            let gap = 42 + (noise % 37) + (step as u64 % 5) * 6;
            at_ms += gap;
            let dx = ((noise & 0x0f) as i16) - 7;
            let dy = (((noise >> 4) & 0x0f) as i16) - 7;
            x = (x as i16 + dx).clamp(2, 120) as u16;
            y = (y as i16 + dy).clamp(2, 50) as u16;
            events.push((at_ms, mouse_sgr_motion(x, y)));
            if step % 11 == 0 {
                events.push((at_ms + 14, b" ".to_vec()));
            }
            if step % 17 == 4 {
                events.push((at_ms + 27, b"\x1b[C".to_vec()));
            }
        }
        events
    }

    fn mouse_sweep_bot_trace(now_ms: u64, family: u64, variant: u64) -> Vec<(u64, Vec<u8>)> {
        let mut at_ms = now_ms - 120_000 + variant * 9;
        let mut events = Vec::new();
        for lap in 0..5 {
            let base_x = 12 + family as u16 * 3;
            let base_y = 8 + ((variant + lap) % 3) as u16 * 2;
            for step in 0..14 {
                at_ms += 28 + (lap % 2) * 2;
                let x = base_x + step as u16 * 3;
                let y = base_y + ((step / 3) % 4) as u16;
                events.push((at_ms, mouse_sgr_motion(x, y)));
            }
            at_ms += 1_200 + family * 15;
            events.push((at_ms, b"\r".to_vec()));
        }
        events
    }

    #[test]
    fn replay_library_cohort_is_evicted_under_high_pressure() {
        let now_ms = 900_000;
        let history_records = (0..6)
            .map(|idx| {
                let started = now_ms - 600_000 - idx as u64 * 4_000;
                let trace = replay_trace(now_ms - 300_000, idx as u64);
                build_session(
                    idx + 1,
                    ipv4((idx + 1) as u8),
                    if idx % 2 == 0 {
                        Transport::Ssh
                    } else {
                        Transport::Web
                    },
                    started,
                    &trace,
                    &[10_000, 38_000, 75_000, 112_000, 148_000, 181_000, 213_000],
                )
            })
            .collect::<Vec<_>>();
        let history = archive(&history_records, now_ms - 240_000);

        let active_records = (0..6)
            .map(|idx| {
                let trace = replay_trace(now_ms, (5 - idx) as u64);
                build_session(
                    100 + idx as u64,
                    IpAddr::V4(Ipv4Addr::new(198, 51, 100, (idx + 10) as u8)),
                    if idx % 2 == 0 {
                        Transport::Web
                    } else {
                        Transport::Ssh
                    },
                    now_ms - 260_000,
                    &trace,
                    &[9_000, 36_000, 74_000, 109_000, 145_000, 179_000, 214_000],
                )
            })
            .collect::<Vec<_>>();

        let debug = debug_summary(&active_records, &history, 0.96, now_ms);
        let evaluation = evaluate(active_records, history, 0.96, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 1, "{debug}");
        assert_eq!(evaluation.evicted_session_ids.len(), 6, "{debug}");
    }

    #[test]
    fn replay_library_is_observed_but_not_evicted_at_low_pressure() {
        let now_ms = 900_000;
        let history_records = (0..4)
            .map(|idx| {
                build_session(
                    idx + 1,
                    ipv4((idx + 1) as u8),
                    Transport::Ssh,
                    now_ms - 500_000,
                    &replay_trace(now_ms - 200_000, idx as u64),
                    &[12_000, 44_000, 81_000, 126_000, 173_000],
                )
            })
            .collect::<Vec<_>>();
        let history = archive(&history_records, now_ms - 180_000);
        let active_records = (0..4)
            .map(|idx| {
                build_session(
                    100 + idx as u64,
                    ipv4((idx + 20) as u8),
                    Transport::Web,
                    now_ms - 240_000,
                    &replay_trace(now_ms, idx as u64),
                    &[12_000, 44_000, 81_000, 126_000, 173_000],
                )
            })
            .collect::<Vec<_>>();

        let evaluation = evaluate(active_records, history, 0.20, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 0);
        assert!(evaluation.evicted_session_ids.is_empty());
    }

    #[test]
    fn isolated_replay_match_does_not_trigger_singleton_eviction() {
        let now_ms = 600_000;
        let history_record = build_session(
            1,
            ipv4(1),
            Transport::Ssh,
            now_ms - 400_000,
            &replay_trace(now_ms - 200_000, 0),
            &[10_000, 40_000, 80_000, 120_000, 160_000],
        );
        let history = archive(&[history_record], now_ms - 180_000);
        let active = build_session(
            2,
            ipv4(33),
            Transport::Web,
            now_ms - 220_000,
            &replay_trace(now_ms, 0),
            &[10_000, 40_000, 80_000, 120_000, 160_000],
        );

        let evaluation = evaluate(vec![active], history, 0.98, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 0);
        assert!(evaluation.evicted_session_ids.is_empty());
    }

    #[test]
    fn sparse_keepalive_swarm_is_still_detected() {
        let now_ms = 500_000;
        let records = (0..6)
            .map(|idx| {
                build_session(
                    idx + 1,
                    ipv4((idx + 1) as u8),
                    Transport::Ssh,
                    now_ms - 6 * 60_000,
                    &[
                        (now_ms - 300_000 + idx as u64 * 40, b"a"),
                        (now_ms - 240_500 + idx as u64 * 35, b"9"),
                        (now_ms - 180_250 + idx as u64 * 45, b"b"),
                        (now_ms - 120_400 + idx as u64 * 30, b"1"),
                        (now_ms - 60_350 + idx as u64 * 50, b"c"),
                    ],
                    &[20_000, 80_000, 140_000, 200_000, 260_000, 320_000],
                )
            })
            .collect::<Vec<_>>();

        let history = VecDeque::new();
        let debug = debug_summary(&records, &history, 0.92, now_ms);
        let evaluation = evaluate(records, history, 0.92, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 1, "{debug}");
        assert_eq!(evaluation.evicted_session_ids.len(), 4, "{debug}");
    }

    #[test]
    fn randomized_interval_keypress_swarm_is_detected() {
        let now_ms = 1_000_000;
        let records = (0..8)
            .map(|idx| {
                let trace = jittered_random_trace(now_ms, 3, idx as u64);
                build_session(
                    idx + 1,
                    IpAddr::V4(Ipv4Addr::new(198, 51, 100, (idx + 30) as u8)),
                    if idx % 2 == 0 {
                        Transport::Ssh
                    } else {
                        Transport::Web
                    },
                    now_ms - 280_000,
                    &trace,
                    &[
                        12_000, 44_000, 72_000, 110_000, 144_000, 177_000, 208_000, 241_000,
                    ],
                )
            })
            .collect::<Vec<_>>();

        let history = VecDeque::new();
        let debug = debug_summary(&records, &history, 0.91, now_ms);
        let evaluation = evaluate(records, history, 0.91, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 1, "{debug}");
        assert!(evaluation.evicted_session_ids.len() >= 4, "{debug}");
    }

    #[test]
    fn randomized_interval_keypress_swarm_is_detected_across_prefixes() {
        let now_ms = 1_040_000;
        let records = (0..8)
            .map(|idx| {
                let trace = jittered_random_trace(now_ms, 4, idx as u64);
                build_session(
                    idx + 1,
                    ipv4_in(40 + idx as u8, 10 + idx as u8),
                    if idx % 2 == 0 {
                        Transport::Web
                    } else {
                        Transport::Ssh
                    },
                    now_ms - 290_000 + idx as u64 * 900,
                    &trace,
                    &[
                        10_000, 39_000, 71_000, 104_000, 138_000, 173_000, 209_000, 243_000,
                    ],
                )
            })
            .collect::<Vec<_>>();

        let history = VecDeque::new();
        let debug = debug_summary(&records, &history, 0.94, now_ms);
        let evaluation = evaluate(records, history, 0.94, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 1, "{debug}");
        assert!(evaluation.evicted_session_ids.len() >= 4, "{debug}");
    }

    #[test]
    fn burst_macro_bot_family_is_detected() {
        let now_ms = 1_200_000;
        let records = (0..6)
            .map(|idx| {
                let trace = burst_macro_trace(now_ms, 5, idx as u64);
                build_session(
                    idx + 1,
                    ipv4((idx + 50) as u8),
                    Transport::Ssh,
                    now_ms - 240_000,
                    &trace,
                    &[
                        8_000, 24_000, 41_000, 66_000, 91_000, 117_000, 143_000, 168_000, 194_000,
                    ],
                )
            })
            .collect::<Vec<_>>();

        let history = VecDeque::new();
        let debug = debug_summary(&records, &history, 0.88, now_ms);
        let evaluation = evaluate(records, history, 0.88, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 1, "{debug}");
        assert!(evaluation.evicted_session_ids.len() >= 2, "{debug}");
    }

    #[test]
    fn noise_injected_replay_family_is_detected() {
        let now_ms = 1_250_000;
        let history_records = (0..5)
            .map(|idx| {
                build_session(
                    idx + 1,
                    ipv4_in(60 + idx as u8, 20 + idx as u8),
                    if idx % 2 == 0 {
                        Transport::Ssh
                    } else {
                        Transport::Web
                    },
                    now_ms - 700_000 - idx as u64 * 5_500,
                    &noise_injected_replay_trace(now_ms - 330_000, idx as u64),
                    &[9_000, 37_000, 74_000, 111_000, 149_000, 184_000, 219_000],
                )
            })
            .collect::<Vec<_>>();
        let history = archive(&history_records, now_ms - 260_000);

        let active_records = (0..6)
            .map(|idx| {
                build_session(
                    100 + idx as u64,
                    ipv4_in(90 + idx as u8, 30 + idx as u8),
                    if idx % 2 == 0 {
                        Transport::Web
                    } else {
                        Transport::Ssh
                    },
                    now_ms - 300_000 + idx as u64 * 700,
                    &noise_injected_replay_trace(now_ms, (idx % 5) as u64),
                    &[11_000, 36_000, 72_000, 107_000, 143_000, 179_000, 214_000],
                )
            })
            .collect::<Vec<_>>();

        let debug = debug_summary(&active_records, &history, 0.95, now_ms);
        let evaluation = evaluate(active_records, history, 0.95, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 1, "{debug}");
        assert!(evaluation.evicted_session_ids.len() >= 4, "{debug}");
    }

    #[test]
    fn human_mouse_hover_flood_is_not_clustered() {
        let now_ms = 1_420_000;
        let records = (0..4)
            .map(|idx| {
                build_session_owned(
                    idx + 1,
                    ipv4_in(150 + idx as u8, 60 + idx as u8),
                    if idx % 2 == 0 {
                        Transport::Ssh
                    } else {
                        Transport::Web
                    },
                    now_ms - 180_000 - idx as u64 * 2_000,
                    human_mouse_hover_trace(now_ms, 200 + idx as u64 * 13),
                    &[
                        6_000, 24_000, 43_000, 66_000, 91_000, 117_000, 142_000, 168_000,
                    ],
                )
            })
            .collect::<Vec<_>>();

        let history = VecDeque::new();
        let debug = debug_summary(&records, &history, 0.96, now_ms);
        let evaluation = evaluate(records, history, 0.96, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 0, "{debug}");
        assert!(evaluation.evicted_session_ids.is_empty(), "{debug}");
    }

    #[test]
    fn deterministic_mouse_motion_bot_family_is_detected() {
        let now_ms = 1_480_000;
        let records = (0..6)
            .map(|idx| {
                build_session_owned(
                    idx + 1,
                    ipv4_in(180 + idx as u8, 80 + idx as u8),
                    if idx % 2 == 0 {
                        Transport::Ssh
                    } else {
                        Transport::Web
                    },
                    now_ms - 200_000 + idx as u64 * 500,
                    mouse_sweep_bot_trace(now_ms, 4, idx as u64),
                    &[
                        9_000, 31_000, 54_000, 76_000, 99_000, 121_000, 145_000, 167_000,
                    ],
                )
            })
            .collect::<Vec<_>>();

        let history = VecDeque::new();
        let debug = debug_summary(&records, &history, 0.95, now_ms);
        let evaluation = evaluate(records, history, 0.95, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 1, "{debug}");
        assert!(evaluation.evicted_session_ids.len() >= 4, "{debug}");
    }

    #[test]
    fn full_alphabet_randomized_bot_swarm_is_detected() {
        let now_ms = 1_560_000;
        let records = (0..10)
            .map(|idx| {
                build_session_owned(
                    idx + 1,
                    ipv4_in(200 + idx as u8, 20 + idx as u8),
                    if idx % 2 == 0 {
                        Transport::Ssh
                    } else {
                        Transport::Web
                    },
                    now_ms - 280_000 + idx as u64 * 700,
                    full_alphabet_bot_trace(now_ms, 9, idx as u64),
                    &[
                        10_000, 33_000, 57_000, 81_000, 108_000, 136_000, 163_000, 191_000,
                    ],
                )
            })
            .collect::<Vec<_>>();

        let history = VecDeque::new();
        let debug = debug_summary(&records, &history, 0.95, now_ms);
        let evaluation = evaluate(records, history, 0.95, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 1, "{debug}");
        assert!(evaluation.evicted_session_ids.len() >= 4, "{debug}");
    }

    #[test]
    fn full_alphabet_sparse_idle_renewal_swarm_is_detected() {
        let now_ms = 1_620_000;
        let records = (0..8)
            .map(|idx| {
                build_session_owned(
                    idx + 1,
                    ipv4_in(220 + idx as u8, 40 + idx as u8),
                    Transport::Ssh,
                    now_ms - 7 * 60_000,
                    full_alphabet_sparse_keepalive_trace(now_ms, 6, idx as u64),
                    &[18_000, 77_000, 136_000, 194_000, 253_000, 311_000],
                )
            })
            .collect::<Vec<_>>();

        let history = VecDeque::new();
        let debug = debug_summary(&records, &history, 0.94, now_ms);
        let evaluation = evaluate(records, history, 0.94, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 1, "{debug}");
        assert!(evaluation.evicted_session_ids.len() >= 4, "{debug}");
    }

    #[test]
    fn independent_full_alphabet_humans_are_not_clustered() {
        let now_ms = 1_680_000;
        let records = (0..8)
            .map(|idx| {
                build_session_owned(
                    idx + 1,
                    ipv4_in(240 + idx as u8, 60 + idx as u8),
                    if idx % 2 == 0 {
                        Transport::Ssh
                    } else {
                        Transport::Web
                    },
                    now_ms - 260_000 - idx as u64 * 1_500,
                    full_alphabet_human_trace(now_ms, 300 + idx as u64 * 23),
                    &[
                        9_000, 28_000, 49_000, 73_000, 98_000, 126_000, 154_000, 183_000,
                    ],
                )
            })
            .collect::<Vec<_>>();

        let history = VecDeque::new();
        let debug = debug_summary(&records, &history, 0.97, now_ms);
        let evaluation = evaluate(records, history, 0.97, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 0, "{debug}");
        assert!(evaluation.evicted_session_ids.is_empty(), "{debug}");
    }

    #[test]
    fn active_human_like_sessions_do_not_cluster() {
        let now_ms = 700_000;
        let records = vec![
            build_session(
                1,
                ipv4(1),
                Transport::Ssh,
                now_ms - 150_000,
                &[
                    (now_ms - 145_000, b"\x1b[A"),
                    (now_ms - 141_200, b"d"),
                    (now_ms - 134_000, b" "),
                    (now_ms - 121_000, b"\x1b[C"),
                    (now_ms - 108_000, b"\r"),
                    (now_ms - 92_500, b"a"),
                    (now_ms - 76_000, b"s"),
                    (now_ms - 51_500, b"\x1b[B"),
                    (now_ms - 27_000, b"1"),
                    (now_ms - 9_000, b"\x1b[D"),
                ],
                &[5_000, 22_000, 39_000, 61_000, 79_000, 103_000, 129_000],
            ),
            build_session(
                2,
                ipv4(2),
                Transport::Web,
                now_ms - 155_000,
                &[
                    (now_ms - 147_000, b"w"),
                    (now_ms - 139_000, b"\x1b[D"),
                    (now_ms - 131_000, b" "),
                    (now_ms - 118_000, b"e"),
                    (now_ms - 97_500, b"\x1b[A"),
                    (now_ms - 88_000, b"q"),
                    (now_ms - 63_000, b"r"),
                    (now_ms - 45_000, b"\r"),
                    (now_ms - 24_000, b"2"),
                    (now_ms - 6_000, b"\x1b[C"),
                ],
                &[8_000, 28_000, 46_000, 68_000, 90_000, 114_000, 136_000],
            ),
            build_session(
                3,
                ipv4(3),
                Transport::Ssh,
                now_ms - 160_000,
                &[
                    (now_ms - 149_000, b"\x1b[C"),
                    (now_ms - 144_500, b"z"),
                    (now_ms - 132_000, b"x"),
                    (now_ms - 116_000, b" "),
                    (now_ms - 100_000, b"\x1b[A"),
                    (now_ms - 78_000, b"c"),
                    (now_ms - 57_000, b"v"),
                    (now_ms - 34_000, b"\r"),
                    (now_ms - 17_000, b"\x1b[B"),
                    (now_ms - 4_000, b"3"),
                ],
                &[11_000, 32_000, 54_000, 72_000, 93_000, 119_000, 143_000],
            ),
        ];

        let history = VecDeque::new();
        let debug = debug_summary(&records, &history, 0.94, now_ms);
        let evaluation = evaluate(records, history, 0.94, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 0, "{debug}");
        assert!(evaluation.evicted_session_ids.is_empty(), "{debug}");
    }

    #[test]
    fn independent_randomized_humans_are_not_clustered() {
        let now_ms = 1_300_000;
        let records = (0..6)
            .map(|idx| {
                build_session(
                    idx + 1,
                    ipv4_in(120 + idx as u8, 40 + idx as u8),
                    if idx % 2 == 0 {
                        Transport::Ssh
                    } else {
                        Transport::Web
                    },
                    now_ms - 240_000 - idx as u64 * 3_000,
                    &human_random_trace(now_ms, 100 + idx as u64 * 17),
                    &[
                        7_000, 24_000, 43_000, 61_000, 84_000, 102_000, 129_000, 151_000, 176_000,
                    ],
                )
            })
            .collect::<Vec<_>>();

        let history = VecDeque::new();
        let debug = debug_summary(&records, &history, 0.97, now_ms);
        let evaluation = evaluate(records, history, 0.97, now_ms);
        assert_eq!(evaluation.suspicious_cluster_count, 0, "{debug}");
        assert!(evaluation.evicted_session_ids.is_empty(), "{debug}");
    }

    #[test]
    fn older_session_survives_when_cluster_is_shrunk() {
        let now_ms = 800_000;
        let history_records = (0..6)
            .map(|idx| {
                build_session(
                    idx + 1,
                    ipv4((idx + 1) as u8),
                    if idx % 2 == 0 {
                        Transport::Ssh
                    } else {
                        Transport::Web
                    },
                    now_ms - 620_000 - idx as u64 * 3_000,
                    &noise_injected_replay_trace(now_ms - 250_000, (idx % 3) as u64),
                    &[10_000, 37_000, 74_000, 111_000, 148_000, 186_000],
                )
            })
            .collect::<Vec<_>>();
        let history = archive(&history_records, now_ms - 200_000);
        let old = build_session(
            20,
            ipv4(40),
            Transport::Ssh,
            now_ms - 320_000,
            &noise_injected_replay_trace(now_ms, 0),
            &[10_000, 37_000, 74_000, 111_000, 148_000, 186_000],
        );
        let newer_a = build_session(
            21,
            ipv4(41),
            Transport::Ssh,
            now_ms - 220_000,
            &noise_injected_replay_trace(now_ms, 1),
            &[10_000, 37_000, 74_000, 111_000, 148_000, 186_000],
        );
        let newer_b = build_session(
            22,
            ipv4(42),
            Transport::Web,
            now_ms - 220_000,
            &noise_injected_replay_trace(now_ms, 2),
            &[10_000, 37_000, 74_000, 111_000, 148_000, 186_000],
        );

        let records = vec![old, newer_a, newer_b];
        let debug = debug_summary(&records, &history, 0.88, now_ms);
        let evaluation = evaluate(records, history, 0.88, now_ms);
        assert!(evaluation.suspicious_session_ids.contains(&20), "{debug}");
        assert!(!evaluation.evicted_session_ids.contains(&20), "{debug}");
        assert_eq!(evaluation.evicted_session_ids.len(), 2, "{debug}");
    }
}
