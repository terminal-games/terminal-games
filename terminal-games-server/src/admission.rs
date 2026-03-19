// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use smallvec::SmallVec;
use tokio::sync::watch;

use crate::cluster_detection::{self, ClusterEvaluationJob, HistoricalSignature, NetworkPrefix};
use crate::metrics::{ServerMetrics, SessionGuard, Transport};
use terminal_games::app::{SessionControl, SessionEndReason};

pub(crate) const INPUT_WINDOW_MS: u64 = 8 * 60_000;
pub(crate) const MAX_INPUT_SAMPLES: usize = 96;
pub(crate) const MAX_OUTPUT_SAMPLES: usize = 64;
pub(crate) const MAX_SAMPLE_BYTES: usize = 24;
pub(crate) const OUTPUT_FLUSH_MS: u64 = 250;
pub(crate) const OUTPUT_FLUSH_BYTES: usize = 4096;
pub(crate) const CLUSTER_REEVALUATION_INTERVAL_MS: u64 = 250;
pub(crate) const IDLE_TIMEOUT_MS: u64 = 60_000;
pub(crate) const SHORT_WINDOW_MS: u64 = 60_000;
pub(crate) const MEDIUM_WINDOW_MS: u64 = 3 * 60_000;
pub(crate) const MIN_INPUTS_FOR_FINGERPRINT: usize = 3;
pub(crate) const MIN_CLUSTER_SIZE: usize = 2;
pub(crate) const LOW_PRESSURE_FLOOR: f64 = 0.35;
pub(crate) const SIGNATURE_RETENTION_MS: u64 = 12 * 60 * 60 * 1000;
pub(crate) const MAX_RECENT_SIGNATURES: usize = 16_384;
pub(crate) const GAP_BUCKETS: usize = 11;
pub(crate) const RESPONSE_BUCKETS: usize = 9;
pub(crate) const CLASS_BUCKETS: usize = 6;
pub(crate) const BURST_BUCKET_MS: u64 = 5_000;
pub(crate) const MOUSE_MOTION_COALESCE_MS: u64 = 40;

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
pub(crate) struct LiveSessionRecord {
    pub(crate) id: u64,
    pub(crate) client_ip: IpAddr,
    pub(crate) ip_prefix: NetworkPrefix,
    pub(crate) transport: Transport,
    pub(crate) started_at_ms: u64,
    pub(crate) inputs: VecDeque<InputSample>,
    pub(crate) outputs: VecDeque<OutputSample>,
    pub(crate) control: SessionControl,
    pub(crate) control_tx: watch::Sender<SessionControl>,
}

#[derive(Clone)]
pub(crate) struct InputSample {
    pub(crate) at_ms: u64,
    // We intentionally retain only a short owned prefix of each input event.
    // Using Bytes here would keep the full upstream allocation alive even though
    // the classifier only consumes a tiny bounded sample.
    pub(crate) bytes: SmallVec<[u8; MAX_SAMPLE_BYTES]>,
}

#[derive(Clone, Copy)]
pub(crate) struct OutputSample {
    pub(crate) at_ms: u64,
    pub(crate) bytes: u32,
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
            let guard = lock(&controller.inner.banned_ips);
            guard.len()
        };
        controller
            .inner
            .metrics
            .record_ip_ban_update(0, 0, 0, active_bans);
        controller
    }

    pub fn issue_ticket(&self, transport: Transport, client_ip: IpAddr) -> AdmissionTicket {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = watch::channel(AdmissionState::Queued(1));
        let is_banned = self.is_ip_banned(client_ip);

        let job = {
            let mut state = lock(&self.inner.state);
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
        let state = lock(&self.inner.state);
        cluster_detection::compute_pressure(state.running, self.inner.config.max_running)
            >= threshold
    }

    pub fn subscribe_ban_changes(&self) -> watch::Receiver<()> {
        self.inner.ban_changes.subscribe()
    }

    pub fn is_ip_banned(&self, client_ip: IpAddr) -> bool {
        let mut banned_ips = lock(&self.inner.banned_ips);
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
            let mut current = lock(&self.inner.banned_ips);
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
        let mut state = lock(&self.inner.state);
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
            let mut state = lock(&self.controller.state);
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
            let mut state = lock(&self.controller.state);
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
            let mut state = lock(&self.controller.state);
            let Some(record) = state.live_sessions.get_mut(&self.id) else {
                return;
            };
            record.inputs.push_back(sample);
            while record.inputs.len() > MAX_INPUT_SAMPLES {
                record.inputs.pop_front();
            }
            cluster_detection::trim_old_inputs(&mut record.inputs, now_ms);
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
            let mut state = lock(&self.controller.state);
            let Some(record) = state.live_sessions.get_mut(&self.id) else {
                return;
            };
            record.outputs.push_back(OutputSample { at_ms, bytes });
            while record.outputs.len() > MAX_OUTPUT_SAMPLES {
                record.outputs.pop_front();
            }
            cluster_detection::trim_old_outputs(&mut record.outputs, now_ms);
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
            let mut state = lock(&self.controller.state);
            if let Some(record) = state.live_sessions.remove(&self.id) {
                cluster_detection::archive_signatures(
                    &record,
                    now_ms,
                    &mut state.recent_signatures,
                );
            }
            cluster_detection::trim_recent_signatures(&mut state.recent_signatures, now_ms);
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

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn maybe_run_cluster_reevaluation(
    inner: &Arc<Inner>,
    state: &mut ControllerState,
    now_ms: u64,
    force: bool,
) -> Option<ClusterEvaluationJob> {
    cluster_detection::trim_recent_signatures(&mut state.recent_signatures, now_ms);
    let pressure = cluster_detection::compute_pressure(state.running, inner.config.max_running);
    if pressure < LOW_PRESSURE_FLOOR || state.live_sessions.len() < MIN_CLUSTER_SIZE {
        state.cluster_dirty = false;
        state.next_cluster_eval_at_ms = now_ms.saturating_add(CLUSTER_REEVALUATION_INTERVAL_MS);
        cluster_detection::clear_cluster_controls(&mut state.live_sessions);
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
    let evaluation = cluster_detection::evaluate_job(&job);
    let mut state = lock(&inner.state);
    state.cluster_eval_in_progress = false;
    if state.cluster_revision != job.revision {
        state.cluster_dirty = true;
        return;
    }
    cluster_detection::apply_cluster_evaluation(
        &inner.metrics,
        &mut state.live_sessions,
        evaluation,
        job.pressure,
    );
}
