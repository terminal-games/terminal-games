// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use ipnet::IpNet;
use iptrie::IpRTrieMap;
use smallvec::SmallVec;
use tokio::sync::{mpsc, watch};

use crate::cluster_detection::{
    self, ClusterEvaluation, ClusterEvaluationJob, HistoricalSignature, NetworkPrefix,
};
use crate::cluster_kicked_ips;
use crate::metrics::{ServerMetrics, SessionHandle, Transport};
use crate::notifications::{
    CapacityThresholdNotification, ClusterEnforcementNotification, ClusterEnforcementSession,
    Notifications,
};
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
    ban_manager: Mutex<BanManager>,
    ban_changes: watch::Sender<()>,
    metrics: Arc<ServerMetrics>,
    notifications: Arc<Notifications>,
    next_id: AtomicU64,
    state: Mutex<ControllerState>,
    capacity_threshold_reached: AtomicBool,
    cluster_tx: mpsc::UnboundedSender<ClusterEvent>,
}

#[derive(Default)]
struct ControllerState {
    running: Vec<(u64, IpAddr)>,
    queue: VecDeque<QueuedTicket>,
}

impl ControllerState {
    fn enqueue(
        &mut self,
        id: u64,
        client_ip: IpAddr,
        transport: Transport,
        tx: watch::Sender<AdmissionState>,
    ) {
        let _ = tx.send(AdmissionState::Queued(self.queue.len() + 1));
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
        self.queue.remove(idx)
    }

    fn remove_queued(&mut self, id: u64) -> Option<Transport> {
        let idx = self.queue.iter().position(|queued| queued.id == id)?;
        self.queue.remove(idx).map(|ticket| ticket.transport)
    }

    fn refresh_queue_positions(&mut self) {
        for (idx, queued) in self.queue.iter_mut().enumerate() {
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
        self.running
            .iter()
            .filter(|(_, ticket_ip)| *ticket_ip == ip)
            .count()
    }

    fn queued_for_ip(&self, ip: IpAddr) -> usize {
        self.queue
            .iter()
            .filter(|ticket| ticket.client_ip == ip)
            .count()
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
    control_rx: watch::Receiver<SessionControl>,
    pending_output_bytes: usize,
    pending_output_started_at_ms: Option<u64>,
    session_guard: Option<SessionHandle>,
}

#[derive(Debug, Clone, Copy)]
pub struct BanUpdateSummary {
    pub activated: usize,
    pub deactivated: usize,
    pub evicted_from_queue: usize,
    pub active_ban_count: usize,
}

pub(crate) fn parse_ban_cidr(raw: impl AsRef<str>) -> Result<IpNet, String> {
    let value = raw.as_ref().trim();
    if value.is_empty() {
        return Err("ban rule is empty".to_string());
    }
    if let Ok(network) = value.parse::<IpNet>() {
        return Ok(network);
    }
    if let Ok(ip) = value.parse::<IpAddr>() {
        return Ok(IpNet::from(ip));
    }
    Err("ban rule must be a valid IPv4/IPv6 address or CIDR range".to_string())
}

pub(crate) struct BanManager {
    trie: IpRTrieMap<Option<(Option<String>, Option<i64>)>>,
}

impl BanManager {
    fn new(initial_bans: Vec<(IpNet, Option<String>, Option<i64>)>) -> Self {
        let mut manager = Self {
            trie: IpRTrieMap::new(),
        };
        let now = current_unix_seconds();
        for (cidr, reason, expires_at) in initial_bans {
            if is_ban_active(expires_at, now) {
                manager.insert(cidr, reason, expires_at);
            }
        }
        manager
    }

    fn active_len(&self) -> usize {
        let now = current_unix_seconds();
        self.trie
            .iter()
            .filter_map(|(_cidr, entry)| entry.as_ref())
            .filter(|(_reason, expires_at)| is_ban_active(*expires_at, now))
            .count()
    }

    fn check(&mut self, client_ip: IpAddr) -> Option<(IpNet, Option<String>)> {
        let now = current_unix_seconds();
        let needle = IpNet::from(client_ip);
        loop {
            let (matched_prefix, entry) = self.trie.lookup(&needle);
            let Some((reason, expires_at)) = entry.clone() else {
                return None;
            };
            if is_ban_active(expires_at, now) && matched_prefix.contains(&client_ip) {
                return Some((matched_prefix, reason));
            }
            self.clear(matched_prefix);
        }
    }

    fn insert(&mut self, cidr: IpNet, reason: Option<String>, expires_at: Option<i64>) {
        self.set(cidr, Some((reason, expires_at)));
    }

    fn remove(&mut self, cidr: IpNet) -> bool {
        self.clear(cidr)
    }

    fn set(&mut self, cidr: IpNet, value: Option<(Option<String>, Option<i64>)>) {
        if cidr.prefix_len() == 0 {
            *self
                .trie
                .get_mut(&cidr)
                .expect("ip trie root must always exist") = value;
        } else {
            self.trie.insert(cidr, value);
        }
    }

    fn clear(&mut self, cidr: IpNet) -> bool {
        if cidr.prefix_len() == 0 {
            let slot = self
                .trie
                .get_mut(&cidr)
                .expect("ip trie root must always exist");
            let removed = slot.is_some();
            *slot = None;
            removed
        } else {
            self.trie.remove(&cidr).flatten().is_some()
        }
    }

    fn is_active(&self, cidr: IpNet, now: i64) -> bool {
        self.trie
            .get(&cidr)
            .and_then(|entry| entry.as_ref())
            .is_some_and(|(_reason, expires_at)| is_ban_active(*expires_at, now))
    }
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

pub(crate) fn encode_resize_input(cols: u16, rows: u16) -> SmallVec<[u8; MAX_SAMPLE_BYTES]> {
    let mut bytes = SmallVec::<[u8; MAX_SAMPLE_BYTES]>::new();
    bytes.extend_from_slice(format!("\x1b[8;{};{}t", rows, cols).as_bytes());
    bytes
}

enum ClusterEvent {
    SessionStarted {
        session: Arc<LiveSessionRecord>,
        control_tx: watch::Sender<SessionControl>,
    },
    InputRecorded {
        session_id: u64,
        sample: InputSample,
    },
    OutputRecorded {
        session_id: u64,
        sample: OutputSample,
    },
    SessionEnded(u64),
}

type ClusterSession = (Arc<LiveSessionRecord>, watch::Sender<SessionControl>);

struct ClusterManager {
    max_running: usize,
    metrics: Arc<ServerMetrics>,
    db: libsql::Connection,
    notifications: Arc<Notifications>,
    rx: mpsc::UnboundedReceiver<ClusterEvent>,
    live_sessions: HashMap<u64, ClusterSession>,
    recent_signatures: Arc<[HistoricalSignature]>,
    dirty: bool,
}

impl AdmissionController {
    pub fn new(
        config: AdmissionConfig,
        initial_banned_ips: Vec<(IpNet, Option<String>, Option<i64>)>,
        metrics: Arc<ServerMetrics>,
        db: libsql::Connection,
        notifications: Arc<Notifications>,
    ) -> Self {
        let (ban_changes, _) = watch::channel(());
        let cluster_tx = spawn_cluster_manager(
            config.max_running,
            metrics.clone(),
            db,
            notifications.clone(),
        );
        let controller = Self {
            inner: Arc::new(Inner {
                config,
                ban_manager: Mutex::new(BanManager::new(initial_banned_ips)),
                ban_changes,
                metrics,
                notifications,
                next_id: AtomicU64::new(1),
                state: Mutex::new(ControllerState::default()),
                capacity_threshold_reached: AtomicBool::new(false),
                cluster_tx,
            }),
        };
        let active_bans = {
            let guard = controller.inner.ban_manager.lock().unwrap();
            guard.active_len()
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
        let matched_ban = self.check_ip_ban(client_ip);

        {
            let mut state = self.inner.state.lock().unwrap();
            if let Some((ban_rule, ban_reason)) = matched_ban {
                tracing::debug!(
                    client_ip = %client_ip,
                    transport = transport.as_str(),
                    ban_rule = %ban_rule,
                    ban_reason = ban_reason.as_deref().unwrap_or("<none>"),
                    running = state.running.len(),
                    running_for_ip = state.running_for_ip(client_ip),
                    queued_for_ip = state.queued_for_ip(client_ip),
                    "Rejected client from active IP ban"
                );
                let _ = tx.send(AdmissionState::Rejected(SessionEndReason::BannedIp));
            } else if state.running.len() < self.inner.config.max_running
                && state.running_for_ip(client_ip) < self.inner.config.max_running_per_ip
            {
                state.running.push((id, client_ip));
                let _ = tx.send(AdmissionState::Allowed);
            } else if state.queued_for_ip(client_ip) < self.inner.config.max_queued_per_ip {
                state.enqueue(id, client_ip, transport, tx);
            } else {
                tracing::debug!(
                    client_ip = %client_ip,
                    transport = transport.as_str(),
                    running = state.running.len(),
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
            self.update_capacity_threshold_alert(state.running.len());
        }

        AdmissionTicket {
            id,
            client_ip,
            transport,
            rx,
            controller: self.inner.clone(),
            control_rx: watch::channel(SessionControl::Active).1,
            pending_output_bytes: 0,
            pending_output_started_at_ms: None,
            session_guard: None,
        }
    }

    pub fn should_require_captcha(&self) -> bool {
        let Some(threshold) = self.inner.config.ssh_captcha_threshold else {
            return false;
        };
        let state = self.inner.state.lock().unwrap();
        cluster_detection::compute_pressure(state.running.len(), self.inner.config.max_running)
            >= threshold
    }

    pub fn subscribe_ban_changes(&self) -> watch::Receiver<()> {
        self.inner.ban_changes.subscribe()
    }

    pub fn check_ip_ban(&self, client_ip: IpAddr) -> Option<(IpNet, Option<String>)> {
        let mut ban_manager = self.inner.ban_manager.lock().unwrap();
        ban_manager.check(client_ip)
    }

    pub fn apply_ban_updates(
        &self,
        updates: Vec<(IpNet, Option<String>, Option<i64>)>,
    ) -> BanUpdateSummary {
        let now = current_unix_seconds();
        let mut newly_banned_rules = Vec::new();
        let mut deactivated_rules = Vec::new();
        let active_ban_count;
        {
            let mut current = self.inner.ban_manager.lock().unwrap();
            for (cidr, reason, expires_at) in updates {
                if is_ban_active(expires_at, now) {
                    let was_active = current.is_active(cidr, now);
                    current.insert(cidr, reason.clone(), expires_at);
                    if !was_active {
                        newly_banned_rules.push((cidr, reason, expires_at));
                    }
                } else if current.remove(cidr) {
                    deactivated_rules.push(cidr);
                }
            }
            active_ban_count = current.active_len();
        }

        let activated = newly_banned_rules.len();
        let deactivated = deactivated_rules.len();

        if newly_banned_rules.is_empty() {
            let summary = BanUpdateSummary {
                activated,
                deactivated,
                evicted_from_queue: 0,
                active_ban_count,
            };
            for cidr in deactivated_rules {
                tracing::debug!(
                    ban_rule = %cidr,
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
        let mut state = self.inner.state.lock().unwrap();
        let mut idx = 0usize;
        let mut removed_any = false;
        let mut evicted_from_queue = 0usize;
        while idx < state.queue.len() {
            if newly_banned_rules
                .iter()
                .any(|(cidr, _, _)| cidr.contains(&state.queue[idx].client_ip))
            {
                let ticket = state.queue.remove(idx).expect("index checked against len");
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
            state.refresh_queue_positions();
            state.record_metrics(&self.inner.metrics);
        }
        let summary = BanUpdateSummary {
            activated,
            deactivated,
            evicted_from_queue,
            active_ban_count,
        };
        for (cidr, reason, expires_at) in newly_banned_rules {
            tracing::debug!(
                ban_rule = %cidr,
                ban_reason = reason.as_deref().unwrap_or("<none>"),
                expires_at,
                evicted_from_queue,
                active_ban_count = summary.active_ban_count,
                "Activated IP ban"
            );
        }
        for cidr in deactivated_rules {
            tracing::debug!(
                ban_rule = %cidr,
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
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn subscribe(&self) -> watch::Receiver<AdmissionState> {
        self.rx.clone()
    }

    pub fn start_session(mut self, session_guard: SessionHandle) -> Self {
        let now_ms = monotonic_millis();
        let (control_tx, control_rx) = watch::channel(SessionControl::Active);
        let _ = self
            .controller
            .cluster_tx
            .send(ClusterEvent::SessionStarted {
                session: Arc::new(LiveSessionRecord {
                    id: self.id,
                    client_ip: self.client_ip,
                    ip_prefix: NetworkPrefix::from_ip(self.client_ip),
                    transport: self.transport,
                    started_at_ms: now_ms,
                    inputs: VecDeque::new(),
                    outputs: VecDeque::new(),
                }),
                control_tx,
            });
        self.control_rx = control_rx;
        self.pending_output_bytes = 0;
        self.pending_output_started_at_ms = None;
        self.session_guard = Some(session_guard);
        self
    }

    pub fn subscribe_control(&self) -> watch::Receiver<SessionControl> {
        self.control_rx.clone()
    }

    pub fn record_input(&mut self, bytes: &[u8]) {
        self.flush_output_batch();
        self.record_input_sample(
            bytes
                .iter()
                .copied()
                .take(MAX_SAMPLE_BYTES)
                .collect::<SmallVec<[u8; MAX_SAMPLE_BYTES]>>(),
        );
    }

    pub fn record_resize(&mut self, cols: u16, rows: u16) {
        if cols == 0 || rows == 0 {
            return;
        }
        self.flush_output_batch();
        self.record_input_sample(encode_resize_input(cols, rows));
    }

    pub fn record_output(&mut self, bytes: usize) {
        let now_ms = monotonic_millis();
        self.pending_output_bytes = self.pending_output_bytes.saturating_add(bytes);
        let started_at = self.pending_output_started_at_ms.get_or_insert(now_ms);
        if now_ms.saturating_sub(*started_at) >= OUTPUT_FLUSH_MS
            || self.pending_output_bytes >= OUTPUT_FLUSH_BYTES
        {
            self.flush_output_batch();
        }
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
        let _ = self
            .controller
            .cluster_tx
            .send(ClusterEvent::OutputRecorded {
                session_id: self.id,
                sample: OutputSample { at_ms, bytes },
            });
    }

    fn record_input_sample(&mut self, bytes: SmallVec<[u8; MAX_SAMPLE_BYTES]>) {
        let sample = InputSample {
            at_ms: monotonic_millis(),
            bytes,
        };
        let _ = self
            .controller
            .cluster_tx
            .send(ClusterEvent::InputRecorded {
                session_id: self.id,
                sample,
            });
    }
}

impl Drop for AdmissionTicket {
    fn drop(&mut self) {
        if self.session_guard.is_some() {
            self.flush_output_batch();
            let _ = self
                .controller
                .cluster_tx
                .send(ClusterEvent::SessionEnded(self.id));
        }
        let mut state = self.controller.state.lock().unwrap();
        match *self.rx.borrow() {
            AdmissionState::Allowed => {
                if let Some(idx) = state.running.iter().position(|(id, _)| *id == self.id) {
                    state.running.swap_remove(idx);
                }
                if let Some(next) =
                    state.dequeue_admissible(self.controller.config.max_running_per_ip)
                {
                    state.running.push((next.id, next.client_ip));
                    let _ = next.tx.send(AdmissionState::Allowed);
                    self.controller
                        .metrics
                        .record_admission_joined_from_queue(next.transport);
                    state.refresh_queue_positions();
                }
            }
            AdmissionState::Queued(_) => {
                if let Some(transport) = state.remove_queued(self.id) {
                    self.controller
                        .metrics
                        .record_admission_abandoned_queue(transport);
                    state.refresh_queue_positions();
                }
            }
            AdmissionState::Rejected(_reason) => {}
        }
        state.record_metrics(&self.controller.metrics);
        AdmissionController {
            inner: self.controller.clone(),
        }
        .update_capacity_threshold_alert(state.running.len());
    }
}

fn spawn_cluster_manager(
    max_running: usize,
    metrics: Arc<ServerMetrics>,
    db: libsql::Connection,
    notifications: Arc<Notifications>,
) -> mpsc::UnboundedSender<ClusterEvent> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(
        ClusterManager {
            max_running,
            metrics,
            db,
            notifications,
            rx,
            live_sessions: HashMap::new(),
            recent_signatures: Arc::from([]),
            dirty: false,
        }
        .run(),
    );
    tx
}

impl ClusterManager {
    async fn run(mut self) {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(
            CLUSTER_REEVALUATION_INTERVAL_MS,
        ));
        loop {
            tokio::select! {
                Some(event) = self.rx.recv() => self.handle(event).await,
                _ = tick.tick() => {
                    self.maybe_evaluate().await;
                }
                else => break,
            }
        }
    }

    async fn handle(&mut self, event: ClusterEvent) {
        match event {
            ClusterEvent::SessionStarted {
                session,
                control_tx,
            } => {
                self.live_sessions.insert(session.id, (session, control_tx));
                self.dirty = true;
                self.maybe_evaluate().await;
            }
            ClusterEvent::InputRecorded { session_id, sample } => {
                if self.update_session(session_id, |snapshot| {
                    snapshot.inputs.push_back(sample);
                    while snapshot.inputs.len() > MAX_INPUT_SAMPLES {
                        snapshot.inputs.pop_front();
                    }
                }) {
                    self.dirty = true;
                }
            }
            ClusterEvent::OutputRecorded { session_id, sample } => {
                if self.update_session(session_id, |snapshot| {
                    snapshot.outputs.push_back(sample);
                    while snapshot.outputs.len() > MAX_OUTPUT_SAMPLES {
                        snapshot.outputs.pop_front();
                    }
                }) {
                    self.dirty = true;
                }
            }
            ClusterEvent::SessionEnded(session_id) => {
                let now_ms = monotonic_millis();
                let Some((session, _)) = self.live_sessions.remove(&session_id) else {
                    return;
                };
                let mut recent_signatures = self
                    .recent_signatures
                    .iter()
                    .cloned()
                    .collect::<VecDeque<_>>();
                cluster_detection::archive_signatures(
                    session.as_ref(),
                    now_ms,
                    &mut recent_signatures,
                );
                self.recent_signatures =
                    Arc::from(recent_signatures.into_iter().collect::<Vec<_>>());
                self.dirty = true;
                self.maybe_evaluate().await;
            }
        }
    }

    fn update_session(
        &mut self,
        session_id: u64,
        update: impl FnOnce(&mut LiveSessionRecord),
    ) -> bool {
        let Some((session, _)) = self.live_sessions.get_mut(&session_id) else {
            return false;
        };
        let mut snapshot = session.as_ref().clone();
        update(&mut snapshot);
        let now_ms = monotonic_millis();
        cluster_detection::trim_old_inputs(&mut snapshot.inputs, now_ms);
        cluster_detection::trim_old_outputs(&mut snapshot.outputs, now_ms);
        *session = Arc::new(snapshot);
        true
    }

    async fn maybe_evaluate(&mut self) {
        if !self.dirty {
            return;
        }
        if self.should_skip_evaluation() {
            self.dirty = false;
            self.clear_controls();
            return;
        }
        self.dirty = false;
        let job = ClusterEvaluationJob {
            pressure: self.pressure(),
            now_ms: monotonic_millis(),
            live_sessions: Arc::from(
                self.live_sessions
                    .values()
                    .map(|(session, _)| session.clone())
                    .collect::<Vec<_>>(),
            ),
            recent_signatures: self.recent_signatures.clone(),
        };
        let pressure = job.pressure;
        let evaluation = cluster_detection::evaluate_job(&job);
        self.apply_evaluation(evaluation, pressure).await;
    }

    fn should_skip_evaluation(&self) -> bool {
        self.pressure() < LOW_PRESSURE_FLOOR || self.live_sessions.len() < MIN_CLUSTER_SIZE
    }

    fn pressure(&self) -> f64 {
        cluster_detection::compute_pressure(self.live_sessions.len(), self.max_running)
    }

    fn clear_controls(&mut self) {
        for (_, control_tx) in self.live_sessions.values_mut() {
            let _ = update_session_control(control_tx, SessionControl::Active);
        }
    }

    async fn apply_evaluation(&mut self, evaluation: ClusterEvaluation, pressure: f64) {
        let mut newly_evicted = Vec::new();
        for (session, control_tx) in self.live_sessions.values_mut() {
            let next = if evaluation.evicted_session_ids.contains(&session.id) {
                SessionControl::Close(SessionEndReason::ClusterLimited)
            } else {
                SessionControl::Active
            };
            if update_session_control(control_tx, next) {
                if matches!(next, SessionControl::Close(_)) {
                    let summary = evaluation
                        .eviction_summaries
                        .get(&session.id)
                        .copied()
                        .expect("evicted session missing cluster summary");
                    tracing::warn!(
                        session_id = session.id,
                        client_ip = %session.client_ip,
                        transport = session.transport.as_str(),
                        pressure,
                        suspicious_cluster_count = evaluation.suspicious_cluster_count,
                        max_cluster_score = evaluation.max_cluster_score,
                        cluster_score = summary.score,
                        required_cluster_score = summary.required_score,
                        cluster_score_margin = summary.score_margin,
                        cluster_size = summary.cluster_size,
                        avg_edge = summary.factors.avg_edge,
                        avg_replay = summary.factors.avg_replay,
                        avg_replay_density = summary.factors.avg_replay_density,
                        avg_low_engagement = summary.factors.avg_low_engagement,
                        avg_coupling_deficit = summary.factors.avg_coupling_deficit,
                        network_cohesion = summary.factors.network_cohesion,
                        start_sync = summary.factors.start_sync,
                        size_factor = summary.factors.size_factor,
                        edge_low_engagement_interaction =
                            summary.factors.edge_low_engagement_interaction,
                        avg_edge_contribution = summary.contributions.avg_edge,
                        avg_replay_contribution = summary.contributions.avg_replay,
                        avg_replay_density_contribution =
                            summary.contributions.avg_replay_density,
                        avg_low_engagement_contribution =
                            summary.contributions.avg_low_engagement,
                        avg_coupling_deficit_contribution =
                            summary.contributions.avg_coupling_deficit,
                        network_cohesion_contribution =
                            summary.contributions.network_cohesion,
                        start_sync_contribution = summary.contributions.start_sync,
                        size_factor_contribution = summary.contributions.size_factor,
                        pressure_contribution = summary.contributions.pressure,
                        edge_low_engagement_interaction_contribution =
                            summary.contributions.edge_low_engagement_interaction,
                        "Evicting session due to suspected bot cluster"
                    );
                    self.metrics.record_cluster_enforcement(session.transport);
                    newly_evicted.push((session.id, session.transport, session.client_ip));
                }
            }
        }
        if newly_evicted.is_empty() {
            return;
        }

        let ip_counts = match cluster_kicked_ips::increment_for_enforcement(
            &self.db,
            newly_evicted.iter().map(|(_, _, client_ip)| *client_ip),
        )
        .await
        {
            Ok(counts) => counts,
            Err(error) => {
                tracing::error!(error = ?error, "failed to persist cluster-kicked ip counts");
                Vec::new()
            }
        };
        self.notifications
            .notify_cluster_enforcement(ClusterEnforcementNotification {
                region_id: self.metrics.region().to_string(),
                current_sessions: self.live_sessions.len(),
                max_capacity: self.max_running,
                suspicious_cluster_count: evaluation.suspicious_cluster_count,
                max_cluster_score: evaluation.max_cluster_score,
                sessions: newly_evicted
                    .into_iter()
                    .map(
                        |(session_id, transport, client_ip)| ClusterEnforcementSession {
                            session_id: format!("{}:{session_id}", self.metrics.region()),
                            transport: transport.as_str().to_string(),
                            client_ip: client_ip.to_string(),
                        },
                    )
                    .collect(),
                ip_counts,
            });
    }
}

impl AdmissionController {
    fn update_capacity_threshold_alert(&self, running_sessions: usize) {
        if self.inner.config.max_running == 0 || self.inner.config.max_running == usize::MAX {
            return;
        }
        let threshold_reached =
            running_sessions.saturating_mul(5) >= self.inner.config.max_running.saturating_mul(4);
        if threshold_reached {
            if self
                .inner
                .capacity_threshold_reached
                .swap(true, Ordering::AcqRel)
            {
                return;
            }
            self.inner
                .notifications
                .notify_capacity_threshold(CapacityThresholdNotification {
                    region_id: self.inner.metrics.region().to_string(),
                    current_sessions: running_sessions,
                    max_capacity: self.inner.config.max_running,
                    threshold_percent: 80,
                });
            return;
        }
        self.inner
            .capacity_threshold_reached
            .store(false, Ordering::Release);
    }
}

fn update_session_control(
    control_tx: &watch::Sender<SessionControl>,
    next: SessionControl,
) -> bool {
    control_tx.send_if_modified(|current| {
        if matches!(
            (*current, next),
            (SessionControl::Close(_), SessionControl::Active)
        ) || *current == next
        {
            false
        } else {
            *current = next;
            true
        }
    })
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

pub(crate) fn encode_cidr_blob(matcher: IpNet) -> Vec<u8> {
    // Compact standalone prefix encoding:
    // - IPv4 headers are 0..=32 and store the prefix length directly
    // - IPv6 headers are 64..=192 and store prefix length as 64 + len
    // - payload stores only the bytes covered by the prefix
    match matcher {
        IpNet::V4(network) => {
            let prefix_len = network.prefix_len();
            let payload_len = prefix_payload_len(prefix_len);
            let mut out = Vec::with_capacity(1 + payload_len);
            out.push(prefix_len);
            out.extend_from_slice(&network.network().octets()[..payload_len]);
            out
        }
        IpNet::V6(network) => {
            let prefix_len = network.prefix_len();
            let payload_len = prefix_payload_len(prefix_len);
            let mut out = Vec::with_capacity(1 + payload_len);
            out.push(64 + prefix_len);
            out.extend_from_slice(&network.network().octets()[..payload_len]);
            out
        }
    }
}

pub(crate) fn decode_cidr_blob(raw: &[u8]) -> Result<IpNet, String> {
    if raw.is_empty() {
        return Err("cidr blob too short".to_string());
    }
    match raw[0] {
        0..=32 => {
            let prefix_len = raw[0];
            let expected_len = 1 + prefix_payload_len(prefix_len);
            if raw.len() != expected_len {
                return Err("invalid ipv4 cidr blob length".to_string());
            }
            let mut octets = [0u8; 4];
            octets[..expected_len - 1].copy_from_slice(&raw[1..]);
            let network = ipnet::Ipv4Net::new(std::net::Ipv4Addr::from(octets), prefix_len)
                .map_err(|error| error.to_string())?;
            Ok(IpNet::V4(network))
        }
        // 33..=63 is intentionally unused to keep family decoding simple and leave
        // room for future format variants.
        64..=192 => {
            let prefix_len = raw[0] - 64;
            let expected_len = 1 + prefix_payload_len(prefix_len);
            if raw.len() != expected_len {
                return Err("invalid ipv6 cidr blob length".to_string());
            }
            let mut octets = [0u8; 16];
            octets[..expected_len - 1].copy_from_slice(&raw[1..]);
            let network = ipnet::Ipv6Net::new(std::net::Ipv6Addr::from(octets), prefix_len)
                .map_err(|error| error.to_string())?;
            Ok(IpNet::V6(network))
        }
        other => Err(format!("unsupported cidr blob header byte {other}")),
    }
}

fn prefix_payload_len(prefix_len: u8) -> usize {
    usize::from(prefix_len).div_ceil(8)
}

fn monotonic_millis() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
