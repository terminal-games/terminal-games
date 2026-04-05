// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::{HashMap, VecDeque},
    net::IpAddr,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::StreamExt;
use terminal_games::{
    app::{
        SessionAppState, SessionControl, SessionEndReason, SessionIdentity, SessionIo,
        SessionOutput, SessionUi,
    },
    control::{SessionSummary, StatusBarDrain, StatusBarState},
    replay::{ReplayTerminalSnapshot, ReplayTerminalSnapshotCapture},
};
use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::{broadcast, mpsc, oneshot, watch};

use crate::idle::INITIAL_FUEL_SECS;
use crate::metrics::Transport;
use crate::notifications::{LongSessionNotification, Notifications};

const LONG_SESSION_WEBHOOK_AFTER: Duration = Duration::from_secs(30 * 60);

#[derive(Clone, Debug)]
pub enum SpyEvent {
    Output { data: Bytes, sequence: u64 },
    Input { data: Bytes },
    Closed { reason: SessionEndReason },
}

pub struct SpySession {
    session: Arc<RuntimeSession>,
    pub snapshot: ReplayTerminalSnapshot,
    pub pending_events: VecDeque<SpyEvent>,
    pub event_rx: broadcast::Receiver<SpyEvent>,
    pub input_tx: Option<mpsc::Sender<Bytes>>,
    pub username_rx: watch::Receiver<String>,
    pub app_rx: watch::Receiver<SessionAppState>,
    pub idle_rx: watch::Receiver<SessionIdleState>,
    pub size_rx: watch::Receiver<(u16, u16)>,
}

impl Drop for SpySession {
    fn drop(&mut self) {
        self.session.active_spies.fetch_sub(1, Ordering::AcqRel);
    }
}

impl SpySession {
    pub fn set_read_write(&mut self, read_write: bool) {
        self.input_tx = read_write.then_some(self.session.input_tx.clone());
    }
}

pub struct SessionRegistration {
    pub identity: SessionIdentity,
    pub session_ui: SessionUi,
    pub control_rx: watch::Receiver<SessionControl>,
    pub idle_rx: watch::Receiver<SessionIdleState>,
    pub app_input_sender: mpsc::Sender<Bytes>,
    pub app_input_receiver: mpsc::Receiver<Bytes>,
    pub spy_snapshot_requests: mpsc::Receiver<oneshot::Sender<ReplayTerminalSnapshotCapture>>,
    pub session_io: Arc<SessionIo>,
    pub cleanup_guard: SessionCleanupGuard,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionIdleState {
    pub fuel_seconds: i32,
    pub paused: bool,
}

impl Default for SessionIdleState {
    fn default() -> Self {
        Self {
            fuel_seconds: INITIAL_FUEL_SECS,
            paused: false,
        }
    }
}

struct RuntimeSession {
    node_id: String,
    local_session_id: u64,
    user_id: Option<u64>,
    client_ip: IpAddr,
    transport: Transport,
    started_at: Instant,
    identity: SessionIdentity,
    size_rx: watch::Receiver<(u16, u16)>,
    idle_tx: watch::Sender<SessionIdleState>,
    input_tx: mpsc::Sender<Bytes>,
    io: Arc<SessionIo>,
    control_tx: watch::Sender<SessionControl>,
    spy_tx: broadcast::Sender<SpyEvent>,
    snapshot_tx: mpsc::Sender<oneshot::Sender<ReplayTerminalSnapshotCapture>>,
    active_spies: AtomicUsize,
    finished: AtomicBool,
}

pub struct SessionCleanupGuard {
    registry: Arc<SessionRegistry>,
    local_session_id: u64,
}

impl Drop for SessionCleanupGuard {
    fn drop(&mut self) {
        self.registry.remove(self.local_session_id);
    }
}

pub struct SessionRegistry {
    node_id: String,
    long_session_notifier: Option<mpsc::UnboundedSender<Weak<RuntimeSession>>>,
    sessions: Mutex<HashMap<u64, Arc<RuntimeSession>>>,
    count_tx: watch::Sender<usize>,
    status_bar_state: Mutex<SharedStatusBarState>,
    status_bar_state_tx: watch::Sender<StatusBarState>,
}

#[derive(Default)]
struct SharedStatusBarState {
    base: StatusBarState,
    drain: Option<StatusBarDrain>,
}

impl SessionRegistry {
    pub fn new(node_id: String, notifications: Arc<Notifications>) -> Arc<Self> {
        let (count_tx, _) = watch::channel(0usize);
        let (status_bar_state_tx, _) = watch::channel(StatusBarState::default());
        let long_session_notifier = notifications
            .enabled()
            .then(|| spawn_long_session_notifier(notifications));
        Arc::new(Self {
            node_id,
            long_session_notifier,
            sessions: Mutex::new(HashMap::new()),
            count_tx,
            status_bar_state: Mutex::new(SharedStatusBarState::default()),
            status_bar_state_tx,
        })
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    #[allow(clippy::too_many_arguments)]
    pub fn register(
        self: &Arc<Self>,
        local_session_id: u64,
        user_id: Option<u64>,
        username: String,
        client_ip: IpAddr,
        transport: Transport,
        initial_shortname: String,
        size_rx: watch::Receiver<(u16, u16)>,
    ) -> SessionRegistration {
        let identity = SessionIdentity::new(username, initial_shortname);
        let session_ui = SessionUi::new(self.status_bar_state_tx.subscribe());
        let (input_tx, input_rx) = mpsc::channel(12);
        let io = Arc::new(SessionIo::default());
        let (control_tx, control_rx) = watch::channel(SessionControl::Active);
        let (idle_tx, idle_rx) = watch::channel(SessionIdleState::default());
        let (snapshot_tx, snapshot_rx) = mpsc::channel(8);
        let (spy_tx, _) = broadcast::channel(64);
        let session = Arc::new(RuntimeSession {
            node_id: self.node_id.clone(),
            local_session_id,
            user_id,
            client_ip,
            transport,
            started_at: Instant::now(),
            identity: identity.clone(),
            size_rx,
            idle_tx: idle_tx.clone(),
            input_tx: input_tx.clone(),
            io: io.clone(),
            control_tx: control_tx.clone(),
            spy_tx,
            snapshot_tx,
            active_spies: AtomicUsize::new(0),
            finished: AtomicBool::new(false),
        });
        let count = {
            let mut sessions = self.sessions.lock().unwrap();
            sessions.insert(local_session_id, session.clone());
            sessions.len()
        };
        self.count_tx.send_replace(count);
        if let Some(notifier) = &self.long_session_notifier {
            let _ = notifier.send(Arc::downgrade(&session));
        }
        SessionRegistration {
            identity,
            session_ui,
            control_rx,
            idle_rx,
            app_input_sender: input_tx,
            app_input_receiver: input_rx,
            spy_snapshot_requests: snapshot_rx,
            session_io: io,
            cleanup_guard: SessionCleanupGuard {
                registry: self.clone(),
                local_session_id,
            },
        }
    }

    pub fn set_status_bar_state(&self, state: StatusBarState) {
        let combined = {
            let mut shared = self.status_bar_state.lock().unwrap();
            shared.base = state;
            compose_status_bar_state(&shared)
        };
        self.status_bar_state_tx.send_replace(combined);
    }

    pub fn set_status_bar_drain(&self, drain: Option<StatusBarDrain>) {
        let combined = {
            let mut shared = self.status_bar_state.lock().unwrap();
            shared.drain = drain;
            compose_status_bar_state(&shared)
        };
        self.status_bar_state_tx.send_replace(combined);
    }

    pub fn remove(&self, local_session_id: u64) {
        let count = {
            let mut sessions = self.sessions.lock().unwrap();
            sessions.remove(&local_session_id);
            sessions.len()
        };
        self.count_tx.send_replace(count);
    }

    pub fn count(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }

    pub fn summaries(&self) -> Vec<SessionSummary> {
        self.sessions
            .lock()
            .unwrap()
            .values()
            .map(|session| SessionSummary {
                session_id: format!("{}:{}", session.node_id, session.local_session_id),
                local_session_id: session.local_session_id,
                user_id: session.user_id,
                node_id: session.node_id.clone(),
                transport: session.transport.as_str().to_string(),
                shortname: session.identity.app().shortname,
                duration_seconds: session.started_at.elapsed().as_secs(),
                username: session.identity.username(),
                ip_address: session.client_ip.to_string(),
            })
            .collect()
    }

    pub fn kick(&self, local_session_id: u64) -> bool {
        self.request_close(local_session_id, SessionEndReason::KickedByAdmin)
    }

    pub fn request_close_all(&self, reason: SessionEndReason) -> usize {
        let local_session_ids = self
            .sessions
            .lock()
            .unwrap()
            .keys()
            .copied()
            .collect::<Vec<_>>();
        local_session_ids
            .into_iter()
            .filter(|local_session_id| self.request_close(*local_session_id, reason))
            .count()
    }

    pub fn request_close(&self, local_session_id: u64, reason: SessionEndReason) -> bool {
        let Some(session) = self.lookup(local_session_id) else {
            return false;
        };
        session.io.close();
        session
            .control_tx
            .send_if_modified(|current| match (*current, reason) {
                (SessionControl::Close(_), _) => false,
                _ => {
                    *current = SessionControl::Close(reason);
                    true
                }
            })
    }

    pub fn finish(&self, local_session_id: u64, reason: SessionEndReason) -> bool {
        let Some(session) = self.lookup(local_session_id) else {
            return false;
        };
        if session.finished.swap(true, Ordering::AcqRel) {
            return false;
        }
        let _ = session.spy_tx.send(SpyEvent::Closed { reason });
        true
    }

    pub async fn wait_for_zero(&self) {
        let mut count_rx = self.count_tx.subscribe();
        loop {
            if *count_rx.borrow_and_update() == 0 {
                return;
            }
            if count_rx.changed().await.is_err() {
                return;
            }
        }
    }

    pub fn set_idle_paused(&self, local_session_id: u64, paused: bool) -> bool {
        let Some(session) = self.lookup(local_session_id) else {
            return false;
        };
        session.idle_tx.send_if_modified(|state| {
            if state.paused == paused {
                return false;
            }
            state.paused = paused;
            true
        })
    }

    pub fn set_idle_fuel(&self, local_session_id: u64, fuel_seconds: i32) -> bool {
        let Some(session) = self.lookup(local_session_id) else {
            return false;
        };
        session.idle_tx.send_if_modified(|state| {
            if state.fuel_seconds == fuel_seconds {
                return false;
            }
            state.fuel_seconds = fuel_seconds;
            true
        })
    }

    pub fn record_output(&self, local_session_id: u64, output: &SessionOutput) {
        let Some(session) = self.lookup(local_session_id) else {
            return;
        };
        if session.active_spies.load(Ordering::Acquire) == 0 {
            return;
        }
        let _ = session.spy_tx.send(SpyEvent::Output {
            data: output.data.clone(),
            sequence: output.sequence,
        });
    }

    pub fn record_input(&self, local_session_id: u64, data: &Bytes) {
        let Some(session) = self.lookup(local_session_id) else {
            return;
        };
        if session.active_spies.load(Ordering::Acquire) == 0 {
            return;
        }
        let _ = session.spy_tx.send(SpyEvent::Input { data: data.clone() });
    }

    pub async fn spy(&self, local_session_id: u64, read_write: bool) -> Option<SpySession> {
        let session = self.lookup(local_session_id)?;
        if session.finished.load(Ordering::Acquire) {
            return None;
        }
        let mut event_rx = session.spy_tx.subscribe();
        session.active_spies.fetch_add(1, Ordering::AcqRel);

        let snapshot = match request_snapshot(&session).await {
            Some(snapshot) => snapshot,
            None => {
                session.active_spies.fetch_sub(1, Ordering::AcqRel);
                return None;
            }
        };

        Some(SpySession {
            session: session.clone(),
            snapshot: snapshot.snapshot,
            pending_events: drain_pending_spy_events(
                &mut event_rx,
                snapshot.output_sequence_cutoff,
            ),
            event_rx,
            input_tx: read_write.then_some(session.input_tx.clone()),
            username_rx: session.identity.username_receiver(),
            app_rx: session.identity.app_receiver(),
            idle_rx: session.idle_tx.subscribe(),
            size_rx: session.size_rx.clone(),
        })
    }

    fn lookup(&self, local_session_id: u64) -> Option<Arc<RuntimeSession>> {
        self.sessions
            .lock()
            .unwrap()
            .get(&local_session_id)
            .cloned()
    }
}

fn compose_status_bar_state(shared: &SharedStatusBarState) -> StatusBarState {
    let mut state = shared.base.clone();
    state.drain = shared.drain.clone();
    state
}

fn spawn_long_session_notifier(
    notifications: Arc<Notifications>,
) -> mpsc::UnboundedSender<Weak<RuntimeSession>> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Weak<RuntimeSession>>();
    tokio::spawn(async move {
        let mut deadlines = tokio_util::time::DelayQueue::new();
        loop {
            tokio::select! {
                Some(session) = rx.recv() => {
                    deadlines.insert(session, LONG_SESSION_WEBHOOK_AFTER);
                }
                Some(expired) = deadlines.next(), if !deadlines.is_empty() => {
                    let session = expired.into_inner();
                    let Some(strong_session) = session.upgrade() else {
                        continue;
                    };
                    if strong_session.finished.load(Ordering::Acquire) {
                        continue;
                    }

                    let session_id = format!(
                        "{}:{}",
                        strong_session.node_id,
                        strong_session.local_session_id
                    );
                    notifications.notify_long_session(LongSessionNotification {
                        session_id,
                        app_shortname: strong_session.identity.app().shortname,
                        duration: strong_session.started_at.elapsed(),
                    });
                    deadlines.insert(session, LONG_SESSION_WEBHOOK_AFTER);
                }
                else => break,
            }
        }
    });
    tx
}

async fn request_snapshot(session: &RuntimeSession) -> Option<ReplayTerminalSnapshotCapture> {
    let (tx, rx) = oneshot::channel();
    session.snapshot_tx.send(tx).await.ok()?;
    rx.await.ok()
}

fn drain_pending_spy_events(
    event_rx: &mut broadcast::Receiver<SpyEvent>,
    output_sequence_cutoff: u64,
) -> VecDeque<SpyEvent> {
    let mut pending_events = VecDeque::new();
    loop {
        match event_rx.try_recv() {
            Ok(event) => {
                let keep_event = match &event {
                    SpyEvent::Output { sequence, .. } => *sequence >= output_sequence_cutoff,
                    SpyEvent::Input { .. } | SpyEvent::Closed { .. } => true,
                };
                if keep_event {
                    pending_events.push_back(event);
                }
            }
            Err(TryRecvError::Empty) | Err(TryRecvError::Closed) | Err(TryRecvError::Lagged(_)) => {
                break;
            }
        }
    }
    pending_events
}
