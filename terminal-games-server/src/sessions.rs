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
        SessionAppState, SessionControl, SessionEndReason, SessionIdentity, SessionOutput,
        SessionUi,
    },
    control::{SessionSummary, StatusBarState},
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
    region_id: String,
    local_session_id: u64,
    user_id: Option<u64>,
    client_ip: IpAddr,
    transport: Transport,
    started_at: Instant,
    identity: SessionIdentity,
    size_rx: watch::Receiver<(u16, u16)>,
    idle_tx: watch::Sender<SessionIdleState>,
    input_tx: mpsc::Sender<Bytes>,
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
    region_id: String,
    long_session_notifier: Option<mpsc::UnboundedSender<Weak<RuntimeSession>>>,
    sessions: Mutex<HashMap<u64, Arc<RuntimeSession>>>,
    status_bar_state_tx: watch::Sender<StatusBarState>,
}

impl SessionRegistry {
    pub fn new(region_id: String, notifications: Arc<Notifications>) -> Arc<Self> {
        let (status_bar_state_tx, _) = watch::channel(StatusBarState::default());
        let long_session_notifier = notifications
            .enabled()
            .then(|| spawn_long_session_notifier(notifications));
        Arc::new(Self {
            region_id,
            long_session_notifier,
            sessions: Mutex::new(HashMap::new()),
            status_bar_state_tx,
        })
    }

    pub fn region_id(&self) -> &str {
        &self.region_id
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
        let (control_tx, control_rx) = watch::channel(SessionControl::Active);
        let (idle_tx, idle_rx) = watch::channel(SessionIdleState::default());
        let (snapshot_tx, snapshot_rx) = mpsc::channel(8);
        let (spy_tx, _) = broadcast::channel(64);
        let session = Arc::new(RuntimeSession {
            region_id: self.region_id.clone(),
            local_session_id,
            user_id,
            client_ip,
            transport,
            started_at: Instant::now(),
            identity: identity.clone(),
            size_rx,
            idle_tx: idle_tx.clone(),
            input_tx: input_tx.clone(),
            control_tx: control_tx.clone(),
            spy_tx,
            snapshot_tx,
            active_spies: AtomicUsize::new(0),
            finished: AtomicBool::new(false),
        });
        self.sessions
            .lock()
            .unwrap()
            .insert(local_session_id, session.clone());
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
            cleanup_guard: SessionCleanupGuard {
                registry: self.clone(),
                local_session_id,
            },
        }
    }

    pub fn set_status_bar_state(&self, state: StatusBarState) {
        self.status_bar_state_tx.send_replace(state);
    }

    pub fn remove(&self, local_session_id: u64) {
        self.sessions.lock().unwrap().remove(&local_session_id);
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
                session_id: format!("{}:{}", session.region_id, session.local_session_id),
                local_session_id: session.local_session_id,
                user_id: session.user_id,
                region_id: session.region_id.clone(),
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

    pub fn request_close(&self, local_session_id: u64, reason: SessionEndReason) -> bool {
        let Some(session) = self.lookup(local_session_id) else {
            return false;
        };
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
                        strong_session.region_id,
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
