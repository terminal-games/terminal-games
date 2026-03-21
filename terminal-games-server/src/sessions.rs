// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::Instant,
};

use bytes::Bytes;
use terminal_games::{
    app::{ActiveShortnameTracker, SessionEndReason},
    control::SessionSummary,
};
use tokio::sync::{broadcast, mpsc, watch};

use crate::metrics::Transport;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionAdminControl {
    Active,
    Kick,
}

#[derive(Clone, Debug)]
pub enum SpyEvent {
    Output(Vec<u8>),
    Resize { cols: u16, rows: u16 },
    Metadata { username: String },
    Input { data: Vec<u8> },
    Closed { reason: SessionEndReason },
}

pub struct SpySession {
    _session_guard: Arc<RuntimeSession>,
    pub initial_cols: u16,
    pub initial_rows: u16,
    pub initial_dump: String,
    pub event_rx: broadcast::Receiver<SpyEvent>,
    pub input_tx: Option<mpsc::Sender<Bytes>>,
}

pub struct FanoutTracker {
    trackers: Vec<Arc<dyn ActiveShortnameTracker>>,
}

impl FanoutTracker {
    pub fn new(trackers: Vec<Arc<dyn ActiveShortnameTracker>>) -> Arc<Self> {
        Arc::new(Self { trackers })
    }
}

impl ActiveShortnameTracker for FanoutTracker {
    fn set_active_shortname(&self, shortname: &str) {
        for tracker in &self.trackers {
            tracker.set_active_shortname(shortname);
        }
    }

    fn set_username(&self, username: &str) {
        for tracker in &self.trackers {
            tracker.set_username(username);
        }
    }
}

pub struct SessionRegistration {
    pub tracker: Arc<dyn ActiveShortnameTracker>,
    pub control_rx: watch::Receiver<SessionAdminControl>,
    pub admin_input_rx: mpsc::Receiver<Bytes>,
}

#[derive(Clone)]
struct SessionMetadata {
    local_session_id: u64,
    user_id: Option<u64>,
    username: String,
    client_ip: IpAddr,
    transport: Transport,
    current_shortname: String,
    started_at: Instant,
}

struct RuntimeSession {
    region_id: String,
    metadata: Mutex<SessionMetadata>,
    terminal: Mutex<avt::Vt>,
    output_tx: broadcast::Sender<SpyEvent>,
    input_tx: mpsc::Sender<Bytes>,
    control_tx: watch::Sender<SessionAdminControl>,
}

struct Tracker {
    session: Arc<RuntimeSession>,
}

impl ActiveShortnameTracker for Tracker {
    fn set_active_shortname(&self, shortname: &str) {
        let mut metadata = match self.session.metadata.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        metadata.current_shortname = shortname.to_string();
    }

    fn set_username(&self, username: &str) {
        {
            let mut metadata = match self.session.metadata.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            metadata.username = username.to_string();
        }
        let _ = self.session.output_tx.send(SpyEvent::Metadata {
            username: username.to_string(),
        });
    }
}

#[derive(Default)]
pub struct SessionRegistry {
    region_id: String,
    sessions: Mutex<HashMap<u64, Arc<RuntimeSession>>>,
}

impl SessionRegistry {
    pub fn new(region_id: String) -> Arc<Self> {
        Arc::new(Self {
            region_id,
            sessions: Mutex::new(HashMap::new()),
        })
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
        cols: u16,
        rows: u16,
    ) -> SessionRegistration {
        let terminal = avt::Vt::new((cols as usize).max(1), (rows as usize).max(1));
        let (output_tx, _) = broadcast::channel(64);
        let (input_tx, input_rx) = mpsc::channel(16);
        let (control_tx, control_rx) = watch::channel(SessionAdminControl::Active);
        let session = Arc::new(RuntimeSession {
            region_id: self.region_id.clone(),
            metadata: Mutex::new(SessionMetadata {
                local_session_id,
                user_id,
                username,
                client_ip,
                transport,
                current_shortname: initial_shortname,
                started_at: Instant::now(),
            }),
            terminal: Mutex::new(terminal),
            output_tx,
            input_tx,
            control_tx,
        });
        match self.sessions.lock() {
            Ok(mut guard) => {
                guard.insert(local_session_id, session.clone());
            }
            Err(poisoned) => {
                poisoned.into_inner().insert(local_session_id, session.clone());
            }
        }
        SessionRegistration {
            tracker: Arc::new(Tracker { session }) as Arc<dyn ActiveShortnameTracker>,
            control_rx,
            admin_input_rx: input_rx,
        }
    }

    pub fn remove(&self, local_session_id: u64) {
        match self.sessions.lock() {
            Ok(mut guard) => {
                guard.remove(&local_session_id);
            }
            Err(poisoned) => {
                poisoned.into_inner().remove(&local_session_id);
            }
        }
    }

    pub fn count(&self) -> usize {
        match self.sessions.lock() {
            Ok(guard) => guard.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        }
    }

    pub fn summaries(&self) -> Vec<SessionSummary> {
        let sessions = match self.sessions.lock() {
            Ok(guard) => guard.values().cloned().collect::<Vec<_>>(),
            Err(poisoned) => poisoned.into_inner().values().cloned().collect::<Vec<_>>(),
        };
        sessions
            .into_iter()
            .map(|session| {
                let metadata = match session.metadata.lock() {
                    Ok(guard) => guard.clone(),
                    Err(poisoned) => poisoned.into_inner().clone(),
                };
                let _transport = metadata.transport;
                SessionSummary {
                    session_id: format!("{}:{}", session.region_id, metadata.local_session_id),
                    local_session_id: metadata.local_session_id,
                    user_id: metadata.user_id,
                    region_id: session.region_id.clone(),
                    transport: metadata.transport.as_str().to_string(),
                    shortname: metadata.current_shortname,
                    duration_seconds: metadata.started_at.elapsed().as_secs(),
                    username: metadata.username,
                    ip_address: metadata.client_ip.to_string(),
                }
            })
            .collect()
    }

    pub fn kick(&self, local_session_id: u64) -> bool {
        let Some(session) = self.lookup(local_session_id) else {
            return false;
        };
        let _ = session.control_tx.send(SessionAdminControl::Kick);
        true
    }

    pub fn finish(&self, local_session_id: u64, reason: SessionEndReason) -> bool {
        let Some(session) = self.lookup(local_session_id) else {
            return false;
        };
        let _ = session.output_tx.send(SpyEvent::Closed { reason });
        true
    }

    pub fn record_output(&self, local_session_id: u64, data: &Arc<Vec<u8>>) {
        let Some(session) = self.lookup(local_session_id) else {
            return;
        };
        {
            let mut terminal = match session.terminal.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            terminal.feed_str(&String::from_utf8_lossy(data));
        }
        let _ = session.output_tx.send(SpyEvent::Output((**data).clone()));
    }

    pub fn record_input(&self, local_session_id: u64, data: &[u8]) {
        let Some(session) = self.lookup(local_session_id) else {
            return;
        };
        let _ = session.output_tx.send(SpyEvent::Input { data: data.to_vec() });
    }

    pub fn record_resize(&self, local_session_id: u64, cols: u16, rows: u16) {
        let Some(session) = self.lookup(local_session_id) else {
            return;
        };
        let mut terminal = match session.terminal.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        terminal.resize((cols as usize).max(1), (rows as usize).max(1));
        let _ = session.output_tx.send(SpyEvent::Resize { cols, rows });
    }

    pub fn spy(&self, local_session_id: u64, read_write: bool) -> Option<SpySession> {
        let session = self.lookup(local_session_id)?;
        let (initial_cols, initial_rows, initial_dump) = {
            let terminal = match session.terminal.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            let (cols, rows) = terminal.size();
            (cols as u16, rows as u16, terminal.dump())
        };
        Some(SpySession {
            _session_guard: session.clone(),
            initial_cols,
            initial_rows,
            initial_dump,
            event_rx: session.output_tx.subscribe(),
            input_tx: read_write.then_some(session.input_tx.clone()),
        })
    }

    fn lookup(&self, local_session_id: u64) -> Option<Arc<RuntimeSession>> {
        match self.sessions.lock() {
            Ok(guard) => guard.get(&local_session_id).cloned(),
            Err(poisoned) => poisoned.into_inner().get(&local_session_id).cloned(),
        }
    }
}
