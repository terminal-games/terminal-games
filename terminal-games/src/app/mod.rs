// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod app;
mod audio;
mod log;
mod menu;
mod net;
mod peer;
mod terminal;

use std::{
    collections::{BTreeMap, HashMap},
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use headless_terminal::{Color, Parser};
use hex::ToHex;
use ipnet::{Ipv4Net, Ipv6Net};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use wasmtime::InstanceAllocationStrategy;
use wasmtime_wasi::I32Exit;

use crate::{
    app_env::decrypt_app_env_blob,
    app_registry::{AppRuntimeRegistry, AppRuntimeSession},
    audio::Mixer,
    control::StatusBarState,
    log_backend::GuestLogBackend,
    mesh::{AppId, BuildId, Mesh, PeerId, PeerMessageApp},
    rate_limiting::{NetworkInfo, TokenBucket},
    replay::ReplayBuffer,
    status_bar::{StatusBar, StatusBarInput, StatusNotification},
    terminal_profile::TerminalProfile,
};

/// Maximum number of concurrent connections per app instance
const MAX_CONNECTIONS: usize = 8;

/// Maximum outbound bandwidth in bytes per second (20 kBps)
const MAX_OUTBOUND_BANDWIDTH: u64 = 20 * 1024;

const REPLAY_RATE_LIMIT_SECS: u64 = 10;
const REPLAY_UPLOAD_NOTIFICATION_SECS: u64 = 30;
const REPLAY_RESULT_NOTIFICATION_SECS: u64 = 15;
// Dial error codes
const DIAL_ERR_ADDRESS_TOO_LONG: i32 = -1;
const DIAL_ERR_TOO_MANY_CONNECTIONS: i32 = -2;

// Poll dial error codes
const POLL_DIAL_PENDING: i32 = -1;
const POLL_DIAL_ERR_INVALID_DIAL_ID: i32 = -2;
const POLL_DIAL_ERR_TASK_FAILED: i32 = -3;
const POLL_DIAL_ERR_TOO_MANY_CONNECTIONS: i32 = -4;
const POLL_DIAL_ERR_DNS_RESOLUTION: i32 = -5;
const POLL_DIAL_ERR_NOT_GLOBALLY_REACHABLE: i32 = -6;
const POLL_DIAL_ERR_CONNECTION_FAILED: i32 = -7;
const POLL_DIAL_ERR_INVALID_DNS_NAME: i32 = -8;
const POLL_DIAL_ERR_TLS_HANDSHAKE: i32 = -9;

// Connection error codes
const CONN_ERR_INVALID_CONN_ID: i32 = -1;
const CONN_ERR_CONNECTION_ERROR: i32 = -2;

// Peer error codes
const PEER_SEND_ERR_INVALID_PEER_COUNT: i32 = -1;
const PEER_SEND_ERR_DATA_TOO_LARGE: i32 = -2;
const PEER_SEND_ERR_CHANNEL_FULL: i32 = -3;
const PEER_SEND_ERR_CHANNEL_CLOSED: i32 = -4;
const PEER_SEND_ERR_INVALID_PEER_ID: i32 = -5;

const PEER_RECV_ERR_CHANNEL_DISCONNECTED: i32 = -1;

const NODE_LATENCY_ERR_UNKNOWN: i32 = -1;

// Menu request error codes
const MENU_REQ_ERR_NOT_MENU_APP: i32 = -1;
const MENU_REQ_ERR_INVALID_INPUT: i32 = -2;
const MENU_REQ_ERR_TOO_MANY_REQUESTS: i32 = -3;

const MAX_MENU_REQUESTS: usize = 4;
const WASM_MEMORY_LIMIT_BYTES: u64 = 32 * 1024 * 1024 * 1024;

const MENU_POLL_PENDING: i32 = -1;
const MENU_POLL_ERR_INVALID_REQUEST_ID: i32 = -2;
const MENU_POLL_ERR_BUFFER_TOO_SMALL: i32 = -4;
const MENU_POLL_ERR_REQUEST_FAILED: i32 = -5;

const MENU_REQ_GAMES_LIST: i32 = 0;
const MENU_REQ_PROFILE_GET: i32 = 1;
const MENU_REQ_PROFILE_SET: i32 = 2;
const MENU_REQ_REPLAYS_LIST: i32 = 3;
const MENU_REQ_REPLAY_DELETE: i32 = 4;

const NEXT_APP_READY_READY: i32 = 1;
const NEXT_APP_READY_NOT_READY: i32 = 0;
const NEXT_APP_READY_ERR_UNKNOWN_SHORTNAME: i32 = -1;
const NEXT_APP_READY_ERR_PREPARE_FAILED_OTHER: i32 = -2;
pub const MAX_SCREEN_COLS: u16 = 80 * 6;
pub const MAX_SCREEN_ROWS: u16 = 24 * 6;
const CHANGE_APP_RATE_LIMIT_SECS: u64 = 10;
const CHANGE_APP_ERR_RATE_LIMITED: i32 = -2;

pub fn clamp_window_size(cols: u16, rows: u16) -> (u16, u16) {
    (cols.min(MAX_SCREEN_COLS), rows.min(MAX_SCREEN_ROWS))
}

pub fn clamp_window_size_u32(cols: u32, rows: u32) -> (u16, u16) {
    clamp_window_size(
        cols.min(u16::MAX as u32) as u16,
        rows.min(u16::MAX as u32) as u16,
    )
}

pub type AppExitResult = Result<I32Exit, AppRunError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionControl {
    Active,
    Close(SessionEndReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEndReason {
    NormalExit,
    ConnectionLost,
    IdleTimeout,
    BannedIp,
    TooManyConnectionsFromIp,
    ClusterLimited,
    KickedByAdmin,
    ServerShutdown,
}

impl SessionEndReason {
    pub fn slug(self) -> &'static str {
        match self {
            Self::NormalExit => "normal_exit",
            Self::ConnectionLost => "connection_lost",
            Self::IdleTimeout => "idle_timeout",
            Self::BannedIp => "banned_ip",
            Self::TooManyConnectionsFromIp => "too_many_connections_from_ip",
            Self::ClusterLimited => "cluster_limited",
            Self::KickedByAdmin => "kicked_by_admin",
            Self::ServerShutdown => "server_shutdown",
        }
    }

    pub fn user_message(self) -> &'static str {
        match self {
            Self::NormalExit => "Thanks for playing!",
            Self::ConnectionLost => "Disconnected because the connection was lost.",
            Self::IdleTimeout => "Disconnected due to idle timeout.",
            Self::BannedIp => "Connections from your IP address are blocked",
            Self::TooManyConnectionsFromIp => "Too many active sessions from your IP address",
            Self::ClusterLimited => "Kicked for likely bot activity",
            Self::KickedByAdmin => "Disconnected by an administrator.",
            Self::ServerShutdown => "Server is shutting down for maintenance.",
        }
    }

    pub fn should_notify_web_client(self) -> bool {
        !matches!(self, Self::ConnectionLost)
    }
}

#[derive(Clone)]
pub struct SessionIdentity {
    username_tx: watch::Sender<String>,
    app_tx: watch::Sender<SessionAppState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionAppState {
    pub app_id: Option<AppId>,
    pub shortname: String,
}

impl SessionIdentity {
    pub fn new(username: String, shortname: String) -> Self {
        let (username_tx, _) = watch::channel(username);
        let (app_tx, _) = watch::channel(SessionAppState {
            app_id: None,
            shortname,
        });
        Self {
            username_tx,
            app_tx,
        }
    }

    pub fn username(&self) -> String {
        self.username_tx.borrow().clone()
    }

    pub fn app(&self) -> SessionAppState {
        self.app_tx.borrow().clone()
    }

    pub fn shortname(&self) -> String {
        self.app_tx.borrow().shortname.clone()
    }

    pub fn username_receiver(&self) -> watch::Receiver<String> {
        self.username_tx.subscribe()
    }

    pub fn app_receiver(&self) -> watch::Receiver<SessionAppState> {
        self.app_tx.subscribe()
    }

    pub fn set_username(&self, username: &str) {
        self.username_tx.send_replace(username.to_string());
    }

    pub fn set_app(&self, app_id: AppId, shortname: &str) {
        self.app_tx.send_replace(SessionAppState {
            app_id: Some(app_id),
            shortname: shortname.to_string(),
        });
    }
}

#[derive(Clone)]
pub struct SessionNotificationSender {
    tx: mpsc::Sender<StatusNotification>,
}

impl SessionNotificationSender {
    fn new(tx: mpsc::Sender<StatusNotification>) -> Self {
        Self { tx }
    }

    pub fn try_info(&self, content: impl Into<String>, duration: Duration) {
        let _ = self
            .tx
            .try_send(StatusNotification::info(content, duration));
    }

    pub fn try_warning(&self, content: impl Into<String>, duration: Duration) {
        let _ = self
            .tx
            .try_send(StatusNotification::warning(content, duration));
    }

    pub fn try_error(&self, content: impl Into<String>, duration: Duration) {
        let _ = self
            .tx
            .try_send(StatusNotification::error(content, duration));
    }
}

pub struct SessionUi {
    notification_tx: SessionNotificationSender,
    notification_rx: mpsc::Receiver<StatusNotification>,
    shared_state_rx: watch::Receiver<StatusBarState>,
}

pub struct SessionIo {
    closed: AtomicBool,
}

impl Default for SessionIo {
    fn default() -> Self {
        Self {
            closed: AtomicBool::new(false),
        }
    }
}

impl SessionIo {
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl SessionUi {
    pub fn new(shared_state_rx: watch::Receiver<StatusBarState>) -> Self {
        let (notification_tx, notification_rx) = mpsc::channel::<StatusNotification>(8);
        Self {
            notification_tx: SessionNotificationSender::new(notification_tx),
            notification_rx,
            shared_state_rx,
        }
    }

    pub fn notification_sender(&self) -> SessionNotificationSender {
        self.notification_tx.clone()
    }

    fn into_parts(
        self,
    ) -> (
        SessionNotificationSender,
        mpsc::Receiver<StatusNotification>,
        watch::Receiver<StatusBarState>,
    ) {
        (
            self.notification_tx,
            self.notification_rx,
            self.shared_state_rx,
        )
    }
}

pub struct AppServer {
    linker: Arc<wasmtime::Linker<AppState>>,
    engine: wasmtime::Engine,
    pub db: libsql::Connection,
    mesh: Mesh,
    app_env_secret_key: Arc<str>,
    app_registry: AppRuntimeRegistry,
}

pub struct AppInstantiationParams {
    pub input_receiver: tokio::sync::mpsc::Receiver<Bytes>,
    pub replay_request_receiver: tokio::sync::mpsc::Receiver<()>,
    pub spy_snapshot_requests: tokio::sync::mpsc::Receiver<
        tokio::sync::oneshot::Sender<crate::replay::ReplayTerminalSnapshotCapture>,
    >,
    pub output_sender: tokio::sync::mpsc::Sender<SessionOutput>,
    pub audio_sender: Option<tokio::sync::mpsc::Sender<Vec<u8>>>,
    pub window_size_receiver: tokio::sync::watch::Receiver<(u16, u16)>,
    pub graceful_shutdown_token: CancellationToken,
    pub session_io: Arc<SessionIo>,
    pub session_identity: SessionIdentity,
    pub session_ui: SessionUi,
    pub remote_sshid: String,
    pub term: Option<String>,
    pub args: Option<Vec<u8>>,
    pub network_info: Arc<dyn NetworkInfo>,
    pub terminal_profile: TerminalProfile,
    pub terminal_parser: Parser,
    pub user_id: Option<u64>,
    pub locale: String,
    pub log_backend: Arc<dyn GuestLogBackend>,
}

#[derive(Debug, Clone)]
pub struct SessionOutput {
    pub data: Bytes,
    pub sequence: u64,
}

impl AppServer {
    pub fn new(
        mesh: Mesh,
        db: libsql::Connection,
        app_env_secret_key: impl Into<Arc<str>>,
    ) -> anyhow::Result<Self> {
        let mut config = wasmtime::Config::new();
        let cache_config = wasmtime::CacheConfig::new();
        config.cache(Some(wasmtime::Cache::new(cache_config)?));
        config.allocation_strategy(InstanceAllocationStrategy::pooling());
        config.compiler_inlining(true);
        let engine = wasmtime::Engine::new(&config)?;

        let mut linker = wasmtime::Linker::<AppState>::new(&engine);
        wasmtime_wasi::p1::add_to_linker_async(&mut linker, |t| &mut t.app.wasi_ctx)?;

        crate::wasm_abi::link(&mut linker)?;
        Ok(Self {
            linker: Arc::new(linker),
            mesh,
            db,
            app_env_secret_key: app_env_secret_key.into(),
            engine,
            app_registry: AppRuntimeRegistry::default(),
        })
    }

    pub fn app_env_secret_key(&self) -> &str {
        &self.app_env_secret_key
    }

    pub fn app_registry(&self) -> &AppRuntimeRegistry {
        &self.app_registry
    }

    /// Run an app with IO defined in [`params`] in the background
    pub fn instantiate_app(
        &self,
        params: AppInstantiationParams,
    ) -> tokio::sync::oneshot::Receiver<AppExitResult> {
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();

        let mesh = self.mesh.clone();
        let db = self.db.clone();
        let engine = self.engine.clone();
        let linker = self.linker.clone();
        let app_registry = self.app_registry.clone();
        let app_env_secret_key = self.app_env_secret_key.clone();

        tokio::task::spawn(async move {
            let AppInstantiationParams {
                input_receiver: mut raw_input_receiver,
                mut replay_request_receiver,
                mut spy_snapshot_requests,
                output_sender,
                audio_sender,
                mut window_size_receiver,
                mut graceful_shutdown_token,
                session_io,
                session_identity,
                session_ui,
                remote_sshid,
                term,
                args: _args,
                network_info,
                terminal_profile,
                mut terminal_parser,
                user_id,
                locale,
                log_backend,
            } = params;
            let startup_shortname = session_identity.shortname();
            let (notification_tx, notification_rx, status_bar_state_rx) = session_ui.into_parts();

            let (first_cols, first_rows) = {
                let (cols, rows) = *window_size_receiver.borrow();
                clamp_window_size(cols, rows)
            };
            terminal_parser
                .screen_mut()
                .set_size(first_rows, first_cols);
            let mut terminal = terminal_parser;
            let mut terminal_snapshot = Arc::new(TerminalSnapshot::from_screen(terminal.screen()));
            let hard_shutdown_token = CancellationToken::new();

            let (app_output_sender, mut app_output_receiver) =
                tokio::sync::mpsc::channel::<BytesMut>(1);
            let has_next_app_shortname = Arc::new(AtomicBool::new(false));
            let change_app_limiter = Arc::new(ChangeAppLimiter::default());

            let audio_enabled = audio_sender.is_some();
            let mut maybe_mixer = if let Some(audio_tx) = audio_sender {
                match Mixer::new(audio_tx) {
                    Ok(mixer) => Some(mixer),
                    Err(error) => {
                        tracing::error!(?error, "Failed to create mixer");
                        None
                    }
                }
            } else {
                None
            };

            let mut exit_result = Ok(I32Exit(0));
            let (mut menu_request_tx, mut menu_request_rx) =
                mpsc::channel::<MenuRequestJob>(MAX_MENU_REQUESTS);
            let (menu_result_tx, mut menu_result_rx) =
                mpsc::channel::<(usize, Result<Vec<u8>, String>, MenuUpdate)>(MAX_MENU_REQUESTS);
            let (menu_completed_tx, mut menu_completed_rx) =
                mpsc::channel::<(usize, Result<Vec<u8>, String>)>(MAX_MENU_REQUESTS);
            let mut menu_session = MenuSessionState {
                locale: locale.clone(),
                replays: Vec::new(),
            };
            let mut ctx = AppContext {
                db: db.clone(),
                linker,
                mesh,
                app_env_secret_key,
                app_registry,
                app_output_sender,
                remote_sshid,
                term,
                audio_enabled,
                user_id,
                locale,
                log_backend,
            };
            let (mut app, mut instance_pre) = match Self::prepare_instantiate(
                &ctx,
                &menu_session,
                &session_identity,
                startup_shortname.clone(),
            )
            .await
            {
                Ok(v) => v,
                Err(error) => {
                    let _ = exit_tx.send(Err(AppRunError::new(format!(
                        "Failed to start app {}: {error:#}",
                        startup_shortname
                    ))));
                    return;
                }
            };
            session_identity.set_app(app.app_id, &app.shortname);

            let mut replay_buffer = ReplayBuffer::new(
                first_cols,
                first_rows,
                &app.shortname,
                app.app_id,
                ctx.term.as_deref(),
            );
            let status_bar_input = StatusBarInput::new();
            let mut status_bar = StatusBar::new(
                session_identity.app_receiver(),
                session_identity.username_receiver(),
                network_info.clone(),
                terminal_profile,
                notification_rx,
                status_bar_state_rx,
                status_bar_input.clone(),
            );
            let mut escape_buffer = EscapeSequenceBuffer::new();
            let mut graceful_shutdown_timeout: Option<Pin<Box<tokio::time::Sleep>>> = None;
            let mut replay_last_at = Duration::ZERO;
            let mut pending_replay_upload: Option<
                tokio::sync::oneshot::Receiver<(StatusNotification, Option<MenuSessionState>)>,
            > = None;
            let mut replay_requests_open = true;
            let mut next_output_sequence = 0_u64;

            loop {
                let app_shortname = app.shortname.clone();
                let state = AppState {
                    app,
                    ctx,
                    conn_manager: ConnectionManager::default(),
                    menu_requests: Vec::with_capacity(MAX_MENU_REQUESTS),
                    completed_menu_results: HashMap::new(),
                    menu_completed_rx,
                    menu_request_tx,
                    menu_session: menu_session.clone(),
                    session_identity: session_identity.clone(),
                    limits: AppLimiter::default(),
                    next_app: None,
                    input_receiver: raw_input_receiver,
                    has_next_app: has_next_app_shortname.clone(),
                    graceful_shutdown_token: graceful_shutdown_token.clone(),
                    session_io: session_io.clone(),
                    network_info: network_info.clone(),
                    terminal_profile,
                    audio: maybe_mixer,
                    status_bar_input: status_bar_input.clone(),
                    terminal_snapshot: terminal_snapshot.clone(),
                    change_app_limiter: change_app_limiter.clone(),
                };

                let mut store = wasmtime::Store::new(&engine, state);
                store.limiter(|state| &mut state.limits);
                store.call_hook_async(AsyncCallHook {});

                let instance = match instance_pre.instantiate_async(&mut store).await {
                    Ok(instance) => instance,
                    Err(err) => {
                        tracing::error!(error = %err, "Failed to instantiate module");
                        break;
                    }
                };
                let func = match instance.get_typed_func::<(), ()>(&mut store, "_start") {
                    Ok(func) => func,
                    Err(err) => {
                        tracing::error!(error = %err, "Failed to locate _start");
                        break;
                    }
                };

                has_next_app_shortname.store(false, Ordering::Release);
                let call_result: Option<wasmtime::Result<()>> = 'block: {
                    let mut call_future = Box::pin(func.call_async(&mut store, ()));
                    loop {
                        tokio::select! {
                            biased;

                            _ = hard_shutdown_token.cancelled() => {
                                break 'block None;
                            }

                            _ = graceful_shutdown_token.cancelled(), if graceful_shutdown_timeout.is_none() => {
                                session_io.close();
                                graceful_shutdown_timeout = Some(Box::pin(tokio::time::sleep(Duration::from_millis(5000))));
                            }

                            result = &mut call_future => {
                                break 'block Some(result);
                            }

                            data = app_output_receiver.recv() => {
                                let Some(mut data) = data else { continue };
                                if session_io.is_closed() {
                                    continue;
                                }
                                if has_next_app_shortname.load(Ordering::Acquire) {
                                    let needle = b"\x1b[?1049l";
                                    let needle2 = b"\x1b[?25h";
                                    let mut read = 0;
                                    let mut write = 0;
                                    while read < data.len() {
                                        if data[read..].starts_with(needle) {
                                            read += needle.len();
                                        } else if data[read..].starts_with(needle2) {
                                            read += needle2.len();
                                        } else {
                                            data[write] = data[read];
                                            write += 1;
                                            read += 1;
                                        }
                                    }
                                    data.truncate(write);
                                }

                                let mut output = escape_buffer.push(data);
                                if output.is_empty() {
                                    continue;
                                }

                                terminal.process(&output);
                                let screen = terminal.screen_mut();
                                let (height, _) = screen.size();
                                let dirty = screen.take_damaged_rows();
                                if dirty.contains(&(height - 1)) {
                                    status_bar.maybe_render_into(screen, &mut output, true);
                                }
                                terminal_snapshot.sync_from_screen(screen);

                                if !output.is_empty() {
                                    let output = output.freeze();
                                    let sequence = next_output_sequence;
                                    next_output_sequence += 1;
                                    replay_buffer.push_output(output.clone());
                                    if output_sender
                                        .send(SessionOutput { data: output, sequence })
                                        .await
                                        .is_err()
                                    {
                                        if session_io.is_closed() {
                                            continue;
                                        }
                                        break 'block None;
                                    }
                                }
                            }

                            result = window_size_receiver.changed(), if !session_io.is_closed() => {
                                if result.is_err() {
                                    break 'block None;
                                }
                                let (width, height) = {
                                    let (cols, rows) = *window_size_receiver.borrow();
                                    clamp_window_size(cols, rows)
                                };
                                replay_buffer.push_resize(width, height);

                                let screen = terminal.screen_mut();
                                screen.set_size(height, width);
                                let mut output = BytesMut::new();
                                status_bar.maybe_render_into(screen, &mut output, true);
                                if !output.is_empty() {
                                    let output = output.freeze();
                                    let sequence = next_output_sequence;
                                    next_output_sequence += 1;
                                    replay_buffer.push_output(output.clone());
                                    if output_sender
                                        .send(SessionOutput { data: output, sequence })
                                        .await
                                        .is_err()
                                    {
                                        if session_io.is_closed() {
                                            continue;
                                        }
                                        break 'block None;
                                    }
                                }
                                terminal_snapshot.sync_from_screen(screen);
                            }

                            status_bar_render = status_bar.drive(), if !session_io.is_closed() => {
                                let Some(force) = status_bar_render else {
                                    break 'block None;
                                };
                                let mut output = BytesMut::new();
                                status_bar.maybe_render_into(terminal.screen(), &mut output, force);
                                if !output.is_empty() {
                                    let output = output.freeze();
                                    let sequence = next_output_sequence;
                                    next_output_sequence += 1;
                                    replay_buffer.push_output(output.clone());
                                    if output_sender
                                        .send(SessionOutput { data: output, sequence })
                                        .await
                                        .is_err()
                                    {
                                        if session_io.is_closed() {
                                            continue;
                                        }
                                        break 'block None;
                                    }
                                }
                            }

                            msg = menu_result_rx.recv() => {
                                if let Some((request_id, result, update)) = msg {
                                    let _ = menu_completed_tx.try_send((request_id, result));
                                    if let Some(username) = update.username {
                                        session_identity.set_username(&username);
                                    }
                                    if let Some(locale) = update.locale {
                                        menu_session.locale = locale;
                                    }
                                    if let Some(replays) = update.replays {
                                        menu_session.replays = replays;
                                    }
                                    if session_io.is_closed() {
                                        continue;
                                    }
                                    let mut output = BytesMut::new();
                                    status_bar.maybe_render_into(terminal.screen(), &mut output, true);
                                    if !output.is_empty() {
                                        let output = output.freeze();
                                        let sequence = next_output_sequence;
                                        next_output_sequence += 1;
                                        replay_buffer.push_output(output.clone());
                                        if output_sender
                                            .send(SessionOutput { data: output, sequence })
                                            .await
                                            .is_err()
                                        {
                                            if session_io.is_closed() {
                                                continue;
                                            }
                                            break 'block None;
                                        }
                                    }
                                }
                            }

                            request = menu_request_rx.recv() => {
                                let Some(request) = request else { continue };
                                let db = db.clone();
                                let menu_result_tx = menu_result_tx.clone();
                                let menu_session = menu_session.clone();
                                let menu_username = session_identity.username();
                                tokio::spawn(async move {
                                    let (result, update) = match request.kind {
                                        MenuRequestKind::AppsList { current_shortname } => {
                                            let result = Self::load_menu_games(db, current_shortname).await;
                                            (result, MenuUpdate::default())
                                        }
                                        MenuRequestKind::ProfileGet => {
                                            Self::load_menu_profile(
                                                db,
                                                user_id,
                                                menu_session,
                                                menu_username,
                                            )
                                            .await
                                        }
                                        MenuRequestKind::ProfileSet { username, locale } => {
                                            Self::save_menu_profile(
                                                db,
                                                user_id,
                                                menu_session,
                                                username,
                                                locale,
                                            )
                                            .await
                                        }
                                        MenuRequestKind::ReplaysList { locale } => {
                                            Self::load_menu_replays(db, user_id, menu_session, locale).await
                                        }
                                        MenuRequestKind::ReplayDelete { created_at } => {
                                            Self::delete_menu_replay(db, user_id, menu_session, created_at).await
                                        }
                                    };
                                    let _ = menu_result_tx
                                        .send((request.request_id, result, update))
                                        .await;
                                });
                            }

                            replay_req = replay_request_receiver.recv(), if replay_requests_open => {
                                if replay_req.is_none() {
                                    replay_requests_open = false;
                                    continue;
                                }
                                pending_replay_upload = pending_replay_upload.or_else(|| {
                                    trigger_replay_upload(
                                        &notification_tx,
                                        &replay_buffer,
                                        &mut replay_last_at,
                                        db.clone(),
                                        menu_session.clone(),
                                        user_id,
                                        session_identity.username(),
                                    )
                                });
                            }

                            snapshot_request = spy_snapshot_requests.recv() => {
                                let Some(snapshot_request) = snapshot_request else {
                                    continue;
                                };
                                let _ = snapshot_request.send(crate::replay::ReplayTerminalSnapshotCapture {
                                    snapshot: replay_buffer.terminal_snapshot(),
                                    output_sequence_cutoff: next_output_sequence,
                                });
                            }

                            _ = async {
                                graceful_shutdown_timeout.as_mut().expect("guarded by select").await;
                            }, if graceful_shutdown_timeout.is_some() => {
                                hard_shutdown_token.cancel();
                                graceful_shutdown_timeout = None;
                            }

                            notification = async {
                                pending_replay_upload.as_mut().expect("guarded by select").await
                            }, if pending_replay_upload.is_some() => {
                                pending_replay_upload = None;
                                let (notif, session_update) = match notification {
                                    Ok(v) => v,
                                    Err(_) => (
                                        StatusNotification::error(
                                            " Failed to upload replay. ",
                                            Duration::from_secs(REPLAY_RATE_LIMIT_SECS),
                                        ),
                                        None,
                                    ),
                                };
                                if let Some(session) = session_update {
                                    menu_session = session;
                                }
                                let _ = notification_tx.tx.try_send(notif);
                            }
                        }
                    }
                };

                if let Some(Err(err)) = call_result {
                    match err.downcast::<wasmtime_wasi::I32Exit>() {
                        Ok(code) => {
                            exit_result = Ok(code);
                        }
                        Err(other) => {
                            let error = AppRunError::new(format!(
                                "App {} trapped: {other:#}",
                                app_shortname
                            ));
                            tracing::error!(error = %error, "Module errored");
                            exit_result = Err(error);
                        }
                    }
                }

                let mut state = store.into_data();
                menu_completed_rx = state.menu_completed_rx;
                menu_request_tx = state.menu_request_tx;
                graceful_shutdown_token = state.graceful_shutdown_token;
                raw_input_receiver = state.input_receiver;
                terminal_snapshot = state.terminal_snapshot;
                maybe_mixer = state.audio;
                ctx = state.ctx;

                if let Some(next_app) = state.next_app.take() {
                    let next_app = match next_app {
                        NextAppState::Pending(next_app) => next_app.await.unwrap_or_else(|_| {
                            Err(NextAppPrepareError::other(
                                "next app preload task closed unexpectedly",
                            ))
                        }),
                        NextAppState::Ready(next_app) => Ok(next_app),
                        NextAppState::Failed(error) => Err(error),
                    };
                    let Ok((next_app, next_instance_pre)) = next_app else {
                        let Err(error) = next_app else { unreachable!() };
                        let error =
                            AppRunError::new(format!("Failed to change app: {}", error.message()));
                        tracing::error!(error = %error, "App change failed");
                        exit_result = Err(error);
                        break;
                    };

                    let shortname = next_app.shortname.clone();
                    replay_buffer.push_app_switch(next_app.app_id, &shortname);
                    session_identity.set_app(next_app.app_id, &shortname);
                    app = next_app;
                    instance_pre = next_instance_pre;
                    continue;
                }

                break;
            }

            hard_shutdown_token.cancel();
            let _ = exit_tx.send(exit_result);
        });

        exit_rx
    }

    async fn prepare_instantiate(
        ctx: &AppContext,
        menu_session: &MenuSessionState,
        session_identity: &SessionIdentity,
        shortname: String,
    ) -> anyhow::Result<(PreloadedAppState, wasmtime::InstancePre<AppState>)> {
        let mut app_rows = ctx
            .db
            .query(
                "SELECT id, wasm, wasm_hash, env_hash, env_salt, env_blob, build_updated_at
                 FROM apps
                 WHERE shortname = ?1
                 LIMIT 1",
                [shortname.as_str()],
            )
            .await?;
        let app_row = app_rows
            .next()
            .await?
            .ok_or_else(|| UnknownGameShortnameError(shortname.clone()))?;
        let app_id = app_row.get::<u64>(0)?;
        let wasm_bytes = app_row.get::<Vec<u8>>(1)?;
        let build_id =
            BuildId::from_hash_slices(&app_row.get::<Vec<u8>>(2)?, &app_row.get::<Vec<u8>>(3)?)?;
        let env_salt = app_row.get::<Vec<u8>>(4)?;
        let env_blob = app_row.get::<Vec<u8>>(5)?;
        let build_updated_at = app_row.get::<i64>(6)?;
        let mut envs: Vec<(String, String)> =
            decrypt_app_env_blob(ctx.app_env_secret_key.as_ref(), &env_salt, &env_blob)?
                .into_iter()
                .map(|env| (env.name, env.value))
                .collect();
        let runtime_app_id = AppId { app_id, build_id };
        let app_runtime_session = ctx
            .app_registry
            .subscribe(app_id, build_id, build_updated_at);

        let (peer_id, peer_rx, peer_tx) = ctx.mesh.new_peer(runtime_app_id).await;
        envs.push(("REMOTE_SSHID".to_string(), ctx.remote_sshid.clone()));
        let username = session_identity.username();
        let mut locale = ctx.locale.clone();
        if !menu_session.locale.is_empty() {
            locale = menu_session.locale.clone();
        }
        envs.push(("USERNAME".to_string(), username));
        envs.push(("PEER_ID".to_string(), peer_id.to_bytes().encode_hex()));
        envs.push(("APP_SHORTNAME".to_string(), shortname.to_string()));
        envs.push(("APP_VERSION_ID".to_string(), build_id.id_string()));
        envs.push((
            "AUDIO_ENABLED".to_string(),
            if ctx.audio_enabled { "1" } else { "0" }.to_string(),
        ));
        if let Some(term) = ctx.term.clone() {
            envs.push(("TERM".to_string(), term));
        }
        if let Some(user_id) = ctx.user_id {
            envs.push(("USER_ID".to_string(), user_id.to_string()));
        }
        if let Some(user_id) = ctx.user_id
            && let Ok(mut rows) = ctx
                .db
                .query(
                    "SELECT locale FROM users WHERE id = ?1 LIMIT 1",
                    libsql::params!(user_id),
                )
                .await
            && let Ok(Some(row)) = rows.next().await
        {
            locale = row.get::<String>(0).unwrap_or(locale);
        }
        envs.push(("LOCALE".to_string(), locale.clone()));

        let wasi_ctx = wasmtime_wasi::WasiCtx::builder()
            .stdout(MyStdoutStream {
                sender: ctx.app_output_sender.clone(),
            })
            .envs(&envs)
            .build_p1();

        let module = ctx
            .app_registry
            .load_or_compile_module(&wasm_bytes, ctx.linker.engine())?;
        let linker = (*ctx.linker).clone();
        let instance_pre = linker.instantiate_pre(&module)?;

        Ok((
            PreloadedAppState {
                wasi_ctx,
                peer_rx,
                peer_tx,
                app_id: runtime_app_id,
                app_runtime_session,
                shortname,
            },
            instance_pre,
        ))
    }
}

fn trigger_replay_upload(
    notification_tx: &SessionNotificationSender,
    replay_buffer: &ReplayBuffer,
    replay_last_at: &mut Duration,
    db: libsql::Connection,
    menu_session: MenuSessionState,
    user_id: Option<u64>,
    username: String,
) -> Option<tokio::sync::oneshot::Receiver<(StatusNotification, Option<MenuSessionState>)>> {
    let now = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    if now.saturating_sub(*replay_last_at) < Duration::from_secs(REPLAY_RATE_LIMIT_SECS) {
        notification_tx.try_warning(
            " Please wait 10 seconds between replays. ",
            Duration::from_secs(REPLAY_RATE_LIMIT_SECS),
        );
        return None;
    }

    *replay_last_at = now;
    notification_tx.try_info(
        " \x1b[3mUploading replay... ",
        Duration::from_secs(REPLAY_UPLOAD_NOTIFICATION_SECS),
    );

    let Ok((initial_app_id, asciicast)) = replay_buffer.serialize_asciicast() else {
        tracing::error!("Failed to serialize replay");
        notification_tx.try_error(
            " Failed to serialize replay. ",
            Duration::from_secs(REPLAY_RATE_LIMIT_SECS),
        );
        return None;
    };

    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (notif, session_update) = save_replay(
            &db,
            menu_session,
            user_id,
            initial_app_id,
            &username,
            &asciicast,
        )
        .await;
        let notif = match notif {
            Ok(url) => {
                tracing::info!(%url, "Uploaded replay");
                StatusNotification::info(
                    format!(" \x1b]8;;{}\x1b\\{}\x1b]8;;\x1b\\ ", url, url),
                    Duration::from_secs(REPLAY_RESULT_NOTIFICATION_SECS),
                )
            }
            Err(e) => StatusNotification::error(
                format!(" {} ", e),
                Duration::from_secs(REPLAY_RATE_LIMIT_SECS),
            ),
        };
        let _ = tx.send((notif, session_update));
    });
    Some(rx)
}

pub struct PreloadedAppState {
    wasi_ctx: wasmtime_wasi::p1::WasiP1Ctx,
    peer_rx: tokio::sync::mpsc::Receiver<PeerMessageApp>,
    peer_tx: tokio::sync::mpsc::Sender<(Vec<PeerId>, Vec<u8>)>,
    app_id: AppId,
    app_runtime_session: AppRuntimeSession,
    shortname: String,
}

#[derive(Clone)]
pub struct AppContext {
    db: libsql::Connection,
    mesh: Mesh,
    app_env_secret_key: Arc<str>,
    remote_sshid: String,
    audio_enabled: bool,
    term: Option<String>,
    linker: Arc<wasmtime::Linker<AppState>>,
    app_registry: AppRuntimeRegistry,
    app_output_sender: tokio::sync::mpsc::Sender<BytesMut>,
    user_id: Option<u64>,
    locale: String,
    log_backend: Arc<dyn GuestLogBackend>,
}

pub struct AppState {
    app: PreloadedAppState,
    ctx: AppContext,
    conn_manager: ConnectionManager,
    menu_requests: Vec<Option<PendingMenuRequest>>,
    completed_menu_results: HashMap<usize, Result<Vec<u8>, String>>,
    menu_completed_rx: mpsc::Receiver<(usize, Result<Vec<u8>, String>)>,
    menu_request_tx: mpsc::Sender<MenuRequestJob>,
    menu_session: MenuSessionState,
    session_identity: SessionIdentity,
    limits: AppLimiter,
    next_app: Option<NextAppState>,
    has_next_app: Arc<AtomicBool>,
    input_receiver: tokio::sync::mpsc::Receiver<Bytes>,
    status_bar_input: StatusBarInput,
    graceful_shutdown_token: CancellationToken,
    session_io: Arc<SessionIo>,
    network_info: Arc<dyn NetworkInfo>,
    terminal_profile: TerminalProfile,
    audio: Option<Mixer>,
    terminal_snapshot: Arc<TerminalSnapshot>,
    change_app_limiter: Arc<ChangeAppLimiter>,
}

enum NextAppState {
    Pending(tokio::sync::oneshot::Receiver<NextAppPrepareResult>),
    Ready((PreloadedAppState, wasmtime::InstancePre<AppState>)),
    Failed(NextAppPrepareError),
}

type NextAppPrepareResult =
    Result<(PreloadedAppState, wasmtime::InstancePre<AppState>), NextAppPrepareError>;

#[derive(Default)]
struct ChangeAppLimiter {
    last_change_app_at: Mutex<Option<Instant>>,
}

impl ChangeAppLimiter {
    fn try_acquire(&self, min_interval: Duration) -> bool {
        let now = Instant::now();
        let mut last_change_app_at = self.last_change_app_at.lock().unwrap();
        if last_change_app_at
            .as_ref()
            .is_some_and(|last| now.saturating_duration_since(*last) < min_interval)
        {
            return false;
        }
        *last_change_app_at = Some(now);
        true
    }
}

#[derive(Debug, Clone, Copy)]
enum NextAppPrepareErrorCode {
    UnknownShortname,
    Other,
}

impl NextAppPrepareErrorCode {
    fn to_i32(self) -> i32 {
        match self {
            Self::UnknownShortname => NEXT_APP_READY_ERR_UNKNOWN_SHORTNAME,
            Self::Other => NEXT_APP_READY_ERR_PREPARE_FAILED_OTHER,
        }
    }
}

#[derive(Debug)]
struct NextAppPrepareError {
    code: NextAppPrepareErrorCode,
    message: String,
}

impl NextAppPrepareError {
    fn other(message: impl Into<String>) -> Self {
        Self {
            code: NextAppPrepareErrorCode::Other,
            message: message.into(),
        }
    }

    fn from_anyhow(error: anyhow::Error) -> Self {
        let code = if error.downcast_ref::<UnknownGameShortnameError>().is_some() {
            NextAppPrepareErrorCode::UnknownShortname
        } else {
            NextAppPrepareErrorCode::Other
        };
        Self {
            code,
            message: format!("{error:#}"),
        }
    }

    fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Debug)]
pub struct AppRunError {
    message: String,
}

impl AppRunError {
    fn new(message: String) -> Self {
        Self { message }
    }
}

impl std::fmt::Display for AppRunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for AppRunError {}

#[derive(Debug)]
struct UnknownGameShortnameError(String);

impl std::fmt::Display for UnknownGameShortnameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unknown app shortname: {}", self.0)
    }
}

impl std::error::Error for UnknownGameShortnameError {}

pub struct TerminalSnapshot {
    packed_dimensions: AtomicU64,
    packed_background: AtomicU32,
}

impl TerminalSnapshot {
    fn from_screen(screen: &headless_terminal::Screen) -> Self {
        let this = Self::new(0, 0, 0, 0, None);
        this.sync_from_screen(screen);
        this
    }

    fn new(
        width: u16,
        height: u16,
        cursor_x: u16,
        cursor_y: u16,
        background_rgb: Option<(u8, u8, u8)>,
    ) -> Self {
        Self {
            packed_dimensions: AtomicU64::new(Self::pack(width, height, cursor_x, cursor_y)),
            packed_background: AtomicU32::new(Self::pack_background(background_rgb)),
        }
    }

    fn size(&self) -> (u16, u16) {
        let (width, height, _, _) = Self::unpack(self.packed_dimensions.load(Ordering::Acquire));
        (width, height)
    }

    fn set(&self, width: u16, height: u16, x: u16, y: u16, background_rgb: Option<(u8, u8, u8)>) {
        self.packed_dimensions
            .store(Self::pack(width, height, x, y), Ordering::Release);
        self.packed_background
            .store(Self::pack_background(background_rgb), Ordering::Release);
    }

    fn cursor_position(&self) -> (u16, u16) {
        let (_, _, x, y) = Self::unpack(self.packed_dimensions.load(Ordering::Acquire));
        (x, y)
    }

    fn background_rgb(&self) -> Option<(u8, u8, u8)> {
        Self::unpack_background(self.packed_background.load(Ordering::Acquire))
    }

    fn sync_from_screen(&self, screen: &headless_terminal::Screen) {
        let (rows, cols) = screen.size();
        let (cursor_y, cursor_x) = screen.cursor_position();
        self.set(
            cols,
            rows,
            cursor_x,
            cursor_y,
            terminal_background_rgb(screen.terminal_background()),
        );
    }

    #[inline]
    fn pack(width: u16, height: u16, cursor_x: u16, cursor_y: u16) -> u64 {
        (width as u64)
            | ((height as u64) << 16)
            | ((cursor_x as u64) << 32)
            | ((cursor_y as u64) << 48)
    }

    #[inline]
    fn unpack(packed: u64) -> (u16, u16, u16, u16) {
        (
            packed as u16,
            (packed >> 16) as u16,
            (packed >> 32) as u16,
            (packed >> 48) as u16,
        )
    }

    #[inline]
    fn pack_background(background_rgb: Option<(u8, u8, u8)>) -> u32 {
        background_rgb.map_or(0, |(r, g, b)| {
            (1 << 24) | ((r as u32) << 16) | ((g as u32) << 8) | (b as u32)
        })
    }

    #[inline]
    fn unpack_background(packed: u32) -> Option<(u8, u8, u8)> {
        ((packed & (1 << 24)) != 0).then_some((
            (packed >> 16) as u8,
            (packed >> 8) as u8,
            packed as u8,
        ))
    }
}

fn terminal_background_rgb(color: Option<Color>) -> Option<(u8, u8, u8)> {
    match color {
        Some(Color::Rgb(r, g, b)) => Some((r, g, b)),
        _ => None,
    }
}

fn is_rgb_dark((r, g, b): (u8, u8, u8)) -> bool {
    let r = (r as f64) / 255.0;
    let g = (g as f64) / 255.0;
    let b = (b as f64) / 255.0;
    (0.2126 * r + 0.7152 * g + 0.0722 * b) < 0.5
}

pub struct ConnectionManager {
    connections: Vec<Option<Connection>>,
    pending_dials: Vec<Option<PendingDial>>,
    bandwidth: TokenBucket,
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self {
            connections: Vec::with_capacity(MAX_CONNECTIONS),
            pending_dials: Vec::new(),
            bandwidth: TokenBucket::new(MAX_OUTBOUND_BANDWIDTH, MAX_OUTBOUND_BANDWIDTH * 2),
        }
    }
}

impl ConnectionManager {
    fn active_connection_count(&self) -> usize {
        self.connections.iter().filter(|c| c.is_some()).count()
    }

    fn find_free_conn_slot(&self) -> Option<usize> {
        for (i, slot) in self.connections.iter().enumerate() {
            if slot.is_none() {
                return Some(i);
            }
        }
        if self.connections.len() < MAX_CONNECTIONS {
            return Some(self.connections.len());
        }
        None
    }
}

pub struct PendingDial {
    receiver: tokio::sync::oneshot::Receiver<Result<(Stream, SocketAddr, SocketAddr), i32>>,
}

pub struct PendingMenuRequest {
    state: MenuRequestState,
}

pub enum MenuRequestState {
    Pending,
    Complete(Result<Box<[u8]>, String>),
}

#[derive(Clone)]
pub struct SessionReplay {
    created_at: i64,
    app_id: u64,
    asciinema_url: String,
}

#[derive(serde::Deserialize, Default, Clone)]
struct LocalizedInput {
    #[serde(default)]
    by_lang: BTreeMap<String, String>,
    #[serde(flatten)]
    flat: BTreeMap<String, String>,
}

impl LocalizedInput {
    fn resolve(&self, locale: &str, fallback: &str) -> String {
        resolve_localized(self.languages(), locale, fallback)
    }

    fn languages(&self) -> &BTreeMap<String, String> {
        if !self.by_lang.is_empty() {
            &self.by_lang
        } else {
            &self.flat
        }
    }
}

fn preferred_locale_value(by_lang: &BTreeMap<String, String>, fallback: &str) -> String {
    if let Some(v) = by_lang.get("en") {
        return v.clone();
    }
    if let Some(v) = by_lang.values().next() {
        return v.clone();
    }
    fallback.to_string()
}

fn resolve_localized(by_lang: &BTreeMap<String, String>, locale: &str, fallback: &str) -> String {
    if let Some(v) = by_lang.get(locale) {
        return v.clone();
    }
    if let Some((base, _)) = locale.split_once('-') {
        if let Some(v) = by_lang.get(base) {
            return v.clone();
        }
    }
    preferred_locale_value(by_lang, fallback)
}

#[derive(serde::Deserialize, Default, Clone)]
struct GameDetailsOut {
    #[serde(default)]
    name: LocalizedInput,
}

#[derive(Clone)]
pub struct MenuSessionState {
    pub locale: String,
    pub replays: Vec<SessionReplay>,
}

#[derive(Clone)]
enum MenuRequestKind {
    AppsList { current_shortname: String },
    ProfileGet,
    ProfileSet { username: String, locale: String },
    ReplaysList { locale: String },
    ReplayDelete { created_at: i64 },
}

struct MenuRequestJob {
    request_id: usize,
    kind: MenuRequestKind,
}

#[derive(Default)]
struct MenuUpdate {
    username: Option<String>,
    locale: Option<String>,
    replays: Option<Vec<SessionReplay>>,
}

pub struct Connection {
    stream: Stream,
}

#[derive(Default)]
struct AppLimiter {
    memory_total: usize,
    table_total: usize,
}

impl wasmtime::ResourceLimiter for AppLimiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        tracing::trace!(current, desired, maximum, "memory growing");
        self.memory_total = self
            .memory_total
            .saturating_sub(current)
            .saturating_add(desired);
        if self.memory_total as u64 >= WASM_MEMORY_LIMIT_BYTES {
            tracing::error!(total = self.memory_total, "rejected memory grow");
            return Ok(false);
        }

        return Ok(true);
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        tracing::trace!(current, desired, maximum, "table growing");
        self.table_total = self
            .table_total
            .saturating_sub(current)
            .saturating_add(desired);
        if self.table_total >= 20_000 {
            tracing::error!(total = self.table_total, "rejected table grow");
            return Ok(false);
        }

        return Ok(true);
    }
}

struct EscapeSequenceBuffer {
    buffer: BytesMut,
}

impl EscapeSequenceBuffer {
    fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(64),
        }
    }

    const MAX_SCAN: usize = 256;

    /// Check if the sequence starting at `seq` (which begins with ESC) is complete.
    /// Returns the length of incomplete data, or 0 if complete.
    #[inline]
    fn check_escape_sequence(seq: &[u8]) -> usize {
        debug_assert!(!seq.is_empty() && seq[0] == 0x1b);

        if seq.len() == 1 {
            return 1;
        }

        match seq[1] {
            b'[' => {
                // CSI sequence: ESC [ (params) (intermediate) final
                if seq.len() == 2 {
                    return 2;
                }
                for i in 2..seq.len() {
                    let b = seq[i];
                    if b >= 0x40 && b <= 0x7E {
                        return 0; // Complete
                    }
                    if b < 0x20 || b > 0x3F {
                        return 0; // Malformed, treat as complete
                    }
                }
                seq.len()
            }
            b']' => {
                // OSC sequence: terminated by BEL or ST
                if seq.len() == 2 {
                    return 2;
                }
                for i in 2..seq.len() {
                    if seq[i] == 0x07 {
                        return 0;
                    }
                    if seq[i] == 0x1b && i + 1 < seq.len() && seq[i + 1] == b'\\' {
                        return 0;
                    }
                }
                seq.len()
            }
            b'P' | b'X' | b'^' | b'_' => {
                // DCS, SOS, PM, APC: terminated by ST
                if seq.len() == 2 {
                    return 2;
                }
                for i in 2..seq.len() {
                    if seq[i] == 0x1b && i + 1 < seq.len() && seq[i + 1] == b'\\' {
                        return 0;
                    }
                }
                seq.len()
            }
            b'(' | b')' | b'*' | b'+' | b'-' | b'.' | b'/' | b'#' | b' ' => {
                // 3-byte sequences
                if seq.len() < 3 { seq.len() } else { 0 }
            }
            _ => 0, // 2-byte sequences are complete
        }
    }

    #[inline]
    fn incomplete_escape_len(data: &[u8]) -> usize {
        if data.is_empty() {
            return 0;
        }

        let scan_start = data.len().saturating_sub(Self::MAX_SCAN);
        let tail = &data[scan_start..];

        let Some(rel_pos) = tail.iter().rposition(|&b| b == 0x1b) else {
            return 0;
        };

        let seq = &tail[rel_pos..];
        Self::check_escape_sequence(seq)
    }

    /// Add data to the buffer and return data that's safe to send
    #[inline]
    fn push(&mut self, mut data: BytesMut) -> BytesMut {
        // Fast path: buffer is empty, check if incoming data is complete
        if self.buffer.is_empty() {
            let incomplete_len = Self::incomplete_escape_len(&data);
            if incomplete_len == 0 {
                return data;
            }
            if incomplete_len == data.len() {
                self.buffer = data;
                return BytesMut::new();
            }
            let split_at = data.len() - incomplete_len;
            self.buffer = data.split_off(split_at);
            return data;
        }

        // Slow path: we have buffered data, need to combine
        self.buffer.extend_from_slice(&data);

        let incomplete_len = Self::incomplete_escape_len(&self.buffer);
        if incomplete_len == 0 {
            self.buffer.split()
        } else if incomplete_len < self.buffer.len() {
            let complete_len = self.buffer.len() - incomplete_len;
            self.buffer.split_to(complete_len)
        } else {
            BytesMut::new()
        }
    }
}

struct AsyncCallHook {}

#[async_trait::async_trait]
impl wasmtime::CallHookHandler<AppState> for AsyncCallHook {
    async fn handle_call_event(
        &self,
        _: wasmtime::StoreContextMut<'_, AppState>,
        ch: wasmtime::CallHook,
    ) -> wasmtime::Result<()> {
        if ch.entering_host() {
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

pub struct AsyncStdoutWriter {
    sender: tokio_util::sync::PollSender<BytesMut>,
    buffer: BytesMut,
}

impl AsyncStdoutWriter {
    pub fn new(sender: tokio::sync::mpsc::Sender<BytesMut>) -> Self {
        Self {
            sender: tokio_util::sync::PollSender::new(sender),
            buffer: BytesMut::with_capacity(16 * 1024),
        }
    }
}

impl tokio::io::AsyncWrite for AsyncStdoutWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.sender.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                let mut buffer = self.buffer.split();
                buffer.extend_from_slice(buf);
                let _ = self.sender.send_item(buffer);
                Poll::Ready(Ok(buf.len()))
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "channel closed",
            ))),
            Poll::Pending => {
                if self.buffer.len() > 16 * 1024 {
                    Poll::Pending
                } else {
                    self.buffer.extend_from_slice(buf);
                    Poll::Ready(Ok(buf.len()))
                }
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.buffer.is_empty() {
            return Poll::Ready(Ok(()));
        }

        match self.sender.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                let buffer = self.buffer.split();
                let _ = self.sender.send_item(buffer);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "channel closed",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // Poll::Ready(Ok(()))
        self.poll_flush(cx)
    }
}

struct MyStdoutStream {
    sender: tokio::sync::mpsc::Sender<BytesMut>,
}

impl wasmtime_wasi::cli::StdoutStream for MyStdoutStream {
    fn async_stream(&self) -> Box<dyn tokio::io::AsyncWrite + Send + Sync> {
        Box::new(AsyncStdoutWriter::new(self.sender.clone()))
    }
}

impl wasmtime_wasi::cli::IsTerminal for MyStdoutStream {
    fn is_terminal(&self) -> bool {
        true
    }
}

pub enum Stream {
    Tcp(tokio::net::TcpStream),
    Tls(tokio_rustls::client::TlsStream<tokio::net::TcpStream>),
}

impl Stream {
    pub fn try_write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Stream::Tcp(stream) => stream.try_write(buf),
            Stream::Tls(stream) => {
                use std::task::{Context, Poll, Waker};
                use tokio::io::AsyncWrite;

                struct NoOpWaker;
                impl std::task::Wake for NoOpWaker {
                    fn wake(self: Arc<Self>) {}
                }
                let waker = Waker::from(Arc::new(NoOpWaker));
                let mut cx = Context::from_waker(&waker);

                match Pin::new(stream).poll_write(&mut cx, buf) {
                    Poll::Ready(Ok(n)) => Ok(n),
                    Poll::Ready(Err(e)) => Err(e),
                    Poll::Pending => Err(std::io::Error::from(std::io::ErrorKind::WouldBlock)),
                }
            }
        }
    }

    pub fn try_read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Tcp(stream) => stream.try_read(buf),
            Stream::Tls(stream) => {
                use std::task::{Context, Poll, Waker};
                use tokio::io::AsyncRead;

                struct NoOpWaker;
                impl std::task::Wake for NoOpWaker {
                    fn wake(self: Arc<Self>) {}
                }
                let waker = Waker::from(Arc::new(NoOpWaker));
                let mut cx = Context::from_waker(&waker);

                let mut read_buf = tokio::io::ReadBuf::new(buf);

                match Pin::new(stream).poll_read(&mut cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => {
                        let filled = read_buf.filled().len();
                        if filled == 0 {
                            Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
                        } else {
                            Ok(filled)
                        }
                    }
                    Poll::Ready(Err(e)) => Err(e),
                    Poll::Pending => Err(std::io::Error::from(std::io::ErrorKind::WouldBlock)),
                }
            }
        }
    }
}

fn get_install_id() -> &'static str {
    static INSTALL_ID: OnceLock<String> = OnceLock::new();
    INSTALL_ID.get_or_init(|| uuid::Uuid::new_v4().to_string())
}

async fn save_replay(
    db: &libsql::Connection,
    mut menu_session: MenuSessionState,
    user_id: Option<u64>,
    app_id: AppId,
    username: &str,
    asciicast: &[u8],
) -> (Result<String, String>, Option<MenuSessionState>) {
    let url = match upload_asciicast(username, asciicast).await {
        Ok(u) => u,
        Err(e) => return (Err(e), None),
    };
    if let Some(uid) = user_id {
        let now = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs() as i64;
        let _ = db
            .execute(
                "INSERT INTO replays (asciinema_url, user_id, app_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                libsql::params!(url.as_str(), uid, app_id.app_id, now),
            )
            .await;
        (Ok(url), None)
    } else {
        let now = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs() as i64;
        menu_session.replays.push(SessionReplay {
            created_at: now,
            app_id: app_id.app_id,
            asciinema_url: url.clone(),
        });
        (Ok(url), Some(menu_session))
    }
}

async fn upload_asciicast(username: &str, data: &[u8]) -> Result<String, String> {
    let install_id = get_install_id();
    let client = reqwest::Client::new();

    let part = reqwest::multipart::Part::bytes(data.to_vec())
        .file_name("replay.cast")
        .mime_str("application/octet-stream")
        .map_err(|e| e.to_string())?;

    let form = reqwest::multipart::Form::new().part("asciicast", part);

    let response = client
        .post("https://asciinema.org/api/v1/recordings")
        .basic_auth(username, Some(install_id))
        .multipart(form)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("{}: {}", status, body));
    }

    #[derive(serde::Deserialize)]
    struct UploadResponse {
        url: String,
    }

    let upload_response: UploadResponse = response.json().await.map_err(|e| e.to_string())?;
    Ok(upload_response.url)
}

pub fn is_globally_reachable(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(addr) => !IPV4_NON_GLOBAL
            .get_or_init(init_ipv4_non_global)
            .iter()
            .any(|net| net.contains(&addr)),
        IpAddr::V6(addr) => !IPV6_NON_GLOBAL
            .get_or_init(init_ipv6_non_global)
            .iter()
            .any(|net| net.contains(&addr)),
    }
}

static IPV4_NON_GLOBAL: OnceLock<Vec<Ipv4Net>> = OnceLock::new();
static IPV6_NON_GLOBAL: OnceLock<Vec<Ipv6Net>> = OnceLock::new();

fn init_ipv4_non_global() -> Vec<Ipv4Net> {
    [
        // https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml
        "192.88.99.0/24",     // Deprecated (6to4 Relay Anycast)
        "0.0.0.0/8",          // "This network"
        "0.0.0.0/32",         // "This host on this network"
        "10.0.0.0/8",         // Private-Use
        "100.64.0.0/10",      // Shared Address Space
        "169.254.0.0/16",     // Link Local
        "172.16.0.0/12",      // Private-Use
        "192.0.0.0/24",       // IETF Protocol Assignments
        "192.0.0.0/29",       // IPv4 Service Continuity Prefix
        "192.0.0.8/32",       // IPv4 dummy address
        "192.0.0.170/32",     // NAT64/DNS64 Discovery
        "192.0.0.171/32",     // NAT64/DNS64 Discovery
        "192.0.2.0/24",       // Documentation (TEST-NET-1)
        "192.88.99.2/32",     // 6a44-relay anycast address
        "192.168.0.0/16",     // Private-Use
        "198.18.0.0/15",      // Benchmarking
        "198.51.100.0/24",    // Documentation (TEST-NET-2)
        "203.0.113.0/24",     // Documentation (TEST-NET-3)
        "240.0.0.0/4",        // Reserved
        "255.255.255.255/32", // Limited Broadcast
        "127.0.0.0/8",        // Loopback
        "224.0.0.0/4",        // Multicast
    ]
    .into_iter()
    .map(|s| s.parse().expect("valid IPv4 CIDR"))
    .collect()
}

fn init_ipv6_non_global() -> Vec<Ipv6Net> {
    [
        // https://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry.xhtml
        "::1/128",        // Loopback Address
        "::/128",         // Unspecified Address
        "::ffff:0:0/96",  // IPv4-mapped Address
        "64:ff9b:1::/48", // IPv4-IPv6 Translat.
        "100::/64",       // Discard-Only Address Block
        "100:0:0:1::/64", // Dummy IPv6 Prefix
        "2001:2::/48",    // Benchmarking
        "2001:db8::/32",  // 2001:db8::/32
        "3fff::/20",      // Documentation
        "5f00::/16",      // Segment Routing (SRv6) SIDs
        "fe80::/10",      // Link-Local Unicast
        "2001::/23",      // IETF Protocol Assignments
        "fc00::/7",       // Unique-Local
        "2001::/32",      // TEREDO
        "2002::/16",      // 6to4
        "ff00::/8",       // Multicast
    ]
    .into_iter()
    .map(|s| s.parse().expect("valid IPv6 CIDR"))
    .collect()
}
