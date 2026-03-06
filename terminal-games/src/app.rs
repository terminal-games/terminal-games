// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::{BTreeMap, HashMap},
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, UNIX_EPOCH},
    u64,
};

use futures::stream::{FuturesUnordered, StreamExt};
use hex::ToHex;
use ipnet::{Ipv4Net, Ipv6Net};
use rustls_platform_verifier::BuilderVerifierExt;
use smallvec::SmallVec;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use wasmtime::InstanceAllocationStrategy;
use wasmtime_wasi::I32Exit;

use crate::{
    audio::{CHANNELS, FRAME_SIZE, Mixer, SAMPLE_RATE},
    mesh::{AppId, Mesh, PeerId, PeerMessageApp, RegionId},
    rate_limiting::{NetworkInfo, TokenBucket},
    replay::ReplayBuffer,
    status_bar::StatusBar,
    terminal_profile::TerminalProfile,
};

/// Maximum number of concurrent connections per app instance
const MAX_CONNECTIONS: usize = 8;

/// Maximum outbound bandwidth in bytes per second (20 kBps)
const MAX_OUTBOUND_BANDWIDTH: u64 = 20 * 1024;

const REPLAY_RATE_LIMIT_SECS: u64 = 10;

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

const PEER_RECV_ERR_CHANNEL_DISCONNECTED: i32 = -1;

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

const NEXT_APP_READY_READY: i32 = 1;
const NEXT_APP_READY_NOT_READY: i32 = 0;
const NEXT_APP_READY_ERR_UNKNOWN_SHORTNAME: i32 = -1;
const NEXT_APP_READY_ERR_PREPARE_FAILED_OTHER: i32 = -2;

pub struct AppServer {
    linker: Arc<wasmtime::Linker<AppState>>,
    engine: wasmtime::Engine,
    pub db: libsql::Connection,
    mesh: Mesh,
    module_cache: Arc<tokio::sync::RwLock<HashMap<u64, Arc<wasmtime::Module>>>>,
}

pub struct AppInstantiationParams {
    pub input_receiver: tokio::sync::mpsc::Receiver<SmallVec<[u8; 16]>>,
    pub replay_request_receiver: tokio::sync::mpsc::Receiver<()>,
    pub output_sender: tokio::sync::mpsc::Sender<Arc<Vec<u8>>>,
    pub audio_sender: Option<tokio::sync::mpsc::Sender<Vec<u8>>>,
    pub window_size_receiver: tokio::sync::watch::Receiver<(u16, u16)>,
    pub graceful_shutdown_token: CancellationToken,
    pub username: String,
    pub remote_sshid: String,
    pub term: Option<String>,
    pub args: Option<Vec<u8>>,
    pub network_info: Arc<dyn NetworkInfo>,
    pub terminal_profile: TerminalProfile,
    pub first_app_shortname: String,
    pub user_id: Option<u64>,
    pub locale: String,
}

impl AppServer {
    pub fn new(mesh: Mesh, db: libsql::Connection) -> anyhow::Result<Self> {
        let mut config = wasmtime::Config::new();
        let cache_config = wasmtime::CacheConfig::new();
        config.cache(Some(wasmtime::Cache::new(cache_config)?));
        config.allocation_strategy(InstanceAllocationStrategy::pooling());
        config.compiler_inlining(true);
        let engine = wasmtime::Engine::new(&config)?;

        let mut linker = wasmtime::Linker::<AppState>::new(&engine);
        wasmtime_wasi::p1::add_to_linker_async(&mut linker, |t| &mut t.app.wasi_ctx)?;

        linker.func_wrap("terminal_games", "dial", Self::host_dial)?;
        linker.func_wrap("terminal_games", "poll_dial", Self::poll_dial)?;
        linker.func_wrap("terminal_games", "conn_close", Self::conn_close)?;
        linker.func_wrap("terminal_games", "conn_write", Self::conn_write)?;
        linker.func_wrap("terminal_games", "conn_read", Self::conn_read)?;
        linker.func_wrap("terminal_games", "terminal_read", Self::terminal_read)?;
        linker.func_wrap("terminal_games", "terminal_size", Self::terminal_size)?;
        linker.func_wrap("terminal_games", "terminal_cursor", Self::terminal_cursor)?;
        linker.func_wrap("terminal_games", "change_app", Self::change_app)?;
        linker.func_wrap("terminal_games", "next_app_ready", Self::next_app_ready)?;
        linker.func_wrap("terminal_games", "peer_send", Self::peer_send)?;
        linker.func_wrap("terminal_games", "peer_recv", Self::peer_recv)?;
        linker.func_wrap_async("terminal_games", "region_latency", Self::region_latency)?;
        linker.func_wrap_async("terminal_games", "peer_list", Self::peer_list)?;
        linker.func_wrap(
            "terminal_games",
            "graceful_shutdown_poll",
            Self::graceful_shutdown_poll,
        )?;
        linker.func_wrap("terminal_games", "network_info", Self::host_network_info)?;
        linker.func_wrap("terminal_games", "terminal_info", Self::host_terminal_info)?;
        linker.func_wrap("terminal_games", "audio_write", Self::audio_write)?;
        linker.func_wrap("terminal_games", "audio_info", Self::audio_info)?;
        linker.func_wrap(
            "terminal_games",
            "menu_games_list_start",
            Self::menu_games_list_start,
        )?;
        linker.func_wrap(
            "terminal_games",
            "menu_profile_get_start",
            Self::menu_profile_get_start,
        )?;
        linker.func_wrap(
            "terminal_games",
            "menu_profile_set_start",
            Self::menu_profile_set_start,
        )?;
        linker.func_wrap(
            "terminal_games",
            "menu_replays_list_start",
            Self::menu_replays_list_start,
        )?;
        linker.func_wrap(
            "terminal_games",
            "menu_replay_delete_start",
            Self::menu_replay_delete_start,
        )?;
        linker.func_wrap("terminal_games", "menu_poll", Self::menu_poll)?;

        Ok(Self {
            linker: Arc::new(linker),
            mesh,
            db,
            engine,
            module_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        })
    }

    /// Run an app with IO defined in [`params`] in the background
    pub fn instantiate_app(
        &self,
        params: AppInstantiationParams,
    ) -> tokio::sync::oneshot::Receiver<I32Exit> {
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();

        let mesh = self.mesh.clone();
        let db = self.db.clone();
        let engine = self.engine.clone();
        let linker = self.linker.clone();
        let module_cache = self.module_cache.clone();

        tokio::task::spawn(async move {
            let mut input_receiver = params.input_receiver;
            let mut replay_request_receiver = params.replay_request_receiver;
            let mut window_size_receiver = params.window_size_receiver;
            let output_sender = params.output_sender;
            let network_info = params.network_info.clone();
            let terminal_profile = params.terminal_profile;
            let term = params.term.clone();
            let user_id = params.user_id;
            let first_app_shortname = params.first_app_shortname;
            let mut graceful_shutdown_token = params.graceful_shutdown_token;

            let (first_cols, first_rows) = *window_size_receiver.borrow();
            let mut terminal = headless_terminal::Parser::new(first_rows, first_cols, 0);
            let mut terminal_snapshot =
                Arc::new(TerminalSnapshot::new(first_cols, first_rows, 0, 0));
            let hard_shutdown_token = CancellationToken::new();

            let (app_output_sender, mut app_output_receiver) =
                tokio::sync::mpsc::channel::<Vec<u8>>(1);
            let has_next_app_shortname = Arc::new(AtomicBool::new(false));
            let (notification_tx, notification_rx) = tokio::sync::mpsc::channel(1);

            let audio_enabled = params.audio_sender.is_some();
            let mut audio = if let Some(audio_tx) = params.audio_sender {
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

            let mut exit_code = I32Exit(0);
            let (menu_future_tx, mut menu_future_rx) = mpsc::channel(MAX_MENU_REQUESTS);
            let mut menu_session = MenuSessionState {
                locale: params.locale.clone(),
                replays: Vec::new(),
            };
            let mut menu_username = params.username;
            let mut ctx = AppContext {
                db: db.clone(),
                linker,
                mesh,
                module_cache,
                app_output_sender,
                remote_sshid: params.remote_sshid,
                term,
                audio_enabled,
                user_id,
                locale: params.locale,
                menu_session: menu_session.clone(),
                menu_username: menu_username.clone(),
                menu_future_tx: menu_future_tx.clone(),
            };
            let (mut app, mut instance_pre) =
                match Self::prepare_instantiate(&ctx, first_app_shortname).await {
                    Ok(v) => v,
                    Err(_) => {
                        let _ = exit_tx.send(I32Exit(1));
                        return;
                    }
                };

            let mut replay_buffer = ReplayBuffer::new(
                first_cols,
                first_rows,
                &app.shortname,
                app.app_id,
                ctx.term.as_deref(),
            );
            let mut status_bar = StatusBar::new(
                app.shortname.clone(),
                &menu_username,
                network_info.clone(),
                terminal_profile,
                notification_rx,
            );
            let mut escape_buffer = EscapeSequenceBuffer::new();
            let mut status_bar_interval = tokio::time::interval(Duration::from_secs(1));
            let mut graceful_shutdown_timeout: Option<Pin<Box<tokio::time::Sleep>>> = None;
            let mut replay_last_at = Duration::ZERO;
            let mut replay_requests_open = true;
            let mut pending_replay_upload: Option<
                Pin<Box<dyn Future<Output = (String, Option<MenuSessionState>)> + Send>>,
            > = None;
            let mut menu_futures: FuturesUnordered<MenuFutureWrapper> = FuturesUnordered::new();
            let completed_menu_results: Arc<
                std::sync::Mutex<HashMap<usize, Result<Vec<u8>, String>>>,
            > = Arc::new(std::sync::Mutex::new(HashMap::new()));

            loop {
                let state = AppState {
                    app,
                    ctx,
                    conn_manager: ConnectionManager::default(),
                    menu_requests: Vec::with_capacity(MAX_MENU_REQUESTS),
                    completed_menu_results: completed_menu_results.clone(),
                    limits: AppLimiter::default(),
                    next_app: None,
                    input_receiver,
                    has_next_app: has_next_app_shortname.clone(),
                    graceful_shutdown_token: graceful_shutdown_token.clone(),
                    network_info: network_info.clone(),
                    terminal_profile,
                    audio,
                    terminal_snapshot: terminal_snapshot.clone(),
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

                            result = &mut call_future => {
                                break 'block Some(result);
                            }

                            data = app_output_receiver.recv() => {
                                let Some(mut data) = data else { continue };
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
                                let (height, width) = screen.size();
                                let dirty = screen.take_damaged_rows();
                                if dirty.contains(&(height - 1)) {
                                    status_bar.maybe_render_into(screen, &mut output, true);
                                }
                                let (cursor_y, cursor_x) = screen.cursor_position();
                                terminal_snapshot.set(width, height, cursor_x, cursor_y);

                                if !output.is_empty() {
                                    let output = Arc::new(output);
                                    replay_buffer.push_output(output.clone());
                                    if output_sender.send(output).await.is_err() {
                                        break 'block None;
                                    }
                                }
                            }

                            result = window_size_receiver.changed() => {
                                if result.is_err() {
                                    break 'block None;
                                }
                                let (width, height) = *window_size_receiver.borrow();
                                replay_buffer.push_resize(width, height);

                                let mut output = Vec::new();
                                let screen = terminal.screen_mut();
                                screen.set_size(height, width);
                                status_bar.maybe_render_into(screen, &mut output, true);
                                let (cursor_y, cursor_x) = screen.cursor_position();
                                terminal_snapshot.set(width, height, cursor_x, cursor_y);
                                if !output.is_empty() {
                                    let output = Arc::new(output);
                                    replay_buffer.push_output(output.clone());
                                    if output_sender.send(output).await.is_err() {
                                        break 'block None;
                                    }
                                }
                            }

                            _ = status_bar_interval.tick() => {
                                let mut output = Vec::new();
                                status_bar.maybe_render_into(terminal.screen(), &mut output, false);
                                if !output.is_empty() {
                                    let output = Arc::new(output);
                                    replay_buffer.push_output(output.clone());
                                    if output_sender.send(output).await.is_err() {
                                        break 'block None;
                                    }
                                }
                            }

                            msg = menu_future_rx.recv() => {
                                if let Some((request_id, future)) = msg {
                                    menu_futures.push(MenuFutureWrapper { request_id, future });
                                }
                            }

                            completion = menu_futures.next(), if !menu_futures.is_empty() => {
                                if let Some((request_id, result, update)) = completion {
                                    completed_menu_results.lock().unwrap().insert(request_id, result);
                                    if let Some(username) = update.username {
                                        menu_username = username;
                                        status_bar.set_username(&menu_username);
                                    }
                                    if let Some(locale) = update.locale {
                                        menu_session.locale = locale;
                                    }
                                    if let Some(replays) = update.replays {
                                        menu_session.replays = replays;
                                    }
                                    let mut output = Vec::new();
                                    status_bar.maybe_render_into(terminal.screen(), &mut output, true);
                                    if !output.is_empty() {
                                        let output = Arc::new(output);
                                        replay_buffer.push_output(output.clone());
                                        if output_sender.send(output).await.is_err() {
                                            break 'block None;
                                        }
                                    }
                                }
                            }

                            replay_req = replay_request_receiver.recv(), if replay_requests_open => {
                                if replay_req.is_none() {
                                    replay_requests_open = false;
                                    continue;
                                }
                                let now = std::time::SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or(Duration::ZERO);
                                if now.saturating_sub(replay_last_at) < Duration::from_secs(REPLAY_RATE_LIMIT_SECS) {
                                    let _ = notification_tx.try_send(" Please wait 10 seconds between replays. ".to_string());
                                    continue;
                                }

                                replay_last_at = now;
                                let _ = notification_tx.try_send(" \x1b[3mUploading replay... ".to_string());
                                let (initial_app_id, asciicast) = match replay_buffer.serialize_asciicast() {
                                    Ok(v) => v,
                                    Err(e) => {
                                        tracing::error!(error = %e, "Failed to serialize replay");
                                        let _ = notification_tx.try_send(" Failed to serialize replay. ".to_string());
                                        continue;
                                    }
                                };
                                let db = db.clone();
                                let menu_session = menu_session.clone();
                                let username = menu_username.clone();
                                pending_replay_upload = Some(Box::pin(async move {
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
                                            format!(
                                                " \x1b[3mReplay saved: \x1b]8;;{}\x1b\\{}\x1b]8;;\x1b\\ ",
                                                url, url
                                            )
                                        }
                                        Err(e) => format!(" {} ", e),
                                    };
                                    (notif, session_update)
                                }));
                            }

                            _ = graceful_shutdown_token.cancelled(), if graceful_shutdown_timeout.is_none() => {
                                graceful_shutdown_timeout = Some(Box::pin(tokio::time::sleep(Duration::from_millis(5000))));
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
                                let (notif, session_update) = notification;
                                if let Some(session) = session_update {
                                    menu_session = session;
                                }
                                let _ = notification_tx.try_send(notif);
                            }
                        }
                    }
                };

                if let Some(Err(err)) = call_result {
                    match err.downcast::<wasmtime_wasi::I32Exit>() {
                        Ok(code) => {
                            exit_code = code;
                        }
                        Err(other) => {
                            tracing::error!(error = %other, "Module errored");
                        }
                    }
                }

                let mut state = store.into_data();
                graceful_shutdown_token = state.graceful_shutdown_token;
                input_receiver = state.input_receiver;
                terminal_snapshot = state.terminal_snapshot;
                audio = state.audio;
                ctx = state.ctx;
                ctx.menu_session = menu_session.clone();
                ctx.menu_username = menu_username.clone();

                if let Some(next_app) = state.next_app.take() {
                    let next_app = match next_app {
                        NextAppState::Pending(next_app) => next_app
                            .await
                            .unwrap_or(Err(NextAppPrepareErrorCode::Other)),
                        NextAppState::Ready(next_app) => Ok(next_app),
                        NextAppState::Failed(_) => break,
                    };
                    let Ok((next_app, next_instance_pre)) = next_app else {
                        break;
                    };

                    let shortname = next_app.shortname.clone();
                    replay_buffer.push_app_switch(next_app.app_id, &shortname);
                    status_bar.shortname = shortname;
                    app = next_app;
                    instance_pre = next_instance_pre;
                    continue;
                }

                break;
            }

            hard_shutdown_token.cancel();
            let _ = exit_tx.send(exit_code);
        });

        exit_rx
    }

    async fn prepare_instantiate(
        ctx: &AppContext,
        shortname: String,
    ) -> anyhow::Result<(PreloadedAppState, wasmtime::InstancePre<AppState>)> {
        let mut app_id_rows = ctx
            .db
            .query(
                "SELECT id FROM games WHERE shortname = ?1",
                [shortname.as_str()],
            )
            .await?;
        let app_id_row = app_id_rows
            .next()
            .await?
            .ok_or_else(|| UnknownGameShortnameError(shortname.clone()))?;
        let app_id = app_id_row.get::<u64>(0)?;

        let (peer_id, peer_rx, peer_tx) = ctx.mesh.new_peer(AppId(app_id)).await;

        let mut envs = vec![];
        let mut rows = ctx
            .db
            .query("SELECT name, value FROM envs WHERE game_id = ?1", [app_id])
            .await?;
        while let Some(row) = rows.next().await? {
            let name = row.get(0)?;
            let value = row.get(1)?;
            envs.push((name, value));
        }
        envs.push(("REMOTE_SSHID".to_string(), ctx.remote_sshid.clone()));
        let username = ctx.menu_username.clone();
        let mut locale = ctx.locale.clone();
        if !ctx.menu_session.locale.is_empty() {
            locale = ctx.menu_session.locale.clone();
        }
        envs.push(("USERNAME".to_string(), username));
        envs.push(("PEER_ID".to_string(), peer_id.to_bytes().encode_hex()));
        envs.push(("APP_SHORTNAME".to_string(), shortname.to_string()));
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
        if let Some(user_id) = ctx.user_id {
            if let Ok(mut rows) = ctx
                .db
                .query(
                    "SELECT locale FROM users WHERE id = ?1 LIMIT 1",
                    libsql::params!(user_id),
                )
                .await
            {
                if let Ok(Some(row)) = rows.next().await {
                    locale = row.get::<String>(0).unwrap_or(locale);
                }
            }
        }
        envs.push(("LOCALE".to_string(), locale.clone()));

        let wasi_ctx = wasmtime_wasi::WasiCtx::builder()
            .stdout(MyStdoutStream {
                sender: ctx.app_output_sender.clone(),
            })
            .envs(&envs)
            .build_p1();

        let mut rows = ctx
            .db
            .query(
                "SELECT path FROM games WHERE id = ?1",
                libsql::params![app_id],
            )
            .await?;

        let row = rows
            .next()
            .await?
            .ok_or_else(|| anyhow::anyhow!("Missing wasm path for game id {app_id}"))?;
        let wasm_path: String = row.get(0)?;
        let module = Self::load_or_compile_module(ctx, app_id, &wasm_path).await?;
        let instance_pre = ctx.linker.instantiate_pre(&module)?;

        Ok((
            PreloadedAppState {
                wasi_ctx,
                peer_rx,
                peer_tx,
                app_id: AppId(app_id),
                shortname,
            },
            instance_pre,
        ))
    }

    async fn load_or_compile_module(
        ctx: &AppContext,
        app_id: u64,
        wasm_path: &str,
    ) -> anyhow::Result<Arc<wasmtime::Module>> {
        {
            let cache = ctx.module_cache.read().await;
            if let Some(module) = cache.get(&app_id) {
                return Ok(module.clone());
            }
        }

        let module = Arc::new(wasmtime::Module::from_file(ctx.linker.engine(), wasm_path)?);
        let mut cache = ctx.module_cache.write().await;
        let entry = cache.entry(app_id).or_insert_with(|| module.clone());
        Ok(entry.clone())
    }

    fn host_dial(
        mut caller: wasmtime::Caller<'_, AppState>,
        address_ptr: i32,
        address_len: u32,
        mode: u32,
    ) -> wasmtime::Result<i32> {
        let mut buf = [0u8; 256];
        if address_len as usize >= buf.len() {
            return Ok(DIAL_ERR_ADDRESS_TOO_LONG);
        }
        let len = address_len as usize;
        let offset = address_ptr as usize;

        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("dial: failed to find host memory");
        };

        mem.read(&caller, offset, &mut buf[..len])?;

        if caller.data().conn_manager.active_connection_count() >= MAX_CONNECTIONS {
            return Ok(DIAL_ERR_TOO_MANY_CONNECTIONS);
        }

        let address = String::from_utf8_lossy(&buf[..len]).to_string();

        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let result = Self::do_connect(address, mode).await;
            let _ = tx.send(result);
        });

        let pending = PendingDial { receiver: rx };
        let pending_dials = &mut caller.data_mut().conn_manager.pending_dials;

        for (i, slot) in pending_dials.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = Some(pending);
                return Ok(i as i32);
            }
        }
        let slot_id = pending_dials.len() as i32;
        pending_dials.push(Some(pending));
        Ok(slot_id)
    }

    async fn do_connect(
        address: String,
        mode: u32,
    ) -> Result<(Stream, SocketAddr, SocketAddr), i32> {
        let addrs: Vec<SocketAddr> = match tokio::net::lookup_host(&address).await {
            Ok(iter) => iter.collect(),
            Err(e) => {
                tracing::error!("do_connect: DNS resolution failed: {:?}", e);
                return Err(POLL_DIAL_ERR_DNS_RESOLUTION);
            }
        };

        if addrs.is_empty() {
            return Err(POLL_DIAL_ERR_DNS_RESOLUTION);
        }

        if !addrs.iter().all(|addr| is_globally_reachable(addr.ip())) {
            return Err(POLL_DIAL_ERR_NOT_GLOBALLY_REACHABLE);
        }

        let tcp_stream = match tokio::net::TcpStream::connect(addrs.as_slice()).await {
            Ok(stream) => stream,
            Err(e) => {
                tracing::error!("do_connect: TcpStream::connect failed: {:?}", e);
                return Err(POLL_DIAL_ERR_CONNECTION_FAILED);
            }
        };

        let local_addr = tcp_stream.local_addr().unwrap_or_else(|_| {
            SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0)
        });
        let remote_addr = tcp_stream.peer_addr().unwrap_or(addrs[0]);

        let stream = if mode == 1 {
            let hostname: String = address.split(':').next().unwrap_or(&address).to_string();

            let mut config =
                match tokio_rustls::rustls::ClientConfig::builder().with_platform_verifier() {
                    Ok(builder) => builder.with_no_client_auth(),
                    Err(e) => {
                        tracing::error!("do_connect: failed to initialize TLS verifier: {:?}", e);
                        return Err(POLL_DIAL_ERR_TLS_HANDSHAKE);
                    }
                };
            config.alpn_protocols.push(b"h2".to_vec());
            let connector = tokio_rustls::TlsConnector::from(Arc::new(config));

            let dnsname = match tokio_rustls::rustls::pki_types::ServerName::try_from(hostname) {
                Ok(name) => name,
                Err(_) => {
                    tracing::error!("do_connect: invalid DNS name");
                    return Err(POLL_DIAL_ERR_INVALID_DNS_NAME);
                }
            };

            let tls_stream = match connector.connect(dnsname, tcp_stream).await {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::error!("do_connect: TLS handshake failed: {:?}", e);
                    return Err(POLL_DIAL_ERR_TLS_HANDSHAKE);
                }
            };

            Stream::Tls(tls_stream)
        } else {
            Stream::Tcp(tcp_stream)
        };

        Ok((stream, local_addr, remote_addr))
    }

    fn poll_dial(
        mut caller: wasmtime::Caller<'_, AppState>,
        dial_id: i32,
        local_addr_ptr: i32,
        local_addr_len_ptr: i32,
        remote_addr_ptr: i32,
        remote_addr_len_ptr: i32,
    ) -> wasmtime::Result<i32> {
        let dial_id_usize = dial_id as usize;

        {
            let conn_manager = &caller.data().conn_manager;
            if dial_id_usize >= conn_manager.pending_dials.len() {
                return Ok(POLL_DIAL_ERR_INVALID_DIAL_ID);
            }
            if conn_manager.pending_dials[dial_id_usize].is_none() {
                return Ok(POLL_DIAL_ERR_INVALID_DIAL_ID);
            }
        }

        let mut pending = caller.data_mut().conn_manager.pending_dials[dial_id_usize]
            .take()
            .ok_or_else(|| wasmtime::Error::msg("pending dial unexpectedly missing"))?;

        match pending.receiver.try_recv() {
            Ok(Ok((stream, local_addr, remote_addr))) => {
                let conn_manager = &mut caller.data_mut().conn_manager;
                let slot = conn_manager.find_free_conn_slot();

                if slot.is_none() {
                    return Ok(POLL_DIAL_ERR_TOO_MANY_CONNECTIONS);
                }

                let Some(slot_id) = slot else {
                    return Ok(POLL_DIAL_ERR_TOO_MANY_CONNECTIONS);
                };
                let conn = Connection { stream };

                if slot_id < conn_manager.connections.len() {
                    conn_manager.connections[slot_id] = Some(conn);
                } else {
                    conn_manager.connections.push(Some(conn));
                }

                let local_addr_str = local_addr.to_string();
                let remote_addr_str = remote_addr.to_string();

                let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                    wasmtime::bail!("failed to find host memory");
                };

                mem.write(
                    &mut caller,
                    local_addr_ptr as usize,
                    local_addr_str.as_bytes(),
                )?;
                mem.write(
                    &mut caller,
                    local_addr_len_ptr as usize,
                    &(local_addr_str.len() as u32).to_le_bytes(),
                )?;
                mem.write(
                    &mut caller,
                    remote_addr_ptr as usize,
                    remote_addr_str.as_bytes(),
                )?;
                mem.write(
                    &mut caller,
                    remote_addr_len_ptr as usize,
                    &(remote_addr_str.len() as u32).to_le_bytes(),
                )?;

                tracing::info!(conn_id = slot_id, %local_addr, %remote_addr, "dial completed");
                Ok(slot_id as i32)
            }
            Ok(Err(error_code)) => Ok(error_code),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                caller.data_mut().conn_manager.pending_dials[dial_id_usize] = Some(pending);
                Ok(POLL_DIAL_PENDING)
            }
            Err(tokio::sync::oneshot::error::TryRecvError::Closed) => Ok(POLL_DIAL_ERR_TASK_FAILED),
        }
    }

    fn conn_close(
        mut caller: wasmtime::Caller<'_, AppState>,
        conn_id: i32,
    ) -> wasmtime::Result<i32> {
        let conn_id_usize = conn_id as usize;
        let connections = &mut caller.data_mut().conn_manager.connections;

        if conn_id_usize >= connections.len() || connections[conn_id_usize].is_none() {
            return Ok(CONN_ERR_INVALID_CONN_ID);
        }

        connections[conn_id_usize] = None;
        Ok(0)
    }

    fn conn_write(
        mut caller: wasmtime::Caller<'_, AppState>,
        conn_id: i32,
        data_ptr: i32,
        data_len: u32,
    ) -> wasmtime::Result<i32> {
        let mut buf = [0u8; 4 * 1024];
        let len = (data_len as usize).min(buf.len());
        let offset = data_ptr as usize;

        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("failed to find host memory");
        };

        mem.read(&caller, offset, &mut buf[..len])?;

        let conn_manager = &mut caller.data_mut().conn_manager;

        let conn_id_usize = conn_id as usize;
        if conn_id_usize >= conn_manager.connections.len() {
            return Ok(CONN_ERR_INVALID_CONN_ID);
        }

        let Some(conn) = conn_manager.connections[conn_id_usize].as_mut() else {
            return Ok(CONN_ERR_INVALID_CONN_ID);
        };

        if conn_manager.bandwidth.tokens() < len {
            return Ok(0);
        }
        conn_manager.bandwidth.consume(len);

        match conn.stream.try_write(&buf[..len]) {
            Ok(n) => Ok(n as i32),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(0),
            Err(e) => {
                tracing::error!("conn_write: stream write failed: {:?}", e);
                Ok(CONN_ERR_CONNECTION_ERROR)
            }
        }
    }

    fn conn_read(
        mut caller: wasmtime::Caller<'_, AppState>,
        conn_id: i32,
        ptr: i32,
        len: u32,
    ) -> wasmtime::Result<i32> {
        let conn_id_usize = conn_id as usize;

        {
            let connections = &caller.data().conn_manager.connections;
            if conn_id_usize >= connections.len() {
                return Ok(CONN_ERR_INVALID_CONN_ID);
            }
            if connections[conn_id_usize].is_none() {
                return Ok(CONN_ERR_INVALID_CONN_ID);
            };
        }

        let mut buf = [0u8; 4 * 1024];
        let read_len = (len as usize).min(buf.len());

        let n = {
            let connections = &mut caller.data_mut().conn_manager.connections;
            let Some(conn) = connections[conn_id_usize].as_mut() else {
                return Ok(CONN_ERR_INVALID_CONN_ID);
            };

            match conn.stream.try_read(&mut buf[..read_len]) {
                Ok(0) => return Ok(CONN_ERR_CONNECTION_ERROR),
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(0),
                Err(e) => {
                    tracing::error!("conn_read: stream read error: {:?}", e);
                    return Ok(CONN_ERR_CONNECTION_ERROR);
                }
            }
        };

        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("failed to find host memory");
        };

        mem.write(&mut caller, ptr as usize, &buf[..n])?;

        Ok(n as i32)
    }

    fn terminal_read(
        mut caller: wasmtime::Caller<'_, AppState>,
        ptr: i32,
        _len: u32,
    ) -> wasmtime::Result<i32> {
        match caller.data_mut().input_receiver.try_recv() {
            Ok(buf) => {
                if buf.len() > 4096 {
                    return Ok(0);
                }
                let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                    wasmtime::bail!("terminal_read: failed to find host memory");
                };
                let offset = ptr as u32 as usize;
                mem.write(&mut caller, offset, buf.as_ref())?;
                Ok(buf.len() as i32)
            }
            Err(_) => Ok(0),
        }
    }

    fn terminal_size(
        mut caller: wasmtime::Caller<'_, AppState>,
        width_ptr: i32,
        height_ptr: i32,
    ) -> wasmtime::Result<()> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("terminal_size: failed to find host memory");
        };
        let (width, height) = caller.data().terminal_snapshot.size();
        let effective_height = if height > 0 { height - 1 } else { 0 };

        let width_offset = width_ptr as u32 as usize;
        mem.write(&mut caller, width_offset, &width.to_le_bytes())?;

        let height_offset = height_ptr as u32 as usize;
        mem.write(&mut caller, height_offset, &effective_height.to_le_bytes())?;

        Ok(())
    }

    fn terminal_cursor(
        mut caller: wasmtime::Caller<'_, AppState>,
        x_ptr: i32,
        y_ptr: i32,
    ) -> wasmtime::Result<()> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("terminal_cursor: failed to find host memory");
        };
        let (x, y) = caller.data().terminal_snapshot.cursor_position();

        let x_offset = x_ptr as u32 as usize;
        mem.write(&mut caller, x_offset, &x.to_le_bytes())?;

        let y_offset = y_ptr as u32 as usize;
        mem.write(&mut caller, y_offset, &y.to_le_bytes())?;

        Ok(())
    }

    fn change_app(
        mut caller: wasmtime::Caller<'_, AppState>,
        ptr: i32,
        len: u32,
    ) -> wasmtime::Result<i32> {
        let len = len as usize;
        if len == 0 || len > 128 {
            return Ok(-1);
        }

        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("change_app: failed to find host memory");
        };

        let mut buf = vec![0u8; len];
        let offset = ptr as usize;
        mem.read(&caller, offset, &mut buf)?;

        let shortname = match String::from_utf8(buf) {
            Ok(s) => s,
            Err(_) => return Ok(-1),
        };

        let has_next_app = caller.data().has_next_app.clone();
        let should_prepare = !matches!(
            caller.data().next_app.as_ref(),
            Some(NextAppState::Pending(_))
        );
        if should_prepare {
            has_next_app.store(false, Ordering::Release);
            let ctx = caller.data().ctx.clone();
            let (tx, rx) = tokio::sync::oneshot::channel();
            caller.data_mut().next_app = Some(NextAppState::Pending(rx));
            tokio::task::spawn(async move {
                let result = match Self::prepare_instantiate(&ctx, shortname).await {
                    Ok(next_app) => {
                        has_next_app.store(true, Ordering::Release);
                        Ok(next_app)
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "change_app preload failed");
                        has_next_app.store(false, Ordering::Release);
                        if err.downcast_ref::<UnknownGameShortnameError>().is_some() {
                            Err(NextAppPrepareErrorCode::UnknownShortname)
                        } else {
                            Err(NextAppPrepareErrorCode::Other)
                        }
                    }
                };
                let _ = tx.send(result);
            });
        }

        Ok(0)
    }

    fn next_app_ready(mut caller: wasmtime::Caller<'_, AppState>) -> wasmtime::Result<i32> {
        let ready = caller.data().has_next_app.load(Ordering::Acquire);
        if ready {
            return Ok(NEXT_APP_READY_READY);
        }

        let Some(next_app_state) = caller.data_mut().next_app.take() else {
            return Ok(NEXT_APP_READY_NOT_READY);
        };
        match next_app_state {
            NextAppState::Pending(mut next_app_rx) => match next_app_rx.try_recv() {
                Ok(Ok(next_app)) => {
                    caller.data_mut().next_app = Some(NextAppState::Ready(next_app));
                    Ok(NEXT_APP_READY_READY)
                }
                Ok(Err(error_code)) => {
                    caller.data_mut().next_app = Some(NextAppState::Failed(error_code));
                    Ok(error_code.to_i32())
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                    caller.data_mut().next_app = Some(NextAppState::Pending(next_app_rx));
                    Ok(NEXT_APP_READY_NOT_READY)
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    Ok(NEXT_APP_READY_ERR_PREPARE_FAILED_OTHER)
                }
            },
            NextAppState::Ready(next_app) => {
                caller.data_mut().next_app = Some(NextAppState::Ready(next_app));
                Ok(NEXT_APP_READY_READY)
            }
            NextAppState::Failed(error_code) => {
                caller.data_mut().next_app = Some(NextAppState::Failed(error_code));
                Ok(error_code.to_i32())
            }
        }
    }

    fn peer_send(
        mut caller: wasmtime::Caller<'_, AppState>,
        peer_ids_ptr: i32,
        peer_ids_count: u32,
        data_ptr: i32,
        data_len: u32,
    ) -> wasmtime::Result<i32> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("peer_send: failed to find host memory");
        };

        if peer_ids_count == 0 || peer_ids_count > 1024 {
            return Ok(PEER_SEND_ERR_INVALID_PEER_COUNT);
        }

        if data_len == 0 {
            return Ok(0);
        }

        if data_len > 64 * 1024 {
            return Ok(PEER_SEND_ERR_DATA_TOO_LARGE);
        }
        const PEER_ID_SIZE: usize = std::mem::size_of::<PeerId>();
        let peer_ids_offset = peer_ids_ptr as usize;
        let peer_ids_count = peer_ids_count as usize;
        let mut peer_ids = Vec::with_capacity(peer_ids_count);
        for i in 0..peer_ids_count {
            let offset = i * PEER_ID_SIZE;
            let mut peer_id_bytes = [0u8; PEER_ID_SIZE];
            mem.read(&caller, peer_ids_offset + offset, &mut peer_id_bytes)?;
            peer_ids.push(crate::mesh::PeerId::from_bytes(peer_id_bytes));
        }

        let data_offset = data_ptr as usize;
        let data_len = data_len as usize;
        let mut data_buf = vec![0u8; data_len];
        mem.read(&caller, data_offset, &mut data_buf)?;

        match caller.data_mut().app.peer_tx.try_send((peer_ids, data_buf)) {
            Ok(_) => Ok(0),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!("peer_send: channel full, message dropped");
                Ok(PEER_SEND_ERR_CHANNEL_FULL)
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                tracing::error!("peer_send: channel closed");
                Ok(PEER_SEND_ERR_CHANNEL_CLOSED)
            }
        }
    }

    fn peer_recv(
        mut caller: wasmtime::Caller<'_, AppState>,
        from_peer_ptr: i32,
        data_ptr: i32,
        data_max_len: u32,
    ) -> wasmtime::Result<i32> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("peer_recv: failed to find host memory");
        };

        let msg = match caller.data_mut().app.peer_rx.try_recv() {
            Ok(msg) => msg,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                return Ok(0);
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                return Ok(PEER_RECV_ERR_CHANNEL_DISCONNECTED);
            }
        };

        let from_peer_offset = from_peer_ptr as usize;
        let peer_id_buf = msg.from_peer().to_bytes();

        mem.write(&mut caller, from_peer_offset, &peer_id_buf)?;

        let data_offset = data_ptr as usize;
        let data_max_len = data_max_len as usize;
        let data = msg.data();
        let data_to_write = std::cmp::min(data.len(), data_max_len);

        mem.write(&mut caller, data_offset, &data[..data_to_write])?;

        Ok(data_to_write as i32)
    }

    fn region_latency(
        mut caller: wasmtime::Caller<'_, AppState>,
        (region_ptr,): (i32,),
    ) -> Box<dyn Future<Output = wasmtime::Result<i32>> + Send + '_> {
        Box::new(async move {
            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                wasmtime::bail!("region_latency: failed to find host memory");
            };

            let region_offset = region_ptr as usize;
            let mut region_bytes = [0u8; 4];
            mem.read(&caller, region_offset, &mut region_bytes)?;

            let region_id = RegionId::from_bytes(region_bytes);

            let mesh = &caller.data().ctx.mesh;
            match mesh.get_region_latency(region_id).await {
                Some(latency) => Ok(latency.as_millis() as i32),
                None => Ok(-1), // Unknown latency is a valid semantic response
            }
        })
    }

    fn peer_list(
        mut caller: wasmtime::Caller<'_, AppState>,
        (peer_ids_ptr, length, total_count_ptr): (i32, u32, i32),
    ) -> Box<dyn Future<Output = wasmtime::Result<i32>> + Send + '_> {
        Box::new(async move {
            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                wasmtime::bail!("peer_list: failed to find host memory");
            };

            let app_id = caller.data().app.app_id;
            let mesh = &caller.data().ctx.mesh;
            let peers = mesh.get_peers_for_app(app_id).await;

            let total_count = peers.len();
            let length = std::cmp::min(length as usize, 65536);

            let total_count_offset = total_count_ptr as usize;
            mem.write(
                &mut caller,
                total_count_offset,
                &(total_count as u32).to_le_bytes(),
            )?;

            const PEER_ID_SIZE: usize = 16;
            let ptr_offset = peer_ids_ptr as usize;

            for (i, peer_id) in peers.iter().take(length).enumerate() {
                let write_offset = ptr_offset + (i * PEER_ID_SIZE);
                let peer_id_bytes = peer_id.to_bytes();
                mem.write(&mut caller, write_offset, &peer_id_bytes)?;
            }

            Ok(length as i32)
        })
    }

    fn menu_games_list_start(mut caller: wasmtime::Caller<'_, AppState>) -> wasmtime::Result<i32> {
        if caller.data().app.shortname != "menu" {
            return Ok(MENU_REQ_ERR_NOT_MENU_APP);
        }
        let db = caller.data().ctx.db.clone();
        let current_shortname = caller.data().app.shortname.clone();
        let tx = caller.data().ctx.menu_future_tx.clone();
        let request_id = Self::insert_menu_request_pending(caller.data_mut());
        if request_id < 0 {
            return Ok(request_id);
        }
        let future = async move {
            let result = Self::load_menu_games(db, current_shortname).await;
            (result, MenuUpdate::default())
        };
        let _ = tx.try_send((request_id as usize, Box::pin(future)));
        Ok(request_id)
    }

    fn menu_profile_get_start(mut caller: wasmtime::Caller<'_, AppState>) -> wasmtime::Result<i32> {
        if caller.data().app.shortname != "menu" {
            return Ok(MENU_REQ_ERR_NOT_MENU_APP);
        }
        let db = caller.data().ctx.db.clone();
        let user_id = caller.data().ctx.user_id;
        let menu_session = caller.data().ctx.menu_session.clone();
        let menu_username = caller.data().ctx.menu_username.clone();
        let tx = caller.data().ctx.menu_future_tx.clone();
        let request_id = Self::insert_menu_request_pending(caller.data_mut());
        if request_id < 0 {
            return Ok(request_id);
        }
        let future =
            async move { Self::load_menu_profile(db, user_id, menu_session, menu_username).await };
        let _ = tx.try_send((request_id as usize, Box::pin(future)));
        Ok(request_id)
    }

    fn menu_profile_set_start(
        mut caller: wasmtime::Caller<'_, AppState>,
        username_ptr: i32,
        username_len: u32,
        locale_ptr: i32,
        locale_len: u32,
    ) -> wasmtime::Result<i32> {
        if caller.data().app.shortname != "menu" {
            return Ok(MENU_REQ_ERR_NOT_MENU_APP);
        }
        if username_len > 1024 || locale_len > 1024 {
            return Ok(MENU_REQ_ERR_INVALID_INPUT);
        }
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("menu_profile_set_start: failed to find host memory");
        };
        let mut username_bytes = vec![0u8; username_len as usize];
        let mut locale_bytes = vec![0u8; locale_len as usize];
        mem.read(&caller, username_ptr as usize, &mut username_bytes)?;
        mem.read(&caller, locale_ptr as usize, &mut locale_bytes)?;
        let Ok(username) = String::from_utf8(username_bytes) else {
            return Ok(MENU_REQ_ERR_INVALID_INPUT);
        };
        let Ok(locale) = String::from_utf8(locale_bytes) else {
            return Ok(MENU_REQ_ERR_INVALID_INPUT);
        };

        let db = caller.data().ctx.db.clone();
        let user_id = caller.data().ctx.user_id;
        let menu_session = caller.data().ctx.menu_session.clone();
        let tx = caller.data().ctx.menu_future_tx.clone();
        let request_id = Self::insert_menu_request_pending(caller.data_mut());
        if request_id < 0 {
            return Ok(request_id);
        }
        let future = async move {
            Self::save_menu_profile(db, user_id, menu_session, username, locale).await
        };
        let _ = tx.try_send((request_id as usize, Box::pin(future)));
        Ok(request_id)
    }

    fn menu_replays_list_start(
        mut caller: wasmtime::Caller<'_, AppState>,
        locale_ptr: i32,
        locale_len: u32,
    ) -> wasmtime::Result<i32> {
        if caller.data().app.shortname != "menu" {
            return Ok(MENU_REQ_ERR_NOT_MENU_APP);
        }
        if locale_len > 64 {
            return Ok(MENU_REQ_ERR_INVALID_INPUT);
        }
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("menu_replays_list_start: failed to find host memory");
        };
        let mut locale_bytes = vec![0u8; locale_len as usize];
        mem.read(&caller, locale_ptr as usize, &mut locale_bytes)?;
        let Ok(locale) = String::from_utf8(locale_bytes) else {
            return Ok(MENU_REQ_ERR_INVALID_INPUT);
        };

        let db = caller.data().ctx.db.clone();
        let user_id = caller.data().ctx.user_id;
        let menu_session = caller.data().ctx.menu_session.clone();
        let tx = caller.data().ctx.menu_future_tx.clone();
        let request_id = Self::insert_menu_request_pending(caller.data_mut());
        if request_id < 0 {
            return Ok(request_id);
        }
        let future =
            async move { Self::load_menu_replays(db, user_id, menu_session, locale).await };
        let _ = tx.try_send((request_id as usize, Box::pin(future)));
        Ok(request_id)
    }

    fn menu_replay_delete_start(
        mut caller: wasmtime::Caller<'_, AppState>,
        created_at: i64,
    ) -> wasmtime::Result<i32> {
        if caller.data().app.shortname != "menu" {
            return Ok(MENU_REQ_ERR_NOT_MENU_APP);
        }
        let db = caller.data().ctx.db.clone();
        let user_id = caller.data().ctx.user_id;
        let menu_session = caller.data().ctx.menu_session.clone();
        let tx = caller.data().ctx.menu_future_tx.clone();
        let request_id = Self::insert_menu_request_pending(caller.data_mut());
        if request_id < 0 {
            return Ok(request_id);
        }
        let future =
            async move { Self::delete_menu_replay(db, user_id, menu_session, created_at).await };
        let _ = tx.try_send((request_id as usize, Box::pin(future)));
        Ok(request_id)
    }

    fn menu_poll(
        mut caller: wasmtime::Caller<'_, AppState>,
        request_id: i32,
        data_ptr: i32,
        data_max_len: u32,
        data_len_ptr: i32,
    ) -> wasmtime::Result<i32> {
        let request_id = request_id as usize;
        {
            let state = caller.data();
            if request_id >= state.menu_requests.len() || state.menu_requests[request_id].is_none()
            {
                return Ok(MENU_POLL_ERR_INVALID_REQUEST_ID);
            }
        }

        let completed_result = caller
            .data()
            .completed_menu_results
            .lock()
            .unwrap()
            .remove(&request_id);
        if let Some(result) = completed_result {
            caller.data_mut().menu_requests[request_id] = Some(PendingMenuRequest {
                state: MenuRequestState::Complete(result.map(Vec::into_boxed_slice)),
            });
        }
        let Some(mut request) = caller.data_mut().menu_requests[request_id].take() else {
            return Ok(MENU_POLL_ERR_INVALID_REQUEST_ID);
        };
        if let MenuRequestState::Pending = request.state {
            caller.data_mut().menu_requests[request_id] = Some(request);
            return Ok(MENU_POLL_PENDING);
        }

        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("menu_poll: failed to find host memory");
        };

        match request.state {
            MenuRequestState::Complete(Ok(data)) => {
                let needed = data.len() as u32;
                mem.write(&mut caller, data_len_ptr as usize, &needed.to_le_bytes())?;
                if needed > data_max_len {
                    request.state = MenuRequestState::Complete(Ok(data));
                    caller.data_mut().menu_requests[request_id] = Some(request);
                    return Ok(MENU_POLL_ERR_BUFFER_TOO_SMALL);
                }
                if needed > 0 {
                    mem.write(&mut caller, data_ptr as usize, &data)?;
                }
                Ok(1)
            }
            MenuRequestState::Complete(Err(_)) => Ok(MENU_POLL_ERR_REQUEST_FAILED),
            MenuRequestState::Pending => unreachable!(),
        }
    }

    fn insert_menu_request_pending(state: &mut AppState) -> i32 {
        let request = PendingMenuRequest {
            state: MenuRequestState::Pending,
        };
        for (i, slot) in state.menu_requests.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = Some(request);
                return i as i32;
            }
        }
        if state.menu_requests.len() >= MAX_MENU_REQUESTS {
            return MENU_REQ_ERR_TOO_MANY_REQUESTS;
        }
        let request_id = state.menu_requests.len() as i32;
        state.menu_requests.push(Some(request));
        request_id
    }

    async fn load_menu_games(
        db: libsql::Connection,
        current_shortname: String,
    ) -> Result<Vec<u8>, String> {
        #[derive(serde::Serialize)]
        struct GameOut {
            id: u64,
            shortname: String,
            details: serde_json::Value,
        }

        #[derive(serde::Serialize)]
        struct GamesOut {
            games: Vec<GameOut>,
        }

        let mut rows = db
            .query(
                "SELECT id, shortname, details FROM games WHERE shortname != ?1 ORDER BY id",
                libsql::params!(current_shortname),
            )
            .await
            .map_err(|e| e.to_string())?;

        let mut games = Vec::<GameOut>::new();
        while let Some(row) = rows.next().await.map_err(|e| e.to_string())? {
            let id = row.get::<u64>(0).map_err(|e| e.to_string())?;
            let shortname = row.get::<String>(1).map_err(|e| e.to_string())?;
            let details_json = row.get::<String>(2).map_err(|e| e.to_string())?;
            let details = serde_json::from_str::<serde_json::Value>(&details_json)
                .unwrap_or_else(|_| serde_json::json!({}));
            let game = GameOut {
                id,
                shortname,
                details,
            };
            games.push(game);
        }

        serde_json::to_vec(&GamesOut { games }).map_err(|e| e.to_string())
    }

    async fn load_menu_profile(
        db: libsql::Connection,
        user_id: Option<u64>,
        menu_session: MenuSessionState,
        menu_username: String,
    ) -> (Result<Vec<u8>, String>, MenuUpdate) {
        #[derive(serde::Serialize)]
        struct ProfileOut {
            username: String,
            locale: String,
        }

        let mut profile = ProfileOut {
            username: menu_username,
            locale: menu_session.locale.clone(),
        };

        let mut update = MenuUpdate::default();
        let mut had_row = false;
        if let Some(user_id) = user_id {
            let rows = db
                .query(
                    "SELECT username, locale FROM users WHERE id = ?1 LIMIT 1",
                    libsql::params!(user_id),
                )
                .await
                .map_err(|e| e.to_string());
            let Ok(mut rows) = rows else {
                return (Err(rows.unwrap_err()), update);
            };
            if let Ok(Some(row)) = rows.next().await {
                had_row = true;
                profile.username = row
                    .get::<String>(0)
                    .unwrap_or_else(|_| std::mem::take(&mut profile.username));
                profile.locale = row
                    .get::<String>(1)
                    .unwrap_or_else(|_| std::mem::take(&mut profile.locale));
            }
        }

        let result = serde_json::to_vec(&profile).map_err(|e| e.to_string());
        if had_row {
            update.username = Some(std::mem::take(&mut profile.username));
            update.locale = Some(std::mem::take(&mut profile.locale));
        }
        (result, update)
    }

    async fn save_menu_profile(
        db: libsql::Connection,
        user_id: Option<u64>,
        _menu_session: MenuSessionState,
        username: String,
        locale: String,
    ) -> (Result<Vec<u8>, String>, MenuUpdate) {
        let mut update = MenuUpdate::default();
        if let Some(user_id) = user_id {
            let result = db
                .execute(
                    "UPDATE users SET username = ?1, locale = ?2 WHERE id = ?3",
                    libsql::params!(username.as_str(), locale.as_str(), user_id),
                )
                .await
                .map_err(|e| e.to_string());
            if let Err(e) = result {
                return (Err(e), update);
            }
        }
        update.username = Some(username);
        update.locale = Some(locale);
        (Ok(Vec::new()), update)
    }

    async fn load_menu_replays(
        db: libsql::Connection,
        user_id: Option<u64>,
        mut menu_session: MenuSessionState,
        locale: String,
    ) -> (Result<Vec<u8>, String>, MenuUpdate) {
        #[derive(serde::Serialize, Clone)]
        struct ReplayOut {
            created_at: i64,
            asciinema_url: String,
            game_title: String,
        }

        #[derive(serde::Serialize)]
        struct ReplaysOut {
            replays: Vec<ReplayOut>,
        }

        let mut replays = Vec::<ReplayOut>::new();

        if let Some(user_id) = user_id {
            let mut rows = match db
                .query(
                    "SELECT r.created_at, r.asciinema_url, g.shortname, g.details FROM replays r LEFT JOIN games g ON r.game_id = g.id WHERE r.user_id = ?1 ORDER BY r.created_at DESC",
                    libsql::params!(user_id),
                )
                .await
                .map_err(|e| e.to_string())
            {
                Ok(r) => r,
                Err(e) => return (Err(e), MenuUpdate::default()),
            };
            loop {
                let row_opt = match rows.next().await.map_err(|e| e.to_string()) {
                    Ok(opt) => opt,
                    Err(e) => return (Err(e), MenuUpdate::default()),
                };
                let Some(row) = row_opt else { break };
                let shortname = row
                    .get::<String>(2)
                    .unwrap_or_else(|_| String::from("unknown"));
                let details = row.get::<String>(3).unwrap_or_default();
                let created_at = match row.get::<i64>(0).map_err(|e| e.to_string()) {
                    Ok(v) => v,
                    Err(e) => return (Err(e), MenuUpdate::default()),
                };
                let asciinema_url = match row.get::<String>(1).map_err(|e| e.to_string()) {
                    Ok(v) => v,
                    Err(e) => return (Err(e), MenuUpdate::default()),
                };
                replays.push(ReplayOut {
                    created_at,
                    asciinema_url,
                    game_title: Self::game_title_from_details(&shortname, &details, &locale),
                });
            }
        } else {
            let session_replays = std::mem::take(&mut menu_session.replays);
            for replay in session_replays.into_iter().rev() {
                let mut shortname = String::new();
                let mut details = String::new();
                if let Ok(mut rows) = db
                    .query(
                        "SELECT shortname, details FROM games WHERE id = ?1 LIMIT 1",
                        libsql::params!(replay.game_id),
                    )
                    .await
                {
                    if let Ok(Some(row)) = rows.next().await {
                        shortname = row.get::<String>(0).unwrap_or_default();
                        details = row.get::<String>(1).unwrap_or_default();
                    }
                }
                replays.push(ReplayOut {
                    created_at: replay.created_at,
                    asciinema_url: replay.asciinema_url,
                    game_title: Self::game_title_from_details(&shortname, &details, &locale),
                });
            }
        }

        let result = serde_json::to_vec(&ReplaysOut { replays }).map_err(|e| e.to_string());
        (result, MenuUpdate::default())
    }

    fn game_title_from_details(shortname: &str, details_json: &str, locale: &str) -> String {
        serde_json::from_str::<GameDetailsOut>(details_json)
            .map(|details| details.name.resolve(locale, shortname))
            .unwrap_or_else(|_| shortname.to_string())
    }

    async fn delete_menu_replay(
        db: libsql::Connection,
        user_id: Option<u64>,
        mut menu_session: MenuSessionState,
        created_at: i64,
    ) -> (Result<Vec<u8>, String>, MenuUpdate) {
        let mut update = MenuUpdate::default();
        if let Some(user_id) = user_id {
            let result = db
                .execute(
                    "DELETE FROM replays WHERE user_id = ?1 AND created_at = ?2",
                    libsql::params!(user_id, created_at),
                )
                .await
                .map_err(|e| e.to_string());
            if let Err(e) = result {
                return (Err(e), update);
            }
        } else {
            menu_session.replays.retain(|r| r.created_at != created_at);
            update.replays = Some(std::mem::take(&mut menu_session.replays));
        }
        (Ok(Vec::new()), update)
    }

    fn graceful_shutdown_poll(caller: wasmtime::Caller<'_, AppState>) -> wasmtime::Result<i32> {
        // tracing::info!(fuel=?caller.get_fuel(), shortname=caller.data().app.shortname, "graceful_shutdown_poll");
        let is_cancelled = caller.data().graceful_shutdown_token.is_cancelled();
        Ok(if is_cancelled { 1 } else { 0 })
    }

    fn host_network_info(
        mut caller: wasmtime::Caller<'_, AppState>,
        bytes_per_sec_in_ptr: i32,
        bytes_per_sec_out_ptr: i32,
        last_throttled_ms_ptr: i32,
        latency_ms_ptr: i32,
    ) -> wasmtime::Result<i32> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("network_info: failed to find host memory");
        };

        let info = &caller.data().network_info;

        let bytes_per_sec_in = info.bytes_per_sec_in();
        let bytes_per_sec_out = info.bytes_per_sec_out();
        let last_throttled_ms = info
            .last_throttled()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let latency_ms = info.latency().map(|d| d.as_millis() as i32).unwrap_or(-1);

        mem.write(
            &mut caller,
            bytes_per_sec_in_ptr as usize,
            &bytes_per_sec_in.to_le_bytes(),
        )?;
        mem.write(
            &mut caller,
            bytes_per_sec_out_ptr as usize,
            &bytes_per_sec_out.to_le_bytes(),
        )?;
        mem.write(
            &mut caller,
            last_throttled_ms_ptr as usize,
            &last_throttled_ms.to_le_bytes(),
        )?;
        mem.write(
            &mut caller,
            latency_ms_ptr as usize,
            &latency_ms.to_le_bytes(),
        )?;

        Ok(0)
    }

    fn host_terminal_info(
        mut caller: wasmtime::Caller<'_, AppState>,
        color_mode_ptr: i32,
        has_bg_ptr: i32,
        bg_r_ptr: i32,
        bg_g_ptr: i32,
        bg_b_ptr: i32,
        has_dark_ptr: i32,
        dark_ptr: i32,
    ) -> wasmtime::Result<i32> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("terminal_info: failed to find host memory");
        };

        let profile = caller.data().terminal_profile;
        let mode = profile.color_mode as u8;
        let (has_bg, bg_r, bg_g, bg_b) = match profile.background_rgb {
            Some((r, g, b)) => (1i32, r, g, b),
            None => (0i32, 0u8, 0u8, 0u8),
        };
        let (has_dark, dark) = match profile.dark_background {
            Some(is_dark) => (1i32, if is_dark { 1i32 } else { 0i32 }),
            None => (0i32, 0i32),
        };

        mem.write(&mut caller, color_mode_ptr as usize, &[mode])?;
        mem.write(&mut caller, has_bg_ptr as usize, &has_bg.to_le_bytes())?;
        mem.write(&mut caller, bg_r_ptr as usize, &[bg_r])?;
        mem.write(&mut caller, bg_g_ptr as usize, &[bg_g])?;
        mem.write(&mut caller, bg_b_ptr as usize, &[bg_b])?;
        mem.write(&mut caller, has_dark_ptr as usize, &has_dark.to_le_bytes())?;
        mem.write(&mut caller, dark_ptr as usize, &dark.to_le_bytes())?;

        Ok(0)
    }

    fn audio_write(
        mut caller: wasmtime::Caller<'_, AppState>,
        ptr: i32,
        sample_count: u32,
    ) -> wasmtime::Result<i32> {
        if sample_count == 0 {
            return Ok(0);
        }

        let sample_count = sample_count.min(SAMPLE_RATE) as usize;
        let float_count = sample_count * CHANNELS;
        let byte_count = float_count * std::mem::size_of::<f32>();

        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("audio_write: failed to find host memory");
        };

        let mut buf = vec![0u8; byte_count];
        let offset = ptr as usize;
        mem.read(&caller, offset, &mut buf)?;

        let samples: Vec<f32> = buf
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect();

        let written = caller
            .data_mut()
            .audio
            .as_mut()
            .map(|mixer| mixer.write(&samples))
            .unwrap_or(0);
        Ok(written as i32)
    }

    fn audio_info(
        mut caller: wasmtime::Caller<'_, AppState>,
        frame_size_ptr: i32,
        sample_rate_ptr: i32,
        pts_ptr: i32,
        buffer_available_ptr: i32,
    ) -> wasmtime::Result<i32> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            wasmtime::bail!("audio_info: failed to find host memory");
        };

        let (pts, buffer_available) = caller
            .data_mut()
            .audio
            .as_mut()
            .map(|mixer| mixer.info())
            .unwrap_or((0, 0));

        mem.write(
            &mut caller,
            frame_size_ptr as usize,
            &(FRAME_SIZE as u32).to_le_bytes(),
        )?;

        mem.write(
            &mut caller,
            sample_rate_ptr as usize,
            &(SAMPLE_RATE).to_le_bytes(),
        )?;

        mem.write(&mut caller, pts_ptr as usize, &(pts as u64).to_le_bytes())?;

        mem.write(
            &mut caller,
            buffer_available_ptr as usize,
            &(buffer_available as u32).to_le_bytes(),
        )?;

        Ok(0)
    }
}

pub struct PreloadedAppState {
    wasi_ctx: wasmtime_wasi::p1::WasiP1Ctx,
    peer_rx: tokio::sync::mpsc::Receiver<PeerMessageApp>,
    peer_tx: tokio::sync::mpsc::Sender<(Vec<PeerId>, Vec<u8>)>,
    app_id: AppId,
    shortname: String,
}

#[derive(Clone)]
pub struct AppContext {
    db: libsql::Connection,
    mesh: Mesh,
    remote_sshid: String,
    audio_enabled: bool,
    term: Option<String>,
    linker: Arc<wasmtime::Linker<AppState>>,
    module_cache: Arc<tokio::sync::RwLock<HashMap<u64, Arc<wasmtime::Module>>>>,
    app_output_sender: tokio::sync::mpsc::Sender<Vec<u8>>,
    user_id: Option<u64>,
    locale: String,
    menu_session: MenuSessionState,
    menu_username: String,
    menu_future_tx: mpsc::Sender<(
        usize,
        Pin<Box<dyn std::future::Future<Output = (Result<Vec<u8>, String>, MenuUpdate)> + Send>>,
    )>,
}

pub struct AppState {
    app: PreloadedAppState,
    ctx: AppContext,
    conn_manager: ConnectionManager,
    menu_requests: Vec<Option<PendingMenuRequest>>,
    completed_menu_results: Arc<std::sync::Mutex<HashMap<usize, Result<Vec<u8>, String>>>>,
    limits: AppLimiter,
    next_app: Option<NextAppState>,
    has_next_app: Arc<AtomicBool>,
    input_receiver: tokio::sync::mpsc::Receiver<smallvec::SmallVec<[u8; 16]>>,
    graceful_shutdown_token: CancellationToken,
    network_info: Arc<dyn NetworkInfo>,
    terminal_profile: TerminalProfile,
    audio: Option<Mixer>,
    terminal_snapshot: Arc<TerminalSnapshot>,
}

enum NextAppState {
    Pending(tokio::sync::oneshot::Receiver<NextAppPrepareResult>),
    Ready((PreloadedAppState, wasmtime::InstancePre<AppState>)),
    Failed(NextAppPrepareErrorCode),
}

type NextAppPrepareResult =
    Result<(PreloadedAppState, wasmtime::InstancePre<AppState>), NextAppPrepareErrorCode>;

#[derive(Clone, Copy)]
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
struct UnknownGameShortnameError(String);

impl std::fmt::Display for UnknownGameShortnameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unknown game shortname: {}", self.0)
    }
}

impl std::error::Error for UnknownGameShortnameError {}

pub struct TerminalSnapshot {
    packed: AtomicU64,
}

impl TerminalSnapshot {
    fn new(width: u16, height: u16, cursor_x: u16, cursor_y: u16) -> Self {
        Self {
            packed: AtomicU64::new(Self::pack(width, height, cursor_x, cursor_y)),
        }
    }

    fn size(&self) -> (u16, u16) {
        let (width, height, _, _) = Self::unpack(self.packed.load(Ordering::Acquire));
        (width, height)
    }

    fn set(&self, width: u16, height: u16, x: u16, y: u16) {
        self.packed
            .store(Self::pack(width, height, x, y), Ordering::Release);
    }

    fn cursor_position(&self) -> (u16, u16) {
        let (_, _, x, y) = Self::unpack(self.packed.load(Ordering::Acquire));
        (x, y)
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
    game_id: u64,
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

struct MenuFutureWrapper {
    request_id: usize,
    future:
        Pin<Box<dyn std::future::Future<Output = (Result<Vec<u8>, String>, MenuUpdate)> + Send>>,
}

impl std::future::Future for MenuFutureWrapper {
    type Output = (usize, Result<Vec<u8>, String>, MenuUpdate);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.future.as_mut().poll(cx) {
            Poll::Ready((result, update)) => Poll::Ready((self.request_id, result, update)),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct MenuUpdate {
    username: Option<String>,
    locale: Option<String>,
    replays: Option<Vec<SessionReplay>>,
}

impl Default for MenuUpdate {
    fn default() -> Self {
        Self {
            username: None,
            locale: None,
            replays: None,
        }
    }
}

pub struct Connection {
    stream: Stream,
}

struct AppLimiter {
    memory_total: usize,
    table_total: usize,
}

impl Default for AppLimiter {
    fn default() -> Self {
        AppLimiter {
            memory_total: 0,
            table_total: 0,
        }
    }
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
    buffer: Vec<u8>,
}

impl EscapeSequenceBuffer {
    fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(64),
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
    fn push(&mut self, mut data: Vec<u8>) -> Vec<u8> {
        // Fast path: buffer is empty, check if incoming data is complete
        if self.buffer.is_empty() {
            let incomplete_len = Self::incomplete_escape_len(&data);
            if incomplete_len == 0 {
                return data;
            }
            if incomplete_len == data.len() {
                self.buffer = data;
                return Vec::new();
            }
            let split_at = data.len() - incomplete_len;
            self.buffer.extend_from_slice(&data[split_at..]);
            data.truncate(split_at);
            return data;
        }

        // Slow path: we have buffered data, need to combine
        self.buffer.append(&mut data);

        let incomplete_len = Self::incomplete_escape_len(&self.buffer);
        if incomplete_len == 0 {
            std::mem::take(&mut self.buffer)
        } else if incomplete_len < self.buffer.len() {
            let split_at = self.buffer.len() - incomplete_len;
            let incomplete = self.buffer.split_off(split_at);
            let complete = std::mem::replace(&mut self.buffer, incomplete);
            complete
        } else {
            Vec::new()
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
    sender: tokio_util::sync::PollSender<Vec<u8>>,
    buffer: Vec<u8>,
}

impl AsyncStdoutWriter {
    pub fn new(sender: tokio::sync::mpsc::Sender<Vec<u8>>) -> Self {
        Self {
            sender: tokio_util::sync::PollSender::new(sender),
            buffer: Vec::with_capacity(16 * 1024),
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
                let mut buffer = std::mem::replace(&mut self.buffer, Vec::with_capacity(16 * 1024));
                buffer.extend_from_slice(buf);
                // let len = self.buffer.len();
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
        // Poll::Ready(Ok(()))
        // self.poll_write(cx, &[]).map_ok(|_| ())

        match self.sender.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                let buffer = std::mem::take(&mut self.buffer);
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
    sender: tokio::sync::mpsc::Sender<Vec<u8>>,
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
                "INSERT INTO replays (asciinema_url, user_id, game_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                libsql::params!(url.as_str(), uid, app_id.0, now),
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
            game_id: app_id.0,
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
