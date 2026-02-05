// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, OnceLock,
    },
    task::{Context, Poll},
    time::{Duration, UNIX_EPOCH},
};

use hex::ToHex;
use ipnet::{Ipv4Net, Ipv6Net};
use smallvec::SmallVec;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use wasmtime_wasi::I32Exit;

use crate::{
    audio::{AudioBuffer, CHANNELS, FRAME_SIZE, Mixer, SAMPLE_RATE},
    mesh::{AppId, Mesh, PeerId, PeerMessageApp, RegionId},
    rate_limiting::{NetworkInfo, TokenBucket},
    replay::ReplayBuffer,
    status_bar::StatusBar,
};

/// Maximum number of concurrent connections per app instance
const MAX_CONNECTIONS: usize = 8;

/// Maximum outbound bandwidth in bytes per second (20 kBps)
const MAX_OUTBOUND_BANDWIDTH: u64 = 20 * 1024;

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

pub struct AppServer {
    linker: Arc<wasmtime::Linker<AppState>>,
    engine: wasmtime::Engine,
    pub db: libsql::Connection,
    mesh: Mesh,
}

pub struct AppInstantiationParams {
    pub input_receiver: tokio::sync::mpsc::Receiver<SmallVec<[u8; 16]>>,
    pub output_sender: tokio::sync::mpsc::Sender<Vec<u8>>,
    pub audio_sender: Option<tokio::sync::mpsc::Sender<Vec<u8>>>,
    pub window_size_receiver: tokio::sync::watch::Receiver<(u16, u16)>,
    pub graceful_shutdown_token: CancellationToken,
    pub username: String,
    pub remote_sshid: String,
    pub term: Option<String>,
    pub args: Option<Vec<u8>>,
    pub network_info: Arc<dyn NetworkInfo>,
    pub first_app_shortname: String,
}

impl AppServer {
    pub fn new(mesh: Mesh, db: libsql::Connection) -> anyhow::Result<Self> {
        let mut config = wasmtime::Config::new();
        config.async_support(true);
        config.epoch_interruption(true);
        let cache_config = wasmtime::CacheConfig::new();
        config.cache(Some(wasmtime::Cache::new(cache_config).unwrap()));
        let engine = wasmtime::Engine::new(&config)?;
        tokio::task::spawn({
            let engine = engine.clone();
            let mut epoch_interval = tokio::time::interval(Duration::from_millis(20));

            async move {
                loop {
                    tokio::select! {
                        _ = epoch_interval.tick() => {
                           engine.increment_epoch();
                        }
                    }
                }
            }
        });

        let mut linker = wasmtime::Linker::<AppState>::new(&engine);
        wasmtime_wasi::p1::add_to_linker_async(&mut linker, |t| &mut t.app.wasi_ctx)?;

        linker.func_wrap("terminal_games", "dial", Self::host_dial)?;
        linker.func_wrap("terminal_games", "poll_dial", Self::poll_dial)?;
        linker.func_wrap("terminal_games", "conn_close", Self::conn_close)?;
        linker.func_wrap("terminal_games", "conn_write", Self::conn_write)?;
        linker.func_wrap("terminal_games", "conn_read", Self::conn_read)?;
        linker.func_wrap("terminal_games", "terminal_read", Self::terminal_read)?;
        linker.func_wrap_async("terminal_games", "terminal_size", Self::terminal_size)?;
        linker.func_wrap_async("terminal_games", "terminal_cursor", Self::terminal_cursor)?;
        linker.func_wrap_async("terminal_games", "change_app", Self::change_app)?;
        linker.func_wrap_async("terminal_games", "next_app_ready", Self::next_app_ready)?;
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
        linker.func_wrap("terminal_games", "audio_write", Self::audio_write)?;
        linker.func_wrap("terminal_games", "audio_info", Self::audio_info)?;

        Ok(Self {
            linker: Arc::new(linker),
            mesh,
            db,
            engine,
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
        tokio::task::spawn(async move {
            let engine = engine;
            
            let mut window_size_receiver = params.window_size_receiver;
            let (first_cols, first_rows) = *window_size_receiver.borrow();
            let mut terminal = Arc::new(Mutex::new(headless_terminal::Parser::new(first_rows, first_cols, 0)));

            let (app_output_sender, mut app_output_receiver) =
                tokio::sync::mpsc::channel::<Vec<u8>>(1);
            let has_next_app_shortname = Arc::new(AtomicBool::new(false));
            let (shortname_sender, mut shortname_receiver) =
                tokio::sync::mpsc::channel::<String>(1);

            let network_info = params.network_info.clone();
            let hard_shutdown_token = CancellationToken::new();
            let audio_tx = params.audio_sender;
            let first_app_shortname = params.first_app_shortname;

            let replay_buffer = Arc::new(Mutex::new(ReplayBuffer::new(first_cols, first_rows, first_app_shortname.clone(), params.term.clone())));
            let (notification_tx, notification_rx) = tokio::sync::mpsc::channel(1);

            let (filtered_input_tx, filtered_input_rx) = tokio::sync::mpsc::channel(1);
            tokio::task::spawn({
                let mut input_receiver = params.input_receiver;
                let graceful_shutdown_token = params.graceful_shutdown_token.clone();
                let replay_buffer = replay_buffer.clone();
                let notification_tx = notification_tx.clone();
                let username = params.username.clone();
                async move {
                    while let Some(data) = input_receiver.recv().await {
                        if data.contains(&0x03) {
                            // CTRL+C found, start graceful shutdown
                            graceful_shutdown_token.cancel();
                        }
                        if data.contains(&0x12) {
                            // CTRL+R found, save replay and filter it out
                            let asciicast = replay_buffer.lock().await.serialize_asciicast();
                            let _ = notification_tx.send(" \x1b[3mUploading replay... ".to_string()).await;
                            let username = username.clone();
                            let notification_tx = notification_tx.clone();
                            tokio::spawn(async move {
                                let notification = match upload_asciicast(&username, &asciicast).await {
                                    Ok(url) => {
                                        tracing::info!(%url, "Uploaded replay");
                                        format!(" \x1b[3mReplay saved: \x1b]8;;{}\x1b\\{}\x1b]8;;\x1b\\ ", url, url)
                                    }
                                    Err(e) => {
                                        tracing::error!(error = %e, "Failed to upload replay");
                                        format!(" Upload failed: {} ", e)
                                    }
                                };
                                let _ = notification_tx.send(notification).await;
                            });
                            let filtered: SmallVec<[u8; 16]> =
                                data.iter().copied().filter(|&b| b != 0x12).collect();
                            if !filtered.is_empty() {
                                let _ = filtered_input_tx.send(filtered).await;
                            }
                        } else {
                            let _ = filtered_input_tx.send(data).await;
                        }
                    }
                }
            });
            let mut input_receiver = filtered_input_rx;

            let audio_buffer = Arc::new(AudioBuffer::new(SAMPLE_RATE as usize));

            let audio_enabled = audio_tx.is_some();
            if let Some(audio_tx) = audio_tx {
                let audio_buffer = audio_buffer.clone();
                let hard_shutdown_token = hard_shutdown_token.clone();
                std::thread::spawn(move || {
                    let mut mixer = match Mixer::new(audio_tx, audio_buffer) {
                        Ok(mixer) => mixer,
                        Err(error) => {
                            tracing::error!(?error, "Failed to create mixer");
                            return;
                        }
                    };
                    _ = mixer.run(hard_shutdown_token);
                });
            }

            // output task
            tokio::task::spawn({
                let hard_shutdown_token = hard_shutdown_token.clone();
                let graceful_shutdown_token = params.graceful_shutdown_token.clone();
                let output_sender = params.output_sender;
                let username = params.username.clone();
                let first_app_shortname = first_app_shortname.clone();
                let has_next_app_shortname = has_next_app_shortname.clone();
                let terminal = terminal.clone();
                let network_info = network_info.clone();
                let replay_buffer = replay_buffer.clone();
                async move {
                    let mut escape_buffer = EscapeSequenceBuffer::new();
                    let mut status_bar_interval = tokio::time::interval(Duration::from_secs(1));
                    let mut status_bar =
                        StatusBar::new(first_app_shortname, username.clone(), network_info, notification_rx);

                    loop {
                        tokio::select! {
                            biased;

                            _ = hard_shutdown_token.cancelled() => {
                                break;
                            }

                            _ = graceful_shutdown_token.cancelled() => {
                                tokio::select! {
                                    _ = tokio::time::sleep(std::time::Duration::from_millis(5000)) => {
                                        hard_shutdown_token.cancel();
                                    }
                                    _ = hard_shutdown_token.cancelled() => {
                                        break;
                                    }
                                }
                            }

                            data = app_output_receiver.recv() => {
                                let Some(mut data) = data else { continue };

                                if has_next_app_shortname.load(Ordering::Acquire) {
                                    // strip exiting the alternate screen buffer to make
                                    // sure transitions between apps are smooth
                                    let needle = b"\x1b[?1049l"; // leave alternate screen buffer
                                    let needle2 = b"\x1b[?25h"; // show cursor
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

                                {
                                    let mut terminal = terminal.lock().await;
                                    terminal.process(&output);
                                    let screen = terminal.screen_mut();
                                    let (height, _width) = screen.size();
                                    let dirty = screen.take_damaged_rows();
                                    if dirty.contains(&(height - 1)) {
                                        status_bar.maybe_render_into(screen, &mut output, true);
                                    }
                                }

                                replay_buffer.lock().await.push_output(output.clone());
                                let _ = output_sender.send(output).await;
                            }

                            result = window_size_receiver.changed() => {
                                if let Err(_) = result { break };
                                let (width, height) = *window_size_receiver.borrow();

                                replay_buffer.lock().await.push_resize(width, height);

                                let mut output = Vec::new();
                                {
                                    let mut terminal = terminal.lock().await;
                                    let screen = terminal.screen_mut();
                                    screen.set_size(height, width);
                                    status_bar.maybe_render_into(screen, &mut output, true);
                                }
                                if !output.is_empty() {
                                    replay_buffer.lock().await.push_output(output.clone());
                                    let _ = output_sender.send(output).await;
                                }
                            }

                            _ = status_bar_interval.tick() => {
                                let mut output = Vec::new();
                                status_bar.maybe_render_into(terminal.lock().await.screen(), &mut output, false);
                                if !output.is_empty() {
                                    replay_buffer.lock().await.push_output(output.clone());
                                    let _ = output_sender.send(output).await;
                                }
                            }

                            shortname = shortname_receiver.recv() => {
                                if let Some(ref shortname) = shortname {
                                    replay_buffer.lock().await.push_app_switch(shortname.clone());
                                    status_bar.shortname = shortname.clone();
                                }
                            }
                        }
                    }
                }
            });

            let mut exit_code = I32Exit(0);
            let mut ctx = Arc::new(AppContext {
                db,
                linker,
                mesh,
                app_output_sender,
                remote_sshid: params.remote_sshid,
                term: params.term,
                username: params.username,
                audio_enabled,
            });
            let (mut app, mut instance_pre) = Self::prepare_instantiate(&ctx, first_app_shortname)
                .await
                .unwrap();

            let mut graceful_shutdown_token = params.graceful_shutdown_token;
            loop {
                let state = AppState {
                    app,
                    ctx,
                    conn_manager: ConnectionManager::default(),
                    limits: AppLimiter::default(),
                    terminal: terminal,
                    next_app: None,
                    input_receiver: input_receiver,
                    has_next_app: has_next_app_shortname.clone(),
                    graceful_shutdown_token,
                    network_info: network_info.clone(),
                    audio_buffer: audio_buffer.clone(),
                };

                let mut store = wasmtime::Store::new(&engine, state);
                store.limiter(|state| &mut state.limits);
                store.epoch_deadline_callback(|_| Ok(wasmtime::UpdateDeadline::Yield(1)));

                let call_result = {
                    let func = {
                        let instance = instance_pre.instantiate_async(&mut store).await.unwrap();
                        let func = instance
                            .get_typed_func::<(), ()>(&mut store, "_start")
                            .unwrap();
                        func
                    };

                    has_next_app_shortname.store(false, Ordering::Release);
                    tokio::select! {
                        _ = hard_shutdown_token.cancelled() => {
                            break;
                        }

                        result = func.call_async(&mut store, ()) => {
                            result
                        }
                    }
                };

                if let Err(err) = call_result {
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
                terminal = state.terminal;
                ctx = state.ctx;

                if let Some(next_app) = state.next_app.take() {
                    let (next_app, next_instance_pre) = next_app.await.unwrap();
                    shortname_sender
                        .send(next_app.shortname.clone())
                        .await
                        .unwrap();
                    app = next_app;
                    instance_pre = next_instance_pre;
                    continue;
                }

                break;
            }

            hard_shutdown_token.cancel();
            let _ = exit_tx.send(exit_code);
        });

        return exit_rx;
    }

    async fn prepare_instantiate(
        ctx: &AppContext,
        shortname: String,
    ) -> anyhow::Result<(PreloadedAppState, wasmtime::InstancePre<AppState>)> {
        let app_id = ctx
            .db
            .query(
                "SELECT id FROM games WHERE shortname = ?1",
                [shortname.as_str()],
            )
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<u64>(0)
            .unwrap();

        let (peer_id, peer_rx, peer_tx) = ctx.mesh.new_peer(AppId(app_id)).await;

        let mut envs = vec![];
        let mut rows = ctx
            .db
            .query("SELECT name, value FROM envs WHERE game_id = ?1", [app_id])
            .await
            .unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            let name = row.get(0).unwrap();
            let value = row.get(1).unwrap();
            envs.push((name, value));
        }
        envs.push(("REMOTE_SSHID".to_string(), ctx.remote_sshid.clone()));
        envs.push(("USERNAME".to_string(), ctx.username.clone()));
        envs.push(("PEER_ID".to_string(), peer_id.to_bytes().encode_hex()));
        envs.push(("APP_SHORTNAME".to_string(), shortname.to_string()));
        envs.push((
            "AUDIO_ENABLED".to_string(),
            if ctx.audio_enabled { "1" } else { "0" }.to_string(),
        ));
        if let Some(term) = ctx.term.clone() {
            envs.push(("TERM".to_string(), term));
        }

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
            .await
            .unwrap();

        let row = rows.next().await.unwrap().unwrap();
        let wasm_path: String = row.get(0).unwrap();
        let module = wasmtime::Module::from_file(ctx.linker.engine(), wasm_path).unwrap();
        let instance_pre = ctx.linker.instantiate_pre(&module).unwrap();

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

    fn host_dial(
        mut caller: wasmtime::Caller<'_, AppState>,
        address_ptr: i32,
        address_len: u32,
        mode: u32,
    ) -> anyhow::Result<i32> {
        let mut buf = [0u8; 256];
        if address_len as usize >= buf.len() {
            return Ok(DIAL_ERR_ADDRESS_TOO_LONG);
        }
        let len = address_len as usize;
        let offset = address_ptr as usize;

        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            anyhow::bail!("dial: failed to find host memory");
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
            let hostname: String = address
                .split(':')
                .next()
                .unwrap_or(&address)
                .to_string();

            let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();
            root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let mut config = tokio_rustls::rustls::ClientConfig::builder()
                .with_root_certificates(root_cert_store)
                .with_no_client_auth();
            config.alpn_protocols.push(b"h2".to_vec());
            let connector = tokio_rustls::TlsConnector::from(Arc::new(config));

            let dnsname = match rustls_pki_types::ServerName::try_from(hostname) {
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
    ) -> anyhow::Result<i32> {
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
            .unwrap();

        match pending.receiver.try_recv() {
            Ok(Ok((stream, local_addr, remote_addr))) => {
                let conn_manager = &mut caller.data_mut().conn_manager;
                let slot = conn_manager.find_free_conn_slot();

                if slot.is_none() {
                    return Ok(POLL_DIAL_ERR_TOO_MANY_CONNECTIONS);
                }

                let slot_id = slot.unwrap();
                let conn = Connection { stream };

                if slot_id < conn_manager.connections.len() {
                    conn_manager.connections[slot_id] = Some(conn);
                } else {
                    conn_manager.connections.push(Some(conn));
                }

                let local_addr_str = local_addr.to_string();
                let remote_addr_str = remote_addr.to_string();

                let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                    anyhow::bail!("failed to find host memory");
                };

                mem.write(&mut caller, local_addr_ptr as usize, local_addr_str.as_bytes())?;
                mem.write(
                    &mut caller,
                    local_addr_len_ptr as usize,
                    &(local_addr_str.len() as u32).to_le_bytes(),
                )?;
                mem.write(&mut caller, remote_addr_ptr as usize, remote_addr_str.as_bytes())?;
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
            Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                Ok(POLL_DIAL_ERR_TASK_FAILED)
            }
        }
    }

    fn conn_close(mut caller: wasmtime::Caller<'_, AppState>, conn_id: i32) -> anyhow::Result<i32> {
        let conn_id_usize = conn_id as usize;
        let connections = &mut caller.data_mut().conn_manager.connections;

        if conn_id_usize >= connections.len() || connections[conn_id_usize].is_none() {
            return Ok(CONN_ERR_INVALID_CONN_ID);
        }

        connections[conn_id_usize] = None;
        tracing::info!(conn_id, "connection closed");
        Ok(0)
    }

    fn conn_write(
        mut caller: wasmtime::Caller<'_, AppState>,
        conn_id: i32,
        data_ptr: i32,
        data_len: u32,
    ) -> anyhow::Result<i32> {
        let mut buf = [0u8; 4 * 1024];
        let len = (data_len as usize).min(buf.len());
        let offset = data_ptr as usize;

        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            anyhow::bail!("failed to find host memory");
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
            Ok(n) => {
                if n > 0 {
                    tracing::debug!(n, "conn_write");
                }
                Ok(n as i32)
            }
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
    ) -> anyhow::Result<i32> {
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
            let conn = connections[conn_id_usize].as_mut().unwrap();

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
            anyhow::bail!("failed to find host memory");
        };

        mem.write(&mut caller, ptr as usize, &buf[..n])?;

        if n > 0 {
            tracing::debug!(n, "conn_read");
        }

        Ok(n as i32)
    }

    fn terminal_read(
        mut caller: wasmtime::Caller<'_, AppState>,
        ptr: i32,
        _len: u32,
    ) -> anyhow::Result<i32> {
        match caller.data_mut().input_receiver.try_recv() {
            Ok(buf) => {
                let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                    anyhow::bail!("terminal_read: failed to find host memory");
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
        (width_ptr, height_ptr): (i32, i32),
    ) -> Box<dyn Future<Output = anyhow::Result<()>> + Send + '_> {
        Box::new(async move {
            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                anyhow::bail!("terminal_size: failed to find host memory");
            };
            let (height, width) = caller.data().terminal.lock().await.screen().size();
            let effective_height = if height > 0 { height - 1 } else { 0 };

            let width_offset = width_ptr as u32 as usize;
            mem.write(&mut caller, width_offset, &width.to_le_bytes())?;

            let height_offset = height_ptr as u32 as usize;
            mem.write(&mut caller, height_offset, &effective_height.to_le_bytes())?;

            Ok(())
        })
    }

    fn terminal_cursor(
        mut caller: wasmtime::Caller<'_, AppState>,
        (x_ptr, y_ptr): (i32, i32),
    ) -> Box<dyn Future<Output = anyhow::Result<()>> + Send + '_> {
        Box::new(async move {
            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                anyhow::bail!("terminal_cursor: failed to find host memory");
            };
            let (y, x) = caller
                .data()
                .terminal
                .lock()
                .await
                .screen()
                .cursor_position();

            let x_offset = x_ptr as u32 as usize;
            mem.write(&mut caller, x_offset, &x.to_le_bytes())?;

            let y_offset = y_ptr as u32 as usize;
            mem.write(&mut caller, y_offset, &y.to_le_bytes())?;

            Ok(())
        })
    }

    fn change_app(
        mut caller: wasmtime::Caller<'_, AppState>,
        (ptr, len): (i32, u32),
    ) -> Box<dyn Future<Output = anyhow::Result<i32>> + Send + '_> {
        Box::new(async move {
            let len = len as usize;
            if len == 0 || len > 128 {
                return Ok(-1);
            }

            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                anyhow::bail!("change_app: failed to find host memory");
            };

            let mut buf = vec![0u8; len];
            let offset = ptr as usize;
            mem.read(&caller, offset, &mut buf)?;

            let shortname = match String::from_utf8(buf) {
                Ok(s) => s,
                Err(_) => return Ok(-1),
            };

            let has_next_app = caller.data().has_next_app.clone();
            if caller.data().next_app.is_none() || has_next_app.load(Ordering::Acquire) {
                has_next_app.store(false, Ordering::Release);
                let ctx = caller.data().ctx.clone();
                let (tx, rx) = tokio::sync::oneshot::channel();
                caller.data_mut().next_app = Some(rx);
                tokio::task::spawn(async move {
                    let next_app = Self::prepare_instantiate(&ctx, shortname).await.unwrap();
                    let _ = tx.send(next_app);
                    has_next_app.store(true, Ordering::Release);
                });
            }

            Ok(0)
        })
    }

    fn next_app_ready(
        caller: wasmtime::Caller<'_, AppState>,
        (): (),
    ) -> Box<dyn Future<Output = anyhow::Result<i32>> + Send + '_> {
        Box::new(async move {
            let ready = caller.data().has_next_app.load(Ordering::Acquire);
            Ok(if ready { 1 } else { 0 })
        })
    }

    fn peer_send(
        mut caller: wasmtime::Caller<'_, AppState>,
        peer_ids_ptr: i32,
        peer_ids_count: u32,
        data_ptr: i32,
        data_len: u32,
    ) -> anyhow::Result<i32> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            anyhow::bail!("peer_send: failed to find host memory");
        };

        if peer_ids_count == 0 || peer_ids_count > 1024 {
            return Ok(PEER_SEND_ERR_INVALID_PEER_COUNT);
        }

        if data_len == 0 {
            // not sure why someone would do this but it's pretty easy to send no data
            return Ok(0);
        }

        if data_len > 64 * 1024 {
            return Ok(PEER_SEND_ERR_DATA_TOO_LARGE);
        }
        const PEER_ID_SIZE: usize = std::mem::size_of::<PeerId>();
        let peer_ids_offset = peer_ids_ptr as usize;
        let peer_ids_count = peer_ids_count as usize;
        let total_peer_ids_size = peer_ids_count * std::mem::size_of::<PeerId>();

        let mut peer_ids_buf = vec![0u8; total_peer_ids_size];
        mem.read(&caller, peer_ids_offset, &mut peer_ids_buf)?;

        let mut peer_ids = Vec::with_capacity(peer_ids_count);
        for i in 0..peer_ids_count {
            let offset = i * PEER_ID_SIZE;
            let mut peer_id_bytes = [0u8; PEER_ID_SIZE];
            peer_id_bytes.copy_from_slice(&peer_ids_buf[offset..offset + PEER_ID_SIZE]);
            peer_ids.push(crate::mesh::PeerId::from_bytes(peer_id_bytes));
        }

        let data_offset = data_ptr as usize;
        let data_len = data_len as usize;
        let mut data_buf = vec![0u8; data_len];
        mem.read(&caller, data_offset, &mut data_buf)?;

        match caller.data_mut().app.peer_tx.try_send((peer_ids, data_buf)) {
            Ok(_) => {
                tracing::debug!("peer_send: sent message to {} peers", peer_ids_count);
                Ok(0)
            }
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
    ) -> anyhow::Result<i32> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            anyhow::bail!("peer_recv: failed to find host memory");
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

        tracing::debug!("peer_recv: received message of {} bytes", data_to_write);
        Ok(data_to_write as i32)
    }

    fn region_latency(
        mut caller: wasmtime::Caller<'_, AppState>,
        (region_ptr,): (i32,),
    ) -> Box<dyn Future<Output = anyhow::Result<i32>> + Send + '_> {
        Box::new(async move {
            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                anyhow::bail!("region_latency: failed to find host memory");
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
    ) -> Box<dyn Future<Output = anyhow::Result<i32>> + Send + '_> {
        Box::new(async move {
            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                anyhow::bail!("peer_list: failed to find host memory");
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

    fn graceful_shutdown_poll(caller: wasmtime::Caller<'_, AppState>) -> anyhow::Result<i32> {
        let is_cancelled = caller.data().graceful_shutdown_token.is_cancelled();
        Ok(if is_cancelled { 1 } else { 0 })
    }

    fn host_network_info(
        mut caller: wasmtime::Caller<'_, AppState>,
        bytes_per_sec_in_ptr: i32,
        bytes_per_sec_out_ptr: i32,
        last_throttled_ms_ptr: i32,
        latency_ms_ptr: i32,
    ) -> anyhow::Result<i32> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            anyhow::bail!("network_info: failed to find host memory");
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

    fn audio_write(
        mut caller: wasmtime::Caller<'_, AppState>,
        ptr: i32,
        sample_count: u32,
    ) -> anyhow::Result<i32> {
        if sample_count == 0 {
            return Ok(0);
        }

        let sample_count = sample_count.min(SAMPLE_RATE) as usize;
        let float_count = sample_count * CHANNELS;
        let byte_count = float_count * std::mem::size_of::<f32>();

        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            anyhow::bail!("audio_write: failed to find host memory");
        };

        let mut buf = vec![0u8; byte_count];
        let offset = ptr as usize;
        mem.read(&caller, offset, &mut buf)?;

        let samples: Vec<f32> = buf
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect();

        let written = caller.data().audio_buffer.write(&samples);

        Ok(written as i32)
    }

    fn audio_info(
        mut caller: wasmtime::Caller<'_, AppState>,
        frame_size_ptr: i32,
        sample_rate_ptr: i32,
        pts_ptr: i32,
        buffer_available_ptr: i32,
    ) -> anyhow::Result<i32> {
        let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
            anyhow::bail!("audio_info: failed to find host memory");
        };

        let data = caller.data();
        let pts = data.audio_buffer.pts.load(Ordering::Acquire);
        let buffer_available = data.audio_buffer.available();

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

pub struct AppContext {
    db: libsql::Connection,
    mesh: Mesh,
    remote_sshid: String,
    username: String,
    audio_enabled: bool,
    term: Option<String>,
    linker: Arc<wasmtime::Linker<AppState>>,
    app_output_sender: tokio::sync::mpsc::Sender<Vec<u8>>,
}

pub struct AppState {
    app: PreloadedAppState,
    ctx: Arc<AppContext>,
    conn_manager: ConnectionManager,
    limits: AppLimiter,
    terminal: Arc<Mutex<Terminal>>,
    next_app: Option<
        tokio::sync::oneshot::Receiver<(PreloadedAppState, wasmtime::InstancePre<AppState>)>,
    >,
    has_next_app: Arc<AtomicBool>,
    input_receiver: tokio::sync::mpsc::Receiver<smallvec::SmallVec<[u8; 16]>>,
    graceful_shutdown_token: CancellationToken,
    network_info: Arc<dyn NetworkInfo>,
    audio_buffer: Arc<AudioBuffer>,
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

pub struct Connection {
    stream: Stream,
}

struct AppLimiter {
    total: usize,
}

impl Default for AppLimiter {
    fn default() -> Self {
        AppLimiter { total: 0 }
    }
}

impl wasmtime::ResourceLimiter for AppLimiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> anyhow::Result<bool> {
        tracing::trace!(current, desired, maximum, "memory growing");
        self.total -= current;
        self.total += desired;
        if self.total >= 32 * 1024 * 1024 {
            tracing::trace!(total = self.total, "rejected memory grow");
            return Ok(false);
        }

        return Ok(true);
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> anyhow::Result<bool> {
        tracing::trace!(current, desired, maximum, "table growing");
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

pub struct AsyncStdoutWriter {
    sender: tokio_util::sync::PollSender<Vec<u8>>,
}

impl AsyncStdoutWriter {
    pub fn new(sender: tokio::sync::mpsc::Sender<Vec<u8>>) -> Self {
        Self {
            sender: tokio_util::sync::PollSender::new(sender),
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
                let data = buf.to_vec();
                let len = data.len();
                let _ = self.sender.send_item(data);
                Poll::Ready(Ok(len))
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "channel closed",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
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

pub type Terminal = headless_terminal::Parser;

fn get_install_id() -> &'static str {
    static INSTALL_ID: OnceLock<String> = OnceLock::new();
    INSTALL_ID.get_or_init(|| uuid::Uuid::new_v4().to_string())
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
        "192.88.99.0/24", // Deprecated (6to4 Relay Anycast)
        "0.0.0.0/8", // "This network"
        "0.0.0.0/32", // "This host on this network"
        "10.0.0.0/8", // Private-Use
        "100.64.0.0/10", // Shared Address Space
        "169.254.0.0/16", // Link Local
        "172.16.0.0/12", // Private-Use
        "192.0.0.0/24", // IETF Protocol Assignments
        "192.0.0.0/29", // IPv4 Service Continuity Prefix
        "192.0.0.8/32", // IPv4 dummy address
        "192.0.0.170/32", // NAT64/DNS64 Discovery
        "192.0.0.171/32", // NAT64/DNS64 Discovery
        "192.0.2.0/24", // Documentation (TEST-NET-1)
        "192.88.99.2/32", // 6a44-relay anycast address
        "192.168.0.0/16", // Private-Use
        "198.18.0.0/15", // Benchmarking
        "198.51.100.0/24", // Documentation (TEST-NET-2)
        "203.0.113.0/24", // Documentation (TEST-NET-3)
        "240.0.0.0/4", // Reserved
        "255.255.255.255/32", // Limited Broadcast
        "127.0.0.0/8", // Loopback

        "224.0.0.0/4", // Multicast
    ]
    .into_iter()
    .map(|s| s.parse().expect("valid IPv4 CIDR"))
    .collect()
}

fn init_ipv6_non_global() -> Vec<Ipv6Net> {
    [
        // https://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry.xhtml
        "::1/128", // Loopback Address
        "::/128", // Unspecified Address
        "::ffff:0:0/96", // IPv4-mapped Address
        "64:ff9b:1::/48", // IPv4-IPv6 Translat.
        "100::/64", // Discard-Only Address Block
        "100:0:0:1::/64", // Dummy IPv6 Prefix
        "2001:2::/48", // Benchmarking
        "2001:db8::/32", // 2001:db8::/32
        "3fff::/20", // Documentation
        "5f00::/16", // Segment Routing (SRv6) SIDs
        "fe80::/10", // Link-Local Unicast
        "2001::/23", // IETF Protocol Assignments
        "fc00::/7", // Unique-Local
        "2001::/32", // TEREDO
        "2002::/16", // 6to4

        "ff00::/8", // Multicast
    ]
    .into_iter()
    .map(|s| s.parse().expect("valid IPv6 CIDR"))
    .collect()
}
