// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use hex::ToHex;
use smallvec::SmallVec;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use wasmtime_wasi::I32Exit;

use crate::{
    mesh::{AppId, Mesh, PeerId, PeerMessageApp, RegionId},
    status_bar::StatusBar,
};

pub struct AppServer {
    linker: Arc<wasmtime::Linker<AppState>>,
    engine: wasmtime::Engine,
    pub db: libsql::Connection,
    mesh: Mesh,
}

pub struct AppInstantiationParams {
    pub input_receiver: tokio::sync::mpsc::Receiver<SmallVec<[u8; 16]>>,
    pub output_sender: tokio::sync::mpsc::Sender<Vec<u8>>,
    pub window_size_receiver: tokio::sync::mpsc::Receiver<(u16, u16)>,
    pub graceful_shutdown_token: CancellationToken,
    pub username: String,
    pub remote_sshid: String,
    pub term: Option<String>,
    pub args: Option<Vec<u8>>,
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

        linker.func_wrap_async("terminal_games", "dial", Self::host_dial)?;
        linker.func_wrap_async("terminal_games", "conn_write", Self::conn_write)?;
        linker.func_wrap_async("terminal_games", "conn_read", Self::conn_read)?;
        linker.func_wrap("terminal_games", "terminal_read", Self::terminal_read)?;
        linker.func_wrap_async("terminal_games", "terminal_size", Self::terminal_size)?;
        linker.func_wrap_async("terminal_games", "terminal_cursor", Self::terminal_cursor)?;
        linker.func_wrap_async("terminal_games", "change_app", Self::change_app)?;
        linker.func_wrap_async("terminal_games", "next_app_ready", Self::next_app_ready)?;
        linker.func_wrap("terminal_games", "peer_send", Self::peer_send)?;
        linker.func_wrap("terminal_games", "peer_recv", Self::peer_recv)?;
        linker.func_wrap_async("terminal_games", "region_latency", Self::region_latency)?;
        linker.func_wrap_async("terminal_games", "peer_list", Self::peer_list)?;

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
            let mut terminal = Arc::new(Mutex::new(headless_terminal::Parser::new(0, 0, 0)));
            let (app_output_sender, mut app_output_receiver) =
                tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
            let mut input_receiver = params.input_receiver;
            let has_next_app_shortname = Arc::new(AtomicBool::new(false));
            let (shortname_sender, mut shortname_receiver) =
                tokio::sync::mpsc::channel::<String>(1);

            let hard_shutdown_token = CancellationToken::new();

            let (first_app_shortname, _app_args) = match params.args {
                None => ("menu".to_string(), None),
                Some(args_bytes) => {
                    let args_str = String::from_utf8_lossy(&args_bytes);
                    if let Some(space_idx) = args_str.find(' ') {
                        let shortname = args_str[..space_idx].to_string();
                        let remaining_args = args_str[space_idx + 1..].to_string();
                        (shortname, Some(remaining_args))
                    } else {
                        (args_str.to_string(), None)
                    }
                }
            };

            // output task
            tokio::task::spawn({
                let hard_shutdown_token = hard_shutdown_token.clone();
                let graceful_shutdown_token = params.graceful_shutdown_token;
                let output_sender = params.output_sender;
                let username = params.username.clone();
                let first_app_shortname = first_app_shortname.clone();
                let has_next_app_shortname = has_next_app_shortname.clone();
                let terminal = terminal.clone();
                async move {
                    let mut escape_buffer = EscapeSequenceBuffer::new();
                    let mut status_bar_interval = tokio::time::interval(Duration::from_secs(1));
                    let mut status_bar = StatusBar::new(first_app_shortname, username);

                    loop {
                        tokio::select! {
                            biased;

                            _ = hard_shutdown_token.cancelled() => {
                                tracing::info!("hard shutdown 1");
                                break;
                            }

                            _ = graceful_shutdown_token.cancelled() => {
                                tracing::info!("initiating graceful shutdown");
                                tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
                                hard_shutdown_token.cancel();
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

                                let _ = output_sender.send(output).await;
                            }

                            result = window_size_receiver.recv() => {
                                let Some((width, height)) = result else { continue };

                                let mut output = Vec::new();
                                {
                                    let mut terminal = terminal.lock().await;
                                    let screen = terminal.screen_mut();
                                    screen.set_size(height, width);
                                    status_bar.maybe_render_into(screen, &mut output, true);
                                }
                                if !output.is_empty() {
                                    let _ = output_sender.send(output).await;
                                }
                            }

                            _ = status_bar_interval.tick() => {
                                let mut output = Vec::new();
                                status_bar.maybe_render_into(terminal.lock().await.screen(), &mut output, false);
                                if !output.is_empty() {
                                    let _ = output_sender.send(output).await;
                                }
                            }

                            shortname = shortname_receiver.recv() => {
                                if let Some(shortname) = shortname {
                                    status_bar.shortname = shortname;
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
            });
            let (mut app, mut instance_pre) = Self::prepare_instantiate(&ctx, first_app_shortname).await.unwrap();

            loop {
                let state = AppState {
                    app,
                    ctx,
                    streams: Vec::default(),
                    limits: AppLimiter::default(),
                    terminal: terminal,
                    next_app: None,
                    input_receiver: input_receiver,
                    has_next_app: has_next_app_shortname.clone(),
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
                            tracing::info!("hard shutdown 2");
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
                            tracing::info!(?code, "exit");
                            exit_code = code;
                        }
                        Err(other) => {
                            tracing::error!(error = %other, "Module errored");
                        }
                    }
                }

                let mut state = store.into_data();
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

    async fn prepare_instantiate(ctx: &AppContext, shortname: String) -> anyhow::Result<(PreloadedAppState, wasmtime::InstancePre<AppState>)> {
        let app_id = ctx.db
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
        let mut rows = ctx.db
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
        if let Some(term) = ctx.term.clone() {
            envs.push(("TERM".to_string(), term));
        }

        let wasi_ctx = wasmtime_wasi::WasiCtx::builder()
            .stdout(MyStdoutStream {
                sender: ctx.app_output_sender.clone(),
            })
            .envs(&envs)
            .build_p1();

        let mut rows = ctx.db
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

        Ok((PreloadedAppState { wasi_ctx, peer_rx, peer_tx, app_id: AppId(app_id), shortname }, instance_pre))
    }

    fn host_dial(
        mut caller: wasmtime::Caller<'_, AppState>,
        (address_ptr, address_len, mode): (i32, u32, u32),
    ) -> Box<dyn Future<Output = anyhow::Result<i32>> + Send + '_> {
        Box::new(async move {
            let mut buf = [0u8; 64];
            if address_len >= buf.len() as u32 {
                anyhow::bail!("dial address too long")
            }
            let len = address_len as usize;
            let offset = address_ptr as usize;

            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                anyhow::bail!("failed to find host memory");
            };

            if let Err(_) = mem.read(&mut caller, offset, &mut buf[..len]) {
                anyhow::bail!("failed to write to host memory");
            }
            let address = String::from_utf8_lossy(&buf[..len]);

            let tcp_stream = match tokio::net::TcpStream::connect(address.as_ref()).await {
                Ok(stream) => stream,
                Err(_) => {
                    tracing::error!("tcp_stream");
                    return Ok(-1);
                }
            };

            if let Err(e) = tcp_stream.set_nodelay(true) {
                tracing::warn!("Failed to set nodelay: {:?}", e);
            }

            let stream = if mode == 1 {
                let hostname: String = address.split(':').next().unwrap_or(&address).to_string();

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
                        tracing::error!("dnsname");
                        return Ok(-1);
                    }
                };

                let tls_stream = match connector.connect(dnsname, tcp_stream).await {
                    Ok(stream) => stream,
                    Err(_) => {
                        tracing::error!("tls_stream");
                        return Ok(-1);
                    }
                };

                Stream::Tls(tls_stream)
            } else {
                Stream::Tcp(tcp_stream)
            };

            caller.data_mut().streams.push(stream);

            tracing::info!(mode, "dial");

            Ok((caller.data().streams.len() - 1) as i32)
        })
    }

    fn conn_write(
        mut caller: wasmtime::Caller<'_, AppState>,
        (conn_id, address_ptr, address_len): (i32, i32, u32),
    ) -> Box<dyn Future<Output = anyhow::Result<i32>> + Send + '_> {
        Box::new(async move {
            let mut buf = [0u8; 4 * 1024];
            if address_len >= buf.len() as u32 {
                tracing::error!("conn_write: address too long");
                return Ok(-1);
            }
            let len = address_len as usize;
            let offset = address_ptr as usize;

            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                tracing::error!("conn_write: failed to find host memory");
                return Ok(-1);
            };

            if let Err(_) = mem.read(&caller, offset, &mut buf[..len]) {
                tracing::error!("conn_write: failed to read from host memory");
                return Ok(-1);
            }

            let Some(stream) = caller.data_mut().streams.get_mut(conn_id as usize) else {
                tracing::error!("conn_write: invalid conn_id: {}", conn_id);
                return Ok(-1);
            };

            tracing::info!(len, "conn_write");

            match stream.write(&buf[..len]).await {
                Ok(n) => Ok(n as i32),
                Err(e) => {
                    tracing::error!("conn_write: stream write failed: {:?}", e);
                    Ok(-1)
                }
            }
        })
    }

    fn conn_read(
        mut caller: wasmtime::Caller<'_, AppState>,
        (conn_id, ptr, len): (i32, i32, u32),
    ) -> Box<dyn Future<Output = anyhow::Result<i32>> + Send + '_> {
        Box::new(async move {
            let Some(stream) = caller.data_mut().streams.get_mut(conn_id as usize) else {
                tracing::error!("conn_read: invalid conn_id: {}", conn_id);
                return Ok(-1);
            };

            let mut buf = [0u8; 4 * 1024];
            let len = std::cmp::min(buf.len(), len as usize);

            let n = match stream.try_read(&mut buf[..len]) {
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    return Ok(0);
                }
                Err(e) => {
                    tracing::error!("conn_read: stream read error: {:?}", e);
                    return Ok(-1);
                }
            };

            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                tracing::error!("conn_read: failed to find host memory");
                return Ok(-1);
            };
            let offset = ptr as u32 as usize;
            if let Err(_) = mem.write(&mut caller, offset, &buf[..n]) {
                tracing::error!("conn_read: failed to write to host memory");
                return Ok(-1);
            }
            if n > 0 {
                tracing::info!(n, "conn_read");
            }

            Ok(n as i32)
        })
    }

    fn terminal_read(
        mut caller: wasmtime::Caller<'_, AppState>,
        ptr: i32,
        _len: u32,
    ) -> anyhow::Result<i32> {
        match caller.data_mut().input_receiver.try_recv() {
            Ok(buf) => {
                let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                    anyhow::bail!("failed to find host memory");
                };
                let offset = ptr as u32 as usize;
                if let Err(_) = mem.write(&mut caller, offset, buf.as_ref()) {
                    anyhow::bail!("failed to write to host memory");
                }
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
                anyhow::bail!("failed to find host memory");
            };
            let (height, width) = caller.data().terminal.lock().await.screen().size();
            let effective_height = if height > 0 { height - 1 } else { 0 };

            let width_offset = width_ptr as u32 as usize;
            if let Err(_) = mem.write(&mut caller, width_offset, &width.to_le_bytes()) {
                anyhow::bail!("failed to write to host memory");
            }

            let height_offset = height_ptr as u32 as usize;
            if let Err(_) = mem.write(&mut caller, height_offset, &effective_height.to_le_bytes()) {
                anyhow::bail!("failed to write to host memory");
            }

            Ok(())
        })
    }

    fn terminal_cursor(
        mut caller: wasmtime::Caller<'_, AppState>,
        (x_ptr, y_ptr): (i32, i32),
    ) -> Box<dyn Future<Output = anyhow::Result<()>> + Send + '_> {
        Box::new(async move {
            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                anyhow::bail!("failed to find host memory");
            };
            let (y, x) = caller
                .data()
                .terminal
                .lock()
                .await
                .screen()
                .cursor_position();

            let x_offset = x_ptr as u32 as usize;
            if let Err(_) = mem.write(&mut caller, x_offset, &x.to_le_bytes()) {
                anyhow::bail!("failed to write to host memory");
            }

            let y_offset = y_ptr as u32 as usize;
            if let Err(_) = mem.write(&mut caller, y_offset, &y.to_le_bytes()) {
                anyhow::bail!("failed to write to host memory");
            }

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
                anyhow::bail!("failed to find host memory");
            };

            let mut buf = vec![0u8; len];
            let offset = ptr as usize;
            if let Err(_) = mem.read(&caller, offset, &mut buf) {
                anyhow::bail!("failed to read from guest memory");
            }

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
            tracing::error!("peer_send: failed to find host memory");
            return Ok(-1);
        };

        if peer_ids_count == 0 || peer_ids_count > 1024 {
            tracing::error!("peer_send: invalid peer_ids_count: {}", peer_ids_count);
            return Ok(-1);
        }

        if data_len == 0 {
            // not sure why someone would do this but it's pretty easy to send no data
            return Ok(0);
        }

        if data_len > 64 * 1024 {
            tracing::error!("peer_send: data_len too large: {}", data_len);
            return Ok(-1);
        }
        const PEER_ID_SIZE: usize = std::mem::size_of::<PeerId>();
        let peer_ids_offset = peer_ids_ptr as usize;
        let peer_ids_count = peer_ids_count as usize;
        let total_peer_ids_size = peer_ids_count * std::mem::size_of::<PeerId>();

        let mut peer_ids_buf = vec![0u8; total_peer_ids_size];
        if let Err(_) = mem.read(&caller, peer_ids_offset, &mut peer_ids_buf) {
            tracing::error!("peer_send: failed to read peer IDs from memory");
            return Ok(-1);
        }

        let mut peer_ids = Vec::with_capacity(peer_ids_count);
        for i in 0..peer_ids_count {
            let offset = i * PEER_ID_SIZE;
            if offset + PEER_ID_SIZE > peer_ids_buf.len() {
                tracing::error!("peer_send: peer ID buffer overflow");
                return Ok(-1);
            }

            let mut peer_id_bytes = [0u8; PEER_ID_SIZE];
            peer_id_bytes.copy_from_slice(&peer_ids_buf[offset..offset + PEER_ID_SIZE]);
            peer_ids.push(crate::mesh::PeerId::from_bytes(peer_id_bytes));
        }

        let data_offset = data_ptr as usize;
        let data_len = data_len as usize;
        let mut data_buf = vec![0u8; data_len];
        if let Err(_) = mem.read(&caller, data_offset, &mut data_buf) {
            tracing::error!("peer_send: failed to read data from memory");
            return Ok(-1);
        }

        match caller.data_mut().app.peer_tx.try_send((peer_ids, data_buf)) {
            Ok(_) => {
                tracing::debug!("peer_send: sent message to {} peers", peer_ids_count);
                Ok(0)
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!("peer_send: channel full, message dropped");
                Ok(-1)
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                tracing::error!("peer_send: channel closed");
                Ok(-1)
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
            tracing::error!("peer_recv: failed to find host memory");
            return Ok(-1);
        };

        let msg = match caller.data_mut().app.peer_rx.try_recv() {
            Ok(msg) => msg,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                return Ok(0);
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                tracing::error!("peer_recv: channel disconnected");
                return Ok(-1);
            }
        };

        let from_peer_offset = from_peer_ptr as usize;
        let peer_id_buf = msg.from_peer().to_bytes();

        if let Err(_) = mem.write(&mut caller, from_peer_offset, &peer_id_buf) {
            tracing::error!("peer_recv: failed to write from_peer to memory");
            return Ok(-1);
        }

        let data_offset = data_ptr as usize;
        let data_max_len = data_max_len as usize;
        let data = msg.data();
        let data_to_write = std::cmp::min(data.len(), data_max_len);

        if let Err(_) = mem.write(&mut caller, data_offset, &data[..data_to_write]) {
            tracing::error!("peer_recv: failed to write data to memory");
            return Ok(-1);
        }

        tracing::debug!("peer_recv: received message of {} bytes", data_to_write);
        Ok(data_to_write as i32)
    }

    fn region_latency(
        mut caller: wasmtime::Caller<'_, AppState>,
        (region_ptr,): (i32,),
    ) -> Box<dyn Future<Output = anyhow::Result<i32>> + Send + '_> {
        Box::new(async move {
            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                tracing::error!("region_latency: failed to find host memory");
                return Ok(-1i32);
            };

            let region_offset = region_ptr as usize;
            let mut region_bytes = [0u8; 4];
            if let Err(_) = mem.read(&caller, region_offset, &mut region_bytes) {
                tracing::error!("region_latency: failed to read region from memory");
                return Ok(-1);
            }

            let region_id = RegionId::from_bytes(region_bytes);

            let mesh = &caller.data().ctx.mesh;
            match mesh.get_region_latency(region_id).await {
                Some(latency) => Ok(latency.as_millis() as i32),
                None => Ok(-1),
            }
        })
    }

    fn peer_list(
        mut caller: wasmtime::Caller<'_, AppState>,
        (peer_ids_ptr, length, total_count_ptr): (i32, u32, i32),
    ) -> Box<dyn Future<Output = anyhow::Result<i32>> + Send + '_> {
        Box::new(async move {
            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                tracing::error!("peer_list: failed to find host memory");
                return Ok(-1i32);
            };

            let app_id = caller.data().app.app_id;
            let mesh = &caller.data().ctx.mesh;
            let peers = mesh.get_peers_for_app(app_id).await;

            let total_count = peers.len();
            let length = std::cmp::min(length as usize, 65536);

            let total_count_offset = total_count_ptr as usize;
            if let Err(_) = mem.write(
                &mut caller,
                total_count_offset,
                &(total_count as u32).to_le_bytes(),
            ) {
                tracing::error!("peer_list: failed to write total count to memory");
                return Ok(-1);
            }

            const PEER_ID_SIZE: usize = 16;
            let ptr_offset = peer_ids_ptr as usize;

            for (i, peer_id) in peers.iter().take(length).enumerate() {
                let write_offset = ptr_offset + (i * PEER_ID_SIZE);
                let peer_id_bytes = peer_id.to_bytes();
                if let Err(_) = mem.write(&mut caller, write_offset, &peer_id_bytes) {
                    tracing::error!("peer_list: failed to write peer ID to memory");
                    return Ok(-1);
                }
            }

            tracing::debug!("peer_list: returned {} of {} peers", length, total_count);
            Ok(length as i32)
        })
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
    term: Option<String>,
    linker: Arc<wasmtime::Linker<AppState>>,
    app_output_sender: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
}

pub struct AppState {
    app: PreloadedAppState,
    ctx: Arc<AppContext>,
    streams: Vec<Stream>,
    limits: AppLimiter,
    terminal: Arc<Mutex<Terminal>>,
    next_app: Option<tokio::sync::oneshot::Receiver<(PreloadedAppState, wasmtime::InstancePre<AppState>)>>,
    /// Must be kept in sync with [`next_app_shortname`]
    has_next_app: Arc<AtomicBool>,
    input_receiver: tokio::sync::mpsc::Receiver<smallvec::SmallVec<[u8; 16]>>,
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

struct AsyncStdoutWriter {
    sender: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    buffer: Vec<u8>,
}

impl tokio::io::AsyncWrite for AsyncStdoutWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if !self.buffer.is_empty() {
            let data = std::mem::take(&mut self.buffer);
            if self.sender.send(data).is_err() {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "channel closed",
                )));
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.poll_flush(cx)
    }
}

struct MyStdoutStream {
    sender: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
}

impl wasmtime_wasi::cli::StdoutStream for MyStdoutStream {
    fn async_stream(&self) -> Box<dyn tokio::io::AsyncWrite + Send + Sync> {
        Box::new(AsyncStdoutWriter {
            sender: self.sender.clone(),
            buffer: Vec::new(),
        })
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
    pub async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Stream::Tcp(stream) => {
                use tokio::io::AsyncWriteExt;
                stream.write(buf).await
            }
            Stream::Tls(stream) => {
                use tokio::io::AsyncWriteExt;
                stream.write(buf).await
            }
        }
    }

    pub fn try_read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Tcp(stream) => stream.try_read(buf),
            Stream::Tls(stream) => {
                use std::pin::Pin;
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
