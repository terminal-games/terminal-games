// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use hex::ToHex;
use smallvec::SmallVec;
use tokio::sync::Mutex;

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
    pub username: String,
    pub remote_sshid: String,
    pub term: Option<String>,
    pub args: Option<String>,
}

impl AppServer {
    pub fn new(mesh: Mesh, db: libsql::Connection) -> anyhow::Result<Self> {
        let mut config = wasmtime::Config::new();
        config.async_support(true);
        config.epoch_interruption(true);
        let engine = wasmtime::Engine::new(&config)?;

        let mut linker = wasmtime::Linker::<AppState>::new(&engine);
        wasmtime_wasi::p1::add_to_linker_async(&mut linker, |t| &mut t.wasi_ctx)?;

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

    pub fn instantiate_app(&self, params: AppInstantiationParams) {
        let mesh = self.mesh.clone();
        let db = self.db.clone();
        let engine = self.engine.clone();
        let linker = self.linker.clone();
        tokio::task::spawn(async move {
            let engine = engine;
            let mut window_size_receiver = params.window_size_receiver;
            let mut terminal = Arc::new(Mutex::new(headless_terminal::Parser::new(0, 0, 0)));
            let mut current_app_shortname = params.args.unwrap_or("menu".into());
            let (app_output_sender, mut app_output_receiver) =
                tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
            let mut input_receiver = params.input_receiver;
            let has_next_app_shortname = Arc::new(AtomicBool::new(false));
            let (shortname_sender, mut shortname_receiver) =
                tokio::sync::mpsc::channel::<String>(1);

            // output task
            tokio::task::spawn({
                let output_sender = params.output_sender;
                let username = params.username.clone();
                let current_app_shortname = current_app_shortname.clone();
                let has_next_app_shortname = has_next_app_shortname.clone();
                let terminal = terminal.clone();
                async move {
                    let mut escape_buffer = EscapeSequenceBuffer::new();
                    let mut status_bar_interval = tokio::time::interval(Duration::from_secs(1));
                    let mut status_bar = StatusBar::new(current_app_shortname, username);

                    loop {
                        tokio::select! {
                            result = app_output_receiver.recv() => {
                                let Some(mut data) = result else { break };

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

                                output_sender.send(output).await.unwrap();
                            }

                            result = window_size_receiver.recv() => {
                                let Some((width, height)) = result else { break };

                                let mut output = Vec::new();
                                {
                                    let mut terminal = terminal.lock().await;
                                    let screen = terminal.screen_mut();
                                    screen.set_size(height, width);
                                    status_bar.maybe_render_into(screen, &mut output, true);
                                }
                                if !output.is_empty() {
                                    output_sender.send(output).await.unwrap();
                                }
                            }

                            _ = status_bar_interval.tick() => {
                                let mut output = Vec::new();
                                status_bar.maybe_render_into(terminal.lock().await.screen(), &mut output, false);
                                if !output.is_empty() {
                                    output_sender.send(output).await.unwrap();
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

            loop {
                let app_id = db
                    .query(
                        "SELECT id FROM games WHERE shortname = ?1",
                        [current_app_shortname.as_str()],
                    )
                    .await
                    .unwrap()
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .get::<u64>(0)
                    .unwrap();

                let (peer_id, peer_rx, peer_tx) = mesh.new_peer(AppId(app_id)).await;

                let mut envs = vec![];
                let mut rows = db
                    .query("SELECT name, value FROM envs WHERE game_id = ?1", [app_id])
                    .await
                    .unwrap();
                while let Some(row) = rows.next().await.unwrap() {
                    let name = row.get(0).unwrap();
                    let value = row.get(1).unwrap();
                    envs.push((name, value));
                }
                envs.push(("REMOTE_SSHID".to_string(), params.remote_sshid.clone()));
                envs.push(("USERNAME".to_string(), params.username.clone()));
                envs.push(("PEER_ID".to_string(), peer_id.to_bytes().encode_hex()));
                if let Some(term) = params.term.clone() {
                    envs.push(("TERM".to_string(), term));
                }

                let wasi_ctx = wasmtime_wasi::WasiCtx::builder()
                    .stdout(MyStdoutStream {
                        sender: app_output_sender.clone(),
                    })
                    .envs(&envs)
                    .build_p1();

                let state = AppState {
                    wasi_ctx,
                    streams: Vec::default(),
                    limits: AppLimiter::default(),
                    terminal: terminal,
                    next_app_shortname: None,
                    input_receiver: input_receiver,
                    has_next_app_shortname: has_next_app_shortname.clone(),
                    // module_cache: server.module_cache.clone(),
                    peer_rx,
                    peer_tx,
                    mesh: mesh.clone(),
                    app_id: AppId(app_id),
                };

                let mut store = wasmtime::Store::new(&engine, state);
                store.limiter(|state| &mut state.limits);
                store.epoch_deadline_callback(|_| Ok(wasmtime::UpdateDeadline::Yield(1)));

                let mut rows = db
                    .query(
                        "SELECT path FROM games WHERE shortname = ?1",
                        libsql::params![current_app_shortname.as_str()],
                    )
                    .await
                    .unwrap();

                let row = rows.next().await.unwrap().unwrap();
                let wasm_path: String = row.get(0).unwrap();
                let module = wasmtime::Module::from_file(&engine, wasm_path).unwrap();

                let call_result = {
                    let func = {
                        let instance = linker.instantiate_async(&mut store, &module).await.unwrap();
                        let func = instance
                            .get_typed_func::<(), ()>(&mut store, "_start")
                            .unwrap();
                        func
                    };

                    has_next_app_shortname.store(false, Ordering::Release);
                    tokio::select! {
                        result = func.call_async(&mut store, ()) => {
                            result
                        }
                        // _ = drop_receiver.changed() => {
                        //     tracing::info!("Drop signal received, exiting gracefully");
                        //     let mut cache = server.module_cache.lock().await;
                        //     cache.release_module(&shortname);
                        //     break;
                        // }
                    }
                };

                if let Err(err) = call_result {
                    match err.downcast::<wasmtime_wasi::I32Exit>() {
                        Ok(_) => {}
                        Err(other) => {
                            tracing::error!(error = %other, shortname = %current_app_shortname, "Module errored");
                            break;
                        }
                    }
                }

                let mut state = store.into_data();
                input_receiver = state.input_receiver;
                terminal = state.terminal;

                if let Some(next_app_shortname) = state.next_app_shortname.take() {
                    shortname_sender
                        .send(next_app_shortname.clone())
                        .await
                        .unwrap();
                    current_app_shortname = next_app_shortname;
                    continue;
                }

                break;
            }
        });
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

            caller.data_mut().next_app_shortname = Some(shortname);
            caller
                .data()
                .has_next_app_shortname
                .store(true, Ordering::Release);

            // *caller.data().next_app_shortname.lock().await = Some(shortname.clone());

            // let module_cache = caller.data().module_cache.clone();
            // tokio::task::spawn(async move {
            //     let mut cache = module_cache.lock().await;
            //     if let Err(e) = cache.get_module(&shortname).await {
            //         tracing::error!(
            //             error = %e,
            //             shortname,
            //             "Failed to warm module cache for next app"
            //         );
            //     } else {
            //         cache.release_module(&shortname);
            //         tracing::info!(shortname, "Module cache warmed for next app");
            //     }
            // });

            Ok(0)
        })
    }

    fn next_app_ready(
        caller: wasmtime::Caller<'_, AppState>,
        (): (),
    ) -> Box<dyn Future<Output = anyhow::Result<i32>> + Send + '_> {
        Box::new(async move {
            // let next_app_shortname = match caller.data().next_app_shortname.try_lock() {
            //     Ok(guard) => guard.clone(),
            //     Err(_) => return Ok(0),
            // };
            // let shortname = match &next_app_shortname {
            //     Some(s) => s,
            //     None => return Ok(0),
            // };

            // let cache = match caller.data().module_cache.try_lock() {
            //     Ok(guard) => guard,
            //     Err(_) => return Ok(0), // Lock held (e.g., by get_module), return not ready
            // };
            // let ready = cache.is_warmed(&shortname);
            // Ok(if ready { 1 } else { 0 })
            Ok(1)
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

        match caller.data_mut().peer_tx.try_send((peer_ids, data_buf)) {
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

        let msg = match caller.data_mut().peer_rx.try_recv() {
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

            let mesh = caller.data().mesh.clone();
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

            let app_id = caller.data().app_id;
            let mesh = caller.data().mesh.clone();
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

pub struct AppState {
    wasi_ctx: wasmtime_wasi::p1::WasiP1Ctx,
    streams: Vec<Stream>,
    limits: AppLimiter,
    terminal: Arc<Mutex<Terminal>>,
    next_app_shortname: Option<String>,
    /// Must be kept in sync with [`next_app_shortname`]
    has_next_app_shortname: Arc<AtomicBool>,
    input_receiver: tokio::sync::mpsc::Receiver<smallvec::SmallVec<[u8; 16]>>,
    // module_cache: Arc<Mutex<ModuleCache>>,
    peer_rx: tokio::sync::mpsc::Receiver<PeerMessageApp>,
    peer_tx: tokio::sync::mpsc::Sender<(Vec<PeerId>, Vec<u8>)>,
    mesh: Mesh,
    app_id: crate::mesh::AppId,
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

    pub async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Tcp(stream) => {
                use tokio::io::AsyncReadExt;
                stream.read(buf).await
            }
            Stream::Tls(stream) => {
                use tokio::io::AsyncReadExt;
                stream.read(buf).await
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

struct ModuleCacheEntry {
    module: wasmtime::Module,
    ref_count: usize,
    last_zero: Option<Instant>,
}

pub(crate) struct ModuleCache {
    cache: HashMap<String, ModuleCacheEntry>,
    engine: wasmtime::Engine,
    db: libsql::Connection,
}

impl ModuleCache {
    fn new(engine: wasmtime::Engine, db: libsql::Connection) -> Self {
        Self {
            cache: HashMap::new(),
            engine,
            db,
        }
    }

    async fn get_module(&mut self, shortname: &str) -> anyhow::Result<wasmtime::Module> {
        if let Some(entry) = self.cache.get_mut(shortname) {
            entry.ref_count += 1;
            entry.last_zero = None;
            tracing::debug!(
                shortname,
                ref_count = entry.ref_count,
                "Module found in cache"
            );
            return Ok(entry.module.clone());
        }

        tracing::info!(shortname, "Fetching module from database");
        let mut rows = self
            .db
            .query(
                "SELECT path FROM games WHERE shortname = ?1",
                libsql::params![shortname],
            )
            .await?;

        let row = rows.next().await?;
        let Some(row) = row else {
            anyhow::bail!("Module '{}' not found in database", shortname);
        };

        let wasm_path: String = row.get(0)?;

        tracing::info!(shortname, "Compiling module");
        let module = wasmtime::Module::from_file(&self.engine, wasm_path)?;

        self.cache.insert(
            shortname.to_string(),
            ModuleCacheEntry {
                module: module.clone(),
                ref_count: 1,
                last_zero: None,
            },
        );

        tracing::info!(shortname, ref_count = 1, "Module added to cache");
        Ok(module)
    }

    fn release_module(&mut self, shortname: &str) {
        if let Some(entry) = self.cache.get_mut(shortname) {
            entry.ref_count = entry.ref_count.saturating_sub(1);
            if entry.ref_count == 0 {
                entry.last_zero = Some(Instant::now());
            }
            tracing::info!(
                shortname,
                ref_count = entry.ref_count,
                "Module reference released"
            );
        }
    }

    fn is_warmed(&self, shortname: &str) -> bool {
        self.cache.contains_key(shortname)
    }

    fn cleanup(&mut self) {
        let now = Instant::now();
        let before_len = self.cache.len();

        self.cache.retain(|shortname, entry| {
            if entry.ref_count == 0 {
                if let Some(since_zero) = entry.last_zero {
                    if now.duration_since(since_zero) >= Duration::from_secs(30) {
                        tracing::info!(
                            shortname,
                            "Evicting module from cache after 30s at refcount 0"
                        );
                        return false;
                    }
                } else {
                    entry.last_zero = Some(now);
                }
            }
            true
        });

        let after_len = self.cache.len();
        if after_len != before_len {
            tracing::debug!(
                removed = before_len - after_len,
                remaining = after_len,
                "Module cache cleanup completed"
            );
        }
    }
}
