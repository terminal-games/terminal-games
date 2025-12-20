// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use rand_core::OsRng;
use russh::keys::ssh_key::{self, PublicKey};
use russh::server::*;
use russh::{Channel, ChannelId, Pty};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::sync::watch;
use tokio_rustls::rustls::pki_types::ServerName;
use unicode_width::UnicodeWidthStr;
use yansi::Paint;
use yansi::hyperlink::HyperlinkExt;

use crate::{ComponentRunStates, MyLimiter};

fn terminal_width(str: &str) -> usize {
    strip_ansi_escapes::strip_str(str).width()
}

fn create_status_bar(
    width: u32,
    height: u32,
    username: &str,
    current_shortname: &str,
    tickers: Vec<String>,
    session_start_time: std::time::Instant,
) -> Vec<u8> {
    let mut bar = Vec::new();
    bar.extend_from_slice(b"\x1b[s");

    bar.extend_from_slice(format!("\x1b[{};1H", height).as_bytes());

    let active_tab_text = format!(" {} ", current_shortname);
    let active_tab = active_tab_text.bold().black().on_green();
    let username = format!(" {} ", username).white().on_fixed(237).to_string();
    let left = active_tab.to_string() + &username;

    let ssh_callout = " ssh terminal-games.fly.dev ".bold().black().on_green();
    let ticker_index =
        ((std::time::Instant::now() - session_start_time).as_secs() / 10) as usize % tickers.len();
    let ticker = format!(
        " {} {}{}{} ",
        tickers[ticker_index],
        "•".repeat(ticker_index).fixed(241),
        "•".fixed(249),
        "•"
            .repeat(tickers.len().saturating_sub(ticker_index + 1))
            .fixed(241),
    )
    .on_fixed(237)
    .wrap()
    .to_string();
    let right = ticker + &ssh_callout.to_string();

    let padding = " "
        .repeat((width as usize).saturating_sub(terminal_width(&left) + terminal_width(&right)))
        .on_fixed(236)
        .to_string();

    bar.extend_from_slice((left + &padding + &right).as_bytes());

    bar.extend_from_slice(b"\x1b[0m");
    bar.extend_from_slice(b"\x1b[u");
    bar
}

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

pub struct App {
    input_sender: tokio::sync::mpsc::Sender<Vec<u8>>,
    dimensions: Arc<Mutex<(u32, u32)>>,
    username: Option<tokio::sync::oneshot::Sender<String>>,
    term: Option<tokio::sync::oneshot::Sender<String>>,
    args: Option<tokio::sync::oneshot::Sender<Vec<u8>>>,
    remote_sshid: Option<tokio::sync::oneshot::Sender<String>>,
    ssh_session: Option<tokio::sync::oneshot::Sender<(Handle, ChannelId)>>,
    drop_sender: tokio::sync::watch::Sender<()>,
    server: AppServer,
}

#[derive(Clone)]
pub(crate) struct AppServer {
    engine: wasmtime::Engine,
    module_cache: Arc<Mutex<ModuleCache>>,
    linker: Arc<wasmtime::Linker<ComponentRunStates>>,
    db: libsql::Connection,
}

impl AppServer {
    pub async fn new() -> anyhow::Result<Self> {
        tracing::info!("Initializing runtime");

        let db = libsql::Builder::new_remote(
            std::env::var("LIBSQL_URL").unwrap(),
            std::env::var("LIBSQL_AUTH_TOKEN").unwrap(),
        )
        .build()
        .await
        .unwrap();

        let conn = db.connect().unwrap();

        let tx = conn.transaction().await.unwrap();
        tx.execute_batch(include_str!("../libsql/migrate-001.sql"))
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let _ = conn
            .execute(
                "INSERT INTO games (shortname, path) VALUES (?1, ?2)",
                libsql::params!("kitchen-sink", "examples/kitchen-sink/main.wasm"),
            )
            .await;

        let _ = conn
            .execute(
                "INSERT INTO games (shortname, path) VALUES (?1, ?2)",
                libsql::params!("menu", "cmd/menu/main.wasm"),
            )
            .await;

        let _ = conn
            .execute(
                "INSERT INTO games (shortname, path) VALUES (?1, ?2)",
                libsql::params!(
                    "ratatui",
                    "target/wasm32-wasip1/release/ratatui-example.wasm"
                ),
            )
            .await;

        let mut config = wasmtime::Config::new();
        config.async_support(true);
        config.epoch_interruption(true);
        let engine = wasmtime::Engine::new(&config)?;

        let engine_weak = engine.weak();
        tokio::task::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                if let Some(engine) = engine_weak.upgrade() {
                    engine.increment_epoch();
                } else {
                    return;
                }
            }
        });

        let module_cache = ModuleCache::new(engine.clone(), conn.clone());
        let module_cache = Arc::new(Mutex::new(module_cache));
        {
            let module_cache = module_cache.clone();
            tokio::task::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let mut cache = module_cache.lock().await;
                    cache.cleanup();
                }
            });
        }

        let mut linker: wasmtime::Linker<ComponentRunStates> = wasmtime::Linker::new(&engine);
        wasmtime_wasi::p1::add_to_linker_async(&mut linker, |t| &mut t.wasi_ctx)?;

        linker.func_wrap_async(
            "terminal_games",
            "dial",
            |mut caller: wasmtime::Caller<'_, ComponentRunStates>,
             (address_ptr, address_len, mode): (i32, u32, u32)| {
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
                        let hostname: String =
                            address.split(':').next().unwrap_or(&address).to_string();

                        let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();
                        root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
                        let mut config = tokio_rustls::rustls::ClientConfig::builder()
                            .with_root_certificates(root_cert_store)
                            .with_no_client_auth();
                        config.alpn_protocols.push(b"h2".to_vec());
                        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));

                        let dnsname = match ServerName::try_from(hostname) {
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

                        crate::Stream::Tls(tls_stream)
                    } else {
                        crate::Stream::Tcp(tcp_stream)
                    };

                    caller.data_mut().streams.push(stream);

                    tracing::info!(mode, "dial");

                    Ok((caller.data().streams.len() - 1) as i32)
                })
            },
        )?;
        linker.func_wrap_async(
            "terminal_games",
            "conn_write",
            |mut caller: wasmtime::Caller<'_, ComponentRunStates>,
             (conn_id, address_ptr, address_len): (i32, i32, u32)| {
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
            },
        )?;

        linker.func_wrap_async(
            "terminal_games",
            "conn_read",
            |mut caller: wasmtime::Caller<'_, ComponentRunStates>,
             (conn_id, ptr, len): (i32, i32, u32)| {
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
            },
        )?;

        linker.func_wrap_async(
            "terminal_games",
            "terminal_read",
            move |mut caller: wasmtime::Caller<'_, ComponentRunStates>, (ptr, _len): (i32, u32)| {
                Box::new(async move {
                    match caller.data_mut().input_receiver.try_recv() {
                        Ok(buf) => {
                            let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory")
                            else {
                                anyhow::bail!("failed to find host memory");
                            };
                            let offset = ptr as u32 as usize;
                            if let Err(_) = mem.write(&mut caller, offset, &buf) {
                                anyhow::bail!("failed to write to host memory");
                            }
                            Ok(buf.len() as i32)
                        }
                        Err(_) => Ok(0),
                    }
                })
            },
        )?;

        linker.func_wrap_async(
            "terminal_games",
            "terminal_size",
            |mut caller: wasmtime::Caller<'_, ComponentRunStates>,
             (width_ptr, height_ptr): (i32, i32)| {
                Box::new(async move {
                    let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                        anyhow::bail!("failed to find host memory");
                    };
                    let (width, height) = *caller.data().dimensions.lock().await;
                    let effective_height = if height > 0 { height - 1 } else { 0 };

                    let width_offset = width_ptr as u32 as usize;
                    if let Err(_) = mem.write(&mut caller, width_offset, &width.to_le_bytes()) {
                        anyhow::bail!("failed to write to host memory");
                    }

                    let height_offset = height_ptr as u32 as usize;
                    if let Err(_) =
                        mem.write(&mut caller, height_offset, &effective_height.to_le_bytes())
                    {
                        anyhow::bail!("failed to write to host memory");
                    }

                    Ok(())
                })
            },
        )?;

        linker.func_wrap_async(
            "terminal_games",
            "change_app",
            move |mut caller: wasmtime::Caller<'_, ComponentRunStates>, (ptr, len): (i32, u32)| {
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

                    *caller.data().next_app_shortname.lock().await = Some(shortname.clone());

                    let module_cache = caller.data().module_cache.clone();
                    tokio::task::spawn(async move {
                        let mut cache = module_cache.lock().await;
                        if let Err(e) = cache.get_module(&shortname).await {
                            tracing::error!(
                                error = %e,
                                shortname,
                                "Failed to warm module cache for next app"
                            );
                        } else {
                            cache.release_module(&shortname);
                            tracing::info!(shortname, "Module cache warmed for next app");
                        }
                    });

                    Ok(0)
                })
            },
        )?;

        linker.func_wrap_async(
            "terminal_games",
            "next_app_ready",
            move |caller: wasmtime::Caller<'_, ComponentRunStates>, (): ()| {
                Box::new(async move {
                    let next_app_shortname = caller.data().next_app_shortname.lock().await.clone();
                    let shortname = match &next_app_shortname {
                        Some(s) => s,
                        None => return Ok(0),
                    };

                    let cache = caller.data().module_cache.lock().await;
                    let ready = cache.is_warmed(&shortname);
                    Ok(if ready { 1 } else { 0 })
                })
            },
        )?;

        Ok(Self {
            db: conn,
            engine,
            linker: Arc::new(linker),
            module_cache,
        })
    }

    pub async fn run(&mut self) -> Result<(), anyhow::Error> {
        let config = Config {
            inactivity_timeout: Some(std::time::Duration::from_secs(3600)),
            auth_rejection_time: std::time::Duration::from_secs(3),
            auth_rejection_time_initial: Some(std::time::Duration::from_secs(0)),
            keys: vec![
                russh::keys::PrivateKey::random(&mut OsRng, ssh_key::Algorithm::Ed25519).unwrap(),
            ],
            nodelay: true,
            ..Default::default()
        };

        tracing::info!("Running SSH server");
        self.run_on_address(Arc::new(config), ("0.0.0.0", 2222))
            .await?;
        Ok(())
    }
}

struct MyStdoutStream {
    sender: UnboundedSender<Vec<u8>>,
}

struct AsyncStdoutWriter {
    sender: UnboundedSender<Vec<u8>>,
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

impl Server for AppServer {
    type Handler = App;
    fn new_client(&mut self, addr: Option<std::net::SocketAddr>) -> App {
        tracing::info!(addr=?addr, "new_client");

        let (input_sender, mut input_receiver) = tokio::sync::mpsc::channel(100);
        let (username_sender, username_receiver) = tokio::sync::oneshot::channel::<String>();
        let (term_sender, mut term_receiver) = tokio::sync::oneshot::channel::<String>();
        let (args_sender, mut args_receiver) = tokio::sync::oneshot::channel::<Vec<u8>>();
        let (remote_sshid_sender, remote_sshid_receiver) =
            tokio::sync::oneshot::channel::<String>();
        let (ssh_session_sender, ssh_session_receiver) =
            tokio::sync::oneshot::channel::<(Handle, ChannelId)>();
        let (drop_sender, mut drop_receiver) = watch::channel(());

        let dimensions = Arc::new(Mutex::new((0, 0)));
        let dimensions_clone = dimensions.clone();

        let server = self.clone();
        tokio::task::spawn(async move {
            let (session_handle, channel_id) = ssh_session_receiver.await.unwrap();
            let remote_sshid = remote_sshid_receiver.await.unwrap();
            let username = username_receiver.await.unwrap();

            // todo: wait for term and args with a 1ms timeout
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            let term = term_receiver.try_recv().ok();
            let args = args_receiver.try_recv().ok();

            let (output_sender, mut output_receiver) = unbounded_channel::<Vec<u8>>();

            let session_start_time = std::time::Instant::now();

            let current_app_shortname = match args {
                None => "menu".to_string(),
                Some(args) => String::from_utf8_lossy(&args).to_string(),
            };

            let current_app_shortname = Arc::new(Mutex::new(current_app_shortname));
            let next_app_shortname: Arc<Mutex<Option<String>>> = Default::default();

            {
                let username = username.clone();
                let current_app_shortname = current_app_shortname.clone();
                let dimensions = dimensions.clone();
                let next_app_shortname = next_app_shortname.clone();
                let session_handle = session_handle.clone();
                tokio::task::spawn(async move {
                    while let Some(data) = output_receiver.recv().await {
                        let data = if next_app_shortname.lock().await.is_some() {
                            let mut out = Vec::with_capacity(data.len());
                            let mut i = 0;
                            while i < data.len() {
                                if data[i..].starts_with(b"\x1b[?1049l") {
                                    i += b"\x1b[?1049l".len();
                                } else {
                                    out.push(data[i]);
                                    i += 1;
                                }
                            }
                            out
                        } else {
                            data
                        };

                        if session_handle.data(channel_id, data.into()).await.is_err() {
                            tracing::error!("Failed to send output data");
                            break;
                        }

                        let (width, height) = *dimensions.lock().await;
                        if width > 0 && height > 0 {
                            let bar = create_status_bar(
                                width,
                                height,
                                username.as_ref(),
                                current_app_shortname.lock().await.as_str(),
                                vec![
                                    "Check out the new Terminal Miner v0.1".to_string(),
                                    format!(
                                        "Link test {}",
                                        "example.com".link("https://example.com")
                                    )
                                    .to_string(),
                                ],
                                session_start_time,
                            );
                            if session_handle.data(channel_id, bar.into()).await.is_err() {
                                tracing::error!("Failed to send status bar");
                                break;
                            }
                        }
                    }
                });
            }

            loop {
                let mut envs = vec![];

                let shortname = { current_app_shortname.lock().await.clone() };

                let mut rows = server.db
                    .query(
                        "SELECT name, value FROM envs JOIN games ON envs.game_id = games.id WHERE shortname = ?1",
                        [shortname.as_str()],
                    )
                    .await
                    .unwrap();
                while let Some(row) = rows.next().await.unwrap() {
                    let name = row.get(0).unwrap();
                    let value = row.get(1).unwrap();
                    envs.push((name, value));
                }

                envs.push(("REMOTE_SSHID".to_string(), remote_sshid.clone()));
                envs.push(("USERNAME".to_string(), username.clone()));
                if let Some(term) = term.clone() {
                    envs.push(("TERM".to_string(), term));
                }

                let wasi_ctx = wasmtime_wasi::WasiCtx::builder()
                    .stdout(MyStdoutStream {
                        sender: output_sender.clone(),
                    })
                    .envs(&envs)
                    .build_p1();

                let state = ComponentRunStates {
                    wasi_ctx,
                    resource_table: wasmtime_wasi::ResourceTable::new(),
                    streams: Vec::default(),
                    limits: MyLimiter::default(),
                    dimensions: dimensions.clone(),
                    next_app_shortname: next_app_shortname.clone(),
                    input_receiver,
                    module_cache: server.module_cache.clone(),
                };

                let mut store = wasmtime::Store::new(&server.engine, state);
                store.limiter(|state| &mut state.limits);
                store.epoch_deadline_callback(|_| Ok(wasmtime::UpdateDeadline::Yield(1)));

                let module = {
                    let mut cache = server.module_cache.lock().await;
                    match cache.get_module(&shortname).await {
                        Ok(module) => module,
                        Err(e) => {
                            tracing::error!(error = %e, shortname = %shortname, "Failed to get module");
                            let _ = session_handle
                                .disconnect(
                                    russh::Disconnect::ByApplication,
                                    format!("Game not found: {}", e),
                                    "en-US".to_string(),
                                )
                                .await;
                            return;
                        }
                    }
                };

                let call_result = {
                    let func = {
                        let instance = server
                            .linker
                            .instantiate_async(&mut store, &module)
                            .await
                            .unwrap();
                        let func = instance
                            .get_typed_func::<(), ()>(&mut store, "_start")
                            .unwrap();
                        func
                    };

                    tokio::select! {
                        result = func.call_async(&mut store, ()) => {
                            result
                        }
                        _ = drop_receiver.changed() => {
                            tracing::info!("Drop signal received, exiting gracefully");
                            let mut cache = server.module_cache.lock().await;
                            cache.release_module(&shortname);
                            break;
                        }
                    }
                };

                let state = store.into_data();
                input_receiver = state.input_receiver;

                {
                    let mut cache = server.module_cache.lock().await;
                    cache.release_module(&shortname);
                }

                if let Err(err) = call_result {
                    match err.downcast::<wasmtime_wasi::I32Exit>() {
                        Ok(_) => {}
                        Err(other) => {
                            tracing::error!(error = %other, shortname = %shortname, "Module errored");
                            break;
                        }
                    }
                }

                {
                    let mut next_app_shortname = next_app_shortname.lock().await;
                    if let Some(next_app_shortname) = next_app_shortname.take() {
                        *current_app_shortname.lock().await = next_app_shortname;
                        continue;
                    }
                }

                break;
            }

            tracing::info!("leaving session");

            let _ = session_handle
                .disconnect(
                    russh::Disconnect::ByApplication,
                    "Thanks for playing!".to_string(),
                    "en-US".to_string(),
                )
                .await;
        });

        let server = self.clone();
        App {
            args: Some(args_sender),
            input_sender,
            term: Some(term_sender),
            username: Some(username_sender),
            remote_sshid: Some(remote_sshid_sender),
            ssh_session: Some(ssh_session_sender),
            drop_sender,
            dimensions: dimensions_clone,
            server,
        }
    }
}

impl Handler for App {
    type Error = anyhow::Error;

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        session: &mut Session,
    ) -> Result<bool, Self::Error> {
        let remote_sshid = String::from_utf8_lossy(session.remote_sshid()).to_string();
        if let Some(remote_sshid_sender) = self.remote_sshid.take() {
            let _ = remote_sshid_sender.send(remote_sshid);
        }

        if let Some(ssh_session_sender) = self.ssh_session.take() {
            let _ = ssh_session_sender.send((session.handle(), channel.id()));
        }

        Ok(true)
    }

    async fn auth_publickey(
        &mut self,
        user: &str,
        pubkey: &PublicKey,
    ) -> Result<Auth, Self::Error> {
        tracing::info!(user, "auth_publickey");
        if let Some(username_sender) = self.username.take() {
            tracing::info!(user, "auth_publickey send");
            let _ = username_sender.send(user.to_string());
        }

        let mut rows = self
            .server
            .db
            .query(
                "
                INSERT INTO users (pubkey_fingerprint, username) VALUES (?1, ?2)
                ON CONFLICT DO UPDATE SET username = ?2
                RETURNING id
            ",
                libsql::params!(pubkey.fingerprint(Default::default()).as_bytes(), user),
            )
            .await
            .unwrap();
        let user_id: u64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
        _ = user_id;

        // let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        // self.drop_sender = Some(tx);
        // let db = self.db.clone();
        // tokio::task::spawn(async move {
        //     if let Ok(_) = rx.await {
        //         let _ = db
        //             .execute(
        //                 "UPDATE users SET session_time = session_time + 1 WHERE id = ?1",
        //                 [user_id],
        //             )
        //             .await
        //             .unwrap();
        //     }
        // });

        Ok(Auth::Accept)
    }

    async fn data(
        &mut self,
        _channel: ChannelId,
        data: &[u8],
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.input_sender.send(data.to_vec()).await?;

        Ok(())
    }

    async fn window_change_request(
        &mut self,
        channel: ChannelId,
        col_width: u32,
        row_height: u32,
        _: u32,
        _: u32,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        *self.dimensions.lock().await = (col_width, row_height);
        session.channel_success(channel)?;
        Ok(())
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if let Some(args_sender) = self.args.take() {
            let _ = args_sender.send(data.to_vec());
        }
        session.channel_success(channel)?;
        Ok(())
    }

    async fn pty_request(
        &mut self,
        channel: ChannelId,
        term: &str,
        col_width: u32,
        row_height: u32,
        _: u32,
        _: u32,
        _: &[(Pty, u32)],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        *self.dimensions.lock().await = (col_width, row_height);
        if let Some(term_sender) = self.term.take() {
            let _ = term_sender.send(term.to_string());
        }
        session.channel_success(channel)?;
        Ok(())
    }
}

impl Drop for App {
    fn drop(&mut self) {
        let _ = self.drop_sender.send(());
    }
}
