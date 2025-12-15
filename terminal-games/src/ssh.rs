// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use rand_core::OsRng;
use russh::keys::ssh_key::{self, PublicKey};
use russh::server::*;
use russh::{Channel, ChannelId, Pty};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
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
    tickers: Vec<String>,
    session_start_time: std::time::Instant,
) -> Vec<u8> {
    let mut bar = Vec::new();
    bar.extend_from_slice(b"\x1b[s");

    bar.extend_from_slice(format!("\x1b[{};1H", height).as_bytes());

    let active_tab = " menu ".bold().black().on_green();
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
}

struct ModuleCache {
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
            },
        );

        tracing::debug!(shortname, ref_count = 1, "Module added to cache");
        Ok(module)
    }

    fn release_module(&mut self, shortname: &str) {
        if let Some(entry) = self.cache.get_mut(shortname) {
            entry.ref_count = entry.ref_count.saturating_sub(1);
            tracing::debug!(
                shortname,
                ref_count = entry.ref_count,
                "Module reference released"
            );

            if entry.ref_count == 0 {
                tracing::info!(shortname, "Evicting module from cache (ref_count = 0)");
                self.cache.remove(shortname);
            }
        }
    }
}

pub struct App {
    linker: Arc<Mutex<wasmtime::Linker<ComponentRunStates>>>,
    module_cache: Arc<Mutex<ModuleCache>>,
    engine: wasmtime::Engine,
    #[allow(dead_code)]
    db: libsql::Connection,
    input_sender: tokio::sync::mpsc::Sender<Vec<u8>>,
    input_receiver: Option<tokio::sync::mpsc::Receiver<Vec<u8>>>,
    dimensions: Arc<Mutex<(u32, u32)>>,
    username: Arc<OnceLock<String>>,
    term: Option<String>,
    args_sender: Option<tokio::sync::oneshot::Sender<Vec<u8>>>,
    args_receiver: Option<tokio::sync::oneshot::Receiver<Vec<u8>>>,
    drop_sender: Option<tokio::sync::oneshot::Sender<()>>,
}

#[derive(Clone)]
pub(crate) struct AppServer {
    engine: wasmtime::Engine,
    module_cache: Arc<Mutex<ModuleCache>>,
    linker: Arc<Mutex<wasmtime::Linker<ComponentRunStates>>>,
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

        let mut linker: wasmtime::Linker<ComponentRunStates> = wasmtime::Linker::new(&engine);
        wasmtime_wasi::p1::add_to_linker_async(&mut linker, |t| &mut t.wasi_ctx)?;

        linker.func_wrap_async(
            "terminal_games",
            "dial",
            |mut caller: wasmtime::Caller<'_, ComponentRunStates>,
             (address_ptr, address_len): (i32, u32)| {
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

                    let stream = match tokio::net::TcpStream::connect(address.as_ref()).await {
                        Ok(stream) => stream,
                        Err(_) => {
                            return Ok(-1);
                        }
                    };
                    caller.data_mut().streams.push(stream);

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
                        anyhow::bail!("address too long")
                    }
                    let len = address_len as usize;
                    let offset = address_ptr as usize;

                    let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                        anyhow::bail!("failed to find host memory");
                    };

                    if let Err(_) = mem.read(&caller, offset, &mut buf[..len]) {
                        anyhow::bail!("failed to write to host memory");
                    }

                    let Some(stream) = caller.data_mut().streams.get_mut(conn_id as usize) else {
                        anyhow::bail!("failed to write to host memory");
                    };

                    match stream.write(&buf[..len]).await {
                        Ok(n) => Ok(n as i32),
                        Err(_) => anyhow::bail!("failed to write"),
                    }
                })
            },
        )?;

        linker.func_wrap(
            "terminal_games",
            "conn_read",
            |mut caller: wasmtime::Caller<'_, ComponentRunStates>,
             conn_id: i32,
             ptr: i32,
             len: u32| {
                let Some(stream) = caller.data_mut().streams.get_mut(conn_id as usize) else {
                    anyhow::bail!("failed to write to host memory");
                };

                let mut buf = [0u8; 4 * 1024];
                let len = std::cmp::min(buf.len(), len as usize);
                let n = match stream.try_read(&mut buf[..len]) {
                    Ok(n) => n,
                    Err(_) => return Ok(0),
                };

                let Some(wasmtime::Extern::Memory(mem)) = caller.get_export("memory") else {
                    anyhow::bail!("failed to find host memory");
                };
                let offset = ptr as u32 as usize;
                if let Err(_) = mem.write(&mut caller, offset, &buf[..n]) {
                    anyhow::bail!("failed to write to host memory");
                }
                Ok(n as i32)
            },
        )?;

        linker.func_wrap(
            "terminal_games",
            "terminal_read",
            move |mut caller: wasmtime::Caller<'_, ComponentRunStates>, ptr: i32, _len: u32| {
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

        let module_cache = ModuleCache::new(engine.clone(), conn.clone());

        Ok(Self {
            engine,
            module_cache: Arc::new(Mutex::new(module_cache)),
            linker: Arc::new(Mutex::new(linker)),
            db: conn,
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
        let dimensions = Arc::new(Mutex::new((0, 0)));

        let (input_sender, input_receiver) = tokio::sync::mpsc::channel(1);
        let (args_sender, args_receiver) = tokio::sync::oneshot::channel();

        App {
            linker: self.linker.clone(),
            module_cache: self.module_cache.clone(),
            engine: self.engine.clone(),
            db: self.db.clone(),
            input_sender,
            input_receiver: Some(input_receiver),
            dimensions,
            args_sender: Some(args_sender),
            args_receiver: Some(args_receiver),
            term: None,
            username: Arc::new(OnceLock::new()),
            drop_sender: None,
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
        let handle1 = session.handle();
        let handle2 = session.handle();
        let channel_id = channel.id();
        let dimensions = self.dimensions.clone();
        let engine = self.engine.clone();
        let module_cache = self.module_cache.clone();
        let linker = self.linker.clone();
        let username = self.username.clone();
        let remote_sshid = String::from_utf8_lossy(session.remote_sshid()).to_string();
        let db = self.db.clone();

        // safety: nothing else takes these
        let input_receiver = self.input_receiver.take().unwrap();
        let mut args_receiver = self.args_receiver.take().unwrap();

        tokio::task::spawn(async move {
            // Wait for a pty request (and maybe also an exec)
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;

            let (output_sender, mut output_receiver) = unbounded_channel::<Vec<u8>>();

            let username = username.get().map(|s| s.clone()).unwrap_or_default();

            let args = match args_receiver.try_recv() {
                Ok(args) => String::from_utf8_lossy(&args).to_string(),
                _ => "kitchen-sink".to_string(),
            };

            let mut envs = vec![];

            let mut rows = db.query("SELECT name, value FROM envs JOIN games ON envs.game_id = games.id WHERE shortname = ?1", [args.as_str()]).await.unwrap();
            while let Some(row) = rows.next().await.unwrap() {
                let name = row.get(0).unwrap();
                let value = row.get(1).unwrap();
                envs.push((name, value));
            }

            envs.push(("REMOTE_SSHID".to_string(), remote_sshid));
            envs.push(("USERNAME".to_string(), username.clone()));

            let wasi_ctx = wasmtime_wasi::WasiCtx::builder()
                .stdout(MyStdoutStream {
                    sender: output_sender,
                })
                .envs(&envs)
                .build_p1();

            let state = ComponentRunStates {
                wasi_ctx: wasi_ctx,
                resource_table: wasmtime_wasi::ResourceTable::new(),
                streams: Vec::default(),
                input_receiver,
                limits: MyLimiter::default(),
                dimensions: dimensions.clone(),
            };

            let mut store = wasmtime::Store::new(&engine, state);
            store.limiter(|state| &mut state.limits);
            store.epoch_deadline_callback(|_| Ok(wasmtime::UpdateDeadline::Yield(1)));

            let (module, shortname) = {
                let mut cache = module_cache.lock().await;
                match cache.get_module(&args).await {
                    Ok(module) => (module, args),
                    Err(e) => {
                        tracing::error!(error = %e, shortname = %args, "Failed to get module");
                        let _ = handle1
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

            let module_cache_for_release = module_cache.clone();
            let shortname_for_release = shortname.clone();

            let session_start_time = std::time::Instant::now();

            tokio::task::spawn(async move {
                let func = {
                    let linker = linker.lock().await;
                    let instance = linker.instantiate_async(&mut store, &module).await.unwrap();
                    let func = instance
                        .get_typed_func::<(), ()>(&mut store, "_start")
                        .unwrap();
                    func
                };

                match func.call_async(&mut store, ()).await {
                    Ok(()) => {}
                    Err(err) => if let Ok(_err) = err.downcast::<wasmtime_wasi::I32Exit>() {},
                }

                {
                    let mut cache = module_cache_for_release.lock().await;
                    cache.release_module(&shortname_for_release);
                }

                let _ = handle1
                    .disconnect(
                        russh::Disconnect::ByApplication,
                        "Thanks for playing!".to_string(),
                        "en-US".to_string(),
                    )
                    .await;
            });

            tokio::task::spawn(async move {
                while let Some(data) = output_receiver.recv().await {
                    if handle2.data(channel_id, data.into()).await.is_err() {
                        tracing::error!("Failed to send output data");
                        break;
                    }

                    let (width, height) = *dimensions.lock().await;
                    if width > 0 && height > 0 {
                        let bar = create_status_bar(
                            width,
                            height,
                            username.as_ref(),
                            vec![
                                "Check out the new Terminal Miner v0.1".to_string(),
                                format!("Link test {}", "example.com".link("https://example.com"))
                                    .to_string(),
                            ],
                            session_start_time,
                        );
                        if handle2.data(channel_id, bar.into()).await.is_err() {
                            tracing::error!("Failed to send status bar");
                            break;
                        }
                    }
                }
            });
        });

        Ok(true)
    }

    async fn auth_publickey(
        &mut self,
        user: &str,
        pubkey: &PublicKey,
    ) -> Result<Auth, Self::Error> {
        let _ = self.username.set(user.to_string());
        let mut rows = self
            .db
            .query(
                "
                INSERT INTO users (pubkey_fingerprint, username) VALUES (?1, ?2)
                ON CONFLICT DO UPDATE SET pubkey_fingerprint = ?1, username = ?2
                RETURNING id
            ",
                libsql::params!(pubkey.fingerprint(Default::default()).as_bytes(), user),
            )
            .await
            .unwrap();
        let user_id: u64 = rows.next().await.unwrap().unwrap().get(0).unwrap();

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        self.drop_sender = Some(tx);
        let db = self.db.clone();
        tokio::task::spawn(async move {
            if let Ok(_) = rx.await {
                let _ = db
                    .execute(
                        "UPDATE users SET session_time = session_time + 1 WHERE id = ?1",
                        [user_id],
                    )
                    .await
                    .unwrap();
            }
        });

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
        if let Some(args_sender) = self.args_sender.take() {
            args_sender.send(data.to_vec()).unwrap();
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
        self.term = Some(term.to_string());
        session.channel_success(channel)?;
        Ok(())
    }
}

impl Drop for App {
    fn drop(&mut self) {
        if let Some(drop_sender) = self.drop_sender.take() {
            let _ = drop_sender.send(());
        }
    }
}
