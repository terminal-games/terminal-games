// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use hex::ToHex;
use rand_core::OsRng;
use russh::keys::ssh_key::{self, PublicKey};
use russh::server::*;
use russh::{Channel, ChannelId, Pty};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::sync::watch;
use tokio_rustls::rustls::pki_types::ServerName;

use crate::mesh::{AppId, Mesh, PeerId, RegionId};
use crate::status_bar::StatusBar;
use crate::{ComponentRunStates, MyLimiter};

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

pub struct App {
    input_sender: tokio::sync::mpsc::Sender<smallvec::SmallVec<[u8; 16]>>,
    username: Option<tokio::sync::oneshot::Sender<String>>,
    term: Option<tokio::sync::oneshot::Sender<String>>,
    args: Option<tokio::sync::oneshot::Sender<Vec<u8>>>,
    remote_sshid: Option<tokio::sync::oneshot::Sender<String>>,
    ssh_session: Option<tokio::sync::oneshot::Sender<(Handle, ChannelId)>>,
    drop_sender: tokio::sync::watch::Sender<()>,
    terminal: Arc<Mutex<Terminal>>,
    server: AppServer,
    resize_signal_tx: tokio::sync::mpsc::UnboundedSender<()>,
}

#[derive(Clone)]
pub(crate) struct AppServer {
    engine: wasmtime::Engine,
    module_cache: Arc<Mutex<ModuleCache>>,
    linker: Arc<wasmtime::Linker<ComponentRunStates>>,
    db: libsql::Connection,
    mesh: Mesh,
}

impl AppServer {
    pub async fn new(mesh: Mesh) -> anyhow::Result<Self> {
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
                    "rust-simple",
                    "target/wasm32-wasip1/release/rust-simple.wasm"
                ),
            )
            .await;

        let _ = conn
            .execute(
                "INSERT INTO games (shortname, path) VALUES (?1, ?2)",
                libsql::params!(
                    "rust-kitchen-sink",
                    "target/wasm32-wasip1/release/rust-kitchen-sink.wasm"
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
                        if let Err(_) = mem.write(&mut caller, offset, buf.as_ref()) {
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
                    let (height, width) = caller.data().terminal.lock().await.screen().size();
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
            "terminal_cursor",
            |mut caller: wasmtime::Caller<'_, ComponentRunStates>, (x_ptr, y_ptr): (i32, i32)| {
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
                    let next_app_shortname = match caller.data().next_app_shortname.try_lock() {
                        Ok(guard) => guard.clone(),
                        Err(_) => return Ok(0),
                    };
                    let shortname = match &next_app_shortname {
                        Some(s) => s,
                        None => return Ok(0),
                    };

                    let cache = match caller.data().module_cache.try_lock() {
                        Ok(guard) => guard,
                        Err(_) => return Ok(0), // Lock held (e.g., by get_module), return not ready
                    };
                    let ready = cache.is_warmed(&shortname);
                    Ok(if ready { 1 } else { 0 })
                })
            },
        )?;

        linker.func_wrap(
            "terminal_games",
            "peer_send",
            move |mut caller: wasmtime::Caller<'_, ComponentRunStates>,
                  peer_ids_ptr: i32,
                  peer_ids_count: u32,
                  data_ptr: i32,
                  data_len: u32| {
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
            },
        )?;

        linker.func_wrap_async(
            "terminal_games",
            "region_latency",
            |mut caller: wasmtime::Caller<'_, ComponentRunStates>, (region_ptr,): (i32,)| {
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
                        Some(latency) => Ok(latency as i32),
                        None => Ok(-1),
                    }
                })
            },
        )?;

        linker.func_wrap(
            "terminal_games",
            "peer_recv",
            move |mut caller: wasmtime::Caller<'_, ComponentRunStates>,
                  from_peer_ptr: i32,
                  data_ptr: i32,
                  data_max_len: u32| {
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
            },
        )?;

        Ok(Self {
            db: conn,
            engine,
            linker: Arc::new(linker),
            module_cache,
            mesh,
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

        let listen_addr: std::net::SocketAddr = std::env::var("SSH_LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:2222".to_string())
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid SSH_LISTEN_ADDR: {}", e))?;

        let ip_str = listen_addr.ip().to_string();
        tracing::info!(addr = %listen_addr, "Running SSH server");
        self.run_on_address(Arc::new(config), (ip_str.as_str(), listen_addr.port()))
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

        let (input_sender, mut input_receiver) = tokio::sync::mpsc::channel(20);
        let (username_sender, username_receiver) = tokio::sync::oneshot::channel::<String>();
        let (term_sender, mut term_receiver) = tokio::sync::oneshot::channel::<String>();
        let (args_sender, mut args_receiver) = tokio::sync::oneshot::channel::<Vec<u8>>();
        let (remote_sshid_sender, remote_sshid_receiver) =
            tokio::sync::oneshot::channel::<String>();
        let (ssh_session_sender, ssh_session_receiver) =
            tokio::sync::oneshot::channel::<(Handle, ChannelId)>();
        let (drop_sender, mut drop_receiver) = watch::channel(());
        let (resize_signal_tx, resize_signal_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        let terminal = Arc::new(Mutex::new(headless_terminal::Parser::new(0, 0, 0)));
        let terminal_clone = terminal.clone();
        let mesh = self.mesh.clone();

        let server = self.clone();
        tokio::task::spawn(async move {
            let (session_handle, channel_id) = ssh_session_receiver.await.unwrap();
            let mut resize_signal_rx = resize_signal_rx;
            let remote_sshid = remote_sshid_receiver.await.unwrap();
            let username = username_receiver.await.unwrap();

            // enter the alternate screen so that we aren't moving the cursor
            // around and overwriting the original terminal
            session_handle
                .data(channel_id, b"\x1b[?1049h".to_vec().into())
                .await
                .unwrap();

            // todo: wait for term and args with a 1ms timeout
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let term = term_receiver.try_recv().ok();
            let args = args_receiver.try_recv().ok();

            let (output_sender, mut output_receiver) = unbounded_channel::<Vec<u8>>();

            let session_start_time = std::time::Instant::now();

            let (current_app_shortname, app_args) = match args {
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
            let app_args = Arc::new(app_args);

            let current_app_shortname = Arc::new(Mutex::new(current_app_shortname));
            let next_app_shortname: Arc<Mutex<Option<String>>> = Default::default();

            let status_bar = Arc::new(Mutex::new(StatusBar::new(
                terminal_clone.clone(),
                current_app_shortname.clone(),
                username.clone(),
                session_start_time,
            )));

            // output task
            tokio::task::spawn({
                let next_app_shortname = next_app_shortname.clone();
                let session_handle = session_handle.clone();
                let status_bar = status_bar.clone();
                let terminal_clone = terminal_clone.clone();
                async move {
                    while let Some(mut data) = output_receiver.recv().await {
                        if next_app_shortname.lock().await.is_some() {
                            // strip exiting the alternate screen buffer to make
                            // sure transitions between apps are smooth
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

                        let is_dirty = {
                            // process raw data through the vt
                            let mut app_terminal = terminal_clone.lock().await;
                            app_terminal.process(&data);

                            // check if status bar was overwritten
                            let screen = app_terminal.screen_mut();
                            let (height, _width) = screen.size();
                            let dirty = screen.take_damaged_rows();
                            dirty.contains(&(height - 1))
                        };
                        if is_dirty {
                            status_bar
                                .lock()
                                .await
                                .maybe_render_into(&mut data, true)
                                .await;
                        }

                        // tracing::info!(data = String::from_utf8_lossy(&data).as_ref(), "output");
                        session_handle.data(channel_id, data.into()).await.unwrap();
                    }
                }
            });

            // status bar task
            tokio::task::spawn({
                let session_handle = session_handle.clone();
                let status_bar = status_bar.clone();
                async move {
                    let mut interval = tokio::time::interval(Duration::from_secs(1));

                    loop {
                        tokio::select! {
                            result = resize_signal_rx.recv() => {
                                match result {
                                    Some(()) => {
                                        let mut buf = Vec::new();
                                        status_bar.lock().await.maybe_render_into(&mut buf, true).await;
                                        if let Err(_) = session_handle.data(channel_id, buf.into()).await {
                                            break;
                                        }
                                    }
                                    None => {
                                        break;
                                    }
                                }
                            }
                            _ = interval.tick() => {
                                let mut buf = Vec::new();
                                if status_bar.lock().await.maybe_render_into(&mut buf, false).await {
                                    if let Err(_) = session_handle.data(channel_id, buf.into()).await {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            });

            loop {
                let mut envs = vec![];

                let shortname = { current_app_shortname.lock().await.clone() };

                let app_id = server
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

                let (peer_id, peer_rx, peer_tx) = mesh.new_peer(AppId(app_id)).await;

                let mut rows = server
                    .db
                    .query("SELECT name, value FROM envs WHERE game_id = ?1", [app_id])
                    .await
                    .unwrap();
                while let Some(row) = rows.next().await.unwrap() {
                    let name = row.get(0).unwrap();
                    let value = row.get(1).unwrap();
                    envs.push((name, value));
                }

                envs.push(("REMOTE_SSHID".to_string(), remote_sshid.clone()));
                envs.push(("USERNAME".to_string(), username.clone()));
                envs.push(("PEER_ID".to_string(), peer_id.to_bytes().encode_hex()));
                if let Some(term) = term.clone() {
                    envs.push(("TERM".to_string(), term));
                }
                if let Some(ref args) = *app_args {
                    envs.push(("APP_ARGS".to_string(), args.clone()));
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
                    terminal: terminal_clone.clone(),
                    next_app_shortname: next_app_shortname.clone(),
                    input_receiver,
                    module_cache: server.module_cache.clone(),
                    peer_rx,
                    peer_tx,
                    mesh: mesh.clone(),
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
            terminal,
            server,
            resize_signal_tx,
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
        // tracing::info!(
        //     data = String::from_utf8_lossy(&data).as_ref(),
        //     len = data.len(),
        //     "input"
        // );
        self.input_sender.send(data.into()).await?;

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
        self.terminal
            .lock()
            .await
            .screen_mut()
            .set_size(row_height as u16, col_width as u16);
        let _ = self.resize_signal_tx.send(());
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
        self.terminal
            .lock()
            .await
            .screen_mut()
            .set_size(row_height as u16, col_width as u16);
        let _ = self.resize_signal_tx.send(());
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
