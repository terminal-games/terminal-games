// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::future::Future;
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    net::SocketAddr,
    os::unix::io::{AsRawFd, RawFd},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use base64::Engine as _;
use futures::StreamExt;
use rand_core::RngCore;
use serde::{Deserialize, Serialize};
use tarpc::server::Channel;
use tarpc::{client, context, server};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Notify, broadcast},
};
use tokio_rustls::{TlsAcceptor, TlsConnector, rustls};
use tokio_util::{
    codec::{Framed, LengthDelimitedCodec},
    sync::CancellationToken,
    task::TaskTracker,
};

use crate::rate_limiting::get_tcp_rtt_from_fd;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Message {
    PeerMessage(PeerMessage),
    PeerListSync(PeerListSyncMessage),
    PeerAdded(PeerChangeMessage),
    PeerRemoved(PeerChangeMessage),
    GameRuntimeUpdated(GameRuntimeUpdateMessage),
}

#[tarpc::service]
trait MeshRpc {
    async fn peer_message(msg: PeerMessage);
    async fn peer_list_sync(msg: PeerListSyncMessage);
    async fn peer_added(msg: PeerChangeMessage);
    async fn peer_removed(msg: PeerChangeMessage);
    async fn game_runtime_updated(msg: GameRuntimeUpdateMessage);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PeerListSyncMessage {
    region: RegionId,
    peers_by_app: Vec<(AppId, Vec<PeerId>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PeerChangeMessage {
    peer_id: PeerId,
    app_id: AppId,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum GameRuntimeUpdateKind {
    Published,
    Deleted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct GameRuntimeUpdateMessage {
    pub game_id: u64,
    pub version: u64,
    pub kind: GameRuntimeUpdateKind,
}

impl GameRuntimeUpdateMessage {
    pub fn published(game_id: u64, version: u64) -> Self {
        Self {
            game_id,
            version,
            kind: GameRuntimeUpdateKind::Published,
        }
    }

    pub fn deleted(game_id: u64, version: u64) -> Self {
        Self {
            game_id,
            version,
            kind: GameRuntimeUpdateKind::Deleted,
        }
    }

    fn supersedes(&self, other: &Self) -> bool {
        self.version > other.version
            || (self.version == other.version
                && self.kind == GameRuntimeUpdateKind::Deleted
                && other.kind == GameRuntimeUpdateKind::Published)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct RegionId([u8; 4]);

impl RegionId {
    pub fn from_bytes(bytes: [u8; 4]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 4] {
        &self.0
    }
}

impl Display for RegionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let filtered: Vec<u8> = self
            .as_bytes()
            .iter()
            .copied()
            .filter(|&b| b != 0)
            .collect();
        if let Ok(s) = std::str::from_utf8(&filtered) {
            write!(f, "{}", s)
        } else {
            write!(f, "{}", hex::encode(self.as_bytes()))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct PeerId {
    region: RegionId,
    timestamp: u64,
    randomness: u32,
}

impl PeerId {
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[0..4].copy_from_slice(self.region.as_bytes());
        bytes[4..12].copy_from_slice(&self.timestamp.to_be_bytes());
        bytes[12..16].copy_from_slice(&self.randomness.to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let mut region_bytes = [0u8; 4];
        region_bytes.copy_from_slice(&bytes[0..4]);
        let mut timestamp_bytes = [0u8; 8];
        timestamp_bytes.copy_from_slice(&bytes[4..12]);
        let mut randomness_bytes = [0u8; 4];
        randomness_bytes.copy_from_slice(&bytes[12..16]);
        let region = RegionId::from_bytes(region_bytes);
        let timestamp = u64::from_be_bytes(timestamp_bytes);
        let randomness = u32::from_be_bytes(randomness_bytes);
        Self {
            timestamp,
            randomness,
            region,
        }
    }
}

impl Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}-{}", self.timestamp, self.randomness, self.region)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerMessage {
    from_peer: PeerId,
    to_peers: Vec<PeerId>,
    app_id: AppId,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct AppId {
    pub game_id: u64,
    pub version: u64,
}

impl Display for AppId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.game_id, self.version)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerMessageApp {
    from_peer: PeerId,
    data: Arc<Vec<u8>>,
}

impl PeerMessageApp {
    pub fn from_peer(&self) -> PeerId {
        self.from_peer
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

#[async_trait::async_trait]
pub trait Discovery: Send + Sync {
    async fn discover_peers(&self) -> anyhow::Result<HashSet<(RegionId, SocketAddr)>>;
}

pub struct EnvDiscovery;

impl EnvDiscovery {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl Discovery for EnvDiscovery {
    async fn discover_peers(&self) -> anyhow::Result<HashSet<(RegionId, SocketAddr)>> {
        let nodes_env = std::env::var("PEER_NODES").unwrap_or_else(|_| String::new());

        if nodes_env.is_empty() {
            return Ok(HashSet::new());
        }

        nodes_env
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| {
                let colon_idx = s.find(':').ok_or_else(|| {
                    anyhow::anyhow!("Invalid peer node format '{}': expected 'region:address' (e.g., 'loca:127.0.0.1:3001')", s)
                })?;

                let region_str = &s[..colon_idx];
                let addr_str = &s[colon_idx + 1..];

                if region_str.is_empty() {
                    return Err(anyhow::anyhow!("Invalid peer node format '{}': region cannot be empty", s));
                }

                let mut region_bytes = [0u8; 4];
                let region_id_bytes = region_str.as_bytes();
                let copy_len = region_id_bytes.len().min(4);
                region_bytes[..copy_len].copy_from_slice(&region_id_bytes[..copy_len]);
                let region = RegionId::from_bytes(region_bytes);

                let addr = addr_str.parse::<SocketAddr>()
                    .map_err(|e| anyhow::anyhow!("Invalid socket address '{}': {}", addr_str, e))?;

                Ok((region, addr))
            })
            .collect()
    }
}

pub struct LocalDiscovery {
    registry_path: std::path::PathBuf,
    self_entry: Mutex<Option<LocalRegistryEntry>>,
}

#[derive(Clone, Copy)]
struct LocalRegistryEntry {
    region: RegionId,
    port: u16,
    pid: u32,
}

impl LocalDiscovery {
    pub fn new() -> Self {
        let uid = unsafe { libc::getuid() };
        let registry_path =
            std::path::PathBuf::from(format!("/tmp/terminal-games-mesh-{}.registry", uid));
        Self {
            registry_path,
            self_entry: Mutex::new(None),
        }
    }

    pub fn allocate_region(&self) -> anyhow::Result<RegionId> {
        let entries = self.read_entries();
        let used: std::collections::HashSet<RegionId> =
            entries.into_iter().map(|e| e.region).collect();

        for i in 0..=9u8 {
            let region = RegionId::from_bytes([b'l', b'o', b'c', b'0' + i]);
            if !used.contains(&region) {
                return Ok(region);
            }
        }
        Err(anyhow::anyhow!(
            "No available region slots (loc0-loc9 all in use)"
        ))
    }

    pub async fn register(&self, region: RegionId, port: u16) -> anyhow::Result<()> {
        let pid = std::process::id();
        let entry = LocalRegistryEntry { region, port, pid };
        *self.self_entry.lock().await = Some(entry);
        self.write_entry(&entry).await
    }

    pub async fn unregister(&self) -> anyhow::Result<()> {
        let entry = self.self_entry.lock().await.take();
        if let Some(entry) = entry {
            self.remove_entry(&entry).await?;
        }
        Ok(())
    }

    async fn write_entry(&self, entry: &LocalRegistryEntry) -> anyhow::Result<()> {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.registry_path)?;
        writeln!(
            file,
            "{}:{}:{}",
            hex::encode(entry.region.as_bytes()),
            entry.port,
            entry.pid
        )?;
        Ok(())
    }

    async fn remove_entry(&self, entry: &LocalRegistryEntry) -> anyhow::Result<()> {
        let contents = match std::fs::read_to_string(&self.registry_path) {
            Ok(c) => c,
            Err(_) => return Ok(()),
        };
        let filtered: Vec<&str> = contents
            .lines()
            .filter(|line| {
                if let Some((_, rest)) = line.split_once(':') {
                    if let Some((port_str, pid_str)) = rest.split_once(':') {
                        let port_match = port_str.parse::<u16>().ok() == Some(entry.port);
                        let pid_match = pid_str.parse::<u32>().ok() == Some(entry.pid);
                        return !(port_match && pid_match);
                    }
                }
                true
            })
            .collect();
        std::fs::write(&self.registry_path, filtered.join("\n") + "\n")?;
        Ok(())
    }

    fn read_entries(&self) -> Vec<LocalRegistryEntry> {
        let contents = match std::fs::read_to_string(&self.registry_path) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        let entries: Vec<LocalRegistryEntry> = contents
            .lines()
            .filter_map(|line| {
                let parts: Vec<&str> = line.splitn(3, ':').collect();
                if parts.len() != 3 {
                    return None;
                }
                let region_hex = parts[0];
                let port = parts[1].parse::<u16>().ok()?;
                let pid = parts[2].parse::<u32>().ok()?;

                let alive = unsafe { libc::kill(pid as i32, 0) == 0 };
                if !alive {
                    return None;
                }

                let region_bytes: [u8; 4] = hex::decode(region_hex).ok()?.try_into().ok()?;
                let region = RegionId::from_bytes(region_bytes);

                Some(LocalRegistryEntry { region, port, pid })
            })
            .collect();

        let canonical = if entries.is_empty() {
            String::new()
        } else {
            let lines: Vec<String> = entries
                .iter()
                .map(|entry| {
                    format!(
                        "{}:{}:{}",
                        hex::encode(entry.region.as_bytes()),
                        entry.port,
                        entry.pid
                    )
                })
                .collect();
            lines.join("\n") + "\n"
        };
        if canonical != contents {
            let _ = std::fs::write(&self.registry_path, canonical);
        }

        entries
    }
}

#[async_trait::async_trait]
impl Discovery for LocalDiscovery {
    async fn discover_peers(&self) -> anyhow::Result<HashSet<(RegionId, SocketAddr)>> {
        let self_entry = *self.self_entry.lock().await;
        let entries = self.read_entries();

        let peers = entries
            .into_iter()
            .filter(|e| {
                if let Some(ref self_e) = self_entry {
                    e.pid != self_e.pid
                } else {
                    true
                }
            })
            .map(|e| {
                let addr: SocketAddr = ([127, 0, 0, 1], e.port).into();
                (e.region, addr)
            })
            .collect();

        Ok(peers)
    }
}

struct ActiveConnection {
    tx: tokio::sync::mpsc::Sender<Message>,
    conn_fd: RawFd,
    addr: SocketAddr,
}

enum RegionState {
    Pending(SocketAddr),
    Active(ActiveConnection),
}

impl RegionState {
    fn as_active(&self) -> Option<&ActiveConnection> {
        match self {
            RegionState::Active(conn) => Some(conn),
            RegionState::Pending(_) => None,
        }
    }
}

struct MeshInner {
    region: RegionId,
    regions: Mutex<HashMap<RegionId, RegionState>>,
    peers: Mutex<HashMap<AppId, HashMap<PeerId, tokio::sync::mpsc::Sender<PeerMessageApp>>>>,
    global_peers: Mutex<HashMap<AppId, HashSet<PeerId>>>,
    latest_game_runtime_updates: Mutex<HashMap<u64, GameRuntimeUpdateMessage>>,
    discovery: Arc<dyn Discovery>,
    cancel: CancellationToken,
    tasks: TaskTracker,
    heal_now: Notify,
    game_runtime_updates: broadcast::Sender<GameRuntimeUpdateMessage>,
}

#[derive(Clone)]
pub struct Mesh {
    inner: Arc<MeshInner>,
}

impl Mesh {
    pub fn new(discovery: Arc<dyn Discovery>) -> Self {
        let region_id_str = std::env::var("REGION_ID").unwrap_or_else(|_| "loca".to_string());
        let mut region_bytes = [0u8; 4];
        let region_id_bytes = region_id_str.as_bytes();
        let copy_len = region_id_bytes.len().min(4);
        region_bytes[..copy_len].copy_from_slice(&region_id_bytes[..copy_len]);
        Self::with_region(discovery, RegionId::from_bytes(region_bytes))
    }

    pub fn with_region(discovery: Arc<dyn Discovery>, region: RegionId) -> Self {
        Self {
            inner: Arc::new(MeshInner {
                game_runtime_updates: broadcast::channel(64).0,
                region,
                regions: Default::default(),
                peers: Default::default(),
                global_peers: Default::default(),
                latest_game_runtime_updates: Default::default(),
                discovery,
                cancel: CancellationToken::new(),
                tasks: TaskTracker::new(),
                heal_now: Notify::new(),
            }),
        }
    }

    pub fn region(&self) -> RegionId {
        self.inner.region
    }

    pub async fn new_peer(
        &self,
        app_id: AppId,
    ) -> (
        PeerId,
        tokio::sync::mpsc::Receiver<PeerMessageApp>,
        tokio::sync::mpsc::Sender<(Vec<PeerId>, Vec<u8>)>,
    ) {
        let peer_id = PeerId {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or_default(),
            randomness: rand_core::OsRng.next_u32(),
            region: self.inner.region,
        };

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let (tx2, mut rx2) = tokio::sync::mpsc::channel(1);

        let inner = self.inner.clone();
        self.inner.tasks.spawn(async move {
            inner.peers.lock().await
                .entry(app_id)
                .or_default()
                .insert(peer_id, tx);
            inner.global_peers.lock().await
                .entry(app_id)
                .or_insert_with(HashSet::new)
                .insert(peer_id);
            inner.broadcast(Message::PeerAdded(PeerChangeMessage { peer_id, app_id })).await;

            loop {
                tokio::select! {
                    _ = inner.cancel.cancelled() => break,
                    msg = rx2.recv() => {
                        match msg {
                            Some((to_peers, data)) => {
                                inner.send_peer_message(PeerMessage { from_peer: peer_id, to_peers, app_id, data }).await;
                            }
                            None => break,
                        }
                    }
                }
            }

            {
                let mut peers = inner.peers.lock().await;
                if let Some(app_peers) = peers.get_mut(&app_id) {
                    app_peers.remove(&peer_id);
                    if app_peers.is_empty() {
                        peers.remove(&app_id);
                    }
                }
            }
            {
                let mut global_peers = inner.global_peers.lock().await;
                if let Some(peers) = global_peers.get_mut(&app_id) {
                    peers.remove(&peer_id);
                    if peers.is_empty() {
                        global_peers.remove(&app_id);
                    }
                }
            }
            inner.broadcast(Message::PeerRemoved(PeerChangeMessage { peer_id, app_id })).await;
        });

        (peer_id, rx, tx2)
    }

    pub async fn get_peers_for_app(&self, app_id: AppId) -> HashSet<PeerId> {
        self.inner
            .global_peers
            .lock()
            .await
            .get(&app_id)
            .cloned()
            .unwrap_or_default()
    }

    pub async fn serve(&self) -> anyhow::Result<SocketAddr> {
        let listen_addr: SocketAddr = std::env::var("PEER_LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:3001".to_string())
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid PEER_LISTEN_ADDR: {}", e))?;
        self.serve_on(listen_addr).await
    }

    pub async fn serve_on(&self, listen_addr: SocketAddr) -> anyhow::Result<SocketAddr> {
        let listener = TcpListener::bind(listen_addr).await?;
        let local_addr = listener.local_addr()?;

        let inner = self.inner.clone();
        self.inner.tasks.spawn(async move {
            loop {
                tokio::select! {
                    _ = inner.cancel.cancelled() => break,
                    result = listener.accept() => {
                        match result {
                            Ok((stream, peer_addr)) => {
                                tracing::trace!(peer = %peer_addr, "Incoming mesh connection");
                                let inner2 = inner.clone();
                                inner.tasks.spawn(async move {
                                    inner2.handle_connection(stream, peer_addr, None).await;
                                });
                            }
                            Err(e) => {
                                tracing::error!("Failed to accept connection: {}", e);
                            }
                        }
                    }
                }
            }
        });

        Ok(local_addr)
    }

    pub async fn start_discovery(&self) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        self.inner.tasks.spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = inner.cancel.cancelled() => break,
                    _ = interval.tick() => {
                        if let Err(e) = inner.heal_network().await {
                            tracing::error!(?e, "Failed to connect to discovered peers");
                        }
                    }
                    _ = inner.heal_now.notified() => {
                        if let Err(e) = inner.heal_network().await {
                            tracing::warn!(?e, "Failed immediate mesh heal");
                        }
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn get_region_latency(&self, region: RegionId) -> Option<Duration> {
        let regions = self.inner.regions.lock().await;
        regions
            .get(&region)
            .and_then(|s| s.as_active())
            .and_then(|conn| get_tcp_rtt_from_fd(conn.conn_fd).ok())
    }

    pub async fn discover_regions(&self) -> anyhow::Result<Vec<RegionId>> {
        let mut regions = self
            .inner
            .discovery
            .discover_peers()
            .await?
            .into_iter()
            .map(|(region, _)| region)
            .collect::<Vec<_>>();
        regions.push(self.inner.region);
        regions.sort_unstable();
        regions.dedup();
        Ok(regions)
    }

    pub async fn graceful_shutdown(&self) {
        tracing::debug!("Mesh graceful shutdown initiated");
        self.inner.cancel.cancel();
        self.inner.tasks.close();
        self.inner.tasks.wait().await;
        tracing::debug!("Mesh graceful shutdown complete");
    }

    pub fn subscribe_game_runtime_updates(&self) -> broadcast::Receiver<GameRuntimeUpdateMessage> {
        self.inner.game_runtime_updates.subscribe()
    }

    pub async fn replace_game_runtime_snapshot(&self, updates: Vec<GameRuntimeUpdateMessage>) {
        self.inner.replace_game_runtime_snapshot(updates).await;
    }

    pub async fn propagate_game_runtime_update(&self, update: GameRuntimeUpdateMessage) {
        if !self.inner.remember_game_runtime_update(update).await {
            return;
        }
        self.inner
            .broadcast(Message::GameRuntimeUpdated(update))
            .await;
    }
}

impl MeshInner {
    async fn broadcast(&self, message: Message) {
        let region_txs: Vec<_> = {
            let regions = self.regions.lock().await;
            regions
                .iter()
                .filter_map(|(_, state)| state.as_active().map(|conn| conn.tx.clone()))
                .collect()
        };

        let send_futures: Vec<_> = region_txs
            .into_iter()
            .map(|tx| {
                let msg = message.clone();
                async move {
                    let _ = tx.send(msg).await;
                }
            })
            .collect();

        futures::future::join_all(send_futures).await;
    }

    async fn send_peer_message(&self, message: PeerMessage) {
        let mut region_partitions: HashMap<RegionId, Vec<PeerId>> = Default::default();
        let num_peers = message.to_peers.len();
        for peer_id in message.to_peers {
            region_partitions
                .entry(peer_id.region)
                .or_insert_with(|| Vec::with_capacity(num_peers))
                .push(peer_id);
        }

        let data = Arc::new(message.data);

        let mut send_futures: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> =
            Vec::with_capacity(region_partitions.len() - 1 + num_peers);
        for (region, peer_ids) in region_partitions {
            if region == self.region {
                let local_sends: Vec<_> = {
                    let peers = self.peers.lock().await;
                    let app_peers = peers.get(&message.app_id);
                    peer_ids
                        .into_iter()
                        .filter_map(|peer_id| {
                            app_peers
                                .and_then(|p| p.get(&peer_id))
                                .map(|tx| (peer_id, tx.clone()))
                        })
                        .collect()
                };

                for (peer_id, tx) in local_sends {
                    let data = data.clone();
                    let from_peer = message.from_peer;
                    let app_id = message.app_id;

                    send_futures.push(Box::pin(async move {
                        if let Err(error) = tx.send(PeerMessageApp { from_peer, data }).await {
                            tracing::error!(%peer_id, %app_id, error=?error, "failed to send to local peer");
                        }
                    }));
                }
            } else {
                let remote_tx = {
                    let regions = self.regions.lock().await;
                    regions
                        .get(&region)
                        .and_then(|s| s.as_active())
                        .map(|conn| conn.tx.clone())
                };

                if let Some(tx) = remote_tx {
                    let batched_message = PeerMessage {
                        from_peer: message.from_peer,
                        to_peers: peer_ids,
                        app_id: message.app_id,
                        data: (*data).clone(),
                    };

                    send_futures.push(Box::pin(async move {
                        if let Err(error) = tx.send(Message::PeerMessage(batched_message)).await {
                            tracing::error!(%region, ?error, "failed to send to remote region");
                        }
                    }));
                } else {
                    tracing::error!(%region, "remote region not connected");
                }
            }
        }

        futures::future::join_all(send_futures).await;
    }

    async fn connect_to_node(
        self: &Arc<Self>,
        region: RegionId,
        addr: SocketAddr,
    ) -> anyhow::Result<()> {
        {
            let mut regions = self.regions.lock().await;
            if let Some(existing) = regions.get(&region) {
                let existing_addr = match existing {
                    RegionState::Pending(existing_addr) => *existing_addr,
                    RegionState::Active(conn) => conn.addr,
                };
                if existing_addr == addr {
                    tracing::debug!(%region, "Already connected or connecting to region, skipping");
                    return Ok(());
                }
            }
            regions.insert(region, RegionState::Pending(addr));
        }

        let stream = match TcpStream::connect(addr).await {
            Ok(stream) => {
                tracing::info!(%region, peer = %addr, "Connected TCP to mesh node");
                stream
            }
            Err(e) => {
                let mut regions = self.regions.lock().await;
                if matches!(regions.get(&region), Some(RegionState::Pending(pending_addr)) if *pending_addr == addr)
                {
                    regions.remove(&region);
                }
                return Err(e.into());
            }
        };

        let inner = self.clone();
        self.tasks.spawn(async move {
            inner.handle_connection(stream, addr, Some(region)).await;
        });

        Ok(())
    }

    async fn heal_network(self: &Arc<Self>) -> anyhow::Result<()> {
        let discovered = self.discovery.discover_peers().await?;
        let discovered_by_region: HashMap<RegionId, SocketAddr> = discovered.into_iter().collect();
        let current_regions: HashMap<RegionId, SocketAddr> = {
            let regions = self.regions.lock().await;
            regions
                .iter()
                .map(|(region, state)| {
                    let addr = match state {
                        RegionState::Pending(addr) => *addr,
                        RegionState::Active(conn) => conn.addr,
                    };
                    (*region, addr)
                })
                .collect()
        };

        let targets: Vec<(RegionId, SocketAddr)> = discovered_by_region
            .into_iter()
            .filter(|(region, _)| *region != self.region)
            .filter(|(region, addr)| current_regions.get(region) != Some(addr))
            .collect();

        if targets.is_empty() {
            return Ok(());
        }

        tracing::info!(count = targets.len(), "Connecting to discovered peers");

        let connect_futures: Vec<_> = targets
            .into_iter()
            .map(|(region, addr)| {
                let inner = self.clone();
                async move {
                    if let Err(e) = inner.connect_to_node(region, addr).await {
                        tracing::warn!(%region, %addr, ?e, "Failed to connect");
                    }
                }
            })
            .collect();

        futures::future::join_all(connect_futures).await;
        Ok(())
    }

    async fn handle_connection(
        self: &Arc<Self>,
        stream: TcpStream,
        addr: SocketAddr,
        expected_region: Option<RegionId>,
    ) {
        let fd = stream.as_raw_fd();
        let result = self
            .handle_connection_inner(stream, addr, expected_region)
            .await;

        if let Err(error) = &result {
            tracing::warn!(%addr, ?expected_region, ?error, "Mesh connection failed");
        }

        if expected_region.is_some() {
            if let Some(region) = result.as_ref().ok().copied().or(expected_region) {
                let removed = {
                    let mut regions = self.regions.lock().await;
                    match regions.get(&region) {
                        Some(RegionState::Pending(pending_addr)) if *pending_addr == addr => {
                            regions.remove(&region);
                            true
                        }
                        Some(RegionState::Active(conn)) if conn.conn_fd == fd => {
                            regions.remove(&region);
                            true
                        }
                        _ => false,
                    }
                };
                if removed {
                    tracing::info!(region=%region, %addr, "Disconnected");
                }
            }
        } else if let Ok(region) = result {
            tracing::info!(region=%region, %addr, "Disconnected");
        }
    }

    async fn handle_connection_inner(
        self: &Arc<Self>,
        stream: TcpStream,
        addr: SocketAddr,
        expected_region: Option<RegionId>,
    ) -> anyhow::Result<RegionId> {
        let role = if expected_region.is_some() {
            ConnectionRole::Outgoing
        } else {
            ConnectionRole::Incoming
        };

        let fd = stream.as_raw_fd();
        let stream = self.tls_wrap_stream(stream, role).await?;

        if matches!(role, ConnectionRole::Outgoing) {
            let mut codec = LengthDelimitedCodec::new();
            codec.set_max_frame_length(MESH_MAX_FRAME_LEN);
            let framed = Framed::new(stream, codec);
            let transport = tarpc::serde_transport::new::<
                _,
                tarpc::Response<MeshRpcResponse>,
                tarpc::ClientMessage<MeshRpcRequest>,
                _,
            >(framed, tarpc::tokio_serde::formats::Bincode::default());
            let rpc_client = MeshRpcClient::new(client::Config::default(), transport).spawn();
            let their_region = expected_region.expect("outgoing must have expected_region");

            let local_peers_by_app = self
                .peers
                .lock()
                .await
                .iter()
                .map(|(app_id, peer_map)| (*app_id, peer_map.keys().copied().collect()))
                .collect();
            let local_game_runtime_updates = self
                .latest_game_runtime_updates
                .lock()
                .await
                .values()
                .cloned()
                .collect::<Vec<_>>();
            rpc_client
                .peer_list_sync(
                    context::current(),
                    PeerListSyncMessage {
                        region: self.region,
                        peers_by_app: local_peers_by_app,
                    },
                )
                .await
                .map_err(|e| anyhow::anyhow!("Failed to send initial peer list sync: {}", e))?;
            for update in local_game_runtime_updates {
                rpc_client
                    .game_runtime_updated(context::current(), update)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to send initial game runtime sync: {}", e)
                    })?;
            }

            let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(1);
            let (done_tx, mut done_rx) = tokio::sync::oneshot::channel::<()>();
            let cancel = self.cancel.clone();
            self.tasks.spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        msg = rx.recv() => {
                            let Some(msg) = msg else { break };
                            let send_result = match msg {
                                Message::PeerMessage(msg) => rpc_client.peer_message(context::current(), msg).await,
                                Message::PeerListSync(msg) => rpc_client.peer_list_sync(context::current(), msg).await,
                                Message::PeerAdded(msg) => rpc_client.peer_added(context::current(), msg).await,
                                Message::PeerRemoved(msg) => rpc_client.peer_removed(context::current(), msg).await,
                                Message::GameRuntimeUpdated(msg) => rpc_client.game_runtime_updated(context::current(), msg).await,
                            };
                            if let Err(error) = send_result {
                                tracing::warn!(region=%their_region, ?error, "RPC send failed");
                                break;
                            }
                        }
                    }
                }
                let _ = done_tx.send(());
            });

            self.regions.lock().await.insert(
                their_region,
                RegionState::Active(ActiveConnection {
                    tx,
                    conn_fd: fd,
                    addr,
                }),
            );

            tokio::select! {
                _ = self.cancel.cancelled() => {}
                _ = &mut done_rx => {}
            }

            self.handle_region_disconnect(their_region).await;
            return Ok(their_region);
        }

        let remote_region = Arc::new(Mutex::new(None));
        let remote_region_for_server = remote_region.clone();
        let inner = self.clone();
        let mut codec = LengthDelimitedCodec::new();
        codec.set_max_frame_length(MESH_MAX_FRAME_LEN);
        let framed = Framed::new(stream, codec);
        let transport = tarpc::serde_transport::new::<
            _,
            tarpc::ClientMessage<MeshRpcRequest>,
            tarpc::Response<MeshRpcResponse>,
            _,
        >(framed, tarpc::tokio_serde::formats::Bincode::default());
        let channel = server::BaseChannel::with_defaults(transport);
        let serve_fut = async move {
            channel
                .execute(
                    MeshRpcServer {
                        inner,
                        remote_region: remote_region_for_server,
                    }
                    .serve(),
                )
                .for_each(|response| async move {
                    let _ = response.await;
                })
                .await;
        };
        tokio::select! {
            _ = self.cancel.cancelled() => {}
            _ = serve_fut => {}
        }

        let maybe_region = *remote_region.lock().await;
        if let Some(region) = maybe_region {
            self.handle_region_disconnect(region).await;
            Ok(region)
        } else {
            Err(anyhow::anyhow!("connection closed before authentication"))
        }
    }

    async fn handle_peer_list_sync(&self, msg: PeerListSyncMessage) {
        let mut global_peers = self.global_peers.lock().await;

        // Remove old peers from this region
        for (_, peers) in global_peers.iter_mut() {
            peers.retain(|p| p.region != msg.region);
        }
        global_peers.retain(|_, peers| !peers.is_empty());

        // Add new peers from this region
        for (app_id, peer_ids) in msg.peers_by_app {
            global_peers
                .entry(app_id)
                .or_insert_with(HashSet::new)
                .extend(peer_ids);
        }

        let total_peers: usize = global_peers.values().map(|s| s.len()).sum();
        tracing::debug!(region=%msg.region, total_peers, "Received peer list sync");
    }

    async fn handle_peer_added(&self, msg: PeerChangeMessage) {
        let mut global_peers = self.global_peers.lock().await;
        global_peers
            .entry(msg.app_id)
            .or_insert_with(HashSet::new)
            .insert(msg.peer_id);

        tracing::debug!(peer=%msg.peer_id, app=%msg.app_id, "Remote peer added");
    }

    async fn handle_peer_removed(&self, msg: PeerChangeMessage) {
        let mut global_peers = self.global_peers.lock().await;
        if let Some(peers) = global_peers.get_mut(&msg.app_id) {
            peers.remove(&msg.peer_id);
            if peers.is_empty() {
                global_peers.remove(&msg.app_id);
            }
        }

        tracing::debug!(peer=%msg.peer_id, app=%msg.app_id, "Remote peer removed");
    }

    async fn handle_region_disconnect(&self, region: RegionId) {
        let mut global_peers = self.global_peers.lock().await;

        for (_, peers) in global_peers.iter_mut() {
            peers.retain(|p| p.region != region);
        }

        global_peers.retain(|_, peers| !peers.is_empty());

        tracing::debug!(%region, "Region disconnected, removed all its peers");
    }

    async fn handle_peer_message(&self, msg: PeerMessage) {
        let data = Arc::new(msg.data);
        let peer_sends: Vec<_> = {
            let peers = self.peers.lock().await;
            let app_peers = peers.get(&msg.app_id);
            msg.to_peers
                .iter()
                .filter_map(|&peer_id| {
                    app_peers
                        .and_then(|p| p.get(&peer_id))
                        .map(|tx| (peer_id, tx.clone()))
                })
                .collect()
        };

        let send_futures: Vec<_> = peer_sends
            .into_iter()
            .map(|(peer_id, tx)| {
                let data = data.clone();
                Box::pin(async move {
                    tx.send(PeerMessageApp {
                        from_peer: msg.from_peer,
                        data,
                    })
                    .await
                    .is_err()
                    .then_some(peer_id)
                }) as Pin<Box<dyn Future<Output = Option<PeerId>> + Send>>
            })
            .collect();

        let failed: Vec<_> = futures::future::join_all(send_futures)
            .await
            .into_iter()
            .flatten()
            .collect();

        if !failed.is_empty() {
            let mut peers = self.peers.lock().await;
            if let Some(app_peers) = peers.get_mut(&msg.app_id) {
                for peer_id in failed {
                    app_peers.remove(&peer_id);
                }
                if app_peers.is_empty() {
                    peers.remove(&msg.app_id);
                }
            }
        }
    }

    async fn handle_game_runtime_updated(&self, msg: GameRuntimeUpdateMessage) {
        if self.remember_game_runtime_update(msg).await {
            let _ = self.game_runtime_updates.send(msg);
        }
    }

    async fn replace_game_runtime_snapshot(&self, updates: Vec<GameRuntimeUpdateMessage>) {
        let snapshot_ids = updates.iter().map(|update| update.game_id).collect::<HashSet<_>>();
        let mut latest = self.latest_game_runtime_updates.lock().await;
        latest.retain(|game_id, update| {
            snapshot_ids.contains(game_id) || update.kind == GameRuntimeUpdateKind::Deleted
        });
        for update in updates {
            latest.insert(update.game_id, update);
        }
    }

    async fn remember_game_runtime_update(&self, update: GameRuntimeUpdateMessage) -> bool {
        let mut latest = self.latest_game_runtime_updates.lock().await;
        match latest.get(&update.game_id) {
            Some(existing) if !update.supersedes(existing) => false,
            _ => {
                latest.insert(update.game_id, update);
                true
            }
        }
    }
}

const MESH_MAX_FRAME_LEN: usize = 8 * 1024 * 1024;
const MESH_TLS_SERVER_NAME: &str = "mesh.internal";
const MESH_PINNED_PEM_ENV: &str = "MESH_PINNED_PEM_BASE64";

#[derive(Clone, Copy)]
enum ConnectionRole {
    Outgoing,
    Incoming,
}

trait MeshIoStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> MeshIoStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

impl MeshInner {
    fn mtls_pem_value() -> anyhow::Result<Vec<u8>> {
        let value = std::env::var(MESH_PINNED_PEM_ENV)
            .map_err(|_| anyhow::anyhow!("{MESH_PINNED_PEM_ENV} must be set"))?;
        if value.is_empty() {
            return Err(anyhow::anyhow!("{MESH_PINNED_PEM_ENV} must not be empty"));
        }
        let pem_data = base64::engine::general_purpose::STANDARD
            .decode(value)
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to decode base64 from {}: {}",
                    MESH_PINNED_PEM_ENV,
                    e
                )
            })?;
        if pem_data.is_empty() {
            return Err(anyhow::anyhow!(
                "{} decoded to empty PEM data",
                MESH_PINNED_PEM_ENV
            ));
        }
        Ok(pem_data)
    }

    fn load_tls_materials() -> anyhow::Result<(
        Vec<rustls::pki_types::CertificateDer<'static>>,
        rustls::pki_types::PrivateKeyDer<'static>,
        rustls::pki_types::CertificateDer<'static>,
    )> {
        let pem_data = Self::mtls_pem_value()?;
        let mut cert_reader = std::io::BufReader::new(pem_data.as_slice());
        let certs = rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to parse certificates from {}: {}",
                    MESH_PINNED_PEM_ENV,
                    e
                )
            })?;
        if certs.is_empty() {
            return Err(anyhow::anyhow!(
                "{} did not contain certificates",
                MESH_PINNED_PEM_ENV
            ));
        }

        let mut key_reader = std::io::BufReader::new(pem_data.as_slice());
        let key = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to parse private key from {}: {}",
                    MESH_PINNED_PEM_ENV,
                    e
                )
            })?
            .ok_or_else(|| {
                anyhow::anyhow!("{} did not contain a private key", MESH_PINNED_PEM_ENV)
            })?;
        let pinned_cert = certs[0].clone();
        Ok((certs, key, pinned_cert))
    }

    fn tls_server_config() -> anyhow::Result<Arc<rustls::ServerConfig>> {
        let (certs, key, pinned_cert) = Self::load_tls_materials()?;
        let client_verifier = Arc::new(PinnedClientCertVerifier::new(pinned_cert));
        let mut config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certs, key)
            .map_err(|e| anyhow::anyhow!("Failed to build TLS server config: {}", e))?;
        config
            .alpn_protocols
            .push(b"terminal-games-mesh-v1".to_vec());
        Ok(Arc::new(config))
    }

    fn tls_client_config() -> anyhow::Result<Arc<rustls::ClientConfig>> {
        let (certs, key, pinned_cert) = Self::load_tls_materials()?;
        let mut config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(PinnedServerCertVerifier::new(pinned_cert)))
            .with_client_auth_cert(certs, key)
            .map_err(|e| anyhow::anyhow!("Failed to build TLS client config: {}", e))?;
        config
            .alpn_protocols
            .push(b"terminal-games-mesh-v1".to_vec());
        Ok(Arc::new(config))
    }

    async fn tls_wrap_stream(
        &self,
        stream: TcpStream,
        role: ConnectionRole,
    ) -> anyhow::Result<Pin<Box<dyn MeshIoStream>>> {
        if !matches!(
            std::env::var(MESH_PINNED_PEM_ENV).as_deref(),
            Ok(value) if !value.is_empty()
        ) {
            tracing::warn!(
                "{MESH_PINNED_PEM_ENV} is not configured, using plain TCP for mesh connections"
            );
            let stream: Pin<Box<dyn MeshIoStream>> = Box::pin(stream);
            return Ok(stream);
        }

        match role {
            ConnectionRole::Outgoing => {
                let connector = TlsConnector::from(Self::tls_client_config()?);
                let dnsname = rustls::pki_types::ServerName::try_from(MESH_TLS_SERVER_NAME)
                    .map_err(|_| {
                        anyhow::anyhow!("Invalid TLS server name '{}'", MESH_TLS_SERVER_NAME)
                    })?;
                let tls_stream = connector
                    .connect(dnsname, stream)
                    .await
                    .map_err(|e| anyhow::anyhow!("TLS client handshake failed: {}", e))?;
                let stream: Pin<Box<dyn MeshIoStream>> = Box::pin(tls_stream);
                Ok(stream)
            }
            ConnectionRole::Incoming => {
                let acceptor = TlsAcceptor::from(Self::tls_server_config()?);
                let tls_stream = acceptor
                    .accept(stream)
                    .await
                    .map_err(|e| anyhow::anyhow!("TLS server handshake failed: {}", e))?;
                let stream: Pin<Box<dyn MeshIoStream>> = Box::pin(tls_stream);
                Ok(stream)
            }
        }
    }
}

#[derive(Debug)]
struct PinnedServerCertVerifier {
    pinned_cert: Vec<u8>,
}

impl PinnedServerCertVerifier {
    fn new(pinned_cert: rustls::pki_types::CertificateDer<'static>) -> Self {
        Self {
            pinned_cert: pinned_cert.as_ref().to_vec(),
        }
    }

    fn supported_algs() -> rustls::crypto::WebPkiSupportedAlgorithms {
        rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms
    }
}

impl rustls::client::danger::ServerCertVerifier for PinnedServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if end_entity.as_ref() != self.pinned_cert.as_slice() {
            return Err(rustls::Error::General(
                "peer certificate does not match pinned mesh certificate".to_string(),
            ));
        }
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &Self::supported_algs())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &Self::supported_algs())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        Self::supported_algs().supported_schemes()
    }
}

#[derive(Debug)]
struct PinnedClientCertVerifier {
    pinned_cert: Vec<u8>,
}

impl PinnedClientCertVerifier {
    fn new(pinned_cert: rustls::pki_types::CertificateDer<'static>) -> Self {
        Self {
            pinned_cert: pinned_cert.as_ref().to_vec(),
        }
    }

    fn supported_algs() -> rustls::crypto::WebPkiSupportedAlgorithms {
        rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms
    }
}

impl rustls::server::danger::ClientCertVerifier for PinnedClientCertVerifier {
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        if end_entity.as_ref() != self.pinned_cert.as_slice() {
            return Err(rustls::Error::General(
                "peer certificate does not match pinned mesh certificate".to_string(),
            ));
        }
        Ok(rustls::server::danger::ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &Self::supported_algs())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &Self::supported_algs())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        Self::supported_algs().supported_schemes()
    }
}

#[derive(Clone)]
struct MeshRpcServer {
    inner: Arc<MeshInner>,
    remote_region: Arc<Mutex<Option<RegionId>>>,
}

impl MeshRpc for MeshRpcServer {
    async fn peer_message(self, _: context::Context, msg: PeerMessage) {
        self.inner.handle_peer_message(msg).await;
    }

    async fn peer_list_sync(self, _: context::Context, msg: PeerListSyncMessage) {
        *self.remote_region.lock().await = Some(msg.region);
        self.inner.handle_peer_list_sync(msg).await;
        self.inner.heal_now.notify_one();
    }

    async fn peer_added(self, _: context::Context, msg: PeerChangeMessage) {
        self.inner.handle_peer_added(msg).await;
    }

    async fn peer_removed(self, _: context::Context, msg: PeerChangeMessage) {
        self.inner.handle_peer_removed(msg).await;
    }

    async fn game_runtime_updated(self, _: context::Context, msg: GameRuntimeUpdateMessage) {
        self.inner.handle_game_runtime_updated(msg).await;
    }
}
