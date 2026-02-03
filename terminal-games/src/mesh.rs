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

use bytes::{Buf, BufMut, BytesMut};
use futures::{SinkExt, StreamExt};
use rand_core::RngCore;
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use tokio_util::{
    codec::{Decoder, Encoder, Framed},
    sync::CancellationToken,
    task::TaskTracker,
};

use crate::rate_limiting::get_tcp_rtt_from_fd;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Message {
    Handshake(HandshakeMessage),
    PeerMessage(PeerMessage),
    PeerListSync(PeerListSyncMessage),
    PeerAdded(PeerChangeMessage),
    PeerRemoved(PeerChangeMessage),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HandshakeMessage {
    region: RegionId,
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
        let region = RegionId::from_bytes(bytes[0..4].try_into().unwrap());
        let timestamp = u64::from_be_bytes(bytes[4..12].try_into().unwrap());
        let randomness = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct AppId(pub u64);

impl Display for AppId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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

#[derive(Clone)]
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
        Err(anyhow::anyhow!("No available region slots (loc0-loc9 all in use)"))
    }

    pub async fn register(&self, region: RegionId, port: u16) -> anyhow::Result<()> {
        let pid = std::process::id();
        let entry = LocalRegistryEntry { region, port, pid };
        *self.self_entry.lock().await = Some(entry.clone());
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

        contents
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
            .collect()
    }
}

#[async_trait::async_trait]
impl Discovery for LocalDiscovery {
    async fn discover_peers(&self) -> anyhow::Result<HashSet<(RegionId, SocketAddr)>> {
        let self_entry = self.self_entry.lock().await.clone();
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
}

enum RegionState {
    Pending,
    Active(ActiveConnection),
}

impl RegionState {
    fn as_active(&self) -> Option<&ActiveConnection> {
        match self {
            RegionState::Active(conn) => Some(conn),
            RegionState::Pending => None,
        }
    }
}

struct MeshInner {
    region: RegionId,
    regions: Mutex<HashMap<RegionId, RegionState>>,
    peers: Mutex<HashMap<AppId, HashMap<PeerId, tokio::sync::mpsc::Sender<PeerMessageApp>>>>,
    global_peers: Mutex<HashMap<AppId, HashSet<PeerId>>>,
    discovery: Arc<dyn Discovery>,
    cancel: CancellationToken,
    tasks: TaskTracker,
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
                region,
                regions: Default::default(),
                peers: Default::default(),
                global_peers: Default::default(),
                discovery,
                cancel: CancellationToken::new(),
                tasks: TaskTracker::new(),
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
                .unwrap()
                .as_millis() as u64,
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
                                tracing::info!(peer = %peer_addr, "Incoming mesh connection");
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

    pub async fn graceful_shutdown(&self) {
        tracing::info!("Mesh graceful shutdown initiated");
        self.inner.cancel.cancel();
        self.inner.tasks.close();
        self.inner.tasks.wait().await;
        tracing::info!("Mesh graceful shutdown complete");
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
            // Mark as pending before connecting to prevent heal_network
            let mut regions = self.regions.lock().await;
            if regions.contains_key(&region) {
                tracing::debug!(%region, "Already connected or connecting to region, skipping");
                return Ok(());
            }
            regions.insert(region, RegionState::Pending);
        }

        let stream = match TcpStream::connect(addr).await {
            Ok(stream) => {
                tracing::info!(%region, peer = %addr, "Connected to mesh node");
                stream
            }
            Err(e) => {
                self.regions.lock().await.remove(&region);
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

        let current_regions: HashSet<RegionId> =
            self.regions.lock().await.keys().copied().collect();

        let targets: Vec<(RegionId, SocketAddr)> = discovered
            .into_iter()
            .filter(|(region, _)| *region != self.region)
            .filter(|(region, _)| !current_regions.contains(region))
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
        let result = self
            .handle_connection_inner(stream, addr, expected_region)
            .await;

        if let Some(region) = result.as_ref().ok().copied().or(expected_region) {
            self.regions.lock().await.remove(&region);
            tracing::info!(region=%region, %addr, "Disconnected");
        }
    }

    async fn handle_connection_inner(
        self: &Arc<Self>,
        stream: TcpStream,
        addr: SocketAddr,
        expected_region: Option<RegionId>,
    ) -> anyhow::Result<RegionId> {
        let fd = stream.as_raw_fd();
        let (mut sink, mut stream) = Framed::new(stream, MessageCodec).split();

        sink.send(Message::Handshake(HandshakeMessage {
            region: self.region,
        }))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send handshake: {}", e))?;

        let their_region = match stream.next().await {
            Some(Ok(Message::Handshake(h))) => h.region,
            _ => return Err(anyhow::anyhow!("Bad handshake")),
        };

        if let Some(expected) = expected_region {
            if their_region != expected {
                tracing::warn!(%expected, actual=%their_region, %addr, "Region mismatch");
                return Err(anyhow::anyhow!(
                    "Region mismatch: expected {}, got {}",
                    expected,
                    their_region
                ));
            }
        }

        let local_peers_by_app = self
            .peers
            .lock()
            .await
            .iter()
            .map(|(app_id, peer_map)| (*app_id, peer_map.keys().copied().collect()))
            .collect();
        sink.send(Message::PeerListSync(PeerListSyncMessage {
            region: self.region,
            peers_by_app: local_peers_by_app,
        }))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send peer list sync: {}", e))?;

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(1);
        let cancel = self.cancel.clone();
        self.tasks.spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    msg = rx.recv() => {
                        match msg {
                            Some(msg) => {
                                if sink.send(msg).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        self.regions.lock().await.insert(
            their_region,
            RegionState::Active(ActiveConnection { tx, conn_fd: fd }),
        );

        tracing::info!(region=%their_region, %addr, "Connected");

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                result = stream.next() => {
                    match result {
                        Some(Ok(message)) => {
                            match message {
                                Message::PeerMessage(msg) => {
                                    self.handle_peer_message(msg).await;
                                }
                                Message::PeerListSync(msg) => {
                                    self.handle_peer_list_sync(msg).await;
                                }
                                Message::PeerAdded(msg) => {
                                    self.handle_peer_added(msg).await;
                                }
                                Message::PeerRemoved(msg) => {
                                    self.handle_peer_removed(msg).await;
                                }
                                Message::Handshake(_) => {
                                    tracing::warn!(%their_region, "Unexpected handshake after connection established");
                                }
                            }
                        }
                        _ => break,
                    }
                }
            }
        }

        self.handle_region_disconnect(their_region).await;

        Ok(their_region)
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
}

struct MessageCodec;

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let length = u32::from_be_bytes(length_bytes) as usize;

        if src.len() < 4 + length {
            src.reserve(4 + length - src.len());
            return Ok(None);
        }

        src.advance(4);

        let message_bytes = src.split_to(length);

        match postcard::from_bytes::<Message>(&message_bytes) {
            Ok(message) => Ok(Some(message)),
            Err(e) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to deserialize message: {}", e),
            )),
        }
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let data = postcard::to_allocvec(&item).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to serialize message: {}", e),
            )
        })?;

        let length = data.len() as u32;
        dst.reserve(4 + data.len());
        dst.put_u32(length);

        dst.extend_from_slice(&data);

        Ok(())
    }
}
