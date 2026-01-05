// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{collections::{HashMap, HashSet}, fmt::Display, net::SocketAddr, pin::Pin, sync::Arc, time::Instant};
use std::future::Future;

use bytes::{Buf, BufMut, BytesMut};
use futures::{SinkExt, StreamExt};
use rand_core::RngCore;
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use tokio_util::{codec::{Decoder, Encoder, Framed}, sync::CancellationToken, task::TaskTracker};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Message {
    Handshake(HandshakeMessage),
    PeerMessage(PeerMessage),
    Ping(u64),
    Pong(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HandshakeMessage {
    region: RegionId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
        let filtered: Vec<u8> = self.as_bytes().iter().copied().filter(|&b| b != 0).collect();
        if let Ok(s) = std::str::from_utf8(&filtered) {
            write!(f, "{}", s)
        } else {
            write!(f, "{}", hex::encode(self.as_bytes()))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerId {
    timestamp: u64,
    randomness: u32,
    region: RegionId,
}

impl PeerId {
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&self.timestamp.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.randomness.to_le_bytes());
        bytes[12..16].copy_from_slice(self.region.as_bytes());
        bytes
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let timestamp = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let randomness = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let region = RegionId::from_bytes(bytes[12..16].try_into().unwrap());
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

/// Number of latency samples to keep for averaging
const LATENCY_SAMPLE_COUNT: usize = 3;

struct LatencyTracker {
    samples: [u32; LATENCY_SAMPLE_COUNT],
    sample_count: usize,
    next_index: usize,
    current_interval: std::time::Duration,
    next_ping_at: Instant,
    pub(crate) pending_ping: Option<(u64, Instant)>,
}

impl LatencyTracker {
    fn new() -> Self {
        Self {
            samples: [0; LATENCY_SAMPLE_COUNT],
            sample_count: 0,
            next_index: 0,
            current_interval: std::time::Duration::from_secs(2),
            next_ping_at: Instant::now(),
            pending_ping: None,
        }
    }

    fn record_sample(&mut self, latency_ms: u32) {
        self.samples[self.next_index] = latency_ms;
        self.next_index = (self.next_index + 1) % LATENCY_SAMPLE_COUNT;
        if self.sample_count < LATENCY_SAMPLE_COUNT {
            self.sample_count += 1;
        }

        self.current_interval = std::cmp::min(self.current_interval.mul_f64(1.5), std::time::Duration::from_secs(60));
        self.next_ping_at = Instant::now() + self.current_interval;
    }

    fn average_latency(&self) -> Option<u32> {
        if self.sample_count == 0 {
            return None;
        }
        let sum: u32 = self.samples[..self.sample_count].iter().sum();
        Some(sum / self.sample_count as u32)
    }

    fn should_ping(&self) -> bool {
        // Don't send a new ping if we're still waiting for a pong (unless it timed out)
        let pending_expired = self.pending_ping
            .as_ref()
            .map(|(_, sent_at)| sent_at.elapsed() > std::time::Duration::from_secs(30))
            .unwrap_or(false);

        (self.pending_ping.is_none() || pending_expired) && Instant::now() >= self.next_ping_at
    }

    fn mark_ping_sent(&mut self, seq: u64) {
        self.pending_ping = Some((seq, Instant::now()));
        // Schedule next ping attempt at current interval (prevents spamming if pong is lost)
        self.next_ping_at = Instant::now() + self.current_interval;
    }
}

struct ActiveConnection {
    tx: tokio::sync::mpsc::Sender<Message>,
    latency: LatencyTracker,
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

    fn as_active_mut(&mut self) -> Option<&mut ActiveConnection> {
        match self {
            RegionState::Active(conn) => Some(conn),
            RegionState::Pending => None,
        }
    }
}

struct MeshInner {
    region: RegionId,
    regions: Mutex<HashMap<RegionId, RegionState>>,
    peers: Mutex<HashMap<(PeerId, AppId), tokio::sync::mpsc::Sender<PeerMessageApp>>>,
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

        Self {
            inner: Arc::new(MeshInner {
                region: RegionId::from_bytes(region_bytes),
                regions: Default::default(),
                peers: Default::default(),
                discovery,
                cancel: CancellationToken::new(),
                tasks: TaskTracker::new(),
            }),
        }
    }

    pub async fn new_peer(
        &self,
        app_id: AppId,
    ) -> (PeerId, tokio::sync::mpsc::Receiver<PeerMessageApp>, tokio::sync::mpsc::Sender<(Vec<PeerId>, Vec<u8>)>) {
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
            inner.peers.lock().await.insert((peer_id, app_id), tx);

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

            inner.peers.lock().await.remove(&(peer_id, app_id));
        });

        (peer_id, rx, tx2)
    }

    pub async fn serve(&self) -> anyhow::Result<()> {
        let listen_addr: SocketAddr = std::env::var("PEER_LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:3001".to_string())
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid PEER_LISTEN_ADDR: {}", e))?;
        let listener = TcpListener::bind(listen_addr).await?;

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

        Ok(())
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

        let inner = self.inner.clone();
        self.inner.tasks.spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            let mut ping_seq: u64 = 0;
            loop {
                tokio::select! {
                    _ = inner.cancel.cancelled() => break,
                    _ = interval.tick() => {
                        inner.send_due_pings(&mut ping_seq).await;
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn get_region_latency(&self, region: RegionId) -> Option<u32> {
        let regions = self.inner.regions.lock().await;
        regions.get(&region)
            .and_then(|s| s.as_active())
            .and_then(|conn| conn.latency.average_latency())
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

        let mut send_futures: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> = Vec::with_capacity(region_partitions.len() - 1 + num_peers);
        for (region, peer_ids) in region_partitions {
            if region == self.region {
                let local_sends: Vec<_> = {
                    let peers = self.peers.lock().await;
                    peer_ids
                        .into_iter()
                        .filter_map(|peer_id| {
                            let key = (peer_id, message.app_id);
                            peers.get(&key).map(|tx| (peer_id, tx.clone()))
                        })
                        .collect()
                };

                for (peer_id, tx) in local_sends {
                    let data = data.clone();
                    let from_peer = message.from_peer;
                    let app_id = message.app_id;
                    
                    send_futures.push(Box::pin(async move {
                        if let Err(e) = tx
                            .send(PeerMessageApp {
                                from_peer,
                                data,
                            })
                            .await
                        {
                            tracing::error!(%peer_id, %app_id, e=?e, 
                                "failed to send to local peer");
                        }
                    }));
                }
            } else {
                let remote_tx = {
                    let regions = self.regions.lock().await;
                    regions.get(&region).and_then(|s| s.as_active()).map(|conn| conn.tx.clone())
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

    async fn connect_to_node(self: &Arc<Self>, region: RegionId, addr: SocketAddr) -> anyhow::Result<()> {
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

    async fn send_due_pings(&self, ping_seq: &mut u64) {
        let regions_to_ping: Vec<(RegionId, tokio::sync::mpsc::Sender<Message>)> = {
            let regions = self.regions.lock().await;
            regions
                .iter()
                .filter_map(|(region, state)| state.as_active().map(|c| (*region, c)))
                .filter(|(_, conn)| conn.latency.should_ping())
                .map(|(region, conn)| (region, conn.tx.clone()))
                .collect()
        };

        for (region, tx) in regions_to_ping {
            *ping_seq = ping_seq.wrapping_add(1);
            let seq = *ping_seq;

            if let Some(conn) = self.regions.lock().await.get_mut(&region).and_then(|s| s.as_active_mut()) {
                conn.latency.mark_ping_sent(seq);
            }

            if let Err(error) = tx.send(Message::Ping(seq)).await {
                tracing::warn!(%region, ?error, "Failed to send ping");
            }
        }
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

    async fn handle_connection(self: &Arc<Self>, stream: TcpStream, addr: SocketAddr, expected_region: Option<RegionId>) {
        let result = self.handle_connection_inner(stream, addr, expected_region).await;

        if let Some(region) = result.as_ref().ok().copied().or(expected_region) {
            self.regions.lock().await.remove(&region);
            tracing::info!(region=%region, %addr, "Disconnected");
        }
    }

    async fn handle_connection_inner(self: &Arc<Self>, stream: TcpStream, addr: SocketAddr, expected_region: Option<RegionId>) -> anyhow::Result<RegionId> {
        let (mut sink, mut stream) = Framed::new(stream, MessageCodec).split();

        sink.send(Message::Handshake(HandshakeMessage { region: self.region }))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send handshake: {}", e))?;

        let their_region = match stream.next().await {
            Some(Ok(Message::Handshake(h))) => h.region,
            _ => return Err(anyhow::anyhow!("Bad handshake")),
        };

        if let Some(expected) = expected_region {
            if their_region != expected {
                tracing::warn!(%expected, actual=%their_region, %addr, "Region mismatch");
                return Err(anyhow::anyhow!("Region mismatch: expected {}, got {}", expected, their_region));
            }
        }

        let tx = {
            let mut regions = self.regions.lock().await;
            
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

            regions.insert(their_region, RegionState::Active(ActiveConnection {
                tx: tx.clone(),
                latency: LatencyTracker::new(),
            }));
            
            tracing::info!(region=%their_region, %addr, "Connected");
            tx
        };

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                result = stream.next() => {
                    match result {
                        Some(Ok(Message::PeerMessage(msg))) => {
                            self.handle_peer_message(msg).await;
                        }
                        Some(Ok(Message::Ping(seq))) => {
                            if tx.send(Message::Pong(seq)).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Pong(seq))) => {
                            let mut regions = self.regions.lock().await;
                            if let Some(conn) = regions.get_mut(&their_region).and_then(|s| s.as_active_mut()) {
                                if conn.latency.pending_ping.as_ref().map(|(s, _)| *s == seq).unwrap_or(false) {
                                    let (_, sent_at) = conn.latency.pending_ping.take().unwrap();
                                    conn.latency.record_sample(sent_at.elapsed().as_millis() as u32);
                                }
                            }
                        }
                        Some(Ok(Message::Handshake(_))) => {}
                        Some(Err(_)) | None => break,
                    }
                }
            }
        }

        Ok(their_region)
    }

    async fn handle_peer_message(&self, msg: PeerMessage) {
        let data = Arc::new(msg.data);
        let peer_sends: Vec<_> = {
            let peers = self.peers.lock().await;
            msg.to_peers
                .iter()
                .filter_map(|&peer_id| {
                    peers.get(&(peer_id, msg.app_id)).map(|tx| (peer_id, tx.clone()))
                })
                .collect()
        };

        let send_futures: Vec<_> = peer_sends
            .into_iter()
            .map(|(peer_id, tx)| {
                let data = data.clone();
                let from_peer = msg.from_peer;
                let app_id = msg.app_id;
                Box::pin(async move {
                    if tx.send(PeerMessageApp { from_peer, data }).await.is_err() {
                        Some((peer_id, app_id))
                    } else {
                        None
                    }
                }) as Pin<Box<dyn Future<Output = Option<(PeerId, AppId)>> + Send>>
            })
            .collect();

        let failed: Vec<_> = futures::future::join_all(send_futures)
            .await
            .into_iter()
            .flatten()
            .collect();

        if !failed.is_empty() {
            let mut peers = self.peers.lock().await;
            for key in failed {
                peers.remove(&key);
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
