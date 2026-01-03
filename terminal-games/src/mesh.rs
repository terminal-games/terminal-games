// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{collections::{HashMap, HashSet}, fmt::Display, net::SocketAddr, pin::Pin, sync::Arc};

use bytes::{Buf, BufMut, BytesMut};
use futures::{SinkExt, StreamExt};
use rand_core::RngCore;
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use tokio_util::codec::{Decoder, Encoder, Framed};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Message {
    Handshake(HandshakeMessage),
    PeerMessage(PeerMessage),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HandshakeMessage {
    region: RegionId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct RegionId([u8; 4]);

impl Display for RegionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let filtered: Vec<u8> = self.0.iter().copied().filter(|&b| b != 0).collect();
        if let Ok(s) = std::str::from_utf8(&filtered) {
            write!(f, "{}", s)
        } else {
            write!(f, "{}", hex::encode(self.0))
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
        bytes[12..16].copy_from_slice(&self.region.0);
        bytes
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let timestamp = u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        let randomness = u32::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11],
        ]);
        let region = RegionId([bytes[12], bytes[13], bytes[14], bytes[15]]);
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
                let region = RegionId(region_bytes);

                let addr = addr_str.parse::<SocketAddr>()
                    .map_err(|e| anyhow::anyhow!("Invalid socket address '{}': {}", addr_str, e))?;

                Ok((region, addr))
            })
            .collect()
    }
}

pub struct Mesh {
    region: RegionId,
    other_regions: Mutex<HashMap<RegionId, (SocketAddr, tokio::sync::mpsc::Sender<Message>)>>,
    peers: Mutex<HashMap<(PeerId, AppId), tokio::sync::mpsc::Sender<PeerMessageApp>>>,
    discovery: Arc<dyn Discovery>,
}

impl Mesh {
    pub fn new(discovery: Arc<dyn Discovery>) -> Arc<Self> {
        let region_id_str = std::env::var("REGION_ID").unwrap_or_else(|_| "loca".to_string());
        let mut region_bytes = [0u8; 4];
        let region_id_bytes = region_id_str.as_bytes();
        let copy_len = region_id_bytes.len().min(4);
        region_bytes[..copy_len].copy_from_slice(&region_id_bytes[..copy_len]);

        Arc::new(Self {
            region: RegionId(region_bytes),
            other_regions: Default::default(),
            peers: Default::default(),
            discovery,
        })
    }

    pub async fn new_peer(
        self: Arc<Self>,
        app_id: AppId,
    ) -> (PeerId, tokio::sync::mpsc::Receiver<PeerMessageApp>, tokio::sync::mpsc::Sender<(Vec<PeerId>, Vec<u8>)>) {
        let peer_id = PeerId {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            randomness: rand_core::OsRng.next_u32(),
            region: self.region,
        };

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let (tx2, mut rx2) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            self.peers.lock().await.insert((peer_id, app_id), tx);

            while let Some((to_peers, data)) = rx2.recv().await {
                self.send_peer_message(PeerMessage { from_peer: peer_id, to_peers, app_id, data }).await
            }

            self.peers.lock().await.remove(&(peer_id, app_id));
        });

        (peer_id, rx, tx2)
    }

    pub async fn serve(self: Arc<Self>) -> anyhow::Result<()> {
        let listen_addr: SocketAddr = std::env::var("PEER_LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:3001".to_string())
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid PEER_LISTEN_ADDR: {}", e))?;
        let listener = TcpListener::bind(listen_addr).await?;

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer_addr)) => {
                        tracing::info!(peer = %peer_addr, "Incoming mesh connection");
                        tokio::spawn(self.clone().handle_connection(stream, peer_addr, None));
                    }
                    Err(e) => {
                        tracing::error!("Failed to accept connection: {}", e);
                    }
                }
            }
        });

        Ok(())
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
                    let other_regions = self.other_regions.lock().await;
                    other_regions.get(&region).map(|(_, tx)| tx.clone())
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

    async fn connect_to_node(self: Arc<Self>, region: RegionId, addr: SocketAddr) -> anyhow::Result<()> {
        let stream = TcpStream::connect(addr).await?;
        tracing::info!(%region, peer = %addr, "Connected to mesh node");
        tokio::spawn(self.handle_connection(stream, addr, Some(region)));
        Ok(())
    }

    pub async fn start_discovery(self: Arc<Self>) -> anyhow::Result<()> {
        self.clone().initial_connect().await?;

        let mesh = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                if let Err(e) = mesh.clone().heal_network().await {
                    tracing::error!(?e, "Failed to heal network");
                }
            }
        });

        Ok(())
    }

    async fn initial_connect(self: Arc<Self>) -> anyhow::Result<()> {
        let discovered = self.discovery.discover_peers().await?;

        let current_regions: HashSet<RegionId> =
            self.other_regions.lock().await.keys().copied().collect();

        let targets: Vec<(RegionId, SocketAddr)> = discovered
            .into_iter()
            .filter(|(region, _)| *region != self.region)
            .filter(|(region, _)| !current_regions.contains(region))
            .collect();

        if targets.is_empty() {
            return Ok(());
        }

        tracing::info!(count = targets.len(), "Initial connection to discovered nodes");

        let connect_futures: Vec<_> = targets
            .into_iter()
            .map(|(region, addr)| {
                let mesh = self.clone();
                async move {
                    if let Err(e) = mesh.connect_to_node(region, addr).await {
                        tracing::warn!(%region, %addr, ?e, "Failed initial connection");
                    }
                }
            })
            .collect();

        futures::future::join_all(connect_futures).await;
        Ok(())
    }

    async fn heal_network(self: Arc<Self>) -> anyhow::Result<()> {
        let discovered = self.discovery.discover_peers().await?;

        let desired_regions: HashMap<RegionId, SocketAddr> = discovered
            .into_iter()
            .filter(|(region, _)| *region != self.region)
            .collect();

        let current_regions: HashSet<RegionId> =
            self.other_regions.lock().await.keys().copied().collect();

        let desired_region_ids: HashSet<RegionId> = desired_regions.keys().copied().collect();

        let to_connect: Vec<(RegionId, SocketAddr)> = desired_regions
            .iter()
            .filter(|(region, _)| !current_regions.contains(region))
            .map(|(r, a)| (*r, *a))
            .collect();

        let to_disconnect: Vec<RegionId> = current_regions
            .iter()
            .filter(|region| !desired_region_ids.contains(region))
            .copied()
            .collect();

        for region in to_disconnect {
            tracing::info!(%region, "Disconnecting stale region");
            self.disconnect_region(region).await;
        }

        for (region, addr) in to_connect {
            tracing::info!(%region, %addr, "Connecting to discovered region");
            if let Err(e) = self.clone().connect_to_node(region, addr).await {
                tracing::error!(%region, %addr, ?e, "Failed to connect to discovered region");
            }
        }

        Ok(())
    }

    async fn disconnect_region(&self, region: RegionId) {
        let removed = self.other_regions.lock().await.remove(&region);
        if let Some((addr, sender)) = removed {
            tracing::info!(%region, %addr, "Cleaned up region connection");
            drop(sender);
        }
    }

    async fn handle_connection(self: Arc<Self>, stream: TcpStream, addr: SocketAddr, expected_region: Option<RegionId>) {
        let (mut sink, mut stream) = Framed::new(stream, MessageCodec).split();

        if let Err(error) = sink
            .send(Message::Handshake(HandshakeMessage {
                region: self.region,
            }))
            .await
        {
            tracing::error!(?error, "failed to send handshake");
            return;
        }

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let Err(error) = sink.send(msg).await {
                    tracing::error!(?error, "Failed to send message to region");
                    break;
                }
            }
        });

        let mut connected_region: Option<RegionId> = expected_region;

        while let Some(result) = stream.next().await {
            match result {
                Ok(Message::PeerMessage(msg)) => {
                    let data = Arc::new(msg.data);

                    let peer_sends: Vec<_> = {
                        let peers = self.peers.lock().await;
                        msg.to_peers
                            .iter()
                            .filter_map(|&peer_id| {
                                let key = (peer_id, msg.app_id);
                                peers.get(&key).map(|tx| (peer_id, tx.clone()))
                            })
                            .collect()
                    };

                    let send_futures: Vec<_> = peer_sends
                        .into_iter()
                        .map(|(peer_id, tx)| {
                            let data = data.clone();
                            Box::pin(async move {
                                if let Err(error) = tx
                                    .send(PeerMessageApp {
                                        from_peer: msg.from_peer,
                                        data,
                                    })
                                    .await
                                {
                                    tracing::error!(%peer_id, app_id=%msg.app_id, ?error, "failed to send to peer");
                                    Some((peer_id, msg.app_id))
                                } else {
                                    None
                                }
                            }) as Pin<Box<dyn Future<Output = Option<(PeerId, AppId)>> + Send>>
                        })
                        .collect();

                    let results = futures::future::join_all(send_futures).await;

                    let failed_peers: Vec<_> = results.into_iter().flatten().collect();
                    if !failed_peers.is_empty() {
                        let mut peers = self.peers.lock().await;
                        for (peer_id, app_id) in failed_peers {
                            peers.remove(&(peer_id, app_id));
                        }
                    }
                }
                Ok(Message::Handshake(msg)) => {
                    if let Some(expected) = expected_region {
                        if msg.region != expected {
                            tracing::warn!(
                                expected = %expected,
                                actual = %msg.region,
                                %addr,
                                "Region mismatch on handshake, closing connection"
                            );
                            break;
                        }
                    }

                    // Only register if not already connected to this region
                    let mut other_regions = self.other_regions.lock().await;
                    if other_regions.contains_key(&msg.region) {
                        tracing::debug!(region = %msg.region, %addr, "Already connected to region, closing duplicate");
                        break;
                    }

                    connected_region = Some(msg.region);
                    other_regions.insert(msg.region, (addr, tx.clone()));
                    drop(other_regions);
                    tracing::info!(region = %msg.region, %addr, "Handshake completed");
                }
                Err(error) => {
                    tracing::error!(%addr, ?error, "Error reading from connection");
                    break;
                }
            }
        }

        if let Some(region) = connected_region {
            let mut other_regions = self.other_regions.lock().await;
            if let Some((stored_addr, _)) = other_regions.get(&region) {
                if *stored_addr == addr {
                    other_regions.remove(&region);
                    tracing::info!(%region, %addr, "Connection ended, removed from other_regions");
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