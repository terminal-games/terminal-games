// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{collections::HashMap, fmt::Display, net::SocketAddr, pin::Pin, sync::Arc};

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

pub struct Mesh {
    region: RegionId,
    other_regions: Mutex<HashMap<RegionId, (SocketAddr, tokio::sync::mpsc::Sender<Message>)>>,
    peers: Mutex<HashMap<(PeerId, AppId), tokio::sync::mpsc::Sender<PeerMessageApp>>>,
}

impl Mesh {
    pub fn new() -> Arc<Self> {
        let region_id_str = std::env::var("REGION_ID").unwrap_or_else(|_| "loca".to_string());
        let mut region_bytes = [0u8; 4];
        let region_id_bytes = region_id_str.as_bytes();
        let copy_len = region_id_bytes.len().min(4);
        region_bytes[..copy_len].copy_from_slice(&region_id_bytes[..copy_len]);
        
        Arc::new(Self {
            region: RegionId(region_bytes),
            other_regions: Default::default(),
            peers: Default::default(),
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
                        tokio::spawn(self.clone().handle_connection(stream, peer_addr));
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

    pub async fn connect_to_node(self: Arc<Self>, addr: SocketAddr) -> anyhow::Result<()> {
        let stream = TcpStream::connect(addr).await?;
        tracing::info!(peer = %addr, "Connecting to mesh node");
        tokio::spawn(self.clone().handle_connection(stream, addr));
        Ok(())
    }

    pub async fn connect_to_nodes(self: Arc<Self>) -> anyhow::Result<()> {
        let nodes_env = std::env::var("PEER_NODES").unwrap_or_else(|_| String::new());
        
        if nodes_env.is_empty() {
            return Ok(());
        }

        let addrs: Vec<SocketAddr> = nodes_env
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<SocketAddr>()
                    .map_err(|e| anyhow::anyhow!("Invalid socket address '{}': {}", s, e))
            })
            .collect::<Result<Vec<_>, _>>()?;

        for addr in addrs {
            if let Err(e) = self.clone().connect_to_node(addr).await {
                tracing::error!(peer = %addr, ?e, "Failed to connect to node");
            }
        }

        Ok(())
    }

    async fn handle_connection(self: Arc<Self>, stream: TcpStream, addr: SocketAddr) {
        let (mut sink, mut stream) = Framed::new(stream, MessageCodec).split();

        if let Err(error) = sink
            .send(Message::Handshake(HandshakeMessage {
                region: self.region,
            }))
            .await
        {
            tracing::error!(?error, "failed to send handshake");
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
                    self.other_regions
                        .lock()
                        .await
                        .insert(msg.region, (addr, tx.clone()));
                }
                Err(error) => {
                    tracing::error!(%addr, ?error, "Error reading from connection");
                    break;
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