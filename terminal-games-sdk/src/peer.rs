// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

const PEER_SEND_ERR_INVALID_PEER_COUNT: i32 = -1;
const PEER_SEND_ERR_DATA_TOO_LARGE: i32 = -2;
const PEER_SEND_ERR_CHANNEL_FULL: i32 = -3;
const PEER_SEND_ERR_CHANNEL_CLOSED: i32 = -4;
const PEER_SEND_ERR_INVALID_PEER_ID: i32 = -5;

const PEER_RECV_ERR_CHANNEL_DISCONNECTED: i32 = -1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId([u8; 4]);

const NODE_ID_ERROR: &str = "node id must be valid UTF-8 and occupy exactly 4 bytes";

impl NodeId {
    pub const BYTE_LEN: usize = 4;

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).expect("NodeId invariant violated")
    }

    /// Returns the current latency to this node in milliseconds.
    /// Returns `None` if the latency is unknown or the node is not connected.
    pub fn latency(&self) -> Option<u32> {
        let ret = unsafe { crate::internal::node_latency(self.0.as_ptr()) };
        if ret < 0 { None } else { Some(ret as u32) }
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<[u8; 4]> for NodeId {
    type Error = PeerError;

    fn try_from(bytes: [u8; 4]) -> Result<Self, Self::Error> {
        std::str::from_utf8(&bytes).map_err(|_| PeerError::InvalidId(NODE_ID_ERROR))?;
        Ok(Self(bytes))
    }
}

impl FromStr for NodeId {
    type Err = PeerError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes: [u8; 4] = value
            .as_bytes()
            .try_into()
            .map_err(|_| PeerError::InvalidId(NODE_ID_ERROR))?;
        Self::try_from(bytes)
    }
}

impl From<NodeId> for [u8; 4] {
    fn from(node_id: NodeId) -> Self {
        node_id.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerId {
    node: NodeId,
    timestamp: u64,
    randomness: u32,
}

const PEER_ID_ERROR: &str = "peer ID contains invalid node bytes";

impl PeerId {
    pub const BYTE_LEN: usize =
        NodeId::BYTE_LEN + std::mem::size_of::<u64>() + std::mem::size_of::<u32>();

    pub fn to_bytes(&self) -> [u8; Self::BYTE_LEN] {
        let mut bytes = [0u8; Self::BYTE_LEN];
        bytes[0..NodeId::BYTE_LEN].copy_from_slice(&<[u8; NodeId::BYTE_LEN]>::from(self.node));
        bytes[NodeId::BYTE_LEN..12].copy_from_slice(&self.timestamp.to_be_bytes());
        bytes[12..Self::BYTE_LEN].copy_from_slice(&self.randomness.to_be_bytes());
        bytes
    }

    /// Returns the time the peer ID was created
    pub fn timestamp(&self) -> SystemTime {
        let ms = self.timestamp;
        UNIX_EPOCH + std::time::Duration::from_millis(ms)
    }

    /// Returns the randomness component of the peer ID
    pub fn randomness(&self) -> u32 {
        self.randomness
    }

    /// Returns the node component of the peer ID
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// Sends data to this peer
    pub fn send(&self, data: &[u8]) -> Result<(), PeerError> {
        send(data, &[*self])
    }

    /// Returns the current latency to the peer in milliseconds.
    /// Returns `None` if the latency is unknown
    pub fn latency(&self) -> Option<u32> {
        self.node().latency()
    }
}

impl TryFrom<[u8; 16]> for PeerId {
    type Error = PeerError;

    fn try_from(bytes: [u8; 16]) -> Result<Self, Self::Error> {
        let node_bytes: [u8; NodeId::BYTE_LEN] = bytes[0..NodeId::BYTE_LEN].try_into().unwrap();
        let node = NodeId::try_from(node_bytes)
            .map_err(|_| PeerError::InvalidId(PEER_ID_ERROR))?;
        let timestamp = u64::from_be_bytes(bytes[NodeId::BYTE_LEN..12].try_into().unwrap());
        let randomness = u32::from_be_bytes(bytes[12..Self::BYTE_LEN].try_into().unwrap());
        Ok(Self {
            node,
            timestamp,
            randomness,
        })
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}-{}", self.node, self.timestamp, self.randomness)
    }
}

impl FromStr for PeerId {
    type Err = PeerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_id(s)
    }
}

/// Parses a hex-encoded string into a peer ID
pub fn parse_id(s: &str) -> Result<PeerId, PeerError> {
    if s.len() != PeerId::BYTE_LEN * 2 {
        return Err(PeerError::InvalidId("expected 32 hex characters"));
    }
    let mut id = [0u8; PeerId::BYTE_LEN];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        if chunk.len() != 2 {
            return Err(PeerError::InvalidId("invalid hex string"));
        }
        let byte = u8::from_str_radix(
            std::str::from_utf8(chunk).map_err(|_| PeerError::InvalidId("invalid UTF-8"))?,
            16,
        )
        .map_err(|_| PeerError::InvalidId("failed to decode hex"))?;
        id[i] = byte;
    }
    PeerId::try_from(id)
}

pub fn current_id() -> PeerId {
    // PEER_ID is always defined as a valid peer id, so we can ignore the error
    parse_id(&std::env::var("PEER_ID").unwrap()).unwrap()
}

/// Returns all peers currently connected to this app across all nodes,
/// including the current peer.
///
/// Note: this list is _eventually consistent_ and unordered, meaning that each
/// instance of your app may not see the same list at the same time or in the
/// same order
pub fn list() -> Result<Vec<PeerId>, PeerError> {
    loop {
        let pre_count = count()?;
        let (peers, total_count) = list_n(pre_count)?;
        if pre_count == total_count {
            return Ok(peers);
        }
    }
}

fn list_n(length: u32) -> Result<(Vec<PeerId>, u32), PeerError> {
    let mut total_count: u32 = 0;

    if length == 0 {
        let ret = unsafe { crate::internal::peer_list(std::ptr::null_mut(), 0, &mut total_count) };
        if ret < 0 {
            return Err(PeerError::ListFailed);
        }
        return Ok((Vec::new(), total_count));
    }

    let mut buf = vec![0u8; length as usize * PeerId::BYTE_LEN];

    let ret = unsafe { crate::internal::peer_list(buf.as_mut_ptr(), length, &mut total_count) };

    if ret < 0 {
        return Err(PeerError::ListFailed);
    }

    let peers_vec = buf[..ret as usize * PeerId::BYTE_LEN]
        .chunks_exact(PeerId::BYTE_LEN)
        .map(|chunk| {
            let peer_bytes: [u8; PeerId::BYTE_LEN] = chunk.try_into().unwrap();
            PeerId::try_from(peer_bytes)
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok((peers_vec, total_count))
}

/// Returns the total count of peers connected to this app without fetching the
/// list.
pub fn count() -> Result<u32, PeerError> {
    let (_, total_count) = list_n(0)?;
    Ok(total_count)
}

/// A message received from a peer
#[derive(Clone, Debug)]
pub struct Message {
    pub from: PeerId,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerError {
    InvalidId(&'static str),
    InvalidPeerCount,
    DataTooLarge,
    ChannelFull,
    ChannelClosed,
    ChannelDisconnected,
    ListFailed,
    Unknown(i32),
}

impl PeerError {
    fn from_send_code(code: i32) -> Self {
        match code {
            PEER_SEND_ERR_INVALID_PEER_COUNT => PeerError::InvalidPeerCount,
            PEER_SEND_ERR_DATA_TOO_LARGE => PeerError::DataTooLarge,
            PEER_SEND_ERR_CHANNEL_FULL => PeerError::ChannelFull,
            PEER_SEND_ERR_CHANNEL_CLOSED => PeerError::ChannelClosed,
            PEER_SEND_ERR_INVALID_PEER_ID => PeerError::InvalidId(PEER_ID_ERROR),
            _ => PeerError::Unknown(code),
        }
    }

    #[allow(unused)]
    fn from_recv_code(code: i32) -> Self {
        match code {
            PEER_RECV_ERR_CHANNEL_DISCONNECTED => PeerError::ChannelDisconnected,
            _ => PeerError::Unknown(code),
        }
    }
}

impl std::fmt::Display for PeerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerError::InvalidId(msg) => write!(f, "invalid peer ID: {}", msg),
            PeerError::InvalidPeerCount => write!(f, "invalid peer count (must be 1-1024)"),
            PeerError::DataTooLarge => write!(f, "data too large: maximum 64KB"),
            PeerError::ChannelFull => write!(f, "send channel full, message dropped"),
            PeerError::ChannelClosed => write!(f, "send channel closed"),
            PeerError::ChannelDisconnected => write!(f, "receive channel disconnected"),
            PeerError::ListFailed => write!(f, "peer_list failed"),
            PeerError::Unknown(code) => write!(f, "unknown error: {}", code),
        }
    }
}

impl std::error::Error for PeerError {}

/// Sends data to one or more peers
pub fn send(data: &[u8], peer_ids: &[PeerId]) -> Result<(), PeerError> {
    if peer_ids.is_empty() || peer_ids.len() > 1024 {
        return Err(PeerError::InvalidPeerCount);
    }
    if data.len() > 64 * 1024 {
        return Err(PeerError::DataTooLarge);
    }

    let mut peer_ids_buf = Vec::with_capacity(peer_ids.len() * PeerId::BYTE_LEN);
    for id in peer_ids {
        peer_ids_buf.extend_from_slice(&id.to_bytes());
    }

    let data_ptr = if data.is_empty() {
        std::ptr::null()
    } else {
        data.as_ptr()
    };

    let ret = unsafe {
        crate::internal::peer_send(
            peer_ids_buf.as_ptr(),
            peer_ids.len() as u32,
            data_ptr,
            data.len() as u32,
        )
    };

    if ret < 0 {
        return Err(PeerError::from_send_code(ret));
    }

    Ok(())
}

/// A synchronous iterator over peer messages. The caller is responsible for
/// managing polling frequency (e.g., with sleep)
pub struct MessageReader {
    from_peer_buf: [u8; PeerId::BYTE_LEN],
    data_buf: Vec<u8>,
}

impl MessageReader {
    pub fn new() -> Self {
        MessageReader {
            from_peer_buf: [0u8; PeerId::BYTE_LEN],
            data_buf: vec![0u8; 64 * 1024],
        }
    }
}

impl Default for MessageReader {
    fn default() -> Self {
        Self::new()
    }
}

impl Iterator for MessageReader {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = unsafe {
            crate::internal::peer_recv(
                self.from_peer_buf.as_mut_ptr(),
                self.data_buf.as_mut_ptr(),
                self.data_buf.len() as u32,
            )
        };

        if ret > 0 {
            let from_peer = PeerId::try_from(self.from_peer_buf)
                .expect("host returned invalid peer ID bytes");
            let data = self.data_buf[..ret as usize].to_vec();
            Some(Message {
                from: from_peer,
                data,
            })
        } else {
            // ret == 0: no data available immediately
            // ret < 0: error (treat as no data for iterator pattern)
            None
        }
    }
}
