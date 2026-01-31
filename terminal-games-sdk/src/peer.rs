// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

const PEER_SEND_ERR_INVALID_PEER_COUNT: i32 = -1;
const PEER_SEND_ERR_DATA_TOO_LARGE: i32 = -2;
const PEER_SEND_ERR_CHANNEL_FULL: i32 = -3;
const PEER_SEND_ERR_CHANNEL_CLOSED: i32 = -4;

const PEER_RECV_ERR_CHANNEL_DISCONNECTED: i32 = -1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RegionId([u8; 4]);

impl RegionId {
    pub fn from_bytes(bytes: [u8; 4]) -> Self {
        Self(bytes)
    }

    /// Returns the current latency to this region in milliseconds.
    /// Returns `None` if the latency is unknown or the region is not connected.
    pub fn latency(&self) -> Option<u32> {
        let ret = unsafe { crate::internal::region_latency(self.0.as_ptr()) };
        if ret < 0 { None } else { Some(ret as u32) }
    }
}

impl std::fmt::Display for RegionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let end = self.0.iter().position(|&b| b == 0).unwrap_or(4);
        if let Ok(s) = std::str::from_utf8(&self.0[..end]) {
            write!(f, "{}", s)
        } else {
            for b in &self.0 {
                write!(f, "{:02x}", b)?;
            }
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerId([u8; 16]);

impl PeerId {
    /// Returns the time the peer ID was created
    pub fn timestamp(&self) -> SystemTime {
        let ms = u64::from_be_bytes(self.0[4..12].try_into().unwrap());
        UNIX_EPOCH + std::time::Duration::from_millis(ms)
    }

    /// Returns the randomness component of the peer ID
    pub fn randomness(&self) -> u32 {
        u32::from_be_bytes(self.0[12..16].try_into().unwrap())
    }

    /// Returns the region component of the peer ID
    pub fn region(&self) -> RegionId {
        RegionId::from_bytes(self.0[0..4].try_into().unwrap())
    }

    /// Sends data to this peer
    pub fn send(&self, data: &[u8]) -> Result<(), PeerError> {
        send(data, &[*self])
    }

    /// Returns the raw bytes of the ID
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Returns the current latency to the peer in milliseconds.
    /// Returns `None` if the latency is unknown
    pub fn latency(&self) -> Option<u32> {
        self.region().latency()
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in &self.0 {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
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
    if s.len() != 32 {
        return Err(PeerError::InvalidId("expected 32 hex characters"));
    }
    let mut id = [0u8; 16];
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
    Ok(PeerId(id))
}

pub fn current_id() -> PeerId {
    // PEER_ID is always defined as a valid peer id, so we can ignore the error
    parse_id(&std::env::var("PEER_ID").unwrap()).unwrap()
}

/// Returns all peers currently connected to this app across all regions,
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

    let mut buf = vec![0u8; length as usize * 16];

    let ret = unsafe { crate::internal::peer_list(buf.as_mut_ptr(), length, &mut total_count) };

    if ret < 0 {
        return Err(PeerError::ListFailed);
    }

    let count = ret as usize;
    let peers_vec = unsafe {
        let ptr = buf.as_mut_ptr() as *mut PeerId;
        let len = count;
        let cap = length as usize;
        std::mem::forget(buf);
        Vec::from_raw_parts(ptr, len, cap)
    };

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

    let mut peer_ids_buf = Vec::with_capacity(peer_ids.len() * 16);
    for id in peer_ids {
        peer_ids_buf.extend_from_slice(&id.0);
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
pub struct MessageReader;

impl MessageReader {
    pub fn new() -> Self {
        MessageReader
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
        let mut from_peer_buf = [0u8; 16];
        let mut data_buf = vec![0u8; 64 * 1024];

        let ret = unsafe {
            crate::internal::peer_recv(
                from_peer_buf.as_mut_ptr(),
                data_buf.as_mut_ptr(),
                data_buf.len() as u32,
            )
        };

        if ret > 0 {
            let from_peer = PeerId(from_peer_buf);
            data_buf.truncate(ret as usize);
            Some(Message {
                from: from_peer,
                data: data_buf,
            })
        } else {
            // ret == 0: no data available immediately
            // ret < 0: error (treat as no data for iterator pattern)
            None
        }
    }
}
