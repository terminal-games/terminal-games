// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PeerId([u8; 16]);

impl PeerId {
    /// Returns the time the peer ID was created
    pub fn timestamp(&self) -> SystemTime {
        let ms = u64::from_le_bytes(self.0[0..8].try_into().unwrap());
        UNIX_EPOCH + std::time::Duration::from_millis(ms)
    }

    /// Returns the randomness component of the peer ID
    pub fn randomness(&self) -> u32 {
        u32::from_le_bytes(self.0[8..12].try_into().unwrap())
    }

    /// Returns the region component of the peer ID
    pub fn region(&self) -> RegionId {
        RegionId::from_bytes(self.0[12..16].try_into().unwrap())
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
        return Err(PeerError::InvalidId(format!(
            "invalid ID length: expected 32 hex characters, got {}",
            s.len()
        )));
    }
    let mut id = [0u8; 16];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        if chunk.len() != 2 {
            return Err(PeerError::InvalidId("invalid hex string".to_string()));
        }
        let byte = u8::from_str_radix(
            std::str::from_utf8(chunk)
                .map_err(|e| PeerError::InvalidId(format!("invalid UTF-8: {}", e)))?,
            16,
        )
        .map_err(|e| PeerError::InvalidId(format!("failed to decode hex: {}", e)))?;
        id[i] = byte;
    }
    Ok(PeerId(id))
}

pub fn current_id() -> PeerId {
    // PEER_ID is always defined as a valid peer id, so we can ignore the error
    parse_id(&std::env::var("PEER_ID").unwrap()).unwrap()
}

/// A message received from a peer
#[derive(Clone, Debug)]
pub struct Message {
    pub from: PeerId,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub enum PeerError {
    InvalidId(String),
    NoPeerIds,
    DataTooLarge,
    TooManyPeerIds,
    SendFailed,
    RecvFailed,
}

impl std::fmt::Display for PeerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerError::InvalidId(msg) => write!(f, "invalid peer ID: {}", msg),
            PeerError::NoPeerIds => write!(f, "at least one peer ID is required"),
            PeerError::DataTooLarge => write!(f, "data too large: maximum 64KB"),
            PeerError::TooManyPeerIds => write!(f, "too many peer IDs: maximum 1024"),
            PeerError::SendFailed => write!(f, "peer_send failed"),
            PeerError::RecvFailed => write!(f, "peer_recv failed"),
        }
    }
}

impl std::error::Error for PeerError {}

/// Sends data to one or more peers
pub fn send(data: &[u8], peer_ids: &[PeerId]) -> Result<(), PeerError> {
    if peer_ids.is_empty() {
        return Err(PeerError::NoPeerIds);
    }
    if data.len() > 64 * 1024 {
        return Err(PeerError::DataTooLarge);
    }
    if peer_ids.len() > 1024 {
        return Err(PeerError::TooManyPeerIds);
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
        return Err(PeerError::SendFailed);
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
