// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    task::Poll,
    time::{Duration, Instant},
};

use tokio::io::{AsyncRead, AsyncWrite};

const DIAL_ERR_ADDRESS_TOO_LONG: i32 = -1;
const DIAL_ERR_TOO_MANY_CONNECTIONS: i32 = -2;

const POLL_DIAL_PENDING: i32 = -1;
const POLL_DIAL_ERR_INVALID_DIAL_ID: i32 = -2;
const POLL_DIAL_ERR_TASK_FAILED: i32 = -3;
const POLL_DIAL_ERR_TOO_MANY_CONNECTIONS: i32 = -4;
const POLL_DIAL_ERR_DNS_RESOLUTION: i32 = -5;
const POLL_DIAL_ERR_NOT_GLOBALLY_REACHABLE: i32 = -6;
const POLL_DIAL_ERR_CONNECTION_FAILED: i32 = -7;
const POLL_DIAL_ERR_INVALID_DNS_NAME: i32 = -8;
const POLL_DIAL_ERR_TLS_HANDSHAKE: i32 = -9;

const CONN_ERR_INVALID_CONN_ID: i32 = -1;
const CONN_ERR_CONNECTION_ERROR: i32 = -2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialError {
    AddressTooLong,
    TooManyConnections,
    DialTaskFailed,
    DnsResolution,
    NotGloballyReachable,
    ConnectionFailed,
    InvalidDnsName,
    TlsHandshake,
    InvalidDialId,
    AddressParseFailed,
    Unknown(i32),
}

impl std::fmt::Display for DialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DialError::AddressTooLong => write!(f, "address too long"),
            DialError::TooManyConnections => write!(f, "too many connections (max 8)"),
            DialError::DialTaskFailed => write!(f, "dial task failed"),
            DialError::DnsResolution => write!(f, "DNS resolution failed"),
            DialError::NotGloballyReachable => write!(f, "address not globally reachable"),
            DialError::ConnectionFailed => write!(f, "connection failed"),
            DialError::InvalidDnsName => write!(f, "invalid DNS name for TLS"),
            DialError::TlsHandshake => write!(f, "TLS handshake failed"),
            DialError::InvalidDialId => write!(f, "invalid dial ID"),
            DialError::AddressParseFailed => write!(f, "failed to parse address"),
            DialError::Unknown(code) => write!(f, "unknown error: {}", code),
        }
    }
}

impl std::error::Error for DialError {}

impl DialError {
    fn from_dial_code(code: i32) -> Self {
        match code {
            DIAL_ERR_ADDRESS_TOO_LONG => DialError::AddressTooLong,
            DIAL_ERR_TOO_MANY_CONNECTIONS => DialError::TooManyConnections,
            _ => DialError::Unknown(code),
        }
    }

    fn from_poll_code(code: i32) -> Self {
        match code {
            POLL_DIAL_ERR_INVALID_DIAL_ID => DialError::InvalidDialId,
            POLL_DIAL_ERR_TASK_FAILED => DialError::DialTaskFailed,
            POLL_DIAL_ERR_TOO_MANY_CONNECTIONS => DialError::TooManyConnections,
            POLL_DIAL_ERR_DNS_RESOLUTION => DialError::DnsResolution,
            POLL_DIAL_ERR_NOT_GLOBALLY_REACHABLE => DialError::NotGloballyReachable,
            POLL_DIAL_ERR_CONNECTION_FAILED => DialError::ConnectionFailed,
            POLL_DIAL_ERR_INVALID_DNS_NAME => DialError::InvalidDnsName,
            POLL_DIAL_ERR_TLS_HANDSHAKE => DialError::TlsHandshake,
            _ => DialError::Unknown(code),
        }
    }
}

/// Result of a successful dial containing addresses
pub struct DialResult {
    pub conn: Conn,
    pub local_addr: SocketAddr,
    pub remote_addr: SocketAddr,
}

/// A pending dial operation
pub struct PendingDial {
    dial_id: i32,
}

impl PendingDial {
    /// Start a dial operation (non-blocking)
    pub fn start(address: &str, tls: bool) -> Result<Self, DialError> {
        let mode = if tls { 1 } else { 0 };
        let result = unsafe { crate::internal::dial(address.as_ptr(), address.len() as u32, mode) };
        if result < 0 {
            return Err(DialError::from_dial_code(result));
        }
        Ok(Self { dial_id: result })
    }

    /// Poll the dial operation (non-blocking)
    /// Returns Ok(Some(DialResult)) when connected, Ok(None) when still pending, Err on failure
    pub fn poll(&self) -> Result<Option<DialResult>, DialError> {
        let mut local_addr_buf = [0u8; 64];
        let mut remote_addr_buf = [0u8; 64];
        let mut local_addr_len: u32 = 0;
        let mut remote_addr_len: u32 = 0;

        let result = unsafe {
            crate::internal::poll_dial(
                self.dial_id,
                local_addr_buf.as_mut_ptr(),
                &mut local_addr_len as *mut u32,
                remote_addr_buf.as_mut_ptr(),
                &mut remote_addr_len as *mut u32,
            )
        };

        if result >= 0 {
            let local_addr_str = std::str::from_utf8(&local_addr_buf[..local_addr_len as usize])
                .map_err(|_| DialError::AddressParseFailed)?;
            let remote_addr_str = std::str::from_utf8(&remote_addr_buf[..remote_addr_len as usize])
                .map_err(|_| DialError::AddressParseFailed)?;

            let local_addr: SocketAddr = local_addr_str
                .parse()
                .map_err(|_| DialError::AddressParseFailed)?;
            let remote_addr: SocketAddr = remote_addr_str
                .parse()
                .map_err(|_| DialError::AddressParseFailed)?;

            return Ok(Some(DialResult {
                conn: Conn::from_id(result),
                local_addr,
                remote_addr,
            }));
        }

        match result {
            POLL_DIAL_PENDING => Ok(None),
            _ => Err(DialError::from_poll_code(result)),
        }
    }

    /// Block until the connection is established or fails (synchronous, not recommended in async contexts)
    pub fn wait(self) -> Result<DialResult, DialError> {
        loop {
            match self.poll()? {
                Some(result) => return Ok(result),
                None => {
                    // Yield to allow other tasks to run
                    std::hint::spin_loop();
                }
            }
        }
    }

    /// Async wait until the connection is established or fails
    /// This yields to the tokio runtime between polls, making it suitable for async contexts
    pub async fn wait_async(self) -> Result<DialResult, DialError> {
        loop {
            match self.poll()? {
                Some(result) => return Ok(result),
                None => {
                    // Yield to tokio runtime to allow other tasks to run
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    }
}

pub struct Conn {
    fd: i32,
    sleep: Pin<Box<tokio::time::Sleep>>,
    read_deadline: Option<Instant>,
    write_deadline: Option<Instant>,
}

impl Conn {
    fn from_id(fd: i32) -> Self {
        Self {
            fd,
            sleep: Box::pin(tokio::time::sleep_until(tokio::time::Instant::now())),
            read_deadline: None,
            write_deadline: None,
        }
    }

    /// Dial a connection (blocking), returns the connection and addresses
    /// Note: This blocks the current thread. Use `dial_async` in async contexts.
    pub fn dial(address: &str, tls: bool) -> Result<DialResult, DialError> {
        PendingDial::start(address, tls)?.wait()
    }

    /// Dial a connection asynchronously, returns the connection and addresses
    /// This is the recommended method for use in tokio/async contexts.
    pub async fn dial_async(address: &str, tls: bool) -> Result<DialResult, DialError> {
        PendingDial::start(address, tls)?.wait_async().await
    }

    /// Start a dial operation (non-blocking)
    /// Returns a `PendingDial` that can be polled manually or awaited with `wait_async()`
    pub fn start_dial(address: &str, tls: bool) -> Result<PendingDial, DialError> {
        PendingDial::start(address, tls)
    }

    /// Close the connection
    pub fn close(&self) -> io::Result<()> {
        let result = unsafe { crate::internal::conn_close(self.fd) };
        if result < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid connection ID",
            ));
        }
        Ok(())
    }

    /// Set both read and write deadlines.
    /// Use Duration::ZERO to clear the deadline.
    pub fn set_deadline(&mut self, timeout: Duration) {
        self.set_read_deadline(timeout);
        self.set_write_deadline(timeout);
    }

    /// Set the read deadline.
    /// Use Duration::ZERO to clear the deadline.
    pub fn set_read_deadline(&mut self, timeout: Duration) {
        self.read_deadline = if timeout.is_zero() {
            None
        } else {
            Some(Instant::now() + timeout)
        };
    }

    /// Set the write deadline.
    /// Use Duration::ZERO to clear the deadline.
    pub fn set_write_deadline(&mut self, timeout: Duration) {
        self.write_deadline = if timeout.is_zero() {
            None
        } else {
            Some(Instant::now() + timeout)
        };
    }

    /// Non-blocking write.
    /// Returns the number of bytes written (may be 0 if bandwidth limited or would block).
    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        if let Some(deadline) = self.write_deadline {
            if Instant::now() > deadline {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "deadline exceeded"));
            }
        }

        let result =
            unsafe { crate::internal::conn_write(self.fd, buf.as_ptr(), buf.len() as u32) };

        if result >= 0 {
            return Ok(result as usize);
        }

        match result {
            CONN_ERR_INVALID_CONN_ID => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid connection ID",
            )),
            CONN_ERR_CONNECTION_ERROR => Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "connection error",
            )),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("unknown error: {}", result),
            )),
        }
    }

    /// Non-blocking read.
    /// Returns the number of bytes read (may be 0 if no data available).
    pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        if let Some(deadline) = self.read_deadline {
            if Instant::now() > deadline {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "deadline exceeded"));
            }
        }

        let result =
            unsafe { crate::internal::conn_read(self.fd, buf.as_mut_ptr(), buf.len() as u32) };

        if result >= 0 {
            return Ok(result as usize);
        }

        match result {
            CONN_ERR_INVALID_CONN_ID => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid connection ID",
            )),
            CONN_ERR_CONNECTION_ERROR => Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "connection closed or error",
            )),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("unknown error: {}", result),
            )),
        }
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        let _ = unsafe { crate::internal::conn_close(self.fd) };
    }
}

impl AsyncWrite for Conn {
    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match self.try_write(buf) {
            Ok(0) => {
                // Would block or bandwidth limited - use timer to avoid busy loop
                let deadline = std::time::Instant::now() + std::time::Duration::from_millis(1);
                self.sleep.as_mut().reset(deadline.into());
                let _ = self.sleep.as_mut().poll(cx);
                Poll::Pending
            }
            Ok(n) => Poll::Ready(Ok(n)),
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for Conn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(deadline) = self.read_deadline {
            if Instant::now() > deadline {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "deadline exceeded",
                )));
            }
        }

        let result = unsafe {
            let unfilled = buf.unfilled_mut();
            crate::internal::conn_read(
                self.fd,
                unfilled.as_mut_ptr() as *mut u8,
                unfilled.len() as u32,
            )
        };

        if result < 0 {
            let err = match result {
                CONN_ERR_INVALID_CONN_ID => {
                    io::Error::new(io::ErrorKind::InvalidInput, "invalid connection ID")
                }
                CONN_ERR_CONNECTION_ERROR => {
                    io::Error::new(io::ErrorKind::ConnectionReset, "connection closed or error")
                }
                _ => io::Error::new(
                    io::ErrorKind::Other,
                    format!("conn_read failed with error code: {}", result),
                ),
            };
            return Poll::Ready(Err(err));
        }

        if result == 0 {
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(1);
            self.sleep.as_mut().reset(deadline.into());
            let _ = self.sleep.as_mut().poll(cx);
            Poll::Pending
        } else {
            let bytes_read = result as usize;
            unsafe { buf.assume_init(bytes_read) };
            buf.advance(bytes_read);
            Poll::Ready(Ok(()))
        }
    }
}
