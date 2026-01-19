use std::os::fd::RawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU64};
use std::task::Poll;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncRead, AsyncWrite};

struct EwmaRate {
    bytes_per_sec: AtomicU64,
    last_update_ns: AtomicU64,
    tau_seconds: f64,
}

impl EwmaRate {
    fn new(tau_seconds: f64) -> Self {
        let now_ns = Self::now_ns();
        Self {
            bytes_per_sec: AtomicU64::new(0.0f64.to_bits()),
            last_update_ns: AtomicU64::new(now_ns),
            tau_seconds,
        }
    }

    fn now_ns() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
    }

    fn update(&self, bytes: usize) {
        let now_ns = Self::now_ns();
        let old_last_update_ns = self.last_update_ns.swap(now_ns, std::sync::atomic::Ordering::Relaxed);
        let old_bytes_per_sec = f64::from_bits(self.bytes_per_sec.load(std::sync::atomic::Ordering::Relaxed));
        let delta_t_sec = (now_ns.saturating_sub(old_last_update_ns) as f64 / 1_000_000_000.0).max(0.001);
        let instant = bytes as f64 / delta_t_sec;
        let alpha = 1.0 - (-delta_t_sec / self.tau_seconds).exp();
        self.bytes_per_sec.store((alpha * instant + (1.0 - alpha) * old_bytes_per_sec).to_bits(), std::sync::atomic::Ordering::Relaxed);
    }

    fn get(&self) -> f64 {
        let now_ns = Self::now_ns();
        let last_update_ns = self.last_update_ns.load(std::sync::atomic::Ordering::Relaxed);
        let bytes_per_sec = f64::from_bits(self.bytes_per_sec.load(std::sync::atomic::Ordering::Relaxed));
        let delta_t_sec = (now_ns.saturating_sub(last_update_ns) as f64 / 1_000_000_000.0).max(0.001);
        let alpha = 1.0 - (-delta_t_sec / self.tau_seconds).exp();
        (1.0 - alpha) * bytes_per_sec
        // f64::from_bits(self.bytes_per_sec.load(std::sync::atomic::Ordering::Relaxed))
    }
}

pub struct NetworkInformation {
    bytes_in: AtomicUsize,
    bytes_out: AtomicUsize,
    send_rate: EwmaRate,
    recv_rate: EwmaRate,
}

impl NetworkInformation {
    pub fn new() -> Self {
        Self {
            bytes_in: AtomicUsize::new(0),
            bytes_out: AtomicUsize::new(0),
            send_rate: EwmaRate::new(1.0),
            recv_rate: EwmaRate::new(1.0),
        }
    }

    pub fn send(&self, bytes: usize) {
        self.bytes_out.fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        self.send_rate.update(bytes);
    }

    pub fn recv(&self, bytes: usize) {
        self.bytes_in.fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        self.recv_rate.update(bytes);
    }

    pub fn bytes_per_sec_out(&self) -> f64 {
        self.send_rate.get()
    }

    #[allow(unused)]
    pub fn bytes_per_sec_in(&self) -> f64 {
        self.recv_rate.get()
    }
}

pub struct RateLimitedStream<S> {
    inner: S,
    write_bucket: TokenBucket,
    sleep: Pin<Box<tokio::time::Sleep>>,
    info: Arc<NetworkInformation>,
}

impl<S> RateLimitedStream<S> {
    pub fn new(inner: S, info: Arc<NetworkInformation>) -> Self {
        RateLimitedStream {
            inner,
            write_bucket: TokenBucket::new(50*1024, 100*1024),
            sleep: Box::pin(tokio::time::sleep_until(tokio::time::Instant::now())),
            info,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for RateLimitedStream<S> {
    fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
        let initial_filled = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &poll {
            let bytes_read = buf.filled().len() - initial_filled;
            if bytes_read > 0 {
                self.info.recv(bytes_read);
            }
        }
        poll
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for RateLimitedStream<S> {
    fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
        let len = buf.len().min(4096);
        let estimate = estimate_with_overhead(len);

        if estimate < self.write_bucket.tokens() {
            let poll = Pin::new(&mut self.inner).poll_write(cx, &buf[..len]);
            if let Poll::Ready(Ok(n)) = poll {
                let tokens_consumed = estimate_with_overhead(n);
                self.write_bucket.consume(tokens_consumed);
                self.info.send(tokens_consumed);
            }
            return poll;
        }

        let until = self.write_bucket.until(estimate);

        let deadline = std::time::Instant::now() + until;
        self.sleep.as_mut().reset(deadline.into());
        let _ = self.sleep.as_mut().poll(cx);
        Poll::Pending
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

fn estimate_with_overhead(bytes: usize) -> usize {
    bytes + 40
}

#[derive(Debug)]
pub struct TokenBucket {
    tokens_per_sec: u64, 
    capacity: u64,
    tokens: u64,
    last: std::time::Instant,
}

impl TokenBucket {
    pub fn new(tokens_per_sec: u64, capacity: u64) -> Self {
        Self {
            tokens_per_sec,
            capacity,
            tokens: capacity,
            last: std::time::Instant::now(),
        }
    }

    fn refill(&mut self) {
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last);
        self.last = now;

        let nanos = elapsed.as_nanos() as u64;
        let added = nanos.saturating_mul(self.tokens_per_sec) / 1_000_000_000;

        self.tokens = (self.tokens.saturating_add(added)).min(self.capacity);
    }

    pub fn tokens(&mut self) -> usize {
        self.refill();
        self.tokens as usize
    }

    pub fn until(&mut self, target: usize) -> std::time::Duration {
        let current = self.tokens();
        let needed = target - current;
        let seconds_needed = needed as f64 / self.tokens_per_sec as f64;
        std::time::Duration::from_secs_f64(seconds_needed)
    }

    pub fn consume(&mut self, tokens: usize) {
        self.refill();
        self.tokens = self.tokens.saturating_sub(tokens as u64);
    }
}

pub fn get_tcp_rtt_from_fd(fd: RawFd) -> std::io::Result<std::time::Duration> {
    let mut tcp_info: libc::tcp_info = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::tcp_info>() as libc::socklen_t;

    let ret = unsafe {
        libc::getsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_INFO,
            &mut tcp_info as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };

    if ret < 0 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(std::time::Duration::from_micros(tcp_info.tcpi_rtt as u64))
}
