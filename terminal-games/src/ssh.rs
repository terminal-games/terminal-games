// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU64};
use std::task::Poll;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncRead, AsyncWrite};
use rand_core::OsRng;
use russh::keys::ssh_key::{self, PublicKey};
use russh::server::*;
use russh::{Channel, ChannelId, Pty};
use tokio_util::sync::CancellationToken;

use crate::app::{AppInstantiationParams, AppServer};

pub struct SshSession {
    input_sender: tokio::sync::mpsc::Sender<smallvec::SmallVec<[u8; 16]>>,
    resize_tx: tokio::sync::mpsc::Sender<(u16, u16)>,
    username: Option<tokio::sync::oneshot::Sender<String>>,
    term: Option<tokio::sync::oneshot::Sender<String>>,
    args: Option<tokio::sync::oneshot::Sender<Vec<u8>>>,
    ssh_session: Option<tokio::sync::oneshot::Sender<(Handle, ChannelId, String)>>,
    cancellation_token: CancellationToken,
    server: SshServer,
}

#[derive(Clone)]
pub(crate) struct SshServer {
    app_server: Arc<AppServer>,
}

impl SshServer {
    pub async fn new(app_server: Arc<AppServer>) -> anyhow::Result<Self> {
        tracing::info!("Initializing ssh server");

        Ok(Self { app_server })
    }

    pub async fn run(&mut self) -> Result<(), anyhow::Error> {
        let config = Config {
            inactivity_timeout: Some(std::time::Duration::from_secs(3600)),
            auth_rejection_time: std::time::Duration::from_secs(3),
            auth_rejection_time_initial: Some(std::time::Duration::from_secs(0)),
            keys: vec![
                russh::keys::PrivateKey::random(&mut OsRng, ssh_key::Algorithm::Ed25519).unwrap(),
            ],
            nodelay: true,
            ..Default::default()
        };
        let config = Arc::new(config);

        let listen_addr: std::net::SocketAddr = std::env::var("SSH_LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:2222".to_string())
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid SSH_LISTEN_ADDR: {}", e))?;

        // let ip_str = listen_addr.ip().to_string();
        tracing::info!(addr = %listen_addr, "Running SSH server");
        // self.run_on_address(Arc::new(config), (ip_str.as_str(), listen_addr.port()))
        //     .await?;

        let socket = tokio::net::TcpListener::bind(listen_addr).await?;
        loop {
            let (tcp_stream, peer_addr) = socket.accept().await?;

            if config.nodelay {
                if let Err(e) = tcp_stream.set_nodelay(true) {
                    tracing::warn!("set_nodelay() failed: {e:?}");
                }
            }

            // let fd = tcp_stream.as_raw_fd();
            // let bytes_per_sec: u64 = 10 * 1024;
            // let ret = unsafe { libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_MAX_PACING_RATE, &bytes_per_sec as *const u64 as *const libc::c_void, std::mem::size_of::<u64>() as libc::socklen_t) };
            // if ret != 0 {
            //     panic!("ret {}", ret);
            // }

            let network_info = Arc::new(NetworkInformation::new());
            let wrapped_stream = RateLimitedStream::new(tcp_stream, network_info.clone());
            tokio::spawn(async move {
                loop {
                    tracing::info!(bytes_per_sec_in=network_info.bytes_per_sec_in(), bytes_per_sec_out=network_info.bytes_per_sec_out(), "network");
                    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                }
            });

            let handler = self.new_client(Some(peer_addr));

            tokio::spawn({
                let config = config.clone();
                async move {
                    let session = match russh::server::run_stream(config, wrapped_stream, handler).await
                    {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::info!("Connection setup failed: {:?}", e);
                            return;
                        }
                    };

                    if let Err(e) = session.await {
                        tracing::info!("Connection closed with error: {:?}", e);
                    } else {
                        tracing::info!("Connection closed");
                    }
                }
            });
        }

        // Ok(())
    }
}

impl Drop for SshSession {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}

impl Server for SshServer {
    type Handler = SshSession;
    fn new_client(&mut self, addr: Option<std::net::SocketAddr>) -> SshSession {
        tracing::info!(addr=?addr, "new_client");

        let (username_sender, username_receiver) = tokio::sync::oneshot::channel::<String>();
        let (term_sender, term_receiver) = tokio::sync::oneshot::channel::<String>();
        let (args_sender, args_receiver) = tokio::sync::oneshot::channel::<Vec<u8>>();
        let (ssh_session_sender, ssh_session_receiver) =
            tokio::sync::oneshot::channel::<(Handle, ChannelId, String)>();

        let (input_tx, input_rx) = tokio::sync::mpsc::channel(20);
        let (resize_tx, resize_rx) = tokio::sync::mpsc::channel(1);
        let cancellation_token = CancellationToken::new();
        let token = cancellation_token.clone();
        let app_server = self.app_server.clone();

        tokio::task::spawn(async move {
            let (session_handle, channel_id, remote_sshid) = ssh_session_receiver.await.unwrap();
            let username = username_receiver.await.unwrap();

            // enter the alternate screen so that we aren't moving the cursor
            // around and overwriting the original terminal
            session_handle
                .data(channel_id, b"\x1b[?1049h".to_vec().into())
                .await
                .unwrap();

            let term =
                match tokio::time::timeout(std::time::Duration::from_millis(500), term_receiver)
                    .await
                {
                    Ok(Ok(term)) => Some(term),
                    Ok(Err(_)) => None,
                    Err(_) => {
                        tracing::info!("No pty_request received within 500ms, cleaning up");
                        let _ = session_handle
                            .disconnect(
                                russh::Disconnect::ByApplication,
                                "Bad terminal or ping too high (>500ms)".to_string(),
                                "en-US".to_string(),
                            )
                            .await;
                        return;
                    }
                };

            let args = match tokio::time::timeout(
                std::time::Duration::from_millis(1),
                args_receiver,
            )
            .await
            {
                Ok(Ok(args)) => Some(args),
                _ => None,
            };

            let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(1);
            let mut exit_rx = app_server.instantiate_app(AppInstantiationParams {
                args,
                input_receiver: input_rx,
                output_sender: output_tx,
                remote_sshid,
                term,
                username,
                window_size_receiver: resize_rx,
                graceful_shutdown_token: token,
            });
            loop {
                tokio::select! {
                    biased;

                    exit_code = &mut exit_rx => {
                        if let Ok(exit_code) = exit_code {
                            tracing::info!(?exit_code, "App exited");
                        }
                        break;
                    }

                    data = output_rx.recv() => {
                        let Some(data) = data else { break };
                        let _ = session_handle.data(channel_id, data.into()).await;
                        // tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
                    }
                }
            }

            let _ = session_handle
                .data(channel_id, b"\x1b[?1049l".to_vec().into())
                .await;

            let _ = session_handle
                .disconnect(
                    russh::Disconnect::ByApplication,
                    "Thanks for playing!".to_string(),
                    "en-US".to_string(),
                )
                .await;
        });

        SshSession {
            cancellation_token,
            input_sender: input_tx,
            resize_tx,
            username: Some(username_sender),
            term: Some(term_sender),
            args: Some(args_sender),
            ssh_session: Some(ssh_session_sender),
            server: self.clone(),
        }
    }
}

impl Handler for SshSession {
    type Error = anyhow::Error;

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        session: &mut Session,
    ) -> Result<bool, Self::Error> {
        let remote_sshid = String::from_utf8_lossy(session.remote_sshid()).to_string();
        if let Some(ssh_session_sender) = self.ssh_session.take() {
            let _ = ssh_session_sender.send((session.handle(), channel.id(), remote_sshid));
        }

        Ok(true)
    }

    async fn auth_publickey(
        &mut self,
        user: &str,
        pubkey: &PublicKey,
    ) -> Result<Auth, Self::Error> {
        tracing::info!(user, "auth_publickey");
        if let Some(username_sender) = self.username.take() {
            tracing::info!(user, "auth_publickey send");
            let _ = username_sender.send(user.to_string());
        }

        let mut rows = self
            .server
            .app_server
            .db
            .query(
                "
                INSERT INTO users (pubkey_fingerprint, username) VALUES (?1, ?2)
                ON CONFLICT DO UPDATE SET username = ?2
                RETURNING id
            ",
                libsql::params!(pubkey.fingerprint(Default::default()).as_bytes(), user),
            )
            .await
            .unwrap();
        let user_id: u64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
        _ = user_id;

        // let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        // self.drop_sender = Some(tx);
        // let db = self.db.clone();
        // tokio::task::spawn(async move {
        //     if let Ok(_) = rx.await {
        //         let _ = db
        //             .execute(
        //                 "UPDATE users SET session_time = session_time + 1 WHERE id = ?1",
        //                 [user_id],
        //             )
        //             .await
        //             .unwrap();
        //     }
        // });

        Ok(Auth::Accept)
    }

    async fn data(
        &mut self,
        _channel: ChannelId,
        data: &[u8],
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        // tracing::info!(
        //     data = String::from_utf8_lossy(&data).as_ref(),
        //     len = data.len(),
        //     "input"
        // );
        self.input_sender.send(data.into()).await?;

        Ok(())
    }

    async fn window_change_request(
        &mut self,
        channel: ChannelId,
        col_width: u32,
        row_height: u32,
        _: u32,
        _: u32,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let _ = self
            .resize_tx
            .send((col_width as u16, row_height as u16))
            .await;
        session.channel_success(channel)?;
        Ok(())
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if let Some(args_sender) = self.args.take() {
            let _ = args_sender.send(data.to_vec());
        }
        session.channel_success(channel)?;
        Ok(())
    }

    async fn pty_request(
        &mut self,
        channel: ChannelId,
        term: &str,
        col_width: u32,
        row_height: u32,
        _: u32,
        _: u32,
        _: &[(Pty, u32)],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let _ = self
            .resize_tx
            .send((col_width as u16, row_height as u16))
            .await;
        if let Some(term_sender) = self.term.take() {
            let _ = term_sender.send(term.to_string());
        }
        session.channel_success(channel)?;
        Ok(())
    }
}

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
    }
}

pub struct NetworkInformation {
    bytes_in: AtomicUsize,
    bytes_out: AtomicUsize,
    latency_ms: AtomicUsize,
    send_rate: EwmaRate,
    recv_rate: EwmaRate,
}

impl NetworkInformation {
    pub fn new() -> Self {
        Self {
            bytes_in: AtomicUsize::new(0),
            bytes_out: AtomicUsize::new(0),
            latency_ms: AtomicUsize::new(0),
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

    pub fn bytes_per_sec_in(&self) -> f64 {
        self.recv_rate.get()
    }
}

struct RateLimitedStream<S> {
    inner: S,
    write_bucket: TokenBucket,
    sleep: Pin<Box<tokio::time::Sleep>>,
    duration: std::time::Duration,
    info: Arc<NetworkInformation>,
}

impl<S> RateLimitedStream<S> {
    pub fn new(inner: S, info: Arc<NetworkInformation>) -> Self {
        let duration = std::time::Duration::from_millis(10);
        RateLimitedStream {
            inner,
            write_bucket: TokenBucket::new(50*1024, 50*1024),
            sleep: Box::pin(tokio::time::sleep(duration)),
            duration,
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
        let want = estimate_overhead(buf.len());
        let allowed = self.write_bucket.allow(want);
        if allowed < want {
            if let Poll::Ready(_) = self.sleep.as_mut().poll(cx) {
                let next = std::time::Instant::now() + self.duration;
                self.sleep.as_mut().reset(next.into());
            }
            return Poll::Pending
        }
        let poll = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &poll {
            let n = estimate_overhead(*n);
            self.write_bucket.consume(n);
            self.info.send(n);
        }

        poll
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

fn estimate_overhead(bytes: usize) -> usize {
    bytes + 40 * bytes.div_ceil(1460)
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

    pub fn allow(&mut self, demand: usize) -> usize {
        self.refill();
        // tracing::info!(tokens=self.tokens, "tokens");
        let want = demand as u64;
        let allow = self.tokens.min(want);
        allow as usize
    }

    pub fn consume(&mut self, tokens: usize) {
        self.tokens = self.tokens.saturating_sub(tokens as u64);
    }
}