// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    sync::Mutex,
    time::{Duration, Instant},
};
use std::{convert::Infallible, io::Write};

use axum::extract::connect_info::Connected;
use axum::extract::{ConnectInfo, Request};
use axum::{
    Router,
    body::Body,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use bytes::Bytes;
use flate2::Compression;
use flate2::write::DeflateEncoder;
use futures::{SinkExt, StreamExt};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{ServerConfig, pki_types::PrivateKeyDer},
};
use tokio_util::sync::CancellationToken;
use tower::{Service, ServiceExt};

use terminal_games::app::{AppInstantiationParams, AppServer};
use terminal_games::rate_limiting::{NetworkInformation, RateLimitedStream, TcpLatencyProvider};
use terminal_games::terminal_profile::TerminalProfile;

use crate::admission::{AdmissionController, AdmissionState, AdmissionTicket};

#[derive(Clone)]
struct MyConnectInfo {
    network_info: Arc<NetworkInformation<TcpLatencyProvider>>,
}

impl Connected<MyConnectInfo> for MyConnectInfo {
    fn connect_info(this: Self) -> Self {
        this
    }
}

#[derive(Clone)]
pub struct WebServer {
    app_server: Arc<AppServer>,
    admission_controller: Arc<AdmissionController>,
    pow: Arc<PowGate>,
}

impl WebServer {
    pub fn new(app_server: Arc<AppServer>, admission_controller: Arc<AdmissionController>) -> Self {
        Self {
            app_server,
            admission_controller,
            pow: Arc::new(PowGate::new(18, Duration::from_secs(90))),
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let tls_acceptor = load_tls_acceptor_from_env()?;
        let default_web_listen_addr = if tls_acceptor.is_some() {
            "0.0.0.0:443"
        } else {
            "0.0.0.0:8080"
        };
        let listen_addr: std::net::SocketAddr = std::env::var("WEB_LISTEN_ADDR")
            .unwrap_or_else(|_| default_web_listen_addr.to_string())
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid WEB_LISTEN_ADDR: {}", e))?;

        tracing::info!(addr = %listen_addr, tls = tls_acceptor.is_some(), "Running web server");

        let app = Router::new()
            .route("/", get(serve_index))
            .route("/styles.css", get(serve_styles))
            .route("/main.js", get(serve_main_js))
            .route("/opus-audio-player.js", get(serve_opus_audio_player_js))
            .route(
                "/jitter-buffer-processor.js",
                get(serve_jitter_buffer_processor_js),
            )
            .route("/pow/challenge", get(pow_challenge_handler))
            .route("/ws", get(websocket_handler))
            .with_state(self.clone());

        let mut make_service = app.into_make_service_with_connect_info::<MyConnectInfo>();
        let listener = tokio::net::TcpListener::bind(listen_addr).await?;
        loop {
            let (stream, _remote_addr) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("failed to accept web connection: {e}");
                    continue;
                }
            };
            if let Err(e) = stream.set_nodelay(true) {
                tracing::warn!("set_nodelay() failed: {e:?}");
            }

            let fd = stream.as_raw_fd();
            let network_info = Arc::new(NetworkInformation::new(fd));
            let tower_service = unwrap_infallible(
                make_service
                    .call(MyConnectInfo {
                        network_info: network_info.clone(),
                    })
                    .await,
            );
            let wrapped_stream = RateLimitedStream::new(stream, network_info);
            let tls_acceptor = tls_acceptor.clone();

            tokio::spawn(async move {
                let hyper_service =
                    hyper::service::service_fn(move |request: Request<Incoming>| {
                        tower_service.clone().oneshot(request)
                    });

                if let Some(tls_acceptor) = tls_acceptor {
                    let tls_stream = match tls_acceptor.accept(wrapped_stream).await {
                        Ok(stream) => stream,
                        Err(err) => {
                            tracing::warn!("TLS handshake failed: {err:#}");
                            return;
                        }
                    };
                    let socket = TokioIo::new(tls_stream);
                    if let Err(err) =
                        hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                            .serve_connection_with_upgrades(socket, hyper_service)
                            .await
                    {
                        tracing::warn!("failed to serve TLS connection: {err:#}");
                    }
                } else {
                    let socket = TokioIo::new(wrapped_stream);
                    if let Err(err) =
                        hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                            .serve_connection_with_upgrades(socket, hyper_service)
                            .await
                    {
                        tracing::warn!("failed to serve connection: {err:#}");
                    }
                }
            });
        }
    }
}

fn load_tls_acceptor_from_env() -> anyhow::Result<Option<TlsAcceptor>> {
    let cert_path = std::env::var("WEB_TLS_CERT_PATH").ok();
    let key_path = std::env::var("WEB_TLS_KEY_PATH").ok();
    let (cert_path, key_path) = match (cert_path, key_path) {
        (Some(cert_path), Some(key_path)) => (cert_path, key_path),
        (None, None) => return Ok(None),
        _ => anyhow::bail!("WEB_TLS_CERT_PATH and WEB_TLS_KEY_PATH must be set together"),
    };

    let mut cert_reader =
        BufReader::new(File::open(&cert_path).map_err(|e| {
            anyhow::anyhow!("Failed to open WEB_TLS_CERT_PATH {}: {}", cert_path, e)
        })?);
    let cert_chain = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, std::io::Error>>()
        .map_err(|e| anyhow::anyhow!("Failed to parse certificate {}: {}", cert_path, e))?;
    if cert_chain.is_empty() {
        anyhow::bail!("No certificates found in WEB_TLS_CERT_PATH {}", cert_path);
    }

    let mut key_reader = BufReader::new(
        File::open(&key_path)
            .map_err(|e| anyhow::anyhow!("Failed to open WEB_TLS_KEY_PATH {}: {}", key_path, e))?,
    );
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| anyhow::anyhow!("Failed to parse private key {}: {}", key_path, e))?
        .ok_or_else(|| anyhow::anyhow!("No private key found in WEB_TLS_KEY_PATH {}", key_path))?;

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .map_err(|e| anyhow::anyhow!("Invalid TLS cert/key configuration: {}", e))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Ok(Some(TlsAcceptor::from(Arc::new(config))))
}

fn unwrap_infallible<T>(result: Result<T, Infallible>) -> T {
    match result {
        Ok(value) => value,
        Err(err) => match err {},
    }
}

async fn serve_index() -> Html<&'static str> {
    Html(include_str!("../web/index.html"))
}

async fn serve_styles() -> Response {
    static_response("text/css", include_str!("../web/styles.css"))
}

async fn serve_main_js() -> Response {
    static_response("application/javascript", include_str!("../web/main.js"))
}

async fn serve_opus_audio_player_js() -> Response {
    static_response(
        "application/javascript",
        include_str!("../web/opus-audio-player.js"),
    )
}

async fn serve_jitter_buffer_processor_js() -> Response {
    static_response(
        "application/javascript",
        include_str!("../web/jitter-buffer-processor.js"),
    )
}

fn static_response(content_type: &'static str, body: &'static str) -> Response {
    match Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type)
        .body(Body::from(body))
    {
        Ok(response) => response,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Deserialize)]
struct WebSocketQuery {
    args: Option<String>,
    pow_id: Option<String>,
    pow_counter: Option<u64>,
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    Query(query): Query<WebSocketQuery>,
    State(server): State<WebServer>,
    ConnectInfo(connect_info): ConnectInfo<MyConnectInfo>,
) -> Response {
    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();
    let locale = headers
        .get("accept-language")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .unwrap_or_else(|| "en".to_string());

    let (first_app_shortname, args) = match query.args.map(|s| s.into_bytes()) {
        Some(args) if !args.is_empty() => {
            if let Some(pos) = args.iter().position(|&b| b.is_ascii_whitespace()) {
                let shortname = String::from_utf8_lossy(&args[..pos]).into();
                let rest: Vec<u8> = args[pos..]
                    .iter()
                    .copied()
                    .skip_while(|b| b.is_ascii_whitespace())
                    .collect();
                (shortname, (!rest.is_empty()).then_some(rest))
            } else {
                (String::from_utf8_lossy(&args).into(), None)
            }
        }
        _ => ("menu".into(), None),
    };

    let verified = match (query.pow_id.as_deref(), query.pow_counter) {
        (Some(pow_id), Some(counter)) => server.pow.verify(pow_id, counter),
        _ => false,
    };
    if !verified {
        return (StatusCode::FORBIDDEN, "invalid proof of work").into_response();
    }

    ws.on_upgrade(move |socket| {
        handle_socket(
            socket,
            server,
            user_agent,
            first_app_shortname,
            args,
            connect_info,
            locale,
        )
    })
}

async fn handle_socket(
    socket: WebSocket,
    server: WebServer,
    user_agent: String,
    first_app_shortname: String,
    args: Option<Vec<u8>>,
    connect_info: MyConnectInfo,
    locale: String,
) {
    let (mut sender, mut receiver) = socket.split();

    let Some(initial_size) = recv_initial_resize(&mut receiver).await else {
        return;
    };

    let remote_sshid = sanitize_user_agent(&user_agent);
    let username = "web".to_string();

    let (input_tx, input_rx) = tokio::sync::mpsc::channel(12);
    let (replay_request_tx, replay_request_rx) = tokio::sync::mpsc::channel(1);
    let (resize_tx, resize_rx) = tokio::sync::watch::channel(initial_size);
    let cancellation_token = CancellationToken::new();
    let token = cancellation_token.clone();

    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(1);
    let (audio_tx, mut audio_rx) = tokio::sync::mpsc::channel(1);
    let admission_ticket = server.admission_controller.issue_ticket();
    if !wait_for_admission(&mut sender, &mut receiver, &resize_tx, &admission_ticket).await {
        return;
    }

    let exit_rx = server.app_server.instantiate_app(AppInstantiationParams {
        first_app_shortname,
        args,
        input_receiver: input_rx,
        replay_request_receiver: replay_request_rx,
        output_sender: output_tx,
        audio_sender: Some(audio_tx),
        remote_sshid,
        term: Some("xterm-256color".to_string()),
        username: username.clone(),
        window_size_receiver: resize_rx,
        graceful_shutdown_token: token,
        network_info: connect_info.network_info,
        terminal_profile: TerminalProfile::web_default(),
        user_id: None,
        locale,
    });

    let (pong_tx, mut pong_rx) = tokio::sync::mpsc::channel(1);

    tokio::task::spawn(async move {
        loop {
            tokio::select! {
                data = output_rx.recv() => {
                    let Some(data) = data else { break };
                    let mut msg = Vec::with_capacity(data.len() / 2);
                    {
                        let mut encoder = DeflateEncoder::new(&mut msg, Compression::default());
                        if encoder.write_all(&data).is_err() || encoder.finish().is_err() {
                            break;
                        }
                    }
                    msg.push(0x00);
                    if sender.send(Message::Binary(Bytes::from(msg))).await.is_err() {
                        break;
                    }
                }
                data = audio_rx.recv() => {
                    let Some(mut data) = data else { break };
                    data.push(0x01);
                    if sender.send(Message::Binary(Bytes::from(data))).await.is_err() {
                        break;
                    }
                }
                ts = pong_rx.recv() => {
                    let Some(ts) = ts else { break };
                    if sender.send(Message::Text(format!("pong:{}", ts).into())).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    let cancellation_token_clone = cancellation_token.clone();
    tokio::task::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    let mut data: smallvec::SmallVec<[u8; 16]> = data.as_ref().into();
                    if data.contains(&0x12) {
                        let _ = replay_request_tx.try_send(());
                        data.retain(|b| *b != 0x12);
                    }
                    if data.is_empty() {
                        continue;
                    }
                    if input_tx.send(data).await.is_err() {
                        break;
                    }
                }
                Ok(Message::Text(text)) => {
                    let text_str = text.to_string();
                    if let Some(rest) = text_str.strip_prefix("resize:") {
                        if let Some((w, h)) = parse_resize(rest) {
                            let _ = resize_tx.send((w, h));
                        }
                    } else if let Some(ts) = text_str.strip_prefix("ping:") {
                        let _ = pong_tx.send(ts.to_string()).await;
                    }
                }
                Ok(Message::Close(_)) | Err(_) => {
                    cancellation_token_clone.cancel();
                    break;
                }
                _ => {}
            }
        }
    });

    if let Ok(exit_code) = exit_rx.await {
        tracing::trace!(?exit_code, "App exited");
    }

    cancellation_token.cancel();
    drop(admission_ticket);
}

async fn recv_initial_resize(
    receiver: &mut futures::stream::SplitStream<WebSocket>,
) -> Option<(u16, u16)> {
    let msg = receiver.next().await?.ok()?;
    let Message::Text(text) = msg else {
        return None;
    };
    parse_resize(text.strip_prefix("resize:")?)
}

fn parse_resize(text: &str) -> Option<(u16, u16)> {
    let (w, h) = text.split_once(':')?;
    Some((w.parse().ok()?, h.parse().ok()?))
}

fn sanitize_user_agent(ua: &str) -> String {
    ua.chars().take(256).collect()
}

#[derive(Serialize)]
struct PowChallengeResponse {
    id: String,
    nonce: String,
    difficulty: u8,
}

async fn pow_challenge_handler(
    State(server): State<WebServer>,
) -> axum::Json<PowChallengeResponse> {
    let challenge = server.pow.issue();
    axum::Json(challenge)
}

struct PowGate {
    difficulty: u8,
    ttl: Duration,
    challenges: Mutex<HashMap<String, PowChallenge>>,
}

struct PowChallenge {
    nonce: String,
    expires_at: Instant,
}

impl PowGate {
    fn new(difficulty: u8, ttl: Duration) -> Self {
        Self {
            difficulty,
            ttl,
            challenges: Mutex::new(HashMap::new()),
        }
    }

    fn issue(&self) -> PowChallengeResponse {
        let mut guard = match self.challenges.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let now = Instant::now();
        guard.retain(|_, challenge| challenge.expires_at > now);
        let id = random_token(16);
        let nonce = random_token(16);
        guard.insert(
            id.clone(),
            PowChallenge {
                nonce: nonce.clone(),
                expires_at: now + self.ttl,
            },
        );
        PowChallengeResponse {
            id,
            nonce,
            difficulty: self.difficulty,
        }
    }

    fn verify(&self, id: &str, counter: u64) -> bool {
        let challenge = {
            let mut guard = match self.challenges.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.remove(id)
        };
        let Some(challenge) = challenge else {
            return false;
        };
        if challenge.expires_at <= Instant::now() {
            return false;
        }
        let mut hasher = Sha256::new();
        hasher.update(challenge.nonce.as_bytes());
        hasher.update(b":");
        hasher.update(counter.to_string().as_bytes());
        let digest = hasher.finalize();
        leading_zero_bits(&digest) >= self.difficulty as u32
    }
}

fn random_token(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    let mut rng = OsRng;
    rng.fill_bytes(&mut buf);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes * 2);
    for b in buf {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn leading_zero_bits(bytes: &[u8]) -> u32 {
    let mut bits = 0u32;
    for byte in bytes {
        if *byte == 0 {
            bits += 8;
            continue;
        }
        bits += byte.leading_zeros();
        return bits;
    }
    bits
}

async fn wait_for_admission(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    receiver: &mut futures::stream::SplitStream<WebSocket>,
    resize_tx: &tokio::sync::watch::Sender<(u16, u16)>,
    ticket: &AdmissionTicket,
) -> bool {
    let mut updates = ticket.subscribe().await;
    loop {
        let status = *updates.borrow();
        match status {
            AdmissionState::Allowed => {
                return sender
                    .send(Message::Text("queue:allowed".into()))
                    .await
                    .is_ok();
            }
            AdmissionState::Queued(position) => {
                if sender
                    .send(Message::Text(format!("queue:queued:{position}").into()))
                    .await
                    .is_err()
                {
                    return false;
                }
            }
        }

        tokio::select! {
            changed = updates.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        if data.contains(&0x03) || data.contains(&b'q') {
                            let _ = sender.send(Message::Close(None)).await;
                            return false;
                        }
                    }
                    Some(Ok(Message::Text(text))) => {
                        let text_str = text.to_string();
                        if let Some(rest) = text_str.strip_prefix("resize:") {
                            if let Some((w, h)) = parse_resize(rest) {
                                let _ = resize_tx.send((w, h));
                            }
                        } else if let Some(ts) = text_str.strip_prefix("ping:") {
                            if sender.send(Message::Text(format!("pong:{}", ts).into())).await.is_err() {
                                return false;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                        return false;
                    }
                    _ => {}
                }
            }
        }
    }
}
