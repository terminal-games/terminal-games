// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::{
    collections::HashMap,
    fs::File,
    future::Future,
    io::BufReader,
    net::SocketAddr,
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
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
    },
    http::{
        HeaderMap, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE, WWW_AUTHENTICATE},
    },
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

use terminal_games::app::{
    AppInstantiationParams, AppServer, SessionControl, SessionEndReason, SessionOutput,
};
use terminal_games::input_guard::{InputForwardError, InputForwarder, TerminalBackgroundTracker};
use terminal_games::log_backend::NoopLogBackend;
use terminal_games::rate_limiting::{NetworkInformation, RateLimitedStream, TcpLatencyProvider};
use terminal_games::terminal_profile::TerminalProfile;

use crate::admission::{AdmissionController, AdmissionState, AdmissionTicket};
use crate::control::ControlPlane;
use crate::idle::IdleMonitor;
use crate::metrics::{AuthKind, Direction, ServerMetrics, Transport};
use crate::sessions::SessionRegistry;

const DEFAULT_WEB_POW_DIFFICULTY: u8 = 18;
const MAX_WEB_POW_DIFFICULTY: u8 = 32;
#[derive(Clone)]
struct MyConnectInfo {
    network_info: Arc<NetworkInformation<TcpLatencyProvider>>,
    remote_addr: SocketAddr,
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
    metrics: Arc<ServerMetrics>,
    metrics_bearer_token: Option<Arc<str>>,
    session_registry: Arc<SessionRegistry>,
    control: ControlPlane,
}

impl WebServer {
    pub fn new(
        app_server: Arc<AppServer>,
        admission_controller: Arc<AdmissionController>,
        metrics: Arc<ServerMetrics>,
        session_registry: Arc<SessionRegistry>,
        control: ControlPlane,
    ) -> anyhow::Result<Self> {
        let metrics_bearer_token = match std::env::var("METRICS_BEARER_TOKEN") {
            Ok(token) => {
                let token = token.trim().to_string();
                if token.is_empty() {
                    tracing::warn!(
                        "METRICS_BEARER_TOKEN is empty; /metrics will be served without authentication"
                    );
                    None
                } else {
                    Some(token.into())
                }
            }
            Err(std::env::VarError::NotPresent) => {
                tracing::warn!(
                    "METRICS_BEARER_TOKEN is not set; /metrics will be served without authentication"
                );
                None
            }
            Err(std::env::VarError::NotUnicode(_)) => {
                anyhow::bail!("METRICS_BEARER_TOKEN is not valid Unicode");
            }
        };
        let pow_difficulty = parse_web_pow_difficulty(std::env::var("WEB_POW_DIFFICULTY"))?;
        tracing::info!(
            pow_difficulty,
            pow_ttl_secs = 90,
            "Configured web proof-of-work gate"
        );

        Ok(Self {
            app_server,
            admission_controller,
            pow: Arc::new(PowGate::new(pow_difficulty, Duration::from_secs(90))),
            metrics,
            metrics_bearer_token,
            session_registry,
            control,
        })
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
            .route("/metrics", get(serve_metrics))
            .route("/pow/challenge", get(pow_challenge_handler))
            .route("/ws", get(websocket_handler))
            .with_state(self.clone())
            .nest("/control", crate::control::router(self.control.clone()));

        let mut make_service = app.into_make_service_with_connect_info::<MyConnectInfo>();
        let listener = tokio::net::TcpListener::bind(listen_addr).await?;
        loop {
            let (stream, remote_addr) = match listener.accept().await {
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
                        remote_addr,
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
    static_bytes_response("text/css", include_str!("../web/styles.css"))
}

async fn serve_main_js() -> Response {
    static_bytes_response("application/javascript", include_str!("../web/main.js"))
}

async fn serve_opus_audio_player_js() -> Response {
    static_bytes_response(
        "application/javascript",
        include_str!("../web/opus-audio-player.js"),
    )
}

async fn serve_jitter_buffer_processor_js() -> Response {
    static_bytes_response(
        "application/javascript",
        include_str!("../web/jitter-buffer-processor.js"),
    )
}

async fn serve_metrics(State(server): State<WebServer>, headers: HeaderMap) -> Response {
    if !metrics_request_authorized(&headers, server.metrics_bearer_token.as_deref()) {
        return Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header(WWW_AUTHENTICATE, "Bearer")
            .body(Body::from("missing or invalid bearer token"))
            .unwrap_or_else(|_| StatusCode::UNAUTHORIZED.into_response());
    }

    match server.metrics.render().await {
        Ok(body) => static_bytes_response("text/plain; version=0.0.4; charset=utf-8", body),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode metrics: {err:#}"),
        )
            .into_response(),
    }
}

fn metrics_request_authorized(headers: &HeaderMap, expected_token: Option<&str>) -> bool {
    let Some(expected_token) = expected_token else {
        return true;
    };
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| token == expected_token)
}

fn static_bytes_response(content_type: &'static str, body: impl Into<Body>) -> Response {
    match Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type)
        .body(body.into())
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

    let app_exists = match server
        .app_server
        .db
        .query(
            "SELECT 1 FROM apps WHERE shortname = ?1 LIMIT 1",
            libsql::params!(first_app_shortname.as_str()),
        )
        .await
    {
        Ok(mut rows) => matches!(rows.next().await, Ok(Some(_))),
        Err(err) => {
            tracing::warn!(error = ?err, "failed to validate requested app");
            false
        }
    };
    if !app_exists {
        let status = if first_app_shortname == "menu" {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::NOT_FOUND
        };
        let message = if first_app_shortname == "menu" {
            "menu is not installed; provision the 'menu' app via the CLI"
        } else {
            "unknown app shortname"
        };
        let _ = sender
            .send(Message::Close(Some(CloseFrame {
                code: 1008,
                reason: message.into(),
            })))
            .await;
        let _ = sender.close().await;
        tracing::debug!(
            shortname = %first_app_shortname,
            status = status.as_u16(),
            "rejected websocket for unavailable app"
        );
        return;
    }

    let Some(initial_size) = recv_initial_resize(&mut receiver).await else {
        return;
    };

    let remote_sshid = sanitize_user_agent(&user_agent);
    let username = "web".to_string();

    let (resize_tx, resize_rx) = tokio::sync::watch::channel(initial_size);
    let cancellation_token = CancellationToken::new();
    let token = cancellation_token.clone();
    let background_tracker = TerminalBackgroundTracker::default();

    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel::<SessionOutput>(1);
    let (audio_tx, mut audio_rx) = tokio::sync::mpsc::channel(8);
    let admission_ticket = server
        .admission_controller
        .issue_ticket(Transport::Web, connect_info.remote_addr.ip());
    match wait_for_admission(&mut sender, &mut receiver, &resize_tx, &admission_ticket).await {
        AdmissionWaitResult::Allowed => {}
        AdmissionWaitResult::Disconnected => return,
        AdmissionWaitResult::Rejected(reason) => {
            tracing::trace!(
                client_ip = %connect_info.remote_addr.ip(),
                reason = reason.slug(),
                "Rejected web admission"
            );
            return;
        }
    }
    let mut ban_changes = server.admission_controller.subscribe_ban_changes();
    if let Some((ban_rule, ban_reason)) = server
        .admission_controller
        .check_ip_ban(connect_info.remote_addr.ip())
    {
        tracing::debug!(
            client_ip = %connect_info.remote_addr.ip(),
            transport = Transport::Web.as_str(),
            ban_rule = %ban_rule,
            ban_reason = ban_reason.as_deref().unwrap_or("<none>"),
            "Rejected client from active IP ban after admission"
        );
        let _ = send_rejection_and_close(&mut sender, SessionEndReason::BannedIp).await;
        return;
    }
    let local_session_id = admission_ticket.id();
    let session_registration = server.session_registry.register(
        local_session_id,
        None,
        username.clone(),
        connect_info.remote_addr.ip(),
        Transport::Web,
        first_app_shortname.clone(),
        resize_rx.clone(),
    );
    let mut session_control = session_registration.control_rx;
    let mut idle_rx = session_registration.idle_rx;
    let app_input_sender = session_registration.app_input_sender.clone();
    let app_input_receiver = session_registration.app_input_receiver;
    let spy_snapshot_requests = session_registration.spy_snapshot_requests;
    let session_ui = session_registration.session_ui;
    let session_identity = session_registration.identity.clone();
    let _session_cleanup_guard = session_registration.cleanup_guard;
    let idle_notifications = session_ui.notification_sender();
    let mut idle_monitor = IdleMonitor::new(idle_notifications);
    idle_monitor.set_paused(idle_rx.borrow().paused);
    let (replay_request_tx, replay_request_rx) = tokio::sync::mpsc::channel(1);
    let input_forwarder = InputForwarder::new_with_sender(
        cancellation_token.clone(),
        app_input_sender.clone(),
        replay_request_tx,
    );
    let session_metrics = server.metrics.start_session(
        session_identity.app(),
        Transport::Web,
        AuthKind::Anonymous,
        true,
        None,
    );
    let mut admitted_session = admission_ticket.start_session(session_metrics.clone());
    let mut cluster_control = admitted_session.subscribe_control();
    let mut app_metrics_rx = session_identity.app_receiver();
    let terminal_parser = background_tracker.into_terminal_parser(initial_size.1, initial_size.0);

    let mut exit_rx = server.app_server.instantiate_app(AppInstantiationParams {
        args,
        input_receiver: app_input_receiver,
        replay_request_receiver: replay_request_rx,
        spy_snapshot_requests,
        output_sender: output_tx,
        audio_sender: Some(audio_tx),
        remote_sshid,
        term: Some("xterm-256color".to_string()),
        session_identity,
        session_ui,
        window_size_receiver: resize_rx,
        graceful_shutdown_token: token,
        network_info: connect_info.network_info,
        terminal_profile: TerminalProfile::web_default(),
        terminal_parser,
        user_id: None,
        locale,
        log_backend: Arc::new(NoopLogBackend),
    });
    let mut pending_input: Option<
        Pin<Box<dyn Future<Output = Result<(), InputForwardError>> + Send>>,
    > = None;
    let close_reason = loop {
        tokio::select! {
            biased;

            exit_code = &mut exit_rx => {
                match exit_code {
                    Ok(Ok(exit_code)) => {
                        tracing::trace!(?exit_code, "App exited");
                    }
                    Ok(Err(error)) => {
                        tracing::error!(error = %error, "App failed");
                    }
                    Err(error) => {
                        tracing::error!(?error, "App exit channel dropped");
                    }
                }
                break SessionEndReason::NormalExit;
            }

            changed = session_control.changed() => {
                if changed.is_err() {
                    continue;
                }
                let SessionControl::Close(reason) = *session_control.borrow() else {
                    continue;
                };
                cancellation_token.cancel();
                break reason;
            }

            result = async {
                pending_input.as_mut().expect("guarded by select").await
            }, if pending_input.is_some() => {
                if result.is_err() {
                    break SessionEndReason::ConnectionLost;
                }
                pending_input = None;
            }

            changed = ban_changes.changed() => {
                if changed.is_err() {
                    continue;
                }
                if server
                    .admission_controller
                    .check_ip_ban(connect_info.remote_addr.ip())
                    .is_some()
                {
                    server
                        .session_registry
                        .request_close(local_session_id, SessionEndReason::BannedIp);
                }
            }

            changed = cluster_control.changed() => {
                if changed.is_err() {
                    continue;
                }
                let SessionControl::Close(reason) = *cluster_control.borrow() else {
                    continue;
                };
                server.session_registry.request_close(local_session_id, reason);
            }

            changed = app_metrics_rx.changed() => {
                if changed.is_err() {
                    continue;
                }
                let app = app_metrics_rx.borrow_and_update().clone();
                session_metrics.set_active_app(&app);
            }

            _ = idle_monitor.wait_for_tick() => {
                if let Some(reason) = idle_monitor.on_tick() {
                    server
                        .session_registry
                        .set_idle_fuel(local_session_id, idle_monitor.idle_state().fuel_seconds);
                    server
                        .session_registry
                        .request_close(local_session_id, reason);
                } else {
                    server
                        .session_registry
                        .set_idle_fuel(local_session_id, idle_monitor.idle_state().fuel_seconds);
                }
            }

            changed = idle_rx.changed() => {
                if changed.is_err() {
                    continue;
                }
                let idle_state = *idle_rx.borrow_and_update();
                idle_monitor.set_paused(idle_state.paused);
                server
                    .session_registry
                    .set_idle_fuel(local_session_id, idle_monitor.idle_state().fuel_seconds);
            }

            data = output_rx.recv() => {
                let Some(data) = data else {
                    break SessionEndReason::NormalExit;
                };
                admitted_session.record_output(data.data.len());
                server.session_registry.record_output(local_session_id, &data);
                let mut msg = Vec::with_capacity(data.data.len() / 2);
                {
                    let mut encoder = DeflateEncoder::new(&mut msg, Compression::default());
                    if encoder.write_all(&data.data).is_err() || encoder.finish().is_err() {
                        break SessionEndReason::NormalExit;
                    }
                }
                msg.push(0x00);
                session_metrics.record_bytes(Direction::Out, msg.len());
                server.control.record_bytes(msg.len());
                if sender.send(Message::Binary(Bytes::from(msg))).await.is_err() {
                    cancellation_token.cancel();
                    break SessionEndReason::ConnectionLost;
                }
            }

            data = audio_rx.recv() => {
                let Some(mut data) = data else {
                    break SessionEndReason::NormalExit;
                };
                admitted_session.record_output(data.len());
                data.push(0x01);
                session_metrics.record_bytes(Direction::Out, data.len());
                server.control.record_bytes(data.len());
                if sender.send(Message::Binary(Bytes::from(data))).await.is_err() {
                    cancellation_token.cancel();
                    break SessionEndReason::ConnectionLost;
                }
            }

            msg = receiver.next(), if pending_input.is_none() => {
                let Some(msg) = msg else {
                    cancellation_token.cancel();
                    break SessionEndReason::ConnectionLost;
                };
                match msg {
                    Ok(Message::Binary(data)) => {
                        idle_monitor.observe_input(&data);
                        session_metrics.record_bytes(Direction::In, data.len());
                        server.control.record_bytes(data.len());
                        admitted_session.record_input(&data);
                        server.session_registry.record_input(local_session_id, &data);
                        pending_input = Some(Box::pin(input_forwarder.prepare_input(data).send()));
                    }
                    Ok(Message::Text(text)) => {
                        let text_str = text.to_string();
                        if let Some(rest) = text_str.strip_prefix("resize:") {
                            if let Some((w, h)) = parse_resize(rest) {
                                let _ = resize_tx.send((w, h));
                            }
                        } else if let Some(ts) = text_str.strip_prefix("ping:") {
                            if sender.send(Message::Text(format!("pong:{}", ts).into())).await.is_err() {
                                cancellation_token.cancel();
                                break SessionEndReason::ConnectionLost;
                            }
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => {
                        cancellation_token.cancel();
                        break SessionEndReason::ConnectionLost;
                    }
                    _ => {}
                }
            }
        }
    };

    cancellation_token.cancel();
    session_metrics.finish(close_reason);
    server
        .session_registry
        .finish(local_session_id, close_reason);
    if close_reason.should_notify_web_client() {
        let _ = send_session_closed_and_close(&mut sender, close_reason).await;
    }
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
        retain_unexpired(&mut guard, now);
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
            retain_unexpired(&mut guard, Instant::now());
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

fn retain_unexpired(challenges: &mut HashMap<String, PowChallenge>, now: Instant) -> usize {
    let before = challenges.len();
    challenges.retain(|_, challenge| challenge.expires_at > now);
    before.saturating_sub(challenges.len())
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

fn parse_web_pow_difficulty(raw: Result<String, std::env::VarError>) -> anyhow::Result<u8> {
    let raw = match raw {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => return Ok(DEFAULT_WEB_POW_DIFFICULTY),
        Err(std::env::VarError::NotUnicode(_)) => {
            anyhow::bail!("WEB_POW_DIFFICULTY is not valid Unicode");
        }
    };

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        tracing::warn!(
            default_difficulty = DEFAULT_WEB_POW_DIFFICULTY,
            "WEB_POW_DIFFICULTY is empty; using default"
        );
        return Ok(DEFAULT_WEB_POW_DIFFICULTY);
    }

    let parsed = trimmed.parse::<u8>().map_err(|_| {
        anyhow::anyhow!(
            "WEB_POW_DIFFICULTY must be an integer between 0 and {MAX_WEB_POW_DIFFICULTY}"
        )
    })?;

    if parsed == 0 {
        tracing::warn!("WEB_POW_DIFFICULTY=0 disables the web proof-of-work gate");
        return Ok(0);
    }

    if parsed > MAX_WEB_POW_DIFFICULTY {
        tracing::warn!(
            configured_difficulty = parsed,
            max_supported_difficulty = MAX_WEB_POW_DIFFICULTY,
            effective_difficulty = MAX_WEB_POW_DIFFICULTY,
            "WEB_POW_DIFFICULTY is too high; clamping to supported maximum"
        );
        return Ok(MAX_WEB_POW_DIFFICULTY);
    }

    Ok(parsed)
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

enum AdmissionWaitResult {
    Allowed,
    Disconnected,
    Rejected(SessionEndReason),
}

async fn wait_for_admission(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    receiver: &mut futures::stream::SplitStream<WebSocket>,
    resize_tx: &tokio::sync::watch::Sender<(u16, u16)>,
    ticket: &AdmissionTicket,
) -> AdmissionWaitResult {
    let mut updates = ticket.subscribe();
    loop {
        let status = *updates.borrow();
        match status {
            AdmissionState::Allowed => {
                return if sender
                    .send(Message::Text("queue:allowed".into()))
                    .await
                    .is_ok()
                {
                    AdmissionWaitResult::Allowed
                } else {
                    AdmissionWaitResult::Disconnected
                };
            }
            AdmissionState::Queued(position) => {
                if sender
                    .send(Message::Text(format!("queue:queued:{position}").into()))
                    .await
                    .is_err()
                {
                    return AdmissionWaitResult::Disconnected;
                }
            }
            AdmissionState::Rejected(reason) => {
                let _ = send_rejection_and_close(sender, reason).await;
                return AdmissionWaitResult::Rejected(reason);
            }
        }

        tokio::select! {
            changed = updates.changed() => {
                if changed.is_err() {
                    return AdmissionWaitResult::Disconnected;
                }
            }
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        if data.contains(&0x03) || data.contains(&b'q') {
                            let _ = sender.send(Message::Close(None)).await;
                            return AdmissionWaitResult::Disconnected;
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
                                return AdmissionWaitResult::Disconnected;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                        return AdmissionWaitResult::Disconnected;
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn send_rejection_and_close(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    reason: SessionEndReason,
) -> Result<(), axum::Error> {
    sender
        .send(Message::Text(
            format!("queue:rejected:{}", reason.slug()).into(),
        ))
        .await?;
    sender
        .send(Message::Close(Some(CloseFrame {
            code: 1008,
            reason: format!("rejected:{}", reason.slug()).into(),
        })))
        .await
}

async fn send_session_closed_and_close(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    reason: SessionEndReason,
) -> Result<(), axum::Error> {
    sender
        .send(Message::Text(
            format!("session:closed:{}", reason.slug()).into(),
        ))
        .await?;
    sender
        .send(Message::Close(Some(CloseFrame {
            code: 1000,
            reason: format!("closed:{}", reason.slug()).into(),
        })))
        .await
}
