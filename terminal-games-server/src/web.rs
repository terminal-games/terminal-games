// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::{convert::Infallible, io::Write};

use axum::extract::connect_info::Connected;
use axum::extract::{ConnectInfo, Request};
use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{header::CONTENT_TYPE, HeaderMap, StatusCode},
    response::{Html, Response},
    routing::get,
    Router,
};
use bytes::Bytes;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use futures::{SinkExt, StreamExt};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use tower::{Service, ServiceExt};

use terminal_games::app::{AppInstantiationParams, AppServer};
use terminal_games::rate_limiting::{NetworkInformation, RateLimitedStream, TcpLatencyProvider};

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
}

impl WebServer {
    pub fn new(app_server: Arc<AppServer>) -> Self {
        Self { app_server }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let listen_addr: std::net::SocketAddr = std::env::var("WEB_LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid WEB_LISTEN_ADDR: {}", e))?;

        tracing::info!(addr = %listen_addr, "Running web server");

        let app = Router::new()
            .route("/", get(serve_index))
            .route("/styles.css", get(serve_styles))
            .route("/main.js", get(serve_main_js))
            .route("/opus-audio-player.js", get(serve_opus_audio_player_js))
            .route(
                "/jitter-buffer-processor.js",
                get(serve_jitter_buffer_processor_js),
            )
            .route("/ws", get(websocket_handler))
            .with_state(self.clone());

        let mut make_service = app.into_make_service_with_connect_info::<MyConnectInfo>();
        let listener = tokio::net::TcpListener::bind(listen_addr).await?;
        loop {
            let (stream, _remote_addr) = listener.accept().await.unwrap();
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

            tokio::spawn(async move {
                let socket = TokioIo::new(wrapped_stream);

                let hyper_service =
                    hyper::service::service_fn(move |request: Request<Incoming>| {
                        tower_service.clone().oneshot(request)
                    });

                if let Err(err) = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection_with_upgrades(socket, hyper_service)
                    .await
                {
                    eprintln!("failed to serve connection: {err:#}");
                }
            });
        }
    }
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
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/css")
        .body(Body::from(include_str!("../web/styles.css")))
        .unwrap()
}

async fn serve_main_js() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/javascript")
        .body(Body::from(include_str!("../web/main.js")))
        .unwrap()
}

async fn serve_opus_audio_player_js() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/javascript")
        .body(Body::from(include_str!("../web/opus-audio-player.js")))
        .unwrap()
}

async fn serve_jitter_buffer_processor_js() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/javascript")
        .body(Body::from(include_str!(
            "../web/jitter-buffer-processor.js"
        )))
        .unwrap()
}

#[derive(Deserialize)]
struct WebSocketQuery {
    args: Option<String>,
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

    let (input_tx, input_rx) = tokio::sync::mpsc::channel(20);
    let (resize_tx, resize_rx) = tokio::sync::watch::channel(initial_size);
    let cancellation_token = CancellationToken::new();
    let token = cancellation_token.clone();

    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(20);
    let (audio_tx, mut audio_rx) = tokio::sync::mpsc::channel(1);
    let exit_rx = server.app_server.instantiate_app(AppInstantiationParams {
        first_app_shortname,
        args,
        input_receiver: input_rx,
        output_sender: output_tx,
        audio_sender: Some(audio_tx),
        remote_sshid,
        term: Some("xterm-256color".to_string()),
        username: username.clone(),
        window_size_receiver: resize_rx,
        graceful_shutdown_token: token,
        network_info: connect_info.network_info,
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
                    let data: smallvec::SmallVec<[u8; 16]> = data.as_ref().into();
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
        tracing::info!(?exit_code, "App exited");
    }

    cancellation_token.cancel();
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
