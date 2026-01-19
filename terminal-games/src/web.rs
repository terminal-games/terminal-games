// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{convert::Infallible, io::Write};
use std::sync::Arc;
use std::os::fd::AsRawFd;

use axum::extract::connect_info::Connected;
use axum::extract::{ConnectInfo, Request};
use axum::{
    Router,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::HeaderMap,
    response::{Html, Response},
    routing::get,
};
use bytes::Bytes;
use flate2::Compression;
use flate2::write::DeflateEncoder;
use futures::{SinkExt, StreamExt};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use tower::{Service, ServiceExt};

use crate::app::{AppInstantiationParams, AppServer};
use crate::rate_limiting::{NetworkInformation, RateLimitedStream};

#[derive(Clone)]
struct MyConnectInfo {
    network_info: Arc<NetworkInformation>,
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
            let tower_service = unwrap_infallible(make_service.call(MyConnectInfo{network_info: network_info.clone()}).await);
            let wrapped_stream = RateLimitedStream::new(stream, network_info);

            tokio::spawn(async move {
                let socket = TokioIo::new(wrapped_stream);

                let hyper_service = hyper::service::service_fn(move |request: Request<Incoming>| {
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

    let args = query.args.map(|s| s.into_bytes());

    ws.on_upgrade(move |socket| handle_socket(socket, server, user_agent, args, connect_info))
}

async fn handle_socket(
    socket: WebSocket,
    server: WebServer,
    user_agent: String,
    args: Option<Vec<u8>>,
    connect_info: MyConnectInfo,
) {
    let (mut sender, mut receiver) = socket.split();

    let remote_sshid = sanitize_user_agent(&user_agent);
    let username = "web".to_string();

    let (input_tx, input_rx) = tokio::sync::mpsc::channel(20);
    let (resize_tx, resize_rx) = tokio::sync::mpsc::channel(10);
    let cancellation_token = CancellationToken::new();
    let token = cancellation_token.clone();

    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(20);
    let exit_rx = server.app_server.instantiate_app(AppInstantiationParams {
        args,
        input_receiver: input_rx,
        output_sender: output_tx,
        remote_sshid,
        term: Some("xterm-256color".to_string()),
        username: username.clone(),
        window_size_receiver: resize_rx,
        graceful_shutdown_token: token,
        network_info: connect_info.network_info,
    });

    tokio::task::spawn(async move {
        while let Some(data) = output_rx.recv().await {
            let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
            if encoder.write_all(&data).is_err() {
                break;
            }
            let compressed = match encoder.finish() {
                Ok(compressed) => compressed,
                Err(_) => break,
            };

            if sender
                .send(Message::Binary(Bytes::from(compressed)))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let input_tx_clone = input_tx.clone();
    let cancellation_token_clone = cancellation_token.clone();
    tokio::task::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    let data: smallvec::SmallVec<[u8; 16]> = data.as_ref().into();
                    if input_tx_clone.send(data).await.is_err() {
                        break;
                    }
                }
                Ok(Message::Text(text)) => {
                    let text_str = text.to_string();
                    if text_str.starts_with("resize:") {
                        if let Some((w, h)) = parse_resize(&text_str) {
                            let _ = resize_tx.send((w, h)).await;
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    cancellation_token_clone.cancel();
                    break;
                }
                Err(_) => {
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

fn parse_resize(text: &str) -> Option<(u16, u16)> {
    let parts: Vec<&str> = text.split(':').collect();
    if parts.len() != 3 {
        return None;
    }
    let w: u16 = parts[1].parse().ok()?;
    let h: u16 = parts[2].parse().ok()?;
    Some((w, h))
}

fn sanitize_user_agent(ua: &str) -> String {
    ua.chars().take(256).collect()
}
