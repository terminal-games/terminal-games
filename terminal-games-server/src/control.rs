// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    error::Error,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll},
    time::{Duration, Instant},
};

use axum::{
    Router,
    extract::{
        FromRequestParts, Path, Query, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
    },
    http::{
        HeaderMap, StatusCode,
        header::{AUTHORIZATION, HeaderName, HeaderValue},
        request::Parts,
    },
    middleware,
    response::{IntoResponse, Response},
    routing::get,
};
use base64::Engine as _;
use bytes::BytesMut;
use futures::{Sink, Stream, StreamExt};
use rand_core::{OsRng, RngCore};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sysinfo::System;
use tarpc::server::Channel;
use tarpc::{context, server};
use terminal_games::{
    app::AppServer,
    app_env::{
        EncryptedAuthorEnvBlob, decrypt_app_env_blob, encrypt_app_env_blob, validate_app_env_name,
        validate_app_envs,
    },
    control::{
        AdminControlRpc, AppControlRpc, AppEnvDeleteRequest, AppEnvListResponse, AppEnvSetRequest,
        AppSelfInfoRequest, AppSelfInfoResponse, AppSelfResponse, AppSummary, AppTokenClaims,
        BanEntry, BanIpAddRequest, BanIpAddResponse, BanIpRemoveRequest, BanIpRequest,
        BroadcastRequest, CONTROL_API_EXPECTED_VERSION_HEADER, CONTROL_API_VERSION,
        ClusterKickedIpListRequest, ClusterKickedIpListResponse, CreateAppRequest,
        CreateAppResponse, DeleteAppRequest, DeleteShortnameRequest, DeleteShortnameResponse,
        DrainStartRequest, KickSessionRequest, NodeDiscoveryResponse, NodeRuntimeStatus,
        RotateAppTokenRequest, RotateAppTokenResponse, RpcError, SessionSummary, SpyClientMessage,
        SpyControlMessage, StatusBarState, StatusBroadcast, TickerAddRequest, TickerEntry,
        TickerRemoveRequest, TickerReorderRequest, UploadAppRequest, UploadAppResponse,
        expiry_from_duration, parse_duration_string, parse_optional_expiry,
    },
    manifest::{extract_manifest_from_wasm, sanitize_manifest, validate_shortname},
    mesh::{AppRuntimeUpdateMessage, BuildId, ContentHash, Mesh, hash_app_envs, hash_bytes},
};
use time::OffsetDateTime;

use crate::{
    admission::{AdmissionController, decode_cidr_blob, encode_cidr_blob, parse_ban_cidr},
    cluster_kicked_ips,
    sessions::SessionRegistry,
    shutdown::ShutdownCoordinator,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::tungstenite::{Error as WsError, error::ProtocolError};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const BANDWIDTH_WINDOW: Duration = Duration::from_secs(5);
const AUTHOR_UPLOAD_MAX_BYTES: usize = 50 * 1024 * 1024;
const CONTROL_RPC_MAX_FRAME_LEN: usize = 64 * 1024 * 1024;
const APP_SELF_INFO_MAX_TOKENS: usize = 100;

#[derive(Clone)]
pub struct ControlPlane {
    app_server: Arc<AppServer>,
    admission_controller: Arc<AdmissionController>,
    session_registry: Arc<SessionRegistry>,
    latest_broadcast: Arc<Mutex<Option<StatusBroadcast>>>,
    mesh: Mesh,
    admin_shared_secret: Option<Arc<str>>,
    rate_limiter: Arc<ApiRateLimiter>,
    bandwidth: Arc<BandwidthTracker>,
    max_capacity: usize,
    node_id: String,
    shutdown: ShutdownCoordinator,
}

impl ControlPlane {
    pub fn new(
        app_server: Arc<AppServer>,
        admission_controller: Arc<AdmissionController>,
        session_registry: Arc<SessionRegistry>,
        mesh: Mesh,
        max_capacity: usize,
        admin_shared_secret: Option<Arc<str>>,
        node_id: String,
        shutdown: ShutdownCoordinator,
    ) -> Self {
        Self {
            app_server,
            admission_controller,
            session_registry,
            latest_broadcast: Arc::new(Mutex::new(None)),
            mesh,
            admin_shared_secret,
            rate_limiter: Arc::new(ApiRateLimiter::new()),
            bandwidth: Arc::new(BandwidthTracker::default()),
            max_capacity,
            node_id,
            shutdown,
        }
    }

    pub fn record_bytes(&self, bytes: usize) {
        self.bandwidth.record(bytes);
    }

    fn require_admin(&self, headers: &HeaderMap) -> Result<AdminGuard, Response> {
        let Some(expected) = self.admin_shared_secret.as_deref() else {
            return Err((StatusCode::SERVICE_UNAVAILABLE, "admin auth disabled").into_response());
        };
        let Some(token) = bearer_token(headers) else {
            return Err((StatusCode::UNAUTHORIZED, "missing bearer token").into_response());
        };
        if constant_time_eq(
            sha256_bytes(token).as_slice(),
            sha256_bytes(expected).as_slice(),
        ) {
            Ok(AdminGuard)
        } else {
            Err((StatusCode::UNAUTHORIZED, "invalid bearer token").into_response())
        }
    }

    async fn require_app(&self, headers: &HeaderMap) -> Result<AppAuth, Response> {
        let Some(token) = bearer_token(headers) else {
            return Err((StatusCode::UNAUTHORIZED, "missing bearer token").into_response());
        };
        let claims = AppTokenClaims::decode(token)
            .map_err(|error| (StatusCode::UNAUTHORIZED, error.to_string()).into_response())?;
        let Some(record) =
            load_app_token_record_by_shortname(&self.app_server.db, &claims.shortname)
                .await
                .map_err(internal_error)?
        else {
            return Err((StatusCode::UNAUTHORIZED, "unknown app").into_response());
        };
        if !constant_time_eq(
            sha256_hex(&claims.secret).as_bytes(),
            record.token_hash.as_bytes(),
        ) {
            return Err((StatusCode::UNAUTHORIZED, "invalid app token").into_response());
        }
        Ok(AppAuth { record, claims })
    }

    fn consume_tokens(
        &self,
        group: RateLimitGroup,
        bucket_key: &str,
        cost: u32,
    ) -> Result<(), ApiRateLimitExceeded> {
        match self.rate_limiter.try_consume(group, bucket_key, cost) {
            Ok(()) => Ok(()),
            Err(error) => {
                tracing::debug!(
                    ?group,
                    cost,
                    retry_after_secs = error.retry_after.as_secs_f64(),
                    bucket_key,
                    "API rate limit exceeded"
                );
                Err(error)
            }
        }
    }

    fn consume_tokens_rpc(
        &self,
        group: RateLimitGroup,
        bucket_key: &str,
        cost: u32,
    ) -> Result<(), RpcError> {
        self.consume_tokens(group, bucket_key, cost)
            .map_err(|error| RpcError::from(error.to_string()))
    }

    async fn discover(&self) -> anyhow::Result<NodeDiscoveryResponse> {
        let mut nodes = self
            .mesh
            .discover_nodes()
            .await?
            .into_iter()
            .map(|node| node.to_string())
            .collect::<Vec<_>>();
        nodes.sort();
        Ok(NodeDiscoveryResponse {
            current_node: self.node_id.clone(),
            nodes,
        })
    }

    async fn publish_app_runtime_update(&self, update: AppRuntimeUpdateMessage) {
        let _ = self.app_server.app_registry().apply_update(update);
        self.mesh.propagate_app_runtime_update(update).await;
    }

    async fn rotate_app_token(
        &self,
        app_id: u64,
        shortname: &str,
        base_url: String,
    ) -> anyhow::Result<RotateAppTokenResponse> {
        let secret = random_token_secret();
        let token_hash = sha256_hex(&secret);
        self.app_server
            .db
            .execute(
                "UPDATE app_tokens SET token_hash = ?2 WHERE id = ?1",
                libsql::params!(app_id, token_hash),
            )
            .await?;
        Ok(RotateAppTokenResponse {
            app: AppSummary {
                app_id,
                author_name: String::new(),
                shortname: shortname.to_string(),
                playtime_seconds: 0.0,
            },
            token: AppTokenClaims::new(base_url, shortname.to_string(), secret).encode()?,
        })
    }

    pub async fn refresh_status_bar_state(&self) -> anyhow::Result<()> {
        let state = load_status_bar_state(&self.app_server.db, self.current_broadcast()).await?;
        self.session_registry.set_status_bar_state(state);
        Ok(())
    }

    fn set_latest_broadcast(&self, broadcast: Option<StatusBroadcast>) {
        *self.latest_broadcast.lock().unwrap() = broadcast;
    }

    fn current_broadcast(&self) -> Option<StatusBroadcast> {
        let mut latest_broadcast = self.latest_broadcast.lock().unwrap();
        if latest_broadcast
            .as_ref()
            .is_some_and(|broadcast| broadcast.expires_at <= current_unix_seconds())
        {
            *latest_broadcast = None;
        }
        latest_broadcast.clone()
    }

    fn local_node_status(&self) -> NodeRuntimeStatus {
        let mut system = System::new();
        system.refresh_memory();
        system.refresh_cpu_usage();
        NodeRuntimeStatus {
            node_id: self.node_id.clone(),
            current_sessions: self.session_registry.count(),
            max_capacity: self.max_capacity,
            cpu_usage_percent: system.global_cpu_usage(),
            memory_used_bytes: system
                .total_memory()
                .saturating_sub(system.available_memory()),
            memory_total_bytes: system.total_memory(),
            bandwidth_bytes_per_second: self.bandwidth.bytes_per_second(),
            shutdown: self.shutdown.snapshot(),
        }
    }
}

pub fn router(control: ControlPlane) -> Router {
    Router::new()
        .route("/admin/rpc", get(admin_rpc))
        .route(
            "/admin/session/spy/{local_session_id}",
            get(admin_session_spy),
        )
        .route("/app/rpc", get(app_rpc))
        .layer(middleware::map_response(with_control_api_version))
        .with_state(control)
}

async fn with_control_api_version<B>(mut response: Response<B>) -> Response<B> {
    response.headers_mut().insert(
        HeaderName::from_static(terminal_games::control::CONTROL_API_VERSION_HEADER),
        HeaderValue::from_static(terminal_games::control::CONTROL_API_VERSION),
    );
    response.headers_mut().insert(
        HeaderName::from_static(terminal_games::control::CONTROL_SERVER_VERSION_HEADER),
        HeaderValue::from_static(env!("CARGO_PKG_VERSION")),
    );
    response
}

struct ServerWsTransport {
    socket: WebSocket,
    read_buf: BytesMut,
}

impl ServerWsTransport {
    fn new(socket: WebSocket) -> Self {
        Self {
            socket,
            read_buf: BytesMut::new(),
        }
    }
}

impl AsyncRead for ServerWsTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if !self.read_buf.is_empty() {
                let len = self.read_buf.len().min(buf.remaining());
                buf.put_slice(&self.read_buf.split_to(len));
                return Poll::Ready(Ok(()));
            }
            let Some(message) = futures::ready!(Pin::new(&mut self.socket).poll_next(cx)) else {
                return Poll::Ready(Ok(()));
            };
            match message {
                Ok(Message::Binary(data)) => self.read_buf = BytesMut::from(&data[..]),
                Ok(Message::Text(text)) => {
                    self.read_buf = BytesMut::from(text.as_str().as_bytes());
                }
                Ok(Message::Close(_)) => return Poll::Ready(Ok(())),
                Ok(Message::Ping(_) | Message::Pong(_)) => continue,
                Err(error) if websocket_disconnect_is_eof(&error) => {
                    return Poll::Ready(Ok(()));
                }
                Err(error) => return Poll::Ready(Err(io::Error::other(error))),
            }
        }
    }
}

impl AsyncWrite for ServerWsTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        futures::ready!(Pin::new(&mut self.socket).poll_ready(cx)).map_err(io::Error::other)?;
        Pin::new(&mut self.socket)
            .start_send(Message::Binary(BytesMut::from(buf).freeze()))
            .map_err(io::Error::other)?;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket)
            .poll_flush(cx)
            .map_err(io::Error::other)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket)
            .poll_close(cx)
            .map_err(io::Error::other)
    }
}

fn websocket_disconnect_is_eof(error: &axum::Error) -> bool {
    let Some(error) = error
        .source()
        .and_then(|source| source.downcast_ref::<WsError>())
    else {
        return false;
    };
    match error {
        WsError::ConnectionClosed | WsError::AlreadyClosed => true,
        WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => true,
        WsError::Io(error) => matches!(
            error.kind(),
            io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::BrokenPipe
                | io::ErrorKind::NotConnected
        ),
        _ => false,
    }
}

#[derive(Clone)]
struct AdminRpcServer {
    control: ControlPlane,
}

#[derive(Clone)]
struct AppRpcServer {
    control: ControlPlane,
    app: AppAuth,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum RateLimitGroup {
    Api,
    Upload,
}

impl RateLimitGroup {
    fn limits(self) -> (f64, u32) {
        match self {
            Self::Api => (0.5, 30),
            Self::Upload => (1.0 / (30.0 * 60.0), 5),
        }
    }
}

struct ApiRateLimiter {
    state: Mutex<ApiRateLimiterState>,
}

struct ApiRateLimiterState {
    buckets: HashMap<(RateLimitGroup, String), ApiTokenBucket>,
}

#[derive(Debug, Clone, Copy)]
struct ApiTokenBucket {
    available_tokens: f64,
    last_refill: Instant,
}

#[derive(Debug, Clone, Copy)]
struct ApiRateLimitExceeded {
    retry_after: Duration,
}

impl std::fmt::Display for ApiRateLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rate limit exceeded; retry in {:.1}s",
            self.retry_after.as_secs_f64()
        )
    }
}

impl ApiRateLimiter {
    fn new() -> Self {
        Self {
            state: Mutex::new(ApiRateLimiterState {
                buckets: HashMap::new(),
            }),
        }
    }

    fn try_consume(
        &self,
        group: RateLimitGroup,
        bucket_key: &str,
        cost: u32,
    ) -> Result<(), ApiRateLimitExceeded> {
        let (refill_tokens_per_sec, capacity) = group.limits();
        let now = Instant::now();
        let mut state = match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.buckets.retain(|(bucket_group, _), bucket| {
            let (bucket_refill_tokens_per_sec, bucket_capacity) = bucket_group.limits();
            bucket.refill(now, bucket_capacity, bucket_refill_tokens_per_sec);
            !bucket.is_full(bucket_capacity)
        });
        let bucket = state
            .buckets
            .entry((group, bucket_key.to_string()))
            .or_insert_with(|| ApiTokenBucket::new(capacity, now));
        bucket.refill(now, capacity, refill_tokens_per_sec);
        let cost = cost as f64;
        if bucket.available_tokens + f64::EPSILON >= cost {
            bucket.available_tokens = (bucket.available_tokens - cost).max(0.0);
            Ok(())
        } else {
            let missing_tokens = (cost - bucket.available_tokens).max(0.0);
            Err(ApiRateLimitExceeded {
                retry_after: Duration::from_secs_f64(missing_tokens / refill_tokens_per_sec),
            })
        }
    }
}

impl ApiTokenBucket {
    fn new(capacity: u32, now: Instant) -> Self {
        Self {
            available_tokens: capacity as f64,
            last_refill: now,
        }
    }

    fn refill(&mut self, now: Instant, capacity: u32, refill_tokens_per_sec: f64) {
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            self.available_tokens =
                (self.available_tokens + elapsed * refill_tokens_per_sec).min(capacity as f64);
            self.last_refill = now;
        }
    }

    fn is_full(&self, capacity: u32) -> bool {
        self.available_tokens + f64::EPSILON >= capacity as f64
    }
}

#[derive(Default)]
struct BandwidthTracker {
    samples: Mutex<VecDeque<(Instant, usize)>>,
}

impl BandwidthTracker {
    fn record(&self, bytes: usize) {
        let now = Instant::now();
        let cutoff = now.checked_sub(BANDWIDTH_WINDOW).unwrap_or(now);
        let mut guard = match self.samples.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.push_back((now, bytes));
        while matches!(guard.front(), Some((at, _)) if *at < cutoff) {
            guard.pop_front();
        }
    }

    fn bytes_per_second(&self) -> u64 {
        let now = Instant::now();
        let cutoff = now.checked_sub(BANDWIDTH_WINDOW).unwrap_or(now);
        let mut guard = match self.samples.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        while matches!(guard.front(), Some((at, _)) if *at < cutoff) {
            guard.pop_front();
        }
        let total = guard.iter().map(|(_, bytes)| *bytes as u64).sum::<u64>();
        total / BANDWIDTH_WINDOW.as_secs().max(1)
    }
}

pub async fn admin_rpc(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(control): State<ControlPlane>,
    _: AdminGuard,
) -> Response {
    if let Err(response) = require_control_api_compatibility(&headers) {
        return response;
    }
    ws.max_message_size(CONTROL_RPC_MAX_FRAME_LEN)
        .max_frame_size(CONTROL_RPC_MAX_FRAME_LEN)
        .on_upgrade(move |socket| run_admin_rpc_socket(socket, control))
        .into_response()
}

pub async fn app_rpc(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(control): State<ControlPlane>,
    app: AppAuth,
) -> Response {
    if let Err(response) = require_control_api_compatibility(&headers) {
        return response;
    }
    ws.max_message_size(CONTROL_RPC_MAX_FRAME_LEN)
        .max_frame_size(CONTROL_RPC_MAX_FRAME_LEN)
        .on_upgrade(move |socket| run_app_rpc_socket(socket, control, app))
        .into_response()
}

async fn run_admin_rpc_socket(socket: WebSocket, control: ControlPlane) {
    let mut codec = LengthDelimitedCodec::new();
    codec.set_max_frame_length(CONTROL_RPC_MAX_FRAME_LEN);
    let transport = tarpc::serde_transport::new::<
        _,
        tarpc::ClientMessage<terminal_games::control::AdminControlRpcRequest>,
        tarpc::Response<terminal_games::control::AdminControlRpcResponse>,
        _,
    >(
        Framed::new(ServerWsTransport::new(socket), codec),
        tarpc::tokio_serde::formats::Bincode::default(),
    );
    server::BaseChannel::with_defaults(transport)
        .execute(AdminRpcServer { control }.serve())
        .for_each(|response| async move {
            let _ = response.await;
        })
        .await;
}

async fn run_app_rpc_socket(socket: WebSocket, control: ControlPlane, app: AppAuth) {
    let mut codec = LengthDelimitedCodec::new();
    codec.set_max_frame_length(CONTROL_RPC_MAX_FRAME_LEN);
    let transport = tarpc::serde_transport::new::<
        _,
        tarpc::ClientMessage<terminal_games::control::AppControlRpcRequest>,
        tarpc::Response<terminal_games::control::AppControlRpcResponse>,
        _,
    >(
        Framed::new(ServerWsTransport::new(socket), codec),
        tarpc::tokio_serde::formats::Bincode::default(),
    );
    server::BaseChannel::with_defaults(transport)
        .execute(AppRpcServer { control, app }.serve())
        .for_each(|response| async move {
            let _ = response.await;
        })
        .await;
}

impl AdminControlRpc for AdminRpcServer {
    async fn discover(self, _: context::Context) -> Result<NodeDiscoveryResponse, RpcError> {
        Ok(self.control.discover().await?)
    }

    async fn local_node_status(self, _: context::Context) -> Result<NodeRuntimeStatus, RpcError> {
        Ok(self.control.local_node_status())
    }

    async fn drain_start(
        self,
        _: context::Context,
        request: DrainStartRequest,
    ) -> Result<terminal_games::control::NodeShutdownStatus, RpcError> {
        Ok(self
            .control
            .shutdown
            .start_drain(
                Duration::from_secs(request.duration_seconds),
                request.maintenance_message,
            )
            .await
            .map_err(RpcError::from)?)
    }

    async fn drain_cancel(
        self,
        _: context::Context,
    ) -> Result<terminal_games::control::NodeShutdownStatus, RpcError> {
        Ok(self
            .control
            .shutdown
            .cancel_drain()
            .await
            .map_err(RpcError::from)?)
    }

    async fn sessions(self, _: context::Context) -> Result<Vec<SessionSummary>, RpcError> {
        let mut sessions = self.control.session_registry.summaries();
        sessions.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        Ok(sessions)
    }

    async fn ban_ip_add(
        self,
        _: context::Context,
        request: BanIpAddRequest,
    ) -> Result<BanIpAddResponse, RpcError> {
        let cidr = parse_ban_cidr(request.ip)?;
        let expires_at =
            parse_optional_expiry(request.duration.as_deref(), request.expires_at.as_deref())?;
        self.control
            .app_server
            .db
            .execute(
                "INSERT INTO ip_bans (cidr, reason, expires_at, inserted_at)
                 VALUES (?1, ?2, ?3, CAST(unixepoch('subsec') * 1000 AS INTEGER))
                 ON CONFLICT(cidr) DO UPDATE SET
                     reason = excluded.reason,
                     expires_at = excluded.expires_at,
                     inserted_at = excluded.inserted_at",
                libsql::params!(encode_cidr_blob(cidr), request.reason.clone(), expires_at),
            )
            .await?;
        Ok(BanIpAddResponse { expires_at })
    }

    async fn ban_ip_list(self, _: context::Context) -> Result<Vec<BanEntry>, RpcError> {
        Ok(load_ban_entries(&self.control.app_server.db).await?)
    }

    async fn cluster_kicked_ip_list(
        self,
        _: context::Context,
        request: ClusterKickedIpListRequest,
    ) -> Result<ClusterKickedIpListResponse, RpcError> {
        let page = request.page.max(1);
        let page_size = request.page_size.clamp(1, 100) as usize;
        let offset = ((page - 1) as usize).saturating_mul(page_size);
        Ok(cluster_kicked_ips::load_visible_page(
            &self.control.app_server.db,
            offset,
            page_size,
            request.exclude_banned,
        )
        .await?)
    }

    async fn ban_ip_remove(
        self,
        _: context::Context,
        request: BanIpRemoveRequest,
    ) -> Result<(), RpcError> {
        let cidr = parse_ban_cidr(request.ip)?;
        self.control
            .app_server
            .db
            .execute(
                "DELETE FROM ip_bans WHERE cidr = ?1",
                libsql::params!(encode_cidr_blob(cidr)),
            )
            .await?;
        Ok(())
    }

    async fn apply_ban(self, _: context::Context, request: BanIpRequest) -> Result<(), RpcError> {
        let cidr = parse_ban_cidr(request.ip)?;
        self.control.admission_controller.apply_ban_updates(vec![(
            cidr,
            Some(request.reason),
            request.expires_at,
        )]);
        Ok(())
    }

    async fn apply_ban_remove(
        self,
        _: context::Context,
        request: BanIpRemoveRequest,
    ) -> Result<(), RpcError> {
        let cidr = parse_ban_cidr(request.ip)?;
        self.control
            .admission_controller
            .apply_ban_updates(vec![(cidr, None, Some(0))]);
        Ok(())
    }

    async fn session_kick(
        self,
        _: context::Context,
        request: KickSessionRequest,
    ) -> Result<(), RpcError> {
        if self.control.session_registry.kick(request.local_session_id) {
            Ok(())
        } else {
            Err("session not found".into())
        }
    }

    async fn ticker_list(self, _: context::Context) -> Result<Vec<TickerEntry>, RpcError> {
        Ok(load_ticker_entries(&self.control.app_server.db).await?)
    }

    async fn ticker_add(
        self,
        _: context::Context,
        request: TickerAddRequest,
    ) -> Result<(), RpcError> {
        let content = request.content.trim();
        if content.is_empty() {
            return Err("ticker content cannot be empty".into());
        }
        if content.len() > 120 {
            return Err("ticker content exceeds 120 bytes".into());
        }
        let expires_at =
            parse_optional_expiry(request.duration.as_deref(), request.expires_at.as_deref())?;
        self.control
            .app_server
            .db
            .execute(
                "INSERT INTO status_tickers (sort_order, content, expires_at)
                 SELECT COALESCE(MAX(sort_order), 0) + 1, ?1, ?2
                 FROM status_tickers",
                libsql::params!(content, expires_at),
            )
            .await?;
        self.control.refresh_status_bar_state().await?;
        Ok(())
    }

    async fn ticker_reorder(
        self,
        _: context::Context,
        request: TickerReorderRequest,
    ) -> Result<(), RpcError> {
        let tx = self.control.app_server.db.transaction().await?;
        let mut rows = tx
            .query(
                "SELECT id FROM status_tickers ORDER BY sort_order ASC, id ASC",
                (),
            )
            .await?;
        let mut existing_ids = Vec::new();
        while let Some(row) = rows.next().await? {
            existing_ids.push(row.get::<u64>(0)?);
        }
        if existing_ids.is_empty() {
            return Err("no tickers exist to reorder".into());
        }
        let mut expected_ids = existing_ids.clone();
        expected_ids.sort_unstable();
        let mut provided_ids = request.ticker_ids.clone();
        provided_ids.sort_unstable();
        if provided_ids != expected_ids {
            return Err("ticker reorder must provide each ticker id exactly once".into());
        }
        let mut query = String::from("UPDATE status_tickers SET sort_order = CASE id");
        let mut params = Vec::with_capacity(request.ticker_ids.len() * 3);
        for (index, id) in request.ticker_ids.iter().copied().enumerate() {
            let id = i64::try_from(id).map_err(|_| RpcError::from("ticker id out of range"))?;
            query.push_str(" WHEN ? THEN ?");
            params.push(libsql::Value::Integer(id));
            params.push(libsql::Value::Integer((index as i64) + 1));
        }
        query.push_str(" END WHERE id IN (");
        for index in 0..request.ticker_ids.len() {
            if index > 0 {
                query.push_str(", ");
            }
            query.push('?');
        }
        query.push(')');
        for id in request.ticker_ids.iter().copied() {
            let id = i64::try_from(id).map_err(|_| RpcError::from("ticker id out of range"))?;
            params.push(libsql::Value::Integer(id));
        }
        tx.execute(&query, libsql::params_from_iter(params)).await?;
        tx.commit().await?;
        self.control.refresh_status_bar_state().await?;
        Ok(())
    }

    async fn ticker_remove(
        self,
        _: context::Context,
        request: TickerRemoveRequest,
    ) -> Result<(), RpcError> {
        self.control
            .app_server
            .db
            .execute(
                "DELETE FROM status_tickers WHERE id = ?1",
                libsql::params!(request.ticker_id),
            )
            .await?;
        self.control.refresh_status_bar_state().await?;
        Ok(())
    }

    async fn broadcast(
        self,
        _: context::Context,
        request: BroadcastRequest,
    ) -> Result<(), RpcError> {
        if request.message.trim().is_empty() {
            return Err("broadcast message cannot be empty".into());
        }
        if request.message.len() > 240 {
            return Err("broadcast message exceeds 240 bytes".into());
        }
        let expires_at = parse_duration_string(&request.duration).and_then(expiry_from_duration)?;
        let nodes = normalize_nodes(&request.nodes);
        let broadcast = if node_matches(&self.control.node_id, &nodes) {
            Some(StatusBroadcast {
                broadcast_id: current_unix_nanos().max(0) as u64,
                level: request.level,
                message: request.message,
                expires_at,
                created_at: current_unix_seconds(),
                nodes,
            })
        } else {
            None
        };
        self.control.set_latest_broadcast(broadcast);
        self.control.refresh_status_bar_state().await?;
        Ok(())
    }

    async fn status_bar_refresh(self, _: context::Context) -> Result<(), RpcError> {
        self.control.refresh_status_bar_state().await?;
        Ok(())
    }

    async fn app_create(
        self,
        _: context::Context,
        request: CreateAppRequest,
    ) -> Result<CreateAppResponse, RpcError> {
        validate_shortname(&request.shortname)?;
        let secret = random_token_secret();
        let token_hash = sha256_hex(&secret);
        let mut rows = self
            .control
            .app_server
            .db
            .query(
                "INSERT INTO app_tokens (shortname, token_hash)
                 VALUES (?1, ?2)
                 RETURNING id, shortname",
                libsql::params!(request.shortname.clone(), token_hash),
            )
            .await
            .map_err(|error| match shortname_exists_error(&error) {
                true => RpcError::from(format!("shortname '{}' already exists", request.shortname)),
                false => RpcError::from(error),
            })?;
        let Some(row) = rows.next().await? else {
            return Err("app row missing after insert".into());
        };
        let shortname = row.get::<String>(1)?;
        let app = AppSummary {
            app_id: row.get::<u64>(0)?,
            author_name: String::new(),
            shortname: shortname.clone(),
            playtime_seconds: 0.0,
        };
        Ok(CreateAppResponse {
            app,
            token: AppTokenClaims::new(request.base_url, shortname, secret).encode()?,
        })
    }

    async fn app_list(self, _: context::Context) -> Result<Vec<AppSummary>, RpcError> {
        let mut rows = self
            .control
            .app_server
            .db
            .query(
                "SELECT a.id,
                        COALESCE(json_extract(g.details, '$.author'), ''),
                        a.shortname,
                        COALESCE(g.duration_seconds, 0.0)
                 FROM app_tokens a
                 LEFT JOIN apps g ON g.shortname = a.shortname
                 ORDER BY a.id ASC",
                (),
            )
            .await?;
        let mut app_tokens = Vec::new();
        while let Some(row) = rows.next().await? {
            app_tokens.push(AppSummary {
                app_id: row.get::<u64>(0)?,
                author_name: row.get::<String>(1)?,
                shortname: row.get::<String>(2)?,
                playtime_seconds: row.get::<f64>(3)?,
            });
        }
        Ok(app_tokens)
    }

    async fn app_rotate_token(
        self,
        _: context::Context,
        request: RotateAppTokenRequest,
    ) -> Result<RotateAppTokenResponse, RpcError> {
        let secret = random_token_secret();
        let token_hash = sha256_hex(&secret);
        let mut rows = self
            .control
            .app_server
            .db
            .query(
                "UPDATE app_tokens
                 SET token_hash = ?2
                 WHERE id = ?1
                 RETURNING shortname",
                libsql::params!(request.app_id, token_hash),
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Err("app not found".into());
        };
        let shortname = row.get::<String>(0)?;
        Ok(RotateAppTokenResponse {
            app: AppSummary {
                app_id: request.app_id,
                author_name: String::new(),
                shortname: shortname.clone(),
                playtime_seconds: 0.0,
            },
            token: AppTokenClaims::new(request.base_url, shortname, secret).encode()?,
        })
    }

    async fn app_delete(
        self,
        _: context::Context,
        request: DeleteAppRequest,
    ) -> Result<Option<DeleteShortnameResponse>, RpcError> {
        let deleted = delete_app_token_and_app(&self.control.app_server.db, request.app_id).await?;
        if let Some((update, shortname)) = deleted {
            if let Some(update) = update {
                self.control.publish_app_runtime_update(update).await;
            }
            Ok(Some(DeleteShortnameResponse { shortname }))
        } else {
            Ok(None)
        }
    }
}

impl AppControlRpc for AppRpcServer {
    async fn self_info(
        self,
        _: context::Context,
        request: AppSelfInfoRequest,
    ) -> Result<AppSelfInfoResponse, RpcError> {
        if request.tokens.len() > APP_SELF_INFO_MAX_TOKENS {
            return Err(RpcError::from(format!(
                "too many app tokens: max {}",
                APP_SELF_INFO_MAX_TOKENS
            )));
        }
        self.control
            .consume_tokens_rpc(RateLimitGroup::Api, &self.app.record.token_hash, 1)?;
        let mut seen_shortnames = HashSet::new();
        let unique_claims = request
            .tokens
            .into_iter()
            .filter(|claims| seen_shortnames.insert(claims.shortname.clone()))
            .collect::<Vec<_>>();
        let records_by_shortname = load_app_self_info_records_by_shortnames(
            &self.control.app_server.db,
            unique_claims.iter().map(|claims| claims.shortname.as_str()),
        )
        .await?;
        let mut apps = Vec::new();
        let mut invalid_shortnames = Vec::new();
        for claims in unique_claims {
            let Some(record) = records_by_shortname.get(&claims.shortname) else {
                invalid_shortnames.push(claims.shortname);
                continue;
            };
            if !constant_time_eq(
                sha256_hex(&claims.secret).as_bytes(),
                record.app_token.token_hash.as_bytes(),
            ) {
                invalid_shortnames.push(claims.shortname);
                continue;
            }
            apps.push(record.to_response(&self.control.node_id));
        }
        Ok(AppSelfInfoResponse {
            apps,
            invalid_shortnames,
        })
    }

    async fn env_list(self, _: context::Context) -> Result<AppEnvListResponse, RpcError> {
        self.control
            .consume_tokens_rpc(RateLimitGroup::Api, &self.app.record.token_hash, 1)?;
        let (_, env_salt, env_blob) =
            load_app_env_blob(&self.control.app_server.db, &self.app.record.shortname)
                .await?
                .ok_or_else(|| RpcError::from("upload an app before managing env vars"))?;
        let envs = decrypt_app_env_blob(
            self.control.app_server.app_env_secret_key(),
            &env_salt,
            &env_blob,
        )?;
        Ok(AppEnvListResponse {
            shortname: self.app.record.shortname,
            envs,
        })
    }

    async fn env_set(self, _: context::Context, request: AppEnvSetRequest) -> Result<(), RpcError> {
        self.control
            .consume_tokens_rpc(RateLimitGroup::Api, &self.app.record.token_hash, 1)?;
        validate_app_envs(&request.envs)?;
        let (app_id, env_salt, env_blob) =
            load_app_env_blob(&self.control.app_server.db, &self.app.record.shortname)
                .await?
                .ok_or_else(|| RpcError::from("upload an app before managing env vars"))?;
        let existing = decrypt_app_env_blob(
            self.control.app_server.app_env_secret_key(),
            &env_salt,
            &env_blob,
        )?;
        let previous_env_hash = hash_app_envs(&existing);
        let mut current = if request.replace {
            Vec::new()
        } else {
            existing
        };
        for env in request.envs.iter().cloned() {
            if let Some(existing) = current
                .iter_mut()
                .find(|existing| existing.name == env.name)
            {
                existing.value = env.value;
            } else {
                current.push(env);
            }
        }
        let encrypted =
            encrypt_app_env_blob(self.control.app_server.app_env_secret_key(), &current)?;
        let env_hash = hash_app_envs(&current);
        if let Some(updated_at_ns) = store_app_envs(
            &self.control.app_server.db,
            app_id,
            previous_env_hash,
            env_hash,
            encrypted,
        )
        .await?
        {
            publish_app_build_update(&self.control, app_id, updated_at_ns).await?;
        }
        Ok(())
    }

    async fn env_delete(
        self,
        _: context::Context,
        request: AppEnvDeleteRequest,
    ) -> Result<(), RpcError> {
        self.control
            .consume_tokens_rpc(RateLimitGroup::Api, &self.app.record.token_hash, 1)?;
        validate_app_env_name(&request.name)?;
        let Some((app_id, env_salt, env_blob)) =
            load_app_env_blob(&self.control.app_server.db, &self.app.record.shortname).await?
        else {
            return Err("upload an app before managing env vars".into());
        };
        let mut current = decrypt_app_env_blob(
            self.control.app_server.app_env_secret_key(),
            &env_salt,
            &env_blob,
        )?;
        let previous_env_hash = hash_app_envs(&current);
        current.retain(|env| env.name != request.name);
        let encrypted =
            encrypt_app_env_blob(self.control.app_server.app_env_secret_key(), &current)?;
        let env_hash = hash_app_envs(&current);
        if let Some(updated_at_ns) = store_app_envs(
            &self.control.app_server.db,
            app_id,
            previous_env_hash,
            env_hash,
            encrypted,
        )
        .await?
        {
            publish_app_build_update(&self.control, app_id, updated_at_ns).await?;
        }
        Ok(())
    }

    async fn rotate_token(self, _: context::Context) -> Result<RotateAppTokenResponse, RpcError> {
        self.control
            .consume_tokens_rpc(RateLimitGroup::Api, &self.app.record.token_hash, 1)?;
        Ok(self
            .control
            .rotate_app_token(
                self.app.record.id,
                &self.app.record.shortname,
                self.app.claims.url,
            )
            .await?)
    }

    async fn upload(
        self,
        _: context::Context,
        request: UploadAppRequest,
    ) -> Result<UploadAppResponse, RpcError> {
        self.control
            .consume_tokens_rpc(RateLimitGroup::Upload, &self.app.record.token_hash, 1)?;
        if request.wasm.len() > AUTHOR_UPLOAD_MAX_BYTES {
            return Err(RpcError::from(format!(
                "wasm upload exceeds {} bytes",
                AUTHOR_UPLOAD_MAX_BYTES
            )));
        }
        let manifest = extract_manifest_from_wasm(&request.wasm).and_then(|manifest| {
            manifest.ok_or_else(|| anyhow::anyhow!("missing embedded terminal-games manifest"))
        })?;
        let manifest = sanitize_manifest(&manifest)?;
        if manifest.shortname != self.app.record.shortname {
            return Err(RpcError::from(format!(
                "manifest shortname '{}' does not match app shortname '{}'",
                manifest.shortname, self.app.record.shortname
            )));
        }
        if let Some(envs) = &request.envs {
            validate_app_envs(envs)?;
        }
        let details_json = serde_json::to_string(&manifest.details)?;
        let tx = self.control.app_server.db.transaction().await?;
        let wasm_hash = hash_bytes(&request.wasm);
        let env_blob = match request.envs.as_deref() {
            Some(envs) => Some((
                encrypt_app_env_blob(self.control.app_server.app_env_secret_key(), envs)?,
                hash_app_envs(envs),
            )),
            None => None,
        };
        let empty_env_blob =
            encrypt_app_env_blob(self.control.app_server.app_env_secret_key(), &[])?;
        let empty_env_hash = hash_app_envs(&[]);
        let mut game_rows = tx
            .query(
                "SELECT id, env_salt, env_blob, env_hash, wasm_hash, build_updated_at FROM apps WHERE shortname = ?1 LIMIT 1",
                libsql::params!(self.app.record.shortname.as_str()),
            )
            .await?;
        let (app_id, build_id, updated_at_ns, publish_update) = if let Some(game_row) =
            game_rows.next().await?
        {
            let app_id = game_row.get::<u64>(0)?;
            let env_salt = game_row.get::<Vec<u8>>(1)?;
            let env_ciphertext = game_row.get::<Vec<u8>>(2)?;
            let existing_build_id = BuildId::from_hash_slices(
                &game_row.get::<Vec<u8>>(4)?,
                &game_row.get::<Vec<u8>>(3)?,
            )?;
            let env_hash = match &env_blob {
                Some((_, env_hash)) => *env_hash,
                None => existing_build_id.env_hash,
            };
            let existing_updated_at_ns = game_row.get::<i64>(5)?;
            let build_id = BuildId {
                wasm_hash,
                env_hash,
            };
            let build_changed = build_id != existing_build_id;
            let updated_at_ns = if build_changed {
                current_unix_nanos()
            } else {
                existing_updated_at_ns
            };
            let (stored_env_salt, stored_env_blob) = match &env_blob {
                Some((blob, _)) => (blob.salt.clone(), blob.ciphertext.clone()),
                None => (env_salt, env_ciphertext),
            };
            tx.execute(
                "UPDATE apps
                 SET wasm = ?2,
                     details = json(?3),
                     wasm_hash = ?4,
                     env_hash = ?5,
                     env_salt = ?6,
                     env_blob = ?7,
                     build_updated_at = ?8
                 WHERE id = ?1",
                libsql::params!(
                    app_id,
                    request.wasm.clone(),
                    details_json.as_str(),
                    build_id.wasm_hash.to_vec(),
                    build_id.env_hash.to_vec(),
                    stored_env_salt,
                    stored_env_blob,
                    updated_at_ns
                ),
            )
            .await?;
            (app_id, build_id, updated_at_ns, build_changed)
        } else {
            let build_id = BuildId {
                wasm_hash,
                env_hash: env_blob
                    .as_ref()
                    .map(|(_, env_hash)| *env_hash)
                    .unwrap_or(empty_env_hash),
            };
            let updated_at_ns = current_unix_nanos();
            let mut id_rows = tx
                .query(
                    "INSERT INTO apps (shortname, wasm, details, wasm_hash, env_hash, env_salt, env_blob, build_updated_at)
                     VALUES (?1, ?2, json(?3), ?4, ?5, ?6, ?7, ?8)
                     RETURNING id",
                    libsql::params!(
                        self.app.record.shortname.as_str(),
                        request.wasm.clone(),
                        details_json.as_str(),
                        build_id.wasm_hash.to_vec(),
                        build_id.env_hash.to_vec(),
                        env_blob
                            .as_ref()
                            .map(|(blob, _)| blob.salt.clone())
                            .unwrap_or_else(|| empty_env_blob.salt.clone()),
                        env_blob
                            .as_ref()
                            .map(|(blob, _)| blob.ciphertext.clone())
                            .unwrap_or_else(|| empty_env_blob.ciphertext.clone()),
                        updated_at_ns
                    ),
                )
                .await?;
            let app_id = id_rows
                .next()
                .await?
                .ok_or_else(|| RpcError::from("missing inserted app row"))?
                .get::<u64>(0)?;
            (app_id, build_id, updated_at_ns, true)
        };
        tx.commit().await?;
        let response = UploadAppResponse {
            shortname: self.app.record.shortname.clone(),
            build_id: build_id.id_string(),
            app_id,
        };
        if publish_update {
            self.control
                .publish_app_runtime_update(AppRuntimeUpdateMessage::published(
                    response.app_id,
                    build_id,
                    updated_at_ns,
                ))
                .await;
        }
        Ok(response)
    }

    async fn delete_shortname(
        self,
        _: context::Context,
        request: DeleteShortnameRequest,
    ) -> Result<Option<DeleteShortnameResponse>, RpcError> {
        self.control
            .consume_tokens_rpc(RateLimitGroup::Api, &self.app.record.token_hash, 1)?;
        if request.shortname != self.app.record.shortname {
            return Err("shortname mismatch".into());
        }
        let deleted =
            delete_app_token_and_app(&self.control.app_server.db, self.app.record.id).await?;
        if let Some((update, shortname)) = deleted {
            if let Some(update) = update {
                self.control.publish_app_runtime_update(update).await;
            }
            Ok(Some(DeleteShortnameResponse { shortname }))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Clone)]
struct AppTokenRecord {
    id: u64,
    shortname: String,
    token_hash: String,
}

#[derive(Debug, Clone)]
pub(crate) struct AppAuth {
    record: AppTokenRecord,
    claims: AppTokenClaims,
}

pub(crate) struct AdminGuard;

impl FromRequestParts<ControlPlane> for AdminGuard {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ControlPlane,
    ) -> Result<Self, Self::Rejection> {
        state.require_admin(&parts.headers)
    }
}

impl FromRequestParts<ControlPlane> for AppAuth {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ControlPlane,
    ) -> Result<Self, Self::Rejection> {
        state.require_app(&parts.headers).await
    }
}

#[derive(Deserialize)]
pub struct SpyQuery {
    #[serde(default)]
    pub rw: bool,
    #[serde(default)]
    pub show_input: bool,
}

pub async fn admin_session_spy(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    Path(local_session_id): Path<u64>,
    Query(query): Query<SpyQuery>,
    State(control): State<ControlPlane>,
    _: AdminGuard,
) -> Response {
    if let Err(response) = require_control_api_compatibility(&headers) {
        return response;
    }
    let Some(spy) = control
        .session_registry
        .spy(local_session_id, query.rw)
        .await
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let session_registry = control.session_registry.clone();
    ws.on_upgrade(move |socket| {
        run_spy_socket(
            socket,
            spy,
            session_registry,
            local_session_id,
            query.rw,
            query.show_input,
        )
    })
    .into_response()
}

async fn run_spy_socket(
    mut socket: WebSocket,
    mut spy: crate::sessions::SpySession,
    session_registry: Arc<SessionRegistry>,
    local_session_id: u64,
    mut rw: bool,
    show_input: bool,
) {
    let init = serde_json::to_string(&SpyControlMessage::Init {
        cols: spy.snapshot.cols,
        rows: spy.snapshot.rows,
        dump: spy.snapshot.dump.clone(),
    });
    let Ok(init) = init else {
        return;
    };
    if socket.send(Message::Text(init.into())).await.is_err() {
        return;
    }
    let initial_app = spy.app_rx.borrow().clone();
    let Ok(payload) = serde_json::to_string(&SpyControlMessage::App {
        app_id: initial_app.app_id.map(|app_id| app_id.to_string()),
        shortname: initial_app.shortname,
    }) else {
        return;
    };
    if socket.send(Message::Text(payload.into())).await.is_err() {
        return;
    }
    let initial_username = spy.username_rx.borrow().clone();
    let Ok(payload) = serde_json::to_string(&SpyControlMessage::Metadata {
        username: initial_username,
    }) else {
        return;
    };
    if socket.send(Message::Text(payload.into())).await.is_err() {
        return;
    }
    let initial_idle = *spy.idle_rx.borrow();
    let Ok(payload) = serde_json::to_string(&SpyControlMessage::Idle {
        fuel_seconds: initial_idle.fuel_seconds,
        paused: initial_idle.paused,
    }) else {
        return;
    };
    if socket.send(Message::Text(payload.into())).await.is_err() {
        return;
    }

    while let Some(message) = spy.pending_events.pop_front() {
        if !send_spy_event(&mut socket, message, show_input).await {
            return;
        }
    }

    loop {
        tokio::select! {
            message = spy.event_rx.recv() => {
                let Ok(message) = message else {
                    let _ = socket.send(Message::Close(Some(CloseFrame {
                        code: 1000,
                        reason: "spy stream ended".into(),
                    }))).await;
                    break;
                };
                if !send_spy_event(&mut socket, message, show_input).await {
                    break;
                }
            }
            changed = spy.size_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let (cols, rows) = *spy.size_rx.borrow_and_update();
                let Ok(payload) = serde_json::to_string(&SpyControlMessage::Resize { cols, rows }) else {
                    break;
                };
                if socket.send(Message::Text(payload.into())).await.is_err() {
                    break;
                }
            }
            changed = spy.username_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let username = spy.username_rx.borrow_and_update().clone();
                let Ok(payload) = serde_json::to_string(&SpyControlMessage::Metadata {
                    username,
                }) else {
                    break;
                };
                if socket.send(Message::Text(payload.into())).await.is_err() {
                    break;
                }
            }
            changed = spy.app_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let app = spy.app_rx.borrow_and_update().clone();
                let Ok(payload) = serde_json::to_string(&SpyControlMessage::App {
                    app_id: app.app_id.map(|app_id| app_id.to_string()),
                    shortname: app.shortname,
                }) else {
                    break;
                };
                if socket.send(Message::Text(payload.into())).await.is_err() {
                    break;
                }
            }
            changed = spy.idle_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let idle = *spy.idle_rx.borrow_and_update();
                let Ok(payload) = serde_json::to_string(&SpyControlMessage::Idle {
                    fuel_seconds: idle.fuel_seconds,
                    paused: idle.paused,
                }) else {
                    break;
                };
                if socket.send(Message::Text(payload.into())).await.is_err() {
                    break;
                }
            }
            incoming = socket.recv() => {
                let Some(incoming) = incoming else {
                    break;
                };
                let Ok(message) = incoming else {
                    break;
                };
                match message {
                    Message::Binary(data) if rw => {
                        if let Some(input_tx) = &spy.input_tx
                            && input_tx.send(data).await.is_err()
                        {
                            break;
                        }
                    }
                    Message::Text(text) => {
                        let Ok(message) = serde_json::from_str::<SpyClientMessage>(&text) else {
                            continue;
                        };
                        match message {
                            SpyClientMessage::SetIdlePaused { paused } => {
                                session_registry.set_idle_paused(local_session_id, paused);
                            }
                            SpyClientMessage::SetReadWrite { read_write } => {
                                rw = read_write;
                                spy.set_read_write(read_write);
                            }
                            SpyClientMessage::Kick => {
                                session_registry.kick(local_session_id);
                            }
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        }
    }
}

async fn send_spy_event(
    socket: &mut WebSocket,
    message: crate::sessions::SpyEvent,
    show_input: bool,
) -> bool {
    match message {
        crate::sessions::SpyEvent::Output { data, .. } => {
            socket.send(Message::Binary(data)).await.is_ok()
        }
        crate::sessions::SpyEvent::Input { data, .. } => {
            if !show_input {
                return true;
            }
            let Ok(payload) = serde_json::to_string(&SpyControlMessage::Input { data }) else {
                return false;
            };
            socket.send(Message::Text(payload.into())).await.is_ok()
        }
        crate::sessions::SpyEvent::Closed { reason, .. } => {
            let Ok(payload) = serde_json::to_string(&SpyControlMessage::Closed {
                reason_slug: reason.slug().to_string(),
                message: reason.user_message().to_string(),
            }) else {
                return false;
            };
            if socket.send(Message::Text(payload.into())).await.is_err() {
                return false;
            }
            let _ = socket
                .send(Message::Close(Some(CloseFrame {
                    code: 1000,
                    reason: reason.user_message().into(),
                })))
                .await;
            false
        }
    }
}

async fn load_app_token_record_by_shortname(
    db: &libsql::Connection,
    shortname: &str,
) -> anyhow::Result<Option<AppTokenRecord>> {
    let mut rows = db
        .query(
            "SELECT id, shortname, token_hash FROM app_tokens WHERE shortname = ?1 LIMIT 1",
            libsql::params!(shortname),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    Ok(Some(AppTokenRecord {
        id: row.get::<u64>(0)?,
        shortname: row.get::<String>(1)?,
        token_hash: row.get::<String>(2)?,
    }))
}

#[derive(Debug, Clone)]
struct AppSelfInfoRecord {
    app_token: AppTokenRecord,
    author_name: String,
    playtime_seconds: f64,
}

impl AppSelfInfoRecord {
    fn to_response(&self, node_id: &str) -> AppSelfResponse {
        AppSelfResponse {
            app_id: self.app_token.id,
            author_name: self.author_name.clone(),
            shortname: self.app_token.shortname.clone(),
            server: node_id.to_string(),
            playtime_seconds: self.playtime_seconds,
        }
    }
}

async fn load_app_self_info_records_by_shortnames<'a>(
    db: &libsql::Connection,
    shortnames: impl IntoIterator<Item = &'a str>,
) -> anyhow::Result<HashMap<String, AppSelfInfoRecord>> {
    let shortnames = shortnames
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    if shortnames.is_empty() {
        return Ok(HashMap::new());
    }

    let placeholders = std::iter::repeat_n("?", shortnames.len())
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT app_tokens.id,
                app_tokens.shortname,
                app_tokens.token_hash,
                COALESCE(json_extract(apps.details, '$.author'), ''),
                COALESCE(apps.duration_seconds, 0.0)
         FROM app_tokens
         LEFT JOIN apps ON apps.shortname = app_tokens.shortname
         WHERE app_tokens.shortname IN ({placeholders})"
    );

    let mut rows = db
        .query(&query, libsql::params_from_iter(shortnames))
        .await?;
    let mut records = HashMap::new();
    while let Some(row) = rows.next().await? {
        let shortname = row.get::<String>(1)?;
        records.insert(
            shortname.clone(),
            AppSelfInfoRecord {
                app_token: AppTokenRecord {
                    id: row.get::<u64>(0)?,
                    shortname,
                    token_hash: row.get::<String>(2)?,
                },
                author_name: row.get::<String>(3).unwrap_or_default(),
                playtime_seconds: row.get::<f64>(4)?,
            },
        );
    }
    Ok(records)
}

async fn load_app_env_blob(
    db: &libsql::Connection,
    shortname: &str,
) -> anyhow::Result<Option<(u64, Vec<u8>, Vec<u8>)>> {
    let mut rows = db
        .query(
            "SELECT id, env_salt, env_blob FROM apps WHERE shortname = ?1 LIMIT 1",
            libsql::params!(shortname),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    Ok(Some((
        row.get::<u64>(0)?,
        row.get::<Vec<u8>>(1)?,
        row.get::<Vec<u8>>(2)?,
    )))
}

async fn store_app_envs(
    db: &libsql::Connection,
    app_id: u64,
    previous_env_hash: ContentHash,
    env_hash: ContentHash,
    encrypted: EncryptedAuthorEnvBlob,
) -> anyhow::Result<Option<i64>> {
    if env_hash == previous_env_hash {
        return Ok(None);
    }
    let updated_at_ns = current_unix_nanos();
    let tx = db.transaction().await?;
    tx.execute(
        "UPDATE apps
         SET env_salt = ?2,
             env_blob = ?3,
             env_hash = ?4,
             build_updated_at = ?5
         WHERE id = ?1",
        libsql::params!(
            app_id,
            encrypted.salt,
            encrypted.ciphertext,
            env_hash.to_vec(),
            updated_at_ns
        ),
    )
    .await?;
    tx.commit().await?;
    Ok(Some(updated_at_ns))
}

async fn publish_app_build_update(
    control: &ControlPlane,
    app_id: u64,
    updated_at_ns: i64,
) -> anyhow::Result<()> {
    control
        .publish_app_runtime_update(AppRuntimeUpdateMessage::published(
            app_id,
            load_app_build_id(&control.app_server.db, app_id).await?,
            updated_at_ns,
        ))
        .await;
    Ok(())
}

async fn load_app_build_id(db: &libsql::Connection, app_id: u64) -> anyhow::Result<BuildId> {
    let mut rows = db
        .query(
            "SELECT wasm_hash, env_hash FROM apps WHERE id = ?1 LIMIT 1",
            libsql::params!(app_id),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| anyhow::anyhow!("app row missing after build update"))?;
    BuildId::from_hash_slices(&row.get::<Vec<u8>>(0)?, &row.get::<Vec<u8>>(1)?)
}

async fn delete_app_token_and_app(
    db: &libsql::Connection,
    app_token_id: u64,
) -> anyhow::Result<Option<(Option<AppRuntimeUpdateMessage>, String)>> {
    let tx = db.transaction().await?;
    let mut rows = tx
        .query(
            "SELECT app_tokens.shortname, apps.id, apps.wasm_hash, apps.env_hash
             FROM app_tokens
             LEFT JOIN apps ON apps.shortname = app_tokens.shortname
             WHERE app_tokens.id = ?1
             LIMIT 1",
            libsql::params!(app_token_id),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let shortname = row.get::<String>(0)?;
    let installed_app_id = row.get::<Option<u64>>(1)?;
    let deleted_build_id = match (
        row.get::<Option<Vec<u8>>>(2)?,
        row.get::<Option<Vec<u8>>>(3)?,
    ) {
        (Some(wasm_hash), Some(env_hash)) => {
            Some(BuildId::from_hash_slices(&wasm_hash, &env_hash)?)
        }
        _ => None,
    };
    tx.execute(
        "DELETE FROM app_tokens WHERE id = ?1",
        libsql::params!(app_token_id),
    )
    .await?;
    tx.execute(
        "DELETE FROM apps WHERE shortname = ?1",
        libsql::params!(shortname.as_str()),
    )
    .await?;
    tx.commit().await?;
    let deleted_at_ns = current_unix_nanos();
    let update = match (installed_app_id, deleted_build_id) {
        (Some(app_id), Some(build_id)) => Some(AppRuntimeUpdateMessage::deleted(
            app_id,
            build_id,
            deleted_at_ns,
        )),
        _ => None,
    };
    Ok(Some((update, shortname)))
}

async fn load_ticker_entries(db: &libsql::Connection) -> anyhow::Result<Vec<TickerEntry>> {
    let mut rows = db
        .query(
            "SELECT id, content, expires_at, sort_order, created_at
             FROM status_tickers
             ORDER BY sort_order ASC, id ASC",
            (),
        )
        .await?;
    let now = current_unix_seconds();
    let mut entries = Vec::new();
    while let Some(row) = rows.next().await? {
        let expires_at = row.get::<Option<i64>>(2)?;
        if expires_at.is_some_and(|expires_at| expires_at <= now) {
            continue;
        }
        entries.push(TickerEntry {
            ticker_id: row.get::<u64>(0)?,
            content: row.get::<String>(1)?,
            expires_at,
            sort_order: row.get::<i64>(3)?,
            created_at: row.get::<i64>(4)?,
        });
    }
    Ok(entries)
}

async fn load_ban_entries(db: &libsql::Connection) -> anyhow::Result<Vec<BanEntry>> {
    let mut rows = db
        .query(
            "SELECT cidr, COALESCE(reason, ''), expires_at, inserted_at
             FROM ip_bans
             ORDER BY inserted_at DESC, cidr ASC",
            (),
        )
        .await?;
    let now = current_unix_seconds();
    let mut entries = Vec::new();
    while let Some(row) = rows.next().await? {
        let cidr = decode_cidr_blob(&row.get::<Vec<u8>>(0)?).map_err(anyhow::Error::msg)?;
        let expires_at = row.get::<Option<i64>>(2)?;
        if !is_ban_active(expires_at, now) {
            continue;
        }
        entries.push(BanEntry {
            ip: cidr.to_string(),
            reason: row.get::<String>(1)?.trim().to_string(),
            expires_at,
            inserted_at: row.get::<i64>(3)?,
        });
    }
    Ok(entries)
}

pub async fn load_status_bar_state(
    db: &libsql::Connection,
    broadcast: Option<StatusBroadcast>,
) -> anyhow::Result<StatusBarState> {
    let tickers = load_ticker_entries(db).await?;
    Ok(StatusBarState {
        tickers,
        broadcasts: broadcast.into_iter().collect(),
        drain: None,
    })
}

fn random_token_secret() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn sha256_bytes(secret: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hasher.finalize().into()
}

fn sha256_hex(secret: &str) -> String {
    hex::encode(sha256_bytes(secret))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&left, &right) in left.iter().zip(right.iter()) {
        diff |= left ^ right;
    }
    diff == 0
}

fn normalize_nodes(nodes: &[String]) -> Vec<String> {
    let mut nodes = nodes
        .iter()
        .map(|node| node.trim().to_string())
        .filter(|node| !node.is_empty())
        .collect::<Vec<_>>();
    nodes.sort();
    nodes.dedup();
    nodes
}

fn node_matches(node_id: &str, nodes: &[String]) -> bool {
    nodes.is_empty() || nodes.iter().any(|node| node == node_id)
}

fn current_unix_seconds() -> i64 {
    OffsetDateTime::now_utc().unix_timestamp()
}

fn current_unix_nanos() -> i64 {
    OffsetDateTime::now_utc().unix_timestamp_nanos() as i64
}

fn is_ban_active(expires_at: Option<i64>, now: i64) -> bool {
    expires_at.is_none_or(|expires_at| expires_at > now)
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn require_control_api_compatibility(headers: &HeaderMap) -> Result<(), Response> {
    let expected_version = match headers.get(CONTROL_API_EXPECTED_VERSION_HEADER) {
        Some(value) => match value.to_str() {
            Ok(value) => {
                let value = value.trim();
                if value.is_empty() {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!(
                            "client must send a non-empty {} header with the control API version it expects.",
                            CONTROL_API_EXPECTED_VERSION_HEADER,
                        ),
                    )
                        .into_response());
                }
                value
            }
            Err(_) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!(
                        "client sent an invalid {} header; it must be a valid UTF-8 control API version string.",
                        CONTROL_API_EXPECTED_VERSION_HEADER,
                    ),
                )
                    .into_response());
            }
        },
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "client must send the {} header. This terminal-games-server {} provides control API {}.",
                    CONTROL_API_EXPECTED_VERSION_HEADER,
                    env!("CARGO_PKG_VERSION"),
                    CONTROL_API_VERSION,
                ),
            )
                .into_response());
        }
    };

    if expected_version == CONTROL_API_VERSION {
        return Ok(());
    }

    Err((
        StatusCode::BAD_REQUEST,
        format!(
            "server API version mismatch: this terminal-games-server {} provides control API {}, but the client expects {}. Upgrade or downgrade the CLI/server so the API versions match.",
            env!("CARGO_PKG_VERSION"),
            CONTROL_API_VERSION,
            expected_version,
        ),
    )
        .into_response())
}

fn shortname_exists_error(error: &libsql::Error) -> bool {
    match error {
        libsql::Error::SqliteFailure(_, detail)
        | libsql::Error::RemoteSqliteFailure(_, _, detail) => {
            detail.contains("UNIQUE constraint failed: app_tokens.shortname")
        }
        _ => false,
    }
}

fn internal_error(error: impl std::fmt::Display) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
}
