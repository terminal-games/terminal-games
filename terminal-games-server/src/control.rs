// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::VecDeque,
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
    http::{HeaderMap, StatusCode, header::AUTHORIZATION, request::Parts},
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
    author_env::{
        EncryptedAuthorEnvBlob, decrypt_author_env_blob, encrypt_author_env_blob,
        validate_author_env_name, validate_author_envs,
    },
    control::{
        AdminControlRpc, AuthorControlRpc, AuthorEnvDeleteRequest, AuthorEnvListResponse,
        AuthorEnvSetRequest, AuthorSelfResponse, AuthorSummary, AuthorTokenClaims, BanEntry,
        BanIpAddRequest, BanIpAddResponse, BanIpRemoveRequest, BanIpRequest, BroadcastLevel,
        BroadcastRequest, CreateAuthorRequest, CreateAuthorResponse, DeleteAuthorRequest,
        DeleteShortnameRequest, DeleteShortnameResponse, KickSessionRequest,
        RegionDiscoveryResponse, RegionRuntimeStatus, RotateAuthorTokenRequest,
        RotateAuthorTokenResponse, RpcError, SessionSummary, SpyClientMessage, SpyControlMessage,
        StatusBarState, StatusBroadcast, TickerAddRequest, TickerEntry, TickerRemoveRequest,
        TickerReorderRequest, UploadGameRequest, UploadGameResponse, expiry_from_duration,
        parse_duration_string, parse_optional_expiry,
    },
    manifest::{extract_manifest_from_wasm, sanitize_manifest, validate_shortname},
    mesh::{BuildId, ContentHash, GameRuntimeUpdateMessage, Mesh, hash_author_envs, hash_bytes},
};
use time::OffsetDateTime;

use crate::{
    admission::{AdmissionController, BanRule},
    sessions::SessionRegistry,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::tungstenite::{Error as WsError, error::ProtocolError};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const BANDWIDTH_WINDOW: Duration = Duration::from_secs(5);
const AUTHOR_UPLOAD_MAX_BYTES: usize = 50 * 1024 * 1024;
const CONTROL_RPC_MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

#[derive(Clone)]
pub struct ControlPlane {
    app_server: Arc<AppServer>,
    admission_controller: Arc<AdmissionController>,
    session_registry: Arc<SessionRegistry>,
    mesh: Mesh,
    admin_shared_secret: Option<Arc<str>>,
    bandwidth: Arc<BandwidthTracker>,
    max_capacity: usize,
    region_id: String,
}

impl ControlPlane {
    pub fn new(
        app_server: Arc<AppServer>,
        admission_controller: Arc<AdmissionController>,
        session_registry: Arc<SessionRegistry>,
        mesh: Mesh,
        max_capacity: usize,
        admin_shared_secret: Option<Arc<str>>,
        region_id: String,
    ) -> Self {
        Self {
            app_server,
            admission_controller,
            session_registry,
            mesh,
            admin_shared_secret,
            bandwidth: Arc::new(BandwidthTracker::default()),
            max_capacity,
            region_id,
        }
    }

    pub fn record_bytes(&self, bytes: usize) {
        self.bandwidth.record(bytes);
    }

    fn require_admin(&self, headers: &HeaderMap) -> Result<(), Response> {
        let Some(expected) = self.admin_shared_secret.as_deref() else {
            return Err((StatusCode::SERVICE_UNAVAILABLE, "admin auth disabled").into_response());
        };
        let Some(token) = bearer_token(headers) else {
            return Err((StatusCode::UNAUTHORIZED, "missing bearer token").into_response());
        };
        if constant_time_eq(token.as_bytes(), expected.as_bytes()) {
            Ok(())
        } else {
            Err((StatusCode::UNAUTHORIZED, "invalid bearer token").into_response())
        }
    }

    async fn require_author(&self, headers: &HeaderMap) -> Result<AuthorAuth, Response> {
        let Some(token) = bearer_token(headers) else {
            return Err((StatusCode::UNAUTHORIZED, "missing bearer token").into_response());
        };
        let claims = AuthorTokenClaims::decode(token)
            .map_err(|error| (StatusCode::UNAUTHORIZED, error.to_string()).into_response())?;
        let Some(record) = load_author_record_by_shortname(&self.app_server.db, &claims.shortname)
            .await
            .map_err(internal_error)?
        else {
            return Err((StatusCode::UNAUTHORIZED, "unknown author").into_response());
        };
        if !constant_time_eq(
            sha256_hex(&claims.secret).as_bytes(),
            record.token_hash.as_bytes(),
        ) {
            return Err((StatusCode::UNAUTHORIZED, "invalid author token").into_response());
        }
        Ok(AuthorAuth { record, claims })
    }

    async fn discover(&self) -> anyhow::Result<RegionDiscoveryResponse> {
        let mut regions = self
            .mesh
            .discover_regions()
            .await?
            .into_iter()
            .map(|region| region.to_string())
            .collect::<Vec<_>>();
        regions.sort();
        Ok(RegionDiscoveryResponse {
            current_region: self.region_id.clone(),
            regions,
        })
    }

    async fn publish_game_runtime_update(&self, update: GameRuntimeUpdateMessage) {
        let _ = self.app_server.game_registry().apply_update(update);
        self.mesh.propagate_game_runtime_update(update).await;
    }

    async fn rotate_author_token(
        &self,
        author_id: u64,
        shortname: &str,
        base_url: String,
    ) -> anyhow::Result<RotateAuthorTokenResponse> {
        let secret = random_token_secret();
        let token_hash = sha256_hex(&secret);
        self.app_server
            .db
            .execute(
                "UPDATE authors SET token_hash = ?2 WHERE id = ?1",
                libsql::params!(author_id, token_hash),
            )
            .await?;
        Ok(RotateAuthorTokenResponse {
            author: AuthorSummary {
                author_id,
                author_name: String::new(),
                shortname: shortname.to_string(),
                playtime_seconds: 0.0,
            },
            token: AuthorTokenClaims::new(base_url, shortname.to_string(), secret).encode()?,
        })
    }

    pub async fn refresh_status_bar_state(&self) -> anyhow::Result<()> {
        let state = load_status_bar_state(&self.app_server.db, &self.region_id).await?;
        self.session_registry.set_status_bar_state(state);
        Ok(())
    }

    fn local_region_status(&self) -> RegionRuntimeStatus {
        let mut system = System::new();
        system.refresh_memory();
        system.refresh_cpu_usage();
        RegionRuntimeStatus {
            region_id: self.region_id.clone(),
            current_sessions: self.session_registry.count(),
            max_capacity: self.max_capacity,
            cpu_usage_percent: system.global_cpu_usage(),
            memory_used_bytes: system
                .total_memory()
                .saturating_sub(system.available_memory()),
            memory_total_bytes: system.total_memory(),
            bandwidth_bytes_per_second: self.bandwidth.bytes_per_second(),
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
        .route("/author/rpc", get(author_rpc))
        .with_state(control)
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
struct AuthorRpcServer {
    control: ControlPlane,
    author: AuthorAuth,
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
    State(control): State<ControlPlane>,
    _: AdminGuard,
) -> Response {
    ws.max_message_size(CONTROL_RPC_MAX_FRAME_LEN)
        .max_frame_size(CONTROL_RPC_MAX_FRAME_LEN)
        .on_upgrade(move |socket| run_admin_rpc_socket(socket, control))
}

pub async fn author_rpc(
    ws: WebSocketUpgrade,
    State(control): State<ControlPlane>,
    author: AuthorAuth,
) -> Response {
    ws.max_message_size(CONTROL_RPC_MAX_FRAME_LEN)
        .max_frame_size(CONTROL_RPC_MAX_FRAME_LEN)
        .on_upgrade(move |socket| run_author_rpc_socket(socket, control, author))
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

async fn run_author_rpc_socket(socket: WebSocket, control: ControlPlane, author: AuthorAuth) {
    let mut codec = LengthDelimitedCodec::new();
    codec.set_max_frame_length(CONTROL_RPC_MAX_FRAME_LEN);
    let transport = tarpc::serde_transport::new::<
        _,
        tarpc::ClientMessage<terminal_games::control::AuthorControlRpcRequest>,
        tarpc::Response<terminal_games::control::AuthorControlRpcResponse>,
        _,
    >(
        Framed::new(ServerWsTransport::new(socket), codec),
        tarpc::tokio_serde::formats::Bincode::default(),
    );
    server::BaseChannel::with_defaults(transport)
        .execute(AuthorRpcServer { control, author }.serve())
        .for_each(|response| async move {
            let _ = response.await;
        })
        .await;
}

impl AdminControlRpc for AdminRpcServer {
    async fn discover(self, _: context::Context) -> Result<RegionDiscoveryResponse, RpcError> {
        Ok(self.control.discover().await?)
    }

    async fn local_region_status(
        self,
        _: context::Context,
    ) -> Result<RegionRuntimeStatus, RpcError> {
        Ok(self.control.local_region_status())
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
        let expires_at =
            parse_optional_expiry(request.duration.as_deref(), request.expires_at.as_deref())?;
        self.control
            .app_server
            .db
            .execute(
                "INSERT INTO ip_bans (ip, reason, expires_at, inserted_at)
             VALUES (?1, ?2, ?3, CAST(unixepoch('subsec') * 1000 AS INTEGER))
             ON CONFLICT(ip) DO UPDATE SET
                 reason = excluded.reason,
                 expires_at = excluded.expires_at,
                 inserted_at = excluded.inserted_at",
                libsql::params!(request.ip, request.reason.clone(), expires_at),
            )
            .await?;
        Ok(BanIpAddResponse { expires_at })
    }

    async fn ban_ip_list(self, _: context::Context) -> Result<Vec<BanEntry>, RpcError> {
        let mut rows = self
            .control
            .app_server
            .db
            .query(
                "SELECT ip, COALESCE(reason, ''), expires_at, inserted_at
                 FROM ip_bans
                 ORDER BY inserted_at DESC, ip ASC",
                (),
            )
            .await?;
        let now = current_unix_seconds();
        let mut entries = Vec::new();
        while let Some(row) = rows.next().await? {
            let expires_at = row.get::<Option<i64>>(2)?;
            if !is_ban_active(expires_at, now) {
                continue;
            }
            entries.push(BanEntry {
                ip: row.get::<String>(0)?,
                reason: row.get::<String>(1)?.trim().to_string(),
                expires_at,
                inserted_at: row.get::<i64>(3)?,
            });
        }
        Ok(entries)
    }

    async fn ban_ip_remove(
        self,
        _: context::Context,
        request: BanIpRemoveRequest,
    ) -> Result<(), RpcError> {
        self.control
            .app_server
            .db
            .execute(
                "DELETE FROM ip_bans WHERE ip = ?1",
                libsql::params!(request.ip),
            )
            .await?;
        Ok(())
    }

    async fn apply_ban(self, _: context::Context, request: BanIpRequest) -> Result<(), RpcError> {
        let rule = BanRule::parse(request.ip)?;
        self.control.admission_controller.apply_ban_updates(vec![(
            rule,
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
        let rule = BanRule::parse(request.ip)?;
        self.control
            .admission_controller
            .apply_ban_updates(vec![(rule, None, Some(0))]);
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
        let mut rows = self
            .control
            .app_server
            .db
            .query(
                "SELECT COALESCE(MAX(sort_order), 0) FROM status_tickers",
                (),
            )
            .await?;
        let next_order = match rows.next().await? {
            Some(row) => row.get::<i64>(0)?,
            None => 0,
        } + 1;
        self.control
            .app_server
            .db
            .execute(
                "INSERT INTO status_tickers (sort_order, content, expires_at) VALUES (?1, ?2, ?3)",
                libsql::params!(next_order, content, expires_at),
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
        for (index, id) in request.ticker_ids.iter().copied().enumerate() {
            tx.execute(
                "UPDATE status_tickers SET sort_order = ?2 WHERE id = ?1",
                libsql::params!(id, (index as i64) + 1),
            )
            .await?;
        }
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
        let regions_csv = normalize_regions_csv(&request.regions);
        self.control
            .app_server
            .db
            .execute(
                "INSERT INTO status_broadcasts (level, regions, message, expires_at)
             VALUES (?1, ?2, ?3, ?4)",
                libsql::params!(
                    broadcast_level_sql(request.level),
                    regions_csv,
                    request.message,
                    expires_at
                ),
            )
            .await?;
        self.control.refresh_status_bar_state().await?;
        Ok(())
    }

    async fn status_bar_refresh(self, _: context::Context) -> Result<(), RpcError> {
        self.control.refresh_status_bar_state().await?;
        Ok(())
    }

    async fn author_create(
        self,
        _: context::Context,
        request: CreateAuthorRequest,
    ) -> Result<CreateAuthorResponse, RpcError> {
        validate_shortname(&request.shortname)?;
        let secret = random_token_secret();
        let token_hash = sha256_hex(&secret);
        self.control
            .app_server
            .db
            .execute(
                "INSERT INTO authors (shortname, token_hash)
             VALUES (?1, ?2)",
                libsql::params!(request.shortname.clone(), token_hash),
            )
            .await?;
        let mut rows = self
            .control
            .app_server
            .db
            .query(
                "SELECT id, shortname FROM authors WHERE shortname = ?1 LIMIT 1",
                libsql::params!(request.shortname),
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Err("author row missing after insert".into());
        };
        let shortname = row.get::<String>(1)?;
        let author = AuthorSummary {
            author_id: row.get::<u64>(0)?,
            author_name: String::new(),
            shortname: shortname.clone(),
            playtime_seconds: 0.0,
        };
        Ok(CreateAuthorResponse {
            author,
            token: AuthorTokenClaims::new(request.base_url, shortname, secret).encode()?,
        })
    }

    async fn author_list(self, _: context::Context) -> Result<Vec<AuthorSummary>, RpcError> {
        let mut rows = self
            .control
            .app_server
            .db
            .query(
                "SELECT a.id,
                        COALESCE(json_extract(g.details, '$.author'), ''),
                        a.shortname,
                        COALESCE(g.duration_seconds, 0.0)
                 FROM authors a
                 LEFT JOIN games g ON g.shortname = a.shortname
                 ORDER BY a.id ASC",
                (),
            )
            .await?;
        let mut authors = Vec::new();
        while let Some(row) = rows.next().await? {
            authors.push(AuthorSummary {
                author_id: row.get::<u64>(0)?,
                author_name: row.get::<String>(1)?,
                shortname: row.get::<String>(2)?,
                playtime_seconds: row.get::<f64>(3)?,
            });
        }
        Ok(authors)
    }

    async fn author_rotate_token(
        self,
        _: context::Context,
        request: RotateAuthorTokenRequest,
    ) -> Result<RotateAuthorTokenResponse, RpcError> {
        let mut rows = self
            .control
            .app_server
            .db
            .query(
                "SELECT id, shortname FROM authors WHERE id = ?1 LIMIT 1",
                libsql::params!(request.author_id),
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Err("author not found".into());
        };
        let author_id = row.get::<u64>(0)?;
        let shortname = row.get::<String>(1)?;
        Ok(self
            .control
            .rotate_author_token(author_id, &shortname, request.base_url)
            .await?)
    }

    async fn author_delete(
        self,
        _: context::Context,
        request: DeleteAuthorRequest,
    ) -> Result<Option<DeleteShortnameResponse>, RpcError> {
        let deleted =
            delete_author_and_game(&self.control.app_server.db, request.author_id).await?;
        if let Some((update, shortname)) = deleted {
            if let Some(update) = update {
                self.control.publish_game_runtime_update(update).await;
            }
            Ok(Some(DeleteShortnameResponse { shortname }))
        } else {
            Ok(None)
        }
    }
}

impl AuthorControlRpc for AuthorRpcServer {
    async fn self_info(self, _: context::Context) -> Result<AuthorSelfResponse, RpcError> {
        let mut rows = self
            .control
            .app_server
            .db
            .query(
                "SELECT COALESCE(json_extract(details, '$.author'), ''),
                        COALESCE(duration_seconds, 0.0)
                 FROM games WHERE shortname = ?1 LIMIT 1",
                libsql::params!(self.author.record.shortname.as_str()),
            )
            .await?;
        let (author_name, playtime_seconds) = match rows.next().await? {
            Some(row) => (row.get::<String>(0).unwrap_or_default(), row.get::<f64>(1)?),
            None => (String::new(), 0.0),
        };
        Ok(AuthorSelfResponse {
            author_id: self.author.record.id,
            author_name,
            shortname: self.author.record.shortname.clone(),
            server: self.control.region_id.clone(),
            playtime_seconds,
        })
    }

    async fn env_list(self, _: context::Context) -> Result<AuthorEnvListResponse, RpcError> {
        let (_, env_salt, env_blob) =
            load_game_env_blob(&self.control.app_server.db, &self.author.record.shortname)
                .await?
                .ok_or_else(|| RpcError::from("upload a game before managing env vars"))?;
        let envs = decrypt_author_env_blob(
            self.control.app_server.author_env_secret_key(),
            &env_salt,
            &env_blob,
        )?;
        Ok(AuthorEnvListResponse {
            shortname: self.author.record.shortname,
            envs,
        })
    }

    async fn env_set(
        self,
        _: context::Context,
        request: AuthorEnvSetRequest,
    ) -> Result<(), RpcError> {
        validate_author_envs(&request.envs)?;
        let (game_id, env_salt, env_blob) =
            load_game_env_blob(&self.control.app_server.db, &self.author.record.shortname)
                .await?
                .ok_or_else(|| RpcError::from("upload a game before managing env vars"))?;
        let existing = decrypt_author_env_blob(
            self.control.app_server.author_env_secret_key(),
            &env_salt,
            &env_blob,
        )?;
        let previous_env_hash = hash_author_envs(&existing);
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
            encrypt_author_env_blob(self.control.app_server.author_env_secret_key(), &current)?;
        let env_hash = hash_author_envs(&current);
        if let Some(updated_at_ns) = store_game_envs(
            &self.control.app_server.db,
            game_id,
            previous_env_hash,
            env_hash,
            encrypted,
        )
        .await?
        {
            publish_game_build_update(&self.control, game_id, updated_at_ns).await?;
        }
        Ok(())
    }

    async fn env_delete(
        self,
        _: context::Context,
        request: AuthorEnvDeleteRequest,
    ) -> Result<(), RpcError> {
        validate_author_env_name(&request.name)?;
        let Some((game_id, env_salt, env_blob)) =
            load_game_env_blob(&self.control.app_server.db, &self.author.record.shortname).await?
        else {
            return Err("upload a game before managing env vars".into());
        };
        let mut current = decrypt_author_env_blob(
            self.control.app_server.author_env_secret_key(),
            &env_salt,
            &env_blob,
        )?;
        let previous_env_hash = hash_author_envs(&current);
        current.retain(|env| env.name != request.name);
        let encrypted =
            encrypt_author_env_blob(self.control.app_server.author_env_secret_key(), &current)?;
        let env_hash = hash_author_envs(&current);
        if let Some(updated_at_ns) = store_game_envs(
            &self.control.app_server.db,
            game_id,
            previous_env_hash,
            env_hash,
            encrypted,
        )
        .await?
        {
            publish_game_build_update(&self.control, game_id, updated_at_ns).await?;
        }
        Ok(())
    }

    async fn rotate_token(
        self,
        _: context::Context,
    ) -> Result<RotateAuthorTokenResponse, RpcError> {
        Ok(self
            .control
            .rotate_author_token(
                self.author.record.id,
                &self.author.record.shortname,
                self.author.claims.url,
            )
            .await?)
    }

    async fn upload(
        self,
        _: context::Context,
        request: UploadGameRequest,
    ) -> Result<UploadGameResponse, RpcError> {
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
        if manifest.shortname != self.author.record.shortname {
            return Err(RpcError::from(format!(
                "manifest shortname '{}' does not match author shortname '{}'",
                manifest.shortname, self.author.record.shortname
            )));
        }
        if let Some(envs) = &request.envs {
            validate_author_envs(envs)?;
        }
        let details_json = serde_json::to_string(&manifest.details)?;
        let tx = self.control.app_server.db.transaction().await?;
        let wasm_hash = hash_bytes(&request.wasm);
        let env_blob = match request.envs.as_deref() {
            Some(envs) => Some((
                encrypt_author_env_blob(self.control.app_server.author_env_secret_key(), envs)?,
                hash_author_envs(envs),
            )),
            None => None,
        };
        let empty_env_blob =
            encrypt_author_env_blob(self.control.app_server.author_env_secret_key(), &[])?;
        let empty_env_hash = hash_author_envs(&[]);
        let mut game_rows = tx
            .query(
                "SELECT id, env_salt, env_blob, env_hash, wasm_hash, build_updated_at FROM games WHERE shortname = ?1 LIMIT 1",
                libsql::params!(self.author.record.shortname.as_str()),
            )
            .await?;
        let (game_id, build_id, updated_at_ns, publish_update) = if let Some(game_row) =
            game_rows.next().await?
        {
            let game_id = game_row.get::<u64>(0)?;
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
                "UPDATE games
                 SET wasm = ?2,
                     details = json(?3),
                     wasm_hash = ?4,
                     env_hash = ?5,
                     env_salt = ?6,
                     env_blob = ?7,
                     build_updated_at = ?8
                 WHERE id = ?1",
                libsql::params!(
                    game_id,
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
            (game_id, build_id, updated_at_ns, build_changed)
        } else {
            let build_id = BuildId {
                wasm_hash,
                env_hash: env_blob
                    .as_ref()
                    .map(|(_, env_hash)| *env_hash)
                    .unwrap_or(empty_env_hash),
            };
            let updated_at_ns = current_unix_nanos();
            tx.execute(
                "INSERT INTO games (shortname, wasm, details, wasm_hash, env_hash, env_salt, env_blob, build_updated_at)
                     VALUES (?1, ?2, json(?3), ?4, ?5, ?6, ?7, ?8)",
                libsql::params!(
                    self.author.record.shortname.as_str(),
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
            let mut id_rows = tx.query("SELECT last_insert_rowid()", ()).await?;
            let game_id = id_rows
                .next()
                .await?
                .ok_or_else(|| RpcError::from("missing last_insert_rowid"))?
                .get::<u64>(0)?;
            (game_id, build_id, updated_at_ns, true)
        };
        tx.commit().await?;
        let response = UploadGameResponse {
            shortname: self.author.record.shortname.clone(),
            build_id: build_id.id_string(),
            game_id,
        };
        if publish_update {
            self.control
                .publish_game_runtime_update(GameRuntimeUpdateMessage::published(
                    response.game_id,
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
        if request.shortname != self.author.record.shortname {
            return Err("shortname mismatch".into());
        }
        let deleted =
            delete_author_and_game(&self.control.app_server.db, self.author.record.id).await?;
        if let Some((update, shortname)) = deleted {
            if let Some(update) = update {
                self.control.publish_game_runtime_update(update).await;
            }
            Ok(Some(DeleteShortnameResponse { shortname }))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Clone)]
struct AuthorRecord {
    id: u64,
    shortname: String,
    token_hash: String,
}

#[derive(Debug, Clone)]
pub(crate) struct AuthorAuth {
    record: AuthorRecord,
    claims: AuthorTokenClaims,
}

pub(crate) struct AdminGuard;

impl FromRequestParts<ControlPlane> for AdminGuard {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ControlPlane,
    ) -> Result<Self, Self::Rejection> {
        state.require_admin(&parts.headers)?;
        Ok(Self)
    }
}

impl FromRequestParts<ControlPlane> for AuthorAuth {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ControlPlane,
    ) -> Result<Self, Self::Rejection> {
        state.require_author(&parts.headers).await
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
    Path(local_session_id): Path<u64>,
    Query(query): Query<SpyQuery>,
    State(control): State<ControlPlane>,
    _: AdminGuard,
) -> Response {
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
}

async fn run_spy_socket(
    mut socket: WebSocket,
    mut spy: crate::sessions::SpySession,
    session_registry: Arc<SessionRegistry>,
    local_session_id: u64,
    rw: bool,
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
                match message {
                    crate::sessions::SpyEvent::Output(data) => {
                        if socket.send(Message::Binary(data)).await.is_err() {
                            break;
                        }
                    }
                    crate::sessions::SpyEvent::Input { data } => {
                        if !show_input {
                            continue;
                        }
                        let Ok(payload) =
                            serde_json::to_string(&SpyControlMessage::Input { data })
                        else {
                            break;
                        };
                        if socket.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    crate::sessions::SpyEvent::Closed { reason } => {
                        let Ok(payload) = serde_json::to_string(&SpyControlMessage::Closed {
                            reason_slug: reason.slug().to_string(),
                            message: reason.user_message().to_string(),
                        }) else {
                            break;
                        };
                        if socket.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                        let _ = socket.send(Message::Close(Some(CloseFrame {
                            code: 1000,
                            reason: reason.user_message().into(),
                        }))).await;
                        break;
                    }
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

async fn load_author_record_by_shortname(
    db: &libsql::Connection,
    shortname: &str,
) -> anyhow::Result<Option<AuthorRecord>> {
    let mut rows = db
        .query(
            "SELECT id, shortname, token_hash FROM authors WHERE shortname = ?1 LIMIT 1",
            libsql::params!(shortname),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    Ok(Some(AuthorRecord {
        id: row.get::<u64>(0)?,
        shortname: row.get::<String>(1)?,
        token_hash: row.get::<String>(2)?,
    }))
}

async fn load_game_env_blob(
    db: &libsql::Connection,
    shortname: &str,
) -> anyhow::Result<Option<(u64, Vec<u8>, Vec<u8>)>> {
    let mut rows = db
        .query(
            "SELECT id, env_salt, env_blob FROM games WHERE shortname = ?1 LIMIT 1",
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

async fn store_game_envs(
    db: &libsql::Connection,
    game_id: u64,
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
        "UPDATE games
         SET env_salt = ?2,
             env_blob = ?3,
             env_hash = ?4,
             build_updated_at = ?5
         WHERE id = ?1",
        libsql::params!(
            game_id,
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

async fn publish_game_build_update(
    control: &ControlPlane,
    game_id: u64,
    updated_at_ns: i64,
) -> anyhow::Result<()> {
    control
        .publish_game_runtime_update(GameRuntimeUpdateMessage::published(
            game_id,
            load_game_build_id(&control.app_server.db, game_id).await?,
            updated_at_ns,
        ))
        .await;
    Ok(())
}

async fn load_game_build_id(db: &libsql::Connection, game_id: u64) -> anyhow::Result<BuildId> {
    let mut rows = db
        .query(
            "SELECT wasm_hash, env_hash FROM games WHERE id = ?1 LIMIT 1",
            libsql::params!(game_id),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| anyhow::anyhow!("game row missing after build update"))?;
    BuildId::from_hash_slices(&row.get::<Vec<u8>>(0)?, &row.get::<Vec<u8>>(1)?)
}

async fn delete_author_and_game(
    db: &libsql::Connection,
    author_id: u64,
) -> anyhow::Result<Option<(Option<GameRuntimeUpdateMessage>, String)>> {
    let tx = db.transaction().await?;
    let mut rows = tx
        .query(
            "SELECT authors.shortname, games.id, games.wasm_hash, games.env_hash
             FROM authors
             LEFT JOIN games ON games.shortname = authors.shortname
             WHERE authors.id = ?1
             LIMIT 1",
            libsql::params!(author_id),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let shortname = row.get::<String>(0)?;
    let game_id = row.get::<Option<u64>>(1)?;
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
        "DELETE FROM authors WHERE id = ?1",
        libsql::params!(author_id),
    )
    .await?;
    tx.execute(
        "DELETE FROM games WHERE shortname = ?1",
        libsql::params!(shortname.as_str()),
    )
    .await?;
    tx.commit().await?;
    let deleted_at_ns = current_unix_nanos();
    let update = match (game_id, deleted_build_id) {
        (Some(game_id), Some(build_id)) => Some(GameRuntimeUpdateMessage::deleted(
            game_id,
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

pub async fn load_status_bar_state(
    db: &libsql::Connection,
    region_id: &str,
) -> anyhow::Result<StatusBarState> {
    let tickers = load_ticker_entries(db).await?;
    let mut rows = db
        .query(
            "SELECT id, level, regions, message, expires_at, created_at
             FROM status_broadcasts
             ORDER BY created_at ASC, id ASC",
            (),
        )
        .await?;
    let now = current_unix_seconds();
    let mut broadcasts = Vec::new();
    while let Some(row) = rows.next().await? {
        let expires_at = row.get::<i64>(4)?;
        if expires_at <= now {
            continue;
        }
        let regions = parse_regions_csv(&row.get::<String>(2)?);
        if !regions.is_empty() && !regions.iter().any(|region| region == region_id) {
            continue;
        }
        broadcasts.push(StatusBroadcast {
            broadcast_id: row.get::<u64>(0)?,
            level: parse_broadcast_level(&row.get::<String>(1)?)?,
            regions,
            message: row.get::<String>(3)?,
            expires_at,
            created_at: row.get::<i64>(5)?,
        });
    }
    Ok(StatusBarState {
        tickers,
        broadcasts,
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

fn parse_broadcast_level(raw: &str) -> anyhow::Result<BroadcastLevel> {
    match raw {
        "info" => Ok(BroadcastLevel::Info),
        "warning" => Ok(BroadcastLevel::Warning),
        "error" => Ok(BroadcastLevel::Error),
        other => anyhow::bail!("invalid broadcast level '{other}'"),
    }
}

fn broadcast_level_sql(level: BroadcastLevel) -> &'static str {
    match level {
        BroadcastLevel::Info => "info",
        BroadcastLevel::Warning => "warning",
        BroadcastLevel::Error => "error",
    }
}

fn normalize_regions_csv(regions: &[String]) -> String {
    let mut regions = regions
        .iter()
        .map(|region| region.trim().to_string())
        .filter(|region| !region.is_empty())
        .collect::<Vec<_>>();
    regions.sort();
    regions.dedup();
    regions.join(",")
}

fn parse_regions_csv(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|region| !region.is_empty())
        .map(str::to_string)
        .collect()
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

fn internal_error(error: impl std::fmt::Display) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
}
