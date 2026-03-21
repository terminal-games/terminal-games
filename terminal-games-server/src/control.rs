// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use base64::Engine as _;
use axum::{
    Json,
    Router,
    body::Bytes,
    extract::{
        DefaultBodyLimit, Path, Query, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use rand_core::{OsRng, RngCore};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sysinfo::System;
use terminal_games::{
    app::AppServer,
    control::{
        AuthorSelfResponse, AuthorSummary, AuthorTokenClaims, BanIpRequest, CacheInvalidateRequest,
        CreateAuthorRequest, CreateAuthorResponse, DeleteAuthorRequest, DeleteShortnameRequest,
        DeleteShortnameResponse, KickSessionRequest, RegionDiscoveryResponse, RegionRuntimeStatus,
        SpyControlMessage, UploadGameResponse,
    },
    manifest::{GameManifest, extract_manifest_from_wasm, validate_shortname},
    mesh::Mesh,
};

use crate::{
    admission::{AdmissionController, BanRule},
    sessions::SessionRegistry,
};

const BANDWIDTH_WINDOW: Duration = Duration::from_secs(5);
const AUTHOR_UPLOAD_MAX_BYTES: usize = 50 * 1024 * 1024;

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
    ) -> Self {
        let admin_shared_secret = std::env::var("ADMIN_SHARED_SECRET")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(Arc::<str>::from);
        let region_id = std::env::var("REGION_ID")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| mesh.region().to_string());
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
        if token == expected {
            Ok(())
        } else {
            Err((StatusCode::UNAUTHORIZED, "invalid bearer token").into_response())
        }
    }

    async fn require_author(&self, headers: &HeaderMap) -> Result<AuthorRecord, Response> {
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
        if sha256_hex(&claims.secret) != record.token_hash {
            return Err((StatusCode::UNAUTHORIZED, "invalid author token").into_response());
        }
        Ok(record)
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

    fn local_region_status(&self) -> RegionRuntimeStatus {
        let mut system = System::new();
        system.refresh_memory();
        system.refresh_cpu_usage();
        RegionRuntimeStatus {
            region_id: self.region_id.clone(),
            current_sessions: self.session_registry.count(),
            max_capacity: self.max_capacity,
            cpu_usage_percent: system.global_cpu_usage(),
            memory_used_bytes: system.total_memory().saturating_sub(system.available_memory()),
            memory_total_bytes: system.total_memory(),
            bandwidth_bytes_per_second: self.bandwidth.bytes_per_second(),
        }
    }
}

pub fn router(control: ControlPlane) -> Router {
    Router::new()
        .route("/admin/discover", get(admin_discover))
        .route("/admin/regions", get(admin_regions))
        .route("/admin/sessions", get(admin_sessions))
        .route("/admin/ban-ip", post(admin_ban_ip))
        .route("/admin/apply-ban", post(admin_apply_ban))
        .route("/admin/session/kick", post(admin_session_kick))
        .route("/admin/session/spy/{local_session_id}", get(admin_session_spy))
        .route("/admin/author/create", post(admin_author_create))
        .route("/admin/author/list", get(admin_author_list))
        .route("/admin/author/delete", post(admin_author_delete))
        .route("/admin/cache/invalidate", post(admin_cache_invalidate))
        .route("/author/self", get(author_self))
        .route(
            "/author/upload",
            post(author_upload).layer(DefaultBodyLimit::max(AUTHOR_UPLOAD_MAX_BYTES)),
        )
        .route("/author/delete", post(author_delete))
        .with_state(control)
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

#[derive(Debug, Clone)]
struct AuthorRecord {
    id: u64,
    shortname: String,
    token_hash: String,
}

pub async fn admin_discover(
    State(control): State<ControlPlane>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = control.require_admin(&headers) {
        return response;
    }
    match control.discover().await {
        Ok(payload) => Json(payload).into_response(),
        Err(error) => internal_error(error),
    }
}

pub async fn admin_regions(
    State(control): State<ControlPlane>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = control.require_admin(&headers) {
        return response;
    }
    Json(control.local_region_status()).into_response()
}

pub async fn admin_sessions(
    State(control): State<ControlPlane>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = control.require_admin(&headers) {
        return response;
    }
    let mut sessions = control.session_registry.summaries();
    sessions.sort_by(|left, right| left.session_id.cmp(&right.session_id));
    Json(sessions).into_response()
}

pub async fn admin_ban_ip(
    State(control): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<BanIpRequest>,
) -> Response {
    if let Err(response) = control.require_admin(&headers) {
        return response;
    }
    let rule = match BanRule::parse(request.ip.clone()) {
        Ok(rule) => rule,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    let result = async {
        control
            .app_server
            .db
            .execute(
                "INSERT INTO ip_bans (ip, reason, expires_at, inserted_at)
                 VALUES (?1, ?2, NULL, CAST(unixepoch('subsec') * 1000 AS INTEGER))
                 ON CONFLICT(ip) DO UPDATE SET
                     reason = excluded.reason,
                     expires_at = excluded.expires_at,
                     inserted_at = excluded.inserted_at",
                libsql::params!(request.ip, request.reason.clone()),
            )
            .await?;
        control
            .admission_controller
            .apply_ban_updates(vec![(rule, Some(request.reason), None)]);
        anyhow::Ok(())
    }
    .await;
    match result {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => internal_error(error),
    }
}

pub async fn admin_apply_ban(
    State(control): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<BanIpRequest>,
) -> Response {
    if let Err(response) = control.require_admin(&headers) {
        return response;
    }
    match BanRule::parse(request.ip) {
        Ok(rule) => {
            control
                .admission_controller
                .apply_ban_updates(vec![(rule, Some(request.reason), None)]);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => (StatusCode::BAD_REQUEST, error).into_response(),
    }
}

pub async fn admin_session_kick(
    State(control): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<KickSessionRequest>,
) -> Response {
    if let Err(response) = control.require_admin(&headers) {
        return response;
    }
    if control.session_registry.kick(request.local_session_id) {
        StatusCode::NO_CONTENT.into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

pub async fn admin_author_create(
    State(control): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<CreateAuthorRequest>,
) -> Response {
    if let Err(response) = control.require_admin(&headers) {
        return response;
    }
    if let Err(error) = validate_shortname(&request.shortname) {
        return (StatusCode::BAD_REQUEST, error.to_string()).into_response();
    }
    let result = async {
        let secret = random_token_secret();
        let token_hash = sha256_hex(&secret);
        control
            .app_server
            .db
            .execute(
                "INSERT INTO authors (shortname, token_hash)
                 VALUES (?1, ?2)",
                libsql::params!(request.shortname.clone(), token_hash),
            )
            .await?;
        let author = load_author_by_shortname(&control.app_server.db, &request.shortname)
            .await?
            .ok_or_else(|| anyhow::anyhow!("author row missing after insert"))?;
        let token = AuthorTokenClaims::new(request.base_url, author.shortname.clone(), secret)
            .encode()?;
        Ok::<_, anyhow::Error>(CreateAuthorResponse {
            author: author.into(),
            token,
        })
    }
    .await;
    match result {
        Ok(response) => Json(response).into_response(),
        Err(error) => internal_error(error),
    }
}

pub async fn admin_author_list(
    State(control): State<ControlPlane>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = control.require_admin(&headers) {
        return response;
    }
    match load_all_authors(&control.app_server.db).await {
        Ok(authors) => Json(authors).into_response(),
        Err(error) => internal_error(error),
    }
}

pub async fn admin_author_delete(
    State(control): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<DeleteAuthorRequest>,
) -> Response {
    if let Err(response) = control.require_admin(&headers) {
        return response;
    }
    match delete_author_and_game(&control.app_server.db, request.author_id).await {
        Ok(Some(shortname)) => {
            control.app_server.invalidate_shortname_cache(&shortname).await;
            Json(DeleteShortnameResponse { shortname }).into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => internal_error(error),
    }
}

pub async fn admin_cache_invalidate(
    State(control): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<CacheInvalidateRequest>,
) -> Response {
    if let Err(response) = control.require_admin(&headers) {
        return response;
    }
    control
        .app_server
        .invalidate_shortname_cache(&request.shortname)
        .await;
    StatusCode::NO_CONTENT.into_response()
}

pub async fn author_self(
    State(control): State<ControlPlane>,
    headers: HeaderMap,
) -> Response {
    let author = match control.require_author(&headers).await {
        Ok(author) => author,
        Err(response) => return response,
    };
    match load_author_self(&control.app_server.db, &author, &control.region_id).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => internal_error(error),
    }
}

pub async fn author_upload(
    State(control): State<ControlPlane>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let author = match control.require_author(&headers).await {
        Ok(author) => author,
        Err(response) => return response,
    };
    if body.len() > AUTHOR_UPLOAD_MAX_BYTES {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "wasm upload exceeds {} bytes",
                AUTHOR_UPLOAD_MAX_BYTES
            ),
        )
            .into_response();
    }
    let manifest = match extract_manifest_from_wasm(&body)
        .and_then(|manifest| {
            manifest.ok_or_else(|| anyhow::anyhow!("missing embedded terminal-games manifest"))
        }) {
        Ok(manifest) => manifest,
        Err(error) => return bad_request(error),
    };
    let manifest = match manifest.sanitized() {
        Ok(manifest) => manifest,
        Err(error) => return bad_request(error),
    };
    if manifest.shortname != author.shortname {
        return bad_request(format!(
            "manifest shortname '{}' does not match author shortname '{}'",
            manifest.shortname, author.shortname
        ));
    }
    match upload_version(&control.app_server.db, &author.shortname, &manifest, &body).await {
        Ok(response) => {
            control
                .app_server
                .apply_uploaded_version(response.game_id, response.version)
                .await;
            control
                .mesh
                .propagate_game_version_update(terminal_games::mesh::GameVersionUpdateMessage {
                    shortname: response.shortname.clone(),
                    game_id: response.game_id,
                    version: response.version,
                })
                .await;
            Json(response).into_response()
        }
        Err(error) => internal_error(error),
    }
}

pub async fn author_delete(
    State(control): State<ControlPlane>,
    headers: HeaderMap,
    Json(request): Json<DeleteShortnameRequest>,
) -> Response {
    let author = match control.require_author(&headers).await {
        Ok(author) => author,
        Err(response) => return response,
    };
    if request.shortname != author.shortname {
        return (StatusCode::FORBIDDEN, "shortname mismatch").into_response();
    }
    match delete_author_and_game(&control.app_server.db, author.id).await {
        Ok(Some(shortname)) => {
            control.app_server.invalidate_shortname_cache(&shortname).await;
            Json(DeleteShortnameResponse { shortname }).into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => internal_error(error),
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
    headers: HeaderMap,
) -> Response {
    if let Err(response) = control.require_admin(&headers) {
        return response;
    }
    let Some(spy) = control.session_registry.spy(local_session_id, query.rw) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    ws.on_upgrade(move |socket| run_spy_socket(socket, spy, query.rw, query.show_input))
}

async fn run_spy_socket(
    mut socket: WebSocket,
    mut spy: crate::sessions::SpySession,
    rw: bool,
    show_input: bool,
) {
    let init = serde_json::to_string(&SpyControlMessage::Init {
        cols: spy.initial_cols,
        rows: spy.initial_rows,
        dump: spy.initial_dump,
    });
    let Ok(init) = init else {
        return;
    };
    if socket
        .send(Message::Text(init.into()))
        .await
        .is_err()
    {
        return;
    }

    if rw {
        loop {
            tokio::select! {
                message = spy.event_rx.recv() => {
                    let Ok(message) = message else {
                        let _ = close_spy_socket(&mut socket, "spy stream ended").await;
                        break;
                    };
                    match message {
                        crate::sessions::SpyEvent::Output(data) => {
                            if socket.send(Message::Binary(data.into())).await.is_err() {
                                break;
                            }
                        }
                        crate::sessions::SpyEvent::Resize { cols, rows } => {
                            let Ok(payload) =
                                serde_json::to_string(&SpyControlMessage::Resize { cols, rows })
                            else {
                                break;
                            };
                            if socket.send(Message::Text(payload.into())).await.is_err() {
                                break;
                            }
                        }
                        crate::sessions::SpyEvent::Metadata { username } => {
                            let Ok(payload) =
                                serde_json::to_string(&SpyControlMessage::Metadata { username })
                            else {
                                break;
                            };
                            if socket.send(Message::Text(payload.into())).await.is_err() {
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
                            let _ = send_spy_closed(&mut socket, reason).await;
                            break;
                        }
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
                        Message::Binary(data) => {
                            if let Some(input_tx) = &spy.input_tx {
                                if input_tx.send(Bytes::from(data)).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Message::Close(_) => break,
                        _ => {}
                    }
                }
            }
        }
        return;
    }

    loop {
        tokio::select! {
            message = spy.event_rx.recv() => {
                let Ok(message) = message else {
                    let _ = close_spy_socket(&mut socket, "spy stream ended").await;
                    break;
                };
                match message {
                    crate::sessions::SpyEvent::Output(data) => {
                        if socket.send(Message::Binary(data.into())).await.is_err() {
                            break;
                        }
                    }
                    crate::sessions::SpyEvent::Resize { cols, rows } => {
                        let Ok(payload) =
                            serde_json::to_string(&SpyControlMessage::Resize { cols, rows })
                        else {
                            break;
                        };
                        if socket.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    crate::sessions::SpyEvent::Metadata { username } => {
                        let Ok(payload) =
                            serde_json::to_string(&SpyControlMessage::Metadata { username })
                        else {
                            break;
                        };
                        if socket.send(Message::Text(payload.into())).await.is_err() {
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
                        let _ = send_spy_closed(&mut socket, reason).await;
                        break;
                    }
                }
            }
            incoming = socket.recv() => {
                let Some(incoming) = incoming else {
                    break;
                };
                let Ok(message) = incoming else {
                    break;
                };
                if matches!(message, Message::Close(_)) {
                    break;
                }
            }
        }
    }
}

async fn send_spy_closed(
    socket: &mut WebSocket,
    reason: terminal_games::app::SessionEndReason,
) -> Result<(), axum::Error> {
    let payload = serde_json::to_string(&SpyControlMessage::Closed {
        reason_slug: reason.slug().to_string(),
        message: reason.user_message().to_string(),
    })
    .map_err(axum::Error::new)?;
    socket.send(Message::Text(payload.into())).await?;
    close_spy_socket(socket, reason.user_message()).await
}

async fn close_spy_socket(socket: &mut WebSocket, reason: &str) -> Result<(), axum::Error> {
    socket
        .send(Message::Close(Some(CloseFrame {
            code: 1000,
            reason: reason.into(),
        })))
        .await
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

async fn load_author_by_shortname(
    db: &libsql::Connection,
    shortname: &str,
) -> anyhow::Result<Option<CreateAuthorSummaryRow>> {
    let mut rows = db
        .query(
            "SELECT id, shortname FROM authors WHERE shortname = ?1 LIMIT 1",
            libsql::params!(shortname),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    Ok(Some(CreateAuthorSummaryRow {
        author_id: row.get::<u64>(0)?,
        shortname: row.get::<String>(1)?,
    }))
}

struct CreateAuthorSummaryRow {
    author_id: u64,
    shortname: String,
}

impl From<CreateAuthorSummaryRow> for AuthorSummary {
    fn from(value: CreateAuthorSummaryRow) -> Self {
        Self {
            author_id: value.author_id,
            author_name: String::new(),
            shortname: value.shortname,
            playtime_seconds: 0.0,
        }
    }
}

async fn load_all_authors(db: &libsql::Connection) -> anyhow::Result<Vec<AuthorSummary>> {
    let mut rows = db
        .query(
            "SELECT a.id,
                    COALESCE(json_extract(g.details, '$.author'), ''),
                    a.shortname,
                    COALESCE(g.duration_seconds, 0)
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
            playtime_seconds: decode_sql_f64(&row, 3)?,
        });
    }
    Ok(authors)
}

async fn load_author_self(
    db: &libsql::Connection,
    author: &AuthorRecord,
    region_id: &str,
) -> anyhow::Result<AuthorSelfResponse> {
    let mut rows = db
        .query(
            "SELECT COALESCE(json_extract(details, '$.author'), ''),
                    COALESCE(duration_seconds, 0)
             FROM games WHERE shortname = ?1 LIMIT 1",
            libsql::params!(author.shortname.as_str()),
        )
        .await?;
    let (author_name, playtime_seconds) = match rows.next().await? {
        Some(row) => (
            row.get::<String>(0).unwrap_or_default(),
            decode_sql_f64(&row, 1)?,
        ),
        None => (String::new(), 0.0),
    };
    Ok(AuthorSelfResponse {
        author_id: author.id,
        author_name,
        shortname: author.shortname.clone(),
        server: region_id.to_string(),
        playtime_seconds,
    })
}

fn decode_sql_f64(row: &libsql::Row, idx: i32) -> anyhow::Result<f64> {
    match row.get_value(idx)? {
        libsql::Value::Real(value) => Ok(value),
        libsql::Value::Integer(value) => Ok(value as f64),
        libsql::Value::Null => Ok(0.0),
        other => Err(anyhow::anyhow!(
            "expected numeric SQL value at column {idx}, got {other:?}"
        )),
    }
}

async fn delete_author_and_game(
    db: &libsql::Connection,
    author_id: u64,
) -> anyhow::Result<Option<String>> {
    let tx = db.transaction().await?;
    let mut rows = tx
        .query(
            "SELECT shortname FROM authors WHERE id = ?1 LIMIT 1",
            libsql::params!(author_id),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let shortname = row.get::<String>(0)?;
    tx.execute("DELETE FROM authors WHERE id = ?1", libsql::params!(author_id))
        .await?;
    tx.execute(
        "DELETE FROM games WHERE shortname = ?1",
        libsql::params!(shortname.as_str()),
    )
    .await?;
    tx.commit().await?;
    Ok(Some(shortname))
}

async fn upload_version(
    db: &libsql::Connection,
    shortname: &str,
    manifest: &GameManifest,
    wasm: &[u8],
) -> anyhow::Result<UploadGameResponse> {
    let details_json = serde_json::to_string(&manifest.details)?;
    let tx = db.transaction().await?;
    let mut game_rows = tx
        .query(
            "SELECT id, current_version FROM games WHERE shortname = ?1 LIMIT 1",
            libsql::params!(shortname),
        )
        .await?;
    let (game_id, next_version) = if let Some(game_row) = game_rows.next().await? {
        let game_id = game_row.get::<u64>(0)?;
        let next_version = game_row.get::<u64>(1)? + 1;
        tx.execute(
            "UPDATE games
             SET wasm = ?2, details = json(?3), current_version = ?4
             WHERE id = ?1",
            libsql::params!(game_id, wasm.to_vec(), details_json.as_str(), next_version),
        )
        .await?;
        (game_id, next_version)
    } else {
        tx.execute(
            "INSERT INTO games (shortname, wasm, details, current_version)
             VALUES (?1, ?2, json(?3), 1)",
            libsql::params!(shortname, wasm.to_vec(), details_json.as_str()),
        )
        .await?;
        let mut id_rows = tx.query("SELECT last_insert_rowid()", ()).await?;
        let game_id = id_rows
            .next()
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing last_insert_rowid"))?
            .get::<u64>(0)?;
        (game_id, 1)
    };
    tx.commit().await?;

    Ok(UploadGameResponse {
        shortname: shortname.to_string(),
        version: next_version,
        game_id,
    })
}

fn random_token_secret() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn sha256_hex(secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hex::encode(hasher.finalize())
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn bad_request(error: impl std::fmt::Display) -> Response {
    (StatusCode::BAD_REQUEST, error.to_string()).into_response()
}

fn internal_error(error: impl std::fmt::Display) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
}
