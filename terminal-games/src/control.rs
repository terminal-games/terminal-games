// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tarpc::context;
use time::{Duration as TimeDuration, OffsetDateTime, format_description::well_known::Rfc3339};

pub use crate::app_env::AppEnvVar;

const APP_TOKEN_PREFIX: &str = "tga1.";
const APP_TOKEN_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcError {
    pub message: String,
}

impl RpcError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RpcError {}

impl From<anyhow::Error> for RpcError {
    fn from(error: anyhow::Error) -> Self {
        Self::new(error.to_string())
    }
}

impl From<libsql::Error> for RpcError {
    fn from(error: libsql::Error) -> Self {
        Self::new(error.to_string())
    }
}

impl From<serde_json::Error> for RpcError {
    fn from(error: serde_json::Error) -> Self {
        Self::new(error.to_string())
    }
}

impl From<String> for RpcError {
    fn from(message: String) -> Self {
        Self { message }
    }
}

impl From<&str> for RpcError {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppTokenClaims {
    pub version: u32,
    pub url: String,
    pub shortname: String,
    pub secret: String,
}

impl AppTokenClaims {
    pub fn new(url: String, shortname: String, secret: String) -> Self {
        Self {
            version: APP_TOKEN_VERSION,
            url,
            shortname,
            secret,
        }
    }

    pub fn encode(&self) -> anyhow::Result<String> {
        anyhow::ensure!(
            self.version == APP_TOKEN_VERSION,
            "unsupported app token version {}",
            self.version
        );
        let json = serde_json::to_vec(self)?;
        Ok(format!(
            "{APP_TOKEN_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(json)
        ))
    }

    pub fn decode(token: &str) -> anyhow::Result<Self> {
        let encoded = token
            .strip_prefix(APP_TOKEN_PREFIX)
            .ok_or_else(|| anyhow::anyhow!("invalid app token prefix"))?;
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|error| anyhow::anyhow!("invalid app token encoding: {error}"))?;
        let claims = serde_json::from_slice::<Self>(&bytes)?;
        anyhow::ensure!(
            claims.version == APP_TOKEN_VERSION,
            "unsupported app token version {}",
            claims.version
        );
        Ok(claims)
    }
}

pub fn parse_optional_expiry(
    duration: Option<&str>,
    expires_at: Option<&str>,
) -> anyhow::Result<Option<i64>> {
    match (duration, expires_at) {
        (Some(_), Some(_)) => anyhow::bail!("pass either duration or expires-at, not both"),
        (Some(duration), None) => Ok(Some(expiry_from_duration(parse_duration_string(
            duration,
        )?)?)),
        (None, Some(expires_at)) => Ok(Some(parse_utc_timestamp(expires_at)?)),
        (None, None) => Ok(None),
    }
}

pub fn expiry_from_duration(duration: TimeDuration) -> anyhow::Result<i64> {
    Ok((OffsetDateTime::now_utc() + duration).unix_timestamp())
}

pub fn parse_duration_string(raw: &str) -> anyhow::Result<TimeDuration> {
    let trimmed = raw.trim().to_ascii_lowercase();
    anyhow::ensure!(!trimmed.is_empty(), "duration cannot be empty");
    let compact = trimmed.replace(' ', "");
    let split_at = compact
        .find(|ch: char| !ch.is_ascii_digit())
        .ok_or_else(|| anyhow::anyhow!("duration must include a unit like 1h or 1 week"))?;
    let amount = compact[..split_at]
        .parse::<i64>()
        .map_err(|_| anyhow::anyhow!("invalid duration amount"))?;
    anyhow::ensure!(amount > 0, "duration must be positive");
    let seconds = match &compact[split_at..] {
        "s" | "sec" | "secs" | "second" | "seconds" => amount,
        "m" | "min" | "mins" | "minute" | "minutes" => amount * 60,
        "h" | "hr" | "hrs" | "hour" | "hours" => amount * 60 * 60,
        "d" | "day" | "days" => amount * 60 * 60 * 24,
        "w" | "wk" | "wks" | "week" | "weeks" => amount * 60 * 60 * 24 * 7,
        unit => anyhow::bail!("unsupported duration unit '{unit}'"),
    };
    Ok(TimeDuration::seconds(seconds))
}

pub fn parse_utc_timestamp(raw: &str) -> anyhow::Result<i64> {
    Ok(OffsetDateTime::parse(raw.trim(), &Rfc3339)?.unix_timestamp())
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RegionDiscoveryResponse {
    pub current_region: String,
    pub regions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionRuntimeStatus {
    pub region_id: String,
    pub current_sessions: usize,
    pub max_capacity: usize,
    pub cpu_usage_percent: f32,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub bandwidth_bytes_per_second: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub local_session_id: u64,
    pub user_id: Option<u64>,
    pub region_id: String,
    pub transport: String,
    pub shortname: String,
    pub duration_seconds: u64,
    pub username: String,
    pub ip_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BanIpRequest {
    pub ip: String,
    pub reason: String,
    pub expires_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BanIpAddRequest {
    pub ip: String,
    pub reason: String,
    pub duration: Option<String>,
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BanIpAddResponse {
    pub expires_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BanIpRemoveRequest {
    pub ip: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BanEntry {
    pub ip: String,
    pub reason: String,
    pub expires_at: Option<i64>,
    pub inserted_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterKickedIpEntry {
    pub ip: String,
    pub count: u64,
    pub is_banned: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterKickedIpListRequest {
    pub page: u32,
    pub page_size: u32,
    pub exclude_banned: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterKickedIpListResponse {
    pub entries: Vec<ClusterKickedIpEntry>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KickSessionRequest {
    pub local_session_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAppRequest {
    pub shortname: String,
    pub base_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSummary {
    pub app_id: u64,
    pub author_name: String,
    pub shortname: String,
    pub playtime_seconds: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAppResponse {
    pub app: AppSummary,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteAppRequest {
    pub app_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RotateAppTokenRequest {
    pub app_id: u64,
    pub base_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RotateAppTokenResponse {
    pub app: AppSummary,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteShortnameRequest {
    pub shortname: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteShortnameResponse {
    pub shortname: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadAppResponse {
    pub shortname: String,
    pub build_id: String,
    pub app_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadAppRequest {
    pub wasm: Vec<u8>,
    pub envs: Option<Vec<AppEnvVar>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppEnvListResponse {
    pub shortname: String,
    pub envs: Vec<AppEnvVar>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppEnvSetRequest {
    pub envs: Vec<AppEnvVar>,
    #[serde(default)]
    pub replace: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppEnvDeleteRequest {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSelfResponse {
    pub app_id: u64,
    pub author_name: String,
    pub shortname: String,
    pub server: String,
    pub playtime_seconds: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSelfInfoRequest {
    pub tokens: Vec<AppTokenClaims>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSelfInfoResponse {
    pub apps: Vec<AppSelfResponse>,
    pub invalid_shortnames: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BroadcastLevel {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickerAddRequest {
    pub content: String,
    pub duration: Option<String>,
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickerRemoveRequest {
    pub ticker_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickerReorderRequest {
    pub ticker_ids: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickerEntry {
    pub ticker_id: u64,
    pub content: String,
    pub expires_at: Option<i64>,
    pub sort_order: i64,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastRequest {
    pub level: BroadcastLevel,
    #[serde(default)]
    pub regions: Vec<String>,
    pub message: String,
    pub duration: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusBroadcast {
    pub broadcast_id: u64,
    pub level: BroadcastLevel,
    pub message: String,
    pub expires_at: i64,
    pub created_at: i64,
    #[serde(default)]
    pub regions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StatusBarState {
    #[serde(default)]
    pub tickers: Vec<TickerEntry>,
    #[serde(default)]
    pub broadcasts: Vec<StatusBroadcast>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SpyControlMessage {
    Init {
        cols: u16,
        rows: u16,
        dump: String,
    },
    Resize {
        cols: u16,
        rows: u16,
    },
    App {
        app_id: Option<String>,
        shortname: String,
    },
    Metadata {
        username: String,
    },
    Input {
        data: Bytes,
    },
    Idle {
        fuel_seconds: i32,
        paused: bool,
    },
    Closed {
        reason_slug: String,
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SpyClientMessage {
    SetIdlePaused { paused: bool },
    SetReadWrite { read_write: bool },
    Kick,
}

#[tarpc::service]
pub trait AdminControlRpc {
    async fn discover() -> Result<RegionDiscoveryResponse, RpcError>;
    async fn local_region_status() -> Result<RegionRuntimeStatus, RpcError>;
    async fn sessions() -> Result<Vec<SessionSummary>, RpcError>;
    async fn ban_ip_add(request: BanIpAddRequest) -> Result<BanIpAddResponse, RpcError>;
    async fn ban_ip_list() -> Result<Vec<BanEntry>, RpcError>;
    async fn cluster_kicked_ip_list(
        request: ClusterKickedIpListRequest,
    ) -> Result<ClusterKickedIpListResponse, RpcError>;
    async fn ban_ip_remove(request: BanIpRemoveRequest) -> Result<(), RpcError>;
    async fn apply_ban(request: BanIpRequest) -> Result<(), RpcError>;
    async fn apply_ban_remove(request: BanIpRemoveRequest) -> Result<(), RpcError>;
    async fn session_kick(request: KickSessionRequest) -> Result<(), RpcError>;
    async fn ticker_list() -> Result<Vec<TickerEntry>, RpcError>;
    async fn ticker_add(request: TickerAddRequest) -> Result<(), RpcError>;
    async fn ticker_remove(request: TickerRemoveRequest) -> Result<(), RpcError>;
    async fn ticker_reorder(request: TickerReorderRequest) -> Result<(), RpcError>;
    async fn broadcast(request: BroadcastRequest) -> Result<(), RpcError>;
    async fn status_bar_refresh() -> Result<(), RpcError>;
    async fn app_create(request: CreateAppRequest) -> Result<CreateAppResponse, RpcError>;
    async fn app_list() -> Result<Vec<AppSummary>, RpcError>;
    async fn app_rotate_token(
        request: RotateAppTokenRequest,
    ) -> Result<RotateAppTokenResponse, RpcError>;
    async fn app_delete(
        request: DeleteAppRequest,
    ) -> Result<Option<DeleteShortnameResponse>, RpcError>;
}

#[tarpc::service]
pub trait AppControlRpc {
    async fn self_info(request: AppSelfInfoRequest) -> Result<AppSelfInfoResponse, RpcError>;
    async fn env_list() -> Result<AppEnvListResponse, RpcError>;
    async fn env_set(request: AppEnvSetRequest) -> Result<(), RpcError>;
    async fn env_delete(request: AppEnvDeleteRequest) -> Result<(), RpcError>;
    async fn rotate_token() -> Result<RotateAppTokenResponse, RpcError>;
    async fn upload(request: UploadAppRequest) -> Result<UploadAppResponse, RpcError>;
    async fn delete_shortname(
        request: DeleteShortnameRequest,
    ) -> Result<Option<DeleteShortnameResponse>, RpcError>;
}

pub fn rpc_context() -> context::Context {
    context::current()
}
