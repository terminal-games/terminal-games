// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

const AUTHOR_TOKEN_PREFIX: &str = "tga1.";
const AUTHOR_TOKEN_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthorTokenClaims {
    pub version: u32,
    pub url: String,
    pub shortname: String,
    pub secret: String,
}

impl AuthorTokenClaims {
    pub fn new(url: String, shortname: String, secret: String) -> Self {
        Self {
            version: AUTHOR_TOKEN_VERSION,
            url,
            shortname,
            secret,
        }
    }

    pub fn encode(&self) -> anyhow::Result<String> {
        anyhow::ensure!(
            self.version == AUTHOR_TOKEN_VERSION,
            "unsupported author token version {}",
            self.version
        );
        let json = serde_json::to_vec(self)?;
        Ok(format!(
            "{AUTHOR_TOKEN_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(json)
        ))
    }

    pub fn decode(token: &str) -> anyhow::Result<Self> {
        let encoded = token
            .strip_prefix(AUTHOR_TOKEN_PREFIX)
            .ok_or_else(|| anyhow::anyhow!("invalid author token prefix"))?;
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|error| anyhow::anyhow!("invalid author token encoding: {error}"))?;
        let claims = serde_json::from_slice::<Self>(&bytes)?;
        anyhow::ensure!(
            claims.version == AUTHOR_TOKEN_VERSION,
            "unsupported author token version {}",
            claims.version
        );
        Ok(claims)
    }
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BanIpAddRequest {
    pub ip: String,
    pub reason: String,
    pub duration: Option<String>,
    pub expires_at: Option<String>,
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
pub struct KickSessionRequest {
    pub local_session_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAuthorRequest {
    pub shortname: String,
    pub base_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorSummary {
    pub author_id: u64,
    pub author_name: String,
    pub shortname: String,
    pub playtime_seconds: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAuthorResponse {
    pub author: AuthorSummary,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteAuthorRequest {
    pub author_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RotateAuthorTokenRequest {
    pub author_id: u64,
    pub base_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RotateAuthorTokenResponse {
    pub author: AuthorSummary,
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
pub struct UploadGameResponse {
    pub shortname: String,
    pub version: u64,
    pub game_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthorEnvVar {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorEnvListResponse {
    pub shortname: String,
    pub envs: Vec<AuthorEnvVar>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorEnvSetRequest {
    pub envs: Vec<AuthorEnvVar>,
    #[serde(default)]
    pub replace: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorEnvDeleteRequest {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorSelfResponse {
    pub author_id: u64,
    pub author_name: String,
    pub shortname: String,
    pub server: String,
    pub playtime_seconds: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheInvalidateRequest {
    pub shortname: String,
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
pub struct TickerEntry {
    pub ticker_id: u64,
    pub content: String,
    pub expires_at: Option<i64>,
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
    Init { cols: u16, rows: u16, dump: String },
    Resize { cols: u16, rows: u16 },
    Metadata { username: String },
    Input { data: Vec<u8> },
    Closed { reason_slug: String, message: String },
}
