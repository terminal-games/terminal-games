// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    path::PathBuf,
};

use anyhow::{Context, Result};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use terminal_games::control::{AppTokenClaims, NodeDiscoveryResponse, StaleImport};
use unicode_width::UnicodeWidthStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminProfile {
    pub url: String,
    pub password: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AdminProfilesFile {
    #[serde(default)]
    server: Vec<AdminProfile>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CliConfig {
    #[serde(rename = "default-url", default)]
    pub default_url: Option<String>,
    #[serde(rename = "app-env-secret-key", default)]
    pub app_env_secret_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredAppToken {
    pub token: String,
}

#[derive(Debug, Clone)]
pub struct StoredAppTokenEntry {
    pub claims: AppTokenClaims,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AppTokensFile {
    #[serde(default)]
    token: Vec<StoredAppToken>,
}

pub fn load_cli_config() -> Result<CliConfig> {
    read_toml(cli_config_path())
}

pub fn save_cli_config(config: &CliConfig) -> Result<()> {
    write_toml(cli_config_path(), config)
}

pub fn load_or_create_app_env_secret_key() -> Result<String> {
    let mut config = load_cli_config()?;
    if let Some(secret_key) = config
        .app_env_secret_key
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return Ok(secret_key.to_string());
    }
    let secret_key = generate_secret_key();
    config.app_env_secret_key = Some(secret_key.clone());
    save_cli_config(&config)?;
    Ok(secret_key)
}

pub fn resolve_admin_profile(url_override: Option<&str>) -> Result<AdminProfile> {
    let url = resolve_target_url(url_override)?;
    let profiles: AdminProfilesFile = read_toml(admin_profiles_path())?;
    profiles
        .server
        .into_iter()
        .filter_map(|profile| normalize_admin_profile(profile).ok())
        .find(|profile| profile.url == url)
        .ok_or_else(|| anyhow::anyhow!("no admin auth configured for '{}'", url))
}

pub fn save_admin_profile(profile: AdminProfile) -> Result<()> {
    let profile = normalize_admin_profile(profile)?;
    let mut profiles: AdminProfilesFile = read_toml(admin_profiles_path())?;
    if let Some(existing) = profiles.server.iter_mut().find(|existing| {
        normalize_base_url(&existing.url)
            .map(|url| url == profile.url)
            .unwrap_or(false)
    }) {
        *existing = profile;
    } else {
        profiles.server.push(profile);
        profiles
            .server
            .sort_by(|left, right| left.url.cmp(&right.url));
    }
    write_toml(admin_profiles_path(), &profiles)
}

pub fn list_admin_urls() -> Result<Vec<String>> {
    let mut urls = read_toml::<AdminProfilesFile>(admin_profiles_path())?
        .server
        .into_iter()
        .filter_map(|profile| {
            normalize_admin_profile(profile)
                .ok()
                .map(|profile| profile.url)
        })
        .collect::<Vec<_>>();
    urls.sort();
    urls.dedup();
    Ok(urls)
}

pub fn default_url_value() -> Result<Option<String>> {
    normalize_optional_url(load_cli_config()?.default_url.as_deref())
}

pub fn list_stored_app_tokens() -> Result<Vec<StoredAppTokenEntry>> {
    let tokens: AppTokensFile = read_toml(app_tokens_path())?;
    tokens
        .token
        .into_iter()
        .map(|stored| {
            let claims = decode_app_token(&stored.token)?;
            Ok(StoredAppTokenEntry { claims })
        })
        .collect()
}

pub fn load_app_tokens_for_shortname(
    shortname: &str,
    url_override: Option<&str>,
) -> Result<Vec<AppTokenClaims>> {
    let requested_url = normalize_optional_url(url_override)?;
    let entries = load_all_app_token_entries()?;
    select_app_claims(
        &entries,
        shortname,
        requested_url.as_deref(),
        default_url_value()?.as_deref(),
    )
}

pub fn load_app_tokens_for_listing(url_filter: Option<&str>) -> Result<Vec<StoredAppTokenEntry>> {
    let filter_url = normalize_optional_url(url_filter)?;
    let mut entries = load_all_app_token_entries()?;
    if let Some(filter_url) = filter_url {
        entries.retain(|entry| entry.claims.url == filter_url);
    }
    Ok(entries)
}

pub fn list_app_shortnames(url_filter: Option<&str>) -> Result<Vec<String>> {
    let mut shortnames = load_app_tokens_for_listing(url_filter)?
        .into_iter()
        .map(|entry| entry.claims.shortname)
        .collect::<Vec<_>>();
    shortnames.sort();
    shortnames.dedup();
    Ok(shortnames)
}

pub fn list_app_urls() -> Result<Vec<String>> {
    let mut urls = load_app_tokens_for_listing(None)?
        .into_iter()
        .map(|entry| entry.claims.url)
        .collect::<Vec<_>>();
    urls.sort();
    urls.dedup();
    Ok(urls)
}

pub fn format_imports(imports: &[String], stale_imports: &[StaleImport]) -> String {
    if imports.is_empty() {
        return "-".to_string();
    }
    let stale_imports = stale_imports
        .iter()
        .map(|stale| (stale.import.as_str(), stale.latest_import.as_str()))
        .collect::<BTreeMap<_, _>>();
    imports
        .iter()
        .map(|import| {
            if let Some(latest) = stale_imports.get(import.as_str()) {
                format!("{import} [old, latest: {latest}]")
            } else {
                import.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn save_app_token(claims: &AppTokenClaims) -> Result<()> {
    let normalized = normalize_app_claims(claims.clone())?;
    let mut tokens: AppTokensFile = read_toml(app_tokens_path())?;
    let encoded = normalized.encode()?;
    tokens.token.retain(|stored| {
        decode_app_token(&stored.token)
            .map(|claims| claims.url != normalized.url || claims.shortname != normalized.shortname)
            .unwrap_or(true)
    });
    tokens.token.push(StoredAppToken { token: encoded });
    tokens
        .token
        .sort_by(|left, right| left.token.cmp(&right.token));
    write_toml(app_tokens_path(), &tokens)
}

pub fn normalize_base_url(input: &str) -> Result<String> {
    let trimmed = input.trim();
    anyhow::ensure!(!trimmed.is_empty(), "url cannot be empty");
    anyhow::ensure!(
        trimmed.contains("://"),
        "url must include an explicit scheme, for example http://localhost:8080 or https://terminalgames.net"
    );
    let mut url =
        reqwest::Url::parse(trimmed).with_context(|| format!("invalid url '{trimmed}'"))?;
    anyhow::ensure!(
        matches!(url.scheme(), "http" | "https"),
        "url scheme must be http:// or https://"
    );
    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.to_string().trim_end_matches('/').to_string())
}

pub fn derive_node_urls(
    base_url: &str,
    discovery: &NodeDiscoveryResponse,
) -> Result<BTreeMap<String, String>> {
    let base = reqwest::Url::parse(base_url)?;
    let scheme = base.scheme().to_string();
    let host = base
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("profile url is missing a host"))?;
    let port = base.port();

    let mut base_host = host.to_string();
    if let Some((first, rest)) = host.split_once('.')
        && discovery.nodes.iter().any(|node| node == first)
    {
        base_host = rest.to_string();
    }

    let mut urls = BTreeMap::new();
    for node in &discovery.nodes {
        let node_host = if node == &discovery.current_node && host == base_host {
            base_host.clone()
        } else if node == &discovery.current_node && host != base_host && host.starts_with(node) {
            host.to_string()
        } else {
            format!("{node}.{base_host}")
        };
        let mut url = reqwest::Url::parse(&format!("{scheme}://{node_host}"))?;
        if let Some(port) = port {
            url.set_port(Some(port))
                .map_err(|_| anyhow::anyhow!("invalid port {port}"))?;
        }
        urls.insert(
            node.clone(),
            url.to_string().trim_end_matches('/').to_string(),
        );
    }
    Ok(urls)
}

pub fn read_secret_stdin() -> Result<String> {
    let mut buf = String::new();
    io::stdin().read_to_string(&mut buf)?;
    Ok(buf.trim().to_string())
}

pub fn print_table(headers: &[&str], rows: &[Vec<String>]) {
    let mut widths = headers
        .iter()
        .map(|header| visible_width(header))
        .collect::<Vec<_>>();
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            if index >= widths.len() {
                widths.push(visible_width(cell));
            } else {
                widths[index] = widths[index].max(visible_width(cell));
            }
        }
    }

    let header = headers
        .iter()
        .enumerate()
        .map(|(index, cell)| pad_visible(cell, widths[index]))
        .collect::<Vec<_>>()
        .join("  ");
    println!("{header}");
    println!(
        "{}",
        widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>()
            .join("  ")
    );
    for row in rows {
        println!(
            "{}",
            row.iter()
                .enumerate()
                .map(|(index, cell)| pad_visible(cell, widths[index]))
                .collect::<Vec<_>>()
                .join("  ")
        );
    }
}

pub fn format_duration(seconds: u64) -> String {
    let hours = seconds / 3600;
    let minutes = (seconds / 60) % 60;
    let secs = seconds % 60;
    format!("{hours:02}:{minutes:02}:{secs:02}")
}

pub fn format_seconds(seconds: f64) -> String {
    format_duration(seconds.max(0.0) as u64)
}

pub fn format_bytes_per_second(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B/s", "KiB/s", "MiB/s", "GiB/s"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

pub(crate) fn config_dir() -> Result<PathBuf> {
    let base = dirs::config_dir().ok_or_else(|| anyhow::anyhow!("config dir not found"))?;
    let path = base.join("terminal-games-cli");
    fs::create_dir_all(&path)?;
    Ok(path)
}

fn resolve_target_url(url_override: Option<&str>) -> Result<String> {
    if let Some(url) = normalize_optional_url(url_override)? {
        return Ok(url);
    }
    default_url_value()?.ok_or_else(|| {
        anyhow::anyhow!(
            "no default server configured; pass --profile or run 'terminal-games-cli admin auth'"
        )
    })
}

fn select_app_claims(
    entries: &[StoredAppTokenEntry],
    shortname: &str,
    requested_url: Option<&str>,
    default_url: Option<&str>,
) -> Result<Vec<AppTokenClaims>> {
    let matches = entries
        .iter()
        .filter(|entry| entry.claims.shortname == shortname)
        .collect::<Vec<_>>();
    if matches.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(requested_url) = requested_url {
        let matches = matches
            .into_iter()
            .filter(|entry| entry.claims.url == requested_url)
            .map(|entry| entry.claims.clone())
            .collect::<Vec<_>>();
        return if matches.is_empty() {
            Err(anyhow::anyhow!(
                "no app token configured for '{}' on '{}'; available on: {}",
                shortname,
                requested_url,
                available_app_urls(
                    &entries
                        .iter()
                        .filter(|entry| entry.claims.shortname == shortname)
                        .collect::<Vec<_>>()
                )
            ))
        } else {
            Ok(matches)
        };
    }

    let urls = matches
        .iter()
        .map(|entry| entry.claims.url.as_str())
        .collect::<BTreeSet<_>>();
    if urls.len() == 1 {
        return Ok(matches.iter().map(|entry| entry.claims.clone()).collect());
    }
    if let Some(default_url) = default_url {
        let matches = matches
            .iter()
            .filter(|entry| entry.claims.url == default_url)
            .map(|entry| entry.claims.clone())
            .collect::<Vec<_>>();
        if !matches.is_empty() {
            return Ok(matches);
        }
    }
    Err(anyhow::anyhow!(
        "multiple app tokens are configured for '{}'; pass --profile to choose one of: {}",
        shortname,
        available_app_urls(&matches)
    ))
}

fn available_app_urls(entries: &[&StoredAppTokenEntry]) -> String {
    entries
        .iter()
        .map(|entry| entry.claims.url.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(", ")
}

fn load_all_app_token_entries() -> Result<Vec<StoredAppTokenEntry>> {
    let mut entries = list_stored_app_tokens()?;
    if let Some(claims) = load_env_app_token()? {
        entries.insert(0, StoredAppTokenEntry { claims });
    }
    Ok(entries)
}

fn load_env_app_token() -> Result<Option<AppTokenClaims>> {
    let author_token = std::env::var("TERMINAL_GAMES_AUTHOR_TOKEN");
    let app_token = std::env::var("TERMINAL_GAMES_APP_TOKEN");
    match (author_token, app_token) {
        (Ok(author_token), Ok(app_token)) => {
            anyhow::ensure!(
                author_token.trim() == app_token.trim(),
                "both TERMINAL_GAMES_AUTHOR_TOKEN and TERMINAL_GAMES_APP_TOKEN are set with different values"
            );
            Ok(Some(decode_app_token(author_token.trim())?))
        }
        (Ok(token), Err(std::env::VarError::NotPresent))
        | (Err(std::env::VarError::NotPresent), Ok(token)) => {
            Ok(Some(decode_app_token(token.trim())?))
        }
        (Err(std::env::VarError::NotPresent), Err(std::env::VarError::NotPresent)) => Ok(None),
        (Err(error), _) | (_, Err(error)) => Err(anyhow::Error::new(error)),
    }
}

fn decode_app_token(token: &str) -> Result<AppTokenClaims> {
    normalize_app_claims(AppTokenClaims::decode(token)?)
}

fn normalize_admin_profile(mut profile: AdminProfile) -> Result<AdminProfile> {
    profile.url = normalize_base_url(&profile.url)?;
    Ok(profile)
}

fn normalize_app_claims(mut claims: AppTokenClaims) -> Result<AppTokenClaims> {
    claims.url = normalize_base_url(&claims.url)?;
    Ok(claims)
}

fn normalize_optional_url(input: Option<&str>) -> Result<Option<String>> {
    input
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(normalize_base_url)
        .transpose()
}

fn cli_config_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("config.toml"))
}

fn admin_profiles_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("admin.toml"))
}

fn app_tokens_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("app.toml"))
}

fn visible_width(value: &str) -> usize {
    strip_ansi_escapes::strip_str(value).width()
}

fn pad_visible(value: &str, width: usize) -> String {
    let padding = width.saturating_sub(visible_width(value));
    format!("{value}{}", " ".repeat(padding))
}

fn read_toml<T>(path: Result<PathBuf>) -> Result<T>
where
    T: Default + for<'de> Deserialize<'de>,
{
    let path = path?;
    if !path.exists() {
        return Ok(T::default());
    }
    let contents =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    toml::from_str(&contents).with_context(|| format!("failed to parse {}", path.display()))
}

fn write_toml<T>(path: Result<PathBuf>, value: &T) -> Result<()>
where
    T: Serialize,
{
    let path = path?;
    let contents = toml::to_string_pretty(value)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&path)
            .with_context(|| format!("failed to write {}", path.display()))?;
        file.write_all(contents.as_bytes())
            .with_context(|| format!("failed to write {}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("failed to write {}", path.display()))?;
        file.write_all(contents.as_bytes())
            .with_context(|| format!("failed to write {}", path.display()))?;
    }
    Ok(())
}

fn generate_secret_key() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}
