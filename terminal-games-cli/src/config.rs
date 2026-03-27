// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    path::PathBuf,
};

use anyhow::{Context, Result};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use terminal_games::control::{AuthorTokenClaims, RegionDiscoveryResponse};
use unicode_width::UnicodeWidthStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminProfile {
    pub url: String,
    pub password: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AdminProfilesFile {
    #[serde(default)]
    profile: BTreeMap<String, AdminProfile>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CliConfig {
    #[serde(rename = "default-profile", default = "default_profile_name")]
    pub default_profile: String,
    #[serde(rename = "author-env-secret-key", default)]
    pub author_env_secret_key: Option<String>,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            default_profile: default_profile_name(),
            author_env_secret_key: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredAuthorToken {
    pub profile: String,
    pub token: String,
}

#[derive(Debug, Clone)]
pub struct StoredAuthorTokenEntry {
    pub profile: String,
    pub claims: AuthorTokenClaims,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AuthorTokensFile {
    #[serde(default)]
    token: BTreeMap<String, StoredAuthorToken>,
}

pub fn load_cli_config() -> Result<CliConfig> {
    read_toml(cli_config_path())
}

pub fn save_cli_config(config: &CliConfig) -> Result<()> {
    write_toml(cli_config_path(), config)
}

pub fn load_or_create_author_env_secret_key() -> Result<String> {
    let mut config = load_cli_config()?;
    if let Some(secret_key) = config
        .author_env_secret_key
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return Ok(secret_key.to_string());
    }
    let secret_key = generate_secret_key();
    config.author_env_secret_key = Some(secret_key.clone());
    save_cli_config(&config)?;
    Ok(secret_key)
}

pub fn resolve_admin_profile(profile_override: Option<&str>) -> Result<(String, AdminProfile)> {
    let config = load_cli_config()?;
    let profile_name = profile_override
        .map(str::to_string)
        .unwrap_or_else(|| config.default_profile.clone());
    let profiles: AdminProfilesFile = read_toml(admin_profiles_path())?;
    let profile = profiles
        .profile
        .get(&profile_name)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("unknown admin profile '{profile_name}'"))?;
    Ok((profile_name, profile))
}

pub fn save_admin_profile(name: &str, profile: AdminProfile) -> Result<()> {
    let mut profiles: AdminProfilesFile = read_toml(admin_profiles_path())?;
    profiles.profile.insert(name.to_string(), profile);
    write_toml(admin_profiles_path(), &profiles)
}

pub fn list_admin_profile_names() -> Result<Vec<String>> {
    let profiles: AdminProfilesFile = read_toml(admin_profiles_path())?;
    Ok(profiles.profile.into_keys().collect())
}

pub fn list_admin_profiles() -> Result<Vec<(String, AdminProfile)>> {
    let profiles: AdminProfilesFile = read_toml(admin_profiles_path())?;
    Ok(profiles.profile.into_iter().collect())
}

pub fn default_profile_name_value() -> Result<String> {
    Ok(load_cli_config()?.default_profile)
}

pub fn list_stored_author_tokens() -> Result<Vec<StoredAuthorTokenEntry>> {
    let tokens: AuthorTokensFile = read_toml(author_tokens_path())?;
    let mut entries = Vec::new();
    for (key, stored) in tokens.token {
        let claims = AuthorTokenClaims::decode(&stored.token)
            .with_context(|| format!("invalid stored author token '{key}'"))?;
        let profile = if stored.profile.trim().is_empty() {
            key.split_once(':')
                .map(|(profile, _)| profile.to_string())
                .unwrap_or_else(default_profile_name)
        } else {
            stored.profile
        };
        entries.push(StoredAuthorTokenEntry { profile, claims });
    }
    Ok(entries)
}

pub fn load_author_token_for_shortname(
    shortname: &str,
    profile_override: Option<&str>,
) -> Result<Option<AuthorTokenClaims>> {
    if let Ok(token) = std::env::var("TERMINAL_GAMES_AUTHOR_TOKEN") {
        let claims = AuthorTokenClaims::decode(token.trim())?;
        return Ok((claims.shortname == shortname).then_some(claims));
    }
    let profile_name = profile_override
        .map(str::to_string)
        .unwrap_or(default_profile_name_value()?);
    let matches = list_stored_author_tokens()?
        .into_iter()
        .filter(|entry| entry.claims.shortname == shortname)
        .collect::<Vec<_>>();
    if let Some(entry) = matches.iter().find(|entry| entry.profile == profile_name) {
        return Ok(Some(entry.claims.clone()));
    }
    if matches.is_empty() {
        return Ok(None);
    }
    let available = matches
        .into_iter()
        .map(|entry| entry.profile)
        .collect::<Vec<_>>()
        .join(", ");
    Err(anyhow::anyhow!(
        "no author token configured for '{}' in profile '{}'; available in: {}",
        shortname,
        profile_name,
        available
    ))
}

pub fn load_author_tokens_for_listing(
    profile_filter: Option<&str>,
) -> Result<Vec<StoredAuthorTokenEntry>> {
    let mut entries = list_stored_author_tokens()?;
    if let Ok(token) = std::env::var("TERMINAL_GAMES_AUTHOR_TOKEN") {
        let claims = AuthorTokenClaims::decode(token.trim())?;
        entries.insert(
            0,
            StoredAuthorTokenEntry {
                profile: "env".to_string(),
                claims,
            },
        );
    }
    if let Some(profile) = profile_filter {
        entries.retain(|entry| entry.profile == profile);
    }
    Ok(entries)
}

pub fn list_author_refs() -> Result<Vec<String>> {
    let mut refs = load_author_tokens_for_listing(None)?
        .into_iter()
        .map(|entry| format!("{}:{}", entry.profile, entry.claims.shortname))
        .collect::<Vec<_>>();
    refs.sort();
    refs.dedup();
    Ok(refs)
}

pub fn parse_author_ref(value: &str) -> Result<(String, String)> {
    let (profile, shortname) = value
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("author target must be PROFILE:SHORTNAME"))?;
    let profile = profile.trim();
    let shortname = shortname.trim();
    anyhow::ensure!(!profile.is_empty(), "author target profile cannot be empty");
    anyhow::ensure!(
        !shortname.is_empty(),
        "author target shortname cannot be empty"
    );
    Ok((profile.to_string(), shortname.to_string()))
}

pub fn save_author_token(profile: &str, claims: &AuthorTokenClaims) -> Result<()> {
    let mut tokens: AuthorTokensFile = read_toml(author_tokens_path())?;
    tokens.token.insert(
        author_token_key(profile, claims),
        StoredAuthorToken {
            profile: profile.to_string(),
            token: claims.encode()?,
        },
    );
    write_toml(author_tokens_path(), &tokens)
}

pub fn normalize_base_url(input: &str) -> Result<String> {
    let trimmed = input.trim();
    anyhow::ensure!(!trimmed.is_empty(), "url cannot be empty");
    let with_scheme = if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        format!("https://{trimmed}")
    };
    let mut url =
        reqwest::Url::parse(&with_scheme).with_context(|| format!("invalid url '{trimmed}'"))?;
    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.to_string().trim_end_matches('/').to_string())
}

pub fn derive_region_urls(
    base_url: &str,
    discovery: &RegionDiscoveryResponse,
) -> Result<BTreeMap<String, String>> {
    let base = reqwest::Url::parse(base_url)?;
    let scheme = base.scheme().to_string();
    let host = base
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("profile url is missing a host"))?;
    let port = base.port();

    let mut base_host = host.to_string();
    if let Some((first, rest)) = host.split_once('.')
        && discovery.regions.iter().any(|region| region == first)
    {
        base_host = rest.to_string();
    }

    let mut urls = BTreeMap::new();
    for region in &discovery.regions {
        let region_host = if region == &discovery.current_region && host == base_host {
            base_host.clone()
        } else if region == &discovery.current_region
            && host != base_host
            && host.starts_with(region)
        {
            host.to_string()
        } else {
            format!("{region}.{base_host}")
        };
        let mut url = reqwest::Url::parse(&format!("{scheme}://{region_host}"))?;
        if let Some(port) = port {
            url.set_port(Some(port))
                .map_err(|_| anyhow::anyhow!("invalid port {port}"))?;
        }
        urls.insert(
            region.clone(),
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

fn visible_width(value: &str) -> usize {
    strip_ansi_escapes::strip_str(value).width()
}

fn pad_visible(value: &str, width: usize) -> String {
    let padding = width.saturating_sub(visible_width(value));
    format!("{value}{}", " ".repeat(padding))
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

fn default_profile_name() -> String {
    "default".to_string()
}

fn config_dir() -> Result<PathBuf> {
    let base = dirs::config_dir().ok_or_else(|| anyhow::anyhow!("config dir not found"))?;
    let path = base.join("terminal-games-cli");
    fs::create_dir_all(&path)?;
    Ok(path)
}

fn cli_config_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("config.toml"))
}

fn admin_profiles_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("admin.toml"))
}

fn author_tokens_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("author.toml"))
}

fn author_token_key(profile: &str, claims: &AuthorTokenClaims) -> String {
    format!("{profile}:{}", claims.shortname)
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
