// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use serde::{Deserialize, Serialize};

use crate::{config::config_dir, control_client::AdminClient};

const CACHE_VERSION: u32 = 2;
const COMPLETION_REFRESH_AGE: Duration = Duration::from_secs(5);
const REFRESH_LOCK_TTL: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TimedValue<T> {
    fetched_at_unix_ms: u64,
    value: T,
}

#[derive(Debug, Serialize, Deserialize)]
struct CompletionCacheFile {
    #[serde(default = "cache_version")]
    version: u32,
    #[serde(default)]
    sessions: BTreeMap<String, TimedValue<Vec<String>>>,
    #[serde(default)]
    apps: BTreeMap<String, TimedValue<Vec<String>>>,
    #[serde(default)]
    tickers: BTreeMap<String, TimedValue<Vec<String>>>,
    #[serde(default)]
    bans: BTreeMap<String, TimedValue<Vec<String>>>,
}

impl Default for CompletionCacheFile {
    fn default() -> Self {
        Self {
            version: CACHE_VERSION,
            sessions: BTreeMap::new(),
            apps: BTreeMap::new(),
            tickers: BTreeMap::new(),
            bans: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CacheLookup<T> {
    Missing,
    Present { value: T, needs_refresh: bool },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum RefreshKind {
    Sessions,
    Apps,
    Tickers,
    Bans,
}

impl RefreshKind {
    fn as_arg(self) -> &'static str {
        match self {
            Self::Sessions => "sessions",
            Self::Apps => "apps",
            Self::Tickers => "tickers",
            Self::Bans => "bans",
        }
    }
}

#[derive(Args)]
pub struct RefreshCli {
    #[arg(value_enum)]
    kind: RefreshKind,
}

pub async fn run_refresh(cli: RefreshCli, profile: Option<String>) -> Result<()> {
    let url = profile
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("completion-refresh requires --profile"))?;
    let _guard = RefreshLockGuard::new(cli.kind, url.to_string());
    let api = AdminClient::load(Some(url))?;
    match cli.kind {
        RefreshKind::Sessions => {
            api.all_sessions().await?;
        }
        RefreshKind::Apps => {
            api.app_list().await?;
        }
        RefreshKind::Tickers => {
            api.ticker_list().await?;
        }
        RefreshKind::Bans => {
            api.ban_ip_list().await?;
        }
    }
    Ok(())
}

pub fn spawn_admin_refresh(kind: RefreshKind, url: &str) -> Result<()> {
    if !claim_refresh_lock(kind, url)? {
        return Ok(());
    }
    let current_exe = std::env::current_exe().context("failed to resolve current executable")?;
    match std::process::Command::new(current_exe)
        .arg("--profile")
        .arg(url)
        .arg("completion-refresh")
        .arg(kind.as_arg())
        .env_remove("COMPLETE")
        .env_remove("_CLAP_COMPLETE_INDEX")
        .env_remove("_CLAP_COMPLETE_COMP_TYPE")
        .env_remove("_CLAP_COMPLETE_SPACE")
        .env_remove("_CLAP_IFS")
        .env_remove("COMP_WORDS")
        .env_remove("COMP_CWORD")
        .env_remove("COMP_TYPE")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(_) => Ok(()),
        Err(error) => {
            let _ = clear_refresh_lock(kind, url);
            Err(anyhow::Error::new(error))
        }
    }
}

pub fn load_sessions(url: &str) -> Result<CacheLookup<Vec<String>>> {
    load_value(url, |cache| &cache.sessions)
}

pub fn store_sessions(url: &str, sessions: &[String]) -> Result<()> {
    store_value(url, sessions.to_vec(), |cache| &mut cache.sessions)
}

pub fn load_apps(url: &str) -> Result<CacheLookup<Vec<String>>> {
    load_value(url, |cache| &cache.apps)
}

pub fn store_apps(url: &str, apps: &[String]) -> Result<()> {
    store_value(url, apps.to_vec(), |cache| &mut cache.apps)
}

pub fn load_tickers(url: &str) -> Result<CacheLookup<Vec<String>>> {
    load_value(url, |cache| &cache.tickers)
}

pub fn store_tickers(url: &str, tickers: &[String]) -> Result<()> {
    store_value(url, tickers.to_vec(), |cache| &mut cache.tickers)
}

pub fn load_bans(url: &str) -> Result<CacheLookup<Vec<String>>> {
    load_value(url, |cache| &cache.bans)
}

pub fn store_bans(url: &str, bans: &[String]) -> Result<()> {
    store_value(url, bans.to_vec(), |cache| &mut cache.bans)
}

fn load_value<T>(
    url: &str,
    access: impl FnOnce(&CompletionCacheFile) -> &BTreeMap<String, TimedValue<T>>,
) -> Result<CacheLookup<T>>
where
    T: Clone,
{
    let cache = read_cache_file()?;
    let Some(entry) = access(&cache).get(url) else {
        return Ok(CacheLookup::Missing);
    };
    Ok(CacheLookup::Present {
        value: entry.value.clone(),
        needs_refresh: should_refresh(entry.fetched_at_unix_ms, now_unix_ms()),
    })
}

fn store_value<T>(
    url: &str,
    value: T,
    access: impl FnOnce(&mut CompletionCacheFile) -> &mut BTreeMap<String, TimedValue<T>>,
) -> Result<()>
where
    T: Clone + Serialize + for<'de> Deserialize<'de>,
{
    let mut cache = read_cache_file()?;
    cache.version = CACHE_VERSION;
    access(&mut cache).insert(
        url.to_string(),
        TimedValue {
            fetched_at_unix_ms: now_unix_ms(),
            value,
        },
    );
    write_cache_file(&cache)
}

fn cache_version() -> u32 {
    CACHE_VERSION
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn should_refresh(fetched_at_unix_ms: u64, now_unix_ms: u64) -> bool {
    now_unix_ms.saturating_sub(fetched_at_unix_ms) >= COMPLETION_REFRESH_AGE.as_millis() as u64
}

fn cache_path() -> Result<std::path::PathBuf> {
    Ok(config_dir()?.join("completion-cache.toml"))
}

fn refresh_lock_path(kind: RefreshKind, url: &str) -> Result<std::path::PathBuf> {
    let dir = config_dir()?.join("completion-refresh");
    fs::create_dir_all(&dir)?;
    Ok(dir.join(format!("{}-{}.lock", kind.as_arg(), hex_key(url))))
}

fn claim_refresh_lock(kind: RefreshKind, url: &str) -> Result<bool> {
    let path = refresh_lock_path(kind, url)?;
    let now = now_unix_ms();
    if let Ok(contents) = fs::read_to_string(&path)
        && let Ok(started_at) = contents.trim().parse::<u64>()
        && now.saturating_sub(started_at) <= REFRESH_LOCK_TTL.as_millis() as u64
    {
        return Ok(false);
    }
    let _ = fs::remove_file(&path);
    match OpenOptions::new().create_new(true).write(true).open(&path) {
        Ok(mut file) => {
            write!(file, "{now}")?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(anyhow::Error::new(error)),
    }
}

fn clear_refresh_lock(kind: RefreshKind, url: &str) -> Result<()> {
    let path = refresh_lock_path(kind, url)?;
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(anyhow::Error::new(error)),
    }
}

fn hex_key(value: &str) -> String {
    let mut out = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

struct RefreshLockGuard {
    kind: RefreshKind,
    url: String,
}

impl RefreshLockGuard {
    fn new(kind: RefreshKind, url: String) -> Self {
        Self { kind, url }
    }
}

impl Drop for RefreshLockGuard {
    fn drop(&mut self) {
        let _ = clear_refresh_lock(self.kind, &self.url);
    }
}

fn read_cache_file() -> Result<CompletionCacheFile> {
    let path = cache_path()?;
    if !path.exists() {
        return Ok(CompletionCacheFile::default());
    }
    let contents =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let cache = match toml::from_str::<CompletionCacheFile>(&contents) {
        Ok(cache) => cache,
        Err(_) => return Ok(CompletionCacheFile::default()),
    };
    if cache.version != CACHE_VERSION {
        return Ok(CompletionCacheFile::default());
    }
    Ok(cache)
}

fn write_cache_file(cache: &CompletionCacheFile) -> Result<()> {
    let path = cache_path()?;
    let contents = toml::to_string_pretty(cache)?;
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
