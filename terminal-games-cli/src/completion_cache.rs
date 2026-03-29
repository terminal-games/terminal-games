// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use terminal_games::control::{AuthorSummary, BanEntry, SessionSummary, TickerEntry};

use crate::config::config_dir;

const CACHE_VERSION: u32 = 1;
const COMPLETION_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TimedValue<T> {
    fetched_at_unix_ms: u64,
    value: T,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct CompletionCacheFile {
    #[serde(default = "cache_version")]
    version: u32,
    #[serde(default)]
    sessions: BTreeMap<String, TimedValue<Vec<SessionSummary>>>,
    #[serde(default)]
    authors: BTreeMap<String, TimedValue<Vec<AuthorSummary>>>,
    #[serde(default)]
    tickers: BTreeMap<String, TimedValue<Vec<TickerEntry>>>,
    #[serde(default)]
    bans: BTreeMap<String, TimedValue<Vec<BanEntry>>>,
}

pub fn load_sessions(url: &str) -> Result<Option<Vec<SessionSummary>>> {
    load_value(url, |cache| &cache.sessions)
}

pub fn store_sessions(url: &str, sessions: &[SessionSummary]) -> Result<()> {
    store_value(url, sessions.to_vec(), |cache| &mut cache.sessions)
}

pub fn load_authors(url: &str) -> Result<Option<Vec<AuthorSummary>>> {
    load_value(url, |cache| &cache.authors)
}

pub fn store_authors(url: &str, authors: &[AuthorSummary]) -> Result<()> {
    store_value(url, authors.to_vec(), |cache| &mut cache.authors)
}

pub fn load_tickers(url: &str) -> Result<Option<Vec<TickerEntry>>> {
    load_value(url, |cache| &cache.tickers)
}

pub fn store_tickers(url: &str, tickers: &[TickerEntry]) -> Result<()> {
    store_value(url, tickers.to_vec(), |cache| &mut cache.tickers)
}

pub fn load_bans(url: &str) -> Result<Option<Vec<BanEntry>>> {
    load_value(url, |cache| &cache.bans)
}

pub fn store_bans(url: &str, bans: &[BanEntry]) -> Result<()> {
    store_value(url, bans.to_vec(), |cache| &mut cache.bans)
}

fn load_value<T>(
    url: &str,
    access: impl FnOnce(&CompletionCacheFile) -> &BTreeMap<String, TimedValue<T>>,
) -> Result<Option<T>>
where
    T: Clone,
{
    let cache = read_cache_file()?;
    let Some(entry) = access(&cache).get(url) else {
        return Ok(None);
    };
    if !is_fresh(entry.fetched_at_unix_ms, now_unix_ms()) {
        return Ok(None);
    }
    Ok(Some(entry.value.clone()))
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

fn is_fresh(fetched_at_unix_ms: u64, now_unix_ms: u64) -> bool {
    now_unix_ms.saturating_sub(fetched_at_unix_ms) <= COMPLETION_TTL.as_millis() as u64
}

fn cache_path() -> Result<std::path::PathBuf> {
    Ok(config_dir()?.join("completion-cache.toml"))
}

fn read_cache_file() -> Result<CompletionCacheFile> {
    let path = cache_path()?;
    if !path.exists() {
        return Ok(CompletionCacheFile::default());
    }
    let contents =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let cache: CompletionCacheFile =
        toml::from_str(&contents).with_context(|| format!("failed to parse {}", path.display()))?;
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
