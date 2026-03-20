// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod admission;
mod cluster_detection;
mod metrics;
mod ssh;
mod web;

use std::{
    collections::HashMap,
    fs,
    net::IpAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde::Deserialize;
use wasmparser::{Parser, Payload};

use crate::admission::AdmissionConfig;
use terminal_games::{
    app::AppServer,
    mesh::{EnvDiscovery, Mesh},
};

// Avoid musl's default allocator due to lackluster performance
// https://nickb.dev/blog/default-musl-allocator-considered-harmful-to-performance
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

async fn upsert_game(
    conn: &libsql::Connection,
    shortname: &str,
    path: &str,
    details_json: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO games (shortname, path, details) VALUES (?1, ?2, json(?3)) ON CONFLICT(shortname) DO UPDATE SET path = excluded.path, details = excluded.details",
        libsql::params!(shortname, path, details_json),
    )
    .await?;
    Ok(())
}

#[derive(Deserialize)]
struct TerminalGamesManifest {
    terminal_games_manifest_version: u32,
    shortname: String,
    details: serde_json::Value,
}

const MANIFEST_VERSION: u32 = 1;
const MANIFEST_MARKER: &[u8] = br#""terminal_games_manifest_version""#;

fn find_subslice(haystack: &[u8], needle: &[u8], start_at: usize) -> Option<usize> {
    if needle.is_empty() || start_at >= haystack.len() {
        return None;
    }
    haystack[start_at..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|idx| start_at + idx)
}

fn extract_manifest_from_blob(bytes: &[u8]) -> Option<TerminalGamesManifest> {
    let mut marker_search_from = 0usize;
    while let Some(marker_pos) = find_subslice(bytes, MANIFEST_MARKER, marker_search_from) {
        marker_search_from = marker_pos + MANIFEST_MARKER.len();
        let scan_start = marker_pos.saturating_sub(128 * 1024);
        for start in (scan_start..=marker_pos).rev() {
            if bytes[start] != b'{' {
                continue;
            }
            let mut stream = serde_json::Deserializer::from_slice(&bytes[start..])
                .into_iter::<TerminalGamesManifest>();
            let Some(Ok(manifest)) = stream.next() else {
                continue;
            };
            let consumed = stream.byte_offset();
            if start + consumed <= marker_pos {
                continue;
            };
            if manifest.terminal_games_manifest_version != MANIFEST_VERSION {
                continue;
            }
            if manifest.shortname.trim().is_empty() {
                continue;
            }
            return Some(manifest);
        }
    }
    None
}

fn extract_manifest_from_wasm(bytes: &[u8]) -> Result<Option<TerminalGamesManifest>> {
    for payload in Parser::new(0).parse_all(bytes) {
        match payload? {
            Payload::CustomSection(section) => {
                if let Some(manifest) = extract_manifest_from_blob(section.data()) {
                    return Ok(Some(manifest));
                }
            }
            Payload::DataSection(reader) => {
                for data in reader {
                    let data = data?;
                    if let Some(manifest) = extract_manifest_from_blob(data.data) {
                        return Ok(Some(manifest));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(None)
}

fn collect_wasm_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            collect_wasm_files(&path, out)?;
            continue;
        }
        if file_type.is_file() && path.extension().is_some_and(|ext| ext == "wasm") {
            out.push(path);
        }
    }
    Ok(())
}

async fn sync_games_from_embedded_manifests(conn: &libsql::Connection) -> Result<usize> {
    let cwd = std::env::current_dir().context("failed to resolve current directory")?;
    let mut wasm_files = Vec::new();
    collect_wasm_files(&cwd, &mut wasm_files)?;
    wasm_files.sort();

    let mut upserted = 0usize;
    for wasm_path in wasm_files {
        let bytes = fs::read(&wasm_path)
            .with_context(|| format!("failed to read {}", wasm_path.display()))?;
        let Some(manifest) = extract_manifest_from_wasm(&bytes)
            .with_context(|| format!("failed to parse wasm {}", wasm_path.display()))?
        else {
            continue;
        };
        let details_json = serde_json::to_string(&manifest.details)
            .context("failed to serialize manifest details")?;
        let db_path = wasm_path
            .strip_prefix(&cwd)
            .unwrap_or(&wasm_path)
            .to_string_lossy()
            .to_string();
        upsert_game(conn, &manifest.shortname, &db_path, &details_json).await?;
        upserted += 1;
        tracing::info!(shortname = %manifest.shortname, path = %db_path, "Upserted game from wasm manifest");
    }
    Ok(upserted)
}

async fn load_ip_ban_updates(
    conn: &libsql::Connection,
    since: Option<i64>,
) -> Result<(Vec<(IpAddr, Option<i64>)>, i64)> {
    let mut rows = match since {
        Some(last_inserted_at) => {
            conn.query(
                "SELECT ip, COALESCE(expires_at, -1), inserted_at FROM ip_bans WHERE inserted_at > ?1 ORDER BY inserted_at ASC",
                libsql::params!(last_inserted_at),
            )
            .await
        }
        None => {
            conn.query(
                "SELECT ip, COALESCE(expires_at, -1), inserted_at FROM ip_bans ORDER BY inserted_at ASC",
                (),
            )
            .await
        }
    }
    .context("failed to query ip bans")?;
    let mut updates = Vec::new();
    let mut newest_inserted_at = since.unwrap_or(0);
    while let Some(row) = rows.next().await.context("failed to read ip ban row")? {
        let ip = row
            .get::<String>(0)
            .context("failed to decode ip ban value")?;
        let expires_at = normalize_expires_at(
            row.get::<i64>(1)
                .context("failed to decode ip ban expiry")?,
        );
        newest_inserted_at = newest_inserted_at.max(
            row.get::<i64>(2)
                .context("failed to decode ip ban inserted_at")?,
        );
        match ip.parse::<IpAddr>() {
            Ok(ip) => updates.push((ip, expires_at)),
            Err(error) => tracing::warn!(ip = %ip, error = ?error, "skipping invalid ip_bans row"),
        }
    }
    Ok((updates, newest_inserted_at))
}

fn build_ban_snapshot(updates: Vec<(IpAddr, Option<i64>)>) -> HashMap<IpAddr, Option<i64>> {
    let now = current_unix_seconds();
    let mut banned_ips = HashMap::new();
    for (ip, expires_at) in updates {
        if is_ban_active(expires_at, now) {
            banned_ips.insert(ip, expires_at);
        } else {
            banned_ips.remove(&ip);
        }
    }
    banned_ips
}

fn spawn_ip_ban_sync_task(
    conn: libsql::Connection,
    admission_controller: Arc<admission::AdmissionController>,
    mut last_inserted_at: i64,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5 * 60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        tracing::debug!(
            interval_seconds = 5 * 60,
            last_inserted_at,
            "Started incremental IP ban sync task"
        );
        loop {
            interval.tick().await;
            match load_ip_ban_updates(&conn, Some(last_inserted_at)).await {
                Ok((updates, _)) if updates.is_empty() => {
                    tracing::trace!(last_inserted_at, "Checked IP bans; no changes detected");
                }
                Ok((updates, newest_inserted_at)) => {
                    last_inserted_at = newest_inserted_at;
                    let summary = admission_controller.apply_ban_updates(updates);
                    tracing::debug!(
                        last_inserted_at,
                        activated = summary.activated,
                        deactivated = summary.deactivated,
                        evicted_from_queue = summary.evicted_from_queue,
                        active_ban_count = summary.active_ban_count,
                        "Applied incremental IP ban updates"
                    );
                }
                Err(error) => {
                    tracing::warn!(error = ?error, "failed to refresh ip bans");
                }
            }
        }
    });
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = tracing_subscriber::fmt()
        // .with_max_level(tracing::Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let db = if let Ok(libsql_url) = std::env::var("LIBSQL_URL") {
        let libsql_auth_token = std::env::var("LIBSQL_AUTH_TOKEN")
            .context("LIBSQL_AUTH_TOKEN must be set if LIBSQL_URL is set")?;
        libsql::Builder::new_remote(libsql_url.clone(), libsql_auth_token.clone())
            .build()
            .await
            .context("Failed to initialize remote libsql client")?
    } else {
        let db_path = std::env::var("TERMINAL_GAMES_DB_PATH")
            .unwrap_or_else(|_| "./terminal-games.db".to_string());
        libsql::Builder::new_local(db_path).build().await?
    };

    let conn = db.connect().context("Failed to connect to libsql")?;

    let tx = conn
        .transaction()
        .await
        .context("Failed to start migration transaction")?;
    tx.execute_batch(include_str!("../../terminal-games/libsql/migrate-001.sql"))
        .await
        .context("Failed to run migrate-001.sql")?;
    tx.commit()
        .await
        .context("Failed to commit migration transaction")?;

    let upserted = sync_games_from_embedded_manifests(&conn).await?;
    tracing::info!(
        count = upserted,
        "Synchronized games from embedded wasm manifests"
    );

    let mesh = Mesh::new(Arc::new(EnvDiscovery::new()));
    mesh.start_discovery().await?;
    mesh.serve().await?;

    let app_server = Arc::new(AppServer::new(mesh.clone(), conn.clone())?);
    let max_active_apps = std::env::var("MAX_ACTIVE_APPS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
        .unwrap_or(usize::MAX);
    let max_active_apps_per_ip = std::env::var("MAX_ACTIVE_APPS_PER_IP")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
        .unwrap_or(max_active_apps.min(3));
    let max_queued_apps_per_ip = std::env::var("MAX_QUEUED_APPS_PER_IP")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(max_active_apps_per_ip);
    let ssh_captcha_threshold =
        parse_ssh_captcha_threshold(std::env::var("SSH_CAPTCHA_THRESHOLD"))?;
    let (ban_updates, last_ban_inserted_at) = load_ip_ban_updates(&conn, None).await?;
    let banned_ips = build_ban_snapshot(ban_updates);
    tracing::debug!(
        active_ban_count = banned_ips.len(),
        last_inserted_at = last_ban_inserted_at,
        "Loaded initial IP ban snapshot"
    );
    let metrics = metrics::ServerMetrics::new(max_active_apps, conn.clone()).await?;
    let admission_controller = Arc::new(admission::AdmissionController::new(
        AdmissionConfig {
            max_running: max_active_apps,
            max_running_per_ip: max_active_apps_per_ip,
            max_queued_per_ip: max_queued_apps_per_ip,
            ssh_captcha_threshold,
        },
        banned_ips,
        metrics.clone(),
    ));
    spawn_ip_ban_sync_task(
        conn.clone(),
        admission_controller.clone(),
        last_ban_inserted_at,
    );
    tracing::info!(
        max_active_apps,
        max_active_apps_per_ip,
        max_queued_apps_per_ip,
        ssh_captcha_threshold = ssh_captcha_threshold
            .map(|value| format!("{value:.3}"))
            .unwrap_or_else(|| "disabled".to_string()),
        "Configured admission limits"
    );

    tokio::select! {
        result = async {
            let mut server = ssh::SshServer::new(
                app_server.clone(),
                admission_controller.clone(),
                metrics.clone(),
            ).await?;
            server.run().await
        } => {
            result?;
        }
        result = async {
            let server = web::WebServer::new(
                app_server.clone(),
                admission_controller.clone(),
                metrics.clone(),
            )?;
            server.run().await
        } => {
            result?;
        }
    }

    mesh.graceful_shutdown().await;

    Ok(())
}

fn parse_ssh_captcha_threshold(raw: Result<String, std::env::VarError>) -> Result<Option<f64>> {
    let raw = match raw {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => return Ok(Some(0.5)),
        Err(std::env::VarError::NotUnicode(_)) => {
            anyhow::bail!("SSH_CAPTCHA_THRESHOLD is not valid Unicode");
        }
    };

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("SSH_CAPTCHA_THRESHOLD cannot be empty");
    }

    if matches!(
        trimmed.to_ascii_lowercase().as_str(),
        "off" | "none" | "disabled" | "false"
    ) {
        return Ok(None);
    }

    let threshold = trimmed.parse::<f64>().map_err(|_| {
        anyhow::anyhow!(
            "SSH_CAPTCHA_THRESHOLD must be a number between 0.0 and 1.0, or one of: off, none, disabled"
        )
    })?;
    if !(0.0..=1.0).contains(&threshold) {
        anyhow::bail!("SSH_CAPTCHA_THRESHOLD must be between 0.0 and 1.0");
    }
    Ok(Some(threshold))
}

fn normalize_expires_at(expires_at_raw: i64) -> Option<i64> {
    (expires_at_raw >= 0).then_some(expires_at_raw)
}

fn is_ban_active(expires_at: Option<i64>, now: i64) -> bool {
    expires_at.is_none_or(|expires_at| expires_at > now)
}

fn current_unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
