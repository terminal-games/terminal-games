// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod admission;
mod cluster_detection;
mod control;
mod idle;
mod metrics;
mod sessions;
mod ssh;
mod web;

use std::{
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use rustls::crypto::aws_lc_rs;
use tracing_subscriber::{Layer, filter::LevelFilter, layer::SubscriberExt};

use crate::admission::{AdmissionConfig, decode_cidr_blob};
use terminal_games::{
    app::AppServer,
    author_env::AUTHOR_ENV_KEY_ENV,
    mesh::{BuildId, EnvDiscovery, GameRuntimeUpdateMessage, Mesh},
};

const AUTHOR_ENV_DEV_FALLBACK_KEY: &str = "terminal-games-dev-author-env-key";

// Avoid musl's default allocator due to lackluster performance
// https://nickb.dev/blog/default-musl-allocator-considered-harmful-to-performance
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

async fn load_game_build_snapshot(
    conn: &libsql::Connection,
) -> Result<Vec<GameRuntimeUpdateMessage>> {
    let mut updates = Vec::new();
    let mut game_rows = conn
        .query(
            "SELECT id, wasm_hash, env_hash, build_updated_at FROM games",
            (),
        )
        .await?;
    while let Some(row) = game_rows.next().await? {
        updates.push(GameRuntimeUpdateMessage::published(
            row.get::<u64>(0)?,
            BuildId::from_hash_slices(&row.get::<Vec<u8>>(1)?, &row.get::<Vec<u8>>(2)?)?,
            row.get::<i64>(3)?,
        ));
    }
    Ok(updates)
}

async fn sync_game_build_snapshot(
    conn: &libsql::Connection,
    app_server: &AppServer,
    mesh: &Mesh,
) -> Result<()> {
    let snapshot = load_game_build_snapshot(conn).await?;
    app_server.game_registry().sync_snapshot(snapshot.clone());
    mesh.replace_game_runtime_snapshot(snapshot).await;
    Ok(())
}

async fn load_ip_ban_updates(
    conn: &libsql::Connection,
    since: Option<i64>,
) -> Result<(Vec<(ipnet::IpNet, Option<String>, Option<i64>)>, i64)> {
    let mut rows = match since {
        Some(last_inserted_at) => {
            conn.query(
                "SELECT cidr, reason, COALESCE(expires_at, -1), inserted_at
                 FROM ip_bans
                 WHERE inserted_at > ?1
                 ORDER BY inserted_at ASC, cidr ASC",
                libsql::params!(last_inserted_at),
            )
            .await
        }
        None => {
            conn.query(
                "SELECT cidr, reason, COALESCE(expires_at, -1), inserted_at
                 FROM ip_bans
                 ORDER BY inserted_at ASC, cidr ASC",
                (),
            )
            .await
        }
    }
    .context("failed to query ip bans")?;
    let mut updates = Vec::new();
    let mut newest_inserted_at = since.unwrap_or(0);
    while let Some(row) = rows.next().await.context("failed to read ip ban row")? {
        let cidr_blob = row
            .get::<Vec<u8>>(0)
            .context("failed to decode ip ban value")?;
        let reason = row
            .get::<Option<String>>(1)
            .context("failed to decode ip ban reason")?
            .map(|reason| reason.trim().to_string())
            .filter(|reason| !reason.is_empty());
        let expires_at = normalize_expires_at(
            row.get::<i64>(2)
                .context("failed to decode ip ban expiry")?,
        );
        newest_inserted_at = newest_inserted_at.max(
            row.get::<i64>(3)
                .context("failed to decode ip ban inserted_at")?,
        );
        match decode_cidr_blob(&cidr_blob) {
            Ok(cidr) => updates.push((cidr, reason, expires_at)),
            Err(error) => tracing::warn!(
                cidr = %hex::encode(&cidr_blob),
                error = %error,
                "skipping invalid ip_bans row"
            ),
        }
    }
    Ok((updates, newest_inserted_at))
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
    let _ = aws_lc_rs::default_provider().install_default();
    let filter = tracing_subscriber::filter::Targets::new()
        .with_target("terminal_games", LevelFilter::INFO)
        .with_target("terminal_games_server", LevelFilter::INFO)
        .with_target("tarpc", LevelFilter::WARN)
        .with_default(LevelFilter::WARN);
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_target(false)
            .with_filter(filter),
    );
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

    let author_env_secret_key = load_author_env_secret_key();

    let mesh = Mesh::new(Arc::new(EnvDiscovery::new()));
    mesh.start_discovery().await?;
    mesh.serve().await?;

    let app_server = Arc::new(AppServer::new(
        mesh.clone(),
        conn.clone(),
        author_env_secret_key,
    )?);
    sync_game_build_snapshot(&conn, &app_server, &mesh).await?;
    {
        let app_server = app_server.clone();
        let conn = conn.clone();
        let mesh = mesh.clone();
        let mut updates = mesh.subscribe_game_runtime_updates();
        tokio::spawn(async move {
            loop {
                match updates.recv().await {
                    Ok(update) => {
                        let _ = app_server.game_registry().apply_update(update);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "lagged mesh game runtime updates");
                        if let Err(error) =
                            sync_game_build_snapshot(&conn, &app_server, &mesh).await
                        {
                            tracing::warn!(
                                error = ?error,
                                "failed to resync game runtime snapshot after lag"
                            );
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }
    let max_active_apps = std::env::var("MAX_ACTIVE_APPS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
        .unwrap_or(usize::MAX);
    let max_active_apps_per_ip = std::env::var("MAX_ACTIVE_APPS_PER_IP")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
        .unwrap_or(usize::MAX);
    let max_queued_apps_per_ip = std::env::var("MAX_QUEUED_APPS_PER_IP")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
        .unwrap_or(usize::MAX);
    let ssh_captcha_threshold =
        parse_ssh_captcha_threshold(std::env::var("SSH_CAPTCHA_THRESHOLD"))?;
    let (banned_ips, last_ban_inserted_at) = load_ip_ban_updates(&conn, None).await?;
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
    let region_id = std::env::var("REGION_ID")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| mesh.region().to_string());
    let admin_shared_secret = std::env::var("ADMIN_SHARED_SECRET")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(Arc::<str>::from);
    let session_registry = sessions::SessionRegistry::new(region_id);
    let initial_status_bar_state = control::load_status_bar_state(&conn, None).await?;
    session_registry.set_status_bar_state(initial_status_bar_state);
    let control_plane = control::ControlPlane::new(
        app_server.clone(),
        admission_controller.clone(),
        session_registry.clone(),
        mesh.clone(),
        max_active_apps,
        admin_shared_secret,
        session_registry.region_id().to_string(),
    );

    tokio::select! {
        result = async {
            let mut server = ssh::SshServer::new(
                app_server.clone(),
                admission_controller.clone(),
                metrics.clone(),
                session_registry.clone(),
                control_plane.clone(),
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
                session_registry.clone(),
                control_plane.clone(),
            )?;
            server.run().await
        } => {
            result?;
        }
    }

    mesh.graceful_shutdown().await;

    Ok(())
}

fn load_author_env_secret_key() -> String {
    match std::env::var(AUTHOR_ENV_KEY_ENV) {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            tracing::warn!(
                "{AUTHOR_ENV_KEY_ENV} is not set; using the built-in development fallback key"
            );
            AUTHOR_ENV_DEV_FALLBACK_KEY.to_string()
        }
    }
}

fn parse_ssh_captcha_threshold(raw: Result<String, std::env::VarError>) -> Result<Option<f64>> {
    let raw = match raw {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => return Ok(None),
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
