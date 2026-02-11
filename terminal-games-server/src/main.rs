// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod ssh;
mod web;

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use serde::Deserialize;
use wasmparser::{Parser, Payload};

use terminal_games::{
    app::AppServer,
    mesh::{EnvDiscovery, Mesh},
};

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

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = tracing_subscriber::fmt()
        // .with_max_level(tracing::Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let libsql_url = std::env::var("LIBSQL_URL").unwrap();
    let libsql_auth_token = std::env::var("LIBSQL_AUTH_TOKEN").unwrap();

    let db = libsql::Builder::new_remote(libsql_url.clone(), libsql_auth_token.clone())
        .build()
        .await
        .unwrap();

    // let db = libsql::Builder::new_local("./terminal-games.db")
    //     .build()
    //     .await
    //     .unwrap();

    let conn = db.connect().unwrap();

    let tx = conn.transaction().await.unwrap();
    tx.execute_batch(include_str!("../../terminal-games/libsql/migrate-001.sql"))
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let upserted = sync_games_from_embedded_manifests(&conn).await?;
    tracing::info!(
        count = upserted,
        "Synchronized games from embedded wasm manifests"
    );

    let mesh = Mesh::new(Arc::new(EnvDiscovery::new()));
    mesh.start_discovery().await.unwrap();
    mesh.serve().await.unwrap();

    let app_server = Arc::new(AppServer::new(mesh.clone(), conn).unwrap());

    let ssh_app_server = app_server.clone();
    let web_app_server = app_server.clone();

    tokio::select! {
        result = async {
            let mut server = ssh::SshServer::new(ssh_app_server).await?;
            server.run().await
        } => {
            result.expect("Failed running SSH server");
        }
        result = async {
            let server = web::WebServer::new(web_app_server);
            server.run().await
        } => {
            result.expect("Failed running web server");
        }
    }

    mesh.graceful_shutdown().await;

    Ok(())
}
