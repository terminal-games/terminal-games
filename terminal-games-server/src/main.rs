// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod ssh;
mod web;

use std::sync::Arc;

use anyhow::Result;

use terminal_games::{
    app::AppServer,
    mesh::{EnvDiscovery, Mesh},
};

async fn upsert_game(conn: &libsql::Connection, shortname: &str, path: &str) -> Result<u64> {
    Ok(conn
        .query(
            "INSERT INTO games (shortname, path) VALUES (?1, ?2) ON CONFLICT(shortname) DO UPDATE SET path = excluded.path RETURNING id",
            libsql::params!(shortname, path),
        )
        .await?
        .next()
        .await?
        .ok_or_else(|| anyhow::anyhow!("failed to upsert game: {shortname}"))?
        .get::<u64>(0)?)
}

async fn upsert_game_localization(
    conn: &libsql::Connection,
    game_id: u64,
    locale: &str,
    title: &str,
    description: &str,
    details: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO game_localizations (game_id, locale, title, description, details) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(game_id, locale) DO UPDATE SET title = excluded.title, description = excluded.description, details = excluded.details",
        libsql::params!(game_id, locale, title, description, details),
    )
    .await?;
    Ok(())
}

async fn upsert_game_screenshot(
    conn: &libsql::Connection,
    game_id: u64,
    locale: &str,
    sort_order: i64,
    image: &str,
    caption: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO game_screenshot_localizations (game_id, locale, sort_order, image, caption) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(game_id, locale, sort_order) DO UPDATE SET image = excluded.image, caption = excluded.caption",
        libsql::params!(game_id, locale, sort_order, image, caption),
    )
    .await?;
    Ok(())
}

fn pad_ascii_line(text: &str, width: usize) -> String {
    let mut out = text.chars().take(width).collect::<String>();
    let len = out.chars().count();
    if len < width {
        out.push_str(&" ".repeat(width - len));
    }
    out
}

fn screenshot_card(title: &str, lines: &[&str]) -> String {
    const INNER_WIDTH: usize = 78;
    const INNER_HEIGHT: usize = 22;

    let mut body = Vec::with_capacity(INNER_HEIGHT);
    body.push(format!("  {}", title));
    body.push(String::new());
    for line in lines {
        body.push(format!("  {}", line));
    }
    while body.len() < INNER_HEIGHT {
        body.push(String::new());
    }

    let mut framed = Vec::with_capacity(INNER_HEIGHT + 2);
    framed.push(format!("+{}+", "-".repeat(INNER_WIDTH)));
    for line in body.into_iter().take(INNER_HEIGHT) {
        framed.push(format!("|{}|", pad_ascii_line(&line, INNER_WIDTH)));
    }
    framed.push(format!("+{}+", "-".repeat(INNER_WIDTH)));
    framed.join("\n")
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

    let conn = db.connect().unwrap();

    let tx = conn.transaction().await.unwrap();
    tx.execute_batch(include_str!("../../terminal-games/libsql/migrate-001.sql"))
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let kitchen_sink_game_id = upsert_game(
        &conn,
        "kitchen-sink",
        "examples/kitchen-sink/kitchen-sink.wasm",
    )
    .await?;
    upsert_game_localization(
        &conn,
        kitchen_sink_game_id,
        "en",
        "Kitchen Sink",
        "Terminal feature showcase.",
        "Explore forms, mouse interactions, viewport behavior, and text styling in one place.",
    )
    .await?;
    upsert_game_localization(
        &conn,
        kitchen_sink_game_id,
        "de",
        "Kuechenbecken",
        "Terminal-Funktionen im Ueberblick.",
        "Formulare, Mausinteraktion, Viewports und Textstile in einer Demo.",
    )
    .await?;
    upsert_game_screenshot(
        &conn,
        kitchen_sink_game_id,
        "en",
        0,
        &screenshot_card(
            "Kitchen Sink - Main View",
            &[
                "Discover interactive widgets and animated panes.",
                "Use keyboard or mouse to inspect each component.",
            ],
        ),
        "Main view with interactive component gallery",
    )
    .await?;
    upsert_game_screenshot(
        &conn,
        kitchen_sink_game_id,
        "en",
        1,
        &screenshot_card(
            "Kitchen Sink - Input Playground",
            &[
                "Try key bindings, filtering, and layout transitions.",
                "Designed to exercise common terminal app edge cases.",
            ],
        ),
        "Input playground and layout transitions",
    )
    .await?;
    upsert_game_screenshot(
        &conn,
        kitchen_sink_game_id,
        "de",
        1,
        &screenshot_card(
            "Kitchen Sink - Eingabe-Spielwiese",
            &[
                "Probiere Tastenkürzel, Filterung und Layout-Übergänge aus.",
                "Entwickelt, um gängige Randfälle von Terminal-Apps zu testen.",
            ],
        ),
        "Eingabe-Spielwiese und Layout-Übergänge",
    )
    .await?;

    let menu_game_id = upsert_game(&conn, "menu", "cmd/menu/menu.wasm").await?;
    upsert_game_localization(
        &conn,
        menu_game_id,
        "en",
        "Menu",
        "Browse and launch games.",
        "Includes localization-aware metadata, replay history, and profile preferences.",
    )
    .await?;
    upsert_game_localization(
        &conn,
        menu_game_id,
        "de",
        "Menü",
        "Spiele finden und starten.",
        "Mit lokalisierter Anzeige, Replay-Verlauf und Profileinstellungen.",
    )
    .await?;
    upsert_game_screenshot(
        &conn,
        menu_game_id,
        "en",
        0,
        &screenshot_card(
            "Terminal Games Menu",
            &[
                "Switch between games, profile, and about tabs.",
                "Select a title and press Enter to play instantly.",
            ],
        ),
        "Games tab with localized list and play action",
    )
    .await?;
    upsert_game_screenshot(
        &conn,
        menu_game_id,
        "de",
        0,
        &screenshot_card(
            "Terminal Games Menü",
            &[
                "Zwischen Spiele-, Profil- und Info-Tab wechseln.",
                "Titel wählen und mit Enter sofort starten.",
            ],
        ),
        "Spiele-Tab mit lokalisierter Liste",
    )
    .await?;

    let _ = conn
        .query(
            "INSERT INTO envs (game_id, name, value) VALUES (?1, ?2, ?3)",
            libsql::params!(
                menu_game_id,
                "TURSO_URL",
                format!("{}?authToken={}", libsql_url, libsql_auth_token)
            ),
        )
        .await;

    let rust_simple_game_id = upsert_game(
        &conn,
        "rust-simple",
        "target/wasm32-wasip1/release/rust-simple.wasm",
    )
    .await?;
    upsert_game_localization(
        &conn,
        rust_simple_game_id,
        "en",
        "Rust Simple",
        "Fast Rust gameplay demo.",
        "Good for smoke-testing runtime behavior and minimal input/output loops.",
    )
    .await?;
    upsert_game_localization(
        &conn,
        rust_simple_game_id,
        "de",
        "Rust Einfach",
        "Schnelle Rust-Spiel Demo.",
        "Ideal fuer schnelle Laufzeittests mit wenig Komplexitaet.",
    )
    .await?;
    upsert_game_screenshot(
        &conn,
        rust_simple_game_id,
        "en",
        0,
        &screenshot_card(
            "Rust Simple",
            &[
                "Fast launch path for quick verification runs.",
                "Minimal UI designed for tight feedback loops.",
            ],
        ),
        "Minimal Rust game experience",
    )
    .await?;

    let rust_kitchen_sink_game_id = upsert_game(
        &conn,
        "rust-kitchen-sink",
        "target/wasm32-wasip1/release/rust-kitchen-sink.wasm",
    )
    .await?;
    upsert_game_localization(
        &conn,
        rust_kitchen_sink_game_id,
        "en",
        "Rust Kitchen Sink",
        "Advanced Rust feature suite.",
        "Use it to validate cross-platform behavior and advanced engine integrations.",
    )
    .await?;
    upsert_game_screenshot(
        &conn,
        rust_kitchen_sink_game_id,
        "en",
        0,
        &screenshot_card(
            "Rust Kitchen Sink - Feature Matrix",
            &[
                "Rendering, input, and async behavior in one scenario.",
                "A practical testbed for complex runtime interactions.",
            ],
        ),
        "Feature matrix overview",
    )
    .await?;
    upsert_game_screenshot(
        &conn,
        rust_kitchen_sink_game_id,
        "en",
        1,
        &screenshot_card(
            "Rust Kitchen Sink - Systems View",
            &[
                "Observe app lifecycle and runtime transitions.",
                "Ideal for diagnosing integration-level regressions.",
            ],
        ),
        "Systems-level runtime view",
    )
    .await?;

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
