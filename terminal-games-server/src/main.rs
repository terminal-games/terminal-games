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

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = tracing_subscriber::fmt()
        // .with_max_level(tracing::Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let libsql_url = std::env::var("LIBSQL_URL").unwrap();
    let libsql_auth_token = std::env::var("LIBSQL_AUTH_TOKEN").unwrap();

    let db = libsql::Builder::new_remote(libsql_url.clone(), libsql_auth_token.clone()).build().await.unwrap();

    let conn = db.connect().unwrap();

    let tx = conn.transaction().await.unwrap();
    tx.execute_batch(include_str!("../../terminal-games/libsql/migrate-001.sql"))
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let _ = conn
        .execute(
            "INSERT INTO games (shortname, title, path) VALUES (?1, ?2, ?3)",
            libsql::params!("kitchen-sink", "Kitchen Sink", "examples/kitchen-sink/kitchen-sink.wasm"),
        )
        .await;

    let menu_game_id = conn
        .query(
            "INSERT INTO games (shortname, title, path) VALUES (?1, ?2, ?3) ON CONFLICT(shortname) DO UPDATE SET path = excluded.path RETURNING id",
            libsql::params!("menu", "Menu", "cmd/menu/menu.wasm"),
        )
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap()
        .get::<u64>(0)
        .unwrap();

    let _ = conn
        .query(
            "INSERT INTO envs (game_id, name, value) VALUES (?1, ?2, ?3)",
            libsql::params!(menu_game_id, "TURSO_URL", format!("{}?authToken={}", libsql_url, libsql_auth_token)),
        )
        .await;

    let _ = conn
        .execute(
            "INSERT INTO games (shortname, title, path) VALUES (?1, ?2, ?3)",
            libsql::params!(
                "rust-simple",
                "Rust Simple",
                "target/wasm32-wasip1/release/rust-simple.wasm"
            ),
        )
        .await;

    let _ = conn
        .execute(
            "INSERT INTO games (shortname, title, path) VALUES (?1, ?2, ?3)",
            libsql::params!(
                "rust-kitchen-sink",
                "Rust Kitchen Sink",
                "target/wasm32-wasip1/release/rust-kitchen-sink.wasm"
            ),
        )
        .await;

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
