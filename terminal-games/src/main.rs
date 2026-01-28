// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod app;
mod audio;
mod mesh;
mod rate_limiting;
mod ssh;
mod status_bar;
mod web;

use std::sync::Arc;

use anyhow::Result;

use crate::{
    app::AppServer,
    mesh::{EnvDiscovery, Mesh},
};

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = tracing_subscriber::fmt()
        // .with_max_level(tracing::Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let db = libsql::Builder::new_remote(
        std::env::var("LIBSQL_URL").unwrap(),
        std::env::var("LIBSQL_AUTH_TOKEN").unwrap(),
    )
    .build()
    .await
    .unwrap();

    let conn = db.connect().unwrap();

    let tx = conn.transaction().await.unwrap();
    tx.execute_batch(include_str!("../libsql/migrate-001.sql"))
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let _ = conn
        .execute(
            "INSERT INTO games (shortname, path) VALUES (?1, ?2)",
            libsql::params!("kitchen-sink", "examples/kitchen-sink/main.wasm"),
        )
        .await;

    let _ = conn
        .execute(
            "INSERT INTO games (shortname, path) VALUES (?1, ?2)",
            libsql::params!("menu", "cmd/menu/main.wasm"),
        )
        .await;

    let _ = conn
        .execute(
            "INSERT INTO games (shortname, path) VALUES (?1, ?2)",
            libsql::params!(
                "rust-simple",
                "target/wasm32-wasip1/release/rust-simple.wasm"
            ),
        )
        .await;

    let _ = conn
        .execute(
            "INSERT INTO games (shortname, path) VALUES (?1, ?2)",
            libsql::params!(
                "rust-kitchen-sink",
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
