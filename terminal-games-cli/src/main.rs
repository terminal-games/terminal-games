// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{io::Read, io::Write, path::Path, sync::Arc, thread};

use anyhow::Result;
use clap::Parser;
use smallvec::SmallVec;

use terminal_games::{
    app::{AppInstantiationParams, AppServer},
    mesh::{LocalDiscovery, Mesh},
    rate_limiting::NetworkInformation,
};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(about = "Run a terminal game from a wasm file")]
struct Args {
    /// Path to the wasm file to run
    wasm_file: String,

    /// Additional games in shortname=path format
    #[arg(short, long = "game", value_name = "SHORTNAME=PATH")]
    games: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // let subscriber = tracing_subscriber::fmt()
    //     // .with_max_level(tracing::Level::ERROR)
    //     .finish();
    // tracing::subscriber::set_global_default(subscriber)?;

    let db = libsql::Builder::new_local(":memory:")
        .build()
        .await
        .unwrap();

    let conn = db.connect().unwrap();

    let tx = conn.transaction().await.unwrap();
    tx.execute_batch(include_str!("../../terminal-games/libsql/migrate-001.sql"))
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let first_app_shortname = Path::new(&args.wasm_file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("app")
        .to_string();

    let _ = conn
        .execute(
            "INSERT INTO games (shortname, path) VALUES (?1, ?2)",
            libsql::params!(first_app_shortname.as_str(), args.wasm_file.as_str()),
        )
        .await;

    for game in &args.games {
        if let Some((shortname, path)) = game.split_once('=') {
            let _ = conn
                .execute(
                    "INSERT INTO games (shortname, path) VALUES (?1, ?2)",
                    libsql::params!(shortname, path),
                )
                .await;
        }
    }

    let local_discovery = Arc::new(LocalDiscovery::new());
    let region = local_discovery
        .allocate_region()
        .expect("Failed to allocate region");
    let mesh = Mesh::with_region(local_discovery.clone(), region);
    let local_addr = mesh
        .serve_on(([127, 0, 0, 1], 0).into())
        .await
        .expect("Failed to start mesh server");
    local_discovery
        .register(region, local_addr.port())
        .await
        .expect("Failed to register with local discovery");
    mesh.start_discovery().await.unwrap();

    let app_server = Arc::new(AppServer::new(mesh.clone(), conn).unwrap());
    
    let mut stdout = std::io::stdout();
    crossterm::terminal::enable_raw_mode()?;
    crossterm::execute!(stdout, crossterm::terminal::EnterAlternateScreen, crossterm::cursor::Hide)?;

    let (input_tx, input_rx) = tokio::sync::mpsc::channel(1);
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(1);
    let (resize_tx, resize_rx) = tokio::sync::watch::channel(crossterm::terminal::size()?);

    let graceful_shutdown_token = CancellationToken::new();
    let network_info = Arc::new(NetworkInformation::new(1));

    let mut exit_rx = app_server.instantiate_app(AppInstantiationParams {
        args: None,
        input_receiver: input_rx,
        output_sender: output_tx,
        audio_sender: None,
        remote_sshid: "cli".to_string(),
        term: Some(std::env::var("TERM").unwrap_or_else(|_| "xterm-256color".to_string())),
        username: std::env::var("USER").unwrap_or_else(|_| "cli".to_string()),
        window_size_receiver: resize_rx,
        graceful_shutdown_token: graceful_shutdown_token.clone(),
        network_info: network_info.clone(),
        first_app_shortname,
    });

    let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel::<SmallVec<[u8; 16]>>(1);
    thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut buf = [0u8; 4096];
        loop {
            match stdin.lock().read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if stdin_tx.blocking_send(SmallVec::from(&buf[..n])).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let mut sigwinch = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::window_change())?;

    loop {
        tokio::select! {
            biased;

            exit_code = &mut exit_rx => {
                if let Ok(exit_code) = exit_code {
                    tracing::info!(?exit_code, "App exited");
                }
                break;
            }

            data = output_rx.recv() => {
                let Some(data) = data else { break };
                if stdout.write_all(&data).is_err() || stdout.flush().is_err() {
                    break;
                }
            }

            Some(data) = stdin_rx.recv() => {
                let _ = input_tx.send(data).await;
            }

            _ = sigwinch.recv() => {
                if let Ok((cols, rows)) = crossterm::terminal::size() {
                    let _ = resize_tx.send((cols, rows));
                }
            }
        }
    }

    crossterm::execute!(
        stdout,
        crossterm::terminal::LeaveAlternateScreen,
        crossterm::cursor::Show,
    )?;
    crossterm::terminal::disable_raw_mode()?;

    let _ = local_discovery.unregister().await;
    mesh.graceful_shutdown().await;

    Ok(())
}
