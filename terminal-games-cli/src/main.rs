// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{io::Write, path::Path, sync::Arc};

use anyhow::Result;
use clap::Parser;
use crossterm::event::{Event, EventStream};
use futures::StreamExt;
use smallvec::SmallVec;
use tokio::io::AsyncReadExt;

use terminal_games::{
    app::{AppInstantiationParams, AppServer},
    mesh::{EnvDiscovery, Mesh},
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

    let subscriber = tracing_subscriber::fmt()
        // .with_max_level(tracing::Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

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

    let mesh = Mesh::new(Arc::new(EnvDiscovery::new()));
    mesh.start_discovery().await.unwrap();
    mesh.serve().await.unwrap();

    let app_server = Arc::new(AppServer::new(mesh.clone(), conn).unwrap());

    let (input_tx, input_rx) = tokio::sync::mpsc::channel(1);
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(1);
    let (resize_tx, resize_rx) = tokio::sync::watch::channel((0, 0));

    crossterm::terminal::enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    crossterm::execute!(stdout, crossterm::terminal::EnterAlternateScreen, crossterm::cursor::Hide)?;

    let (cols, rows) = crossterm::terminal::size()?;
    resize_tx.send((cols, rows))?;

    let cancel_token = CancellationToken::new();
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
        graceful_shutdown_token: cancel_token.clone(),
        network_info: network_info.clone(),
        first_app_shortname,
    });

    let mut stdin = tokio::io::stdin();
    let mut stdin_buf = [0u8; 4096];
    let mut event_stream = EventStream::new();

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
                if let Err(_) = stdout.write_all(&data) {
                    break;
                }
                if let Err(_) = stdout.flush() {
                    break;
                }
            }

            result = stdin.read(&mut stdin_buf) => {
                match result {
                    Ok(0) => break,
                    Ok(n) => {
                        let data: SmallVec<[u8; 16]> = SmallVec::from(&stdin_buf[..n]);
                        match input_tx.try_send(data) {
                            Ok(()) => {}
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {}
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                        }
                    }
                    Err(_) => break,
                }
            }

            Some(Ok(Event::Resize(cols, rows))) = event_stream.next() => {
                let _ = resize_tx.send((cols, rows));
            }
        }
    }

    crossterm::execute!(
        stdout,
        crossterm::terminal::LeaveAlternateScreen,
        crossterm::cursor::Show
    )?;
    crossterm::terminal::disable_raw_mode()?;
    
    mesh.graceful_shutdown().await;

    Ok(())
}
