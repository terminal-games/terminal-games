// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod audio;

use std::{
    io::{Read, Write},
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    thread,
    time::Duration,
};

use anyhow::Result;
use clap::Parser;
use flate2::{write::DeflateEncoder, Compression};
use rand::Rng;
use smallvec::SmallVec;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use terminal_games::{
    app::{AppInstantiationParams, AppServer},
    mesh::{LocalDiscovery, Mesh},
    rate_limiting::{NetworkInformation, RateLimitedStream},
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

    /// Enable flate2 compression on output
    #[arg(short = 'C', long)]
    compress: bool,

    /// Simulated network latency in milliseconds
    #[arg(long, default_value = "0")]
    latency: u64,

    /// Simulated latency jitter in milliseconds (±)
    #[arg(long, default_value = "0")]
    jitter: u64,

    /// Bandwidth token bucket refill rate in bytes per second (default: 50000)
    #[arg(long, default_value = "50000")]
    bandwidth: u64,

    /// Bandwidth token bucket capacity in bytes (default: 100000)
    #[arg(long, default_value = "100000")]
    bandwidth_capacity: u64,

    /// Disable audio playback
    #[arg(long)]
    no_audio: bool,

    /// Log verbosity level (off, error, warn, info, debug, trace)
    #[arg(long, default_value = "debug")]
    log_level: tracing::Level,

    /// Username for CLI auth (default: $USER)
    #[arg(long)]
    username: Option<String>,
}

struct LogBuffer(Arc<Mutex<Vec<u8>>>);

impl Write for LogBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A sink that discards all data but reports successful writes.
struct NullSink;

impl AsyncWrite for NullSink {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn compute_jittered_latency(latency_ms: u64, jitter_ms: u64) -> Duration {
    if latency_ms == 0 && jitter_ms == 0 {
        return Duration::ZERO;
    }
    let jitter = if jitter_ms > 0 {
        let mut rng = rand::rng();
        rng.random_range(0..=jitter_ms * 2) as i64 - jitter_ms as i64
    } else {
        0
    };
    Duration::from_millis((latency_ms as i64 + jitter).max(0) as u64)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let log_buffer: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let log_buffer_writer = log_buffer.clone();
    let filter = tracing_subscriber::filter::Targets::new()
        .with_target("terminal_games", args.log_level)
        .with_target("terminal_games_cli", args.log_level)
        .with_default(tracing::Level::WARN);
    use tracing_subscriber::{layer::SubscriberExt, Layer};
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_writer(move || LogBuffer(log_buffer_writer.clone()))
            .with_filter(filter),
    );
    tracing::subscriber::set_global_default(subscriber)?;

    let data_dir = dirs::data_dir()
        .expect("Failed to determine data directory")
        .join("terminal-games");
    std::fs::create_dir_all(&data_dir)?;
    let db_path = data_dir.join("terminal-games.db");

    let db = libsql::Builder::new_local(&db_path).build().await.unwrap();

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

    let username = args
        .username
        .clone()
        .unwrap_or_else(|| std::env::var("USER").unwrap_or_else(|_| "cli".to_string()));
    let user_id = match conn
        .query(
            "SELECT id FROM users WHERE username = ?1 AND pubkey_fingerprint IS NULL LIMIT 1",
            libsql::params!(username.as_str()),
        )
        .await
    {
        Ok(mut rows) => match rows.next().await {
            Ok(Some(row)) => row.get::<u64>(0).ok(),
            _ => None,
        },
        Err(_) => None,
    };
    let user_id = if user_id.is_none() {
        match conn
            .query(
                "INSERT INTO users (pubkey_fingerprint, username, locale) VALUES (NULL, ?1, 'en') RETURNING id",
                libsql::params!(username.as_str()),
            )
            .await
        {
            Ok(mut rows) => match rows.next().await {
                Ok(Some(row)) => row.get::<u64>(0).ok(),
                _ => None,
            },
            Err(_) => None,
        }
    } else {
        user_id
    };
    let locale = if let Some(uid) = user_id {
        match conn
            .query(
                "SELECT locale FROM users WHERE id = ?1 LIMIT 1",
                libsql::params!(uid),
            )
            .await
        {
            Ok(mut rows) => match rows.next().await {
                Ok(Some(row)) => row.get::<String>(0).unwrap_or_else(|_| "en".to_string()),
                _ => "en".to_string(),
            },
            Err(_) => "en".to_string(),
        }
    } else {
        "en".to_string()
    };

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
    crossterm::execute!(
        stdout,
        crossterm::terminal::EnterAlternateScreen,
        crossterm::cursor::Hide
    )?;

    let (input_tx, input_rx) = tokio::sync::mpsc::channel(1);
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(1);
    let (resize_tx, resize_rx) = tokio::sync::watch::channel(crossterm::terminal::size()?);

    let graceful_shutdown_token = CancellationToken::new();
    let network_info = Arc::new(NetworkInformation::new_simulated(args.latency));
    let compress = args.compress;
    let bandwidth = args.bandwidth;
    let bandwidth_capacity = args.bandwidth_capacity;
    let latency = args.latency;
    let jitter = args.jitter;
    let audio_enabled = !args.no_audio;

    let (audio_tx, mut audio_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
    let audio_player = if audio_enabled {
        tracing::debug!("Audio enabled, initializing player");
        match audio::spawn_audio_player() {
            Ok(player) => {
                tracing::debug!("Audio player initialized");
                Some(player)
            }
            Err(e) => {
                tracing::error!(?e, "Failed to initialize audio");
                None
            }
        }
    } else {
        None
    };

    let mut exit_rx = app_server.instantiate_app(AppInstantiationParams {
        args: None,
        input_receiver: input_rx,
        output_sender: output_tx,
        audio_sender: audio_player.as_ref().map(|_| audio_tx),
        remote_sshid: "cli".to_string(),
        term: Some(std::env::var("TERM").unwrap_or_else(|_| "xterm-256color".to_string())),
        username: username.clone(),
        window_size_receiver: resize_rx,
        graceful_shutdown_token: graceful_shutdown_token.clone(),
        network_info: network_info.clone(),
        first_app_shortname,
        user_id,
        locale,
    });

    thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut buf = [0u8; 4096];
        loop {
            match stdin.lock().read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if input_tx.blocking_send(SmallVec::from(&buf[..n])).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let mut sigwinch =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::window_change())?;

    enum DelayedData {
        Terminal { raw: Vec<u8>, metered: Vec<u8> },
        Audio(Vec<u8>),
    }

    let (delayed_tx, mut delayed_rx) = tokio::sync::mpsc::channel(100);

    tokio::spawn(async move {
        let mut rate_limited = RateLimitedStream::with_rate(
            NullSink,
            network_info.clone(),
            bandwidth,
            bandwidth_capacity,
        );
        let mut stdout = std::io::stdout();

        while let Some((data, deliver_at, delay_ms)) = delayed_rx.recv().await {
            tokio::time::sleep_until(deliver_at).await;
            network_info.set_simulated_latency(delay_ms);

            match data {
                DelayedData::Terminal { raw, metered } => {
                    if rate_limited.write_all(&metered).await.is_err() {
                        break;
                    }
                    if stdout.write_all(&raw).is_err() || stdout.flush().is_err() {
                        break;
                    }
                }
                DelayedData::Audio(audio_data) => {
                    if rate_limited.write_all(&audio_data).await.is_err() {
                        break;
                    }
                    if let Some(ref player) = audio_player {
                        player.push_audio(audio_data);
                    }
                }
            }
        }
    });

    loop {
        tokio::select! {
            biased;

            exit_code = &mut exit_rx => {
                if let Ok(exit_code) = exit_code {
                    tracing::info!(?exit_code, "App exited");
                }
                break;
            }

            data = audio_rx.recv(), if audio_enabled => {
                let Some(data) = data else { break };

                let delay = compute_jittered_latency(latency, jitter);
                let delay_ms = delay.as_millis() as u64;
                let deliver_at = tokio::time::Instant::now() + delay;

                let _ = delayed_tx.send((DelayedData::Audio(data), deliver_at, delay_ms)).await;
            }

            data = output_rx.recv() => {
                let Some(data) = data else { break };

                let delay = compute_jittered_latency(latency, jitter);
                let delay_ms = delay.as_millis() as u64;
                let deliver_at = tokio::time::Instant::now() + delay;

                let metered_data = if compress {
                    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
                    encoder.write_all(&data).ok();
                    encoder.finish().unwrap_or_default()
                } else {
                    data.clone()
                };

                let _ = delayed_tx.send((DelayedData::Terminal { raw: data, metered: metered_data }, deliver_at, delay_ms)).await;
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

    std::io::stderr().write_all(&log_buffer.lock().unwrap())?;

    Ok(())
}
