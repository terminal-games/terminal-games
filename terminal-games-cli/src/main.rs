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
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context as _, Result};
use clap::Parser;
use flate2::{Compression, write::DeflateEncoder};
use rand::Rng;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{Layer, filter::LevelFilter, fmt::time::FormatTime, layer::SubscriberExt};

use terminal_games::{
    app::{AppInstantiationParams, AppServer},
    log_backend::{GuestLogBackend, GuestLogRecord, LogLevel},
    mesh::{LocalDiscovery, Mesh},
    rate_limiting::{NetworkInformation, RateLimitedStream},
    terminal_profile::TerminalProfile,
};

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

    /// Bandwidth token bucket refill rate in bytes per second
    #[arg(long, default_value = "65536")]
    bandwidth: u64,

    /// Bandwidth token bucket capacity in bytes
    #[arg(long, default_value = "131072")]
    bandwidth_capacity: u64,

    /// Disable audio playback
    #[arg(long)]
    no_audio: bool,

    /// Log level for the game/app
    #[arg(long = "app-log-level", default_value = "info")]
    app_log_level: LogLevelFilter,

    /// Log level for the platform
    #[arg(long = "platform-log-level", default_value = "warn")]
    platform_log_level: LogLevelFilter,

    /// Username for CLI auth (default: $USER)
    #[arg(long)]
    username: Option<String>,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum LogLevelFilter {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl From<LogLevelFilter> for tracing_subscriber::filter::LevelFilter {
    fn from(f: LogLevelFilter) -> Self {
        use tracing_subscriber::filter::LevelFilter;
        match f {
            LogLevelFilter::Off => LevelFilter::OFF,
            LogLevelFilter::Error => LevelFilter::ERROR,
            LogLevelFilter::Warn => LevelFilter::WARN,
            LogLevelFilter::Info => LevelFilter::INFO,
            LogLevelFilter::Debug => LevelFilter::DEBUG,
            LogLevelFilter::Trace => LevelFilter::TRACE,
        }
    }
}

impl LogLevelFilter {
    fn allows(self, level: LogLevel) -> bool {
        match self {
            Self::Off => false,
            Self::Error => level >= LogLevel::Error,
            Self::Warn => level >= LogLevel::Warn,
            Self::Info => level >= LogLevel::Info,
            Self::Debug => level >= LogLevel::Debug,
            Self::Trace => true,
        }
    }
}

struct BufferedLogWriter(Arc<Mutex<Vec<u8>>>);

struct FlushGuard(Arc<Mutex<Vec<u8>>>);

impl BufferedLogWriter {
    fn new() -> (Self, FlushGuard) {
        let buf = Arc::new(Mutex::new(Vec::new()));
        (Self(buf.clone()), FlushGuard(buf))
    }

    fn append(&self, buf: &[u8]) {
        let mut locked = match self.0.lock() {
            Ok(locked) => locked,
            Err(poisoned) => poisoned.into_inner(),
        };
        locked.extend_from_slice(buf);
    }
}

impl Clone for BufferedLogWriter {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Drop for FlushGuard {
    fn drop(&mut self) {
        let logs = match self.0.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        let _ = std::io::stderr().write_all(&logs);
    }
}

impl Write for BufferedLogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.append(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct CliTimer;

impl FormatTime for CliTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let (h, m, s) = (
            (t.as_secs() / 3600) % 24,
            (t.as_secs() / 60) % 60,
            t.as_secs() % 60,
        );
        write!(w, "{h:02}:{m:02}:{s:02}.{:06} ", t.subsec_micros())
    }
}

struct CliLogBackend {
    level_filter: LogLevelFilter,
    writer: BufferedLogWriter,
}

const DIM: &str = "\x1b[2m";
const ITALIC: &str = "\x1b[3m";
const PURPLE: &str = "\x1b[35m";
const BLUE: &str = "\x1b[34m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RED: &str = "\x1b[31m";
const RESET: &str = "\x1b[0m";

impl GuestLogBackend for CliLogBackend {
    fn log(&self, shortname: &str, _: Option<u64>, record: &GuestLogRecord) {
        if !self.level_filter.allows(record.level) {
            return;
        }
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let ts = format!(
            "{:02}:{:02}:{:02}.{:06}",
            (t.as_secs() / 3600) % 24,
            (t.as_secs() / 60) % 60,
            t.as_secs() % 60,
            t.subsec_micros()
        );
        let (level_color, level) = match record.level {
            LogLevel::Error => (RED, "ERROR"),
            LogLevel::Warn => (YELLOW, "WARN "),
            LogLevel::Info => (GREEN, "INFO "),
            LogLevel::Debug => (BLUE, "DEBUG"),
            LogLevel::Trace => (PURPLE, "TRACE"),
        };
        let loc = record
            .file
            .as_ref()
            .map(|f| match record.line {
                Some(l) => format!("{f}:{l}: "),
                None => format!("{f}: "),
            })
            .unwrap_or_default();
        let attrs: Vec<_> = record
            .attributes
            .iter()
            .filter(|(k, _)| !matches!(k.as_str(), "shortname" | "module_path" | "file" | "line"))
            .map(|(k, v)| {
                format!(
                    "{ITALIC}{k}{RESET}={}",
                    v.as_str()
                        .map(String::from)
                        .unwrap_or_else(|| v.to_string())
                )
            })
            .collect();
        let attrs_str = if attrs.is_empty() {
            String::new()
        } else {
            format!(" {}", attrs.join(" "))
        };
        let line = format!(
            "{DIM}{ts}{RESET} {level_color}{level}{RESET} {shortname}: {DIM}{loc}{RESET}{}{attrs_str}\n",
            record.message
        );
        self.writer.append(line.as_bytes());
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

async fn upsert_game(
    conn: &libsql::Connection,
    shortname: &str,
    path: &str,
    details: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO games (shortname, path, details) VALUES (?1, ?2, json(?3)) ON CONFLICT(shortname) DO UPDATE SET path = excluded.path, details = excluded.details",
        libsql::params!(shortname, path, details),
    )
    .await
    .with_context(|| format!("Failed to upsert game {shortname}"))?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let (log_writer, _flush_guard) = BufferedLogWriter::new();
    let platform_filter: LevelFilter = args.platform_log_level.into();
    let filter = tracing_subscriber::filter::Targets::new()
        .with_target("terminal_games", platform_filter)
        .with_target("terminal_games_cli", platform_filter)
        .with_default(LevelFilter::OFF);
    let log_writer_for_sub = log_writer.clone();
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .compact()
            .with_ansi(true)
            .with_timer(CliTimer)
            .with_writer(move || log_writer_for_sub.clone())
            .with_filter(filter),
    );
    tracing::subscriber::set_global_default(subscriber)?;
    let guest_log_backend = Arc::new(CliLogBackend {
        level_filter: args.app_log_level,
        writer: log_writer,
    });

    let data_dir = dirs::data_dir()
        .expect("Failed to determine data directory")
        .join("terminal-games");
    std::fs::create_dir_all(&data_dir)?;
    let db_path = data_dir.join("terminal-games.db");

    let db = libsql::Builder::new_local(&db_path)
        .build()
        .await
        .context("Failed to initialize local libsql client")?;

    let conn = db
        .connect()
        .context("Failed to connect to local database")?;

    let tx = conn
        .transaction()
        .await
        .context("Failed to start migration transaction")?;
    tx.execute_batch(include_str!("../../terminal-games/libsql/migrate-001.sql"))
        .await
        .context("Failed to run migrations")?;
    tx.commit()
        .await
        .context("Failed to commit migration transaction")?;

    let wasm_path = Path::new(&args.wasm_file)
        .canonicalize()
        .context("Failed to resolve wasm file path")?
        .display()
        .to_string();
    let first_app_shortname = Path::new(&wasm_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("app")
        .to_string();
    let default_details = |shortname: &str| {
        format!(
            r#"{{"name":{{"en":"{shortname}"}},"description":{{"en":""}},"details":{{"en":""}},"screenshots":{{}}}}"#
        )
    };

    upsert_game(
        &conn,
        first_app_shortname.as_str(),
        wasm_path.as_str(),
        default_details(first_app_shortname.as_str()).as_str(),
    )
    .await?;

    for game in &args.games {
        if let Some((shortname, path)) = game.split_once('=') {
            let path = Path::new(path)
                .canonicalize()
                .context("Failed to resolve game path")?
                .display()
                .to_string();
            upsert_game(
                &conn,
                shortname,
                path.as_str(),
                default_details(shortname).as_str(),
            )
            .await?;
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
    mesh.start_discovery().await?;

    let app_server = Arc::new(AppServer::new(mesh.clone(), conn)?);

    let mut stdout = std::io::stdout();
    crossterm::terminal::enable_raw_mode()?;
    crossterm::execute!(
        stdout,
        crossterm::terminal::EnterAlternateScreen,
        crossterm::cursor::Hide
    )?;

    let (input_tx, input_rx) = tokio::sync::mpsc::channel(12);
    let (replay_request_tx, replay_request_rx) = tokio::sync::mpsc::channel(1);
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
        match audio::spawn_audio_player() {
            Ok(player) => Some(player),
            Err(e) => {
                tracing::error!(?e, "Failed to initialize audio");
                None
            }
        }
    } else {
        None
    };

    let term = std::env::var("TERM").unwrap_or_else(|_| "xterm-256color".to_string());
    let colorterm = std::env::var("COLORTERM").ok();
    let mut exit_rx = app_server.instantiate_app(AppInstantiationParams {
        args: None,
        input_receiver: input_rx,
        replay_request_receiver: replay_request_rx,
        output_sender: output_tx,
        audio_sender: audio_player.as_ref().map(|_| audio_tx),
        remote_sshid: "cli".to_string(),
        term: Some(term.clone()),
        username: username.clone(),
        window_size_receiver: resize_rx,
        graceful_shutdown_token: graceful_shutdown_token.clone(),
        network_info: network_info.clone(),
        terminal_profile: TerminalProfile::from_term(Some(&term), colorterm.as_deref()),
        first_app_shortname,
        user_id,
        locale,
        log_backend: guest_log_backend,
    });

    let graceful_shutdown_token_input = graceful_shutdown_token.clone();
    thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut buf = [0u8; 4096];
        loop {
            match stdin.lock().read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let data = &buf[..n];
                    if data.is_empty() {
                        continue;
                    }
                    if data == b"\x03" {
                        // CTRL+C
                        graceful_shutdown_token_input.cancel();
                    }
                    if data == b"\x12" || data == b"\x1b[27;5;114~" {
                        let _ = replay_request_tx.try_send(());
                    }
                    if input_tx.blocking_send(data.into()).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let mut sigwinch =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::window_change())?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;

    enum DelayedData {
        Terminal {
            raw: Arc<Vec<u8>>,
            metered: Arc<Vec<u8>>,
        },
        Audio(Vec<u8>),
    }

    let (delayed_tx, mut delayed_rx) = tokio::sync::mpsc::unbounded_channel();

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
                match exit_code {
                    Ok(Ok(exit_code)) => {
                        tracing::trace!(?exit_code, "App exited");
                    }
                    Ok(Err(_)) => {
                        // Host already logged "Module errored" with full backtrace
                    }
                    Err(error) => {
                        tracing::error!(?error, "App exit channel dropped");
                    }
                }
                break;
            }

            _ = sigint.recv() => {
                graceful_shutdown_token.cancel();
            }

            data = audio_rx.recv(), if audio_enabled => {
                let Some(data) = data else { break };

                let delay = compute_jittered_latency(latency, jitter);
                let delay_ms = delay.as_millis() as u64;
                let deliver_at = tokio::time::Instant::now() + delay;

                let _ = delayed_tx.send((DelayedData::Audio(data), deliver_at, delay_ms));
            }

            data = output_rx.recv() => {
                let Some(data) = data else { break };

                let delay = compute_jittered_latency(latency, jitter);
                let delay_ms = delay.as_millis() as u64;
                let deliver_at = tokio::time::Instant::now() + delay;

                let metered_data = if compress {
                    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
                    encoder.write_all(&data).ok();
                    Arc::new(encoder.finish().unwrap_or_default())
                } else {
                    data.clone()
                };

                let _ = delayed_tx.send((DelayedData::Terminal { raw: data, metered: metered_data }, deliver_at, delay_ms));
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
