// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod audio;

use std::{
    fmt,
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
use serde_json::Value;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tracing::field::Field;
use tracing_subscriber::{
    Layer,
    field::{RecordFields, VisitOutput},
    filter::LevelFilter,
    fmt::{FormatFields, format::Writer, time::FormatTime},
    layer::SubscriberExt,
};

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

    /// Bandwidth token bucket refill rate in bytes per second (default: 50000)
    #[arg(long, default_value = "65536")]
    bandwidth: u64,

    /// Bandwidth token bucket capacity in bytes (default: 100000)
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

#[derive(Default)]
struct NativeLikeFields;

impl<'writer> FormatFields<'writer> for NativeLikeFields {
    fn format_fields<R: RecordFields>(&self, writer: Writer<'writer>, fields: R) -> fmt::Result {
        let mut visitor = NativeLikeVisitor::new(writer);
        fields.record(&mut visitor);
        visitor.finish()
    }
}

struct NativeLikeVisitor<'writer> {
    writer: Writer<'writer>,
    fields: Vec<(String, String)>,
    location: Option<String>,
    message: Option<String>,
}

const ANSI_RESET: &str = "\x1b[0m";
const ANSI_DIM: &str = "\x1b[2m";
const ANSI_ITALIC: &str = "\x1b[3m";

impl<'writer> NativeLikeVisitor<'writer> {
    fn new(writer: Writer<'writer>) -> Self {
        Self {
            writer,
            fields: Vec::new(),
            location: None,
            message: None,
        }
    }

    fn push_field(&mut self, name: &str, value: impl fmt::Display) {
        let name = name.strip_prefix("r#").unwrap_or(name);
        if matches!(name, "shortname" | "module_path") {
            return;
        }
        self.fields.push((name.to_owned(), value.to_string()));
    }

    fn record_attributes(&mut self, raw: &str) {
        let Ok(Value::Object(attributes)) = serde_json::from_str::<Value>(raw) else {
            self.push_field("attributes", raw);
            return;
        };

        let mut flattened = Vec::new();
        flatten_json_attributes(None, &attributes, &mut flattened);

        let file = take_flattened_value(&mut flattened, "file");
        let line = take_flattened_value(&mut flattened, "line");
        let column = take_flattened_value(&mut flattened, "column");
        if let Some(location) =
            format_clickable_location(file.as_deref(), line.as_deref(), column.as_deref())
        {
            self.location = Some(location);
        }

        for (key, value) in flattened {
            self.push_field(&key, value);
        }
    }

    fn record_value(&mut self, field: &Field, value: impl fmt::Display) {
        let name = field.name();

        match name {
            "message" => self.message = Some(value.to_string()),
            "attributes" => self.record_attributes(&value.to_string()),
            _ => self.push_field(name, value),
        }
    }

    fn write_spaced(&mut self, first: &mut bool, value: impl fmt::Display) -> fmt::Result {
        if !*first {
            write!(self.writer, " ")?;
        }
        *first = false;
        write!(self.writer, "{value}")
    }

    fn write_location(&mut self, first: &mut bool, value: &str) -> fmt::Result {
        if !*first {
            write!(self.writer, " ")?;
        }
        *first = false;

        if self.writer.has_ansi_escapes() {
            write!(self.writer, "{ANSI_DIM}{value}{ANSI_RESET}")
        } else {
            write!(self.writer, "{value}")
        }
    }

    fn write_field_rendered(&mut self, first: &mut bool, name: &str, value: &str) -> fmt::Result {
        if !*first {
            write!(self.writer, " ")?;
        }
        *first = false;

        if self.writer.has_ansi_escapes() {
            write!(
                self.writer,
                "{}{}{}",
                ansi_italic(name),
                ansi_dim("="),
                value
            )
        } else {
            write!(self.writer, "{name}={value}")
        }
    }
}

fn ansi_dim(value: &str) -> String {
    format!("{ANSI_DIM}{value}{ANSI_RESET}")
}

fn ansi_italic(value: &str) -> String {
    format!("{ANSI_ITALIC}{value}{ANSI_RESET}")
}

#[derive(Clone, Copy)]
struct CliTimer;

impl FormatTime for CliTimer {
    fn format_time(&self, writer: &mut Writer<'_>) -> fmt::Result {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| fmt::Error)?;
        let total_secs = duration.as_secs();
        let hours = (total_secs / 3600) % 24;
        let minutes = (total_secs / 60) % 60;
        let seconds = total_secs % 60;
        let micros = duration.subsec_micros();
        write!(writer, "{hours:02}:{minutes:02}:{seconds:02}.{micros:06} ")
    }
}

impl tracing::field::Visit for NativeLikeVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "message" => self.message = Some(value.to_owned()),
            "attributes" => self.record_attributes(value),
            _ => self.push_field(field.name(), format!("{value:?}")),
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_value(field, value);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_value(field, value);
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_value(field, value);
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.record_value(field, value);
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.record_value(field, value);
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record_value(field, format!("{value:?}"));
    }
}

impl VisitOutput<fmt::Result> for NativeLikeVisitor<'_> {
    fn finish(mut self) -> fmt::Result {
        let mut first = true;

        if let Some(location) = self.location.clone() {
            self.write_location(&mut first, &location)?;
        }

        if let Some(message) = self.message.clone() {
            self.write_spaced(&mut first, message)?;
        }

        for (name, value) in self.fields.clone() {
            self.write_field_rendered(&mut first, &name, &value)?;
        }

        Ok(())
    }
}

fn flatten_json_attributes(
    prefix: Option<&str>,
    object: &serde_json::Map<String, Value>,
    out: &mut Vec<(String, String)>,
) {
    for (key, value) in object {
        let key = match prefix {
            Some(prefix) => format!("{prefix}.{key}"),
            None => key.clone(),
        };
        match value {
            Value::Object(object) => flatten_json_attributes(Some(&key), object, out),
            Value::String(value) => out.push((key, value.clone())),
            Value::Null => out.push((key, "null".to_owned())),
            _ => out.push((key, value.to_string())),
        }
    }
}

fn take_flattened_value(fields: &mut Vec<(String, String)>, name: &str) -> Option<String> {
    let index = fields.iter().position(|(key, _)| key == name)?;
    Some(fields.remove(index).1)
}

fn format_clickable_location(
    file: Option<&str>,
    line: Option<&str>,
    column: Option<&str>,
) -> Option<String> {
    let file = file?;
    let mut location = file.to_owned();
    if let Some(line) = line {
        location.push(':');
        location.push_str(line);
        if let Some(column) = column {
            location.push(':');
            location.push_str(column);
        }
    }
    location.push(':');
    Some(location)
}

struct CliLogBackend {
    level_filter: LogLevelFilter,
}

impl GuestLogBackend for CliLogBackend {
    fn log(&self, shortname: &str, _user_id: Option<u64>, record: &GuestLogRecord) {
        if !self.level_filter.allows(record.level) {
            return;
        }

        let attributes = guest_attributes_json(record);

        macro_rules! emit_guest {
            ($emit:ident) => {{
                match attributes.as_deref() {
                    Some(attributes) => tracing::$emit!(
                        target: "app",
                        message = %record.message,
                        shortname = %shortname,
                        attributes = %attributes,
                    ),
                    None => tracing::$emit!(
                        target: "app",
                        message = %record.message,
                        shortname = %shortname,
                    ),
                }
            }};
        }

        match record.level {
            LogLevel::Trace => emit_guest!(trace),
            LogLevel::Debug => emit_guest!(debug),
            LogLevel::Info => emit_guest!(info),
            LogLevel::Warn => emit_guest!(warn),
            LogLevel::Error => emit_guest!(error),
        }
    }
}

fn guest_attributes_json(record: &GuestLogRecord) -> Option<String> {
    let mut attributes = serde_json::Map::new();
    if let Some(file) = &record.file {
        attributes.insert("file".to_owned(), Value::String(file.clone()));
    }
    if let Some(line) = record.line {
        attributes.insert("line".to_owned(), Value::Number(line.into()));
    }
    if let Some(module_path) = &record.module_path {
        attributes.insert("module_path".to_owned(), Value::String(module_path.clone()));
    }
    for (key, value) in &record.attributes {
        attributes.insert(key.clone(), value.clone());
    }

    if attributes.is_empty() {
        None
    } else {
        serde_json::to_string(&attributes).ok()
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
        .with_target("app", LevelFilter::TRACE)
        .with_target("terminal_games", platform_filter)
        .with_target("terminal_games_cli", platform_filter)
        .with_default(LevelFilter::OFF);
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .compact()
            .with_ansi(true)
            .with_timer(CliTimer)
            .fmt_fields(NativeLikeFields)
            .with_writer(move || log_writer.clone())
            .with_filter(filter),
    );
    tracing::subscriber::set_global_default(subscriber)?;
    let guest_log_backend = Arc::new(CliLogBackend {
        level_filter: args.app_log_level,
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
