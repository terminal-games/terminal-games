// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use rand_core::{OsRng, RngCore};
use russh::keys::ssh_key::{self, PublicKey};
use russh::server::*;
use russh::{Channel, ChannelId, Pty};
use tokio_util::sync::CancellationToken;

use terminal_games::app::{AppInstantiationParams, AppServer};
use terminal_games::palette;
use terminal_games::rate_limiting::{NetworkInformation, RateLimitedStream, TcpLatencyProvider};
use terminal_games::terminal_profile::TerminalProfile;

use crate::admission::{AdmissionController, AdmissionState, AdmissionTicket};

pub struct SshSession {
    input_sender: tokio::sync::mpsc::Sender<smallvec::SmallVec<[u8; 16]>>,
    resize_tx: tokio::sync::watch::Sender<(u16, u16)>,
    auth: Option<tokio::sync::oneshot::Sender<(String, Option<u64>)>>,
    term: Option<tokio::sync::oneshot::Sender<String>>,
    args: Option<tokio::sync::oneshot::Sender<Vec<u8>>>,
    ssh_session: Option<tokio::sync::oneshot::Sender<(Handle, ChannelId, String)>>,
    cancellation_token: CancellationToken,
    server: SshServer,
}

#[derive(Clone)]
pub(crate) struct SshServer {
    app_server: Arc<AppServer>,
    admission_controller: Arc<AdmissionController>,
}

const SSH_EXTENDED_DATA_STDERR: u32 = 1;
const SPINNER_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
const CAPTCHA_ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

impl SshServer {
    pub async fn new(
        app_server: Arc<AppServer>,
        admission_controller: Arc<AdmissionController>,
    ) -> anyhow::Result<Self> {
        tracing::info!("Initializing ssh server");

        Ok(Self {
            app_server,
            admission_controller,
        })
    }

    pub async fn run(&mut self) -> Result<(), anyhow::Error> {
        let host_key = match std::env::var("SSH_HOST_KEY_BASE64") {
            Ok(encoded) => {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(encoded.trim())
                    .map_err(|e| {
                        anyhow::anyhow!("Invalid SSH_HOST_KEY_BASE64 (base64 decode failed): {e}")
                    })?;
                russh::keys::PrivateKey::from_openssh(&decoded).map_err(|e| {
                    anyhow::anyhow!(
                        "Invalid SSH_HOST_KEY_BASE64 (OpenSSH private key parse failed): {e}"
                    )
                })?
            }
            Err(std::env::VarError::NotPresent) => {
                tracing::warn!(
                    "SSH_HOST_KEY_BASE64 not set; generating ephemeral SSH host key for this run"
                );
                russh::keys::PrivateKey::random(&mut OsRng, ssh_key::Algorithm::Ed25519)
                    .map_err(|e| anyhow::anyhow!("Failed to generate SSH host key: {e}"))?
            }
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(anyhow::anyhow!(
                    "Invalid SSH_HOST_KEY_BASE64 (environment variable is not valid Unicode)"
                ));
            }
        };

        let config = Config {
            inactivity_timeout: Some(std::time::Duration::from_secs(3600)),
            auth_rejection_time: std::time::Duration::from_secs(3),
            auth_rejection_time_initial: Some(std::time::Duration::from_secs(0)),
            keys: vec![host_key],
            nodelay: true,
            ..Default::default()
        };
        let config = Arc::new(config);

        let listen_addr: std::net::SocketAddr = std::env::var("SSH_LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:2222".to_string())
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid SSH_LISTEN_ADDR: {}", e))?;

        tracing::info!(addr = %listen_addr, "Running SSH server");
        let socket = tokio::net::TcpListener::bind(listen_addr).await?;
        loop {
            let (stream, remote_addr) = socket.accept().await?;
            if config.nodelay {
                if let Err(e) = stream.set_nodelay(true) {
                    tracing::warn!("set_nodelay() failed: {e:?}");
                }
            }

            let fd = stream.as_raw_fd();
            let network_info = Arc::new(NetworkInformation::new(fd));
            let wrapped_stream = RateLimitedStream::new(stream, network_info.clone());
            let handler = self.new_client(remote_addr, network_info);

            tokio::spawn({
                let config = config.clone();
                async move {
                    let session =
                        match russh::server::run_stream(config, wrapped_stream, handler).await {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::debug!("Connection setup failed: {:?}", e);
                                return;
                            }
                        };

                    if let Err(e) = session.await {
                        tracing::debug!("Connection closed with error: {:?}", e);
                    } else {
                        tracing::debug!("Connection closed");
                    }
                }
            });
        }
    }

    fn new_client(
        &self,
        _addr: std::net::SocketAddr,
        network_info: Arc<NetworkInformation<TcpLatencyProvider>>,
    ) -> SshSession {
        let (auth_sender, auth_receiver) = tokio::sync::oneshot::channel::<(String, Option<u64>)>();
        let (term_sender, term_receiver) = tokio::sync::oneshot::channel::<String>();
        let (args_sender, args_receiver) = tokio::sync::oneshot::channel::<Vec<u8>>();
        let (ssh_session_sender, ssh_session_receiver) =
            tokio::sync::oneshot::channel::<(Handle, ChannelId, String)>();

        let (input_tx, mut input_rx) = tokio::sync::mpsc::channel(20);
        let (resize_tx, mut resize_rx) = tokio::sync::watch::channel((0, 0));
        let cancellation_token = CancellationToken::new();
        let token = cancellation_token.clone();
        let app_server = self.app_server.clone();
        let admission_controller = self.admission_controller.clone();
        tokio::task::spawn(async move {
            let (session_handle, channel_id, remote_sshid) = match ssh_session_receiver.await {
                Ok(v) => v,
                Err(err) => {
                    tracing::trace!(error = ?err, "client disconnected before ssh session init");
                    return;
                }
            };
            let (username, user_id) = match auth_receiver.await {
                Ok(v) => v,
                Err(err) => {
                    tracing::trace!(error = ?err, "missing auth context from client");
                    let _ = session_handle
                        .disconnect(
                            russh::Disconnect::ByApplication,
                            "Authentication failed".to_string(),
                            "en-US".to_string(),
                        )
                        .await;
                    return;
                }
            };

            let locale = if let Some(uid) = user_id {
                match app_server
                    .db
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

            let (first_app_shortname, args, has_audio): (String, Option<Vec<u8>>, bool) =
                match tokio::time::timeout(std::time::Duration::from_millis(1), args_receiver).await
                {
                    Ok(Ok(mut args)) => {
                        let has_audio = args.starts_with(b"audio");
                        if has_audio {
                            args.drain(..5);
                            while !args.is_empty() && args[0].is_ascii_whitespace() {
                                args.remove(0);
                            }
                        }
                        if args.is_empty() {
                            ("menu".into(), None, has_audio)
                        } else if let Some(pos) = args.iter().position(|&b| b.is_ascii_whitespace())
                        {
                            let shortname = String::from_utf8_lossy(&args[..pos]).into();
                            let rest: Vec<u8> = args[pos..]
                                .iter()
                                .copied()
                                .skip_while(|b| b.is_ascii_whitespace())
                                .collect();
                            (shortname, (!rest.is_empty()).then_some(rest), has_audio)
                        } else {
                            (String::from_utf8_lossy(&args).into(), None, has_audio)
                        }
                    }
                    _ => ("menu".into(), None, false),
                };

            let app_exists = match app_server
                .db
                .query(
                    "SELECT 1 FROM games WHERE shortname = ?1 LIMIT 1",
                    libsql::params!(first_app_shortname.as_str()),
                )
                .await
            {
                Ok(mut rows) => matches!(rows.next().await, Ok(Some(_))),
                Err(err) => {
                    tracing::warn!(error = ?err, "failed to validate requested app");
                    false
                }
            };
            if !app_exists {
                let _ = session_handle
                    .disconnect(
                        russh::Disconnect::ByApplication,
                        format!("Unknown game shortname: {}", first_app_shortname),
                        "en-US".to_string(),
                    )
                    .await;
                return;
            }

            // enter the alternate screen so that we aren't moving the cursor
            // around and overwriting the original terminal
            if let Err(err) = session_handle
                .data(channel_id, b"\x1b[?1049h".to_vec().into())
                .await
            {
                tracing::trace!(error = ?err, "failed to enter alternate screen");
                let _ = session_handle
                    .disconnect(
                        russh::Disconnect::ByApplication,
                        "Connection setup failed".to_string(),
                        "en-US".to_string(),
                    )
                    .await;
                return;
            }
            request_terminal_background(&session_handle, channel_id).await;

            let term =
                match tokio::time::timeout(std::time::Duration::from_millis(500), term_receiver)
                    .await
                {
                    Ok(Ok(term)) => Some(term),
                    Ok(Err(_)) => None,
                    Err(_) => {
                        tracing::trace!("no pty_request received within 500ms");
                        let _ = session_handle
                            .disconnect(
                                russh::Disconnect::ByApplication,
                                "Bad terminal or ping too high (>500ms)".to_string(),
                                "en-US".to_string(),
                            )
                            .await;
                        return;
                    }
                };
            let mut terminal_profile = TerminalProfile::from_term(term.as_deref(), None);
            let buffered_input = probe_terminal_background(
                &mut input_rx,
                &mut terminal_profile,
                Duration::from_millis(500),
            )
            .await;
            let (filtered_input_tx, mut filtered_input_rx) = tokio::sync::mpsc::channel(20);
            for data in buffered_input {
                if filtered_input_tx.send(data).await.is_err() {
                    return;
                }
            }
            tokio::task::spawn(async move {
                while let Some(data) = input_rx.recv().await {
                    if filtered_input_tx.send(data).await.is_err() {
                        break;
                    }
                }
            });

            let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(1);
            let (audio_tx, mut audio_rx) = tokio::sync::mpsc::channel(1);
            if admission_controller.should_require_captcha() {
                let captcha = generate_captcha(7);
                if !solve_captcha(
                    &session_handle,
                    channel_id,
                    &mut resize_rx,
                    &mut filtered_input_rx,
                    &captcha,
                    terminal_profile,
                )
                .await
                {
                    let _ = session_handle
                        .data(channel_id, b"\x1b[?1049l".to_vec().into())
                        .await;
                    let _ = session_handle
                        .disconnect(
                            russh::Disconnect::ByApplication,
                            "Bye!".to_string(),
                            "en-US".to_string(),
                        )
                        .await;
                    return;
                }
            }
            let admission_ticket = admission_controller.issue_ticket();
            if !wait_for_admission(
                &session_handle,
                channel_id,
                &mut resize_rx,
                &mut filtered_input_rx,
                &admission_ticket,
                terminal_profile,
            )
            .await
            {
                let _ = session_handle
                    .data(channel_id, b"\x1b[?1049l".to_vec().into())
                    .await;
                let _ = session_handle
                    .disconnect(
                        russh::Disconnect::ByApplication,
                        "Bye!".to_string(),
                        "en-US".to_string(),
                    )
                    .await;
                return;
            }
            let _ = session_handle
                .data(channel_id, b"\x1b[2J\x1b[H".to_vec().into())
                .await;

            let mut exit_rx = app_server.instantiate_app(AppInstantiationParams {
                first_app_shortname,
                args,
                input_receiver: filtered_input_rx,
                output_sender: output_tx,
                audio_sender: has_audio.then_some(audio_tx),
                remote_sshid,
                term,
                username,
                window_size_receiver: resize_rx,
                graceful_shutdown_token: token,
                network_info,
                terminal_profile,
                user_id,
                locale,
            });
            loop {
                tokio::select! {
                    biased;

                    exit_code = &mut exit_rx => {
                        if let Ok(exit_code) = exit_code {
                            tracing::trace!(?exit_code, "App exited");
                        }
                        break;
                    }

                    data = audio_rx.recv(), if has_audio => {
                        let Some(data) = data else { break };
                        let _ = session_handle.extended_data(channel_id, SSH_EXTENDED_DATA_STDERR, russh::CryptoVec::from_slice(&data)).await;
                    }

                    data = output_rx.recv() => {
                        let Some(data) = data else { break };
                        let _ = session_handle.data(channel_id, russh::CryptoVec::from_slice(&data)).await;
                    }
                }
            }
            drop(admission_ticket);

            let _ = session_handle
                .data(channel_id, b"\x1b[?1049l".to_vec().into())
                .await;

            let _ = session_handle
                .disconnect(
                    russh::Disconnect::ByApplication,
                    "Thanks for playing!".to_string(),
                    "en-US".to_string(),
                )
                .await;
        });

        SshSession {
            cancellation_token,
            input_sender: input_tx,
            resize_tx,
            auth: Some(auth_sender),
            term: Some(term_sender),
            args: Some(args_sender),
            ssh_session: Some(ssh_session_sender),
            server: self.clone(),
        }
    }
}

fn generate_captcha(len: usize) -> String {
    let mut out = String::with_capacity(len);
    let mut rng = OsRng;
    for _ in 0..len {
        let idx = (rng.next_u32() as usize) % CAPTCHA_ALPHABET.len();
        out.push(CAPTCHA_ALPHABET[idx] as char);
    }
    out
}

async fn solve_captcha(
    session_handle: &Handle,
    channel_id: ChannelId,
    resize_rx: &mut tokio::sync::watch::Receiver<(u16, u16)>,
    input_rx: &mut tokio::sync::mpsc::Receiver<smallvec::SmallVec<[u8; 16]>>,
    captcha: &str,
    terminal_profile: TerminalProfile,
) -> bool {
    let mut entered = String::with_capacity(captcha.len());
    loop {
        let window = *resize_rx.borrow();
        if render_captcha_screen(
            session_handle,
            channel_id,
            window,
            captcha,
            &entered,
            terminal_profile,
        )
        .await
        .is_err()
        {
            return false;
        }
        tokio::select! {
            changed = resize_rx.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
            data = input_rx.recv() => {
                let Some(data) = data else {
                    return false;
                };
                for byte in data {
                    match byte {
                        0x03 => return false,
                        0x08 | 0x7f => {
                            entered.pop();
                        }
                        b'\r' | b'\n' => {
                            if entered == captcha {
                                return true;
                            }
                        }
                        _ if byte.is_ascii_alphanumeric() => {
                            if entered.len() < captcha.len() {
                                let next_char = captcha.as_bytes()[entered.len()] as char;
                                if (byte as char).to_ascii_uppercase() == next_char {
                                    entered.push(next_char);
                                    if entered.len() == captcha.len() {
                                        return true;
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

async fn wait_for_admission(
    session_handle: &Handle,
    channel_id: ChannelId,
    resize_rx: &mut tokio::sync::watch::Receiver<(u16, u16)>,
    input_rx: &mut tokio::sync::mpsc::Receiver<smallvec::SmallVec<[u8; 16]>>,
    admission_ticket: &AdmissionTicket,
    terminal_profile: TerminalProfile,
) -> bool {
    let mut updates = admission_ticket.subscribe().await;
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
    let mut frame = 0usize;
    let mut status = *updates.borrow();
    loop {
        if status == AdmissionState::Allowed {
            return true;
        }
        let window = *resize_rx.borrow();
        if render_loading_screen(
            session_handle,
            channel_id,
            window,
            status,
            frame,
            terminal_profile,
        )
        .await
        .is_err()
        {
            return false;
        }
        frame = (frame + 1) % SPINNER_FRAMES.len();
        tokio::select! {
            _ = tick.tick() => {}
            changed = updates.changed() => {
                if changed.is_err() {
                    return false;
                }
                status = *updates.borrow();
            }
            changed = resize_rx.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
            data = input_rx.recv() => {
                let Some(data) = data else {
                    return false;
                };
                if data.contains(&0x03) || data.contains(&b'q') {
                    return false;
                }
            }
        }
    }
}

async fn render_captcha_screen(
    session_handle: &Handle,
    channel_id: ChannelId,
    (width, height): (u16, u16),
    captcha: &str,
    entered: &str,
    terminal_profile: TerminalProfile,
) -> anyhow::Result<()> {
    let title_plain = " Terminal Games ";
    let title = styled_terminal_games_title(terminal_profile, title_plain);
    let prompt = styled_regular_text(terminal_profile, "Type to continue");
    let hint = styled_subtle_text(terminal_profile, "Ctrl+C to quit");
    let entered_styled = styled_captcha_entered(terminal_profile, entered);
    let remaining = &captcha[entered.len()..];
    let subtle = styled_subtle_text(terminal_profile, remaining);
    let captcha_line = format!("{}{}", entered_styled, subtle);
    let center_row = if height == 0 { 1 } else { height.max(4) / 2 };
    let title_row = center_row.saturating_sub(2).max(1);
    let prompt_row = center_row.saturating_sub(1).max(1);
    let captcha_row = center_row;
    let hint_row = center_row.saturating_add(1);

    let mut out = Vec::new();
    out.extend_from_slice(b"\x1b[?25l\x1b[2J");
    out.extend_from_slice(
        format!(
            "\x1b[{};{}H{}",
            title_row,
            centered_col(width, title_plain.chars().count()),
            title
        )
        .as_bytes(),
    );
    out.extend_from_slice(
        format!(
            "\x1b[{};{}H{}",
            prompt_row,
            centered_col(width, "Type to continue".chars().count()),
            prompt
        )
        .as_bytes(),
    );
    out.extend_from_slice(
        format!(
            "\x1b[{};{}H{}",
            captcha_row,
            centered_col(width, captcha.chars().count()),
            captcha_line
        )
        .as_bytes(),
    );
    out.extend_from_slice(
        format!(
            "\x1b[{};{}H{}",
            hint_row,
            centered_col(width, "Ctrl+C to quit".chars().count()),
            hint
        )
        .as_bytes(),
    );
    out.extend_from_slice(b"\x1b[H");
    session_handle
        .data(channel_id, out.into())
        .await
        .map_err(|_| anyhow::anyhow!("failed writing captcha frame"))?;
    Ok(())
}

async fn render_loading_screen(
    session_handle: &Handle,
    channel_id: ChannelId,
    (width, height): (u16, u16),
    status: AdmissionState,
    frame: usize,
    terminal_profile: TerminalProfile,
) -> anyhow::Result<()> {
    let title_plain = " Terminal Games ";
    let title = styled_terminal_games_title(terminal_profile, title_plain);
    let spinner_plain = format!("{} Loading...", SPINNER_FRAMES[frame]);
    let spinner_line = styled_regular_text(terminal_profile, &spinner_plain);
    let queue_plain = match status {
        AdmissionState::Allowed => "Starting app...".to_string(),
        AdmissionState::Queued(position) => format!("Position in queue: {}", position),
    };
    let queue_line = styled_regular_text(terminal_profile, &queue_plain);
    let center_row = if height == 0 { 1 } else { height.max(2) / 2 };
    let title_row = center_row.saturating_sub(1).max(1);
    let queue_row = center_row.saturating_add(1);
    let title_col = centered_col(width, title_plain.chars().count());
    let spinner_col = centered_col(width, spinner_plain.chars().count());
    let queue_col = centered_col(width, queue_plain.chars().count());

    let mut out = Vec::new();
    out.extend_from_slice(b"\x1b[?25l\x1b[2J");
    out.extend_from_slice(format!("\x1b[{};{}H{}", title_row, title_col, title).as_bytes());
    out.extend_from_slice(
        format!("\x1b[{};{}H{}", center_row, spinner_col, spinner_line).as_bytes(),
    );
    out.extend_from_slice(format!("\x1b[{};{}H{}", queue_row, queue_col, queue_line).as_bytes());
    out.extend_from_slice(b"\x1b[H");
    session_handle
        .data(channel_id, out.into())
        .await
        .map_err(|_| anyhow::anyhow!("failed writing loading frame"))?;
    Ok(())
}

fn centered_col(width: u16, text_chars: usize) -> u16 {
    if width <= 1 {
        return 1;
    }
    let width = width as usize;
    if text_chars >= width {
        1
    } else {
        ((width - text_chars) / 2 + 1) as u16
    }
}

async fn request_terminal_background(session_handle: &Handle, channel_id: ChannelId) {
    session_handle
        .data(channel_id, b"\x1b]11;?\x07".to_vec().into())
        .await
        .ok();
}

fn styled_terminal_games_title(profile: TerminalProfile, text: &str) -> String {
    let p = palette::palette(profile);
    let text = styled_on_primary_text(profile, text);
    format!(
        "\x1b[{}m{text}\x1b[0m",
        palette::render_color_code(p.primary, true)
    )
}

#[derive(Default)]
struct Osc11Parser {
    buffer: Vec<u8>,
}

impl Osc11Parser {
    fn consume(&mut self, input: &[u8], terminal_profile: &mut TerminalProfile) -> Vec<u8> {
        self.buffer.extend_from_slice(input);
        let mut out = Vec::with_capacity(input.len());
        let mut i = 0usize;

        while i < self.buffer.len() {
            if self.buffer[i..].starts_with(b"\x1b]11;") {
                let Some((consumed, rgb)) = parse_osc11_sequence(&self.buffer[i..]) else {
                    break;
                };
                if let Some(rgb) = rgb {
                    *terminal_profile = terminal_profile.with_background_rgb(rgb);
                }
                i += consumed;
                continue;
            }
            out.push(self.buffer[i]);
            i += 1;
        }

        if i > 0 {
            self.buffer.drain(..i);
        }
        out
    }

    fn finish(mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }
}

fn parse_osc11_sequence(data: &[u8]) -> Option<(usize, Option<(u8, u8, u8)>)> {
    if !data.starts_with(b"\x1b]11;") {
        return Some((1, None));
    }
    let start = 5usize;
    let mut end = None;
    let mut consumed = 0usize;
    for i in start..data.len() {
        if data[i] == 0x07 {
            end = Some(i);
            consumed = i + 1;
            break;
        }
        if data[i] == 0x1b && i + 1 < data.len() && data[i + 1] == b'\\' {
            end = Some(i);
            consumed = i + 2;
            break;
        }
    }
    let Some(end) = end else {
        return None;
    };
    let content = &data[start..end];
    let rgb = parse_rgb_osc_payload(content);
    Some((consumed, rgb))
}

fn parse_rgb_osc_payload(payload: &[u8]) -> Option<(u8, u8, u8)> {
    let s = std::str::from_utf8(payload).ok()?;
    let value = s.strip_prefix("rgb:")?;
    let mut parts = value.split('/');
    let r = parse_hex_component(parts.next()?)?;
    let g = parse_hex_component(parts.next()?)?;
    let b = parse_hex_component(parts.next()?)?;
    if parts.next().is_some() {
        return None;
    }
    Some((r, g, b))
}

fn parse_hex_component(part: &str) -> Option<u8> {
    if part.is_empty() || part.len() > 4 {
        return None;
    }
    let value = u16::from_str_radix(part, 16).ok()?;
    let bits = (part.len() * 4) as u32;
    let max = (1u32 << bits).saturating_sub(1);
    let scaled = ((value as u32) * 255 + (max / 2)) / max;
    u8::try_from(scaled).ok()
}

async fn probe_terminal_background(
    input_rx: &mut tokio::sync::mpsc::Receiver<smallvec::SmallVec<[u8; 16]>>,
    terminal_profile: &mut TerminalProfile,
    timeout: Duration,
) -> Vec<smallvec::SmallVec<[u8; 16]>> {
    let deadline = Instant::now() + timeout;
    let mut buffered = Vec::new();
    let mut osc11 = Osc11Parser::default();

    while terminal_profile.background_rgb.is_none() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, input_rx.recv()).await {
            Ok(Some(data)) => {
                let filtered = osc11.consume(data.as_slice(), terminal_profile);
                if !filtered.is_empty() {
                    buffered.push(filtered.into_iter().collect());
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let pending = osc11.finish();
    if !pending.is_empty() {
        buffered.push(pending.into_iter().collect());
    }
    buffered
}

fn styled_captcha_entered(profile: TerminalProfile, text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
    styled_regular_text(profile, text)
}

fn styled_fg(color: palette::Color, text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
    format!(
        "\x1b[{}m{text}\x1b[0m",
        palette::render_color_code(color, false)
    )
}

fn styled_subtle_text(profile: TerminalProfile, text: &str) -> String {
    styled_fg(palette::palette(profile).text_subtle, text)
}

fn styled_regular_text(profile: TerminalProfile, text: &str) -> String {
    styled_fg(palette::palette(profile).text, text)
}

fn styled_on_primary_text(profile: TerminalProfile, text: &str) -> String {
    styled_fg(palette::palette(profile).on_primary, text)
}

impl Drop for SshSession {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}

impl Handler for SshSession {
    type Error = anyhow::Error;

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        session: &mut Session,
    ) -> Result<bool, Self::Error> {
        let remote_sshid = String::from_utf8_lossy(session.remote_sshid()).to_string();
        if let Some(ssh_session_sender) = self.ssh_session.take() {
            let _ = ssh_session_sender.send((session.handle(), channel.id(), remote_sshid));
        }

        Ok(true)
    }

    async fn auth_publickey(
        &mut self,
        user: &str,
        pubkey: &PublicKey,
    ) -> Result<Auth, Self::Error> {
        let user_id = if let Ok(mut rows) = self
            .server
            .app_server
            .db
            .query(
                "INSERT INTO users (pubkey_fingerprint, username, locale) VALUES (?1, ?2, 'en')
                 ON CONFLICT(pubkey_fingerprint) DO UPDATE SET username = excluded.username
                 RETURNING id",
                libsql::params!(pubkey.fingerprint(Default::default()).as_bytes(), user),
            )
            .await
        {
            if let Ok(Some(row)) = rows.next().await {
                row.get::<u64>(0).ok()
            } else {
                None
            }
        } else {
            None
        };
        if let Some(auth_sender) = self.auth.take() {
            let _ = auth_sender.send((user.to_string(), user_id));
        }
        Ok(Auth::Accept)
    }

    async fn auth_keyboard_interactive<'a>(
        &'a mut self,
        user: &str,
        _submethods: &str,
        _response: Option<Response<'a>>,
    ) -> Result<Auth, Self::Error> {
        if let Some(auth_sender) = self.auth.take() {
            let _ = auth_sender.send((user.to_string(), None));
        }
        Ok(Auth::Accept)
    }

    async fn data(
        &mut self,
        _channel: ChannelId,
        data: &[u8],
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        match self.input_sender.try_send(data.into()) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                anyhow::bail!("input channel closed");
            }
        }

        Ok(())
    }

    async fn window_change_request(
        &mut self,
        channel: ChannelId,
        col_width: u32,
        row_height: u32,
        _: u32,
        _: u32,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let _ = self.resize_tx.send((col_width as u16, row_height as u16));
        session.channel_success(channel)?;
        Ok(())
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if let Some(args_sender) = self.args.take() {
            let _ = args_sender.send(data.to_vec());
        }
        session.channel_success(channel)?;
        Ok(())
    }

    async fn pty_request(
        &mut self,
        channel: ChannelId,
        term: &str,
        col_width: u32,
        row_height: u32,
        _: u32,
        _: u32,
        _: &[(Pty, u32)],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let _ = self.resize_tx.send((col_width as u16, row_height as u16));
        if let Some(term_sender) = self.term.take() {
            let _ = term_sender.send(term.to_string());
        }
        session.channel_success(channel)?;
        Ok(())
    }
}
