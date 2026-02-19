// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::os::fd::AsRawFd;
use std::sync::Arc;

use rand_core::{OsRng, RngCore};
use russh::keys::ssh_key::{self, PublicKey};
use russh::server::*;
use russh::{Channel, ChannelId, Pty};
use tokio_util::sync::CancellationToken;

use terminal_games::app::{AppInstantiationParams, AppServer};
use terminal_games::rate_limiting::{NetworkInformation, RateLimitedStream, TcpLatencyProvider};

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
        let config = Config {
            inactivity_timeout: Some(std::time::Duration::from_secs(3600)),
            auth_rejection_time: std::time::Duration::from_secs(3),
            auth_rejection_time_initial: Some(std::time::Duration::from_secs(0)),
            keys: vec![
                russh::keys::PrivateKey::random(&mut OsRng, ssh_key::Algorithm::Ed25519).unwrap(),
            ],
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
                                tracing::info!("Connection setup failed: {:?}", e);
                                return;
                            }
                        };

                    if let Err(e) = session.await {
                        tracing::info!("Connection closed with error: {:?}", e);
                    } else {
                        tracing::info!("Connection closed");
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
            let (session_handle, channel_id, remote_sshid) = ssh_session_receiver.await.unwrap();
            let (username, user_id) = auth_receiver.await.unwrap();

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

            // enter the alternate screen so that we aren't moving the cursor
            // around and overwriting the original terminal
            session_handle
                .data(channel_id, b"\x1b[?1049h".to_vec().into())
                .await
                .unwrap();

            let term =
                match tokio::time::timeout(std::time::Duration::from_millis(500), term_receiver)
                    .await
                {
                    Ok(Ok(term)) => Some(term),
                    Ok(Err(_)) => None,
                    Err(_) => {
                        tracing::info!("No pty_request received within 500ms, cleaning up");
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

            let (first_app_shortname, args, has_audio) = match tokio::time::timeout(
                std::time::Duration::from_millis(1),
                args_receiver,
            )
            .await
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
                    } else if let Some(pos) = args.iter().position(|&b| b.is_ascii_whitespace()) {
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

            let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(1);
            let (audio_tx, mut audio_rx) = tokio::sync::mpsc::channel(1);
            if admission_controller.should_require_captcha() {
                let captcha = generate_captcha(7);
                if !solve_captcha(
                    &session_handle,
                    channel_id,
                    &mut resize_rx,
                    &mut input_rx,
                    &captcha,
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
                &mut input_rx,
                &admission_ticket,
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
                input_receiver: input_rx,
                output_sender: output_tx,
                audio_sender: has_audio.then_some(audio_tx),
                remote_sshid,
                term,
                username,
                window_size_receiver: resize_rx,
                graceful_shutdown_token: token,
                network_info,
                user_id,
                locale,
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

                    data = audio_rx.recv(), if has_audio => {
                        let Some(data) = data else { break };
                        let _ = session_handle.extended_data(channel_id, SSH_EXTENDED_DATA_STDERR, data.into()).await;
                    }

                    data = output_rx.recv() => {
                        let Some(data) = data else { break };
                        let _ = session_handle.data(channel_id, data.into()).await;
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
) -> bool {
    let mut entered = String::with_capacity(captcha.len());
    loop {
        let window = *resize_rx.borrow();
        if render_captcha_screen(session_handle, channel_id, window, captcha, &entered)
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
        if render_loading_screen(session_handle, channel_id, window, status, frame)
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
) -> anyhow::Result<()> {
    let title = "Terminal Games";
    let prompt = "Type to continue";
    let hint = "Ctrl+C or q to quit";
    let subtle = format!("\x1b[2m{}\x1b[0m", &captcha[entered.len()..]);
    let captcha_line = format!("{}{}", entered, subtle);
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
            centered_col(width, title.chars().count()),
            title
        )
        .as_bytes(),
    );
    out.extend_from_slice(
        format!(
            "\x1b[{};{}H{}",
            prompt_row,
            centered_col(width, prompt.chars().count()),
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
            "\x1b[{};{}H\x1b[2m{}\x1b[0m",
            hint_row,
            centered_col(width, hint.chars().count()),
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
) -> anyhow::Result<()> {
    let title = "Terminal Games";
    let spinner_line = format!("{} Loading...", SPINNER_FRAMES[frame]);
    let queue_line = match status {
        AdmissionState::Allowed => "Starting app...".to_string(),
        AdmissionState::Queued(position) => format!("Position in queue: {}", position),
    };
    let center_row = if height == 0 { 1 } else { height.max(2) / 2 };
    let title_row = center_row.saturating_sub(1).max(1);
    let queue_row = center_row.saturating_add(1);
    let title_col = centered_col(width, title.chars().count());
    let spinner_col = centered_col(width, spinner_line.chars().count());
    let queue_col = centered_col(width, queue_line.chars().count());

    let mut out = Vec::new();
    out.extend_from_slice(b"\x1b[?25l\x1b[2J");
    out.extend_from_slice(format!("\x1b[{};{}H{}", title_row, title_col, title).as_bytes());
    out.extend_from_slice(format!("\x1b[{};{}H{}", center_row, spinner_col, spinner_line).as_bytes());
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

    async fn auth_none(&mut self, user: &str) -> Result<Auth, Self::Error> {
        if let Some(auth_sender) = self.auth.take() {
            let _ = auth_sender.send((user.to_string(), None));
        }
        Ok(Auth::Accept)
    }

    async fn auth_password(&mut self, user: &str, _password: &str) -> Result<Auth, Self::Error> {
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
