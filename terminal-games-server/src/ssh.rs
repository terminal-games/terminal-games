// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use rand_core::{OsRng, RngCore};
use russh::keys::ssh_key::{self, PublicKey};
use russh::server::*;
use russh::{Channel, ChannelId, Pty};
use tokio_util::sync::CancellationToken;

use terminal_games::app::{AppInstantiationParams, AppServer, SessionControl, SessionEndReason};
use terminal_games::input_guard::InputGuard;
use terminal_games::log_backend::NoopLogBackend;
use terminal_games::palette;
use terminal_games::rate_limiting::{NetworkInformation, RateLimitedStream, TcpLatencyProvider};
use terminal_games::terminal_profile::TerminalProfile;

use crate::admission::{AdmissionController, AdmissionState, AdmissionTicket};
use crate::control::ControlPlane;
use crate::metrics::{AuthKind, Direction, ServerMetrics, Transport};
use crate::sessions::{FanoutTracker, SessionAdminControl, SessionRegistry};

pub struct SshSession {
    raw_input_tx: tokio::sync::mpsc::Sender<Bytes>,
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
    metrics: Arc<ServerMetrics>,
    session_registry: Arc<SessionRegistry>,
    control: ControlPlane,
}

const SSH_EXTENDED_DATA_STDERR: u32 = 1;
const SPINNER_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
const CAPTCHA_ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const RESTORE_TERMINAL_STATE: &[u8] =
    b"\x1b[0m\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1004l\x1b[?1005l\x1b[?1006l\x1b[?1015l\x1b[?2004l\x1b[?1l\x1b[>4;0m\x1b[<u\x1b>\x1b[?1049l\x1b[?25h";

impl SshServer {
    pub async fn new(
        app_server: Arc<AppServer>,
        admission_controller: Arc<AdmissionController>,
        metrics: Arc<ServerMetrics>,
        session_registry: Arc<SessionRegistry>,
        control: ControlPlane,
    ) -> anyhow::Result<Self> {
        tracing::info!("Initializing ssh server");

        Ok(Self {
            app_server,
            admission_controller,
            metrics,
            session_registry,
            control,
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
        addr: std::net::SocketAddr,
        network_info: Arc<NetworkInformation<TcpLatencyProvider>>,
    ) -> SshSession {
        let (auth_sender, auth_receiver) = tokio::sync::oneshot::channel::<(String, Option<u64>)>();
        let (term_sender, term_receiver) = tokio::sync::oneshot::channel::<String>();
        let (args_sender, args_receiver) = tokio::sync::oneshot::channel::<Vec<u8>>();
        let (ssh_session_sender, ssh_session_receiver) =
            tokio::sync::oneshot::channel::<(Handle, ChannelId, String)>();

        let cancellation_token = CancellationToken::new();
        let token = cancellation_token.clone();
        let shutdown_token = cancellation_token.clone();
        let (replay_request_tx, replay_request_rx) = tokio::sync::mpsc::channel(1);
        let (mut input_guard, input_rx, idle_fuel_rx) =
            InputGuard::new(cancellation_token.clone(), replay_request_tx.clone());
        let (raw_input_tx, mut raw_input_rx) = tokio::sync::mpsc::channel(12);
        let (resize_tx, mut resize_rx) = tokio::sync::watch::channel((0, 0));
        let app_server = self.app_server.clone();
        let admission_controller = self.admission_controller.clone();
        let metrics = self.metrics.clone();
        let session_registry = self.session_registry.clone();
        let control = self.control.clone();
        let client_ip = addr.ip();
        tokio::task::spawn(async move {
            let mut input_tick = InputGuard::tick_interval();
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
                match tokio::time::timeout(std::time::Duration::from_millis(100), args_receiver)
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
                        close_terminal_session(
                            &session_handle,
                            channel_id,
                            Some("Bad terminal or ping too high (>500ms)"),
                            1,
                        )
                        .await;
                        return;
                    }
                };
            let mut terminal_profile = TerminalProfile::from_term(term.as_deref(), None);
            if let Ok(Some(rgb)) = input_guard
                .wait_for_terminal_background(&mut raw_input_rx, Duration::from_millis(500))
                .await
            {
                terminal_profile = terminal_profile.with_background_rgb(rgb);
            }

            let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(1);
            let (audio_tx, mut audio_rx) = tokio::sync::mpsc::channel(8);
            if admission_controller.should_require_captcha() {
                let captcha = generate_captcha(7);
                let solved = solve_captcha(
                    &session_handle,
                    channel_id,
                    &mut resize_rx,
                    &mut raw_input_rx,
                    &mut input_guard,
                    &captcha,
                    terminal_profile,
                )
                .await;
                if !solved {
                    close_terminal_session(&session_handle, channel_id, Some("Bye!"), 1).await;
                    return;
                }
            }
            let admission_ticket = admission_controller.issue_ticket(Transport::Ssh, client_ip);
            match wait_for_admission(
                &session_handle,
                channel_id,
                &mut resize_rx,
                &mut raw_input_rx,
                &mut input_guard,
                &admission_ticket,
                terminal_profile,
            )
            .await
            {
                AdmissionWaitResult::Allowed => {}
                AdmissionWaitResult::Disconnected => {
                    close_terminal_session(&session_handle, channel_id, Some("Bye!"), 1).await;
                    return;
                }
                AdmissionWaitResult::Rejected(reason) => {
                    tracing::trace!(
                        client_ip = %client_ip,
                        reason = reason.slug(),
                        "Rejected SSH admission"
                    );
                    close_terminal_session(
                        &session_handle,
                        channel_id,
                        Some(reason.user_message()),
                        1,
                    )
                    .await;
                    return;
                }
            }
            let mut ban_changes = admission_controller.subscribe_ban_changes();
            if let Some(ban) = admission_controller.matching_ip_ban(client_ip) {
                tracing::warn!(
                    client_ip = %client_ip,
                    transport = Transport::Ssh.as_str(),
                    ban_rule = %ban.rule,
                    ban_reason = ban.reason.as_deref().unwrap_or("<none>"),
                    "Rejected client from active IP ban after admission"
                );
                close_terminal_session(
                    &session_handle,
                    channel_id,
                    Some(SessionEndReason::BannedIp.user_message()),
                    1,
                )
                .await;
                return;
            }
            let _ = session_handle
                .data(channel_id, b"\x1b[2J\x1b[H".to_vec().into())
                .await;
            let local_session_id = admission_ticket.id();
            let (first_cols, first_rows) = *resize_rx.borrow();
            let session_registration = session_registry.register(
                local_session_id,
                user_id,
                username.clone(),
                client_ip,
                Transport::Ssh,
                first_app_shortname.clone(),
                first_cols,
                first_rows,
            );
            let mut admin_control = session_registration.control_rx;
            let mut admin_input_rx = session_registration.admin_input_rx;
            let mut spy_resize_rx = resize_rx.clone();
            let session_guard = metrics.start_session(
                Transport::Ssh,
                if user_id.is_some() {
                    AuthKind::Authenticated
                } else {
                    AuthKind::Anonymous
                },
                has_audio,
                user_id,
            );
            let active_shortname_tracker = FanoutTracker::new(vec![
                session_guard.active_shortname_tracker(),
                session_registration.tracker,
            ]);
            let mut admitted_session = admission_ticket.start_session(session_guard);
            let mut cluster_control = admitted_session.subscribe_control();
            let terminal_parser = input_guard.take_terminal_parser(first_rows, first_cols);

            let mut exit_rx = app_server.instantiate_app(AppInstantiationParams {
                first_app_shortname,
                args,
                input_receiver: input_rx,
                replay_request_receiver: replay_request_rx,
                output_sender: output_tx,
                audio_sender: has_audio.then_some(audio_tx),
                remote_sshid,
                term,
                username,
                window_size_receiver: resize_rx,
                graceful_shutdown_token: token,
                network_info,
                terminal_profile,
                terminal_parser,
                user_id,
                locale,
                log_backend: Arc::new(NoopLogBackend),
                active_shortname_tracker: Some(active_shortname_tracker),
                idle_fuel_receiver: Some(idle_fuel_rx),
            });
            let close_reason = loop {
                tokio::select! {
                    biased;

                    exit_code = &mut exit_rx => {
                        match exit_code {
                            Ok(Ok(exit_code)) => {
                                tracing::trace!(?exit_code, "App exited");
                            }
                            Ok(Err(error)) => {
                                tracing::error!(error = %error, "App failed");
                            }
                            Err(error) => {
                                tracing::error!(?error, "App exit channel dropped");
                            }
                        }
                        if input_guard.is_idle_timed_out() {
                            break SessionEndReason::IdleTimeout;
                        }
                        break SessionEndReason::NormalExit;
                    }

                    changed = cluster_control.changed() => {
                        if changed.is_err() {
                            continue;
                        }
                        let SessionControl::Close(reason) = *cluster_control.borrow() else {
                            continue;
                        };
                        shutdown_token.cancel();
                        break reason;
                    }

                    changed = ban_changes.changed() => {
                        if changed.is_err() || !admission_controller.is_ip_banned(client_ip) {
                            continue;
                        }
                        shutdown_token.cancel();
                        break SessionEndReason::BannedIp;
                    }

                    changed = admin_control.changed() => {
                        if changed.is_err() {
                            continue;
                        }
                        let SessionAdminControl::Kick = *admin_control.borrow() else {
                            continue;
                        };
                        shutdown_token.cancel();
                        break SessionEndReason::KickedByAdmin;
                    }

                    data = raw_input_rx.recv() => {
                        let Some(data) = data else {
                            break SessionEndReason::ConnectionLost;
                        };
                        admitted_session.record_input(&data);
                        control.record_bytes(data.len());
                        session_registry.record_input(local_session_id, &data);
                        let _ = input_guard.prepare_input(data).try_send();
                    }

                    data = admin_input_rx.recv() => {
                        let Some(data) = data else {
                            continue;
                        };
                        admitted_session.record_input(&data);
                        let _ = input_guard.prepare_input(data).try_send();
                    }

                    _ = input_tick.tick() => {
                        input_guard.tick();
                    }

                    changed = spy_resize_rx.changed() => {
                        if changed.is_err() {
                            continue;
                        }
                        let (cols, rows) = *spy_resize_rx.borrow_and_update();
                        session_registry.record_resize(local_session_id, cols, rows);
                    }

                    data = audio_rx.recv(), if has_audio => {
                        let Some(data) = data else {
                            break SessionEndReason::NormalExit;
                        };
                        admitted_session.record_output(data.len());
                        metrics.record_bytes(Direction::Out, Transport::Ssh, data.len());
                        control.record_bytes(data.len());
                        if session_handle.extended_data(channel_id, SSH_EXTENDED_DATA_STDERR, russh::CryptoVec::from_slice(&data)).await.is_err() {
                            break SessionEndReason::ConnectionLost;
                        }
                    }

                    data = output_rx.recv() => {
                        let Some(data) = data else {
                            break SessionEndReason::NormalExit;
                        };
                        admitted_session.record_output(data.len());
                        metrics.record_bytes(Direction::Out, Transport::Ssh, data.len());
                        control.record_bytes(data.len());
                        session_registry.record_output(local_session_id, &data);
                        if session_handle.data(channel_id, russh::CryptoVec::from_slice(&data)).await.is_err() {
                            break SessionEndReason::ConnectionLost;
                        }
                    }
                }
            };
            session_registry.finish(local_session_id, close_reason);
            session_registry.remove(local_session_id);
            close_terminal_session(
                &session_handle,
                channel_id,
                (close_reason != SessionEndReason::NormalExit)
                    .then_some(close_reason.user_message()),
                if close_reason == SessionEndReason::NormalExit {
                    0
                } else {
                    1
                },
            )
            .await;
        });

        SshSession {
            cancellation_token,
            raw_input_tx,
            resize_tx,
            auth: Some(auth_sender),
            term: Some(term_sender),
            args: Some(args_sender),
            ssh_session: Some(ssh_session_sender),
            server: self.clone(),
        }
    }
}

async fn restore_terminal_state(session_handle: &Handle, channel_id: ChannelId) {
    let _ = session_handle
        .data(channel_id, RESTORE_TERMINAL_STATE.to_vec().into())
        .await;
}

async fn close_terminal_session(
    session_handle: &Handle,
    channel_id: ChannelId,
    reason: Option<&str>,
    exit_status: u32,
) {
    restore_terminal_state(session_handle, channel_id).await;
    if let Some(reason) = reason {
        let mut out = Vec::with_capacity(reason.len() + 4);
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(reason.as_bytes());
        out.extend_from_slice(b"\r\n");
        let _ = session_handle.data(channel_id, out.into()).await;
    }
    let _ = session_handle.exit_status_request(channel_id, exit_status).await;
    let _ = session_handle.eof(channel_id).await;
    tokio::time::sleep(Duration::from_millis(40)).await;
    let _ = session_handle.close(channel_id).await;
    tokio::time::sleep(Duration::from_millis(40)).await;
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
    raw_input_rx: &mut tokio::sync::mpsc::Receiver<Bytes>,
    input_guard: &mut InputGuard,
    captcha: &str,
    base_terminal_profile: TerminalProfile,
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
            input_guard.terminal_profile(base_terminal_profile),
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
            data = raw_input_rx.recv() => {
                let Some(data) = data else { return false; };
                let pending = input_guard.prepare_input(data.clone());
                drop(pending);
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

enum AdmissionWaitResult {
    Allowed,
    Disconnected,
    Rejected(SessionEndReason),
}

async fn wait_for_admission(
    session_handle: &Handle,
    channel_id: ChannelId,
    resize_rx: &mut tokio::sync::watch::Receiver<(u16, u16)>,
    raw_input_rx: &mut tokio::sync::mpsc::Receiver<Bytes>,
    input_guard: &mut InputGuard,
    admission_ticket: &AdmissionTicket,
    base_terminal_profile: TerminalProfile,
) -> AdmissionWaitResult {
    let mut updates = admission_ticket.subscribe();
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
    let mut frame = 0usize;
    let mut status = *updates.borrow();
    loop {
        match status {
            AdmissionState::Allowed => return AdmissionWaitResult::Allowed,
            AdmissionState::Rejected(reason) => return AdmissionWaitResult::Rejected(reason),
            AdmissionState::Queued(_) => {}
        }
        let window = *resize_rx.borrow();
        if render_loading_screen(
            session_handle,
            channel_id,
            window,
            status,
            frame,
            input_guard.terminal_profile(base_terminal_profile),
        )
        .await
        .is_err()
        {
            return AdmissionWaitResult::Disconnected;
        }
        frame = (frame + 1) % SPINNER_FRAMES.len();
        tokio::select! {
            _ = tick.tick() => {}
            changed = updates.changed() => {
                if changed.is_err() {
                    return AdmissionWaitResult::Disconnected;
                }
                status = *updates.borrow();
            }
            changed = resize_rx.changed() => {
                if changed.is_err() {
                    return AdmissionWaitResult::Disconnected;
                }
            }
            data = raw_input_rx.recv() => {
                let Some(data) = data else { return AdmissionWaitResult::Disconnected; };
                let pending = input_guard.prepare_input(data.clone());
                drop(pending);
                if data.contains(&0x03) || data.contains(&b'q') {
                    return AdmissionWaitResult::Disconnected;
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
        AdmissionState::Rejected(reason) => reason.user_message().to_string(),
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
        self.server
            .metrics
            .record_bytes(Direction::In, Transport::Ssh, data.len());
        let _ = self.raw_input_tx.try_send(Bytes::copy_from_slice(data));

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
