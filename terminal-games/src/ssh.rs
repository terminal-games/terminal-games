// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::os::fd::AsRawFd;
use std::sync::Arc;

use rand_core::OsRng;
use russh::keys::ssh_key::{self, PublicKey};
use russh::server::*;
use russh::{Channel, ChannelId, Pty};
use tokio_util::sync::CancellationToken;

use crate::app::{AppInstantiationParams, AppServer};
use crate::rate_limiting::{NetworkInformation, RateLimitedStream};

pub struct SshSession {
    input_sender: tokio::sync::mpsc::Sender<smallvec::SmallVec<[u8; 16]>>,
    resize_tx: tokio::sync::mpsc::Sender<(u16, u16)>,
    username: Option<tokio::sync::oneshot::Sender<String>>,
    term: Option<tokio::sync::oneshot::Sender<String>>,
    args: Option<tokio::sync::oneshot::Sender<Vec<u8>>>,
    ssh_session: Option<tokio::sync::oneshot::Sender<(Handle, ChannelId, String)>>,
    cancellation_token: CancellationToken,
    server: SshServer,
}

#[derive(Clone)]
pub(crate) struct SshServer {
    app_server: Arc<AppServer>,
}

const SSH_EXTENDED_DATA_STDERR: u32 = 1;

impl SshServer {
    pub async fn new(app_server: Arc<AppServer>) -> anyhow::Result<Self> {
        tracing::info!("Initializing ssh server");

        Ok(Self { app_server })
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

        // Ok(())
    }

    fn new_client(
        &self,
        addr: std::net::SocketAddr,
        network_info: Arc<NetworkInformation>,
    ) -> SshSession {
        tracing::info!(addr=?addr, "new_client");

        let (username_sender, username_receiver) = tokio::sync::oneshot::channel::<String>();
        let (term_sender, term_receiver) = tokio::sync::oneshot::channel::<String>();
        let (args_sender, args_receiver) = tokio::sync::oneshot::channel::<Vec<u8>>();
        let (ssh_session_sender, ssh_session_receiver) =
            tokio::sync::oneshot::channel::<(Handle, ChannelId, String)>();

        let (input_tx, input_rx) = tokio::sync::mpsc::channel(20);
        let (resize_tx, resize_rx) = tokio::sync::mpsc::channel(1);
        let cancellation_token = CancellationToken::new();
        let token = cancellation_token.clone();
        let app_server = self.app_server.clone();

        tokio::task::spawn(async move {
            let (session_handle, channel_id, remote_sshid) = ssh_session_receiver.await.unwrap();
            let username = username_receiver.await.unwrap();

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

            let args = match tokio::time::timeout(
                std::time::Duration::from_millis(1),
                args_receiver,
            )
            .await
            {
                Ok(Ok(args)) => Some(args),
                _ => None,
            };

            let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(1);
            let (audio_tx, mut audio_rx) = tokio::sync::mpsc::channel(1);
            let mut exit_rx = app_server.instantiate_app(AppInstantiationParams {
                args,
                input_receiver: input_rx,
                output_sender: output_tx,
                audio_sender: audio_tx,
                remote_sshid,
                term,
                username,
                window_size_receiver: resize_rx,
                graceful_shutdown_token: token,
                network_info,
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

                    data = output_rx.recv() => {
                        let Some(data) = data else { break };
                        let _ = session_handle.data(channel_id, data.into()).await;
                    }

                    data = audio_rx.recv() => {
                        let Some(data) = data else { break };
                        let _ = session_handle.extended_data(channel_id, SSH_EXTENDED_DATA_STDERR, data.into()).await;
                    }
                }
            }

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
            username: Some(username_sender),
            term: Some(term_sender),
            args: Some(args_sender),
            ssh_session: Some(ssh_session_sender),
            server: self.clone(),
        }
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
        tracing::info!(user, "auth_publickey");
        if let Some(username_sender) = self.username.take() {
            tracing::info!(user, "auth_publickey send");
            let _ = username_sender.send(user.to_string());
        }

        let mut rows = self
            .server
            .app_server
            .db
            .query(
                "
                INSERT INTO users (pubkey_fingerprint, username) VALUES (?1, ?2)
                ON CONFLICT DO UPDATE SET username = ?2
                RETURNING id
            ",
                libsql::params!(pubkey.fingerprint(Default::default()).as_bytes(), user),
            )
            .await
            .unwrap();
        let user_id: u64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
        _ = user_id;

        // let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        // self.drop_sender = Some(tx);
        // let db = self.db.clone();
        // tokio::task::spawn(async move {
        //     if let Ok(_) = rx.await {
        //         let _ = db
        //             .execute(
        //                 "UPDATE users SET session_time = session_time + 1 WHERE id = ?1",
        //                 [user_id],
        //             )
        //             .await
        //             .unwrap();
        //     }
        // });

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
        let _ = self
            .resize_tx
            .send((col_width as u16, row_height as u16))
            .await;
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
        let _ = self
            .resize_tx
            .send((col_width as u16, row_height as u16))
            .await;
        if let Some(term_sender) = self.term.take() {
            let _ = term_sender.send(term.to_string());
        }
        session.channel_success(channel)?;
        Ok(())
    }
}
