// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use terminal_games::{
    app::SessionEndReason,
    control::{NodeShutdownStatus, ShutdownPhase, StatusBarDrain},
};

use crate::{admission::AdmissionController, sessions::SessionRegistry};

#[derive(Clone)]
pub struct ShutdownCoordinator {
    command_tx: mpsc::UnboundedSender<ShutdownCommand>,
    status_rx: watch::Receiver<NodeShutdownStatus>,
    completion_token: CancellationToken,
    listener_token: CancellationToken,
}

enum ShutdownCommand {
    StartDrain {
        duration: Duration,
        response: oneshot::Sender<Result<NodeShutdownStatus, String>>,
    },
    CancelDrain {
        response: oneshot::Sender<Result<NodeShutdownStatus, String>>,
    },
    ShutdownNow {
        source: String,
        response: oneshot::Sender<Result<NodeShutdownStatus, String>>,
    },
}

struct ShutdownWorker {
    admission_controller: Arc<AdmissionController>,
    session_registry: Arc<SessionRegistry>,
    command_rx: mpsc::UnboundedReceiver<ShutdownCommand>,
    status_tx: watch::Sender<NodeShutdownStatus>,
    completion_token: CancellationToken,
    listener_token: CancellationToken,
}

impl ShutdownCoordinator {
    pub fn new(
        admission_controller: Arc<AdmissionController>,
        session_registry: Arc<SessionRegistry>,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (status_tx, status_rx) = watch::channel(NodeShutdownStatus {
            phase: ShutdownPhase::Running,
            accepting_new_sessions: true,
            deadline_unix_ms: None,
        });
        let completion_token = CancellationToken::new();
        let listener_token = CancellationToken::new();
        tokio::spawn(
            ShutdownWorker {
                admission_controller,
                session_registry,
                command_rx,
                status_tx,
                completion_token: completion_token.clone(),
                listener_token: listener_token.clone(),
            }
            .run(),
        );
        Self {
            command_tx,
            status_rx,
            completion_token,
            listener_token,
        }
    }

    pub fn listener_token(&self) -> CancellationToken {
        self.listener_token.clone()
    }

    pub fn snapshot(&self) -> NodeShutdownStatus {
        self.status_rx.borrow().clone()
    }

    pub async fn start_drain(&self, duration: Duration) -> Result<NodeShutdownStatus, String> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(ShutdownCommand::StartDrain {
                duration,
                response: response_tx,
            })
            .map_err(|_| "shutdown coordinator is unavailable".to_string())?;
        response_rx
            .await
            .map_err(|_| "shutdown coordinator dropped the response".to_string())?
    }

    pub async fn cancel_drain(&self) -> Result<NodeShutdownStatus, String> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(ShutdownCommand::CancelDrain {
                response: response_tx,
            })
            .map_err(|_| "shutdown coordinator is unavailable".to_string())?;
        response_rx
            .await
            .map_err(|_| "shutdown coordinator dropped the response".to_string())?
    }

    pub async fn begin_immediate_shutdown(
        &self,
        source: impl Into<String>,
    ) -> Result<NodeShutdownStatus, String> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(ShutdownCommand::ShutdownNow {
                source: source.into(),
                response: response_tx,
            })
            .map_err(|_| "shutdown coordinator is unavailable".to_string())?;
        response_rx
            .await
            .map_err(|_| "shutdown coordinator dropped the response".to_string())?
    }

    pub async fn wait_for_completion(&self) {
        self.completion_token.cancelled().await;
    }
}

impl ShutdownWorker {
    async fn run(mut self) {
        let mut deadline_sleep: Option<Pin<Box<tokio::time::Sleep>>> = None;
        loop {
            tokio::select! {
                Some(command) = self.command_rx.recv() => {
                    match self.handle_command(command, &mut deadline_sleep).await {
                        WorkerAction::Continue => {}
                        WorkerAction::Exit => break,
                    }
                }
                _ = async {
                    deadline_sleep.as_mut().expect("guarded by select").await;
                }, if deadline_sleep.is_some() => {
                    deadline_sleep = None;
                    self.force_drain_sessions("scheduled drain deadline reached".to_string()).await;
                }
                else => break,
            }
        }
    }

    async fn handle_command(
        &mut self,
        command: ShutdownCommand,
        deadline_sleep: &mut Option<Pin<Box<tokio::time::Sleep>>>,
    ) -> WorkerAction {
        match command {
            ShutdownCommand::StartDrain { duration, response } => {
                let current = self.status_tx.borrow().clone();
                if current.phase == ShutdownPhase::ShuttingDown {
                    let _ = response.send(Err("shutdown is already in progress".to_string()));
                    return WorkerAction::Continue;
                }

                let rejected = self
                    .admission_controller
                    .pause_new_sessions(SessionEndReason::ServerShutdown);

                if duration.is_zero() {
                    let snapshot = NodeShutdownStatus {
                        phase: ShutdownPhase::Draining,
                        accepting_new_sessions: false,
                        deadline_unix_ms: Some(current_unix_ms()),
                    };
                    self.set_status(snapshot.clone());
                    let _ = response.send(Ok(snapshot));
                    tracing::info!(
                        rejected_queued_sessions = rejected,
                        "Starting immediate drain"
                    );
                    self.force_drain_sessions(
                        "drain started with a zero-second deadline".to_string(),
                    )
                    .await;
                    return WorkerAction::Continue;
                }

                let deadline_unix_ms = current_unix_ms().saturating_add(
                    duration
                        .as_millis()
                        .min(i64::MAX as u128)
                        .try_into()
                        .unwrap_or(i64::MAX),
                );
                let snapshot = NodeShutdownStatus {
                    phase: ShutdownPhase::Draining,
                    accepting_new_sessions: false,
                    deadline_unix_ms: Some(deadline_unix_ms),
                };
                self.set_status(snapshot.clone());
                *deadline_sleep = Some(Box::pin(tokio::time::sleep(duration)));
                tracing::info!(
                    duration_seconds = duration.as_secs(),
                    rejected_queued_sessions = rejected,
                    "Started drain"
                );
                let _ = response.send(Ok(snapshot));
                WorkerAction::Continue
            }
            ShutdownCommand::CancelDrain { response } => {
                let current = self.status_tx.borrow().clone();
                let result = match current.phase {
                    ShutdownPhase::Running => Ok(current),
                    ShutdownPhase::Draining => {
                        *deadline_sleep = None;
                        self.admission_controller.resume_new_sessions();
                        let snapshot = NodeShutdownStatus {
                            phase: ShutdownPhase::Running,
                            accepting_new_sessions: true,
                            deadline_unix_ms: None,
                        };
                        self.set_status(snapshot.clone());
                        tracing::info!("Cancelled drain");
                        Ok(snapshot)
                    }
                    ShutdownPhase::ShuttingDown => {
                        Err("shutdown is already in progress and cannot be cancelled".to_string())
                    }
                };
                let _ = response.send(result);
                WorkerAction::Continue
            }
            ShutdownCommand::ShutdownNow { source, response } => {
                let current = self.status_tx.borrow().clone();
                if current.phase == ShutdownPhase::ShuttingDown {
                    let _ = response.send(Ok(current));
                    return WorkerAction::Continue;
                }
                let snapshot = NodeShutdownStatus {
                    phase: ShutdownPhase::ShuttingDown,
                    accepting_new_sessions: false,
                    deadline_unix_ms: Some(current_unix_ms()),
                };
                self.set_status(snapshot.clone());
                let _ = response.send(Ok(snapshot));
                self.begin_shutdown(source).await;
                WorkerAction::Exit
            }
        }
    }

    async fn begin_shutdown(&mut self, source: String) {
        let current = self.status_tx.borrow().clone();
        if current.phase != ShutdownPhase::ShuttingDown {
            let deadline_unix_ms = current.deadline_unix_ms.or(Some(current_unix_ms()));
            self.set_status(NodeShutdownStatus {
                phase: ShutdownPhase::ShuttingDown,
                accepting_new_sessions: false,
                deadline_unix_ms,
            });
        }
        self.listener_token.cancel();
        let rejected = self
            .admission_controller
            .pause_new_sessions(SessionEndReason::ServerShutdown);
        let requested = self
            .session_registry
            .request_close_all(SessionEndReason::ServerShutdown);
        tracing::info!(
            source,
            rejected_queued_sessions = rejected,
            requested_session_shutdowns = requested,
            active_sessions = self.session_registry.count(),
            "Beginning graceful shutdown"
        );
        self.session_registry.wait_for_zero().await;
        tracing::info!("Graceful shutdown complete");
        self.completion_token.cancel();
    }

    async fn force_drain_sessions(&mut self, source: String) {
        let requested = self
            .session_registry
            .request_close_all(SessionEndReason::ServerShutdown);
        tracing::info!(
            source,
            requested_session_shutdowns = requested,
            active_sessions = self.session_registry.count(),
            "Force-draining active sessions at drain deadline"
        );
    }

    fn set_status(&self, status: NodeShutdownStatus) {
        self.status_tx.send_replace(status.clone());
        self.session_registry
            .set_status_bar_drain(status_bar_drain(&status));
    }
}

enum WorkerAction {
    Continue,
    Exit,
}

fn current_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn status_bar_drain(status: &NodeShutdownStatus) -> Option<StatusBarDrain> {
    match status.phase {
        ShutdownPhase::Running => None,
        ShutdownPhase::Draining | ShutdownPhase::ShuttingDown => Some(StatusBarDrain {
            phase: status.phase,
            deadline_unix_ms: status.deadline_unix_ms,
        }),
    }
}
