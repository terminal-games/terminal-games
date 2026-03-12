// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::metrics::{ServerMetrics, Transport};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionState {
    Allowed,
    Queued(usize),
}

#[derive(Clone)]
pub struct AdmissionController {
    inner: Arc<Inner>,
}

struct Inner {
    max_running: usize,
    metrics: Arc<ServerMetrics>,
    next_id: AtomicU64,
    state: Mutex<ControllerState>,
}

#[derive(Default)]
struct ControllerState {
    running: usize,
    running_ssh: usize,
    running_web: usize,
    queue: VecDeque<QueuedTicket>,
    queued_ssh: usize,
    queued_web: usize,
}

struct QueuedTicket {
    id: u64,
    transport: Transport,
    queued_at: Instant,
    tx: watch::Sender<AdmissionState>,
}

pub struct AdmissionTicket {
    id: u64,
    transport: Transport,
    rx: watch::Receiver<AdmissionState>,
    controller: Arc<Inner>,
}

impl AdmissionController {
    pub fn new(max_running: usize, metrics: Arc<ServerMetrics>) -> Self {
        Self {
            inner: Arc::new(Inner {
                max_running,
                metrics,
                next_id: AtomicU64::new(1),
                state: Mutex::new(ControllerState::default()),
            }),
        }
    }

    pub fn issue_ticket(&self, transport: Transport) -> AdmissionTicket {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = watch::channel(AdmissionState::Queued(1));

        {
            let mut state = match self.inner.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            if state.running < self.inner.max_running {
                state.running += 1;
                increment_running_for_transport(&mut state, transport);
                let _ = tx.send(AdmissionState::Allowed);
                self.inner
                    .metrics
                    .record_admission_wait(transport, Duration::ZERO);
            } else {
                let position = state.queue.len() + 1;
                let _ = tx.send(AdmissionState::Queued(position));
                increment_queued_for_transport(&mut state, transport);
                state.queue.push_back(QueuedTicket {
                    id,
                    transport,
                    queued_at: Instant::now(),
                    tx,
                });
            }
            self.inner.metrics.record_admission_state(
                state.running_ssh,
                state.running_web,
                state.queued_ssh,
                state.queued_web,
            );
        }

        AdmissionTicket {
            id,
            transport,
            rx,
            controller: self.inner.clone(),
        }
    }

    pub fn should_require_captcha(&self) -> bool {
        let state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.running.saturating_mul(2) >= self.inner.max_running
    }
}

impl AdmissionTicket {
    pub async fn subscribe(&self) -> watch::Receiver<AdmissionState> {
        self.rx.clone()
    }
}

impl Drop for AdmissionTicket {
    fn drop(&mut self) {
        let mut state = match self.controller.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        match *self.rx.borrow() {
            AdmissionState::Allowed => {
                if state.running > 0 {
                    state.running -= 1;
                }
                decrement_running_for_transport(&mut state, self.transport);
                if let Some(next) = state.queue.pop_front() {
                    state.running += 1;
                    decrement_queued_for_transport(&mut state, next.transport);
                    increment_running_for_transport(&mut state, next.transport);
                    let _ = next.tx.send(AdmissionState::Allowed);
                    self.controller
                        .metrics
                        .record_admission_wait(next.transport, next.queued_at.elapsed());
                    for (idx, queued) in state.queue.iter_mut().enumerate() {
                        let _ = queued.tx.send(AdmissionState::Queued(idx + 1));
                    }
                }
            }
            AdmissionState::Queued(_) => {
                if let Some(idx) = state.queue.iter().position(|queued| queued.id == self.id) {
                    state.queue.remove(idx);
                    decrement_queued_for_transport(&mut state, self.transport);
                    for (next_idx, queued) in state.queue.iter_mut().enumerate().skip(idx) {
                        let _ = queued.tx.send(AdmissionState::Queued(next_idx + 1));
                    }
                }
            }
        }
        self.controller.metrics.record_admission_state(
            state.running_ssh,
            state.running_web,
            state.queued_ssh,
            state.queued_web,
        );
    }
}

fn increment_running_for_transport(state: &mut ControllerState, transport: Transport) {
    match transport {
        Transport::Ssh => state.running_ssh += 1,
        Transport::Web => state.running_web += 1,
    }
}

fn decrement_running_for_transport(state: &mut ControllerState, transport: Transport) {
    match transport {
        Transport::Ssh => {
            if state.running_ssh > 0 {
                state.running_ssh -= 1;
            }
        }
        Transport::Web => {
            if state.running_web > 0 {
                state.running_web -= 1;
            }
        }
    }
}

fn increment_queued_for_transport(state: &mut ControllerState, transport: Transport) {
    match transport {
        Transport::Ssh => state.queued_ssh += 1,
        Transport::Web => state.queued_web += 1,
    }
}

fn decrement_queued_for_transport(state: &mut ControllerState, transport: Transport) {
    match transport {
        Transport::Ssh => {
            if state.queued_ssh > 0 {
                state.queued_ssh -= 1;
            }
        }
        Transport::Web => {
            if state.queued_web > 0 {
                state.queued_web -= 1;
            }
        }
    }
}
