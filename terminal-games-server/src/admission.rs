// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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
    queue: VecDeque<QueuedTicket>,
    queued: TransportCounts,
}

#[derive(Default)]
struct TransportCounts {
    ssh: usize,
    web: usize,
}

impl TransportCounts {
    fn increment(&mut self, transport: Transport) {
        *self.get_mut(transport) += 1;
    }

    fn decrement(&mut self, transport: Transport) {
        let count = self.get_mut(transport);
        *count = count.saturating_sub(1);
    }

    fn get_mut(&mut self, transport: Transport) -> &mut usize {
        match transport {
            Transport::Ssh => &mut self.ssh,
            Transport::Web => &mut self.web,
        }
    }
}

impl ControllerState {
    fn increment_running(&mut self) {
        self.running += 1;
    }

    fn decrement_running(&mut self) {
        self.running = self.running.saturating_sub(1);
    }

    fn increment_queued(&mut self, transport: Transport) {
        self.queued.increment(transport);
    }

    fn decrement_queued(&mut self, transport: Transport) {
        self.queued.decrement(transport);
    }

    fn enqueue(&mut self, id: u64, transport: Transport, tx: watch::Sender<AdmissionState>) {
        let _ = tx.send(AdmissionState::Queued(self.queue.len() + 1));
        self.increment_queued(transport);
        self.queue.push_back(QueuedTicket {
            id,
            transport,
            tx,
        });
    }

    fn dequeue(&mut self) -> Option<QueuedTicket> {
        let ticket = self.queue.pop_front()?;
        self.decrement_queued(ticket.transport);
        Some(ticket)
    }

    fn remove_queued(&mut self, id: u64) -> Option<(usize, Transport)> {
        let idx = self.queue.iter().position(|queued| queued.id == id)?;
        if let Some(ticket) = self.queue.remove(idx) {
            self.decrement_queued(ticket.transport);
            return Some((idx, ticket.transport));
        }
        None
    }

    fn refresh_queue_positions(&mut self, start: usize) {
        for (idx, queued) in self.queue.iter_mut().enumerate().skip(start) {
            let _ = queued.tx.send(AdmissionState::Queued(idx + 1));
        }
    }

    fn record_metrics(&self, metrics: &ServerMetrics) {
        metrics.record_admission_state(self.queued.ssh, self.queued.web);
    }
}

struct QueuedTicket {
    id: u64,
    transport: Transport,
    tx: watch::Sender<AdmissionState>,
}

pub struct AdmissionTicket {
    id: u64,
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
                state.increment_running();
                let _ = tx.send(AdmissionState::Allowed);
            } else {
                state.enqueue(id, transport, tx);
            }
            state.record_metrics(&self.inner.metrics);
        }

        AdmissionTicket {
            id,
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
                state.decrement_running();
                if let Some(next) = state.dequeue() {
                    state.increment_running();
                    let _ = next.tx.send(AdmissionState::Allowed);
                    self.controller
                        .metrics
                        .record_admission_joined_from_queue(next.transport);
                    state.refresh_queue_positions(0);
                }
            }
            AdmissionState::Queued(_) => {
                if let Some((idx, transport)) = state.remove_queued(self.id) {
                    self.controller
                        .metrics
                        .record_admission_abandoned_queue(transport);
                    state.refresh_queue_positions(idx);
                }
            }
        }
        state.record_metrics(&self.controller.metrics);
    }
}
