// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

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
    next_id: AtomicU64,
    state: Mutex<ControllerState>,
}

#[derive(Default)]
struct ControllerState {
    running: usize,
    queue: VecDeque<QueuedTicket>,
}

struct QueuedTicket {
    id: u64,
    tx: watch::Sender<AdmissionState>,
}

pub struct AdmissionTicket {
    id: u64,
    rx: watch::Receiver<AdmissionState>,
    controller: Arc<Inner>,
}

impl AdmissionController {
    pub fn new(max_running: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                max_running,
                next_id: AtomicU64::new(1),
                state: Mutex::new(ControllerState::default()),
            }),
        }
    }

    pub fn issue_ticket(&self) -> AdmissionTicket {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = watch::channel(AdmissionState::Queued(1));

        {
            let mut state = self.inner.state.lock().unwrap();
            if state.running < self.inner.max_running {
                state.running += 1;
                let _ = tx.send(AdmissionState::Allowed);
            } else {
                let position = state.queue.len() + 1;
                let _ = tx.send(AdmissionState::Queued(position));
                state.queue.push_back(QueuedTicket { id, tx });
            }
        }

        AdmissionTicket {
            id,
            rx,
            controller: self.inner.clone(),
        }
    }

    pub fn should_require_captcha(&self) -> bool {
        let state = self.inner.state.lock().unwrap();
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
        let mut state = self.controller.state.lock().unwrap();
        match *self.rx.borrow() {
            AdmissionState::Allowed => {
                if state.running > 0 {
                    state.running -= 1;
                }
                if let Some(next) = state.queue.pop_front() {
                    state.running += 1;
                    let _ = next.tx.send(AdmissionState::Allowed);
                    for (idx, queued) in state.queue.iter_mut().enumerate() {
                        let _ = queued.tx.send(AdmissionState::Queued(idx + 1));
                    }
                }
            }
            AdmissionState::Queued(_) => {
                if let Some(idx) = state.queue.iter().position(|queued| queued.id == self.id) {
                    state.queue.remove(idx);
                    for (next_idx, queued) in state.queue.iter_mut().enumerate().skip(idx) {
                        let _ = queued.tx.send(AdmissionState::Queued(next_idx + 1));
                    }
                }
            }
        }
    }
}
