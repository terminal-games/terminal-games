// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::watch;

use crate::metrics::{ServerMetrics, Transport};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionState {
    Allowed,
    Queued(usize),
    Rejected(AdmissionRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionRejection {
    BannedIp,
    TooManyConnectionsFromIp,
}

impl AdmissionRejection {
    pub fn slug(self) -> &'static str {
        match self {
            Self::BannedIp => "banned_ip",
            Self::TooManyConnectionsFromIp => "too_many_connections_from_ip",
        }
    }

    pub fn user_message(self) -> &'static str {
        match self {
            Self::BannedIp => "Connections from your IP address are blocked",
            Self::TooManyConnectionsFromIp => "Too many active sessions from your IP address",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AdmissionConfig {
    pub max_running: usize,
    pub max_running_per_ip: usize,
    pub max_queued_per_ip: usize,
}

#[derive(Clone)]
pub struct AdmissionController {
    inner: Arc<Inner>,
}

struct Inner {
    config: AdmissionConfig,
    banned_ips: Mutex<HashMap<IpAddr, Option<i64>>>,
    ban_changes: watch::Sender<()>,
    metrics: Arc<ServerMetrics>,
    next_id: AtomicU64,
    state: Mutex<ControllerState>,
}

#[derive(Default)]
struct ControllerState {
    running: usize,
    running_by_ip: HashMap<IpAddr, usize>,
    queue: VecDeque<QueuedTicket>,
    queued_by_ip: HashMap<IpAddr, usize>,
}

impl ControllerState {
    fn enqueue(
        &mut self,
        id: u64,
        client_ip: IpAddr,
        transport: Transport,
        tx: watch::Sender<AdmissionState>,
    ) {
        let _ = tx.send(AdmissionState::Queued(self.queue.len() + 1));
        *self.queued_by_ip.entry(client_ip).or_default() += 1;
        self.queue.push_back(QueuedTicket {
            id,
            client_ip,
            transport,
            tx,
        });
    }

    fn dequeue_admissible(&mut self, max_running_per_ip: usize) -> Option<QueuedTicket> {
        let idx = self
            .queue
            .iter()
            .position(|ticket| self.running_for_ip(ticket.client_ip) < max_running_per_ip)?;
        let ticket = self.queue.remove(idx)?;
        decrement_ip_count(&mut self.queued_by_ip, ticket.client_ip);
        Some(ticket)
    }

    fn remove_queued(&mut self, id: u64) -> Option<(usize, Transport)> {
        let idx = self.queue.iter().position(|queued| queued.id == id)?;
        if let Some(ticket) = self.queue.remove(idx) {
            decrement_ip_count(&mut self.queued_by_ip, ticket.client_ip);
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
        let (ssh, web) =
            self.queue
                .iter()
                .fold((0, 0), |(ssh, web), ticket| match ticket.transport {
                    Transport::Ssh => (ssh + 1, web),
                    Transport::Web => (ssh, web + 1),
                });
        metrics.record_admission_state(ssh, web);
    }

    fn running_for_ip(&self, ip: IpAddr) -> usize {
        self.running_by_ip.get(&ip).copied().unwrap_or(0)
    }

    fn queued_for_ip(&self, ip: IpAddr) -> usize {
        self.queued_by_ip.get(&ip).copied().unwrap_or(0)
    }
}

struct QueuedTicket {
    id: u64,
    client_ip: IpAddr,
    transport: Transport,
    tx: watch::Sender<AdmissionState>,
}

pub struct AdmissionTicket {
    id: u64,
    client_ip: IpAddr,
    rx: watch::Receiver<AdmissionState>,
    controller: Arc<Inner>,
}

#[derive(Debug, Clone, Copy)]
pub struct BanUpdateSummary {
    pub activated: usize,
    pub deactivated: usize,
    pub evicted_from_queue: usize,
    pub active_ban_count: usize,
}

impl AdmissionController {
    pub fn new(
        config: AdmissionConfig,
        initial_banned_ips: HashMap<IpAddr, Option<i64>>,
        metrics: Arc<ServerMetrics>,
    ) -> Self {
        let (ban_changes, _) = watch::channel(());
        Self {
            inner: Arc::new(Inner {
                config,
                banned_ips: Mutex::new(initial_banned_ips),
                ban_changes,
                metrics,
                next_id: AtomicU64::new(1),
                state: Mutex::new(ControllerState::default()),
            }),
        }
    }

    pub fn issue_ticket(&self, transport: Transport, client_ip: IpAddr) -> AdmissionTicket {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = watch::channel(AdmissionState::Queued(1));
        let is_banned = self.is_ip_banned(client_ip);

        {
            let mut state = match self.inner.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            if is_banned {
                tracing::debug!(
                    client_ip = %client_ip,
                    transport = transport.as_str(),
                    running = state.running,
                    running_for_ip = state.running_for_ip(client_ip),
                    queued_for_ip = state.queued_for_ip(client_ip),
                    "Rejected client from banned IP"
                );
                let _ = tx.send(AdmissionState::Rejected(AdmissionRejection::BannedIp));
            } else if state.running < self.inner.config.max_running
                && state.running_for_ip(client_ip) < self.inner.config.max_running_per_ip
            {
                bump_count(&mut state.running_by_ip, client_ip, 1);
                state.running += 1;
                let _ = tx.send(AdmissionState::Allowed);
            } else if state.queued_for_ip(client_ip) < self.inner.config.max_queued_per_ip {
                state.enqueue(id, client_ip, transport, tx);
            } else {
                tracing::debug!(
                    client_ip = %client_ip,
                    transport = transport.as_str(),
                    running = state.running,
                    max_running = self.inner.config.max_running,
                    running_for_ip = state.running_for_ip(client_ip),
                    max_running_per_ip = self.inner.config.max_running_per_ip,
                    queued_for_ip = state.queued_for_ip(client_ip),
                    max_queued_per_ip = self.inner.config.max_queued_per_ip,
                    "Rejected client due to per-IP admission limit"
                );
                let _ = tx.send(AdmissionState::Rejected(
                    AdmissionRejection::TooManyConnectionsFromIp,
                ));
            }
            state.record_metrics(&self.inner.metrics);
        }

        AdmissionTicket {
            id,
            client_ip,
            rx,
            controller: self.inner.clone(),
        }
    }

    pub fn should_require_captcha(&self) -> bool {
        let state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.running.saturating_mul(2) >= self.inner.config.max_running
    }

    pub fn subscribe_ban_changes(&self) -> watch::Receiver<()> {
        self.inner.ban_changes.subscribe()
    }

    pub fn is_ip_banned(&self, client_ip: IpAddr) -> bool {
        let mut banned_ips = match self.inner.banned_ips.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        match banned_ips.get(&client_ip).copied() {
            Some(expires_at) if is_ban_active(expires_at, current_unix_seconds()) => true,
            Some(_) => {
                banned_ips.remove(&client_ip);
                false
            }
            None => false,
        }
    }

    pub fn apply_ban_updates(&self, updates: Vec<(IpAddr, Option<i64>)>) -> BanUpdateSummary {
        let now = current_unix_seconds();
        let mut newly_banned = HashSet::new();
        let mut deactivated = 0usize;
        let mut activated = 0usize;
        let active_ban_count;
        {
            let mut current = match self.inner.banned_ips.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            for (ip, expires_at) in updates {
                let was_active = current
                    .get(&ip)
                    .copied()
                    .is_some_and(|expires_at| is_ban_active(expires_at, now));
                if is_ban_active(expires_at, now) {
                    current.insert(ip, expires_at);
                    if !was_active {
                        newly_banned.insert(ip);
                        activated += 1;
                    }
                } else {
                    deactivated += usize::from(current.remove(&ip).is_some());
                }
            }
            active_ban_count = current.len();
        }

        if newly_banned.is_empty() {
            return BanUpdateSummary {
                activated,
                deactivated,
                evicted_from_queue: 0,
                active_ban_count,
            };
        }
        let _ = self.inner.ban_changes.send(());
        let mut state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        let mut idx = 0usize;
        let mut removed_any = false;
        let mut evicted_from_queue = 0usize;
        while idx < state.queue.len() {
            if newly_banned.contains(&state.queue[idx].client_ip) {
                let ticket = state.queue.remove(idx).expect("index checked against len");
                decrement_ip_count(&mut state.queued_by_ip, ticket.client_ip);
                let _ = ticket
                    .tx
                    .send(AdmissionState::Rejected(AdmissionRejection::BannedIp));
                removed_any = true;
                evicted_from_queue += 1;
                continue;
            }
            idx += 1;
        }
        if removed_any {
            state.refresh_queue_positions(0);
            state.record_metrics(&self.inner.metrics);
        }
        BanUpdateSummary {
            activated,
            deactivated,
            evicted_from_queue,
            active_ban_count,
        }
    }
}

impl AdmissionTicket {
    pub fn subscribe(&self) -> watch::Receiver<AdmissionState> {
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
                state.running = state.running.saturating_sub(1);
                decrement_ip_count(&mut state.running_by_ip, self.client_ip);
                if let Some(next) =
                    state.dequeue_admissible(self.controller.config.max_running_per_ip)
                {
                    state.running += 1;
                    bump_count(&mut state.running_by_ip, next.client_ip, 1);
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
            AdmissionState::Rejected(_) => {}
        }
        state.record_metrics(&self.controller.metrics);
    }
}

fn decrement_ip_count(counts: &mut HashMap<IpAddr, usize>, ip: IpAddr) {
    let Some(count) = counts.get_mut(&ip) else {
        return;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        counts.remove(&ip);
    }
}

fn bump_count(counts: &mut HashMap<IpAddr, usize>, ip: IpAddr, delta: usize) {
    *counts.entry(ip).or_default() += delta;
}

fn is_ban_active(expires_at: Option<i64>, now: i64) -> bool {
    expires_at.is_none_or(|expires_at| expires_at > now)
}

fn current_unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
