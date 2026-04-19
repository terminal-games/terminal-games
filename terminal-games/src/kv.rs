// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::sync::{Arc, Mutex as StdMutex};

use anyhow::Context as _;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[path = "kv/libsql.rs"]
mod libsql;
#[path = "kv/mesh.rs"]
mod mesh;

pub use libsql::{LibsqlKvBackendOptions, load_libsql_backend};
pub use mesh::load_mesh_backend;

pub const DEFAULT_KV_APP_MAX_BYTES: u64 = 256 * 1024 * 1024;
pub const KV_LIST_PAGE_SIZE: usize = 64;

pub type KvKey = Vec<u8>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvEntry {
    pub key: KvKey,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvListPage {
    pub entries: Vec<KvEntry>,
    pub next_after: Option<KvKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KvCommand {
    Set { key: KvKey, value: Vec<u8> },
    Delete { key: KvKey },
    CheckValue { key: KvKey, value: Vec<u8> },
    CheckExists { key: KvKey },
    CheckMissing { key: KvKey },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KvWrite {
    Set { key: KvKey, value: Vec<u8> },
    Delete { key: KvKey },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KvCheckFailedReason {
    KeyMissing,
    KeyExists,
    ValueMismatch,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KvError {
    CheckFailed(KvCheckFailedReason),
    QuotaExceeded {
        app_id: u64,
        used_bytes: u64,
        limit_bytes: u64,
    },
    Unavailable,
    Internal(String),
}

impl KvError {
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

impl std::fmt::Display for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CheckFailed(reason) => match reason {
                KvCheckFailedReason::KeyMissing => write!(f, "kv check failed: key missing"),
                KvCheckFailedReason::KeyExists => write!(f, "kv check failed: key exists"),
                KvCheckFailedReason::ValueMismatch => {
                    write!(f, "kv check failed: value mismatch")
                }
            },
            Self::QuotaExceeded {
                app_id,
                used_bytes,
                limit_bytes,
            } => write!(
                f,
                "app {app_id} kv quota exceeded: {used_bytes} > {limit_bytes}"
            ),
            Self::Unavailable => write!(f, "kv unavailable"),
            Self::Internal(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for KvError {}

#[async_trait]
pub trait KvBackend: Send + Sync {
    async fn get(&self, app_id: u64, key: KvKey) -> Result<Option<Vec<u8>>, KvError>;

    async fn exec(&self, app_id: u64, commands: Vec<KvCommand>) -> Result<(), KvError>;

    async fn list_page(
        &self,
        app_id: u64,
        prefix: KvKey,
        start: Option<KvKey>,
        end: Option<KvKey>,
        after: Option<KvKey>,
    ) -> Result<KvListPage, KvError>;

    async fn storage_used(&self, app_id: u64) -> Result<u64, KvError>;
}

pub fn collect_kv_writes(commands: &[KvCommand]) -> Result<Vec<KvWrite>, KvError> {
    let mut writes = Vec::new();
    for command in commands {
        match command {
            KvCommand::Set { key, value } => writes.push(KvWrite::Set {
                key: key.clone(),
                value: value.clone(),
            }),
            KvCommand::Delete { key } => writes.push(KvWrite::Delete { key: key.clone() }),
            KvCommand::CheckValue { .. }
            | KvCommand::CheckExists { .. }
            | KvCommand::CheckMissing { .. } => {}
        }
    }
    Ok(writes)
}

pub fn collect_touched_keys(commands: &[KvCommand]) -> Vec<KvKey> {
    commands
        .iter()
        .map(|command| match command {
            KvCommand::Set { key, .. }
            | KvCommand::Delete { key }
            | KvCommand::CheckValue { key, .. }
            | KvCommand::CheckExists { key }
            | KvCommand::CheckMissing { key } => key.clone(),
        })
        .collect()
}

pub fn exclusive_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for index in (0..upper.len()).rev() {
        if upper[index] != u8::MAX {
            upper[index] += 1;
            upper.truncate(index + 1);
            return Some(upper);
        }
    }
    None
}

#[derive(Clone, Default)]
pub struct KvRangeLock {
    inner: Arc<KvRangeLockInner>,
}

#[derive(Default)]
struct KvRangeLockInner {
    state: StdMutex<KvRangeLockState>,
    notify: Notify,
}

#[derive(Default)]
struct KvRangeLockState {
    next_id: u64,
    active: Vec<KvRangeReservation>,
}

#[derive(Clone)]
struct KvRangeReservation {
    id: u64,
    access: KvAccessPattern,
}

#[derive(Debug, Clone)]
pub struct KvAccessPattern {
    write: bool,
    ranges: Vec<KvKeyRange>,
}

#[derive(Debug, Clone)]
struct KvKeyRange {
    start: Option<KvLowerBound>,
    end: Option<KvUpperBound>,
}

#[derive(Debug, Clone)]
struct KvLowerBound {
    key: KvKey,
    inclusive: bool,
}

#[derive(Debug, Clone)]
struct KvUpperBound {
    key: KvKey,
    inclusive: bool,
}

impl KvAccessPattern {
    pub fn read_key(key: KvKey) -> Self {
        Self {
            write: false,
            ranges: vec![KvKeyRange::point(key)],
        }
    }

    pub fn write_keys(keys: impl IntoIterator<Item = KvKey>) -> Self {
        Self {
            write: true,
            ranges: keys.into_iter().map(KvKeyRange::point).collect(),
        }
    }

    pub fn read_list(
        prefix: &[u8],
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        after: Option<&[u8]>,
    ) -> Self {
        let mut range = KvKeyRange::full();
        if !prefix.is_empty() {
            range.start = Some(KvLowerBound {
                key: prefix.to_vec(),
                inclusive: true,
            });
            range.end = exclusive_upper_bound(prefix).map(|key| KvUpperBound {
                key,
                inclusive: false,
            });
        }
        if let Some(start) = start {
            range.start = max_lower_bound(
                range.start,
                Some(KvLowerBound {
                    key: start.to_vec(),
                    inclusive: true,
                }),
            );
        }
        if let Some(after) = after {
            range.start = max_lower_bound(
                range.start,
                Some(KvLowerBound {
                    key: after.to_vec(),
                    inclusive: false,
                }),
            );
        }
        if let Some(end) = end {
            range.end = min_upper_bound(
                range.end,
                Some(KvUpperBound {
                    key: end.to_vec(),
                    inclusive: false,
                }),
            );
        }
        Self {
            write: false,
            ranges: if range.is_empty() {
                Vec::new()
            } else {
                vec![range]
            },
        }
    }

    pub fn read_all() -> Self {
        Self {
            write: false,
            ranges: vec![KvKeyRange::full()],
        }
    }

    fn conflicts_with(&self, other: &Self) -> bool {
        (self.write || other.write)
            && self
                .ranges
                .iter()
                .any(|left| other.ranges.iter().any(|right| left.overlaps(right)))
    }
}

impl KvRangeLock {
    pub async fn acquire(&self, access: KvAccessPattern) -> KvRangeGuard {
        loop {
            let notified = {
                let mut state = self.inner.state.lock().unwrap();
                if !state
                    .active
                    .iter()
                    .any(|reservation| reservation.access.conflicts_with(&access))
                {
                    let id = state.next_id;
                    state.next_id = state.next_id.saturating_add(1);
                    state.active.push(KvRangeReservation {
                        id,
                        access: access.clone(),
                    });
                    return KvRangeGuard {
                        inner: self.inner.clone(),
                        id,
                    };
                }
                self.inner.notify.notified()
            };
            notified.await;
        }
    }
}

pub struct KvRangeGuard {
    inner: Arc<KvRangeLockInner>,
    id: u64,
}

impl Drop for KvRangeGuard {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.active.retain(|reservation| reservation.id != self.id);
        self.inner.notify.notify_waiters();
    }
}

impl KvKeyRange {
    fn point(key: KvKey) -> Self {
        Self {
            start: Some(KvLowerBound {
                key: key.clone(),
                inclusive: true,
            }),
            end: Some(KvUpperBound {
                key,
                inclusive: true,
            }),
        }
    }

    fn full() -> Self {
        Self {
            start: None,
            end: None,
        }
    }

    fn is_empty(&self) -> bool {
        end_before_start(self.end.as_ref(), self.start.as_ref())
    }

    fn overlaps(&self, other: &Self) -> bool {
        !end_before_start(self.end.as_ref(), other.start.as_ref())
            && !end_before_start(other.end.as_ref(), self.start.as_ref())
    }
}

fn max_lower_bound(
    left: Option<KvLowerBound>,
    right: Option<KvLowerBound>,
) -> Option<KvLowerBound> {
    match (left, right) {
        (None, other) | (other, None) => other,
        (Some(left), Some(right)) => match left.key.cmp(&right.key) {
            std::cmp::Ordering::Less => Some(right),
            std::cmp::Ordering::Greater => Some(left),
            std::cmp::Ordering::Equal => Some(KvLowerBound {
                key: left.key,
                inclusive: left.inclusive && right.inclusive,
            }),
        },
    }
}

fn min_upper_bound(
    left: Option<KvUpperBound>,
    right: Option<KvUpperBound>,
) -> Option<KvUpperBound> {
    match (left, right) {
        (None, other) | (other, None) => other,
        (Some(left), Some(right)) => match left.key.cmp(&right.key) {
            std::cmp::Ordering::Less => Some(left),
            std::cmp::Ordering::Greater => Some(right),
            std::cmp::Ordering::Equal => Some(KvUpperBound {
                key: left.key,
                inclusive: left.inclusive && right.inclusive,
            }),
        },
    }
}

fn end_before_start(end: Option<&KvUpperBound>, start: Option<&KvLowerBound>) -> bool {
    let (Some(end), Some(start)) = (end, start) else {
        return false;
    };
    match end.key.cmp(&start.key) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Greater => false,
        std::cmp::Ordering::Equal => !(end.inclusive && start.inclusive),
    }
}

fn logical_entry_size(key_len: usize, value_len: usize) -> anyhow::Result<u64> {
    let total = key_len
        .checked_add(value_len)
        .context("entry size overflow")?;
    u64::try_from(total).context("entry size exceeds u64")
}
