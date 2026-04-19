// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    path::PathBuf,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use anyhow::Context as _;
use async_trait::async_trait;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy::WriteOnInsertion, LruConfig,
};
use futures::TryStreamExt as _;
use opendal::raw::oio::Read as _;
use opendal::raw::{
    Access, Layer, LayeredAccess, OpList, OpRead, OpWrite, RpDelete, RpList, RpRead, RpWrite,
};
use opendal::{Buffer, EntryMode, ErrorKind as OpenDalErrorKind, Operator, services::S3};
use serde::{Deserialize, Serialize};
use terminal_games::kv::{
    DEFAULT_KV_APP_MAX_BYTES, KV_LIST_PAGE_SIZE, KvAccessPattern, KvBackend, KvCheckFailedReason,
    KvCommand, KvEntry, KvError, KvKey, KvListPage, KvRangeLock, KvWrite, LibsqlKvBackendOptions,
    collect_kv_writes, collect_touched_keys, load_libsql_backend,
};
use tokio::sync::Mutex;

const DEFAULT_KV_S3_PREFIX: &str = "kv";
const DEFAULT_KV_CACHE_DIR: &str = "/opt/terminal-games/kv-cache";
const DEFAULT_KV_CACHE_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_KV_CACHE_DISK_BYTES: u64 = 32 * 1024 * 1024 * 1024;
const MAX_KV_WAL_BATCHES: usize = 8;
const WAL_RETRY_BASE_DELAY_MS: u64 = 250;
const WAL_RETRY_MAX_DELAY_MS: u64 = 30_000;

pub async fn load_backend_from_env() -> anyhow::Result<Arc<dyn KvBackend>> {
    let max_app_bytes = read_env_u64("KV_APP_MAX_BYTES").unwrap_or(DEFAULT_KV_APP_MAX_BYTES);
    let backend_kind = std::env::var("KV_BACKEND")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "libsql".to_string());

    match backend_kind.as_str() {
        "s3" => {
            let backend =
                S3KvBackend::new(Arc::new(KvStore::from_s3_env().await?), max_app_bytes, true);
            backend.spawn_startup_checkpoint();
            Ok(backend)
        }
        "libsql" => {
            let path = std::env::var("KV_LIBSQL_PATH")
                .ok()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("./kv-store/kv.db"));
            load_libsql_backend(LibsqlKvBackendOptions {
                path,
                max_app_bytes,
            })
            .await
        }
        other => anyhow::bail!("unsupported KV_BACKEND '{other}', expected 'libsql' or 's3'"),
    }
}

#[derive(Clone)]
struct S3KvBackend {
    store: Arc<KvStore>,
    max_app_bytes: u64,
    namespaces: Arc<Mutex<HashMap<u64, Arc<S3AppNamespace>>>>,
    background_checkpointing: bool,
}

impl S3KvBackend {
    fn new(store: Arc<KvStore>, max_app_bytes: u64, background_checkpointing: bool) -> Arc<Self> {
        Arc::new(Self {
            store,
            max_app_bytes,
            namespaces: Arc::new(Mutex::new(HashMap::new())),
            background_checkpointing,
        })
    }

    async fn namespace(&self, app_id: u64) -> Arc<S3AppNamespace> {
        let mut namespaces = self.namespaces.lock().await;
        namespaces
            .entry(app_id)
            .or_insert_with(|| Arc::new(S3AppNamespace::default()))
            .clone()
    }

    fn spawn_startup_checkpoint(self: &Arc<Self>) {
        if !self.background_checkpointing {
            return;
        }
        let backend = self.clone();
        tokio::spawn(async move {
            let app_ids = match backend.store.list_wal_app_ids().await {
                Ok(app_ids) => app_ids,
                Err(error) => {
                    tracing::error!(error = ?error, "failed to discover kv wal files on startup");
                    return;
                }
            };
            for app_id in app_ids {
                let namespace = backend.namespace(app_id).await;
                let load_result = {
                    let _wal_guard = namespace.wal_io.lock().await;
                    let mut state = namespace.state.lock().await;
                    backend.ensure_namespace_loaded(app_id, &mut state).await
                };
                if let Err(error) = load_result {
                    tracing::error!(app_id, error = ?error, "failed to load kv wal on startup");
                    continue;
                }
                backend.maybe_spawn_checkpoint(app_id, namespace).await;
            }
        });
    }

    async fn maybe_spawn_checkpoint(&self, app_id: u64, namespace: Arc<S3AppNamespace>) {
        if !self.background_checkpointing {
            return;
        }
        let should_spawn = {
            let mut state = namespace.state.lock().await;
            if state.checkpointing || state.wal.is_empty() {
                false
            } else {
                state.checkpointing = true;
                true
            }
        };
        if !should_spawn {
            return;
        }

        let backend = self.clone();
        tokio::spawn(async move {
            backend.checkpoint_namespace(app_id, namespace).await;
        });
    }

    async fn checkpoint_namespace(self, app_id: u64, namespace: Arc<S3AppNamespace>) {
        let mut backoff = Duration::from_millis(WAL_RETRY_BASE_DELAY_MS);
        loop {
            match self.checkpoint_one_batch(app_id, &namespace).await {
                Ok(true) => {
                    backoff = Duration::from_millis(WAL_RETRY_BASE_DELAY_MS);
                }
                Ok(false) => return,
                Err(error) => {
                    tracing::error!(
                        app_id,
                        error = ?error,
                        retry_ms = backoff.as_millis(),
                        "failed to checkpoint kv wal"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff.saturating_mul(2))
                        .min(Duration::from_millis(WAL_RETRY_MAX_DELAY_MS));
                }
            }
        }
    }

    async fn checkpoint_one_batch(
        &self,
        app_id: u64,
        namespace: &Arc<S3AppNamespace>,
    ) -> anyhow::Result<bool> {
        let batch = {
            let state = namespace.state.lock().await;
            if state.wal.is_empty() {
                None
            } else {
                state.wal.front().cloned()
            }
        };
        let Some(batch) = batch else {
            let mut state = namespace.state.lock().await;
            state.checkpointing = false;
            return Ok(false);
        };
        let _range_guard = namespace
            .locks
            .acquire(KvAccessPattern::write_keys(batch.iter().map(
                |write| match write {
                    KvWrite::Set { key, .. } | KvWrite::Delete { key } => key.clone(),
                },
            )))
            .await;
        let _wal_guard = namespace.wal_io.lock().await;
        let remaining = {
            let state = namespace.state.lock().await;
            if state.wal.front() != Some(&batch) {
                return Ok(true);
            }
            let mut remaining = state.wal.clone();
            remaining.pop_front();
            remaining
        };
        self.store.apply_entry_writes(app_id, &batch).await?;
        self.store.write_wal(app_id, &remaining).await?;
        let mut state = namespace.state.lock().await;
        state.wal = remaining;
        state.rebuild_overlay();
        self.store.apply_cache_overlay(app_id, &state.overlay);
        Ok(true)
    }

    async fn ensure_namespace_loaded(
        &self,
        app_id: u64,
        state: &mut S3AppState,
    ) -> anyhow::Result<()> {
        if state.loaded {
            return Ok(());
        }
        let (entry_sizes, total_bytes) = self.store.list_entry_sizes(app_id).await?;
        state.wal = self.store.read_wal(app_id).await?;
        state.entry_sizes = entry_sizes;
        state.total_bytes = total_bytes;
        state.rebuild_overlay();
        let wal_writes = state
            .wal
            .iter()
            .flat_map(|batch| batch.iter().cloned())
            .collect::<Vec<_>>();
        state.apply_writes(&wal_writes)?;
        self.store.apply_cache_overlay(app_id, &state.overlay);
        state.loaded = true;
        Ok(())
    }

    async fn ensure_namespace_ready(
        &self,
        app_id: u64,
        namespace: &S3AppNamespace,
    ) -> Result<(), KvError> {
        {
            let state = namespace.state.lock().await;
            if state.loaded {
                return Ok(());
            }
        }
        let _wal_guard = namespace.wal_io.lock().await;
        let mut state = namespace.state.lock().await;
        self.ensure_namespace_loaded(app_id, &mut state)
            .await
            .map_err(|error| KvError::internal(format!("failed to load kv wal: {error:#}")))
    }
}

#[cfg(test)]
impl S3KvBackend {
    async fn checkpoint_all_for_tests(&self, app_id: u64) -> anyhow::Result<()> {
        let namespace = self.namespace(app_id).await;
        loop {
            if !self.checkpoint_one_batch(app_id, &namespace).await? {
                return Ok(());
            }
        }
    }
}

#[derive(Clone, Default)]
struct S3AppState {
    loaded: bool,
    wal: VecDeque<KvWalBatch>,
    overlay: HashMap<KvKey, Option<Vec<u8>>>,
    entry_sizes: HashMap<KvKey, u64>,
    total_bytes: u64,
    checkpointing: bool,
}

impl S3AppState {
    fn reset(&mut self) {
        *self = Self::default();
    }

    fn rebuild_overlay(&mut self) {
        self.overlay.clear();
        for batch in &self.wal {
            for write in batch {
                match write {
                    KvWrite::Set { key, value } => {
                        self.overlay.insert(key.clone(), Some(value.clone()));
                    }
                    KvWrite::Delete { key } => {
                        self.overlay.insert(key.clone(), None);
                    }
                }
            }
        }
    }

    fn predicted_total_after(&self, writes: &[KvWrite]) -> Result<u64, KvError> {
        let mut entry_sizes = self.entry_sizes.clone();
        let mut total_bytes = self.total_bytes;

        for write in writes {
            match write {
                KvWrite::Set { key, value } => {
                    let new_size = logical_entry_size(key.len(), value.len())
                        .map_err(|error| KvError::internal(error.to_string()))?;
                    let old_size = entry_sizes.insert(key.clone(), new_size).unwrap_or(0);
                    total_bytes = total_bytes
                        .saturating_sub(old_size)
                        .saturating_add(new_size);
                }
                KvWrite::Delete { key } => {
                    total_bytes = total_bytes.saturating_sub(entry_sizes.remove(key).unwrap_or(0));
                }
            }
        }

        Ok(total_bytes)
    }

    fn apply_writes(&mut self, writes: &[KvWrite]) -> Result<(), KvError> {
        for write in writes {
            match write {
                KvWrite::Set { key, value } => {
                    let new_size = logical_entry_size(key.len(), value.len())
                        .map_err(|error| KvError::internal(error.to_string()))?;
                    let old_size = self.entry_sizes.insert(key.clone(), new_size).unwrap_or(0);
                    self.total_bytes = self
                        .total_bytes
                        .saturating_sub(old_size)
                        .saturating_add(new_size);
                }
                KvWrite::Delete { key } => {
                    self.total_bytes = self
                        .total_bytes
                        .saturating_sub(self.entry_sizes.remove(key).unwrap_or(0));
                }
            }
        }

        Ok(())
    }
}

#[derive(Default)]
struct S3AppNamespace {
    state: Mutex<S3AppState>,
    locks: KvRangeLock,
    wal_io: Mutex<()>,
}

type KvWalBatch = Vec<KvWrite>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StoredWal {
    batches: Vec<KvWalBatch>,
}

#[async_trait]
impl KvBackend for S3KvBackend {
    async fn get(&self, app_id: u64, key: KvKey) -> Result<Option<Vec<u8>>, KvError> {
        let namespace = self.namespace(app_id).await;
        let _guard = namespace
            .locks
            .acquire(KvAccessPattern::read_key(key.clone()))
            .await;
        self.ensure_namespace_ready(app_id, &namespace).await?;
        let overlay_value = {
            let state = namespace.state.lock().await;
            if state.overlay.contains_key(&key) {
                Some(state.overlay.get(&key).cloned().unwrap_or(None))
            } else {
                None
            }
        };
        let value = if let Some(value) = overlay_value {
            value
        } else {
            self.store
                .get_entry(app_id, &key)
                .await
                .map_err(|error| KvError::internal(format!("failed to read kv entry: {error:#}")))?
        };
        self.maybe_spawn_checkpoint(app_id, namespace).await;
        Ok(value)
    }

    async fn exec(&self, app_id: u64, commands: Vec<KvCommand>) -> Result<(), KvError> {
        let namespace = self.namespace(app_id).await;
        let writes = collect_kv_writes(&commands)?;
        let touched_keys = collect_touched_keys(&commands);
        let _range_guard = namespace
            .locks
            .acquire(KvAccessPattern::write_keys(touched_keys.clone()))
            .await;
        let should_spawn_checkpoint = {
            let _wal_guard = namespace.wal_io.lock().await;
            enum MutationPlan {
                Direct(KvWrite),
                CheckOnly {
                    visible_values: HashMap<KvKey, Option<Vec<u8>>>,
                    store_keys: Vec<KvKey>,
                },
                AppendWal {
                    next_wal: VecDeque<KvWalBatch>,
                    visible_values: HashMap<KvKey, Option<Vec<u8>>>,
                    store_keys: Vec<KvKey>,
                },
            }

            let plan = {
                let mut state = namespace.state.lock().await;
                self.ensure_namespace_loaded(app_id, &mut state)
                    .await
                    .map_err(|error| {
                        KvError::internal(format!("failed to load kv wal: {error:#}"))
                    })?;

                if let Some(single_write) = single_write_command(&commands)
                    && state.wal.is_empty()
                {
                    let predicted_total_bytes =
                        state.predicted_total_after(std::slice::from_ref(&single_write))?;
                    if predicted_total_bytes > self.max_app_bytes {
                        return Err(KvError::QuotaExceeded {
                            app_id,
                            used_bytes: predicted_total_bytes,
                            limit_bytes: self.max_app_bytes,
                        });
                    }
                    MutationPlan::Direct(single_write)
                } else {
                    let mut visible_values = HashMap::new();
                    let mut store_keys = Vec::new();
                    for key in touched_keys {
                        if let Some(value) = state.overlay.get(&key) {
                            visible_values.insert(key, value.clone());
                        } else {
                            store_keys.push(key);
                        }
                    }
                    let predicted_total_bytes = state.predicted_total_after(&writes)?;
                    if predicted_total_bytes > self.max_app_bytes {
                        return Err(KvError::QuotaExceeded {
                            app_id,
                            used_bytes: predicted_total_bytes,
                            limit_bytes: self.max_app_bytes,
                        });
                    }
                    if writes.is_empty() {
                        MutationPlan::CheckOnly {
                            visible_values,
                            store_keys,
                        }
                    } else if state.wal.len() >= MAX_KV_WAL_BATCHES {
                        return Err(KvError::Unavailable);
                    } else {
                        let mut next_wal = state.wal.clone();
                        next_wal.push_back(writes.clone());
                        MutationPlan::AppendWal {
                            next_wal,
                            visible_values,
                            store_keys,
                        }
                    }
                }
            };

            match plan {
                MutationPlan::Direct(single_write) => {
                    let result = match &single_write {
                        KvWrite::Set { key, value } => {
                            self.store.write_entry(app_id, key, value).await
                        }
                        KvWrite::Delete { key } => self.store.delete_entry(app_id, key).await,
                    };
                    let mut state = namespace.state.lock().await;
                    if let Err(error) = result {
                        state.reset();
                        return Err(KvError::internal(format!(
                            "failed to persist kv entry: {error:#}"
                        )));
                    }
                    state.apply_writes(std::slice::from_ref(&single_write))?;
                    false
                }
                MutationPlan::CheckOnly {
                    mut visible_values,
                    store_keys,
                } => {
                    visible_values.extend(
                        self.store
                            .load_entries(app_id, store_keys)
                            .await
                            .map_err(|error| {
                                KvError::internal(format!("failed to load kv entries: {error:#}"))
                            })?,
                    );

                    for command in &commands {
                        match command {
                            KvCommand::Set { .. } | KvCommand::Delete { .. } => {}
                            KvCommand::CheckValue { key, value } => {
                                let Some(existing) =
                                    visible_values.get(key).and_then(|value| value.as_ref())
                                else {
                                    return Err(KvError::CheckFailed(
                                        KvCheckFailedReason::KeyMissing,
                                    ));
                                };
                                if existing != value {
                                    return Err(KvError::CheckFailed(
                                        KvCheckFailedReason::ValueMismatch,
                                    ));
                                }
                            }
                            KvCommand::CheckExists { key } => {
                                if visible_values
                                    .get(key)
                                    .and_then(|value| value.as_ref())
                                    .is_none()
                                {
                                    return Err(KvError::CheckFailed(
                                        KvCheckFailedReason::KeyMissing,
                                    ));
                                }
                            }
                            KvCommand::CheckMissing { key } => {
                                if visible_values
                                    .get(key)
                                    .and_then(|value| value.as_ref())
                                    .is_some()
                                {
                                    return Err(KvError::CheckFailed(
                                        KvCheckFailedReason::KeyExists,
                                    ));
                                }
                            }
                        }
                    }

                    return Ok(());
                }
                MutationPlan::AppendWal {
                    mut visible_values,
                    store_keys,
                    next_wal,
                } => {
                    visible_values.extend(
                        self.store
                            .load_entries(app_id, store_keys)
                            .await
                            .map_err(|error| {
                                KvError::internal(format!("failed to load kv entries: {error:#}"))
                            })?,
                    );

                    for command in &commands {
                        match command {
                            KvCommand::Set { .. } | KvCommand::Delete { .. } => {}
                            KvCommand::CheckValue { key, value } => {
                                let Some(existing) =
                                    visible_values.get(key).and_then(|value| value.as_ref())
                                else {
                                    return Err(KvError::CheckFailed(
                                        KvCheckFailedReason::KeyMissing,
                                    ));
                                };
                                if existing != value {
                                    return Err(KvError::CheckFailed(
                                        KvCheckFailedReason::ValueMismatch,
                                    ));
                                }
                            }
                            KvCommand::CheckExists { key } => {
                                if visible_values
                                    .get(key)
                                    .and_then(|value| value.as_ref())
                                    .is_none()
                                {
                                    return Err(KvError::CheckFailed(
                                        KvCheckFailedReason::KeyMissing,
                                    ));
                                }
                            }
                            KvCommand::CheckMissing { key } => {
                                if visible_values
                                    .get(key)
                                    .and_then(|value| value.as_ref())
                                    .is_some()
                                {
                                    return Err(KvError::CheckFailed(
                                        KvCheckFailedReason::KeyExists,
                                    ));
                                }
                            }
                        }
                    }

                    let write_result = self.store.write_wal(app_id, &next_wal).await;
                    let mut state = namespace.state.lock().await;
                    if let Err(error) = write_result {
                        state.reset();
                        return Err(KvError::internal(format!(
                            "failed to write kv wal: {error:#}"
                        )));
                    }
                    state.wal = next_wal;
                    state.rebuild_overlay();
                    state.apply_writes(&writes)?;
                    self.store.apply_cache_overlay(app_id, &state.overlay);
                    true
                }
            }
        };

        if should_spawn_checkpoint {
            self.maybe_spawn_checkpoint(app_id, namespace).await;
        }
        Ok(())
    }

    async fn list_page(
        &self,
        app_id: u64,
        prefix: KvKey,
        start: Option<KvKey>,
        end: Option<KvKey>,
        after: Option<KvKey>,
    ) -> Result<KvListPage, KvError> {
        let namespace = self.namespace(app_id).await;
        let _guard = namespace
            .locks
            .acquire(KvAccessPattern::read_list(
                &prefix,
                start.as_deref(),
                end.as_deref(),
                after.as_deref(),
            ))
            .await;
        self.ensure_namespace_ready(app_id, &namespace).await?;
        let overlay = {
            let state = namespace.state.lock().await;
            state.overlay.clone()
        };
        let page = self
            .list_visible_page(app_id, &overlay, prefix, start, end, after)
            .await?;
        self.maybe_spawn_checkpoint(app_id, namespace).await;
        Ok(page)
    }

    async fn storage_used(&self, app_id: u64) -> Result<u64, KvError> {
        let namespace = self.namespace(app_id).await;
        let _guard = namespace.locks.acquire(KvAccessPattern::read_all()).await;
        self.ensure_namespace_ready(app_id, &namespace).await?;
        let total = {
            let state = namespace.state.lock().await;
            state.total_bytes
        };
        self.maybe_spawn_checkpoint(app_id, namespace).await;
        Ok(total)
    }
}

impl S3KvBackend {
    async fn list_visible_page(
        &self,
        app_id: u64,
        overlay: &HashMap<KvKey, Option<Vec<u8>>>,
        prefix: KvKey,
        start: Option<KvKey>,
        end: Option<KvKey>,
        after: Option<KvKey>,
    ) -> Result<KvListPage, KvError> {
        let overlay_keys = overlay
            .iter()
            .filter(|(key, _)| {
                key_in_range(
                    key,
                    &prefix,
                    start.as_deref(),
                    end.as_deref(),
                    after.as_deref(),
                )
            })
            .map(|(key, _)| key.clone())
            .collect::<HashSet<_>>();
        let mut overlay_entries = overlay
            .iter()
            .filter_map(|(key, entry)| {
                if !key_in_range(
                    key,
                    &prefix,
                    start.as_deref(),
                    end.as_deref(),
                    after.as_deref(),
                ) {
                    return None;
                }
                Some(KvEntry {
                    key: key.clone(),
                    value: entry.clone()?,
                })
            })
            .collect::<Vec<_>>();
        overlay_entries.sort_by(|left, right| left.key.cmp(&right.key));

        let target_s3_entries = KV_LIST_PAGE_SIZE + 1 + overlay_keys.len();
        let mut s3_entries = Vec::new();
        let mut cursor = after;
        while s3_entries.len() < target_s3_entries {
            let page = self
                .store
                .list_entries_page(
                    app_id,
                    prefix.clone(),
                    start.clone(),
                    end.clone(),
                    cursor.clone(),
                )
                .await
                .map_err(|error| {
                    KvError::internal(format!("failed to list kv entries: {error:#}"))
                })?;

            for entry in page.entries {
                if !overlay_keys.contains(&entry.key) {
                    s3_entries.push(entry);
                }
            }
            let Some(next_after) = page.next_after else {
                break;
            };
            cursor = Some(next_after);
        }

        let mut entries = overlay_entries;
        entries.extend(s3_entries);
        entries.sort_by(|left, right| left.key.cmp(&right.key));

        let next_after = if entries.len() > KV_LIST_PAGE_SIZE {
            let next_after = entries[KV_LIST_PAGE_SIZE - 1].key.clone();
            entries.truncate(KV_LIST_PAGE_SIZE);
            Some(next_after)
        } else {
            None
        };

        Ok(KvListPage {
            entries,
            next_after,
        })
    }
}

fn single_write_command(commands: &[KvCommand]) -> Option<KvWrite> {
    match commands {
        [KvCommand::Set { key, value }] => Some(KvWrite::Set {
            key: key.clone(),
            value: value.clone(),
        }),
        [KvCommand::Delete { key }] => Some(KvWrite::Delete { key: key.clone() }),
        _ => None,
    }
}

fn key_in_range(
    key: &[u8],
    prefix: &[u8],
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    after: Option<&[u8]>,
) -> bool {
    key.starts_with(prefix)
        && start.is_none_or(|start| key >= start)
        && after.is_none_or(|after| key > after)
        && end.is_none_or(|end| key < end)
}

fn logical_entry_size(key_len: usize, value_len: usize) -> anyhow::Result<u64> {
    let total = key_len
        .checked_add(value_len)
        .context("entry size overflow")?;
    u64::try_from(total).context("entry size exceeds u64")
}

#[derive(Clone)]
struct KvCache {
    inner: HybridCache<String, Vec<u8>>,
    deleted: Arc<StdMutex<HashSet<String>>>,
}

impl KvCache {
    async fn from_env() -> anyhow::Result<Self> {
        let cache_dir = std::env::var("KV_CACHE_DIR")
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_KV_CACHE_DIR));
        let cache_memory_bytes =
            read_env_u64("KV_CACHE_MEMORY_BYTES").unwrap_or(DEFAULT_KV_CACHE_MEMORY_BYTES);
        let cache_disk_bytes =
            read_env_u64("KV_CACHE_DISK_BYTES").unwrap_or(DEFAULT_KV_CACHE_DISK_BYTES);

        Self::open(cache_dir, cache_memory_bytes, cache_disk_bytes).await
    }

    async fn open(
        cache_dir: PathBuf,
        cache_memory_bytes: u64,
        cache_disk_bytes: u64,
    ) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&cache_dir)
            .await
            .with_context(|| format!("failed to create kv cache dir {}", cache_dir.display()))?;

        let device = FsDeviceBuilder::new(&cache_dir)
            .with_capacity(
                usize::try_from(cache_disk_bytes).context("KV_CACHE_DISK_BYTES exceeds usize")?,
            )
            .build()
            .with_context(|| {
                format!(
                    "failed to build foyer cache device for {}",
                    cache_dir.display()
                )
            })?;
        let inner = HybridCacheBuilder::new()
            .with_name("terminal-games-kv")
            .with_policy(WriteOnInsertion)
            .memory(
                usize::try_from(cache_memory_bytes)
                    .context("KV_CACHE_MEMORY_BYTES exceeds usize")?,
            )
            .with_eviction_config(LruConfig {
                high_priority_pool_ratio: 0.0,
            })
            .with_weighter(|key: &String, value: &Vec<u8>| key.len().saturating_add(value.len()))
            .storage()
            .with_engine_config(BlockEngineConfig::new(device))
            .build()
            .await
            .context("failed to initialize kv cache")?;

        Ok(Self {
            inner,
            deleted: Arc::new(StdMutex::new(HashSet::new())),
        })
    }

    #[cfg(test)]
    async fn get(&self, path: &str) -> Option<Vec<u8>> {
        if self.deleted.lock().unwrap().contains(path) {
            return None;
        }
        match self.inner.get(&path.to_string()).await {
            Ok(Some(entry)) => Some(entry.value().clone()),
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(error = ?error, path, "failed to read kv cache");
                None
            }
        }
    }

    async fn lookup(&self, path: &str) -> Option<Option<Vec<u8>>> {
        if self.deleted.lock().unwrap().contains(path) {
            return Some(None);
        }
        match self.inner.get(&path.to_string()).await {
            Ok(Some(entry)) => Some(Some(entry.value().clone())),
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(error = ?error, path, "failed to read kv cache");
                None
            }
        }
    }

    fn insert(&self, path: &str, bytes: Vec<u8>) {
        self.deleted.lock().unwrap().remove(path);
        self.inner.insert(path.to_string(), bytes);
    }

    fn remove(&self, path: &str) {
        self.deleted.lock().unwrap().insert(path.to_string());
        self.inner.remove(&path.to_string());
    }
}

#[derive(Clone)]
struct FoyerLayer {
    cache: Arc<KvCache>,
}

impl FoyerLayer {
    fn new(cache: Arc<KvCache>) -> Self {
        Self { cache }
    }
}

impl<A: Access> Layer<A> for FoyerLayer {
    type LayeredAccess = FoyerAccess<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        FoyerAccess {
            inner,
            cache: self.cache.clone(),
        }
    }
}

#[derive(Clone)]
struct FoyerAccess<A: Access> {
    inner: A,
    cache: Arc<KvCache>,
}

impl<A: Access> fmt::Debug for FoyerAccess<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FoyerAccess")
    }
}

impl<A: Access> LayeredAccess for FoyerAccess<A> {
    type Inner = A;
    type Reader = Buffer;
    type Writer = A::Writer;
    type Lister = A::Lister;
    type Deleter = A::Deleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
        let cacheable = args.range().is_full();
        if cacheable && let Some(bytes) = self.cache.lookup(path).await {
            return match bytes {
                Some(bytes) => Ok((RpRead::new(), bytes.into())),
                None => Err(opendal::Error::new(
                    OpenDalErrorKind::NotFound,
                    "kv cache tombstone",
                )),
            };
        }

        let (rp, mut reader) = self.inner.read(path, args).await?;
        let buffer = reader.read_all().await?;
        if cacheable {
            self.cache.insert(path, buffer.to_vec());
        }
        Ok((rp, buffer))
    }

    async fn write(&self, path: &str, args: OpWrite) -> opendal::Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
        self.inner.delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }
}

struct KvStore {
    operator: Operator,
    cache: Arc<KvCache>,
}

impl KvStore {
    async fn from_s3_env() -> anyhow::Result<Self> {
        let cache = Arc::new(KvCache::from_env().await?);
        let bucket = read_env_string("KV_S3_BUCKET")
            .context("KV_S3_BUCKET must be set when KV_BACKEND=s3")?;
        let prefix = read_env_string("KV_S3_PREFIX").unwrap_or_else(|| DEFAULT_KV_S3_PREFIX.into());

        let mut builder = S3::default().bucket(&bucket);
        if !prefix.is_empty() {
            builder = builder.root(&prefix);
        }
        if let Some(region) = read_env_string("KV_S3_REGION") {
            builder = builder.region(&region);
        }
        if let Some(endpoint) = read_env_string("KV_S3_ENDPOINT_URL") {
            builder = builder.endpoint(&endpoint);
        }

        let operator = Operator::new(builder)
            .context("failed to configure S3 kv store")?
            .layer(FoyerLayer::new(cache.clone()))
            .finish();

        Ok(Self { operator, cache })
    }

    async fn get_entry(&self, app_id: u64, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        read_s3(&self.operator, &entry_path(app_id, key), "read kv entry").await
    }

    async fn write_entry(&self, app_id: u64, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        write_s3(
            &self.operator,
            &self.cache,
            &entry_path(app_id, key),
            value,
            "write kv entry",
        )
        .await
    }

    async fn delete_entry(&self, app_id: u64, key: &[u8]) -> anyhow::Result<()> {
        delete_s3(&self.operator, &self.cache, &entry_path(app_id, key)).await
    }

    fn apply_cache_overlay(&self, app_id: u64, overlay: &HashMap<KvKey, Option<Vec<u8>>>) {
        for (key, value) in overlay {
            let path = entry_path(app_id, key);
            match value {
                Some(value) => self.cache.insert(&path, value.clone()),
                None => self.cache.remove(&path),
            }
        }
    }

    async fn load_entries(
        &self,
        app_id: u64,
        keys: impl IntoIterator<Item = KvKey>,
    ) -> anyhow::Result<HashMap<KvKey, Option<Vec<u8>>>> {
        let mut values = HashMap::new();
        for key in keys {
            values.insert(key.clone(), self.get_entry(app_id, &key).await?);
        }
        Ok(values)
    }

    async fn apply_entry_writes(&self, app_id: u64, writes: &[KvWrite]) -> anyhow::Result<()> {
        for write in writes {
            match write {
                KvWrite::Set { key, value } => {
                    self.write_entry(app_id, key, value).await?;
                }
                KvWrite::Delete { key } => {
                    self.delete_entry(app_id, key).await?;
                }
            }
        }
        Ok(())
    }

    async fn list_wal_app_ids(&self) -> anyhow::Result<Vec<u64>> {
        let mut lister = self
            .operator
            .lister_with("app/")
            .recursive(true)
            .await
            .context("failed to create kv wal lister")?;
        let mut app_ids = Vec::new();
        let mut seen = HashSet::new();

        while let Some(entry) = lister.try_next().await.context("failed to list kv wals")? {
            if entry.metadata().mode() != EntryMode::FILE {
                continue;
            }
            let Some(app_id) = parse_wal_app_id(entry.path()) else {
                continue;
            };
            if seen.insert(app_id) {
                app_ids.push(app_id);
            }
        }

        Ok(app_ids)
    }

    async fn read_wal(&self, app_id: u64) -> anyhow::Result<VecDeque<KvWalBatch>> {
        let bytes = read_s3(&self.operator, &wal_path(app_id), "read kv wal").await?;
        let Some(bytes) = bytes else {
            return Ok(VecDeque::new());
        };
        let wal = bincode::deserialize::<StoredWal>(&bytes).context("failed to decode kv wal")?;
        Ok(wal.batches.into())
    }

    async fn write_wal(&self, app_id: u64, wal: &VecDeque<KvWalBatch>) -> anyhow::Result<()> {
        let path = wal_path(app_id);
        if wal.is_empty() {
            delete_s3(&self.operator, &self.cache, &path).await?;
            return Ok(());
        }

        let bytes = bincode::serialize(&StoredWal {
            batches: wal.iter().cloned().collect(),
        })
        .context("failed to encode kv wal")?;
        write_s3(&self.operator, &self.cache, &path, &bytes, "write kv wal").await
    }

    async fn list_entry_sizes(&self, app_id: u64) -> anyhow::Result<(HashMap<KvKey, u64>, u64)> {
        let mut lister = self
            .operator
            .lister_with(&entry_prefix(app_id))
            .recursive(true)
            .await
            .context("failed to create kv usage lister")?;
        let mut entry_sizes = HashMap::new();
        let mut total_bytes = 0_u64;

        while let Some(entry) = lister.try_next().await.context("failed to list kv usage")? {
            if entry.metadata().mode() != EntryMode::FILE {
                continue;
            }
            let Some(key) = parse_entry_key_from_path(entry.path()) else {
                continue;
            };
            let value_len = usize::try_from(entry.metadata().content_length())
                .context("kv entry value length exceeds usize")?;
            let entry_size = logical_entry_size(key.len(), value_len)?;
            total_bytes = total_bytes
                .checked_add(entry_size)
                .context("kv usage overflow")?;
            entry_sizes.insert(key, entry_size);
        }

        Ok((entry_sizes, total_bytes))
    }

    async fn list_entries_page(
        &self,
        app_id: u64,
        prefix: KvKey,
        start: Option<KvKey>,
        end: Option<KvKey>,
        after: Option<KvKey>,
    ) -> anyhow::Result<KvListPage> {
        let mut request = self
            .operator
            .lister_with(&entry_prefix_for_key_prefix(app_id, &prefix))
            .recursive(true)
            .limit(KV_LIST_PAGE_SIZE + 1);
        if let Some(after) = after.as_ref().filter(|after| after.starts_with(&prefix)) {
            request = request.start_after(&entry_path(app_id, after));
        }

        let mut lister = request.await.context("failed to create kv entry lister")?;
        let mut entries = Vec::new();

        while let Some(entry) = lister
            .try_next()
            .await
            .context("failed to list kv entries")?
        {
            if entry.metadata().mode() != EntryMode::FILE {
                continue;
            }

            let path = entry.path().to_string();
            let Some(key) = parse_entry_key_from_path(&path) else {
                continue;
            };
            if !key.starts_with(&prefix) {
                continue;
            }
            if start
                .as_ref()
                .is_some_and(|start| key.as_slice() < start.as_slice())
            {
                continue;
            }
            if after
                .as_ref()
                .is_some_and(|after| key.as_slice() <= after.as_slice())
            {
                continue;
            }
            if end
                .as_ref()
                .is_some_and(|end| key.as_slice() >= end.as_slice())
            {
                break;
            }

            if let Some(value) = read_s3(&self.operator, &path, "read kv entry").await? {
                entries.push(KvEntry { key, value });
            }
            if entries.len() > KV_LIST_PAGE_SIZE {
                break;
            }
        }

        let next_after = if entries.len() > KV_LIST_PAGE_SIZE {
            let next_after = entries[KV_LIST_PAGE_SIZE - 1].key.clone();
            entries.truncate(KV_LIST_PAGE_SIZE);
            Some(next_after)
        } else {
            None
        };

        Ok(KvListPage {
            entries,
            next_after,
        })
    }
}

fn entry_path(app_id: u64, key: &[u8]) -> String {
    format!("app/{app_id}/entries/{}.bin", hex::encode(key))
}

fn wal_path(app_id: u64) -> String {
    format!("app/{app_id}/wal.bin")
}

fn entry_prefix(app_id: u64) -> String {
    format!("app/{app_id}/entries/")
}

fn entry_prefix_for_key_prefix(app_id: u64, prefix: &[u8]) -> String {
    format!("{}{}", entry_prefix(app_id), hex::encode(prefix))
}

fn parse_entry_key_from_path(path: &str) -> Option<Vec<u8>> {
    let name = path.rsplit('/').next()?;
    let encoded = name.strip_suffix(".bin")?;
    hex::decode(encoded).ok()
}

fn parse_wal_app_id(path: &str) -> Option<u64> {
    let rest = path.strip_prefix("app/")?;
    let (app_id, tail) = rest.split_once('/')?;
    if tail != "wal.bin" {
        return None;
    }
    app_id.parse().ok()
}

async fn read_s3(
    operator: &Operator,
    path: &str,
    context: &str,
) -> anyhow::Result<Option<Vec<u8>>> {
    match operator.read(path).await {
        Ok(buffer) => Ok(Some(buffer.to_vec())),
        Err(error) if error.kind() == OpenDalErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("failed to {context} {path}")),
    }
}

async fn write_s3(
    operator: &Operator,
    cache: &KvCache,
    path: &str,
    bytes: &[u8],
    context: &str,
) -> anyhow::Result<()> {
    operator
        .write(path, bytes.to_vec())
        .await
        .with_context(|| format!("failed to {context} {path}"))?;
    cache.insert(path, bytes.to_vec());
    Ok(())
}

async fn delete_s3(operator: &Operator, cache: &KvCache, path: &str) -> anyhow::Result<()> {
    match operator.delete(path).await {
        Ok(()) => {
            cache.remove(path);
            Ok(())
        }
        Err(error) if error.kind() == OpenDalErrorKind::NotFound => {
            cache.remove(path);
            Ok(())
        }
        Err(error) => Err(error).with_context(|| format!("failed to delete kv object {path}")),
    }
}

fn read_env_u64(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<u64>().ok())
}

fn read_env_string(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests;
