// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{collections::HashMap, fmt, path::PathBuf, sync::Arc};

use anyhow::Context as _;
use async_trait::async_trait;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy::WriteOnInsertion, LruConfig,
};
use opendal::raw::oio::Read as _;
use opendal::raw::{
    Access, Layer, LayeredAccess, OpList, OpRead, OpWrite, RpDelete, RpList, RpRead, RpWrite,
};
use opendal::{Buffer, EntryMode, ErrorKind as OpenDalErrorKind, Operator, services::S3};
use serde::{Deserialize, Serialize};
use terminal_games::kv::{
    DEFAULT_KV_APP_MAX_BYTES, KvBackend, KvCommand, KvEntry, KvKey, KvListPage, KvWrite,
    LibsqlKvBackendOptions, load_libsql_backend,
};
use tokio::sync::Mutex;

const DEFAULT_KV_S3_PREFIX: &str = "kv";
const DEFAULT_KV_CACHE_DIR: &str = "/opt/terminal-games/kv-cache";
const DEFAULT_KV_CACHE_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_KV_CACHE_DISK_BYTES: u64 = 32 * 1024 * 1024 * 1024;
const DEFAULT_KV_S3_CHECKPOINT_INTERVAL_COMMITS: u64 = 256;

pub async fn load_backend_from_env() -> anyhow::Result<Arc<dyn KvBackend>> {
    let max_app_bytes = read_env_u64("KV_APP_MAX_BYTES").unwrap_or(DEFAULT_KV_APP_MAX_BYTES);
    let checkpoint_interval_commits = read_env_u64("KV_CHECKPOINT_INTERVAL_COMMITS")
        .unwrap_or(DEFAULT_KV_S3_CHECKPOINT_INTERVAL_COMMITS)
        .max(1);
    let backend_kind = std::env::var("KV_BACKEND")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "libsql".to_string());

    let store = match backend_kind.as_str() {
        "s3" => KvStore::from_s3_env().await?,
        "libsql" | "local" => {
            let path = std::env::var("KV_LIBSQL_PATH")
                .ok()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("./kv-store/kv.db"));
            return load_libsql_backend(LibsqlKvBackendOptions {
                path,
                max_app_bytes,
            })
            .await;
        }
        other => anyhow::bail!("unsupported KV_BACKEND '{other}', expected 'libsql' or 's3'"),
    };

    Ok(Arc::new(S3KvBackend {
        store: Arc::new(store),
        max_app_bytes,
        checkpoint_interval_commits,
        namespaces: Arc::new(Mutex::new(HashMap::new())),
    }))
}

#[derive(Clone)]
struct S3KvBackend {
    store: Arc<KvStore>,
    max_app_bytes: u64,
    checkpoint_interval_commits: u64,
    namespaces: Arc<Mutex<HashMap<u64, Arc<Mutex<AppState>>>>>,
}

#[derive(Default)]
struct AppState {
    loaded: bool,
    last_seq: u64,
    last_checkpoint_seq: u64,
    total_bytes: u64,
    records: HashMap<KvKey, Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckpointRecord {
    last_seq: u64,
    total_bytes: u64,
    records: HashMap<KvKey, Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JournalRecord {
    seq: u64,
    writes: Vec<KvWrite>,
}

impl S3KvBackend {
    async fn namespace(&self, app_id: u64) -> Arc<Mutex<AppState>> {
        let mut namespaces = self.namespaces.lock().await;
        namespaces
            .entry(app_id)
            .or_insert_with(|| Arc::new(Mutex::new(AppState::default())))
            .clone()
    }

    async fn load_state(&self, app_id: u64, state: &mut AppState) -> Result<(), String> {
        if state.loaded {
            return Ok(());
        }

        if let Some(bytes) = self
            .store
            .load_checkpoint(app_id)
            .await
            .map_err(|error| format!("failed to load kv checkpoint: {error:#}"))?
        {
            let checkpoint: CheckpointRecord = bincode::deserialize(&bytes)
                .map_err(|error| format!("failed to decode kv checkpoint: {error}"))?;
            state.last_seq = checkpoint.last_seq;
            state.last_checkpoint_seq = checkpoint.last_seq;
            state.total_bytes = checkpoint.total_bytes;
            state.records = checkpoint.records;
        }

        for bytes in self
            .store
            .load_journal_since(app_id, state.last_seq)
            .await
            .map_err(|error| format!("failed to load kv journal: {error:#}"))?
        {
            let journal: JournalRecord = bincode::deserialize(&bytes)
                .map_err(|error| format!("failed to decode kv journal entry: {error}"))?;
            apply_journal_record(state, journal)?;
        }

        state.loaded = true;
        Ok(())
    }

    async fn maybe_checkpoint(&self, app_id: u64, state: &mut AppState) {
        let needs_checkpoint = state.last_seq.saturating_sub(state.last_checkpoint_seq)
            >= self.checkpoint_interval_commits;
        if !needs_checkpoint {
            return;
        }

        let checkpoint = CheckpointRecord {
            last_seq: state.last_seq,
            total_bytes: state.total_bytes,
            records: state.records.clone(),
        };
        let checkpoint_bytes = match bincode::serialize(&checkpoint) {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::warn!(app_id, error = ?error, "failed to encode kv checkpoint");
                return;
            }
        };

        let checkpoint_seq = state.last_seq;
        if let Err(error) = self
            .store
            .store_checkpoint(app_id, checkpoint_seq, &checkpoint_bytes)
            .await
        {
            tracing::warn!(app_id, checkpoint_seq, error = ?error, "failed to store kv checkpoint");
            return;
        }

        state.last_checkpoint_seq = checkpoint_seq;
        if let Err(error) = self.store.prune_before(app_id, checkpoint_seq).await {
            tracing::warn!(app_id, checkpoint_seq, error = ?error, "failed to prune kv journal");
        }
    }
}

#[async_trait]
impl KvBackend for S3KvBackend {
    async fn get(&self, app_id: u64, key: KvKey) -> Result<Option<Vec<u8>>, String> {
        let namespace = self.namespace(app_id).await;
        let mut state = namespace.lock().await;
        self.load_state(app_id, &mut state).await?;
        Ok(state.records.get(&key).cloned())
    }

    async fn exec(&self, app_id: u64, commands: Vec<KvCommand>) -> Result<(), String> {
        let namespace = self.namespace(app_id).await;
        let mut state = namespace.lock().await;
        self.load_state(app_id, &mut state).await?;

        let mut writes = Vec::new();
        for command in commands {
            match command {
                KvCommand::Set { key, value } => writes.push(KvWrite::Set { key, value }),
                KvCommand::Delete { key } => writes.push(KvWrite::Delete { key }),
                KvCommand::CheckValue { key, value } => {
                    let Some(existing) = state.records.get(&key) else {
                        return Err("kv check failed: key missing".to_string());
                    };
                    if existing != &value {
                        return Err("kv check failed: value mismatch".to_string());
                    }
                }
                KvCommand::CheckExists { key } => {
                    if !state.records.contains_key(&key) {
                        return Err("kv check failed: key missing".to_string());
                    }
                }
                KvCommand::CheckMissing { key } => {
                    if state.records.contains_key(&key) {
                        return Err("kv check failed: key exists".to_string());
                    }
                }
            }
        }

        let predicted_total_bytes = predicted_total_bytes(&state, &writes)?;
        if predicted_total_bytes > self.max_app_bytes {
            return Err(format!(
                "app {app_id} kv quota exceeded: {predicted_total_bytes} > {}",
                self.max_app_bytes
            ));
        }
        if writes.is_empty() {
            return Ok(());
        }

        let seq = state.last_seq.saturating_add(1);
        let journal = JournalRecord { seq, writes };
        let journal_bytes = bincode::serialize(&journal)
            .map_err(|error| format!("failed to encode kv journal entry: {error}"))?;
        self.store
            .append_journal(app_id, seq, &journal_bytes)
            .await
            .map_err(|error| format!("failed to append kv journal entry: {error:#}"))?;
        apply_journal_record(&mut state, journal)?;
        self.maybe_checkpoint(app_id, &mut state).await;
        Ok(())
    }

    async fn list_page(
        &self,
        app_id: u64,
        prefix: KvKey,
        start: Option<KvKey>,
        end: Option<KvKey>,
        after: Option<KvKey>,
    ) -> Result<KvListPage, String> {
        let namespace = self.namespace(app_id).await;
        let mut state = namespace.lock().await;
        self.load_state(app_id, &mut state).await?;

        let mut entries = state
            .records
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .filter(|(key, _)| start.as_ref().is_none_or(|start| *key >= start))
            .filter(|(key, _)| after.as_ref().is_none_or(|after| *key > after))
            .filter(|(key, _)| end.as_ref().is_none_or(|end| *key < end))
            .map(|(key, value)| KvEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        entries.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        let next_after = if entries.len() > terminal_games::kv::KV_LIST_PAGE_SIZE {
            let next_after = entries[terminal_games::kv::KV_LIST_PAGE_SIZE - 1]
                .key
                .clone();
            entries.truncate(terminal_games::kv::KV_LIST_PAGE_SIZE);
            Some(next_after)
        } else {
            None
        };
        Ok(KvListPage {
            entries,
            next_after,
        })
    }

    async fn storage_used(&self, app_id: u64) -> Result<u64, String> {
        let namespace = self.namespace(app_id).await;
        let mut state = namespace.lock().await;
        self.load_state(app_id, &mut state).await?;
        Ok(state.total_bytes)
    }
}

fn apply_journal_record(state: &mut AppState, journal: JournalRecord) -> Result<(), String> {
    if journal.seq <= state.last_seq {
        return Ok(());
    }
    if journal.seq != state.last_seq.saturating_add(1) {
        return Err(format!(
            "kv journal sequence gap: expected {}, got {}",
            state.last_seq.saturating_add(1),
            journal.seq
        ));
    }

    for write in journal.writes {
        match write {
            KvWrite::Set { key, value } => {
                let new_size = logical_entry_size(key.len(), value.len())
                    .map_err(|error| error.to_string())?;
                let old_size = state
                    .records
                    .get(&key)
                    .map(|existing| logical_entry_size(key.len(), existing.len()))
                    .transpose()
                    .map_err(|error| error.to_string())?
                    .unwrap_or(0);
                state.total_bytes = state
                    .total_bytes
                    .saturating_sub(old_size)
                    .saturating_add(new_size);
                state.records.insert(key, value);
            }
            KvWrite::Delete { key } => {
                if let Some(existing) = state.records.remove(&key) {
                    state.total_bytes = state.total_bytes.saturating_sub(
                        logical_entry_size(key.len(), existing.len())
                            .map_err(|error| error.to_string())?,
                    );
                }
            }
        }
    }

    state.last_seq = journal.seq;
    Ok(())
}

fn predicted_total_bytes(state: &AppState, writes: &[KvWrite]) -> Result<u64, String> {
    let mut total = state.total_bytes;
    for write in writes {
        match write {
            KvWrite::Set { key, value } => {
                let new_size = logical_entry_size(key.len(), value.len())
                    .map_err(|error| error.to_string())?;
                let old_size = state
                    .records
                    .get(key)
                    .map(|existing| logical_entry_size(key.len(), existing.len()))
                    .transpose()
                    .map_err(|error| error.to_string())?
                    .unwrap_or(0);
                total = total.saturating_sub(old_size).saturating_add(new_size);
            }
            KvWrite::Delete { key } => {
                if let Some(existing) = state.records.get(key) {
                    total = total.saturating_sub(
                        logical_entry_size(key.len(), existing.len())
                            .map_err(|error| error.to_string())?,
                    );
                }
            }
        }
    }
    Ok(total)
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

        Ok(Self { inner })
    }

    async fn get(&self, path: &str) -> Option<Vec<u8>> {
        match self.inner.get(&path.to_string()).await {
            Ok(Some(entry)) => Some(entry.value().clone()),
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(error = ?error, path, "failed to read kv cache");
                None
            }
        }
    }

    fn insert(&self, path: &str, bytes: Vec<u8>) {
        self.inner.insert(path.to_string(), bytes);
    }

    fn remove(&self, path: &str) {
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
        if cacheable && let Some(bytes) = self.cache.get(path).await {
            return Ok((RpRead::new(), bytes.into()));
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

enum KvStore {
    S3 {
        operator: Operator,
        cache: Arc<KvCache>,
    },
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

        Ok(Self::S3 { operator, cache })
    }
    async fn load_checkpoint(&self, app_id: u64) -> anyhow::Result<Option<Vec<u8>>> {
        match self {
            Self::S3 { operator, .. } => {
                let paths = list_s3_paths(operator, &checkpoint_prefix(app_id)).await?;
                let Some(path) = paths.last() else {
                    return Ok(None);
                };
                read_s3(operator, path, "read kv checkpoint").await
            }
        }
    }

    async fn load_journal_since(
        &self,
        app_id: u64,
        after_seq: u64,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        match self {
            Self::S3 { operator, .. } => {
                let paths = list_s3_paths(operator, &journal_prefix(app_id)).await?;
                let mut entries = Vec::new();
                for path in paths {
                    if parse_seq_from_path(&path).is_some_and(|seq| seq > after_seq)
                        && let Some(bytes) = read_s3(operator, &path, "read kv journal").await?
                    {
                        entries.push(bytes);
                    }
                }
                Ok(entries)
            }
        }
    }

    async fn append_journal(&self, app_id: u64, seq: u64, bytes: &[u8]) -> anyhow::Result<()> {
        match self {
            Self::S3 { operator, cache } => {
                write_s3(
                    operator,
                    cache,
                    &journal_path(app_id, seq),
                    bytes,
                    "write kv journal",
                )
                .await
            }
        }
    }

    async fn store_checkpoint(&self, app_id: u64, seq: u64, bytes: &[u8]) -> anyhow::Result<()> {
        match self {
            Self::S3 { operator, cache } => {
                write_s3(
                    operator,
                    cache,
                    &checkpoint_path(app_id, seq),
                    bytes,
                    "write kv checkpoint",
                )
                .await
            }
        }
    }

    async fn prune_before(&self, app_id: u64, seq: u64) -> anyhow::Result<()> {
        match self {
            Self::S3 { operator, cache } => {
                for path in list_s3_paths(operator, &journal_prefix(app_id)).await? {
                    if parse_seq_from_path(&path).is_some_and(|entry_seq| entry_seq <= seq) {
                        delete_s3(operator, cache, &path).await?;
                    }
                }
                for path in list_s3_paths(operator, &checkpoint_prefix(app_id)).await? {
                    if parse_seq_from_path(&path).is_some_and(|entry_seq| entry_seq < seq) {
                        delete_s3(operator, cache, &path).await?;
                    }
                }
                Ok(())
            }
        }
    }
}

fn checkpoint_path(app_id: u64, seq: u64) -> String {
    format!("app-{app_id}/checkpoint/{seq:020}.bin")
}

fn checkpoint_prefix(app_id: u64) -> String {
    format!("app-{app_id}/checkpoint/")
}

fn journal_path(app_id: u64, seq: u64) -> String {
    format!("app-{app_id}/journal/{seq:020}.bin")
}

fn journal_prefix(app_id: u64) -> String {
    format!("app-{app_id}/journal/")
}

async fn list_s3_paths(operator: &Operator, prefix: &str) -> anyhow::Result<Vec<String>> {
    let mut paths = operator
        .list_with(prefix)
        .recursive(true)
        .await
        .with_context(|| format!("failed to list kv objects under {prefix}"))?
        .into_iter()
        .filter(|entry| entry.metadata().mode() == EntryMode::FILE)
        .map(|entry| entry.path().to_string())
        .collect::<Vec<_>>();
    paths.sort_unstable();
    Ok(paths)
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

fn parse_seq_from_path(path: &str) -> Option<u64> {
    let name = path.rsplit('/').next()?;
    let seq = name.strip_suffix(".bin")?;
    seq.parse::<u64>().ok()
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
