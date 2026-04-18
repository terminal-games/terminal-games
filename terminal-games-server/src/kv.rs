// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    path::PathBuf,
    sync::Arc,
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
use terminal_games::kv::{
    DEFAULT_KV_APP_MAX_BYTES, KV_LIST_PAGE_SIZE, KvBackend, KvCheckFailedReason, KvCommand,
    KvEntry, KvError, KvKey, KvListPage, KvWrite, LibsqlKvBackendOptions, collect_kv_writes,
    load_libsql_backend,
};
use tokio::sync::Mutex;

const DEFAULT_KV_S3_PREFIX: &str = "kv";
const DEFAULT_KV_CACHE_DIR: &str = "/opt/terminal-games/kv-cache";
const DEFAULT_KV_CACHE_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_KV_CACHE_DISK_BYTES: u64 = 32 * 1024 * 1024 * 1024;

pub async fn load_backend_from_env() -> anyhow::Result<Arc<dyn KvBackend>> {
    let max_app_bytes = read_env_u64("KV_APP_MAX_BYTES").unwrap_or(DEFAULT_KV_APP_MAX_BYTES);
    let backend_kind = std::env::var("KV_BACKEND")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "libsql".to_string());

    match backend_kind.as_str() {
        "s3" => Ok(Arc::new(S3KvBackend {
            store: Arc::new(KvStore::from_s3_env().await?),
            max_app_bytes,
            namespaces: Arc::new(Mutex::new(HashMap::new())),
        })),
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
    namespaces: Arc<Mutex<HashMap<u64, Arc<Mutex<()>>>>>,
}

impl S3KvBackend {
    async fn namespace(&self, app_id: u64) -> Arc<Mutex<()>> {
        let mut namespaces = self.namespaces.lock().await;
        namespaces
            .entry(app_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

#[async_trait]
impl KvBackend for S3KvBackend {
    async fn get(&self, app_id: u64, key: KvKey) -> Result<Option<Vec<u8>>, KvError> {
        let namespace = self.namespace(app_id).await;
        let _guard = namespace.lock().await;
        self.store
            .get_entry(app_id, &key)
            .await
            .map_err(|error| KvError::internal(format!("failed to read kv entry: {error:#}")))
    }

    async fn exec(&self, app_id: u64, commands: Vec<KvCommand>) -> Result<(), KvError> {
        let namespace = self.namespace(app_id).await;
        let _guard = namespace.lock().await;
        let writes = collect_kv_writes(&commands)?;

        let touched_keys = commands
            .iter()
            .map(|command| match command {
                KvCommand::Set { key, .. }
                | KvCommand::Delete { key }
                | KvCommand::CheckValue { key, .. }
                | KvCommand::CheckExists { key }
                | KvCommand::CheckMissing { key } => key.clone(),
            })
            .collect::<HashSet<_>>();

        let current_values = self
            .store
            .load_entries(app_id, touched_keys)
            .await
            .map_err(|error| KvError::internal(format!("failed to load kv entries: {error:#}")))?;
        let current_total_bytes =
            self.store.storage_used(app_id).await.map_err(|error| {
                KvError::internal(format!("failed to measure kv usage: {error:#}"))
            })?;

        for command in &commands {
            match command {
                KvCommand::Set { .. } | KvCommand::Delete { .. } => {}
                KvCommand::CheckValue { key, value } => {
                    let Some(existing) = current_values.get(key).and_then(|value| value.as_ref())
                    else {
                        return Err(KvError::CheckFailed(KvCheckFailedReason::KeyMissing));
                    };
                    if existing != value {
                        return Err(KvError::CheckFailed(KvCheckFailedReason::ValueMismatch));
                    }
                }
                KvCommand::CheckExists { key } => {
                    if current_values
                        .get(key)
                        .and_then(|value| value.as_ref())
                        .is_none()
                    {
                        return Err(KvError::CheckFailed(KvCheckFailedReason::KeyMissing));
                    }
                }
                KvCommand::CheckMissing { key } => {
                    if current_values
                        .get(key)
                        .and_then(|value| value.as_ref())
                        .is_some()
                    {
                        return Err(KvError::CheckFailed(KvCheckFailedReason::KeyExists));
                    }
                }
            }
        }

        let predicted_total_bytes =
            predicted_total_bytes(current_total_bytes, &current_values, &writes)?;
        if predicted_total_bytes > self.max_app_bytes {
            return Err(KvError::QuotaExceeded {
                app_id,
                used_bytes: predicted_total_bytes,
                limit_bytes: self.max_app_bytes,
            });
        }
        if writes.is_empty() {
            return Ok(());
        }

        self.store
            .apply_entry_writes(app_id, &writes)
            .await
            .map_err(|error| {
                KvError::internal(format!("failed to persist kv entries: {error:#}"))
            })?;
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
        let _guard = namespace.lock().await;
        self.store
            .list_entries_page(app_id, prefix, start, end, after)
            .await
            .map_err(|error| KvError::internal(format!("failed to list kv entries: {error:#}")))
    }

    async fn storage_used(&self, app_id: u64) -> Result<u64, KvError> {
        let namespace = self.namespace(app_id).await;
        let _guard = namespace.lock().await;
        self.store
            .storage_used(app_id)
            .await
            .map_err(|error| KvError::internal(format!("failed to measure kv usage: {error:#}")))
    }
}

fn predicted_total_bytes(
    current_total_bytes: u64,
    current_values: &HashMap<KvKey, Option<Vec<u8>>>,
    writes: &[KvWrite],
) -> Result<u64, KvError> {
    let mut total = current_total_bytes;
    let mut current_values = current_values.clone();

    for write in writes {
        match write {
            KvWrite::Set { key, value } => {
                let new_size = logical_entry_size(key.len(), value.len())
                    .map_err(|error| KvError::internal(error.to_string()))?;
                let old_size = current_values
                    .get(key)
                    .and_then(|value| value.as_ref())
                    .map(|existing| logical_entry_size(key.len(), existing.len()))
                    .transpose()
                    .map_err(|error| KvError::internal(error.to_string()))?
                    .unwrap_or(0);
                total = total.saturating_sub(old_size).saturating_add(new_size);
                current_values.insert(key.clone(), Some(value.clone()));
            }
            KvWrite::Delete { key } => {
                if let Some(existing) = current_values.get(key).and_then(|value| value.as_ref()) {
                    total = total.saturating_sub(
                        logical_entry_size(key.len(), existing.len())
                            .map_err(|error| KvError::internal(error.to_string()))?,
                    );
                }
                current_values.insert(key.clone(), None);
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
                    write_s3(
                        &self.operator,
                        &self.cache,
                        &entry_path(app_id, key),
                        value,
                        "write kv entry",
                    )
                    .await?;
                }
                KvWrite::Delete { key } => {
                    delete_s3(&self.operator, &self.cache, &entry_path(app_id, key)).await?;
                }
            }
        }
        Ok(())
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

    async fn storage_used(&self, app_id: u64) -> anyhow::Result<u64> {
        let mut lister = self
            .operator
            .lister_with(&entry_prefix(app_id))
            .recursive(true)
            .await
            .context("failed to create kv usage lister")?;
        let mut total = 0_u64;

        while let Some(entry) = lister.try_next().await.context("failed to list kv usage")? {
            if entry.metadata().mode() != EntryMode::FILE {
                continue;
            }
            let Some(key) = parse_entry_key_from_path(entry.path()) else {
                continue;
            };
            let value_len = usize::try_from(entry.metadata().content_length())
                .context("kv entry value length exceeds usize")?;
            total = total
                .checked_add(logical_entry_size(key.len(), value_len)?)
                .context("kv usage overflow")?;
        }

        Ok(total)
    }
}

fn entry_path(app_id: u64, key: &[u8]) -> String {
    format!("app/{app_id}/entries/{}.bin", hex::encode(key))
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
