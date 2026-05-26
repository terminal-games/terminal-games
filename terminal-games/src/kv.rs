// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod mesh;

pub use mesh::load_mesh_backend;

use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
};

use anyhow::Context as _;
use async_trait::async_trait;
use bytes::Bytes;
use foyer::{BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder};
use futures::TryStreamExt;
use opendal::{ErrorKind, Operator, services};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::db::DbPool;

pub const DEFAULT_NAMESPACE_MAX_BYTES: u64 = 256 * 1024 * 1024;
const LIST_PAGE_SIZE: usize = 64;
const CACHE_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const CACHE_DISK_BYTES: usize = 4 * 1024 * 1024 * 1024;

pub type KvKey = Bytes;
pub type KvValue = Bytes;
type KvCache = foyer::HybridCache<String, Bytes>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvEntry {
    pub key: KvKey,
    pub value: KvValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvListPage {
    pub entries: Vec<KvEntry>,
    pub next_after: Option<KvKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KvCommand {
    Set { key: KvKey, value: KvValue },
    Delete { key: KvKey },
    CheckValue { key: KvKey, value: KvValue },
    CheckExists { key: KvKey },
    CheckMissing { key: KvKey },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum Write {
    Set { key: KvKey, value: KvValue },
    Delete { key: KvKey },
}

impl Write {
    fn key(&self) -> &[u8] {
        match self {
            Self::Set { key, .. } | Self::Delete { key } => key,
        }
    }
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
        namespace_id: u64,
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
                namespace_id,
                used_bytes,
                limit_bytes,
            } => write!(
                f,
                "kv namespace {namespace_id} quota exceeded: {used_bytes} > {limit_bytes}"
            ),
            Self::Unavailable => write!(f, "kv unavailable"),
            Self::Internal(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for KvError {}

#[async_trait]
pub trait KvQuota: Send + Sync {
    async fn max_bytes(&self, namespace_id: u64) -> u64;
}

#[async_trait]
pub trait KvUsageStore: Send + Sync {
    async fn storage_used(&self, namespace_id: u64) -> anyhow::Result<u64>;

    async fn apply_delta(&self, namespace_id: u64, delta: i64) -> anyhow::Result<u64>;
}

#[derive(Debug, Clone)]
pub struct StaticKvQuota {
    max_bytes: u64,
}

impl StaticKvQuota {
    pub fn new(max_bytes: u64) -> Self {
        Self { max_bytes }
    }
}

#[async_trait]
impl KvQuota for StaticKvQuota {
    async fn max_bytes(&self, _namespace_id: u64) -> u64 {
        self.max_bytes
    }
}

pub fn static_quota(max_bytes: u64) -> Arc<dyn KvQuota> {
    Arc::new(StaticKvQuota::new(max_bytes))
}

#[derive(Default)]
struct MemoryKvUsageStore {
    usage: Mutex<HashMap<u64, u64>>,
}

#[async_trait]
impl KvUsageStore for MemoryKvUsageStore {
    async fn storage_used(&self, namespace_id: u64) -> anyhow::Result<u64> {
        Ok(self
            .usage
            .lock()
            .await
            .get(&namespace_id)
            .copied()
            .unwrap_or(0))
    }

    async fn apply_delta(&self, namespace_id: u64, delta: i64) -> anyhow::Result<u64> {
        let mut usage = self.usage.lock().await;
        let current = usage.get(&namespace_id).copied().unwrap_or(0);
        let next = apply_size_delta(current, delta)?;
        usage.insert(namespace_id, next);
        Ok(next)
    }
}

pub fn memory_usage_store() -> Arc<dyn KvUsageStore> {
    Arc::new(MemoryKvUsageStore::default())
}

pub struct LibsqlKvUsageStore {
    db: DbPool,
}

impl LibsqlKvUsageStore {
    pub fn new(db: DbPool) -> Self {
        Self { db }
    }
}

#[async_trait]
impl KvUsageStore for LibsqlKvUsageStore {
    async fn storage_used(&self, namespace_id: u64) -> anyhow::Result<u64> {
        let namespace_id = to_i64(namespace_id, "namespace_id")?;
        let db = self.db.get().await?;
        let mut rows = db
            .query(
                "SELECT kv_storage_bytes FROM apps WHERE id = ?1 LIMIT 1",
                libsql::params!(namespace_id),
            )
            .await
            .context("failed to query kv storage usage")?;
        Ok(
            match rows.next().await.context("failed to read kv usage row")? {
                Some(row) => row.get::<u64>(0)?,
                None => 0,
            },
        )
    }

    async fn apply_delta(&self, namespace_id: u64, delta: i64) -> anyhow::Result<u64> {
        let namespace_id = to_i64(namespace_id, "namespace_id")?;
        let db = self.db.get().await?;
        let mut rows = db
            .query(
                "UPDATE apps
                 SET kv_storage_bytes = kv_storage_bytes + ?2
                 WHERE id = ?1 AND kv_storage_bytes + ?2 >= 0
                 RETURNING kv_storage_bytes",
                libsql::params!(namespace_id, delta),
            )
            .await
            .context("failed to update kv storage usage")?;
        let Some(row) = rows
            .next()
            .await
            .context("failed to read updated kv usage")?
        else {
            anyhow::bail!("failed to update kv storage usage for namespace {namespace_id}");
        };
        Ok(row.get::<u64>(0)?)
    }
}

pub fn libsql_usage_store(db: DbPool) -> Arc<dyn KvUsageStore> {
    Arc::new(LibsqlKvUsageStore::new(db))
}

#[async_trait]
pub trait KvBackend: Send + Sync {
    async fn get(&self, namespace_id: u64, key: KvKey) -> Result<Option<KvValue>, KvError>;

    async fn exec(&self, namespace_id: u64, commands: Vec<KvCommand>) -> Result<(), KvError>;

    async fn list_page(
        &self,
        namespace_id: u64,
        prefix: KvKey,
        start: Option<KvKey>,
        end: Option<KvKey>,
        after: Option<KvKey>,
    ) -> Result<KvListPage, KvError>;

    async fn storage_used(&self, namespace_id: u64) -> Result<u64, KvError>;
}

#[derive(Clone)]
pub struct OpenDalKvBackendOptions {
    pub operator: OpenDalKvOperator,
    pub quota: Arc<dyn KvQuota>,
    pub usage: Arc<dyn KvUsageStore>,
}

impl OpenDalKvBackendOptions {
    pub fn new(operator: OpenDalKvOperator) -> Self {
        Self {
            operator,
            quota: static_quota(DEFAULT_NAMESPACE_MAX_BYTES),
            usage: memory_usage_store(),
        }
    }
}

#[derive(Clone)]
pub struct OpenDalKvOperator {
    operator: Operator,
    cache: KvCache,
}

impl OpenDalKvOperator {
    async fn new(operator: Operator) -> anyhow::Result<Self> {
        Ok(Self {
            operator,
            cache: foyer_cache().await?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct OpenDalS3Options {
    pub bucket: String,
    pub prefix: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
}

impl OpenDalS3Options {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: String::new(),
            region: None,
            endpoint: None,
        }
    }
}

pub async fn opendal_fs_operator(root: impl AsRef<Path>) -> anyhow::Result<OpenDalKvOperator> {
    std::fs::create_dir_all(root.as_ref())
        .with_context(|| format!("failed to create {}", root.as_ref().display()))?;
    let root = root
        .as_ref()
        .to_str()
        .with_context(|| format!("kv root path is not UTF-8: {}", root.as_ref().display()))?;
    OpenDalKvOperator::new(Operator::new(services::Fs::default().root(root))?.finish()).await
}

pub async fn opendal_s3_operator(options: OpenDalS3Options) -> anyhow::Result<OpenDalKvOperator> {
    let mut builder = services::S3::default()
        .bucket(&options.bucket)
        .root(options.prefix.trim_matches('/'));
    if let Some(region) = options.region.as_deref() {
        builder = builder.region(region);
    }
    if let Some(endpoint) = options.endpoint.as_deref() {
        builder = builder.endpoint(endpoint);
    }
    OpenDalKvOperator::new(Operator::new(builder)?.finish()).await
}

async fn foyer_cache() -> anyhow::Result<KvCache> {
    let cache_dir = std::env::temp_dir().join("terminal-games-kv-foyer");
    let device = FsDeviceBuilder::new(cache_dir)
        .with_capacity(CACHE_DISK_BYTES)
        .build()
        .context("failed to build kv foyer cache device")?;
    HybridCacheBuilder::<String, Bytes>::new()
        .memory(CACHE_MEMORY_BYTES)
        .with_weighter(|key: &String, value: &Bytes| key.len() + value.len())
        .storage()
        .with_engine_config(BlockEngineConfig::new(device))
        .build()
        .await
        .context("failed to build kv foyer cache")
}

pub async fn load_opendal_backend(
    options: OpenDalKvBackendOptions,
) -> anyhow::Result<Arc<dyn KvBackend>> {
    Ok(Arc::new(OpenDalKvBackend {
        store: OpenDalStore::new(options.operator),
        quota: options.quota,
        usage: options.usage,
        locks: NamespaceLocks::default(),
    }))
}

struct OpenDalKvBackend {
    store: OpenDalStore,
    quota: Arc<dyn KvQuota>,
    usage: Arc<dyn KvUsageStore>,
    locks: NamespaceLocks,
}

#[async_trait]
impl KvBackend for OpenDalKvBackend {
    async fn get(&self, namespace_id: u64, key: KvKey) -> Result<Option<KvValue>, KvError> {
        self.store
            .get(namespace_id, &key)
            .await
            .map_err(|error| KvError::internal(format!("failed to read kv entry: {error:#}")))
    }

    async fn exec(&self, namespace_id: u64, commands: Vec<KvCommand>) -> Result<(), KvError> {
        let write = parse_write(&commands)?;
        let lock = self.locks.get(namespace_id).await;
        let _guard = lock.lock().await;

        let checked_values = self
            .store
            .load_many(namespace_id, checked_keys(&commands))
            .await
            .map_err(|error| KvError::internal(format!("failed to load kv checks: {error:#}")))?;
        validate_checks(&commands, &checked_values)?;

        let Some(write) = write else {
            return Ok(());
        };

        let old_value = self
            .store
            .get(namespace_id, write.key())
            .await
            .map_err(|error| {
                KvError::internal(format!("failed to read existing kv entry: {error:#}"))
            })?;
        let delta = storage_delta(&write, old_value.as_deref())?;
        let used_bytes = self
            .usage
            .storage_used(namespace_id)
            .await
            .map_err(|error| {
                KvError::internal(format!("failed to load kv storage usage: {error:#}"))
            })?;
        let predicted_bytes = apply_size_delta(used_bytes, delta)
            .map_err(|error| KvError::internal(error.to_string()))?;
        let limit_bytes = self.quota.max_bytes(namespace_id).await;
        if predicted_bytes > limit_bytes {
            return Err(KvError::QuotaExceeded {
                namespace_id,
                used_bytes: predicted_bytes,
                limit_bytes,
            });
        }

        match &write {
            Write::Set { key, value } => self
                .store
                .put(namespace_id, key, value.clone())
                .await
                .map_err(|error| {
                    KvError::internal(format!("failed to write kv entry: {error:#}"))
                })?,
            Write::Delete { key } => {
                self.store
                    .delete(namespace_id, key)
                    .await
                    .map_err(|error| {
                        KvError::internal(format!("failed to delete kv entry: {error:#}"))
                    })?
            }
        }
        if delta != 0 {
            self.usage
                .apply_delta(namespace_id, delta)
                .await
                .map_err(|error| {
                    KvError::internal(format!("failed to update kv storage usage: {error:#}"))
                })?;
        }
        Ok(())
    }

    async fn list_page(
        &self,
        namespace_id: u64,
        prefix: KvKey,
        start: Option<KvKey>,
        end: Option<KvKey>,
        after: Option<KvKey>,
    ) -> Result<KvListPage, KvError> {
        self.store
            .list_page(namespace_id, prefix, start, end, after)
            .await
            .map_err(|error| KvError::internal(format!("failed to list kv entries: {error:#}")))
    }

    async fn storage_used(&self, namespace_id: u64) -> Result<u64, KvError> {
        self.usage
            .storage_used(namespace_id)
            .await
            .map_err(|error| {
                KvError::internal(format!("failed to load kv storage usage: {error:#}"))
            })
    }
}

#[derive(Default)]
struct NamespaceLocks {
    namespaces: Mutex<HashMap<u64, Arc<Mutex<()>>>>,
}

impl NamespaceLocks {
    async fn get(&self, namespace_id: u64) -> Arc<Mutex<()>> {
        let mut namespaces = self.namespaces.lock().await;
        namespaces
            .entry(namespace_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

fn parse_write(commands: &[KvCommand]) -> Result<Option<Write>, KvError> {
    let mut write = None;
    for command in commands {
        let next = match command {
            KvCommand::Set { key, value } => Some(Write::Set {
                key: key.clone(),
                value: value.clone(),
            }),
            KvCommand::Delete { key } => Some(Write::Delete { key: key.clone() }),
            KvCommand::CheckValue { .. }
            | KvCommand::CheckExists { .. }
            | KvCommand::CheckMissing { .. } => None,
        };
        if let Some(next) = next {
            if write.is_some() {
                return Err(KvError::internal("kv exec may modify only one key"));
            }
            write = Some(next);
        }
    }
    Ok(write)
}

fn checked_keys(commands: &[KvCommand]) -> Vec<KvKey> {
    let mut seen = HashSet::new();
    let mut keys = Vec::new();
    for command in commands {
        let key = match command {
            KvCommand::CheckValue { key, .. }
            | KvCommand::CheckExists { key }
            | KvCommand::CheckMissing { key } => key,
            KvCommand::Set { .. } | KvCommand::Delete { .. } => continue,
        };
        if seen.insert(key.clone()) {
            keys.push(key.clone());
        }
    }
    keys
}

fn validate_checks(
    commands: &[KvCommand],
    values: &HashMap<KvKey, Option<KvValue>>,
) -> Result<(), KvError> {
    for command in commands {
        match command {
            KvCommand::Set { .. } | KvCommand::Delete { .. } => {}
            KvCommand::CheckValue { key, value } => {
                let Some(existing) = values.get(key).and_then(|value| value.as_ref()) else {
                    return Err(KvError::CheckFailed(KvCheckFailedReason::KeyMissing));
                };
                if existing != value {
                    return Err(KvError::CheckFailed(KvCheckFailedReason::ValueMismatch));
                }
            }
            KvCommand::CheckExists { key } => {
                if values.get(key).and_then(|value| value.as_ref()).is_none() {
                    return Err(KvError::CheckFailed(KvCheckFailedReason::KeyMissing));
                }
            }
            KvCommand::CheckMissing { key } => {
                if values.get(key).and_then(|value| value.as_ref()).is_some() {
                    return Err(KvError::CheckFailed(KvCheckFailedReason::KeyExists));
                }
            }
        }
    }
    Ok(())
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

fn storage_delta(write: &Write, old_value: Option<&[u8]>) -> Result<i64, KvError> {
    let old_size = match old_value {
        Some(value) => logical_entry_size(write.key().len(), value.len())
            .map_err(|error| KvError::internal(error.to_string()))?,
        None => 0,
    };
    let new_size = match write {
        Write::Set { key, value } => logical_entry_size(key.len(), value.len())
            .map_err(|error| KvError::internal(error.to_string()))?,
        Write::Delete { .. } => 0,
    };
    let delta = i128::from(new_size) - i128::from(old_size);
    i64::try_from(delta).map_err(|_| KvError::internal("kv storage delta exceeds i64"))
}

fn apply_size_delta(current: u64, delta: i64) -> anyhow::Result<u64> {
    if delta >= 0 {
        current
            .checked_add(delta as u64)
            .context("kv storage usage overflow")
    } else {
        current
            .checked_sub(delta.unsigned_abs())
            .context("kv storage usage underflow")
    }
}

fn page_from_entries(mut entries: Vec<KvEntry>) -> KvListPage {
    let next_after = if entries.len() > LIST_PAGE_SIZE {
        let next_after = entries[LIST_PAGE_SIZE - 1].key.clone();
        entries.truncate(LIST_PAGE_SIZE);
        Some(next_after)
    } else {
        None
    };
    KvListPage {
        entries,
        next_after,
    }
}

struct OpenDalStore {
    operator: Operator,
    cache: KvCache,
}

impl OpenDalStore {
    fn new(operator: OpenDalKvOperator) -> Self {
        Self {
            operator: operator.operator,
            cache: operator.cache,
        }
    }

    async fn get(&self, namespace_id: u64, key: &[u8]) -> anyhow::Result<Option<KvValue>> {
        let path = entry_path(namespace_id, key);
        if let Some(entry) = self.cache.get(&path).await? {
            return Ok(Some(entry.value().clone()));
        }
        match self.operator.read(&path).await {
            Ok(bytes) => {
                let value = bytes.to_bytes();
                self.cache.insert(path, value.clone());
                Ok(Some(value))
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).with_context(|| format!("failed to read {path}")),
        }
    }

    async fn put(&self, namespace_id: u64, key: &[u8], value: KvValue) -> anyhow::Result<()> {
        let path = entry_path(namespace_id, key);
        self.operator
            .write(&path, value.clone())
            .await
            .with_context(|| format!("failed to write {path}"))?;
        self.cache.remove(&path);
        self.cache.insert(path, value);
        Ok(())
    }

    async fn delete(&self, namespace_id: u64, key: &[u8]) -> anyhow::Result<()> {
        let path = entry_path(namespace_id, key);
        self.operator
            .delete(&path)
            .await
            .with_context(|| format!("failed to delete {path}"))?;
        self.cache.remove(&path);
        Ok(())
    }

    async fn load_many(
        &self,
        namespace_id: u64,
        keys: Vec<KvKey>,
    ) -> anyhow::Result<HashMap<KvKey, Option<KvValue>>> {
        let mut values = HashMap::new();
        for key in keys {
            values.insert(key.clone(), self.get(namespace_id, &key).await?);
        }
        Ok(values)
    }

    async fn list_page(
        &self,
        namespace_id: u64,
        prefix: KvKey,
        start: Option<KvKey>,
        end: Option<KvKey>,
        after: Option<KvKey>,
    ) -> anyhow::Result<KvListPage> {
        let mut lister = self
            .operator
            .lister_with(&entry_prefix_for_key_prefix(namespace_id, &prefix))
            .recursive(false)
            .await
            .context("failed to list kv entries")?;
        let mut keys = Vec::new();

        while let Some(entry) = lister.try_next().await? {
            if !entry.metadata().mode().is_file() {
                continue;
            }
            let Some(key) = key_from_entry_path(entry.path()) else {
                continue;
            };
            if key_in_range(
                &key,
                &prefix,
                start.as_deref(),
                end.as_deref(),
                after.as_deref(),
            ) {
                keys.push(key);
            }
        }

        keys.sort();
        let mut entries = Vec::new();
        for key in keys.into_iter().take(LIST_PAGE_SIZE + 1) {
            if let Some(value) = self.get(namespace_id, &key).await? {
                entries.push(KvEntry { key, value });
            }
        }
        Ok(page_from_entries(entries))
    }
}

fn entry_path(namespace_id: u64, key: &[u8]) -> String {
    format!("namespace/{namespace_id}/entries/{}.bin", hex::encode(key))
}

fn entry_prefix(namespace_id: u64) -> String {
    format!("namespace/{namespace_id}/entries/")
}

fn entry_prefix_for_key_prefix(namespace_id: u64, prefix: &[u8]) -> String {
    format!("{}{}", entry_prefix(namespace_id), hex::encode(prefix))
}

fn key_from_entry_path(path: &str) -> Option<KvKey> {
    let name = path.rsplit('/').next()?;
    let encoded = name.strip_suffix(".bin")?;
    hex::decode(encoded).ok().map(Bytes::from)
}

fn to_i64(value: u64, field: &str) -> anyhow::Result<i64> {
    i64::try_from(value).with_context(|| format!("{field} exceeds i64"))
}
