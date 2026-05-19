// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

mod mesh;

pub use mesh::load_mesh_backend;

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
};

use anyhow::Context as _;
use async_trait::async_trait;
use aws_sdk_s3::{
    Client,
    config::{
        BehaviorVersion, Credentials, Region, RequestChecksumCalculation,
        ResponseChecksumValidation,
    },
    error::SdkError,
    operation::get_object::GetObjectError,
    primitives::ByteStream,
};
use libsql::{Builder, Connection, Value};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::db::DbPool;

pub const DEFAULT_NAMESPACE_MAX_BYTES: u64 = 256 * 1024 * 1024;
const LIST_PAGE_SIZE: usize = 64;
const MAX_CACHE_ENTRIES: usize = 4096;

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
enum KvWrite {
    Set { key: KvKey, value: Vec<u8> },
    Delete { key: KvKey },
}

impl KvWrite {
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
    async fn get(&self, namespace_id: u64, key: KvKey) -> Result<Option<Vec<u8>>, KvError>;

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
pub struct S3KvBackendOptions {
    pub bucket: String,
    pub prefix: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub quota: Arc<dyn KvQuota>,
    pub usage: Arc<dyn KvUsageStore>,
}

impl S3KvBackendOptions {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: String::new(),
            region: None,
            endpoint: None,
            quota: static_quota(DEFAULT_NAMESPACE_MAX_BYTES),
            usage: memory_usage_store(),
        }
    }
}

pub async fn load_s3_backend(options: S3KvBackendOptions) -> anyhow::Result<Arc<dyn KvBackend>> {
    Ok(Arc::new(S3KvBackend {
        store: Arc::new(S3KvStore::from_options(&options).await?),
        quota: options.quota,
        usage: options.usage,
        namespaces: Mutex::new(HashMap::new()),
    }))
}

struct S3KvBackend {
    store: Arc<S3KvStore>,
    quota: Arc<dyn KvQuota>,
    usage: Arc<dyn KvUsageStore>,
    namespaces: Mutex<HashMap<u64, Arc<Mutex<()>>>>,
}

#[async_trait]
impl KvBackend for S3KvBackend {
    async fn get(&self, namespace_id: u64, key: KvKey) -> Result<Option<Vec<u8>>, KvError> {
        self.store
            .get_entry(namespace_id, &key)
            .await
            .map_err(|error| KvError::internal(format!("failed to read kv entry: {error:#}")))
    }

    async fn exec(&self, namespace_id: u64, commands: Vec<KvCommand>) -> Result<(), KvError> {
        let request = parse_write_request(&commands)?;
        let namespace_lock = self.namespace_lock(namespace_id).await;
        let _namespace_lock = namespace_lock.lock().await;

        let checked_values = self
            .store
            .load_entries(namespace_id, checked_keys(&commands))
            .await
            .map_err(|error| KvError::internal(format!("failed to load kv checks: {error:#}")))?;
        validate_checks(&commands, &checked_values)?;

        let Some(write) = request.write else {
            return Ok(());
        };

        let old_value = self
            .store
            .get_entry(namespace_id, write.key())
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
            KvWrite::Set { key, value } => {
                self.store
                    .write_entry(namespace_id, key, value)
                    .await
                    .map_err(|error| {
                        KvError::internal(format!("failed to write kv entry: {error:#}"))
                    })?;
            }
            KvWrite::Delete { key } => {
                self.store
                    .delete_entry(namespace_id, key)
                    .await
                    .map_err(|error| {
                        KvError::internal(format!("failed to delete kv entry: {error:#}"))
                    })?;
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
            .list_entries_page(namespace_id, prefix, start, end, after)
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

impl S3KvBackend {
    async fn namespace_lock(&self, namespace_id: u64) -> Arc<Mutex<()>> {
        let mut namespaces = self.namespaces.lock().await;
        namespaces
            .entry(namespace_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

#[derive(Clone)]
pub struct SqliteKvBackendOptions {
    pub path: PathBuf,
    pub quota: Arc<dyn KvQuota>,
    pub usage: Arc<dyn KvUsageStore>,
}

impl SqliteKvBackendOptions {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            quota: static_quota(DEFAULT_NAMESPACE_MAX_BYTES),
            usage: memory_usage_store(),
        }
    }
}

pub async fn load_sqlite_backend(
    options: SqliteKvBackendOptions,
) -> anyhow::Result<Arc<dyn KvBackend>> {
    Ok(Arc::new(SqliteKvBackend {
        store: Arc::new(SqliteKvStore::open(&options.path).await?),
        quota: options.quota,
        usage: options.usage,
        namespaces: Mutex::new(HashMap::new()),
    }))
}

struct SqliteKvBackend {
    store: Arc<SqliteKvStore>,
    quota: Arc<dyn KvQuota>,
    usage: Arc<dyn KvUsageStore>,
    namespaces: Mutex<HashMap<u64, Arc<Mutex<()>>>>,
}

#[async_trait]
impl KvBackend for SqliteKvBackend {
    async fn get(&self, namespace_id: u64, key: KvKey) -> Result<Option<Vec<u8>>, KvError> {
        self.store
            .get_entry(namespace_id, &key)
            .await
            .map_err(|error| KvError::internal(format!("failed to read kv entry: {error:#}")))
    }

    async fn exec(&self, namespace_id: u64, commands: Vec<KvCommand>) -> Result<(), KvError> {
        let request = parse_write_request(&commands)?;
        let namespace_lock = self.namespace_lock(namespace_id).await;
        let _namespace_lock = namespace_lock.lock().await;

        let checked_values = self
            .store
            .load_entries(namespace_id, checked_keys(&commands))
            .await
            .map_err(|error| KvError::internal(format!("failed to load kv checks: {error:#}")))?;
        validate_checks(&commands, &checked_values)?;

        let Some(write) = request.write else {
            return Ok(());
        };

        let old_value = self
            .store
            .get_entry(namespace_id, write.key())
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

        self.store
            .apply_write(namespace_id, &write)
            .await
            .map_err(|error| KvError::internal(format!("failed to write kv entry: {error:#}")))?;
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
            .list_entries_page(namespace_id, prefix, start, end, after)
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

impl SqliteKvBackend {
    async fn namespace_lock(&self, namespace_id: u64) -> Arc<Mutex<()>> {
        let mut namespaces = self.namespaces.lock().await;
        namespaces
            .entry(namespace_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

struct WriteRequest {
    write: Option<KvWrite>,
}

fn parse_write_request(commands: &[KvCommand]) -> Result<WriteRequest, KvError> {
    let mut write = None;
    for command in commands {
        let next = match command {
            KvCommand::Set { key, value } => Some(KvWrite::Set {
                key: key.clone(),
                value: value.clone(),
            }),
            KvCommand::Delete { key } => Some(KvWrite::Delete { key: key.clone() }),
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
    Ok(WriteRequest { write })
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
    values: &HashMap<KvKey, Option<Vec<u8>>>,
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

fn exclusive_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
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

fn storage_delta(write: &KvWrite, old_value: Option<&[u8]>) -> Result<i64, KvError> {
    let old_size = match old_value {
        Some(value) => logical_entry_size(write.key().len(), value.len())
            .map_err(|error| KvError::internal(error.to_string()))?,
        None => 0,
    };
    let new_size = match write {
        KvWrite::Set { key, value } => logical_entry_size(key.len(), value.len())
            .map_err(|error| KvError::internal(error.to_string()))?,
        KvWrite::Delete { .. } => 0,
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

#[derive(Clone, Default)]
struct KvCache {
    entries: Arc<StdMutex<HashMap<String, Option<Vec<u8>>>>>,
}

impl KvCache {
    fn lookup(&self, path: &str) -> Option<Option<Vec<u8>>> {
        self.entries.lock().unwrap().get(path).cloned()
    }

    fn insert(&self, path: &str, bytes: Vec<u8>) {
        let mut entries = self.entries.lock().unwrap();
        if entries.len() >= MAX_CACHE_ENTRIES && !entries.contains_key(path) {
            entries.clear();
        }
        entries.insert(path.to_string(), Some(bytes));
    }

    fn remove(&self, path: &str) {
        let mut entries = self.entries.lock().unwrap();
        if entries.len() >= MAX_CACHE_ENTRIES && !entries.contains_key(path) {
            entries.clear();
        }
        entries.insert(path.to_string(), None);
    }
}

struct S3KvStore {
    client: Client,
    bucket: String,
    prefix: String,
    cache: KvCache,
}

impl S3KvStore {
    async fn from_options(options: &S3KvBackendOptions) -> anyhow::Result<Self> {
        let region = options
            .region
            .clone()
            .or_else(|| s3_env_string("AWS_REGION"))
            .or_else(|| s3_env_string("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|| "us-east-1".to_string());
        let mut config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region))
            .force_path_style(options.endpoint.is_some())
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired);
        if let Some(endpoint) = &options.endpoint {
            config = config.endpoint_url(endpoint);
        }
        if let Some(credentials) = s3_env_credentials() {
            config = config.credentials_provider(credentials);
        }
        Ok(Self {
            client: Client::from_conf(config.build()),
            bucket: options.bucket.clone(),
            prefix: normalize_s3_prefix(&options.prefix),
            cache: KvCache::default(),
        })
    }

    async fn get_entry(&self, namespace_id: u64, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let path = entry_path(namespace_id, key);
        if let Some(value) = self.cache.lookup(&path) {
            return Ok(value);
        }
        let value = self.read_s3(&path, "read kv entry").await?;
        if let Some(value) = &value {
            self.cache.insert(&path, value.clone());
        } else {
            self.cache.remove(&path);
        }
        Ok(value)
    }

    async fn write_entry(&self, namespace_id: u64, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        let path = entry_path(namespace_id, key);
        self.write_s3(&path, value, "write kv entry").await?;
        self.cache.insert(&path, value.to_vec());
        Ok(())
    }

    async fn delete_entry(&self, namespace_id: u64, key: &[u8]) -> anyhow::Result<()> {
        let path = entry_path(namespace_id, key);
        self.delete_s3(&path).await?;
        self.cache.remove(&path);
        Ok(())
    }

    async fn load_entries(
        &self,
        namespace_id: u64,
        keys: impl IntoIterator<Item = KvKey>,
    ) -> anyhow::Result<HashMap<KvKey, Option<Vec<u8>>>> {
        let mut values = HashMap::new();
        for key in keys {
            values.insert(key.clone(), self.get_entry(namespace_id, &key).await?);
        }
        Ok(values)
    }

    async fn list_entries_page(
        &self,
        namespace_id: u64,
        prefix: KvKey,
        start: Option<KvKey>,
        end: Option<KvKey>,
        after: Option<KvKey>,
    ) -> anyhow::Result<KvListPage> {
        let list_prefix = self.object_key(&entry_prefix_for_key_prefix(namespace_id, &prefix));
        let start_after = after
            .as_ref()
            .filter(|after| after.starts_with(&prefix))
            .map(|after| self.object_key(&entry_path(namespace_id, after)));
        let mut continuation_token = None;
        let mut entries = Vec::new();

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&list_prefix)
                .max_keys(1000);
            if let Some(token) = continuation_token.take() {
                request = request.continuation_token(token);
            } else if let Some(start_after) = &start_after {
                request = request.start_after(start_after);
            }

            let output = request
                .send()
                .await
                .context("failed to list kv entries from S3")?;
            for object in output.contents() {
                let Some(object_key) = object.key() else {
                    continue;
                };
                let Some(key) = parse_entry_key_from_path(object_key) else {
                    continue;
                };
                if !key_in_range(
                    &key,
                    &prefix,
                    start.as_deref(),
                    end.as_deref(),
                    after.as_deref(),
                ) {
                    continue;
                }
                if let Some(value) = self.get_entry(namespace_id, &key).await? {
                    entries.push(KvEntry { key, value });
                }
                if entries.len() > LIST_PAGE_SIZE {
                    break;
                }
            }

            if entries.len() > LIST_PAGE_SIZE || !output.is_truncated().unwrap_or(false) {
                break;
            }
            continuation_token = output.next_continuation_token().map(str::to_string);
            if continuation_token.is_none() {
                break;
            }
        }

        Ok(page_from_entries(entries))
    }

    fn object_key(&self, path: &str) -> String {
        if self.prefix.is_empty() {
            path.to_string()
        } else {
            format!("{}/{}", self.prefix, path)
        }
    }

    async fn read_s3(&self, path: &str, context: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let object_key = self.object_key(path);
        let output = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&object_key)
            .send()
            .await
        {
            Ok(output) => output,
            Err(error) if is_s3_not_found(&error) => return Ok(None),
            Err(error) => {
                return Err(error).with_context(|| format!("failed to {context} {object_key}"));
            }
        };
        let bytes = output
            .body
            .collect()
            .await
            .with_context(|| format!("failed to read S3 object body {object_key}"))?
            .into_bytes();
        Ok(Some(bytes.to_vec()))
    }

    async fn write_s3(&self, path: &str, bytes: &[u8], context: &str) -> anyhow::Result<()> {
        let object_key = self.object_key(path);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&object_key)
            .body(ByteStream::from(bytes.to_vec()))
            .send()
            .await
            .with_context(|| format!("failed to {context} {object_key}"))?;
        Ok(())
    }

    async fn delete_s3(&self, path: &str) -> anyhow::Result<()> {
        let object_key = self.object_key(path);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&object_key)
            .send()
            .await
            .with_context(|| format!("failed to delete kv object {object_key}"))?;
        Ok(())
    }
}

fn normalize_s3_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}

fn s3_env_credentials() -> Option<Credentials> {
    Some(Credentials::new(
        s3_env_string("AWS_ACCESS_KEY_ID")?,
        s3_env_string("AWS_SECRET_ACCESS_KEY")?,
        s3_env_string("AWS_SESSION_TOKEN"),
        None,
        "terminal-games-env",
    ))
}

fn s3_env_string(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
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

fn entry_path(namespace_id: u64, key: &[u8]) -> String {
    format!("namespace/{namespace_id}/entries/{}.bin", hex::encode(key))
}

fn entry_prefix(namespace_id: u64) -> String {
    format!("namespace/{namespace_id}/entries/")
}

fn entry_prefix_for_key_prefix(namespace_id: u64, prefix: &[u8]) -> String {
    format!("{}{}", entry_prefix(namespace_id), hex::encode(prefix))
}

fn parse_entry_key_from_path(path: &str) -> Option<Vec<u8>> {
    let name = path.rsplit('/').next()?;
    let encoded = name.strip_suffix(".bin")?;
    hex::decode(encoded).ok()
}

fn is_s3_not_found(error: &SdkError<GetObjectError>) -> bool {
    match error {
        SdkError::ServiceError(error) => {
            error.err().is_no_such_key() || error.err().meta().code() == Some("NoSuchKey")
        }
        _ => false,
    }
}

struct SqliteKvStore {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteKvStore {
    async fn open(path: &Path) -> anyhow::Result<Self> {
        ensure_parent_dir(path).await?;
        let database = Builder::new_local(path)
            .build()
            .await
            .with_context(|| format!("failed to open kv sqlite database {}", path.display()))?;
        let connection = database
            .connect()
            .context("failed to create kv sqlite connection")?;
        connection
            .execute_batch(
                "
                CREATE TABLE IF NOT EXISTS kv_entries (
                    namespace_id INTEGER NOT NULL,
                    key BLOB NOT NULL,
                    value BLOB NOT NULL,
                    PRIMARY KEY (namespace_id, key)
                ) WITHOUT ROWID;
                ",
            )
            .await
            .context("failed to initialize kv sqlite schema")?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    async fn get_entry(&self, namespace_id: u64, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let namespace_id = to_i64(namespace_id, "namespace_id")?;
        let connection = self.connection.lock().await;
        let mut rows = connection
            .query(
                "SELECT value FROM kv_entries WHERE namespace_id = ?1 AND key = ?2",
                libsql::params![namespace_id, key.to_vec()],
            )
            .await
            .context("failed to query kv entry")?;
        Ok(
            match rows.next().await.context("failed to read kv entry row")? {
                Some(row) => Some(blob_from_value(row.get_value(0)?)?),
                None => None,
            },
        )
    }

    async fn load_entries(
        &self,
        namespace_id: u64,
        keys: impl IntoIterator<Item = KvKey>,
    ) -> anyhow::Result<HashMap<KvKey, Option<Vec<u8>>>> {
        let mut values = HashMap::new();
        for key in keys {
            values.insert(key.clone(), self.get_entry(namespace_id, &key).await?);
        }
        Ok(values)
    }

    async fn apply_write(&self, namespace_id: u64, write: &KvWrite) -> anyhow::Result<()> {
        let namespace_id = to_i64(namespace_id, "namespace_id")?;
        let connection = self.connection.lock().await;
        match write {
            KvWrite::Set { key, value } => {
                connection
                    .execute(
                        "INSERT OR REPLACE INTO kv_entries (namespace_id, key, value) VALUES (?1, ?2, ?3)",
                        libsql::params![namespace_id, key.clone(), value.clone()],
                    )
                    .await
                    .context("failed to upsert kv entry")?;
            }
            KvWrite::Delete { key } => {
                connection
                    .execute(
                        "DELETE FROM kv_entries WHERE namespace_id = ?1 AND key = ?2",
                        libsql::params![namespace_id, key.clone()],
                    )
                    .await
                    .context("failed to delete kv entry")?;
            }
        }
        Ok(())
    }

    async fn list_entries_page(
        &self,
        namespace_id: u64,
        prefix: KvKey,
        start: Option<KvKey>,
        end: Option<KvKey>,
        after: Option<KvKey>,
    ) -> anyhow::Result<KvListPage> {
        let namespace_id = to_i64(namespace_id, "namespace_id")?;
        let connection = self.connection.lock().await;
        let mut query = "SELECT key, value FROM kv_entries WHERE namespace_id = ?1".to_string();
        let mut params = vec![Value::Integer(namespace_id)];
        let mut param_index = 2;

        if !prefix.is_empty() {
            append_blob_filter(
                &mut query,
                &mut params,
                &mut param_index,
                "key >= ",
                prefix.clone(),
            );
            if let Some(prefix_end) = exclusive_upper_bound(&prefix) {
                append_blob_filter(
                    &mut query,
                    &mut params,
                    &mut param_index,
                    "key < ",
                    prefix_end,
                );
            }
        }
        if let Some(start) = start {
            append_blob_filter(&mut query, &mut params, &mut param_index, "key >= ", start);
        }
        if let Some(after) = after {
            append_blob_filter(&mut query, &mut params, &mut param_index, "key > ", after);
        }
        if let Some(end) = end {
            append_blob_filter(&mut query, &mut params, &mut param_index, "key < ", end);
        }

        query.push_str(&format!(" ORDER BY key ASC LIMIT {}", LIST_PAGE_SIZE + 1));

        let mut rows = connection
            .query(&query, libsql::params_from_iter(params))
            .await
            .context("failed to query kv page")?;
        let mut entries = Vec::new();
        while let Some(row) = rows.next().await.context("failed to read kv page row")? {
            entries.push(KvEntry {
                key: blob_from_value(row.get_value(0)?)?,
                value: blob_from_value(row.get_value(1)?)?,
            });
        }
        Ok(page_from_entries(entries))
    }
}

fn append_blob_filter(
    query: &mut String,
    params: &mut Vec<Value>,
    param_index: &mut usize,
    predicate: &str,
    value: Vec<u8>,
) {
    query.push_str(" AND ");
    query.push_str(predicate);
    query.push('?');
    query.push_str(&param_index.to_string());
    params.push(Value::Blob(value));
    *param_index += 1;
}

async fn ensure_parent_dir(path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    Ok(())
}

fn to_i64(value: u64, field: &str) -> anyhow::Result<i64> {
    i64::try_from(value).with_context(|| format!("{field} exceeds i64"))
}

fn blob_from_value(value: Value) -> anyhow::Result<Vec<u8>> {
    match value {
        Value::Blob(bytes) => Ok(bytes),
        other => anyhow::bail!("expected BLOB from kv sqlite row, got {other:?}"),
    }
}
