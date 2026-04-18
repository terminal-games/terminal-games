// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context as _;
use async_trait::async_trait;
use libsql::{Builder, Connection, Value};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::mesh::{Mesh, NodeId};

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
    TooManyWritesInAtomicTransaction,
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
            Self::TooManyWritesInAtomicTransaction => {
                write!(f, "kv atomic transactions may contain at most one write")
            }
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
        if writes.len() > 1 {
            return Err(KvError::TooManyWritesInAtomicTransaction);
        }
    }
    Ok(writes)
}

#[derive(Debug, Clone)]
pub struct LibsqlKvBackendOptions {
    pub path: PathBuf,
    pub max_app_bytes: u64,
}

impl LibsqlKvBackendOptions {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            max_app_bytes: DEFAULT_KV_APP_MAX_BYTES,
        }
    }
}

pub async fn load_libsql_backend(
    options: LibsqlKvBackendOptions,
) -> anyhow::Result<Arc<dyn KvBackend>> {
    let store = LibsqlKvStore::open(&options.path).await?;
    Ok(Arc::new(LibsqlKvBackend {
        store: Arc::new(store),
        max_app_bytes: options.max_app_bytes,
        namespaces: Arc::new(Mutex::new(HashMap::new())),
    }))
}

pub fn load_mesh_backend(
    mesh: Mesh,
    leader: NodeId,
    local_backend: Option<Arc<dyn KvBackend>>,
) -> anyhow::Result<Arc<dyn KvBackend>> {
    if mesh.node() == leader && local_backend.is_none() {
        anyhow::bail!("kv leader node requires a local KV backend");
    }
    Ok(Arc::new(MeshKvBackend {
        mesh,
        leader,
        local_backend,
    }))
}

#[derive(Clone)]
struct LibsqlKvBackend {
    store: Arc<LibsqlKvStore>,
    max_app_bytes: u64,
    namespaces: Arc<Mutex<HashMap<u64, Arc<Mutex<AppState>>>>>,
}

#[derive(Clone)]
struct MeshKvBackend {
    mesh: Mesh,
    leader: NodeId,
    local_backend: Option<Arc<dyn KvBackend>>,
}

#[derive(Clone, Default)]
struct AppState {
    loaded: bool,
    total_bytes: u64,
    records: HashMap<KvKey, Vec<u8>>,
}

impl LibsqlKvBackend {
    async fn namespace(&self, app_id: u64) -> Arc<Mutex<AppState>> {
        let mut namespaces = self.namespaces.lock().await;
        namespaces
            .entry(app_id)
            .or_insert_with(|| Arc::new(Mutex::new(AppState::default())))
            .clone()
    }

    async fn load_state(&self, app_id: u64, state: &mut AppState) -> Result<(), KvError> {
        if state.loaded {
            return Ok(());
        }

        state.records =
            self.store.load_records(app_id).await.map_err(|error| {
                KvError::internal(format!("failed to load kv records: {error:#}"))
            })?;
        state.total_bytes = total_bytes_for_records(&state.records)
            .map_err(|error| KvError::internal(format!("failed to measure kv state: {error:#}")))?;
        state.loaded = true;
        Ok(())
    }
}

#[async_trait]
impl KvBackend for LibsqlKvBackend {
    async fn get(&self, app_id: u64, key: KvKey) -> Result<Option<Vec<u8>>, KvError> {
        let namespace = self.namespace(app_id).await;
        let mut state = namespace.lock().await;
        self.load_state(app_id, &mut state).await?;
        Ok(state.records.get(&key).cloned())
    }

    async fn exec(&self, app_id: u64, commands: Vec<KvCommand>) -> Result<(), KvError> {
        let namespace = self.namespace(app_id).await;
        let mut state = namespace.lock().await;
        self.load_state(app_id, &mut state).await?;

        let writes = collect_kv_writes(&commands)?;
        for command in &commands {
            match command {
                KvCommand::Set { .. } | KvCommand::Delete { .. } => {}
                KvCommand::CheckValue { key, value } => {
                    let Some(existing) = state.records.get(key) else {
                        return Err(KvError::CheckFailed(KvCheckFailedReason::KeyMissing));
                    };
                    if existing != value {
                        return Err(KvError::CheckFailed(KvCheckFailedReason::ValueMismatch));
                    }
                }
                KvCommand::CheckExists { key } => {
                    if !state.records.contains_key(key) {
                        return Err(KvError::CheckFailed(KvCheckFailedReason::KeyMissing));
                    }
                }
                KvCommand::CheckMissing { key } => {
                    if state.records.contains_key(key) {
                        return Err(KvError::CheckFailed(KvCheckFailedReason::KeyExists));
                    }
                }
            }
        }

        let predicted_total_bytes = predicted_total_bytes(&state, &writes)?;
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
            .apply_writes(app_id, &writes)
            .await
            .map_err(|error| {
                KvError::internal(format!("failed to persist kv writes: {error:#}"))
            })?;
        apply_writes(&mut state, writes.into_iter())?;
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
        self.store
            .list_page(app_id, prefix, start, end, after)
            .await
            .map_err(|error| KvError::internal(format!("failed to list kv entries: {error:#}")))
    }

    async fn storage_used(&self, app_id: u64) -> Result<u64, KvError> {
        let namespace = self.namespace(app_id).await;
        let mut state = namespace.lock().await;
        self.load_state(app_id, &mut state).await?;
        Ok(state.total_bytes)
    }
}

#[async_trait]
impl KvBackend for MeshKvBackend {
    async fn get(&self, app_id: u64, key: KvKey) -> Result<Option<Vec<u8>>, KvError> {
        if self.mesh.node() == self.leader {
            let backend = self.local_backend.as_ref().ok_or(KvError::Unavailable)?;
            backend.get(app_id, key).await
        } else {
            self.mesh
                .execute_kv_get_on_node(self.leader, app_id, key)
                .await
        }
    }

    async fn exec(&self, app_id: u64, commands: Vec<KvCommand>) -> Result<(), KvError> {
        if self.mesh.node() == self.leader {
            let backend = self.local_backend.as_ref().ok_or(KvError::Unavailable)?;
            backend.exec(app_id, commands).await
        } else {
            self.mesh
                .execute_kv_exec_on_node(self.leader, app_id, commands)
                .await
        }
    }

    async fn list_page(
        &self,
        app_id: u64,
        prefix: KvKey,
        start: Option<KvKey>,
        end: Option<KvKey>,
        after: Option<KvKey>,
    ) -> Result<KvListPage, KvError> {
        if self.mesh.node() == self.leader {
            let backend = self.local_backend.as_ref().ok_or(KvError::Unavailable)?;
            backend.list_page(app_id, prefix, start, end, after).await
        } else {
            self.mesh
                .execute_kv_list_page_on_node(self.leader, app_id, prefix, start, end, after)
                .await
        }
    }

    async fn storage_used(&self, app_id: u64) -> Result<u64, KvError> {
        if self.mesh.node() == self.leader {
            let backend = self.local_backend.as_ref().ok_or(KvError::Unavailable)?;
            backend.storage_used(app_id).await
        } else {
            self.mesh
                .execute_kv_storage_used_on_node(self.leader, app_id)
                .await
        }
    }
}

fn apply_writes(
    state: &mut AppState,
    writes: impl IntoIterator<Item = KvWrite>,
) -> Result<(), KvError> {
    for write in writes {
        match write {
            KvWrite::Set { key, value } => {
                let new_size = logical_entry_size(key.len(), value.len())
                    .map_err(|error| KvError::internal(error.to_string()))?;
                let old_size = state
                    .records
                    .get(&key)
                    .map(|existing| logical_entry_size(key.len(), existing.len()))
                    .transpose()
                    .map_err(|error| KvError::internal(error.to_string()))?
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
                            .map_err(|error| KvError::internal(error.to_string()))?,
                    );
                }
            }
        }
    }
    Ok(())
}

fn predicted_total_bytes(state: &AppState, writes: &[KvWrite]) -> Result<u64, KvError> {
    let mut predicted = state.clone();
    apply_writes(&mut predicted, writes.iter().cloned())?;
    Ok(predicted.total_bytes)
}

fn logical_entry_size(key_len: usize, value_len: usize) -> anyhow::Result<u64> {
    let total = key_len
        .checked_add(value_len)
        .context("entry size overflow")?;
    u64::try_from(total).context("entry size exceeds u64")
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

fn total_bytes_for_records(records: &HashMap<KvKey, Vec<u8>>) -> anyhow::Result<u64> {
    let mut total = 0_u64;
    for (key, value) in records {
        total = total
            .checked_add(logical_entry_size(key.len(), value.len())?)
            .context("kv state size overflow")?;
    }
    Ok(total)
}

#[derive(Clone)]
struct LibsqlKvStore {
    connection: Arc<Mutex<Connection>>,
}

impl LibsqlKvStore {
    async fn open(path: &Path) -> anyhow::Result<Self> {
        ensure_parent_dir(path).await?;

        let database = Builder::new_local(path)
            .build()
            .await
            .with_context(|| format!("failed to open kv libsql database {}", path.display()))?;
        let connection = database
            .connect()
            .context("failed to create kv libsql connection")?;
        connection
            .execute_batch(
                "
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = FULL;
                CREATE TABLE IF NOT EXISTS kv_entries (
                    app_id INTEGER NOT NULL,
                    key BLOB NOT NULL,
                    value BLOB NOT NULL,
                    PRIMARY KEY (app_id, key)
                ) WITHOUT ROWID;
                ",
            )
            .await
            .context("failed to initialize kv libsql schema")?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    async fn load_records(&self, app_id: u64) -> anyhow::Result<HashMap<KvKey, Vec<u8>>> {
        let app_id = to_i64(app_id, "app_id")?;
        let connection = self.connection.lock().await;
        let mut rows = connection
            .query(
                "SELECT key, value FROM kv_entries WHERE app_id = ?1",
                libsql::params![app_id],
            )
            .await
            .context("failed to query kv entries")?;
        let mut records = HashMap::new();
        while let Some(row) = rows.next().await.context("failed to read kv entry row")? {
            records.insert(
                blob_from_value(row.get_value(0)?)?,
                blob_from_value(row.get_value(1)?)?,
            );
        }
        Ok(records)
    }

    async fn apply_writes(&self, app_id: u64, writes: &[KvWrite]) -> anyhow::Result<()> {
        let app_id = to_i64(app_id, "app_id")?;
        let connection = self.connection.lock().await;
        let tx = connection
            .transaction()
            .await
            .context("failed to start kv write transaction")?;
        for write in writes {
            match write {
                KvWrite::Set { key, value } => {
                    tx.execute(
                        "INSERT OR REPLACE INTO kv_entries (app_id, key, value) VALUES (?1, ?2, ?3)",
                        libsql::params![app_id, key.clone(), value.clone()],
                    )
                    .await
                    .context("failed to upsert kv entry")?;
                }
                KvWrite::Delete { key } => {
                    tx.execute(
                        "DELETE FROM kv_entries WHERE app_id = ?1 AND key = ?2",
                        libsql::params![app_id, key.clone()],
                    )
                    .await
                    .context("failed to delete kv entry")?;
                }
            }
        }
        tx.commit().await.context("failed to commit kv write")?;
        Ok(())
    }

    async fn list_page(
        &self,
        app_id: u64,
        prefix: KvKey,
        start: Option<KvKey>,
        end: Option<KvKey>,
        after: Option<KvKey>,
    ) -> anyhow::Result<KvListPage> {
        let app_id = to_i64(app_id, "app_id")?;
        let connection = self.connection.lock().await;

        let mut query = "SELECT key, value FROM kv_entries WHERE app_id = ?1".to_string();
        let mut params = vec![Value::Integer(app_id)];
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

        query.push_str(&format!(
            " ORDER BY key ASC LIMIT {}",
            KV_LIST_PAGE_SIZE + 1
        ));

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
        other => anyhow::bail!("expected BLOB from kv libsql row, got {other:?}"),
    }
}
