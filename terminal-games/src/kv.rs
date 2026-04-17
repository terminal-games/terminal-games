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
pub const DEFAULT_KV_CHECKPOINT_INTERVAL_COMMITS: u64 = 256;
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

#[async_trait]
pub trait KvBackend: Send + Sync {
    async fn get(&self, app_id: u64, key: KvKey) -> Result<Option<Vec<u8>>, String>;

    async fn exec(&self, app_id: u64, commands: Vec<KvCommand>) -> Result<(), String>;

    async fn list_page(
        &self,
        app_id: u64,
        prefix: KvKey,
        start: Option<KvKey>,
        end: Option<KvKey>,
        after: Option<KvKey>,
    ) -> Result<KvListPage, String>;

    async fn storage_used(&self, app_id: u64) -> Result<u64, String>;
}

#[derive(Debug, Clone)]
pub struct LibsqlKvBackendOptions {
    pub path: PathBuf,
    pub max_app_bytes: u64,
    pub checkpoint_interval_commits: u64,
}

impl LibsqlKvBackendOptions {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            max_app_bytes: DEFAULT_KV_APP_MAX_BYTES,
            checkpoint_interval_commits: DEFAULT_KV_CHECKPOINT_INTERVAL_COMMITS,
        }
    }
}

pub async fn load_libsql_backend(
    options: LibsqlKvBackendOptions,
) -> anyhow::Result<Arc<dyn KvBackend>> {
    let checkpoint_interval_commits = options.checkpoint_interval_commits.max(1);
    let store = LibsqlKvStore::open(&options.path).await?;
    Ok(Arc::new(JournaledKvBackend {
        store: Arc::new(store),
        max_app_bytes: options.max_app_bytes,
        checkpoint_interval_commits,
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
struct JournaledKvBackend {
    store: Arc<LibsqlKvStore>,
    max_app_bytes: u64,
    checkpoint_interval_commits: u64,
    namespaces: Arc<Mutex<HashMap<u64, Arc<Mutex<AppState>>>>>,
}

#[derive(Clone)]
struct MeshKvBackend {
    mesh: Mesh,
    leader: NodeId,
    local_backend: Option<Arc<dyn KvBackend>>,
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

impl JournaledKvBackend {
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
            tracing::warn!(
                app_id,
                checkpoint_seq,
                error = ?error,
                "failed to store kv checkpoint"
            );
            return;
        }

        state.last_checkpoint_seq = checkpoint_seq;
        if let Err(error) = self.store.prune_before(app_id, checkpoint_seq).await {
            tracing::warn!(
                app_id,
                checkpoint_seq,
                error = ?error,
                "failed to prune kv journal"
            );
        }
    }
}

#[async_trait]
impl KvBackend for JournaledKvBackend {
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

    async fn storage_used(&self, app_id: u64) -> Result<u64, String> {
        let namespace = self.namespace(app_id).await;
        let mut state = namespace.lock().await;
        self.load_state(app_id, &mut state).await?;
        Ok(state.total_bytes)
    }
}

#[async_trait]
impl KvBackend for MeshKvBackend {
    async fn get(&self, app_id: u64, key: KvKey) -> Result<Option<Vec<u8>>, String> {
        if self.mesh.node() == self.leader {
            let backend = self
                .local_backend
                .as_ref()
                .ok_or_else(|| "kv backend is not configured on KV leader".to_string())?;
            backend.get(app_id, key).await
        } else {
            self.mesh
                .execute_kv_get_on_node(self.leader, app_id, key)
                .await
        }
    }

    async fn exec(&self, app_id: u64, commands: Vec<KvCommand>) -> Result<(), String> {
        if self.mesh.node() == self.leader {
            let backend = self
                .local_backend
                .as_ref()
                .ok_or_else(|| "kv backend is not configured on KV leader".to_string())?;
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
    ) -> Result<KvListPage, String> {
        if self.mesh.node() == self.leader {
            let backend = self
                .local_backend
                .as_ref()
                .ok_or_else(|| "kv backend is not configured on KV leader".to_string())?;
            backend.list_page(app_id, prefix, start, end, after).await
        } else {
            self.mesh
                .execute_kv_list_page_on_node(self.leader, app_id, prefix, start, end, after)
                .await
        }
    }

    async fn storage_used(&self, app_id: u64) -> Result<u64, String> {
        if self.mesh.node() == self.leader {
            let backend = self
                .local_backend
                .as_ref()
                .ok_or_else(|| "kv backend is not configured on KV leader".to_string())?;
            backend.storage_used(app_id).await
        } else {
            self.mesh
                .execute_kv_storage_used_on_node(self.leader, app_id)
                .await
        }
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
                CREATE TABLE IF NOT EXISTS kv_objects (
                    kind TEXT NOT NULL,
                    app_id INTEGER NOT NULL,
                    seq INTEGER NOT NULL,
                    data BLOB NOT NULL,
                    PRIMARY KEY (kind, app_id, seq)
                );
                ",
            )
            .await
            .context("failed to initialize kv libsql schema")?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    async fn load_checkpoint(&self, app_id: u64) -> anyhow::Result<Option<Vec<u8>>> {
        let app_id = to_i64(app_id, "app_id")?;
        let connection = self.connection.lock().await;
        let mut rows = connection
            .query(
                "SELECT data FROM kv_objects WHERE kind = 'checkpoint' AND app_id = ?1 ORDER BY seq DESC LIMIT 1",
                libsql::params![app_id],
            )
            .await
            .context("failed to query kv checkpoint")?;
        let Some(row) = rows
            .next()
            .await
            .context("failed to read kv checkpoint row")?
        else {
            return Ok(None);
        };
        Ok(Some(blob_from_value(row.get_value(0)?)?))
    }

    async fn load_journal_since(
        &self,
        app_id: u64,
        after_seq: u64,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let app_id = to_i64(app_id, "app_id")?;
        let after_seq = to_i64(after_seq, "after_seq")?;
        let connection = self.connection.lock().await;
        let mut rows = connection
            .query(
                "SELECT data FROM kv_objects WHERE kind = 'journal' AND app_id = ?1 AND seq > ?2 ORDER BY seq ASC",
                libsql::params![app_id, after_seq],
            )
            .await
            .context("failed to query kv journal")?;
        let mut entries = Vec::new();
        while let Some(row) = rows.next().await.context("failed to read kv journal row")? {
            entries.push(blob_from_value(row.get_value(0)?)?);
        }
        Ok(entries)
    }

    async fn append_journal(&self, app_id: u64, seq: u64, bytes: &[u8]) -> anyhow::Result<()> {
        self.insert_object("journal", app_id, seq, bytes).await
    }

    async fn store_checkpoint(&self, app_id: u64, seq: u64, bytes: &[u8]) -> anyhow::Result<()> {
        self.insert_object("checkpoint", app_id, seq, bytes).await
    }

    async fn insert_object(
        &self,
        kind: &str,
        app_id: u64,
        seq: u64,
        data: &[u8],
    ) -> anyhow::Result<()> {
        let app_id = to_i64(app_id, "app_id")?;
        let seq = to_i64(seq, "seq")?;
        let connection = self.connection.lock().await;
        connection
            .execute(
                "INSERT OR REPLACE INTO kv_objects (kind, app_id, seq, data) VALUES (?1, ?2, ?3, ?4)",
                libsql::params![kind, app_id, seq, data.to_vec()],
            )
            .await
            .with_context(|| format!("failed to persist kv {kind} object"))?;
        Ok(())
    }

    async fn prune_before(&self, app_id: u64, seq: u64) -> anyhow::Result<()> {
        let app_id = to_i64(app_id, "app_id")?;
        let seq = to_i64(seq, "seq")?;
        let connection = self.connection.lock().await;
        let tx = connection
            .transaction()
            .await
            .context("failed to start kv prune transaction")?;
        tx.execute(
            "DELETE FROM kv_objects WHERE app_id = ?1 AND ((kind = 'journal' AND seq <= ?2) OR (kind = 'checkpoint' AND seq < ?2))",
            libsql::params![app_id, seq],
        )
        .await
        .context("failed to prune kv objects")?;
        tx.commit().await.context("failed to commit kv prune")?;
        Ok(())
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
