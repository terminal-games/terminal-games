use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context as _;
use async_trait::async_trait;
use libsql::{Builder, Connection, Value};
use tokio::sync::Mutex;

use super::{
    DEFAULT_KV_APP_MAX_BYTES, KV_LIST_PAGE_SIZE, KvBackend, KvCheckFailedReason, KvCommand,
    KvEntry, KvError, KvKey, KvListPage, KvWrite, collect_kv_writes, exclusive_upper_bound,
    logical_entry_size,
};

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
    Ok(Arc::new(LibsqlKvBackend {
        store: Arc::new(LibsqlKvStore::open(&options.path).await?),
        max_app_bytes: options.max_app_bytes,
    }))
}

#[derive(Clone)]
struct LibsqlKvBackend {
    store: Arc<LibsqlKvStore>,
    max_app_bytes: u64,
}

#[async_trait]
impl KvBackend for LibsqlKvBackend {
    async fn get(&self, app_id: u64, key: KvKey) -> Result<Option<Vec<u8>>, KvError> {
        self.store
            .get_entry(app_id, &key)
            .await
            .map_err(|error| KvError::internal(format!("failed to read kv entry: {error:#}")))
    }

    async fn exec(&self, app_id: u64, commands: Vec<KvCommand>) -> Result<(), KvError> {
        let writes = collect_kv_writes(&commands)?;
        let app_id =
            to_i64(app_id, "app_id").map_err(|error| KvError::internal(error.to_string()))?;
        let connection = self.store.connection.lock().await;
        let tx = connection.transaction().await.map_err(|error| {
            KvError::internal(format!("failed to start kv transaction: {error:#}"))
        })?;
        let mut records = self
            .store
            .load_records_in_tx(&tx, app_id)
            .await
            .map_err(|error| KvError::internal(format!("failed to load kv records: {error:#}")))?;

        for command in &commands {
            match command {
                KvCommand::Set { .. } | KvCommand::Delete { .. } => {}
                KvCommand::CheckValue { key, value } => match records.get(key) {
                    Some(existing) if existing == value => {}
                    Some(_) => {
                        return Err(KvError::CheckFailed(KvCheckFailedReason::ValueMismatch));
                    }
                    None => return Err(KvError::CheckFailed(KvCheckFailedReason::KeyMissing)),
                },
                KvCommand::CheckExists { key } if !records.contains_key(key) => {
                    return Err(KvError::CheckFailed(KvCheckFailedReason::KeyMissing));
                }
                KvCommand::CheckMissing { key } if records.contains_key(key) => {
                    return Err(KvError::CheckFailed(KvCheckFailedReason::KeyExists));
                }
                KvCommand::CheckExists { .. } | KvCommand::CheckMissing { .. } => {}
            }
        }

        let mut used_bytes = total_bytes_for_records(&records)
            .map_err(|error| KvError::internal(format!("failed to measure kv state: {error:#}")))?;
        for write in &writes {
            match write {
                KvWrite::Set { key, value } => {
                    let new_size = logical_entry_size(key.len(), value.len())
                        .map_err(|error| KvError::internal(error.to_string()))?;
                    let old_size = records
                        .insert(key.clone(), value.clone())
                        .map(|existing| logical_entry_size(key.len(), existing.len()))
                        .transpose()
                        .map_err(|error| KvError::internal(error.to_string()))?
                        .unwrap_or(0);
                    used_bytes = used_bytes.saturating_sub(old_size).saturating_add(new_size);
                }
                KvWrite::Delete { key } => {
                    if let Some(existing) = records.remove(key) {
                        used_bytes = used_bytes.saturating_sub(
                            logical_entry_size(key.len(), existing.len())
                                .map_err(|error| KvError::internal(error.to_string()))?,
                        );
                    }
                }
            }
        }
        if used_bytes > self.max_app_bytes {
            return Err(KvError::QuotaExceeded {
                app_id: u64::try_from(app_id).unwrap(),
                used_bytes,
                limit_bytes: self.max_app_bytes,
            });
        }
        if writes.is_empty() {
            return Ok(());
        }

        for write in &writes {
            match write {
                KvWrite::Set { key, value } => {
                    tx.execute(
                        "INSERT OR REPLACE INTO kv_entries (app_id, key, value) VALUES (?1, ?2, ?3)",
                        libsql::params![app_id, key.clone(), value.clone()],
                    )
                    .await
                    .map_err(|error| KvError::internal(format!("failed to upsert kv entry: {error:#}")))?;
                }
                KvWrite::Delete { key } => {
                    tx.execute(
                        "DELETE FROM kv_entries WHERE app_id = ?1 AND key = ?2",
                        libsql::params![app_id, key.clone()],
                    )
                    .await
                    .map_err(|error| {
                        KvError::internal(format!("failed to delete kv entry: {error:#}"))
                    })?;
                }
            }
        }
        tx.commit()
            .await
            .map_err(|error| KvError::internal(format!("failed to commit kv write: {error:#}")))?;
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
        self.store
            .storage_used(app_id)
            .await
            .map_err(|error| KvError::internal(format!("failed to query kv usage: {error:#}")))
    }
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

    async fn get_entry(&self, app_id: u64, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let app_id = to_i64(app_id, "app_id")?;
        let connection = self.connection.lock().await;
        let mut rows = connection
            .query(
                "SELECT value FROM kv_entries WHERE app_id = ?1 AND key = ?2",
                libsql::params![app_id, key.to_vec()],
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

    async fn storage_used(&self, app_id: u64) -> anyhow::Result<u64> {
        let app_id = to_i64(app_id, "app_id")?;
        let connection = self.connection.lock().await;
        let mut rows = connection
            .query(
                "SELECT COALESCE(SUM(length(key) + length(value)), 0) FROM kv_entries WHERE app_id = ?1",
                libsql::params![app_id],
            )
            .await
            .context("failed to query kv usage")?;
        let row = rows
            .next()
            .await
            .context("failed to read kv usage row")?
            .context("missing kv usage row")?;
        integer_from_value(row.get_value(0)?)
    }

    async fn load_records_in_tx(
        &self,
        tx: &libsql::Transaction,
        app_id: i64,
    ) -> anyhow::Result<HashMap<KvKey, Vec<u8>>> {
        let mut rows = tx
            .query(
                "SELECT key, value FROM kv_entries WHERE app_id = ?1",
                libsql::params![app_id],
            )
            .await
            .context("failed to query kv records")?;
        let mut records = HashMap::new();
        while let Some(row) = rows.next().await.context("failed to read kv row")? {
            records.insert(
                blob_from_value(row.get_value(0)?)?,
                blob_from_value(row.get_value(1)?)?,
            );
        }
        Ok(records)
    }

    async fn list_page(
        &self,
        app_id: u64,
        prefix: KvKey,
        start: Option<KvKey>,
        end: Option<KvKey>,
        after: Option<KvKey>,
    ) -> anyhow::Result<super::KvListPage> {
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

fn integer_from_value(value: Value) -> anyhow::Result<u64> {
    match value {
        Value::Integer(value) => {
            u64::try_from(value).context("expected non-negative integer from kv libsql row")
        }
        other => anyhow::bail!("expected INTEGER from kv libsql row, got {other:?}"),
    }
}
