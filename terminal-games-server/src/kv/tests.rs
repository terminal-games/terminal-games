
use std::{
    collections::{BTreeSet, VecDeque},
    sync::{Arc, Mutex as StdMutex},
};

use opendal::raw::oio;
use opendal::raw::{
    Access, Layer, LayeredAccess, OpDelete, OpList, OpRead, OpWrite, RpDelete, RpList, RpRead,
    RpWrite,
};
use opendal::{Error as OpenDalError, ErrorKind as OpenDalErrorKind, Operator, services::Memory};
use tempfile::TempDir;

use super::*;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct MockBucketStats {
    reads: usize,
    writes: usize,
    deletes: usize,
    lists: usize,
    entry_write_requests: usize,
}

#[derive(Default)]
struct MockBucketState {
    stats: MockBucketStats,
    fail_entry_write_requests: BTreeSet<usize>,
}

#[derive(Clone, Default)]
struct MockBucket {
    inner: Arc<StdMutex<MockBucketState>>,
}

impl MockBucket {
    fn stats(&self) -> MockBucketStats {
        self.inner.lock().unwrap().stats.clone()
    }

    fn fail_entry_write_requests(&self, requests: impl IntoIterator<Item = usize>) {
        let mut state = self.inner.lock().unwrap();
        state.fail_entry_write_requests = requests.into_iter().collect();
    }

    fn clear_failures(&self) {
        self.inner.lock().unwrap().fail_entry_write_requests.clear();
    }

    fn record_read(&self) {
        self.inner.lock().unwrap().stats.reads += 1;
    }

    fn record_write(&self, path: &str) -> opendal::Result<()> {
        let mut state = self.inner.lock().unwrap();
        state.stats.writes += 1;
        if path.contains("/entries/") {
            state.stats.entry_write_requests += 1;
            if state
                .fail_entry_write_requests
                .contains(&state.stats.entry_write_requests)
            {
                return Err(OpenDalError::new(
                    OpenDalErrorKind::Unexpected,
                    "injected entry write failure",
                ));
            }
        }
        Ok(())
    }

    fn record_delete(&self) {
        self.inner.lock().unwrap().stats.deletes += 1;
    }

    fn record_list(&self) {
        self.inner.lock().unwrap().stats.lists += 1;
    }
}

#[derive(Clone)]
struct CountingLayer {
    bucket: MockBucket,
}

impl CountingLayer {
    fn new(bucket: MockBucket) -> Self {
        Self { bucket }
    }
}

impl<A: Access> Layer<A> for CountingLayer {
    type LayeredAccess = CountingAccess<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        CountingAccess {
            inner,
            bucket: self.bucket.clone(),
        }
    }
}

#[derive(Clone)]
struct CountingAccess<A: Access> {
    inner: A,
    bucket: MockBucket,
}

impl<A: Access> fmt::Debug for CountingAccess<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CountingAccess")
    }
}

impl<A: Access> LayeredAccess for CountingAccess<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = A::Writer;
    type Lister = A::Lister;
    type Deleter = CountingDeleter<A::Deleter>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
        self.bucket.record_read();
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> opendal::Result<(RpWrite, Self::Writer)> {
        self.bucket.record_write(path)?;
        self.inner.write(path, args).await
    }

    async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
        let (rp, deleter) = self.inner.delete().await?;
        Ok((
            rp,
            CountingDeleter {
                inner: deleter,
                bucket: self.bucket.clone(),
            },
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
        let _ = path;
        self.bucket.record_list();
        self.inner.list(path, args).await
    }
}

struct CountingDeleter<D> {
    inner: D,
    bucket: MockBucket,
}

impl<D: oio::Delete> oio::Delete for CountingDeleter<D> {
    fn delete(&mut self, path: &str, args: OpDelete) -> opendal::Result<()> {
        let _ = path;
        self.bucket.record_delete();
        self.inner.delete(path, args)
    }

    async fn flush(&mut self) -> opendal::Result<usize> {
        self.inner.flush().await
    }
}

struct TestHarness {
    backend: Arc<S3KvBackend>,
    store: Arc<KvStore>,
    bucket: MockBucket,
    _cache_dir: TempDir,
}

impl TestHarness {
    async fn new(background_checkpointing: bool) -> Self {
        Self::with_max_app_bytes(background_checkpointing, DEFAULT_KV_APP_MAX_BYTES).await
    }

    async fn with_max_app_bytes(background_checkpointing: bool, max_app_bytes: u64) -> Self {
        let cache_dir = TempDir::new().unwrap();
        let cache = Arc::new(
            KvCache::open(cache_dir.path().join("cache"), 1024 * 1024, 8 * 1024 * 1024)
                .await
                .unwrap(),
        );
        let bucket = MockBucket::default();
        let operator = Operator::new(Memory::default())
            .unwrap()
            .layer(CountingLayer::new(bucket.clone()))
            .layer(FoyerLayer::new(cache.clone()))
            .finish();
        let store = Arc::new(KvStore { operator, cache });
        let backend = S3KvBackend::new(store.clone(), max_app_bytes, background_checkpointing);
        Self {
            backend,
            store,
            bucket,
            _cache_dir: cache_dir,
        }
    }

    async fn cache_value(&self, path: &str) -> Option<Vec<u8>> {
        self.store.cache.get(path).await
    }
}

fn set(key: &[u8], value: &[u8]) -> KvCommand {
    KvCommand::Set {
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

fn delete(key: &[u8]) -> KvCommand {
    KvCommand::Delete { key: key.to_vec() }
}

#[tokio::test]
async fn s3_single_set_loads_once_updates_cache_and_immediate_read_is_cached() {
    let harness = TestHarness::new(false).await;
    let app_id = 1;
    let key = b"a".to_vec();
    let value = b"value".to_vec();

    harness
        .backend
        .exec(app_id, vec![set(&key, &value)])
        .await
        .unwrap();

    assert_eq!(
        harness.bucket.stats(),
        MockBucketStats {
            reads: 1,
            writes: 1,
            deletes: 0,
            lists: 1,
            entry_write_requests: 1,
        }
    );
    assert_eq!(
        harness.cache_value(&entry_path(app_id, &key)).await,
        Some(value.clone())
    );

    let before = harness.bucket.stats();
    assert_eq!(harness.backend.get(app_id, key).await.unwrap(), Some(value));
    assert_eq!(harness.bucket.stats(), before);
}

#[tokio::test]
async fn s3_subsequent_single_overwrite_is_one_put_and_usage_stays_exact() {
    let harness = TestHarness::new(false).await;
    let app_id = 2;

    harness
        .backend
        .exec(app_id, vec![set(b"a", b"alpha")])
        .await
        .unwrap();
    let before = harness.bucket.stats();

    harness
        .backend
        .exec(app_id, vec![set(b"a", b"bet")])
        .await
        .unwrap();

    assert_eq!(
        harness.bucket.stats(),
        MockBucketStats {
            reads: before.reads,
            writes: before.writes + 1,
            deletes: before.deletes,
            lists: before.lists,
            entry_write_requests: before.entry_write_requests + 1,
        }
    );
    assert_eq!(
        harness.backend.get(app_id, b"a".to_vec()).await.unwrap(),
        Some(b"bet".to_vec())
    );
    assert_eq!(
        harness.backend.storage_used(app_id).await.unwrap(),
        logical_entry_size(1, 3).unwrap()
    );
}

#[tokio::test]
async fn s3_single_delete_is_one_delete_and_tombstones_the_cache() {
    let harness = TestHarness::new(false).await;
    let app_id = 3;
    let key = b"gone".to_vec();
    let path = entry_path(app_id, &key);

    harness
        .backend
        .exec(app_id, vec![set(&key, b"value")])
        .await
        .unwrap();
    let before = harness.bucket.stats();

    harness
        .backend
        .exec(app_id, vec![delete(&key)])
        .await
        .unwrap();

    assert_eq!(
        harness.bucket.stats(),
        MockBucketStats {
            reads: before.reads,
            writes: before.writes,
            deletes: before.deletes + 1,
            lists: before.lists,
            entry_write_requests: before.entry_write_requests,
        }
    );
    assert_eq!(harness.cache_value(&path).await, None);

    let before = harness.bucket.stats();
    assert_eq!(harness.backend.get(app_id, key).await.unwrap(), None);
    assert_eq!(harness.bucket.stats(), before);
}

#[tokio::test]
async fn s3_failed_check_does_not_mutate_state_or_persist_anything() {
    let harness = TestHarness::new(false).await;
    let app_id = 4;

    harness
        .backend
        .exec(app_id, vec![set(b"a", b"alpha")])
        .await
        .unwrap();
    let before = harness.bucket.stats();

    let error = harness
        .backend
        .exec(
            app_id,
            vec![
                KvCommand::CheckValue {
                    key: b"a".to_vec(),
                    value: b"wrong".to_vec(),
                },
                set(b"b", b"bravo"),
            ],
        )
        .await
        .unwrap_err();
    assert_eq!(
        error,
        KvError::CheckFailed(KvCheckFailedReason::ValueMismatch)
    );

    assert_eq!(harness.bucket.stats().writes, before.writes);
    assert_eq!(harness.bucket.stats().deletes, before.deletes);
    assert_eq!(
        harness.bucket.stats().entry_write_requests,
        before.entry_write_requests
    );
    assert_eq!(
        harness.backend.get(app_id, b"a".to_vec()).await.unwrap(),
        Some(b"alpha".to_vec())
    );
    assert_eq!(
        harness.backend.get(app_id, b"b".to_vec()).await.unwrap(),
        None
    );

    let namespace = harness.backend.namespace(app_id).await;
    let state = namespace.state.lock().await;
    assert!(state.wal.is_empty());
    assert!(state.overlay.is_empty());
    assert_eq!(state.total_bytes, logical_entry_size(1, 5).unwrap());
}

#[tokio::test]
async fn s3_quota_exceeded_rejects_without_persisting() {
    let harness = TestHarness::with_max_app_bytes(false, 5).await;
    let app_id = 5;

    let error = harness
        .backend
        .exec(app_id, vec![set(b"a", b"value!")])
        .await
        .unwrap_err();
    assert_eq!(
        error,
        KvError::QuotaExceeded {
            app_id,
            used_bytes: 7,
            limit_bytes: 5,
        }
    );
    assert_eq!(
        harness.bucket.stats(),
        MockBucketStats {
            reads: 1,
            writes: 0,
            deletes: 0,
            lists: 1,
            entry_write_requests: 0,
        }
    );
    assert_eq!(harness.backend.storage_used(app_id).await.unwrap(), 0);
    assert_eq!(
        harness.backend.get(app_id, b"a".to_vec()).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn s3_multi_write_batch_is_visible_immediately_and_checkpoint_persists_it() {
    let harness = TestHarness::new(false).await;
    let app_id = 6;

    harness
        .backend
        .exec(app_id, vec![set(b"a", b"alpha"), set(b"b", b"bravo")])
        .await
        .unwrap();

    assert_eq!(
        harness.bucket.stats(),
        MockBucketStats {
            reads: 3,
            writes: 1,
            deletes: 0,
            lists: 1,
            entry_write_requests: 0,
        }
    );

    let before = harness.bucket.stats();
    assert_eq!(
        harness.backend.get(app_id, b"a".to_vec()).await.unwrap(),
        Some(b"alpha".to_vec())
    );
    assert_eq!(
        harness.backend.get(app_id, b"b".to_vec()).await.unwrap(),
        Some(b"bravo".to_vec())
    );
    assert_eq!(harness.bucket.stats(), before);

    let before = harness.bucket.stats();
    harness
        .backend
        .checkpoint_all_for_tests(app_id)
        .await
        .unwrap();
    assert_eq!(harness.bucket.stats().reads, before.reads);
    assert_eq!(harness.bucket.stats().writes, before.writes + 2);
    assert_eq!(harness.bucket.stats().deletes, before.deletes + 1);
    assert_eq!(
        harness.bucket.stats().entry_write_requests,
        before.entry_write_requests + 2
    );

    let restarted = S3KvBackend::new(harness.store.clone(), DEFAULT_KV_APP_MAX_BYTES, false);
    assert_eq!(
        restarted.get(app_id, b"a".to_vec()).await.unwrap(),
        Some(b"alpha".to_vec())
    );
    assert_eq!(
        restarted.get(app_id, b"b".to_vec()).await.unwrap(),
        Some(b"bravo".to_vec())
    );
}

#[tokio::test]
async fn s3_checkpoint_failure_keeps_wal_authoritative_across_restart() {
    let harness = TestHarness::new(false).await;
    let app_id = 7;
    harness.bucket.fail_entry_write_requests([2]);

    harness
        .backend
        .exec(app_id, vec![set(b"a", b"alpha"), set(b"b", b"bravo")])
        .await
        .unwrap();

    let namespace = harness.backend.namespace(app_id).await;
    let error = harness
        .backend
        .checkpoint_one_batch(app_id, &namespace)
        .await
        .unwrap_err();
    assert!(!error.to_string().is_empty());

    assert_eq!(
        harness.backend.get(app_id, b"a".to_vec()).await.unwrap(),
        Some(b"alpha".to_vec())
    );
    assert_eq!(
        harness.backend.get(app_id, b"b".to_vec()).await.unwrap(),
        Some(b"bravo".to_vec())
    );
    assert_eq!(
        harness.backend.storage_used(app_id).await.unwrap(),
        logical_entry_size(1, 5).unwrap() + logical_entry_size(1, 5).unwrap()
    );

    let restarted = S3KvBackend::new(harness.store.clone(), DEFAULT_KV_APP_MAX_BYTES, false);
    assert_eq!(
        restarted.get(app_id, b"a".to_vec()).await.unwrap(),
        Some(b"alpha".to_vec())
    );
    assert_eq!(
        restarted.get(app_id, b"b".to_vec()).await.unwrap(),
        Some(b"bravo".to_vec())
    );
    assert_eq!(
        restarted.storage_used(app_id).await.unwrap(),
        logical_entry_size(1, 5).unwrap() + logical_entry_size(1, 5).unwrap()
    );

    harness.bucket.clear_failures();
    restarted.checkpoint_all_for_tests(app_id).await.unwrap();
    let namespace = restarted.namespace(app_id).await;
    let state = namespace.state.lock().await;
    assert!(state.wal.is_empty());
    assert!(state.overlay.is_empty());
}

#[tokio::test]
async fn s3_list_page_merges_base_entries_pending_wal_and_pagination() {
    let harness = TestHarness::new(false).await;
    let app_id = 8;

    for index in 0..KV_LIST_PAGE_SIZE {
        let key = format!("k{index:02}").into_bytes();
        let value = format!("v{index:02}").into_bytes();
        harness
            .store
            .write_entry(app_id, &key, &value)
            .await
            .unwrap();
    }

    let mut wal = VecDeque::new();
    wal.push_back(vec![
        KvWrite::Delete {
            key: b"k00".to_vec(),
        },
        KvWrite::Set {
            key: b"k64".to_vec(),
            value: b"v64".to_vec(),
        },
        KvWrite::Set {
            key: b"k65".to_vec(),
            value: b"v65".to_vec(),
        },
    ]);
    harness.store.write_wal(app_id, &wal).await.unwrap();

    let first_page = harness
        .backend
        .list_page(app_id, Vec::new(), None, None, None)
        .await
        .unwrap();
    assert_eq!(first_page.entries.len(), KV_LIST_PAGE_SIZE);
    assert_eq!(first_page.entries.first().unwrap().key, b"k01".to_vec());
    assert_eq!(first_page.entries.last().unwrap().key, b"k64".to_vec());
    assert_eq!(first_page.next_after, Some(b"k64".to_vec()));

    let second_page = harness
        .backend
        .list_page(app_id, Vec::new(), None, None, first_page.next_after)
        .await
        .unwrap();
    assert_eq!(
        second_page
            .entries
            .into_iter()
            .map(|entry| entry.key)
            .collect::<Vec<_>>(),
        vec![b"k65".to_vec()]
    );
    assert_eq!(second_page.next_after, None);
}

#[tokio::test]
async fn s3_concurrent_non_overlapping_wal_batches_keep_usage_exact() {
    let harness = TestHarness::new(false).await;
    let app_id = 9;

    let backend = harness.backend.clone();
    let tx_a = tokio::spawn(async move {
        backend
            .exec(app_id, vec![set(b"a1", b"alpha"), set(b"a2", b"able")])
            .await
    });
    let backend = harness.backend.clone();
    let tx_b = tokio::spawn(async move {
        backend
            .exec(app_id, vec![set(b"b1", b"bravo"), set(b"b2", b"baker")])
            .await
    });

    tx_a.await.unwrap().unwrap();
    tx_b.await.unwrap().unwrap();

    let expected = logical_entry_size(2, 5).unwrap()
        + logical_entry_size(2, 4).unwrap()
        + logical_entry_size(2, 5).unwrap()
        + logical_entry_size(2, 5).unwrap();
    let namespace = harness.backend.namespace(app_id).await;
    {
        let state = namespace.state.lock().await;
        assert_eq!(state.wal.len(), 2);
        assert_eq!(state.overlay.len(), 4);
        assert_eq!(state.total_bytes, expected);
    }

    assert_eq!(
        harness.backend.get(app_id, b"a1".to_vec()).await.unwrap(),
        Some(b"alpha".to_vec())
    );
    assert_eq!(
        harness.backend.get(app_id, b"a2".to_vec()).await.unwrap(),
        Some(b"able".to_vec())
    );
    assert_eq!(
        harness.backend.get(app_id, b"b1".to_vec()).await.unwrap(),
        Some(b"bravo".to_vec())
    );
    assert_eq!(
        harness.backend.get(app_id, b"b2".to_vec()).await.unwrap(),
        Some(b"baker".to_vec())
    );
    assert_eq!(
        harness.backend.storage_used(app_id).await.unwrap(),
        expected
    );
}
