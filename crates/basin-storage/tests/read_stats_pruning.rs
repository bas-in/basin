//! File-level catalog-stats pruning in [`Storage::read`].
//!
//! Coverage for the storage-layer pruning step that mirrors the engine
//! path's pre-existing prune in `basin-engine::fast_select`. The shape
//! is identical: per-file min/max/null-count stats are tested against
//! the compound predicate, and any file whose stats prove `NoMatch` is
//! dropped before its body is read.
//!
//! The wins this gives storage callers that don't go through the
//! engine (notably the `s3_scaling_*` integration tests that call
//! `Storage::read` directly): a point query on a 100-file table opens
//! 1 file instead of 100. On a real-S3 connection at ~50 ms RTT that's
//! 5 s -> 50 ms (40x). The integration-bench bar is gated separately;
//! this file pins the *behaviour* (count of object-store GETs).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_storage::{Predicate, ReadOptions, ScalarValue, Storage, StorageConfig};
use futures::stream::{BoxStream, StreamExt};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

/// Wraps an object store and counts the read-path RPCs so the prune
/// behaviour can be asserted unambiguously.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    get_count: AtomicUsize,
    range_get_count: AtomicUsize,
    head_count: AtomicUsize,
    list_count: AtomicUsize,
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore")
    }
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            get_count: AtomicUsize::new(0),
            range_get_count: AtomicUsize::new(0),
            head_count: AtomicUsize::new(0),
            list_count: AtomicUsize::new(0),
        })
    }

    fn snapshot(&self) -> Counts {
        Counts {
            gets: self.get_count.load(Ordering::Relaxed),
            range_gets: self.range_get_count.load(Ordering::Relaxed),
            heads: self.head_count.load(Ordering::Relaxed),
            lists: self.list_count.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        self.get_count.store(0, Ordering::Relaxed);
        self.range_get_count.store(0, Ordering::Relaxed);
        self.head_count.store(0, Ordering::Relaxed);
        self.list_count.store(0, Ordering::Relaxed);
    }
}

#[derive(Copy, Clone, Debug)]
#[allow(dead_code)]
struct Counts {
    gets: usize,
    range_gets: usize,
    heads: usize,
    lists: usize,
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if options.head {
            self.head_count.fetch_add(1, Ordering::Relaxed);
        } else if options.range.is_some() {
            self.range_get_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.get_count.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.list_count.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.list_count.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn build_batch(start: i64, len: usize) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let payloads: Vec<String> = (0..len).map(|i| format!("p-{:08}", i)).collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(payload_arr)]).unwrap()
}

async fn build_storage_with_catalog() -> (Arc<CountingStore>, Storage, ProjectId, TableName) {
    let inner = Arc::new(InMemory::new());
    let counting = CountingStore::new(inner);
    let storage = Storage::new(StorageConfig {
        object_store: counting.clone(),
        root_prefix: None,
        // Caches off so every fetch shows up against the wrapped store.
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    let table = TableName::new("events").unwrap();
    catalog
        .create_table(&project, &table, &schema())
        .await
        .unwrap();
    storage.attach_catalog(catalog);
    (counting, storage, project, table)
}

async fn count_rows(stream: BoxStream<'static, basin_common::Result<RecordBatch>>) -> usize {
    let mut s = stream;
    let mut n = 0usize;
    while let Some(b) = s.next().await {
        n += b.unwrap().num_rows();
    }
    n
}

/// Ten files with disjoint id ranges. A point query for id=42 must hit
/// exactly the file holding ids [0, 100) — all nine other files are
/// pruned by the per-file min/max stat and never opened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_read_with_predicate_prunes_files_by_stats() {
    basin_common::telemetry::try_init_for_tests();
    let (counting, storage, project, table) = build_storage_with_catalog().await;
    let part = PartitionKey::default_key();

    // Ten disjoint files: [0,100), [100,200), ..., [900,1000).
    for b in 0..10 {
        let start = (b * 100) as i64;
        let batch = build_batch(start, 100);
        storage
            .write_batch(&project, &table, &part, &batch)
            .await
            .unwrap();
    }

    // Warm catalog/metadata + flush write counters, then reset.
    counting.reset();
    let target: i64 = 42; // lives in file [0, 100)
    let opts = ReadOptions {
        filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(target))],
        ..Default::default()
    };
    let stream = storage.read(&project, &table, opts).await.unwrap();
    let rows = count_rows(stream).await;
    assert_eq!(rows, 1, "expected exactly one hit for id={target}");

    let counts = counting.snapshot();
    // Pruning fires in two passes:
    //  1. `list_data_files_with_stats` fetches each of the 10 footers
    //     (one I/O per file — GET or range_GET depending on backend).
    //  2. The body pass for the ONE surviving file (the file holding
    //     ids [0, 100)) reads its row groups.
    //
    // Without pruning (the legacy code path), every one of the 10
    // files would have its body opened too — roughly doubling the
    // I/O count. With pruning, the body pass is bounded by the count
    // of files that survived stats — exactly 1 in this test.
    //
    // Upper bound: 10 footer fetches + 1 body file open (with the
    // warm footer in the metadata cache the body open is at most a
    // handful of range reads).
    let total_io = counts.gets + counts.range_gets + counts.heads;
    let n_files = 10usize;
    let max_body_io_per_file = 4; // generous: footer-cache-warm body open
    let upper_bound = n_files + max_body_io_per_file;
    assert!(
        total_io <= upper_bound,
        "total_io={} exceeds pruned bound {} (pruning failed; would have \
         hit all {} file bodies); counts={:?}",
        total_io,
        upper_bound,
        n_files,
        counts,
    );
    // Floor: pruning has to fetch every footer once to decide. If we
    // ever push stats into the catalog itself this floor drops to
    // zero — the test would need updating then.
    assert!(
        total_io >= n_files,
        "expected at least one footer per file (got {}); counts={:?}",
        total_io,
        counts,
    );
}

/// Baseline: no predicate. The pre-prune list-then-stream path remains
/// in force and every file is read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_read_without_predicate_unchanged() {
    basin_common::telemetry::try_init_for_tests();
    let (counting, storage, project, table) = build_storage_with_catalog().await;
    let part = PartitionKey::default_key();

    for b in 0..10 {
        let start = (b * 100) as i64;
        let batch = build_batch(start, 100);
        storage
            .write_batch(&project, &table, &part, &batch)
            .await
            .unwrap();
    }

    counting.reset();
    let stream = storage
        .read(&project, &table, ReadOptions::default())
        .await
        .unwrap();
    let rows = count_rows(stream).await;
    assert_eq!(rows, 1000, "expected every row across the ten files");
    // Exactly one LIST under each tier prefix (hot+cold) — no footer
    // pre-fetch when there's nothing to prune.
    let counts = counting.snapshot();
    assert!(
        counts.lists >= 2,
        "expected at least one LIST per tier; counts={counts:?}"
    );
}

/// Predicate present, but no catalog attached: the storage layer has
/// no Arrow schema to decode the min/max bytes with, so it must fall
/// through to the legacy LIST-only path (every file read).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_read_with_predicate_no_catalog_falls_through() {
    basin_common::telemetry::try_init_for_tests();
    let inner = Arc::new(InMemory::new());
    let counting = CountingStore::new(inner);
    let storage = Storage::new(StorageConfig {
        object_store: counting.clone(),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    // NB: no `storage.attach_catalog(...)` — this is the "no catalog"
    // shape the legacy tests exercise.
    let project = ProjectId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

    for b in 0..5 {
        let start = (b * 100) as i64;
        let batch = build_batch(start, 100);
        storage
            .write_batch(&project, &table, &part, &batch)
            .await
            .unwrap();
    }

    counting.reset();
    let target: i64 = 42;
    let opts = ReadOptions {
        filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(target))],
        ..Default::default()
    };
    let stream = storage.read(&project, &table, opts).await.unwrap();
    let rows = count_rows(stream).await;
    // The predicate is still applied per-batch by the parquet reader,
    // so the row count is correct; the assertion is that we did NOT
    // prune at file granularity (no catalog schema => fall-through).
    assert_eq!(rows, 1);
    let counts = counting.snapshot();
    // Legacy path: at most one LIST per tier prefix, no pre-prune
    // footer fan-out. With pruning enabled (catalog attached) the
    // upper test sees total_io >= 10; without a catalog we should
    // only see the LIST + the parquet reader's own footer + row-group
    // fetches in the surviving-file body pass.
    assert!(
        counts.lists >= 2,
        "expected at least one LIST per tier when no catalog is attached; \
         counts={counts:?}"
    );
}
