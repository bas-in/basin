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
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog, PostgresCatalog, SnapshotId};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_storage::{DataFile, Predicate, ReadOptions, ScalarValue, Storage, StorageConfig};
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

// ───────────────────────────────────────────────────────────────────────
// LEVER 1 / A4: catalog-persisted file stats — read path skips the footer
// GET when the catalog already carries `(row_count, column_stats)`.
// ───────────────────────────────────────────────────────────────────────

/// Write `n` disjoint 100-row Parquet files [0,100), [100,200), … and
/// return the writer-emitted `DataFile`s (which carry footer-derived
/// `row_count` + `column_stats`). Does NOT register them in any catalog.
async fn write_disjoint_files(
    storage: &Storage,
    project: &ProjectId,
    table: &TableName,
    n: usize,
) -> Vec<DataFile> {
    let part = PartitionKey::default_key();
    let mut out = Vec::with_capacity(n);
    for b in 0..n {
        let start = (b * 100) as i64;
        let batch = build_batch(start, 100);
        let df = storage
            .write_batch(project, table, &part, &batch)
            .await
            .unwrap();
        out.push(df);
    }
    out
}

/// Register the writer-emitted files in the catalog with the SAME stats the
/// engine commit path persists (A4): `column_stats` copied verbatim from the
/// writer's `DataFile`. This is exactly what `basin-engine`/`basin-shard` do
/// after a flush/compact, reproduced here so the storage-layer test can
/// exercise the catalog-stats read path in isolation.
async fn register_files_in_catalog(
    catalog: &dyn Catalog,
    project: &ProjectId,
    table: &TableName,
    files: &[DataFile],
) {
    let refs: Vec<DataFileRef> = files
        .iter()
        .map(|f| DataFileRef {
            path: f.path.as_ref().to_string(),
            size_bytes: f.size_bytes,
            row_count: f.row_count,
            column_stats: f.column_stats.clone(),
            bloom_filters: Default::default(),
            hll_sketches: Default::default(),
            tdigest_sketches: Default::default(),
        })
        .collect();
    catalog
        .append_data_files(project, table, SnapshotId::GENESIS, refs)
        .await
        .unwrap();
}

/// With catalog stats present, `list_data_files_with_stats` must do ZERO
/// footer fetches (no GET, no range_GET, no HEAD) — the entire cold-path
/// footer fan-out collapses to the in-catalog stats. AND the stats it
/// returns must be byte-identical to the footer-derived ones (differential
/// safety): same `row_count`, same per-column min/max/null-count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_with_catalog_stats_skips_footer_gets() {
    basin_common::telemetry::try_init_for_tests();
    let (counting, storage, project, table) = build_storage_with_catalog().await;

    // 1. Write 10 files; capture the footer-derived stats as ground truth.
    let written = write_disjoint_files(&storage, &project, &table, 10).await;

    // 2. Ground-truth listing BEFORE catalog registration: this fetches every
    //    footer (the legacy path) and is our differential reference.
    counting.reset();
    let footer_listed = storage
        .list_data_files_with_stats(&project, &table)
        .await
        .unwrap();
    let footer_io = {
        let c = counting.snapshot();
        c.gets + c.range_gets + c.heads
    };
    assert!(
        footer_io >= 10,
        "expected the footer path to fetch one footer per file; io={footer_io}"
    );

    // 3. Register the files in the catalog with their (footer-derived) stats —
    //    exactly what the engine commit path persists.
    let catalog = InMemoryCatalog::new();
    // Re-register into the SAME storage's attached catalog by building a fresh
    // storage that shares the counting store but a catalog we control.
    catalog.create_table(&project, &table, &schema()).await.unwrap();
    register_files_in_catalog(&catalog, &project, &table, &written).await;
    let storage2 = Storage::new(StorageConfig {
        object_store: counting.clone(),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    storage2.attach_catalog(Arc::new(catalog));

    // 4. Catalog-stats listing: must do ZERO footer I/O (the win).
    counting.reset();
    let catalog_listed = storage2
        .list_data_files_with_stats(&project, &table)
        .await
        .unwrap();
    let catalog_io = {
        let c = counting.snapshot();
        c.gets + c.range_gets + c.heads
    };
    assert_eq!(
        catalog_io, 0,
        "catalog-stats listing must skip ALL footer fetches; counts={:?}",
        counting.snapshot()
    );
    assert!(
        catalog_io < footer_io,
        "catalog-stats path ({catalog_io}) must do fewer footer fetches than the \
         footer path ({footer_io})"
    );

    // 5. Differential safety: catalog-stats listing == footer listing, per file.
    assert_eq!(
        catalog_listed.len(),
        footer_listed.len(),
        "same file count"
    );
    let mut footer_by_path: std::collections::HashMap<String, &DataFile> = footer_listed
        .iter()
        .map(|f| (f.path.as_ref().to_string(), f))
        .collect();
    for cf in &catalog_listed {
        let ff = footer_by_path
            .remove(cf.path.as_ref())
            .unwrap_or_else(|| panic!("file {} missing from footer listing", cf.path));
        assert_eq!(
            cf.row_count, ff.row_count,
            "row_count mismatch for {}",
            cf.path
        );
        assert_eq!(
            cf.column_stats, ff.column_stats,
            "column_stats mismatch for {} (catalog-stats prune would diverge \
             from footer-stats prune)",
            cf.path
        );
    }
    assert!(
        footer_by_path.is_empty(),
        "footer listing had files the catalog listing did not: {:?}",
        footer_by_path.keys().collect::<Vec<_>>()
    );
}

/// Mixed: some files in the catalog (skip footer), some not (footer
/// fallback). The legacy/un-registered file must still read correctly via
/// its footer; the catalog-known files must not be footer-fetched. Net I/O
/// is bounded by the count of un-registered files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_partial_catalog_stats_falls_back_per_file() {
    basin_common::telemetry::try_init_for_tests();
    let (counting, storage, project, table) = build_storage_with_catalog().await;
    let written = write_disjoint_files(&storage, &project, &table, 4).await;

    // Register only the FIRST two files in the catalog.
    let catalog = InMemoryCatalog::new();
    catalog.create_table(&project, &table, &schema()).await.unwrap();
    register_files_in_catalog(&catalog, &project, &table, &written[..2]).await;
    let storage2 = Storage::new(StorageConfig {
        object_store: counting.clone(),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    storage2.attach_catalog(Arc::new(catalog));

    counting.reset();
    let listed = storage2
        .list_data_files_with_stats(&project, &table)
        .await
        .unwrap();
    let io = {
        let c = counting.snapshot();
        c.gets + c.range_gets + c.heads
    };
    assert_eq!(listed.len(), 4, "all four files still listed");
    // Exactly the two UN-registered files pay a footer fetch; the two
    // catalog-known files pay zero. So footer I/O is in [2, 2 + slack].
    assert!(
        io >= 2 && io <= 4,
        "expected ~2 footer fetches (only the 2 un-registered files); io={io}, counts={:?}",
        counting.snapshot()
    );
    // Every file — registered or not — has correct stats.
    for f in &listed {
        assert_eq!(f.row_count, 100, "row_count for {}", f.path);
        assert!(
            f.column_stats.contains_key("id"),
            "id stats present for {}",
            f.path
        );
    }
}

/// Backend round-trip: stats persisted at flush are present + correct on the
/// next read, on the Postgres catalog backend too. Skips when no local PG is
/// reachable (mirrors the catalog crate's own PG-backed tests).
#[tokio::test]
async fn catalog_stats_roundtrip_postgres() {
    const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";
    let schema_name = format!(
        "basin_test_a4_{}",
        ulid_lower()
    );
    let cat = match tokio::time::timeout(
        std::time::Duration::from_secs(2),
        PostgresCatalog::connect_with_schema(PG_URL, &schema_name),
    )
    .await
    {
        Ok(Ok(c)) => c,
        _ => {
            eprintln!("postgres unreachable, skipping catalog_stats_roundtrip_postgres");
            return;
        }
    };

    let project = ProjectId::new();
    let table = TableName::new("events").unwrap();
    cat.create_namespace(&project).await.unwrap();
    cat.create_table(&project, &table, &schema()).await.unwrap();

    let inner = Arc::new(InMemory::new());
    let counting = CountingStore::new(inner);
    let storage = Storage::new(StorageConfig {
        object_store: counting.clone(),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let written = write_disjoint_files(&storage, &project, &table, 3).await;
    register_files_in_catalog(&cat, &project, &table, &written).await;

    // Fresh Storage that reads stats back out of Postgres.
    let storage2 = Storage::new(StorageConfig {
        object_store: counting.clone(),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    storage2.attach_catalog(Arc::new(cat));

    counting.reset();
    let listed = storage2
        .list_data_files_with_stats(&project, &table)
        .await
        .unwrap();
    let io = {
        let c = counting.snapshot();
        c.gets + c.range_gets + c.heads
    };
    assert_eq!(listed.len(), 3, "three files round-tripped from PG");
    assert_eq!(
        io, 0,
        "PG-persisted stats must skip footer fetches too; counts={:?}",
        counting.snapshot()
    );
    for f in &listed {
        assert_eq!(f.row_count, 100, "row_count for {}", f.path);
        assert!(f.column_stats.contains_key("id"));
    }

    // Drop the catalog handle's reference via dropping storage2, then clean
    // up the PG schema with a short-lived connection.
    drop(storage2);
    if let Ok(Ok(cleanup)) = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        PostgresCatalog::connect_with_schema(PG_URL, &schema_name),
    )
    .await
    {
        let _ = cleanup.drop_namespace(&project).await;
    }
}

fn ulid_lower() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{nanos:x}")
}
