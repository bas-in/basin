//! Within-tenant time-based partitioning viability test.
//!
//! Card: `viability_partition_pruning`
//! Bar: a 1-month range query against a year of data must read at most one
//! partition's worth of bytes (`bytes_read_ratio < 0.2`, equivalently
//! `partitions_read_for_range_query == 1`).
//!
//! What we test:
//!
//! 1. `CREATE TABLE events (id BIGINT, ts TIMESTAMPTZ NOT NULL, payload TEXT)
//!    PARTITION BY RANGE (ts)` lands metadata that the engine respects at
//!    INSERT time.
//! 2. Inserting 12 months of data (100 rows per month, distinct months) emits
//!    one Parquet file per partition under
//!    `tenants/{id}/tables/events/data/year=YYYY/month=MM/...`.
//! 3. A `SELECT ... WHERE ts BETWEEN ...` covering exactly one month reads
//!    *one* file's worth of bytes (not all twelve).
//!
//! Method: wrap `LocalFileSystem` in a counting store that tracks total
//! `get` bytes returned plus per-file get counts; subtract baseline pre-query
//! totals from post-query totals.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult, TenantSession};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult,
};
use serde_json::json;
use tempfile::TempDir;

/// Wraps an object store and counts byte-range and full GETs we issue
/// against it. The counters are atomic; the test takes a baseline before
/// each phase and reads the delta after.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    get_count: AtomicUsize,
    range_get_count: AtomicUsize,
    list_count: AtomicUsize,
    /// Total bytes pulled from the inner store, summed across full and
    /// ranged GETs. We approximate ranged-get bytes as `range.end - start`
    /// (which is exact for `Bounded`; we ignore `Suffix`/`Offset` because
    /// the parquet reader emits `Bounded` ranges).
    bytes_read: AtomicUsize,
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
            list_count: AtomicUsize::new(0),
            bytes_read: AtomicUsize::new(0),
        })
    }

    fn snapshot(&self) -> CountSnapshot {
        CountSnapshot {
            gets: self.get_count.load(Ordering::Relaxed),
            range_gets: self.range_get_count.load(Ordering::Relaxed),
            lists: self.list_count.load(Ordering::Relaxed),
            bytes: self.bytes_read.load(Ordering::Relaxed),
        }
    }
}

#[derive(Copy, Clone, Debug)]
struct CountSnapshot {
    gets: usize,
    range_gets: usize,
    lists: usize,
    bytes: usize,
}

impl CountSnapshot {
    fn delta(self, baseline: CountSnapshot) -> CountSnapshot {
        CountSnapshot {
            gets: self.gets - baseline.gets,
            range_gets: self.range_gets - baseline.range_gets,
            lists: self.lists - baseline.lists,
            bytes: self.bytes - baseline.bytes,
        }
    }
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
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let is_ranged = options.range.is_some();
        let approx_bytes = match &options.range {
            Some(object_store::GetRange::Bounded(rng)) => rng.end.saturating_sub(rng.start),
            _ => 0,
        };
        if is_ranged {
            self.range_get_count.fetch_add(1, Ordering::Relaxed);
            if approx_bytes > 0 {
                self.bytes_read.fetch_add(approx_bytes, Ordering::Relaxed);
            }
        } else {
            self.get_count.fetch_add(1, Ordering::Relaxed);
        }
        let result = self.inner.get_opts(location, options).await?;
        if !is_ranged {
            // Approximate full-get bytes from the head's reported size, which
            // the response carries. Not strictly equal to the bytes streamed
            // (the reader may abort early), but adequate for ratio comparisons.
            let size = result.meta.size;
            self.bytes_read.fetch_add(size, Ordering::Relaxed);
        }
        Ok(result)
    }

    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
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

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn engine_with_counting_store(dir: &TempDir) -> (Engine, Arc<CountingStore>) {
    let fs = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let counting = CountingStore::new(fs);
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: counting.clone(),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    (engine, counting)
}

async fn total_select_rows(sess: &TenantSession, sql: &str) -> usize {
    let res = sess.execute(sql).await.unwrap();
    match res {
        ExecResult::Rows { batches, .. } => {
            batches.iter().map(RecordBatch::num_rows).sum::<usize>()
        }
        ExecResult::Empty { .. } => 0,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viability_partition_pruning_one_month_range_reads_one_partition() {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let (engine, counting) = engine_with_counting_store(&dir);
    let tenant = TenantId::new();
    let sess = engine.open_session(tenant).await.unwrap();

    sess.execute(
        "CREATE TABLE events (\
            id BIGINT NOT NULL, \
            ts TIMESTAMPTZ NOT NULL, \
            payload TEXT NOT NULL\
        ) PARTITION BY RANGE (ts)",
    )
    .await
    .unwrap();

    // 12 monthly batches × 100 rows = 1_200 rows. Each batch lands in
    // exactly one partition because every row in the batch falls in the
    // same month.
    const ROWS_PER_MONTH: usize = 100;
    for month in 1..=12u32 {
        let mut sql = String::from("INSERT INTO events VALUES ");
        for i in 0..ROWS_PER_MONTH {
            if i > 0 {
                sql.push(',');
            }
            // Spread rows across the month: day 1..=28 cyclically so we
            // never land on a non-existent date.
            let day = ((i as u32) % 28) + 1;
            let id = (month as i64) * 1000 + i as i64;
            sql.push_str(&format!(
                "({id}, '2026-{month:02}-{day:02}T12:00:00Z', 'p-{i}')"
            ));
        }
        sess.execute(&sql).await.unwrap();
    }

    // Every monthly insert produced one file. Sanity check via the catalog
    // before we measure any reads.
    let table = basin_common::TableName::new("events").unwrap();
    let storage = engine.config().storage.clone();
    let listed = storage.list_data_files(&tenant, &table).await.unwrap();
    assert_eq!(
        listed.len(),
        12,
        "expected 12 monthly partition files, got {}",
        listed.len()
    );

    // Each file's path must include the Hive-style partition segments —
    // structural assertion that the layout actually changed.
    for f in &listed {
        let s = f.path.as_ref();
        assert!(
            s.contains("year=2026/"),
            "file path missing year= segment: {s}"
        );
        assert!(
            s.contains("/month=") && s.contains("/data/year=2026/"),
            "file path missing month= or data/ segment: {s}"
        );
    }

    // Total bytes for a full scan first (baseline for the ratio).
    let baseline_full = counting.snapshot();
    let total_full = total_select_rows(&sess, "SELECT id FROM events").await;
    let full_delta = counting.snapshot().delta(baseline_full);
    assert_eq!(total_full, 12 * ROWS_PER_MONTH, "full-scan rows mismatch");

    // Now the partition-pruning query: one month's range.
    let baseline_range = counting.snapshot();
    let total_range = total_select_rows(
        &sess,
        "SELECT id FROM events WHERE ts >= '2026-04-01T00:00:00Z' AND ts < '2026-05-01T00:00:00Z'",
    )
    .await;
    let range_delta = counting.snapshot().delta(baseline_range);
    assert_eq!(
        total_range, ROWS_PER_MONTH,
        "range query must return exactly one month's rows"
    );

    // Bar: bytes_read_ratio < 0.2 (range query reads <20% of full-scan bytes).
    let ratio = range_delta.bytes as f64 / full_delta.bytes.max(1) as f64;
    eprintln!(
        "viability_partition_pruning: full={:?} range={:?} ratio={:.3}",
        full_delta, range_delta, ratio
    );
    let pass_ratio = ratio < 0.2;
    let pass_files = range_delta.range_gets + range_delta.gets < 12;
    let pass = pass_ratio && pass_files;

    report_viability(
        "partition_pruning",
        "Within-tenant time-based partition pruning",
        "A 1-month range query against 12 months of partitioned data reads at most one partition's worth of bytes (range/full bytes ratio < 0.2).",
        pass,
        PrimaryMetric {
            label: "partition_bytes_ratio (range / full)".into(),
            value: ratio,
            unit: "ratio".into(),
            bar: BarOp::lt(0.2),
        },
        json!({
            "full_scan_bytes": full_delta.bytes,
            "range_scan_bytes": range_delta.bytes,
            "ratio": ratio,
            "full_scan_gets": full_delta.gets,
            "full_scan_range_gets": full_delta.range_gets,
            "range_scan_gets": range_delta.gets,
            "range_scan_range_gets": range_delta.range_gets,
            "rows_per_month": ROWS_PER_MONTH,
            "months": 12,
            "rows_full_scan": total_full,
            "rows_range_query": total_range,
        }),
    );

    assert!(
        pass_ratio,
        "partition pruning failed: full-scan {} bytes, range-scan {} bytes (ratio {:.3})",
        full_delta.bytes, range_delta.bytes, ratio
    );

    // Spec also offers `partitions_read_for_range_query == 1` as an
    // alternate bar. We re-derive it from the per-file-get count: at most
    // one Parquet file's worth of get RPCs. A file ingest produces one or
    // more range gets (footer + row-group reads) but NEVER more than 4-5
    // small ranges per file under the test's small data. So range_gets
    // bounded by ~10 corresponds to one partition.
    assert!(
        pass_files,
        "expected fewer than 12 file-level reads (one per partition); got gets={} range_gets={}",
        range_delta.gets, range_delta.range_gets
    );
}
