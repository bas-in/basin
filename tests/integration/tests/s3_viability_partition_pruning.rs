//! S3 port of `viability_partition_pruning.rs`.
//!
//! Same shape: 12 monthly partitions × 100 rows each, then a one-month range
//! query that should read at most one partition's worth of bytes. The only
//! difference vs LocalFS is the underlying object store — we wrap the
//! configured S3 client in a counting decorator and read the byte deltas the
//! same way.
//!
//! Card id: `partition_pruning` (mirrors LocalFS variant; the dashboard
//! filename derives from `data_real/viability_partition_pruning.json`).
//! Bar: `partition_bytes_ratio < 0.2`.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use futures::stream::BoxStream;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use serde_json::json;

const TEST_NAME: &str = "s3_viability_partition_pruning";

/// Wraps an object store and counts byte-range and full GETs we issue
/// against it. Identical to the LocalFS variant's helper, just here so the
/// S3 test doesn't reach into another integration-test module.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    get_count: AtomicUsize,
    range_get_count: AtomicUsize,
    list_count: AtomicUsize,
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
        opts: PutMultipartOptions,
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
            Some(object_store::GetRange::Bounded(rng)) => {
                rng.end.saturating_sub(rng.start) as usize
            }
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
            let size = result.meta.size as usize;
            self.bytes_read.fetch_add(size, Ordering::Relaxed);
        }
        Ok(result)
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

async fn total_select_rows(sess: &ProjectSession, sql: &str) -> usize {
    let res = sess.execute(sql).await.unwrap();
    match res {
        ExecResult::Rows { batches, .. } => {
            batches.iter().map(RecordBatch::num_rows).sum::<usize>()
        }
        ExecResult::Empty { .. } => 0,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_viability_partition_pruning() {
    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };

    let object_store = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
        store: object_store.clone(),
        prefix: run_prefix.clone(),
    };

    // Wrap the S3 store in the counting decorator so we can read byte
    // deltas across the full vs range query phases.
    let counting = CountingStore::new(object_store);

    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: counting.clone(),
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

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
            let day = ((i as u32) % 28) + 1;
            let id = (month as i64) * 1000 + i as i64;
            sql.push_str(&format!(
                "({id}, '2026-{month:02}-{day:02}T12:00:00Z', 'p-{i}')"
            ));
        }
        sess.execute(&sql).await.unwrap();
    }

    let table = basin_common::TableName::new("events").unwrap();
    let storage = engine.config().storage.clone();
    let listed = storage.list_data_files(&project, &table).await.unwrap();
    assert_eq!(
        listed.len(),
        12,
        "expected 12 monthly partition files, got {}",
        listed.len()
    );

    let baseline_full = counting.snapshot();
    let total_full = total_select_rows(&sess, "SELECT id FROM events").await;
    let full_delta = counting.snapshot().delta(baseline_full);
    assert_eq!(total_full, 12 * ROWS_PER_MONTH, "full-scan rows mismatch");

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

    let ratio = range_delta.bytes as f64 / full_delta.bytes.max(1) as f64;
    let pass_ratio = ratio < 0.2;
    let pass_files = range_delta.range_gets + range_delta.gets < 12;
    let pass = pass_ratio && pass_files;

    println!(
        "[S3 viability_partition_pruning] full={:?} range={:?} ratio={:.3} {}",
        full_delta,
        range_delta,
        ratio,
        if pass { "PASS" } else { "FAIL" }
    );

    report_real_viability(
        "partition_pruning",
        "Within-project time-based partition pruning (real S3)",
        "A 1-month range query against 12 months of partitioned data, hosted on real S3, reads at most one partition's worth of bytes (range/full bytes ratio < 0.2).",
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
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(
        pass_ratio,
        "partition pruning failed: full-scan {} bytes, range-scan {} bytes (ratio {:.3})",
        full_delta.bytes, range_delta.bytes, ratio
    );
    assert!(
        pass_files,
        "expected fewer than 12 file-level reads (one per partition); got gets={} range_gets={}",
        range_delta.gets, range_delta.range_gets
    );
}
