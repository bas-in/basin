//! S3 port of `viability_predicate_pushdown.rs`.
//!
//! Same wrapped-counting-store pattern, but the inner store is the configured
//! S3 service. The byte-counting wrapper forwards through to S3 so reads are
//! HTTP byte-range GETs.
//!
//! Bar: full / point >= 10x. Same shape as LocalFS — the predicate-pushdown
//! claim is what we're proving here, and S3 latency doesn't change the bytes.

#![allow(clippy::print_stdout)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{Predicate, ReadOptions, ScalarValue, Storage, StorageConfig};
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult,
};
use serde_json::json;

const TEST_NAME: &str = "s3_predicate_pushdown";
const ROWS_PER_BATCH: usize = 10_000;
const BATCHES: usize = 100;
const TARGET_ID: i64 = 424_242;

/// Wraps an `ObjectStore` and counts bytes returned by ranged GETs. Same
/// structure as the LocalFS test; the inner store is whatever we point it at.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    range_bytes: AtomicU64,
    full_bytes: AtomicU64,
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore")
    }
}

#[async_trait::async_trait]
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
        let is_head = options.head;
        let bounded_len = match options.range.as_ref() {
            Some(object_store::GetRange::Bounded(rng)) => Some(rng.end.saturating_sub(rng.start)),
            _ => None,
        };
        let res = self.inner.get_opts(location, options).await?;
        if is_head {
            // metadata only
        } else if let Some(n) = bounded_len {
            self.range_bytes.fetch_add(n as u64, Ordering::Relaxed);
        } else {
            self.full_bytes
                .fetch_add(res.meta.size as u64, Ordering::Relaxed);
        }
        Ok(res)
    }

    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> futures::stream::BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
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

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn build_batch(start: i64, len: usize) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let payloads: Vec<String> = (0..len)
        .map(|i| {
            let v = start + i as i64;
            format!("payload-{:040}", v)
        })
        .collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(payload_arr)]).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn s3_predicate_pushdown() {
    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };

    let inner = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
        store: inner.clone(),
        prefix: run_prefix.clone(),
    };

    let counting = Arc::new(CountingStore {
        inner,
        range_bytes: AtomicU64::new(0),
        full_bytes: AtomicU64::new(0),
    });
    let storage = Storage::new(StorageConfig {
        object_store: counting.clone(),
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
    });

    let tenant = TenantId::new();
    let table = TableName::new("rg").unwrap();
    let part = PartitionKey::default_key();

    // 100 batches of 10k rows -> 100 Parquet files. Each file's id range is
    // disjoint, so file-level stats let us prune all but one. Within the
    // matching file, row-group stats prune all but one row group.
    for b in 0..BATCHES {
        let start = (b * ROWS_PER_BATCH) as i64;
        let batch = build_batch(start, ROWS_PER_BATCH);
        storage
            .write_batch(&tenant, &table, &part, &batch)
            .await
            .unwrap();
    }

    // ---- Full scan ----------------------------------------------------------
    counting.range_bytes.store(0, Ordering::Relaxed);
    counting.full_bytes.store(0, Ordering::Relaxed);
    let mut full_stream = storage
        .read(&tenant, &table, ReadOptions::default())
        .await
        .unwrap();
    let mut full_rows = 0usize;
    while let Some(b) = full_stream.next().await {
        full_rows += b.unwrap().num_rows();
    }
    assert_eq!(full_rows, BATCHES * ROWS_PER_BATCH);
    let full_scan_bytes =
        counting.range_bytes.load(Ordering::Relaxed) + counting.full_bytes.load(Ordering::Relaxed);

    // ---- Point query --------------------------------------------------------
    counting.range_bytes.store(0, Ordering::Relaxed);
    counting.full_bytes.store(0, Ordering::Relaxed);
    let opts = ReadOptions {
        filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(TARGET_ID))],
        ..Default::default()
    };
    let mut stream = storage.read(&tenant, &table, opts).await.unwrap();
    let mut hit_rows = 0usize;
    while let Some(b) = stream.next().await {
        let b = b.unwrap();
        hit_rows += b.num_rows();
    }
    assert_eq!(hit_rows, 1, "expected exactly one row for id={TARGET_ID}, got {hit_rows}");
    let point_bytes =
        counting.range_bytes.load(Ordering::Relaxed) + counting.full_bytes.load(Ordering::Relaxed);

    let reduction = full_scan_bytes as f64 / point_bytes.max(1) as f64;
    let pass = reduction >= 10.0;

    println!(
        "[S3 predicate_pushdown] full_scan_bytes={}, point_query_bytes={}, reduction={:.1}x (bar >=10x) {}",
        full_scan_bytes,
        point_bytes,
        reduction,
        if pass { "PASS" } else { "FAIL" }
    );

    report_real_viability(
        "predicate_pushdown",
        "Predicate pushdown vs full scan (real S3)",
        "On real S3, a point query reads at least 10x less than a full scan would.",
        pass,
        PrimaryMetric {
            label: "reduction_x (full_scan_bytes / point_query_bytes)".into(),
            value: reduction,
            unit: "x".into(),
            bar: BarOp::ge(10.0),
        },
        json!({
            "full_scan_bytes": full_scan_bytes,
            "point_query_bytes": point_bytes,
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(
        reduction >= 10.0,
        "point query read {point_bytes} of {full_scan_bytes} full-scan bytes = {reduction:.1}x reduction (need >=10x)"
    );
}
