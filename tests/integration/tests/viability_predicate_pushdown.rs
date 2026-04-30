//! Viability test 3: predicate pushdown.
//!
//! Claim: a point query reads ≥10× less than a full scan would. Without
//! effective row-group statistics + page index pruning, Basin's substrate is
//! just an awkward file format; with them, scans are bounded by selectivity.
//!
//! We measure both directions to keep this honest under compression changes:
//! a full `SELECT *` to establish the "full scan" byte budget, and a point
//! `WHERE id = K` to establish the pushdown budget, on the *same* dataset
//! and the *same* counting store. The bar is `full / point >= 10`.
//!
//! Comparing to file-on-disk bytes (the previous metric) is misleading after
//! enabling ZSTD: the point-query budget is dominated by per-file footer
//! reads, which become a larger fraction as files compress smaller. The
//! "vs full scan" framing is the metric the wedge customer actually cares
//! about — selective queries should be cheap relative to scanning the table.

#![allow(clippy::print_stdout)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_storage::{Predicate, ReadOptions, ScalarValue, Storage, StorageConfig};
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use serde_json::json;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult,
};
use tempfile::TempDir;

const ROWS_PER_BATCH: usize = 10_000;
const BATCHES: usize = 100;
// 1_000_000 total rows. Target id chosen to land in the middle so footer +
// row-group reads are representative.
const TARGET_ID: i64 = 424_242;

/// Wraps an `ObjectStore` and counts bytes returned by ranged GETs. Reused
/// pattern from `basin-storage`'s test module, duplicated here per the task
/// brief (don't touch storage's public surface).
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
            // Metadata only; no body bytes leave the store.
        } else if let Some(n) = bounded_len {
            self.range_bytes.fetch_add(n as u64, Ordering::Relaxed);
        } else {
            // Full-object GET. Charge the full object size.
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
    // 50-byte payload per row, varying so Parquet doesn't dictionary-encode it
    // into a single value (we want a representative byte budget per row).
    let payloads: Vec<String> = (0..len)
        .map(|i| {
            let v = start + i as i64;
            format!("payload-{:040}", v)
        })
        .collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(payload_arr)]).unwrap()
}

fn total_bytes_on_disk(root: &std::path::Path) -> u64 {
    let mut total = 0u64;
    for entry in walkdir::WalkDir::new(root) {
        let entry = entry.unwrap();
        if entry.file_type().is_file()
            && entry
                .path()
                .extension()
                .and_then(|s| s.to_str())
                == Some("parquet")
        {
            total += std::fs::metadata(entry.path()).unwrap().len();
        }
    }
    total
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn viability_3_predicate_pushdown() {
    let dir = TempDir::new().unwrap();
    let inner: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let counting = Arc::new(CountingStore {
        inner,
        range_bytes: AtomicU64::new(0),
        full_bytes: AtomicU64::new(0),
    });
    let storage = Storage::new(StorageConfig {
        object_store: counting.clone(),
        root_prefix: None,
    });

    let tenant = TenantId::new();
    let table = TableName::new("rg").unwrap();
    let part = PartitionKey::default_key();

    // Strategy: 100 batches of 10k rows each.
    //
    // - Each batch becomes one Parquet file (one `write_batch` call -> one
    //   PUT). That gives the reader many files to consider, each with a
    //   non-overlapping `id` range, so file-level stats pruning can drop 99
    //   files outright.
    // - Within each file, `basin-storage` uses
    //   `WriterProperties::set_max_row_group_size(1024)`, splitting the 10k
    //   rows into ~10 row groups. Stats-based pruning then drops 9 of the 10
    //   row groups inside the matching file.
    //
    // Net: on a single-id point lookup, we expect to fetch the Parquet
    // footer for ~all files and the body of one row group in one file.
    for b in 0..BATCHES {
        let start = (b * ROWS_PER_BATCH) as i64;
        let batch = build_batch(start, ROWS_PER_BATCH);
        storage
            .write_batch(&tenant, &table, &part, &batch)
            .await
            .unwrap();
    }

    let total_file_bytes = total_bytes_on_disk(dir.path());
    assert!(total_file_bytes > 0, "expected parquet on disk");

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
        "[VIABILITY 3] predicate pushdown: file_bytes={}, full_scan_bytes={}, point_query_bytes={}, reduction={:.1}x (bar >=10x) {}",
        total_file_bytes,
        full_scan_bytes,
        point_bytes,
        reduction,
        if pass { "PASS" } else { "FAIL" }
    );

    report_viability(
        "predicate_pushdown",
        "Predicate pushdown vs full scan",
        "A point query reads at least 10x less than a full scan would.",
        pass,
        PrimaryMetric {
            label: "reduction_x (full_scan_bytes / point_query_bytes)".into(),
            value: reduction,
            unit: "x".into(),
            bar: BarOp::ge(10.0),
        },
        json!({
            "file_bytes": total_file_bytes,
            "full_scan_bytes": full_scan_bytes,
            "point_query_bytes": point_bytes,
        }),
    );

    assert!(
        reduction >= 10.0,
        "point query read {point_bytes} of {full_scan_bytes} full-scan bytes = {reduction:.1}x reduction (need >=10x)"
    );
}
