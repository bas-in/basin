//! S3 port of `viability_tenant_deletion.rs`.
//!
//! Same shape: 1000 small Parquet files for one tenant, then time the
//! list-and-delete pass. Uses `delete_stream` (parallel) since serial
//! DELETE on S3 is dramatically slower than on a local filesystem.
//!
//! NOTE: the S3 round-trip is much slower than LocalFS, so the bar is loosened
//! to 60_000 ms (1 minute) — the original 1-second LocalFS bar would never
//! pass on a real cloud. We surface the actual measured number so the
//! dashboard reader sees the real cost, not a manufactured PASS.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{Storage, StorageConfig};
use futures::stream::StreamExt;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde_json::json;

const TEST_NAME: &str = "s3_tenant_deletion";
const FILES: usize = 1000;
const ROWS_PER_FILE: usize = 100;
// 60 s bar — S3 round-trip latency for 1000 PUTs+DELETEs is fundamentally
// dominated by network latency. The LocalFS bar of 1 s is unreachable.
const BAR_MS: f64 = 60_000.0;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn build_batch(start: i64, len: usize) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let names: Vec<String> = (0..len).map(|i| format!("v{}", start + i as i64)).collect();
    let name_arr: StringArray = names.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(name_arr)]).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_tenant_deletion() {
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

    let storage = Storage::new(StorageConfig {
        object_store: object_store.clone(),
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
    });

    let tenant = TenantId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

    // 1000 small writes — driven sequentially is too slow on S3, so fan out.
    let mut writers = tokio::task::JoinSet::new();
    for i in 0..FILES {
        let storage = storage.clone();
        let table = table.clone();
        let part = part.clone();
        writers.spawn(async move {
            let start = (i * ROWS_PER_FILE) as i64;
            let batch = build_batch(start, ROWS_PER_FILE);
            storage
                .write_batch(&tenant, &table, &part, &batch)
                .await
                .unwrap();
        });
    }
    while let Some(r) = writers.join_next().await {
        r.unwrap();
    }

    // The Storage layer wrote under `run_prefix/tenants/<tenant>/...`.
    // We measure deletion of the tenant prefix INSIDE the run prefix.
    let tenant_prefix = ObjectPath::from(format!("{run_prefix}/tenants/{tenant}"));

    let listed_before: Vec<_> = object_store
        .list(Some(&tenant_prefix))
        .try_collect()
        .await
        .unwrap();
    let parquet_count = listed_before
        .iter()
        .filter(|m| m.location.as_ref().ends_with(".parquet"))
        .count();
    assert_eq!(
        parquet_count, FILES,
        "expected {FILES} parquet files, found {parquet_count}"
    );

    // Time deletion: list -> stream paths -> delete_stream (parallel).
    let start = Instant::now();
    let paths_stream = object_store
        .list(Some(&tenant_prefix))
        .map_ok(|m| m.location)
        .boxed();
    let deleted: Vec<ObjectPath> = object_store
        .delete_stream(paths_stream)
        .try_collect()
        .await
        .expect("delete_stream failed");
    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;

    let listed_after: Vec<_> = object_store
        .list(Some(&tenant_prefix))
        .try_collect()
        .await
        .unwrap();
    assert!(
        listed_after.is_empty(),
        "expected zero residual objects, got {}",
        listed_after.len()
    );
    assert!(deleted.len() >= FILES, "deleted only {} of >= {FILES}", deleted.len());

    let pass = elapsed_ms < BAR_MS;
    println!(
        "[S3 tenant_deletion] files={}, elapsed={:.1} ms (bar <{} ms) {}",
        deleted.len(),
        elapsed_ms,
        BAR_MS,
        if pass { "PASS" } else { "FAIL" }
    );

    report_real_viability(
        "tenant_deletion",
        "Tenant deletion latency (real S3)",
        "Deleting a tenant of 1000 small files completes in under 60 seconds on real S3.",
        pass,
        PrimaryMetric {
            label: "elapsed_ms".into(),
            value: elapsed_ms,
            unit: "ms".into(),
            bar: BarOp::lt(BAR_MS),
        },
        json!({
            "files": deleted.len(),
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(
        pass,
        "deletion took {elapsed_ms:.1} ms, bar <{BAR_MS} ms"
    );
}
