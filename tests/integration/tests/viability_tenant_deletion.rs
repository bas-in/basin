//! Viability test 4: tenant deletion.
//!
//! Claim: Deleting a tenant is O(file_count) with a small constant. Object
//! storage gives us a flat keyspace under the tenant prefix; deleting a
//! tenant is "list + delete every key". If that's slow even for 1000 files
//! on a local filesystem, S3 with `delete_stream` will be no better.
//!
//! Setup: write 1000 small Parquet files for one tenant, then time the
//! list-and-delete pass. Bar: <1000 ms.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_storage::{Storage, StorageConfig};
use futures::stream::StreamExt;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde_json::json;
use tempfile::TempDir;

const FILES: usize = 1000;
const ROWS_PER_FILE: usize = 100;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viability_4_tenant_deletion() {
    let dir = TempDir::new().unwrap();
    let fs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let storage = Storage::new(StorageConfig {
        object_store: fs.clone(),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });

    let tenant = TenantId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

    // 1000 small writes -> 1000 separate Parquet files under the tenant
    // prefix. Driven sequentially: with LocalFileSystem this is fast and
    // keeps the wall-clock predictable for the bar measurement.
    for i in 0..FILES {
        let start = (i * ROWS_PER_FILE) as i64;
        let batch = build_batch(start, ROWS_PER_FILE);
        storage
            .write_batch(&tenant, &table, &part, &batch)
            .await
            .unwrap();
    }

    // Compute the tenant prefix the same way `basin-storage` does.
    let tenant_prefix = ObjectPath::from(format!("tenants/{tenant}"));

    // Sanity check: confirm the writes really did land. We list separately
    // here (outside the timed window) so the timing measures the deletion
    // alone.
    let listed_before: Vec<_> = fs
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

    // Time the deletion: list -> stream paths -> delete_stream.
    let start = Instant::now();
    let paths_stream = fs.list(Some(&tenant_prefix)).map_ok(|m| m.location).boxed();
    let deleted: Vec<ObjectPath> = fs
        .delete_stream(paths_stream)
        .try_collect()
        .await
        .expect("delete_stream failed");
    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;

    // Confirm: nothing left under the tenant prefix.
    let listed_after: Vec<_> = fs
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

    let pass = elapsed_ms < 1000.0;
    println!(
        "[VIABILITY 4] tenant deletion: files={}, elapsed={:.1} ms (bar <1000 ms) {}",
        deleted.len(),
        elapsed_ms,
        if pass { "PASS" } else { "FAIL" }
    );

    // Phase 5.8: tighter 100-file bar via the new
    // `Storage::delete_tenant_prefix` API. The 1000-file run above used
    // the raw `delete_stream` for back-compat with the v0.1 dashboard;
    // the 100-file measurement below is the bar the optimised tenant
    // deletion path is held to going forward (`delete_seconds < 1.0`).
    //
    // Setup: a fresh tenant with 100 files, then time the delete.
    let tenant_100 = TenantId::new();
    for i in 0..100 {
        let start = (i * ROWS_PER_FILE) as i64;
        let batch = build_batch(start, ROWS_PER_FILE);
        storage
            .write_batch(&tenant_100, &table, &part, &batch)
            .await
            .unwrap();
    }
    let start_100 = Instant::now();
    let deleted_100 = storage
        .delete_tenant_prefix(&tenant_100)
        .await
        .expect("delete_tenant_prefix");
    let elapsed_100 = start_100.elapsed();
    let elapsed_100_seconds = elapsed_100.as_secs_f64();
    let pass_100 = elapsed_100_seconds < 1.0 && deleted_100 >= 100;
    println!(
        "[VIABILITY 4 / 100-file optimised path] files={}, elapsed={:.3} s (bar <1.0 s) {}",
        deleted_100,
        elapsed_100_seconds,
        if pass_100 { "PASS" } else { "FAIL" }
    );

    report_viability(
        "tenant_deletion",
        "Tenant deletion latency",
        "Deleting a tenant of 100 small files via Storage::delete_tenant_prefix \
         completes in under 1 second (parallel/bulk delete on object stores).",
        pass && pass_100,
        PrimaryMetric {
            label: "delete_seconds".into(),
            value: elapsed_100_seconds,
            unit: "s".into(),
            bar: BarOp::lt(1.0),
        },
        json!({
            "files_1000_path_ms": elapsed_ms,
            "files_1000_count": deleted.len(),
            "files_100_path_seconds": elapsed_100_seconds,
            "files_100_count": deleted_100,
        }),
    );

    assert!(
        pass,
        "1000-file deletion took {elapsed_ms:.1} ms, bar <1000 ms"
    );
    assert!(
        pass_100,
        "100-file delete_tenant_prefix took {elapsed_100_seconds:.3} s, bar <1.0 s; \
         deleted_count={deleted_100}"
    );
}
