//! Viability test 4: tenant deletion.
//!
//! Claim: Deleting a tenant is O(file_count) with a small constant. Object
//! storage gives us a flat keyspace under the tenant prefix; deleting a
//! tenant is "drop catalog rows + parallel DeleteObjects". On LocalFS
//! this is unlinked-inode fast; on S3 it's a single bulk DeleteObjects
//! RPC fired in parallel with a LIST mop-up (see `s3_tenant_deletion.rs`).
//!
//! Setup: write 100 small Parquet files for one tenant *and* register
//! them in an `InMemoryCatalog` — NOT timed (reported as `setup_ms`
//! only). Then **reset caches** (build a fresh `Storage` with
//! caches=None against the same backing dir) and time
//! `Storage::delete_tenant` end-to-end. The catalog-aware path fires
//! `DeleteObjects` against the catalog file set in parallel with a
//! LIST RPC, hiding one network round-trip on high-RTT object stores.
//!
//! Primary metric: `deletion_ms` < 3000 (consistent with the S3 sibling
//! so dashboards plot the same number on both backends).

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog, SnapshotId};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_storage::{Storage, StorageConfig};
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde_json::json;
use tempfile::TempDir;

const FILES: usize = 100;
const ROWS_PER_FILE: usize = 1_000;
// 3 s bar — the catalog-first path eliminates the LIST → DELETE serial
// dependency so the wall clock is dominated by a single bulk
// DeleteObjects RTT (plus a parallel LIST that finishes inside the
// same window). LocalFS is sub-second; the bar exists for the S3
// sibling (where R2 from APAC has ~300-500 ms RTT with significant
// long-tail variance) and is mirrored here so dashboards plot the
// same number on both backends.
const BAR_MS: f64 = 3_000.0;

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

    // ---- Setup phase (NOT primary) --------------------------------------
    let setup_storage = Storage::new(StorageConfig {
        object_store: fs.clone(),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });

    let tenant = TenantId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

    // Stand up an in-memory catalog so the deletion path can lift its
    // file paths from the catalog instead of paying a LIST RTT. The
    // catalog is also the surface the test asserts on at the end
    // (drop_namespace must clear every row).
    let catalog = Arc::new(InMemoryCatalog::new());
    catalog
        .create_table(&tenant, &table, schema().as_ref())
        .await
        .unwrap();

    let setup_started = Instant::now();
    let mut written: Vec<DataFileRef> = Vec::with_capacity(FILES);
    for i in 0..FILES {
        let start = (i * ROWS_PER_FILE) as i64;
        let batch = build_batch(start, ROWS_PER_FILE);
        let f = setup_storage
            .write_batch(&tenant, &table, &part, &batch)
            .await
            .unwrap();
        written.push(DataFileRef {
            path: f.path.as_ref().to_string(),
            size_bytes: f.size_bytes,
            row_count: f.row_count,
            column_stats: f.column_stats.clone(),
        });
    }
    // Register every file with the catalog in one append so the
    // deletion path's catalog query returns the full set. (Outside the
    // timed window — same scope as `setup_ms`.)
    catalog
        .append_data_files(&tenant, &table, SnapshotId::GENESIS, written)
        .await
        .unwrap();
    let setup_ms = setup_started.elapsed().as_secs_f64() * 1000.0;

    // Sanity check (outside any timed window).
    let tenant_prefix = ObjectPath::from(format!("tenants/{tenant}"));
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

    // ---- Reset caches before timing -------------------------------------
    // `Storage` doesn't expose `clear_disk_cache()` / `clear_page_cache()`
    // yet, so build a fresh `Storage` with caches=None against the same
    // backing dir. Same wire path, no warm-cache cheat.
    drop(setup_storage);
    let storage = Storage::new(StorageConfig {
        object_store: fs.clone(),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });

    // ---- Deletion phase (THE primary metric) ----------------------------
    // Catalog-first: storage fires DeleteObjects against the catalog file
    // set and a LIST mop-up in parallel, then drops every catalog row.
    let started = Instant::now();
    let deleted = storage
        .delete_tenant(catalog.as_ref(), &tenant)
        .await
        .expect("delete_tenant");
    let deletion_ms = started.elapsed().as_secs_f64() * 1000.0;

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
    assert!(
        deleted >= FILES,
        "deleted only {deleted} of >= {FILES}"
    );

    // Catalog must be empty after deletion: drop_table for every table
    // and drop_namespace for the tenant must have fired inside
    // `delete_tenant`.
    let tables_after = catalog.list_tables(&tenant).await.unwrap();
    assert!(
        tables_after.is_empty(),
        "expected zero residual catalog tables, got {tables_after:?}"
    );
    let load_err = catalog.load_table(&tenant, &table).await.unwrap_err();
    assert!(
        matches!(load_err, basin_common::BasinError::NotFound(_)),
        "expected NotFound on dropped table, got {load_err:?}"
    );

    let pass = deletion_ms < BAR_MS;
    println!(
        "[VIABILITY 4] tenant deletion: files={deleted}, setup={setup_ms:.1} ms, \
         deletion={deletion_ms:.1} ms (bar <{BAR_MS} ms) {}",
        if pass { "PASS" } else { "FAIL" }
    );

    report_viability(
        "tenant_deletion",
        "Tenant deletion latency",
        "Deleting a tenant of 100 small files via Storage::delete_tenant \
         (catalog-first; LIST mop-up in parallel) completes in under 3 \
         seconds (caches reset; cold path).",
        pass,
        PrimaryMetric {
            label: "deletion_ms".into(),
            value: deletion_ms,
            unit: "ms".into(),
            bar: BarOp::lt(BAR_MS),
        },
        json!({
            "setup_ms": setup_ms,
            "files": deleted,
        }),
    );

    assert!(
        pass,
        "deletion took {deletion_ms:.1} ms, bar <{BAR_MS} ms"
    );
}
