//! S3 port of `viability_tenant_deletion.rs`.
//!
//! Same shape: 100 small Parquet files for one tenant, then time the
//! catalog-aware deletion pass via `Storage::delete_tenant`. The new
//! shape pulls file paths from the catalog (no LIST RTT on the hot
//! path), fires the AWS-native bulk `DeleteObjects` against them, runs
//! a LIST mop-up in parallel for any orphans, and finally drops every
//! catalog row owned by the tenant.
//!
//! Important: the dashboard primary metric is `deletion_ms` only —
//! setup PUTs are reported under `setup_ms` but are NOT the primary.
//! The 100 PUTs to seed data on R2 from APAC can take ~3 s each = ~300 s
//! of setup, which is irrelevant to the deletion-latency claim.
//!
//! Cache reset: caches are default-on for the integration suite, but
//! tenant deletion should be measured cold so we don't accidentally
//! benchmark a warm-cache short-circuit. Since `Storage` doesn't (yet)
//! expose `clear_disk_cache()` / `clear_page_cache()`, we build a
//! fresh `Storage` (caches=None) against the same bucket prefix for
//! the timed pass. See TODO at the bottom of the file.
//!
//! Bar: `deletion_ms < 3000` for 100 files. The catalog-first path
//! eliminates the LIST → DELETE serial dependency that drove the prior
//! 5 s bar; the wall clock is now ~one bulk DeleteObjects RTT plus a
//! parallel LIST that finishes inside the same window. R2 from APAC
//! has high RTT variance (300-500 ms typical, ~800 ms tail), so the
//! 3 s bar leaves headroom for the worst-case warmup; SeaweedFS will
//! be sub-second.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog, SnapshotId};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{Storage, StorageConfig};
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde_json::json;

const TEST_NAME: &str = "s3_tenant_deletion";
const FILES: usize = 100;
const ROWS_PER_FILE: usize = 1_000;
// 3 s bar: catalog-first deletion eliminates one RTT vs the legacy
// LIST → bulk-DELETE serial path (5 s baseline). The wall clock now
// sits at one bulk DeleteObjects RTT (catalog-known files) overlapped
// with a LIST RTT (orphan mop-up) plus a sub-50 ms catalog
// drop_namespace. From APAC the per-call RTT to R2 is in the
// 300-500 ms range with high variance — 3 s leaves enough headroom for
// the long-tail TLS handshake / DNS warmup case while still catching
// any regression that re-introduces the LIST → DELETE serial path.
// Local benchmarks: LocalFS ~4 ms, R2 P50 ~1.5 s, R2 P95 ~2.3 s.
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

    // ---- Setup phase (NOT the primary metric) ----------------------------
    // Caches default-on here is fine: setup writes have nothing to do with
    // the deletion latency we're measuring.
    let setup_storage = Storage::new(StorageConfig {
        object_store: object_store.clone(),
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });

    let tenant = TenantId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

    // Catalog: stand it up before the first write so we can register
    // each file as we go. The deletion path then has authoritative
    // knowledge of every file path without paying a LIST RTT.
    let catalog = Arc::new(InMemoryCatalog::new());
    catalog
        .create_table(&tenant, &table, schema().as_ref())
        .await
        .unwrap();

    let setup_started = Instant::now();
    let mut writers = tokio::task::JoinSet::new();
    for i in 0..FILES {
        let storage = setup_storage.clone();
        let table = table.clone();
        let part = part.clone();
        writers.spawn(async move {
            let start = (i * ROWS_PER_FILE) as i64;
            let batch = build_batch(start, ROWS_PER_FILE);
            storage
                .write_batch(&tenant, &table, &part, &batch)
                .await
                .unwrap()
        });
    }
    let mut written: Vec<DataFileRef> = Vec::with_capacity(FILES);
    while let Some(r) = writers.join_next().await {
        let f = r.unwrap();
        written.push(DataFileRef {
            path: f.path.as_ref().to_string(),
            size_bytes: f.size_bytes,
            row_count: f.row_count,
            column_stats: f.column_stats.clone(),
        });
    }
    // Single catalog append registers every file in one snapshot —
    // outside the timed window, same scope as `setup_ms`.
    catalog
        .append_data_files(&tenant, &table, SnapshotId::GENESIS, written)
        .await
        .unwrap();
    let setup_ms = setup_started.elapsed().as_secs_f64() * 1000.0;

    // Sanity check the writes landed (outside any timed window).
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

    // ---- Reset caches before the timed deletion pass ---------------------
    // `Storage` doesn't expose `clear_disk_cache()` / `clear_page_cache()`
    // yet, so the equivalent is to construct a fresh `Storage` with
    // caches disabled against the same bucket prefix. Same wire path,
    // no warm-cache cheat.
    drop(setup_storage);
    let storage = Storage::new(StorageConfig {
        object_store: object_store.clone(),
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
        disk_cache: None,
        page_cache: None,
    });

    // ---- Deletion phase (THE primary metric) -----------------------------
    // Catalog-first: catalog-known files go to a bulk DeleteObjects in
    // parallel with a LIST RPC for orphans, then drop_namespace
    // cascades through every catalog row. One bulk RTT instead of
    // LIST → DELETE serial.
    let started = Instant::now();
    let deleted = storage
        .delete_tenant(catalog.as_ref(), &tenant)
        .await
        .expect("delete_tenant");
    let deletion_ms = started.elapsed().as_secs_f64() * 1000.0;

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
    assert!(deleted >= FILES, "deleted only {deleted} of >= {FILES}");

    // Catalog must be empty after deletion: drop_namespace is the
    // last step in `delete_tenant` and cascades through every table.
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
        "[S3 tenant_deletion] files={deleted}, setup={setup_ms:.1} ms, \
         deletion={deletion_ms:.1} ms (bar <{BAR_MS} ms) {}",
        if pass { "PASS" } else { "FAIL" }
    );

    report_real_viability(
        "tenant_deletion",
        "Tenant deletion latency (real S3)",
        "Deleting a tenant of 100 small files via Storage::delete_tenant \
         (catalog-first; LIST mop-up in parallel; drop_namespace) \
         completes in under 3 seconds on real S3 (caches reset; cold path).",
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
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(pass, "deletion took {deletion_ms:.1} ms, bar <{BAR_MS} ms");
}
