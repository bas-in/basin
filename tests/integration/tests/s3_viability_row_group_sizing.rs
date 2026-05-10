//! S3 port of `viability_row_group_sizing.rs`.
//!
//! Card: `viability_row_group_sizing` (real-cloud dashboard).
//! Bar: a table with `row_group_rows = 4096` scans strictly fewer than
//! half as many rows on a point query than the same data laid out at
//! the default 65,536-row group size — same ratio as the LocalFS card.
//!
//! Method:
//!   1. Two tables, identical schema.
//!      * `events_default` — no override (writer default 65k rows / group).
//!      * `events_small`   — `ALTER TABLE … SET row_group_rows = 4096`.
//!   2. Insert 16,384 rows into each.
//!   3. Run `SELECT … WHERE id = X` on each, where X exists.
//!   4. Snapshot `row_groups_scanned` and compute scan-rows estimate.
//!   5. Assert `small / default < 0.5`.
//!
//! On a real S3 backend the storage path is identical from the writer's
//! perspective — the row-group sizing decision is footer-local. The
//! point of this card on the cloud dashboard is to confirm the
//! per-table catalog override survives the `Storage::write_batch` →
//! Parquet → object-store PUT round trip identically across backends.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{TableName, TenantId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use serde_json::json;

const TEST_NAME: &str = "s3_viability_row_group_sizing";

const ROWS: i64 = 16_384;
const SMALL_RG_ROWS: usize = 4_096;
const DEFAULT_RG_ROWS: usize = 65_536;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_viability_row_group_sizing() {
    basin_common::telemetry::try_init_for_tests();

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
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let counters = storage.read_counters().clone();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog: catalog.clone(),
        shard: None,
    });

    let tenant = TenantId::new();
    let sess = engine.open_session(tenant).await.unwrap();

    sess.execute("CREATE TABLE events_default (id BIGINT NOT NULL, payload TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE events_small (id BIGINT NOT NULL, payload TEXT NOT NULL)")
        .await
        .unwrap();

    sess.execute("ALTER TABLE events_small SET row_group_rows = 4096")
        .await
        .expect("SET row_group_rows");

    let m_small = catalog
        .load_table(&tenant, &TableName::new("events_small").unwrap())
        .await
        .unwrap();
    assert_eq!(m_small.row_group_rows, Some(SMALL_RG_ROWS));
    let m_default = catalog
        .load_table(&tenant, &TableName::new("events_default").unwrap())
        .await
        .unwrap();
    assert_eq!(m_default.row_group_rows, None);

    insert_rows(&sess, "events_default", ROWS).await;
    insert_rows(&sess, "events_small", ROWS).await;

    let target: i64 = ROWS / 2 + 7;

    // ---- default layout ---------------------------------------------------
    counters.reset();
    let res = sess
        .execute(&format!(
            "SELECT id, payload FROM events_default WHERE id = {target}"
        ))
        .await
        .unwrap();
    let default_hits = match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        ExecResult::Empty { .. } => 0,
    };
    let default_snap = counters.snapshot();
    assert_eq!(
        default_hits, 1,
        "expected one row for id={target} (default)"
    );
    let default_scan_rows = default_snap.row_groups_scanned as usize * DEFAULT_RG_ROWS;
    println!(
        "[S3 row_group_sizing] DEFAULT 65k: groups_considered={}, groups_scanned={}, rows_scanned≈{}",
        default_snap.row_groups_considered, default_snap.row_groups_scanned, default_scan_rows,
    );

    // ---- small layout -----------------------------------------------------
    counters.reset();
    let res = sess
        .execute(&format!(
            "SELECT id, payload FROM events_small WHERE id = {target}"
        ))
        .await
        .unwrap();
    let small_hits = match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        ExecResult::Empty { .. } => 0,
    };
    let small_snap = counters.snapshot();
    assert_eq!(small_hits, 1, "expected one row for id={target} (small)");
    let small_scan_rows = small_snap.row_groups_scanned as usize * SMALL_RG_ROWS;
    println!(
        "[S3 row_group_sizing] SMALL 4k: groups_considered={}, groups_scanned={}, rows_scanned≈{}",
        small_snap.row_groups_considered, small_snap.row_groups_scanned, small_scan_rows,
    );

    assert!(
        small_snap.row_groups_considered >= 4,
        "expected ≥4 considered row groups in small layout, got {}",
        small_snap.row_groups_considered,
    );
    assert_eq!(
        default_snap.row_groups_considered, 1,
        "expected exactly 1 row group in the default layout (got {})",
        default_snap.row_groups_considered,
    );

    let ratio = small_scan_rows as f64 / default_scan_rows.max(1) as f64;
    let bar = 0.5;
    let pass = ratio < bar;
    println!(
        "[S3 row_group_sizing] small/default = {:.3} (bar < {:.2}) {}",
        ratio,
        bar,
        if pass { "PASS" } else { "FAIL" },
    );

    report_real_viability(
        "row_group_sizing",
        "Per-table row-group sizing for point queries (real S3)",
        "A table with `row_group_rows = 4096` scans strictly fewer than half \
         the rows of an identical table at the default 65,536-row group size \
         when answering a `WHERE id = X` point query, on a real S3-compatible \
         backend.",
        pass,
        PrimaryMetric {
            label: "small_rg_scan_rows / default_rg_scan_rows".into(),
            value: ratio,
            unit: "fraction".into(),
            bar: BarOp::lt(bar),
        },
        json!({
            "rows": ROWS,
            "small_rg_rows": SMALL_RG_ROWS,
            "default_rg_rows": DEFAULT_RG_ROWS,
            "default_phase": {
                "groups_considered": default_snap.row_groups_considered,
                "groups_scanned": default_snap.row_groups_scanned,
                "rows_scanned_estimate": default_scan_rows,
            },
            "small_phase": {
                "groups_considered": small_snap.row_groups_considered,
                "groups_scanned": small_snap.row_groups_scanned,
                "rows_scanned_estimate": small_scan_rows,
            },
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(
        pass,
        "row-group sizing ratio {:.3} ≥ bar {:.2} (default rows scanned≈{}, small rows scanned≈{})",
        ratio, bar, default_scan_rows, small_scan_rows,
    );
}

/// One INSERT statement per call, batched as `VALUES (id, 'payload-id'),
/// …` so the engine writes one Parquet file per table.
async fn insert_rows(sess: &basin_engine::TenantSession, table: &str, n: i64) {
    let mut sql = String::with_capacity((n as usize) * 32 + 64);
    sql.push_str("INSERT INTO ");
    sql.push_str(table);
    sql.push_str(" VALUES ");
    for i in 0..n {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({}, 'p-{}')", i, i));
    }
    let res = sess.execute(&sql).await.expect("insert succeeds");
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, format!("INSERT 0 {n}")),
        other => panic!("unexpected exec result: {other:?}"),
    }
}
