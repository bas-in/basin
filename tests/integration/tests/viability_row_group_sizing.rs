//! Viability test: per-table row-group sizing.
//!
//! Card: `viability_row_group_sizing`
//! Bar: a table with `row_group_rows = 4096` (Phase 5.7 B3) scans
//!      strictly fewer than half as many rows on a point query than the
//!      same data laid out at the default 65,536-row group size.
//!
//! Why this matters. Parquet pruning is row-group-granular: the smallest
//! unit the reader can drop is one row group. With the default 65k-row
//! group and a 16k-row table, the whole table is one group — every point
//! query reads every row. Halving the row-group size halves the worst-case
//! data read; quartering it (4k vs 16k) gives a 4× win for true point
//! queries on a 16k-row table. Pair with bloom filters (Phase 5.7 A3) and
//! a single point query lands on a single row group instead of the whole
//! file.
//!
//! Method:
//!   1. Create two tables with identical schema and data.
//!      * `events_default` — no override, gets the writer-default 65k.
//!      * `events_small`   — `ALTER TABLE … SET row_group_rows = 4096`.
//!   2. Insert 16,384 rows into each (4× the small group, ~1/4 of default).
//!   3. Run `SELECT … WHERE id = X` on each, where X exists.
//!   4. Snapshot `row_groups_scanned` from the storage counters and
//!      compute *rows actually scanned* = `row_groups_scanned × rows_per_group`.
//!      Assert `small / default < 0.5`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

/// Row count chosen so the small-row-group table gets exactly 4 groups
/// (16384 / 4096 = 4) and the default-row-group table gets exactly 1
/// group (16384 < 65536). With monotonic ids the per-row-group min/max
/// stats prune all but one group on the small layout, while the default
/// layout has nothing to prune at the row-group level.
const ROWS: i64 = 16_384;
const SMALL_RG_ROWS: usize = 4_096;
const DEFAULT_RG_ROWS: usize = 65_536;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn viability_row_group_sizing() {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let fs = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let storage = Storage::new(StorageConfig {
        object_store: fs,
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let counters = storage.read_counters().clone();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog: catalog.clone(),
        shard: None,
    });

    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // Create both tables with identical schema. `events_small` immediately
    // gets the override; `events_default` keeps the writer global default.
    sess.execute("CREATE TABLE events_default (id BIGINT NOT NULL, payload TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE events_small (id BIGINT NOT NULL, payload TEXT NOT NULL)")
        .await
        .unwrap();

    // Catalog round-trip for the override before any INSERT runs — the
    // INSERT path reads `meta.row_group_rows` and feeds it to the writer.
    sess.execute("ALTER TABLE events_small SET row_group_rows = 4096")
        .await
        .expect("SET row_group_rows");

    // Sanity: the catalog actually persisted the override and its absence on
    // the default table is recorded as None.
    let m_small = catalog
        .load_table(&project, &TableName::new("events_small").unwrap())
        .await
        .unwrap();
    assert_eq!(m_small.row_group_rows, Some(SMALL_RG_ROWS));
    let m_default = catalog
        .load_table(&project, &TableName::new("events_default").unwrap())
        .await
        .unwrap();
    assert_eq!(m_default.row_group_rows, None);

    // Insert 16,384 rows into each table in one statement. We use VALUES
    // so the engine takes the synchronous Parquet write path (no shard is
    // configured), which calls write_batch_with_options → encode_parquet
    // with our overridden max_row_group_size.
    insert_rows(&sess, "events_default", ROWS).await;
    insert_rows(&sess, "events_small", ROWS).await;

    // Pick an id known to exist; mid-range so it doesn't sit at a group
    // boundary in either layout.
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
        "[VIABILITY row_group_sizing] DEFAULT 65k: groups_considered={}, groups_scanned={}, rows_scanned≈{}",
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
        "[VIABILITY row_group_sizing] SMALL 4k: groups_considered={}, groups_scanned={}, rows_scanned≈{}",
        small_snap.row_groups_considered, small_snap.row_groups_scanned, small_scan_rows,
    );

    // Sanity: the small layout actually produced ≥4 row groups in its
    // file, so there's a real pruning surface to measure. The default
    // layout collapses ROWS rows (< 65k) into a single row group.
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

    // Headline metric.
    let ratio = small_scan_rows as f64 / default_scan_rows.max(1) as f64;
    let bar = 0.5;
    let pass = ratio < bar;
    println!(
        "[VIABILITY row_group_sizing] small/default = {:.3} (bar < {:.2}) {}",
        ratio,
        bar,
        if pass { "PASS" } else { "FAIL" },
    );

    report_viability(
        "row_group_sizing",
        "Per-table row-group sizing for point queries",
        "A table with `row_group_rows = 4096` scans strictly fewer than half \
         the rows of an identical table at the default 65,536-row group size \
         when answering a `WHERE id = X` point query.",
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
        }),
    );

    assert!(
        pass,
        "row-group sizing ratio {:.3} ≥ bar {:.2} (default rows scanned≈{}, small rows scanned≈{})",
        ratio, bar, default_scan_rows, small_scan_rows,
    );
}

/// One INSERT statement per call, batched as `VALUES (id, 'payload-id'),
/// …` so the engine writes one Parquet file per table. We chunk into
/// multiple INSERTs to keep any single `VALUES` clause small enough to
/// avoid pathological parser heap pressure on a single 16k-row literal,
/// then rely on the writer's row-group sizing to lay each multi-statement
/// landing into the configured number of groups.
///
/// Important: every chunk lands as its OWN Parquet file. To get the
/// "many row groups in one file" property the test relies on, we issue
/// ONE giant insert per table — small enough to stay sane, large enough
/// to land as a single Parquet file with N internal row groups.
async fn insert_rows(sess: &basin_engine::ProjectSession, table: &str, n: i64) {
    // Build one VALUES list with all `n` rows. At ~30 bytes/row this is
    // under 600 KB of SQL for 16k rows — well within sqlparser's comfort.
    let mut sql = String::with_capacity((n as usize) * 32 + 64);
    sql.push_str("INSERT INTO ");
    sql.push_str(table);
    sql.push_str(" VALUES ");
    for i in 0..n {
        if i > 0 {
            sql.push(',');
        }
        // payload kept short so the file's bytes stay reasonable; the
        // bar is about row-group pruning, not bytes.
        sql.push_str(&format!("({}, 'p-{}')", i, i));
    }
    let res = sess.execute(&sql).await.expect("insert succeeds");
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, format!("INSERT 0 {n}")),
        other => panic!("unexpected exec result: {other:?}"),
    }
}
