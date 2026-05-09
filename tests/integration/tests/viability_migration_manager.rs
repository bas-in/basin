//! Viability test: Migration Manager v0.2 — project-wide snapshot
//! operations through the engine's catalog handle.
//!
//! Card: `viability_migration_manager`
//! Bar: every table's project-wide snapshot is listed *and* a project-wide
//! rollback to a captured wall time leaves each table at its pre-second-batch
//! state.
//!
//! The new catalog surface (`list_snapshots_project_wide`,
//! `rollback_to_snapshot_project_wide`) is the load-bearing path for the
//! migration UI / `basinctl migrations` family. This test boots an in-process
//! engine over an InMemoryCatalog, creates 5 tables via SQL, INSERTs a first
//! batch, captures wall time, INSERTs a second batch, then:
//!
//! 1. Calls `list_snapshots_project_wide` directly on the catalog handle and
//!    asserts every table's snapshots appear.
//! 2. Calls `rollback_to_snapshot_project_wide(tenant, cutoff)` and asserts
//!    every table's `SELECT count(*)` matches the first-batch row count
//!    (i.e. the second batch is gone).

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{TableName, TenantId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

/// Sum the first column of the first batch as i64. Used to read back
/// `SELECT count(*) FROM <table>` results.
fn count_from_result(result: ExecResult) -> i64 {
    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty(tag={tag:?})"),
    };
    let batch = batches.first().expect("no batches");
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column not Int64");
    assert_eq!(arr.len(), 1, "expected a single row");
    arr.value(0)
}

#[tokio::test]
async fn viability_migration_manager() {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let fs = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let storage = Storage::new(StorageConfig {
        object_store: fs,
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog: catalog.clone(),
        shard: None,
    });

    let tenant = TenantId::new();
    let session = engine.open_session(tenant).await.unwrap();

    // 1. Create 5 tables.
    let table_names: Vec<&str> = vec!["t_a", "t_b", "t_c", "t_d", "t_e"];
    for name in &table_names {
        session
            .execute(&format!("CREATE TABLE {name} (id BIGINT, v BIGINT)"))
            .await
            .unwrap_or_else(|e| panic!("CREATE TABLE {name}: {e:?}"));
    }

    // 2. First batch: 3 rows per table.
    for name in &table_names {
        for i in 0..3_i64 {
            session
                .execute(&format!(
                    "INSERT INTO {name} (id, v) VALUES ({i}, {})",
                    i * 10
                ))
                .await
                .unwrap_or_else(|e| panic!("INSERT {name}/{i}: {e:?}"));
        }
    }

    // Bias the captured cutoff so the second-batch commits are strictly
    // *after* it on platforms with coarse system clocks.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cutoff = chrono::Utc::now();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // 3. Second batch: 2 more rows per table.
    for name in &table_names {
        for i in 3..5_i64 {
            session
                .execute(&format!(
                    "INSERT INTO {name} (id, v) VALUES ({i}, {})",
                    i * 10
                ))
                .await
                .unwrap_or_else(|e| panic!("INSERT-second {name}/{i}: {e:?}"));
        }
    }

    // Sanity: each table now holds 5 rows.
    for name in &table_names {
        let n = count_from_result(
            session
                .execute(&format!("SELECT count(*) FROM {name}"))
                .await
                .unwrap(),
        );
        assert_eq!(n, 5, "post-second-batch count for {name} = {n}");
    }

    // 4. Project-wide list: every table's snapshots should appear in one
    // sorted timeline.
    let entries = catalog
        .list_snapshots_project_wide(&tenant)
        .await
        .expect("list_snapshots_project_wide");
    let observed_tables: std::collections::BTreeSet<String> = entries
        .iter()
        .map(|e| e.table.to_string())
        .collect();
    for name in &table_names {
        assert!(
            observed_tables.contains(*name),
            "table {name} missing from project-wide list: {observed_tables:?}"
        );
    }
    // Strictly non-decreasing by committed_at — the contract.
    for w in entries.windows(2) {
        assert!(
            w[0].committed_at <= w[1].committed_at,
            "out-of-order: {:?} > {:?}",
            w[0],
            w[1]
        );
    }
    let project_wide_total = entries.len();

    // 5. Project-wide rollback to the captured wall time.
    let pairs = catalog
        .rollback_to_snapshot_project_wide(&tenant, cutoff)
        .await
        .expect("rollback_to_snapshot_project_wide");
    assert_eq!(
        pairs.len(),
        table_names.len(),
        "expected one rollback pair per table, got {pairs:?}"
    );

    // 6. Each table now reads back to its pre-second-batch state.
    let mut rows_after: Vec<(String, i64)> = Vec::new();
    for name in &table_names {
        let n = count_from_result(
            session
                .execute(&format!("SELECT count(*) FROM {name}"))
                .await
                .unwrap(),
        );
        rows_after.push(((*name).into(), n));
        assert_eq!(
            n, 3,
            "post-rollback count for {name} = {n} (expected 3, the first-batch size)"
        );
    }

    let pass = rows_after.iter().all(|(_, n)| *n == 3);
    println!(
        "[VIABILITY migration_manager] tables={} project_wide_entries={project_wide_total} \
         post_rollback={rows_after:?}",
        table_names.len(),
    );

    report_viability(
        "migration_manager",
        "Migration Manager v0.2 catalog ops end-to-end",
        "Boot engine over InMemoryCatalog, CREATE 5 tables, INSERT first batch, \
         capture wall time, INSERT second batch; list_snapshots_project_wide \
         returns every table's snapshots in commit order; \
         rollback_to_snapshot_project_wide(captured_time) rewinds every table \
         to its first-batch state (count(*) = 3).",
        pass,
        PrimaryMetric {
            label: "tables_rolled_back".into(),
            value: pairs.len() as f64,
            unit: "tables".into(),
            bar: BarOp::eq(table_names.len() as f64),
        },
        json!({
            "tables": table_names.len(),
            "project_wide_total_entries": project_wide_total,
            "post_rollback_counts": rows_after,
        }),
    );

    assert!(pass, "migration_manager viability bar not met");
}
