//! Viability test: copy-on-write UPDATE / DELETE on a 10K-row table.
//!
//! Claim: a tenant can issue an UPDATE and a DELETE that each touch a
//! handful of rows in a 10K-row table, and both commit through the
//! engine + catalog + storage round trip in well under 2 seconds.
//! End-to-end this exercises:
//!
//! - sqlparser → executor dispatch on `Statement::Update` / `Delete`
//! - inline predicate evaluation (no DataFusion plan round trip)
//! - replacement-file write through `basin_storage::Storage::write_batch`
//! - `Catalog::replace_data_files` commit
//! - object-store deletion of the parent's data files
//! - `ListingTable` refresh so the next SELECT sees the new state
//!
//! The 2 s budget is intentionally loose for v0.1: copy-on-write rewrites
//! the whole table per mutation, so the per-call cost grows with table
//! size. The point of the bar is to catch regressions, not to claim
//! production-grade performance.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

const ROWS: usize = 10_000;
const BUDGET_SECS: f64 = 2.0;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn viability_update_delete() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .unwrap();

    // Build one big VALUES list. Splitting into smaller batches would
    // produce multiple Parquet files and make the rewrite cost more
    // representative of the multi-file case; keeping it as one file is
    // the easy path and matches what `INSERT ... VALUES (...)` produces
    // today.
    let mut sql = String::with_capacity(ROWS * 24);
    sql.push_str("INSERT INTO t VALUES ");
    for i in 0..ROWS {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i}, 'row-{i}')"));
    }
    sess.execute(&sql).await.unwrap();

    // UPDATE one row.
    let t0 = Instant::now();
    let res = sess
        .execute("UPDATE t SET name = 'patched' WHERE id = 4242")
        .await
        .unwrap();
    let update_ms = t0.elapsed().as_secs_f64() * 1000.0;
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "UPDATE 1"),
        other => panic!("unexpected: {other:?}"),
    }

    // DELETE three rows.
    let t0 = Instant::now();
    let res = sess.execute("DELETE FROM t WHERE id < 3").await.unwrap();
    let delete_ms = t0.elapsed().as_secs_f64() * 1000.0;
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "DELETE 3"),
        other => panic!("unexpected: {other:?}"),
    }

    // Confirm the table reflects both operations.
    let after = sess
        .execute("SELECT name FROM t WHERE id = 4242")
        .await
        .unwrap();
    let name = match after {
        ExecResult::Rows { batches, .. } => {
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
            let arr = batches[0]
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            arr.value(0).to_string()
        }
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(name, "patched");

    let row_count = sess.execute("SELECT id FROM t").await.unwrap();
    let total = match row_count {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(total, ROWS - 3);

    let total_ms = update_ms + delete_ms;
    let pass = total_ms < BUDGET_SECS * 1000.0;

    println!(
        "[VIABILITY update_delete] rows={ROWS} update_ms={update_ms:.1} delete_ms={delete_ms:.1} total_ms={total_ms:.1} (bar < {budget:.0} ms) {status}",
        budget = BUDGET_SECS * 1000.0,
        status = if pass { "PASS" } else { "FAIL" },
    );

    report_viability(
        "update_delete",
        "UPDATE / DELETE via copy-on-write",
        "An UPDATE and a DELETE on a 10K-row table commit end-to-end (parser → executor → storage rewrite → catalog Replace snapshot) in under 2 seconds.",
        pass,
        PrimaryMetric {
            label: "UPDATE+DELETE elapsed (ms)".into(),
            value: total_ms,
            unit: "ms".into(),
            bar: BarOp::lt(BUDGET_SECS * 1000.0),
        },
        json!({
            "rows": ROWS,
            "update_ms": update_ms,
            "delete_ms": delete_ms,
            "total_ms": total_ms,
            "remaining_rows": total,
        }),
    );

    assert!(
        pass,
        "UPDATE+DELETE took {total_ms:.1} ms on a {ROWS}-row table; budget {} ms",
        BUDGET_SECS * 1000.0
    );
}
