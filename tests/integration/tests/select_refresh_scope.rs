//! Integration tests for statement-scoped SELECT refresh
//! (executor::compute_select_refresh_set).
//!
//! `exec_select` no longer refreshes EVERY table in the project before
//! planning a query — it narrows the refresh set to the base tables the
//! statement can actually read, expanding views to their underlying tables and
//! falling back to refresh-all whenever the set can't be enumerated with
//! confidence. These tests pin the observable contract: any table a SELECT
//! reads — directly, via a join, or through a view — always reflects writes
//! committed earlier in the same session, and the fallbacks (CTE names,
//! information_schema, nonexistent tables) behave exactly as before.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::Int64Array;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

async fn open_engine() -> (TempDir, Engine) {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    (dir, engine)
}

async fn run(sess: &basin_engine::ProjectSession, sql: &str) {
    match sess.execute(sql).await {
        Ok(_) => {}
        Err(e) => panic!("unexpected error for [{sql}]: {e}"),
    }
}

/// Execute a single-cell COUNT/SUM-style query and return the i64 value.
async fn count(sess: &basin_engine::ProjectSession, sql: &str) -> i64 {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches
                .first()
                .unwrap_or_else(|| panic!("no batches for: {sql}"));
            assert_eq!(b.num_rows(), 1, "expected 1 row for: {sql}");
            let col = b.column(0);
            col.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| {
                    panic!("not Int64 for: {sql} (type={:?})", col.data_type())
                })
                .value(0)
        }
        Ok(other) => panic!("expected Rows for: {sql}, got {other:?}"),
        Err(e) => panic!("execute error for: {sql}: {e}"),
    }
}

/// Create a 5-table project: t1..t5, each `(id INT)`, with one seed row each
/// so the tables physically exist with a file before the test writes more.
async fn seed_five_tables(sess: &basin_engine::ProjectSession) {
    for n in 1..=5 {
        run(sess, &format!("CREATE TABLE t{n} (id INT)")).await;
        run(sess, &format!("INSERT INTO t{n} (id) VALUES ({n}0)")).await;
    }
}

// ---------------------------------------------------------------------------
// Core: a query touching one table sees its own fresh writes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn single_table_query_sees_fresh_writes_in_multi_table_project() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_five_tables(&sess).await;

    // Baseline: t3 has its seed row.
    assert_eq!(count(&sess, "SELECT count(*) FROM t3").await, 1);

    // Write more rows to t3, then read t3 again. The scoped refresh must
    // pick up t3 even though t1,t2,t4,t5 were not touched.
    run(&sess, "INSERT INTO t3 (id) VALUES (1), (2), (3)").await;
    assert_eq!(count(&sess, "SELECT count(*) FROM t3").await, 4);
}

// ---------------------------------------------------------------------------
// View over a table: fresh underlying data is visible
// ---------------------------------------------------------------------------

#[tokio::test]
async fn view_over_table_sees_fresh_underlying_data() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_five_tables(&sess).await;

    // A view whose body references t2 (a base table not named in the outer
    // SELECT). The refresh-set computation must expand the view to t2 and
    // refresh t2, or the view query would see stale data.
    run(&sess, "CREATE VIEW v_t2 AS SELECT id FROM t2").await;
    assert_eq!(count(&sess, "SELECT count(*) FROM v_t2").await, 1);

    run(&sess, "INSERT INTO t2 (id) VALUES (7), (8)").await;
    // The SELECT names only `v_t2`; t2 is reached purely via view expansion.
    assert_eq!(count(&sess, "SELECT count(*) FROM v_t2").await, 3);
}

#[tokio::test]
async fn view_over_multiple_tables_refreshes_each_underlying_table() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_five_tables(&sess).await;

    // View body joins two base tables (t4, t5). Both must be expanded out of
    // the view name and refreshed, or the cross-product would be stale.
    run(
        &sess,
        "CREATE VIEW v_join AS SELECT t4.id AS a, t5.id AS b FROM t4, t5",
    )
    .await;
    // 1 row in each seeded table -> 1 * 1 = 1.
    assert_eq!(count(&sess, "SELECT count(*) FROM v_join").await, 1);

    run(&sess, "INSERT INTO t4 (id) VALUES (100), (200)").await; // t4: 3 rows
    run(&sess, "INSERT INTO t5 (id) VALUES (300)").await; // t5: 2 rows
    // Reached via v_join -> {t4, t5}; cross-product = 3 * 2 = 6.
    assert_eq!(count(&sess, "SELECT count(*) FROM v_join").await, 6);
}

// ---------------------------------------------------------------------------
// CTE named like a table doesn't break (CTE name is not a base table)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cte_named_like_a_table_does_not_break() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_five_tables(&sess).await;

    run(&sess, "INSERT INTO t1 (id) VALUES (1), (2)").await;

    // `t5` here is a CTE that shadows the base table t5. The refresh-set
    // computation must NOT treat the CTE reference as an unknown name (which
    // would still be safe via fallback) but more importantly must refresh the
    // REAL base table t1 that the CTE body reads. Result reflects t1's writes.
    let n = count(
        &sess,
        "WITH t5 AS (SELECT id FROM t1) SELECT count(*) FROM t5",
    )
    .await;
    assert_eq!(n, 3); // t1 had 1 seed row + 2 inserted
}

// ---------------------------------------------------------------------------
// Join across two tables: both refresh, both fresh
// ---------------------------------------------------------------------------

#[tokio::test]
async fn join_across_two_tables_refreshes_both() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_five_tables(&sess).await;

    // Write fresh rows to BOTH t1 and t2, then a join that reads both. If
    // either table were skipped by the scoped refresh, the join would see
    // stale data and the cross-product count would be wrong.
    run(&sess, "INSERT INTO t1 (id) VALUES (1), (2)").await; // t1: 3 rows
    run(&sess, "INSERT INTO t2 (id) VALUES (3)").await; // t2: 2 rows

    // CROSS JOIN cardinality = |t1| * |t2| = 3 * 2 = 6.
    let n = count(&sess, "SELECT count(*) FROM t1, t2").await;
    assert_eq!(n, 6);
}

// ---------------------------------------------------------------------------
// information_schema query still works (fallback path: synthesized provider)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn information_schema_query_still_works() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_five_tables(&sess).await;

    // Schema-qualified system table — collected as zero single-ident refs, so
    // the scope computation falls back to refresh-all and the synthesized
    // provider serves the query unchanged.
    run(&sess, "SELECT * FROM information_schema.tables").await;

    // Sanity: the five base tables are visible in the catalog view.
    let n = count(
        &sess,
        "SELECT count(*) FROM information_schema.tables \
         WHERE table_name IN ('t1','t2','t3','t4','t5')",
    )
    .await;
    assert_eq!(n, 5);
}

// ---------------------------------------------------------------------------
// Nonexistent table errors identically to before (no scope-specific change)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn nonexistent_table_errors_as_before() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_five_tables(&sess).await;

    // A name that is neither a base table nor a view → scope computation
    // returns None (refresh-all), and DataFusion planning still fails on the
    // unregistered table with its usual error.
    let res = sess.execute("SELECT * FROM no_such_table").await;
    assert!(
        res.is_err(),
        "expected error for SELECT against a nonexistent table"
    );
}
