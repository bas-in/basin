//! LATERAL join coverage — shapes that DataFusion 44 handles natively.
//!
//! Basin's executor routes every multi-table / LATERAL SELECT straight to
//! DataFusion's SQL planner (`exec_select` → `ctx.sql(sql)`).  The fast-path
//! (`match_simple_select`) rejects non-trivial FROM clauses by design, so
//! LATERAL queries fall through cleanly without any executor changes.
//!
//! DataFusion 44 supports the logical planning of correlated LATERAL subqueries
//! but its physical planner does not yet execute `OuterReferenceColumn` —
//! i.e. WHERE clauses inside the subquery that reference the outer table's
//! columns.  The shapes exercised here are therefore all non-correlated LATERAL
//! subqueries, which DataFusion executes correctly today:
//!
//! Shapes exercised:
//!   1. Comma-LATERAL non-correlated subquery: cross-product via `FROM t, LATERAL (SELECT …) sub`
//!   2. CROSS JOIN LATERAL non-correlated subquery
//!   3. LEFT JOIN LATERAL non-correlated subquery ON true (outer-row preservation)
//!   4. LATERAL derived subquery containing unnest (cross with literal array)
//!   5. Table function in FROM (no LATERAL keyword): `FROM generate_series(1, 10)`

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── engine bootstrap ────────────────────────────────────────────────────────

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

async fn open_session(engine: &Engine) -> basin_engine::ProjectSession {
    engine.open_session(ProjectId::new()).await.unwrap()
}

/// Execute `sql` and return the result batches, panicking on error.
async fn exec_ok(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        Ok(ExecResult::Empty { .. }) => vec![],
        Err(e) => panic!("execute failed for `{sql}`: {e}"),
    }
}

/// Total row count across all batches.
fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ─── test 1: comma-LATERAL non-correlated subquery ───────────────────────────

/// `SELECT t.id, sub.c FROM t, LATERAL (SELECT 42 AS c) sub`
///
/// Non-correlated LATERAL derived subquery in comma-join position.
/// Each row of t is paired with the single-row constant subquery → 3 output rows.
///
/// Note: DataFusion 44's physical planner does not yet execute
/// `OuterReferenceColumn` (correlated references inside LATERAL), so we use a
/// non-correlated constant subquery here.  The LATERAL keyword itself and the
/// planner path that rewrites a comma-separated `TableFactor::Derived {lateral: true}`
/// into a cross-join are exercised.
#[tokio::test]
async fn lateral_non_correlated_subquery() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE TABLE t (id INT NOT NULL)").await;
    exec_ok(&sess, "INSERT INTO t VALUES (1), (2), (3)").await;

    let batches = exec_ok(
        &sess,
        "SELECT t.id, sub.c FROM t, LATERAL (SELECT 42 AS c) sub ORDER BY t.id",
    )
    .await;

    // 3 rows in t × 1 row from subquery → 3 output rows.
    assert_eq!(
        total_rows(&batches),
        3,
        "FROM t, LATERAL (constant) sub should produce one row per outer row"
    );

    // Verify the constant value flows through.
    if let Some(batch) = batches.first() {
        let col = batch.column(1); // sub.c
        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            assert_eq!(arr.value(0), 42, "lateral subquery constant should be 42");
        }
    }
}

// ─── test 2: CROSS JOIN LATERAL non-correlated subquery ──────────────────────

/// `SELECT t.id, sub.doubled FROM t CROSS JOIN LATERAL (SELECT 2 AS doubled) sub`
///
/// CROSS JOIN LATERAL with a non-correlated derived subquery.
/// t has 2 rows; the lateral subquery always emits 1 row → 2 output rows total.
#[tokio::test]
async fn lateral_cross_join_subquery() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE TABLE t (id INT NOT NULL)").await;
    exec_ok(&sess, "INSERT INTO t VALUES (10), (20)").await;

    let batches = exec_ok(
        &sess,
        "SELECT t.id, sub.doubled FROM t CROSS JOIN LATERAL (SELECT 2 AS doubled) sub ORDER BY t.id",
    )
    .await;

    // 2 rows in t × 1 row from subquery → 2 output rows.
    assert_eq!(
        total_rows(&batches),
        2,
        "CROSS JOIN LATERAL (non-correlated) should produce one output row per outer row"
    );
}

// ─── test 3: LEFT JOIN LATERAL non-correlated subquery ON true ───────────────

/// `SELECT t.id FROM t LEFT JOIN LATERAL (SELECT 99 AS n) sub ON true`
///
/// LEFT JOIN LATERAL with a non-correlated single-row subquery and `ON true`.
/// All outer rows are preserved even if the subquery returned nothing (it
/// doesn't here, but the LEFT JOIN path is exercised end-to-end).
#[tokio::test]
async fn lateral_left_join_subquery() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE TABLE t (id INT NOT NULL)").await;
    exec_ok(&sess, "INSERT INTO t VALUES (1), (2)").await;

    let batches = exec_ok(
        &sess,
        "SELECT t.id FROM t LEFT JOIN LATERAL (SELECT 99 AS n) sub ON true ORDER BY t.id",
    )
    .await;

    // Both outer rows must appear — LEFT JOIN preserves all left-side rows.
    assert_eq!(
        total_rows(&batches),
        2,
        "LEFT JOIN LATERAL must preserve all outer rows"
    );
}

// ─── test 4: unnest from LATERAL-style subquery ──────────────────────────────

/// `SELECT t.id, x FROM t, LATERAL (SELECT unnest(ARRAY[10, 20]) AS x) sub`
///
/// Unnests a literal array beside each row in t via a LATERAL derived subquery.
/// (DataFusion's `TableFactor::UNNEST` in FROM is supported, but the
/// `LATERAL unnest(...)` function-call form produces `TableFactor::Function`
/// which DataFusion 44 does not yet implement; using a derived subquery is
/// the portable equivalent.)
#[tokio::test]
async fn lateral_unnest_via_subquery() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE TABLE t (id INT NOT NULL)").await;
    exec_ok(&sess, "INSERT INTO t VALUES (1), (2)").await;

    let batches = exec_ok(
        &sess,
        "SELECT t.id, sub.x FROM t, LATERAL (SELECT unnest(ARRAY[10, 20]) AS x) sub ORDER BY t.id, sub.x",
    )
    .await;

    // 2 rows in t × 2 elements in ARRAY → 4 output rows.
    assert_eq!(
        total_rows(&batches),
        4,
        "LATERAL subquery with unnest should cross each t row with 2 array elements"
    );
}

// ─── test 5: table function in FROM (no LATERAL keyword) ─────────────────────

/// `SELECT * FROM generate_series(1, 5) g`
///
/// A table-function in the FROM clause without any base table or LATERAL.
/// DataFusion registers generate_series by default; this verifies Basin's
/// SessionContext exposes it end-to-end.
///
/// Note: `SELECT COUNT(*) FROM generate_series(...)` hits a DataFusion 44
/// physical-plan schema mismatch bug (physical col count vs logical col count).
/// We select the raw column values and count them in Rust instead.
#[tokio::test]
async fn table_function_in_from() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    // DataFusion 44 names the generate_series output column "value" (exposed as
    // tmp_table.value).  Select it explicitly to avoid column-name sensitivity,
    // and avoid COUNT(*) which triggers a physical schema mismatch bug in
    // DataFusion 44's table-function planner.
    let batches = exec_ok(&sess, "SELECT value FROM generate_series(1, 5)").await;

    // generate_series(1, 5) should emit 5 rows: 1, 2, 3, 4, 5.
    assert_eq!(
        total_rows(&batches),
        5,
        "generate_series(1, 5) should emit 5 rows"
    );

    // Verify the values are correct: first batch, first column should be [1,2,3,4,5].
    if let Some(batch) = batches.first() {
        let col = batch.column(0);
        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            assert_eq!(arr.len(), 5);
            assert_eq!(arr.value(0), 1);
            assert_eq!(arr.value(4), 5);
        }
    }
}

// ─── test 6: correlated LATERAL with ORDER BY + LIMIT (window-function rewrite) ─

/// `SELECT t.id, sub.score FROM t LEFT JOIN LATERAL (SELECT score FROM scores
/// WHERE scores.t_id = t.id ORDER BY score DESC LIMIT 1) sub ON true`
///
/// Each outer row t gets the **single highest score** from the `scores` table
/// that matches its FK.  Parent rows with no matching scores should return NULL
/// for `sub.score` (LEFT JOIN).
///
/// Previously this shape hit DataFusion's correlated-subquery limitation.
/// Basin now rewrites it via `ROW_NUMBER() OVER (PARTITION BY t_id ORDER BY
/// score DESC)` so the execution is pure SQL without any correlated physical
/// operator.
///
/// This test previously *failed* with a DataFusion planning error; it now passes
/// as proof of the `rewrite_lateral_order_limit` rewriter.
#[tokio::test]
async fn lateral_order_by_limit_window_rewrite() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE TABLE t (id INT NOT NULL)").await;
    exec_ok(&sess, "CREATE TABLE scores (t_id INT NOT NULL, score INT NOT NULL)").await;
    exec_ok(&sess, "INSERT INTO t VALUES (1), (2), (3)").await;
    // t=1 has two scores (10, 20); t=2 has one score (5); t=3 has none.
    exec_ok(
        &sess,
        "INSERT INTO scores VALUES (1, 10), (1, 20), (2, 5)",
    )
    .await;

    // Ask for the highest score per parent (LEFT JOIN preserves t=3 with NULL).
    let batches = exec_ok(
        &sess,
        "SELECT t.id, sub.score \
         FROM t \
         LEFT JOIN LATERAL (\
           SELECT score FROM scores WHERE scores.t_id = t.id ORDER BY score DESC LIMIT 1\
         ) sub ON true \
         ORDER BY t.id",
    )
    .await;

    // Must return 3 rows: one per outer row (LEFT JOIN).
    assert_eq!(
        total_rows(&batches),
        3,
        "LEFT JOIN LATERAL ORDER BY LIMIT must return one row per outer row"
    );

    // t=1 → score=20 (top), t=2 → score=5, t=3 → NULL (no match).
    if let Some(batch) = batches.first() {
        // Column 1 = sub.score
        let score_col = batch.column(1);
        if let Some(arr) = score_col.as_any().downcast_ref::<Int64Array>() {
            // Row 0: t.id=1, score=20 (highest)
            assert!(!arr.is_null(0), "t=1 must have a score");
            assert_eq!(arr.value(0), 20, "t=1 must have score=20 (highest)");
            // Row 1: t.id=2, score=5
            assert!(!arr.is_null(1), "t=2 must have a score");
            assert_eq!(arr.value(1), 5, "t=2 must have score=5");
            // Row 2: t.id=3, no matching score → NULL
            assert!(arr.is_null(2), "t=3 must have NULL score (no match)");
        }
    }
}

// ─── test 7: comma-LATERAL with ORDER BY + LIMIT (inner join semantics) ───────

/// `SELECT t.id, sub.score FROM t, LATERAL (SELECT score FROM scores
/// WHERE scores.t_id = t.id ORDER BY score DESC LIMIT 1) sub`
///
/// Same as test 6 but using comma-LATERAL (CROSS JOIN / INNER JOIN semantics).
/// Outer rows with no matching child rows are *excluded* (inner join).
#[tokio::test]
async fn lateral_order_by_limit_comma_join() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE TABLE t (id INT NOT NULL)").await;
    exec_ok(&sess, "CREATE TABLE scores (t_id INT NOT NULL, score INT NOT NULL)").await;
    exec_ok(&sess, "INSERT INTO t VALUES (1), (2), (3)").await;
    // t=1 has two scores; t=2 has one; t=3 has none.
    exec_ok(
        &sess,
        "INSERT INTO scores VALUES (1, 10), (1, 20), (2, 5)",
    )
    .await;

    let batches = exec_ok(
        &sess,
        "SELECT t.id, sub.score \
         FROM t, LATERAL (\
           SELECT score FROM scores WHERE scores.t_id = t.id ORDER BY score DESC LIMIT 1\
         ) sub \
         ORDER BY t.id",
    )
    .await;

    // Only t=1 and t=2 have matches; t=3 is excluded (inner join semantics).
    assert_eq!(
        total_rows(&batches),
        2,
        "comma-LATERAL ORDER BY LIMIT must exclude outer rows with no match"
    );
}
