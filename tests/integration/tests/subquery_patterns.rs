//! Exhaustive subquery pattern integration tests.
//!
//! Verifies foundational subquery shapes against basin's DataFusion-backed
//! engine. Each test uses a minimal fixture (two tables, ~5 rows each) and
//! is self-contained.
//!
//! Shapes covered
//! ──────────────
//! Existence:
//!   ✅/🛠 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.fk = t1.id)
//!   ✅/🛠 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.fk = t1.id)
//!
//! IN / NOT IN:
//!   ✅/🛠 WHERE col IN (SELECT col FROM t2)
//!   ✅/🛠 WHERE col NOT IN (SELECT col FROM t2)
//!   ✅/🛠 WHERE (col1, col2) IN (SELECT a, b FROM t2)  — row constructor
//!
//! ANY / ALL / SOME:
//!   ✅/🛠 WHERE col > ANY (SELECT col FROM t2)
//!   ✅/🛠 WHERE col >= ALL (SELECT col FROM t2)
//!   ✅/🛠 WHERE col = ANY (SELECT col FROM t2)
//!
//! Scalar subqueries:
//!   ✅/🛠 SELECT (SELECT max(col) FROM t2) AS m FROM t1
//!   ✅/🛠 SELECT a, (SELECT count(*) FROM t2 WHERE t2.fk = t1.id) AS n FROM t1
//!   ✅/🛠 WHERE col = (SELECT min(col) FROM t2)
//!
//! Derived tables (subquery in FROM):
//!   ✅/🛠 SELECT * FROM (SELECT a, b FROM t1 WHERE c > 0) sub
//!   ✅/🛠 SELECT * FROM (SELECT a, b FROM t1) sub JOIN t2 ON sub.a = t2.a
//!
//! Correlated:
//!   — correlated EXISTS already in Existence tests above
//!   ✅/🛠 Correlated WHERE in scalar subquery (t2.fk = t1.id)

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::Int64Array;
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

fn make_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

fn i64_col(res: ExecResult, col: &str) -> Vec<i64> {
    match res {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let arr = b
                    .column_by_name(col)
                    .unwrap_or_else(|| panic!("column {col:?} not found"))
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap_or_else(|| panic!("column {col:?} is not Int64"));
                for i in 0..b.num_rows() {
                    out.push(arr.value(i));
                }
            }
            out
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// Seed a standard two-table fixture:
///   orders(id BIGINT, amount BIGINT, name TEXT)  — 5 rows, ids 1-5
///   items(id BIGINT, order_id BIGINT, price BIGINT)  — 5 rows
///
/// items.order_id links back to orders.id (ids 1, 2, 3 have items; 4, 5 do not).
async fn seed(sess: &basin_engine::ProjectSession) {
    sess.execute(
        "CREATE TABLE orders (id BIGINT NOT NULL, amount BIGINT NOT NULL, name TEXT NOT NULL)",
    )
    .await
    .unwrap();
    sess.execute(
        "INSERT INTO orders VALUES (1, 100, 'alice'), (2, 200, 'bob'), \
         (3, 300, 'carol'), (4, 400, 'dave'), (5, 500, 'eve')",
    )
    .await
    .unwrap();

    sess.execute(
        "CREATE TABLE items (id BIGINT NOT NULL, order_id BIGINT NOT NULL, price BIGINT NOT NULL)",
    )
    .await
    .unwrap();
    sess.execute(
        "INSERT INTO items VALUES (10, 1, 50), (20, 1, 60), (30, 2, 70), (40, 3, 80), (50, 3, 90)",
    )
    .await
    .unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Correlated EXISTS
// ─────────────────────────────────────────────────────────────────────────────

/// WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)
/// Expected: orders 1, 2, 3 (they have items)
#[tokio::test]
async fn subq_exists_correlated() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = sess
        .execute(
            "SELECT id FROM orders \
             WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id) \
             ORDER BY id",
        )
        .await
        .unwrap();
    let ids = i64_col(res, "id");
    assert_eq!(ids, vec![1, 2, 3]);
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Correlated NOT EXISTS
// ─────────────────────────────────────────────────────────────────────────────

/// WHERE NOT EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)
/// Expected: orders 4, 5 (no items)
#[tokio::test]
async fn subq_not_exists_correlated() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = sess
        .execute(
            "SELECT id FROM orders \
             WHERE NOT EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id) \
             ORDER BY id",
        )
        .await
        .unwrap();
    let ids = i64_col(res, "id");
    assert_eq!(ids, vec![4, 5]);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. IN with subquery
// ─────────────────────────────────────────────────────────────────────────────

/// WHERE id IN (SELECT order_id FROM items)
/// Expected: orders 1, 2, 3
#[tokio::test]
async fn subq_in_subquery() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = sess
        .execute(
            "SELECT id FROM orders \
             WHERE id IN (SELECT order_id FROM items) \
             ORDER BY id",
        )
        .await
        .unwrap();
    let ids = i64_col(res, "id");
    assert_eq!(ids, vec![1, 2, 3]);
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. NOT IN with subquery
// ─────────────────────────────────────────────────────────────────────────────

/// WHERE id NOT IN (SELECT order_id FROM items)
/// Expected: orders 4, 5
#[tokio::test]
async fn subq_not_in_subquery() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = sess
        .execute(
            "SELECT id FROM orders \
             WHERE id NOT IN (SELECT order_id FROM items) \
             ORDER BY id",
        )
        .await
        .unwrap();
    let ids = i64_col(res, "id");
    assert_eq!(ids, vec![4, 5]);
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. Row constructor IN — (col1, col2) IN (SELECT a, b FROM t2)
// ─────────────────────────────────────────────────────────────────────────────

/// WHERE (id, amount) IN (SELECT order_id, price FROM items)
///
/// Row-constructor IN is standard SQL but support varies by engine.
/// DataFusion 44 may or may not support multi-column IN subquery.
/// Test documents the actual behavior.
#[tokio::test]
async fn subq_row_constructor_in() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let result = sess
        .execute(
            "SELECT id FROM orders \
             WHERE (id, amount) IN (SELECT order_id, price FROM items) \
             ORDER BY id",
        )
        .await;

    match result {
        Ok(res) => {
            // ✅ Row-constructor IN subquery works.
            // No row in orders has (id, amount) == any (order_id, price) in items
            // since amounts are 100-500 and prices are 50-90.
            let ids = i64_col(res, "id");
            assert_eq!(ids, Vec::<i64>::new(), "expected no matching rows for row-constructor IN");
            println!("✅ subq_row_constructor_in: works, 0 rows matched");
        }
        Err(e) => {
            // 🛠 Row-constructor IN not supported in this DataFusion version.
            println!("🛠 subq_row_constructor_in: {}", e);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. ANY — col > ANY (SELECT col FROM t2)
// ─────────────────────────────────────────────────────────────────────────────

/// WHERE amount > ANY (SELECT price FROM items)
///
/// DataFusion 44: `AnyOp` with a non-`=` operator against a subquery is not
/// supported — `datafusion-sql` translates the right-hand subquery to a
/// ScalarSubquery first, then passes both sides to the registered
/// `ExprPlanner::plan_any` chain. The only registered implementation
/// (NestedFunctionPlanner) handles `= ANY` (for arrays) and rejects everything
/// else with `plan_err!("Unsupported AnyOp: '>', only '=' is supported")`.
/// Expected: plan error.
#[tokio::test]
async fn subq_any_greater_than() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let result = sess
        .execute(
            "SELECT id FROM orders \
             WHERE amount > ANY (SELECT price FROM items) \
             ORDER BY id",
        )
        .await;
    // 🛠 DataFusion 44 does not support `> ANY (subquery)`: the AnyOp path
    // in datafusion-sql only delegates to ExprPlanners, and none handles
    // a non-eq operator against a subquery operand.
    match result {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("AnyOp") || msg.contains("not supported") || msg.contains("plan:"),
                "unexpected error message: {msg}"
            );
            println!("🛠 subq_any_greater_than: {msg}");
        }
        Ok(rows) => {
            // If DataFusion added support in a newer patch, accept it.
            let ids = i64_col(rows, "id");
            assert_eq!(ids, vec![1, 2, 3, 4, 5], "> ANY result unexpected");
            println!("✅ subq_any_greater_than: now works");
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. ALL — col >= ALL (SELECT col FROM t2)
// ─────────────────────────────────────────────────────────────────────────────

/// WHERE amount >= ALL (SELECT price FROM items)
///
/// DataFusion 44: `AllOp` is parsed by sqlparser but has no match arm in
/// datafusion-sql's `sql_expr_to_logical_expr` — it falls through to the
/// catch-all `not_impl_err!("Unsupported ast node in sqltorel")`.
/// Expected: plan error.
#[tokio::test]
async fn subq_all_greater_eq() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let result = sess
        .execute(
            "SELECT id FROM orders \
             WHERE amount >= ALL (SELECT price FROM items) \
             ORDER BY id",
        )
        .await;
    // 🛠 DataFusion 44 does not handle AllOp: sqlparser produces `Expr::AllOp`
    // but datafusion-sql has no match arm for it and returns
    // `not_impl_err!("Unsupported ast node in sqltorel: ...")`.
    match result {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("AllOp") || msg.contains("not supported") || msg.contains("plan:")
                || msg.contains("Unsupported"),
                "unexpected error message: {msg}"
            );
            println!("🛠 subq_all_greater_eq: {msg}");
        }
        Ok(rows) => {
            let ids = i64_col(rows, "id");
            assert_eq!(ids, vec![1, 2, 3, 4, 5], ">= ALL result unexpected");
            println!("✅ subq_all_greater_eq: now works");
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. = ANY — equivalent to IN
// ─────────────────────────────────────────────────────────────────────────────

/// WHERE id = ANY (SELECT order_id FROM items)
///
/// DataFusion 44: `= ANY (subquery)` hits the NestedFunctionPlanner's
/// `plan_any` which rewrites `= ANY` to `array_has(right, left)`. When
/// `right` is a scalar subquery rather than an array literal this rewrite
/// produces a type-incompatible plan and fails at plan time.
/// Expected: either works (if DataFusion special-cases subquery rhs) or
/// plan error.
#[tokio::test]
async fn subq_eq_any() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let result = sess
        .execute(
            "SELECT id FROM orders \
             WHERE id = ANY (SELECT order_id FROM items) \
             ORDER BY id",
        )
        .await;
    match result {
        Ok(rows) => {
            // ✅ Scalar-subquery rhs handled correctly.
            let ids = i64_col(rows, "id");
            assert_eq!(ids, vec![1, 2, 3]);
            println!("✅ subq_eq_any: works");
        }
        Err(e) => {
            // 🛠 Expected in DataFusion 44 — array_has rewrite can't handle
            // subquery operand.
            println!("🛠 subq_eq_any: {}", e);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. Scalar subquery in SELECT list (non-correlated)
// ─────────────────────────────────────────────────────────────────────────────

/// SELECT (SELECT max(price) FROM items) AS m FROM orders
/// Expected: 5 rows all with m = 90.
#[tokio::test]
async fn subq_scalar_in_select() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = sess
        .execute("SELECT (SELECT max(price) FROM items) AS m FROM orders ORDER BY m")
        .await
        .unwrap();
    let vals = i64_col(res, "m");
    assert_eq!(vals.len(), 5, "should have one row per order");
    assert!(vals.iter().all(|&v| v == 90), "each row should have m=90");
}

// ─────────────────────────────────────────────────────────────────────────────
// 10. Correlated scalar subquery in SELECT list
// ─────────────────────────────────────────────────────────────────────────────

/// SELECT id, (SELECT count(*) FROM items WHERE items.order_id = o.id) AS n
/// FROM orders o ORDER BY id
///
/// Correlated scalar subquery in SELECT list. DataFusion 44 supports
/// decorrelation of scalar subqueries in projections via the
/// `DecorrelatePredicateSubquery` optimizer rule.
/// Expected: id 1→2, 2→1, 3→2, 4→0, 5→0
#[tokio::test]
async fn subq_correlated_scalar_in_select() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let result = sess
        .execute(
            "SELECT id, (SELECT count(*) FROM items WHERE items.order_id = o.id) AS n \
             FROM orders o ORDER BY id",
        )
        .await;

    match result {
        Ok(res) => {
            let (ids, counts) = match res {
                ExecResult::Rows { batches, .. } => {
                    let mut ids = Vec::new();
                    let mut counts = Vec::new();
                    for b in &batches {
                        let id_arr = b
                            .column_by_name("id")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        let n_arr = b
                            .column_by_name("n")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        for i in 0..b.num_rows() {
                            ids.push(id_arr.value(i));
                            counts.push(n_arr.value(i));
                        }
                    }
                    (ids, counts)
                }
                other => panic!("expected Rows, got {other:?}"),
            };
            assert_eq!(ids, vec![1, 2, 3, 4, 5]);
            assert_eq!(counts, vec![2, 1, 2, 0, 0]);
            println!("✅ subq_correlated_scalar_in_select: works");
        }
        Err(e) => {
            // 🛠 Correlated scalar subquery in SELECT not supported.
            println!("🛠 subq_correlated_scalar_in_select: {}", e);
            panic!("correlated scalar subquery in SELECT failed: {}", e);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 11. Scalar subquery in WHERE
// ─────────────────────────────────────────────────────────────────────────────

/// WHERE amount = (SELECT min(price) * 2 FROM items)
/// min(price) = 50 → 100. Order id=1 has amount=100. Expected: [1].
#[tokio::test]
async fn subq_scalar_in_where() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = sess
        .execute(
            "SELECT id FROM orders \
             WHERE amount = (SELECT min(price) * 2 FROM items) \
             ORDER BY id",
        )
        .await
        .unwrap();
    let ids = i64_col(res, "id");
    assert_eq!(ids, vec![1]);
}

// ─────────────────────────────────────────────────────────────────────────────
// 12. Derived table — simple subquery in FROM
// ─────────────────────────────────────────────────────────────────────────────

/// SELECT * FROM (SELECT id, name FROM orders WHERE amount > 200) sub
/// Expected: ids 3, 4, 5
#[tokio::test]
async fn subq_derived_table_simple() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = sess
        .execute(
            "SELECT id FROM (SELECT id, name FROM orders WHERE amount > 200) sub \
             ORDER BY id",
        )
        .await
        .unwrap();
    let ids = i64_col(res, "id");
    assert_eq!(ids, vec![3, 4, 5]);
}

// ─────────────────────────────────────────────────────────────────────────────
// 13. Derived table joined to another table
// ─────────────────────────────────────────────────────────────────────────────

/// SELECT sub.id FROM (SELECT id FROM orders) sub JOIN items ON sub.id = items.order_id
/// Expected: 5 rows total (ids 1,1,2,3,3 — one per item)
#[tokio::test]
async fn subq_derived_table_join() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = sess
        .execute(
            "SELECT sub.id FROM (SELECT id FROM orders) sub \
             JOIN items ON sub.id = items.order_id \
             ORDER BY sub.id",
        )
        .await
        .unwrap();
    let ids = i64_col(res, "id");
    assert_eq!(ids.len(), 5, "expected 5 joined rows (one per item)");
    // The joined order_ids should be 1,1,2,3,3
    assert_eq!(ids, vec![1, 1, 2, 3, 3]);
}
