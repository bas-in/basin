//! Integration tests for SQL set operations (UNION, INTERSECT, EXCEPT and their
//! ALL variants) and Common Table Expressions (CTEs), including recursive and
//! materialized forms.
//!
//! Each `#[tokio::test]` exercises one variant end-to-end through the Engine
//! (DataFusion backend) and asserts on the actual row output, not just that
//! execution doesn't panic.
//!
//! Audit cross-reference:
//! - `UNION` (deduped)             → set_op_union_deduplicates
//! - `UNION ALL` (raw concat)      → set_op_union_all_keeps_duplicates
//! - `INTERSECT`                   → set_op_intersect
//! - `INTERSECT ALL`               → set_op_intersect_all
//! - `EXCEPT`                      → set_op_except
//! - `EXCEPT ALL`                  → set_op_except_all
//! - Simple CTE                    → cte_simple
//! - Multiple CTEs with JOIN       → cte_multiple_with_join
//! - Recursive CTE                 → cte_recursive
//! - MATERIALIZED hint             → cte_materialized_hint

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
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

/// Collect all values from the first column of a `Rows` result as sorted i64.
fn collect_i64_sorted(res: ExecResult) -> Vec<i64> {
    let ExecResult::Rows { batches, .. } = res else {
        panic!("expected Rows result");
    };
    let mut out = Vec::new();
    for b in &batches {
        let col = b.column(0);
        let arr = col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("expected Int64Array in first column");
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out.sort_unstable();
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Set operations
// ─────────────────────────────────────────────────────────────────────────────

/// `UNION` deduplicates rows: {1,2} ∪ {2,3} → {1,2,3}.
#[tokio::test]
async fn set_op_union_deduplicates() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    s.execute("CREATE TABLE a (v BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("CREATE TABLE b (v BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO a VALUES (1), (2)").await.unwrap();
    s.execute("INSERT INTO b VALUES (2), (3)").await.unwrap();

    let res = s
        .execute("SELECT v FROM a UNION SELECT v FROM b")
        .await
        .expect("UNION should execute");
    let rows = collect_i64_sorted(res);
    assert_eq!(rows, vec![1, 2, 3], "UNION must deduplicate: got {rows:?}");
}

/// `UNION ALL` retains duplicate rows: {1,2} ∪ {2,3} → {1,2,2,3}.
#[tokio::test]
async fn set_op_union_all_keeps_duplicates() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    s.execute("CREATE TABLE a (v BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("CREATE TABLE b (v BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO a VALUES (1), (2)").await.unwrap();
    s.execute("INSERT INTO b VALUES (2), (3)").await.unwrap();

    let res = s
        .execute("SELECT v FROM a UNION ALL SELECT v FROM b")
        .await
        .expect("UNION ALL should execute");
    let rows = collect_i64_sorted(res);
    assert_eq!(
        rows,
        vec![1, 2, 2, 3],
        "UNION ALL must keep duplicates: got {rows:?}"
    );
}

/// `INTERSECT` returns only rows present in both sets: {1,2,3} ∩ {2,3,4} → {2,3}.
#[tokio::test]
async fn set_op_intersect() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    s.execute("CREATE TABLE a (v BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("CREATE TABLE b (v BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO a VALUES (1), (2), (3)")
        .await
        .unwrap();
    s.execute("INSERT INTO b VALUES (2), (3), (4)")
        .await
        .unwrap();

    let res = s
        .execute("SELECT v FROM a INTERSECT SELECT v FROM b")
        .await
        .expect("INTERSECT should execute");
    let rows = collect_i64_sorted(res);
    assert_eq!(
        rows,
        vec![2, 3],
        "INTERSECT must return common rows: got {rows:?}"
    );
}

/// `INTERSECT ALL` keeps per-occurrence minimums:
/// {1,2,2,3} ∩ {2,2,2,4} → {2,2} (min(2,3) = 2 copies of 2).
#[tokio::test]
async fn set_op_intersect_all() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    s.execute("CREATE TABLE a (v BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("CREATE TABLE b (v BIGINT NOT NULL)")
        .await
        .unwrap();
    // a = {1, 2, 2, 3}
    s.execute("INSERT INTO a VALUES (1), (2), (2), (3)")
        .await
        .unwrap();
    // b = {2, 2, 2, 4}
    s.execute("INSERT INTO b VALUES (2), (2), (2), (4)")
        .await
        .unwrap();

    let res = s
        .execute("SELECT v FROM a INTERSECT ALL SELECT v FROM b")
        .await
        .expect("INTERSECT ALL should execute");
    let rows = collect_i64_sorted(res);
    // min(2 occurrences in a, 3 occurrences in b) = 2 copies of 2
    assert_eq!(
        rows,
        vec![2, 2],
        "INTERSECT ALL must respect per-occurrence minimums: got {rows:?}"
    );
}

/// `EXCEPT` removes rows present in the right set: {1,2,3} − {2,3,4} → {1}.
#[tokio::test]
async fn set_op_except() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    s.execute("CREATE TABLE a (v BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("CREATE TABLE b (v BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO a VALUES (1), (2), (3)")
        .await
        .unwrap();
    s.execute("INSERT INTO b VALUES (2), (3), (4)")
        .await
        .unwrap();

    let res = s
        .execute("SELECT v FROM a EXCEPT SELECT v FROM b")
        .await
        .expect("EXCEPT should execute");
    let rows = collect_i64_sorted(res);
    assert_eq!(
        rows,
        vec![1],
        "EXCEPT must subtract right set: got {rows:?}"
    );
}

/// `EXCEPT ALL` subtracts per-occurrence (PG multiset semantics).
///
/// Status: 🛠 — DataFusion's `EXCEPT ALL` implementation applies set-distinct
/// semantics rather than true bag semantics: it removes *all* occurrences of
/// any value found in the right leg, not just one per occurrence. This diverges
/// from PostgreSQL where `{1,2,2,3} EXCEPT ALL {2,4}` → `{1,2,3}`.
///
/// This test asserts the *actual DataFusion behaviour* so regressions are
/// caught. Once DataFusion fixes bag semantics the expected value should be
/// updated to `vec![1, 2, 3]`.
#[tokio::test]
async fn set_op_except_all() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    s.execute("CREATE TABLE a (v BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("CREATE TABLE b (v BIGINT NOT NULL)")
        .await
        .unwrap();
    // a = {1, 2, 2, 3}
    s.execute("INSERT INTO a VALUES (1), (2), (2), (3)")
        .await
        .unwrap();
    // b = {2, 4}
    s.execute("INSERT INTO b VALUES (2), (4)").await.unwrap();

    let res = s
        .execute("SELECT v FROM a EXCEPT ALL SELECT v FROM b")
        .await
        .expect("EXCEPT ALL should execute");
    let rows = collect_i64_sorted(res);
    // DataFusion drops ALL copies of 2 (set semantics), leaving {1, 3}.
    // PG-correct result would be {1, 2, 3} (bag semantics).
    // TODO: update to vec![1, 2, 3] when DataFusion ships correct bag semantics.
    assert_eq!(
        rows,
        vec![1, 3],
        "EXCEPT ALL (DataFusion set-semantics divergence): got {rows:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Common Table Expressions (CTEs)
// ─────────────────────────────────────────────────────────────────────────────

/// Simple CTE: `WITH name AS (SELECT …) SELECT * FROM name`.
#[tokio::test]
async fn cte_simple() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    s.execute("CREATE TABLE items (id BIGINT NOT NULL, val BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO items VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let res = s
        .execute("WITH big AS (SELECT id, val FROM items WHERE val > 15) SELECT id FROM big")
        .await
        .expect("simple CTE should execute");
    let rows = collect_i64_sorted(res);
    assert_eq!(
        rows,
        vec![2, 3],
        "simple CTE filter must work: got {rows:?}"
    );
}

/// Multiple CTEs: `WITH a AS (…), b AS (…) SELECT * FROM a JOIN b ON …`.
#[tokio::test]
async fn cte_multiple_with_join() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    s.execute("CREATE TABLE users (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .unwrap();
    s.execute(
        "CREATE TABLE orders (id BIGINT NOT NULL, user_id BIGINT NOT NULL, amount BIGINT NOT NULL)",
    )
    .await
    .unwrap();
    s.execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .await
        .unwrap();
    s.execute("INSERT INTO orders VALUES (10, 1, 500), (11, 2, 150), (12, 1, 300)")
        .await
        .unwrap();

    let sql = "
        WITH active_users AS (SELECT id FROM users WHERE id <= 2),
             big_orders   AS (SELECT user_id FROM orders WHERE amount >= 300)
        SELECT active_users.id
        FROM active_users
        JOIN big_orders ON active_users.id = big_orders.user_id
    ";
    let res = s
        .execute(sql)
        .await
        .expect("multi-CTE with JOIN should execute");
    let rows = collect_i64_sorted(res);
    // user 1 has orders of 500 and 300 (both ≥ 300), user 2 has order of 150 (< 300)
    // active_users = {1, 2}; big_orders user_ids = {1, 1}
    // join produces two rows for user 1
    assert_eq!(
        rows,
        vec![1, 1],
        "multi-CTE join must produce correct rows: got {rows:?}"
    );
}

/// Recursive CTE: count from 1 to 5 via `UNION ALL` recursive step.
///
/// Note: DataFusion requires explicit `AS <name>` aliases on anchor-leg
/// literals; column-list syntax `WITH RECURSIVE counter(n)` is parsed but
/// the column name is not propagated into the schema, so the recursive leg
/// must reference the alias name rather than the positional alias.
#[tokio::test]
async fn cte_recursive() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    let sql = "
        WITH RECURSIVE counter AS (
            SELECT 1 AS n
            UNION ALL
            SELECT n + 1 FROM counter WHERE n < 5
        )
        SELECT n FROM counter
    ";
    let res = s.execute(sql).await.expect("recursive CTE should execute");
    let rows = collect_i64_sorted(res);
    assert_eq!(
        rows,
        vec![1, 2, 3, 4, 5],
        "recursive CTE must generate sequence 1..=5: got {rows:?}"
    );
}

/// `WITH RECURSIVE … INSERT INTO target SELECT * FROM cte` — the combination
/// of a recursive generator CTE feeding a DML statement (v0.1 gap, PR #XXXX).
///
/// sqlparser routes this as `Statement::Query{body: SetExpr::Insert}`.
/// Basin's executor lifts the `WITH RECURSIVE` clause onto the INSERT's source
/// query so DataFusion can expand the recursion before the rows are written.
#[tokio::test]
async fn cte_recursive_feeding_insert() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    s.execute("CREATE TABLE target (id BIGINT NOT NULL)")
        .await
        .unwrap();

    // Generate ids 1..=5 via a recursive CTE and INSERT them into `target`.
    let sql = "
        WITH RECURSIVE seq(id) AS (
            SELECT 1 AS id
            UNION ALL
            SELECT id + 1 FROM seq WHERE id < 5
        )
        INSERT INTO target SELECT id FROM seq
    ";
    s.execute(sql)
        .await
        .expect("WITH RECURSIVE feeding INSERT must succeed");

    // Verify rows were written.
    let res = s
        .execute("SELECT id FROM target ORDER BY id")
        .await
        .expect("SELECT after recursive INSERT");
    let rows = collect_i64_sorted(res);
    assert_eq!(
        rows,
        vec![1, 2, 3, 4, 5],
        "recursive CTE must insert ids 1..=5: got {rows:?}"
    );
}

/// `WITH RECURSIVE … DELETE FROM t WHERE id IN (SELECT id FROM cte)` —
/// recursive CTE feeding a DELETE statement.
///
/// Basin materialises the recursive CTE as a MemTable, then runs the DELETE
/// whose WHERE subquery references that MemTable.  Only the rows whose ids
/// appear in the CTE result are deleted; the rest survive.
#[tokio::test]
async fn cte_recursive_feeding_delete() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    s.execute("CREATE TABLE items (id BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO items VALUES (1),(2),(3),(4),(5),(6),(7)")
        .await
        .unwrap();

    // Recursive CTE generates ids 1..=4; delete those rows.
    let sql = "
        WITH RECURSIVE to_delete(id) AS (
            SELECT 1 AS id
            UNION ALL
            SELECT id + 1 FROM to_delete WHERE id < 4
        )
        DELETE FROM items WHERE id IN (SELECT id FROM to_delete)
    ";
    s.execute(sql)
        .await
        .expect("WITH RECURSIVE feeding DELETE must succeed");

    // Ids 5, 6, 7 should remain.
    let res = s
        .execute("SELECT id FROM items ORDER BY id")
        .await
        .expect("SELECT after recursive DELETE");
    let rows = collect_i64_sorted(res);
    assert_eq!(
        rows,
        vec![5, 6, 7],
        "recursive CTE DELETE must leave ids 5..=7: got {rows:?}"
    );
}

/// `WITH RECURSIVE … UPDATE t SET flag=true WHERE id IN (SELECT id FROM cte)`
/// — recursive CTE feeding an UPDATE statement.
///
/// Basin materialises the recursive CTE as a MemTable, then runs the UPDATE
/// whose WHERE subquery references that MemTable.  Only the matching rows are
/// updated; others are left unchanged.
#[tokio::test]
async fn cte_recursive_feeding_update() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    s.execute(
        "CREATE TABLE nodes (id BIGINT NOT NULL, flagged BOOLEAN NOT NULL DEFAULT FALSE)",
    )
    .await
    .unwrap();
    s.execute("INSERT INTO nodes (id) VALUES (1),(2),(3),(4),(5),(6)")
        .await
        .unwrap();

    // Recursive CTE generates ids 1..=3; update those rows to flagged = true.
    let sql = "
        WITH RECURSIVE to_flag(id) AS (
            SELECT 1 AS id
            UNION ALL
            SELECT id + 1 FROM to_flag WHERE id < 3
        )
        UPDATE nodes SET flagged = true WHERE id IN (SELECT id FROM to_flag)
    ";
    s.execute(sql)
        .await
        .expect("WITH RECURSIVE feeding UPDATE must succeed");

    // Ids 1..=3 must be flagged; 4..=6 must not.
    let res = s
        .execute("SELECT id FROM nodes WHERE flagged = true ORDER BY id")
        .await
        .expect("SELECT flagged rows after recursive UPDATE");
    let flagged = collect_i64_sorted(res);
    assert_eq!(
        flagged,
        vec![1, 2, 3],
        "recursive CTE UPDATE must flag ids 1..=3: got {flagged:?}"
    );

    let res2 = s
        .execute("SELECT id FROM nodes WHERE flagged = false ORDER BY id")
        .await
        .expect("SELECT unflagged rows after recursive UPDATE");
    let unflagged = collect_i64_sorted(res2);
    assert_eq!(
        unflagged,
        vec![4, 5, 6],
        "recursive CTE UPDATE must leave ids 4..=6 unflagged: got {unflagged:?}"
    );
}

/// `WITH name AS MATERIALIZED (…) SELECT …` — PG-15 materialization hint.
///
/// Status: 🛠 — sqlparser-rs (0.52/0.53) rejects `AS MATERIALIZED` with a
/// parse error. Basin passes the SQL through sqlparser before DataFusion, so
/// this hint is not yet supported. This test documents the current limitation
/// and will become a positive assertion once sqlparser support lands.
#[tokio::test]
async fn cte_materialized_hint() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    s.execute("CREATE TABLE nums (n BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO nums VALUES (10), (20), (30)")
        .await
        .unwrap();

    let sql = "WITH src AS MATERIALIZED (SELECT n FROM nums WHERE n >= 20) SELECT n FROM src";
    let result = s.execute(sql).await;
    // Currently rejected by sqlparser with a parse error. Assert the error
    // shape so a regression (e.g. silent wrong answer) is immediately visible.
    assert!(
        result.is_err(),
        "AS MATERIALIZED hint is not yet supported by sqlparser — \
         expected parse error, got Ok"
    );
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.contains("MATERIALIZED") || err_msg.contains("Parser") || err_msg.contains("parse"),
        "unexpected error message for MATERIALIZED CTE: {err_msg}"
    );
}
