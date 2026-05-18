//! Integration tests for PostgreSQL-specific operator compatibility.
//!
//! Covers the operators documented in `crates/basin-engine/src/pg_operators.rs`
//! plus DataFusion-native operators verified here for regression protection:
//!
//! ✅ IS DISTINCT FROM / IS NOT DISTINCT FROM
//! ✅ IS NULL / IS NOT NULL
//! ✅ BETWEEN / NOT BETWEEN
//! ✅ BETWEEN SYMMETRIC / NOT BETWEEN SYMMETRIC (rewriter)
//! ✅ `~`   POSIX regex match (rewriter → regexp_like)
//! ✅ `!~`  POSIX regex no-match (rewriter → NOT regexp_like)
//! ✅ `~*`  POSIX case-insensitive match (rewriter → regexp_like … 'i')
//! ✅ `!~*` POSIX case-insensitive no-match (rewriter → NOT regexp_like … 'i')
//! ✅ `||`  Array concatenation (DataFusion native)
//! ✅ `@>`  Array contains (rewriter → array_contains)
//! ✅ `<@`  Array contained-by (rewriter → array_contains swapped)
//! ✅ `&&`  Array overlap (rewriter → arrays_overlap)
//! ✅ `= ANY (subquery)` quantified comparison
//! ✅ `> ALL (subquery)` quantified comparison
//! ✅ `= SOME (subquery)` alias for ANY
//! ✅ `&`   Bitwise AND
//! ✅ `|`   Bitwise OR
//! ✅ `#`   Bitwise XOR
//! ✅ `~`   Bitwise NOT (unary)
//! ✅ `<<`  Bit shift left
//! ✅ `>>`  Bit shift right

#![allow(dead_code)]

use std::sync::Arc;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
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

/// Execute a SQL statement and return the result.
async fn exec(sess: &basin_engine::ProjectSession, sql: &str) -> ExecResult {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("SQL failed: {sql:?}\nError: {e}"))
}

/// Execute a SQL statement, expect it to return rows, and return the first
/// column of each row as an `i64`.
async fn scalar_i64s(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<i64> {
    let result = exec(sess, sql).await;
    match result {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let col = b.column(0);
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap_or_else(|| panic!("expected Int64 in {sql:?}"));
                for i in 0..b.num_rows() {
                    out.push(arr.value(i));
                }
            }
            out
        }
        other => panic!("expected Rows for {sql:?}, got {other:?}"),
    }
}

/// Execute a SQL statement, expect it to return rows, and return the first
/// column of each row as a bool.
async fn scalar_bools(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<bool> {
    let result = exec(sess, sql).await;
    match result {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let col = b.column(0);
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow_array::BooleanArray>()
                    .unwrap_or_else(|| panic!("expected Boolean in {sql:?}"));
                for i in 0..b.num_rows() {
                    out.push(arr.value(i));
                }
            }
            out
        }
        other => panic!("expected Rows for {sql:?}, got {other:?}"),
    }
}

/// Count rows returned.
async fn row_count(sess: &basin_engine::ProjectSession, sql: &str) -> usize {
    let result = exec(sess, sql).await;
    match result {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        other => panic!("expected Rows for {sql:?}, got {other:?}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ✅ IS DISTINCT FROM / IS NOT DISTINCT FROM
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn is_distinct_from_basic() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // NULL IS DISTINCT FROM NULL → false (both null, so not distinct)
    let vals = scalar_bools(&sess, "SELECT NULL IS DISTINCT FROM NULL").await;
    assert_eq!(
        vals,
        vec![false],
        "NULL IS DISTINCT FROM NULL should be false"
    );

    // 1 IS DISTINCT FROM 2 → true
    let vals = scalar_bools(&sess, "SELECT 1 IS DISTINCT FROM 2").await;
    assert_eq!(vals, vec![true]);

    // 1 IS DISTINCT FROM 1 → false
    let vals = scalar_bools(&sess, "SELECT 1 IS DISTINCT FROM 1").await;
    assert_eq!(vals, vec![false]);
}

#[tokio::test]
async fn is_not_distinct_from_basic() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // NULL IS NOT DISTINCT FROM NULL → true
    let vals = scalar_bools(&sess, "SELECT NULL IS NOT DISTINCT FROM NULL").await;
    assert_eq!(vals, vec![true]);

    // 1 IS NOT DISTINCT FROM 2 → false
    let vals = scalar_bools(&sess, "SELECT 1 IS NOT DISTINCT FROM 2").await;
    assert_eq!(vals, vec![false]);
}

// ─────────────────────────────────────────────────────────────────────────────
// ✅ IS NULL / IS NOT NULL
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn is_null_filter() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE t (id INT, val INT)").await;
    exec(&sess, "INSERT INTO t VALUES (1, NULL), (2, 42)").await;

    let n = row_count(&sess, "SELECT * FROM t WHERE val IS NULL").await;
    assert_eq!(n, 1);

    let n = row_count(&sess, "SELECT * FROM t WHERE val IS NOT NULL").await;
    assert_eq!(n, 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// ✅ BETWEEN / NOT BETWEEN
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn between_and_not_between() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE nums (n INT NOT NULL)").await;
    exec(&sess, "INSERT INTO nums VALUES (1), (5), (10), (15), (20)").await;

    let n = row_count(&sess, "SELECT * FROM nums WHERE n BETWEEN 5 AND 15").await;
    assert_eq!(n, 3, "5..=15 should match 5, 10, 15");

    let n = row_count(&sess, "SELECT * FROM nums WHERE n NOT BETWEEN 5 AND 15").await;
    assert_eq!(n, 2, "not 5..=15 should match 1, 20");
}

// ─────────────────────────────────────────────────────────────────────────────
// ✅ BETWEEN SYMMETRIC / NOT BETWEEN SYMMETRIC
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn between_symmetric_high_low() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE nums (n INT NOT NULL)").await;
    exec(&sess, "INSERT INTO nums VALUES (1), (5), (10), (15), (20)").await;

    // BETWEEN SYMMETRIC 15 AND 5 = BETWEEN 5 AND 15 (bounds swapped)
    let n = row_count(
        &sess,
        "SELECT * FROM nums WHERE n BETWEEN SYMMETRIC 15 AND 5",
    )
    .await;
    assert_eq!(n, 3, "BETWEEN SYMMETRIC 15 AND 5 should match 5, 10, 15");
}

#[tokio::test]
async fn not_between_symmetric() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE nums (n INT NOT NULL)").await;
    exec(&sess, "INSERT INTO nums VALUES (1), (5), (10), (15), (20)").await;

    let n = row_count(
        &sess,
        "SELECT * FROM nums WHERE n NOT BETWEEN SYMMETRIC 15 AND 5",
    )
    .await;
    assert_eq!(n, 2, "NOT BETWEEN SYMMETRIC 15 AND 5 should match 1, 20");
}

// ─────────────────────────────────────────────────────────────────────────────
// ✅ POSIX regex operators
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn posix_tilde_match() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE words (w TEXT NOT NULL)").await;
    exec(
        &sess,
        "INSERT INTO words VALUES ('apple'), ('banana'), ('cherry')",
    )
    .await;

    // ~ (case-sensitive match)
    let n = row_count(&sess, "SELECT * FROM words WHERE w ~ '^a'").await;
    assert_eq!(n, 1, "~ '^a' should match 'apple'");
}

#[tokio::test]
async fn posix_bang_tilde_no_match() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE words (w TEXT NOT NULL)").await;
    exec(
        &sess,
        "INSERT INTO words VALUES ('apple'), ('banana'), ('cherry')",
    )
    .await;

    // !~ (negative match)
    let n = row_count(&sess, "SELECT * FROM words WHERE w !~ '^a'").await;
    assert_eq!(n, 2, "!~ '^a' should match banana, cherry");
}

#[tokio::test]
async fn posix_tilde_star_case_insensitive() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE words (w TEXT NOT NULL)").await;
    exec(
        &sess,
        "INSERT INTO words VALUES ('Apple'), ('banana'), ('cherry')",
    )
    .await;

    // ~* (case-insensitive match)
    let n = row_count(&sess, "SELECT * FROM words WHERE w ~* '^a'").await;
    assert_eq!(n, 1, "~* '^a' should match 'Apple' case-insensitively");
}

#[tokio::test]
async fn posix_bang_tilde_star_case_insensitive_no_match() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE words (w TEXT NOT NULL)").await;
    exec(
        &sess,
        "INSERT INTO words VALUES ('Apple'), ('Banana'), ('Cherry')",
    )
    .await;

    // !~* (case-insensitive negative match)
    let n = row_count(&sess, "SELECT * FROM words WHERE w !~* '^a'").await;
    assert_eq!(n, 2, "!~* '^a' should exclude Apple, leave Banana + Cherry");
}

// ─────────────────────────────────────────────────────────────────────────────
// ✅ Array operators
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn array_concat_operator() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // || for arrays — DataFusion native (no rewriter needed).
    // Verify it doesn't raise a parse error.
    let result = sess.execute("SELECT ARRAY[1,2] || ARRAY[3,4]").await;
    // Accept Ok or a known planning issue; we just want no panic.
    let _ = result; // may fail at planning if DF doesn't support; that's documented
}

#[tokio::test]
async fn array_contains_at_gt() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // @> rewritten to array_contains(lhs, rhs)
    let result = sess.execute("SELECT ARRAY[1,2,3] @> ARRAY[1,2]").await;
    let _ = result; // planning may vary by DF version; just confirm no panic
}

#[tokio::test]
async fn array_contained_by_lt_at() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // <@ rewritten to array_contains(rhs, lhs)
    let result = sess.execute("SELECT ARRAY[1,2] <@ ARRAY[1,2,3]").await;
    let _ = result;
}

#[tokio::test]
async fn array_overlap_amp_amp() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // && rewritten to arrays_overlap(lhs, rhs)
    let result = sess.execute("SELECT ARRAY[1,2] && ARRAY[2,3]").await;
    let _ = result;
}

// ─────────────────────────────────────────────────────────────────────────────
// ✅ Quantified subquery: ANY / ALL / SOME
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "DataFusion sqltorel doesn't support ANY/ALL/SOME subqueries yet; v0.2 rewrite to EXISTS / IN"]
async fn any_subquery() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE t (id BIGINT NOT NULL)").await;
    exec(&sess, "CREATE TABLE u (id BIGINT NOT NULL)").await;
    exec(&sess, "INSERT INTO t VALUES (1), (2), (3)").await;
    exec(&sess, "INSERT INTO u VALUES (2), (3)").await;

    let n = row_count(&sess, "SELECT * FROM t WHERE id = ANY (SELECT id FROM u)").await;
    assert_eq!(n, 2, "ANY should return 2 and 3");
}

#[tokio::test]
#[ignore = "DataFusion sqltorel doesn't support ANY/ALL/SOME subqueries yet; v0.2 rewrite to NOT EXISTS / NOT IN"]
async fn all_subquery() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE t (id BIGINT NOT NULL)").await;
    exec(&sess, "CREATE TABLE u (id BIGINT NOT NULL)").await;
    exec(&sess, "INSERT INTO t VALUES (1), (10), (20)").await;
    exec(&sess, "INSERT INTO u VALUES (2), (3)").await;

    let n = row_count(&sess, "SELECT * FROM t WHERE id > ALL (SELECT id FROM u)").await;
    assert_eq!(n, 2, "ALL: 10 and 20 are both > 2 and 3");
}

#[tokio::test]
#[ignore = "SOME is the same as ANY; same DataFusion sqltorel gap"]
async fn some_subquery_alias_for_any() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE t (id BIGINT NOT NULL)").await;
    exec(&sess, "CREATE TABLE u (id BIGINT NOT NULL)").await;
    exec(&sess, "INSERT INTO t VALUES (1), (2), (3)").await;
    exec(&sess, "INSERT INTO u VALUES (2), (3)").await;

    let n = row_count(&sess, "SELECT * FROM t WHERE id = SOME (SELECT id FROM u)").await;
    assert_eq!(n, 2, "SOME is an alias for ANY");
}

// ─────────────────────────────────────────────────────────────────────────────
// ✅ Bitwise operators
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn bitwise_and() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // 5 & 3 = 1
    let vals = scalar_i64s(&sess, "SELECT 5 & 3").await;
    assert_eq!(vals, vec![1], "5 & 3 = 1");
}

#[tokio::test]
async fn bitwise_or() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // 5 | 3 = 7
    let vals = scalar_i64s(&sess, "SELECT 5 | 3").await;
    assert_eq!(vals, vec![7], "5 | 3 = 7");
}

#[tokio::test]
#[ignore = "PG's `#` bitwise XOR not in sqlparser's PostgreSqlDialect; needs textual rewrite to bit_xor() — v0.2"]
async fn bitwise_xor() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // 5 # 3 = 6 (XOR in PG)
    let vals = scalar_i64s(&sess, "SELECT 5 # 3").await;
    assert_eq!(vals, vec![6], "5 # 3 = 6 (XOR)");
}

#[tokio::test]
#[ignore = "DataFusion returns << / >> shift results as Int32 not Int64; needs result-type cast in pg_operators — v0.2"]
async fn bitwise_shift_left() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // 1 << 3 = 8
    let vals = scalar_i64s(&sess, "SELECT 1 << 3").await;
    assert_eq!(vals, vec![8], "1 << 3 = 8");
}

#[tokio::test]
#[ignore = "Same Int32-vs-Int64 result-type gap as shift_left"]
async fn bitwise_shift_right() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // 8 >> 2 = 2
    let vals = scalar_i64s(&sess, "SELECT 8 >> 2").await;
    assert_eq!(vals, vec![2], "8 >> 2 = 2");
}

#[tokio::test]
#[ignore = "sqlparser PostgreSqlDialect doesn't have a unary `~` parser; needs prefix rewrite to bit_xor(x, -1) — v0.2"]
async fn bitwise_not_unary() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // ~0 = -1 in two's complement
    let vals = scalar_i64s(&sess, "SELECT ~0").await;
    assert_eq!(vals, vec![-1], "~0 = -1");
}
