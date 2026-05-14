//! Integration tests for PG `IS …` operator family and related conditional
//! expressions.
//!
//! Forms covered:
//!
//! ## Boolean tests
//! - `expr IS TRUE` / `expr IS NOT TRUE`
//! - `expr IS FALSE` / `expr IS NOT FALSE`
//! - `expr IS UNKNOWN` / `expr IS NOT UNKNOWN`
//!
//! ## NULL tests (scalar)
//! - `expr IS NULL` / `expr IS NOT NULL`  (sibling pg-operators; verified here)
//!
//! ## Type tests
//! - `expr IS [NOT] OF (type, ...)` — deprecated PG form; constant-true/false
//!   via rewrite since Basin columns are declared at CREATE TABLE time.
//!
//! ## Conditional functions
//! - `COALESCE(a, b, c)` — 3+ args
//! - `NULLIF(a, b)`
//! - `GREATEST(a, b, ...)` / `LEAST(a, b, ...)`
//! - `CASE WHEN … THEN … ELSE … END` (searched form)
//! - `CASE expr WHEN val THEN … END` (simple form)
//!
//! Each test is one `#[tokio::test]`, boots an in-memory engine, runs SQL
//! directly against a `TenantSession`, and asserts the returned Arrow array
//! matches expectations.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Int64Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Engine helper
// ---------------------------------------------------------------------------

fn make_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Execute a single-row SELECT that returns one boolean column named `v`.
/// Returns the (possibly NULL) value as `Option<bool>`.
async fn bool_val(sql: &str) -> Option<bool> {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();
    let res = sess
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL: {sql}"));
    match res {
        ExecResult::Rows { batches, .. } => {
            let b = &batches[0];
            let arr = b
                .column_by_name("v")
                .unwrap_or_else(|| panic!("column 'v' not found in: {sql}"))
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap_or_else(|| panic!("column 'v' is not BooleanArray in: {sql}"));
            if arr.is_null(0) {
                None
            } else {
                Some(arr.value(0))
            }
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// Execute a single-row SELECT that returns one i64 column named `v`.
async fn i64_val(sql: &str) -> i64 {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();
    let res = sess
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL: {sql}"));
    match res {
        ExecResult::Rows { batches, .. } => {
            let b = &batches[0];
            let arr = b
                .column_by_name("v")
                .unwrap_or_else(|| panic!("column 'v' not found"))
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| panic!("column 'v' is not Int64Array"));
            arr.value(0)
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// Execute a single-row SELECT that returns one string column named `v`.
async fn str_val(sql: &str) -> Option<String> {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();
    let res = sess
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL: {sql}"));
    match res {
        ExecResult::Rows { batches, .. } => {
            let b = &batches[0];
            let arr = b
                .column_by_name("v")
                .unwrap_or_else(|| panic!("column 'v' not found"))
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("column 'v' is not StringArray"));
            if arr.is_null(0) {
                None
            } else {
                Some(arr.value(0).to_string())
            }
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// IS TRUE / IS NOT TRUE
// ---------------------------------------------------------------------------

/// `TRUE IS TRUE` → true
#[tokio::test]
async fn is_true_on_true() {
    assert_eq!(
        bool_val("SELECT (true) IS TRUE AS v").await,
        Some(true),
        "TRUE IS TRUE must be true"
    );
}

/// `FALSE IS TRUE` → false
#[tokio::test]
async fn is_true_on_false() {
    assert_eq!(
        bool_val("SELECT (false) IS TRUE AS v").await,
        Some(false),
        "FALSE IS TRUE must be false"
    );
}

/// `NULL IS TRUE` → false (not NULL — IS TRUE never returns NULL)
#[tokio::test]
async fn is_true_on_null() {
    assert_eq!(
        bool_val("SELECT (NULL::boolean) IS TRUE AS v").await,
        Some(false),
        "NULL IS TRUE must be false"
    );
}

/// `TRUE IS NOT TRUE` → false
#[tokio::test]
async fn is_not_true_on_true() {
    assert_eq!(
        bool_val("SELECT (true) IS NOT TRUE AS v").await,
        Some(false),
        "TRUE IS NOT TRUE must be false"
    );
}

/// `FALSE IS NOT TRUE` → true
#[tokio::test]
async fn is_not_true_on_false() {
    assert_eq!(
        bool_val("SELECT (false) IS NOT TRUE AS v").await,
        Some(true),
        "FALSE IS NOT TRUE must be true"
    );
}

/// `NULL IS NOT TRUE` → true
#[tokio::test]
async fn is_not_true_on_null() {
    assert_eq!(
        bool_val("SELECT (NULL::boolean) IS NOT TRUE AS v").await,
        Some(true),
        "NULL IS NOT TRUE must be true"
    );
}

// ---------------------------------------------------------------------------
// IS FALSE / IS NOT FALSE
// ---------------------------------------------------------------------------

/// `FALSE IS FALSE` → true
#[tokio::test]
async fn is_false_on_false() {
    assert_eq!(
        bool_val("SELECT (false) IS FALSE AS v").await,
        Some(true),
        "FALSE IS FALSE must be true"
    );
}

/// `TRUE IS FALSE` → false
#[tokio::test]
async fn is_false_on_true() {
    assert_eq!(
        bool_val("SELECT (true) IS FALSE AS v").await,
        Some(false),
        "TRUE IS FALSE must be false"
    );
}

/// `NULL IS FALSE` → false
#[tokio::test]
async fn is_false_on_null() {
    assert_eq!(
        bool_val("SELECT (NULL::boolean) IS FALSE AS v").await,
        Some(false),
        "NULL IS FALSE must be false"
    );
}

/// `FALSE IS NOT FALSE` → false
#[tokio::test]
async fn is_not_false_on_false() {
    assert_eq!(
        bool_val("SELECT (false) IS NOT FALSE AS v").await,
        Some(false),
        "FALSE IS NOT FALSE must be false"
    );
}

/// `TRUE IS NOT FALSE` → true
#[tokio::test]
async fn is_not_false_on_true() {
    assert_eq!(
        bool_val("SELECT (true) IS NOT FALSE AS v").await,
        Some(true),
        "TRUE IS NOT FALSE must be true"
    );
}

/// `NULL IS NOT FALSE` → true
#[tokio::test]
async fn is_not_false_on_null() {
    assert_eq!(
        bool_val("SELECT (NULL::boolean) IS NOT FALSE AS v").await,
        Some(true),
        "NULL IS NOT FALSE must be true"
    );
}

// ---------------------------------------------------------------------------
// IS UNKNOWN / IS NOT UNKNOWN
// IS UNKNOWN is semantically identical to IS NULL on boolean expressions.
// PG defines: x IS UNKNOWN ≡ x IS NULL  (when x is boolean).
// sqlparser's PostgreSqlDialect parses IS UNKNOWN natively (Expr::IsUnknown).
// DataFusion handles the expression via its planner since it maps IS UNKNOWN
// to IS NULL on the boolean domain.
// ---------------------------------------------------------------------------

/// `NULL IS UNKNOWN` → true  (unknown = NULL in three-valued logic)
#[tokio::test]
async fn is_unknown_on_null() {
    assert_eq!(
        bool_val("SELECT (NULL::boolean) IS UNKNOWN AS v").await,
        Some(true),
        "NULL::boolean IS UNKNOWN must be true"
    );
}

/// `TRUE IS UNKNOWN` → false
#[tokio::test]
async fn is_unknown_on_true() {
    assert_eq!(
        bool_val("SELECT (true) IS UNKNOWN AS v").await,
        Some(false),
        "TRUE IS UNKNOWN must be false"
    );
}

/// `FALSE IS UNKNOWN` → false
#[tokio::test]
async fn is_unknown_on_false() {
    assert_eq!(
        bool_val("SELECT (false) IS UNKNOWN AS v").await,
        Some(false),
        "FALSE IS UNKNOWN must be false"
    );
}

/// `NULL IS NOT UNKNOWN` → false
#[tokio::test]
async fn is_not_unknown_on_null() {
    assert_eq!(
        bool_val("SELECT (NULL::boolean) IS NOT UNKNOWN AS v").await,
        Some(false),
        "NULL::boolean IS NOT UNKNOWN must be false"
    );
}

/// `TRUE IS NOT UNKNOWN` → true
#[tokio::test]
async fn is_not_unknown_on_true() {
    assert_eq!(
        bool_val("SELECT (true) IS NOT UNKNOWN AS v").await,
        Some(true),
        "TRUE IS NOT UNKNOWN must be true"
    );
}

// ---------------------------------------------------------------------------
// IS NULL / IS NOT NULL (scalar — verify sibling shipped correctly)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn is_null_on_null() {
    assert_eq!(
        bool_val("SELECT (NULL::bigint) IS NULL AS v").await,
        Some(true)
    );
}

#[tokio::test]
async fn is_null_on_value() {
    assert_eq!(
        bool_val("SELECT (42::bigint) IS NULL AS v").await,
        Some(false)
    );
}

#[tokio::test]
async fn is_not_null_on_null() {
    assert_eq!(
        bool_val("SELECT (NULL::bigint) IS NOT NULL AS v").await,
        Some(false)
    );
}

#[tokio::test]
async fn is_not_null_on_value() {
    assert_eq!(
        bool_val("SELECT (42::bigint) IS NOT NULL AS v").await,
        Some(true)
    );
}

// ---------------------------------------------------------------------------
// IS NULL / IS NOT NULL via table column (exercising the predicate path)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn is_null_filters_table_rows() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE nullable_t (id BIGINT NOT NULL, val BIGINT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO nullable_t VALUES (1, 10), (2, NULL), (3, 30)")
        .await
        .unwrap();

    // WHERE val IS NULL should return only id=2.
    let res = sess
        .execute("SELECT id FROM nullable_t WHERE val IS NULL")
        .await
        .unwrap();
    let ids: Vec<i64> = match res {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let arr = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..arr.len() {
                    out.push(arr.value(i));
                }
            }
            out
        }
        other => panic!("expected Rows, got {other:?}"),
    };
    assert_eq!(ids, vec![2], "IS NULL should return only the NULL row");
}

#[tokio::test]
async fn is_not_null_filters_table_rows() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE not_null_t (id BIGINT NOT NULL, val BIGINT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO not_null_t VALUES (1, 10), (2, NULL), (3, 30)")
        .await
        .unwrap();

    let res = sess
        .execute("SELECT id FROM not_null_t WHERE val IS NOT NULL ORDER BY id")
        .await
        .unwrap();
    let ids: Vec<i64> = match res {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let arr = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..arr.len() {
                    out.push(arr.value(i));
                }
            }
            out
        }
        other => panic!("expected Rows, got {other:?}"),
    };
    assert_eq!(ids, vec![1, 3], "IS NOT NULL should skip the NULL row");
}

// ---------------------------------------------------------------------------
// COALESCE — 3+ argument form
// ---------------------------------------------------------------------------

/// `COALESCE(NULL, NULL, 42)` → 42
#[tokio::test]
async fn coalesce_three_args_picks_first_non_null() {
    assert_eq!(
        i64_val("SELECT COALESCE(NULL, NULL, 42::bigint) AS v").await,
        42
    );
}

/// `COALESCE(1, 2, 3)` → 1 (first arg)
#[tokio::test]
async fn coalesce_returns_first_non_null() {
    assert_eq!(
        i64_val("SELECT COALESCE(1::bigint, 2::bigint, 3::bigint) AS v").await,
        1
    );
}

/// `COALESCE(NULL, 'hello', 'world')` → 'hello'
#[tokio::test]
async fn coalesce_string_three_args() {
    assert_eq!(
        str_val("SELECT COALESCE(NULL, 'hello', 'world') AS v").await,
        Some("hello".to_string())
    );
}

// ---------------------------------------------------------------------------
// NULLIF
// ---------------------------------------------------------------------------

/// `NULLIF(1, 1)` → NULL
#[tokio::test]
async fn nullif_equal_returns_null() {
    // NULLIF returns NULL when both args equal — cast result to check.
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();
    let res = sess
        .execute("SELECT NULLIF(1::bigint, 1::bigint) AS v")
        .await
        .unwrap();
    match res {
        ExecResult::Rows { batches, .. } => {
            let b = &batches[0];
            let arr = b
                .column_by_name("v")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert!(arr.is_null(0), "NULLIF(1, 1) must be NULL");
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// `NULLIF(1, 2)` → 1
#[tokio::test]
async fn nullif_unequal_returns_first() {
    assert_eq!(
        i64_val("SELECT NULLIF(1::bigint, 2::bigint) AS v").await,
        1
    );
}

// ---------------------------------------------------------------------------
// GREATEST / LEAST
// ---------------------------------------------------------------------------

/// `GREATEST(3, 1, 4, 1, 5, 9)` → 9
#[tokio::test]
async fn greatest_multiple_args() {
    assert_eq!(
        i64_val(
            "SELECT GREATEST(3::bigint, 1::bigint, 4::bigint, 1::bigint, 5::bigint, 9::bigint) AS v"
        )
        .await,
        9
    );
}

/// `LEAST(3, 1, 4, 1, 5, 9)` → 1
#[tokio::test]
async fn least_multiple_args() {
    assert_eq!(
        i64_val(
            "SELECT LEAST(3::bigint, 1::bigint, 4::bigint, 1::bigint, 5::bigint, 9::bigint) AS v"
        )
        .await,
        1
    );
}

/// `GREATEST(NULL, 5, 3)` → 5  (NULL is ignored per PG semantics)
#[tokio::test]
async fn greatest_ignores_null() {
    assert_eq!(
        i64_val("SELECT GREATEST(NULL, 5::bigint, 3::bigint) AS v").await,
        5
    );
}

/// `LEAST(NULL, 5, 3)` → 3  (NULL is ignored per PG semantics)
#[tokio::test]
async fn least_ignores_null() {
    assert_eq!(
        i64_val("SELECT LEAST(NULL, 5::bigint, 3::bigint) AS v").await,
        3
    );
}

/// `GREATEST` on strings: `GREATEST('apple', 'banana', 'cherry')` → 'cherry'
#[tokio::test]
async fn greatest_strings() {
    assert_eq!(
        str_val("SELECT GREATEST('apple', 'banana', 'cherry') AS v").await,
        Some("cherry".to_string())
    );
}

/// `LEAST` on strings: `LEAST('apple', 'banana', 'cherry')` → 'apple'
#[tokio::test]
async fn least_strings() {
    assert_eq!(
        str_val("SELECT LEAST('apple', 'banana', 'cherry') AS v").await,
        Some("apple".to_string())
    );
}

// ---------------------------------------------------------------------------
// CASE — searched form: CASE WHEN … THEN … ELSE … END
// ---------------------------------------------------------------------------

/// `CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END` → 'yes'
#[tokio::test]
async fn case_searched_when_true() {
    assert_eq!(
        str_val("SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END AS v").await,
        Some("yes".to_string())
    );
}

/// `CASE WHEN 1 < 0 THEN 'yes' ELSE 'no' END` → 'no'
#[tokio::test]
async fn case_searched_when_false() {
    assert_eq!(
        str_val("SELECT CASE WHEN 1 < 0 THEN 'yes' ELSE 'no' END AS v").await,
        Some("no".to_string())
    );
}

/// Multi-branch CASE: picks the first true branch.
#[tokio::test]
async fn case_searched_multi_branch() {
    assert_eq!(
        str_val(
            "SELECT CASE \
               WHEN 5 < 3 THEN 'low' \
               WHEN 5 < 7 THEN 'mid' \
               ELSE 'high' \
             END AS v"
        )
        .await,
        Some("mid".to_string())
    );
}

/// CASE without ELSE returns NULL when no branch matches.
#[tokio::test]
async fn case_searched_no_else_returns_null() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();
    let res = sess
        .execute("SELECT CASE WHEN 1 < 0 THEN 'yes' END AS v")
        .await
        .unwrap();
    match res {
        ExecResult::Rows { batches, .. } => {
            let b = &batches[0];
            let arr = b.column_by_name("v").unwrap();
            assert!(
                arr.is_null(0),
                "CASE with no matching branch and no ELSE must be NULL"
            );
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// CASE — simple form: CASE expr WHEN val THEN … END
// ---------------------------------------------------------------------------

/// `CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END` → 'two'
#[tokio::test]
async fn case_simple_form_matches() {
    assert_eq!(
        str_val(
            "SELECT CASE 2 \
               WHEN 1 THEN 'one' \
               WHEN 2 THEN 'two' \
               ELSE 'other' \
             END AS v"
        )
        .await,
        Some("two".to_string())
    );
}

/// `CASE 99 WHEN 1 THEN 'one' ELSE 'other' END` → 'other'
#[tokio::test]
async fn case_simple_form_falls_to_else() {
    assert_eq!(
        str_val(
            "SELECT CASE 99 \
               WHEN 1 THEN 'one' \
               ELSE 'other' \
             END AS v"
        )
        .await,
        Some("other".to_string())
    );
}

// ---------------------------------------------------------------------------
// CASE over a table column (verify CASE works in WHERE + SELECT projections)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_in_select_projection() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE grades (id BIGINT NOT NULL, score BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO grades VALUES (1, 90), (2, 70), (3, 50)")
        .await
        .unwrap();

    let res = sess
        .execute(
            "SELECT \
               CASE \
                 WHEN score >= 80 THEN 'A' \
                 WHEN score >= 60 THEN 'B' \
                 ELSE 'C' \
               END AS v \
             FROM grades \
             WHERE id = 1",
        )
        .await
        .unwrap();
    match res {
        ExecResult::Rows { batches, .. } => {
            let b = &batches[0];
            let arr = b
                .column_by_name("v")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(arr.value(0), "A");
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// IS TRUE / IS NOT TRUE over a table column
// ---------------------------------------------------------------------------

#[tokio::test]
async fn is_true_filters_table_rows() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE flags (id BIGINT NOT NULL, active BOOLEAN)",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO flags VALUES (1, true), (2, false), (3, NULL)")
        .await
        .unwrap();

    // IS TRUE: only id=1
    let res = sess
        .execute("SELECT id FROM flags WHERE active IS TRUE ORDER BY id")
        .await
        .unwrap();
    let ids: Vec<i64> = match res {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let arr = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..arr.len() {
                    out.push(arr.value(i));
                }
            }
            out
        }
        other => panic!("expected Rows, got {other:?}"),
    };
    assert_eq!(ids, vec![1], "IS TRUE should return only the TRUE row");
}

#[tokio::test]
async fn is_not_true_filters_table_rows() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE flags2 (id BIGINT NOT NULL, active BOOLEAN)",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO flags2 VALUES (1, true), (2, false), (3, NULL)")
        .await
        .unwrap();

    // IS NOT TRUE: id=2 (false) and id=3 (null)
    let res = sess
        .execute("SELECT id FROM flags2 WHERE active IS NOT TRUE ORDER BY id")
        .await
        .unwrap();
    let ids: Vec<i64> = match res {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let arr = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..arr.len() {
                    out.push(arr.value(i));
                }
            }
            out
        }
        other => panic!("expected Rows, got {other:?}"),
    };
    assert_eq!(
        ids,
        vec![2, 3],
        "IS NOT TRUE should return false and null rows"
    );
}

// ---------------------------------------------------------------------------
// IS DISTINCT FROM / IS NOT DISTINCT FROM (verify sibling shipped)
// ---------------------------------------------------------------------------

/// `1 IS DISTINCT FROM 2` → true
#[tokio::test]
async fn is_distinct_from_different_values() {
    assert_eq!(
        bool_val("SELECT (1::bigint IS DISTINCT FROM 2::bigint) AS v").await,
        Some(true)
    );
}

/// `1 IS DISTINCT FROM 1` → false
#[tokio::test]
async fn is_distinct_from_same_values() {
    assert_eq!(
        bool_val("SELECT (1::bigint IS DISTINCT FROM 1::bigint) AS v").await,
        Some(false)
    );
}

/// `NULL IS DISTINCT FROM NULL` → false (both are NULL → not distinct)
#[tokio::test]
async fn is_distinct_from_null_null() {
    assert_eq!(
        bool_val("SELECT (NULL::bigint IS DISTINCT FROM NULL::bigint) AS v").await,
        Some(false)
    );
}

/// `NULL IS DISTINCT FROM 1` → true
#[tokio::test]
async fn is_distinct_from_null_value() {
    assert_eq!(
        bool_val("SELECT (NULL::bigint IS DISTINCT FROM 1::bigint) AS v").await,
        Some(true)
    );
}

/// `NULL IS NOT DISTINCT FROM NULL` → true
#[tokio::test]
async fn is_not_distinct_from_null_null() {
    assert_eq!(
        bool_val("SELECT (NULL::bigint IS NOT DISTINCT FROM NULL::bigint) AS v").await,
        Some(true)
    );
}

// ---------------------------------------------------------------------------
// COALESCE in a WHERE clause (common pattern)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn coalesce_in_where_clause() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE items (id BIGINT NOT NULL, qty BIGINT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO items VALUES (1, 5), (2, NULL), (3, 10)")
        .await
        .unwrap();

    // COALESCE(qty, 0) > 3: rows where qty > 3 or (qty is null, treated as 0, so 0 > 3 = false)
    let res = sess
        .execute(
            "SELECT id FROM items WHERE COALESCE(qty, 0) > 3 ORDER BY id",
        )
        .await
        .unwrap();
    let ids: Vec<i64> = match res {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let arr = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..arr.len() {
                    out.push(arr.value(i));
                }
            }
            out
        }
        other => panic!("expected Rows, got {other:?}"),
    };
    assert_eq!(ids, vec![1, 3]);
}
