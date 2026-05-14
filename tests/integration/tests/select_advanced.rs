//! Integration tests for advanced SELECT clauses.
//!
//! Covers the ✅ rows added to CAPABILITIES.md under "Advanced SELECT":
//!
//! - `DISTINCT ON (cols)` — sqlparser parses; DataFusion 44 plans via
//!   `DistinctOn` logical node. Real semantic deduplication.
//! - `FOR UPDATE [OF tbl] [SKIP LOCKED|NOWAIT]` / `FOR SHARE …` — parsed by
//!   sqlparser, silently ignored by DataFusion (advisory; basin is
//!   append-only/optimistic).
//! - `FOR NO KEY UPDATE` / `FOR KEY SHARE` — pre-screened to `FOR UPDATE` /
//!   `FOR SHARE` by `select_advanced::rewrite_for_no_key_update_and_key_share`
//!   before sqlparser sees the SQL.
//! - `FETCH FIRST N ROWS ONLY` / `FETCH NEXT N ROWS ONLY` — pre-screened to
//!   `LIMIT N` by `select_advanced::rewrite_fetch_to_limit`.
//! - `OFFSET N ROW` / `OFFSET N ROWS` — parsed natively by sqlparser;
//!   DataFusion reads `Offset.value` and ignores the ROW/ROWS keyword.
//! - `ORDER BY … NULLS FIRST` / `ORDER BY … NULLS LAST` — sqlparser
//!   `OrderByExpr.nulls_first`; DataFusion honours it via `SortExpr`.
//! - `TABLE <name>` shorthand — pre-screened to `SELECT * FROM <name>`.
//! - `TABLESAMPLE BERNOULLI(N)` / `SYSTEM(N)` — clause stripped; full scan.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
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

/// Execute SQL and return the first batch's i64 column as a vec.
async fn i64_column(engine: &Engine, sql: &str, col: &str) -> Vec<i64> {
    let s = engine.open_session(TenantId::new()).await.unwrap();
    // seed table
    s.execute("CREATE TABLE t (id BIGINT NOT NULL, cat TEXT NOT NULL, val BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute(
        "INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', 40)",
    )
    .await
    .unwrap();
    collect_i64(&s.execute(sql).await.unwrap(), col)
}

fn collect_i64(res: &ExecResult, col: &str) -> Vec<i64> {
    let ExecResult::Rows { batches, .. } = res else {
        panic!("expected Rows, got {:?}", res);
    };
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(col)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("column {col} is not Int64"));
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

fn collect_str(res: &ExecResult, col: &str) -> Vec<String> {
    let ExecResult::Rows { batches, .. } = res else {
        panic!("expected Rows, got {:?}", res);
    };
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(col)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("column {col} is not Utf8"));
        for i in 0..arr.len() {
            out.push(arr.value(i).to_owned());
        }
    }
    out
}

// ---------------------------------------------------------------------------
// DISTINCT ON
// ---------------------------------------------------------------------------

/// `DISTINCT ON (cat)` should return one row per distinct category value,
/// choosing the first row in the ORDER BY order (lowest id).
#[tokio::test]
async fn distinct_on_returns_one_row_per_key() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(TenantId::new()).await.unwrap();
    s.execute("CREATE TABLE events (id BIGINT NOT NULL, cat TEXT NOT NULL, val BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute(
        "INSERT INTO events VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', 40)",
    )
    .await
    .unwrap();

    let res = s
        .execute("SELECT DISTINCT ON (cat) id, cat, val FROM events ORDER BY cat, id")
        .await
        .unwrap();
    // Should return one row per category (cat='a' → id=1, cat='b' → id=3).
    let ids = collect_i64(&res, "id");
    assert_eq!(ids, vec![1, 3], "DISTINCT ON should pick the first row per cat");
}

// ---------------------------------------------------------------------------
// FOR UPDATE / FOR SHARE (advisory; query still executes)
// ---------------------------------------------------------------------------

/// `FOR UPDATE` must not prevent the SELECT from returning rows.
#[tokio::test]
async fn for_update_advisory_executes_normally() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let ids = i64_column(
        &engine,
        "SELECT id FROM t ORDER BY id FOR UPDATE",
        "id",
    )
    .await;
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

/// `FOR UPDATE OF t SKIP LOCKED` — full modifier set, still advisory.
#[tokio::test]
async fn for_update_of_skip_locked_executes_normally() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let ids = i64_column(
        &engine,
        "SELECT id FROM t ORDER BY id FOR UPDATE OF t SKIP LOCKED",
        "id",
    )
    .await;
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

/// `FOR UPDATE NOWAIT` — advisory.
#[tokio::test]
async fn for_update_nowait_executes_normally() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let ids = i64_column(
        &engine,
        "SELECT id FROM t ORDER BY id FOR UPDATE NOWAIT",
        "id",
    )
    .await;
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

/// `FOR SHARE` — advisory.
#[tokio::test]
async fn for_share_advisory_executes_normally() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let ids = i64_column(
        &engine,
        "SELECT id FROM t ORDER BY id FOR SHARE",
        "id",
    )
    .await;
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

// ---------------------------------------------------------------------------
// FOR NO KEY UPDATE / FOR KEY SHARE (pre-screened to FOR UPDATE / FOR SHARE)
// ---------------------------------------------------------------------------

/// `FOR NO KEY UPDATE` must be accepted and execute the SELECT normally.
#[tokio::test]
async fn for_no_key_update_rewrites_and_executes() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let ids = i64_column(
        &engine,
        "SELECT id FROM t ORDER BY id FOR NO KEY UPDATE",
        "id",
    )
    .await;
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

/// `FOR NO KEY UPDATE SKIP LOCKED` — modifier preserved through rewrite.
#[tokio::test]
async fn for_no_key_update_skip_locked_executes() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let ids = i64_column(
        &engine,
        "SELECT id FROM t ORDER BY id FOR NO KEY UPDATE SKIP LOCKED",
        "id",
    )
    .await;
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

/// `FOR KEY SHARE` must be accepted and execute normally.
#[tokio::test]
async fn for_key_share_rewrites_and_executes() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let ids = i64_column(
        &engine,
        "SELECT id FROM t ORDER BY id FOR KEY SHARE",
        "id",
    )
    .await;
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

// ---------------------------------------------------------------------------
// FETCH FIRST / NEXT N ROWS ONLY
// ---------------------------------------------------------------------------

/// `FETCH FIRST N ROWS ONLY` should limit results like `LIMIT N`.
#[tokio::test]
async fn fetch_first_rows_only_limits_results() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let ids = i64_column(
        &engine,
        "SELECT id FROM t ORDER BY id FETCH FIRST 2 ROWS ONLY",
        "id",
    )
    .await;
    assert_eq!(ids, vec![1, 2]);
}

/// `FETCH NEXT N ROWS ONLY` — same behaviour.
#[tokio::test]
async fn fetch_next_rows_only_limits_results() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let ids = i64_column(
        &engine,
        "SELECT id FROM t ORDER BY id FETCH NEXT 3 ROWS ONLY",
        "id",
    )
    .await;
    assert_eq!(ids, vec![1, 2, 3]);
}

/// `OFFSET M ROWS FETCH NEXT N ROWS ONLY` — combined form.
#[tokio::test]
async fn offset_rows_fetch_next_rows_only_paginated() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let ids = i64_column(
        &engine,
        "SELECT id FROM t ORDER BY id OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY",
        "id",
    )
    .await;
    assert_eq!(ids, vec![3, 4]);
}

// ---------------------------------------------------------------------------
// OFFSET N ROW / ROWS
// ---------------------------------------------------------------------------

/// `OFFSET N ROWS` (without FETCH) is handled by sqlparser natively.
#[tokio::test]
async fn offset_n_rows_skips_leading_rows() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let ids = i64_column(
        &engine,
        "SELECT id FROM t ORDER BY id LIMIT 10 OFFSET 2 ROWS",
        "id",
    )
    .await;
    assert_eq!(ids, vec![3, 4]);
}

/// `OFFSET N ROW` (singular) is also accepted.
#[tokio::test]
async fn offset_n_row_singular_skips_leading_rows() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let ids = i64_column(
        &engine,
        "SELECT id FROM t ORDER BY id LIMIT 10 OFFSET 1 ROW",
        "id",
    )
    .await;
    assert_eq!(ids, vec![2, 3, 4]);
}

// ---------------------------------------------------------------------------
// ORDER BY … NULLS FIRST / LAST
// ---------------------------------------------------------------------------

/// `ORDER BY val NULLS FIRST` should be accepted (DataFusion honours it).
/// We can't test NULL ordering without NULLs in the data, but we verify the
/// clause doesn't cause a parse or execution error and returns the right rows.
#[tokio::test]
async fn order_by_nulls_first_accepted() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let ids = i64_column(
        &engine,
        "SELECT id FROM t ORDER BY val ASC NULLS FIRST",
        "id",
    )
    .await;
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

/// `ORDER BY val DESC NULLS LAST` — likewise.
#[tokio::test]
async fn order_by_nulls_last_accepted() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let ids = i64_column(
        &engine,
        "SELECT id FROM t ORDER BY val DESC NULLS LAST",
        "id",
    )
    .await;
    assert_eq!(ids, vec![4, 3, 2, 1]);
}

// ---------------------------------------------------------------------------
// TABLE <name> shorthand
// ---------------------------------------------------------------------------

/// `TABLE t` (SQL-standard) rewrites to `SELECT * FROM t`.
#[tokio::test]
async fn table_shorthand_selects_all_rows() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(TenantId::new()).await.unwrap();
    s.execute("CREATE TABLE items (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO items VALUES (1, 'alpha'), (2, 'beta')")
        .await
        .unwrap();
    let res = s.execute("TABLE items").await.unwrap();
    let names = collect_str(&res, "name");
    // Order is not guaranteed; just check both are present.
    assert!(
        names.contains(&"alpha".to_owned()) && names.contains(&"beta".to_owned()),
        "TABLE shorthand did not return expected rows: {names:?}"
    );
}

// ---------------------------------------------------------------------------
// TABLESAMPLE (strip; returns all rows)
// ---------------------------------------------------------------------------

/// `TABLESAMPLE BERNOULLI(N)` is stripped; all rows are returned.
#[tokio::test]
async fn tablesample_bernoulli_returns_all_rows() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let ids = i64_column(
        &engine,
        "SELECT id FROM t TABLESAMPLE BERNOULLI(50) ORDER BY id",
        "id",
    )
    .await;
    // Strip means we get all 4 rows.
    assert_eq!(ids, vec![1, 2, 3, 4]);
}
