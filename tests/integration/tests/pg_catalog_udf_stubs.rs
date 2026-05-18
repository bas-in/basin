//! Integration tests for the pg_catalog scalar-UDF stubs (Phase 5.11.N).
//!
//! Exercises every stub registered by `pg_catalog_udf::register_pg_catalog_udfs`.
//! Tests are intentionally lightweight: we assert only that
//!
//!   1. The engine plans and executes the query without a "Invalid function"
//!      error, and
//!   2. The returned value matches the documented stub constant.
//!
//! A `\dt`-style probe (the full JOIN that psql issues) is included as a
//! smoke test — it only asserts no UDF-resolution error fires.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Int64Array, StringArray};
use arrow_schema::DataType;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Test harness
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

/// Execute `sql`, expect exactly one row × one column. Return the single
/// `ArrayRef` column from that batch so callers can downcast as needed.
async fn single_col(sess: &basin_engine::ProjectSession, sql: &str) -> arrow_array::ArrayRef {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches
                .first()
                .unwrap_or_else(|| panic!("no batches from: {sql}"));
            assert_eq!(b.num_columns(), 1, "expected 1 column from: {sql}");
            b.column(0).clone()
        }
        Ok(other) => panic!("non-rows result for {sql}: {other:?}"),
        Err(e) => panic!("execute error for {sql}: {e}"),
    }
}

/// Execute `sql` and assert it completes without error. Rows are discarded.
async fn assert_no_error(sess: &basin_engine::ProjectSession, sql: &str) {
    match sess.execute(sql).await {
        Ok(_) => {}
        Err(e) => panic!("unexpected error for query [{sql}]: {e}"),
    }
}

// ---------------------------------------------------------------------------
// pg_table_is_visible
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_table_is_visible_bare_name() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT pg_table_is_visible('foo'::text)").await;
    let bools = col.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(bools.len(), 1);
    assert!(bools.value(0), "pg_table_is_visible should return true");
}

#[tokio::test]
async fn pg_table_is_visible_schema_qualified() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    // Schema-qualified spelling: pg_catalog.pg_table_is_visible
    let col = single_col(&sess, "SELECT pg_catalog.pg_table_is_visible('foo'::text)").await;
    let bools = col.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(bools.len(), 1);
    assert!(
        bools.value(0),
        "pg_catalog.pg_table_is_visible should return true"
    );
}

// ---------------------------------------------------------------------------
// pg_get_userbyid
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_get_userbyid_returns_text() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT pg_get_userbyid(0)").await;
    assert_eq!(col.data_type(), &DataType::Utf8);
    assert_eq!(col.len(), 1);
    // We don't assert a specific value — just that it returns something without error.
    assert!(
        !col.is_null(0),
        "pg_get_userbyid should return non-null text"
    );
    let s = col.as_any().downcast_ref::<StringArray>().unwrap();
    assert!(
        !s.value(0).is_empty(),
        "pg_get_userbyid returned empty string"
    );
}

// ---------------------------------------------------------------------------
// current_schema
// ---------------------------------------------------------------------------

#[tokio::test]
async fn current_schema_returns_public() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT current_schema()").await;
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "public");
}

#[tokio::test]
async fn pg_catalog_current_schema_returns_public() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT pg_catalog.current_schema()").await;
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "public");
}

// ---------------------------------------------------------------------------
// current_schemas
// ---------------------------------------------------------------------------

#[tokio::test]
async fn current_schemas_returns_list_with_public() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT current_schemas(true)").await;
    // Basin returns the PG text-array literal form as Utf8 (the df↔ws bridge
    // does not support List types).  Check that it contains 'public'.
    assert_eq!(col.data_type(), &DataType::Utf8);
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    let val = strings.value(0);
    assert!(
        val.contains("public"),
        "current_schemas result should mention 'public', got {val:?}"
    );
}

// ---------------------------------------------------------------------------
// pg_encoding_to_char
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_encoding_to_char_returns_utf8() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT pg_encoding_to_char(6)").await;
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "UTF8");
}

// ---------------------------------------------------------------------------
// format_type
// ---------------------------------------------------------------------------

#[tokio::test]
async fn format_type_known_oid() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    // OID 25 = text, OID 23 = integer
    let col = single_col(&sess, "SELECT format_type(25, NULL)").await;
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "text");
}

#[tokio::test]
async fn format_type_unknown_oid() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT format_type(99999, NULL)").await;
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "unknown");
}

// ---------------------------------------------------------------------------
// size functions — return 0
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_total_relation_size_returns_zero() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT pg_total_relation_size('foo'::text)").await;
    let ints = col.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ints.value(0), 0);
}

#[tokio::test]
async fn pg_table_size_returns_zero() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT pg_table_size('foo'::text)").await;
    let ints = col.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ints.value(0), 0);
}

#[tokio::test]
async fn pg_relation_size_returns_zero() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT pg_relation_size('foo'::text)").await;
    let ints = col.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ints.value(0), 0);
}

// ---------------------------------------------------------------------------
// description / comment stubs — return NULL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn obj_description_returns_null() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT obj_description(1, 'pg_class')").await;
    assert!(col.is_null(0), "obj_description should return NULL");
}

#[tokio::test]
async fn col_description_returns_null() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT col_description(1, 1)").await;
    assert!(col.is_null(0), "col_description should return NULL");
}

#[tokio::test]
async fn pg_get_partkeydef_returns_null() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT pg_get_partkeydef(1)").await;
    assert!(col.is_null(0), "pg_get_partkeydef should return NULL");
}

// ---------------------------------------------------------------------------
// privilege functions — return true
// ---------------------------------------------------------------------------

#[tokio::test]
async fn has_table_privilege_returns_true() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(
        &sess,
        "SELECT has_table_privilege('alice', 'pg_class', 'SELECT')",
    )
    .await;
    let bools = col.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(bools.value(0));
}

#[tokio::test]
async fn has_schema_privilege_returns_true() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(
        &sess,
        "SELECT has_schema_privilege('alice', 'public', 'USAGE')",
    )
    .await;
    let bools = col.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(bools.value(0));
}

// ---------------------------------------------------------------------------
// pg_get_expr, pg_get_indexdef, pg_get_constraintdef — empty string stubs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_get_expr_two_arg() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT pg_get_expr(NULL::text, 0)").await;
}

#[tokio::test]
async fn pg_get_indexdef_one_arg() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT pg_get_indexdef(1)").await;
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "");
}

#[tokio::test]
async fn pg_get_constraintdef_one_arg() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let col = single_col(&sess, "SELECT pg_get_constraintdef(1)").await;
    let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "");
}

// ---------------------------------------------------------------------------
// Full psql \dt-style probe — the exact query psql sends
//
// We just assert this executes without an "Invalid function" error.
// Row count is not checked — the pg_catalog views may be empty in tests.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn psql_dt_style_probe_no_udf_error() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // This is the core of psql's \dt query: it joins pg_class with
    // pg_namespace and filters using pg_catalog.pg_table_is_visible.
    let sql = r#"
        SELECT n.nspname, c.relname
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE pg_catalog.pg_table_is_visible(c.oid)
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    "#;

    assert_no_error(&sess, sql).await;
}

#[tokio::test]
async fn psql_dt_style_probe_with_relkind() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Extended probe including relkind filter (common in psql \dt output).
    // Note: Basin's pg_class view does not include relowner; we use
    // pg_get_userbyid with a literal 0 instead to exercise the UDF without
    // referencing a non-existent column.
    let sql = r#"
        SELECT n.nspname AS "Schema",
               c.relname AS "Name",
               pg_catalog.pg_get_userbyid(0) AS "Owner"
        FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r','p','')
          AND n.nspname <> 'pg_catalog'
          AND n.nspname !~ '^pg_toast'
          AND n.nspname <> 'information_schema'
          AND pg_catalog.pg_table_is_visible(c.oid)
        ORDER BY 1, 2
    "#;

    assert_no_error(&sess, sql).await;
}
