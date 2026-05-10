//! Integration tests for engine-side routing of the Phase 5.11.M Tier 3
//! constraint introspection views: `information_schema.table_constraints`,
//! `.key_column_usage`, and `.referential_constraints`.
//!
//! Each test opens a `TenantSession` and runs SQL through the same
//! `execute()` entry point a pgwire connection would hit.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult, TenantSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

fn col_string(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            if arr.is_null(i) {
                out.push(String::new());
            } else {
                out.push(arr.value(i).to_string());
            }
        }
    }
    out
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

async fn rows(sess: &TenantSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

#[tokio::test]
async fn select_information_schema_table_constraints_routes() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE orders ( \
            id BIGINT NOT NULL, \
            payload TEXT, \
            shipped BOOLEAN NOT NULL \
         )",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "SELECT constraint_name, table_name, constraint_type, is_deferrable, initially_deferred \
         FROM information_schema.table_constraints \
         ORDER BY constraint_name",
    )
    .await;
    assert_eq!(total_rows(&batches), 2);
    let cnames = col_string(&batches, "constraint_name");
    let cstable = col_string(&batches, "table_name");
    let ctypes = col_string(&batches, "constraint_type");
    let cdef = col_string(&batches, "is_deferrable");
    let cinit = col_string(&batches, "initially_deferred");
    assert_eq!(
        cnames,
        vec![
            "orders_id_not_null".to_string(),
            "orders_shipped_not_null".to_string(),
        ]
    );
    assert!(cstable.iter().all(|n| n == "orders"));
    assert!(ctypes.iter().all(|t| t == "NOT NULL"));
    assert!(cdef.iter().all(|d| d == "NO"));
    assert!(cinit.iter().all(|d| d == "NO"));
}

#[tokio::test]
async fn select_information_schema_key_column_usage_routes() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, payload TEXT)")
        .await
        .unwrap();

    let batches = rows(
        &sess,
        "SELECT constraint_name, table_name, column_name, ordinal_position \
         FROM information_schema.key_column_usage",
    )
    .await;
    assert_eq!(
        total_rows(&batches),
        0,
        "v0.1 ships no PK / UNIQUE / FK constraints — key_column_usage must be empty"
    );
}

#[tokio::test]
async fn select_information_schema_referential_constraints_routes() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let batches = rows(
        &sess,
        "SELECT constraint_name, match_option, update_rule, delete_rule \
         FROM information_schema.referential_constraints",
    )
    .await;
    assert_eq!(
        total_rows(&batches),
        0,
        "v0.1 has no FOREIGN KEY support — referential_constraints must be empty"
    );
}

#[tokio::test]
async fn where_predicate_filters_constraint_type() {
    // DataFusion filters on top of our full-batch scan: a `WHERE
    // constraint_type = 'NOT NULL'` predicate must restrict to NOT NULL
    // rows. v0.1 only emits NOT NULL rows, so this is also a smoke test
    // that the predicate doesn't filter them all out.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE t ( \
            a BIGINT NOT NULL, \
            b TEXT NOT NULL, \
            c BOOLEAN \
         )",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "SELECT constraint_name FROM information_schema.table_constraints \
         WHERE constraint_type = 'NOT NULL' \
         ORDER BY constraint_name",
    )
    .await;
    let names = col_string(&batches, "constraint_name");
    assert_eq!(
        names,
        vec!["t_a_not_null".to_string(), "t_b_not_null".to_string()]
    );

    // No PRIMARY KEY rows in v0.1 — predicate must filter to zero.
    let batches = rows(
        &sess,
        "SELECT constraint_name FROM information_schema.table_constraints \
         WHERE constraint_type = 'PRIMARY KEY'",
    )
    .await;
    assert_eq!(total_rows(&batches), 0);
}
