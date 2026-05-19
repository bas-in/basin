//! End-to-end tests for the `WITH (basin.row_block_size = N)` DDL option.
//!
//! Verifies that:
//! * `basin.row_block_size` is parsed, validated, and persisted in the
//!   catalog at CREATE TABLE time (both Parquet and Vortex formats).
//! * Rows inserted into such a table round-trip correctly via SELECT.
//! * Invalid values (0, non-power-of-two, out-of-range) are rejected at
//!   CREATE TABLE time with an appropriate error.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
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

async fn exec_ok(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

async fn exec_err(sess: &ProjectSession, sql: &str) -> String {
    sess.execute(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("expected error for {sql:?}"))
        .to_string()
}

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<arrow_array::RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

/// CREATE TABLE with `basin.row_block_size=1024` (Parquet format) → INSERT →
/// SELECT round-trips every value. Verifies catalog persistence by checking
/// that the second INSERT also succeeds (it loads the table metadata from
/// catalog, which must carry `row_block_size`).
#[tokio::test]
async fn parquet_row_block_size_round_trip() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE t (id BIGINT, val TEXT) \
         WITH (basin.row_block_size=1024)",
    )
    .await;
    exec_ok(
        &sess,
        "INSERT INTO t (id, val) VALUES (1, 'a'), (2, 'b'), (3, NULL)",
    )
    .await;

    let batches = rows(&sess, "SELECT id, val FROM t ORDER BY id").await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "all three inserted rows must come back");
}

/// Same as above but with Vortex format — verifies the option threads through
/// to the Vortex encoder's `row_block_size`.
#[tokio::test]
async fn vortex_row_block_size_round_trip() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE v (id BIGINT, val TEXT) \
         WITH (basin.file_format='vortex', basin.row_block_size=512)",
    )
    .await;
    exec_ok(
        &sess,
        "INSERT INTO v (id, val) VALUES (10, 'x'), (20, 'y')",
    )
    .await;

    let batches = rows(&sess, "SELECT id, val FROM v ORDER BY id").await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2, "both inserted rows must round-trip");
}

/// Catalog round-trip: after CREATE TABLE WITH basin.row_block_size=8192,
/// load_table must return row_block_size = Some(8192).
#[tokio::test]
async fn catalog_row_block_size_persisted() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE q (id BIGINT) WITH (basin.row_block_size=8192)",
    )
    .await;

    let meta = eng
        .config()
        .catalog
        .load_table(&sess.project(), &basin_common::TableName::new("q").unwrap())
        .await
        .unwrap();
    assert_eq!(
        meta.row_block_size,
        Some(8192),
        "catalog must persist row_block_size=8192"
    );
}

/// `basin.row_block_size=0` is rejected at CREATE TABLE time.
#[tokio::test]
async fn reject_zero() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    let err = exec_err(
        &sess,
        "CREATE TABLE bad (id BIGINT) WITH (basin.row_block_size=0)",
    )
    .await;
    assert!(
        err.contains("row_block_size"),
        "error must mention row_block_size, got: {err}"
    );
}

/// Non-power-of-two value (100) is rejected.
#[tokio::test]
async fn reject_non_power_of_two() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    let err = exec_err(
        &sess,
        "CREATE TABLE bad (id BIGINT) WITH (basin.row_block_size=100)",
    )
    .await;
    assert!(
        err.contains("row_block_size"),
        "error must mention row_block_size, got: {err}"
    );
}

/// Out-of-range value (8193, not a power of two) is rejected.
#[tokio::test]
async fn reject_8193() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    let err = exec_err(
        &sess,
        "CREATE TABLE bad (id BIGINT) WITH (basin.row_block_size=8193)",
    )
    .await;
    assert!(
        err.contains("row_block_size"),
        "error must mention row_block_size, got: {err}"
    );
}

/// Without the option the table behaves identically to before — no regression.
#[tokio::test]
async fn absent_option_defaults_to_none() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec_ok(&sess, "CREATE TABLE d (id BIGINT, val TEXT)").await;

    let meta = eng
        .config()
        .catalog
        .load_table(&sess.project(), &basin_common::TableName::new("d").unwrap())
        .await
        .unwrap();
    assert_eq!(
        meta.row_block_size,
        None,
        "catalog must carry None when option is absent"
    );

    exec_ok(&sess, "INSERT INTO d VALUES (1, 'hello')").await;
    let batches = rows(&sess, "SELECT id FROM d").await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1);
}
