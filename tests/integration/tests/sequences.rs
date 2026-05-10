//! SQL-level coverage for the sequence UDFs (`nextval` / `currval` /
//! `setval`). The Rust-API surface is covered by
//! `crates/basin-catalog/tests/sequences.rs`; this file exercises the
//! same machinery through `TenantSession::execute`.

use std::sync::Arc;

use arrow_array::{Array, Int64Array};
use basin_catalog::{InMemoryCatalog, SequenceDef};
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult, TenantSession};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

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

async fn open_session(engine: &Engine) -> TenantSession {
    engine.open_session(TenantId::new()).await.unwrap()
}

/// Pull the single i64 result from a SELECT that returns exactly one
/// row × one column.
async fn one_i64(sess: &TenantSession, sql: &str) -> i64 {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().expect("no batches");
            assert_eq!(b.num_rows(), 1, "expected 1 row, got {}", b.num_rows());
            assert_eq!(b.num_columns(), 1, "expected 1 column");
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| panic!("not Int64 result for {sql}"))
                .value(0)
        }
        Ok(other) => panic!("non-rows result for {sql}: {other:?}"),
        Err(e) => panic!("execute {sql}: {e}"),
    }
}

fn def(tenant: TenantId, name: &str, start: i64, increment: i64) -> SequenceDef {
    SequenceDef {
        tenant,
        name: name.into(),
        start,
        increment,
        min_value: 1,
        max_value: i64::MAX,
        cache_size: 1,
        cycle: false,
    }
}

#[tokio::test]
async fn select_nextval_round_trip() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    let cat = engine.config().catalog.clone();
    cat.create_sequence(def(sess.tenant(), "order_id_seq", 1, 1))
        .await
        .unwrap();

    assert_eq!(one_i64(&sess, "SELECT nextval('order_id_seq')").await, 1);
    assert_eq!(one_i64(&sess, "SELECT nextval('order_id_seq')").await, 2);
    assert_eq!(one_i64(&sess, "SELECT nextval('order_id_seq')").await, 3);
}

#[tokio::test]
async fn select_currval_after_nextval() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    let cat = engine.config().catalog.clone();
    cat.create_sequence(def(sess.tenant(), "s", 1, 1))
        .await
        .unwrap();

    let n = one_i64(&sess, "SELECT nextval('s')").await;
    let c = one_i64(&sess, "SELECT currval('s')").await;
    assert_eq!(n, c);
}

#[tokio::test]
async fn select_currval_without_nextval_errors() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    let cat = engine.config().catalog.clone();
    cat.create_sequence(def(sess.tenant(), "s", 1, 1))
        .await
        .unwrap();

    let err = sess
        .execute("SELECT currval('s')")
        .await
        .expect_err("currval should error when nextval has not been called");
    let msg = err.to_string();
    assert!(
        msg.contains("55000") || msg.contains("not yet defined"),
        "expected SQLSTATE 55000-shaped error, got: {msg}"
    );
}

#[tokio::test]
async fn select_setval_advances_state() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    let cat = engine.config().catalog.clone();
    cat.create_sequence(def(sess.tenant(), "s", 1, 1))
        .await
        .unwrap();

    // setval(seq, n) returns n and arms the sequence so next nextval returns n+1.
    assert_eq!(one_i64(&sess, "SELECT setval('s', 100)").await, 100);
    assert_eq!(one_i64(&sess, "SELECT nextval('s')").await, 101);

    // setval(seq, n, false) makes next nextval return exactly n.
    assert_eq!(one_i64(&sess, "SELECT setval('s', 200, false)").await, 200);
    assert_eq!(one_i64(&sess, "SELECT nextval('s')").await, 200);
}

#[tokio::test]
async fn nextval_cross_tenant_isolation() {
    // The same sequence name in two tenants must resolve through the
    // calling session's tenant — A's `nextval` only ever sees A's
    // sequence, never B's.
    let (_dir, engine) = open_engine().await;
    let cat = engine.config().catalog.clone();

    let a = TenantId::new();
    let b = TenantId::new();
    cat.create_sequence(def(a, "ids", 1, 1)).await.unwrap();
    cat.create_sequence(def(b, "ids", 1000, 1)).await.unwrap();

    let sa = engine.open_session(a).await.unwrap();
    let sb = engine.open_session(b).await.unwrap();

    assert_eq!(one_i64(&sa, "SELECT nextval('ids')").await, 1);
    assert_eq!(one_i64(&sb, "SELECT nextval('ids')").await, 1000);
    assert_eq!(one_i64(&sa, "SELECT nextval('ids')").await, 2);
    assert_eq!(one_i64(&sb, "SELECT nextval('ids')").await, 1001);
}

#[tokio::test]
async fn nextval_on_unknown_sequence_errors() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    let err = sess
        .execute("SELECT nextval('does_not_exist')")
        .await
        .expect_err("nextval on missing sequence should error");
    let msg = err.to_string();
    assert!(
        msg.contains("not found") || msg.contains("does_not_exist"),
        "expected NotFound-shaped error, got: {msg}"
    );
}
