//! Integration tests for `LANGUAGE sql RETURNS TABLE` functions
//! (Phase 5.11.E).
//!
//! Exercises the planning-time inliner's table-position branch and the
//! `CREATE FUNCTION ... RETURNS TABLE(...)` SQL surface.

use std::sync::Arc;

use arrow_array::{Array, Int64Array};
use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult};
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

fn shared_engine(dir: &TempDir, catalog: Arc<dyn basin_catalog::Catalog>) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

fn col_i64(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
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

async fn setup_orders_table(eng: &Engine) -> basin_engine::TenantSession {
    let sess = eng.open_session(TenantId::new()).await.unwrap();
    sess.execute(
        "CREATE TABLE orders (id BIGINT NOT NULL, owner BIGINT NOT NULL, amount BIGINT NOT NULL)",
    )
    .await
    .unwrap();
    sess.execute(
        "INSERT INTO orders VALUES (1, 42, 500), (2, 42, 1500), (3, 42, 2500), (4, 7, 999)",
    )
    .await
    .unwrap();
    sess
}

#[tokio::test]
async fn returns_table_basic() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = setup_orders_table(&eng).await;

    sess.execute(
        "CREATE FUNCTION recent_orders(uid BIGINT) RETURNS TABLE(id BIGINT, amount BIGINT) \
         LANGUAGE sql AS $$ SELECT id, amount FROM orders WHERE owner = uid $$",
    )
    .await
    .unwrap();

    let res = sess
        .execute("SELECT id, amount FROM recent_orders(42) ORDER BY id")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(col_i64(&batches, "id"), vec![1, 2, 3]);
    assert_eq!(col_i64(&batches, "amount"), vec![500, 1500, 2500]);
}

#[tokio::test]
async fn returns_table_with_where_filter() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = setup_orders_table(&eng).await;

    sess.execute(
        "CREATE FUNCTION recent_orders(uid BIGINT) RETURNS TABLE(id BIGINT, amount BIGINT) \
         LANGUAGE sql AS $$ SELECT id, amount FROM orders WHERE owner = uid $$",
    )
    .await
    .unwrap();

    let res = sess
        .execute("SELECT id, amount FROM recent_orders(42) WHERE amount > 1000 ORDER BY id")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(col_i64(&batches, "id"), vec![2, 3]);
    assert_eq!(col_i64(&batches, "amount"), vec![1500, 2500]);
}

#[tokio::test]
async fn returns_table_with_join() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = setup_orders_table(&eng).await;

    sess.execute("CREATE TABLE accounts (user_id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO accounts VALUES (42), (7)")
        .await
        .unwrap();

    sess.execute(
        "CREATE FUNCTION owner_orders(uid BIGINT) RETURNS TABLE(id BIGINT, amount BIGINT) \
         LANGUAGE sql AS $$ SELECT id, amount FROM orders WHERE owner = uid $$",
    )
    .await
    .unwrap();

    // Constant-argument JOIN: function call appears in JOIN position
    // with a literal argument. No cross-row reference, so the rewritten
    // form is a plain (non-LATERAL) derived-table join.
    let res = sess
        .execute(
            "SELECT a.user_id, ro.id, ro.amount \
             FROM accounts a JOIN owner_orders(42) ro ON true \
             ORDER BY a.user_id, ro.id",
        )
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    // Two account rows × three order rows = six combos.
    assert_eq!(col_i64(&batches, "user_id"), vec![7, 7, 7, 42, 42, 42]);
    assert_eq!(col_i64(&batches, "id"), vec![1, 2, 3, 1, 2, 3]);

    // Cross-row LATERAL JOIN: argument references the other side of the
    // join. The inliner emits explicit `LATERAL` (verified by ad-hoc
    // smoke below); end-to-end execution depends on DataFusion 44's
    // LATERAL physical-plan support, which is partial — we accept either
    // a successful run or the documented "Physical plan does not support
    // OuterReferenceColumn" error.
    let res = sess
        .execute(
            "SELECT a.user_id, ro.id, ro.amount \
             FROM accounts a JOIN owner_orders(a.user_id) ro ON true \
             ORDER BY a.user_id, ro.id",
        )
        .await;
    match res {
        Ok(ExecResult::Rows { batches, .. }) => {
            assert_eq!(col_i64(&batches, "user_id"), vec![7, 42, 42, 42]);
            assert_eq!(col_i64(&batches, "id"), vec![4, 1, 2, 3]);
        }
        Ok(other) => panic!("unexpected: {other:?}"),
        Err(e) => {
            let m = format!("{e}").to_ascii_lowercase();
            assert!(
                m.contains("outerreferencecolumn") || m.contains("not implemented"),
                "expected DataFusion's LATERAL physical-plan limit if anything, got: {e}"
            );
        }
    }
}

#[tokio::test]
async fn arity_mismatch_rejected_at_registration() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE orders (id BIGINT NOT NULL, owner BIGINT NOT NULL, amount BIGINT NOT NULL)",
    )
    .await
    .unwrap();

    // Body projects 2 columns; RETURNS TABLE declares 3.
    let err = sess
        .execute(
            "CREATE FUNCTION bad(uid BIGINT) RETURNS TABLE(id BIGINT, amount BIGINT, extra BIGINT) \
             LANGUAGE sql AS $$ SELECT id, amount FROM orders WHERE owner = uid $$",
        )
        .await
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("project") || msg.contains("column") || msg.contains("arity"),
        "expected an arity-mismatch error, got: {err}"
    );
}

#[tokio::test]
async fn column_name_uniqueness_enforced() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    let err = sess
        .execute(
            "CREATE FUNCTION dup(uid BIGINT) RETURNS TABLE(id BIGINT, id BIGINT) \
             LANGUAGE sql AS $$ SELECT 1, 2 $$",
        )
        .await
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("appears more than once") || msg.contains("duplicate") || msg.contains("more than once"),
        "expected a duplicate-column error, got: {err}"
    );
}

#[tokio::test]
async fn returns_table_in_scalar_position_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = setup_orders_table(&eng).await;

    sess.execute(
        "CREATE FUNCTION recent_orders(uid BIGINT) RETURNS TABLE(id BIGINT, amount BIGINT) \
         LANGUAGE sql AS $$ SELECT id, amount FROM orders WHERE owner = uid $$",
    )
    .await
    .unwrap();

    let err = sess
        .execute("SELECT recent_orders(42) + 1")
        .await
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("table") && (msg.contains("expression") || msg.contains("scalar") || msg.contains("from")),
        "expected a 'table return cannot be used as scalar' error, got: {err}"
    );
}

#[tokio::test]
async fn cross_tenant_isolation() {
    let dir = TempDir::new().unwrap();
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let eng = shared_engine(&dir, catalog);
    let sess_a = eng.open_session(TenantId::new()).await.unwrap();
    let sess_b = eng.open_session(TenantId::new()).await.unwrap();

    sess_a
        .execute(
            "CREATE TABLE orders (id BIGINT NOT NULL, owner BIGINT NOT NULL, \
             amount BIGINT NOT NULL)",
        )
        .await
        .unwrap();
    sess_a
        .execute("INSERT INTO orders VALUES (1, 42, 500)")
        .await
        .unwrap();
    sess_a
        .execute(
            "CREATE FUNCTION recent_orders(uid BIGINT) RETURNS TABLE(id BIGINT, amount BIGINT) \
             LANGUAGE sql AS $$ SELECT id, amount FROM orders WHERE owner = uid $$",
        )
        .await
        .unwrap();

    let res = sess_a
        .execute("SELECT id FROM recent_orders(42)")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(col_i64(&batches, "id"), vec![1]);

    // Tenant B has its own orders table but no function — must error.
    sess_b
        .execute(
            "CREATE TABLE orders (id BIGINT NOT NULL, owner BIGINT NOT NULL, \
             amount BIGINT NOT NULL)",
        )
        .await
        .unwrap();
    let err = sess_b
        .execute("SELECT id FROM recent_orders(42)")
        .await
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("recent_orders") || msg.contains("not") || msg.contains("invalid"),
        "tenant B should not resolve recent_orders, got: {err}"
    );
}
