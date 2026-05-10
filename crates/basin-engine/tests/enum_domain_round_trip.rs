//! Integration test for the `CREATE/ALTER/DROP TYPE … AS ENUM` and
//! `CREATE/DROP DOMAIN` SQL surface.
//!
//! Round-trips enum / domain DDL end-to-end through the engine's SQL
//! entry point. The catalog API (`Catalog::register_enum_type` etc.)
//! is exercised by `crates/basin-catalog/tests/enums_and_domains.rs`;
//! this file proves the SQL surface routes correctly to the same
//! catalog calls and that subsequent INSERTs honour enum-label and
//! domain-CHECK validation.

use std::sync::Arc;

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

#[tokio::test]
async fn create_type_enum_then_use_in_table() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    let res = sess
        .execute("CREATE TYPE order_status AS ENUM ('pending', 'paid', 'shipped')")
        .await
        .unwrap();
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "CREATE TYPE"),
        other => panic!("unexpected: {other:?}"),
    }

    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL, status order_status NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO orders VALUES (1, 'pending'), (2, 'paid')")
        .await
        .unwrap();

    let res = sess
        .execute("SELECT id, status FROM orders ORDER BY id")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[tokio::test]
async fn insert_unknown_enum_label_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TYPE color AS ENUM ('red', 'green', 'blue')")
        .await
        .unwrap();
    sess.execute("CREATE TABLE t (c color NOT NULL)")
        .await
        .unwrap();

    let err = sess
        .execute("INSERT INTO t VALUES ('purple')")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("22P02") || msg.to_ascii_lowercase().contains("invalid_text_representation"),
        "expected SQLSTATE 22P02, got: {err}"
    );
    assert!(
        msg.contains("'purple'") || msg.contains("\"purple\"") || msg.contains("purple"),
        "error should reference the offending label, got: {err}"
    );
}

#[tokio::test]
async fn alter_type_add_value() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TYPE status AS ENUM ('a', 'b')")
        .await
        .unwrap();
    sess.execute("CREATE TABLE t (s status NOT NULL)")
        .await
        .unwrap();

    // Before ADD VALUE, 'c' must be rejected.
    let err = sess
        .execute("INSERT INTO t VALUES ('c')")
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("22P02"), "got: {err}");

    let res = sess
        .execute("ALTER TYPE status ADD VALUE 'c'")
        .await
        .unwrap();
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "ALTER TYPE"),
        other => panic!("unexpected: {other:?}"),
    }

    // After ADD VALUE, 'c' must succeed.
    sess.execute("INSERT INTO t VALUES ('c')").await.unwrap();
}

#[tokio::test]
async fn add_existing_enum_value_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TYPE k AS ENUM ('a', 'b')").await.unwrap();
    let err = sess
        .execute("ALTER TYPE k ADD VALUE 'a'")
        .await
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(msg.contains("already") || msg.contains("contains"), "got: {err}");
}

#[tokio::test]
async fn drop_type_with_dependent_column_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TYPE used AS ENUM ('a', 'b')")
        .await
        .unwrap();
    sess.execute("CREATE TABLE t (s used NOT NULL)")
        .await
        .unwrap();

    let err = sess.execute("DROP TYPE used").await.unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("referenced") || msg.contains("cascade") || msg.contains("drop the column"),
        "expected refcount error, got: {err}"
    );
}

#[tokio::test]
async fn drop_type_if_exists_idempotent() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    // Idempotent: dropping a non-existent type with IF EXISTS is Ok.
    sess.execute("DROP TYPE IF EXISTS missing")
        .await
        .unwrap();
    // Without IF EXISTS, the same DROP errors.
    let err = sess.execute("DROP TYPE missing").await.unwrap_err();
    assert!(matches!(err, basin_common::BasinError::NotFound(_)), "got: {err}");
}

#[tokio::test]
async fn create_domain_then_use_in_table() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    let res = sess
        .execute("CREATE DOMAIN positive_int AS INT CHECK (VALUE > 0)")
        .await
        .unwrap();
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "CREATE DOMAIN"),
        other => panic!("unexpected: {other:?}"),
    }

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, q positive_int)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 5), (2, 100)")
        .await
        .unwrap();
}

#[tokio::test]
async fn domain_check_enforced_on_insert() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE DOMAIN positive_int AS INT CHECK (VALUE > 0)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, q positive_int)")
        .await
        .unwrap();

    // Valid row succeeds.
    sess.execute("INSERT INTO t VALUES (1, 5)").await.unwrap();

    // Violating row surfaces SQLSTATE 23514.
    let err = sess
        .execute("INSERT INTO t VALUES (2, -1)")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("23514") || msg.to_ascii_lowercase().contains("check_violation"),
        "expected SQLSTATE 23514, got: {err}"
    );
}

#[tokio::test]
async fn drop_domain_with_dependent_column_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE DOMAIN d AS INT CHECK (VALUE > 0)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE t (q d)").await.unwrap();
    let err = sess.execute("DROP DOMAIN d").await.unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("referenced") || msg.contains("drop the column"),
        "expected refcount error, got: {err}"
    );
}

#[tokio::test]
async fn drop_domain_if_exists_idempotent() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("DROP DOMAIN IF EXISTS missing").await.unwrap();
    let err = sess.execute("DROP DOMAIN missing").await.unwrap_err();
    assert!(matches!(err, basin_common::BasinError::NotFound(_)), "got: {err}");
}

#[tokio::test]
async fn cross_tenant_type_isolation() {
    let dir = TempDir::new().unwrap();
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let eng = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let sess_a = eng.open_session(TenantId::new()).await.unwrap();
    let sess_b = eng.open_session(TenantId::new()).await.unwrap();

    sess_a
        .execute("CREATE TYPE k AS ENUM ('a', 'b')")
        .await
        .unwrap();

    // Tenant B cannot see tenant A's type — using `k` in a column type
    // should surface as an "unsupported custom type" error from the
    // standard arrow_data_type fallback.
    let err = sess_b
        .execute("CREATE TABLE t (c k)")
        .await
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("unsupported") || msg.contains("custom type") || msg.contains("invalid"),
        "tenant B should not see tenant A's enum type, got: {err}"
    );
}
