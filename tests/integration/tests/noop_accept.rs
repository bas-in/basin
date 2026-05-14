//! Integration tests for the no-op-accept dispatch table.
//!
//! Each test verifies that a statement in the noop-accept set returns `Ok`
//! (not an error) when executed against a real Basin engine. The statements
//! are syntactically accepted and produce an `ExecResult::Empty` (or, for
//! `EXPLAIN`, an `ExecResult::Rows`) without performing any real
//! server-side operation.
//!
//! Real permission enforcement is not tested here — it is covered by the
//! auth / RLS integration tests. See ADRs 0005 and 0013.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

async fn open_session() -> (TempDir, basin_engine::TenantSession) {
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
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    (dir, sess)
}

/// Assert that `sql` executes without error and returns `ExecResult::Empty`.
async fn assert_empty(sess: &basin_engine::TenantSession, sql: &str) {
    match sess.execute(sql).await {
        Ok(ExecResult::Empty { .. }) => {}
        Ok(other) => panic!("expected Empty for [{sql}], got: {other:?}"),
        Err(e) => panic!("expected Ok for [{sql}], got error: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Maintenance / utility
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vacuum_is_accepted() {
    let (_dir, sess) = open_session().await;
    assert_empty(&sess, "VACUUM").await;
}

#[tokio::test]
async fn vacuum_table_is_accepted() {
    let (_dir, sess) = open_session().await;
    sess.execute("CREATE TABLE t (id BIGINT)").await.unwrap();
    assert_empty(&sess, "VACUUM t").await;
}

#[tokio::test]
async fn analyze_is_accepted() {
    let (_dir, sess) = open_session().await;
    assert_empty(&sess, "ANALYZE").await;
}

#[tokio::test]
async fn analyze_table_is_accepted() {
    let (_dir, sess) = open_session().await;
    sess.execute("CREATE TABLE t (id BIGINT)").await.unwrap();
    assert_empty(&sess, "ANALYZE t").await;
}

#[tokio::test]
async fn cluster_is_accepted() {
    let (_dir, sess) = open_session().await;
    sess.execute("CREATE TABLE t (id BIGINT)").await.unwrap();
    assert_empty(&sess, "CLUSTER t").await;
}

#[tokio::test]
async fn lock_table_is_accepted() {
    let (_dir, sess) = open_session().await;
    sess.execute("CREATE TABLE t (id BIGINT)").await.unwrap();
    assert_empty(&sess, "LOCK TABLE t").await;
}

// ---------------------------------------------------------------------------
// Metadata annotation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn comment_on_table_is_accepted() {
    let (_dir, sess) = open_session().await;
    sess.execute("CREATE TABLE t (id BIGINT)").await.unwrap();
    assert_empty(&sess, "COMMENT ON TABLE t IS 'a test table'").await;
}

#[tokio::test]
async fn comment_on_column_is_accepted() {
    let (_dir, sess) = open_session().await;
    sess.execute("CREATE TABLE t (id BIGINT)").await.unwrap();
    assert_empty(&sess, "COMMENT ON COLUMN t.id IS 'primary key'").await;
}

// ---------------------------------------------------------------------------
// EXPLAIN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn explain_select_returns_row() {
    let (_dir, sess) = open_session().await;
    match sess.execute("EXPLAIN SELECT 1").await {
        Ok(ExecResult::Rows { batches, schema }) => {
            assert_eq!(schema.fields().len(), 1);
            assert_eq!(schema.field(0).name(), "QUERY PLAN");
            assert!(!batches.is_empty(), "expected at least one batch");
        }
        Ok(other) => panic!("expected Rows for EXPLAIN, got: {other:?}"),
        Err(e) => panic!("expected Ok for EXPLAIN, got: {e}"),
    }
}

// ---------------------------------------------------------------------------
// RBAC primitives
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_role_is_accepted() {
    let (_dir, sess) = open_session().await;
    assert_empty(&sess, "CREATE ROLE readonly").await;
}

#[tokio::test]
async fn create_role_with_login_is_accepted() {
    let (_dir, sess) = open_session().await;
    assert_empty(&sess, "CREATE ROLE app_user WITH LOGIN").await;
}

#[tokio::test]
async fn drop_role_is_accepted() {
    let (_dir, sess) = open_session().await;
    assert_empty(&sess, "DROP ROLE readonly").await;
}

#[tokio::test]
async fn alter_role_is_accepted() {
    let (_dir, sess) = open_session().await;
    assert_empty(&sess, "ALTER ROLE app_user WITH LOGIN").await;
}

#[tokio::test]
async fn grant_is_accepted() {
    let (_dir, sess) = open_session().await;
    sess.execute("CREATE TABLE t (id BIGINT)").await.unwrap();
    assert_empty(&sess, "GRANT SELECT ON t TO readonly").await;
}

#[tokio::test]
async fn revoke_is_accepted() {
    let (_dir, sess) = open_session().await;
    sess.execute("CREATE TABLE t (id BIGINT)").await.unwrap();
    assert_empty(&sess, "REVOKE SELECT ON t FROM readonly").await;
}

// ---------------------------------------------------------------------------
// Session-variable control
// ---------------------------------------------------------------------------

#[tokio::test]
async fn set_search_path_is_accepted() {
    let (_dir, sess) = open_session().await;
    assert_empty(&sess, "SET search_path = public").await;
}

#[tokio::test]
async fn reset_role_is_accepted() {
    let (_dir, sess) = open_session().await;
    assert_empty(&sess, "RESET ROLE").await;
}

#[tokio::test]
async fn show_search_path_is_accepted() {
    let (_dir, sess) = open_session().await;
    // SHOW <param> returns Empty (tag = "SHOW"); SHOW TABLES is special.
    match sess.execute("SHOW search_path").await {
        Ok(ExecResult::Empty { tag }) => {
            assert_eq!(tag, "SHOW");
        }
        Ok(other) => panic!("expected Empty for SHOW search_path, got: {other:?}"),
        Err(e) => panic!("expected Ok for SHOW search_path, got: {e}"),
    }
}

#[tokio::test]
async fn show_tables_still_works() {
    // SHOW TABLES must NOT be intercepted by noop_accept — it returns the real
    // table list via the existing sqlparser/DataFusion path.
    let (_dir, sess) = open_session().await;
    sess.execute("CREATE TABLE alpha (id BIGINT)").await.unwrap();
    match sess.execute("SHOW TABLES").await {
        Ok(ExecResult::Rows { batches, .. }) => {
            assert!(!batches.is_empty(), "SHOW TABLES should return rows");
        }
        Ok(other) => panic!("expected Rows for SHOW TABLES, got: {other:?}"),
        Err(e) => panic!("expected Ok for SHOW TABLES, got: {e}"),
    }
}
