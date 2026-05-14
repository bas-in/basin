//! Integration tests for the bulk catalog-view expansion (agent-bulk-catalog-views).
//!
//! Each test does a bare `SELECT * FROM <view>` and asserts it returns without
//! error. Row counts are not asserted — some views return 0 rows on an empty
//! tenant (locks, triggers, …) and some return ≥1 (settings, stat_activity).

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig};
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

/// Execute `sql` and assert it completes without error. Rows are discarded.
async fn assert_no_error(sess: &basin_engine::TenantSession, sql: &str) {
    match sess.execute(sql).await {
        Ok(_) => {}
        Err(e) => panic!("unexpected error for query [{sql}]: {e}"),
    }
}

// ---------------------------------------------------------------------------
// pg_catalog new views
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_database_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_database").await;
}

#[tokio::test]
async fn pg_roles_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_roles").await;
}

#[tokio::test]
async fn pg_views_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_views").await;
}

#[tokio::test]
async fn pg_indexes_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_indexes").await;
}

#[tokio::test]
async fn pg_tables_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_tables").await;
}

#[tokio::test]
async fn pg_settings_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_settings").await;
}

#[tokio::test]
async fn pg_extension_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_extension").await;
}

#[tokio::test]
async fn pg_description_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_description").await;
}

#[tokio::test]
async fn pg_stat_user_tables_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_stat_user_tables").await;
}

#[tokio::test]
async fn pg_stat_user_indexes_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_stat_user_indexes").await;
}

#[tokio::test]
async fn pg_locks_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_locks").await;
}

#[tokio::test]
async fn pg_stat_activity_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_stat_activity").await;
}

// ---------------------------------------------------------------------------
// information_schema new views
// ---------------------------------------------------------------------------

#[tokio::test]
async fn information_schema_check_constraints_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.check_constraints",
    )
    .await;
}

#[tokio::test]
async fn information_schema_triggers_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM information_schema.triggers").await;
}

#[tokio::test]
async fn information_schema_sequences_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM information_schema.sequences").await;
}

#[tokio::test]
async fn information_schema_domains_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM information_schema.domains").await;
}

#[tokio::test]
async fn information_schema_parameters_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM information_schema.parameters").await;
}

#[tokio::test]
async fn information_schema_role_table_grants_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.role_table_grants",
    )
    .await;
}

#[tokio::test]
async fn information_schema_user_defined_types_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.user_defined_types",
    )
    .await;
}

#[tokio::test]
async fn information_schema_column_domain_usage_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.column_domain_usage",
    )
    .await;
}

#[tokio::test]
async fn information_schema_column_udt_usage_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM information_schema.column_udt_usage").await;
}

// ---------------------------------------------------------------------------
// Sanity: pg_settings has rows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_settings_has_rows() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    match sess
        .execute("SELECT count(*) FROM pg_catalog.pg_settings")
        .await
    {
        Ok(basin_engine::ExecResult::Rows { batches, .. }) => {
            let b = batches.first().expect("no batch");
            let col = b.column(0);
            assert!(col.len() > 0, "pg_settings must have at least one row");
        }
        Ok(other) => panic!("unexpected result: {other:?}"),
        Err(e) => panic!("pg_settings count error: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Sanity: pg_database has exactly one row
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_database_has_one_row() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(TenantId::new()).await.unwrap();
    match sess
        .execute("SELECT count(*) FROM pg_catalog.pg_database")
        .await
    {
        Ok(basin_engine::ExecResult::Rows { batches, .. }) => {
            let b = batches.first().expect("no batch");
            let col = b.column(0);
            assert_eq!(col.len(), 1, "pg_database must have exactly one row");
        }
        Ok(other) => panic!("unexpected result: {other:?}"),
        Err(e) => panic!("pg_database count error: {e}"),
    }
}
