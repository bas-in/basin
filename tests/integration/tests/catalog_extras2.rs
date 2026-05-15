//! Integration smoke tests for the bulk catalog-extras expansion.
//!
//! Covers the new views added by agent-bulk-catalog-extras:
//! - pg_catalog: pg_database, pg_roles, pg_views, pg_indexes, pg_tables,
//!   pg_settings, pg_extension, pg_description, pg_stat_user_tables,
//!   pg_stat_user_indexes, pg_locks, pg_stat_activity, pg_stat_database,
//!   pg_stat_bgwriter, pg_stat_replication, pg_stat_archiver,
//!   pg_stat_wal_receiver, pg_stat_subscription, pg_stat_user_functions,
//!   pg_stat_progress_vacuum, pg_stat_progress_create_index,
//!   pg_stat_progress_analyze.
//! - information_schema: check_constraints, triggers, usage_privileges,
//!   table_privileges, column_privileges, role_column_grants,
//!   role_routine_grants, applicable_roles, enabled_roles,
//!   foreign_data_wrappers, foreign_data_wrapper_options, foreign_servers,
//!   foreign_server_options, foreign_tables, foreign_table_options,
//!   user_mappings, user_mapping_options.
//!
//! Each test does a bare `SELECT * FROM <view> LIMIT 1` and asserts it
//! completes without error. Row counts are not asserted.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
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
async fn assert_no_error(sess: &basin_engine::ProjectSession, sql: &str) {
    match sess.execute(sql).await {
        Ok(_) => {}
        Err(e) => panic!("unexpected error for query [{sql}]: {e}"),
    }
}

// ---------------------------------------------------------------------------
// pg_catalog views
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_database_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_database LIMIT 1").await;
}

#[tokio::test]
async fn pg_roles_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_roles LIMIT 1").await;
}

#[tokio::test]
async fn pg_views_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_views LIMIT 1").await;
}

#[tokio::test]
async fn pg_indexes_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_indexes LIMIT 1").await;
}

#[tokio::test]
async fn pg_tables_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_tables LIMIT 1").await;
}

#[tokio::test]
async fn pg_settings_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_settings LIMIT 1").await;
}

#[tokio::test]
async fn pg_extension_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_extension LIMIT 1").await;
}

#[tokio::test]
async fn pg_description_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_description LIMIT 1").await;
}

#[tokio::test]
async fn pg_stat_user_tables_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_stat_user_tables LIMIT 1").await;
}

#[tokio::test]
async fn pg_stat_user_indexes_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_stat_user_indexes LIMIT 1").await;
}

#[tokio::test]
async fn pg_locks_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_locks LIMIT 1").await;
}

#[tokio::test]
async fn pg_stat_activity_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_stat_activity LIMIT 1").await;
}

#[tokio::test]
async fn pg_stat_database_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_stat_database LIMIT 1").await;
}

#[tokio::test]
async fn pg_stat_bgwriter_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_stat_bgwriter LIMIT 1").await;
}

#[tokio::test]
async fn pg_stat_replication_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_stat_replication LIMIT 1").await;
}

#[tokio::test]
async fn pg_stat_archiver_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_stat_archiver LIMIT 1").await;
}

#[tokio::test]
async fn pg_stat_wal_receiver_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_stat_wal_receiver LIMIT 1").await;
}

#[tokio::test]
async fn pg_stat_subscription_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_stat_subscription LIMIT 1").await;
}

#[tokio::test]
async fn pg_stat_user_functions_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_stat_user_functions LIMIT 1").await;
}

#[tokio::test]
async fn pg_stat_progress_vacuum_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_stat_progress_vacuum LIMIT 1").await;
}

#[tokio::test]
async fn pg_stat_progress_create_index_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM pg_catalog.pg_stat_progress_create_index LIMIT 1",
    )
    .await;
}

#[tokio::test]
async fn pg_stat_progress_analyze_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM pg_catalog.pg_stat_progress_analyze LIMIT 1").await;
}

// ---------------------------------------------------------------------------
// information_schema privilege views
// ---------------------------------------------------------------------------

#[tokio::test]
async fn check_constraints_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.check_constraints LIMIT 1",
    )
    .await;
}

#[tokio::test]
async fn triggers_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(&sess, "SELECT * FROM information_schema.triggers LIMIT 1").await;
}

#[tokio::test]
async fn usage_privileges_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.usage_privileges LIMIT 1",
    )
    .await;
}

#[tokio::test]
async fn table_privileges_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.table_privileges LIMIT 1",
    )
    .await;
}

#[tokio::test]
async fn column_privileges_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.column_privileges LIMIT 1",
    )
    .await;
}

#[tokio::test]
async fn role_column_grants_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.role_column_grants LIMIT 1",
    )
    .await;
}

#[tokio::test]
async fn role_routine_grants_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.role_routine_grants LIMIT 1",
    )
    .await;
}

#[tokio::test]
async fn applicable_roles_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.applicable_roles LIMIT 1",
    )
    .await;
}

#[tokio::test]
async fn enabled_roles_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.enabled_roles LIMIT 1",
    )
    .await;
}

// ---------------------------------------------------------------------------
// information_schema FDW stubs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn foreign_data_wrappers_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.foreign_data_wrappers LIMIT 1",
    )
    .await;
}

#[tokio::test]
async fn foreign_data_wrapper_options_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.foreign_data_wrapper_options LIMIT 1",
    )
    .await;
}

#[tokio::test]
async fn foreign_servers_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.foreign_servers LIMIT 1",
    )
    .await;
}

#[tokio::test]
async fn foreign_server_options_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.foreign_server_options LIMIT 1",
    )
    .await;
}

#[tokio::test]
async fn foreign_tables_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.foreign_tables LIMIT 1",
    )
    .await;
}

#[tokio::test]
async fn foreign_table_options_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.foreign_table_options LIMIT 1",
    )
    .await;
}

#[tokio::test]
async fn user_mappings_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.user_mappings LIMIT 1",
    )
    .await;
}

#[tokio::test]
async fn user_mapping_options_selectable() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_no_error(
        &sess,
        "SELECT * FROM information_schema.user_mapping_options LIMIT 1",
    )
    .await;
}
