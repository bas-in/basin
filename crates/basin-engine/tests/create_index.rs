//! `CREATE INDEX` / `DROP INDEX` surface tests.
//!
//! v0.1's index machinery is metadata-only: the catalog records the
//! declaration, but every query still does a table scan. These tests
//! check that the DDL is *accepted syntactically* and that introspection
//! through `Catalog::load_table().indexes` is honest — the load_test that
//! exposes the basin-auth dropped indexes use case.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, TableName, ProjectId};
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

async fn open(eng: &Engine) -> ProjectSession {
    eng.open_session(ProjectId::new()).await.unwrap()
}

fn assert_empty(res: ExecResult, expected_tag: &str) {
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, expected_tag, "wrong tag"),
        other => panic!("expected ExecResult::Empty, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// CREATE INDEX accepted
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_single_column_index_accepted() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    assert_empty(
        sess.execute("CREATE INDEX users_email_idx ON users (email)")
            .await
            .unwrap(),
        "CREATE INDEX",
    );

    let project = sess.project();
    let meta = eng
        .config()
        .catalog
        .load_table(&project, &TableName::new("users").unwrap())
        .await
        .unwrap();
    assert_eq!(meta.indexes.len(), 1);
    assert_eq!(meta.indexes[0].name, "users_email_idx");
    assert_eq!(meta.indexes[0].columns, vec!["email".to_string()]);
}

#[tokio::test]
async fn create_composite_index_accepted() {
    // basin-auth's `users_project_email ON basin_auth_users (project_id, email)`
    // shape — the use case driving this whole feature.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute(
        "CREATE TABLE basin_auth_users (\
             id BIGINT PRIMARY KEY, \
             project_id BIGINT NOT NULL, \
             email TEXT NOT NULL)",
    )
    .await
    .unwrap();

    sess.execute("CREATE INDEX users_project_email ON basin_auth_users (project_id, email)")
        .await
        .unwrap();

    let project = sess.project();
    let meta = eng
        .config()
        .catalog
        .load_table(&project, &TableName::new("basin_auth_users").unwrap())
        .await
        .unwrap();
    assert_eq!(meta.indexes.len(), 1);
    assert_eq!(meta.indexes[0].name, "users_project_email");
    assert_eq!(
        meta.indexes[0].columns,
        vec!["project_id".to_string(), "email".to_string()]
    );
}

#[tokio::test]
async fn create_index_anonymous_name_synthesized() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE INDEX ON t (name)").await.unwrap();

    let project = sess.project();
    let meta = eng
        .config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
    assert_eq!(meta.indexes.len(), 1);
    assert_eq!(meta.indexes[0].name, "t_name_idx");
}

// ---------------------------------------------------------------------------
// IF NOT EXISTS is a no-op on a second create
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_index_if_not_exists_is_noop_on_duplicate() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE INDEX ix_email ON t (email)")
        .await
        .unwrap();
    sess.execute("CREATE INDEX IF NOT EXISTS ix_email ON t (email)")
        .await
        .unwrap();

    let project = sess.project();
    let meta = eng
        .config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
    assert_eq!(
        meta.indexes.len(),
        1,
        "duplicate IF NOT EXISTS must be no-op"
    );
}

#[tokio::test]
async fn create_index_without_if_not_exists_rejects_duplicate() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE INDEX ix_email ON t (email)")
        .await
        .unwrap();
    let err = sess
        .execute("CREATE INDEX ix_email ON t (email)")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::Catalog(_)), "got {err:?}");
}

// ---------------------------------------------------------------------------
// Rejection: missing table / column
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_index_on_missing_table_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    let err = sess
        .execute("CREATE INDEX ix ON ghost_table (email)")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::InvalidSchema(_)), "got {err:?}");
    assert!(err.to_string().contains("does not exist"), "msg: {err}");
}

#[tokio::test]
async fn create_index_on_missing_column_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    let err = sess
        .execute("CREATE INDEX ix_ghost ON t (ghost_col)")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::InvalidSchema(_)), "got {err:?}");
}

// ---------------------------------------------------------------------------
// DROP INDEX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drop_index_removes_catalog_row() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE INDEX ix_email ON t (email)")
        .await
        .unwrap();
    sess.execute("DROP INDEX ix_email").await.unwrap();

    let project = sess.project();
    let meta = eng
        .config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
    assert!(meta.indexes.is_empty(), "DROP INDEX must clear the row");
}

#[tokio::test]
async fn drop_index_if_exists_swallows_missing() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    sess.execute("DROP INDEX IF EXISTS ghost").await.unwrap();
}

#[tokio::test]
async fn drop_index_missing_without_if_exists_errors() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    let err = sess.execute("DROP INDEX ghost").await.unwrap_err();
    assert!(matches!(err, BasinError::NotFound(_)), "got {err:?}");
}
