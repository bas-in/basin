//! Table-level and column-level `UNIQUE` constraint enforcement.
//!
//! v0.1 enforces via a full-table scan on every INSERT / UPDATE — same
//! cost shape as PRIMARY KEY. These tests verify the SQLSTATE 23505
//! shape, intra-batch dedup, UPDATE conflict detection, and PG-shaped
//! NULL handling (multiple NULLs allowed by default).

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ProjectSession};
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

// ---------------------------------------------------------------------------
// Column-level UNIQUE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn column_unique_first_insert_succeeds_second_rejects() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT UNIQUE)")
        .await
        .unwrap();
    sess.execute("INSERT INTO users (id, email) VALUES (1, 'ada@example.com')")
        .await
        .unwrap();

    let err = sess
        .execute("INSERT INTO users (id, email) VALUES (2, 'ada@example.com')")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");
    let msg = err.to_string();
    assert!(msg.contains("users_email_key"), "msg = {msg}");
}

#[tokio::test]
async fn column_unique_distinct_values_succeed() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT UNIQUE)")
        .await
        .unwrap();
    sess.execute("INSERT INTO users (id, email) VALUES (1, 'a@x.com')")
        .await
        .unwrap();
    sess.execute("INSERT INTO users (id, email) VALUES (2, 'b@x.com')")
        .await
        .unwrap();
    sess.execute("INSERT INTO users (id, email) VALUES (3, 'c@x.com')")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Table-level UNIQUE (the basin-auth use case)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn composite_unique_distinct_tuples_succeed() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute(
        "CREATE TABLE users (\
             id BIGINT PRIMARY KEY, \
             project_id BIGINT NOT NULL, \
             email TEXT NOT NULL, \
             UNIQUE (project_id, email))",
    )
    .await
    .unwrap();

    sess.execute("INSERT INTO users VALUES (1, 1, 'a@x.com')")
        .await
        .unwrap();
    // Same email, different project — fine.
    sess.execute("INSERT INTO users VALUES (2, 2, 'a@x.com')")
        .await
        .unwrap();
    // Same project, different email — fine.
    sess.execute("INSERT INTO users VALUES (3, 1, 'b@x.com')")
        .await
        .unwrap();
}

#[tokio::test]
async fn composite_unique_duplicate_tuple_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute(
        "CREATE TABLE users (\
             id BIGINT PRIMARY KEY, \
             project_id BIGINT NOT NULL, \
             email TEXT NOT NULL, \
             UNIQUE (project_id, email))",
    )
    .await
    .unwrap();

    sess.execute("INSERT INTO users VALUES (1, 1, 'a@x.com')")
        .await
        .unwrap();
    let err = sess
        .execute("INSERT INTO users VALUES (2, 1, 'a@x.com')")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");
    let msg = err.to_string();
    assert!(msg.contains("users_project_id_email_key"), "msg = {msg}");
}

#[tokio::test]
async fn named_unique_constraint_uses_user_name() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute(
        "CREATE TABLE users (\
             id BIGINT PRIMARY KEY, \
             project_id BIGINT NOT NULL, \
             email TEXT NOT NULL, \
             CONSTRAINT users_project_email_uk UNIQUE (project_id, email))",
    )
    .await
    .unwrap();

    sess.execute("INSERT INTO users VALUES (1, 1, 'a@x.com')")
        .await
        .unwrap();
    let err = sess
        .execute("INSERT INTO users VALUES (2, 1, 'a@x.com')")
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("users_project_email_uk"),
        "user-supplied constraint name must surface in error, got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn update_to_existing_value_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT UNIQUE)")
        .await
        .unwrap();
    sess.execute("INSERT INTO users VALUES (1, 'a@x.com')")
        .await
        .unwrap();
    sess.execute("INSERT INTO users VALUES (2, 'b@x.com')")
        .await
        .unwrap();

    let err = sess
        .execute("UPDATE users SET email = 'a@x.com' WHERE id = 2")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");
}

#[tokio::test]
async fn update_to_distinct_value_succeeds() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT UNIQUE)")
        .await
        .unwrap();
    sess.execute("INSERT INTO users VALUES (1, 'a@x.com')")
        .await
        .unwrap();
    sess.execute("UPDATE users SET email = 'newmail@x.com' WHERE id = 1")
        .await
        .unwrap();
}

#[tokio::test]
async fn delete_then_reinsert_succeeds() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT UNIQUE)")
        .await
        .unwrap();
    sess.execute("INSERT INTO users VALUES (1, 'a@x.com')")
        .await
        .unwrap();
    sess.execute("DELETE FROM users WHERE id = 1")
        .await
        .unwrap();
    // The slot is free now — reinsert must succeed.
    sess.execute("INSERT INTO users VALUES (2, 'a@x.com')")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Intra-batch duplicate detection (single statement)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn intra_batch_duplicate_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT UNIQUE)")
        .await
        .unwrap();
    let err = sess
        .execute("INSERT INTO users VALUES (1, 'a@x.com'), (2, 'a@x.com')")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");
}

// ---------------------------------------------------------------------------
// NULL handling: PG default allows multiple NULLs in a UNIQUE column.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multiple_nulls_allowed_in_unique_column() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT UNIQUE)")
        .await
        .unwrap();
    sess.execute("INSERT INTO users (id, email) VALUES (1, NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO users (id, email) VALUES (2, NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO users (id, email) VALUES (3, NULL)")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Composite-UNIQUE invalid-column rejection at CREATE TABLE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unique_referencing_missing_column_rejected_at_create_table() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    let err = sess
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT, UNIQUE (name, ghost))")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::InvalidSchema(_)), "got {err:?}");
}
