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

// ---------------------------------------------------------------------------
// CREATE UNIQUE INDEX enforcement (BUG #136)
//
// A plain-column `CREATE UNIQUE INDEX` is semantically identical to an inline
// `UNIQUE` constraint in PostgreSQL and MUST reject duplicate keys with
// SQLSTATE 23505. Previously basin accepted it as metadata-only, silently
// letting duplicate rows accumulate.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_unique_index_rejects_duplicate_insert() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE UNIQUE INDEX users_email_uidx ON users (email)")
        .await
        .unwrap();

    sess.execute("INSERT INTO users (id, email) VALUES (1, 'ada@example.com')")
        .await
        .unwrap();

    let err = sess
        .execute("INSERT INTO users (id, email) VALUES (2, 'ada@example.com')")
        .await
        .unwrap_err();
    // PG: ERROR 23505 duplicate key value violates unique constraint.
    assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");
    let msg = err.to_string();
    assert!(
        msg.contains("users_email_uidx") && msg.contains("duplicate key"),
        "msg = {msg}"
    );
}

#[tokio::test]
async fn create_unique_index_distinct_values_succeed() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE UNIQUE INDEX users_email_uidx ON users (email)")
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

#[tokio::test]
async fn create_unique_index_update_to_existing_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE UNIQUE INDEX users_email_uidx ON users (email)")
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
async fn create_unique_index_multi_column_enforces_on_tuple() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute(
        "CREATE TABLE memberships (\
             id BIGINT PRIMARY KEY, \
             org_id BIGINT NOT NULL, \
             user_id BIGINT NOT NULL)",
    )
    .await
    .unwrap();
    sess.execute(
        "CREATE UNIQUE INDEX memberships_org_user_uidx \
         ON memberships (org_id, user_id)",
    )
    .await
    .unwrap();

    sess.execute("INSERT INTO memberships VALUES (1, 10, 100)")
        .await
        .unwrap();
    // Same org, different user — OK.
    sess.execute("INSERT INTO memberships VALUES (2, 10, 200)")
        .await
        .unwrap();
    // Same user, different org — OK.
    sess.execute("INSERT INTO memberships VALUES (3, 20, 100)")
        .await
        .unwrap();
    // Duplicate (org_id, user_id) tuple — must reject.
    let err = sess
        .execute("INSERT INTO memberships VALUES (4, 10, 100)")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");
    let msg = err.to_string();
    assert!(
        msg.contains("memberships_org_user_uidx"),
        "msg = {msg}"
    );
}

#[tokio::test]
async fn create_unique_index_intra_batch_duplicate_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE UNIQUE INDEX users_email_uidx ON users (email)")
        .await
        .unwrap();
    let err = sess
        .execute("INSERT INTO users VALUES (1, 'a@x.com'), (2, 'a@x.com')")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");
}

#[tokio::test]
async fn drop_index_removes_unique_enforcement() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE UNIQUE INDEX users_email_uidx ON users (email)")
        .await
        .unwrap();
    sess.execute("INSERT INTO users VALUES (1, 'a@x.com')")
        .await
        .unwrap();
    // Enforcement is live: duplicate rejected.
    sess.execute("INSERT INTO users VALUES (2, 'a@x.com')")
        .await
        .unwrap_err();

    // Drop the index — uniqueness enforcement must go away (PG parity).
    sess.execute("DROP INDEX users_email_uidx").await.unwrap();

    // The previously-rejected duplicate now succeeds.
    sess.execute("INSERT INTO users VALUES (3, 'a@x.com')")
        .await
        .unwrap();
}

#[tokio::test]
async fn non_unique_index_does_not_enforce() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    // Plain (non-UNIQUE) index: duplicates are allowed.
    sess.execute("CREATE INDEX users_email_idx ON users (email)")
        .await
        .unwrap();
    sess.execute("INSERT INTO users VALUES (1, 'a@x.com')")
        .await
        .unwrap();
    sess.execute("INSERT INTO users VALUES (2, 'a@x.com')")
        .await
        .unwrap();
}

#[tokio::test]
async fn create_unique_index_if_not_exists_idempotent() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE UNIQUE INDEX users_email_uidx ON users (email)")
        .await
        .unwrap();
    // Re-run under IF NOT EXISTS — must not double-register the constraint
    // and must still enforce.
    sess.execute("CREATE UNIQUE INDEX IF NOT EXISTS users_email_uidx ON users (email)")
        .await
        .unwrap();

    sess.execute("INSERT INTO users VALUES (1, 'a@x.com')")
        .await
        .unwrap();
    let err = sess
        .execute("INSERT INTO users VALUES (2, 'a@x.com')")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");
}
