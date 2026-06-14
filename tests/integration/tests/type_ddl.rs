//! SQL-level coverage for `CREATE TYPE … AS ENUM`, `ALTER TYPE … ADD VALUE`,
//! `DROP TYPE`, `CREATE DOMAIN … AS <type> [CHECK (…)]`, and `DROP DOMAIN`.
//!
//! Tests exercise the full path through `ProjectSession::execute` so that
//! parser pre-screens, catalog registration, and INSERT-time enforcement
//! are all covered.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
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

async fn open_session(engine: &Engine) -> ProjectSession {
    engine.open_session(ProjectId::new()).await.unwrap()
}

/// Execute SQL and expect success, returning the result.
async fn exec_ok(sess: &ProjectSession, sql: &str) -> ExecResult {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("execute {sql:?}: {e}"))
}

/// Execute SQL and expect an error, returning the error message.
async fn exec_err(sess: &ProjectSession, sql: &str) -> String {
    sess.execute(sql)
        .await
        .expect_err(&format!("expected error for {sql:?}"))
        .to_string()
}

// ==========================================================================
// CREATE TYPE … AS ENUM
// ==========================================================================

#[tokio::test]
async fn create_type_enum_basic() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(
        &sess,
        "CREATE TYPE order_status AS ENUM ('pending', 'paid', 'cancelled')",
    )
    .await;

    // Create a table that uses the enum type.
    exec_ok(
        &sess,
        "CREATE TABLE orders (id BIGINT NOT NULL, status order_status NOT NULL)",
    )
    .await;

    // Insert valid labels.
    exec_ok(&sess, "INSERT INTO orders VALUES (1, 'pending')").await;
    exec_ok(&sess, "INSERT INTO orders VALUES (2, 'paid')").await;
}

#[tokio::test]
async fn create_type_enum_invalid_label_rejected() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE TYPE status AS ENUM ('active', 'inactive')").await;
    exec_ok(
        &sess,
        "CREATE TABLE t (id BIGINT NOT NULL, s status NOT NULL)",
    )
    .await;

    let err = exec_err(&sess, "INSERT INTO t VALUES (1, 'deleted')").await;
    assert!(
        err.contains("22P02")
            || err.contains("invalid_text_representation")
            || err.contains("deleted"),
        "expected label-validation error, got: {err}"
    );
}

/// `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY …` — ORMs create tables first,
/// then wire up FKs in a follow-up ALTER (often in the same migration tx, so a
/// rejection there used to roll back the whole migration). The FK must register
/// in catalog metadata and be enforced on subsequent child INSERTs.
#[tokio::test]
async fn alter_add_foreign_key_registers_and_enforces() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)").await;
    exec_ok(
        &sess,
        "CREATE TABLE child (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT)",
    )
    .await;
    exec_ok(&sess, "INSERT INTO parent VALUES (1)").await;

    // Add the FK after creation (the migration shape that used to be rejected).
    exec_ok(
        &sess,
        "ALTER TABLE child ADD CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES parent (id)",
    )
    .await;

    // Enforced on subsequent writes: a valid reference inserts; a dangling one
    // is rejected (proves the FK was registered, not silently accepted).
    exec_ok(&sess, "INSERT INTO child VALUES (1, 1)").await;
    let err = exec_err(&sess, "INSERT INTO child VALUES (2, 999)").await;
    assert!(
        err.contains("foreign key")
            || err.contains("23503")
            || err.contains("violates")
            || err.contains("999")
            || err.contains("parent"),
        "dangling FK insert must be rejected after ALTER ADD FK, got: {err}"
    );
}

/// `DEFERRABLE INITIALLY DEFERRED` FKs are checked at COMMIT in Postgres, not
/// at the statement, so a transaction may insert a child before its parent —
/// the exact shape Django/Rails migrations emit. Basin accepts the deferred FK
/// without per-row enforcement (documented v0.1 limitation): the child insert
/// that would violate an *immediate* FK must succeed here. This is the case my
/// ALTER-ADD-FK enforcement fix regressed; the `initially_deferred` flag
/// distinguishes it from an immediate FK (which still rejects — see above).
#[tokio::test]
async fn deferred_foreign_key_allows_child_before_parent() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)").await;
    exec_ok(
        &sess,
        "CREATE TABLE child (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT)",
    )
    .await;
    exec_ok(
        &sess,
        "ALTER TABLE child ADD CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) \
         REFERENCES parent (id) DEFERRABLE INITIALLY DEFERRED",
    )
    .await;

    // A reference with no matching parent row would be rejected for an
    // immediate FK; for a deferred FK it is accepted (not enforced per-row).
    exec_ok(&sess, "INSERT INTO child VALUES (1, 999)").await;
}

/// ORMs (Django, et al.) stamp enum values in INSERT as an explicit cast —
/// `'USER'::"Role"` (sqlparser: `CAST('USER' AS "Role")`, sometimes nested via
/// `::TEXT`). The cast-wrapped literal must coerce to the enum (stored as Utf8)
/// exactly as a bare `'USER'` label does, and label validation must still fire.
#[tokio::test]
async fn enum_insert_with_cast_label() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, r#"CREATE TYPE "Role" AS ENUM ('USER', 'ADMIN')"#).await;
    exec_ok(
        &sess,
        r#"CREATE TABLE accounts (id BIGINT NOT NULL, role "Role" NOT NULL)"#,
    )
    .await;

    // The two cast spellings drivers emit.
    exec_ok(&sess, r#"INSERT INTO accounts VALUES (1, 'USER'::"Role")"#).await;
    exec_ok(&sess, r#"INSERT INTO accounts VALUES (2, CAST('ADMIN' AS "Role"))"#).await;

    // Label validation still applies through the cast.
    let err = exec_err(&sess, r#"INSERT INTO accounts VALUES (3, 'GUEST'::"Role")"#).await;
    assert!(
        err.contains("GUEST")
            || err.contains("22P02")
            || err.contains("invalid")
            || err.contains("label"),
        "cast-wrapped invalid enum label must still be rejected, got: {err}"
    );
}

#[tokio::test]
async fn alter_type_add_value_appends_label() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE TYPE mood AS ENUM ('happy', 'sad')").await;
    exec_ok(&sess, "ALTER TYPE mood ADD VALUE 'neutral'").await;

    // Neutral is now a valid label.
    exec_ok(
        &sess,
        "CREATE TABLE t (id BIGINT NOT NULL, m mood NOT NULL)",
    )
    .await;
    exec_ok(&sess, "INSERT INTO t VALUES (1, 'neutral')").await;
}

#[tokio::test]
async fn drop_type_removes_enum() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE TYPE color AS ENUM ('red', 'green', 'blue')").await;
    exec_ok(&sess, "DROP TYPE color").await;

    // After drop, the type name should no longer exist.
    let err_or_ok = sess.execute("DROP TYPE color").await;
    // Either an error (type doesn't exist) or IF EXISTS path would succeed.
    // Without IF EXISTS, it must error.
    assert!(
        err_or_ok.is_err(),
        "DROP TYPE on non-existent type should error (no IF EXISTS)"
    );
}

#[tokio::test]
async fn drop_type_if_exists_is_idempotent() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    // DROP TYPE IF EXISTS on a non-existent type must succeed silently.
    exec_ok(&sess, "DROP TYPE IF EXISTS nonexistent_type").await;

    // And again after creating + dropping.
    exec_ok(&sess, "CREATE TYPE t AS ENUM ('x')").await;
    exec_ok(&sess, "DROP TYPE t").await;
    exec_ok(&sess, "DROP TYPE IF EXISTS t").await;
}

// ==========================================================================
// CREATE DOMAIN / DROP DOMAIN
// ==========================================================================

#[tokio::test]
async fn create_domain_no_check() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE DOMAIN my_text AS TEXT").await;

    // A table using the domain type.
    exec_ok(
        &sess,
        "CREATE TABLE t (id BIGINT NOT NULL, name my_text NOT NULL)",
    )
    .await;
    exec_ok(&sess, "INSERT INTO t VALUES (1, 'hello')").await;
}

#[tokio::test]
async fn create_domain_with_check_enforced_on_insert() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE DOMAIN positive_int AS INT CHECK (VALUE > 0)").await;
    exec_ok(
        &sess,
        "CREATE TABLE t (id BIGINT NOT NULL, n positive_int NOT NULL)",
    )
    .await;

    // Valid insertion.
    exec_ok(&sess, "INSERT INTO t VALUES (1, 5)").await;

    // Value 0 should violate CHECK (VALUE > 0).
    let err = exec_err(&sess, "INSERT INTO t VALUES (2, 0)").await;
    assert!(
        err.contains("23514") || err.contains("check_violation") || err.contains("CHECK"),
        "expected check violation error, got: {err}"
    );
}

#[tokio::test]
async fn drop_domain_if_exists_idempotent() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "DROP DOMAIN IF EXISTS no_such_domain").await;

    exec_ok(&sess, "CREATE DOMAIN d AS TEXT").await;
    exec_ok(&sess, "DROP DOMAIN d").await;
    exec_ok(&sess, "DROP DOMAIN IF EXISTS d").await;
}

// ==========================================================================
// CREATE SEQUENCE + ALTER SEQUENCE + nextval / currval / setval / lastval
// ==========================================================================

#[tokio::test]
async fn create_sequence_ddl_and_nextval() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE SEQUENCE my_seq START 10 INCREMENT 2").await;

    // First nextval returns START.
    let res = sess.execute("SELECT nextval('my_seq')").await.unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        let b = batches.first().unwrap();
        assert_eq!(b.num_rows(), 1);
        let v = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(v, 10, "first nextval should return START=10");

        // Second call returns 12 (10 + increment 2).
        let res2 = sess.execute("SELECT nextval('my_seq')").await.unwrap();
        if let ExecResult::Rows { batches: b2, .. } = res2 {
            let v2 = b2
                .first()
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(0);
            assert_eq!(v2, 12);
        }
    }
}

#[tokio::test]
async fn alter_sequence_restart() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE SEQUENCE s START 1").await;
    // Advance a few times.
    sess.execute("SELECT nextval('s')").await.unwrap();
    sess.execute("SELECT nextval('s')").await.unwrap();

    // RESTART WITH 100: next nextval should return 100.
    exec_ok(&sess, "ALTER SEQUENCE s RESTART WITH 100").await;
    let res = sess.execute("SELECT nextval('s')").await.unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        let v = batches
            .first()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(v, 100, "after RESTART WITH 100, nextval should return 100");
    }
}

#[tokio::test]
async fn lastval_after_nextval() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE SEQUENCE s START 42").await;
    sess.execute("SELECT nextval('s')").await.unwrap();

    let res = sess.execute("SELECT lastval()").await.unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        let v = batches
            .first()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(v, 42, "lastval() should return the last nextval result");
    }
}

#[tokio::test]
async fn lastval_without_nextval_errors() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE SEQUENCE s START 1").await;

    let err = exec_err(&sess, "SELECT lastval()").await;
    assert!(
        err.contains("55000") || err.contains("not yet defined"),
        "expected SQLSTATE 55000-shaped error, got: {err}"
    );
}

#[tokio::test]
async fn drop_sequence_ddl() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(&sess, "CREATE SEQUENCE s").await;
    exec_ok(&sess, "DROP SEQUENCE s").await;

    let err = exec_err(&sess, "SELECT nextval('s')").await;
    assert!(
        err.contains("not found")
            || err.contains("does_not_exist")
            || err.to_lowercase().contains("sequence"),
        "expected sequence-not-found error, got: {err}"
    );
}

// ==========================================================================
// CV DDL — CREATE MATERIALIZED VIEW (basin.continuous)
// ==========================================================================

#[tokio::test]
async fn create_continuous_view_stores_spec() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(
        &sess,
        "CREATE TABLE events (id BIGINT NOT NULL, ts TIMESTAMPTZ NOT NULL)",
    )
    .await;

    // Insert seed rows so the source query returns a result for schema inference.
    exec_ok(
        &sess,
        "INSERT INTO events VALUES (1, '2026-01-01T00:00:00Z'), (2, '2026-01-02T00:00:00Z')",
    )
    .await;

    // Create a continuous view. This exercises the cv_ddl parse + catalog
    // registration path (including the schema-inference step).
    exec_ok(
        &sess,
        "CREATE MATERIALIZED VIEW events_daily \
         WITH (basin.continuous, refresh_interval = '1 hour') AS \
         SELECT cast(date_trunc('day', ts) AS TEXT) AS day, count(*) AS n FROM events GROUP BY day",
    )
    .await;

    // The view must be queryable and reflect the seed data.
    let res = sess
        .execute("SELECT day, n FROM events_daily")
        .await
        .unwrap();
    match res {
        ExecResult::Rows { batches, .. } => {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            // Seed data spans 2 distinct days → 2 row-buckets.
            assert_eq!(
                total_rows, 2,
                "CV should contain 2 day-buckets for seed data"
            );
        }
        other => panic!("expected Rows result, got: {other:?}"),
    }
}

#[tokio::test]
async fn drop_materialized_view() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;

    exec_ok(
        &sess,
        "CREATE TABLE src (id BIGINT NOT NULL, ts TIMESTAMPTZ NOT NULL)",
    )
    .await;
    // Seed a row so the source query returns rows for schema inference.
    exec_ok(&sess, "INSERT INTO src VALUES (1, '2026-01-01T00:00:00Z')").await;
    exec_ok(
        &sess,
        "CREATE MATERIALIZED VIEW mv \
         WITH (basin.continuous, refresh_interval = '1h') AS \
         SELECT count(*) AS n FROM src",
    )
    .await;

    exec_ok(&sess, "DROP MATERIALIZED VIEW mv").await;

    // After drop, querying should fail.
    let err = sess.execute("SELECT n FROM mv").await;
    assert!(err.is_err(), "querying dropped view should fail");
}
