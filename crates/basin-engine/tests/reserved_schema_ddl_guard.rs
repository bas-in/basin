//! 6.SEC.P1 — user DDL must not be able to create / drop / alter objects
//! inside reserved system schemas (`auth`, `storage`, `cron`, `net`,
//! `realtime`, `pg_catalog`, `information_schema`). The reserved-schema
//! membership is reportable (5.18.D / ADR 0022) but until this guard
//! landed, a regular session could `CREATE TABLE auth.users` and shadow
//! basin-auth's provisioning surface (audit P2-5).
//!
//! The internal subsystems (basin-auth, basin-blob, basin-cron,
//! basin-net, basin-realtime) write their tables sqlx-direct against the
//! Postgres backing store with prefixed names (e.g. `basin_auth_users`),
//! *not* through this engine's SQL parser — so blocking the SQL parser
//! path does not regress them. Their existing integration tests are the
//! end-to-end coverage of that property; this file pins the parser-side
//! guard.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, ProjectId};
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

fn assert_permission_denied(err: BasinError, schema_hint: &str) {
    match err {
        BasinError::PermissionDenied(msg) => {
            assert!(
                msg.contains(&format!("\"{schema_hint}\"")),
                "expected message to mention reserved schema {schema_hint:?}, got: {msg}"
            );
        }
        other => panic!("expected PermissionDenied, got {other:?}"),
    }
}

// ── CREATE TABLE in each reserved schema is rejected ────────────────────────

#[tokio::test]
async fn create_table_in_auth_schema_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    let err = sess
        .execute("CREATE TABLE auth.users (id BIGINT)")
        .await
        .expect_err("CREATE TABLE auth.users must be rejected");
    assert_permission_denied(err, "auth");
}

#[tokio::test]
async fn create_table_in_storage_schema_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    let err = sess
        .execute("CREATE TABLE storage.objects (id BIGINT)")
        .await
        .expect_err("CREATE TABLE storage.objects must be rejected");
    assert_permission_denied(err, "storage");
}

#[tokio::test]
async fn create_table_in_pg_catalog_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    let err = sess
        .execute("CREATE TABLE pg_catalog.evil (id BIGINT)")
        .await
        .expect_err("CREATE TABLE pg_catalog.evil must be rejected");
    assert_permission_denied(err, "pg_catalog");
}

#[tokio::test]
async fn create_table_in_information_schema_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    let err = sess
        .execute("CREATE TABLE information_schema.evil (id BIGINT)")
        .await
        .expect_err("CREATE TABLE information_schema.evil must be rejected");
    assert_permission_denied(err, "information_schema");
}

#[tokio::test]
async fn create_table_in_each_reserved_schema_rejected() {
    // The compact loop — every reserved-non-public schema must reject.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    for schema in ["auth", "storage", "cron", "net", "realtime"] {
        let sql = format!("CREATE TABLE {schema}.foo (id BIGINT)");
        let err = sess
            .execute(&sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("expected reject for `{sql}`"));
        assert_permission_denied(err, schema);
    }
}

// ── DROP TABLE on each reserved schema is rejected ──────────────────────────

#[tokio::test]
async fn drop_table_in_storage_schema_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    let err = sess
        .execute("DROP TABLE storage.objects")
        .await
        .expect_err("DROP TABLE storage.objects must be rejected");
    assert_permission_denied(err, "storage");
}

#[tokio::test]
async fn drop_table_in_auth_schema_rejected_if_exists() {
    // Even with IF EXISTS — IF EXISTS only swallows NotFound from the
    // catalog, NOT the privilege check. PG behaves the same way.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    let err = sess
        .execute("DROP TABLE IF EXISTS auth.users")
        .await
        .expect_err("DROP TABLE IF EXISTS auth.users must still be rejected");
    assert_permission_denied(err, "auth");
}

// ── ALTER TABLE in a reserved schema is rejected ────────────────────────────

#[tokio::test]
async fn alter_table_in_auth_schema_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    let err = sess
        .execute("ALTER TABLE auth.users ADD COLUMN evil TEXT")
        .await
        .expect_err("ALTER TABLE auth.users must be rejected");
    assert_permission_denied(err, "auth");
}

// ── CREATE INDEX on a reserved-schema table is rejected ─────────────────────

#[tokio::test]
async fn create_index_on_reserved_schema_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    let err = sess
        .execute("CREATE INDEX nope ON auth.users (id)")
        .await
        .expect_err("CREATE INDEX on auth.users must be rejected");
    assert_permission_denied(err, "auth");
}

#[tokio::test]
async fn create_index_with_reserved_schema_index_name_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    sess.execute("CREATE TABLE t (id BIGINT)").await.unwrap();

    let err = sess
        .execute("CREATE INDEX auth.evil_idx ON t (id)")
        .await
        .expect_err("CREATE INDEX with auth.evil_idx as name must be rejected");
    assert_permission_denied(err, "auth");
}

// ── public is NOT reserved — back-compat default still works ────────────────

#[tokio::test]
async fn create_table_in_public_succeeds() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    let res = sess
        .execute("CREATE TABLE public.foo (id BIGINT)")
        .await
        .expect("CREATE TABLE public.foo must succeed");
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "CREATE TABLE"),
        other => panic!("expected ExecResult::Empty, got {other:?}"),
    }
}

#[tokio::test]
async fn create_table_unqualified_still_succeeds() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    let res = sess
        .execute("CREATE TABLE foo (id BIGINT)")
        .await
        .expect("bare CREATE TABLE must succeed");
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "CREATE TABLE"),
        other => panic!("expected ExecResult::Empty, got {other:?}"),
    }
}

#[tokio::test]
async fn drop_table_public_round_trip_succeeds() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    sess.execute("CREATE TABLE public.foo (id BIGINT)")
        .await
        .unwrap();
    sess.execute("DROP TABLE public.foo")
        .await
        .expect("DROP TABLE public.foo must succeed");
}

// ── A user-defined schema is not reserved and still routes through ──────────

#[tokio::test]
async fn create_table_in_user_schema_succeeds() {
    // ADR 0022 — user schemas alias to public; the qualifier must be
    // CREATE'd first.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    sess.execute("CREATE SCHEMA myapp").await.unwrap();

    let res = sess
        .execute("CREATE TABLE myapp.t (id BIGINT)")
        .await
        .expect("CREATE TABLE myapp.t must succeed");
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "CREATE TABLE"),
        other => panic!("expected ExecResult::Empty, got {other:?}"),
    }
}
