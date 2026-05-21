//! Integration coverage for the extended `CREATE INDEX` / `DROP INDEX`
//! surface accepted in v0.1 (ADR 0014).
//!
//! None of the indexes listed here are enforced at query time — every table
//! scan is still a full scan. The goal is SQL-surface compatibility so that
//! schema migration scripts that create indexes do not fail.
//!
//! Acceptance criteria (per task #46):
//! 1. `CREATE INDEX idx ON t(id)` — basic
//! 2. `CREATE UNIQUE INDEX idx ON t(id)` — unique flag accepted, not enforced
//! 3. `CREATE INDEX idx ON t(id) WHERE id > 0` — partial index predicate accepted
//! 4. `CREATE INDEX idx ON t(LOWER(name))` — expression index accepted
//! 5. `CREATE INDEX idx ON t USING gin (data)` — alternative method accepted
//! 6. `CREATE INDEX idx ON t (a, b, c)` — multi-column accepted
//! 7. `DROP INDEX idx [IF EXISTS]` — accepted

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn make_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

fn assert_empty(res: ExecResult, tag: &str) {
    match res {
        ExecResult::Empty { tag: t } => assert_eq!(t, tag, "unexpected tag"),
        other => panic!("expected ExecResult::Empty({tag}), got {other:?}"),
    }
}

async fn setup(dir: &TempDir, ddl: &str) -> basin_engine::ProjectSession {
    let engine = make_engine(dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    assert_empty(
        sess.execute(ddl)
            .await
            .unwrap_or_else(|e| panic!("setup failed: {e}")),
        "CREATE TABLE",
    );
    sess
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Basic single-column index
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn basic_single_column_index_accepted() {
    let dir = TempDir::new().unwrap();
    let sess = setup(&dir, "CREATE TABLE t (id INT NOT NULL)").await;

    let res = sess
        .execute("CREATE INDEX idx ON t(id)")
        .await
        .expect("basic CREATE INDEX should be accepted");
    assert_empty(res, "CREATE INDEX");
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. UNIQUE index — accepted without enforcement
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn unique_index_accepted_without_enforcement() {
    let dir = TempDir::new().unwrap();
    let sess = setup(&dir, "CREATE TABLE t (id INT NOT NULL)").await;

    let res = sess
        .execute("CREATE UNIQUE INDEX idx ON t(id)")
        .await
        .expect("CREATE UNIQUE INDEX should be accepted in v0.1");
    assert_empty(res, "CREATE INDEX");
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Partial index (WHERE predicate) — accepted as metadata
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn partial_index_where_predicate_accepted() {
    let dir = TempDir::new().unwrap();
    let sess = setup(&dir, "CREATE TABLE t (id INT NOT NULL)").await;

    let res = sess
        .execute("CREATE INDEX idx ON t(id) WHERE id > 0")
        .await
        .expect("partial index WHERE predicate should be accepted");
    assert_empty(res, "CREATE INDEX");
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Expression index — accepted as metadata
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn expression_index_accepted() {
    let dir = TempDir::new().unwrap();
    let sess = setup(&dir, "CREATE TABLE t (id INT NOT NULL, name TEXT)").await;

    let res = sess
        .execute("CREATE INDEX idx ON t(LOWER(name))")
        .await
        .expect("expression index should be accepted");
    assert_empty(res, "CREATE INDEX");
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. USING gin / gist / hash — accepted, treated as B-tree internally
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn using_gin_accepted() {
    let dir = TempDir::new().unwrap();
    let sess = setup(&dir, "CREATE TABLE t (id INT NOT NULL, name TEXT)").await;

    let res = sess
        .execute("CREATE INDEX idx ON t USING gin (name)")
        .await
        .expect("CREATE INDEX USING gin should be accepted");
    assert_empty(res, "CREATE INDEX");
}

#[tokio::test]
async fn using_hash_accepted() {
    let dir = TempDir::new().unwrap();
    let sess = setup(&dir, "CREATE TABLE t (id INT NOT NULL)").await;

    let res = sess
        .execute("CREATE INDEX idx ON t USING hash (id)")
        .await
        .expect("CREATE INDEX USING hash should be accepted");
    assert_empty(res, "CREATE INDEX");
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. Multi-column index
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn multi_column_index_accepted() {
    let dir = TempDir::new().unwrap();
    let sess = setup(
        &dir,
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, c TEXT)",
    )
    .await;

    let res = sess
        .execute("CREATE INDEX idx ON t (a, b, c)")
        .await
        .expect("multi-column CREATE INDEX should be accepted");
    assert_empty(res, "CREATE INDEX");
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. DROP INDEX and DROP INDEX IF EXISTS
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn drop_index_accepted() {
    let dir = TempDir::new().unwrap();
    let sess = setup(&dir, "CREATE TABLE t (id INT NOT NULL)").await;

    sess.execute("CREATE INDEX idx ON t(id)")
        .await
        .expect("setup index");

    let res = sess
        .execute("DROP INDEX idx")
        .await
        .expect("DROP INDEX should be accepted");
    assert_empty(res, "DROP INDEX");
}

#[tokio::test]
async fn drop_index_if_exists_accepted() {
    let dir = TempDir::new().unwrap();
    let sess = setup(&dir, "CREATE TABLE t (id INT NOT NULL)").await;

    // No index created — IF EXISTS should swallow the missing-index error.
    let res = sess
        .execute("DROP INDEX IF EXISTS nonexistent_idx")
        .await
        .expect("DROP INDEX IF EXISTS should be accepted even when index missing");
    assert_empty(res, "DROP INDEX");
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. INCLUDE covering-columns — accepted as metadata
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn include_covering_columns_accepted() {
    let dir = TempDir::new().unwrap();
    let sess = setup(
        &dir,
        "CREATE TABLE t (id INT NOT NULL, name TEXT, email TEXT)",
    )
    .await;

    let res = sess
        .execute("CREATE INDEX idx ON t (id) INCLUDE (name, email)")
        .await
        .expect("CREATE INDEX … INCLUDE should be accepted");
    assert_empty(res, "CREATE INDEX");
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. CREATE INDEX IF NOT EXISTS — idempotent re-creation
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn create_index_if_not_exists_idempotent() {
    let dir = TempDir::new().unwrap();
    let sess = setup(&dir, "CREATE TABLE t (id INT NOT NULL)").await;

    sess.execute("CREATE INDEX idx ON t (id)")
        .await
        .expect("first CREATE INDEX should succeed");

    // Second creation with IF NOT EXISTS should not error.
    let res = sess
        .execute("CREATE INDEX IF NOT EXISTS idx ON t (id)")
        .await
        .expect("CREATE INDEX IF NOT EXISTS should be idempotent");
    assert_empty(res, "CREATE INDEX");
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge: UNIQUE + partial — loud-rejected (uniqueness not enforced for partial)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn unique_partial_index_combined_rejected() {
    let dir = TempDir::new().unwrap();
    let sess = setup(&dir, "CREATE TABLE t (id INT NOT NULL, active BOOLEAN)").await;

    // A partial UNIQUE index cannot be enforced by Basin's plain-column unique
    // machinery; silently accepting it as metadata would break the uniqueness
    // contract, so it must fail loudly at DDL time.
    let err = sess
        .execute("CREATE UNIQUE INDEX idx ON t(id) WHERE active = true")
        .await
        .expect_err("partial UNIQUE index must be rejected, not silently accepted");
    let msg = err.to_string();
    assert!(
        msg.contains("not yet enforced") && msg.contains("plain-column UNIQUE"),
        "expected loud-reject for partial UNIQUE, got: {msg}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge: multi-column UNIQUE with INCLUDE — loud-rejected (covering cols not enforced)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn multi_column_with_include_rejected() {
    let dir = TempDir::new().unwrap();
    let sess = setup(
        &dir,
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, c TEXT, d TEXT)",
    )
    .await;

    let err = sess
        .execute("CREATE UNIQUE INDEX idx ON t (a, b) INCLUDE (c, d)")
        .await
        .expect_err("UNIQUE index with INCLUDE must be rejected, not silently accepted");
    let msg = err.to_string();
    assert!(
        msg.contains("not yet enforced") && msg.contains("plain-column UNIQUE"),
        "expected loud-reject for UNIQUE … INCLUDE, got: {msg}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge: non-unique INCLUDE / partial indexes remain accepted as metadata
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn non_unique_partial_and_include_still_accepted() {
    let dir = TempDir::new().unwrap();
    let sess = setup(
        &dir,
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, c TEXT)",
    )
    .await;

    // Non-unique partial index: only a performance hint, correctness preserved.
    let res = sess
        .execute("CREATE INDEX p ON t(a) WHERE b > 0")
        .await
        .expect("non-unique partial index should still be accepted");
    assert_empty(res, "CREATE INDEX");

    // Non-unique INCLUDE: covering-column hint, also fine to accept.
    let res = sess
        .execute("CREATE INDEX cov ON t (a) INCLUDE (c)")
        .await
        .expect("non-unique INCLUDE index should still be accepted");
    assert_empty(res, "CREATE INDEX");
}
