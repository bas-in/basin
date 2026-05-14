//! Integration tests for the noop-accept dispatch layer (ADR 0014 Phase 1).
//!
//! Each test sends a statement in the noop-accept set through the full engine
//! path (`TenantSession::execute`) and asserts that:
//!
//! 1. No error is returned (the statement is accepted, not rejected with 0A000).
//! 2. The result is `ExecResult::Empty` with the expected Postgres-style
//!    command-complete tag.
//!
//! Statements *not* tested here (they use real implementations):
//! - VACUUM / ANALYZE / CLUSTER / LOCK / COMMENT / EXPLAIN / GRANT / REVOKE /
//!   SET / SHOW / CREATE ROLE / DROP ROLE / ALTER ROLE — shipped by sibling
//!   agent a0f45b8b5d58608d0.
//! - DECLARE CURSOR / FETCH / CLOSE / MOVE — real cursor lifecycle shipped by
//!   sibling agent a193aadd56ce5cb56.
//! - LISTEN / NOTIFY / UNLISTEN — explicit non-goals per ADR 0012; they remain
//!   rejected with SQLSTATE 0A000 and are NOT tested here.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult, TenantSession};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ──────────────────────────────────────────────────────────────────────────────
// Test harness
// ──────────────────────────────────────────────────────────────────────────────

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

async fn open_session(engine: &Engine) -> TenantSession {
    engine.open_session(TenantId::new()).await.unwrap()
}

/// Assert that `sql` executes without error and returns an empty tag matching
/// `expected_tag`. Panics with a descriptive message on failure.
async fn assert_noop(sess: &TenantSession, sql: &str, expected_tag: &str) {
    match sess.execute(sql).await {
        Ok(ExecResult::Empty { tag }) => {
            assert_eq!(
                tag, expected_tag,
                "sql={sql:?}: expected tag {expected_tag:?}, got {tag:?}"
            );
        }
        Ok(ExecResult::Rows { .. }) => {
            panic!("sql={sql:?}: expected Empty({expected_tag:?}), got Rows");
        }
        Err(e) => {
            panic!("sql={sql:?}: expected Empty({expected_tag:?}), got error: {e}");
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Prepared-statement protocol forms (text)
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn prepare_text_form_is_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    // Text-form PREPARE — rare (some migration tools use it). The real
    // extended-query path uses pgwire Parse/Bind/Execute messages.
    assert_noop(&sess, "PREPARE my_stmt AS SELECT 1", "PREPARE").await;
}

#[tokio::test]
async fn execute_text_form_is_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(&sess, "EXECUTE my_stmt", "EXECUTE").await;
}

#[tokio::test]
async fn deallocate_is_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(&sess, "DEALLOCATE my_stmt", "DEALLOCATE").await;
}

#[tokio::test]
async fn deallocate_all_is_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(&sess, "DEALLOCATE ALL", "DEALLOCATE").await;
}

// ──────────────────────────────────────────────────────────────────────────────
// Transaction control — Basin is auto-commit in v0.1
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn begin_is_accepted() {
    // Basin is auto-commit. Accepting BEGIN silently keeps drivers that emit
    // implicit BEGIN/COMMIT (e.g. lib/pq in non-autocommit mode) from
    // bouncing with 0A000.
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(&sess, "BEGIN", "BEGIN").await;
}

#[tokio::test]
async fn begin_transaction_is_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(&sess, "BEGIN TRANSACTION", "BEGIN").await;
}

#[tokio::test]
async fn start_transaction_is_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(&sess, "START TRANSACTION", "BEGIN").await;
}

#[tokio::test]
async fn commit_is_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(&sess, "COMMIT", "COMMIT").await;
}

#[tokio::test]
async fn rollback_is_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(&sess, "ROLLBACK", "ROLLBACK").await;
}

#[tokio::test]
async fn savepoint_is_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(&sess, "SAVEPOINT my_sp", "SAVEPOINT").await;
}

#[tokio::test]
async fn release_savepoint_is_accepted() {
    // RELEASE SAVEPOINT also arrives as TransactionStmt / RELEASE kind;
    // the StmtKind::Savepoint arm covers both SAVEPOINT and RELEASE.
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(&sess, "RELEASE SAVEPOINT my_sp", "SAVEPOINT").await;
}

#[tokio::test]
async fn rollback_to_savepoint_is_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(&sess, "ROLLBACK TO SAVEPOINT my_sp", "ROLLBACK").await;
}

// ──────────────────────────────────────────────────────────────────────────────
// Trigger DDL — syntactic accept; Basin does not execute trigger bodies
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn create_trigger_is_accepted() {
    // Basin does not execute PL/pgSQL trigger bodies; declarative lifecycle
    // columns and SQL-bodied reactors (Phase 5.11.B / 5.11.C) cover the
    // common trigger use cases. CREATE TRIGGER is accepted syntactically so
    // migration scripts don't bounce.
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(
        &sess,
        "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE TRIGGER",
    )
    .await;
}

#[tokio::test]
async fn drop_trigger_is_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(&sess, "DROP TRIGGER trg ON t", "DROP TRIGGER").await;
}

// ──────────────────────────────────────────────────────────────────────────────
// Extension DDL — syntactic accept; Basin ships equivalents natively (ADR 0002)
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn create_extension_is_accepted() {
    // Basin ships its own equivalents natively per ADR 0002 (no upstream
    // `.so` loading). Loading an external extension is therefore a no-op —
    // the equivalent Basin crate is already active (e.g. pgcrypto →
    // basin-engine UDFs, uuid-ossp → native UUID, pg_vector → native
    // vector(N), PostGIS subset → basin-geo, pg_trgm → basin-trgm, etc.).
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(&sess, "CREATE EXTENSION IF NOT EXISTS pgcrypto", "CREATE EXTENSION").await;
}

#[tokio::test]
async fn drop_extension_is_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(&sess, "DROP EXTENSION pgcrypto", "DROP EXTENSION").await;
}

// ──────────────────────────────────────────────────────────────────────────────
// MERGE — syntactic accept; v0.1 does not execute MERGE logic
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn merge_basic_is_accepted() {
    // MERGE INTO … USING … ON … WHEN MATCHED / NOT MATCHED is accepted
    // syntactically in v0.1 but the MERGE logic is not executed. Run the
    // WHEN MATCHED UPDATE and WHEN NOT MATCHED INSERT branches as separate
    // UPDATE / INSERT statements until Phase 5 ships real MERGE execution.
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(
        &sess,
        "MERGE INTO target t \
         USING source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET val = s.val \
         WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)",
        "MERGE",
    )
    .await;
}

#[tokio::test]
async fn merge_matched_only_is_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(
        &sess,
        "MERGE INTO t USING s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET val = s.val",
        "MERGE",
    )
    .await;
}

// ──────────────────────────────────────────────────────────────────────────────
// REINDEX — syntactic accept; Basin indexes managed by DataFusion planner
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn reindex_table_is_accepted() {
    // Basin's indexes are managed by DataFusion's adaptive planner; no
    // manual rebuild is needed. REINDEX is accepted so tooling scripts
    // (e.g. pg_restore with REINDEX DATABASE) don't bounce.
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(&sess, "REINDEX TABLE t", "REINDEX").await;
}

#[tokio::test]
async fn reindex_index_is_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    assert_noop(&sess, "REINDEX INDEX idx", "REINDEX").await;
}

// ──────────────────────────────────────────────────────────────────────────────
// Regression — LISTEN / NOTIFY / UNLISTEN must still be rejected
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn listen_is_still_rejected_with_0a000() {
    // LISTEN is an explicit non-goal per ADR 0012 and must NOT be in the
    // noop-accept set. This test proves it was not accidentally accepted.
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    let err = sess
        .execute("LISTEN my_channel")
        .await
        .expect_err("LISTEN must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("0A000") || msg.contains("not supported"),
        "expected 0A000 rejection for LISTEN, got: {msg}"
    );
}

#[tokio::test]
async fn notify_is_still_rejected_with_0a000() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    let err = sess
        .execute("NOTIFY my_channel")
        .await
        .expect_err("NOTIFY must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("0A000") || msg.contains("not supported"),
        "expected 0A000 rejection for NOTIFY, got: {msg}"
    );
}

#[tokio::test]
async fn unlisten_is_still_rejected_with_0a000() {
    let (_dir, engine) = open_engine().await;
    let sess = open_session(&engine).await;
    let err = sess
        .execute("UNLISTEN my_channel")
        .await
        .expect_err("UNLISTEN must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("0A000") || msg.contains("not supported"),
        "expected 0A000 rejection for UNLISTEN, got: {msg}"
    );
}
