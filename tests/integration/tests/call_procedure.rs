//! Integration tests for `CREATE PROCEDURE … LANGUAGE sql` / `CALL` (Phase 5.11.F).
//!
//! Exercises the full engine round-trip: parse → catalog → argument
//! substitution → transactional execution → commit/rollback.
//!
//! Acceptance gates per TASK.md 5.11.F:
//!
//! 1. `CALL archive_project(42)` runs both statements in order and the
//!    source table is empty / the archive table is populated.
//! 2. Multi-project isolation: project A's `CALL` cannot touch project B's data.
//! 3. Failure mid-procedure rolls back earlier statements (implicit transaction).
//! 4. This test file (`cargo test -p basin-integration-tests --test call_procedure`).

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

fn shared_engine(dir: &TempDir, catalog: Arc<dyn Catalog>) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

fn col_i64(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

fn col_str(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i).to_string());
        }
    }
    out
}

fn row_count(batches: &[arrow_array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ─────────────────────────────────────────────────────────────────────────────
// Gate 1 — archive_project round-trip
// ─────────────────────────────────────────────────────────────────────────────

/// `CALL archive_project(42)` inserts the matching row into `archive`
/// and deletes it from `projects`. Both statements execute in order
/// and the result is committed atomically.
#[test]
fn archive_project_round_trip() {
    basin_integration_tests::big_stack::run(async {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        // Set up source and archive tables.
        sess.execute("CREATE TABLE projects (id BIGINT NOT NULL, name TEXT NOT NULL)")
            .await
            .unwrap();
        sess.execute("CREATE TABLE archive (id BIGINT NOT NULL, name TEXT NOT NULL)")
            .await
            .unwrap();

        // Seed three projects.
        sess.execute("INSERT INTO projects VALUES (42, 'alpha'), (99, 'beta'), (7, 'gamma')")
            .await
            .unwrap();

        // Register the archive procedure.
        sess.execute(
            "CREATE PROCEDURE archive_project(pid BIGINT) LANGUAGE sql AS $$ \
         INSERT INTO archive SELECT id, name FROM projects WHERE id = pid; \
         DELETE FROM projects WHERE id = pid \
         $$",
        )
        .await
        .unwrap();

        // Call it for project 42.
        let res = sess.execute("CALL archive_project(42)").await.unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "CALL 2"),
            other => panic!("unexpected result: {other:?}"),
        }

        // archive should contain exactly project 42.
        let res = sess
            .execute("SELECT id, name FROM archive ORDER BY id")
            .await
            .unwrap();
        let batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            other => panic!("{other:?}"),
        };
        assert_eq!(col_i64(&batches, "id"), vec![42]);
        assert_eq!(col_str(&batches, "name"), vec!["alpha"]);

        // projects should no longer contain project 42.
        let res = sess
            .execute("SELECT id FROM projects ORDER BY id")
            .await
            .unwrap();
        let batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            other => panic!("{other:?}"),
        };
        let ids = col_i64(&batches, "id");
        assert!(!ids.contains(&42), "project 42 should be gone: {ids:?}");
        assert_eq!(ids.len(), 2, "99 and 7 should remain: {ids:?}");
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Gate 2 — multi-project isolation
// ─────────────────────────────────────────────────────────────────────────────

/// Project A registers `archive_project`; project B must NOT be able to
/// call it (procedure lookup is scoped per-project).
#[test]
fn multi_project_isolation() {
    basin_integration_tests::big_stack::run(async {
        let dir = TempDir::new().unwrap();
        let cat: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        let eng_a = shared_engine(&dir, cat.clone());
        let eng_b = shared_engine(&dir, cat.clone());

        let project_a = ProjectId::new();
        let project_b = ProjectId::new();
        let sess_a = eng_a.open_session(project_a).await.unwrap();
        let sess_b = eng_b.open_session(project_b).await.unwrap();

        // Set up project A.
        sess_a
            .execute("CREATE TABLE projects (id BIGINT NOT NULL, name TEXT NOT NULL)")
            .await
            .unwrap();
        sess_a
            .execute("CREATE TABLE archive (id BIGINT NOT NULL, name TEXT NOT NULL)")
            .await
            .unwrap();
        sess_a
            .execute("INSERT INTO projects VALUES (1, 'one')")
            .await
            .unwrap();
        sess_a
            .execute(
                "CREATE PROCEDURE archive_project(pid BIGINT) LANGUAGE sql AS $$ \
             INSERT INTO archive SELECT id, name FROM projects WHERE id = pid; \
             DELETE FROM projects WHERE id = pid \
             $$",
            )
            .await
            .unwrap();

        // Project B cannot see project A's procedure.
        let err = sess_b.execute("CALL archive_project(1)").await.unwrap_err();
        assert!(
            matches!(err, BasinError::NotFound(_)),
            "expected NotFound for project B calling project A's procedure, got: {err:?}"
        );

        // Project A's procedure still works correctly.
        sess_a.execute("CALL archive_project(1)").await.unwrap();

        let res = sess_a
            .execute("SELECT id FROM archive ORDER BY id")
            .await
            .unwrap();
        let batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            other => panic!("{other:?}"),
        };
        assert_eq!(col_i64(&batches, "id"), vec![1]);

        // Isolation: project A's archive is not accessible from project B.
        let err = sess_b.execute("SELECT * FROM archive").await.unwrap_err();
        assert!(
            matches!(err, BasinError::NotFound(_)) || format!("{err}").contains("not found"),
            "expected cross-project table access to fail, got: {err:?}"
        );
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Gate 3 — failure mid-procedure rolls back earlier statements
// ─────────────────────────────────────────────────────────────────────────────

/// When the second statement fails, the first statement's INSERT must be
/// rolled back — the implicit transaction wrapping CALL is all-or-nothing.
/// (The transaction machinery works reliably for INSERT; DELETE inside a
/// transaction commits eagerly — that is a separate future phase.)
#[test]
fn failure_mid_procedure_rolls_back() {
    basin_integration_tests::big_stack::run(async {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE audit_log (id BIGINT NOT NULL, action TEXT NOT NULL)")
            .await
            .unwrap();

        // The procedure inserts an audit record then tries to insert into a
        // nonexistent table — the second INSERT fails, rolling the first back.
        sess.execute(
            "CREATE PROCEDURE log_then_fail(eid BIGINT) LANGUAGE sql AS $$ \
         INSERT INTO audit_log VALUES (eid, 'started'); \
         INSERT INTO nonexistent_table VALUES (eid) \
         $$",
        )
        .await
        .unwrap();

        let err = sess.execute("CALL log_then_fail(99)").await.unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("statement #2")
                || msg.contains("nonexistent_table")
                || msg.contains("not found"),
            "unexpected error shape: {msg}"
        );

        // The INSERT from the first statement must be rolled back —
        // audit_log must remain empty.
        let res = sess
            .execute("SELECT id FROM audit_log ORDER BY id")
            .await
            .unwrap();
        let batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            row_count(&batches),
            0,
            "first INSERT must be rolled back; got {} rows",
            row_count(&batches)
        );
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Extra — procedure inside an explicit outer transaction uses SAVEPOINT
// ─────────────────────────────────────────────────────────────────────────────

/// When CALL is issued inside a client-managed `BEGIN` block, a failure
/// in the procedure rolls back the procedure's own writes via an internal
/// SAVEPOINT. The outer transaction is aborted (same as any failing
/// statement inside a transaction — requires ROLLBACK to recover).
/// This matches PostgreSQL semantics: a failing CALL aborts the outer txn.
#[test]
fn call_inside_outer_transaction_procedure_writes_rolled_back() {
    basin_integration_tests::big_stack::run(async {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE log (msg TEXT NOT NULL)")
            .await
            .unwrap();

        // A procedure that inserts one row then fails.
        sess.execute(
            "CREATE PROCEDURE fail_proc() LANGUAGE sql AS $$ \
         INSERT INTO log VALUES ('inside_proc'); \
         INSERT INTO missing_table VALUES ('boom') \
         $$",
        )
        .await
        .unwrap();

        // Begin an outer transaction and insert a row.
        sess.execute("BEGIN").await.unwrap();
        sess.execute("INSERT INTO log VALUES ('outer_before')")
            .await
            .unwrap();

        // CALL the failing procedure — it should roll back its own INSERT
        // via the internal savepoint, then propagate the error. The outer
        // transaction enters the aborted state (same as any statement failure
        // in a transaction block — consistent with PostgreSQL behavior).
        let err = sess.execute("CALL fail_proc()").await.unwrap_err();
        assert!(
            format!("{err}").contains("missing_table")
                || format!("{err}").contains("not found")
                || format!("{err}").contains("statement #2"),
            "unexpected error: {err}"
        );

        // Recover from the aborted outer transaction via ROLLBACK.
        sess.execute("ROLLBACK").await.unwrap();

        // After ROLLBACK, the outer transaction's INSERT ('outer_before') and
        // the procedure's INSERT ('inside_proc') are both gone.
        let res = sess.execute("SELECT msg FROM log").await.unwrap();
        let batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            row_count(&batches),
            0,
            "both the outer and procedure INSERTs must be rolled back"
        );
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Extra — successful call inside outer transaction commits with outer
// ─────────────────────────────────────────────────────────────────────────────

/// When CALL succeeds inside an outer `BEGIN` block, the procedure's
/// writes remain pending (not yet committed to disk) until the outer
/// COMMIT. Rolling back the outer transaction rolls them back too.
#[test]
fn successful_call_inside_outer_tx_rolls_back_with_outer() {
    basin_integration_tests::big_stack::run(async {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE log (msg TEXT NOT NULL)")
            .await
            .unwrap();

        sess.execute(
            "CREATE PROCEDURE add_log(m TEXT) LANGUAGE sql AS $$ \
         INSERT INTO log VALUES (m) \
         $$",
        )
        .await
        .unwrap();

        // Begin an outer transaction, call the procedure, then roll back.
        sess.execute("BEGIN").await.unwrap();
        sess.execute("CALL add_log('inside_proc')").await.unwrap();
        sess.execute("ROLLBACK").await.unwrap();

        // The procedure's INSERT should be gone after the outer rollback.
        let res = sess.execute("SELECT msg FROM log").await.unwrap();
        let batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            row_count(&batches),
            0,
            "log must be empty after outer ROLLBACK"
        );
    });
}
