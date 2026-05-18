//! Integration tests for PG-faithful SAVEPOINT / ROLLBACK TO SAVEPOINT /
//! RELEASE SAVEPOINT executed end-to-end through `ProjectSession::execute`.
//!
//! The session-level unit tests in `session.rs` cover the `TxState` stack
//! mechanics directly. These tests drive the full executor dispatch path
//! (statement classification -> tx-block guard -> savepoint stack ->
//! pending file truncation -> DataFusion ctx refresh) so a real client
//! sequence (BEGIN; INSERT; SAVEPOINT s; INSERT; ROLLBACK TO SAVEPOINT s;
//! COMMIT) produces the right rows.
//!
//! ## Pre-existing limitation this suite works around
//!
//! Mid-transaction read-your-writes for the #83/#92 deferred-commit txn
//! model is exercised only via the *aggregate* read path
//! (`SELECT count(*)`), which `refresh_table_with_extra` makes correct.
//! A bare *projection* scan (`SELECT id FROM t`) of pending (not-yet-
//! committed) rows returns zero rows *inside* an open transaction on the
//! current engine — see `in_txn_projection_visibility_is_a_known_gap`.
//! This is unrelated to savepoints (it reproduces with zero savepoints)
//! and out of scope for the SAVEPOINT fix. These tests therefore assert
//! within-txn state with `count(*)` and assert row *identity* only after
//! `COMMIT`, where projection scans are correct.

use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, RecordBatch};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
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

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("execute failed for {sql:?}: {e:?}"));
}

/// `SELECT count(*) FROM t` — the in-txn-correct read path.
async fn count(sess: &ProjectSession) -> i64 {
    match sess.execute("SELECT count(*) FROM t").await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let c = batches[0].column(0);
            if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
                a.value(0)
            } else {
                c.as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("count(*) is an integer column")
                    .value(0) as i64
            }
        }
        other => panic!("expected a count row, got {other:?}"),
    }
}

/// Sorted `id` values from a projection scan. Reliable only outside an
/// open transaction (post-COMMIT) on the current engine.
async fn ids(sess: &ProjectSession, sql: &str) -> Vec<i32> {
    let batches: Vec<RecordBatch> = match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    };
    let mut out = Vec::new();
    for b in &batches {
        let arr = b
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id is i32");
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out.sort_unstable();
    out
}

/// SAVEPOINT then write then ROLLBACK TO SAVEPOINT: the post-savepoint
/// write is gone, the pre-savepoint write retained, the txn still usable,
/// and COMMIT persists exactly the surviving rows.
#[tokio::test]
async fn savepoint_rollback_to_keeps_pre_savepoint_write() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    exec(&sess, "CREATE TABLE t (id INT)").await;
    exec(&sess, "BEGIN").await;
    exec(&sess, "INSERT INTO t (id) VALUES (1)").await;
    exec(&sess, "SAVEPOINT s").await;
    exec(&sess, "INSERT INTO t (id) VALUES (2)").await;

    // Both pending writes visible inside the txn (aggregate read path).
    assert_eq!(
        count(&sess).await,
        2,
        "both rows pending before rollback-to"
    );

    exec(&sess, "ROLLBACK TO SAVEPOINT s").await;

    // Post-savepoint write (2) discarded; pre-savepoint write (1) retained.
    assert_eq!(count(&sess).await, 1, "post-savepoint write rolled back");

    // Txn still usable: another write then COMMIT.
    exec(&sess, "INSERT INTO t (id) VALUES (3)").await;
    assert_eq!(count(&sess).await, 2, "txn usable after rollback-to");
    exec(&sess, "COMMIT").await;

    // After COMMIT the pre-savepoint write (1) and post-rollback write (3)
    // persist; the rolled-back write (2) does not.
    assert_eq!(ids(&sess, "SELECT id FROM t").await, vec![1, 3]);
}

/// `ROLLBACK TO s` (the `SAVEPOINT` keyword is optional in PG) behaves
/// identically to `ROLLBACK TO SAVEPOINT s`.
#[tokio::test]
async fn rollback_to_without_savepoint_keyword() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    exec(&sess, "CREATE TABLE t (id INT)").await;
    exec(&sess, "BEGIN").await;
    exec(&sess, "INSERT INTO t (id) VALUES (10)").await;
    exec(&sess, "SAVEPOINT s").await;
    exec(&sess, "INSERT INTO t (id) VALUES (20)").await;
    assert_eq!(count(&sess).await, 2);
    exec(&sess, "ROLLBACK TO s").await;
    assert_eq!(count(&sess).await, 1, "ROLLBACK TO (no keyword) undid (20)");
    exec(&sess, "COMMIT").await;

    assert_eq!(ids(&sess, "SELECT id FROM t").await, vec![10]);
}

/// RELEASE SAVEPOINT keeps post-savepoint writes (no undo).
#[tokio::test]
async fn release_savepoint_keeps_writes() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    exec(&sess, "CREATE TABLE t (id INT)").await;
    exec(&sess, "BEGIN").await;
    exec(&sess, "INSERT INTO t (id) VALUES (1)").await;
    exec(&sess, "SAVEPOINT s").await;
    exec(&sess, "INSERT INTO t (id) VALUES (2)").await;
    exec(&sess, "RELEASE SAVEPOINT s").await;
    assert_eq!(count(&sess).await, 2, "RELEASE does not undo writes");
    exec(&sess, "COMMIT").await;

    // RELEASE only discards the savepoint marker -- both rows persist.
    assert_eq!(ids(&sess, "SELECT id FROM t").await, vec![1, 2]);
}

/// ROLLBACK TO a non-existent savepoint inside a txn errors (PG 3B001),
/// and the txn enters the aborted state.
#[tokio::test]
async fn rollback_to_nonexistent_savepoint_errors() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    exec(&sess, "CREATE TABLE t (id INT)").await;
    exec(&sess, "BEGIN").await;
    exec(&sess, "INSERT INTO t (id) VALUES (1)").await;

    let err = sess
        .execute("ROLLBACK TO SAVEPOINT nope")
        .await
        .expect_err("rolling back to a non-existent savepoint must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("does not exist") && msg.contains("3B001"),
        "expected PG no_such_savepoint (3B001), got: {msg}"
    );

    // Recover and finish cleanly.
    exec(&sess, "ROLLBACK").await;
}

/// RELEASE of a non-existent savepoint errors with PG 3B001.
#[tokio::test]
async fn release_nonexistent_savepoint_errors() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    exec(&sess, "BEGIN").await;
    let err = sess
        .execute("RELEASE SAVEPOINT ghost")
        .await
        .expect_err("releasing a non-existent savepoint must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("does not exist") && msg.contains("3B001"),
        "expected PG no_such_savepoint (3B001), got: {msg}"
    );
    exec(&sess, "ROLLBACK").await;
}

/// Nested savepoints: s1, s2; ROLLBACK TO s1 drops s2 as well. After
/// rolling back to s1, s2 no longer exists (rolling back to it errors).
#[tokio::test]
async fn nested_savepoints_rollback_to_outer_drops_inner() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    exec(&sess, "CREATE TABLE t (id INT)").await;
    exec(&sess, "BEGIN").await;
    exec(&sess, "INSERT INTO t (id) VALUES (1)").await;
    exec(&sess, "SAVEPOINT s1").await;
    exec(&sess, "INSERT INTO t (id) VALUES (2)").await;
    exec(&sess, "SAVEPOINT s2").await;
    exec(&sess, "INSERT INTO t (id) VALUES (3)").await;
    assert_eq!(count(&sess).await, 3);

    // Roll back to the outer savepoint -- drops the inner (s2) and the
    // writes after s1.
    exec(&sess, "ROLLBACK TO SAVEPOINT s1").await;
    assert_eq!(count(&sess).await, 1, "only the pre-s1 write survives");

    // s2 was destroyed by the rollback-to-s1.
    let err = sess
        .execute("ROLLBACK TO SAVEPOINT s2")
        .await
        .expect_err("s2 must no longer exist after ROLLBACK TO s1");
    assert!(format!("{err}").contains("3B001"));

    // s1 is still re-establishable (PG keeps it).
    exec(&sess, "ROLLBACK TO SAVEPOINT s1").await;
    assert_eq!(count(&sess).await, 1);

    exec(&sess, "COMMIT").await;
    assert_eq!(ids(&sess, "SELECT id FROM t").await, vec![1]);
}

/// Savepoint then full ROLLBACK: everything (including the pre-savepoint
/// write) is discarded; the table is empty after the transaction.
#[tokio::test]
async fn savepoint_then_full_rollback_drops_everything() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    exec(&sess, "CREATE TABLE t (id INT)").await;
    exec(&sess, "BEGIN").await;
    exec(&sess, "INSERT INTO t (id) VALUES (1)").await;
    exec(&sess, "SAVEPOINT s").await;
    exec(&sess, "INSERT INTO t (id) VALUES (2)").await;
    exec(&sess, "ROLLBACK").await;

    // Full ROLLBACK undoes the whole transaction.
    assert_eq!(ids(&sess, "SELECT id FROM t").await, Vec::<i32>::new());
}

/// PG: SAVEPOINT outside a transaction block is an error (25P01),
/// NOT a silent no-op.
#[tokio::test]
async fn savepoint_outside_transaction_errors() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    let err = sess
        .execute("SAVEPOINT s")
        .await
        .expect_err("SAVEPOINT outside a txn block must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("can only be used in transaction blocks") && msg.contains("25P01"),
        "expected PG 25P01, got: {msg}"
    );
}

/// PG: ROLLBACK TO SAVEPOINT outside a transaction block errors with
/// 25P01 (no_active_sql_transaction) -- NOT "savepoint does not exist".
#[tokio::test]
async fn rollback_to_savepoint_outside_transaction_errors() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    let err = sess
        .execute("ROLLBACK TO SAVEPOINT s")
        .await
        .expect_err("ROLLBACK TO SAVEPOINT outside a txn block must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("can only be used in transaction blocks") && msg.contains("25P01"),
        "expected PG 25P01 (not 'does not exist'), got: {msg}"
    );
}

/// PG: RELEASE SAVEPOINT outside a transaction block errors with 25P01.
#[tokio::test]
async fn release_savepoint_outside_transaction_errors() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    let err = sess
        .execute("RELEASE SAVEPOINT s")
        .await
        .expect_err("RELEASE SAVEPOINT outside a txn block must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("can only be used in transaction blocks") && msg.contains("25P01"),
        "expected PG 25P01, got: {msg}"
    );
}

/// Re-establishing a savepoint after rolling back to it: PG keeps the
/// savepoint after ROLLBACK TO, so a second ROLLBACK TO the same name
/// works again. (Regression pin for the `truncate(pos)` -> `truncate(pos+1)`
/// fix in `tx_rollback_to_savepoint`.)
#[tokio::test]
async fn savepoint_reusable_after_rollback_to() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    exec(&sess, "CREATE TABLE t (id INT)").await;
    exec(&sess, "BEGIN").await;
    exec(&sess, "INSERT INTO t (id) VALUES (1)").await;
    exec(&sess, "SAVEPOINT s").await;
    exec(&sess, "INSERT INTO t (id) VALUES (2)").await;
    exec(&sess, "ROLLBACK TO SAVEPOINT s").await;
    // s still exists; a fresh write then a second rollback-to-s.
    exec(&sess, "INSERT INTO t (id) VALUES (3)").await;
    exec(&sess, "ROLLBACK TO SAVEPOINT s").await;
    assert_eq!(count(&sess).await, 1, "second rollback-to-s also worked");
    exec(&sess, "COMMIT").await;

    assert_eq!(ids(&sess, "SELECT id FROM t").await, vec![1]);
}

/// #83/#92 mid-txn projection read-your-own-writes: NOW FIXED. A bare
/// projection scan (`SELECT id FROM t`) of pending rows inside an open
/// transaction now correctly returns those rows (it previously returned
/// nothing — the documented deferred-commit visibility gap). Reproduces
/// with ZERO savepoints, so this exercises the general mid-txn
/// visibility path, not a savepoint defect. The gap closed as a
/// side-effect of the Vortex read-path rework (#161); the test author's
/// own note said to flip `assert_eq!(rows, Vec::new())` → `[1]` once
/// RYOW landed — done here. `count(*)` was already correct mid-txn and
/// still is, so projection and aggregate now agree.
#[tokio::test]
async fn in_txn_projection_sees_pending_rows_ryow() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    exec(&sess, "CREATE TABLE t (id INT)").await;
    exec(&sess, "BEGIN").await;
    exec(&sess, "INSERT INTO t (id) VALUES (1)").await;

    // Aggregate read path: correct mid-txn.
    assert_eq!(count(&sess).await, 1, "count(*) sees the pending row");

    // Projection read path: now also correct mid-txn (RYOW) — #83/#92
    // gap closed; projection and aggregate agree on the pending row.
    let rows = ids(&sess, "SELECT id FROM t").await;
    assert_eq!(
        rows,
        vec![1],
        "#83/#92 fixed: mid-txn projection sees this txn's pending row"
    );

    exec(&sess, "ROLLBACK").await;
}
