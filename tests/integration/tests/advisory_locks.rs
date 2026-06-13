//! End-to-end advisory-lock coverage (BUG #138 + ADR 0026).
//!
//! Exercises `pg_advisory_lock` / `pg_advisory_unlock` / `pg_advisory_unlock_all`
//! and the xact-scoped + try variants through the *real* statement path
//! (`SELECT pg_advisory_lock(N)`), which is exactly the shape Prisma and sqlx
//! emit. These run engine sessions directly — no pgwire — but go through the
//! same executor/UDF dispatch that the wire protocol uses, so simple-query and
//! the function interception are both covered.
//!
//! What is asserted:
//!   * two sessions contend: a blocking `pg_advisory_lock` waits, then acquires
//!     the instant the holder releases;
//!   * `pg_try_advisory_lock` returns false under contention, true once free;
//!   * reentrancy: N locks need N unlocks;
//!   * `pg_advisory_unlock_all` drops every session lock at once;
//!   * xact locks auto-release on COMMIT *and* on ROLLBACK;
//!   * session-scoped and xact-scoped locks share one keyspace (each blocks the
//!     other on the same key);
//!   * project isolation: the same numeric key in two projects does not contend;
//!   * disconnect (session drop) releases everything the session held;
//!   * a lock-wait timeout surfaces `BasinError::LockNotAvailable` (→ 55P03);
//!   * an ABBA waiter cycle is broken promptly by the wait-for-graph detector:
//!     one session gets `BasinError::DeadlockDetected` (→ 40P01) and the other
//!     acquires, well under the lock_timeout;
//!   * a self-lock is re-entrant (PG semantics), never a self-deadlock.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, BooleanArray};
use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Harness
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

/// Run a statement, panicking on error.
async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("execute [{sql}]: {e}"));
}

/// Run a `SELECT pg_*_lock(...)` that returns a single boolean scalar and
/// return that boolean. Panics on a non-rows / non-boolean result.
async fn scalar_bool(sess: &ProjectSession, sql: &str) -> bool {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().expect("at least one batch");
            assert!(b.num_rows() >= 1, "no rows for [{sql}]");
            let col = b.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap_or_else(|| panic!("expected BooleanArray for [{sql}], got {col:?}"));
            assert!(!arr.is_null(0), "boolean result was NULL for [{sql}]");
            arr.value(0)
        }
        Ok(other) => panic!("non-rows result for [{sql}]: {other:?}"),
        Err(e) => panic!("execute [{sql}]: {e}"),
    }
}

// ---------------------------------------------------------------------------
// 1. try-lock contention across two sessions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn try_lock_excludes_other_session_then_releases() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let a = engine.open_session(project).await.unwrap();
    let b = engine.open_session(project).await.unwrap();

    let key = 100_001_i64;

    // A takes it.
    assert!(scalar_bool(&a, &format!("SELECT pg_try_advisory_lock({key})")).await);
    // B is excluded.
    assert!(!scalar_bool(&b, &format!("SELECT pg_try_advisory_lock({key})")).await);
    // A releases.
    assert!(scalar_bool(&a, &format!("SELECT pg_advisory_unlock({key})")).await);
    // B now succeeds.
    assert!(scalar_bool(&b, &format!("SELECT pg_try_advisory_lock({key})")).await);
    assert!(scalar_bool(&b, &format!("SELECT pg_advisory_unlock({key})")).await);
}

// ---------------------------------------------------------------------------
// 2. blocking lock: B waits, A releases, B acquires
// ---------------------------------------------------------------------------

// Multi-thread runtime: the blocking `pg_advisory_lock` UDF parks the
// executor thread (sync sleep) while waiting, so session A's release must run
// on a *different* worker thread or the test would deadlock.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn blocking_lock_acquires_when_holder_releases() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let a = engine.open_session(project).await.unwrap();
    let b = engine.open_session(project).await.unwrap();

    let key = 100_002_i64;

    // A holds the lock.
    assert!(scalar_bool(&a, &format!("SELECT pg_try_advisory_lock({key})")).await);

    // B blocks on the same key with a generous timeout; the call only returns
    // once A releases.
    exec(&b, "SET lock_timeout = '5s'").await;
    let b_task = tokio::spawn(async move {
        // Blocking acquire — returns void (NULL).
        b.execute(&format!("SELECT pg_advisory_lock({key})"))
            .await
            .expect("blocking acquire must succeed after holder releases");
        b
    });

    // Give B a moment to start blocking, then release from A.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(scalar_bool(&a, &format!("SELECT pg_advisory_unlock({key})")).await);

    // B should now have acquired.
    let b = tokio::time::timeout(Duration::from_secs(5), b_task)
        .await
        .expect("B must not hang after A releases")
        .expect("B task panicked");

    // B holds it now: a fresh try from A fails.
    assert!(!scalar_bool(&a, &format!("SELECT pg_try_advisory_lock({key})")).await);
    assert!(scalar_bool(&b, &format!("SELECT pg_advisory_unlock({key})")).await);
}

// ---------------------------------------------------------------------------
// 3. reentrancy: N locks need N unlocks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reentrant_lock_needs_matching_unlocks() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let a = engine.open_session(project).await.unwrap();
    let b = engine.open_session(project).await.unwrap();

    let key = 100_003_i64;

    assert!(scalar_bool(&a, &format!("SELECT pg_try_advisory_lock({key})")).await);
    assert!(scalar_bool(&a, &format!("SELECT pg_try_advisory_lock({key})")).await); // count == 2

    // One unlock: still held.
    assert!(scalar_bool(&a, &format!("SELECT pg_advisory_unlock({key})")).await);
    assert!(!scalar_bool(&b, &format!("SELECT pg_try_advisory_lock({key})")).await);

    // Second unlock: fully released.
    assert!(scalar_bool(&a, &format!("SELECT pg_advisory_unlock({key})")).await);
    assert!(scalar_bool(&b, &format!("SELECT pg_try_advisory_lock({key})")).await);
    assert!(scalar_bool(&b, &format!("SELECT pg_advisory_unlock({key})")).await);
}

// ---------------------------------------------------------------------------
// 4. unlock_all releases everything session-scoped
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unlock_all_releases_session_locks() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let a = engine.open_session(project).await.unwrap();
    let b = engine.open_session(project).await.unwrap();

    let k1 = 100_010_i64;
    let k2 = 100_011_i64;

    assert!(scalar_bool(&a, &format!("SELECT pg_try_advisory_lock({k1})")).await);
    assert!(scalar_bool(&a, &format!("SELECT pg_try_advisory_lock({k2})")).await);
    assert!(!scalar_bool(&b, &format!("SELECT pg_try_advisory_lock({k1})")).await);

    // Release them all in one call.
    exec(&a, "SELECT pg_advisory_unlock_all()").await;

    assert!(scalar_bool(&b, &format!("SELECT pg_try_advisory_lock({k1})")).await);
    assert!(scalar_bool(&b, &format!("SELECT pg_try_advisory_lock({k2})")).await);
    assert!(scalar_bool(&b, &format!("SELECT pg_advisory_unlock({k1})")).await);
    assert!(scalar_bool(&b, &format!("SELECT pg_advisory_unlock({k2})")).await);
}

// ---------------------------------------------------------------------------
// 5. xact lock auto-releases on COMMIT and on ROLLBACK
// ---------------------------------------------------------------------------

#[tokio::test]
async fn xact_lock_releases_on_commit() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let a = engine.open_session(project).await.unwrap();
    let b = engine.open_session(project).await.unwrap();

    let key = 100_020_i64;

    exec(&a, "BEGIN").await;
    assert!(scalar_bool(&a, &format!("SELECT pg_try_advisory_xact_lock({key})")).await);
    // B is excluded while A's txn holds it.
    assert!(!scalar_bool(&b, &format!("SELECT pg_try_advisory_lock({key})")).await);

    // COMMIT auto-releases the xact lock.
    exec(&a, "COMMIT").await;
    assert!(scalar_bool(&b, &format!("SELECT pg_try_advisory_lock({key})")).await);
    assert!(scalar_bool(&b, &format!("SELECT pg_advisory_unlock({key})")).await);
}

#[tokio::test]
async fn xact_lock_releases_on_rollback() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let a = engine.open_session(project).await.unwrap();
    let b = engine.open_session(project).await.unwrap();

    let key = 100_021_i64;

    exec(&a, "BEGIN").await;
    assert!(scalar_bool(&a, &format!("SELECT pg_try_advisory_xact_lock({key})")).await);
    assert!(!scalar_bool(&b, &format!("SELECT pg_try_advisory_lock({key})")).await);

    // ROLLBACK auto-releases the xact lock.
    exec(&a, "ROLLBACK").await;
    assert!(scalar_bool(&b, &format!("SELECT pg_try_advisory_lock({key})")).await);
    assert!(scalar_bool(&b, &format!("SELECT pg_advisory_unlock({key})")).await);
}

// ---------------------------------------------------------------------------
// 6. session and xact locks share one keyspace
// ---------------------------------------------------------------------------

#[tokio::test]
async fn session_and_xact_locks_share_keyspace() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let a = engine.open_session(project).await.unwrap();
    let b = engine.open_session(project).await.unwrap();

    let key = 100_030_i64;

    // A holds a *session* lock; B's *xact* attempt on the same key is blocked.
    assert!(scalar_bool(&a, &format!("SELECT pg_try_advisory_lock({key})")).await);
    exec(&b, "BEGIN").await;
    assert!(!scalar_bool(&b, &format!("SELECT pg_try_advisory_xact_lock({key})")).await);
    exec(&b, "ROLLBACK").await;

    // A releases its session lock; now B's *xact* lock excludes A's *session*
    // attempt — the reverse direction of the same shared keyspace.
    assert!(scalar_bool(&a, &format!("SELECT pg_advisory_unlock({key})")).await);
    exec(&b, "BEGIN").await;
    assert!(scalar_bool(&b, &format!("SELECT pg_try_advisory_xact_lock({key})")).await);
    assert!(!scalar_bool(&a, &format!("SELECT pg_try_advisory_lock({key})")).await);
    exec(&b, "COMMIT").await;
    // After B's commit A can take it.
    assert!(scalar_bool(&a, &format!("SELECT pg_try_advisory_lock({key})")).await);
    assert!(scalar_bool(&a, &format!("SELECT pg_advisory_unlock({key})")).await);
}

// ---------------------------------------------------------------------------
// 7. project isolation: same numeric key in two projects must not contend
// ---------------------------------------------------------------------------

#[tokio::test]
async fn same_key_isolated_across_projects() {
    let (_dir, engine) = open_engine().await;
    // Two *different* projects on the same engine/process.
    let p1 = engine.open_session(ProjectId::new()).await.unwrap();
    let p2 = engine.open_session(ProjectId::new()).await.unwrap();

    // Prisma's migration lock key — both projects "migrate" concurrently.
    let key = 72_707_369_i64;

    assert!(scalar_bool(&p1, &format!("SELECT pg_try_advisory_lock({key})")).await);
    assert!(
        scalar_bool(&p2, &format!("SELECT pg_try_advisory_lock({key})")).await,
        "a different project must not contend on the same numeric key"
    );

    assert!(scalar_bool(&p1, &format!("SELECT pg_advisory_unlock({key})")).await);
    assert!(scalar_bool(&p2, &format!("SELECT pg_advisory_unlock({key})")).await);
}

// ---------------------------------------------------------------------------
// 8. disconnect (session drop) releases everything
// ---------------------------------------------------------------------------

#[tokio::test]
async fn session_drop_releases_locks() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let key = 100_040_i64;

    {
        let a = engine.open_session(project).await.unwrap();
        assert!(scalar_bool(&a, &format!("SELECT pg_try_advisory_lock({key})")).await);
        // also an xact lock with no COMMIT — must still be released on drop.
        exec(&a, "BEGIN").await;
        assert!(scalar_bool(&a, &format!("SELECT pg_try_advisory_xact_lock({key})")).await);
        // `a` drops here without unlock / commit — simulates a disconnect.
    }

    // A new session in the same project can take the key.
    let b = engine.open_session(project).await.unwrap();
    assert!(
        scalar_bool(&b, &format!("SELECT pg_try_advisory_lock({key})")).await,
        "disconnect must release all advisory locks the session held"
    );
    assert!(scalar_bool(&b, &format!("SELECT pg_advisory_unlock({key})")).await);
}

// ---------------------------------------------------------------------------
// 9. two-int4 form addresses the same lock as the packed bigint
// ---------------------------------------------------------------------------

#[tokio::test]
async fn two_int4_form_matches_packed_bigint() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let a = engine.open_session(project).await.unwrap();
    let b = engine.open_session(project).await.unwrap();

    // PG packs (classid, objid) = (1, 5) into ((1 as u32) << 32) | 5.
    let packed: i64 = (1_i64 << 32) | 5;

    // A locks via the two-int4 form.
    assert!(scalar_bool(&a, "SELECT pg_try_advisory_lock(1, 5)").await);
    // B trying the equivalent packed bigint is excluded.
    assert!(!scalar_bool(&b, &format!("SELECT pg_try_advisory_lock({packed})")).await);
    // A unlocks via the packed bigint (same lock).
    assert!(scalar_bool(&a, &format!("SELECT pg_advisory_unlock({packed})")).await);
    assert!(scalar_bool(&b, "SELECT pg_try_advisory_lock(1, 5)").await);
    assert!(scalar_bool(&b, "SELECT pg_advisory_unlock(1, 5)").await);
}

// ---------------------------------------------------------------------------
// 10. lock-wait timeout surfaces LockNotAvailable (→ SQLSTATE 55P03)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn blocking_lock_times_out_with_lock_not_available() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let a = engine.open_session(project).await.unwrap();
    let b = engine.open_session(project).await.unwrap();

    let key = 100_050_i64;

    // A holds the lock and never releases.
    assert!(scalar_bool(&a, &format!("SELECT pg_try_advisory_lock({key})")).await);

    // B has a short lock_timeout; the blocking acquire must fail with
    // LockNotAvailable, which the router maps to SQLSTATE 55P03.
    exec(&b, "SET lock_timeout = '100ms'").await;
    let t0 = std::time::Instant::now();
    let err = b
        .execute(&format!("SELECT pg_advisory_lock({key})"))
        .await
        .expect_err("blocking acquire must time out while A holds the lock");
    let elapsed = t0.elapsed();

    assert!(
        elapsed < Duration::from_secs(2),
        "timeout fired too late: {elapsed:?}"
    );
    assert!(
        matches!(err, BasinError::LockNotAvailable(_)),
        "expected LockNotAvailable (→ 55P03), got: {err:?}"
    );

    assert!(scalar_bool(&a, &format!("SELECT pg_advisory_unlock({key})")).await);
}

// ---------------------------------------------------------------------------
// 11. try-xact lock false under contention
// ---------------------------------------------------------------------------

#[tokio::test]
async fn try_xact_lock_false_under_contention() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let a = engine.open_session(project).await.unwrap();
    let b = engine.open_session(project).await.unwrap();

    let key = 100_060_i64;

    exec(&a, "BEGIN").await;
    assert!(scalar_bool(&a, &format!("SELECT pg_try_advisory_xact_lock({key})")).await);

    exec(&b, "BEGIN").await;
    // B's try must report false while A's xact holds the key — the sqlx corpus
    // shape `SELECT pg_try_advisory_xact_lock(N)`.
    assert!(!scalar_bool(&b, &format!("SELECT pg_try_advisory_xact_lock({key})")).await);

    exec(&a, "COMMIT").await;
    // Now B can take it.
    assert!(scalar_bool(&b, &format!("SELECT pg_try_advisory_xact_lock({key})")).await);
    exec(&b, "COMMIT").await;
}

// ---------------------------------------------------------------------------
// 12. ABBA deadlock: one session gets 40P01 promptly, the other acquires
// ---------------------------------------------------------------------------

// Two sessions form a classic ABBA cycle:
//   A holds k1, B holds k2; A then blocks on k2 while B blocks on k1.
// The wait-for-graph detector must abort exactly one waiter with
// DeadlockDetected (→ SQLSTATE 40P01) promptly — well under the generous
// 10s lock_timeout. Once the victim's session drops (its transaction aborts
// and releases the lock the survivor is waiting on, exactly as a driver would
// on receiving 40P01), the surviving waiter ACQUIRES.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abba_deadlock_detected_promptly() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let a = engine.open_session(project).await.unwrap();
    let b = engine.open_session(project).await.unwrap();

    let k1 = 100_070_i64;
    let k2 = 100_071_i64;

    // A holds k1, B holds k2.
    assert!(scalar_bool(&a, &format!("SELECT pg_try_advisory_lock({k1})")).await);
    assert!(scalar_bool(&b, &format!("SELECT pg_try_advisory_lock({k2})")).await);

    // Generous timeouts so a *timeout* (55P03) would take 10s — i.e. if
    // detection failed this test would clearly fail the <5s prompt assertion.
    exec(&a, "SET lock_timeout = '10s'").await;
    exec(&b, "SET lock_timeout = '10s'").await;

    let t0 = std::time::Instant::now();

    // A blocks on k2 (held by B).
    let mut a_task = tokio::spawn(async move {
        let r = a.execute(&format!("SELECT pg_advisory_lock({k2})")).await;
        (a, r)
    });
    // B blocks on k1 (held by A) — closes the cycle.
    let mut b_task = tokio::spawn(async move {
        let r = b.execute(&format!("SELECT pg_advisory_lock({k1})")).await;
        (b, r)
    });

    // Whichever task finishes first is the deadlock victim (it aborts promptly
    // with 40P01). The other is still blocked on the victim's held lock.
    let (victim_sess, victim_res, mut winner_task) = tokio::select! {
        joined = &mut a_task => {
            let (s, r) = joined.expect("A task panicked");
            (s, r, b_task)
        }
        joined = &mut b_task => {
            let (s, r) = joined.expect("B task panicked");
            (s, r, a_task)
        }
    };
    let victim_elapsed = t0.elapsed();

    assert!(
        victim_elapsed < Duration::from_secs(5),
        "deadlock must be detected promptly (not via the 10s timeout): {victim_elapsed:?}"
    );
    assert!(
        matches!(victim_res, Err(BasinError::DeadlockDetected(_))),
        "the first waiter to return must be the 40P01 victim, got: {victim_res:?}"
    );

    // The victim's transaction unwinds — drop its session, releasing the lock
    // the survivor is blocked on (this is what a driver does on 40P01).
    drop(victim_sess);

    // The survivor must now acquire (Ok), not hang.
    let (winner_sess, winner_res) = tokio::time::timeout(Duration::from_secs(8), &mut winner_task)
        .await
        .expect("survivor must acquire once the victim releases — must not hang")
        .expect("survivor task panicked");
    assert!(
        winner_res.is_ok(),
        "survivor must ACQUIRE after the victim aborts: {winner_res:?}"
    );

    drop(winner_sess);
}

// ---------------------------------------------------------------------------
// 13. self-deadlock is re-entrant, not a deadlock (PG semantics)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_lock_reentrant_not_deadlock() {
    let (_dir, engine) = open_engine().await;
    let project = ProjectId::new();
    let a = engine.open_session(project).await.unwrap();

    let key = 100_080_i64;

    // First acquire.
    assert!(scalar_bool(&a, &format!("SELECT pg_try_advisory_lock({key})")).await);
    // Blocking re-acquire of the SAME key by the SAME session must succeed
    // immediately (re-entrancy) — never a 40P01 self-deadlock.
    let t0 = std::time::Instant::now();
    a.execute(&format!("SELECT pg_advisory_lock({key})"))
        .await
        .expect("re-entrant self-acquire must succeed, not deadlock");
    assert!(
        t0.elapsed() < Duration::from_secs(1),
        "re-entrant self-acquire must be immediate"
    );

    // Two acquisitions → two unlocks to release.
    assert!(scalar_bool(&a, &format!("SELECT pg_advisory_unlock({key})")).await);
    assert!(scalar_bool(&a, &format!("SELECT pg_advisory_unlock({key})")).await);
}
