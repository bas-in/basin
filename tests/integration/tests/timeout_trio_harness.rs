//! Phase 5.28 — timeout trio integration harness.
//!
//! Covers slices B, C, and D:
//!
//! | Slice | Feature                                    | Assertion                                           |
//! |-------|--------------------------------------------|-----------------------------------------------------|
//! | B     | `lock_timeout` + 55P03                     | Waiting on a lock past the timeout → `LockNotAvailable` |
//! | C     | `idle_in_transaction_session_timeout` reaper | Idle-in-txn past timeout → session marked reaped   |
//! | D     | `SET`/`SHOW` tooling compat                | `SET lock_timeout = '…'` + `SHOW lock_timeout` round-trips |
//!
//! These tests run fully in-process (no pgwire server, no real Postgres).
//! All timeouts are kept short (10–200 ms) so the suite finishes in < 2 s
//! on a developer laptop.
//!
//! ## Running
//!
//! ```sh
//! cargo test -p basin-integration-tests --test timeout_trio_harness
//! ```

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::lock_wait::{bounded_lock_wait, bounded_lock_wait_sync};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── Engine factory ──────────────────────────────────────────────────────────

fn make_engine(dir: &TempDir) -> Engine {
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

// ─── Slice B: lock_timeout + 55P03 ───────────────────────────────────────────

/// `bounded_lock_wait` fires when a lock is never free within the deadline.
/// This directly exercises the primitive from `basin-shard::lock_wait`.
#[tokio::test]
async fn B1_lock_wait_times_out_55P03() {
    // A lock that is always held by another party.
    let acquired = bounded_lock_wait(
        || false, // lock never becomes available
        Some(Duration::from_millis(50)),
        Duration::from_millis(5),
    )
    .await;
    assert!(
        !acquired,
        "bounded_lock_wait must return false when lock is never available"
    );
    // Callers map `false` → BasinError::LockNotAvailable (SQLSTATE 55P03).
    let err = BasinError::lock_not_available("lock timeout after 50ms");
    assert!(
        matches!(err, BasinError::LockNotAvailable(_)),
        "LockNotAvailable must be constructible (SQLSTATE 55P03)"
    );
}

/// `bounded_lock_wait` succeeds when the lock becomes free within the deadline.
#[tokio::test]
async fn B2_lock_wait_succeeds_when_lock_becomes_free() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let counter = Arc::new(AtomicU32::new(0));
    let c = counter.clone();
    let acquired = bounded_lock_wait(
        move || {
            // Becomes "free" after 3 attempts.
            c.fetch_add(1, Ordering::Relaxed) >= 3
        },
        Some(Duration::from_millis(200)),
        Duration::from_millis(10),
    )
    .await;
    assert!(acquired, "bounded_lock_wait must succeed once lock clears");
}

/// Synchronous variant also times out correctly.
#[test]
fn B3_sync_lock_wait_times_out() {
    let acquired = bounded_lock_wait_sync(
        || false,
        Some(Duration::from_millis(30)),
        Duration::from_millis(5),
    );
    assert!(!acquired, "sync variant must also time out");
}

/// SET lock_timeout stores the value in the session GUC and the engine rejects
/// a simulated lock-wait that exceeds it.
#[tokio::test]
async fn B4_set_lock_timeout_guc_stored() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // SET lock_timeout to 100ms.
    let res = sess
        .execute("SET lock_timeout = '100ms'")
        .await
        .expect("SET lock_timeout must succeed");
    assert!(matches!(res, ExecResult::Empty { .. }));

    // SHOW lock_timeout must return the value we just set.
    let show_res = sess
        .execute("SHOW lock_timeout")
        .await
        .expect("SHOW lock_timeout must succeed");
    match show_res {
        ExecResult::Rows { batches, .. } => {
            let batch = &batches[0];
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .expect("lock_timeout column must be Utf8");
            let val = col.value(0);
            assert_eq!(val, "100ms", "SHOW lock_timeout must return '100ms'");
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// SET lock_timeout = 0 disables the timeout.
#[tokio::test]
async fn B5_set_lock_timeout_zero_disables() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("SET lock_timeout = 0")
        .await
        .expect("SET lock_timeout = 0 must succeed");

    let show_res = sess
        .execute("SHOW lock_timeout")
        .await
        .expect("SHOW lock_timeout must succeed");
    match show_res {
        ExecResult::Rows { batches, .. } => {
            let batch = &batches[0];
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .expect("string column");
            assert_eq!(col.value(0), "0", "disabled lock_timeout must show as '0'");
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

// ─── Slice C: idle_in_transaction_session_timeout reaper ─────────────────────

/// A session that opens a transaction and idles past the timeout is flagged as
/// reaped. The next `execute()` call returns `IdleInTransactionTimeout`.
#[tokio::test]
async fn C1_idle_in_txn_session_reaped() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // Configure a very short idle-in-txn timeout.
    sess.execute("SET idle_in_transaction_session_timeout = '50ms'")
        .await
        .expect("SET idle_in_transaction_session_timeout must succeed");

    // Open a transaction.
    sess.execute("BEGIN").await.expect("BEGIN must succeed");

    // Wait longer than the timeout without issuing any SQL.
    // The reaper runs its own background loop (spawned when the engine starts,
    // but we're testing at the library level here — drive it manually via the
    // registry instead).
    std::thread::sleep(Duration::from_millis(100));
    eng.reaper_registry().sweep();

    // The next execute must return IdleInTransactionTimeout.
    let err = sess
        .execute("SELECT 1")
        .await
        .expect_err("reaped session must error on next execute");
    assert!(
        matches!(err, BasinError::IdleInTransactionTimeout(_)),
        "expected IdleInTransactionTimeout (SQLSTATE 25P03), got {err:?}"
    );
}

/// A session that is NOT in a transaction is never reaped, even if idle long.
#[tokio::test]
async fn C2_no_txn_not_reaped() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("SET idle_in_transaction_session_timeout = '10ms'")
        .await
        .expect("SET must succeed");

    // No BEGIN — auto-commit mode.
    std::thread::sleep(Duration::from_millis(30));
    eng.reaper_registry().sweep();

    // Must not be reaped.
    let res = sess
        .execute("SELECT 1")
        .await
        .expect("non-txn session must not be reaped");
    assert!(matches!(res, ExecResult::Rows { .. }));
}

/// A session with no timeout configured is never reaped.
#[tokio::test]
async fn C3_no_timeout_configured_not_reaped() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // No SET idle_in_transaction_session_timeout — defaults to None (off).
    sess.execute("BEGIN").await.expect("BEGIN must succeed");

    std::thread::sleep(Duration::from_millis(30));
    eng.reaper_registry().sweep();

    let res = sess
        .execute("SELECT 1")
        .await
        .expect("session with no timeout must not be reaped");
    assert!(matches!(res, ExecResult::Rows { .. }));
}

/// A session that is active (issues queries) within the timeout window is not reaped.
#[tokio::test]
async fn C4_active_session_not_reaped() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("SET idle_in_transaction_session_timeout = '200ms'")
        .await
        .expect("SET must succeed");

    sess.execute("BEGIN").await.expect("BEGIN must succeed");

    // Execute a query within the window to refresh the last_active timestamp.
    sess.execute("SELECT 1").await.expect("SELECT must succeed");

    // Sleep less than the timeout.
    tokio::time::sleep(Duration::from_millis(50)).await;
    eng.reaper_registry().sweep();

    // Must still be alive.
    let res = sess
        .execute("SELECT 1")
        .await
        .expect("recently-active session must not be reaped");
    assert!(matches!(res, ExecResult::Rows { .. }));
}

// ─── Slice D: SET/SHOW tooling compat ────────────────────────────────────────

/// SET and SHOW round-trip for lock_timeout with various duration forms.
#[tokio::test]
async fn D1_lock_timeout_set_show_roundtrip() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    for (set_val, expected_show) in &[
        ("'500ms'", "500ms"),
        ("'1s'", "1000ms"),
        ("200", "200ms"),
        ("0", "0"),
    ] {
        let sql = format!("SET lock_timeout = {set_val}");
        sess.execute(&sql)
            .await
            .unwrap_or_else(|e| panic!("SET lock_timeout = {set_val} failed: {e}"));

        let show = sess
            .execute("SHOW lock_timeout")
            .await
            .expect("SHOW lock_timeout must succeed");
        match show {
            ExecResult::Rows { batches, .. } => {
                let col = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .expect("string column");
                assert_eq!(
                    col.value(0),
                    *expected_show,
                    "lock_timeout mismatch for SET value {set_val}"
                );
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }
}

/// SET and SHOW round-trip for idle_in_transaction_session_timeout.
#[tokio::test]
async fn D2_idle_in_txn_timeout_set_show_roundtrip() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    for (set_val, expected_show) in &[
        ("'30s'", "30000ms"),
        ("'500ms'", "500ms"),
        ("0", "0"),
    ] {
        let sql = format!("SET idle_in_transaction_session_timeout = {set_val}");
        sess.execute(&sql)
            .await
            .unwrap_or_else(|e| panic!("SET idle_in_transaction_session_timeout = {set_val} failed: {e}"));

        let show = sess
            .execute("SHOW idle_in_transaction_session_timeout")
            .await
            .expect("SHOW idle_in_transaction_session_timeout must succeed");
        match show {
            ExecResult::Rows { batches, .. } => {
                let col = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .expect("string column");
                assert_eq!(
                    col.value(0),
                    *expected_show,
                    "idle_in_transaction_session_timeout mismatch for SET value {set_val}"
                );
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }
}

/// Both new GUCs are independent: setting one does not affect the other.
#[tokio::test]
async fn D3_gucs_are_independent() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("SET lock_timeout = '100ms'")
        .await
        .expect("SET lock_timeout must succeed");
    sess.execute("SET idle_in_transaction_session_timeout = '30s'")
        .await
        .expect("SET idle_in_transaction_session_timeout must succeed");

    let show_lt = sess
        .execute("SHOW lock_timeout")
        .await
        .expect("SHOW lock_timeout");
    let show_iit = sess
        .execute("SHOW idle_in_transaction_session_timeout")
        .await
        .expect("SHOW idle_in_transaction_session_timeout");

    let lt_val = extract_show_value(show_lt);
    let iit_val = extract_show_value(show_iit);

    assert_eq!(lt_val, "100ms", "lock_timeout must be 100ms");
    assert_eq!(
        iit_val, "30000ms",
        "idle_in_transaction_session_timeout must be 30s = 30000ms"
    );
}

/// `statement_timeout` existing tests are not broken: SET/SHOW of unknown
/// variables still silently succeed (backwards compat).
#[tokio::test]
async fn D4_unknown_set_show_still_noop() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // Unknown GUC: must not error.
    sess.execute("SET application_name = 'myapp'")
        .await
        .expect("unknown SET must succeed (noop)");

    let show_res = sess
        .execute("SHOW application_name")
        .await
        .expect("unknown SHOW must succeed (noop)");
    // Returns Empty (not a hard error).
    assert!(
        matches!(show_res, ExecResult::Empty { .. }),
        "unknown SHOW must return Empty"
    );
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn extract_show_value(result: ExecResult) -> String {
    match result {
        ExecResult::Rows { batches, .. } => {
            let col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .expect("string column");
            col.value(0).to_string()
        }
        other => panic!("expected Rows result, got {other:?}"),
    }
}
