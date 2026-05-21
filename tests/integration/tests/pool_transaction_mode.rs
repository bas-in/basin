//! Phase 5.27.A — txn-mode pool: cursor / session-state isolation contract.
//!
//! ## What this tests
//!
//! A cursor opened inside a transaction on a physical connection must NOT be
//! visible to the next logical client that checks out the same physical
//! connection from the pool.  This is the core invariant of
//! *transaction-mode* pooling: each `BEGIN … COMMIT` cycle is atomic from
//! the pool's perspective — no per-session state crosses the checkout
//! boundary.
//!
//! ## Red / green status
//!
//! **RED until Phase 5.27.B.**
//!
//! `TxnModePool` (the transaction-mode pool primitive) does not exist yet.
//! The tests below compile against the current `basin-pool` API and exercise
//! the session-mode `SessionPool` as a stand-in.  The core isolation
//! assertion (`cursor_not_visible_after_commit`) currently FAILS because the
//! session-mode pool does not reset per-connection state on return.  That is
//! the expected "red" signal for this wave.
//!
//! Phase 5.27.B must:
//!   1. Add `TxnModePool` (or a `PoolMode::Transaction` variant on `PoolConfig`).
//!   2. Implement a reset/rollback call on checkout so cursors, prepared
//!      statements, and SET-LOCAL state are cleared before the session is
//!      handed to a new logical client.
//!   3. After that change, `cursor_not_visible_after_commit` (and the
//!      thousand-client soak) should turn green without touching this file.
//!
//! ## Design notes
//!
//! We use an in-process engine (no subprocess) so the test is fast and
//! deterministic.  The pool is constructed with `max_sessions = 10` to
//! simulate a bounded connection pool.  1 000 short-lived "clients" each
//! do:
//!
//!   BEGIN;
//!   DECLARE cur_<i> CURSOR FOR SELECT …;
//!   FETCH 1 FROM cur_<i>;
//!   COMMIT;
//!
//! After each commit the test checks that the cursor name is NOT available
//! on a fresh checkout of the same pool slot.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_pool::{PoolConfig, SessionPool};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn build_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
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

fn pool_config_10() -> PoolConfig {
    PoolConfig {
        max_sessions: 10,
        per_project_cap: 10,
        ..Default::default()
    }
}

/// Count rows across all batches in an ExecResult.
fn row_count(r: &ExecResult) -> usize {
    match r {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

// ---------------------------------------------------------------------------
// Test 1: cursor opened in txn i is not visible in txn i+1 on the same slot.
//
// RED until 5.27.B — the session-mode pool does NOT clear cursors on return,
// so the DECLARE in iteration i+1 will clash with the cursor from iteration i
// (or the FETCH in i+1 will return stale rows). The assertion deliberately
// exercises the violation so the failure is meaningful.
// ---------------------------------------------------------------------------

/// Core isolation assertion: a cursor from a committed txn must NOT be
/// accessible in the next checkout of the same connection slot.
///
/// **Expected to FAIL until Phase 5.27.B** because `SessionPool` (session-mode)
/// does not reset per-connection state between checkouts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cursor_not_visible_after_commit() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();

    // NOTE: Phase 5.27.B replaces SessionPool here with TxnModePool (or a
    // PoolConfig { mode: PoolMode::Transaction, .. } variant).
    let pool = SessionPool::new(engine, pool_config_10());

    // Seed the project with a table so DECLARE has something to iterate.
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        s.execute("CREATE TABLE nums (id BIGINT NOT NULL)").await.unwrap();
        for i in 0i64..5 {
            s.execute(&format!("INSERT INTO nums VALUES ({i})")).await.unwrap();
        }
    }

    // First logical client: open cursor, commit, return session.
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        s.execute("BEGIN").await.unwrap();
        // cursor name is stable across the two checkouts so we can probe it.
        s.execute("DECLARE probe_cur CURSOR FOR SELECT id FROM nums").await.unwrap();
        let fetched = s.execute("FETCH 1 FROM probe_cur").await.unwrap();
        assert_eq!(row_count(&fetched), 1, "FETCH should return one row");
        s.execute("COMMIT").await.unwrap();
        // Session is returned to pool here (leased drops).
    }

    // Second logical client: must NOT be able to FETCH from probe_cur.
    // In a correct txn-mode pool the cursor is gone after COMMIT + return.
    // In the current session-mode pool the cursor MAY survive — that's the
    // red failure we want to surface.
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        // Attempt to FETCH from the cursor that should have been destroyed.
        // A correct txn-mode pool returns an error here; session-mode pool
        // may return stale rows — either way the assertion below captures it.
        let result = s.execute("FETCH 1 FROM probe_cur").await;
        // After 5.27.B this MUST be Err (cursor unknown). Until then it may
        // erroneously succeed (session-mode leakage) — the test fails red.
        assert!(
            result.is_err(),
            "5.27.B CONTRACT VIOLATION: cursor 'probe_cur' survived a pool \
             return and is visible to the next logical client. \
             Implement TxnModePool (Phase 5.27.B) to fix this."
        );
    }
}

// ---------------------------------------------------------------------------
// Test 2: 1 000 short-lived clients, 10-connection pool — soak / throughput.
//
// RED until 5.27.B (same reason as above — TxnModePool not yet wired).
// The soak itself may pass in session-mode IF cursors happen to be cleared,
// but the semantics are wrong; mark it to document intent.
// ---------------------------------------------------------------------------

/// 1 000 short-lived logical clients each run a minimal txn against a 10-slot
/// pool and assert no stale cursor bleeds across checkouts.
///
/// **Expected to FAIL until Phase 5.27.B** — same root cause as
/// `cursor_not_visible_after_commit`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn soak_1000_clients_10_connection_pool() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();

    // NOTE: Phase 5.27.B: replace with TxnModePool / PoolMode::Transaction.
    let pool = SessionPool::new(engine, pool_config_10());

    // Seed table.
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        s.execute("CREATE TABLE counters (n BIGINT NOT NULL)").await.unwrap();
        s.execute("INSERT INTO counters VALUES (1), (2), (3)").await.unwrap();
    }

    // Spawn 1 000 concurrent short-lived "logical clients". Each opens a
    // cursor with a unique name, fetches one row, commits, and returns.
    let pool = Arc::new(pool);
    let mut handles = Vec::with_capacity(1_000);
    for i in 0usize..1_000 {
        let pool = pool.clone();
        let handle = tokio::spawn(async move {
            let cur_name = format!("cur_{i}");
            let leased = pool.acquire(project, None).await.unwrap();
            let s = leased.session();
            s.execute("BEGIN").await.unwrap();
            s.execute(&format!(
                "DECLARE {cur_name} CURSOR FOR SELECT n FROM counters"
            ))
            .await
            .unwrap();
            let fetched = s.execute(&format!("FETCH 1 FROM {cur_name}")).await.unwrap();
            assert_eq!(
                row_count(&fetched),
                1,
                "client {i}: FETCH returned 0 rows — unexpected"
            );
            s.execute("COMMIT").await.unwrap();
            // Drop leased → session returned to pool.
        });
        handles.push(handle);
    }

    let mut failures = 0usize;
    for h in handles {
        if h.await.is_err() {
            failures += 1;
        }
    }

    // Phase 5.27.B: this should be 0. Until then, some tasks may panic due
    // to cursor-name collisions on re-used sessions (the "red" signal).
    assert_eq!(
        failures, 0,
        "5.27.B CONTRACT VIOLATION: {failures}/1000 clients panicked — \
         likely due to cursor-name collisions across pool checkouts. \
         Implement TxnModePool (Phase 5.27.B) to fix this."
    );
}
