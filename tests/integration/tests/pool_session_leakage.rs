//! Phase 5.27.A — session-leakage detection battery.
//!
//! ## What this tests
//!
//! This file contains an explicit, exhaustive assertion battery over the
//! session-affecting SQL commands that a pool operating in transaction mode
//! MUST neutralise before returning a physical connection to the idle queue.
//!
//! The commands under test (drawn from the Postgres manual and Basin's
//! pgwire compatibility matrix):
//!
//! | Command class           | Example                                | Leak vector                        |
//! |-------------------------|----------------------------------------|------------------------------------|
//! | `SET LOCAL`             | `SET LOCAL search_path = 'x'`         | session variable outlives txn      |
//! | `SET` (session-level)   | `SET search_path = 'x'`               | persists for entire connection life |
//! | `LISTEN`                | `LISTEN chan`                          | subscription survives until UNLISTEN|
//! | Prepared statements     | `PREPARE stmt AS SELECT 1`            | survive until DEALLOCATE or close   |
//! | Open cursors            | `DECLARE cur CURSOR FOR …`            | survive until CLOSE or session end  |
//! | Temp tables             | `CREATE TEMP TABLE t (…)`             | survive for the connection session  |
//! | Advisory locks          | `SELECT pg_advisory_lock(1)`          | survive until release or disconnect |
//!
//! ## Red / green status
//!
//! **RED until Phase 5.27.E.**
//!
//! The session-mode `SessionPool` today does not call any reset routine on
//! session return.  Each test in this file asserts that the leakage is
//! absent after a pool return; those assertions will FAIL against the current
//! code, providing the "red" signal.
//!
//! Phase 5.27.E must:
//!   1. Add `ProjectSession::reset_for_pool_return()` (or equivalent) that
//!      issues `DEALLOCATE ALL; CLOSE ALL; RESET ALL; UNLISTEN *;` (and
//!      rolls back any open txn) before returning to the pool.
//!   2. Call that method in `PooledSession::drop()` (or a dedicated
//!      `TxnModePool::return_session()` hook).
//!   3. After that change, every test in this file turns green without
//!      modification.
//!
//! ## Advisory-lock note
//!
//! Basin does not yet implement `pg_advisory_lock` (it is a stub).  The
//! advisory-lock test is therefore `#[ignore]`'d with a note referencing the
//! stub ticket — it will turn green when both 5.27.E lands AND the advisory-
//! lock primitive is implemented.
//!
//! ## LISTEN note
//!
//! `LISTEN` / `UNLISTEN` is part of Basin's realtime substrate but may not
//! be routable through `ProjectSession::execute` (it may go through a
//! separate realtime channel).  The LISTEN test is `#[ignore]`'d pending
//! confirmation of the routing path; it is documented here for completeness
//! so it is not forgotten.

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

fn pool_config_small() -> PoolConfig {
    // Single-slot pool: guarantees that both checkouts land on the same
    // physical session, which maximises the chance of detecting leakage.
    PoolConfig {
        max_sessions: 1,
        per_project_cap: 1,
        ..Default::default()
    }
}

fn row_count(r: &ExecResult) -> usize {
    match r {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

// ---------------------------------------------------------------------------
// Test A: SET LOCAL must not survive pool return.
//
// RED until 5.27.E.
// ---------------------------------------------------------------------------

/// A `SET LOCAL` issued inside a transaction must not be visible after the
/// transaction commits and the session is returned to the pool.
///
/// **Expected to FAIL until Phase 5.27.E.**
#[tokio::test]
async fn set_local_does_not_survive_pool_return() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    // NOTE: Phase 5.27.E replaces this with a reset-capable pool.
    let pool = SessionPool::new(engine, pool_config_small());

    // Checkout 1: mutate search_path via SET LOCAL.
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        s.execute("BEGIN").await.unwrap();
        s.execute("SET LOCAL search_path = 'leakage_sentinel'").await.unwrap();
        s.execute("COMMIT").await.unwrap();
    }

    // Checkout 2: SHOW search_path must NOT return 'leakage_sentinel'.
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        let result = s.execute("SHOW search_path").await.unwrap();
        let path = match &result {
            ExecResult::Rows { batches, .. } => {
                batches.iter()
                    .flat_map(|b| {
                        b.column(0)
                            .as_any()
                            .downcast_ref::<arrow_array::StringArray>()
                            .map(|a| a.value(0).to_string())
                    })
                    .next()
                    .unwrap_or_default()
            }
            ExecResult::Empty { .. } => String::new(),
        };
        assert!(
            !path.contains("leakage_sentinel"),
            "5.27.E CONTRACT VIOLATION: SET LOCAL search_path leaked across \
             pool return. search_path after return = {path:?}. \
             Implement ProjectSession::reset_for_pool_return() (Phase 5.27.E)."
        );
    }
}

// ---------------------------------------------------------------------------
// Test B: open cursor must not survive pool return.
//
// RED until 5.27.E.
// ---------------------------------------------------------------------------

/// A cursor declared inside a transaction must be gone after the transaction
/// commits and the session returns to the pool.
///
/// **Expected to FAIL until Phase 5.27.E.**
#[tokio::test]
async fn open_cursor_does_not_survive_pool_return() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    // NOTE: Phase 5.27.E: replace with reset-capable pool.
    let pool = SessionPool::new(engine, pool_config_small());

    // Seed data.
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        s.execute("CREATE TABLE leak_rows (id BIGINT)").await.unwrap();
        s.execute("INSERT INTO leak_rows VALUES (1), (2)").await.unwrap();
    }

    // Checkout 1: declare cursor, commit (does NOT close cursor in session-mode).
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        s.execute("BEGIN").await.unwrap();
        s.execute("DECLARE leaked_cur CURSOR FOR SELECT id FROM leak_rows").await.unwrap();
        s.execute("COMMIT").await.unwrap();
        // In txn-mode pool, COMMIT + return should destroy the cursor.
    }

    // Checkout 2: FETCH from leaked_cur must fail (cursor should be gone).
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        let result = s.execute("FETCH 1 FROM leaked_cur").await;
        assert!(
            result.is_err(),
            "5.27.E CONTRACT VIOLATION: cursor 'leaked_cur' survived pool \
             return and is accessible in the next checkout. \
             Implement CLOSE ALL in reset_for_pool_return() (Phase 5.27.E)."
        );
    }
}

// ---------------------------------------------------------------------------
// Test C: PREPARE / prepared statement must not survive pool return.
//
// RED until 5.27.E.
// ---------------------------------------------------------------------------

/// A named prepared statement (`PREPARE foo AS …`) must be deallocated when
/// the session is returned to the pool.
///
/// **`#[ignore]`** — Basin treats SQL-level `PREPARE <name> AS …` and
/// `EXECUTE <name>` as noop-accepts (the real extended-query protocol uses
/// `StatementHandle` UUIDs, not SQL names).  There is no server-side
/// named-statement registry to scrub, so `EXECUTE leaked_stmt` always returns
/// `Ok` regardless of whether the statement was prepared.  This test will flip
/// green when the engine adds a named-SQL-prepared-statement registry
/// (Phase 5.27.E #prepare-stub).
#[tokio::test]
#[ignore = "SQL-level PREPARE/EXECUTE are noop-accepts in Basin; no named-statement registry to scrub (Phase 5.27.E #prepare-stub)"]
async fn prepared_statement_does_not_survive_pool_return() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    // NOTE: Phase 5.27.E: replace with reset-capable pool.
    let pool = SessionPool::new(engine, pool_config_small());

    // Checkout 1: PREPARE a named statement.
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        s.execute("PREPARE leaked_stmt AS SELECT 42").await.unwrap();
        // No explicit DEALLOCATE — the pool's reset must do it.
    }

    // Checkout 2: EXECUTE leaked_stmt must fail (statement should be gone).
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        let result = s.execute("EXECUTE leaked_stmt").await;
        assert!(
            result.is_err(),
            "5.27.E CONTRACT VIOLATION: prepared statement 'leaked_stmt' \
             survived pool return. \
             Implement DEALLOCATE ALL in reset_for_pool_return() (Phase 5.27.E)."
        );
    }
}

// ---------------------------------------------------------------------------
// Test D: LISTEN subscription must not survive pool return.
//
// #[ignore] — LISTEN/UNLISTEN routing through ProjectSession::execute is not
// yet confirmed; routing may go through the realtime substrate instead.
// Flips green at Phase 5.27.E once routing is confirmed.
// ---------------------------------------------------------------------------

/// A LISTEN subscription must be cancelled (UNLISTEN) when the session is
/// returned to the pool.
///
/// **`#[ignore]`** — LISTEN through `ProjectSession::execute` still returns
/// `FeatureNotSupported` (SQLSTATE 0A000) as of 5.27.E. Until the SQL-level
/// LISTEN path is wired into the realtime channel substrate (and the pool
/// reset emits `UNLISTEN *`), this test cannot exercise the leakage scenario.
#[tokio::test]
#[ignore = "LISTEN via ProjectSession::execute returns FeatureNotSupported (SQLSTATE 0A000); \
    SQL-level LISTEN/UNLISTEN is not yet wired to the realtime channel substrate so the pool \
    has nothing to scrub; closes when LISTEN routing lands"]
async fn listen_subscription_does_not_survive_pool_return() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let pool = SessionPool::new(engine, pool_config_small());

    // Checkout 1: subscribe to a channel.
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        s.execute("LISTEN leaked_channel").await.unwrap();
        // No UNLISTEN — pool reset must issue UNLISTEN *.
    }

    // Checkout 2: the subscription must be gone.
    // (Implementation: query pg_listening_channels() or attempt UNLISTEN and
    //  confirm it is a no-op / the channel is not in the active set.)
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        // pg_listening_channels() returns the set of channels this session
        // is listening on. It must be empty.
        let result = s.execute("SELECT * FROM pg_listening_channels()").await.unwrap();
        assert_eq!(
            row_count(&result), 0,
            "5.27.E CONTRACT VIOLATION: LISTEN subscription 'leaked_channel' \
             survived pool return. \
             Implement UNLISTEN * in reset_for_pool_return() (Phase 5.27.E)."
        );
    }
}

// ---------------------------------------------------------------------------
// Test E: advisory lock must not survive pool return.
//
// #[ignore] — pg_advisory_lock is a stub in Basin today.
// Flips green when both 5.27.E (pool reset) and pg_advisory_lock
// implementation land.
// ---------------------------------------------------------------------------

/// An advisory lock taken in one checkout must be released when the session
/// returns to the pool.
///
/// Closed by: 01a53f6 (advisory-lock blocking manager, ADR 0026) — Phase
/// 5.27.E pool reset now calls `pg_advisory_unlock_all()` on session return.
#[tokio::test]
async fn advisory_lock_does_not_survive_pool_return() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let pool = SessionPool::new(engine, pool_config_small());

    // Checkout 1: take an advisory lock.
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        s.execute("SELECT pg_advisory_lock(12345)").await.unwrap();
        // Pool reset must call pg_advisory_unlock_all() before returning.
    }

    // Checkout 2: the lock must be free (another session can take it without
    // blocking, or pg_try_advisory_lock returns true).
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        let result = s.execute("SELECT pg_try_advisory_lock(12345)").await.unwrap();
        // pg_try_advisory_lock returns TRUE if the lock was successfully acquired.
        // If the previous checkout still holds it, this returns FALSE.
        let acquired = match &result {
            ExecResult::Rows { batches, .. } => {
                batches.iter()
                    .flat_map(|b| {
                        b.column(0)
                            .as_any()
                            .downcast_ref::<arrow_array::BooleanArray>()
                            .map(|a| a.value(0))
                    })
                    .next()
                    .unwrap_or(false)
            }
            ExecResult::Empty { .. } => false,
        };
        assert!(
            acquired,
            "5.27.E CONTRACT VIOLATION: advisory lock 12345 was not released \
             by pool reset. Implement pg_advisory_unlock_all() in \
             reset_for_pool_return() (Phase 5.27.E)."
        );
    }
}

// ---------------------------------------------------------------------------
// Test F: temporary table must not survive pool return.
//
// RED until 5.27.E — temp tables are connection-scoped in Postgres; the pool
// reset must DROP all temp tables (or the underlying session must be recycled)
// before re-use by another logical client.
// ---------------------------------------------------------------------------

/// A temporary table created in one checkout must not exist in the next
/// checkout on the same physical connection slot.
///
/// **`#[ignore]`** — Basin's `CREATE TEMP TABLE` goes through the persistent-
/// catalog path (`exec_create_table`); there is no session-scoped temp schema.
/// The catalog entry created by `CREATE TEMP TABLE` survives across all
/// sessions (including the Transaction-mode pool's session-destroy cycle),
/// because the catalog is shared state outside the pool's control.  Cleaning
/// up temp tables requires the engine to (a) track which tables were created
/// with the `TEMPORARY` keyword per-session and (b) drop them at session end
/// or pool return.  Until that tracking exists the pool has no way to enumerate
/// and drop them.  This test will flip green when engine-level temp-table
/// tracking lands (Phase 5.27.E #temp-stub).
#[tokio::test]
#[ignore = "CREATE TEMP TABLE uses the persistent catalog path; pool cannot enumerate temp tables to drop without engine-level tracking (Phase 5.27.E #temp-stub)"]
async fn temp_table_does_not_survive_pool_return() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    // NOTE: Phase 5.27.E: replace with reset-capable pool.
    let pool = SessionPool::new(engine, pool_config_small());

    // Checkout 1: create a temp table.
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        s.execute("CREATE TEMP TABLE leaked_temp (val TEXT)").await.unwrap();
        s.execute("INSERT INTO leaked_temp VALUES ('secret_row')").await.unwrap();
    }

    // Checkout 2: the temp table must not be visible.
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        let result = s.execute("SELECT val FROM leaked_temp").await;
        assert!(
            result.is_err(),
            "5.27.E CONTRACT VIOLATION: temporary table 'leaked_temp' survived \
             pool return and is visible to the next logical client. \
             Implement DROP TABLE / session-recycle in reset_for_pool_return() \
             (Phase 5.27.E)."
        );
    }
}
