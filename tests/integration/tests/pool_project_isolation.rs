//! Phase 5.27.A — per-project isolation under txn-mode pooling.
//!
//! ## What this tests
//!
//! In a *transaction-mode* pool, a single pool of physical connections is
//! shared across ALL projects (logical tenants).  A `SET LOCAL` (or any
//! session-local mutation) that one project performs inside an open
//! transaction MUST NOT survive into another project's checkout on the same
//! physical connection after the first transaction commits.
//!
//! The invariant in Basin's txn-mode pool (Phase 5.27.B–D):
//!   Alice does `BEGIN; SET LOCAL search_path = 'alice_secret'; COMMIT;`
//!   Bob then checks out the SAME physical slot — Bob must see the default
//!   `search_path`, NOT `alice_secret`.
//!
//! ## Simulation approach (Phase 5.27.A harness only)
//!
//! `TxnModePool` does not exist yet.  To simulate cross-project slot sharing
//! we use `SessionPool` with `max_sessions = 1` and the SAME `ProjectId` but
//! different `client_key` values.  The single-slot pool forces both Alice and
//! Bob to share the one physical `ProjectSession`; the current pool has no
//! reset step on return, so Alice's mutation bleeds through.
//!
//! This is a deliberate approximation: in production `TxnModePool` will share
//! slots across different `ProjectId`s (not just different `client_key`s),
//! which is even more dangerous.  The approximation is sufficient to surface
//! the red failure today.
//!
//! ## Red / green status
//!
//! **RED until Phase 5.27.D.**
//!
//! The `SET LOCAL` test below FAILS today: the session-mode pool does not
//! issue `RESET ALL` on return, so Alice's `SET LOCAL search_path` persists
//! into Bob's checkout.
//!
//! Phase 5.27.D must:
//!   1. Extend `TxnModePool` (5.27.B) with a per-checkout `RESET ALL` so
//!      `SET LOCAL` mutations cannot outlive the owning transaction.
//!   2. The parallel-checkouts test must also turn green (no contamination
//!      under concurrent load).

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

/// Single-slot pool: guarantees Alice and Bob share the one physical session.
/// Phase 5.27.D: replace with `TxnModePool` (cross-project slot sharing).
fn pool_config_single_slot() -> PoolConfig {
    PoolConfig {
        max_sessions: 1,
        per_project_cap: 1,
        ..Default::default()
    }
}

/// Extract the first string value from the first column of an ExecResult.
fn first_string(result: &ExecResult) -> Option<String> {
    match result {
        ExecResult::Rows { batches, .. } => {
            for batch in batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                let col = batch.column(0);
                use arrow::array::Array;
                if let Some(s) = col
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                {
                    return Some(s.value(0).to_string());
                }
                return Some(
                    arrow::util::display::array_value_to_string(col, 0)
                        .unwrap_or_default(),
                );
            }
            None
        }
        ExecResult::Empty { .. } => None,
    }
}

// ---------------------------------------------------------------------------
// Test 1: SET LOCAL search_path in "Alice's" txn must not bleed into "Bob's".
//
// Simulation: same ProjectId, single-slot pool, different client_key labels.
//
// RED until 5.27.D — session-mode pool does not reset state between checkouts;
// Alice's SET LOCAL survives into Bob's view of the shared session.
// ---------------------------------------------------------------------------

/// "Alice" sets `search_path` inside her txn on the single shared slot; after
/// she commits and returns the slot, "Bob" must see the default `search_path`.
///
/// **Expected to FAIL until Phase 5.27.D** because `SessionPool` does not
/// reset per-session variables on return-to-pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_local_search_path_does_not_bleed_across_projects() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    // Single project for simulation; TxnModePool (5.27.D) will use separate
    // ProjectIds on a shared physical connection pool.
    let project = ProjectId::new();

    // NOTE: Phase 5.27.D: replace with TxnModePool where physical connections
    // are shared across distinct ProjectIds, not just distinct client_keys.
    let pool = SessionPool::new(engine, pool_config_single_slot());

    // "Alice" checkout: mutate search_path via SET LOCAL.
    // client_key = None so Alice and Bob share the same pool slot (same key).
    // In the future TxnModePool the same physical connection will be shared
    // across distinct ProjectIds; today we simulate it with a shared key.
    {
        let leased = pool
            .acquire(project, None)
            .await
            .unwrap();
        let s = leased.session();
        s.execute("BEGIN").await.unwrap();
        s.execute("SET LOCAL search_path = 'alice_secret'")
            .await
            .unwrap();
        s.execute("COMMIT").await.unwrap();
        // Drop: slot returned to pool. No reset issued (session-mode).
    }

    // "Bob" checkout: must get the same physical slot (same key, single slot)
    // but must NOT see Alice's search_path.
    {
        let leased = pool
            .acquire(project, None)
            .await
            .unwrap();
        let s = leased.session();
        s.execute("BEGIN").await.unwrap();
        let result = s.execute("SHOW search_path").await;
        s.execute("COMMIT").await.unwrap();

        match result {
            Ok(r) => {
                let path = first_string(&r).unwrap_or_default();
                assert!(
                    !path.contains("alice_secret"),
                    "5.27.D CONTRACT VIOLATION: Bob sees search_path={path:?} \
                     which contains Alice's private schema 'alice_secret'. \
                     This is a cross-project isolation breach. \
                     Implement RESET ALL in TxnModePool checkout (Phase 5.27.D)."
                );
            }
            Err(e) => {
                panic!("SHOW search_path failed for Bob: {e}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Test 2: 20 rounds of interleaved checkouts; no contamination across rounds.
//
// RED until 5.27.D (same root cause).
// ---------------------------------------------------------------------------

/// 20 rounds of interleaved Alice / Bob checkouts on a single shared slot;
/// Bob's `search_path` must never reflect Alice's value.
///
/// **Expected to FAIL until Phase 5.27.D.**
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_checkouts_no_search_path_cross_contamination() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();

    // NOTE: Phase 5.27.D: replace with TxnModePool.
    let pool = Arc::new(SessionPool::new(engine, pool_config_single_slot()));

    // Run rounds sequentially so Alice always finishes before Bob on the
    // same slot — this maximises the probability of observing the leak.
    for round in 0u32..20 {
        let schema = format!("alice_round_{round}");

        // Alice: set a unique-per-round schema name.
        // Both Alice and Bob use client_key = None so they share the slot.
        {
            let leased = pool
                .acquire(project, None)
                .await
                .unwrap();
            let s = leased.session();
            s.execute("BEGIN").await.unwrap();
            s.execute(&format!("SET LOCAL search_path = '{schema}'"))
                .await
                .unwrap();
            s.execute("COMMIT").await.unwrap();
        }

        // Bob: immediately after Alice on the same single slot.
        {
            let leased = pool
                .acquire(project, None)
                .await
                .unwrap();
            let s = leased.session();
            s.execute("BEGIN").await.unwrap();
            let result = s.execute("SHOW search_path").await;
            s.execute("COMMIT").await.unwrap();

            if let Ok(r) = result {
                let path = first_string(&r).unwrap_or_default();
                assert!(
                    !path.contains("alice_round_"),
                    "5.27.D CONTRACT VIOLATION (round {round}): Bob sees \
                     search_path={path:?} containing Alice's schema '{schema}'. \
                     Implement RESET ALL in TxnModePool checkout (Phase 5.27.D)."
                );
            }
        }
    }
}
