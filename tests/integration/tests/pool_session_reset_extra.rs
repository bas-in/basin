//! Phase 5.27.E — supplementary pool-reset coverage.
//!
//! The `pool_session_leakage.rs` battery (5.27.A) predates two pieces of
//! session state that landed later and are therefore NOT covered there:
//!
//!   1. **Session-scoped advisory locks** — the blocking advisory-lock manager
//!      (ADR 0026) tracks locks per session. Pool checkin is NOT session end,
//!      so releasing them must happen in `reset_for_pool_reuse` explicitly
//!      (not via `Drop for SessionState`). The 5.27.A advisory test exercises
//!      the SQL `pg_try_advisory_lock` path; this file additionally asserts the
//!      release happens through the *native pool checkout scrub* even when the
//!      lock is taken via the pool's `execute` wrapper, and that a second
//!      logical client on the same physical slot sees the lock free.
//!
//!   2. **pg_trgm threshold GUCs** — `pg_trgm.similarity_threshold` /
//!      `pg_trgm.word_similarity_threshold` are AtomicU32 session GUCs added
//!      after 5.27.A. A `SET pg_trgm.similarity_threshold = …` in one checkout
//!      must not survive into the next. `RESET ALL` (and the checkout scrub)
//!      must restore the basin-trgm defaults.
//!
//! These tests use the same single-slot Session-mode pool the 5.27.A battery
//! uses, so both checkouts land on the same physical `ProjectSession` and any
//! leak is observable.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_pool::{PoolConfig, SessionPool};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

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
    PoolConfig {
        max_sessions: 1,
        per_project_cap: 1,
        ..Default::default()
    }
}

fn first_bool(r: &ExecResult) -> Option<bool> {
    match r {
        ExecResult::Rows { batches, .. } => batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::BooleanArray>()
                    .map(|a| a.value(0))
            })
            .next(),
        ExecResult::Empty { .. } => None,
    }
}

fn first_string(r: &ExecResult) -> String {
    match r {
        ExecResult::Rows { batches, .. } => batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .map(|a| a.value(0).to_string())
            })
            .next()
            .unwrap_or_default(),
        ExecResult::Empty { .. } => String::new(),
    }
}

// ---------------------------------------------------------------------------
// Advisory lock released by the native pool checkout scrub.
// ---------------------------------------------------------------------------

/// A session-scoped advisory lock taken in checkout 1 must be released by the
/// pool's checkout scrub before checkout 2 (same physical slot) — proving the
/// release does NOT rely on `Drop for SessionState` (pool checkin is not
/// session end).
#[tokio::test]
async fn advisory_lock_released_by_checkout_scrub() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let pool = SessionPool::new(engine, pool_config_small());

    // Checkout 1: take a session-scoped advisory lock, leave it held.
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let r = leased
            .session()
            .execute("SELECT pg_advisory_lock(987654321)")
            .await
            .unwrap();
        // pg_advisory_lock returns void/NULL; just assert it didn't error.
        let _ = r;
    }

    // Checkout 2: same physical slot. The lock must be free.
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let r = leased
            .session()
            .execute("SELECT pg_try_advisory_lock(987654321)")
            .await
            .unwrap();
        assert_eq!(
            first_bool(&r),
            Some(true),
            "advisory lock 987654321 was not released by the pool checkout scrub; \
             reset_for_pool_reuse() must call release_all_on_session_end()"
        );
    }
}

// ---------------------------------------------------------------------------
// pg_trgm threshold GUC does not survive pool return.
// ---------------------------------------------------------------------------

/// `SET pg_trgm.similarity_threshold = 0.9` in checkout 1 must not survive into
/// checkout 2; the scrub must restore the basin-trgm default (0.3).
#[tokio::test]
async fn pg_trgm_threshold_does_not_survive_pool_return() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let pool = SessionPool::new(engine, pool_config_small());

    // Checkout 1: bump the similarity threshold away from the default.
    {
        let leased = pool.acquire(project, None).await.unwrap();
        leased
            .session()
            .execute("SET pg_trgm.similarity_threshold = 0.9")
            .await
            .unwrap();
        let shown = leased
            .session()
            .execute("SHOW pg_trgm.similarity_threshold")
            .await
            .unwrap();
        assert!(
            first_string(&shown).starts_with("0.9"),
            "sanity: SET pg_trgm.similarity_threshold should be visible in the same checkout, got {:?}",
            first_string(&shown)
        );
    }

    // Checkout 2: same physical slot. The threshold must be back to the default.
    {
        let leased = pool.acquire(project, None).await.unwrap();
        let shown = leased
            .session()
            .execute("SHOW pg_trgm.similarity_threshold")
            .await
            .unwrap();
        let val = first_string(&shown);
        assert!(
            !val.starts_with("0.9"),
            "pg_trgm.similarity_threshold = 0.9 leaked across pool return: {val:?}. \
             reset_for_pool_reuse() must reset every session GUC (reset_gucs())."
        );
    }
}

// ---------------------------------------------------------------------------
// statement_timeout GUC does not survive pool return.
// ---------------------------------------------------------------------------

/// `SET statement_timeout = '7s'` in checkout 1 must not survive into checkout
/// 2 — guards the GUC-reset path for a non-trgm timeout knob.
#[tokio::test]
async fn statement_timeout_does_not_survive_pool_return() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let pool = SessionPool::new(engine, pool_config_small());

    {
        let leased = pool.acquire(project, None).await.unwrap();
        leased
            .session()
            .execute("SET statement_timeout = '7s'")
            .await
            .unwrap();
    }

    {
        let leased = pool.acquire(project, None).await.unwrap();
        let shown = leased
            .session()
            .execute("SHOW statement_timeout")
            .await
            .unwrap();
        let val = first_string(&shown);
        assert!(
            !val.contains("7s") && !val.starts_with("7000"),
            "statement_timeout = '7s' leaked across pool return: {val:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// DISCARD ALL as a SQL statement performs the real reset.
// ---------------------------------------------------------------------------

/// `DISCARD ALL` issued as SQL (pgbouncer-style server_reset_query) must
/// perform the real engine reset: a search_path set before it must be gone
/// afterwards, within the SAME checkout.
///
/// NOTE: this test depends on the executor-side DISCARD/RESET interception
/// (item 3). If that diff is not applied it will fail; see APPLY.md.
#[tokio::test]
async fn discard_all_sql_resets_search_path() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let pool = SessionPool::new(engine, pool_config_small());

    let leased = pool.acquire(project, None).await.unwrap();
    let s = leased.session();
    s.execute("SET search_path = 'discard_sentinel'").await.unwrap();
    s.execute("DISCARD ALL").await.unwrap();
    let shown = s.execute("SHOW search_path").await.unwrap();
    assert!(
        !first_string(&shown).contains("discard_sentinel"),
        "DISCARD ALL did not reset search_path: {:?}",
        first_string(&shown)
    );
}
