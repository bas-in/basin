//! Correctness guard for the process-global PK → Row hot cache
//! (`crate::pk_row_cache`, gated behind `BASIN_PK_ROW_CACHE=1`).
//!
//! This cache is in the RLS-bypass bug family (#159): a stale entry silently
//! serves deleted / old / RLS-bypassed rows. Every test here runs with the
//! flag ON and pins the dual-watermark invalidation (hot_tier_epoch +
//! snapshot_id) end-to-end through a real `Engine`:
//!
//! - `pk_cache_hit_returns_correct_row` — warm + second read hits, correct value.
//! - `pk_cache_invalidated_by_update` — UPDATE bumps the hot epoch; next read
//!   sees the NEW value, not the cached one.
//! - `pk_cache_invalidated_by_delete` — DELETE → row is GONE, not the stale
//!   cached row. THE canary.
//! - `pk_cache_invalidated_by_cold_write` — a bulk/range rewrite that commits a
//!   new snapshot invalidates the entry.
//! - `pk_cache_bypassed_under_rls` — an RLS table must never serve a cached row
//!   that bypasses the policy predicate. THE other canary.
//! - `pk_cache_ddl_invalidates` — ALTER TABLE clears the cache.
//!
//! The flag is set per process at the top of every test (cargo runs them in the
//! same binary; they all want it ON). The cache reads the flag fresh per query.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardBackgroundHandle, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Set the cache flag ON for this process. Idempotent; safe under concurrent
/// test execution (all tests in this file want it ON).
fn enable_cache() {
    std::env::set_var("BASIN_PK_ROW_CACHE", "1");
}

/// Shard-backed engine so rows can be flushed into cold Parquet — the cache
/// only fires on cold-tier point lookups (a hot memtable hit returns earlier).
async fn build() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    ShardBackgroundHandle,
    Arc<dyn Wal>,
) {
    let sd = TempDir::new().unwrap();
    let wd = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(sd.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wd.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal)
}

async fn int_at(sess: &basin_engine::ProjectSession, sql: &str) -> Option<i64> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            if total == 0 {
                return None;
            }
            let b = batches.iter().find(|b| b.num_rows() > 0).unwrap();
            Some(
                b.column_by_name("v")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
            )
        }
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty({tag})"),
    }
}

async fn rows_seen(sess: &basin_engine::ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty({tag})"),
    }
}

/// 1) Warm the cache, then a second identical read HITS and returns the correct
///    value (and the hit counter advances).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pk_cache_hit_returns_correct_row() {
    enable_cache();
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 100), (2, 200)")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap(); // rows now cold-only

    // First read: cache MISS → cold decode → populate.
    assert_eq!(int_at(&sess, "SELECT v FROM t WHERE id = 1").await, Some(100));
    let after_first = engine.pk_row_cache_counters();
    assert!(
        after_first.inserts >= 1,
        "first read should populate the cache; inserts={}",
        after_first.inserts
    );

    // Second identical read: cache HIT, same value.
    assert_eq!(int_at(&sess, "SELECT v FROM t WHERE id = 1").await, Some(100));
    let after_second = engine.pk_row_cache_counters();
    assert!(
        after_second.hits >= 1,
        "second read should hit the cache; hits={}",
        after_second.hits
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// 2) An UPDATE bumps the hot_tier_epoch; the next read MUST see the new value,
///    not the cached stale one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pk_cache_invalidated_by_update() {
    enable_cache();
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 10)").await.unwrap();
    shard.flush_to_parquet().await.unwrap();

    assert_eq!(int_at(&sess, "SELECT v FROM t WHERE id = 1").await, Some(10)); // warm
    sess.execute("UPDATE t SET v = 99 WHERE id = 1").await.unwrap(); // hot overlay → epoch bump
    assert_eq!(
        int_at(&sess, "SELECT v FROM t WHERE id = 1").await,
        Some(99),
        "after UPDATE the cache must be invalidated and serve the NEW value"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// 3) THE canary: a DELETE writes a memtable tombstone (no catalog snapshot
///    advance). The cached row must NOT be served — the row must be GONE.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pk_cache_invalidated_by_delete() {
    enable_cache();
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();

    // Warm the cache for id=1.
    assert_eq!(int_at(&sess, "SELECT v FROM t WHERE id = 1").await, Some(10));

    // DELETE id=1 (fast-path tombstone; bumps hot_tier_epoch).
    sess.execute("DELETE FROM t WHERE id = 1").await.unwrap();

    // The row MUST be gone — never the stale cached (1, 10).
    assert_eq!(
        int_at(&sess, "SELECT v FROM t WHERE id = 1").await,
        None,
        "DELETE must invalidate the cache — a stale cached row here is the #159 bug"
    );
    // id=2 untouched.
    assert_eq!(int_at(&sess, "SELECT v FROM t WHERE id = 2").await, Some(20));

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// 4) A range/bulk UPDATE takes the cold copy-on-write path, rewriting files
///    and committing a NEW snapshot. The snapshot watermark must invalidate the
///    cached entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pk_cache_invalidated_by_cold_write() {
    enable_cache();
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    // Shut the background flusher so we control flushing; range UPDATE still
    // rewrites cold files synchronously.
    bg.shutdown().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();

    // Warm id=1.
    assert_eq!(int_at(&sess, "SELECT v FROM t WHERE id = 1").await, Some(10));

    // Range UPDATE (predicate, not PK-eq literal) → cold copy-on-write rewrite +
    // new snapshot commit. This touches id=1.
    sess.execute("UPDATE t SET v = v + 1000 WHERE id < 3")
        .await
        .unwrap();

    assert_eq!(
        int_at(&sess, "SELECT v FROM t WHERE id = 1").await,
        Some(1010),
        "cold-tier rewrite committed a new snapshot; the cache must serve the NEW value"
    );

    wal.close().await.unwrap();
}

/// 5) THE other canary: an RLS-enabled table must NEVER serve a cached row that
///    bypasses the policy predicate. Even with the cache flag ON, a principal
///    must only see their own rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pk_cache_bypassed_under_rls() {
    enable_cache();
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let admin = engine.open_session(ProjectId::new()).await.unwrap();
    let project = admin.project();
    admin
        .execute("CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, owner_id TEXT NOT NULL, v BIGINT)")
        .await
        .unwrap();
    // Alice owns id=1, Bob owns id=2.
    admin
        .execute("INSERT INTO orders VALUES (1, 'alice', 11), (2, 'bob', 22)")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();

    let alice = engine.open_session_as(project, "alice").await.unwrap();
    let bob = engine.open_session_as(project, "bob").await.unwrap();

    // BEFORE RLS: warm the cache with the raw rows via point lookups (no policy).
    assert_eq!(rows_seen(&alice, "SELECT v FROM orders WHERE id = 1").await, 1);
    assert_eq!(rows_seen(&alice, "SELECT v FROM orders WHERE id = 2").await, 1);

    // Enable RLS with owner_id = current_user.
    admin
        .execute("ALTER TABLE orders ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin
        .execute(
            "CREATE POLICY orders_owner ON orders FOR ALL TO PUBLIC USING (owner_id = current_user)",
        )
        .await
        .unwrap();

    // Alice may see id=1 (hers) but MUST NOT see id=2 (Bob's) — the cache must
    // not serve the raw row it warmed before RLS was enabled.
    assert_eq!(
        rows_seen(&alice, "SELECT v FROM orders WHERE id = 2").await,
        0,
        "RLS-bypass: alice must NOT see bob's row (id=2) even though the cache warmed it pre-RLS"
    );
    assert_eq!(
        rows_seen(&alice, "SELECT v FROM orders WHERE id = 1").await,
        1,
        "alice still sees her own row under RLS"
    );
    // Bob: symmetric.
    assert_eq!(
        rows_seen(&bob, "SELECT v FROM orders WHERE id = 1").await,
        0,
        "RLS-bypass: bob must NOT see alice's row (id=1)"
    );
    assert_eq!(rows_seen(&bob, "SELECT v FROM orders WHERE id = 2").await, 1);

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// 6) DDL (ALTER TABLE) must invalidate the cache so a pre-change row is never
///    served. We warm the cache, run an ALTER (metadata DDL that may NOT advance
///    the snapshot), and assert the cache was CLEARED by the central
///    `invalidate_project` hook (current_bytes drops to 0). This pins the DDL
///    invalidation wiring directly, independent of any post-ALTER read path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pk_cache_ddl_invalidates() {
    enable_cache();
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 10)").await.unwrap();
    shard.flush_to_parquet().await.unwrap();

    // Warm — the cache now holds id=1's cold row for THIS project.
    assert_eq!(int_at(&sess, "SELECT v FROM t WHERE id = 1").await, Some(10));
    assert_eq!(
        engine.pk_row_cache_entries(project),
        1,
        "cache must hold exactly this project's warmed row"
    );

    // ALTER: add a column. Metadata DDL that may not advance current_snapshot —
    // the explicit invalidate_project hook on the non-SELECT statement MUST
    // clear the PK row cache for this project. Project-scoped count avoids any
    // interference from other tests sharing the process-global cache.
    sess.execute("ALTER TABLE t ADD COLUMN w BIGINT")
        .await
        .unwrap();

    assert_eq!(
        engine.pk_row_cache_entries(project),
        0,
        "ALTER TABLE must invalidate (clear) this project's PK row cache entries"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}
