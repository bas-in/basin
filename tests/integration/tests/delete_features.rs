//! End-to-end coverage for two DELETE features that previously GAPped on the
//! benchmark card:
//!
//!   1. Range DELETE — `DELETE FROM t WHERE id BETWEEN lo AND hi`, plus the
//!      `>` / `<` / compound (`> AND <`) range shapes. These flow through the
//!      cold copy-on-write rewrite (NOT the pk-eq/IN tombstone fast path); the
//!      rewrite re-evaluates the predicate per row, so it works against
//!      flushed (cold Parquet) and unflushed (hot memtable) data and respects
//!      a prior fast-path UPDATE overlay.
//!
//!   2. FOREIGN KEY ... ON DELETE CASCADE — deleting a parent row deletes the
//!      referencing child rows (and grandchildren, via natural recursion when
//!      the cascade DELETE re-enters the same path). NO ACTION (the default,
//!      and what RESTRICT also maps to today) instead rejects the parent
//!      delete with a foreign-key-violation error, matching Postgres 23503.
//!
//! These tests use the real engine (LocalFileSystem storage + InMemoryCatalog
//! + a Shard with background flush) so the cold/hot tiering is exercised for
//! real — `flush_to_parquet()` forces seeded rows cold to Parquet.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::Int64Array;
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Build a real engine: LocalFileSystem storage + InMemoryCatalog + a Shard
/// with background flush. Returns the Shard so a test can `flush_to_parquet()`
/// to force seeded data cold, and the bg handle / wal so the test can shut the
/// background loop down cleanly.
async fn build() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
) {
    let sd = TempDir::new().unwrap();
    let wd = TempDir::new().unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
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
            commit_delay: Duration::from_millis(2),
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

/// Run `SELECT COUNT(*) FROM <where>` and return the scalar count.
async fn count(sess: &basin_engine::ProjectSession, from_where: &str) -> i64 {
    match sess
        .execute(&format!("SELECT COUNT(*) FROM {from_where}"))
        .await
        .unwrap()
    {
        ExecResult::Rows { batches, .. } => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1, "COUNT(*) must return exactly one row");
            batches
                .first()
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("COUNT(*) column must be Int64")
                .value(0)
        }
        other => panic!("COUNT(*) returned non-Rows: {other:?}"),
    }
}

/// Seed `[0, n)` as `(id, v=id)` rows in one multi-VALUES INSERT.
async fn seed(sess: &basin_engine::ProjectSession, table: &str, n: i64) {
    let mut stmt = String::with_capacity(n as usize * 16);
    stmt.push_str(&format!("INSERT INTO {table} VALUES "));
    for k in 0..n {
        if k > 0 {
            stmt.push(',');
        }
        stmt.push_str(&format!("({k},{k})"));
    }
    sess.execute(&stmt).await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// GAP 1 — range DELETE
// ─────────────────────────────────────────────────────────────────────────────

/// `DELETE … WHERE id BETWEEN lo AND hi` removes exactly the inclusive range
/// against cold Parquet data, and only that range.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_delete_between_cold() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    seed(&sess, "t", 1000).await;
    // Force everything cold so the DELETE exercises the copy-on-write rewrite.
    shard.flush_to_parquet().await.unwrap();

    // BETWEEN is inclusive on both ends: 100..=400 → 301 rows.
    let res = sess
        .execute("DELETE FROM t WHERE id BETWEEN 100 AND 400")
        .await
        .unwrap();
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "DELETE 301"),
        other => panic!("DELETE returned non-Empty: {other:?}"),
    }

    assert_eq!(count(&sess, "t").await, 699, "total after range delete");
    assert_eq!(
        count(&sess, "t WHERE id BETWEEN 100 AND 400").await,
        0,
        "no rows remain inside the deleted range"
    );
    assert_eq!(
        count(&sess, "t WHERE id < 100").await,
        100,
        "rows below the range are untouched"
    );
    assert_eq!(
        count(&sess, "t WHERE id > 400").await,
        599,
        "rows above the range are untouched"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// Open-ended `>` and `<` range deletes, and a compound `> AND <` range.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_delete_gt_lt_and_compound() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // `>` : delete the tail.
    sess.execute("CREATE TABLE a (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    seed(&sess, "a", 500).await;
    shard.flush_to_parquet().await.unwrap();
    sess.execute("DELETE FROM a WHERE id > 400").await.unwrap();
    // ids 401..=499 → 99 rows removed.
    assert_eq!(count(&sess, "a").await, 401, "> deletes the strict tail");
    assert_eq!(count(&sess, "a WHERE id > 400").await, 0);

    // `<` : delete the head.
    sess.execute("CREATE TABLE b (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    seed(&sess, "b", 500).await;
    shard.flush_to_parquet().await.unwrap();
    sess.execute("DELETE FROM b WHERE id < 100").await.unwrap();
    // ids 0..=99 → 100 rows removed.
    assert_eq!(count(&sess, "b").await, 400, "< deletes the strict head");
    assert_eq!(count(&sess, "b WHERE id < 100").await, 0);

    // Compound `> AND <` : delete a strict open interval (200, 300).
    sess.execute("CREATE TABLE c (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    seed(&sess, "c", 500).await;
    shard.flush_to_parquet().await.unwrap();
    sess.execute("DELETE FROM c WHERE id > 200 AND id < 300")
        .await
        .unwrap();
    // ids 201..=299 → 99 rows removed.
    assert_eq!(count(&sess, "c").await, 401, "open interval delete");
    assert_eq!(count(&sess, "c WHERE id > 200 AND id < 300").await, 0);
    assert_eq!(
        count(&sess, "c WHERE id = 200 OR id = 300").await,
        2,
        "interval endpoints survive (strict bounds)"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// Overlay interplay: a row updated through the hot-tier fast path and then
/// caught by a range DELETE must be gone (the cold rewrite must see the
/// materialized overlay, not the stale base value). Also exercises mixed
/// flushed + unflushed rows in the same range.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_delete_overlay_and_mixed_tiers() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    // Seed 0..500 and flush cold.
    seed(&sess, "t", 500).await;
    shard.flush_to_parquet().await.unwrap();

    // Fast-path UPDATE (pk = lit) on a row that sits inside the range we will
    // later delete. This writes a hot-tier overlay over the cold base row.
    sess.execute("UPDATE t SET v = 9999 WHERE id = 250")
        .await
        .unwrap();
    // Sanity: the overlay is visible.
    assert_eq!(
        count(&sess, "t WHERE id = 250 AND v = 9999").await,
        1,
        "fast-path UPDATE overlay is visible before the range delete"
    );

    // Insert 500..600 WITHOUT flushing — these live in the hot memtable.
    seed_range(&sess, "t", 500, 600).await;

    // Range delete spanning cold rows (0..500), the overlaid row (250), and
    // unflushed hot rows (500..600). BETWEEN 200 AND 550 inclusive.
    sess.execute("DELETE FROM t WHERE id BETWEEN 200 AND 550")
        .await
        .unwrap();

    // The overlaid row 250 must be gone — not resurrected from the stale base.
    assert_eq!(
        count(&sess, "t WHERE id = 250").await,
        0,
        "fast-path-updated row inside the range is deleted"
    );
    assert_eq!(
        count(&sess, "t WHERE id BETWEEN 200 AND 550").await,
        0,
        "entire range (cold + overlaid + hot) is removed"
    );
    // Surviving rows: 0..=199 (200) and 551..=599 (49) = 249.
    assert_eq!(count(&sess, "t").await, 249, "survivors across both tiers");
    assert_eq!(count(&sess, "t WHERE id < 200").await, 200);
    assert_eq!(count(&sess, "t WHERE id > 550").await, 49);

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// Seed `[lo, hi)` as `(id, v=id)` rows.
async fn seed_range(sess: &basin_engine::ProjectSession, table: &str, lo: i64, hi: i64) {
    let mut stmt = String::new();
    stmt.push_str(&format!("INSERT INTO {table} VALUES "));
    for k in lo..hi {
        if k > lo {
            stmt.push(',');
        }
        stmt.push_str(&format!("({k},{k})"));
    }
    sess.execute(&stmt).await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// GAP 2 — FOREIGN KEY ... ON DELETE CASCADE
// ─────────────────────────────────────────────────────────────────────────────

/// Deleting a parent row CASCADEs to its children and, via natural recursion,
/// to grandchildren. Child rows live both cold (flushed) and hot (unflushed)
/// to prove the cascade scan sees both tiers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cascade_delete_children_and_grandchildren() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE parent (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    sess.execute(
        "CREATE TABLE child (id BIGINT PRIMARY KEY, parent_id BIGINT \
         REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .await
    .unwrap();
    sess.execute(
        "CREATE TABLE grandchild (id BIGINT PRIMARY KEY, child_id BIGINT \
         REFERENCES child(id) ON DELETE CASCADE)",
    )
    .await
    .unwrap();

    // parent 1 and 2.
    sess.execute("INSERT INTO parent VALUES (1),(2)")
        .await
        .unwrap();
    // children 10,11 → parent 1 ; child 20 → parent 2.
    sess.execute("INSERT INTO child VALUES (10,1),(11,1),(20,2)")
        .await
        .unwrap();
    // grandchildren under child 10 and 11 (parent 1's subtree).
    sess.execute("INSERT INTO grandchild VALUES (100,10),(101,10),(110,11)")
        .await
        .unwrap();
    // Flush the parent-1 subtree cold...
    shard.flush_to_parquet().await.unwrap();
    // ...then add a hot (unflushed) grandchild under child 20 (parent 2).
    sess.execute("INSERT INTO grandchild VALUES (200,20)")
        .await
        .unwrap();

    // Delete parent 1: cascade should remove children 10,11 and grandchildren
    // 100,101,110 — but leave parent 2's subtree intact.
    let res = sess.execute("DELETE FROM parent WHERE id = 1").await.unwrap();
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "DELETE 1", "one parent row deleted"),
        other => panic!("DELETE returned non-Empty: {other:?}"),
    }

    assert_eq!(count(&sess, "parent").await, 1, "parent 2 survives");
    assert_eq!(count(&sess, "parent WHERE id = 1").await, 0);
    assert_eq!(
        count(&sess, "child").await,
        1,
        "only child 20 (parent 2) survives"
    );
    assert_eq!(count(&sess, "child WHERE parent_id = 1").await, 0);
    assert_eq!(
        count(&sess, "grandchild").await,
        1,
        "only grandchild 200 (under child 20) survives — incl. the hot one"
    );
    assert_eq!(count(&sess, "grandchild WHERE id = 200").await, 1);

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// CASCADE on a parent with zero children is a no-op cascade: the parent is
/// deleted and nothing else changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cascade_delete_zero_children() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE parent (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    sess.execute(
        "CREATE TABLE child (id BIGINT PRIMARY KEY, parent_id BIGINT \
         REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO parent VALUES (1),(2)")
        .await
        .unwrap();
    // child references parent 2 only — parent 1 has no children.
    sess.execute("INSERT INTO child VALUES (20,2)").await.unwrap();
    shard.flush_to_parquet().await.unwrap();

    let res = sess.execute("DELETE FROM parent WHERE id = 1").await.unwrap();
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "DELETE 1"),
        other => panic!("DELETE returned non-Empty: {other:?}"),
    }
    assert_eq!(count(&sess, "parent").await, 1, "parent 2 survives");
    assert_eq!(count(&sess, "child").await, 1, "child untouched (0 children)");

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// A non-CASCADE FK (default NO ACTION) rejects deleting a parent that still
/// has children, matching Postgres 23503. This documents the current
/// semantics: NO ACTION (and RESTRICT, which maps to NO ACTION today) error
/// rather than orphan or silently delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_cascade_fk_rejects_parent_delete() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE parent (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    // No ON DELETE clause → NO ACTION.
    sess.execute(
        "CREATE TABLE child (id BIGINT PRIMARY KEY, parent_id BIGINT REFERENCES parent(id))",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO parent VALUES (1)").await.unwrap();
    sess.execute("INSERT INTO child VALUES (10,1)")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();

    let err = sess
        .execute("DELETE FROM parent WHERE id = 1")
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("foreign key") || msg.contains("still referenced"),
        "NO ACTION parent delete must be rejected (PG 23503); got: {msg}"
    );
    // Parent and child both survive — the delete was rejected, not partial.
    assert_eq!(count(&sess, "parent").await, 1, "parent survives the reject");
    assert_eq!(count(&sess, "child").await, 1, "child survives the reject");

    bg.shutdown().await;
    wal.close().await.unwrap();
}
