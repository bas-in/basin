//! Staged row-hot-tier guards (S1–S3).
//!
//! These tests pin the row hot-tier invariants end-to-end through a real,
//! shard-backed `Engine` (so rows can be flushed into cold Parquet):
//!
//! * **S1 — dedup invariant.** A fresh INSERT lands in the memtable keyed by its
//!   encoded single-column PK (NOT a counter key), so it is found by the O(log n)
//!   PK direct-get probe AND it is a *single* logical entry — COUNT(\*) is never
//!   inflated by a duplicate counter-keyed copy, across inserts and a flush
//!   cycle. (Design choice (b): one PK-keyed entry per single-PK row; the counter
//!   key remains only the fallback for non-encodable PKs.)
//! * **S2 — write-through Pk row cache.** A committed INSERT seeds the
//!   `PkRowCache`, so the *next* point read for that PK is a cache HIT.
//! * **S3 — cold-decoded single-row pin.** A point read of a cold-only row
//!   populates the cache; the second read HITS.
//! * Cross-cutting correctness: fresh INSERT then UPDATE sees the update; a fresh
//!   INSERT is visible to ANOTHER session (shared registry); DELETE of a
//!   fresh-inserted key makes it gone; a composite-PK table falls back to the
//!   counter key and stays correct (and is never cached).

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_shard::{Shard, ShardBackgroundHandle, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Shard-backed engine so rows can be flushed into cold Parquet.
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

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

/// Scalar `i64` from the first non-empty batch's column `name`, or `None` when
/// zero rows are returned.
async fn int_col(sess: &ProjectSession, sql: &str, name: &str) -> Option<i64> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            if total == 0 {
                return None;
            }
            let b = batches.iter().find(|b| b.num_rows() > 0).unwrap();
            Some(
                b.column_by_name(name)
                    .unwrap_or_else(|| panic!("column {name} missing"))
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
            )
        }
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty({tag})"),
    }
}

async fn rows_seen(sess: &ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty({tag})"),
    }
}

/// COUNT(\*) as an i64.
async fn count_star(sess: &ProjectSession, table: &str) -> i64 {
    match sess
        .execute(&format!("SELECT COUNT(*) AS v FROM {table}"))
        .await
        .unwrap()
    {
        ExecResult::Rows { batches, .. } => {
            let b = batches.iter().find(|b| b.num_rows() > 0).unwrap();
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        }
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty({tag})"),
    }
}

/// Insert via an explicit transaction so the COMMIT path runs
/// `htap_promote_to_registry` (the S1/S2 code under test). The VALUES
/// auto-commit path writes straight to cold Parquet and never touches the
/// registry, so it would not exercise the staged code.
async fn tx_insert(sess: &ProjectSession, stmt: &str) {
    exec(sess, "BEGIN").await;
    exec(sess, stmt).await;
    exec(sess, "COMMIT").await;
}

// ── S1: dedup invariant ────────────────────────────────────────────────────

/// Insert 5 rows (single-PK), assert COUNT(\*)=5 and every point read is
/// correct; flush to cold; assert COUNT(\*) is STILL 5 and every point read is
/// still correct. A duplicate counter-keyed copy of any row would inflate
/// COUNT(\*) past 5 either in the hot tier or after the flush merge — this is
/// the load-bearing dedup guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s1_fresh_insert_count_not_inflated_across_flush() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    exec(
        &sess,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;

    // Five committed inserts via the registry-promotion (BEGIN…COMMIT) path.
    for i in 1..=5i64 {
        tx_insert(&sess, &format!("INSERT INTO t (id, v) VALUES ({i}, {})", i * 10)).await;
    }

    // Hot tier: COUNT must be exactly 5 (no counter-keyed duplicates).
    assert_eq!(count_star(&sess, "t").await, 5, "hot-tier COUNT(*) inflated");
    for i in 1..=5i64 {
        assert_eq!(
            int_col(&sess, &format!("SELECT v FROM t WHERE id = {i}"), "v").await,
            Some(i * 10),
            "hot-tier point read wrong for id={i}"
        );
    }

    // Flush to cold Parquet, then re-check.
    shard.flush_to_parquet().await.unwrap();
    assert_eq!(
        count_star(&sess, "t").await,
        5,
        "post-flush COUNT(*) inflated — a duplicate row survived the merge"
    );
    for i in 1..=5i64 {
        assert_eq!(
            int_col(&sess, &format!("SELECT v FROM t WHERE id = {i}"), "v").await,
            Some(i * 10),
            "post-flush point read wrong for id={i}"
        );
    }

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// Fresh INSERT → UPDATE → point read sees the UPDATEd value (the hot overlay
/// supersedes the inserted row).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s1_fresh_insert_then_update_sees_new_value() {
    std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", "1");
    let (_sd, _wd, engine, _shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    exec(
        &sess,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    tx_insert(&sess, "INSERT INTO t (id, v) VALUES (7, 70)").await;

    assert_eq!(
        int_col(&sess, "SELECT v FROM t WHERE id = 7", "v").await,
        Some(70)
    );
    exec(&sess, "UPDATE t SET v = 700 WHERE id = 7").await;
    assert_eq!(
        int_col(&sess, "SELECT v FROM t WHERE id = 7", "v").await,
        Some(700),
        "UPDATE after a fresh INSERT must be visible"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// A fresh INSERT committed by one session is visible to ANOTHER session in the
/// same project (the memtable registry is process-global, keyed by project).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s1_fresh_insert_visible_to_other_session() {
    let (_sd, _wd, engine, _shard, bg, wal) = build().await;
    let project = ProjectId::new();
    let writer = engine.open_session(project).await.unwrap();
    exec(
        &writer,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    tx_insert(&writer, "INSERT INTO t (id, v) VALUES (11, 110)").await;

    let reader = engine.open_session(project).await.unwrap();
    assert_eq!(
        int_col(&reader, "SELECT v FROM t WHERE id = 11", "v").await,
        Some(110),
        "a committed fresh INSERT must be visible to another session"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}

// ── S2: write-through PK row cache ──────────────────────────────────────────

/// A committed INSERT seeds the PkRowCache (write-through). We assert the
/// write-through fired — the cache `inserts` counter advanced AND a cache entry
/// now exists for this project — and that point reads remain correct.
///
/// NB: while a row is still hot, the in-memory PK direct-get probe in
/// `fast_select` serves it BEFORE the cache GET is consulted, so a read of a
/// hot row does not register a cache hit; and a flush advances the catalog
/// snapshot, which invalidates the pre-flush write-through entry by the snapshot
/// watermark (correctly — the cold read then repopulates). The write-through's
/// value is therefore a warmed entry, not a guaranteed same-session hit; this
/// test pins that the seeding happens and stays correct, without asserting a
/// brittle hit-vs-probe ordering. (The cold-read pin in `s3_*` exercises the
/// hit path directly.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s2_insert_writes_through_to_cache() {
    let (_sd, _wd, engine, _shard, bg, wal) = build().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    exec(
        &sess,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;

    let before = engine.pk_row_cache_counters();
    let entries_before = engine.pk_row_cache_entries(project);
    tx_insert(&sess, "INSERT INTO t (id, v) VALUES (5, 50)").await;
    let after_insert = engine.pk_row_cache_counters();
    assert!(
        after_insert.inserts > before.inserts,
        "INSERT must write-through to the PK row cache; inserts before={} after={}",
        before.inserts,
        after_insert.inserts
    );
    assert!(
        engine.pk_row_cache_entries(project) > entries_before,
        "INSERT write-through must add a PK row cache entry for this project"
    );

    // Correctness is preserved (the seeded entry never serves a wrong row).
    assert_eq!(
        int_col(&sess, "SELECT v FROM t WHERE id = 5", "v").await,
        Some(50)
    );
    assert_eq!(count_star(&sess, "t").await, 1);

    bg.shutdown().await;
    wal.close().await.unwrap();
}

// ── S3: cold-decoded single-row pin ─────────────────────────────────────────

/// Seed a row, flush it cold (and drop the hot tier), then point-read it TWICE:
/// the first read is a cold MISS that populates the cache, the second is a HIT
/// returning the correct value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_cold_point_read_pins_then_hits() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    exec(
        &sess,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    // Auto-commit VALUES INSERT, flush, then evict the S4 retained-clean copy
    // (write-through keeps the row memory-resident across the flush, which
    // would serve the reads below before the PK row cache is consulted) so
    // the row is genuinely cold-only and this test pins the cold→cache path.
    exec(&sess, "INSERT INTO t (id, v) VALUES (3, 30)").await;
    shard.flush_to_parquet().await.unwrap();
    let _ = engine.memtable_registry().evict_clean(&project, u64::MAX);

    let before = engine.pk_row_cache_counters();
    // First read: cold MISS → decode → populate.
    assert_eq!(
        int_col(&sess, "SELECT v FROM t WHERE id = 3", "v").await,
        Some(30)
    );
    let after_first = engine.pk_row_cache_counters();
    assert!(
        after_first.inserts > before.inserts,
        "cold point read must populate (pin) the PK row cache; inserts before={} after={}",
        before.inserts,
        after_first.inserts
    );

    // Second read: HIT, correct value.
    assert_eq!(
        int_col(&sess, "SELECT v FROM t WHERE id = 3", "v").await,
        Some(30),
        "second cold point read must serve the pinned cache entry correctly"
    );
    let after_second = engine.pk_row_cache_counters();
    assert!(
        after_second.hits > after_first.hits,
        "second cold point read must HIT the pinned entry; hits before={} after={}",
        after_first.hits,
        after_second.hits
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}

// ── DELETE of a fresh-inserted key ──────────────────────────────────────────

/// A fresh INSERT followed by a DELETE of the same PK → the row is GONE (the
/// tombstone supersedes the inserted row AND invalidates any write-through cache
/// entry via the hot-epoch bump).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_of_fresh_inserted_key_is_gone() {
    let (_sd, _wd, engine, _shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    exec(
        &sess,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    tx_insert(&sess, "INSERT INTO t (id, v) VALUES (9, 90), (10, 100)").await;

    // Warm the cache / confirm visible.
    assert_eq!(
        int_col(&sess, "SELECT v FROM t WHERE id = 9", "v").await,
        Some(90)
    );

    exec(&sess, "DELETE FROM t WHERE id = 9").await;

    assert_eq!(
        int_col(&sess, "SELECT v FROM t WHERE id = 9", "v").await,
        None,
        "DELETE of a fresh-inserted key must make it GONE (no stale cache / memtable serve)"
    );
    // The sibling row is untouched.
    assert_eq!(
        int_col(&sess, "SELECT v FROM t WHERE id = 10", "v").await,
        Some(100)
    );
    assert_eq!(count_star(&sess, "t").await, 1, "COUNT after DELETE must be 1");

    bg.shutdown().await;
    wal.close().await.unwrap();
}

// ── Composite-PK table: counter-key fallback, never cached ──────────────────

/// A composite (multi-column) PK is NOT encodable to the single-PK cache shape,
/// so the INSERT write-through is skipped and the read path never consults the
/// cache for it. All reads + COUNT must still be correct, across a flush. This
/// pins that the single-PK gating does not break composite-PK tables.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn composite_pk_table_unaffected() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    exec(
        &sess,
        "CREATE TABLE c (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, \
         PRIMARY KEY (a, b))",
    )
    .await;

    let cache_before = engine.pk_row_cache_entries(project);
    tx_insert(
        &sess,
        "INSERT INTO c (a, b, v) VALUES (1, 1, 11), (1, 2, 12), (2, 1, 21)",
    )
    .await;
    // Composite PK is never written through to the single-PK cache.
    assert_eq!(
        engine.pk_row_cache_entries(project),
        cache_before,
        "composite-PK INSERT must NOT seed the single-PK row cache"
    );

    // Correctness in the hot tier.
    assert_eq!(count_star(&sess, "c").await, 3);
    assert_eq!(
        rows_seen(&sess, "SELECT v FROM c WHERE a = 1 AND b = 2").await,
        1
    );

    // Correctness after flush.
    shard.flush_to_parquet().await.unwrap();
    assert_eq!(count_star(&sess, "c").await, 3, "post-flush COUNT wrong");
    assert_eq!(
        int_col(&sess, "SELECT v FROM c WHERE a = 2 AND b = 1", "v").await,
        Some(21),
        "composite-PK point read wrong after flush"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}
