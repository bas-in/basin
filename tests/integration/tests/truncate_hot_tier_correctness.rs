//! Audit gap #2 — TRUNCATE with a live hot-tier overlay + un-flushed WAL tail,
//! on a REAL shard.
//!
//! Every existing TRUNCATE test (`ddl_completeness.rs`) runs `shard: None`
//! (cold-only). This suite wires a real shard + WAL so the four resurrection
//! vectors the audit named are all live at TRUNCATE time:
//!
//!   (a) memtable rows               — HTAP-resident clean rows;
//!   (b) overlay entries            — a fast-path UPDATE override + a DELETE
//!                                     tombstone in the MemTableRegistry;
//!   (c) the shard's WAL tail        — rows INSERTed but not yet flushed;
//!   (d) the PK row cache + provider/head caches.
//!
//! `truncate.rs` flushes the shard tail (`flush_to_parquet`) BEFORE capturing
//! the file list, then commits an empty snapshot removing every captured file,
//! then `clear_hot_tier` drops the whole MemTableRegistry entry for the table
//! (clean rows, UPDATE overrides AND tombstones — they all live in the one
//! MemTable). The dispatch-top gate (`executor.rs` ~2117, `!stmt_keeps_cache`
//! for a non-`Query` statement) invalidates the table-meta / provider /
//! head-probe / PK-row caches. So every vector is covered.
//!
//! A surviving `MemRowValue::Update` would RESURRECT a "truncated" row on the
//! next overlay read; a surviving WAL-tail entry would be re-flushed (or
//! replayed after a crash) into a fresh cold file. The durability mirror's
//! compaction-watermark change (`compact_one` persists the watermark BEFORE
//! truncating the WAL; replay starts strictly above it) makes the crash-replay
//! case duplicate-safe too: the rows TRUNCATE flushed-then-removed sit at/below
//! the watermark and are not re-replayed.
//!
//! The test asserts ZERO rows through EVERY read path after TRUNCATE — point-PK
//! probe, keyset/ORDER BY, full scan, COUNT(*) — then forces a flush + stripe
//! merge and asserts STILL zero (no compaction resurrection).

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ── Env isolation (fast-path gates are process-wide) ────────────────────────────

static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}
impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => unsafe { std::env::set_var(self.key, v) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}
fn set_env(key: &'static str) -> EnvGuard {
    let prev = std::env::var(key).ok();
    unsafe { std::env::set_var(key, "1") };
    EnvGuard { key, prev }
}

// ── Shard + WAL engine, NO background flush task ────────────────────────────────
//
// With the background flush task NOT spawned, an INSERT's rows stay resident in
// the shard tail until something explicitly drains them. That lets us hold a
// genuine un-flushed WAL tail across the TRUNCATE — the (c) vector.

async fn build_engine_with_unflushed_shard() -> (TempDir, TempDir, Engine, Shard, Arc<dyn Wal>) {
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_secs(3600),
            flush_max_bytes: 1024 * 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let shard_probe = shard.clone();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard),
    });
    (storage_dir, wal_dir, engine, shard_probe, wal)
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

async fn count_all(sess: &ProjectSession, table: &str) -> i64 {
    match sess
        .execute(&format!("SELECT COUNT(*) FROM {table}"))
        .await
        .unwrap()
    {
        ExecResult::Rows { batches, .. } => batches
            .first()
            .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
            .map(|a| a.value(0))
            .expect("COUNT(*) Int64"),
        other => panic!("COUNT(*) got {other:?}"),
    }
}

async fn row_count(sess: &ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

/// Assert ZERO rows through every read path. `where_pk` is a PK value to point-
/// probe; `tag` labels the assertion phase.
async fn assert_empty_all_paths(sess: &ProjectSession, table: &str, where_pk: i64, tag: &str) {
    // 1. Point-PK probe.
    let n_pk = row_count(
        sess,
        &format!("SELECT * FROM {table} WHERE id = {where_pk}"),
    )
    .await;
    assert_eq!(
        n_pk, 0,
        "{tag}: point-PK probe id={where_pk} must be empty; got {n_pk}"
    );

    // 2. A second point-PK probe on a different key (an UPDATE-overlay row).
    let n_pk2 = row_count(sess, &format!("SELECT * FROM {table} WHERE id = 5")).await;
    assert_eq!(
        n_pk2, 0,
        "{tag}: point-PK probe id=5 (overlay) must be empty; got {n_pk2}"
    );

    // 3. Keyset / ORDER BY + LIMIT paging path.
    let n_keyset = row_count(
        sess,
        &format!("SELECT * FROM {table} WHERE id > 0 ORDER BY id LIMIT 1000"),
    )
    .await;
    assert_eq!(
        n_keyset, 0,
        "{tag}: keyset ORDER BY+LIMIT must be empty; got {n_keyset}"
    );

    // 4. Full scan.
    let n_scan = row_count(sess, &format!("SELECT * FROM {table}")).await;
    assert_eq!(n_scan, 0, "{tag}: full scan must be empty; got {n_scan}");

    // 5. COUNT(*).
    let n_count = count_all(sess, table).await;
    assert_eq!(n_count, 0, "{tag}: COUNT(*) must be 0; got {n_count}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncate_clears_overlay_and_unflushed_tail_no_resurrection() {
    let _g = ENV_LOCK.lock().await;
    let _upd = set_env("BASIN_HOTTIER_UPDATE_FASTPATH");
    let _del = set_env("BASIN_HOTTIER_DELETE_FASTPATH");

    basin_common::telemetry::try_init_for_tests();
    let (_sd, _wd, engine, shard, wal) = build_engine_with_unflushed_shard().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    let table = TableName::new("t").unwrap();

    exec(
        &sess,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;

    // ── Wave 1: INSERT 10 rows and flush them to COLD via an explicit drain.
    // These become real cold files.
    {
        let mut stmt = "INSERT INTO t (id, v) VALUES ".to_string();
        for k in 1i64..=10 {
            if k > 1 {
                stmt.push(',');
            }
            stmt.push_str(&format!("({k}, {})", k * 10));
        }
        exec(&sess, &stmt).await;
    }
    shard
        .flush_to_parquet()
        .await
        .expect("flush wave 1 to cold");

    // ── (b) Overlay: point-UPDATE id=5 (plants a MemRowValue::Update override)
    // and point-DELETE id=7 (plants a tombstone). With the fastpath envs set and
    // RLS off, these write into the MemTableRegistry overlay rather than CoW.
    exec(&sess, "UPDATE t SET v = 99999 WHERE id = 5").await;
    exec(&sess, "DELETE FROM t WHERE id = 7").await;

    // ── (c) WAL tail: INSERT 3 more rows and DO NOT flush — they stay resident
    // in the shard tail. Confirm the tail is genuinely pending so the test is
    // exercising the (c) vector and not a no-op.
    exec(
        &sess,
        "INSERT INTO t (id, v) VALUES (11, 110), (12, 120), (13, 130)",
    )
    .await;
    assert!(
        shard.has_pending_tail(&project, &table).await,
        "the 3 un-flushed INSERT rows must leave a pending tail before TRUNCATE — \
         otherwise the WAL-tail resurrection vector is not under test"
    );

    // Sanity pre-TRUNCATE: 10 - 1 deleted + 3 tail = 12 rows; id=5 reads 99999.
    assert_eq!(
        count_all(&sess, "t").await,
        12,
        "pre-TRUNCATE COUNT(*) must be 12"
    );
    let v5_pre = row_count(&sess, "SELECT * FROM t WHERE id = 5 AND v = 99999").await;
    assert_eq!(
        v5_pre, 1,
        "pre-TRUNCATE the overlay UPDATE on id=5 must be visible"
    );

    // ── TRUNCATE: flushes the tail, removes every file, clears the overlay,
    // invalidates the caches.
    exec(&sess, "TRUNCATE TABLE t").await;

    // The hot-tier overlay must be GONE — a surviving Update override on id=5
    // would resurrect a row.
    assert!(
        engine.memtable_registry().get(&project, &table).is_none(),
        "TRUNCATE must drop the MemTableRegistry entry (overlay + tombstones) \
         for the table; a surviving Update override resurrects a truncated row"
    );
    // And no un-flushed tail may remain.
    assert!(
        !shard.has_pending_tail(&project, &table).await,
        "TRUNCATE must drain (then remove) the shard tail; a surviving tail \
         row would be re-flushed into a fresh cold file post-TRUNCATE"
    );

    // ── Zero rows through EVERY read path immediately after TRUNCATE.
    assert_empty_all_paths(&sess, "t", 5, "post-TRUNCATE").await;

    // ── Force a flush + stripe-merge: a tail entry or tombstone the truncate
    // missed would re-materialise here. Must STILL be zero.
    shard.flush_to_parquet().await.expect("post-truncate flush");
    shard
        .run_stripe_merge_once()
        .await
        .expect("post-truncate stripe merge");
    assert_empty_all_paths(&sess, "t", 5, "post-flush+compact").await;

    // ── Re-INSERT must work cleanly and show ONLY the new rows (no resurrected
    // pre-TRUNCATE rows commingled).
    exec(&sess, "INSERT INTO t (id, v) VALUES (5, 7), (100, 1000)").await;
    assert_eq!(
        count_all(&sess, "t").await,
        2,
        "post-TRUNCATE re-INSERT must yield exactly the 2 new rows — \
         any extra row is a resurrection of pre-TRUNCATE data"
    );
    let v5_new = row_count(&sess, "SELECT * FROM t WHERE id = 5 AND v = 7").await;
    assert_eq!(
        v5_new, 1,
        "re-INSERTed id=5 must read v=7, not the pre-TRUNCATE overlay value 99999"
    );
    // The old overlay value must be absent.
    let v5_old = row_count(&sess, "SELECT * FROM t WHERE id = 5 AND v = 99999").await;
    assert_eq!(
        v5_old, 0,
        "the pre-TRUNCATE overlay value 99999 must not survive"
    );

    wal.close().await.unwrap();
    println!("[gap#2] TRUNCATE clears overlay + WAL tail; no resurrection on any read path ✓");
}
