//! S4 age-based residency — engine-side guards (commits 3–5 of the S4 design).
//!
//! These tests pin the residency invariants end-to-end through a real,
//! shard-backed `Engine` and assert on deterministic per-query WORK COUNTERS
//! (`ReadCounters` snapshot deltas — the `scale_invariants.rs` pattern), not
//! wall-clock:
//!
//! * **read-own-insert** — an auto-commit INSERT (≤ 128 rows) writes through
//!   to the hot tier as CLEAN entries, so the very next point read is served
//!   from memory: zero file opens, zero rows decoded, and NO forced tail
//!   flush (the catalog snapshot does not move).
//! * **residency survives flush** — flushing the shard tail to cold Parquet
//!   does not evict the retained clean row; the point read still costs zero
//!   file opens.
//! * **kill switch** — `BASIN_HOTTIER_RETAIN_SECS=0` disables the
//!   write-through entirely: reads stay correct and the post-flush point read
//!   pays the cold open, byte-for-byte today's behavior.
//! * **stale-row guard** — a cold copy-on-write UPDATE clears the table's
//!   CLEAN rows at the `commit_replace` choke point, so a retained copy can
//!   never shadow the rewritten cold value.
//! * **non-PK Eq completeness** — the auto-commit memtable fallback skips
//!   CLEAN rows (they are cold-visible), so a non-PK equality returns ALL
//!   matches instead of under-returning just the resident hot row.
//! * **pinned read across retention** — a transaction pinned before a
//!   concurrent overwrite keeps reading its version (MVCC chain), clean
//!   residency notwithstanding.
//!
//! # Env / counter discipline
//!
//! `ReadCounters` are process-global atomics on `Storage` and the
//! `BASIN_HOTTIER_*` gates are process-wide env vars, so every test takes the
//! file-level `ENV_LOCK` for its whole duration (serialising the binary —
//! same convention as `row_tier_mvcc.rs` / `tx_overlay_isolation.rs`), and
//! each test builds its OWN engine + storage.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_shard::{Shard, ShardBackgroundHandle, ShardConfig};
use basin_storage::{ReadCountersSnapshot, Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Serialises env-var mutation AND counter-delta measurement across the tests
/// in this binary. Held for each test's full duration.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn restore_env(key: &str, prev: Option<String>) {
    match prev {
        Some(v) => std::env::set_var(key, v),
        None => std::env::remove_var(key),
    }
}

/// Shard-backed engine (rows can flush to cold Parquet) that ALSO returns the
/// `Storage` handle (for `read_counters()`) and the `Catalog` (for snapshot
/// assertions). Mirrors `row_tier_stages::build` + `scale_invariants::build`.
async fn build() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    ShardBackgroundHandle,
    Arc<dyn Wal>,
    Storage,
    Arc<dyn Catalog>,
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
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal, storage, catalog)
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

/// Scalar `i64` from the first non-empty batch's column `name`, or `None`
/// when zero rows are returned.
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

/// Run `sql` and return its per-query `ReadCounters` delta.
async fn probe(
    sess: &ProjectSession,
    storage: &Storage,
    sql: &str,
) -> (ExecResult, ReadCountersSnapshot) {
    let before = storage.read_counters().snapshot();
    let res = sess.execute(sql).await.unwrap();
    let after = storage.read_counters().snapshot();
    (res, after.delta(&before))
}

fn one_row_value(res: &ExecResult, col: &str) -> i64 {
    match res {
        ExecResult::Rows { batches, .. } => {
            let b = batches
                .iter()
                .find(|b| b.num_rows() > 0)
                .unwrap_or_else(|| panic!("expected one row, got zero"));
            b.column_by_name(col)
                .unwrap_or_else(|| panic!("column {col} missing"))
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        }
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty({tag})"),
    }
}

// ── 1. read-own-insert: zero file opens, no forced flush ────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_own_insert_opens_zero_files() {
    let _g = ENV_LOCK.lock().await;
    let (_sd, _wd, engine, _shard, bg, wal, storage, catalog) = build().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    exec(
        &sess,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;

    // Auto-commit INSERT (≤ 128 rows) — the shard write-through path.
    exec(&sess, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)").await;

    let tname = TableName::new("t").unwrap();
    let snap_before = catalog
        .load_table(&project, &tname)
        .await
        .unwrap()
        .current_snapshot;

    // Point SELECT of the just-inserted row: served from the retained CLEAN
    // memtable entry — zero cold file opens, zero rows decoded, and the read
    // must NOT have forced a tail flush (catalog snapshot unchanged).
    let (res, d) = probe(&sess, &storage, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(one_row_value(&res, "v"), 10, "read-own-insert wrong value");
    assert_eq!(
        d.files_opened, 0,
        "read-own-insert must open 0 cold files, opened {}",
        d.files_opened
    );
    assert_eq!(
        d.rows_decoded, 0,
        "read-own-insert must decode 0 cold rows, decoded {}",
        d.rows_decoded
    );

    let snap_after = catalog
        .load_table(&project, &tname)
        .await
        .unwrap()
        .current_snapshot;
    assert_eq!(
        snap_before, snap_after,
        "the pre-flush probe must not force a tail flush (snapshot moved)"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}

// ── 2. residency survives the tail flush ────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn residency_survives_flush() {
    let _g = ENV_LOCK.lock().await;
    let (_sd, _wd, engine, shard, bg, wal, storage, _catalog) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    exec(
        &sess,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    exec(&sess, "INSERT INTO t (id, v) VALUES (7, 70)").await;

    // Force the tail cold. The CLEAN memtable copy must survive (a flush ack
    // retains clean rows; only the budget/age sweeps evict them).
    shard.flush_to_parquet().await.unwrap();

    let (res, d) = probe(&sess, &storage, "SELECT v FROM t WHERE id = 7").await;
    assert_eq!(one_row_value(&res, "v"), 70, "post-flush value wrong");
    assert_eq!(
        d.files_opened, 0,
        "post-flush point read must serve from the retained clean row \
         (0 cold opens), opened {}",
        d.files_opened
    );
    assert_eq!(
        d.rows_decoded, 0,
        "post-flush point read decoded {} cold rows; residency hit must decode 0",
        d.rows_decoded
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}

// ── 3. retain_secs = 0 kill switch ──────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retain_zero_kill_switch() {
    let _g = ENV_LOCK.lock().await;
    let prev = std::env::var("BASIN_HOTTIER_RETAIN_SECS").ok();
    std::env::set_var("BASIN_HOTTIER_RETAIN_SECS", "0");
    // Build AFTER the env flip — the engine reads MemTableConfig::from_env()
    // at construction.
    let (_sd, _wd, engine, shard, bg, wal, storage, _catalog) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    exec(
        &sess,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    exec(&sess, "INSERT INTO t (id, v) VALUES (3, 30)").await;
    shard.flush_to_parquet().await.unwrap();

    // Kill switch = today's behavior: nothing was retained, so the post-flush
    // point read is correct AND pays the cold open.
    let (res, d) = probe(&sess, &storage, "SELECT v FROM t WHERE id = 3").await;
    assert_eq!(one_row_value(&res, "v"), 30, "kill-switch read wrong value");
    assert!(
        d.files_opened >= 1,
        "with BASIN_HOTTIER_RETAIN_SECS=0 nothing is retained — the \
         post-flush read must pay the cold open (opened {})",
        d.files_opened
    );

    restore_env("BASIN_HOTTIER_RETAIN_SECS", prev);
    bg.shutdown().await;
    wal.close().await.unwrap();
}

// ── 4. stale-row guard: cold CoW UPDATE clears retained clean rows ──────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_row_guard() {
    let _g = ENV_LOCK.lock().await;
    // Fast paths are default-ON; pin them explicitly so an inherited
    // environment can never reroute the first UPDATE to the cold path.
    let prev_u = std::env::var("BASIN_HOTTIER_UPDATE_FASTPATH").ok();
    let prev_kill = std::env::var("BASIN_HOTTIER_FASTPATH_DISABLE").ok();
    std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", "1");
    std::env::remove_var("BASIN_HOTTIER_FASTPATH_DISABLE");

    let (_sd, _wd, engine, shard, bg, wal, _storage, _catalog) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    exec(
        &sess,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    exec(&sess, "INSERT INTO t (id, v) VALUES (1, 100)").await;
    shard.flush_to_parquet().await.unwrap();

    // Fast-path UPDATE: scalar SET → hot-tier Update override (dirty).
    exec(&sess, "UPDATE t SET v = 200 WHERE id = 1").await;
    assert_eq!(
        int_col(&sess, "SELECT v FROM t WHERE id = 1", "v").await,
        Some(200),
        "fast-path UPDATE must be visible"
    );

    // Cold-path UPDATE: division is OUTSIDE the RMW fast-path allowlist
    // (`rmw_rhs_is_fast_path_eligible` rejects `/`), forcing the copy-on-write
    // route: materialize the v=200 overlay into cold (acked → retained clean),
    // rewrite cold applying v = v / 2, then `commit_replace` — which must
    // clear the table's CLEAN rows so the retained v=200 copy cannot shadow
    // the rewritten cold value.
    exec(&sess, "UPDATE t SET v = v / 2 WHERE id = 1").await;
    assert_eq!(
        int_col(&sess, "SELECT v FROM t WHERE id = 1", "v").await,
        Some(100),
        "point read after a cold CoW UPDATE must return the COLD value — a \
         stale retained clean row leaked through commit_replace"
    );

    restore_env("BASIN_HOTTIER_UPDATE_FASTPATH", prev_u);
    restore_env("BASIN_HOTTIER_FASTPATH_DISABLE", prev_kill);
    bg.shutdown().await;
    wal.close().await.unwrap();
}

// ── 5. non-PK Eq completeness: clean rows must not under-return ─────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_pk_eq_completeness() {
    let _g = ENV_LOCK.lock().await;
    let (_sd, _wd, engine, shard, bg, wal, _storage, _catalog) = build().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    exec(
        &sess,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;

    // Three cold rows matching v = 10. Flush them cold, then drop their
    // residency (simulating retention expiry) so the memtable holds NOTHING
    // for them.
    exec(
        &sess,
        "INSERT INTO t (id, v) VALUES (1, 10), (2, 10), (3, 10)",
    )
    .await;
    shard.flush_to_parquet().await.unwrap();
    let _ = engine.memtable_registry().evict_clean(&project, u64::MAX);

    // One more matching row, freshly inserted: retained CLEAN in the memtable
    // (and in the un-flushed tail).
    exec(&sess, "INSERT INTO t (id, v) VALUES (4, 10)").await;

    // A non-PK equality must return ALL FOUR matches. Pre-fix, the memtable
    // fallback returned ONLY the resident hot row and skipped the cold read —
    // the under-return this test pins.
    let n = rows_seen(&sess, "SELECT id FROM t WHERE v = 10").await;
    assert_eq!(
        n, 4,
        "non-PK Eq must return every match (3 cold + 1 resident), got {n}"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}

// ── 6. pinned read across retention (two-session MVCC) ──────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinned_read_across_retention() {
    let _g = ENV_LOCK.lock().await;
    let prev_u = std::env::var("BASIN_HOTTIER_UPDATE_FASTPATH").ok();
    let prev_kill = std::env::var("BASIN_HOTTIER_FASTPATH_DISABLE").ok();
    std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", "1");
    std::env::remove_var("BASIN_HOTTIER_FASTPATH_DISABLE");

    let (_sd, _wd, engine, _shard, bg, wal, _storage, _catalog) = build().await;
    let project = ProjectId::new();
    let a = engine.open_session(project).await.unwrap();
    let b = engine.open_session(project).await.unwrap();
    exec(
        &a,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
    )
    .await;
    // Auto-commit INSERT → write-through residency (a CLEAN memtable row).
    exec(&a, "INSERT INTO t (id, val) VALUES (1, 10)").await;

    // A pins its read view at first touch, while the row is retained clean.
    exec(&a, "BEGIN").await;
    assert_eq!(
        int_col(&a, "SELECT val FROM t WHERE id = 1", "val").await,
        Some(10),
        "pinned first read must see the seeded value"
    );

    // B overwrites the SAME key after A pinned (fast-path Update override —
    // it re-dirties the chain, pushing a new version OVER the clean one).
    exec(&b, "UPDATE t SET val = 999 WHERE id = 1").await;
    assert_eq!(
        int_col(&b, "SELECT val FROM t WHERE id = 1", "val").await,
        Some(999),
        "auto-commit reader must see the overwrite"
    );

    // A keeps its version: the retained pre-overwrite version resolves at
    // A's hot-seq watermark (MVCC chain), residency state notwithstanding.
    assert_eq!(
        int_col(&a, "SELECT val FROM t WHERE id = 1", "val").await,
        Some(10),
        "pinned reader must keep its version across a concurrent overwrite"
    );
    exec(&a, "COMMIT").await;

    // Post-COMMIT A sees the live head.
    assert_eq!(
        int_col(&a, "SELECT val FROM t WHERE id = 1", "val").await,
        Some(999),
        "after COMMIT the pinned session must see the live head"
    );

    restore_env("BASIN_HOTTIER_UPDATE_FASTPATH", prev_u);
    restore_env("BASIN_HOTTIER_FASTPATH_DISABLE", prev_kill);
    bg.shutdown().await;
    wal.close().await.unwrap();
}
