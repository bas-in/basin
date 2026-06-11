//! Background overlay reconciler (delta-update design, commit 5).
//!
//! With the delta-UPDATE cardinality cap at 10k keys, the hot-tier overlay
//! can hold ~150x more committed-but-unmaterialized rows than before, and it
//! historically drained ONLY when a conflicting cold-path op ran
//! `materialize_hot_overlay_into_cold`. The `overlay_reconcile` background
//! task drains it on a cadence instead. These tests pin:
//!
//!   1. the overlay drains in the background with NO foreground cold op —
//!      values stay correct, the overlay empties, replacement cold files
//!      exist;
//!   2. reads are identical before / during / after the drain;
//!   3. fast-path UPDATEs landing WHILE the reconciler materializes are not
//!      lost (post-snapshot seqs stay dirty — the e2d5ab2 ack semantics);
//!   4. `BASIN_OVERLAY_RECONCILE_SECS=0` disables the task (knob pin);
//!   5. a foreground cold (non-allowlisted) UPDATE racing the reconciler
//!      composes correctly — optimistic catalog concurrency, loser retries.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// Serialise env-var mutation across test threads in this binary. The
// reconciler reads its knobs ONCE at `Engine::new`, so each engine must be
// built while its env is held — after the guard drops, the captured config
// is immune to other tests' env mutation.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

type Built = (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
    Arc<dyn Catalog>,
);

async fn build() -> Built {
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
        storage,
        catalog: catalog.clone(),
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal, catalog)
}

fn restore_env(key: &str, prev: Option<String>) {
    match prev {
        Some(v) => std::env::set_var(key, v),
        None => std::env::remove_var(key),
    }
}

/// Build an engine with the reconciler knobs pinned: `secs` tick cadence and
/// `bytes` dirty-bytes threshold. Env is mutated only under `ENV_LOCK` and
/// restored before the guard drops; the engine captures the knobs at
/// `Engine::new` so the spawned task keeps them for its lifetime.
async fn build_with_reconcile(secs: &str, bytes: &str) -> Built {
    let _g = ENV_LOCK.lock().await;
    let prev_secs = std::env::var("BASIN_OVERLAY_RECONCILE_SECS").ok();
    let prev_bytes = std::env::var("BASIN_OVERLAY_RECONCILE_BYTES").ok();
    std::env::set_var("BASIN_OVERLAY_RECONCILE_SECS", secs);
    std::env::set_var("BASIN_OVERLAY_RECONCILE_BYTES", bytes);
    let out = build().await;
    restore_env("BASIN_OVERLAY_RECONCILE_SECS", prev_secs);
    restore_env("BASIN_OVERLAY_RECONCILE_BYTES", prev_bytes);
    out
}

async fn int_at(sess: &basin_engine::ProjectSession, sql: &str) -> i64 {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let b = &batches[0];
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        }
        _ => panic!("expected rows"),
    }
}

/// `update_count + tombstone_count` for `(project, table)` — the exact
/// overlay-presence gate the materialize prologue and the reconciler use.
fn overlay_pending(engine: &Engine, project: &ProjectId, table: &TableName) -> u64 {
    match engine.memtable_registry().get(project, table) {
        Some(e) => e.memtable.update_count() + e.memtable.tombstone_count(),
        None => 0,
    }
}

/// Poll until the table's overlay is empty (or `budget` elapses). Returns
/// `true` when drained.
async fn wait_overlay_drained(
    engine: &Engine,
    project: &ProjectId,
    table: &TableName,
    budget: Duration,
) -> bool {
    let deadline = std::time::Instant::now() + budget;
    while std::time::Instant::now() < deadline {
        if overlay_pending(engine, project, table) == 0 {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    overlay_pending(engine, project, table) == 0
}

/// Live cold-file path set for `(project, table)` straight from the catalog.
async fn live_paths(
    catalog: &Arc<dyn Catalog>,
    project: &ProjectId,
    table: &TableName,
) -> Vec<String> {
    let meta = catalog.load_table(project, table).await.unwrap();
    let mut v: Vec<String> = meta.live_data_files().into_iter().map(|f| f.path).collect();
    v.sort();
    v
}

/// 1. The overlay drains within a few ticks WITHOUT any foreground cold op:
/// 200 fast-path UPDATE overrides → poll until `update_count == 0` → values
/// correct, zero overlay left, and the cold file set was replaced.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlay_drains_in_background_without_foreground_cold_op() {
    let (_sd, _wd, engine, shard, bg, wal, catalog) = build_with_reconcile("1", "1").await;
    bg.shutdown().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let project = sess.project();
    let table = TableName::new("t").unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    let mut stmt = String::from("INSERT INTO t VALUES ");
    for k in 0..200i64 {
        if k > 0 {
            stmt.push(',');
        }
        stmt.push_str(&format!("({k},{})", k * 10));
    }
    sess.execute(&stmt).await.unwrap();
    shard.flush_to_parquet().await.unwrap();
    let paths_before = live_paths(&catalog, &project, &table).await;
    assert!(!paths_before.is_empty(), "flush produced cold files");

    // No-WHERE RMW on a ≤cap table → delta fast path → 200 overlay overrides.
    sess.execute("UPDATE t SET v = v + 1000").await.unwrap();
    assert_eq!(
        overlay_pending(&engine, &project, &table),
        200,
        "all 200 rows must be parked in the overlay (fast path engaged)"
    );

    let expected_sum: i64 = (0..200i64).map(|k| k * 10 + 1000).sum();
    assert_eq!(int_at(&sess, "SELECT SUM(v) FROM t").await, expected_sum);

    // Drain happens in the BACKGROUND: no cold op, no DDL — just ticks.
    assert!(
        wait_overlay_drained(&engine, &project, &table, Duration::from_secs(30)).await,
        "reconciler must drain the overlay within the poll budget"
    );
    let entry = engine
        .memtable_registry()
        .get(&project, &table)
        .expect("registry entry persists (clean residency) after drain");
    assert_eq!(entry.memtable.update_count(), 0, "zero update overlay left");
    assert_eq!(entry.memtable.tombstone_count(), 0, "zero tombstones left");

    // Values identical post-drain, now served from the materialized cold base.
    assert_eq!(int_at(&sess, "SELECT SUM(v) FROM t").await, expected_sum);
    assert_eq!(int_at(&sess, "SELECT v FROM t WHERE id = 0").await, 1000);
    assert_eq!(int_at(&sess, "SELECT v FROM t WHERE id = 199").await, 2990);
    assert_eq!(int_at(&sess, "SELECT COUNT(*) FROM t").await, 200);

    // Replacement files exist: the live set advanced past the pre-drain set.
    let paths_after = live_paths(&catalog, &project, &table).await;
    assert!(!paths_after.is_empty(), "post-drain live cold files exist");
    assert_ne!(
        paths_before, paths_after,
        "materialize must have replaced the cold file set"
    );

    wal.close().await.unwrap();
}

/// 2. Reads are IDENTICAL before, during, and after the background drain:
/// every poll iteration re-reads the full aggregate while the reconciler is
/// (possibly mid-) materializing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_identical_before_during_after_background_drain() {
    let (_sd, _wd, engine, shard, bg, wal, _catalog) = build_with_reconcile("1", "1").await;
    bg.shutdown().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let project = sess.project();
    let table = TableName::new("t").unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    let mut stmt = String::from("INSERT INTO t VALUES ");
    for k in 0..200i64 {
        if k > 0 {
            stmt.push(',');
        }
        stmt.push_str(&format!("({k},{k})"));
    }
    sess.execute(&stmt).await.unwrap();
    shard.flush_to_parquet().await.unwrap();

    sess.execute("UPDATE t SET v = v * 3").await.unwrap();
    assert_eq!(overlay_pending(&engine, &project, &table), 200);
    let expected_sum: i64 = (0..200i64).map(|k| k * 3).sum();

    // BEFORE the drain.
    assert_eq!(int_at(&sess, "SELECT SUM(v) FROM t").await, expected_sum);

    // DURING: read on every poll step until drained. A torn materialize
    // (committed cold but un-acked overlay, or vice versa) would double or
    // lose rows here.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut drained = false;
    while std::time::Instant::now() < deadline {
        assert_eq!(
            int_at(&sess, "SELECT SUM(v) FROM t").await,
            expected_sum,
            "reads must be identical at every point of the drain"
        );
        assert_eq!(int_at(&sess, "SELECT COUNT(*) FROM t").await, 200);
        if overlay_pending(&engine, &project, &table) == 0 {
            drained = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(drained, "reconciler must drain within the poll budget");

    // AFTER.
    assert_eq!(int_at(&sess, "SELECT SUM(v) FROM t").await, expected_sum);

    wal.close().await.unwrap();
}

/// 3. Fast-path UPDATEs landing WHILE the reconciler is materializing
/// survive: `dirty_snapshot` captures per-key seqs and `mark_flushed` acks
/// only those, so a post-snapshot write carries a higher seq and stays
/// DIRTY. Sequential read-your-writes across ≥3 ticks, then a final drain
/// must converge on the LAST written value (a broken ack would regress it).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_update_during_reconcile_survives() {
    let (_sd, _wd, engine, shard, bg, wal, _catalog) = build_with_reconcile("1", "1").await;
    bg.shutdown().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let project = sess.project();
    let table = TableName::new("t").unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    let mut stmt = String::from("INSERT INTO t VALUES ");
    for k in 0..100i64 {
        if k > 0 {
            stmt.push(',');
        }
        stmt.push_str(&format!("({k},0)"));
    }
    sess.execute(&stmt).await.unwrap();
    shard.flush_to_parquet().await.unwrap();

    // ~3 seconds of sequential scalar fast-path UPDATEs on id=1 — spanning
    // several 1s reconcile ticks, each of which is "due" (bytes threshold 1).
    let rounds = 30i64;
    for i in 1..=rounds {
        sess.execute(&format!("UPDATE t SET v = {i} WHERE id = 1"))
            .await
            .unwrap();
        assert_eq!(
            int_at(&sess, "SELECT v FROM t WHERE id = 1").await,
            i,
            "read-your-writes must hold while the reconciler races the writer"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Stop writing; the overlay must now fully drain and the FINAL value win.
    assert!(
        wait_overlay_drained(&engine, &project, &table, Duration::from_secs(30)).await,
        "overlay drains once the writer stops"
    );
    assert_eq!(
        int_at(&sess, "SELECT v FROM t WHERE id = 1").await,
        rounds,
        "post-drain value must be the newest write — an over-eager flush ack \
         (acking a seq newer than its snapshot) would regress it"
    );
    assert_eq!(int_at(&sess, "SELECT COUNT(*) FROM t").await, 100);

    wal.close().await.unwrap();
}

/// 4. Knob pin: `BASIN_OVERLAY_RECONCILE_SECS=0` disables the reconciler —
/// the overlay persists across what would have been several ticks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconcile_disabled_via_env_zero_overlays_persist() {
    let (_sd, _wd, engine, shard, bg, wal, _catalog) = build_with_reconcile("0", "1").await;
    bg.shutdown().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let project = sess.project();
    let table = TableName::new("t").unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    let mut stmt = String::from("INSERT INTO t VALUES ");
    for k in 0..200i64 {
        if k > 0 {
            stmt.push(',');
        }
        stmt.push_str(&format!("({k},{k})"));
    }
    sess.execute(&stmt).await.unwrap();
    shard.flush_to_parquet().await.unwrap();

    sess.execute("UPDATE t SET v = v + 1").await.unwrap();
    assert_eq!(overlay_pending(&engine, &project, &table), 200);

    // Several would-be ticks at the default 1s test cadence.
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(
        overlay_pending(&engine, &project, &table),
        200,
        "with BASIN_OVERLAY_RECONCILE_SECS=0 the overlay must NOT drain in \
         the background"
    );
    // Overlay reads still correct, of course.
    let expected_sum: i64 = (0..200i64).map(|k| k + 1).sum();
    assert_eq!(int_at(&sess, "SELECT SUM(v) FROM t").await, expected_sum);

    wal.close().await.unwrap();
}

/// 5. A foreground cold-path (non-allowlisted RMW — `abs(v)` is not on the
/// fast-path allowlist) UPDATE racing the live reconciler stays correct.
/// Both sides commit through `replace_data_files` optimistic concurrency;
/// the loser surfaces `CommitConflict`, which a client (here: the test, just
/// like the pgwire router's `execute_with_conflict_retry`) retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreground_cold_update_racing_reconciler_composes() {
    let (_sd, _wd, engine, shard, bg, wal, _catalog) = build_with_reconcile("1", "1").await;
    bg.shutdown().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let project = sess.project();
    let table = TableName::new("t").unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    let mut stmt = String::from("INSERT INTO t VALUES ");
    for k in 0..300i64 {
        if k > 0 {
            stmt.push(',');
        }
        stmt.push_str(&format!("({k},{})", -(k + 1)));
    }
    sess.execute(&stmt).await.unwrap();
    shard.flush_to_parquet().await.unwrap();

    // Park 300 overlay overrides: v = -(k+1) - 1000.
    sess.execute("UPDATE t SET v = v - 1000").await.unwrap();
    assert_eq!(overlay_pending(&engine, &project, &table), 300);
    let overlay_sum: i64 = (0..300i64).map(|k| -(k + 1) - 1000).sum();
    assert_eq!(int_at(&sess, "SELECT SUM(v) FROM t").await, overlay_sum);

    // Cold-path UPDATE while the overlay exists AND the reconciler is live:
    // its prologue materializes the overlay, racing the background tick.
    // Either side may lose the optimistic commit — retry like the router.
    let mut attempts = 0;
    loop {
        match sess.execute("UPDATE t SET v = abs(v)").await {
            Ok(_) => break,
            Err(e) if attempts < 10 => {
                attempts += 1;
                println!("[reconcile-race] cold UPDATE retry {attempts}: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("cold UPDATE failed beyond retry budget: {e}"),
        }
    }

    // Composition: abs applied ON TOP of the overlay values — losing either
    // mutation (or double-applying a row via a stale base) breaks the sum.
    let expected_sum: i64 = (0..300i64).map(|k| k + 1 + 1000).sum();
    assert_eq!(
        int_at(&sess, "SELECT SUM(v) FROM t").await,
        expected_sum,
        "cold UPDATE must compose on top of the materialized overlay"
    );
    assert_eq!(int_at(&sess, "SELECT COUNT(*) FROM t").await, 300);
    assert_eq!(int_at(&sess, "SELECT v FROM t WHERE id = 0").await, 1001);
    assert_eq!(int_at(&sess, "SELECT v FROM t WHERE id = 299").await, 1300);

    // And the overlay still drains to zero afterwards.
    assert!(
        wait_overlay_drained(&engine, &project, &table, Duration::from_secs(30)).await,
        "overlay empties after the cold op + reconciler settle"
    );

    wal.close().await.unwrap();
}
