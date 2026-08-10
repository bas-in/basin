//! Correctness guard for read-modify-write UPDATEs through the hot-tier
//! fast path (the `SET col = <expr>` routing fix in dml_mutate.rs).
//!
//! The fix removed the gate that bailed `SET col = f(col)` UPDATEs to the
//! cold copy-on-write rewrite, routing them through the hot-tier overlay
//! instead (~20x faster). The load-bearing invariant is that **each RMW reads
//! the LATEST value** (memtable overlay first, then cold), so repeated
//! `SET v = v + 1` accumulates correctly rather than each re-reading the cold
//! base. This test pins that on cold-only rows, multi-row RMW, float
//! arithmetic, and `SET = CASE`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Float64Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

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
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal)
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

async fn float_at(sess: &basin_engine::ProjectSession, sql: &str) -> f64 {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let b = &batches[0];
            b.column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
        }
        _ => panic!("expected rows"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rmw_update_through_hot_tier_is_correct() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT, amt DOUBLE PRECISION)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 10, 1.5), (2, 20, 2.5), (3, 30, 3.5)")
        .await
        .unwrap();
    // Flush so the rows live ONLY in cold Vortex — this is the path the fix
    // changed (cold-only RMW used to take the full-file rewrite).
    shard.flush_to_parquet().await.unwrap();

    // 1. Repeated RMW must accumulate (each read sees the latest overlay).
    for _ in 0..3 {
        sess.execute("UPDATE t SET v = v + 1 WHERE id = 1")
            .await
            .unwrap();
    }
    assert_eq!(
        int_at(&sess, "SELECT v FROM t WHERE id = 1").await,
        13,
        "v=v+1 thrice on a cold row must be 10+3=13 (each RMW reads the latest overlay)"
    );

    // 2. Multi-row RMW.
    sess.execute("UPDATE t SET v = v + 5 WHERE id < 3")
        .await
        .unwrap();
    assert_eq!(
        int_at(&sess, "SELECT v FROM t WHERE id = 1").await,
        18,
        "id1: 13+5"
    );
    assert_eq!(
        int_at(&sess, "SELECT v FROM t WHERE id = 2").await,
        25,
        "id2: 20+5"
    );
    assert_eq!(
        int_at(&sess, "SELECT v FROM t WHERE id = 3").await,
        30,
        "id3 untouched"
    );

    // 3. Float RMW.
    sess.execute("UPDATE t SET amt = amt * 2 WHERE id = 3")
        .await
        .unwrap();
    assert!(
        (float_at(&sess, "SELECT amt FROM t WHERE id = 3").await - 7.0).abs() < 1e-9,
        "3.5*2"
    );

    // 4. SET = CASE WHEN (expression RHS, non-arithmetic).
    sess.execute("UPDATE t SET v = CASE WHEN v > 20 THEN 0 ELSE v END WHERE id = 3")
        .await
        .unwrap();
    assert_eq!(
        int_at(&sess, "SELECT v FROM t WHERE id = 3").await,
        0,
        "30>20 -> 0"
    );

    // 5. RMW referencing another column.
    sess.execute("UPDATE t SET v = id + 100 WHERE id = 2")
        .await
        .unwrap();
    assert_eq!(
        int_at(&sess, "SELECT v FROM t WHERE id = 2").await,
        102,
        "id(2)+100"
    );

    // 6. Full-table check: row count unchanged, no rows lost/duplicated.
    assert_eq!(
        int_at(&sess, "SELECT COUNT(*) FROM t").await,
        3,
        "no rows lost/duplicated"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// Does COUNT(*) over-count when a row has a hot-tier UPDATE overlay? Uses a
/// SCALAR UPDATE (always routes to the overlay — no env flag needed), so this
/// detects whether the count bug is PRE-EXISTING (independent of the RMW perf
/// experiment). An UPDATE replaces a row; it must not change COUNT(*).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn count_star_after_scalar_hot_update_is_correct() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    bg.shutdown().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();
    let c0 = int_at(&sess, "SELECT COUNT(*) FROM t").await;
    sess.execute("UPDATE t SET v = 99 WHERE id = 1")
        .await
        .unwrap(); // scalar -> overlay
    let c1 = int_at(&sess, "SELECT COUNT(*) FROM t").await;
    println!(
        "[count-overlay] COUNT before update={c0} after scalar hot update={c1} (want 2 and 2)"
    );
    assert_eq!(c0, 2, "baseline");
    assert_eq!(
        c1, 2,
        "COUNT(*) must stay 2 after a hot-tier UPDATE (replace, not insert)"
    );
    wal.close().await.unwrap();
}

/// Read all values of `col` ordered by id (bulk read path — exercises the
/// `snapshot_updates` overlay merge, and therefore the decoded-overlay memo).
async fn ints_ordered(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<i64> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                for r in 0..b.num_rows() {
                    out.push(a.value(r));
                }
            }
            out
        }
        _ => panic!("expected rows"),
    }
}

/// `mark_flushed` re-tag interaction with the decoded-overlay memo:
///
/// 1. Hot RMW UPDATEs write `Update` overrides for id 1 and 2.
/// 2. A bulk SELECT decodes + memoizes the overlay (auto-commit path).
/// 3. A NON-allowlisted UPDATE (`abs(v)`, cold copy-on-write) on id 3 first
///    runs `materialize_hot_overlay_into_cold`, whose flush ack
///    (`mark_flushed`) re-tags the acked `Update`s to `Row` — changing
///    `snapshot_updates`' output WITHOUT bumping the memtable epoch (the
///    documented `mark_flushed` invariant). The memo's `(epoch, update_count)`
///    key catches this via the `update_count` drain (2 → 0).
/// 4. Post-materialize reads must still see the SAME values: the overlay
///    vanished from the override map, but the acked image IS the cold image
///    now. A stale memo would also surface the (value-identical) overrides;
///    the load-bearing assert is that nothing is lost, doubled, or stale.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlay_memo_survives_materialize_retag() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    bg.shutdown().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, -30)")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();

    // Hot overlay: two RMW overrides (fast path is default-ON).
    sess.execute("UPDATE t SET v = v + 1 WHERE id = 1")
        .await
        .unwrap();
    sess.execute("UPDATE t SET v = v + 1 WHERE id = 2")
        .await
        .unwrap();
    // Warm the memo with a bulk read while both overrides are outstanding.
    assert_eq!(
        ints_ordered(&sess, "SELECT v FROM t ORDER BY id").await,
        vec![11, 21, -30],
        "bulk read sees both overrides before the materialize"
    );

    // `abs(v)` is not on the RMW allowlist → cold copy-on-write path → it
    // materializes the overlay into cold first and `mark_flushed` re-tags the
    // two `Update` entries to `Row` (no epoch bump, update_count 2 → 0).
    sess.execute("UPDATE t SET v = abs(v) WHERE id = 3")
        .await
        .unwrap();

    // Values must be exactly the post-materialize state on both read paths:
    // the former overrides now live in cold, id=3 got the cold-path abs().
    assert_eq!(
        ints_ordered(&sess, "SELECT v FROM t ORDER BY id").await,
        vec![11, 21, 30],
        "post-retag bulk read: materialized values correct, no stale/doubled rows"
    );
    assert_eq!(int_at(&sess, "SELECT v FROM t WHERE id = 1").await, 11);
    assert_eq!(int_at(&sess, "SELECT v FROM t WHERE id = 2").await, 21);
    assert_eq!(int_at(&sess, "SELECT v FROM t WHERE id = 3").await, 30);
    assert_eq!(
        int_at(&sess, "SELECT COUNT(*) FROM t").await,
        3,
        "no rows lost/duplicated"
    );

    wal.close().await.unwrap();
}

/// Repro for the hot-overlay vs cold-path-mutation interaction: a scalar
/// hot-tier UPDATE (overlay) followed by a RANGE UPDATE (cold copy-on-write
/// path) on the SAME row must not lose the cold-path update.
///
/// History: this was deterministically broken (#94), then flaky-~60% pass after
/// b38d6fa added `materialize_hot_overlay_into_cold` — the materialize committed
/// the catalog swap but did NOT physically delete the replaced cold files, so the
/// subsequent cold-path UPDATE's `list_data_files_with_stats` (which lists the
/// object store directly, not the catalog) saw BOTH the stale base and the
/// freshly-materialized file. Rewriting both produced a file with the row
/// duplicated (one copy with +1 from stale base, one with +1 from the overlay),
/// and SELECT could pick the stale-base copy, losing the overlaid value. The fix
/// in materialize_hot_overlay_into_cold pairs `commit_replace` with
/// `delete_objects` like every other rewrite site. Tracked in #95.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scalar_overlay_then_cold_range_update_composes() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    bg.shutdown().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();

    // Scalar hot-tier UPDATE (PK-eq, literal) -> memtable overlay sets id=1 v=99.
    sess.execute("UPDATE t SET v = 99 WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(
        int_at(&sess, "SELECT v FROM t WHERE id = 1").await,
        99,
        "overlay applied"
    );

    // Range UPDATE (cold copy-on-write path) on the same row.
    sess.execute("UPDATE t SET v = v + 1 WHERE id < 2")
        .await
        .unwrap();
    // Correct result: 99 + 1 = 100. If the cold path rewrote stale cold (10->11)
    // and the overlay shadows it, we'd wrongly read 99 (cold-path update lost).
    assert_eq!(
        int_at(&sess, "SELECT v FROM t WHERE id = 1").await,
        100,
        "hot overlay (99) + cold-path range UPDATE (+1) must compose to 100, not lose the +1"
    );

    wal.close().await.unwrap();
}
