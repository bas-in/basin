//! Diagnostic: clean single-row UPDATE latency on the post-#205 / post-#209
//! engine, with NO Postgres involved (the compare_postgres UPDATE timing is
//! contaminated by accumulated shared-PG state, so we avoid it entirely).
//!
//! Pre-fix the single-row UPDATE p50 was ~7ms@100k / ~15ms@1M — dominated by
//! the per-update cold re-encode + PUT + commit (each UPDATE eagerly
//! materialized the previous one in a tight loop). Post-#205 the fast path is
//! memtable-overlay + WAL with the cold materialize deferred to the cold path,
//! so a loop of consecutive single-row UPDATEs should no longer pay the
//! synchronous CoW cost per statement.
//!
//! This probe seeds via the ENGINE INSERT path (so the UPDATE fast path + the
//! #204 single-column-PK sort are exercised), quiesces cold with
//! `flush_to_parquet`, then times a loop of N consecutive single-row UPDATEs
//! each hitting a DIFFERENT existing id spread across the key space, and
//! reports p50 + p99.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Build a real engine: LocalFileSystem storage + InMemoryCatalog + a Shard
/// with background flush. Page/disk caches are the integration-test defaults.
/// Returns the Shard so the probe can `flush_to_parquet()` to force data cold.
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

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic — run explicitly with --ignored"]
async fn update_latency_probe() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let batch = 10_000i64;
    // 100k and 1M. If 1M seeding proves too slow this can be trimmed, but on an
    // idle machine the engine INSERT path should manage it. Each scale uses a
    // fresh table so the seeded id ranges never collide on the PK.
    for scale in [100_000i64, 1_000_000] {
        let table = format!("t{scale}");
        // Single-column PK so the fast-path UPDATE + #204 single-column-PK sort
        // apply.
        sess.execute(&format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT)"
        ))
        .await
        .unwrap();
        // Seed up to `scale` via the engine INSERT path (NOT raw
        // storage.write_batch) in 10k batches — each batch is a disjoint id
        // range, so after flush it becomes one Parquet file with a tight PK
        // zone-map.
        let mut id = 0i64;
        while id < scale {
            let lo = id;
            let hi = (id + batch).min(scale);
            let mut stmt = String::with_capacity((hi - lo) as usize * 16);
            stmt.push_str(&format!("INSERT INTO {table} VALUES "));
            for k in lo..hi {
                if k > lo {
                    stmt.push(',');
                }
                stmt.push_str(&format!("({k},{k})"));
            }
            sess.execute(&stmt).await.unwrap();
            id = hi;
        }

        // Quiesce: force all seeded data cold to Parquet so the timed UPDATEs
        // exercise the cold-read + overlay path, not a hot memtable.
        shard.flush_to_parquet().await.unwrap();

        // Warm-up discard: one UPDATE to pay any first-touch cost (table
        // metadata cache, plan cache, etc.) before we time.
        match sess
            .execute(&format!("UPDATE {table} SET v = -1 WHERE id = 1"))
            .await
            .unwrap()
        {
            ExecResult::Empty { tag } => assert_eq!(tag, "UPDATE 1", "warm-up UPDATE affected !=1"),
            other => panic!("warm-up UPDATE returned non-Empty: {other:?}"),
        }

        // Timed loop: N consecutive single-row UPDATEs, each a DIFFERENT id
        // spread across the key space. This is exactly the loop shape where the
        // old eager-materialize hurt — each UPDATE used to materialize the
        // prior one.
        let n: usize = 500;
        let mut times = Vec::with_capacity(n);
        for i in 0..n {
            // Spread targets evenly across [0, scale); start at 2 to avoid the
            // warm-up id and id used above.
            let target = ((i as i64) * (scale / n as i64) + 2) % scale;
            let newval = 1_000_000_000i64 + i as i64;
            let t0 = Instant::now();
            let res = sess
                .execute(&format!(
                    "UPDATE {table} SET v = {newval} WHERE id = {target}"
                ))
                .await
                .unwrap();
            let ms = t0.elapsed().as_secs_f64() * 1000.0;
            times.push(ms);
            match res {
                ExecResult::Empty { tag } => {
                    assert_eq!(tag, "UPDATE 1", "UPDATE id={target} affected != 1 row");
                }
                other => panic!("UPDATE returned non-Empty: {other:?}"),
            }
            assert!(ms.is_finite(), "UPDATE latency must be finite");
        }

        let mut sorted = times.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = percentile(&sorted, 0.50);
        let p99 = percentile(&sorted, 0.99);
        println!("[update-latency] scale={scale} p50={p50:.3}ms p99={p99:.3}ms");
    }

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// Diagnostic: read-modify-write (`SET v = v + 1`) single-row UPDATE latency.
///
/// RMW was the worst shape in the product: before the allowlist widening,
/// `SET v = v + 1 WHERE id = k` was REJECTED by the hot-tier fast-path gate and
/// fell to the cold copy-on-write rewrite — 2.7–16.5s per statement at 1M rows
/// (the whole file is read, the SET re-evaluated, and the file rewritten).
/// Once the gate admits the row-local RMW allowlist, each RMW takes the same
/// memtable-overlay + WAL path as a scalar UPDATE (~20ms), reading the LATEST
/// value so consecutive increments on the same key accumulate.
///
/// Same convention as `update_latency_probe`: seed via the engine INSERT path,
/// quiesce cold with `flush_to_parquet`, warm up, then time N=200 consecutive
/// `SET v = v + 1` UPDATEs each hitting a DIFFERENT id across the key space.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic — run explicitly with --ignored"]
async fn rmw_update_latency_probe() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let batch = 10_000i64;
    for scale in [100_000i64, 1_000_000] {
        let table = format!("rmw{scale}");
        sess.execute(&format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT)"
        ))
        .await
        .unwrap();
        let mut id = 0i64;
        while id < scale {
            let lo = id;
            let hi = (id + batch).min(scale);
            let mut stmt = String::with_capacity((hi - lo) as usize * 16);
            stmt.push_str(&format!("INSERT INTO {table} VALUES "));
            for k in lo..hi {
                if k > lo {
                    stmt.push(',');
                }
                stmt.push_str(&format!("({k},{k})"));
            }
            sess.execute(&stmt).await.unwrap();
            id = hi;
        }

        // Quiesce all seeded rows to cold Parquet so each RMW exercises the
        // cold-read-or-cache + overlay path (the shape the cold CoW used to
        // dominate), not a hot memtable.
        shard.flush_to_parquet().await.unwrap();

        // Warm-up discard: pay first-touch costs before timing.
        match sess
            .execute(&format!("UPDATE {table} SET v = v + 1 WHERE id = 1"))
            .await
            .unwrap()
        {
            ExecResult::Empty { tag } => assert_eq!(tag, "UPDATE 1", "warm-up RMW affected !=1"),
            other => panic!("warm-up RMW returned non-Empty: {other:?}"),
        }

        // Timed loop: N consecutive `SET v = v + 1`, each a DIFFERENT id spread
        // across the key space.
        let n: usize = 200;
        let mut times = Vec::with_capacity(n);
        for i in 0..n {
            let target = ((i as i64) * (scale / n as i64) + 2) % scale;
            let t0 = Instant::now();
            let res = sess
                .execute(&format!("UPDATE {table} SET v = v + 1 WHERE id = {target}"))
                .await
                .unwrap();
            let ms = t0.elapsed().as_secs_f64() * 1000.0;
            times.push(ms);
            match res {
                ExecResult::Empty { tag } => {
                    assert_eq!(tag, "UPDATE 1", "RMW id={target} affected != 1 row");
                }
                other => panic!("RMW UPDATE returned non-Empty: {other:?}"),
            }
            assert!(ms.is_finite(), "RMW latency must be finite");
        }

        let mut sorted = times.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = percentile(&sorted, 0.50);
        let p99 = percentile(&sorted, 0.99);
        println!("[rmw-latency] scale={scale} p50={p50:.3}ms p99={p99:.3}ms");
    }

    bg.shutdown().await;
    wal.close().await.unwrap();
}
