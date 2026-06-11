//! Diagnostic: clean single-row DELETE latency on the post-#205 / post-#209
//! engine, with NO Postgres involved (the compare_postgres timings are
//! contaminated by accumulated shared-PG state, so we avoid them entirely).
//!
//! DELETE by PK is expected to be memtable-tombstone-only — the fast path
//! writes a tombstone overlay + WAL with the cold materialize deferred, so a
//! loop of consecutive single-row DELETEs should pay no cold-read or CoW cost
//! per statement. This probe verifies that claim at scale.
//!
//! This probe seeds via the ENGINE INSERT path (so the #204 single-column-PK
//! sort is exercised), quiesces cold with `flush_to_parquet`, then times a
//! loop of N consecutive single-row DELETEs each hitting a DIFFERENT existing
//! id spread across the key space (no id is ever reused or deleted twice),
//! and reports p50 + p99. A cheap `SELECT COUNT(*)` afterwards confirms
//! exactly the expected number of rows was deleted.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::Int64Array;
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
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
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
async fn delete_latency_probe() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let batch = 10_000i64;
    // 100k and 1M. If 1M seeding proves too slow this can be trimmed, but on an
    // idle machine the engine INSERT path should manage it. Each scale uses a
    // fresh table so the seeded id ranges never collide on the PK.
    for scale in [100_000i64, 1_000_000] {
        let table = format!("t{scale}");
        // Single-column PK so the fast-path DELETE + #204 single-column-PK sort
        // apply.
        sess.execute(&format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT)"))
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

        // Quiesce: force all seeded data cold to Parquet so the timed DELETEs
        // exercise the tombstone-overlay path against cold data, not a hot
        // memtable.
        shard.flush_to_parquet().await.unwrap();

        // Warm-up discard: one DELETE to pay any first-touch cost (table
        // metadata cache, plan cache, etc.) before we time. Uses id=1, which
        // no timed iteration targets (timed targets start at 2 and stride by
        // scale/n), so every key is deleted exactly once.
        match sess
            .execute(&format!("DELETE FROM {table} WHERE id = 1"))
            .await
            .unwrap()
        {
            ExecResult::Empty { tag } => assert_eq!(tag, "DELETE 1", "warm-up DELETE affected !=1"),
            other => panic!("warm-up DELETE returned non-Empty: {other:?}"),
        }

        // Timed loop: N consecutive single-row DELETEs, each a DIFFERENT id
        // spread across the key space. The stride is scale/n so all n targets
        // are distinct (2, 2+stride, 2+2*stride, …) and never wrap onto a key
        // a later iteration will use — no key is deleted twice.
        let n: usize = 500;
        let mut times = Vec::with_capacity(n);
        for i in 0..n {
            // Spread targets evenly across [0, scale); start at 2 to avoid the
            // warm-up id used above.
            let target = ((i as i64) * (scale / n as i64) + 2) % scale;
            let t0 = Instant::now();
            let res = sess
                .execute(&format!("DELETE FROM {table} WHERE id = {target}"))
                .await
                .unwrap();
            let ms = t0.elapsed().as_secs_f64() * 1000.0;
            times.push(ms);
            match res {
                ExecResult::Empty { tag } => {
                    assert_eq!(tag, "DELETE 1", "DELETE id={target} affected != 1 row");
                }
                other => panic!("DELETE returned non-Empty: {other:?}"),
            }
            assert!(ms.is_finite(), "DELETE latency must be finite");
        }

        let mut sorted = times.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = percentile(&sorted, 0.50);
        let p99 = percentile(&sorted, 0.99);
        println!("[delete-latency] scale={scale} p50={p50:.3}ms p99={p99:.3}ms");

        // Correctness sanity check: seeded rows minus the warm-up DELETE and
        // the n timed DELETEs must remain visible.
        let expected = scale - 1 - n as i64;
        match sess
            .execute(&format!("SELECT COUNT(*) FROM {table}"))
            .await
            .unwrap()
        {
            ExecResult::Rows { batches, .. } => {
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total_rows, 1, "COUNT(*) must return a single row");
                let count = batches
                    .first()
                    .unwrap()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("COUNT(*) column must be Int64")
                    .value(0);
                assert_eq!(
                    count, expected,
                    "scale={scale}: COUNT(*) after deletes must be {expected}"
                );
            }
            other => panic!("COUNT(*) returned non-Rows: {other:?}"),
        }
    }

    bg.shutdown().await;
    wal.close().await.unwrap();
}
