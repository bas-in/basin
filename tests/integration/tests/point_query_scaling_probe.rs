//! Diagnostic: does the PK point-probe deliver O(1)-ish point-query latency
//! as the file count grows, or does it scan O(n_files)?
//!
//! The bench shows `SELECT ... WHERE id = <mid>` at ~16ms at 1M (2300x PG)
//! but ~0.24ms at 100k. Sequential ids → disjoint per-file id ranges → the
//! zone-map prune SHOULD narrow to one file regardless of scale. This test
//! reproduces the shard-path point query across a growing file count and
//! reports the bloom/zone-map skip counter so we can see whether the probe
//! actually prunes.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, Int64Array};
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
        shard: Some(shard),
    });
    (sd, wd, engine, bg, wal)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "diagnostic — run explicitly with --ignored"]
async fn point_query_file_scaling() {
    let (_sd, _wd, engine, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    // Mirror the bench events schema EXACTLY — including the heavy JSONB
    // payload column. The bench point query does NOT select payload, so a
    // working projection pushdown should never decode it.
    sess.execute(
        "CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT, amount DOUBLE PRECISION, status TEXT, created_at BIGINT, payload JSONB)",
    )
    .await
    .unwrap();

    let batch = 10_000i64;
    for scale in [1_000_000i64, 10_000_000] {
        // Seed up to `scale` in 10k batches (each batch → one Parquet file
        // after flush, disjoint id range).
        let already = {
            // count existing
            match sess.execute("SELECT COUNT(*) FROM events").await.unwrap() {
                ExecResult::Rows { batches, .. } => batches
                    .first()
                    .map(|b| {
                        b.column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0)
                    })
                    .unwrap_or(0),
                _ => 0,
            }
        };
        let mut id = already;
        while id < scale {
            let lo = id;
            let hi = (id + batch).min(scale);
            let mut stmt = String::with_capacity((hi - lo) as usize * 40);
            stmt.push_str("INSERT INTO events VALUES ");
            for k in lo..hi {
                if k > lo {
                    stmt.push(',');
                }
                // ~200-byte JSONB payload per row, mirroring the bench's
                // payload_for() shape (nested object + array).
                let payload = format!(
                    r#"{{"category":"cat{}","device":{{"os":"ios","version":"1.{}"}},"tags":[{},{},{}],"metadata":{{"score":{}}}}}"#,
                    k % 20,
                    k % 9,
                    k % 3,
                    k % 5,
                    k % 7,
                    k % 100
                );
                stmt.push_str(&format!(
                    "({k}, {}, {}, 'pending', {k}, '{payload}'::jsonb)",
                    k % 1000,
                    k as f64 * 0.5
                ));
            }
            sess.execute(&stmt).await.unwrap();
            id = hi;
        }

        let target = scale / 2 + 7;
        // Warm-up — MUST match the timed query's projection (mirrors the
        // fixed bench warm-up, efd07d1) so the first timed sample is warm.
        let _ = sess
            .execute(&format!(
                "SELECT id, user_id, amount, status, created_at FROM events WHERE id = {target}"
            ))
            .await
            .unwrap();

        let before_skips = engine.blooms_skipped_count();
        let mut times = Vec::new();
        for _ in 0..7 {
            let t0 = Instant::now();
            let res = sess
                .execute(&format!(
                    "SELECT id, user_id, amount, status, created_at FROM events WHERE id = {target}"
                ))
                .await
                .unwrap();
            times.push(t0.elapsed().as_secs_f64() * 1000.0);
            let rows = match res {
                ExecResult::Rows { batches, .. } => {
                    batches.iter().map(|b| b.num_rows()).sum::<usize>()
                }
                _ => 0,
            };
            assert_eq!(
                rows, 1,
                "point query must return exactly 1 row at scale {scale}"
            );
        }
        let after_skips = engine.blooms_skipped_count();
        // Raw order (NOT sorted) — sample[0] is the first timed call after the
        // projection-MISMATCHED warm-up (warm-up projects only `id`; the timed
        // query projects 5 columns, so sample[0] cold-decodes the extra column
        // chunks). The bench at 1M takes only 2 samples and its `median` of 2
        // returns the HIGHER element — so it reports this cold sample as p50.
        let raw = times.clone();
        // Bench-style: median of the first 2 samples (median of 2 → higher).
        let mut first2 = vec![raw[0], raw[1]];
        first2.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let bench_style_p50 = first2[1]; // median(2) returns index len/2 = 1
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50_of_7 = times[times.len() / 2];
        let approx_files = scale / batch;
        println!(
            "[point-scaling] scale={scale:>7} files~{approx_files:>3} \
             p50(7-sample)={p50_of_7:7.3}ms  BENCH-STYLE p50(2-sample,median-hi)={bench_style_p50:7.3}ms  \
             raw[0..3]=[{:.3}, {:.3}, {:.3}]  skips/q={:.1}",
            raw[0], raw[1], raw[2],
            (after_skips - before_skips) as f64 / 7.0
        );
    }

    bg.shutdown().await;
    wal.close().await.unwrap();
}
