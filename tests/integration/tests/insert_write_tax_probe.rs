//! Diagnostic: where does Basin's bulk-INSERT "write tax" go?
//!
//! The bench shows Bulk INSERT ~5.7x slower than PG at 1M. This test
//! separates the two cost centres so we can attack the right one:
//!
//!   1. INSERT critical path — the shard auto-commit path returns after
//!      `write_batch` = WAL IPC-encode + append + in-memory tail push. This
//!      is what the bench's INSERT-loop timer actually measures. Vortex
//!      encoding is NOT on this path (it happens on the background flush).
//!
//!   2. Vortex flush encode — `flush_to_parquet` drains the tail and encodes
//!      to Vortex. EncodingMode::Best (default) runs the full BtrBlocks
//!      cascade (smallest files, slowest); EncodingMode::Fast
//!      (BASIN_FAST_BULK_INSERT=1) skips the encoding search.
//!
//! By timing (1) and (2) separately, under Best vs Fast, we learn:
//!   - If Best vs Fast barely changes the INSERT-loop time, the tax is in
//!     SQL parse + WAL, NOT the cascade → COPY path / WAL work is the lever.
//!   - If the flush time dominates and Fast >> Best, the cascade is the lever
//!     → defer Best-encode to compaction (LSM pattern).
//!
//! Run: cargo test --release -p basin-integration-tests \
//!        --test insert_write_tax_probe -- --ignored --nocapture

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

async fn build() -> (TempDir, TempDir, Engine, basin_shard::ShardBackgroundHandle, Arc<dyn Wal>) {
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
    let engine = Engine::new(EngineConfig { storage, catalog, shard: Some(shard) });
    (sd, wd, engine, bg, wal)
}

/// Seed `rows` into `events` in 10k batches (mirrors the bench shape: 5
/// scalar columns + a ~200B JSONB payload), timing the INSERT loop. Returns
/// (insert_loop_ms, flush_ms). The flush is timed separately via a SELECT
/// that forces `shard.flush_to_parquet()`.
async fn measure(engine: &Engine, rows: i64, label: &str) {
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    sess.execute(
        "CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT, amount DOUBLE PRECISION, status TEXT, created_at BIGINT, payload JSONB)",
    )
    .await
    .unwrap();

    let batch = 10_000i64;
    let insert_started = Instant::now();
    let mut id = 0i64;
    while id < rows {
        let lo = id;
        let hi = (id + batch).min(rows);
        let mut stmt = String::with_capacity((hi - lo) as usize * 240);
        stmt.push_str("INSERT INTO events VALUES ");
        for k in lo..hi {
            if k > lo {
                stmt.push(',');
            }
            let payload = format!(
                r#"{{"category":"cat{}","device":{{"os":"ios","version":"1.{}"}},"tags":[{},{},{}],"metadata":{{"score":{}}}}}"#,
                k % 20, k % 9, k % 3, k % 5, k % 7, k % 100
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
    let insert_loop_ms = insert_started.elapsed().as_secs_f64() * 1000.0;

    // Force the tail → Vortex flush and time it. A SELECT triggers
    // shard.flush_to_parquet() in the fast_select path.
    let flush_started = Instant::now();
    let _ = sess
        .execute("SELECT id FROM events WHERE id = 1")
        .await
        .unwrap();
    let flush_ms = flush_started.elapsed().as_secs_f64() * 1000.0;

    let mode = if std::env::var("BASIN_FAST_BULK_INSERT").as_deref() == Ok("1") {
        "Fast"
    } else {
        "Best"
    };
    println!(
        "[write-tax] {label:>6} rows={rows:>7} mode={mode:<4} \
         insert-loop={insert_loop_ms:8.1}ms ({:.1}us/row)  first-SELECT(incl flush)={flush_ms:8.1}ms",
        insert_loop_ms * 1000.0 / rows as f64
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic — run explicitly with --ignored"]
async fn write_tax_best_mode() {
    // NB: run this WITHOUT BASIN_FAST_BULK_INSERT to measure Best (default).
    let (_sd, _wd, engine, bg, wal) = build().await;
    measure(&engine, 200_000, "best").await;
    bg.shutdown().await;
    wal.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic — run explicitly with --ignored + BASIN_FAST_BULK_INSERT=1"]
async fn write_tax_fast_mode() {
    // NB: run this WITH BASIN_FAST_BULK_INSERT=1 to measure Fast.
    let (_sd, _wd, engine, bg, wal) = build().await;
    measure(&engine, 200_000, "fast").await;
    bg.shutdown().await;
    wal.close().await.unwrap();
}
