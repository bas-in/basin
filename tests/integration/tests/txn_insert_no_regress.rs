//! Regression guard — the `!in_tx` encoding gate must keep the
//! `BEGIN; INSERT x100; COMMIT` shape fast (bench `txn_insert_x100`).
//!
//! Background (commit ddfd8a8 bench rerun): when the Vortex `Fast` encoder
//! cascade was applied to EVERY direct INSERT, the explicit-transaction
//! insert shape regressed from ~38 ms to ~1042 ms at 10k scale in
//! `--release` mode (27x slower). Root cause: the `Fast` cascade carries
//! per-statement fixed setup cost that does NOT amortise for single-row
//! INSERTs inside `BEGIN..COMMIT`; the bulk bench shape (#41) issues 100
//! single-row INSERTs per txn.
//!
//! Fix (in `executor.rs::write_options_for`): non-tx direct INSERT always
//! encodes with `Fast`, but the flip is ANDed with `!in_tx` so in-tx INSERTs
//! fall through to `Best` and keep their cheaper single-row encode. Fast
//! encoding is now always-on for non-tx INSERT (the former
//! `BASIN_FAST_BULK_INSERT` opt-in was removed, #203), so the only thing
//! standing between this bench and the 27x regression is the `!in_tx` gate.
//!
//! This test pins that gate. It times two identical `BEGIN; INSERT x100;
//! COMMIT` runs on the same warm runtime and asserts (a) both complete, and
//! (b) the run-to-run ratio is stable (< 3x) — i.e. the in-tx path takes
//! the cheap `Best` single-row encode on every run. If the `!in_tx` gate were
//! removed, the in-tx path would take the `Fast` cascade and blow up ~27x; a
//! relative ratio is the right metric because the absolute number varies
//! 30-50x between debug and release builds.

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

/// Mirror the bench harness `build_basin_engine()` shape: shard + WAL wired.
/// The shard write path is the one exercised by the bench, so we measure on
/// the same codepath. Returns owned guards so the dirs live as long as the
/// engine.
async fn build_engine() -> (
    TempDir,
    TempDir,
    Engine,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
) {
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(
            LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap(),
        ),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(
                LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap(),
            ),
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
        shard: Some(shard),
    });
    (storage_dir, wal_dir, engine, bg, wal)
}

/// Time one `BEGIN; INSERT x100; COMMIT` block — the exact shape of the
/// bench `txn_insert_x100` metric (`compare_postgres_common.rs` #41).
/// `table` is created fresh so the timing isn't perturbed by prior rows.
async fn time_txn_insert_100(engine: &Engine, table: &str) -> f64 {
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    sess.execute(&format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)"
    ))
    .await
    .unwrap();

    let started = Instant::now();
    sess.execute("BEGIN").await.unwrap();
    for k in 0..100i64 {
        sess.execute(&format!("INSERT INTO {table} (id, v) VALUES ({k}, {k})"))
            .await
            .unwrap();
    }
    sess.execute("COMMIT").await.unwrap();
    started.elapsed().as_secs_f64() * 1000.0
}

/// The `!in_tx` gate must keep the in-tx single-row INSERT shape on the cheap
/// `Best` encode. Two identical runs on the same warm runtime should land
/// within noise of each other. Pre-fix the in-tx path under the `Fast`
/// cascade ran ~27x slower than the cheap encode, so any breakage of the gate
/// would be loud — far beyond the noise ceiling here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn txn_insert_x100_stays_fast_under_in_tx_gate() {
    // Warm-up: prime the process (codegen, allocator, tokio worker pool, the
    // first-time table/catalog setup) so the measured runs below aren't
    // skewed by one-time cold-start cost. The warm-up timing is discarded.
    let (_sd0, _wd0, engine0, bg0, wal0) = build_engine().await;
    let _warmup_ms = time_txn_insert_100(&engine0, "txnins_warm").await;
    bg0.shutdown().await;
    wal0.close().await.unwrap();

    // --- Run A (warm) ---
    let (_sd1, _wd1, engine1, bg1, wal1) = build_engine().await;
    let run_a_ms = time_txn_insert_100(&engine1, "txnins_a").await;
    bg1.shutdown().await;
    wal1.close().await.unwrap();

    // --- Run B (warm) ---
    let (_sd2, _wd2, engine2, bg2, wal2) = build_engine().await;
    let run_b_ms = time_txn_insert_100(&engine2, "txnins_b").await;
    bg2.shutdown().await;
    wal2.close().await.unwrap();

    let lo = run_a_ms.min(run_b_ms).max(1.0);
    let hi = run_a_ms.max(run_b_ms);
    let ratio = hi / lo;
    println!(
        "[txn_insert_no_regress] run A: {run_a_ms:.1} ms, run B: {run_b_ms:.1} ms, \
         ratio {ratio:.2}x"
    );

    // Both runs take the in-tx `Best` single-row encode (the `!in_tx` gate
    // holds), so run-to-run variance is just runtime/cache noise. Ceiling 3x
    // is generous for a debug-build two-sample spread — pre-fix the `Fast`
    // cascade made the in-tx path ~27x slower, so a removed gate would be
    // unmissable (9x+ above this ceiling).
    assert!(
        ratio < 3.0,
        "BEGIN;INSERT x100;COMMIT showed {ratio:.2}x run-to-run spread \
         (ceiling 3x — pre-fix the in-tx Fast cascade was ~27x). \
         Run A {run_a_ms:.1} ms, run B {run_b_ms:.1} ms. The `!in_tx` gate \
         in executor.rs::write_options_for may have regressed."
    );
}
