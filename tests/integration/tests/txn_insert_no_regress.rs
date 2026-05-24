//! Regression guard — `BASIN_FAST_BULK_INSERT=1` must NOT degrade the
//! `BEGIN; INSERT x100; COMMIT` shape (bench `txn_insert_x100`).
//!
//! Background (commit ddfd8a8 bench rerun): with `BASIN_FAST_BULK_INSERT=1`
//! globally enabled, the explicit-transaction insert shape regressed from
//! ~38 ms to ~1042 ms at 10k scale in `--release` mode (27x slower). Root
//! cause: the Vortex `Fast` encoder cascade carries per-statement fixed
//! setup cost that does NOT amortise for single-row INSERTs inside
//! `BEGIN..COMMIT`; the bulk bench shape (#41) issues 100 single-row
//! INSERTs per txn.
//!
//! Fix (in `executor.rs::write_options_for`): the env-var-gated flip to
//! `EncodingMode::Fast` is now ANDed with `!in_tx`. Outside-txn direct
//! INSERTs still get the ~3-4x encode speedup; in-tx INSERTs fall through
//! to `Best` and recover the pre-regression latency.
//!
//! This test asserts the **relative** claim of the fix: WITH the env var,
//! the in-tx insert path must NOT be significantly slower than WITHOUT it.
//! A relative ratio is the right metric here because the absolute number
//! varies by 30-50x between debug and release builds — a hard ms-ceiling
//! would either be vacuous in release or flake in debug.
//!
//! Pre-fix the ratio would be ~27x (1042 / 38). Post-fix the in-tx path
//! falls through to `Best` regardless of env, so the ratio is ~1.0 plus
//! noise. We assert ratio < 1.5 — anything above that signals the
//! `!in_tx` gate is no longer being honoured.

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

/// `BASIN_FAST_BULK_INSERT=1` must NOT regress the in-tx single-row INSERT
/// shape. We measure WITHOUT then WITH the env var in a single test so
/// both run on the same warm tokio runtime and shard background, and
/// compare the ratio. Pre-fix this ratio was ~27x; post-fix it is ~1x
/// because the `!in_tx` gate falls through to `Best` regardless of env.
///
/// Uses `serial_test::serial` (process-wide env var) so a parallel test
/// can't observe a flipped gate. Restores prior env on drop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn txn_insert_x100_no_regress_under_fast_bulk_insert_env() {
    struct EnvGuard {
        key: &'static str,
        prev: Option<String>,
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // SAFETY: tests in this process don't race on this env var; the
            // guard restores the snapshot taken before the test set it.
            match &self.prev {
                Some(v) => unsafe { std::env::set_var(self.key, v) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }

    let prev = std::env::var("BASIN_FAST_BULK_INSERT").ok();
    let _guard = EnvGuard {
        key: "BASIN_FAST_BULK_INSERT",
        prev,
    };

    // --- WITHOUT env (baseline) ---
    // SAFETY: scoped via EnvGuard which restores on drop.
    unsafe { std::env::remove_var("BASIN_FAST_BULK_INSERT") };
    let (_sd1, _wd1, engine1, bg1, wal1) = build_engine().await;
    let baseline_ms = time_txn_insert_100(&engine1, "txnins_base").await;
    bg1.shutdown().await;
    wal1.close().await.unwrap();

    // --- WITH env (must not regress) ---
    // SAFETY: scoped via EnvGuard which restores on drop.
    unsafe { std::env::set_var("BASIN_FAST_BULK_INSERT", "1") };
    let (_sd2, _wd2, engine2, bg2, wal2) = build_engine().await;
    let with_env_ms = time_txn_insert_100(&engine2, "txnins_fast").await;
    bg2.shutdown().await;
    wal2.close().await.unwrap();

    let ratio = with_env_ms / baseline_ms.max(1.0);
    println!(
        "[txn_insert_no_regress] WITHOUT env: {baseline_ms:.1} ms, \
         WITH BASIN_FAST_BULK_INSERT=1: {with_env_ms:.1} ms, ratio {ratio:.2}x"
    );

    // The fix's claim: in-tx encoding is identical regardless of the env
    // var, because the `!in_tx` gate falls through to `Best`. Allow some
    // headroom for runtime/cache noise (1.5x) — pre-fix this ratio was
    // ~27x, so any breakage is loud.
    assert!(
        ratio < 1.5,
        "BEGIN;INSERT x100;COMMIT under BASIN_FAST_BULK_INSERT=1 ran \
         {ratio:.2}x slower than baseline (ceiling 1.5x — pre-fix was ~27x). \
         Baseline {baseline_ms:.1} ms, with-env {with_env_ms:.1} ms. \
         The `!in_tx` gate in executor.rs::write_options_for may have \
         regressed."
    );
}
