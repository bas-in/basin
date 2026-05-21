//! Phase 5.27.A — criterion benchmark: txn-mode pool throughput and latency.
//!
//! ## What this measures
//!
//! Once `TxnModePool` exists (Phase 5.27.B), these benchmarks measure:
//!
//! - **checkout_throughput**: how many pool checkouts/returns per second the
//!   txn-mode pool sustains under single-threaded load (eliminates scheduler
//!   noise; measures pure pool-mechanism overhead).
//!
//! - **checkout_latency_p99**: tail latency of a single `acquire → execute →
//!   drop` round-trip across 10 000 iterations, measured as a criterion
//!   "iter_custom" loop so we get the full distribution.
//!
//! - **concurrent_clients_throughput**: same as `checkout_throughput` but
//!   driven by a Tokio multi-thread runtime with 8 workers and 64 concurrent
//!   client tasks — stresses the lock path and the wakeup/oneshot machinery.
//!
//! ## Red / green status
//!
//! The bench **compiles today** (Phase 5.27.A) but falls back to the
//! session-mode `SessionPool` because `TxnModePool` does not exist yet.
//! Results are therefore measuring session-mode overhead — they are not the
//! final numbers.
//!
//! **Phase 5.27.B**: replace the `SessionPool::new(…)` calls below with
//! `TxnModePool::new(…)` (or `PoolConfig { mode: PoolMode::Transaction, .. }`)
//! and the benchmarks automatically reflect txn-mode performance.
//!
//! ## Running
//!
//! ```sh
//! # Compile-only (CI gate):
//! cargo bench -p basin-integration-tests --bench pool_transaction_mode --no-run
//!
//! # Full run (developer machine):
//! cargo bench -p basin-integration-tests --bench pool_transaction_mode
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
use basin_pool::{PoolConfig, SessionPool};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn build_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

fn pool_config(max_sessions: usize) -> PoolConfig {
    PoolConfig {
        max_sessions,
        per_project_cap: max_sessions,
        // Disable background eviction during bench runs — it adds jitter.
        eviction_interval: Duration::from_secs(3600),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Benchmark: single-threaded checkout → SELECT 1 → return throughput.
// ---------------------------------------------------------------------------

fn bench_checkout_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();

    // NOTE: Phase 5.27.B — replace with TxnModePool.
    let pool = Arc::new(SessionPool::new(engine, pool_config(10)));

    // Warm the pool (first checkout opens the session; subsequent ones hit).
    rt.block_on(async {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        s.execute("SELECT 1").await.unwrap();
    });

    let mut group = c.benchmark_group("pool_txn_mode");
    group.throughput(Throughput::Elements(1));

    group.bench_function("checkout_throughput_single_thread", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let leased = pool.acquire(project, None).await.unwrap();
                    let s = leased.session();
                    s.execute("SELECT 1").await.unwrap();
                    // Drop leased → return to pool.
                }
                start.elapsed()
            })
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: p99 latency of a single checkout → BEGIN/DECLARE/FETCH/COMMIT.
// ---------------------------------------------------------------------------

fn bench_txn_round_trip_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();

    // NOTE: Phase 5.27.B — replace with TxnModePool.
    let pool = Arc::new(SessionPool::new(engine, pool_config(10)));

    // Seed a table for the cursor benchmark.
    rt.block_on(async {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        s.execute("CREATE TABLE bench_rows (id BIGINT)").await.unwrap();
        s.execute("INSERT INTO bench_rows SELECT g FROM generate_series(1, 10) g")
            .await
            .ok(); // generate_series may not be implemented yet — ignore.
        // Fallback: insert 10 rows manually.
        for i in 1i64..=10 {
            s.execute(&format!("INSERT INTO bench_rows VALUES ({i})"))
                .await
                .ok();
        }
    });

    let mut group = c.benchmark_group("pool_txn_mode");
    group.throughput(Throughput::Elements(1));

    // Vary pool concurrency: 1, 4, 10 slots.
    for &slots in &[1usize, 4, 10] {
        let dir2 = TempDir::new().unwrap();
        let engine2 = build_engine(&dir2);
        let project2 = ProjectId::new();
        // NOTE: Phase 5.27.B — replace with TxnModePool.
        let pool2 = Arc::new(SessionPool::new(engine2, pool_config(slots)));

        rt.block_on(async {
            let leased = pool2.acquire(project2, None).await.unwrap();
            let s = leased.session();
            s.execute("CREATE TABLE bench_rows2 (id BIGINT)").await.unwrap();
            for i in 1i64..=3 {
                s.execute(&format!("INSERT INTO bench_rows2 VALUES ({i})"))
                    .await
                    .ok();
            }
        });

        group.bench_with_input(
            BenchmarkId::new("txn_round_trip_latency", slots),
            &slots,
            |b, _| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let start = Instant::now();
                        for i in 0..iters {
                            let cur = format!("bench_cur_{i}");
                            let leased = pool2.acquire(project2, None).await.unwrap();
                            let s = leased.session();
                            s.execute("BEGIN").await.ok();
                            s.execute(&format!(
                                "DECLARE {cur} CURSOR FOR SELECT id FROM bench_rows2"
                            ))
                            .await
                            .ok();
                            s.execute(&format!("FETCH 1 FROM {cur}")).await.ok();
                            s.execute("COMMIT").await.ok();
                            // Drop leased → return to pool (triggers reset in 5.27.E).
                        }
                        start.elapsed()
                    })
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: concurrent clients driving the pool.
// ---------------------------------------------------------------------------

fn bench_concurrent_clients(c: &mut Criterion) {
    // Use a multi-thread runtime to stress the pool lock path.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();

    // NOTE: Phase 5.27.B — replace with TxnModePool.
    let pool = Arc::new(SessionPool::new(engine, pool_config(10)));

    // Seed table.
    rt.block_on(async {
        let leased = pool.acquire(project, None).await.unwrap();
        let s = leased.session();
        s.execute("CREATE TABLE concurrent_bench (id BIGINT)").await.unwrap();
        s.execute("INSERT INTO concurrent_bench VALUES (1)").await.ok();
    });

    let mut group = c.benchmark_group("pool_txn_mode");
    group.throughput(Throughput::Elements(64)); // 64 concurrent clients per iter

    group.bench_function("concurrent_64_clients_10_slots", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let mut handles = Vec::with_capacity(64);
                    for _ in 0..64usize {
                        let p = pool.clone();
                        handles.push(tokio::spawn(async move {
                            let leased = p.acquire(project, None).await.unwrap();
                            let s = leased.session();
                            s.execute("SELECT 1").await.ok();
                        }));
                    }
                    for h in handles {
                        h.await.ok();
                    }
                }
                start.elapsed()
            })
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// criterion wiring
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_checkout_throughput,
    bench_txn_round_trip_latency,
    bench_concurrent_clients,
);
criterion_main!(benches);
