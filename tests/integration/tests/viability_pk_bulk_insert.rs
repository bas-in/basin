//! Viability: bulk INSERT against a table with a PRIMARY KEY must be
//! competitive with the no-PK shape.
//!
//! The original cost root cause was that `enforce_pk_on_insert` was
//! re-reading every cold data file's PK column on every batch, and was
//! decoding all columns (including JSONB / TEXT payloads) because
//! `Storage::read_file` defaulted to `ReadOptions::default()`. The fix
//! ships in three tiers:
//!
//!   - Tier 1: projection pushdown — the constraint scan reads only the
//!     PK column(s), skipping JSONB / TEXT payload decode.
//!   - Tier 2: cross-batch memoization — the on-disk PK set is cached by
//!     `(project, table)` keyed on the file-set FNV signature so identical
//!     batches reuse the prior scan.
//!   - Tier 3: i64 PK fast path — single Int64 PK probes go through a
//!     `HashSet<i64>` instead of `HashSet<Vec<String>>`.
//!
//! This test boots an in-process Basin engine with a shard, inserts 50
//! batches × 2k rows into both `events_pk` (BIGINT PRIMARY KEY) and
//! `events_nopk` (BIGINT NOT NULL, no PK), and asserts the wall-clock ratio
//! is bounded — pre-fix the ratio was ~10×; post-fix it should stay under
//! 3×. Lenient debug-build cushion: the bar accounts for fixed PK-path
//! costs (intra-batch dedup HashSet build, FK lookup if any) that still
//! exist after the cache hits.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
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
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard),
    });
    (sd, wd, engine, bg, wal)
}

/// Insert `n_batches` × `batch_size` rows of sequential ids into `table`,
/// returning the total wall-clock elapsed. The payload column mirrors the
/// compare_postgres shape (a ~150B TEXT) so the projection-pushdown win is
/// measurable: without it every cold file's payload column is decoded.
///
/// Every `flush_every` batches we issue a no-op SELECT to force the shard's
/// memtable → Parquet flush (Option A: `fast_select` calls
/// `shard.flush_to_parquet()` before reading). This is the only way to
/// guarantee cold-tier files exist mid-loop so that `enforce_pk_on_insert`'s
/// scan path is actually exercised — without it small bulks may finish
/// before the memtable hits its size cap and the constraint enforcer sees
/// zero data files.
async fn run_inserts(
    engine: &Engine,
    table: &str,
    n_batches: usize,
    batch_size: i64,
    project: ProjectId,
    flush_every: usize,
    start_id: i64,
) -> Duration {
    let sess = engine.open_session(project).await.unwrap();
    let started = Instant::now();
    let mut id = start_id;
    for b in 0..n_batches {
        let hi = id + batch_size;
        let mut stmt = String::with_capacity(batch_size as usize * 180);
        stmt.push_str(&format!("INSERT INTO {table} VALUES "));
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            stmt.push_str(&format!(
                "({k}, 'payload-row-{k}-{}-{}-{}-{}-{}-{}-{}-{}-{}')",
                k % 7,
                k % 11,
                k % 13,
                k % 17,
                k % 19,
                k % 23,
                k % 29,
                k % 31,
                k % 37,
            ));
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
        if flush_every > 0 && (b + 1) % flush_every == 0 {
            // Force a parquet flush so subsequent batches actually go through
            // the cold-tier PK scan.
            let _ = sess
                .execute(&format!("SELECT id FROM {table} WHERE id = -1"))
                .await
                .unwrap();
        }
    }
    started.elapsed()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_insert_with_pk_is_competitive_with_no_pk() {
    let (_sd, _wd, engine, bg, wal) = build().await;

    // Two projects so neither bench leaks cache state into the other; each
    // gets its own (ProjectId, TableName) cache key in the PkSetCache.
    let proj_pk = ProjectId::new();
    let proj_nopk = ProjectId::new();

    // Create both tables. `events_pk` has a BIGINT PRIMARY KEY (the shape
    // that hits enforce_pk_on_insert); `events_nopk` has no PK at all.
    {
        let sess = engine.open_session(proj_pk).await.unwrap();
        sess.execute(
            "CREATE TABLE events_pk (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL)",
        )
        .await
        .unwrap();
    }
    {
        let sess = engine.open_session(proj_nopk).await.unwrap();
        sess.execute("CREATE TABLE events_nopk (id BIGINT NOT NULL, payload TEXT NOT NULL)")
            .await
            .unwrap();
    }

    // 50 batches × 2k rows per the task spec. Total: 100k rows per table.
    // Force a parquet flush every 5 batches so the constraint scan runs
    // against a growing cold-tier file set — this is the path the perf fix
    // optimises. Without periodic flushes a 100k-row test stays entirely in
    // the memtable and never touches `enforce_pk_on_insert`'s cold scan.
    const N_BATCHES: usize = 50;
    const BATCH_SIZE: i64 = 2_000;
    const FLUSH_EVERY: usize = 5;

    // Warm-up batch on each side absorbs the one-off cost of session
    // setup (UDF Arc-clones, DataFusion state build) so the timed loop
    // measures steady-state work. Ids 0..50 are claimed by the warm-up;
    // the timed loop starts at 1_000_000 so PK ranges never collide.
    let _ = run_inserts(&engine, "events_nopk", 1, 50, proj_nopk, 0, 0).await;
    let _ = run_inserts(&engine, "events_pk", 1, 50, proj_pk, 0, 0).await;

    let elapsed_nopk = run_inserts(
        &engine,
        "events_nopk",
        N_BATCHES,
        BATCH_SIZE,
        proj_nopk,
        FLUSH_EVERY,
        1_000_000,
    )
    .await;
    let elapsed_pk = run_inserts(
        &engine,
        "events_pk",
        N_BATCHES,
        BATCH_SIZE,
        proj_pk,
        FLUSH_EVERY,
        1_000_000,
    )
    .await;

    let ratio = elapsed_pk.as_secs_f64() / elapsed_nopk.as_secs_f64().max(1e-6);
    println!(
        "[viability_pk_bulk_insert] nopk={elapsed_nopk:?} pk={elapsed_pk:?} ratio={ratio:.2}x"
    );

    // Bar: pk must be < 3x nopk. Pre-fix the ratio was ~10x.
    let bar_ratio = 3.0;
    let pass = ratio < bar_ratio;

    report_viability(
        "pk_bulk_insert",
        "Bulk INSERT with PRIMARY KEY",
        "After the projection+memoization+i64-fastpath fix, the per-batch cold-tier PK scan no longer dominates bulk INSERT — wall-clock with a PK column stays within 3x of the no-PK shape.",
        pass,
        PrimaryMetric {
            label: "pk_to_nopk_ratio".into(),
            value: ratio,
            unit: "x".into(),
            bar: BarOp::lt(bar_ratio),
        },
        json!({
            "n_batches": N_BATCHES,
            "batch_size": BATCH_SIZE,
            "nopk_ms": elapsed_nopk.as_secs_f64() * 1000.0,
            "pk_ms": elapsed_pk.as_secs_f64() * 1000.0,
        }),
    );

    bg.shutdown().await;
    wal.close().await.unwrap();

    assert!(
        pass,
        "bulk INSERT with PK was {ratio:.2}x no-PK (bar: < {bar_ratio}x). \
         nopk={elapsed_nopk:?} pk={elapsed_pk:?}"
    );
}
