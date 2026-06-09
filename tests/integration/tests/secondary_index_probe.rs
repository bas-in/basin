//! Diagnostic: point-SELECT via a non-PK column (user_id), with and without a
//! secondary B-tree index, using the in-process engine path (no pgwire).
//!
//! Background (from secondary_index.rs + executor.rs exploration):
//!
//!   `CREATE INDEX <name> ON <table> (<col>)` is accepted and records the index
//!   in the catalog's `TableMetadata::indexes` list (returned tag: "CREATE
//!   INDEX").  The in-memory `secondary_index::ProjectIndexRegistry` is
//!   populated **at INSERT time** for rows inserted *after* the CREATE INDEX —
//!   rows that existed before the index declaration are NOT automatically
//!   backfilled.
//!
//! Probe design (two-table approach to get a true before/after on identical
//! data volumes):
//!
//!   t_noindex : seed 100k rows first, then CREATE INDEX (pre-existing rows are
//!               not in the in-memory registry, so queries do a full scan).
//!               Measured as the "no-index" baseline.
//!
//!   t_indexed : CREATE INDEX first (on empty table), then seed the same 100k
//!               rows (so every inserted row is recorded in the registry).
//!               Measured as the "indexed" phase.
//!
//! For each phase we measure p50 and p99 over 200 iterations of
//! `SELECT * FROM t WHERE user_id = k` with a distinct k per iteration.
//!
//! Correctness sanity: assert the SELECT returns the expected row count for at
//! least one known key in each phase (user_id values are in [0, 1000), so each
//! unique user_id has ~100 rows).
//!
//! Mirrors `update_latency_probe.rs`: TempDir storage + InMemoryCatalog +
//! LocalWal + Shard + background flush.

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

// ── engine build (mirrors update_latency_probe) ───────────────────────────────

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

// ── percentile (same as update_latency_probe) ─────────────────────────────────

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

// ── seeding ───────────────────────────────────────────────────────────────────

/// Seed `total` rows into `table` in batches of `batch`.
///
/// Schema: id BIGINT PK, user_id BIGINT (≈ total/1000 rows per value),
///         v BIGINT.
async fn seed(sess: &basin_engine::ProjectSession, table: &str, total: i64, batch: i64) {
    let mut id = 0i64;
    while id < total {
        let hi = (id + batch).min(total);
        let mut stmt = String::with_capacity((hi - id) as usize * 24);
        stmt.push_str(&format!("INSERT INTO {table} VALUES "));
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            let user_id = k % 1_000;
            stmt.push_str(&format!("({k},{user_id},{k})"));
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
    }
}

// ── measure ───────────────────────────────────────────────────────────────────

/// Measure p50 and p99 over `n` iterations of `SELECT * FROM table WHERE
/// user_id = k`, cycling through `n` distinct user_id values.
/// Returns `(p50_ms, p99_ms)`.
async fn measure(sess: &basin_engine::ProjectSession, table: &str, n: usize) -> (f64, f64) {
    let mut times = Vec::with_capacity(n);
    for i in 0..n {
        // Spread evenly across the 1000 distinct user_id values.
        let uid = (i as i64) % 1_000;
        let t0 = Instant::now();
        let _res = sess
            .execute(&format!("SELECT * FROM {table} WHERE user_id = {uid}"))
            .await
            .unwrap();
        times.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = percentile(&times, 0.50);
    let p99 = percentile(&times, 0.99);
    (p50, p99)
}

/// Return the row count for a `SELECT WHERE user_id = k` query.
async fn count_for_uid(
    sess: &basin_engine::ProjectSession,
    table: &str,
    uid: i64,
) -> usize {
    let res = sess
        .execute(&format!("SELECT * FROM {table} WHERE user_id = {uid}"))
        .await
        .unwrap();
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

// ── probe ─────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic — run explicitly with --ignored"]
async fn secondary_index_probe() {
    const TOTAL: i64 = 100_000;
    const BATCH: i64 = 5_000;
    const ITERS: usize = 200;
    // Each user_id value has TOTAL/1000 = 100 rows.
    const ROWS_PER_UID: usize = (TOTAL / 1_000) as usize;

    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // ── Phase 1: no-index baseline ────────────────────────────────────────────
    //
    // Seed all rows FIRST, then create the index. Pre-existing rows are not
    // backfilled into the in-memory registry, so queries fall through to a
    // full scan — this is the "no-index" baseline.
    sess.execute(
        "CREATE TABLE t_noindex (id BIGINT PRIMARY KEY, user_id BIGINT, v BIGINT)",
    )
    .await
    .unwrap();

    seed(&sess, "t_noindex", TOTAL, BATCH).await;
    shard.flush_to_parquet().await.unwrap();

    // Warm-up: one unindexed scan before the timed window.
    let _ = sess
        .execute("SELECT * FROM t_noindex WHERE user_id = 42")
        .await
        .unwrap();

    // Create the index AFTER seeding — pre-existing rows not backfilled.
    // We still call CREATE INDEX so the catalog entry exists; the measured
    // queries will be full scans because the registry has no entries for
    // pre-existing rows.
    let idx_result = sess
        .execute("CREATE INDEX t_noindex_user_id_idx ON t_noindex (user_id)")
        .await;

    let idx_supported = match &idx_result {
        Ok(ExecResult::Empty { tag }) if tag == "CREATE INDEX" => true,
        Ok(other) => {
            println!(
                "[secondary-index] CREATE INDEX returned unexpected result: {other:?}; \
                 treating as unsupported"
            );
            false
        }
        Err(e) => {
            println!("[secondary-index] CREATE INDEX unsupported: {e}");
            false
        }
    };

    let (noindex_p50, noindex_p99) = measure(&sess, "t_noindex", ITERS).await;
    println!(
        "[secondary-index] no-index p50={noindex_p50:.3}ms p99={noindex_p99:.3}ms \
         (full-scan, pre-index rows not backfilled)"
    );

    // Correctness sanity: uid=7 should return exactly ROWS_PER_UID rows.
    let got = count_for_uid(&sess, "t_noindex", 7).await;
    assert_eq!(
        got, ROWS_PER_UID,
        "no-index phase: uid=7 should have {ROWS_PER_UID} rows, got {got}"
    );

    if !idx_supported {
        println!("[secondary-index] skipping indexed phase (CREATE INDEX unsupported)");
        bg.shutdown().await;
        wal.close().await.unwrap();
        return;
    }

    // ── Phase 2: indexed ──────────────────────────────────────────────────────
    //
    // CREATE INDEX first (on empty table), then INSERT all rows so every
    // inserted row is recorded in the in-memory secondary index registry.
    // Queries can therefore use the index for file-level pruning.
    sess.execute(
        "CREATE TABLE t_indexed (id BIGINT PRIMARY KEY, user_id BIGINT, v BIGINT)",
    )
    .await
    .unwrap();

    sess.execute("CREATE INDEX t_indexed_user_id_idx ON t_indexed (user_id)")
        .await
        .unwrap_or_else(|e| panic!("CREATE INDEX on t_indexed: {e}"));

    seed(&sess, "t_indexed", TOTAL, BATCH).await;
    shard.flush_to_parquet().await.unwrap();

    // Warm-up: one indexed scan before the timed window.
    let _ = sess
        .execute("SELECT * FROM t_indexed WHERE user_id = 42")
        .await
        .unwrap();

    let (indexed_p50, indexed_p99) = measure(&sess, "t_indexed", ITERS).await;
    println!(
        "[secondary-index] indexed   p50={indexed_p50:.3}ms p99={indexed_p99:.3}ms \
         (index populated at INSERT time)"
    );

    // Correctness sanity: uid=7 should still return exactly ROWS_PER_UID rows.
    let got_idx = count_for_uid(&sess, "t_indexed", 7).await;
    assert_eq!(
        got_idx, ROWS_PER_UID,
        "indexed phase: uid=7 should have {ROWS_PER_UID} rows, got {got_idx}"
    );

    // Summary line.
    let speedup = noindex_p50 / indexed_p50;
    println!(
        "[secondary-index] speedup p50={speedup:.2}x  (no-index/indexed)"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}
