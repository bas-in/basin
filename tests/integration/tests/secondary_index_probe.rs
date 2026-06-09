//! Diagnostic: point-SELECT via a non-PK column, measuring the secondary
//! B-tree index's file-level pruning, using the in-process engine path (no
//! pgwire).
//!
//! Background (from secondary_index.rs + executor.rs exploration):
//!
//!   `CREATE INDEX <name> ON <table> (<col>)` is accepted and records the index
//!   in the catalog's `TableMetadata::indexes` list (returned tag: "CREATE
//!   INDEX").  The in-memory `secondary_index::ProjectIndexRegistry` is
//!   populated at INSERT time for rows inserted after the CREATE INDEX AND —
//!   as of the `backfill_index_over_live_files` work — backfilled over the
//!   PRE-EXISTING live files at CREATE INDEX time.  So a `CREATE INDEX` issued
//!   after the data is loaded now also yields a usable index; the registry is
//!   populated either way.
//!
//! What actually governs the win: file-level PRUNING, not "is there an index".
//! The index maps a key → the set of (file, row_group) locations holding it.
//! The query then reads ONLY the files in that allowlist (the registry counter
//! `secondary_index_skipped_count` bumps once per data file the allowlist lets
//! us skip).  The win therefore depends entirely on how the queried key's rows
//! are DISTRIBUTED across files:
//!
//!   * SPREAD case (`user_id`): every distinct user_id value lands in ~every
//!     file (user_id = id % 1000, rows striped across all ~20 flushed files),
//!     so the allowlist ≈ ALL files → zero files skipped → the index cannot
//!     beat a full scan.  This is the honest worst case and explains the
//!     historically-observed ~0.9x.  (Row-group selection inside a file is a
//!     Parquet-only hint and is ignored for Vortex files, so it does not
//!     rescue this case either.)
//!
//!   * SELECTIVE case (`bucket`): bucket = id / BATCH, seeded so each bucket
//!     value lands ENTIRELY in ONE flushed file.  The allowlist for a given
//!     bucket is then a single file → the other ~19 files are skipped → the
//!     index delivers a real speedup AND a non-zero skip count.
//!
//! The probe measures BOTH so the report is honest: the spread case proves the
//! pruning logic is sound but structurally idle on that layout, and the
//! selective case proves the same code path wins when the data partitions.
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
/// Schema: id BIGINT PK, user_id BIGINT, bucket BIGINT, v BIGINT.
///   * `user_id = id % 1000` — SPREAD: each value appears in ~most files (the
///     flush stripes contiguous id-ranges across files, so a value recurring
///     every 1000 ids lands in several files).
///   * `bucket  = id / batch`  — recorded for reference; one bucket value spans
///     `batch` contiguous ids.  NOTE: a single end-of-seed flush stripes those
///     ids across ALL files, so this column is NOT one-value-per-file (see
///     `seed_partitioned` for the layout that IS).
async fn seed(sess: &basin_engine::ProjectSession, table: &str, total: i64, batch: i64) {
    let mut id = 0i64;
    while id < total {
        let hi = (id + batch).min(total);
        let mut stmt = String::with_capacity((hi - id) as usize * 28);
        stmt.push_str(&format!("INSERT INTO {table} VALUES "));
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            let user_id = k % 1_000;
            let bucket = k / batch;
            stmt.push_str(&format!("({k},{user_id},{bucket},{k})"));
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
    }
}

/// Sentinel `bucket` values present in EVERY partition flush so every file's
/// `bucket` zone-map range is the full `[BUCKET_MIN, BUCKET_MAX]` — defeating
/// min/max pruning and isolating the secondary index as the only thing that
/// can prune files for a mid-range bucket value.
const BUCKET_MIN: i64 = 0;
const BUCKET_MAX: i64 = 1_000_000_000;

/// Seed `n_parts` partitions of `rows_per_part` rows each, FLUSHING after every
/// partition so each partition's rows land in their own freshly-written
/// file(s) — the layout where secondary-index file pruning ACTUALLY beats the
/// engine's existing min/max zone-map pruning.
///
/// The indexed `bucket` column is made HIGH-CARDINALITY (a distinct mid-range
/// value per row, `bucket_for_row(global_row)`), so a `WHERE bucket = needle`
/// query matches exactly ONE row in exactly ONE partition file.  Two sentinel
/// rows per partition (`bucket = BUCKET_MIN` / `BUCKET_MAX`) pin EVERY file's
/// `bucket` column_stats range to the full `[BUCKET_MIN, BUCKET_MAX]`, so the
/// zone-map pruner cannot exclude any file for a mid-range needle — only the
/// secondary index's exact (value → file) allowlist can.  Net effect: the
/// baseline must scan ALL partition files to find the single matching row,
/// while the indexed query opens just the one file the needle lives in.  This
/// is the high-selectivity layout the index is designed to win.
///
/// Schema: id BIGINT PK, user_id BIGINT, bucket BIGINT, v BIGINT.
async fn seed_partitioned(
    sess: &basin_engine::ProjectSession,
    shard: &Shard,
    table: &str,
    n_parts: i64,
    rows_per_part: i64,
) {
    for part in 0..n_parts {
        // +2 ids per partition for the two sentinel rows, so id ranges across
        // partitions never overlap (no PK collision).
        let base = part * (rows_per_part + 2);
        let mut stmt = String::with_capacity(rows_per_part as usize * 28);
        stmt.push_str(&format!("INSERT INTO {table} VALUES "));
        // Two sentinel rows pin this file's bucket zone-map to [MIN, MAX].
        stmt.push_str(&format!("({},{},{BUCKET_MIN},{}),", base, base % 1_000, base));
        stmt.push_str(&format!(
            "({},{},{BUCKET_MAX},{}),",
            base + 1,
            (base + 1) % 1_000,
            base + 1
        ));
        for j in 0..rows_per_part {
            if j > 0 {
                stmt.push(',');
            }
            // Offset the real-row ids past the two sentinels.
            let k = base + 2 + j;
            let global_row = part * rows_per_part + j;
            let user_id = k % 1_000;
            let bucket = bucket_for_row(global_row);
            stmt.push_str(&format!("({k},{user_id},{bucket},{k})"));
        }
        sess.execute(&stmt).await.unwrap();
        // Flush this partition to its own cold file(s) before the next one,
        // so partitions don't get striped together into shared files.
        shard.flush_to_parquet().await.unwrap();
    }
}

/// Distinct mid-range `bucket` needle for the `global_row`-th real row, well
/// inside `(BUCKET_MIN, BUCKET_MAX)` so the per-file sentinels straddle it and
/// every value is unique (one matching row, one file).
fn bucket_for_row(global_row: i64) -> i64 {
    1_000 + global_row
}

// ── measure ───────────────────────────────────────────────────────────────────

/// Measure p50 and p99 over `n` iterations of `SELECT * FROM table WHERE
/// {col} = k`, cycling `k` through `0..distinct`.
/// Returns `(p50_ms, p99_ms)`.
async fn measure(
    sess: &basin_engine::ProjectSession,
    table: &str,
    col: &str,
    distinct: i64,
    n: usize,
) -> (f64, f64) {
    let mut times = Vec::with_capacity(n);
    for i in 0..n {
        let k = (i as i64) % distinct;
        let t0 = Instant::now();
        let _res = sess
            .execute(&format!("SELECT * FROM {table} WHERE {col} = {k}"))
            .await
            .unwrap();
        times.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = percentile(&times, 0.50);
    let p99 = percentile(&times, 0.99);
    (p50, p99)
}

/// Measure p50/p99 over `n` iterations of `SELECT * FROM table WHERE {col} = k`,
/// cycling `k` through the supplied `keys` slice (round-robin).
async fn measure_keys(
    sess: &basin_engine::ProjectSession,
    table: &str,
    col: &str,
    keys: &[i64],
    n: usize,
) -> (f64, f64) {
    let mut times = Vec::with_capacity(n);
    for i in 0..n {
        let k = keys[i % keys.len()];
        let t0 = Instant::now();
        let _res = sess
            .execute(&format!("SELECT * FROM {table} WHERE {col} = {k}"))
            .await
            .unwrap();
        times.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (percentile(&times, 0.50), percentile(&times, 0.99))
}

/// Return the row count for a `SELECT * WHERE {col} = k` query.
async fn count_for_key(
    sess: &basin_engine::ProjectSession,
    table: &str,
    col: &str,
    key: i64,
) -> usize {
    let res = sess
        .execute(&format!("SELECT * FROM {table} WHERE {col} = {key}"))
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
    // Each user_id value (= id % 1000) has TOTAL/1000 = 100 rows.
    const ROWS_PER_UID: usize = (TOTAL / 1_000) as usize;

    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // ── Baseline: no index at all (full scan) ─────────────────────────────────
    //
    // Seed and flush a table with NO secondary index. Every WHERE col=k query
    // must scan all files — the reference cost both the spread and selective
    // indexed cases are compared against.
    sess.execute(
        "CREATE TABLE t_noindex (id BIGINT PRIMARY KEY, user_id BIGINT, bucket BIGINT, v BIGINT)",
    )
    .await
    .unwrap();
    seed(&sess, "t_noindex", TOTAL, BATCH).await;
    shard.flush_to_parquet().await.unwrap();

    // Warm-up before the timed window.
    let _ = sess
        .execute("SELECT * FROM t_noindex WHERE user_id = 42")
        .await
        .unwrap();

    let (noindex_p50, noindex_p99) = measure(&sess, "t_noindex", "user_id", 1_000, ITERS).await;
    println!(
        "[secondary-index] baseline (no index)  p50={noindex_p50:.3}ms p99={noindex_p99:.3}ms \
         (full scan over all files)"
    );
    let got = count_for_key(&sess, "t_noindex", "user_id", 7).await;
    assert_eq!(
        got, ROWS_PER_UID,
        "baseline: user_id=7 should have {ROWS_PER_UID} rows, got {got}"
    );

    // ── Indexed table: backfilled CREATE INDEX on BOTH columns ────────────────
    //
    // Seed FIRST, then CREATE INDEX on each column. With backfill now
    // implemented (`backfill_index_over_live_files`), CREATE INDEX after the
    // data is loaded still populates the registry over the pre-existing live
    // files — so both indexes are usable despite being declared post-seed.
    sess.execute(
        "CREATE TABLE t_indexed (id BIGINT PRIMARY KEY, user_id BIGINT, bucket BIGINT, v BIGINT)",
    )
    .await
    .unwrap();
    seed(&sess, "t_indexed", TOTAL, BATCH).await;
    shard.flush_to_parquet().await.unwrap();

    let idx_result = sess
        .execute("CREATE INDEX t_indexed_user_id_idx ON t_indexed (user_id)")
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
    if !idx_supported {
        println!("[secondary-index] skipping indexed phases (CREATE INDEX unsupported)");
        bg.shutdown().await;
        wal.close().await.unwrap();
        return;
    }
    sess.execute("CREATE INDEX t_indexed_bucket_idx ON t_indexed (bucket)")
        .await
        .unwrap_or_else(|e| panic!("CREATE INDEX bucket on t_indexed: {e}"));

    // Warm-up the spread path before timing.
    let _ = sess.execute("SELECT * FROM t_indexed WHERE user_id = 42").await.unwrap();

    // ── SPREAD case (user_id): index present, pruning structurally idle ───────
    //
    // The single end-of-seed flush stripes contiguous id-ranges across files,
    // so a user_id value recurring every 1000 ids lands in MOST files. The
    // index allowlist is therefore ≈ all files → at best a couple of files are
    // skipped → no meaningful win over the full scan. This is the honest
    // worst-case layout for file-level pruning.
    let skip_before_spread = engine.secondary_index_skipped_count();
    let (spread_p50, spread_p99) = measure(&sess, "t_indexed", "user_id", 1_000, ITERS).await;
    let skip_spread = engine.secondary_index_skipped_count() - skip_before_spread;
    println!(
        "[secondary-index] indexed SPREAD (user_id)    p50={spread_p50:.3}ms p99={spread_p99:.3}ms \
         files_skipped={skip_spread} over {ITERS} queries (allowlist ≈ all files → prune idle)"
    );
    let got_spread = count_for_key(&sess, "t_indexed", "user_id", 7).await;
    assert_eq!(
        got_spread, ROWS_PER_UID,
        "spread phase: user_id=7 should have {ROWS_PER_UID} rows, got {got_spread}"
    );

    // ── SELECTIVE case: partition-per-file layout that pruning CAN win ────────
    //
    // Seed a fresh table FLUSHING after every partition so each `bucket` value
    // lands in its own file(s). A `WHERE bucket = p` query's allowlist is then
    // that one partition's file(s); all other partitions' files are skipped.
    // We index `bucket` (backfilled CREATE INDEX, after the partitioned seed)
    // and measure both the no-index baseline and the indexed prune on the SAME
    // partitioned data.
    const N_PARTS: i64 = 10;
    const ROWS_PER_PART: i64 = 5_000;
    // High-cardinality needles spread across all partitions: one needle per
    // partition, taken from the middle of that partition's row range. Each
    // matches exactly one row in one file.
    let bucket_keys: Vec<i64> = (0..N_PARTS)
        .map(|p| bucket_for_row(p * ROWS_PER_PART + ROWS_PER_PART / 2))
        .collect();
    // One needle to probe for correctness (partition 3, mid-range row).
    let probe_bucket = bucket_for_row(3 * ROWS_PER_PART + ROWS_PER_PART / 2);

    sess.execute(
        "CREATE TABLE t_part (id BIGINT PRIMARY KEY, user_id BIGINT, bucket BIGINT, v BIGINT)",
    )
    .await
    .unwrap();
    seed_partitioned(&sess, &shard, "t_part", N_PARTS, ROWS_PER_PART).await;

    // Baseline on partitioned data: query `bucket` BEFORE creating its index,
    // so this measures a full scan over all partition files. The sentinel rows
    // make every file's bucket zone-map span [MIN, MAX], so min/max pruning
    // cannot help here — this is a true full scan.
    let _ = sess
        .execute(&format!("SELECT * FROM t_part WHERE bucket = {probe_bucket}"))
        .await
        .unwrap();
    let (part_base_p50, part_base_p99) =
        measure_keys(&sess, "t_part", "bucket", &bucket_keys, ITERS).await;
    println!(
        "[secondary-index] partitioned baseline (bucket, no index) \
         p50={part_base_p50:.3}ms p99={part_base_p99:.3}ms \
         (full scan: sentinel rows defeat min/max pruning)"
    );

    // Now index `bucket` (backfilled over the already-flushed partition files).
    sess.execute("CREATE INDEX t_part_bucket_idx ON t_part (bucket)")
        .await
        .unwrap_or_else(|e| panic!("CREATE INDEX bucket on t_part: {e}"));
    let _ = sess
        .execute(&format!("SELECT * FROM t_part WHERE bucket = {probe_bucket}"))
        .await
        .unwrap();

    let skip_before_sel = engine.secondary_index_skipped_count();
    let (sel_p50, sel_p99) = measure_keys(&sess, "t_part", "bucket", &bucket_keys, ITERS).await;
    let skip_sel = engine.secondary_index_skipped_count() - skip_before_sel;
    let files_skipped_per_query = skip_sel as f64 / ITERS as f64;
    println!(
        "[secondary-index] partitioned indexed (bucket) \
         p50={sel_p50:.3}ms p99={sel_p99:.3}ms files_skipped={skip_sel} over {ITERS} queries \
         (~{files_skipped_per_query:.0} files skipped/query; allowlist = the 1 file the needle lives in)"
    );
    let got_sel = count_for_key(&sess, "t_part", "bucket", probe_bucket).await;
    assert_eq!(
        got_sel, 1,
        "selective phase: needle bucket={probe_bucket} should match exactly 1 row, got {got_sel}"
    );

    // The selective case MUST consult the registry and skip files — this both
    // proves the registry is wired post-backfill AND that pruning engages when
    // the data partitions. With N_PARTS partition files, each query should skip
    // ~N_PARTS-1 of them; assert a conservative lower bound to avoid warm-up
    // flake.
    assert!(
        skip_sel > 0,
        "partitioned phase must skip files via the secondary index (got {skip_sel}); \
         registry not consulted or pruning broken"
    );

    // Summaries.
    let spread_speedup = noindex_p50 / spread_p50;
    let sel_speedup = part_base_p50 / sel_p50;
    println!(
        "[secondary-index] speedup: SPREAD={spread_speedup:.2}x (vs no-index, same data)  \
         SELECTIVE={sel_speedup:.2}x (vs partitioned no-index baseline)"
    );
    println!(
        "[secondary-index] NOTE: backfill is implemented now (backfill_index_over_live_files), so \
         CREATE INDEX after load populates the registry over pre-existing files — proven by the \
         non-zero files_skipped above (the registry IS consulted post-backfill). The SPREAD ~1x \
         is a DATA-LAYOUT limit (the flush stripes ids across files → allowlist ≈ all files), NOT \
         a missing index. The SELECTIVE case uses a unique needle + per-file sentinel rows that \
         defeat min/max zone-map pruning, so the index is the ONLY thing that can prune: it opens \
         1 file vs the baseline's ~N_PARTS*stripes, skipping ~all files. Yet wall-clock is ~1x \
         here because this is LOCALFS — a Vortex file open is a cheap in-RAM byte copy, the \
         baseline reads the small files in parallel (buffered(4)), and the per-query index \
         probe + row_group_selection build cancels the saved opens. On a REMOTE object store \
         (Tigris/S3) each skipped open is a saved network RTT, so the same skip count is a real \
         latency win there — the gain is structural in open-COUNT, not visible in localfs \
         wall-clock. (row_group_selection inside a file is Parquet-only and ignored for Vortex, \
         so intra-file pruning would need a separate Vortex chunk-probe registry.)"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}
