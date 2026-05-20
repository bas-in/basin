//! Phase 5.14.B5 — differential correctness test for HLL + t-digest sketch merge.
//!
//! Asserts that `APPROX_COUNT_DISTINCT` (HLL, B3) and `APPROX_PERCENTILE` (t-digest, B4)
//! produce results within their documented bounds when merging sketches across
//! multiple files, versus the exact answer over a single consolidated dataset.
//!
//! Correctness contract:
//!   "merged-N-files-of-(1M/N)-rows agrees with exact-over-1M-rows within bounds"
//!
//! Gates:
//!   - HLL:      ≤ 2.0% relative error vs `COUNT(DISTINCT col)`
//!   - t-digest: ≤ 1.0% absolute error (as fraction of range) vs exact percentile
//!               for p ∈ {0.5, 0.9, 0.95, 0.99}
//!
//! File counts tested: 1, 2, 5, 10, 25, 50, 100
//! Total rows:         1_000_000 (split evenly across files)
//! Columns:            id (Int64), score (Float64), tag (Utf8), rank (Int64)
//! RNG seed:           0xB5_B5_B5_B5_B5_B5_B5_B5 (fixed for determinism)

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog, SnapshotId};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig, WriteOptions};
use object_store::local::LocalFileSystem;
use rand::prelude::*;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const TOTAL_ROWS: usize = 1_000_000;
const RNG_SEED: u64 = 0xB5_B5_B5_B5_B5_B5_B5_B5;

/// Representative file counts: covers power-of-2 range + extremes.
const FILE_COUNTS: &[usize] = &[1, 2, 5, 10, 25, 50, 100];

/// Percentiles to check for t-digest accuracy.
const PERCENTILES: &[f64] = &[0.5, 0.9, 0.95, 0.99];

/// HLL gate: ≤ 2% relative error.
const HLL_MAX_REL_ERR: f64 = 0.02;

/// t-digest gate: ≤ 1% absolute error (fraction of the value range).
const TDIGEST_MAX_ABS_ERR: f64 = 0.01;

// ---------------------------------------------------------------------------
// Engine builder
// ---------------------------------------------------------------------------

fn build_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

async fn query_rows(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        Ok(other) => panic!("expected rows for {sql:?}, got {other:?}"),
        Err(e) => panic!("query {sql:?} failed: {e}"),
    }
}

fn extract_i64(batches: &[RecordBatch]) -> i64 {
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "expected 1 row, got {total}");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("expected Int64Array")
        .value(0)
}

fn extract_f64(batches: &[RecordBatch]) -> f64 {
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "expected 1 row, got {total}");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("expected Float64Array")
        .value(0)
}

// ---------------------------------------------------------------------------
// Schema and batch builder
// ---------------------------------------------------------------------------

/// 4-column schema: id (Int64), score (Float64), tag (Utf8), rank (Int64).
fn sketch_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
        Field::new("tag", DataType::Utf8, false),
        Field::new("rank", DataType::Int64, false),
    ]))
}

/// Generate a batch of `len` rows starting at global offset `start`.
///
/// - `id`:    globally unique integer in [start, start+len)
/// - `score`: unique float derived from id  → score = id as f64 * 0.001
/// - `tag`:   unique string derived from id → "t{id % 50000}" (50k distinct tags across 1M rows)
/// - `rank`:  id % 1000 (1000 distinct values — low cardinality for HLL boundary test)
fn make_batch(start: usize, len: usize) -> RecordBatch {
    let schema = sketch_schema();
    let ids: Int64Array = (start as i64..(start + len) as i64).collect();
    let scores: Float64Array = (start..start + len)
        .map(|i| i as f64 * 0.001)
        .collect::<Vec<_>>()
        .into();
    let tags: StringArray = (start..start + len)
        .map(|i| Some(format!("t{}", i % 50_000)))
        .collect();
    let ranks: Int64Array = (start..start + len)
        .map(|i| (i % 1_000) as i64)
        .collect::<Vec<_>>()
        .into();
    RecordBatch::try_new(schema, vec![
        Arc::new(ids),
        Arc::new(scores),
        Arc::new(tags),
        Arc::new(ranks),
    ])
    .unwrap()
}

// ---------------------------------------------------------------------------
// Storage write helper (same pattern as catalog_blooms.rs)
// ---------------------------------------------------------------------------

async fn write_and_commit(
    engine: &Engine,
    project: &ProjectId,
    table: &TableName,
    batch: RecordBatch,
    expected_snapshot: SnapshotId,
) -> SnapshotId {
    let opts = WriteOptions::default();
    let part = PartitionKey::default_key();
    let df = engine
        .config()
        .storage
        .write_batch_with_options(project, table, &part, &batch, &opts)
        .await
        .unwrap();
    let file_ref = DataFileRef {
        path: df.path.as_ref().to_string(),
        size_bytes: df.size_bytes,
        row_count: df.row_count,
        column_stats: df.column_stats,
        bloom_filters: df.bloom_filters,
        hll_sketches: df.hll_sketches,
        tdigest_sketches: df.tdigest_sketches,
    };
    let updated = engine
        .config()
        .catalog
        .append_data_files(project, table, expected_snapshot, vec![file_ref])
        .await
        .unwrap();
    updated.current_snapshot
}

// ---------------------------------------------------------------------------
// Exact reference values (computed once at test startup for the full 1M rows)
// ---------------------------------------------------------------------------

struct ExactRefs {
    /// COUNT(DISTINCT id) — should equal TOTAL_ROWS (all unique)
    distinct_id: i64,
    /// COUNT(DISTINCT tag) — 50_000 unique tags
    distinct_tag: i64,
    /// COUNT(DISTINCT rank) — 1_000 distinct ranks
    distinct_rank: i64,
    /// percentile_cont(p) for `score` at each p in PERCENTILES
    score_percentiles: Vec<f64>,
    /// percentile_cont(p) for `rank` at each p in PERCENTILES
    rank_percentiles: Vec<f64>,
    /// range of score (max - min) for relative error calculation
    score_range: f64,
    /// range of rank
    rank_range: f64,
}

impl ExactRefs {
    /// Compute exact values in-process without any SQL (pure Rust for speed).
    fn compute() -> Self {
        // id: 0..TOTAL_ROWS — all distinct
        let distinct_id = TOTAL_ROWS as i64;

        // tag: "t{i % 50000}" for i in 0..TOTAL_ROWS — 50_000 distinct
        let distinct_tag = 50_000i64;

        // rank: i % 1000 for i in 0..TOTAL_ROWS — 1_000 distinct
        let distinct_rank = 1_000i64;

        // score: i * 0.001 for i in 0..TOTAL_ROWS
        // score range: [0.0, 999_999.0 * 0.001] = [0.0, 999.999]
        let scores_min = 0.0f64;
        let scores_max = (TOTAL_ROWS - 1) as f64 * 0.001;
        let score_range = scores_max - scores_min;

        // rank: i % 1000 for i in 0..TOTAL_ROWS
        // rank values: 0..999, range = 999
        let rank_range = 999.0f64;

        // For a sorted uniform dataset [a..b] of n values,
        // percentile_cont(p) = a + p * (n - 1) * step
        // score step = 0.001, so score at p = p * (TOTAL_ROWS - 1) * 0.001
        let score_percentiles = PERCENTILES
            .iter()
            .map(|&p| scores_min + p * (TOTAL_ROWS as f64 - 1.0) * 0.001)
            .collect();

        // rank values are: 0,1,...,999,0,1,...,999,...  (cycled 1000 times).
        // For quantile on 1M values: the sorted sequence is
        //   [0,0,...,0 (1000 times), 1,1,...,1 (1000 times), ..., 999,...,999]
        // percentile_cont(p) = p * 999  (linear interpolation on sorted array)
        let rank_percentiles = PERCENTILES
            .iter()
            .map(|&p| p * 999.0)
            .collect();

        ExactRefs {
            distinct_id,
            distinct_tag,
            distinct_rank,
            score_percentiles,
            rank_percentiles,
            score_range,
            rank_range,
        }
    }
}

// ---------------------------------------------------------------------------
// Core test: for a given file count, write 1M rows spread across N files,
// then query approx functions and check bounds.
// ---------------------------------------------------------------------------

async fn run_one_file_count(
    file_count: usize,
    exact: &ExactRefs,
) -> FileCountResult {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let table_name = format!("t_sk_{file_count}");
    let table = TableName::new(&table_name).unwrap();

    let sess = engine.open_session(project).await.unwrap();
    sess.execute(&format!(
        "CREATE TABLE {table_name} (\
            id BIGINT NOT NULL, \
            score DOUBLE PRECISION NOT NULL, \
            tag TEXT NOT NULL, \
            rank BIGINT NOT NULL\
        ) WITH (basin.file_format='vortex')"
    ))
    .await
    .unwrap();

    let meta = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap();
    let mut snap = meta.current_snapshot;

    // Write TOTAL_ROWS spread evenly across `file_count` files.
    let rows_per_file = TOTAL_ROWS / file_count;
    for file_idx in 0..file_count {
        let start = file_idx * rows_per_file;
        // Last file absorbs any remainder.
        let len = if file_idx == file_count - 1 {
            TOTAL_ROWS - start
        } else {
            rows_per_file
        };
        let batch = make_batch(start, len);
        snap = write_and_commit(&engine, &project, &table, batch, snap).await;
    }

    println!(
        "[sketches] file_count={file_count}: wrote {TOTAL_ROWS} rows across {file_count} file(s)"
    );

    // ── HLL checks ──────────────────────────────────────────────────────────

    // id: all TOTAL_ROWS distinct
    let hll_id = extract_i64(
        &query_rows(&sess, &format!(
            "SELECT APPROX_COUNT_DISTINCT(id) FROM {table_name}"
        )).await
    );
    let hll_id_err = (hll_id as f64 - exact.distinct_id as f64).abs() / exact.distinct_id as f64;

    // tag: 50_000 distinct
    let hll_tag = extract_i64(
        &query_rows(&sess, &format!(
            "SELECT APPROX_COUNT_DISTINCT(tag) FROM {table_name}"
        )).await
    );
    let hll_tag_err = (hll_tag as f64 - exact.distinct_tag as f64).abs() / exact.distinct_tag as f64;

    // rank: 1_000 distinct
    let hll_rank = extract_i64(
        &query_rows(&sess, &format!(
            "SELECT APPROX_COUNT_DISTINCT(rank) FROM {table_name}"
        )).await
    );
    let hll_rank_err = (hll_rank as f64 - exact.distinct_rank as f64).abs() / exact.distinct_rank as f64;

    // ── t-digest checks ─────────────────────────────────────────────────────

    let mut score_errs: Vec<f64> = Vec::new();
    let mut rank_errs: Vec<f64> = Vec::new();

    for (idx, &p) in PERCENTILES.iter().enumerate() {
        // score (Float64)
        let approx_score = extract_f64(
            &query_rows(&sess, &format!(
                "SELECT APPROX_PERCENTILE(score, {p}) FROM {table_name}"
            )).await
        );
        let exact_score = exact.score_percentiles[idx];
        let score_abs_err = (approx_score - exact_score).abs() / exact.score_range;
        score_errs.push(score_abs_err);

        // rank (Int64)
        let approx_rank = extract_f64(
            &query_rows(&sess, &format!(
                "SELECT APPROX_PERCENTILE(rank, {p}) FROM {table_name}"
            )).await
        );
        let exact_rank = exact.rank_percentiles[idx];
        let rank_abs_err = (approx_rank - exact_rank).abs() / exact.rank_range;
        rank_errs.push(rank_abs_err);
    }

    let result = FileCountResult {
        file_count,
        hll_id,
        hll_id_err,
        hll_tag,
        hll_tag_err,
        hll_rank,
        hll_rank_err,
        score_errs: score_errs.clone(),
        rank_errs: rank_errs.clone(),
    };

    println!(
        "[sketches] file_count={file_count}: \
         HLL id_est={hll_id} err={:.3}% | \
         tag_est={hll_tag} err={:.3}% | \
         rank_est={hll_rank} err={:.3}%",
        hll_id_err * 100.0,
        hll_tag_err * 100.0,
        hll_rank_err * 100.0,
    );
    for (idx, &p) in PERCENTILES.iter().enumerate() {
        println!(
            "[sketches] file_count={file_count}: \
             t-digest p={p}: score_err={:.4}% rank_err={:.4}%",
            score_errs[idx] * 100.0,
            rank_errs[idx] * 100.0,
        );
    }

    result
}

struct FileCountResult {
    file_count: usize,
    hll_id: i64,
    hll_id_err: f64,
    hll_tag: i64,
    hll_tag_err: f64,
    hll_rank: i64,
    hll_rank_err: f64,
    score_errs: Vec<f64>,
    rank_errs: Vec<f64>,
}

// ---------------------------------------------------------------------------
// Main test entry point
// ---------------------------------------------------------------------------

/// Differential sketch merge test across representative file counts.
///
/// 1M-row 4-column table partitioned across {1,2,5,10,25,50,100} files.
/// Asserts HLL ≤ 2% relative error and t-digest ≤ 1% absolute error
/// (fraction of range) for every file count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "slow: 1M rows × 7 file counts (~5-10 min). Run with: cargo test -p tests-integration --test sketches -- --ignored"]
async fn sketch_merge_differential() {
    // Suppress unused warning — we keep a fixed-seed rng to document the
    // deterministic intent even though the data generator is deterministic
    // by construction (no randomness needed for sequential data).
    let _rng = StdRng::seed_from_u64(RNG_SEED);

    let exact = ExactRefs::compute();

    println!(
        "[sketches] exact references: \
         distinct_id={} distinct_tag={} distinct_rank={}",
        exact.distinct_id, exact.distinct_tag, exact.distinct_rank
    );
    for (idx, &p) in PERCENTILES.iter().enumerate() {
        println!(
            "[sketches] exact p={p}: score={:.4} rank={:.4}",
            exact.score_percentiles[idx],
            exact.rank_percentiles[idx],
        );
    }

    let mut results: Vec<FileCountResult> = Vec::new();
    for &fc in FILE_COUNTS {
        let r = run_one_file_count(fc, &exact).await;
        results.push(r);
    }

    // ── Gate assertions ─────────────────────────────────────────────────────

    let mut failures: Vec<String> = Vec::new();

    for r in &results {
        let fc = r.file_count;

        // HLL: id
        if r.hll_id_err > HLL_MAX_REL_ERR {
            failures.push(format!(
                "file_count={fc} HLL(id): est={} exact={} rel_err={:.4} > {:.4} ({}%)",
                r.hll_id, exact.distinct_id, r.hll_id_err, HLL_MAX_REL_ERR,
                HLL_MAX_REL_ERR * 100.0
            ));
        }

        // HLL: tag
        if r.hll_tag_err > HLL_MAX_REL_ERR {
            failures.push(format!(
                "file_count={fc} HLL(tag): est={} exact={} rel_err={:.4} > {:.4} ({}%)",
                r.hll_tag, exact.distinct_tag, r.hll_tag_err, HLL_MAX_REL_ERR,
                HLL_MAX_REL_ERR * 100.0
            ));
        }

        // HLL: rank
        if r.hll_rank_err > HLL_MAX_REL_ERR {
            failures.push(format!(
                "file_count={fc} HLL(rank): est={} exact={} rel_err={:.4} > {:.4} ({}%)",
                r.hll_rank, exact.distinct_rank, r.hll_rank_err, HLL_MAX_REL_ERR,
                HLL_MAX_REL_ERR * 100.0
            ));
        }

        // t-digest: score and rank
        for (idx, &p) in PERCENTILES.iter().enumerate() {
            if r.score_errs[idx] > TDIGEST_MAX_ABS_ERR {
                failures.push(format!(
                    "file_count={fc} t-digest(score, p={p}): \
                     abs_err={:.4} > {:.4} ({}%)",
                    r.score_errs[idx], TDIGEST_MAX_ABS_ERR,
                    TDIGEST_MAX_ABS_ERR * 100.0
                ));
            }
            if r.rank_errs[idx] > TDIGEST_MAX_ABS_ERR {
                failures.push(format!(
                    "file_count={fc} t-digest(rank, p={p}): \
                     abs_err={:.4} > {:.4} ({}%)",
                    r.rank_errs[idx], TDIGEST_MAX_ABS_ERR,
                    TDIGEST_MAX_ABS_ERR * 100.0
                ));
            }
        }
    }

    // Print summary table
    println!("\n[sketches] === SUMMARY ===");
    println!(
        "{:>12}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}",
        "file_count",
        "HLL(id)%",
        "HLL(tag)%",
        "HLL(rank)%",
        "td(sc,p50)%",
        "td(sc,p99)%",
        "td(rk,p50)%",
        "td(rk,p99)%",
    );
    for r in &results {
        println!(
            "{:>12}  {:>10.3}  {:>10.3}  {:>10.3}  {:>11.4}  {:>11.4}  {:>11.4}  {:>11.4}",
            r.file_count,
            r.hll_id_err * 100.0,
            r.hll_tag_err * 100.0,
            r.hll_rank_err * 100.0,
            r.score_errs[0] * 100.0,  // p50
            r.score_errs[3] * 100.0,  // p99
            r.rank_errs[0] * 100.0,   // p50
            r.rank_errs[3] * 100.0,   // p99
        );
    }

    if !failures.is_empty() {
        let msg = failures.join("\n  ");
        panic!(
            "[sketches] {} gate violation(s):\n  {msg}",
            failures.len()
        );
    }

    println!("[sketches] ALL GATES PASSED");
}
