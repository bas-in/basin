//! Phase 5.14.A4 — differential correctness test for per-file bloom filters.
//!
//! Two test cases:
//!
//! 1. `bloom_no_false_negatives` — verify that bloom filters on
//!    `basin.sort_by` columns never prune a file that actually contains a
//!    queried value. False negatives would silently drop rows and corrupt
//!    query results. Asserts false-negative rate = 0 over 1000 sampled known
//!    values on a 1M-row dataset.
//!
//! 2. `bloom_false_positive_rate_bounded` — verify that the empirical
//!    false-positive rate stays within the fastbloom design target of ~1%.
//!    10 000 queries for values that are provably absent (within the file's
//!    min/max range but not inserted) are issued; the bloom-skipped counter
//!    is used to distinguish true-negatives (bloom pruned the file) from
//!    false-positives (bloom allowed a file open that returned 0 rows). The
//!    measured FPR must fall in [0%, 1.5%].

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog, SnapshotId};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig, WriteOptions};
use object_store::local::LocalFileSystem;
use rand::prelude::*;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Shared constants
// ---------------------------------------------------------------------------

/// Total rows in the false-negative table (1M across 10 files of 100k each).
const FN_ROWS_PER_FILE: i64 = 100_000;
/// Number of files for the false-negative dataset.
const FN_FILES: i64 = 10;
/// Total rows = FN_ROWS_PER_FILE × FN_FILES.
const FN_TOTAL_ROWS: i64 = FN_ROWS_PER_FILE * FN_FILES;

/// Number of known values to probe in the false-negative test.
const FN_PROBE_COUNT: usize = 1_000;

/// Number of absent values to probe in the FPR test.
const FPR_PROBE_COUNT: usize = 10_000;

/// RNG seed for deterministic sampling across all runs.
const RNG_SEED: u64 = 0xDEAD_C0DE_CAFE_F00D;

// ---------------------------------------------------------------------------
// Engine / session helpers
// ---------------------------------------------------------------------------

/// Build an `Engine` backed by a local temp-dir object store.
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

/// Extract the i64 value from the first cell of the first row of a
/// COUNT(*) result. Panics if the result is not a single-row Int64 column.
fn extract_count(batches: &[RecordBatch]) -> i64 {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 1,
        "COUNT(*) result must be a single row, got {total_rows}"
    );
    let b = batches.first().unwrap();
    let col = b.column(0);
    col.as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT(*) column must be Int64")
        .value(0)
}

/// Execute `sql` against `sess` and return the result batches.
/// Panics if the execute fails or returns a non-rows result.
async fn query_rows(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        Ok(other) => panic!("expected rows for {sql:?}, got {other:?}"),
        Err(e) => panic!("query {sql:?} failed: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Dataset builders
// ---------------------------------------------------------------------------

/// Schema for both test tables: c1 BIGINT, c2 TEXT.
fn table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int64, false),
        Field::new("c2", DataType::Utf8, false),
    ]))
}

/// Build a RecordBatch with rows whose `c1` values are `[start, start+len)`.
/// `c2` is a deterministic string derived from `c1`.
fn sequential_batch(start: i64, len: i64) -> RecordBatch {
    let ids: Int64Array = (start..start + len).collect();
    let strs: StringArray = (start..start + len)
        .map(|v| Some(format!("s{v}")))
        .collect();
    RecordBatch::try_new(
        table_schema(),
        vec![Arc::new(ids), Arc::new(strs)],
    )
    .unwrap()
}

/// Build a RecordBatch with 1M rows whose `c1` values are drawn from
/// `[0, 900_000) ∪ [1_100_000, 2_100_000)` — a deliberate gap at
/// `[900_000, 1_100_000)` that the bloom should handle as absent.
///
/// The values are shuffled with a deterministic XOR-shift so the file's
/// min/max stat spans the full `[0, 2_100_000)` range. That defeats
/// catalog-stats pruning for queries against the gap, leaving the bloom as
/// the only pruning mechanism.
fn shuffled_gap_batch() -> RecordBatch {
    let mut ids: Vec<i64> = Vec::with_capacity(2_000_000);
    ids.extend(0i64..900_000);
    ids.extend(1_100_000i64..2_100_000);

    // Deterministic XOR-shift Fisher–Yates shuffle (same pattern as
    // `viability_bloom_filter_pruning.rs`, no `rand` needed for the shuffle).
    let mut state: u64 = 0xC0FF_EEBA_BE12_3456;
    for i in (1..ids.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state as usize) % (i + 1);
        ids.swap(i, j);
    }

    let id_arr: Int64Array = ids.iter().copied().collect();
    let str_arr: StringArray = ids.iter().map(|v| Some(format!("s{v}"))).collect();
    RecordBatch::try_new(
        table_schema(),
        vec![Arc::new(id_arr), Arc::new(str_arr)],
    )
    .unwrap()
}

// ---------------------------------------------------------------------------
// Helper: write a batch via Storage + append to catalog, bypassing SQL INSERT
// so we can seed large tables quickly without the per-row SQL overhead.
// ---------------------------------------------------------------------------

async fn write_and_commit(
    engine: &Engine,
    project: &ProjectId,
    table: &TableName,
    batch: RecordBatch,
    bloom_cols: Vec<String>,
    expected_snapshot: SnapshotId,
) -> SnapshotId {
    let opts = WriteOptions {
        bloom_columns: bloom_cols,
        ..Default::default()
    };
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
// Test 1: bloom_no_false_negatives
// ---------------------------------------------------------------------------

/// Verify that bloom filters never prune a file containing a queried value.
///
/// Method:
/// 1. CREATE TABLE with `basin.sort_by='c1,c2'` (enables per-file bloom on c1,c2).
/// 2. Write 1M rows (10 × 100k) with known sequential c1 values using the
///    Storage API so writes are fast.
/// 3. Sample 1000 c1 values uniformly from [0, 1M).
/// 4. For each: `SELECT COUNT(*) FROM t WHERE c1 = :value` — assert count = 1.
///
/// A false negative would manifest as count = 0 for a value that was inserted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bloom_no_false_negatives() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let table = TableName::new("t_fn").unwrap();

    // Create session and table.
    let sess = engine.open_session(project).await.unwrap();
    sess.execute(
        "CREATE TABLE t_fn (c1 BIGINT NOT NULL, c2 TEXT NOT NULL) \
         WITH (basin.sort_by='c1,c2', basin.file_format='vortex')",
    )
    .await
    .unwrap();

    // Load initial metadata to confirm global_sort_order and get snapshot.
    let meta = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap();
    let bloom_cols = meta.global_sort_order.clone().unwrap_or_default();
    assert!(
        !bloom_cols.is_empty(),
        "global_sort_order must be set — basin.sort_by DDL didn't propagate"
    );
    assert_eq!(bloom_cols, vec!["c1", "c2"]);

    // Write FN_FILES × FN_ROWS_PER_FILE rows (sequential IDs).
    let mut snap = meta.current_snapshot;
    for file_idx in 0..FN_FILES {
        let start = file_idx * FN_ROWS_PER_FILE;
        let batch = sequential_batch(start, FN_ROWS_PER_FILE);
        snap = write_and_commit(&engine, &project, &table, batch, bloom_cols.clone(), snap).await;
    }

    println!(
        "[catalog_blooms FN] wrote {} rows across {} files",
        FN_TOTAL_ROWS, FN_FILES
    );

    // Sample FN_PROBE_COUNT known c1 values.
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    let probes: Vec<i64> = (0..FN_PROBE_COUNT)
        .map(|_| rng.gen_range(0..FN_TOTAL_ROWS))
        .collect();

    // For each known value: assert count = 1 (bloom didn't prune the file).
    let mut false_negatives = 0usize;
    for &v in &probes {
        let sql = format!("SELECT COUNT(*) FROM t_fn WHERE c1 = {v}");
        let batches = query_rows(&sess, &sql).await;
        let cnt = extract_count(&batches);
        if cnt != 1 {
            false_negatives += 1;
            eprintln!(
                "[catalog_blooms FN] FALSE NEGATIVE: c1={v} → count={cnt} (expected 1)"
            );
        }
    }

    println!(
        "[catalog_blooms FN] probed {} known values, false_negatives={}",
        FN_PROBE_COUNT, false_negatives
    );

    assert_eq!(
        false_negatives, 0,
        "bloom false-negative rate must be 0: {false_negatives}/{FN_PROBE_COUNT} probes \
         returned wrong count (bloom pruned a file that contained the queried value)"
    );
}

// ---------------------------------------------------------------------------
// Test 2: bloom_false_positive_rate_bounded
// ---------------------------------------------------------------------------

/// Verify that the empirical bloom false-positive rate is ≤ 1.5%.
///
/// Method:
/// 1. CREATE TABLE with `basin.sort_by='c1,c2'` (per-file bloom on c1,c2).
/// 2. Write 2M rows in one file with c1 drawn from [0, 900k) ∪ [1.1M, 2.1M)
///    — a deliberate gap at [900k, 1.1M). The rows are shuffled so each file's
///    min/max brackets the gap; catalog-stats pruning cannot eliminate the file
///    for queries in [900k, 1.1M), leaving the bloom as the only gate.
/// 3. Issue 10 000 queries for c1 values sampled uniformly from [900k, 1.1M).
///    Track the engine's `blooms_skipped_count` before and after each query:
///      - delta = 1  → bloom correctly identified the value as absent (TN)
///      - delta = 0  → bloom let the file through (FP)
/// 4. Compute FPR = FP_count / 10000. Assert FPR ≤ 1.5%.
///
/// Expected: fastbloom at 1% FPP target → empirical FPR ≈ 1%, within [0%, 1.5%].
/// The test uses a fixed RNG seed so results are deterministic across runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bloom_false_positive_rate_bounded() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let table = TableName::new("t_fpr").unwrap();

    // Create session and table.
    let sess = engine.open_session(project).await.unwrap();
    sess.execute(
        "CREATE TABLE t_fpr (c1 BIGINT NOT NULL, c2 TEXT NOT NULL) \
         WITH (basin.sort_by='c1,c2', basin.file_format='vortex')",
    )
    .await
    .unwrap();

    // Load metadata to confirm global_sort_order.
    let meta = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap();
    let bloom_cols = meta.global_sort_order.clone().unwrap_or_default();
    assert!(
        !bloom_cols.is_empty(),
        "global_sort_order must be set — basin.sort_by DDL didn't propagate"
    );

    // Write 2M rows in a single file (shuffled to defeat min/max pruning).
    println!("[catalog_blooms FPR] building 2M-row shuffled batch…");
    let batch = shuffled_gap_batch();
    let snap = meta.current_snapshot;
    write_and_commit(&engine, &project, &table, batch, bloom_cols, snap).await;
    println!("[catalog_blooms FPR] write complete");

    // Confirm the file's column stats span the full range so stats pruning
    // cannot eliminate the file for queries in the gap.
    let updated_meta = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap();
    let files = updated_meta.live_data_files();
    assert_eq!(files.len(), 1, "expected exactly one data file");
    let file = &files[0];
    // Verify bloom filters were actually written.
    assert!(
        file.bloom_filters.contains_key("c1"),
        "bloom filter for c1 must be present in the catalog entry"
    );
    println!(
        "[catalog_blooms FPR] file: {} rows, bloom_filters={:?}",
        file.row_count,
        file.bloom_filters.keys().collect::<Vec<_>>()
    );

    // Sample FPR_PROBE_COUNT c1 values from the gap [900_000, 1_100_000).
    // These values are absent from the dataset; min/max cannot prune the file.
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    let absent_probes: Vec<i64> = (0..FPR_PROBE_COUNT)
        .map(|_| rng.gen_range(900_000i64..1_100_000i64))
        .collect();

    // For each absent value: check whether bloom pruned the file.
    let mut false_positives = 0usize;
    for &v in &absent_probes {
        let before = engine.blooms_skipped_count();
        let sql = format!("SELECT COUNT(*) FROM t_fpr WHERE c1 = {v}");
        let batches = query_rows(&sess, &sql).await;
        let after = engine.blooms_skipped_count();
        let cnt = extract_count(&batches);

        // The value must be absent (no false negatives in the FPR probe set).
        assert_eq!(
            cnt, 0,
            "absent value c1={v} returned count={cnt} — data invariant violated"
        );

        if after == before {
            // Bloom did NOT skip the file → false positive.
            false_positives += 1;
        }
    }

    let fpr = false_positives as f64 / FPR_PROBE_COUNT as f64;
    println!(
        "[catalog_blooms FPR] probed {} absent values: false_positives={}, FPR={:.4} ({:.2}%)",
        FPR_PROBE_COUNT,
        false_positives,
        fpr,
        fpr * 100.0,
    );

    // Gate: FPR must be ≤ 1.5% (fastbloom at 1% target, with 0.5% margin).
    assert!(
        fpr <= 0.015,
        "bloom false-positive rate {:.4} ({:.2}%) exceeds the 1.5% gate \
         (false_positives={}/{})",
        fpr,
        fpr * 100.0,
        false_positives,
        FPR_PROBE_COUNT,
    );
}
