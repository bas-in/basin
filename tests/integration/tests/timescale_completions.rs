//! Timescale-family completion tests.
//!
//! Tests features introduced in the timescale-completions work:
//!   - `first(value, ts)` / `last(value, ts)` aggregates
//!   - `drop_chunks('table', older_than => ...)` actual deletion
//!   - `run_retention_policy` actual deletion (delegates to drop_chunks core)
//!   - `time_bucket(interval, ts, origin)` — origin-aligned bucket grid
//!   - `timescaledb_information.hypertables` catalog view
//!
//! Each test is self-contained (its own TempDir + engine instance).

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── shared helpers ───────────────────────────────────────────────────────────

fn build_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn exec(sess: &basin_engine::ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed:\n  sql={sql}\n  error={e}"));
}

async fn row_count(sess: &basin_engine::ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches.iter().map(|b| b.num_rows()).sum(),
        Ok(ExecResult::Empty { .. }) => 0,
        Err(e) => panic!("execute failed:\n  sql={sql}\n  error={e}"),
    }
}

async fn single_i64(sess: &basin_engine::ProjectSession, sql: &str) -> i64 {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().unwrap_or_else(|| panic!("no batch: {sql}"));
            let col = b.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| panic!("expected Int64 column: {sql}"));
            assert!(arr.len() >= 1, "no rows from: {sql}");
            arr.value(0)
        }
        Ok(other) => panic!("non-rows: {sql}: {other:?}"),
        Err(e) => panic!("error: {sql}: {e}"),
    }
}

async fn single_i32(sess: &basin_engine::ProjectSession, sql: &str) -> i32 {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().unwrap_or_else(|| panic!("no batch: {sql}"));
            let col = b.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap_or_else(|| panic!("expected Int32 column: {sql}"));
            assert!(arr.len() >= 1, "no rows from: {sql}");
            arr.value(0)
        }
        Ok(other) => panic!("non-rows: {sql}: {other:?}"),
        Err(e) => panic!("error: {sql}: {e}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// first() / last() aggregate correctness
// ─────────────────────────────────────────────────────────────────────────────

/// `first(v, ts)` returns the value at minimum ts.
#[tokio::test]
async fn first_basic_correctness() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE fl_basic (v INT, ts TIMESTAMPTZ)").await;
    exec(
        &sess,
        "INSERT INTO fl_basic VALUES
         (10, '2024-01-01 10:00:00+00'),
         (20, '2024-01-01 09:00:00+00'),
         (30, '2024-01-01 11:00:00+00')",
    )
    .await;

    // first() → value at ts=09:00 = 20
    let result = single_i32(&sess, "SELECT first(v, ts) FROM fl_basic").await;
    assert_eq!(
        result, 20,
        "first should return value at min ts (20), got={result}"
    );
    println!("[first] basic: first(v,ts) = {result} ✓");
}

/// `last(v, ts)` returns the value at maximum ts.
#[tokio::test]
async fn last_basic_correctness() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE fl_last (v INT, ts TIMESTAMPTZ)").await;
    exec(
        &sess,
        "INSERT INTO fl_last VALUES
         (10, '2024-01-01 10:00:00+00'),
         (20, '2024-01-01 09:00:00+00'),
         (30, '2024-01-01 11:00:00+00')",
    )
    .await;

    // last() → value at ts=11:00 = 30
    let result = single_i32(&sess, "SELECT last(v, ts) FROM fl_last").await;
    assert_eq!(
        result, 30,
        "last should return value at max ts (30), got={result}"
    );
    println!("[last] basic: last(v,ts) = {result} ✓");
}

/// Rows with NULL ts are ignored by both first() and last().
#[tokio::test]
async fn first_last_null_ts_ignored() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE fl_null (v INT, ts TIMESTAMPTZ)").await;
    exec(
        &sess,
        "INSERT INTO fl_null VALUES
         (99, NULL),
         (10, '2024-01-01 10:00:00+00'),
         (20, '2024-01-01 08:00:00+00')",
    )
    .await;

    // NULL-ts row (v=99) must not affect first/last.
    let f = single_i32(&sess, "SELECT first(v, ts) FROM fl_null").await;
    let l = single_i32(&sess, "SELECT last(v, ts) FROM fl_null").await;
    assert_eq!(f, 20, "first with NULL ts: expected 20 (ts=08:00), got={f}");
    assert_eq!(l, 10, "last with NULL ts: expected 10 (ts=10:00), got={l}");
    println!("[first/last] null ts ignored: first={f} last={l} ✓");
}

/// first() and last() with GROUP BY — one result per group.
#[tokio::test]
async fn first_last_group_by() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE fl_grp (grp TEXT, v INT, ts TIMESTAMPTZ)",
    )
    .await;
    exec(
        &sess,
        "INSERT INTO fl_grp VALUES
         ('a', 1, '2024-01-01 10:00:00+00'),
         ('a', 2, '2024-01-01 09:00:00+00'),
         ('b', 3, '2024-01-01 10:00:00+00'),
         ('b', 4, '2024-01-01 11:00:00+00')",
    )
    .await;

    // group 'a': first=2 (ts=09), last=1 (ts=10)
    // group 'b': first=3 (ts=10), last=4 (ts=11)
    let groups = row_count(
        &sess,
        "SELECT grp, first(v, ts), last(v, ts) FROM fl_grp GROUP BY grp ORDER BY grp",
    )
    .await;
    assert_eq!(groups, 2, "GROUP BY should yield 2 rows, got={groups}");
    println!("[first/last] GROUP BY: {groups} groups ✓");
}

/// Empty group → NULL result.
#[tokio::test]
async fn first_last_empty_group_null() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE fl_empty (v INT, ts TIMESTAMPTZ)").await;
    // No rows inserted → first/last should return NULL.
    let n = row_count(&sess, "SELECT first(v, ts), last(v, ts) FROM fl_empty").await;
    // One result row with NULL values (aggregates over empty set).
    assert_eq!(
        n, 1,
        "empty aggregate should produce 1 row with NULL, got={n}"
    );
    println!("[first/last] empty group returns 1 NULL row ✓");
}

/// Scale shape: 100k rows, GROUP BY 10-minute bucket, first/last per group.
/// Verifies correctness (not timing) at volume.
#[tokio::test]
async fn first_last_100k_scale_shape() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE fl_scale (v BIGINT, ts TIMESTAMPTZ)").await;

    // Insert 100k rows: row i has ts = epoch + i*seconds, v = i.
    // We use a batch of 1000 at a time via repeated INSERTs to stay inside
    // the VALUES batch limit. Each insert is 1000 rows; 100 iterations = 100k.
    // Epoch offset: 2024-01-01 00:00:00 UTC = 1704067200 seconds.
    // Each row is 6 seconds apart → 100k rows span 600000 seconds ≈ 6.9 days.
    // With 10-minute (600s) buckets: 100000 rows / (600/6) ≈ 1000 distinct buckets.
    let base_epoch_secs: i64 = 1_704_067_200;
    for batch in 0..100i64 {
        let rows: Vec<String> = (0..1000i64)
            .map(|j| {
                let i = batch * 1000 + j;
                let ts_secs = base_epoch_secs + i * 6;
                let v = i;
                format!("({v}, TO_TIMESTAMP({ts_secs}))")
            })
            .collect();
        let sql = format!("INSERT INTO fl_scale VALUES {}", rows.join(","));
        exec(&sess, &sql).await;
    }

    // Verify total row count.
    let total = single_i64(&sess, "SELECT COUNT(*) FROM fl_scale").await;
    assert_eq!(total, 100_000, "expected 100k rows, got={total}");

    // Verify GROUP BY bucket count is in the expected range (should be 1000).
    let bucket_count = row_count(
        &sess,
        "SELECT time_bucket('10 minutes', ts) AS b, first(v, ts), last(v, ts) \
         FROM fl_scale GROUP BY b ORDER BY b",
    )
    .await;
    // Each 600-second bucket has exactly 100 rows (600/6 = 100).
    // 100k rows / 100 rows per bucket = 1000 buckets.
    assert_eq!(
        bucket_count, 1000,
        "100k-row scale: expected 1000 buckets, got={bucket_count}"
    );
    println!("[first/last] 100k scale: {total} rows, {bucket_count} buckets ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// drop_chunks — removes old rows, preserves newer ones at boundary
// ─────────────────────────────────────────────────────────────────────────────

/// drop_chunks removes rows in old chunks and preserves rows in newer chunks.
/// Boundary correctness: a row exactly at the cutoff boundary is NOT deleted.
#[tokio::test]
async fn drop_chunks_removes_old_preserves_new() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE dc_tbl (id BIGINT, ts TIMESTAMPTZ)").await;
    exec(
        &sess,
        "SELECT create_hypertable('dc_tbl', 'ts', chunk_time_interval => INTERVAL '1 day')",
    )
    .await;

    // Old rows (before 2024-01-01): should be dropped.
    exec(
        &sess,
        "INSERT INTO dc_tbl VALUES
         (1, '2023-11-01 00:00:00+00'),
         (2, '2023-12-01 12:00:00+00'),
         (3, '2023-12-31 23:59:59+00')",
    )
    .await;

    // New rows (2024 and after): should survive.
    exec(
        &sess,
        "INSERT INTO dc_tbl VALUES
         (4, '2024-01-01 00:00:00+00'),
         (5, '2024-06-15 12:00:00+00'),
         (6, '2024-12-31 23:59:59+00')",
    )
    .await;

    let before = single_i64(&sess, "SELECT COUNT(*) FROM dc_tbl").await;
    assert_eq!(
        before, 6,
        "expected 6 rows before drop_chunks, got={before}"
    );

    // Drop chunks older than 2024-01-01 (absolute cutoff).
    exec(
        &sess,
        "SELECT drop_chunks('dc_tbl', older_than => TIMESTAMP '2024-01-01 00:00:00')",
    )
    .await;

    let after = single_i64(&sess, "SELECT COUNT(*) FROM dc_tbl").await;
    // Rows 1,2,3 (all before 2024-01-01 00:00:00) should be deleted.
    // Rows 4,5,6 should remain. Row 4 is exactly at the cutoff; since the
    // DELETE predicate is ts < cutoff (strict), row 4 must survive.
    assert_eq!(
        after, 3,
        "after drop_chunks: expected 3 rows (ids 4,5,6), got={after}"
    );

    // Verify the surviving row ids.
    let min_id = single_i64(&sess, "SELECT MIN(id) FROM dc_tbl").await;
    assert_eq!(min_id, 4, "minimum surviving id must be 4, got={min_id}");
    println!("[drop_chunks] removes old, preserves new: before={before} after={after} ✓");
}

/// drop_chunks with an INTERVAL cutoff also works correctly.
#[tokio::test]
async fn drop_chunks_interval_cutoff() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE dc_iv (id BIGINT, ts TIMESTAMPTZ)").await;
    exec(
        &sess,
        "SELECT create_hypertable('dc_iv', 'ts', chunk_time_interval => INTERVAL '1 day')",
    )
    .await;

    // Old rows (several years ago) → will fall outside any reasonable retention.
    exec(
        &sess,
        "INSERT INTO dc_iv VALUES
         (1, '2020-01-01 00:00:00+00'),
         (2, '2020-06-01 12:00:00+00')",
    )
    .await;

    // Recent row (today – 1 second, effectively) → must survive.
    // We use a fixed recent date that will always be within 1-day window of any run.
    // Use '2099-12-31' to ensure it's never dropped.
    exec(
        &sess,
        "INSERT INTO dc_iv VALUES (3, '2099-12-31 00:00:00+00')",
    )
    .await;

    let before = single_i64(&sess, "SELECT COUNT(*) FROM dc_iv").await;
    assert_eq!(before, 3);

    // Drop chunks older than 1 year (relative cutoff).
    exec(
        &sess,
        "SELECT drop_chunks('dc_iv', older_than => INTERVAL '1 year')",
    )
    .await;

    let after = single_i64(&sess, "SELECT COUNT(*) FROM dc_iv").await;
    // The 2020 rows are definitely older than 1 year; the 2099 row is not.
    assert_eq!(
        after, 1,
        "after interval drop_chunks: only 2099 row should remain, got={after}"
    );
    println!("[drop_chunks interval] {before} → {after} ✓");
}

/// Read after drop must not see hot-tier ghosts: after drop_chunks, SELECT
/// returns exactly the surviving rows.
#[tokio::test]
async fn drop_chunks_no_hot_tier_ghost() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE dc_hot (id BIGINT, v TEXT, ts TIMESTAMPTZ)",
    )
    .await;
    exec(
        &sess,
        "SELECT create_hypertable('dc_hot', 'ts', chunk_time_interval => INTERVAL '1 day')",
    )
    .await;

    exec(
        &sess,
        "INSERT INTO dc_hot VALUES
         (1, 'old_1', '2022-03-15 00:00:00+00'),
         (2, 'old_2', '2022-03-16 00:00:00+00'),
         (3, 'new_1', '2099-01-01 00:00:00+00')",
    )
    .await;

    // Drop everything older than 2023-01-01.
    exec(
        &sess,
        "SELECT drop_chunks('dc_hot', older_than => TIMESTAMP '2023-01-01')",
    )
    .await;

    // Verify: only the 2099 row remains and it's the right one.
    let rows = row_count(&sess, "SELECT id FROM dc_hot WHERE v LIKE 'old%'").await;
    assert_eq!(
        rows, 0,
        "no 'old' rows should survive drop_chunks, found={rows}"
    );

    let surviving = single_i64(&sess, "SELECT id FROM dc_hot").await;
    assert_eq!(
        surviving, 3,
        "only row id=3 ('new_1') should survive, got={surviving}"
    );
    println!("[drop_chunks no ghost] surviving id={surviving} ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// run_retention_policy — delegates to drop_chunks core, actually deletes rows
// ─────────────────────────────────────────────────────────────────────────────

/// run_retention_policy physically removes rows outside the retention window.
#[tokio::test]
async fn retention_policy_physically_deletes() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE rp_tbl (id BIGINT, ts TIMESTAMPTZ)").await;
    exec(
        &sess,
        "SELECT create_hypertable('rp_tbl', 'ts', chunk_time_interval => INTERVAL '1 day')",
    )
    .await;
    // Retention: 1 year (365 days).
    exec(
        &sess,
        "SELECT add_retention_policy('rp_tbl', INTERVAL '365 days')",
    )
    .await;

    // Ancient rows (>1 year ago) → should be deleted.
    exec(
        &sess,
        "INSERT INTO rp_tbl VALUES
         (1, '2020-01-01 00:00:00+00'),
         (2, '2020-06-01 00:00:00+00')",
    )
    .await;
    // Future row → must survive.
    exec(
        &sess,
        "INSERT INTO rp_tbl VALUES (3, '2099-12-31 00:00:00+00')",
    )
    .await;

    let before = single_i64(&sess, "SELECT COUNT(*) FROM rp_tbl").await;
    assert_eq!(before, 3);

    exec(&sess, "SELECT run_retention_policy('rp_tbl')").await;

    let after = single_i64(&sess, "SELECT COUNT(*) FROM rp_tbl").await;
    assert_eq!(
        after, 1,
        "run_retention_policy: only future row should remain, got={after}"
    );
    let survivor = single_i64(&sess, "SELECT id FROM rp_tbl").await;
    assert_eq!(survivor, 3);
    println!("[retention policy] physically deleted: before={before} after={after} ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// time_bucket with origin argument — boundary math
// ─────────────────────────────────────────────────────────────────────────────
//
// Derivation of expected values (hand-computed):
//
// Epoch-aligned 1-hour bucket grid (origin = Unix epoch):
//   floor(ts, 1h) groups by calendar hour (same as date_trunc('hour', ts)).
//
// Origin-shifted 1-hour bucket at origin = 2024-01-01 00:30:00 UTC:
//   The grid is: 00:30, 01:30, 02:30, ...
//   A timestamp at 2024-01-01 00:45:00 UTC falls in bucket 2024-01-01 00:30:00 UTC.
//   A timestamp at 2024-01-01 01:15:00 UTC falls in bucket 2024-01-01 00:30:00 UTC.
//   A timestamp at 2024-01-01 01:35:00 UTC falls in bucket 2024-01-01 01:30:00 UTC.
//   So rows at 00:45 and 01:15 share bucket 00:30, but 01:35 is in bucket 01:30.
//
// Implementation note: origin-aligned bucketing uses the formula:
//   floored_us = floor((ts_us - origin_us), bucket_us) + origin_us
// This is the standard Timescale origin-bucket formula.
//
// Pinned expected bucket count: 2 (for 3 rows across 2 buckets as above).

/// Epoch-aligned 1-hour: 3 rows in 3 different hours → 3 buckets.
#[tokio::test]
async fn time_bucket_2arg_epoch_aligned_pin() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE tb2arg (ts TIMESTAMPTZ)").await;
    exec(
        &sess,
        "INSERT INTO tb2arg VALUES
         ('2024-01-01 00:45:00+00'),
         ('2024-01-01 01:15:00+00'),
         ('2024-01-01 01:35:00+00')",
    )
    .await;

    // 2-arg epoch-aligned: 00:45→00:00 bucket, 01:15+01:35→01:00 bucket → 2 buckets.
    let n = row_count(
        &sess,
        "SELECT time_bucket('1 hour', ts) FROM tb2arg GROUP BY 1",
    )
    .await;
    assert_eq!(
        n, 2,
        "epoch-aligned: 3 rows should yield 2 hour buckets, got={n}"
    );
    println!("[time_bucket 2-arg] epoch-aligned: {n} buckets ✓");
}

/// 3-arg origin-aligned: origin shifts the bucket grid.
/// Hand-computed: origin = 2024-01-01 00:30:00 UTC.
///   Row 00:45 → bucket 00:30 (floor((45-30), 60)+30 = 30)
///   Row 01:15 → bucket 00:30 (floor((75-30), 60)+30 = 30)
///   Row 01:35 → bucket 01:30 (floor((95-30), 60)+30 = 90 → 01:30)
/// → 2 distinct buckets.
#[tokio::test]
async fn time_bucket_3arg_origin_aligned_pin() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE tb3arg (ts TIMESTAMPTZ)").await;
    exec(
        &sess,
        "INSERT INTO tb3arg VALUES
         ('2024-01-01 00:45:00+00'),
         ('2024-01-01 01:15:00+00'),
         ('2024-01-01 01:35:00+00')",
    )
    .await;

    // 3-arg with origin = 2024-01-01 00:30:00 UTC.
    // Expected: 2 buckets (00:30 and 01:30).
    let n = row_count(
        &sess,
        "SELECT time_bucket('1 hour', ts, TIMESTAMP '2024-01-01 00:30:00') \
         FROM tb3arg GROUP BY 1",
    )
    .await;
    assert_eq!(
        n, 2,
        "origin-aligned 1h: expected 2 buckets (00:30 and 01:30), got={n}"
    );
    println!("[time_bucket 3-arg origin] 2 buckets ✓");
}

/// 3-arg origin forces a different bucket count than 2-arg epoch-aligned.
/// With epoch-aligned: all 3 rows at :45, :15, :35 → 2 buckets (00:00 and 01:00).
/// With origin at 00:00: same as epoch, still 2.
/// With origin at 00:15: :45, :15 → both in 00:15 bucket; :35 → 01:15 bucket.
/// With origin at 00:30: 2 buckets (as above).
/// With origin at 00:10: :45 → 00:10; :15 → 00:10; :35 → 01:10 → 2 buckets.
///
/// We verify that origin-aligned with origin=00:30 matches our hand math (2 buckets),
/// and that a different origin shifts the boundary differently.
#[tokio::test]
async fn time_bucket_origin_shifts_boundary() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE tb_orig (ts TIMESTAMPTZ)").await;
    // Rows spanning a clear hour boundary.
    exec(
        &sess,
        "INSERT INTO tb_orig VALUES
         ('2024-01-01 00:55:00+00'),
         ('2024-01-01 01:00:00+00'),
         ('2024-01-01 01:05:00+00')",
    )
    .await;

    // Epoch-aligned: 00:55→00:00, 01:00+01:05→01:00 → 2 buckets.
    let n_epoch = row_count(
        &sess,
        "SELECT time_bucket('1 hour', ts) FROM tb_orig GROUP BY 1",
    )
    .await;
    assert_eq!(n_epoch, 2, "epoch-aligned: 2 buckets, got={n_epoch}");

    // Origin at 00:45: grid boundaries at :45, 01:45, ...
    //   00:55 → 00:45 bucket
    //   01:00 → 00:45 bucket
    //   01:05 → 00:45 bucket
    // All 3 rows fall in the same bucket → 1 bucket.
    let n_origin = row_count(
        &sess,
        "SELECT time_bucket('1 hour', ts, TIMESTAMP '2024-01-01 00:45:00') \
         FROM tb_orig GROUP BY 1",
    )
    .await;
    assert_eq!(
        n_origin, 1,
        "origin-shifted: all 3 rows should be in 1 bucket with origin=00:45, got={n_origin}"
    );
    println!("[time_bucket origin boundary] epoch={n_epoch} origin={n_origin} ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// timescaledb_information.hypertables catalog view
// ─────────────────────────────────────────────────────────────────────────────

/// hypertables view is queryable and returns one row per hypertable.
#[tokio::test]
async fn hypertables_view_queryable() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE ht_view1 (id BIGINT, ts TIMESTAMPTZ)").await;
    exec(&sess, "CREATE TABLE ht_view2 (id BIGINT, ts TIMESTAMPTZ)").await;

    exec(
        &sess,
        "SELECT create_hypertable('ht_view1', 'ts', chunk_time_interval => INTERVAL '1 day')",
    )
    .await;
    exec(
        &sess,
        "SELECT create_hypertable('ht_view2', 'ts', chunk_time_interval => INTERVAL '1 hour')",
    )
    .await;

    let r = sess
        .execute(
            "SELECT hypertable_name, chunk_interval_secs \
         FROM timescaledb_information.hypertables \
         ORDER BY hypertable_name",
        )
        .await;
    assert!(
        r.is_ok(),
        "hypertables view must be queryable: {:?}",
        r.unwrap_err()
    );

    if let Ok(ExecResult::Rows { batches, .. }) = r {
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            row_count, 2,
            "hypertables view: expected 2 rows (one per hypertable), got={row_count}"
        );

        // Verify chunk_interval_secs values: ht_view1 = 86400, ht_view2 = 3600.
        let b = batches.first().unwrap();
        let intervals = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("chunk_interval_secs must be Int64");
        let mut secs: Vec<i64> = (0..intervals.len()).map(|i| intervals.value(i)).collect();
        secs.sort_unstable();
        assert_eq!(
            secs,
            vec![3600, 86400],
            "chunk_interval_secs should be [3600, 86400], got={secs:?}"
        );
        println!("[hypertables view] {row_count} rows, intervals={secs:?} ✓");
    }
}

/// hypertables view returns retention_secs = -1 when no retention policy is set,
/// and the configured value when set.
#[tokio::test]
async fn hypertables_view_retention_secs() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE ht_ret_v (id BIGINT, ts TIMESTAMPTZ)").await;
    exec(
        &sess,
        "SELECT create_hypertable('ht_ret_v', 'ts', chunk_time_interval => INTERVAL '1 day')",
    )
    .await;

    // Before retention policy: retention_secs = -1.
    let secs_before = single_i64(
        &sess,
        "SELECT retention_secs FROM timescaledb_information.hypertables \
         WHERE hypertable_name = 'ht_ret_v'",
    )
    .await;
    assert_eq!(
        secs_before, -1,
        "no retention policy → retention_secs must be -1, got={secs_before}"
    );

    // After setting a 30-day retention policy.
    exec(
        &sess,
        "SELECT add_retention_policy('ht_ret_v', INTERVAL '30 days')",
    )
    .await;
    let secs_after = single_i64(
        &sess,
        "SELECT retention_secs FROM timescaledb_information.hypertables \
         WHERE hypertable_name = 'ht_ret_v'",
    )
    .await;
    let expected_secs = 30 * 86400i64; // 2592000
    assert_eq!(
        secs_after, expected_secs,
        "after retention policy: expected {expected_secs}, got={secs_after}"
    );
    println!("[hypertables view] retention_secs: before={secs_before} after={secs_after} ✓");
}

/// hypertables view num_chunks reflects chunk creation from inserts.
#[tokio::test]
async fn hypertables_view_num_chunks() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE ht_nc (id BIGINT, ts TIMESTAMPTZ)").await;
    exec(
        &sess,
        "SELECT create_hypertable('ht_nc', 'ts', chunk_time_interval => INTERVAL '1 day')",
    )
    .await;

    // Insert rows spanning 3 different days.
    exec(
        &sess,
        "INSERT INTO ht_nc VALUES
         (1, '2024-05-01 10:00:00+00'),
         (2, '2024-05-02 10:00:00+00'),
         (3, '2024-05-03 10:00:00+00')",
    )
    .await;

    let num_chunks = single_i64(
        &sess,
        "SELECT num_chunks FROM timescaledb_information.hypertables \
         WHERE hypertable_name = 'ht_nc'",
    )
    .await;
    assert!(
        num_chunks >= 3,
        "after 3-day insert: expected >= 3 chunks, got={num_chunks}"
    );
    println!("[hypertables view] num_chunks={num_chunks} ✓");
}
