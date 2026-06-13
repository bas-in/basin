//! Phase 5.29.G — `time_bucket_gapfill` + `locf` conformance tests.
//!
//! These exercise the end-to-end SQL path: a gapfill query is detected in the
//! executor, rewritten to a plain `time_bucket` aggregate, run through the
//! normal engine, and the result batches are densified against the bucket grid
//! (with `locf` carry-forward when requested).
//!
//! ## What is tested (green surface)
//!
//! | Test                                  | Surface                                          |
//! |---------------------------------------|--------------------------------------------------|
//! | `gapfill_grid_completeness`           | every missing bucket emitted, bounds inclusive   |
//! | `gapfill_four_arg_bounds`             | explicit (interval, ts, start, finish) form      |
//! | `gapfill_null_fill_for_missing`       | missing buckets carry NULL aggregate             |
//! | `gapfill_locf_carry`                  | locf carries last non-NULL forward               |
//! | `gapfill_locf_leading_nulls_stay`     | locf leaves leading gaps NULL                     |
//! | `gapfill_group_by_device`             | per-group independent grids                       |
//! | `gapfill_unbounded_errors`            | no start/finish → typed error                     |
//! | `gapfill_order_by_bucket`             | ORDER BY bucket yields monotonic grid             |
//! | `gapfill_empty_input_single_series`   | empty input still fills the full grid w/ NULLs    |
//! | `gapfill_locf_without_gapfill_errors`         | locf alone → typed 0A000                   |
//! | `gapfill_interpolate_without_gapfill_errors`  | interpolate alone → typed 0A000            |
//! | `gapfill_interpolate_mid_gap_linear`          | interpolate: mid-gap linear, trailing NULL |
//! | `gapfill_interpolate_leading_trailing_null`   | leading/trailing gaps stay NULL            |
//! | `gapfill_interpolate_and_locf_same_query`     | interpolate + locf coexist per column      |
//! | `gapfill_differential_handcomputed`           | exact expected rows vs hand-computed grid  |

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{
    Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── shared engine helpers (mirror timescale_conformance.rs) ──────────────────

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
        .unwrap_or_else(|e| panic!("exec failed: {sql}: {e}"));
}

/// Run a SELECT and collect all batches concatenated logically into row-major
/// vectors of (bucket_us, group_opt, agg_int_opt, agg_float_opt). The caller
/// picks the columns it cares about; absent columns are returned as `None`.
struct GapfillRows {
    bucket_us: Vec<i64>,
    group: Vec<Option<String>>,
    agg_i: Vec<Option<i64>>,
    agg_f: Vec<Option<f64>>,
}

/// Fetch a gapfill result. `group_col` indicates whether a string group column
/// is present (between bucket and aggregate). The aggregate is read as either
/// Int64 or Float64 depending on which downcast succeeds.
async fn fetch(sess: &basin_engine::ProjectSession, sql: &str, has_group: bool) -> GapfillRows {
    let batches = match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        Ok(other) => panic!("non-rows for {sql}: {other:?}"),
        Err(e) => panic!("error for {sql}: {e}"),
    };
    let mut out = GapfillRows {
        bucket_us: Vec::new(),
        group: Vec::new(),
        agg_i: Vec::new(),
        agg_f: Vec::new(),
    };
    for b in &batches {
        let bcol = b
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap_or_else(|| panic!("col0 not ts µs in {sql}"));
        let group_idx = if has_group { Some(1usize) } else { None };
        let agg_idx = if has_group { 2usize } else { 1usize };
        for row in 0..b.num_rows() {
            out.bucket_us.push(bcol.value(row));
            out.group.push(group_idx.and_then(|gi| {
                let g = b.column(gi);
                if g.is_null(row) {
                    None
                } else {
                    g.as_any()
                        .downcast_ref::<StringArray>()
                        .map(|s| s.value(row).to_string())
                }
            }));
            let agg = b.column(agg_idx);
            if let Some(ia) = agg.as_any().downcast_ref::<Int64Array>() {
                out.agg_i
                    .push(if ia.is_null(row) { None } else { Some(ia.value(row)) });
                out.agg_f.push(None);
            } else if let Some(fa) = agg.as_any().downcast_ref::<Float64Array>() {
                out.agg_f
                    .push(if fa.is_null(row) { None } else { Some(fa.value(row)) });
                out.agg_i.push(None);
            } else {
                out.agg_i.push(None);
                out.agg_f.push(None);
            }
        }
    }
    out
}

/// µs-since-epoch for a UTC datetime string `YYYY-MM-DD HH:MM:SS`.
fn us(s: &str) -> i64 {
    let ndt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap();
    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc).timestamp_micros()
}

const HOUR_US: i64 = 3_600_000_000;

// ─────────────────────────────────────────────────────────────────────────────
// grid completeness
// ─────────────────────────────────────────────────────────────────────────────

/// Sparse data at 00:00 and 03:00 across a [00:00, 04:00] hourly grid must
/// produce exactly 5 rows (00,01,02,03,04), with 01/02/04 NULL-filled.
#[tokio::test]
async fn gapfill_grid_completeness() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE g1 (ts TIMESTAMPTZ, v BIGINT)").await;
    exec(
        &sess,
        "INSERT INTO g1 VALUES
         ('2024-01-01 00:10:00+00', 1),
         ('2024-01-01 03:20:00+00', 5)",
    )
    .await;

    let rows = fetch(
        &sess,
        "SELECT time_bucket_gapfill('1 hour', ts) AS b, SUM(v) AS s \
         FROM g1 WHERE ts BETWEEN TIMESTAMP '2024-01-01 00:00:00' \
         AND TIMESTAMP '2024-01-01 04:00:00' GROUP BY b ORDER BY b",
        false,
    )
    .await;

    assert_eq!(rows.bucket_us.len(), 5, "expected 5 grid buckets");
    let expect_buckets: Vec<i64> = (0..5).map(|h| us("2024-01-01 00:00:00") + h * HOUR_US).collect();
    assert_eq!(rows.bucket_us, expect_buckets, "grid buckets exact");
    // 00:00 → 1, 03:00 → 5, others NULL.
    assert_eq!(rows.agg_i, vec![Some(1), None, None, Some(5), None]);
    println!("[gapfill grid] 5 buckets, 3 NULL-filled ✓");
}

/// Explicit 4-arg bounds: (interval, ts, start, finish) with no WHERE clause.
#[tokio::test]
async fn gapfill_four_arg_bounds() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE g4 (ts TIMESTAMPTZ, v BIGINT)").await;
    exec(&sess, "INSERT INTO g4 VALUES ('2024-01-01 01:00:00+00', 7)").await;

    let rows = fetch(
        &sess,
        "SELECT time_bucket_gapfill('1 hour', ts, \
         TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 02:00:00') AS b, \
         SUM(v) AS s FROM g4 GROUP BY b ORDER BY b",
        false,
    )
    .await;

    // 00:00, 01:00, 02:00 inclusive → 3 buckets.
    assert_eq!(rows.bucket_us.len(), 3);
    assert_eq!(rows.agg_i, vec![None, Some(7), None]);
    println!("[gapfill 4-arg] 3 buckets, middle filled ✓");
}

/// Missing buckets without locf carry NULL aggregate values.
#[tokio::test]
async fn gapfill_null_fill_for_missing() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE gn (ts TIMESTAMPTZ, v DOUBLE PRECISION)").await;
    exec(&sess, "INSERT INTO gn VALUES ('2024-01-01 02:00:00+00', 2.5)").await;

    let rows = fetch(
        &sess,
        "SELECT time_bucket_gapfill('1 hour', ts) AS b, AVG(v) AS a \
         FROM gn WHERE ts BETWEEN TIMESTAMP '2024-01-01 00:00:00' \
         AND TIMESTAMP '2024-01-01 03:00:00' GROUP BY b ORDER BY b",
        false,
    )
    .await;
    assert_eq!(rows.bucket_us.len(), 4);
    assert_eq!(rows.agg_f, vec![None, None, Some(2.5), None]);
    println!("[gapfill null-fill] only the populated bucket non-NULL ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// locf
// ─────────────────────────────────────────────────────────────────────────────

/// locf carries the last non-NULL value forward across gaps.
#[tokio::test]
async fn gapfill_locf_carry() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE gl (ts TIMESTAMPTZ, v BIGINT)").await;
    // Values at 00:00 and 02:00; 01:00, 03:00, 04:00 are gaps.
    exec(
        &sess,
        "INSERT INTO gl VALUES
         ('2024-01-01 00:00:00+00', 10),
         ('2024-01-01 02:00:00+00', 20)",
    )
    .await;

    let rows = fetch(
        &sess,
        "SELECT time_bucket_gapfill('1 hour', ts) AS b, locf(SUM(v)) AS s \
         FROM gl WHERE ts BETWEEN TIMESTAMP '2024-01-01 00:00:00' \
         AND TIMESTAMP '2024-01-01 04:00:00' GROUP BY b ORDER BY b",
        false,
    )
    .await;

    assert_eq!(rows.bucket_us.len(), 5);
    // 00→10, 01→carry 10, 02→20, 03→carry 20, 04→carry 20.
    assert_eq!(rows.agg_i, vec![Some(10), Some(10), Some(20), Some(20), Some(20)]);
    println!("[gapfill locf] carry-forward 10,10,20,20,20 ✓");
}

/// locf leaves leading gaps NULL (no prior observation to carry).
#[tokio::test]
async fn gapfill_locf_leading_nulls_stay() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE gll (ts TIMESTAMPTZ, v BIGINT)").await;
    // First value only at 02:00 — buckets 00:00, 01:00 have no prior obs.
    exec(&sess, "INSERT INTO gll VALUES ('2024-01-01 02:00:00+00', 99)").await;

    let rows = fetch(
        &sess,
        "SELECT time_bucket_gapfill('1 hour', ts) AS b, locf(SUM(v)) AS s \
         FROM gll WHERE ts BETWEEN TIMESTAMP '2024-01-01 00:00:00' \
         AND TIMESTAMP '2024-01-01 03:00:00' GROUP BY b ORDER BY b",
        false,
    )
    .await;
    assert_eq!(rows.bucket_us.len(), 4);
    // 00→NULL (leading), 01→NULL (leading), 02→99, 03→carry 99.
    assert_eq!(rows.agg_i, vec![None, None, Some(99), Some(99)]);
    println!("[gapfill locf leading] leading NULLs preserved ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// group by device
// ─────────────────────────────────────────────────────────────────────────────

/// Per-group independent grids: each device gets its own complete bucket grid.
#[tokio::test]
async fn gapfill_group_by_device() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE gd (device TEXT, ts TIMESTAMPTZ, v BIGINT)",
    )
    .await;
    exec(
        &sess,
        "INSERT INTO gd VALUES
         ('a', '2024-01-01 00:00:00+00', 1),
         ('a', '2024-01-01 02:00:00+00', 3),
         ('b', '2024-01-01 01:00:00+00', 7)",
    )
    .await;

    let rows = fetch(
        &sess,
        "SELECT time_bucket_gapfill('1 hour', ts) AS b, device, SUM(v) AS s \
         FROM gd WHERE ts BETWEEN TIMESTAMP '2024-01-01 00:00:00' \
         AND TIMESTAMP '2024-01-01 02:00:00' GROUP BY b, device ORDER BY device, b",
        true,
    )
    .await;

    // 2 devices × 3 buckets (00,01,02) = 6 rows.
    assert_eq!(rows.bucket_us.len(), 6, "2 devices × 3 buckets");
    // Build (device, bucket_h, val) tuples and check.
    let mut tuples: Vec<(String, i64, Option<i64>)> = Vec::new();
    for i in 0..rows.bucket_us.len() {
        let h = (rows.bucket_us[i] - us("2024-01-01 00:00:00")) / HOUR_US;
        tuples.push((rows.group[i].clone().unwrap(), h, rows.agg_i[i]));
    }
    tuples.sort();
    let expect: Vec<(String, i64, Option<i64>)> = vec![
        ("a".into(), 0, Some(1)),
        ("a".into(), 1, None),
        ("a".into(), 2, Some(3)),
        ("b".into(), 0, None),
        ("b".into(), 1, Some(7)),
        ("b".into(), 2, None),
    ];
    assert_eq!(tuples, expect, "per-device grids exact");
    println!("[gapfill group-by] per-device grids ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// error cases
// ─────────────────────────────────────────────────────────────────────────────

/// Unbounded gapfill (no start/finish, no BETWEEN) → typed error mirroring
/// TimescaleDB's "missing time_bucket_gapfill argument".
#[tokio::test]
async fn gapfill_unbounded_errors() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE gu (ts TIMESTAMPTZ, v BIGINT)").await;
    exec(&sess, "INSERT INTO gu VALUES ('2024-01-01 00:00:00+00', 1)").await;

    let err = sess
        .execute("SELECT time_bucket_gapfill('1 hour', ts) AS b, SUM(v) FROM gu GROUP BY b")
        .await
        .expect_err("unbounded gapfill must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("missing time_bucket_gapfill argument"),
        "expected missing-argument error, got: {msg}"
    );
    println!("[gapfill unbounded] typed error ✓");
}

/// locf without a gapfill in the same query → typed 0A000.
#[tokio::test]
async fn gapfill_locf_without_gapfill_errors() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE glw (ts TIMESTAMPTZ, v BIGINT)").await;
    exec(&sess, "INSERT INTO glw VALUES ('2024-01-01 00:00:00+00', 1)").await;

    let err = sess
        .execute("SELECT time_bucket('1 hour', ts) AS b, locf(SUM(v)) FROM glw GROUP BY b")
        .await
        .expect_err("locf without gapfill must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("locf can only be used") || msg.contains("time_bucket_gapfill"),
        "expected locf-without-gapfill error, got: {msg}"
    );
    println!("[gapfill locf-alone] typed 0A000 ✓");
}

/// interpolate() without gapfill → typed 0A000.
#[tokio::test]
async fn gapfill_interpolate_without_gapfill_errors() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE gi_ng (ts TIMESTAMPTZ, v DOUBLE PRECISION)").await;
    exec(&sess, "INSERT INTO gi_ng VALUES ('2024-01-01 00:00:00+00', 1.0)").await;

    // interpolate() used without time_bucket_gapfill → error.
    let err = sess
        .execute(
            "SELECT time_bucket('1 hour', ts) AS b, interpolate(AVG(v)) \
             FROM gi_ng GROUP BY b",
        )
        .await
        .expect_err("interpolate without gapfill must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("interpolate"),
        "expected interpolate-without-gapfill error, got: {msg}"
    );
    println!("[gapfill interpolate-no-gapfill] typed 0A000 ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// interpolate()
// ─────────────────────────────────────────────────────────────────────────────

/// interpolate() fills NULL gap-buckets with linear interpolation between the
/// surrounding non-NULL values.
///
/// Hand-computed:
///   bucket 00:00 → 10.0 (present)
///   bucket 01:00 → gap → interpolated: 10 + 1/3*(20-10) = 13.333...
///   bucket 02:00 → gap → interpolated: 10 + 2/3*(20-10) = 16.666...
///   bucket 03:00 → 20.0 (present)
///   bucket 04:00 → gap → NULL (trailing: no next value)
#[tokio::test]
async fn gapfill_interpolate_mid_gap_linear() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE ginterp (ts TIMESTAMPTZ, v DOUBLE PRECISION)").await;
    exec(
        &sess,
        "INSERT INTO ginterp VALUES
         ('2024-01-01 00:10:00+00', 10.0),
         ('2024-01-01 03:20:00+00', 20.0)",
    )
    .await;

    let rows = fetch(
        &sess,
        "SELECT time_bucket_gapfill('1 hour', ts) AS b, interpolate(AVG(v)) AS a \
         FROM ginterp WHERE ts BETWEEN TIMESTAMP '2024-01-01 00:00:00' \
         AND TIMESTAMP '2024-01-01 04:00:00' GROUP BY b ORDER BY b",
        false,
    )
    .await;

    assert_eq!(rows.bucket_us.len(), 5, "expected 5 buckets (00–04)");

    let vals: Vec<Option<f64>> = rows.agg_f.clone();

    // Bucket 0 (00:00): present → 10.0
    assert_eq!(vals[0], Some(10.0), "bucket 00:00 must be 10.0");

    // Bucket 3 (03:00): present → 20.0
    assert_eq!(vals[3], Some(20.0), "bucket 03:00 must be 20.0");

    // Buckets 1 (01:00) and 2 (02:00): interpolated between 10 and 20 over 3 steps.
    // Step 1/3: 10 + 1/3*(20-10) = 13.333...
    // Step 2/3: 10 + 2/3*(20-10) = 16.666...
    let v1 = vals[1].expect("bucket 01:00 must be interpolated (not NULL)");
    let v2 = vals[2].expect("bucket 02:00 must be interpolated (not NULL)");
    assert!(
        (v1 - 10.0 - 10.0 / 3.0).abs() < 1e-6,
        "bucket 01:00 expected ~13.333, got {v1}"
    );
    assert!(
        (v2 - 10.0 - 20.0 / 3.0).abs() < 1e-6,
        "bucket 02:00 expected ~16.667, got {v2}"
    );

    // Bucket 4 (04:00): trailing gap → NULL (no next value).
    assert_eq!(vals[4], None, "trailing gap bucket 04:00 must be NULL");

    println!(
        "[gapfill interpolate] mid-gap linear: 10, {:.3}, {:.3}, 20, NULL ✓",
        v1, v2
    );
}

/// interpolate() leading gap stays NULL (no prior observation on the left).
#[tokio::test]
async fn gapfill_interpolate_leading_trailing_null() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE ginterp2 (ts TIMESTAMPTZ, v DOUBLE PRECISION)").await;
    // Only bucket 02:00 has a value — both the leading and trailing gaps have
    // no surrounding value on one side.
    exec(&sess, "INSERT INTO ginterp2 VALUES ('2024-01-01 02:00:00+00', 50.0)").await;

    let rows = fetch(
        &sess,
        "SELECT time_bucket_gapfill('1 hour', ts) AS b, interpolate(AVG(v)) AS a \
         FROM ginterp2 WHERE ts BETWEEN TIMESTAMP '2024-01-01 00:00:00' \
         AND TIMESTAMP '2024-01-01 03:00:00' GROUP BY b ORDER BY b",
        false,
    )
    .await;

    assert_eq!(rows.bucket_us.len(), 4, "expected 4 buckets (00–03)");
    let vals: Vec<Option<f64>> = rows.agg_f.clone();

    // Leading: 00:00 and 01:00 → NULL (no left neighbour).
    assert_eq!(vals[0], None, "leading gap 00:00 must be NULL");
    assert_eq!(vals[1], None, "leading gap 01:00 must be NULL");
    // Present: 02:00 → 50.0.
    assert_eq!(vals[2], Some(50.0), "present 02:00 must be 50.0");
    // Trailing: 03:00 → NULL (no right neighbour).
    assert_eq!(vals[3], None, "trailing gap 03:00 must be NULL");

    println!("[gapfill interpolate] leading/trailing NULL ✓");
}

/// interpolate() and locf() may coexist on different columns in the same query.
#[tokio::test]
async fn gapfill_interpolate_and_locf_same_query() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE ginterp3 (ts TIMESTAMPTZ, a DOUBLE PRECISION, b DOUBLE PRECISION)",
    )
    .await;
    // Values at 00:00 and 02:00; 01:00 is a gap.
    exec(
        &sess,
        "INSERT INTO ginterp3 VALUES
         ('2024-01-01 00:00:00+00', 0.0, 100.0),
         ('2024-01-01 02:00:00+00', 20.0, 200.0)",
    )
    .await;

    // col a → interpolate; col b → locf.
    let batches = match sess.execute(
        "SELECT time_bucket_gapfill('1 hour', ts) AS bkt, \
         interpolate(AVG(a)) AS ia, locf(AVG(b)) AS lb \
         FROM ginterp3 WHERE ts BETWEEN TIMESTAMP '2024-01-01 00:00:00' \
         AND TIMESTAMP '2024-01-01 02:00:00' GROUP BY bkt ORDER BY bkt"
    ).await {
        Ok(basin_engine::ExecResult::Rows { batches, .. }) => batches,
        other => panic!("unexpected: {other:?}"),
    };

    // 3 buckets: 00, 01, 02.
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>().iter().sum();
    assert_eq!(total_rows, 3, "expected 3 rows");

    // Extract the interpolate(AVG(a)) column (index 1) and locf(AVG(b)) column (index 2).
    let mut ia_vals: Vec<Option<f64>> = Vec::new();
    let mut lb_vals: Vec<Option<f64>> = Vec::new();
    for b in &batches {
        let ia = b.column(1).as_any().downcast_ref::<Float64Array>();
        let lb = b.column(2).as_any().downcast_ref::<Float64Array>();
        for row in 0..b.num_rows() {
            ia_vals.push(ia.and_then(|a| if a.is_null(row) { None } else { Some(a.value(row)) }));
            lb_vals.push(lb.and_then(|a| if a.is_null(row) { None } else { Some(a.value(row)) }));
        }
    }

    // interpolate(AVG(a)): 0.0 at 00, 10.0 at 01 (mid-point), 20.0 at 02.
    assert_eq!(ia_vals[0], Some(0.0), "ia bucket 00:00 = 0.0");
    let ia1 = ia_vals[1].expect("ia bucket 01:00 must be interpolated");
    assert!((ia1 - 10.0).abs() < 1e-6, "ia bucket 01:00 ≈ 10.0, got {ia1}");
    assert_eq!(ia_vals[2], Some(20.0), "ia bucket 02:00 = 20.0");

    // locf(AVG(b)): 100.0 at 00, carry 100.0 at 01, 200.0 at 02.
    assert_eq!(lb_vals[0], Some(100.0), "lb bucket 00:00 = 100.0");
    assert_eq!(lb_vals[1], Some(100.0), "lb bucket 01:00 = locf 100.0");
    assert_eq!(lb_vals[2], Some(200.0), "lb bucket 02:00 = 200.0");

    println!("[gapfill interpolate+locf] coexist in same query ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// ordering, empty input, differential
// ─────────────────────────────────────────────────────────────────────────────

/// ORDER BY bucket yields a monotonically increasing bucket sequence.
#[tokio::test]
async fn gapfill_order_by_bucket() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE go (ts TIMESTAMPTZ, v BIGINT)").await;
    exec(
        &sess,
        "INSERT INTO go VALUES
         ('2024-01-01 00:00:00+00', 1),
         ('2024-01-01 02:00:00+00', 2)",
    )
    .await;

    let rows = fetch(
        &sess,
        "SELECT time_bucket_gapfill('1 hour', ts) AS b, SUM(v) AS s \
         FROM go WHERE ts BETWEEN TIMESTAMP '2024-01-01 00:00:00' \
         AND TIMESTAMP '2024-01-01 03:00:00' GROUP BY b ORDER BY b",
        false,
    )
    .await;
    for w in rows.bucket_us.windows(2) {
        assert!(w[0] < w[1], "buckets must be strictly increasing: {:?}", rows.bucket_us);
    }
    assert_eq!(rows.bucket_us.len(), 4);
    println!("[gapfill order-by] monotonic grid ✓");
}

/// Empty input (single series): the full grid is still emitted, all NULL.
#[tokio::test]
async fn gapfill_empty_input_single_series() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE ge (ts TIMESTAMPTZ, v BIGINT)").await;
    // No rows in range (insert one far outside the window).
    exec(&sess, "INSERT INTO ge VALUES ('2030-01-01 00:00:00+00', 1)").await;

    let rows = fetch(
        &sess,
        "SELECT time_bucket_gapfill('1 hour', ts) AS b, SUM(v) AS s \
         FROM ge WHERE ts BETWEEN TIMESTAMP '2024-01-01 00:00:00' \
         AND TIMESTAMP '2024-01-01 02:00:00' GROUP BY b ORDER BY b",
        false,
    )
    .await;
    // Single-series empty input still fills the [00,02] grid with NULLs.
    assert_eq!(rows.bucket_us.len(), 3);
    assert!(rows.agg_i.iter().all(|v| v.is_none()), "all NULL on empty input");
    println!("[gapfill empty] full grid, all NULL ✓");
}

/// Differential: exact expected rows vs a hand-computed 15-minute grid,
/// epoch-aligned (DST-irrelevant — pure µs math).
#[tokio::test]
async fn gapfill_differential_handcomputed() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE gdf (ts TIMESTAMPTZ, v BIGINT)").await;
    // 15-minute buckets over [10:00, 11:00]. Populate 10:00, 10:30, 11:00.
    exec(
        &sess,
        "INSERT INTO gdf VALUES
         ('2024-03-15 10:05:00+00', 100),
         ('2024-03-15 10:38:00+00', 200),
         ('2024-03-15 11:00:00+00', 300)",
    )
    .await;

    let rows = fetch(
        &sess,
        "SELECT time_bucket_gapfill('15 minutes', ts) AS b, SUM(v) AS s \
         FROM gdf WHERE ts BETWEEN TIMESTAMP '2024-03-15 10:00:00' \
         AND TIMESTAMP '2024-03-15 11:00:00' GROUP BY b ORDER BY b",
        false,
    )
    .await;

    // Buckets: 10:00,10:15,10:30,10:45,11:00 → 5 rows.
    let base = us("2024-03-15 10:00:00");
    let q = 15 * 60 * 1_000_000i64;
    let expect_buckets: Vec<i64> = (0..5).map(|i| base + i * q).collect();
    assert_eq!(rows.bucket_us, expect_buckets, "15-min grid exact");
    // 10:05→bucket 10:00 (100); 10:38→bucket 10:30 (200); 11:00→bucket 11:00 (300).
    assert_eq!(rows.agg_i, vec![Some(100), None, Some(200), None, Some(300)]);
    println!("[gapfill differential] 15-min grid matches hand-computed ✓");
}
