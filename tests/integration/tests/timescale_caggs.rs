//! TimescaleDB continuous-aggregate conformance tests.
//!
//! Exercises the end-to-end SQL path for Timescale continuous aggregates,
//! which map onto Basin's `basin.continuous` materialised-view engine:
//!
//! * `CREATE MATERIALIZED VIEW ... WITH (timescaledb.continuous) AS
//!   SELECT time_bucket(...), aggs FROM hypertable GROUP BY bucket`
//! * `refresh_continuous_aggregate(cagg, start, end)` — windowed upsert
//! * `add_continuous_aggregate_policy(cagg, start_offset, end_offset,
//!   schedule_interval)` — policy registration
//! * reading the cagg returns the materialised aggregate (materialised-only;
//!   the freshness boundary is the watermark — documented in `crate::cagg`)
//! * `drop_chunks` on the source preserves already-materialised cagg rows
//!
//! ## What is tested (green surface)
//!
//! | Test                                    | Surface                                          |
//! |-----------------------------------------|--------------------------------------------------|
//! | `cagg_create_initial_materialization`   | CREATE ... WITH (timescaledb.continuous) bootstrap|
//! | `cagg_matches_direct_agg`               | materialised rows == direct time_bucket query     |
//! | `cagg_refresh_advances_on_new_rows`     | insert + refresh updates materialised buckets     |
//! | `cagg_refresh_window`                   | windowed refresh_continuous_aggregate             |
//! | `cagg_add_policy_and_run`               | add_continuous_aggregate_policy + deterministic run|
//! | `cagg_query_reads_materialized`         | SELECT from cagg serves materialised store        |
//! | `cagg_drop_chunks_preserves_rows`       | drop_chunks on source keeps cagg rows             |
//! | `cagg_various_widths`                   | cagg over time_bucket of several widths           |
//! | `cagg_differential_handcomputed`        | exact buckets vs hand-computed grid               |

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int64Array, TimestampMicrosecondArray};
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

/// Collect `(bucket_us, agg_i64)` rows from a query, sorted by bucket.
/// Assumes the first column is a Timestamp(Microsecond) bucket and the
/// second is an Int64 aggregate.
async fn bucket_rows(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<(i64, i64)> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let mut out: Vec<(i64, i64)> = Vec::new();
            for b in &batches {
                if b.num_rows() == 0 {
                    continue;
                }
                let bucket = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap_or_else(|| panic!("col0 not Timestamp(us): {sql}"));
                let agg = b
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap_or_else(|| panic!("col1 not Int64: {sql}"));
                for i in 0..b.num_rows() {
                    if bucket.is_null(i) {
                        continue;
                    }
                    out.push((
                        bucket.value(i),
                        if agg.is_null(i) { 0 } else { agg.value(i) },
                    ));
                }
            }
            out.sort_unstable();
            out
        }
        Ok(ExecResult::Empty { .. }) => Vec::new(),
        Err(e) => panic!("error: {sql}: {e}"),
    }
}

async fn row_count(sess: &basin_engine::ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches.iter().map(|b| b.num_rows()).sum(),
        Ok(ExecResult::Empty { .. }) => 0,
        Err(e) => panic!("execute failed: {sql}: {e}"),
    }
}

/// Seed a `metrics(ts timestamptz, device text, val bigint)` hypertable with
/// hourly chunk interval and a handful of rows across three hours of a single
/// day. Returns the session.
async fn seed_hypertable(engine: &Engine) -> basin_engine::ProjectSession {
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    exec(
        &sess,
        "CREATE TABLE metrics (ts TIMESTAMPTZ NOT NULL, device TEXT, val BIGINT)",
    )
    .await;
    exec(
        &sess,
        "SELECT create_hypertable('metrics', 'ts', chunk_time_interval => INTERVAL '1 hour')",
    )
    .await;
    // Hour 10: three rows summing to 6; Hour 11: two rows summing to 9;
    // Hour 12: one row of 4.
    exec(
        &sess,
        "INSERT INTO metrics (ts, device, val) VALUES \
         ('2024-01-15 10:05:00+00','a',1), \
         ('2024-01-15 10:25:00+00','a',2), \
         ('2024-01-15 10:55:00+00','b',3), \
         ('2024-01-15 11:10:00+00','a',4), \
         ('2024-01-15 11:40:00+00','b',5), \
         ('2024-01-15 12:30:00+00','a',4)",
    )
    .await;
    sess
}

/// Microseconds-since-epoch for a UTC `YYYY-MM-DD HH:00:00`.
fn hour_us(y: i32, mo: u32, d: u32, h: u32) -> i64 {
    use chrono::{TimeZone, Utc};
    Utc.with_ymd_and_hms(y, mo, d, h, 0, 0)
        .unwrap()
        .timestamp_micros()
}

const CREATE_CAGG: &str = "CREATE MATERIALIZED VIEW metrics_hourly \
     WITH (timescaledb.continuous) AS \
     SELECT time_bucket('1 hour', ts) AS bucket, sum(val) AS total \
     FROM metrics GROUP BY bucket";

// ─────────────────────────────────────────────────────────────────────────────

/// Creating a cagg over a seeded hypertable materialises the buckets up front.
#[tokio::test]
async fn cagg_create_initial_materialization() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = seed_hypertable(&engine).await;

    exec(&sess, CREATE_CAGG).await;

    let rows = bucket_rows(&sess, "SELECT bucket, total FROM metrics_hourly").await;
    let expected = vec![
        (hour_us(2024, 1, 15, 10), 6),
        (hour_us(2024, 1, 15, 11), 9),
        (hour_us(2024, 1, 15, 12), 4),
    ];
    assert_eq!(rows, expected, "initial materialisation mismatch");
}

/// The materialised rows equal the direct aggregation query over the source.
#[tokio::test]
async fn cagg_matches_direct_agg() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = seed_hypertable(&engine).await;
    exec(&sess, CREATE_CAGG).await;

    let cagg = bucket_rows(
        &sess,
        "SELECT bucket, total FROM metrics_hourly ORDER BY bucket",
    )
    .await;
    let direct = bucket_rows(
        &sess,
        "SELECT time_bucket('1 hour', ts) AS bucket, sum(val) AS total \
         FROM metrics GROUP BY bucket ORDER BY bucket",
    )
    .await;
    assert_eq!(cagg, direct, "cagg should equal a direct aggregation");
}

/// Inserting new source rows and refreshing advances the materialised buckets.
#[tokio::test]
async fn cagg_refresh_advances_on_new_rows() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = seed_hypertable(&engine).await;
    exec(&sess, CREATE_CAGG).await;

    // Add a new hour-13 row and another hour-10 row (late arrival into an
    // already-materialised bucket).
    exec(
        &sess,
        "INSERT INTO metrics (ts, device, val) VALUES \
         ('2024-01-15 13:00:00+00','a',7), \
         ('2024-01-15 10:45:00+00','b',10)",
    )
    .await;

    // Full refresh (NULL, NULL) re-materialises every bucket.
    exec(
        &sess,
        "CALL refresh_continuous_aggregate('metrics_hourly', NULL, NULL)",
    )
    .await;

    let rows = bucket_rows(&sess, "SELECT bucket, total FROM metrics_hourly").await;
    let expected = vec![
        (hour_us(2024, 1, 15, 10), 16), // 1+2+3+10
        (hour_us(2024, 1, 15, 11), 9),
        (hour_us(2024, 1, 15, 12), 4),
        (hour_us(2024, 1, 15, 13), 7),
    ];
    assert_eq!(rows, expected, "refresh should re-aggregate all buckets");
}

/// A windowed refresh only re-materialises buckets inside `[start, end)`.
#[tokio::test]
async fn cagg_refresh_window() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = seed_hypertable(&engine).await;
    exec(&sess, CREATE_CAGG).await;

    // Mutate source: add to hour 10 (inside window) and hour 12 (outside).
    exec(
        &sess,
        "INSERT INTO metrics (ts, device, val) VALUES \
         ('2024-01-15 10:50:00+00','a',100), \
         ('2024-01-15 12:50:00+00','a',100)",
    )
    .await;

    // Refresh only [10:00, 11:00): hour 10 should pick up +100; hour 12 must
    // retain its original materialised value (4) because it is outside.
    exec(
        &sess,
        "CALL refresh_continuous_aggregate('metrics_hourly', \
         TIMESTAMP '2024-01-15 10:00:00+00', TIMESTAMP '2024-01-15 11:00:00+00')",
    )
    .await;

    let rows = bucket_rows(&sess, "SELECT bucket, total FROM metrics_hourly").await;
    let expected = vec![
        (hour_us(2024, 1, 15, 10), 106), // 6 + 100
        (hour_us(2024, 1, 15, 11), 9),   // untouched
        (hour_us(2024, 1, 15, 12), 4),   // untouched: +100 NOT applied (outside window)
    ];
    assert_eq!(rows, expected, "windowed refresh upserted wrong buckets");
}

/// `add_continuous_aggregate_policy` registers; a deterministic policy run
/// refreshes the materialised store.
#[tokio::test]
async fn cagg_add_policy_and_run() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = seed_hypertable(&engine).await;
    exec(&sess, CREATE_CAGG).await;

    // Register a policy (unbounded start, no end-offset lag, hourly schedule).
    let res = sess
        .execute(
            "SELECT add_continuous_aggregate_policy('metrics_hourly', \
             start_offset => NULL, end_offset => NULL, \
             schedule_interval => INTERVAL '1 hour')",
        )
        .await
        .expect("add policy");
    match res {
        ExecResult::Rows { batches, .. } => {
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        }
        other => panic!("expected job-id row, got {other:?}"),
    }

    // Add a late row, then trigger the policy deterministically via the
    // engine's test hook (same core the background refresher uses).
    exec(
        &sess,
        "INSERT INTO metrics (ts, device, val) VALUES ('2024-01-15 11:55:00+00','a',6)",
    )
    .await;
    let ran = sess
        .run_continuous_aggregate_policy("metrics_hourly")
        .await
        .expect("run policy");
    assert!(ran, "policy should have run");

    let rows = bucket_rows(&sess, "SELECT bucket, total FROM metrics_hourly").await;
    let h11 = rows
        .iter()
        .find(|(b, _)| *b == hour_us(2024, 1, 15, 11))
        .map(|(_, v)| *v);
    assert_eq!(
        h11,
        Some(15),
        "policy run should re-aggregate hour 11 (9+6)"
    );
}

/// Reading a cagg serves the materialised store (a regular table SELECT).
#[tokio::test]
async fn cagg_query_reads_materialized() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = seed_hypertable(&engine).await;
    exec(&sess, CREATE_CAGG).await;

    // A filter + aggregate over the cagg works like any table.
    let total: i64 = match sess
        .execute("SELECT sum(total) AS s FROM metrics_hourly")
        .await
        .unwrap()
    {
        ExecResult::Rows { batches, .. } => batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        other => panic!("non-rows: {other:?}"),
    };
    assert_eq!(total, 19, "6+9+4");
    assert_eq!(row_count(&sess, "SELECT * FROM metrics_hourly").await, 3);
}

/// `drop_chunks` on the source removes source rows but leaves the already-
/// materialised cagg rows intact (TimescaleDB semantics).
#[tokio::test]
async fn cagg_drop_chunks_preserves_rows() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = seed_hypertable(&engine).await;
    exec(&sess, CREATE_CAGG).await;

    let before = bucket_rows(&sess, "SELECT bucket, total FROM metrics_hourly").await;
    assert_eq!(before.len(), 3);

    // Drop source chunks older than 2024-01-15 12:00 — removes hours 10 & 11.
    exec(
        &sess,
        "SELECT drop_chunks('metrics', older_than => TIMESTAMP '2024-01-15 12:00:00+00')",
    )
    .await;

    // Source has lost the early rows...
    let src_early = row_count(
        &sess,
        "SELECT * FROM metrics WHERE ts < TIMESTAMPTZ '2024-01-15 12:00:00+00'",
    )
    .await;
    assert_eq!(
        src_early, 0,
        "drop_chunks should have removed early source rows"
    );

    // ...but the cagg keeps every materialised bucket.
    let after = bucket_rows(&sess, "SELECT bucket, total FROM metrics_hourly").await;
    assert_eq!(after, before, "cagg rows must survive source drop_chunks");
}

/// A cagg over various bucket widths materialises correctly.
#[tokio::test]
async fn cagg_various_widths() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = seed_hypertable(&engine).await;

    // Daily cagg: all six seeded rows fall on 2024-01-15 → one bucket of 19.
    exec(
        &sess,
        "CREATE MATERIALIZED VIEW metrics_daily WITH (timescaledb.continuous) AS \
         SELECT time_bucket('1 day', ts) AS bucket, sum(val) AS total \
         FROM metrics GROUP BY bucket",
    )
    .await;
    let daily = bucket_rows(&sess, "SELECT bucket, total FROM metrics_daily").await;
    assert_eq!(daily, vec![(hour_us(2024, 1, 15, 0), 19)]);

    // 30-minute cagg over the same data — finer grid, more buckets.
    exec(
        &sess,
        "CREATE MATERIALIZED VIEW metrics_30m WITH (timescaledb.continuous) AS \
         SELECT time_bucket('30 minutes', ts) AS bucket, sum(val) AS total \
         FROM metrics GROUP BY bucket",
    )
    .await;
    // Buckets: 10:00 (1+2=3), 10:30 (3), 11:00 (4), 11:30 (5), 12:30 (4).
    let half = bucket_rows(&sess, "SELECT bucket, total FROM metrics_30m").await;
    assert_eq!(half.iter().map(|(_, v)| *v).sum::<i64>(), 19);
    assert_eq!(half.len(), 5, "five distinct 30-min buckets");
}

/// Differential: the cagg's exact rows match a hand-computed bucket grid.
#[tokio::test]
async fn cagg_differential_handcomputed() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = seed_hypertable(&engine).await;
    exec(&sess, CREATE_CAGG).await;

    // Hand-computed hourly grid for the seed data.
    let expected: Vec<(i64, i64)> = vec![
        (hour_us(2024, 1, 15, 10), 1 + 2 + 3),
        (hour_us(2024, 1, 15, 11), 4 + 5),
        (hour_us(2024, 1, 15, 12), 4),
    ];
    let got = bucket_rows(
        &sess,
        "SELECT bucket, total FROM metrics_hourly ORDER BY bucket",
    )
    .await;
    assert_eq!(got, expected, "hand-computed bucket grid mismatch");
}
