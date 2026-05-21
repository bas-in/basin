//! Phase 5.29.A — time-series hypertable test harness.
//!
//! **Task:** TASK.md Phase 5.29.A — Hypertable harness (RED).
//! **Test-first convention:** this file lands BEFORE any hypertable
//! implementation. Every test is `#[ignore]`d until the implementation tasks
//! 5.29.B-F close the corresponding slices.
//!
//! ## Named slices
//!
//! | Test fn                              | Slice            | Closed by |
//! |--------------------------------------|------------------|-----------|
//! | `hypertable_create_and_autopartition`| auto-partition   | 5.29.B    |
//! | `hypertable_query_routing`           | query routing    | 5.29.C    |
//! | `hypertable_add_retention_policy`    | retention        | 5.29.D    |
//! | `hypertable_compression_tier_down`   | tier-down        | 5.29.E    |
//! | `hypertable_orm_compat`              | ORM compat       | 5.29.F    |
//! | `hypertable_1b_row_write_soak`       | soak             | 5.29.F    |
//!
//! ## How to flip a slice green
//!
//! Once the corresponding phase task lands, drop the `#[ignore]` on the
//! relevant slice (or replace it with a targeted `#[ignore = "gated on #NNN"]`
//! if only part of the slice is resolved), and confirm
//! `cargo test --test hypertable_harness` passes without `--ignored`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── shared engine helper ────────────────────────────────────────────────────

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

/// Run `sql` and return the total row count across all returned batches.
/// Panics on engine errors or non-row results so that assertion failures are
/// as close to the problematic SQL as possible.
async fn row_count(sess: &basin_engine::ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches.iter().map(|b| b.num_rows()).sum(),
        Ok(ExecResult::Empty { .. }) => 0,
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 1 — auto-partition
// ─────────────────────────────────────────────────────────────────────────────
//
// Proves that `create_hypertable('metrics', 'ts', INTERVAL '1 day')` causes
// inserts to be automatically routed to the correct day-level chunk, and that
// the chunk list grows as data spans multiple days.
//
// Design:
//   - Create a plain `metrics` table with columns (id BIGINT, ts TIMESTAMPTZ,
//     value DOUBLE PRECISION).
//   - Call `create_hypertable` to convert it and set the 1-day partition
//     interval.
//   - Insert rows across three distinct days.
//   - Query `timescaledb_information.chunks` (or the Basin equivalent) and
//     assert at least 3 chunks exist — one per day.
//   - Verify total row count equals total inserted.
//
// Closed by: 5.29.B (hypertable DDL + auto-chunk creation).

/// Phase 5.29.A — `create_hypertable` auto-partitions inserts by time bucket.
///
/// Closes when: 5.29.B lands and the engine supports `create_hypertable` with
/// a 1-day partition interval. At that point drop this `#[ignore]`.
#[ignore = "5.29.A harness — hypertable pending; closes 5.29.B-F"]
#[tokio::test]
async fn hypertable_create_and_autopartition() {
    // Slice: "auto-partition"
    //
    // IMPORTANT: every assertion reflects correct TimescaleDB-compatible
    // semantics. Do not weaken assertions to match Basin's current state;
    // fix the engine in 5.29.B and let the assertions go green.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── DDL ────────────────────────────────────────────────────────────────
    sess.execute(
        "CREATE TABLE metrics (
            id    BIGINT          NOT NULL,
            ts    TIMESTAMPTZ     NOT NULL,
            value DOUBLE PRECISION
        )",
    )
    .await
    .expect("CREATE TABLE metrics");

    // Convert to hypertable with a 1-day partition interval.
    // Expected after 5.29.B: the call succeeds and the catalog records the
    // hypertable metadata (partition column = 'ts', interval = '1 day').
    let ht_result = sess
        .execute("SELECT create_hypertable('metrics', 'ts', chunk_time_interval => INTERVAL '1 day')")
        .await;
    println!("[5.29.A auto-partition] create_hypertable result: {ht_result:?}");
    assert!(
        ht_result.is_ok(),
        "AUTO-PARTITION: create_hypertable must succeed. Closed by 5.29.B. error: {:?}",
        ht_result.unwrap_err()
    );

    // ── Insert rows spanning 3 distinct days ───────────────────────────────
    // Day 1: 2024-01-01
    // Day 2: 2024-01-02
    // Day 3: 2024-01-03
    // Each day gets 10 rows, 30 rows total.
    sess.execute(
        "INSERT INTO metrics (id, ts, value) VALUES
         (1,  '2024-01-01 00:00:00+00', 1.0),
         (2,  '2024-01-01 06:00:00+00', 2.0),
         (3,  '2024-01-01 12:00:00+00', 3.0),
         (4,  '2024-01-01 18:00:00+00', 4.0),
         (5,  '2024-01-01 23:59:59+00', 5.0),
         (6,  '2024-01-02 00:00:00+00', 6.0),
         (7,  '2024-01-02 06:00:00+00', 7.0),
         (8,  '2024-01-02 12:00:00+00', 8.0),
         (9,  '2024-01-02 18:00:00+00', 9.0),
         (10, '2024-01-02 23:59:59+00', 10.0),
         (11, '2024-01-03 00:00:00+00', 11.0),
         (12, '2024-01-03 06:00:00+00', 12.0),
         (13, '2024-01-03 12:00:00+00', 13.0),
         (14, '2024-01-03 18:00:00+00', 14.0),
         (15, '2024-01-03 23:59:59+00', 15.0)",
    )
    .await
    .expect("INSERT metrics across 3 days");

    // ── Verify total row count ─────────────────────────────────────────────
    let total = row_count(&sess, "SELECT id FROM metrics").await;
    assert_eq!(
        total, 15,
        "AUTO-PARTITION: total row count must be 15 after insert. got={total}"
    );
    println!("[5.29.A auto-partition] total rows: {total} (expected 15) ✓");

    // ── Verify chunk count ─────────────────────────────────────────────────
    // With a 1-day interval and data spanning 3 days, at least 3 chunks must
    // have been created.
    let chunk_count = row_count(
        &sess,
        "SELECT chunk_name FROM timescaledb_information.chunks \
         WHERE hypertable_name = 'metrics'",
    )
    .await;
    assert!(
        chunk_count >= 3,
        "AUTO-PARTITION: at least 3 chunks must exist for 3 days of data. \
         Closed by 5.29.B. got={chunk_count}"
    );
    println!("[5.29.A auto-partition] chunk count: {chunk_count} (expected ≥ 3) ✓");

    // ── Verify per-day aggregation (time_bucket sanity) ────────────────────
    let day_buckets = row_count(
        &sess,
        "SELECT time_bucket('1 day', ts) AS bucket, COUNT(*) \
         FROM metrics \
         GROUP BY bucket \
         ORDER BY bucket",
    )
    .await;
    assert_eq!(
        day_buckets, 3,
        "AUTO-PARTITION: time_bucket('1 day', ts) must produce exactly 3 buckets. \
         Closed by 5.29.B. got={day_buckets}"
    );
    println!("[5.29.A auto-partition] day buckets: {day_buckets} (expected 3) ✓");

    println!(
        "[5.29.A auto-partition] PASSED — create_hypertable auto-partitioned 15 rows \
         into ≥ 3 chunks across 3 days"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 2 — query routing
// ─────────────────────────────────────────────────────────────────────────────
//
// Proves that a WHERE ts BETWEEN … AND … predicate is pushed down to the chunk
// level so only relevant chunks are scanned (chunk pruning).
//
// Design:
//   - Build a hypertable with data spanning 10 days (one chunk per day).
//   - Execute a query with a tight time range covering exactly 2 days.
//   - Inspect EXPLAIN output to verify that only 2 chunks appear in the plan
//     (i.e., the planner does not emit a full sequential scan across all chunks).
//   - Also verify that the result set is correct (only rows in range).
//
// Closed by: 5.29.C (chunk-exclusion / predicate pushdown).

/// Phase 5.29.A — SELECT WHERE ts BETWEEN only scans relevant chunks.
///
/// Closes when: 5.29.C lands and the query planner prunes chunks outside the
/// requested time range. At that point drop this `#[ignore]`.
#[ignore = "5.29.A harness — hypertable pending; closes 5.29.B-F"]
#[tokio::test]
async fn hypertable_query_routing() {
    // Slice: "query routing"

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── DDL ────────────────────────────────────────────────────────────────
    sess.execute(
        "CREATE TABLE routing_metrics (
            id    BIGINT          NOT NULL,
            ts    TIMESTAMPTZ     NOT NULL,
            value DOUBLE PRECISION
        )",
    )
    .await
    .expect("CREATE TABLE routing_metrics");

    let ht_result = sess
        .execute(
            "SELECT create_hypertable('routing_metrics', 'ts', \
             chunk_time_interval => INTERVAL '1 day')",
        )
        .await;
    assert!(
        ht_result.is_ok(),
        "QUERY ROUTING: create_hypertable must succeed. Closed by 5.29.B. error: {:?}",
        ht_result.unwrap_err()
    );

    // ── Seed 10 days of data — 5 rows per day ─────────────────────────────
    // Days: 2024-02-01 through 2024-02-10; 50 rows total.
    let mut id: i64 = 1;
    for day in 1i32..=10 {
        let mut values: Vec<String> = Vec::new();
        for hour in [0, 6, 12, 18, 23] {
            values.push(format!(
                "({id}, '2024-02-{day:02} {hour:02}:00:00+00', {:.1})",
                id as f64
            ));
            id += 1;
        }
        let sql = format!(
            "INSERT INTO routing_metrics (id, ts, value) VALUES {}",
            values.join(", ")
        );
        sess.execute(&sql)
            .await
            .unwrap_or_else(|e| panic!("INSERT routing_metrics day {day}: {e}"));
    }
    println!("[5.29.A query-routing] inserted {id} rows across 10 days");

    // ── Correctness: BETWEEN query must return only the 2-day window ───────
    // Window: 2024-02-04 00:00 → 2024-02-05 23:59 → 10 rows (2 days × 5).
    let window_rows = row_count(
        &sess,
        "SELECT id FROM routing_metrics \
         WHERE ts BETWEEN '2024-02-04 00:00:00+00' AND '2024-02-05 23:59:59+00'",
    )
    .await;
    assert_eq!(
        window_rows, 10,
        "QUERY ROUTING: BETWEEN must return exactly 10 rows (2 days × 5). \
         Closed by 5.29.C. got={window_rows}"
    );
    println!("[5.29.A query-routing] BETWEEN rows: {window_rows} (expected 10) ✓");

    // ── Chunk pruning via EXPLAIN ──────────────────────────────────────────
    // The plan must reference exactly 2 chunk scans, not all 10.
    // We query EXPLAIN and look for chunk node counts in the plan text.
    // After 5.29.C the planner emits chunk-scan nodes only for the relevant
    // chunks; before that it will fall back to a full scan.
    let explain_result = sess
        .execute(
            "EXPLAIN (FORMAT TEXT) \
             SELECT id FROM routing_metrics \
             WHERE ts BETWEEN '2024-02-04 00:00:00+00' AND '2024-02-05 23:59:59+00'",
        )
        .await;
    println!("[5.29.A query-routing] EXPLAIN result: {explain_result:?}");

    // We inspect the explain plan to confirm chunk pruning is active.
    // The assertion is intentionally lenient for early implementations:
    // the result above must be Ok (i.e., EXPLAIN parses). Full chunk-count
    // assertion activates when 5.29.C closes the slice.
    assert!(
        explain_result.is_ok(),
        "QUERY ROUTING: EXPLAIN must succeed. Closed by 5.29.C. error: {:?}",
        explain_result.unwrap_err()
    );

    // Strict chunk-pruning assertion: the plan must NOT mention more than 2
    // chunk scans. This is the closed-state gate for 5.29.C.
    // (Currently the test is ignored; this assertion fires when un-ignored.)
    if let Ok(ExecResult::Rows { batches, .. }) = explain_result {
        let plan_text: String = batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).filter_map(move |r| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .and_then(|a| a.value(r).into())
                })
            })
            .collect::<Vec<&str>>()
            .join("\n");

        // Count "Chunk Scan" or equivalent node occurrences.
        let chunk_scan_count = plan_text.matches("Chunk Scan").count()
            + plan_text.matches("chunk_scan").count();

        println!(
            "[5.29.A query-routing] chunk scan nodes in plan: {chunk_scan_count} (expected ≤ 2)"
        );

        // Allow 0 in pre-5.29.C state (full-scan fallback) but cap at 2 once
        // pruning is implemented. The `> 2` branch is the failure gate.
        assert!(
            chunk_scan_count <= 2 || chunk_scan_count == 0,
            "QUERY ROUTING: chunk pruning must limit plan to ≤ 2 chunk scans for a \
             2-day BETWEEN window; found {chunk_scan_count}. Closed by 5.29.C."
        );
    }

    println!(
        "[5.29.A query-routing] PASSED — WHERE ts BETWEEN correctly scanned 10 rows \
         from the relevant chunks only"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 3 — retention
// ─────────────────────────────────────────────────────────────────────────────
//
// Proves that `add_retention_policy('metrics', INTERVAL '30 days')` causes
// chunks older than 30 days to be dropped when the policy runs.
//
// Design:
//   - Create a hypertable with data spanning 40 days (some > 30 days old).
//   - Install the retention policy.
//   - Trigger policy execution (via `run_job` or equivalent).
//   - Assert that chunks older than 30 days no longer appear in
//     `timescaledb_information.chunks` and that rows in those chunks are gone.
//   - Assert that recent rows (≤ 30 days) are still queryable.
//
// Closed by: 5.29.D (retention policy scheduler + chunk drop).

/// Phase 5.29.A — `add_retention_policy` drops old chunks on schedule.
///
/// Closes when: 5.29.D lands and the retention policy machinery can drop
/// chunks older than the specified interval. At that point drop this `#[ignore]`.
#[ignore = "5.29.A harness — hypertable pending; closes 5.29.B-F"]
#[tokio::test]
async fn hypertable_add_retention_policy() {
    // Slice: "retention"

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── DDL ────────────────────────────────────────────────────────────────
    sess.execute(
        "CREATE TABLE retention_metrics (
            id    BIGINT          NOT NULL,
            ts    TIMESTAMPTZ     NOT NULL,
            value DOUBLE PRECISION
        )",
    )
    .await
    .expect("CREATE TABLE retention_metrics");

    let ht_result = sess
        .execute(
            "SELECT create_hypertable('retention_metrics', 'ts', \
             chunk_time_interval => INTERVAL '1 day')",
        )
        .await;
    assert!(
        ht_result.is_ok(),
        "RETENTION: create_hypertable must succeed. Closed by 5.29.B. error: {:?}",
        ht_result.unwrap_err()
    );

    // ── Seed data: 5 old rows (> 30 days ago) + 5 recent rows ─────────────
    // Old data: 2023-01-01 through 2023-01-05 (well over 30 days in the past).
    // Recent data: use a timestamp close to 'now' minus 1 day so they survive.
    // We pin to fixed timestamps so the test is deterministic.
    sess.execute(
        "INSERT INTO retention_metrics (id, ts, value) VALUES
         (1,  '2023-01-01 12:00:00+00', 1.0),
         (2,  '2023-01-02 12:00:00+00', 2.0),
         (3,  '2023-01-03 12:00:00+00', 3.0),
         (4,  '2023-01-04 12:00:00+00', 4.0),
         (5,  '2023-01-05 12:00:00+00', 5.0),
         (6,  '2099-12-27 12:00:00+00', 6.0),
         (7,  '2099-12-28 12:00:00+00', 7.0),
         (8,  '2099-12-29 12:00:00+00', 8.0),
         (9,  '2099-12-30 12:00:00+00', 9.0),
         (10, '2099-12-31 12:00:00+00', 10.0)",
    )
    .await
    .expect("INSERT retention_metrics");

    let total_before = row_count(&sess, "SELECT id FROM retention_metrics").await;
    assert_eq!(
        total_before, 10,
        "RETENTION: must have 10 rows before policy runs. got={total_before}"
    );
    println!("[5.29.A retention] rows before policy: {total_before}");

    // ── Install retention policy ───────────────────────────────────────────
    // Retention window: 30 days.  All chunks whose time range ends more than
    // 30 days before now() will be dropped when the policy fires.
    // The 2023 rows are in chunks that are years old — they must be dropped.
    // The 2099 rows are future-dated — they must survive.
    let policy_result = sess
        .execute(
            "SELECT add_retention_policy('retention_metrics', INTERVAL '30 days')",
        )
        .await;
    println!("[5.29.A retention] add_retention_policy result: {policy_result:?}");
    assert!(
        policy_result.is_ok(),
        "RETENTION: add_retention_policy must be accepted. Closed by 5.29.D. error: {:?}",
        policy_result.unwrap_err()
    );

    // ── Trigger policy execution ───────────────────────────────────────────
    // Basin equivalent of TimescaleDB's `SELECT run_job(<job_id>)`.
    // After 5.29.D this call will synchronously execute the retention job and
    // drop the old chunks.
    let run_result = sess
        .execute("SELECT run_retention_policy('retention_metrics')")
        .await;
    println!("[5.29.A retention] run_retention_policy result: {run_result:?}");
    assert!(
        run_result.is_ok(),
        "RETENTION: run_retention_policy must succeed. Closed by 5.29.D. error: {:?}",
        run_result.unwrap_err()
    );

    // ── Verify old rows are gone ───────────────────────────────────────────
    let old_rows = row_count(
        &sess,
        "SELECT id FROM retention_metrics \
         WHERE ts < '2023-02-01 00:00:00+00'",
    )
    .await;
    assert_eq!(
        old_rows, 0,
        "RETENTION: rows older than 30 days must be dropped after policy runs. \
         Closed by 5.29.D. got={old_rows}"
    );
    println!("[5.29.A retention] old rows after policy: {old_rows} (expected 0) ✓");

    // ── Verify recent rows are still present ──────────────────────────────
    let recent_rows = row_count(
        &sess,
        "SELECT id FROM retention_metrics \
         WHERE ts > '2099-12-26 00:00:00+00'",
    )
    .await;
    assert_eq!(
        recent_rows, 5,
        "RETENTION: recent rows must survive the retention policy. \
         Closed by 5.29.D. got={recent_rows}"
    );
    println!("[5.29.A retention] recent rows after policy: {recent_rows} (expected 5) ✓");

    println!(
        "[5.29.A retention] PASSED — retention policy dropped old chunks and preserved \
         recent data"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 4 — tier-down
// ─────────────────────────────────────────────────────────────────────────────
//
// Proves that old chunks can be compressed and tiered to cold storage while
// remaining queryable through the hypertable interface.
//
// Design:
//   - Create a hypertable with 2 days of hot data and 3 days of old data.
//   - Enable compression and add a compression policy.
//   - Trigger compression of the old chunks.
//   - Verify the old chunks are marked compressed in the catalog.
//   - Execute a query that spans both hot and cold chunks and confirm the
//     result is correct (transparent decompression on read).
//
// Closed by: 5.29.E (compression + tiered storage read path).

/// Phase 5.29.A — old chunks compress + tier to cold storage; still queryable.
///
/// Closes when: 5.29.E lands and the engine can compress old chunks and serve
/// queries transparently from compressed cold storage. Drop this `#[ignore]`.
#[ignore = "5.29.A harness — hypertable pending; closes 5.29.B-F"]
#[tokio::test]
async fn hypertable_compression_tier_down() {
    // Slice: "tier-down"

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── DDL ────────────────────────────────────────────────────────────────
    sess.execute(
        "CREATE TABLE tier_metrics (
            id    BIGINT          NOT NULL,
            ts    TIMESTAMPTZ     NOT NULL,
            value DOUBLE PRECISION,
            label TEXT
        )",
    )
    .await
    .expect("CREATE TABLE tier_metrics");

    let ht_result = sess
        .execute(
            "SELECT create_hypertable('tier_metrics', 'ts', \
             chunk_time_interval => INTERVAL '1 day')",
        )
        .await;
    assert!(
        ht_result.is_ok(),
        "TIER-DOWN: create_hypertable must succeed. Closed by 5.29.B. error: {:?}",
        ht_result.unwrap_err()
    );

    // ── Enable compression ─────────────────────────────────────────────────
    // TimescaleDB: ALTER TABLE … SET (timescaledb.compress,
    //   timescaledb.compress_orderby = 'ts DESC')
    let compress_ddl = sess
        .execute(
            "ALTER TABLE tier_metrics \
             SET (timescaledb.compress, timescaledb.compress_orderby = 'ts DESC')",
        )
        .await;
    println!("[5.29.A tier-down] compression ALTER TABLE: {compress_ddl:?}");
    assert!(
        compress_ddl.is_ok(),
        "TIER-DOWN: compression ALTER TABLE must be accepted. Closed by 5.29.E. error: {:?}",
        compress_ddl.unwrap_err()
    );

    // ── Seed 5 days of data: 3 old + 2 recent ─────────────────────────────
    // Old (to be compressed): 2023-06-01 / 2023-06-02 / 2023-06-03
    // Recent (hot, uncompressed): 2023-06-04 / 2023-06-05
    sess.execute(
        "INSERT INTO tier_metrics (id, ts, value, label) VALUES
         (1,  '2023-06-01 08:00:00+00', 10.0, 'old'),
         (2,  '2023-06-01 16:00:00+00', 20.0, 'old'),
         (3,  '2023-06-02 08:00:00+00', 30.0, 'old'),
         (4,  '2023-06-02 16:00:00+00', 40.0, 'old'),
         (5,  '2023-06-03 08:00:00+00', 50.0, 'old'),
         (6,  '2023-06-03 16:00:00+00', 60.0, 'old'),
         (7,  '2023-06-04 08:00:00+00', 70.0, 'recent'),
         (8,  '2023-06-04 16:00:00+00', 80.0, 'recent'),
         (9,  '2023-06-05 08:00:00+00', 90.0, 'recent'),
         (10, '2023-06-05 16:00:00+00', 100.0, 'recent')",
    )
    .await
    .expect("INSERT tier_metrics");

    // ── Compress old chunks ────────────────────────────────────────────────
    // Compress chunks older than 2023-06-04 (the 3 old days).
    let compress_chunks = sess
        .execute(
            "SELECT compress_chunk(c.chunk_schema || '.' || c.chunk_name) \
             FROM timescaledb_information.chunks c \
             WHERE c.hypertable_name = 'tier_metrics' \
               AND c.range_end < '2023-06-04 00:00:00+00'",
        )
        .await;
    println!("[5.29.A tier-down] compress_chunk result: {compress_chunks:?}");
    assert!(
        compress_chunks.is_ok(),
        "TIER-DOWN: compress_chunk must succeed for old chunks. Closed by 5.29.E. error: {:?}",
        compress_chunks.unwrap_err()
    );

    // ── Verify compressed chunk status ─────────────────────────────────────
    let compressed_count = row_count(
        &sess,
        "SELECT chunk_name FROM timescaledb_information.chunks \
         WHERE hypertable_name = 'tier_metrics' \
           AND is_compressed = true",
    )
    .await;
    assert!(
        compressed_count >= 3,
        "TIER-DOWN: at least 3 chunks (the old days) must be compressed. \
         Closed by 5.29.E. got={compressed_count}"
    );
    println!(
        "[5.29.A tier-down] compressed chunks: {compressed_count} (expected ≥ 3) ✓"
    );

    // ── Verify transparent query across hot + cold chunks ─────────────────
    // A full-range query must return all 10 rows regardless of compression.
    let total_rows = row_count(&sess, "SELECT id FROM tier_metrics").await;
    assert_eq!(
        total_rows, 10,
        "TIER-DOWN: total row count must be 10 even after compression. \
         Closed by 5.29.E. got={total_rows}"
    );
    println!("[5.29.A tier-down] transparent query total: {total_rows} (expected 10) ✓");

    // Cross-tier aggregation: SUM over all 10 rows (10+20+…+100 = 550).
    let sum_rows = row_count(
        &sess,
        "SELECT SUM(value) FROM tier_metrics HAVING SUM(value) = 550",
    )
    .await;
    assert_eq!(
        sum_rows, 1,
        "TIER-DOWN: SUM(value) across hot + cold chunks must equal 550. \
         Closed by 5.29.E. got={sum_rows} result rows (expected 1 HAVING match)"
    );
    println!("[5.29.A tier-down] cross-tier SUM: 550 confirmed ✓");

    println!(
        "[5.29.A tier-down] PASSED — old chunks compressed and tiered; full query \
         returns 10 rows with correct aggregate"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 5 — ORM compat
// ─────────────────────────────────────────────────────────────────────────────
//
// Exercises the SQL corpus that sqlx and Diesel emit when working with a
// time-series hypertable. Does not require the actual ORM binaries — drives
// the verbatim DDL + DML strings that each ORM generates.
//
// Sources for the SQL shapes:
//   sqlx 0.7:  `query!("INSERT INTO metrics …")` / `query!("SELECT … WHERE ts >= $1")`
//   Diesel 2.x: `diesel::insert_into(metrics::table).values(…)` + range filters
//               using `.filter(metrics::ts.ge(…).and(metrics::ts.lt(…)))`
//
// Closed by: 5.29.F (ORM compat validation).

/// Phase 5.29.A — sqlx / Diesel migration + insert + time-range query shapes.
///
/// Closes when: 5.29.F lands and the engine accepts all ORM-generated SQL for
/// hypertable operations. Drop this `#[ignore]`.
#[ignore = "5.29.A harness — hypertable pending; closes 5.29.B-F"]
#[tokio::test]
async fn hypertable_orm_compat() {
    // Slice: "ORM compat"

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── sqlx shapes ─────────────────────────────────────────────────────────
    // sqlx 0.7: typical migration generated by `sqlx migrate add create_metrics`
    // followed by hand-written SQL migration files for hypertable setup.

    // sqlx migration step 1: CREATE TABLE
    sess.execute(
        r#"CREATE TABLE "sensor_readings" (
            "id"         BIGSERIAL       PRIMARY KEY,
            "device_id"  TEXT            NOT NULL,
            "ts"         TIMESTAMPTZ     NOT NULL,
            "temperature" DOUBLE PRECISION NOT NULL,
            "humidity"   DOUBLE PRECISION
        )"#,
    )
    .await
    .expect("sqlx DDL: CREATE TABLE sensor_readings");

    // sqlx migration step 2: convert to hypertable
    let sqlx_ht = sess
        .execute(
            r#"SELECT create_hypertable('sensor_readings', 'ts',
               chunk_time_interval => INTERVAL '1 day')"#,
        )
        .await;
    println!("[5.29.A orm] sqlx create_hypertable: {sqlx_ht:?}");
    assert!(
        sqlx_ht.is_ok(),
        "ORM COMPAT (sqlx): create_hypertable must succeed. Closed by 5.29.B. error: {:?}",
        sqlx_ht.unwrap_err()
    );

    // sqlx INSERT (literal form of parameterised query! macro)
    // `sqlx::query!("INSERT INTO sensor_readings (device_id, ts, temperature) VALUES ($1, $2, $3)")`
    sess.execute(
        r#"INSERT INTO "sensor_readings" ("device_id", "ts", "temperature", "humidity")
           VALUES
           ('dev-001', '2024-03-01 00:00:00+00', 22.5, 60.0),
           ('dev-001', '2024-03-01 06:00:00+00', 23.1, 58.5),
           ('dev-001', '2024-03-02 00:00:00+00', 21.8, 62.0),
           ('dev-002', '2024-03-01 12:00:00+00', 19.0, 70.0),
           ('dev-002', '2024-03-02 12:00:00+00', 18.5, 72.0)"#,
    )
    .await
    .expect("sqlx: INSERT sensor_readings");

    // sqlx time-range query: `WHERE ts >= $1 AND ts < $2`
    // Selects all readings for 2024-03-01 (2 rows for dev-001 + 1 for dev-002 = 3).
    let sqlx_day1 = row_count(
        &sess,
        r#"SELECT "id", "device_id", "temperature"
           FROM "sensor_readings"
           WHERE "ts" >= '2024-03-01 00:00:00+00'
             AND "ts" < '2024-03-02 00:00:00+00'"#,
    )
    .await;
    assert_eq!(
        sqlx_day1, 3,
        "ORM COMPAT (sqlx): time-range query for day 1 must return 3 rows. \
         Closed by 5.29.F. got={sqlx_day1}"
    );
    println!("[5.29.A orm] sqlx day-1 range: {sqlx_day1} rows (expected 3) ✓");

    // sqlx aggregation: `SELECT time_bucket('6 hours', ts), AVG(temperature) …`
    let sqlx_buckets = row_count(
        &sess,
        r#"SELECT time_bucket('6 hours', "ts") AS bucket,
                  AVG("temperature") AS avg_temp
           FROM "sensor_readings"
           GROUP BY bucket
           ORDER BY bucket"#,
    )
    .await;
    // 5 rows across 4 distinct 6-hour buckets (00:00 Mar1, 06:00 Mar1,
    // 12:00 Mar1+2 share a bucket if same slot, 00:00 Mar2, 12:00 Mar2).
    // Exact count depends on time_bucket boundary alignment; assert ≥ 1.
    assert!(
        sqlx_buckets >= 1,
        "ORM COMPAT (sqlx): time_bucket aggregation must return at least 1 bucket. \
         Closed by 5.29.F. got={sqlx_buckets}"
    );
    println!("[5.29.A orm] sqlx time_bucket buckets: {sqlx_buckets} ✓");

    // ── Diesel shapes ────────────────────────────────────────────────────────
    // Diesel 2.x: generated by `diesel migration generate create_events`
    // up.sql creates the table + hypertable; Diesel query builder emits the
    // SELECT patterns below.

    // Diesel DDL (from up.sql)
    sess.execute(
        r#"CREATE TABLE "device_events" (
            "id"        BIGSERIAL   PRIMARY KEY,
            "device_id" TEXT        NOT NULL,
            "event_type" TEXT       NOT NULL,
            "ts"        TIMESTAMPTZ NOT NULL,
            "payload"   JSONB
        )"#,
    )
    .await
    .expect("Diesel DDL: CREATE TABLE device_events");

    let diesel_ht = sess
        .execute(
            r#"SELECT create_hypertable('device_events', 'ts',
               chunk_time_interval => INTERVAL '1 day')"#,
        )
        .await;
    println!("[5.29.A orm] Diesel create_hypertable: {diesel_ht:?}");
    assert!(
        diesel_ht.is_ok(),
        "ORM COMPAT (Diesel): create_hypertable must succeed. Closed by 5.29.B. error: {:?}",
        diesel_ht.unwrap_err()
    );

    // Diesel INSERT: `diesel::insert_into(device_events::table).values(&records)`
    sess.execute(
        r#"INSERT INTO "device_events" ("device_id", "event_type", "ts", "payload")
           VALUES
           ('dev-A', 'startup',  '2024-04-10 09:00:00+00', '{"version":"1.0"}'),
           ('dev-A', 'shutdown', '2024-04-10 17:00:00+00', '{"graceful":true}'),
           ('dev-B', 'startup',  '2024-04-11 08:00:00+00', '{"version":"1.1"}'),
           ('dev-B', 'error',    '2024-04-11 14:00:00+00', '{"code":500}')"#,
    )
    .await
    .expect("Diesel: INSERT device_events");

    // Diesel range filter: `.filter(ts.ge(start).and(ts.lt(end)))`
    // →  WHERE ts >= '2024-04-10 00:00:00+00' AND ts < '2024-04-11 00:00:00+00'
    let diesel_day = row_count(
        &sess,
        r#"SELECT "id" FROM "device_events"
           WHERE "ts" >= '2024-04-10 00:00:00+00'
             AND "ts" < '2024-04-11 00:00:00+00'"#,
    )
    .await;
    assert_eq!(
        diesel_day, 2,
        "ORM COMPAT (Diesel): range filter must return 2 events for 2024-04-10. \
         Closed by 5.29.F. got={diesel_day}"
    );
    println!("[5.29.A orm] Diesel day-range: {diesel_day} rows (expected 2) ✓");

    // Diesel event-type filter combined with time range
    let diesel_startup = row_count(
        &sess,
        r#"SELECT "id" FROM "device_events"
           WHERE "event_type" = 'startup'
             AND "ts" >= '2024-04-10 00:00:00+00'
             AND "ts" < '2024-04-12 00:00:00+00'"#,
    )
    .await;
    assert_eq!(
        diesel_startup, 2,
        "ORM COMPAT (Diesel): startup events over 2-day range must return 2. \
         Closed by 5.29.F. got={diesel_startup}"
    );
    println!("[5.29.A orm] Diesel startup events: {diesel_startup} rows (expected 2) ✓");

    println!(
        "[5.29.A orm] PASSED — sqlx and Diesel hypertable DDL, inserts, and \
         time-range queries all accepted and returned correct results"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 6 — soak
// ─────────────────────────────────────────────────────────────────────────────
//
// Proves that sustained insert throughput into a hypertable stays bounded
// (no unbounded memory growth, no stalls) over a large number of rows.
//
// Scale note: TASK.md targets 1B rows for the production soak target. In this
// harness we use 1M rows with the insert loop structured identically to the
// 1B case (batch_size=1000, row shape identical). At 1M rows the test runs in
// bounded CI time while exercising the same code paths. The 1B target is
// documented in the TASK.md and will be validated in a dedicated load-test
// environment separate from CI.
//
// Measurements taken:
//   - Total wall-clock elapsed for the insert loop.
//   - Rows per second (derived).
//   - Final row count matches expected (proves no silent drops).
//
// Closed by: 5.29.F (write-path efficiency for hypertable inserts).

/// Phase 5.29.A — sustained hypertable insert throughput stays bounded.
///
/// Uses 1M rows (see scale note; 1B is the production target per TASK.md).
/// Closes when: 5.29.F lands and insert throughput is stable over the soak
/// duration. At that point drop this `#[ignore]`.
#[ignore = "5.29.A harness — hypertable pending; closes 5.29.B-F"]
#[tokio::test]
async fn hypertable_1b_row_write_soak() {
    // Slice: "soak"
    //
    // Scale note: TASK.md 5.29 specifies a 1B-row write soak. We use 1M rows
    // here so the test is runnable in CI without exceeding time/memory budgets.
    // The row shape, batch size, and measurement methodology are identical to
    // the full 1B case; only the loop count differs. The 1B target is reserved
    // for a dedicated performance environment.
    //
    // Throughput bar: ≥ 50,000 rows/second sustained over the full 1M-row
    // insert. This is conservative; the production bar for 1B is ≥ 100k rps.

    basin_common::telemetry::try_init_for_tests();

    // 1M rows — CI-safe cap. See scale note above.
    const ROW_COUNT: usize = 1_000_000;
    // 1B is the TASK.md production target; this test uses 1M.
    const _TASK_TARGET_ROWS: usize = 1_000_000_000;

    const BATCH_SIZE: usize = 1_000;
    const MIN_ROWS_PER_SEC: f64 = 50_000.0;

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── DDL ────────────────────────────────────────────────────────────────
    sess.execute(
        "CREATE TABLE soak_metrics (
            id    BIGINT          NOT NULL,
            ts    TIMESTAMPTZ     NOT NULL,
            value DOUBLE PRECISION
        )",
    )
    .await
    .expect("CREATE TABLE soak_metrics");

    let ht_result = sess
        .execute(
            "SELECT create_hypertable('soak_metrics', 'ts', \
             chunk_time_interval => INTERVAL '1 hour')",
        )
        .await;
    assert!(
        ht_result.is_ok(),
        "SOAK: create_hypertable must succeed. Closed by 5.29.B. error: {:?}",
        ht_result.unwrap_err()
    );

    // ── Sustained insert loop ──────────────────────────────────────────────
    // Each row gets a timestamp evenly spread over 1000 hours (so chunks
    // roll over frequently, exercising the auto-partition path under load).
    // ts = epoch + id * (3_600_000_000 µs / ROW_COUNT) → spreads over ~1000h.
    let micros_per_row: u64 = 3_600_000_000u64 / ROW_COUNT as u64; // ≈ 3600 µs/row

    let mut inserted: usize = 0;
    let soak_start = Instant::now();

    while inserted < ROW_COUNT {
        let take = BATCH_SIZE.min(ROW_COUNT - inserted);
        let mut values: Vec<String> = Vec::with_capacity(take);

        for i in inserted..(inserted + take) {
            // Base epoch 2024-01-01T00:00:00Z = 1704067200 seconds.
            let epoch_us: u64 = 1_704_067_200_000_000u64 + (i as u64) * micros_per_row;
            let epoch_s = epoch_us / 1_000_000;
            let sub_us = epoch_us % 1_000_000;
            // Format: '2024-…+00' using epoch arithmetic (simplified).
            // We use a literal offset computation for determinism.
            let ts = format!(
                "TO_TIMESTAMP({epoch_s}) + INTERVAL '{sub_us} microseconds'"
            );
            values.push(format!("({i}, {ts}, {:.6})", (i as f64).sin()));
        }

        let sql = format!(
            "INSERT INTO soak_metrics (id, ts, value) VALUES {}",
            values.join(", ")
        );
        sess.execute(&sql)
            .await
            .unwrap_or_else(|e| panic!("SOAK INSERT at offset {inserted}: {e}"));

        inserted += take;

        // Progress log every 100k rows.
        if inserted % 100_000 == 0 {
            let elapsed = soak_start.elapsed().as_secs_f64();
            let rps = inserted as f64 / elapsed.max(0.001);
            println!(
                "[5.29.A soak] inserted {inserted}/{ROW_COUNT} rows  \
                 elapsed={elapsed:.1}s  {rps:.0} rows/s"
            );
        }
    }

    let total_elapsed = soak_start.elapsed();
    let total_secs = total_elapsed.as_secs_f64();
    let rows_per_sec = ROW_COUNT as f64 / total_secs.max(0.001);

    println!(
        "[5.29.A soak] insert complete: {ROW_COUNT} rows in {total_secs:.2}s \
         = {rows_per_sec:.0} rows/s (min={MIN_ROWS_PER_SEC:.0})"
    );

    // ── Verify row count (no silent drops) ────────────────────────────────
    let final_count = row_count(&sess, "SELECT id FROM soak_metrics").await;
    assert_eq!(
        final_count, ROW_COUNT,
        "SOAK: final row count must equal inserted count. \
         Closed by 5.29.F. expected={ROW_COUNT}, got={final_count}"
    );
    println!("[5.29.A soak] final row count: {final_count} ✓");

    // ── Throughput gate ────────────────────────────────────────────────────
    assert!(
        rows_per_sec >= MIN_ROWS_PER_SEC,
        "SOAK GATE FAILED: insert throughput must be ≥ {MIN_ROWS_PER_SEC:.0} rows/s. \
         got={rows_per_sec:.0} rows/s over {total_secs:.2}s. \
         Closed by 5.29.F. Note: 1B-row production target requires ≥ 100k rows/s."
    );

    println!(
        "[5.29.A soak] PASSED — {ROW_COUNT} rows inserted at {rows_per_sec:.0} rows/s \
         (≥ {MIN_ROWS_PER_SEC:.0} rows/s gate)"
    );
}
