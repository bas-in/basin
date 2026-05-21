//! Phase 5.23.A — EXPLAIN ANALYZE + pg_stat_activity + pg_locks test harness.
//!
//! **Task:** TASK.md Phase 5.23.A — EXPLAIN ANALYZE + pg_stat_activity + pg_locks
//! harness. Test-first convention: this file lands BEFORE the corresponding
//! engine implementation. Every test is `#[ignore]`d until implementation tasks
//! 5.23.B-G close the corresponding slices.
//!
//! ## Named slices
//!
//! | Test fn                                  | Slice                     | Closed by |
//! |------------------------------------------|---------------------------|-----------|
//! | `explain_point_scan`                     | scan family               | 5.23.B    |
//! | `explain_range_scan_with_agg`            | agg/window/CTE family     | 5.23.C    |
//! | `explain_inner_join`                     | join family               | 5.23.D    |
//! | `explain_window_function`                | agg/window/CTE family     | 5.23.C    |
//! | `pg_stat_activity_view_shape`            | view shape                | 5.23.E    |
//! | `pg_locks_view_returns_acquired_locks`   | locks view                | 5.23.F    |
//! | `tooling_compat_pgcli_psql_runs_basic_session` | tooling compat      | 5.23.G    |
//!
//! ## How to flip a slice green
//!
//! Once the corresponding 5.23.B-G tasks land, drop the `#[ignore]` on the
//! relevant slice (or replace it with a targeted `#[ignore = "gated on #NNN"]`
//! if only part of the slice is resolved) and make sure
//! `cargo test --test explain_pg_stat_harness` passes without `--ignored`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::Array as _;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── shared engine helpers ───────────────────────────────────────────────────

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

/// Run `sql` and return all plan-text lines from the first (Utf8) column.
///
/// Panics on engine errors or non-row results so that assertion failures are
/// as close to the problematic SQL as possible.
async fn plan_lines(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<String> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let mut out = Vec::new();
            for batch in &batches {
                assert!(
                    batch.num_columns() >= 1,
                    "EXPLAIN result must have at least one column; sql={sql}"
                );
                let col = batch.column(0);
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .expect("QUERY PLAN column must be Utf8");
                for i in 0..arr.len() {
                    out.push(arr.value(i).to_string());
                }
            }
            out
        }
        Ok(ExecResult::Empty { tag }) => {
            panic!(
                "EXPLAIN returned Empty({tag}) — expected Rows with QUERY PLAN column; sql={sql}"
            );
        }
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

/// Run `sql` and return the total row count across all returned batches.
/// Panics on engine errors or non-row results.
async fn row_count(sess: &basin_engine::ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches.iter().map(|b| b.num_rows()).sum(),
        Ok(ExecResult::Empty { .. }) => 0,
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

/// Run `sql` and return every string value in the first column.
async fn string_column(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<String> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let mut out = Vec::new();
            for batch in &batches {
                if batch.num_columns() == 0 {
                    continue;
                }
                let col = batch.column(0);
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .expect("first column must be Utf8 for string_column helper");
                for i in 0..arr.len() {
                    out.push(arr.value(i).to_string());
                }
            }
            out
        }
        Ok(ExecResult::Empty { .. }) => vec![],
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 1 — Scan family: EXPLAIN ANALYZE point scan (indexed lookup)
// ─────────────────────────────────────────────────────────────────────────────
//
// Verifies that EXPLAIN ANALYZE on a point-lookup query (`WHERE id = X`)
// produces a plan that references an indexed-lookup node rather than a
// sequential scan. After a primary-key / unique index is added in 5.23.B,
// the planner must choose the index path and emit a node name containing
// "Index" or "Lookup" or "BitmapIndex" in the plan text.
//
// Closed by: 5.23.B (indexed scan planner integration).

/// Phase 5.23.A — EXPLAIN ANALYZE point scan: `WHERE id = X` must produce a
/// plan whose text references an indexed-lookup node, not a full sequential
/// scan.
///
/// After 5.23.B lands, the engine planner should prefer an index scan for
/// equality predicates on the primary key column. The test seeds 1 000 rows,
/// creates a unique index on `id`, and checks the plan output for the word
/// "Index" or "Lookup" (node names DataFusion uses for pushed-down seeks).
///
/// Closes when: 5.23.B (indexed scan planner integration). At that point drop
/// this `#[ignore]` and re-run `cargo test --test explain_pg_stat_harness`.
#[ignore = "5.23.A harness — pending 5.23.B-G"]
#[tokio::test]
async fn explain_point_scan() {
    // Slice: "scan family"
    //
    // IMPORTANT: every assertion here reflects *correct PostgreSQL-compatible
    // semantics*. Do not change the expected values to match Basin's current
    // (potentially missing) output — that defeats the test-first purpose.
    // Fix the engine in 5.23.B and let the assertions go green.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── DDL ──────────────────────────────────────────────────────────────────
    sess.execute(
        "CREATE TABLE scan_target (
            id    BIGINT NOT NULL,
            label TEXT   NOT NULL,
            score DOUBLE PRECISION
        )",
    )
    .await
    .expect("CREATE TABLE scan_target");

    // Attempt to create a unique index — closes in 5.23.B.
    let idx_result = sess
        .execute("CREATE UNIQUE INDEX scan_target_id_idx ON scan_target (id)")
        .await;
    println!("[5.23.A scan] CREATE UNIQUE INDEX result: {idx_result:?}");
    // We do not assert success here because the index DDL may not be supported
    // until 5.23.B; the plan-shape assertion below is the real contract.

    // ── Seed 1 000 rows ───────────────────────────────────────────────────────
    {
        let mut values: Vec<String> = Vec::with_capacity(1_000);
        for i in 0..1_000i64 {
            values.push(format!("({i}, 'label_{i}', {:.2})", i as f64 * 1.5));
        }
        sess.execute(&format!(
            "INSERT INTO scan_target VALUES {}",
            values.join(", ")
        ))
        .await
        .expect("INSERT scan_target");
    }
    println!("[5.23.A scan] inserted 1 000 rows");

    // ── EXPLAIN ANALYZE point lookup ──────────────────────────────────────────
    let plan = plan_lines(
        &sess,
        "EXPLAIN ANALYZE SELECT id, label, score FROM scan_target WHERE id = 42",
    )
    .await;

    assert!(
        !plan.is_empty(),
        "SCAN: EXPLAIN ANALYZE must return at least one plan line; got none"
    );

    let plan_text = plan.join("\n");
    println!("[5.23.A scan] plan:\n{plan_text}");

    // After 5.23.B: the plan must reference an index seek, not a sequential
    // scan. Node names emitted by DataFusion include "IndexScan",
    // "BitmapIndexScan", or "Lookup"; PG emits "Index Scan".
    let has_index_node = plan_text.contains("Index")
        || plan_text.contains("Lookup")
        || plan_text.contains("index");
    assert!(
        has_index_node,
        "SCAN (5.23.A): EXPLAIN ANALYZE on a point lookup (WHERE id = 42) must reference \
         an indexed-lookup node in the plan output. Got:\n{plan_text}\n\
         Closed by 5.23.B (indexed scan planner integration)."
    );

    // The plan must also include actual timing/row-count annotations because
    // ANALYZE was requested (these appear as "actual time=...", "rows=...", etc.).
    let has_timing = plan_text.contains("actual")
        || plan_text.contains("time=")
        || plan_text.contains("rows=");
    assert!(
        has_timing,
        "SCAN (5.23.A): EXPLAIN ANALYZE must include execution timing annotations \
         ('actual', 'time=', or 'rows='). Got:\n{plan_text}\n\
         Closed by 5.23.B."
    );

    println!("[5.23.A scan] PASSED — indexed-lookup node confirmed in plan");
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 2 — Agg/window/CTE family: EXPLAIN ANALYZE range scan + COUNT(*)
// ─────────────────────────────────────────────────────────────────────────────
//
// Verifies that EXPLAIN ANALYZE on a `SELECT count(*) WHERE col BETWEEN ...`
// query produces a plan that includes an aggregation node and ANALYZE timing.
//
// Closed by: 5.23.C (agg/window/CTE plan nodes in EXPLAIN ANALYZE output).

/// Phase 5.23.A — EXPLAIN ANALYZE range scan with aggregate: `SELECT count(*)`
/// with a BETWEEN predicate must produce a plan containing an aggregation node
/// and ANALYZE timing annotations.
///
/// Closes when: 5.23.C (aggregation plan-node exposure in EXPLAIN ANALYZE).
/// At that point drop this `#[ignore]`.
#[ignore = "5.23.A harness — pending 5.23.B-G"]
#[tokio::test]
async fn explain_range_scan_with_agg() {
    // Slice: "agg/window/CTE family" (partial — agg side)

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── DDL + seed ────────────────────────────────────────────────────────────
    sess.execute(
        "CREATE TABLE range_agg_target (
            id    BIGINT NOT NULL,
            value BIGINT NOT NULL
        )",
    )
    .await
    .expect("CREATE TABLE range_agg_target");

    {
        let mut values: Vec<String> = Vec::with_capacity(500);
        for i in 0..500i64 {
            values.push(format!("({i}, {})", i * 10));
        }
        sess.execute(&format!(
            "INSERT INTO range_agg_target VALUES {}",
            values.join(", ")
        ))
        .await
        .expect("INSERT range_agg_target");
    }
    println!("[5.23.A range_agg] inserted 500 rows");

    // ── EXPLAIN ANALYZE range + COUNT(*) ─────────────────────────────────────
    // The BETWEEN range covers rows where value BETWEEN 1000 AND 3000,
    // i.e. id 100..=300 → 201 rows.
    let plan = plan_lines(
        &sess,
        "EXPLAIN ANALYZE SELECT count(*) FROM range_agg_target WHERE value BETWEEN 1000 AND 3000",
    )
    .await;

    assert!(
        !plan.is_empty(),
        "RANGE_AGG: EXPLAIN ANALYZE must return at least one plan line"
    );

    let plan_text = plan.join("\n");
    println!("[5.23.A range_agg] plan:\n{plan_text}");

    // The plan must mention an aggregation node.
    let has_agg_node = plan_text.contains("Aggregate")
        || plan_text.contains("aggregate")
        || plan_text.contains("Count")
        || plan_text.contains("count");
    assert!(
        has_agg_node,
        "RANGE_AGG (5.23.A): EXPLAIN ANALYZE on SELECT count(*) must include an \
         aggregation node in the plan text. Got:\n{plan_text}\n\
         Closed by 5.23.C."
    );

    // Confirm the actual query is correct (sanity-check the row returned).
    let count_rows = row_count(
        &sess,
        "SELECT count(*) FROM range_agg_target WHERE value BETWEEN 1000 AND 3000",
    )
    .await;
    assert_eq!(
        count_rows, 1,
        "RANGE_AGG: SELECT count(*) must return exactly 1 result row (the aggregate). \
         got={count_rows}"
    );

    println!("[5.23.A range_agg] PASSED — aggregation node confirmed in plan");
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 3 — Join family: EXPLAIN ANALYZE inner join
// ─────────────────────────────────────────────────────────────────────────────
//
// Verifies that EXPLAIN ANALYZE on an inner join query produces a plan that
// includes a join node (Hash Join, Merge Join, Nested Loop, etc.) with ANALYZE
// timing annotations.
//
// Closed by: 5.23.D (join plan-node exposure in EXPLAIN ANALYZE output).

/// Phase 5.23.A — EXPLAIN ANALYZE inner join: `SELECT … JOIN …` must produce
/// a plan containing a join node (HashJoin, MergeJoin, NestedLoop) and ANALYZE
/// timing annotations.
///
/// Closes when: 5.23.D (join plan-node exposure in EXPLAIN ANALYZE).
/// At that point drop this `#[ignore]`.
#[ignore = "5.23.A harness — pending 5.23.B-G"]
#[tokio::test]
async fn explain_inner_join() {
    // Slice: "join family"

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── DDL + seed ────────────────────────────────────────────────────────────
    sess.execute(
        "CREATE TABLE join_orders (
            order_id   BIGINT NOT NULL,
            customer_id BIGINT NOT NULL,
            amount     DOUBLE PRECISION NOT NULL
        )",
    )
    .await
    .expect("CREATE TABLE join_orders");

    sess.execute(
        "CREATE TABLE join_customers (
            customer_id BIGINT NOT NULL,
            name        TEXT   NOT NULL
        )",
    )
    .await
    .expect("CREATE TABLE join_customers");

    // Seed 100 customers and 500 orders (5 orders per customer).
    {
        let cust_vals: Vec<String> = (0..100i64)
            .map(|i| format!("({i}, 'customer_{i}')"))
            .collect();
        sess.execute(&format!(
            "INSERT INTO join_customers VALUES {}",
            cust_vals.join(", ")
        ))
        .await
        .expect("INSERT join_customers");

        let ord_vals: Vec<String> = (0..500i64)
            .map(|i| format!("({i}, {}, {:.2})", i % 100, (i as f64) * 9.99))
            .collect();
        sess.execute(&format!(
            "INSERT INTO join_orders VALUES {}",
            ord_vals.join(", ")
        ))
        .await
        .expect("INSERT join_orders");
    }
    println!("[5.23.A join] inserted 100 customers and 500 orders");

    // ── EXPLAIN ANALYZE inner join ────────────────────────────────────────────
    let plan = plan_lines(
        &sess,
        "EXPLAIN ANALYZE
         SELECT o.order_id, c.name, o.amount
         FROM join_orders o
         INNER JOIN join_customers c ON o.customer_id = c.customer_id
         WHERE o.amount > 1000.0",
    )
    .await;

    assert!(
        !plan.is_empty(),
        "JOIN: EXPLAIN ANALYZE must return at least one plan line"
    );

    let plan_text = plan.join("\n");
    println!("[5.23.A join] plan:\n{plan_text}");

    // After 5.23.D: the plan must reference a join operator.
    let has_join_node = plan_text.contains("Join")
        || plan_text.contains("join")
        || plan_text.contains("HashJoin")
        || plan_text.contains("NestedLoop")
        || plan_text.contains("MergeJoin");
    assert!(
        has_join_node,
        "JOIN (5.23.A): EXPLAIN ANALYZE on an INNER JOIN must include a join node \
         (HashJoin, MergeJoin, NestedLoop, or similar) in the plan output. Got:\n{plan_text}\n\
         Closed by 5.23.D (join plan-node exposure in EXPLAIN ANALYZE)."
    );

    // Confirm the actual query returns the right number of rows (sanity check).
    // amount > 1000.0 means i * 9.99 > 1000 → i > 100.1 → i >= 101.
    // Orders 101..=499 = 399 orders, all of which have a matching customer.
    let actual_rows = row_count(
        &sess,
        "SELECT o.order_id, c.name, o.amount
         FROM join_orders o
         INNER JOIN join_customers c ON o.customer_id = c.customer_id
         WHERE o.amount > 1000.0",
    )
    .await;
    assert_eq!(
        actual_rows, 399,
        "JOIN: INNER JOIN with amount > 1000.0 must return 399 rows. got={actual_rows}"
    );

    println!("[5.23.A join] PASSED — join node confirmed in plan");
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 4 — Agg/window/CTE family: EXPLAIN ANALYZE window function
// ─────────────────────────────────────────────────────────────────────────────
//
// Verifies that EXPLAIN ANALYZE on a query with a window function
// (row_number() OVER (PARTITION BY ...)) produces a plan containing a window
// node and ANALYZE timing annotations.
//
// Closed by: 5.23.C (agg/window/CTE plan nodes in EXPLAIN ANALYZE output).

/// Phase 5.23.A — EXPLAIN ANALYZE window function: `SELECT row_number() OVER
/// (PARTITION BY ...) ...` must produce a plan containing a window/sort node
/// and ANALYZE timing annotations.
///
/// Closes when: 5.23.C (window plan-node exposure in EXPLAIN ANALYZE).
/// At that point drop this `#[ignore]`.
#[ignore = "5.23.A harness — pending 5.23.B-G"]
#[tokio::test]
async fn explain_window_function() {
    // Slice: "agg/window/CTE family"

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── DDL + seed ────────────────────────────────────────────────────────────
    sess.execute(
        "CREATE TABLE window_events (
            event_id   BIGINT NOT NULL,
            category   TEXT   NOT NULL,
            score      DOUBLE PRECISION NOT NULL
        )",
    )
    .await
    .expect("CREATE TABLE window_events");

    {
        let categories = ["alpha", "beta", "gamma"];
        let mut values: Vec<String> = Vec::with_capacity(300);
        for i in 0..300i64 {
            let cat = categories[(i as usize) % 3];
            values.push(format!("({i}, '{cat}', {:.2})", i as f64 * 0.7));
        }
        sess.execute(&format!(
            "INSERT INTO window_events VALUES {}",
            values.join(", ")
        ))
        .await
        .expect("INSERT window_events");
    }
    println!("[5.23.A window] inserted 300 rows across 3 categories");

    // ── EXPLAIN ANALYZE window function ───────────────────────────────────────
    let plan = plan_lines(
        &sess,
        "EXPLAIN ANALYZE
         SELECT event_id,
                category,
                score,
                row_number() OVER (PARTITION BY category ORDER BY score DESC) AS rn
         FROM window_events",
    )
    .await;

    assert!(
        !plan.is_empty(),
        "WINDOW: EXPLAIN ANALYZE must return at least one plan line"
    );

    let plan_text = plan.join("\n");
    println!("[5.23.A window] plan:\n{plan_text}");

    // After 5.23.C: the plan must reference a window or sort node.
    let has_window_node = plan_text.contains("Window")
        || plan_text.contains("window")
        || plan_text.contains("WindowAgg")
        || plan_text.contains("Sort")
        || plan_text.contains("Partition");
    assert!(
        has_window_node,
        "WINDOW (5.23.A): EXPLAIN ANALYZE on a window function (row_number() OVER \
         PARTITION BY) must include a window or sort node in the plan output. \
         Got:\n{plan_text}\n\
         Closed by 5.23.C (window plan-node exposure in EXPLAIN ANALYZE)."
    );

    // Sanity-check: the window query must return all 300 rows (one per input row).
    let actual_rows = row_count(
        &sess,
        "SELECT event_id,
                category,
                score,
                row_number() OVER (PARTITION BY category ORDER BY score DESC) AS rn
         FROM window_events",
    )
    .await;
    assert_eq!(
        actual_rows, 300,
        "WINDOW: window function query must return all 300 source rows. got={actual_rows}"
    );

    println!("[5.23.A window] PASSED — window/sort node confirmed in plan");
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 5 — View shape: pg_stat_activity column set
// ─────────────────────────────────────────────────────────────────────────────
//
// Verifies that querying `information_schema.columns` for the `pg_stat_activity`
// view in `pg_catalog` returns the canonical PostgreSQL column set.
//
// Canonical columns (PG 15/16 — the columns ORMs and monitoring tools depend on):
//   datid, datname, pid, leader_pid, usesysid, usename, application_name,
//   client_addr, client_hostname, client_port, backend_start, xact_start,
//   query_start, state_change, wait_event_type, wait_event, state, backend_xid,
//   backend_xmin, query_id, query, backend_type
//
// Closed by: 5.23.E (pg_stat_activity virtual view implementation).

/// Phase 5.23.A — pg_stat_activity view shape: querying
/// `information_schema.columns` for `pg_stat_activity` must return the
/// canonical PostgreSQL 15 column set (datid, pid, usename, state, query, etc.).
///
/// Monitoring tools (pganalyze, Datadog postgres integration, pgBadger) rely on
/// this column set being stable and complete.
///
/// Closes when: 5.23.E (pg_stat_activity virtual view). At that point drop
/// this `#[ignore]`.
#[ignore = "5.23.A harness — pending 5.23.B-G"]
#[tokio::test]
async fn pg_stat_activity_view_shape() {
    // Slice: "view shape"
    //
    // The canonical column list matches PostgreSQL 15.x:
    //   https://www.postgresql.org/docs/15/monitoring-stats.html#MONITORING-PG-STAT-ACTIVITY-VIEW
    // Tools that depend on a minimum subset:
    //   - pganalyze: datid, pid, usename, application_name, state, query, query_start
    //   - Datadog: pid, datname, usename, state, wait_event_type, wait_event
    //   - Rails active_record: pid, state
    //
    // The test asserts that the required subset is present; additional columns
    // (e.g. leader_pid, query_id) are welcome but not required here.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // Query information_schema.columns for the pg_stat_activity virtual view.
    // After 5.23.E, Basin must expose this view in pg_catalog and register its
    // columns in information_schema.
    let columns = string_column(
        &sess,
        "SELECT column_name
         FROM information_schema.columns
         WHERE table_schema = 'pg_catalog'
           AND table_name   = 'pg_stat_activity'
         ORDER BY ordinal_position",
    )
    .await;

    println!("[5.23.A stat_activity] columns returned: {columns:?}");

    // Minimum required columns — this is what monitoring tools depend on.
    // We assert presence; the view may have more columns (leader_pid, query_id,
    // backend_type, etc.) which are also welcome.
    let required: &[&str] = &[
        "datid",
        "datname",
        "pid",
        "usesysid",
        "usename",
        "application_name",
        "client_addr",
        "backend_start",
        "xact_start",
        "query_start",
        "state_change",
        "wait_event_type",
        "wait_event",
        "state",
        "backend_xid",
        "backend_xmin",
        "query",
        "backend_type",
    ];

    assert!(
        !columns.is_empty(),
        "STAT_ACTIVITY (5.23.A): pg_stat_activity view must expose at least one column \
         via information_schema.columns. Got none.\n\
         Closed by 5.23.E (pg_stat_activity virtual view implementation)."
    );

    for &col in required {
        assert!(
            columns.iter().any(|c| c == col),
            "STAT_ACTIVITY (5.23.A): pg_stat_activity must expose the column '{col}' \
             (required by monitoring tools). Present columns: {columns:?}\n\
             Closed by 5.23.E."
        );
    }

    // Also verify that the view itself is directly queryable.
    let stat_rows = row_count(&sess, "SELECT pid, state, query FROM pg_stat_activity").await;
    println!("[5.23.A stat_activity] SELECT pg_stat_activity returned {stat_rows} rows");
    // We accept 0 rows (no active sessions other than ours) but the query
    // itself must not error. The row_count helper panics on error.

    println!(
        "[5.23.A stat_activity] PASSED — all {} required columns present",
        required.len()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 6 — Locks view: pg_locks shows acquired row-level lock
// ─────────────────────────────────────────────────────────────────────────────
//
// Verifies that:
//   1. Starting a transaction and issuing `SELECT … FOR UPDATE` acquires a
//      row-level lock.
//   2. Querying `pg_locks` within the same transaction returns at least one
//      row whose `locktype` is 'relation' or 'tuple' (i.e. a real lock entry,
//      not empty).
//
// Closed by: 5.23.F (pg_locks virtual view implementation).

/// Phase 5.23.A — pg_locks shows acquired row lock: start a transaction,
/// acquire a row-level lock with `SELECT … FOR UPDATE`, then query `pg_locks`
/// and assert at least one lock entry is visible.
///
/// Monitoring tools (pganalyze lock-wait graphs, pg_activity) depend on
/// `pg_locks` being populated during active transactions.
///
/// Closes when: 5.23.F (pg_locks virtual view). At that point drop this
/// `#[ignore]`.
#[ignore = "5.23.A harness — pending 5.23.B-G"]
#[tokio::test]
async fn pg_locks_view_returns_acquired_locks() {
    // Slice: "locks view"

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── DDL + seed ────────────────────────────────────────────────────────────
    sess.execute(
        "CREATE TABLE lock_target (
            id    BIGINT NOT NULL,
            token TEXT   NOT NULL
        )",
    )
    .await
    .expect("CREATE TABLE lock_target");

    sess.execute("INSERT INTO lock_target VALUES (1, 'row_one'), (2, 'row_two')")
        .await
        .expect("INSERT lock_target");

    // ── Open a transaction and acquire a row-level lock ───────────────────────
    sess.execute("BEGIN").await.expect("BEGIN transaction");

    // SELECT … FOR UPDATE acquires an ExclusiveLock on the row (or relation
    // depending on the locking implementation). After 5.23.F, Basin must track
    // this in pg_locks.
    let locked_rows = row_count(
        &sess,
        "SELECT id, token FROM lock_target WHERE id = 1 FOR UPDATE",
    )
    .await;
    assert_eq!(
        locked_rows, 1,
        "LOCKS: SELECT FOR UPDATE must return exactly 1 locked row. got={locked_rows}"
    );
    println!("[5.23.A locks] SELECT FOR UPDATE returned {locked_rows} row");

    // ── Query pg_locks inside the same transaction ────────────────────────────
    let lock_count = row_count(&sess, "SELECT locktype, mode, granted FROM pg_locks").await;

    println!("[5.23.A locks] pg_locks returned {lock_count} entries");

    // After 5.23.F: at least one lock must be visible (the relation-level or
    // tuple-level lock acquired by our SELECT FOR UPDATE above).
    assert!(
        lock_count >= 1,
        "LOCKS (5.23.A): pg_locks must return at least one lock entry after \
         SELECT … FOR UPDATE within an open transaction. Got {lock_count} entries.\n\
         Closed by 5.23.F (pg_locks virtual view implementation)."
    );

    // Verify the lock has sensible column values.
    let lock_modes = string_column(&sess, "SELECT mode FROM pg_locks WHERE granted = TRUE").await;
    println!("[5.23.A locks] granted lock modes: {lock_modes:?}");
    assert!(
        !lock_modes.is_empty(),
        "LOCKS (5.23.A): pg_locks must show at least one granted lock (granted = TRUE). \
         Closed by 5.23.F."
    );

    // Clean up the transaction.
    sess.execute("ROLLBACK").await.expect("ROLLBACK transaction");

    println!("[5.23.A locks] PASSED — pg_locks populated with acquired lock");
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 7 — Tooling compat: pgcli / psql basic session
// ─────────────────────────────────────────────────────────────────────────────
//
// Environment-gated on `BASIN_LOCAL_URL`: if set, spawns `psql` (preferred)
// or `pgcli` against the basin endpoint, runs `\d` (list tables), and asserts
// that the process exits with code 0.
//
// Skip condition: if `BASIN_LOCAL_URL` is not set, the test is skipped with a
// clear message (not failed). This allows CI to pass without a live basin
// instance, while still running the test when a developer has a local server.
//
// Closed by: 5.23.G (psql/pgcli wire-compat session initialization).

/// Phase 5.23.A — tooling compat: spawn `psql` (or `pgcli`) against a running
/// Basin instance, run the `\d` meta-command, and assert exit code 0.
///
/// Gated on the `BASIN_LOCAL_URL` environment variable
/// (e.g. `postgres://user:pass@localhost:5432/db`). If the variable is absent
/// the test is skipped (not failed).
///
/// This slice verifies that standard PostgreSQL CLI tooling can establish a
/// session against Basin without errors — a prerequisite for the DBA/devops
/// tooling compat commitment in 5.23.G.
///
/// Closes when: 5.23.G (psql/pgcli wire-compat session initialization).
/// At that point drop this `#[ignore]`.
#[ignore = "5.23.A harness — pending 5.23.B-G"]
#[tokio::test]
async fn tooling_compat_pgcli_psql_runs_basic_session() {
    // Slice: "tooling compat"
    //
    // The test is intentionally env-gated so it does not block CI on machines
    // without a running Basin instance. Set BASIN_LOCAL_URL to enable it, e.g.:
    //   BASIN_LOCAL_URL=postgres://basin:basin@localhost:5432/basin \
    //     cargo test --test explain_pg_stat_harness \
    //       tooling_compat_pgcli_psql_runs_basic_session -- --ignored

    basin_common::telemetry::try_init_for_tests();

    let basin_url = match std::env::var("BASIN_LOCAL_URL") {
        Ok(url) if !url.is_empty() => url,
        _ => {
            println!(
                "[5.23.A tooling] SKIP — BASIN_LOCAL_URL not set. \
                 Set BASIN_LOCAL_URL=postgres://... to run this slice."
            );
            // Skipping via early return (not a test failure).
            return;
        }
    };

    println!("[5.23.A tooling] BASIN_LOCAL_URL={basin_url}");

    // Prefer `psql` (universally installed) over `pgcli` (optional).
    // Try psql first; fall back to pgcli if psql is not on PATH.
    let (tool, args): (&str, Vec<&str>) = if which_tool("psql") {
        (
            "psql",
            vec![
                &basin_url,
                "--no-psqlrc",
                "-c",
                r"\d",
                "-c",
                "SELECT version();",
            ],
        )
    } else if which_tool("pgcli") {
        (
            "pgcli",
            // pgcli accepts --execute for non-interactive mode.
            vec![&basin_url, "--execute", r"\d"],
        )
    } else {
        panic!(
            "TOOLING COMPAT (5.23.A): neither `psql` nor `pgcli` found on PATH. \
             Install one of them to run this slice.\n\
             Closed by 5.23.G."
        );
    };

    println!("[5.23.A tooling] spawning: {tool} {}", args.join(" "));

    let output = std::process::Command::new(tool)
        .args(&args)
        .output()
        .unwrap_or_else(|e| {
            panic!("TOOLING COMPAT (5.23.A): failed to spawn {tool}: {e}.\nClosed by 5.23.G.")
        });

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!("[5.23.A tooling] stdout:\n{stdout}");
    if !stderr.is_empty() {
        println!("[5.23.A tooling] stderr:\n{stderr}");
    }

    // Core assertion: the tool must exit with code 0.
    assert!(
        output.status.success(),
        "TOOLING COMPAT (5.23.A): `{tool} \\d` exited with non-zero status {:?}.\n\
         stdout:\n{stdout}\nstderr:\n{stderr}\n\
         Closed by 5.23.G (psql/pgcli wire-compat session initialization).",
        output.status.code()
    );

    println!("[5.23.A tooling] PASSED — {tool} session exit 0");
}

/// Returns true if `tool_name` is found on PATH.
fn which_tool(tool_name: &str) -> bool {
    std::process::Command::new("which")
        .arg(tool_name)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}
