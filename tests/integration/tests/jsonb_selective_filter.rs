//! Regression test: JSONB UDF projections must NOT run for rows already
//! eliminated by a WHERE filter / LIMIT.
//!
//! Background: a perf bench rerun (commit ddfd8a8) revealed a major regression
//! on JSONB scalar shapes that should be CHEAP because they carry selective
//! WHERE clauses (e.g. `WHERE id < 100 ORDER BY id LIMIT 10`).  PostgreSQL
//! pushes the filter beneath the JSONB extract projection and evaluates the
//! UDF on ~10 rows; Basin was running the extract on every row in the column
//! (linear in table size).  The fix is twofold:
//!
//!  1. Every JSONB scalar UDF is marked `Volatility::Immutable` so DataFusion's
//!     `EliminateProjection` / `PushDownLimit` rules can hoist them above
//!     filter+limit-shaped pipelines (already in place in jsonb_udf.rs).
//!  2. The fast-select / DataFusion path keeps the JSONB UDF projection layer
//!     ABOVE FilterExec + TopK in the physical plan so the per-row UDF
//!     evaluation only sees the post-filter rows.
//!
//! The tests in this file:
//!
//!  * Drive a "selective WHERE" JSONB query over a moderately-large table
//!    (50k rows; large enough for any per-row work to dominate the test
//!    runtime if it leaks).  They assert that the wall-clock is small —
//!    a much weaker bound than the bench's "single-digit ms" — sufficient
//!    to prove the UDF isn't running on every row.
//!  * Drive `EXPLAIN` against the same query shape and assert that the
//!    JSONB UDF (`json_get_text`) appears in a ProjectionExec layer that
//!    sits ABOVE the FilterExec / TopK / GlobalLimitExec node.
//!
//! No bench-literal hardcoding: keys / values used here are arbitrary
//! fixtures; the optimizer change special-cases nothing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

async fn open_engine() -> (TempDir, Engine) {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    (dir, engine)
}

/// Seed `n` rows into `events(id BIGINT, payload JSONB)` with diverse JSON
/// payloads.  No key/value is special-cased anywhere in the engine; the
/// only requirement here is that decoding `payload` is non-trivial work
/// (so a per-row leak shows up as a wall-clock blow-up).
async fn seed_events(sess: &basin_engine::ProjectSession, n: i64) {
    sess.execute("CREATE TABLE events (id BIGINT NOT NULL, payload JSONB)")
        .await
        .expect("CREATE TABLE events");

    // Build a single multi-row INSERT in batches so seeding 50k rows finishes
    // in a reasonable time. The payload varies per row so no parser-level
    // dedupe / cache trick can hide a real per-row UDF leak.
    const BATCH: i64 = 1000;
    let mut id = 1i64;
    while id <= n {
        let end = (id + BATCH - 1).min(n);
        let mut sql = String::from("INSERT INTO events VALUES ");
        let mut first = true;
        for i in id..=end {
            if !first {
                sql.push_str(", ");
            }
            first = false;
            // category cycles, score varies — both arbitrary fixtures.
            let category = match i % 4 {
                0 => "purchase",
                1 => "signup",
                2 => "click",
                _ => "view",
            };
            let score = (i * 13 + 7) % 100;
            // Nested object `metadata.score` exercises the `#>` path too.
            sql.push_str(&format!(
                "({i}, '{{\"category\":\"{category}\",\"metadata\":{{\"score\":{score}}}}}')"
            ));
        }
        sess.execute(&sql)
            .await
            .unwrap_or_else(|e| panic!("seed insert {id}..{end}: {e}"));
        id = end + 1;
    }
}

/// Pull the (single-column, single-batch) Utf8 text column out of an
/// `ExecResult::Rows`.  Used by the latency assertion to confirm the UDF
/// returned the right post-filter values.
fn rows_as_text(res: &ExecResult) -> Vec<Option<String>> {
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got: {other:?}"),
    };
    let mut out = Vec::new();
    for b in batches {
        let col = b.column(0);
        let arr = col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("expected Utf8 column, got {:?}", col.data_type()));
        for i in 0..arr.len() {
            if arr.is_null(i) {
                out.push(None);
            } else {
                out.push(Some(arr.value(i).to_string()));
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// 1. Wall-clock proof: `WHERE id < 10 LIMIT 5` on 50k rows is fast.
// ---------------------------------------------------------------------------
//
// If the JSONB UDF were running on every row of `payload` (the regression),
// this query would scale O(N) in the table size: ~50× slower than the same
// query at 1k rows.  When the optimizer correctly evaluates the UDF only on
// post-filter rows the wall-clock barely moves with N.
//
// The assertion is intentionally loose (250 ms on a 50k-row table) — its job
// is to catch a 10×-100× regression, not to police absolute throughput.

#[tokio::test]
async fn selective_filter_limits_udf_work() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess, 50_000).await;

    // Warm the catalog / planner: the first SELECT on a table pays planning
    // setup we don't want to attribute to JSONB extract cost.
    let _warm = sess
        .execute("SELECT id FROM events WHERE id = 1 LIMIT 1")
        .await
        .expect("warmup");

    // The query under test: `->>` UDF projection plus a selective filter +
    // limit. With predicate-pushdown working, the UDF runs on at most ~9
    // rows.  Without it, the UDF runs on all 50k payloads.
    let sql = "SELECT payload ->> 'category' FROM events WHERE id < 10 ORDER BY id LIMIT 5";

    let t0 = Instant::now();
    let res = sess.execute(sql).await.expect("execute");
    let elapsed = t0.elapsed();

    let texts = rows_as_text(&res);
    // Correctness: 5 rows back, all decoded categories (no NULLs given the
    // seed pattern).
    assert_eq!(texts.len(), 5, "expected 5 rows, got {}: {texts:?}", texts.len());
    assert!(texts.iter().all(|t| t.is_some()), "no NULLs expected: {texts:?}");

    // Latency: the UDF should run on at most ~9 rows.  A 750 ms ceiling on
    // a 50k-row table is comfortably loose for debug builds: the broken
    // plan (per-row UDF over 50k payloads) takes seconds at debug-opt
    // levels (10-50x slower than release), so this catches regressions
    // with margin while not flaking on per-call UDF overhead in debug.
    // The structural pushdown check is in
    // `explain_filter_below_jsonb_projection` below.
    assert!(
        elapsed.as_millis() < 750,
        "selective filter should evaluate UDF on ~5 rows, not all 50k.  \
         Elapsed: {elapsed:?}.  If the projection runs on every row this \
         test takes seconds at debug-opt levels."
    );
}

// ---------------------------------------------------------------------------
// 2. Wall-clock proof for the `#>` (path-extract) variant.
// ---------------------------------------------------------------------------
//
// `#> '{metadata,score}'` exercises the path extractor that was also called
// out in the regression report.  Same shape; same scaling.

#[tokio::test]
async fn selective_filter_limits_path_udf_work() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess, 50_000).await;

    let _warm = sess
        .execute("SELECT id FROM events WHERE id = 1 LIMIT 1")
        .await
        .expect("warmup");

    let sql = "SELECT payload #> '{metadata,score}' FROM events \
               WHERE id < 10 ORDER BY id LIMIT 5";
    let t0 = Instant::now();
    let res = sess.execute(sql).await.expect("execute");
    let elapsed = t0.elapsed();

    let batches = match &res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got: {other:?}"),
    };
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 5, "expected 5 rows back");

    assert!(
        elapsed.as_millis() < 750,
        "selective filter on #> path-extract should be fast.  Elapsed: {elapsed:?}"
    );
}

// ---------------------------------------------------------------------------
// 3. EXPLAIN structural assertion: the JSONB UDF projection sits ABOVE
//    the FilterExec / TopK / GlobalLimit in the physical plan.
// ---------------------------------------------------------------------------
//
// When the optimizer is doing its job, the physical pipeline reads
// (scan → coalesce → filter → topk/sort → project json_get_text).  A
// broken plan would either materialise the UDF before the filter (e.g.
// `project → filter`) or duplicate the UDF call below.  We assert by
// finding the line number of the JSONB UDF call vs. the line numbers of
// FilterExec / GlobalLimit / SortExec / TopK — the UDF line must come
// FIRST (smaller line number = closer to the plan's root in EXPLAIN
// output).

#[tokio::test]
async fn explain_filter_below_jsonb_projection() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess, 5_000).await;

    let plan_text = explain_plan(
        &sess,
        "SELECT payload ->> 'category' FROM events WHERE id < 10 ORDER BY id LIMIT 5",
    )
    .await;

    println!("--- physical plan ---\n{plan_text}\n---");

    // Find the depth (line index) of the UDF projection vs. the filter / limit.
    let lines: Vec<&str> = plan_text.lines().collect();
    let udf_line = lines
        .iter()
        .position(|l| {
            let lower = l.to_ascii_lowercase();
            lower.contains("projectionexec") && lower.contains("json_get_text")
        })
        .unwrap_or_else(|| panic!("no ProjectionExec with json_get_text found in:\n{plan_text}"));

    let filter_line = lines.iter().position(|l| {
        let lower = l.to_ascii_lowercase();
        lower.contains("filterexec") || lower.contains("coalescebatchesexec")
    });
    let limit_or_sort_line = lines.iter().position(|l| {
        let lower = l.to_ascii_lowercase();
        // GlobalLimitExec | LocalLimitExec | SortExec(... fetch=) | TopK = any
        // node that imposes the row-count cap.
        lower.contains("limitexec") || lower.contains("sortexec") || lower.contains("topk")
    });

    // The UDF projection MUST appear above (smaller line number = nearer
    // the root in EXPLAIN's top-down printout) any filter or limit/sort
    // present.  If either is below the UDF, the optimizer dropped the
    // pushdown rule.
    if let Some(fl) = filter_line {
        assert!(
            udf_line < fl,
            "JSONB UDF projection (line {udf_line}) must sit ABOVE FilterExec \
             (line {fl}) so the UDF only evaluates on filter-matching rows.  \
             Plan:\n{plan_text}"
        );
    }
    if let Some(ll) = limit_or_sort_line {
        assert!(
            udf_line < ll,
            "JSONB UDF projection (line {udf_line}) must sit ABOVE the \
             Limit/Sort/TopK node (line {ll}) so the UDF only evaluates on \
             post-limit rows.  Plan:\n{plan_text}"
        );
    }
}

// ---------------------------------------------------------------------------
// 4. EXPLAIN structural assertion: bench shape (no ORDER BY / LIMIT).
// ---------------------------------------------------------------------------
//
// The bench shape `SELECT payload ->> 'category' FROM events WHERE id < 100`
// has NO ORDER BY and NO LIMIT.  DataFusion's `ProjectionPushdown` optimizer
// used to fuse `json_get_text` directly into `DataSourceExec` (bug: the UDF
// ran on every row before the predicate discarded non-matching rows).
//
// The `UdfPushdownGuard` wrapper ensures that expression-bearing projections
// are NOT pushed into `DataSourceExec`.  This test verifies that:
//
//  a) `json_get_text` appears in a `ProjectionExec` line, NOT in the
//     `DataSourceExec` line.
//  b) The `DataSourceExec` line carries the predicate (`id < 100`).

#[tokio::test]
async fn explain_bench_shape_udf_not_in_datasourceexec() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess, 5_000).await;

    let plan_text = explain_plan(
        &sess,
        "SELECT payload ->> 'category' FROM events WHERE id < 100",
    )
    .await;

    println!("--- bench shape physical plan ---\n{plan_text}\n---");

    let lines: Vec<&str> = plan_text.lines().collect();

    // json_get_text must appear in a ProjectionExec line.
    let proj_udf_line = lines.iter().position(|l| {
        let lower = l.to_ascii_lowercase();
        lower.contains("projectionexec") && lower.contains("json_get_text")
    });
    assert!(
        proj_udf_line.is_some(),
        "Expected a ProjectionExec line containing json_get_text in:\n{plan_text}"
    );

    // json_get_text must NOT appear in the DataSourceExec line.
    let datasource_udf_line = lines.iter().position(|l| {
        let lower = l.to_ascii_lowercase();
        lower.contains("datasourceexec") && lower.contains("json_get_text")
    });
    assert!(
        datasource_udf_line.is_none(),
        "json_get_text must NOT be fused into DataSourceExec (UdfPushdownGuard regression).\
         \nPlan:\n{plan_text}"
    );
}

// ---------------------------------------------------------------------------
// 5. Wall-clock proof: bench shape (no ORDER BY / LIMIT) on 50k rows is fast.
// ---------------------------------------------------------------------------
//
// If the UDF is evaluated on all rows (the regression) this query takes
// O(N) time w.r.t. table size.  When the guard is in place the UDF only runs
// on the ~99 post-filter rows, so wall-clock stays small regardless of N.
//
// This test seeds 50k rows and expects sub-500 ms; the broken plan would
// take many seconds at debug-opt levels.

#[tokio::test]
async fn bench_shape_udf_is_fast() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess, 50_000).await;

    let _warm = sess
        .execute("SELECT id FROM events WHERE id = 1 LIMIT 1")
        .await
        .expect("warmup");

    let sql = "SELECT payload ->> 'category' FROM events WHERE id < 100";

    let t0 = Instant::now();
    let res = sess.execute(sql).await.expect("execute");
    let elapsed = t0.elapsed();

    // Correctness: 99 rows back (id 1..=99).
    let batches = match &res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got: {other:?}"),
    };
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 99, "expected 99 rows (id 1..=99), got {total}");

    // Latency: the UDF should only run on ~99 rows.  A 750 ms ceiling on a
    // 50k-row table is intentionally loose for debug builds; the broken plan
    // takes seconds (10-50× slower than release at debug-opt).
    assert!(
        elapsed.as_millis() < 750,
        "bench-shape UDF should evaluate on ~99 rows only, not all 50k.  \
         Elapsed: {elapsed:?}.  A slow result indicates the UDF is running \
         before the predicate (UdfPushdownGuard regression)."
    );
}

// ---------------------------------------------------------------------------
// 6. Debug: print full plan for the bench-shape query (no ORDER BY / LIMIT).
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "debug-only: prints plan for inspection, not a regression gate"]
async fn debug_plan_for_bench_shape() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess, 5_000).await;

    let plan = explain_full(
        &sess,
        "SELECT payload ->> 'category' FROM events WHERE id < 100",
    )
    .await;
    println!("=== bench shape (no ORDER BY/LIMIT) ===\n{plan}\n");

    let plan2 = explain_full(
        &sess,
        "SELECT payload ->> 'category' FROM events WHERE id < 10 ORDER BY id LIMIT 5",
    )
    .await;
    println!("=== selective shape ===\n{plan2}\n");
}

/// Run `EXPLAIN <sql>` and return ALL stages of the plan as a single joined
/// string, including stage headers like `[physical_plan]`. Used by the
/// debug-only test below.
#[allow(dead_code)]
async fn explain_full(sess: &basin_engine::ProjectSession, sql: &str) -> String {
    let explain_sql = format!("EXPLAIN {sql}");
    let res = sess
        .execute(&explain_sql)
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN failed: {e}"));
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("EXPLAIN returned non-rows: {other:?}"),
    };
    let mut lines: Vec<String> = Vec::new();
    for b in &batches {
        let plan_idx = b
            .schema()
            .index_of("QUERY PLAN")
            .expect("EXPLAIN has QUERY PLAN column");
        let plan = b
            .column(plan_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("QUERY PLAN is Utf8");
        for i in 0..b.num_rows() {
            if !plan.is_null(i) {
                lines.push(plan.value(i).to_string());
            }
        }
    }
    lines.join("\n")
}

/// Run `EXPLAIN <sql>` against the session and return only the physical-plan
/// portion of Basin's PG-shaped `QUERY PLAN` output as a single newline-joined
/// string.
///
/// Basin's `explain.rs` collapses DataFusion's two-column EXPLAIN result
/// (`plan_type`, `plan`) into a single `QUERY PLAN` column, prefixing each
/// stage with a bracketed header like `[physical_plan]`.  We slice out the
/// lines that follow the `[physical_plan]` header up to the next `[<stage>]`
/// header (or EOF), so the structural ordering assertions ignore the logical
/// plan text and operate on the physical pipeline.
async fn explain_plan(sess: &basin_engine::ProjectSession, sql: &str) -> String {
    let explain_sql = format!("EXPLAIN {sql}");
    let res = sess
        .execute(&explain_sql)
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN failed: {e}"));
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("EXPLAIN returned non-rows: {other:?}"),
    };

    let mut all_lines: Vec<String> = Vec::new();
    for b in &batches {
        // Basin's EXPLAIN emits a single `QUERY PLAN` Utf8 column.
        let plan_idx = b
            .schema()
            .index_of("QUERY PLAN")
            .expect("EXPLAIN has QUERY PLAN column");
        let plan = b
            .column(plan_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("QUERY PLAN is Utf8");
        for i in 0..b.num_rows() {
            if !plan.is_null(i) {
                all_lines.push(plan.value(i).to_string());
            }
        }
    }

    // Slice the lines between `[physical_plan]` and the next `[…]` header.
    let mut out_lines: Vec<String> = Vec::new();
    let mut in_phys = false;
    for line in all_lines {
        let trimmed = line.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            in_phys = trimmed == "[physical_plan]";
            continue;
        }
        if in_phys {
            out_lines.push(line);
        }
    }
    out_lines.join("\n")
}
