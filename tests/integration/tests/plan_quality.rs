//! Plan-quality regression tests vs PostgreSQL (Phase 5.x perf gaps).
//!
//! Four query shapes were measured slower than PostgreSQL at 10k–100k scale.
//! Each is a query-planning / logical→physical lowering question, so we assert
//! on the EXPLAIN plan *shape* (not just correctness) to lock in the fix and
//! catch regressions:
//!
//!   1. `ORDER BY … NULLS LAST LIMIT k` must lower to a bounded TopK
//!      (`SortExec` carrying `fetch=k`), not a full sort followed by a
//!      separate limit.
//!   2. `EXISTS (…)` in WHERE must lower to a `LeftSemi` hash join (semijoin),
//!      not a correlated subquery re-evaluated per outer row.
//!   3. `LIKE 'prefix%'` must retain its filter so prefix-range pushdown
//!      survives planning even at mid-cardinality (100k) row estimates.
//!   4. Window `LAG() OVER (PARTITION BY … ORDER BY …)` correctness.
//!
//! The probe test `dump_all_plans` prints every plan with `--nocapture` for
//! diagnosis; the remaining tests assert shape + correctness.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::Array;
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::cache_defaults;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: cache_defaults::default_test_disk_cache(),
        page_cache: cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Extract the QUERY PLAN column from an EXPLAIN result as one joined string.
fn plan_text(result: ExecResult) -> String {
    let ExecResult::Rows { batches, .. } = result else {
        panic!("EXPLAIN returned Empty — expected Rows with a QUERY PLAN column");
    };
    let mut out = Vec::new();
    for batch in &batches {
        // EXPLAIN output: column 0 is plan_type, the *last* column is the plan
        // body. Concatenate every Utf8 column so we capture the full plan text
        // regardless of which column DataFusion put it in.
        for c in 0..batch.num_columns() {
            if let Some(arr) = batch
                .column(c)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
            {
                for i in 0..arr.len() {
                    if arr.is_valid(i) {
                        out.push(arr.value(i).to_string());
                    }
                }
            }
        }
    }
    out.join("\n")
}

async fn explain(sess: &basin_engine::ProjectSession, sql: &str) -> String {
    let res = sess
        .execute(&format!("EXPLAIN {sql}"))
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN {sql} failed: {e}"));
    plan_text(res)
}

/// Collect the named i64 column across all batches (nulls → None).
fn collect_i64(res: ExecResult, col: &str) -> Vec<Option<i64>> {
    let ExecResult::Rows { batches, .. } = res else {
        panic!("expected Rows result");
    };
    let mut out = Vec::new();
    for b in &batches {
        let arr = b
            .column_by_name(col)
            .unwrap_or_else(|| panic!("column '{col}' not found"))
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap_or_else(|| panic!("column '{col}' is not Int64Array"));
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            });
        }
    }
    out
}

fn row_count(res: &ExecResult) -> usize {
    let ExecResult::Rows { batches, .. } = res else {
        panic!("expected Rows");
    };
    batches.iter().map(|b| b.num_rows()).sum()
}

/// A `users` table with `n` rows: id 1..=n, some NULL `score`s, a `status`
/// text column with a known fraction of 'pending%' prefixes, and an `org_id`
/// for partition/semijoin shapes. Returns the open session.
async fn seed_users(engine: &Engine, project: ProjectId, n: i64) -> basin_engine::ProjectSession {
    let sess = engine.open_session(project).await.unwrap();
    sess.execute(
        "CREATE TABLE users (\
            id BIGINT NOT NULL, \
            org_id BIGINT NOT NULL, \
            score BIGINT, \
            status TEXT NOT NULL)",
    )
    .await
    .unwrap();
    sess.execute("CREATE TABLE orgs (org_id BIGINT NOT NULL, active BIGINT NOT NULL)")
        .await
        .unwrap();

    // Insert in batches to keep the VALUES list small.
    let mut buf = String::new();
    let mut first = true;
    for id in 1..=n {
        if first {
            buf.push_str("INSERT INTO users VALUES ");
            first = false;
        } else {
            buf.push(',');
        }
        // Every 7th row has a NULL score so NULLS LAST ordering is observable.
        let score = if id % 7 == 0 {
            "NULL".to_string()
        } else {
            (id % 1000).to_string()
        };
        // ~1% of rows are 'pending', the rest 'active'/'closed'.
        let status = match id % 100 {
            0 => "pending_review",
            1 => "pending_payment",
            x if x < 50 => "active",
            _ => "closed",
        };
        let org = id % 50;
        buf.push_str(&format!("({id},{org},{score},'{status}')"));
        if id % 500 == 0 {
            sess.execute(&buf).await.unwrap();
            buf.clear();
            first = true;
        }
    }
    if !first {
        sess.execute(&buf).await.unwrap();
    }
    // orgs: half active.
    let mut obuf = String::from("INSERT INTO orgs VALUES ");
    for o in 0..50 {
        if o > 0 {
            obuf.push(',');
        }
        obuf.push_str(&format!("({o},{})", o % 2));
    }
    sess.execute(&obuf).await.unwrap();
    sess
}

// ---------------------------------------------------------------------------
// Diagnostic probe — prints every plan. Run with --nocapture.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dump_all_plans() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let project = ProjectId::new();
    let sess = seed_users(&engine, project, 2000).await;

    let queries = [
        (
            "NULLS LAST + LIMIT",
            "SELECT id, score FROM users ORDER BY score DESC NULLS LAST LIMIT 10",
        ),
        (
            "EXISTS in WHERE",
            "SELECT u.id FROM users u WHERE EXISTS \
             (SELECT 1 FROM orgs o WHERE o.org_id = u.org_id AND o.active = 1) LIMIT 20",
        ),
        (
            "Window LAG OVER PARTITION",
            "SELECT id, org_id, LAG(score) OVER (PARTITION BY org_id ORDER BY id) AS prev \
             FROM users LIMIT 20",
        ),
        (
            "LIKE prefix",
            "SELECT id, status FROM users WHERE status LIKE 'pending%' LIMIT 20",
        ),
    ];
    for (label, q) in queries {
        let plan = explain(&sess, q).await;
        println!("\n===== {label} =====\n{q}\n-----\n{plan}\n");
    }
}

// ---------------------------------------------------------------------------
// Gap 1: ORDER BY … NULLS LAST LIMIT k  →  bounded TopK (not a full sort).
//
// Diagnosis: ALREADY CORRECT. DataFusion's `LogicalPlanBuilder` carries the
// `fetch` onto the `Sort` node, and `EnforceSorting` lowers it to
// `SortExec: TopK(fetch=k)`. There is nothing for Basin's executor lowering to
// fix — the TopK is already present. We lock that in here so a future change
// to the optimizer-rule list / SessionConfig that accidentally splits the sort
// and the limit (producing a full SortExec + GlobalLimitExec) is caught.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn nulls_last_limit_lowers_to_topk() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let project = ProjectId::new();
    let sess = seed_users(&engine, project, 2000).await;

    let q = "SELECT id, score FROM users ORDER BY score DESC NULLS LAST LIMIT 10";
    let plan = explain(&sess, q).await;

    // The physical sort must be a bounded TopK carrying the fetch, NOT a full
    // sort that materialises every row before a separate limit.
    assert!(
        plan.contains("TopK(fetch=10)"),
        "ORDER BY NULLS LAST LIMIT 10 must lower to a bounded TopK; plan was:\n{plan}"
    );
    assert!(
        plan.contains("NULLS LAST"),
        "NULLS LAST ordering must be preserved in the physical sort; plan was:\n{plan}"
    );
    // Guard against the regression shape: a full SortExec (no fetch) feeding a
    // GlobalLimitExec. If TopK is present this cannot also be present.
    assert!(
        !plan.contains("GlobalLimitExec"),
        "expected fused TopK, not a full sort + GlobalLimitExec; plan was:\n{plan}"
    );

    // Correctness: scores descending, the 10 highest non-null scores, NULLs
    // sorted last (so they never appear in the top-10 of a 2000-row table that
    // has plenty of non-null high scores).
    let res = sess.execute(q).await.unwrap();
    let scores = collect_i64(res, "score");
    assert_eq!(scores.len(), 10, "LIMIT 10 must return exactly 10 rows");
    assert!(
        scores.iter().all(|s| s.is_some()),
        "NULLS LAST means no NULL score reaches the top-10; got {scores:?}"
    );
    let vals: Vec<i64> = scores.into_iter().map(Option::unwrap).collect();
    let mut sorted = vals.clone();
    sorted.sort_by(|a, b| b.cmp(a));
    assert_eq!(vals, sorted, "results must be in DESC score order");
    assert_eq!(vals[0], 999, "highest score in [0,999] range must be 999");
}

// ---------------------------------------------------------------------------
// Gap 2: EXISTS (…) in WHERE  →  LeftSemi join (semijoin), not a per-row
// correlated subquery.
//
// Diagnosis: ALREADY CORRECT. DataFusion's `decorrelate_predicate_subquery`
// optimizer rule turns the correlated EXISTS into a `LeftSemi Join`, lowered
// to `HashJoinExec join_type=LeftSemi`. The outer table is scanned once; the
// subquery is NOT re-evaluated per outer row. We assert the semijoin shape.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exists_lowers_to_semijoin() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let project = ProjectId::new();
    let sess = seed_users(&engine, project, 2000).await;

    let q = "SELECT u.id FROM users u WHERE EXISTS \
             (SELECT 1 FROM orgs o WHERE o.org_id = u.org_id AND o.active = 1)";
    let plan = explain(&sess, q).await;

    // Logical plan must contain a LeftSemi join; physical plan a LeftSemi
    // HashJoinExec. The absence of a per-row subquery is implied: a correlated
    // subquery would surface as a `Subquery`/`__correlated_sq` re-evaluation
    // node rather than a join.
    assert!(
        plan.contains("LeftSemi"),
        "EXISTS must decorrelate to a LeftSemi (semi)join; plan was:\n{plan}"
    );
    assert!(
        plan.contains("HashJoinExec") && plan.contains("join_type=LeftSemi"),
        "physical plan must be a LeftSemi HashJoinExec, not a nested-loop \
         correlated subquery; plan was:\n{plan}"
    );

    // Correctness: only users whose org is active (org_id % 2 == 1) survive.
    let res = sess.execute(q).await.unwrap();
    let ids = collect_i64(res, "id");
    assert!(!ids.is_empty(), "some users belong to active orgs");
    for id in ids.into_iter().flatten() {
        // org_id = id % 50; active orgs are those with org_id % 2 == 1.
        let org = id % 50;
        assert_eq!(
            org % 2,
            1,
            "user id={id} (org_id={org}) should only appear if its org is active"
        );
    }
}

// ---------------------------------------------------------------------------
// Gap 4: Window LAG() OVER (PARTITION BY … ORDER BY …) — correctness + the
// "no per-partition re-sort" plan property.
//
// Diagnosis: DF-INTERNAL for the perf delta. The physical plan is a single
// `SortExec` on [partition_cols, order_cols] feeding a `BoundedWindowAggExec
// mode=[Sorted]`. There is exactly ONE sort and the window operator consumes
// the already-sorted stream (Sorted mode) rather than re-sorting per partition.
// That is the optimal DataFusion window plan; the measured 5.9x is the cost of
// the sort + windowing inside DataFusion, not a Basin lowering bug. We assert
// the single-sort/Sorted-mode shape plus full correctness.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn window_lag_correctness_and_single_sort() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let project = ProjectId::new();
    let sess = seed_users(&engine, project, 300).await;

    let q = "SELECT id, org_id, LAG(score) OVER (PARTITION BY org_id ORDER BY id) AS prev \
             FROM users";
    let plan = explain(&sess, q).await;

    // Window must run in Sorted mode (consumes pre-sorted input) — not a
    // per-partition re-sort.
    assert!(
        plan.contains("BoundedWindowAggExec") && plan.contains("mode=[Sorted]"),
        "LAG window must use Sorted mode over pre-sorted input; plan was:\n{plan}"
    );
    // Exactly one SortExec in the physical plan (the single sort the window
    // reuses). More than one would indicate a redundant re-sort.
    let phys = plan
        .split("[physical_plan]")
        .nth(1)
        .unwrap_or(&plan);
    let sort_count = phys.matches("SortExec").count();
    assert_eq!(
        sort_count, 1,
        "window plan must contain exactly one SortExec (no per-partition \
         re-sort); found {sort_count} in:\n{plan}"
    );

    // Correctness: within each org partition ordered by id, prev = score of the
    // previous row; the first row of each partition has prev = NULL.
    let res = sess.execute(q).await.unwrap();
    let ExecResult::Rows { batches, .. } = res else {
        panic!("expected Rows");
    };
    // Reconstruct (org_id, id, prev) triples and verify against a recomputed
    // LAG over the same data definition (score = id % 1000, NULL every 7th).
    let mut rows: Vec<(i64, i64, Option<i64>)> = Vec::new();
    for b in &batches {
        let ids = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        let orgs = b
            .column_by_name("org_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        let prev = b
            .column_by_name("prev")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            rows.push((
                orgs.value(i),
                ids.value(i),
                if prev.is_null(i) {
                    None
                } else {
                    Some(prev.value(i))
                },
            ));
        }
    }
    assert_eq!(rows.len(), 300, "every input row produces one output row");

    // score(id) per the seed definition.
    let score_of = |id: i64| -> Option<i64> {
        if id % 7 == 0 {
            None
        } else {
            Some(id % 1000)
        }
    };
    // Group by org, sort by id, recompute LAG, compare.
    use std::collections::BTreeMap;
    let mut by_org: BTreeMap<i64, Vec<(i64, Option<i64>)>> = BTreeMap::new();
    for (org, id, prev) in rows {
        by_org.entry(org).or_default().push((id, prev));
    }
    for (org, mut part) in by_org {
        part.sort_by_key(|(id, _)| *id);
        let mut expected_prev: Option<i64> = None;
        for (id, got_prev) in part {
            assert_eq!(
                got_prev, expected_prev,
                "org={org} id={id}: LAG(score) mismatch (expected {expected_prev:?}, got {got_prev:?})"
            );
            expected_prev = score_of(id);
        }
    }
}

// ---------------------------------------------------------------------------
// Gap 3: LIKE 'prefix%' — predicate AND limit must survive into the scan so
// the storage layer can apply prefix-range pruning, even at mid cardinality.
//
// Diagnosis: NOT a Basin executor lowering bug. The logical plan keeps the
// `Filter: status LIKE 'pending%'` with `partial_filters` advertised on the
// TableScan, and the physical plan pushes BOTH the `predicate: status LIKE
// pending%` and the `limit=N` down onto the `DataSourceExec`. The plan shape is
// scale-invariant (single-table filter+limit), so the 100k-scale spike is a
// storage/Vortex pruning cost-model effect (min/max prefix stats), not visible
// at — nor fixable from — the executor's logical→physical lowering. We assert
// the predicate + limit pushdown survives at the larger (denser) row count.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn like_prefix_pushes_predicate_and_limit() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let project = ProjectId::new();
    // 12k rows: enough that DataFusion's selectivity estimate is well past the
    // tiny-table regime where it might inline everything, exercising the same
    // planner cost path the 100k benchmark hits.
    let sess = seed_users(&engine, project, 12_000).await;

    let q = "SELECT id, status FROM users WHERE status LIKE 'pending%' LIMIT 20";
    let plan = explain(&sess, q).await;

    // Predicate must be pushed onto the scan (prefix-range pruning happens
    // there), not stranded in a separate FilterExec above the scan.
    assert!(
        plan.contains("predicate: status") && plan.to_lowercase().contains("like pending%"),
        "LIKE 'pending%' predicate must be pushed onto the DataSourceExec; plan was:\n{plan}"
    );
    // The fetch must also reach the scan so the reader can stop early.
    assert!(
        plan.contains("limit=20"),
        "LIMIT 20 must be pushed onto the scan; plan was:\n{plan}"
    );

    // Correctness: every returned status starts with 'pending'.
    let res = sess.execute(q).await.unwrap();
    assert_eq!(row_count(&res), 20, "LIMIT 20 with ~1% selectivity over 12k rows");
    let ExecResult::Rows { batches, .. } = res else {
        panic!("expected Rows");
    };
    for b in &batches {
        let status = b
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        for i in 0..status.len() {
            assert!(
                status.value(i).starts_with("pending"),
                "LIKE 'pending%' returned non-matching status {:?}",
                status.value(i)
            );
        }
    }
}
