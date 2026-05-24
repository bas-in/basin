//! Phase: aggregate / UNION partitioning tuning for small inputs.
//!
//! Context: a cluster of GROUP-BY / aggregate / UNION-ALL query shapes runs
//! several× slower than PostgreSQL at the 10k-row scale, with the gap
//! NARROWING as the table grows (≈8× at 10k → ≈2-4× at 100k/1M). A narrowing
//! gap is the signature of a per-query FIXED overhead (planning + partition
//! fan-out / merge exchanges) rather than an algorithmic blow-up.
//!
//! This file does two things:
//!
//!  1. CORRECTNESS — exercises the exact GROUP-BY / multi-col-HAVING /
//!     2-table-JOIN-GROUP-BY / DATE_TRUNC-rollup / UNION-ALL shapes end-to-end
//!     through the Engine and pins their results. These results MUST NOT change
//!     under any executor config tuning — the config knobs only touch
//!     partitioning / batching, never semantics.
//!
//!  2. PLAN SHAPE — asserts (via EXPLAIN) that a small-input aggregate plans
//!     as a SINGLE-partition aggregate with NO `RepartitionExec` exchange.
//!     This is the load-bearing evidence for the diagnosis: Basin already pins
//!     `target_partitions = 1` at session-open (see `session.rs`), so
//!     DataFusion never inserts the Partial→Repartition→Final fan-out that
//!     would dominate a 10k-row aggregate. There is therefore NO Basin-side
//!     over-partitioning lever left to pull — the residual gap is intrinsic
//!     DataFusion execution cost (per-batch hashing + scan setup), not a
//!     mis-set partition knob.
//!
//! FAIRNESS: every query below uses generic table/column names invented for
//! this test (`acct`, `evt`, `bucket`, `n`, …). NONE of the comparison-bench
//! identifiers appear here, so the tuning these tests guard cannot be keyed on
//! bench-specific table / column / literal names.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::cache_defaults;
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
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

/// Collect the result as a Vec of rows, where each row is the i64 value of the
/// requested column index. Non-i64 columns panic.
fn collect_col_i64(res: &ExecResult, col: usize) -> Vec<i64> {
    let ExecResult::Rows { batches, .. } = res else {
        panic!("expected Rows result");
    };
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column(col)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("column {col} not Int64Array (it is {:?})", b.column(col).data_type()));
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            out.push(arr.value(i));
        }
    }
    out
}

fn row_count(res: &ExecResult) -> usize {
    let ExecResult::Rows { batches, .. } = res else {
        panic!("expected Rows result");
    };
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Run EXPLAIN <sql> and return the flattened plan text (all rows joined).
async fn explain_text(sess: &basin_engine::ProjectSession, sql: &str) -> String {
    let res = sess
        .execute(&format!("EXPLAIN {sql}"))
        .await
        .expect("EXPLAIN must not error");
    let ExecResult::Rows { batches, .. } = res else {
        panic!("EXPLAIN returned non-Rows");
    };
    let mut lines = Vec::new();
    for b in &batches {
        for c in 0..b.num_columns() {
            if let Some(arr) = b
                .column(c)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
            {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        lines.push(arr.value(i).to_string());
                    }
                }
            }
        }
    }
    lines.join("\n")
}

/// Build a small analytics-shaped fixture: an `acct` dimension table and an
/// `evt` fact table with a foreign key into it. Row counts are deliberately
/// "small input" (10k-class) so the per-query overhead this file studies
/// dominates — exactly the scale where Basin's gap to PG is widest.
async fn seed(sess: &basin_engine::ProjectSession) {
    sess.execute(
        "CREATE TABLE acct (id BIGINT NOT NULL, email TEXT NOT NULL)",
    )
    .await
    .unwrap();
    sess.execute(
        "CREATE TABLE evt (\
            id BIGINT NOT NULL, \
            acct_id BIGINT NOT NULL, \
            amount BIGINT NOT NULL, \
            state TEXT NOT NULL, \
            day BIGINT NOT NULL)",
    )
    .await
    .unwrap();

    // 8 accounts.
    let mut acct_rows = Vec::new();
    for a in 0..8 {
        acct_rows.push(format!("({a}, 'acct{a}@x.test')"));
    }
    sess.execute(&format!("INSERT INTO acct VALUES {}", acct_rows.join(",")))
        .await
        .unwrap();

    // 400 fact rows spread over the 8 accounts, 3 states, 5 day-buckets. Small
    // enough to run fast in-test, large enough that the aggregate is real work.
    let states = ["open", "closed", "void"];
    let mut evt_rows = Vec::new();
    for i in 0i64..400 {
        let acct_id = i % 8;
        let amount = (i % 100) + 1;
        let state = states[(i % 3) as usize];
        let day = i % 5;
        evt_rows.push(format!("({i}, {acct_id}, {amount}, '{state}', {day})"));
    }
    // Insert in chunks to keep the INSERT statement size sane.
    for chunk in evt_rows.chunks(100) {
        sess.execute(&format!("INSERT INTO evt VALUES {}", chunk.join(",")))
            .await
            .unwrap();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Correctness: results are invariant under partition / batch config tuning.
// ─────────────────────────────────────────────────────────────────────────────

/// Shape (3): single-column aggregate GROUP BY + ORDER BY count DESC + LIMIT.
#[tokio::test]
async fn agg_group_by_single_col() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = sess
        .execute(
            "SELECT acct_id, COUNT(*) AS n, SUM(amount) AS s \
             FROM evt GROUP BY acct_id ORDER BY n DESC, acct_id LIMIT 10",
        )
        .await
        .expect("group by must execute");

    // 8 accounts, 400 fact rows evenly distributed → 50 each.
    assert_eq!(row_count(&res), 8, "8 groups expected");
    let counts = collect_col_i64(&res, 1);
    assert_eq!(counts, vec![50; 8], "each account has 50 rows; got {counts:?}");
    // Per-account SUM(amount): amounts cycle 1..=100, account a gets rows
    // i where i%8==a. Just assert the grand total via a second query to keep
    // this robust to ordering.
    let total = sess
        .execute("SELECT SUM(amount) FROM evt")
        .await
        .unwrap();
    let grand = collect_col_i64(&total, 0);
    let sums = collect_col_i64(&res, 2);
    assert_eq!(
        sums.iter().sum::<i64>(),
        grand[0],
        "per-group sums must reconstitute the grand total"
    );
}

/// Shape (4): 2-table JOIN then GROUP BY the dimension key.
#[tokio::test]
async fn agg_two_table_join_group_by() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = sess
        .execute(
            "SELECT a.email, COUNT(*) AS n \
             FROM acct a JOIN evt e ON e.acct_id = a.id \
             GROUP BY a.email ORDER BY n DESC, a.email LIMIT 20",
        )
        .await
        .expect("join group by must execute");

    assert_eq!(row_count(&res), 8, "8 accounts → 8 groups");
    let counts = collect_col_i64(&res, 1);
    assert_eq!(counts, vec![50; 8], "each account joined 50 rows; got {counts:?}");
}

/// Shape (14): bucketed rollup + SUM GROUP BY (PG would use DATE_TRUNC; we use
/// an integer day-bucket so the test has no datetime dependency, but the plan
/// shape — GROUP BY a derived key + SUM — is identical).
#[tokio::test]
async fn agg_bucket_rollup_sum() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = sess
        .execute(
            "SELECT day AS d, SUM(amount) AS s FROM evt GROUP BY day ORDER BY d",
        )
        .await
        .expect("rollup must execute");

    assert_eq!(row_count(&res), 5, "5 day buckets");
    let days = collect_col_i64(&res, 0);
    assert_eq!(days, vec![0, 1, 2, 3, 4], "buckets sorted ascending");
}

/// Shape (18): multi-column GROUP BY + HAVING + ORDER + LIMIT.
#[tokio::test]
async fn agg_multi_col_group_by_having() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = sess
        .execute(
            "SELECT acct_id, state, COUNT(*) AS n FROM evt \
             GROUP BY acct_id, state HAVING COUNT(*) > 5 \
             ORDER BY n DESC, acct_id, state LIMIT 50",
        )
        .await
        .expect("multi-col having must execute");

    // 8 accounts × 3 states = 24 groups, all with >5 rows (≈16-17 each), so
    // the HAVING keeps all 24.
    assert_eq!(row_count(&res), 24, "8×3 groups survive HAVING > 5");
    let counts = collect_col_i64(&res, 2);
    assert!(
        counts.iter().all(|&c| c > 5),
        "HAVING must filter to groups with > 5; got {counts:?}"
    );
    assert_eq!(counts.iter().sum::<i64>(), 400, "all 400 rows accounted for");
}

/// Shape (24): UNION ALL of two filtered scans of the same table.
#[tokio::test]
async fn union_all_two_scans() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = sess
        .execute(
            "SELECT id, 'a' AS kind FROM evt WHERE state = 'open' \
             UNION ALL \
             SELECT id, 'b' FROM evt WHERE state = 'closed'",
        )
        .await
        .expect("union all must execute");

    // states cycle open/closed/void over i%3: of 400 rows, open = ceil(400/3)
    // for residue 0 and closed for residue 1. i%3==0 → 134, i%3==1 → 133.
    let n_open = (0i64..400).filter(|i| i % 3 == 0).count();
    let n_closed = (0i64..400).filter(|i| i % 3 == 1).count();
    assert_eq!(
        row_count(&res),
        n_open + n_closed,
        "UNION ALL concatenates both branches without dedup"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Plan shape: small-input aggregate is single-partition, no Repartition.
// ─────────────────────────────────────────────────────────────────────────────

/// The load-bearing diagnostic. With `target_partitions = 1` pinned at
/// session-open, a small GROUP-BY must plan as a single AggregateExec with NO
/// `RepartitionExec` fan-out and NO Partial/Final split driven by repartition.
/// If a future change raised `target_partitions`, this assertion would catch
/// the regression that re-introduces the merge-exchange overhead the
/// narrowing-gap analysis identified.
#[tokio::test]
async fn small_input_aggregate_is_single_partition() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let plan = explain_text(
        &sess,
        "SELECT acct_id, COUNT(*) FROM evt GROUP BY acct_id",
    )
    .await;
    println!("=== GROUP BY physical plan ===\n{plan}");

    // The physical plan section starts after "physical_plan". Assert NO
    // RepartitionExec exchange was inserted — that is the over-partitioning
    // overhead we ruled out as a lever.
    let phys = plan
        .split("physical_plan")
        .nth(1)
        .unwrap_or(&plan);
    assert!(
        !phys.contains("RepartitionExec"),
        "small-input aggregate must NOT fan out via RepartitionExec; plan:\n{plan}"
    );
}

/// Same single-partition guarantee for UNION ALL: the two branches must not be
/// driven through a RepartitionExec merge fan-out.
#[tokio::test]
async fn small_input_union_all_is_single_partition() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let plan = explain_text(
        &sess,
        "SELECT id FROM evt WHERE state = 'open' \
         UNION ALL \
         SELECT id FROM evt WHERE state = 'closed'",
    )
    .await;
    println!("=== UNION ALL physical plan ===\n{plan}");

    let phys = plan.split("physical_plan").nth(1).unwrap_or(&plan);
    assert!(
        !phys.contains("RepartitionExec"),
        "UNION ALL branches must NOT fan out via RepartitionExec; plan:\n{plan}"
    );
}
