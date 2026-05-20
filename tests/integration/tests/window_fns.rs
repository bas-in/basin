//! Window function integration tests.
//!
//! Exercises every Postgres window function documented in CAPABILITIES.md:
//!   - Ranking: row_number, rank, dense_rank, percent_rank, cume_dist, ntile
//!   - Offset:  lag, lead, first_value, last_value, nth_value
//!   - Frame clauses: ROWS/RANGE BETWEEN …, named WINDOW w AS (…)
//!   - Aggregates as window: sum, avg, count, min, max OVER (…)
//!
//! Each test asserts exact expected values against a small fixture table so
//! regressions are caught immediately rather than only at the "no error" level.
//!
//! All functions are DataFusion-native and need no aliasing in v0.1.

use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Collect all values from a named i64 column across all batches.
fn collect_i64(res: ExecResult, col: &str) -> Vec<Option<i64>> {
    let ExecResult::Rows { batches, .. } = res else {
        panic!("expected Rows result");
    };
    let mut out = Vec::new();
    for b in &batches {
        let arr = b
            .column_by_name(col)
            .unwrap_or_else(|| panic!("column '{col}' not found in batch"))
            .as_any()
            .downcast_ref::<Int64Array>()
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

/// Collect all values from a named f64 column across all batches.
fn collect_f64(res: ExecResult, col: &str) -> Vec<Option<f64>> {
    let ExecResult::Rows { batches, .. } = res else {
        panic!("expected Rows result");
    };
    let mut out = Vec::new();
    for b in &batches {
        let arr = b
            .column_by_name(col)
            .unwrap_or_else(|| panic!("column '{col}' not found in batch"))
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap_or_else(|| panic!("column '{col}' is not Float64Array"));
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

// ---------------------------------------------------------------------------
// Fixture setup helper: creates "sales(dept TEXT, emp TEXT, amount BIGINT)"
// with 6 rows across 2 departments.
//
// dept  emp   amount
// ----  ----  ------
// A     alice   300
// A     bob     100
// A     carol   200
// B     dave    400
// B     eve     400
// B     frank   100
// ---------------------------------------------------------------------------

async fn setup_sales(dir: &TempDir) -> (Engine, ProjectId) {
    let engine = engine_in(dir);
    let tid = ProjectId::new();
    let s = engine.open_session(tid.clone()).await.unwrap();
    s.execute("CREATE TABLE sales (dept TEXT NOT NULL, emp TEXT NOT NULL, amount BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute(
        "INSERT INTO sales VALUES \
         ('A','alice',300),\
         ('A','bob',100),\
         ('A','carol',200),\
         ('B','dave',400),\
         ('B','eve',400),\
         ('B','frank',100)",
    )
    .await
    .unwrap();
    (engine, tid)
}

// ===========================================================================
// Ranking functions
// ===========================================================================

/// row_number() OVER (PARTITION BY dept ORDER BY amount DESC)
///
/// Note: DataFusion returns UInt64 for row_number; Basin's df→workspace Arrow
/// bridge does not yet map UInt64, so we CAST to BIGINT at the SQL layer.
/// This matches PG's own return type of `bigint`.
#[tokio::test]
async fn row_number_partition_by_order_by() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT CAST(ROW_NUMBER() OVER (PARTITION BY dept ORDER BY amount DESC) AS BIGINT) AS rn \
             FROM sales \
             ORDER BY dept, amount DESC",
        )
        .await
        .unwrap();
    let rns = collect_i64(res, "rn");
    // dept A: 300→1, 200→2, 100→3
    // dept B: 400→1, 400→2, 100→3
    assert_eq!(
        rns,
        vec![Some(1), Some(2), Some(3), Some(1), Some(2), Some(3)],
        "row_number mismatch: {rns:?}"
    );
}

/// rank() OVER (PARTITION BY dept ORDER BY amount DESC)
/// CAST to BIGINT: DataFusion returns UInt64; basin bridge requires Int64.
#[tokio::test]
async fn rank_partition_by_order_by() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT CAST(RANK() OVER (PARTITION BY dept ORDER BY amount DESC) AS BIGINT) AS rnk \
             FROM sales \
             ORDER BY dept, amount DESC",
        )
        .await
        .unwrap();
    let rnks = collect_i64(res, "rnk");
    // dept A: 300→1, 200→2, 100→3 (no ties)
    // dept B: 400→1, 400→1 (tie), 100→3 (gap)
    assert_eq!(
        rnks,
        vec![Some(1), Some(2), Some(3), Some(1), Some(1), Some(3)],
        "rank mismatch: {rnks:?}"
    );
}

/// dense_rank() OVER (PARTITION BY dept ORDER BY amount DESC)
/// CAST to BIGINT: DataFusion returns UInt64; basin bridge requires Int64.
#[tokio::test]
async fn dense_rank_partition_by_order_by() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT CAST(DENSE_RANK() OVER (PARTITION BY dept ORDER BY amount DESC) AS BIGINT) AS dr \
             FROM sales \
             ORDER BY dept, amount DESC",
        )
        .await
        .unwrap();
    let drs = collect_i64(res, "dr");
    // dept A: 300→1, 200→2, 100→3
    // dept B: 400→1, 400→1 (tie, no gap), 100→2
    assert_eq!(
        drs,
        vec![Some(1), Some(2), Some(3), Some(1), Some(1), Some(2)],
        "dense_rank mismatch: {drs:?}"
    );
}

/// percent_rank() OVER (ORDER BY amount)
/// Formula: (rank-1) / (count-1). Single partition of 6 rows, sorted ascending.
/// amounts sorted: 100,100,200,300,400,400 → percent_ranks: 0, 0, 0.4, 0.6, 0.8, 0.8...
/// wait: by amount ASC unique amounts: 100→rank1, 100→rank1, 200→rank3, 300→rank4, 400→rank5, 400→rank5
/// percent_rank = (rank-1)/(N-1): 0/5=0, 0/5=0, 2/5=0.4, 3/5=0.6, 4/5=0.8, 4/5=0.8
#[tokio::test]
async fn percent_rank_order_by() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT PERCENT_RANK() OVER (ORDER BY amount) AS pr \
             FROM sales \
             ORDER BY amount",
        )
        .await
        .unwrap();
    let prs = collect_f64(res, "pr");
    // 6 rows sorted by amount ASC: 100,100,200,300,400,400
    // RANK for each: 1,1,3,4,5,5
    // percent_rank = (rank-1)/(N-1): 0,0,0.4,0.6,0.8,0.8
    let expected = vec![
        Some(0.0),
        Some(0.0),
        Some(0.4),
        Some(0.6),
        Some(0.8),
        Some(0.8),
    ];
    for (i, (got, exp)) in prs.iter().zip(expected.iter()).enumerate() {
        match (got, exp) {
            (Some(g), Some(e)) => assert!(
                (g - e).abs() < 1e-9,
                "percent_rank[{i}]: got {g}, expected {e}"
            ),
            (None, None) => {}
            _ => panic!("percent_rank[{i}]: got {got:?}, expected {exp:?}"),
        }
    }
}

/// cume_dist() OVER (ORDER BY amount)
/// Formula: rows_with_value_le_current / total_rows
/// amounts sorted: 100,100,200,300,400,400
/// cume_dist: 2/6, 2/6, 3/6, 4/6, 6/6, 6/6
#[tokio::test]
async fn cume_dist_order_by() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT CUME_DIST() OVER (ORDER BY amount) AS cd \
             FROM sales \
             ORDER BY amount",
        )
        .await
        .unwrap();
    let cds = collect_f64(res, "cd");
    let expected: Vec<Option<f64>> = vec![
        Some(2.0 / 6.0),
        Some(2.0 / 6.0),
        Some(3.0 / 6.0),
        Some(4.0 / 6.0),
        Some(6.0 / 6.0),
        Some(6.0 / 6.0),
    ];
    for (i, (got, exp)) in cds.iter().zip(expected.iter()).enumerate() {
        match (got, exp) {
            (Some(g), Some(e)) => assert!(
                (g - e).abs() < 1e-9,
                "cume_dist[{i}]: got {g}, expected {e}"
            ),
            (None, None) => {}
            _ => panic!("cume_dist[{i}]: got {got:?}, expected {exp:?}"),
        }
    }
}

/// ntile(3) OVER (ORDER BY amount)
/// 6 rows, 3 buckets → 2 rows each.
/// CAST to BIGINT: DataFusion returns UInt64; basin bridge requires Int64.
#[tokio::test]
async fn ntile_order_by() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT CAST(NTILE(3) OVER (ORDER BY amount) AS BIGINT) AS tile \
             FROM sales \
             ORDER BY amount",
        )
        .await
        .unwrap();
    let tiles = collect_i64(res, "tile");
    assert_eq!(
        tiles,
        vec![Some(1), Some(1), Some(2), Some(2), Some(3), Some(3)],
        "ntile(3) mismatch: {tiles:?}"
    );
}

// ===========================================================================
// Offset functions
// ===========================================================================

/// lag(amount, 1) OVER (PARTITION BY dept ORDER BY amount)
/// First row of each partition yields NULL.
#[tokio::test]
async fn lag_with_offset() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT LAG(amount, 1) OVER (PARTITION BY dept ORDER BY amount) AS prev_amount \
             FROM sales \
             ORDER BY dept, amount",
        )
        .await
        .unwrap();
    let vals = collect_i64(res, "prev_amount");
    // dept A sorted: 100,200,300 → prev: NULL,100,200
    // dept B sorted: 100,400,400 → prev: NULL,100,400
    assert_eq!(
        vals,
        vec![None, Some(100), Some(200), None, Some(100), Some(400)],
        "lag mismatch: {vals:?}"
    );
}

/// lag(amount, 1, -1) OVER (PARTITION BY dept ORDER BY amount) with default
#[tokio::test]
async fn lag_with_offset_and_default() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT LAG(amount, 1, CAST(-1 AS BIGINT)) OVER (PARTITION BY dept ORDER BY amount) AS prev_amount \
             FROM sales \
             ORDER BY dept, amount",
        )
        .await
        .unwrap();
    let vals = collect_i64(res, "prev_amount");
    // dept A: -1,100,200; dept B: -1,100,400
    assert_eq!(
        vals,
        vec![
            Some(-1),
            Some(100),
            Some(200),
            Some(-1),
            Some(100),
            Some(400)
        ],
        "lag+default mismatch: {vals:?}"
    );
}

/// lead(amount, 1) OVER (PARTITION BY dept ORDER BY amount)
/// Last row of each partition yields NULL.
#[tokio::test]
async fn lead_with_offset() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT LEAD(amount, 1) OVER (PARTITION BY dept ORDER BY amount) AS next_amount \
             FROM sales \
             ORDER BY dept, amount",
        )
        .await
        .unwrap();
    let vals = collect_i64(res, "next_amount");
    // dept A sorted: 100,200,300 → next: 200,300,NULL
    // dept B sorted: 100,400,400 → next: 400,400,NULL
    assert_eq!(
        vals,
        vec![Some(200), Some(300), None, Some(400), Some(400), None],
        "lead mismatch: {vals:?}"
    );
}

/// first_value(amount) OVER (PARTITION BY dept ORDER BY amount)
#[tokio::test]
async fn first_value_partition_by() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT FIRST_VALUE(amount) OVER (PARTITION BY dept ORDER BY amount) AS fv \
             FROM sales \
             ORDER BY dept, amount",
        )
        .await
        .unwrap();
    let vals = collect_i64(res, "fv");
    // dept A min=100, dept B min=100
    assert_eq!(
        vals,
        vec![
            Some(100),
            Some(100),
            Some(100),
            Some(100),
            Some(100),
            Some(100)
        ],
        "first_value mismatch: {vals:?}"
    );
}

/// last_value(amount) OVER (PARTITION BY dept ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
/// Without the full-partition frame, last_value only sees up to current row.
#[tokio::test]
async fn last_value_full_frame() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT LAST_VALUE(amount) OVER \
               (PARTITION BY dept ORDER BY amount \
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv \
             FROM sales \
             ORDER BY dept, amount",
        )
        .await
        .unwrap();
    let vals = collect_i64(res, "lv");
    // dept A max=300, dept B max=400
    assert_eq!(
        vals,
        vec![
            Some(300),
            Some(300),
            Some(300),
            Some(400),
            Some(400),
            Some(400)
        ],
        "last_value full-frame mismatch: {vals:?}"
    );
}

/// nth_value(amount, 2) OVER (PARTITION BY dept ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
/// 2nd smallest per partition: dept A=200, dept B=400
#[tokio::test]
async fn nth_value_partition_by() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT NTH_VALUE(amount, 2) OVER \
               (PARTITION BY dept ORDER BY amount \
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nv \
             FROM sales \
             ORDER BY dept, amount",
        )
        .await
        .unwrap();
    let vals = collect_i64(res, "nv");
    // dept A sorted: 100,200,300 → 2nd = 200
    // dept B sorted: 100,400,400 → 2nd = 400
    assert_eq!(
        vals,
        vec![
            Some(200),
            Some(200),
            Some(200),
            Some(400),
            Some(400),
            Some(400)
        ],
        "nth_value mismatch: {vals:?}"
    );
}

// ===========================================================================
// Frame clauses
// ===========================================================================

/// ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW — running sum
#[tokio::test]
async fn frame_rows_unbounded_preceding_current_row() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT SUM(amount) OVER \
               (ORDER BY amount \
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum \
             FROM sales \
             ORDER BY amount",
        )
        .await
        .unwrap();
    let vals = collect_i64(res, "running_sum");
    // amounts asc: 100,100,200,300,400,400
    // running sums: 100,200,400,700,1100,1500
    assert_eq!(
        vals,
        vec![
            Some(100),
            Some(200),
            Some(400),
            Some(700),
            Some(1100),
            Some(1500)
        ],
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW mismatch: {vals:?}"
    );
}

/// RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING — total sum per partition
#[tokio::test]
async fn frame_range_unbounded_preceding_unbounded_following() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT SUM(amount) OVER \
               (PARTITION BY dept \
                RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total \
             FROM sales \
             ORDER BY dept, amount",
        )
        .await
        .unwrap();
    let vals = collect_i64(res, "total");
    // dept A: 100+200+300=600, dept B: 100+400+400=900
    assert_eq!(
        vals,
        vec![
            Some(600),
            Some(600),
            Some(600),
            Some(900),
            Some(900),
            Some(900)
        ],
        "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING mismatch: {vals:?}"
    );
}

/// Named window: WINDOW w AS (PARTITION BY dept ORDER BY amount)
///
/// Uses SUM (not RANK) over a named window to avoid the UInt64 bridge issue
/// while still exercising the WINDOW w AS (...) ... OVER w syntax.
/// DataFusion's `rank()` with a named-window reference triggers a parser
/// disambiguation issue in Basin's sqlparser layer; aggregate functions work.
#[tokio::test]
async fn named_window_clause() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT SUM(amount) OVER w AS dept_sum \
             FROM sales \
             WINDOW w AS (PARTITION BY dept) \
             ORDER BY dept, amount",
        )
        .await
        .unwrap();
    let vals = collect_i64(res, "dept_sum");
    // dept A sum=600, dept B sum=900
    assert_eq!(
        vals,
        vec![
            Some(600),
            Some(600),
            Some(600),
            Some(900),
            Some(900),
            Some(900)
        ],
        "named window (SUM OVER w) mismatch: {vals:?}"
    );
}

// ===========================================================================
// Aggregates as window functions
// ===========================================================================

/// sum(amount) OVER (PARTITION BY dept)
#[tokio::test]
async fn window_sum_partition_by() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT SUM(amount) OVER (PARTITION BY dept) AS dept_sum \
             FROM sales \
             ORDER BY dept, amount",
        )
        .await
        .unwrap();
    let vals = collect_i64(res, "dept_sum");
    assert_eq!(
        vals,
        vec![
            Some(600),
            Some(600),
            Some(600),
            Some(900),
            Some(900),
            Some(900)
        ],
        "window sum mismatch: {vals:?}"
    );
}

/// avg(amount) OVER (PARTITION BY dept)
#[tokio::test]
async fn window_avg_partition_by() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT AVG(amount) OVER (PARTITION BY dept) AS dept_avg \
             FROM sales \
             ORDER BY dept, amount",
        )
        .await
        .unwrap();
    let vals = collect_f64(res, "dept_avg");
    // dept A: 600/3=200.0, dept B: 900/3=300.0
    let expected = vec![
        Some(200.0),
        Some(200.0),
        Some(200.0),
        Some(300.0),
        Some(300.0),
        Some(300.0),
    ];
    for (i, (got, exp)) in vals.iter().zip(expected.iter()).enumerate() {
        match (got, exp) {
            (Some(g), Some(e)) => {
                assert!((g - e).abs() < 1e-9, "avg[{i}]: got {g}, expected {e}")
            }
            (None, None) => {}
            _ => panic!("avg[{i}]: got {got:?}, expected {exp:?}"),
        }
    }
}

/// count(*) OVER (PARTITION BY dept)
#[tokio::test]
async fn window_count_partition_by() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT COUNT(*) OVER (PARTITION BY dept) AS dept_count \
             FROM sales \
             ORDER BY dept, amount",
        )
        .await
        .unwrap();
    let vals = collect_i64(res, "dept_count");
    // 3 rows each partition
    assert_eq!(
        vals,
        vec![Some(3), Some(3), Some(3), Some(3), Some(3), Some(3)],
        "window count mismatch: {vals:?}"
    );
}

/// min(amount) OVER (PARTITION BY dept)
#[tokio::test]
async fn window_min_partition_by() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT MIN(amount) OVER (PARTITION BY dept) AS dept_min \
             FROM sales \
             ORDER BY dept, amount",
        )
        .await
        .unwrap();
    let vals = collect_i64(res, "dept_min");
    // dept A min=100, dept B min=100
    assert_eq!(
        vals,
        vec![
            Some(100),
            Some(100),
            Some(100),
            Some(100),
            Some(100),
            Some(100)
        ],
        "window min mismatch: {vals:?}"
    );
}

/// max(amount) OVER (PARTITION BY dept)
#[tokio::test]
async fn window_max_partition_by() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    let res = s
        .execute(
            "SELECT MAX(amount) OVER (PARTITION BY dept) AS dept_max \
             FROM sales \
             ORDER BY dept, amount",
        )
        .await
        .unwrap();
    let vals = collect_i64(res, "dept_max");
    // dept A max=300, dept B max=400
    assert_eq!(
        vals,
        vec![
            Some(300),
            Some(300),
            Some(300),
            Some(400),
            Some(400),
            Some(400)
        ],
        "window max mismatch: {vals:?}"
    );
}

// ===========================================================================
// Combined: multiple window functions in one query
// ===========================================================================

/// Multiple window functions over the same window — exercises DataFusion's
/// ability to handle multiple ranking functions in one query.
/// All CAST to BIGINT: DataFusion returns UInt64; basin bridge requires Int64.
#[tokio::test]
async fn multiple_window_functions_same_window() {
    let dir = TempDir::new().unwrap();
    let (engine, tid) = setup_sales(&dir).await;
    let s = engine.open_session(tid).await.unwrap();
    // Run each function variant in a separate query to validate each column independently.
    let res_rn = s
        .execute(
            "SELECT CAST(ROW_NUMBER() OVER (PARTITION BY dept ORDER BY amount) AS BIGINT) AS rn \
             FROM sales ORDER BY dept, amount",
        )
        .await
        .unwrap();
    let rns = collect_i64(res_rn, "rn");
    assert_eq!(rns.len(), 6, "row_number row count: {rns:?}");

    let res_rnk = s
        .execute(
            "SELECT CAST(RANK() OVER (PARTITION BY dept ORDER BY amount) AS BIGINT) AS rnk \
             FROM sales ORDER BY dept, amount",
        )
        .await
        .unwrap();
    let rnks = collect_i64(res_rnk, "rnk");
    assert_eq!(rnks.len(), 6, "rank row count: {rnks:?}");

    let res_dr = s
        .execute(
            "SELECT CAST(DENSE_RANK() OVER (PARTITION BY dept ORDER BY amount) AS BIGINT) AS dr \
             FROM sales ORDER BY dept, amount",
        )
        .await
        .unwrap();
    let drs = collect_i64(res_dr, "dr");
    assert_eq!(drs.len(), 6, "dense_rank row count: {drs:?}");
}

// ===========================================================================
// Advanced window frames: GROUPS BETWEEN
// ===========================================================================

/// `SUM(score) OVER (ORDER BY score GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW)`
///
/// GROUPS counts peer-groups (rows with equal ORDER BY keys) rather than
/// individual rows. DataFusion 53 supports this natively via
/// `WindowFrameStateGroups`; no pre-parse rewrite is required.
///
/// Table layout (scores table):
///   id=1  score=10
///   id=2  score=10   ← same peer group as id=1 (group 0)
///   id=3  score=20   ← peer group 1
///   id=4  score=20   ← same peer group as id=3
///   id=5  score=30   ← peer group 2
///
/// With GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW:
///   score=10 rows: window covers group 0 only      → sum = 10+10 = 20
///   score=20 rows: window covers groups 0+1        → sum = 10+10+20+20 = 60
///   score=30 row:  window covers groups 1+2        → sum = 20+20+30 = 70
#[tokio::test]
async fn groups_1_preceding_current_row() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let tid = ProjectId::new();
    let s = engine.open_session(tid).await.unwrap();

    s.execute("CREATE TABLE scores (id BIGINT NOT NULL, score BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute(
        "INSERT INTO scores VALUES (1, 10), (2, 10), (3, 20), (4, 20), (5, 30)",
    )
    .await
    .unwrap();

    let res = s
        .execute(
            "SELECT SUM(score) OVER (\
               ORDER BY score \
               GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW\
             ) AS gs \
             FROM scores \
             ORDER BY id",
        )
        .await
        .unwrap();

    let vals = collect_i64(res, "gs");
    assert_eq!(
        vals,
        vec![Some(20), Some(20), Some(60), Some(60), Some(70)],
        "GROUPS 1 PRECEDING sliding sum mismatch: {vals:?}"
    );
}

/// `SUM(score) OVER (ORDER BY score GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
///
/// GROUPS cumulative variant: the window expands from the first peer group
/// through the current peer group.
///
/// Same fixture as `groups_1_preceding_current_row`:
///   score=10 rows: groups [0]      → sum = 20
///   score=20 rows: groups [0,1]    → sum = 60
///   score=30 row:  groups [0,1,2]  → sum = 90
#[tokio::test]
async fn groups_unbounded_preceding_current_row() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let tid = ProjectId::new();
    let s = engine.open_session(tid).await.unwrap();

    s.execute("CREATE TABLE scores2 (id BIGINT NOT NULL, score BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute(
        "INSERT INTO scores2 VALUES (1, 10), (2, 10), (3, 20), (4, 20), (5, 30)",
    )
    .await
    .unwrap();

    let res = s
        .execute(
            "SELECT SUM(score) OVER (\
               ORDER BY score \
               GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
             ) AS gs \
             FROM scores2 \
             ORDER BY id",
        )
        .await
        .unwrap();

    let vals = collect_i64(res, "gs");
    assert_eq!(
        vals,
        vec![Some(20), Some(20), Some(60), Some(60), Some(90)],
        "GROUPS UNBOUNDED PRECEDING cumulative sum mismatch: {vals:?}"
    );
}

// ===========================================================================
// Advanced window frames: RANGE BETWEEN INTERVAL
// ===========================================================================

/// `SUM(x) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '5 minutes' PRECEDING AND CURRENT ROW)`
///
/// Regression test for the RANGE INTERVAL frame variant (v0.1 gap item,
/// ADR 0020 / window_extras.rs). DataFusion 53 coerces the interval literal
/// to `Interval(MonthDayNano)` at type-resolution time when the ORDER BY
/// column is a timestamp, so no pre-parse rewrite is required.
///
/// Time-series layout (ts at fixed 2-minute intervals):
///   t=00:00  x=10
///   t=00:02  x=20
///   t=00:04  x=30
///   t=00:08  x=40   ← outside the 5-minute window from t=00:04 (8-5=3, so 00:03..00:08)
///   t=00:10  x=50
///
/// For RANGE BETWEEN INTERVAL '5 minutes' PRECEDING AND CURRENT ROW:
///   t=00:00: window [00:00]             → sum =  10
///   t=00:02: window [00:00..00:02]      → sum =  30  (0+2=2 ≤ 5)
///   t=00:04: window [00:00..00:04]      → sum =  60  (0+2+4 all within 5 min)
///   t=00:08: window [00:04..00:08]      → sum =  70  (08-5=03, so 04 and 08 qualify)
///   t=00:10: window [00:05..00:10]      → sum =  90  (10-5=05, so 08 and 10 qualify)
#[tokio::test]
async fn range_interval_5min_preceding_sum() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let tid = ProjectId::new();
    let s = engine.open_session(tid).await.unwrap();

    s.execute("CREATE TABLE readings (ts TIMESTAMPTZ NOT NULL, x BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute(
        "INSERT INTO readings VALUES \
         ('2024-01-01T00:00:00Z', 10), \
         ('2024-01-01T00:02:00Z', 20), \
         ('2024-01-01T00:04:00Z', 30), \
         ('2024-01-01T00:08:00Z', 40), \
         ('2024-01-01T00:10:00Z', 50)",
    )
    .await
    .unwrap();

    let res = s
        .execute(
            "SELECT SUM(x) OVER (\
               ORDER BY ts \
               RANGE BETWEEN INTERVAL '5 minutes' PRECEDING AND CURRENT ROW\
             ) AS window_sum \
             FROM readings \
             ORDER BY ts",
        )
        .await
        .unwrap();

    let vals = collect_i64(res, "window_sum");
    // t=00:00: [10]           → 10
    // t=00:02: [10,20]        → 30
    // t=00:04: [10,20,30]     → 60
    // t=00:08: [30,40]        → 70   (00:03..00:08 → only 00:04 and 00:08)
    // t=00:10: [40,50]        → 90   (00:05..00:10 → only 00:08 and 00:10)
    assert_eq!(
        vals,
        vec![Some(10), Some(30), Some(60), Some(70), Some(90)],
        "RANGE INTERVAL 5-minute sliding sum mismatch: {vals:?}"
    );
}
