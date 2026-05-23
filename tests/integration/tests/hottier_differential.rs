//! Differential harness: 88-shape battery × 3 storage modes produce
//! byte-identical results (Phase 5.14.C6).
//!
//! # What is tested
//!
//! The HTAP read-merge path (C3) promises that a SELECT result is
//! independent of whether the queried rows live in the cold tier (Vortex
//! files committed to the catalog) or the hot tier (transaction-local
//! pending files + in-memory batches visible within the same session).
//!
//! The three modes are:
//!
//! * **Mode A — cold only**: all rows inserted via auto-commit INSERTs
//!   → each INSERT writes + commits a Vortex file to the catalog.
//!   SELECT uses DataFusion's `ListingTable` backed by catalog files.
//!
//! * **Mode B — hot only (no committed files)**: `BEGIN`, insert all rows
//!   (writes Vortex files as pending tx-local files + buffers Arrow batches
//!   in `TxState::htap_rows`), then SELECT *within the open transaction*.
//!   The executor calls `refresh_table_with_extra` which combines the
//!   catalog's live files (none yet — nothing is committed) with the
//!   pending Vortex files. ROLLBACK afterwards so no state is committed.
//!
//! * **Mode C — split (half cold, half hot)**: insert the first half via
//!   auto-commit (committed → cold tier), then `BEGIN` and insert the
//!   second half (pending → hot tier), then SELECT within the open
//!   transaction. The executor sees both committed files and pending files
//!   via `refresh_table_with_extra`. ROLLBACK afterwards.
//!
//! All three modes must produce row-for-row identical results for every
//! shape in the 88-shape battery.
//!
//! # Implementation notes
//!
//! * Fixed seed for reproducibility (42).
//! * The shapes and data formulas are identical to `vortex_vs_parquet_smoke`
//!   so the same correctness property is tested end-to-end.
//! * Any shape that fails in all three modes (SQL not supported by Basin)
//!   is skipped uniformly — a one-sided failure (one mode errors, others
//!   succeed) is still reported as a correctness divergence.
//! * The 100k / 1M row variants are marked `#[ignore]` and only run with
//!   `cargo test -- --ignored` to keep the CI loop fast.
//!
//! # Running
//!
//! ```text
//! # Fast (default 10k rows):
//! cargo test -p basin-integration-tests --test hottier_differential -- --nocapture
//!
//! # Large (100k rows, slow):
//! BASIN_SMOKE_ROWS_LARGE=100000 \
//!   cargo test -p basin-integration-tests --test hottier_differential -- --ignored --nocapture
//! ```

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch,
    StringArray, UInt32Array, UInt64Array,
};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ── Tunable scale ─────────────────────────────────────────────────────────────

/// Default total row count for the fast path (stays under 5 minutes).
/// Keep in sync with the acceptance gate at TASK.md §5.14.C6.
const TOTAL_ROWS_FAST: i64 = 10_000;
const BATCH_SIZE: i64 = 2_000;

// ── Engine factory ────────────────────────────────────────────────────────────

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

// ── SQL helpers ───────────────────────────────────────────────────────────────

async fn exec_ok(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e}"));
}

/// Try to execute `sql` and return `Ok(rows)` or `Err(msg)`.
async fn try_query(sess: &ProjectSession, sql: &str) -> Result<Vec<String>, String> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => Ok(normalized(&batches)),
        Ok(other) => Err(format!("non-rows result: {other:?}")),
        Err(e) => Err(format!("{e:?}")),
    }
}

/// Try a query within an open transaction.
///
/// Wraps the query in `SAVEPOINT c6_probe` / `RELEASE SAVEPOINT c6_probe`
/// so that a query failure (e.g. unsupported SQL) does not leave the
/// transaction in an aborted state for subsequent shapes.
async fn try_query_in_tx(sess: &ProjectSession, sql: &str) -> Result<Vec<String>, String> {
    // Set a savepoint before the query.
    let _ = sess.execute("SAVEPOINT c6_probe").await;

    let result = match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => Ok(normalized(&batches)),
        Ok(other) => Err(format!("non-rows result: {other:?}")),
        Err(e) => Err(format!("{e:?}")),
    };

    if result.is_ok() {
        // Release the savepoint (merges it into the outer tx).
        let _ = sess.execute("RELEASE SAVEPOINT c6_probe").await;
    } else {
        // Rollback to savepoint to un-abort the transaction.
        let _ = sess
            .execute("ROLLBACK TO SAVEPOINT c6_probe")
            .await;
    }

    result
}

// ── Result normalisation (same formula as vortex_vs_parquet_smoke) ────────────

/// Canonical, order-independent result form. Each row is serialised as a
/// `|`-separated string of column values (NULL → `\0`). The vector is sorted
/// so comparison is position-independent.
fn normalized(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = Vec::new();
    for b in batches {
        for r in 0..b.num_rows() {
            let mut cells = Vec::with_capacity(b.num_columns());
            for c in 0..b.num_columns() {
                let col = b.column(c);
                let v = if col.is_null(r) {
                    "\0".to_string()
                } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                    a.value(r).to_string()
                } else if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                    a.value(r).to_string()
                } else if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
                    // Round so last-ULP differences between compute paths don't
                    // cause false mismatches.
                    format!("{:.6}", a.value(r))
                } else if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
                    a.value(r).to_string()
                } else if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
                    a.value(r).to_string()
                } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
                    a.value(r).to_string()
                } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
                    a.value(r).to_string()
                } else if let Some(a) = col.as_any().downcast_ref::<Int16Array>() {
                    a.value(r).to_string()
                } else {
                    panic!(
                        "normalized: unhandled column type {:?} — extend for new output types",
                        col.data_type()
                    )
                };
                cells.push(v);
            }
            rows.push(cells.join("|"));
        }
    }
    rows.sort();
    rows
}

// ── Data generation ───────────────────────────────────────────────────────────

/// Build one VALUES fragment row for row index `id`.
fn row_literal(id: i64) -> String {
    let s = if id % 7 == 0 {
        "NULL".to_string()
    } else {
        format!("'v{}'", id % 13)
    };
    let bb = if id % 5 == 0 {
        "NULL".to_string()
    } else if id % 2 == 0 {
        "true".to_string()
    } else {
        "false".to_string()
    };
    format!("({id}, {}, {s}, {}, {bb})", id % 17, id as f64 * 1.5)
}

/// Build a VALUES clause string for rows in `start..start+n`.
fn batch_values(start: i64, n: i64) -> String {
    (0..n)
        .map(|j| row_literal(start + j))
        .collect::<Vec<_>>()
        .join(",")
}

/// Dimension-table VALUES (fixed: dk = 0..16).
fn dim_values() -> String {
    (0..17i64)
        .map(|dk| format!("({dk}, 'lbl{dk}', {})", dk as f64 * 2.5))
        .collect::<Vec<_>>()
        .join(",")
}

// ── Seeding helpers (one per mode) ───────────────────────────────────────────

/// **Mode A — cold only**: auto-commit INSERTs, each batch committed
/// individually as a Vortex file in the catalog.
async fn seed_cold(sess: &ProjectSession, fact: &str, dim: &str, total_rows: i64) {
    exec_ok(
        sess,
        &format!("CREATE TABLE {fact} (id BIGINT, k BIGINT, s TEXT, f DOUBLE, b BOOLEAN)"),
    )
    .await;
    exec_ok(
        sess,
        &format!("CREATE TABLE {dim} (dk BIGINT, label TEXT, w DOUBLE)"),
    )
    .await;

    let mut written = 0i64;
    while written < total_rows {
        let n = (total_rows - written).min(BATCH_SIZE);
        let vals = batch_values(written, n);
        exec_ok(
            sess,
            &format!("INSERT INTO {fact} (id, k, s, f, b) VALUES {vals}"),
        )
        .await;
        written += n;
    }

    let dv = dim_values();
    exec_ok(sess, &format!("INSERT INTO {dim} (dk, label, w) VALUES {dv}")).await;
}

/// **Mode B — hot only**: `BEGIN`, insert all rows (pending Vortex files +
/// htap row buffer), leave the transaction open for queries.
/// Caller must issue `ROLLBACK` after querying.
async fn seed_hot(sess: &ProjectSession, fact: &str, dim: &str, total_rows: i64) {
    exec_ok(
        sess,
        &format!("CREATE TABLE {fact} (id BIGINT, k BIGINT, s TEXT, f DOUBLE, b BOOLEAN)"),
    )
    .await;
    exec_ok(
        sess,
        &format!("CREATE TABLE {dim} (dk BIGINT, label TEXT, w DOUBLE)"),
    )
    .await;

    // Open a transaction: all subsequent INSERTs go to pending (not catalog).
    exec_ok(sess, "BEGIN").await;

    let mut written = 0i64;
    while written < total_rows {
        let n = (total_rows - written).min(BATCH_SIZE);
        let vals = batch_values(written, n);
        exec_ok(
            sess,
            &format!("INSERT INTO {fact} (id, k, s, f, b) VALUES {vals}"),
        )
        .await;
        written += n;
    }

    let dv = dim_values();
    exec_ok(sess, &format!("INSERT INTO {dim} (dk, label, w) VALUES {dv}")).await;

    // Transaction is left open: queries within the tx see pending files via
    // refresh_table_with_extra. Caller issues ROLLBACK when done.
}

/// **Mode C — split**: first half committed (cold), second half pending (hot).
/// Caller must issue `ROLLBACK` after querying.
async fn seed_split(sess: &ProjectSession, fact: &str, dim: &str, total_rows: i64) {
    exec_ok(
        sess,
        &format!("CREATE TABLE {fact} (id BIGINT, k BIGINT, s TEXT, f DOUBLE, b BOOLEAN)"),
    )
    .await;
    exec_ok(
        sess,
        &format!("CREATE TABLE {dim} (dk BIGINT, label TEXT, w DOUBLE)"),
    )
    .await;

    // Dimension table: committed (same as Mode A).
    let dv = dim_values();
    exec_ok(sess, &format!("INSERT INTO {dim} (dk, label, w) VALUES {dv}")).await;

    // First half committed (auto-commit).
    let cold_rows = total_rows / 2;
    let mut written = 0i64;
    while written < cold_rows {
        let n = (cold_rows - written).min(BATCH_SIZE);
        let vals = batch_values(written, n);
        exec_ok(
            sess,
            &format!("INSERT INTO {fact} (id, k, s, f, b) VALUES {vals}"),
        )
        .await;
        written += n;
    }

    // Second half in an open transaction (pending files only).
    exec_ok(sess, "BEGIN").await;

    let hot_start = cold_rows;
    let hot_rows = total_rows - cold_rows;
    written = 0i64;
    while written < hot_rows {
        let n = (hot_rows - written).min(BATCH_SIZE);
        let vals = batch_values(hot_start + written, n);
        exec_ok(
            sess,
            &format!("INSERT INTO {fact} (id, k, s, f, b) VALUES {vals}"),
        )
        .await;
        written += n;
    }

    // Transaction left open: queries see cold (catalog) + hot (pending) rows.
    // Caller issues ROLLBACK when done.
}

// ── 88-shape catalogue (identical to vortex_vs_parquet_smoke) ─────────────────

/// All 88 query shapes. `{Q}` = fact table, `{D}` = dimension table.
fn all_shapes(total: i64) -> Vec<(&'static str, String)> {
    let lo = total / 4;
    let hi = lo + total / 2;
    let mid = total / 2 + 7;

    vec![
        // ── simple / already-optimised ────────────────────────────────────────
        ("full_scan", "SELECT * FROM {Q}".to_string()),
        ("projection_2col", "SELECT id, k FROM {Q}".to_string()),
        ("point_eq", format!("SELECT * FROM {{Q}} WHERE id = {mid}")),
        (
            "range_between",
            format!("SELECT * FROM {{Q}} WHERE id BETWEEN {lo} AND {hi}"),
        ),
        ("inequality_gt", "SELECT id, k FROM {Q} WHERE k > 10".to_string()),
        ("is_null", "SELECT * FROM {Q} WHERE s IS NULL".to_string()),
        ("string_eq", "SELECT * FROM {Q} WHERE s = 'v3'".to_string()),
        (
            "compound",
            format!("SELECT * FROM {{Q}} WHERE id BETWEEN {lo} AND {hi} AND b = true"),
        ),
        (
            "aggregate_full",
            "SELECT COUNT(*), SUM(id), MIN(k), MAX(k) FROM {Q}".to_string(),
        ),
        (
            "aggregate_filtered",
            format!("SELECT COUNT(*), SUM(id) FROM {{Q}} WHERE id BETWEEN {lo} AND {hi}"),
        ),
        ("group_by", "SELECT k, COUNT(*) FROM {Q} GROUP BY k".to_string()),
        (
            "order_by_limit",
            "SELECT * FROM {Q} ORDER BY id LIMIT 20".to_string(),
        ),
        (
            "filter_order_limit",
            format!("SELECT * FROM {{Q}} WHERE id >= {lo} ORDER BY id DESC LIMIT 10"),
        ),
        // ── complex: joins / subqueries ───────────────────────────────────────
        (
            "inner_join",
            format!(
                "SELECT a.id, d.label FROM {{Q}} a JOIN {{D}} d ON a.k = d.dk \
                 WHERE a.id BETWEEN {lo} AND {hi}"
            ),
        ),
        (
            "left_join",
            "SELECT a.id, d.label FROM {Q} a LEFT JOIN {D} d ON a.k = d.dk".to_string(),
        ),
        (
            "join_group_by",
            "SELECT d.label, COUNT(*) c FROM {Q} a JOIN {D} d ON a.k = d.dk \
             GROUP BY d.label ORDER BY c DESC"
                .to_string(),
        ),
        (
            "semi_in_subq",
            "SELECT id FROM {Q} WHERE k IN (SELECT dk FROM {D} WHERE w > 10.0)".to_string(),
        ),
        (
            "anti_not_in",
            "SELECT id FROM {Q} WHERE k NOT IN (SELECT dk FROM {D} WHERE w < 5.0)".to_string(),
        ),
        (
            "correlated_exists",
            "SELECT id FROM {Q} a WHERE EXISTS \
             (SELECT 1 FROM {D} d WHERE d.dk = a.k AND d.w > 20.0)"
                .to_string(),
        ),
        (
            "scalar_corr_subq",
            format!(
                "SELECT id, (SELECT label FROM {{D}} d WHERE d.dk = a.k) \
                 FROM {{Q}} a WHERE a.id < {lo}"
            ),
        ),
        (
            "in_list",
            "SELECT * FROM {Q} WHERE k IN (1, 3, 5, 7, 9, 11, 13, 15)".to_string(),
        ),
        (
            "or_predicate",
            "SELECT * FROM {Q} WHERE k = 1 OR k = 9 OR k = 13".to_string(),
        ),
        ("like_prefix", "SELECT * FROM {Q} WHERE s LIKE 'v1%'".to_string()),
        ("not_eq", "SELECT * FROM {Q} WHERE k <> 7".to_string()),
        (
            "multi_group_by",
            "SELECT k, b, COUNT(*) FROM {Q} GROUP BY k, b".to_string(),
        ),
        (
            "high_card_group_by",
            "SELECT id, COUNT(*) FROM {Q} GROUP BY id".to_string(),
        ),
        ("count_distinct", "SELECT COUNT(DISTINCT k) FROM {Q}".to_string()),
        ("distinct_rows", "SELECT DISTINCT k, b FROM {Q}".to_string()),
        (
            "having",
            "SELECT k, COUNT(*) c FROM {Q} GROUP BY k HAVING COUNT(*) > 100".to_string(),
        ),
        (
            "window_row_number",
            format!(
                "SELECT id, ROW_NUMBER() OVER (ORDER BY id) rn FROM {{Q}} WHERE id < {lo}"
            ),
        ),
        (
            "window_partition_sum",
            format!(
                "SELECT id, SUM(k) OVER (PARTITION BY b) sw FROM {{Q}} WHERE id < {lo}"
            ),
        ),
        (
            "subquery_from",
            format!(
                "SELECT COUNT(*) FROM (SELECT k FROM {{Q}} WHERE id BETWEEN {lo} AND {hi}) t"
            ),
        ),
        (
            "order_by_multi",
            "SELECT id, k, b FROM {Q} ORDER BY b, k, id LIMIT 50".to_string(),
        ),
        (
            "expr_projection",
            format!(
                "SELECT id, k, id + k AS s1, k * 2 AS s2, id - k AS s3 \
                 FROM {{Q}} WHERE id < {lo}"
            ),
        ),
        (
            "agg_group_order_limit",
            "SELECT k, COUNT(*) c, SUM(id) s FROM {Q} GROUP BY k ORDER BY c DESC LIMIT 5"
                .to_string(),
        ),
        // ── CTEs / set ops ────────────────────────────────────────────────────
        (
            "cte_agg",
            format!(
                "WITH agg AS (SELECT k, COUNT(*) c, SUM(id) s FROM {{Q}} GROUP BY k) \
                 SELECT k, c FROM agg WHERE c > 100 ORDER BY k"
            ),
        ),
        (
            "cte_join_chain",
            format!(
                "WITH hot AS (SELECT id, k FROM {{Q}} WHERE id BETWEEN {lo} AND {hi}) \
                 SELECT h.id, d.label FROM hot h JOIN {{D}} d ON h.k = d.dk"
            ),
        ),
        (
            "recursive_cte",
            "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 64) \
             SELECT COUNT(*) FROM seq"
                .to_string(),
        ),
        (
            "union_all",
            format!(
                "SELECT id FROM {{Q}} WHERE id < {lo} \
                 UNION ALL SELECT id FROM {{Q}} WHERE id >= {hi}"
            ),
        ),
        (
            "union_distinct",
            "SELECT k FROM {Q} UNION SELECT dk FROM {D}".to_string(),
        ),
        (
            "intersect",
            "SELECT k FROM {Q} INTERSECT SELECT dk FROM {D}".to_string(),
        ),
        (
            "except",
            "SELECT dk FROM {D} EXCEPT SELECT k FROM {Q}".to_string(),
        ),
        (
            "self_join",
            format!(
                "SELECT a.id, b.id FROM {{Q}} a JOIN {{Q}} b \
                 ON a.k = b.k AND a.id < b.id WHERE a.id < {lo}"
            ),
        ),
        (
            "three_way_join",
            format!(
                "SELECT a.id, d.label, d2.w FROM {{Q}} a \
                 JOIN {{D}} d ON a.k = d.dk JOIN {{D}} d2 ON a.k = d2.dk \
                 WHERE a.id < {lo}"
            ),
        ),
        (
            "full_outer_join",
            format!(
                "SELECT a.id, d.label FROM {{Q}} a \
                 FULL OUTER JOIN {{D}} d ON a.k = d.dk WHERE a.id < {lo}"
            ),
        ),
        (
            "lateral_count",
            format!(
                "SELECT a.id, x.c FROM {{Q}} a \
                 CROSS JOIN LATERAL (SELECT COUNT(*) c FROM {{D}} d WHERE d.dk = a.k) x \
                 WHERE a.id < {lo}"
            ),
        ),
        (
            "agg_filter_clause",
            "SELECT COUNT(*) FILTER (WHERE b = true) tc, COUNT(*) FILTER (WHERE b = false) fc, \
             COUNT(*) total FROM {Q}"
                .to_string(),
        ),
        (
            "rollup",
            "SELECT k, b, COUNT(*) FROM {Q} GROUP BY ROLLUP (k, b)".to_string(),
        ),
        (
            "grouping_sets",
            "SELECT k, b, COUNT(*) FROM {Q} GROUP BY GROUPING SETS ((k), (b), ())".to_string(),
        ),
        (
            "distinct_on",
            "SELECT DISTINCT ON (k) k, id FROM {Q} ORDER BY k, id".to_string(),
        ),
        (
            "case_group",
            "SELECT CASE WHEN k < 5 THEN 'lo' WHEN k < 10 THEN 'mid' ELSE 'hi' END g, \
             COUNT(*) c FROM {Q} GROUP BY 1 ORDER BY g"
                .to_string(),
        ),
        (
            "window_frame_avg",
            format!(
                "SELECT id, AVG(k) OVER (ORDER BY id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) a \
                 FROM {{Q}} WHERE id < {lo}"
            ),
        ),
        (
            "window_rank_partition",
            format!(
                "SELECT id, RANK() OVER (PARTITION BY b ORDER BY k DESC) rk \
                 FROM {{Q}} WHERE id < {lo}"
            ),
        ),
        (
            "percentile_cont",
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY k) FROM {Q}".to_string(),
        ),
        (
            "string_agg",
            format!(
                "SELECT b, string_agg(s, ',') FROM {{Q}} WHERE id < {lo} GROUP BY b"
            ),
        ),
        (
            "generate_series_join",
            "SELECT g.v, COUNT(a.id) FROM generate_series(0, 16) AS g(v) \
             LEFT JOIN {Q} a ON a.k = g.v GROUP BY g.v ORDER BY g.v"
                .to_string(),
        ),
        // ── Hard shapes ───────────────────────────────────────────────────────
        (
            "anti_join_not_exists",
            "SELECT COUNT(*) FROM {Q} q \
             WHERE NOT EXISTS (SELECT 1 FROM {D} d WHERE d.dk = q.k AND d.w > 20.0)"
                .to_string(),
        ),
        (
            "scalar_correlated_select",
            format!(
                "SELECT id, (SELECT label FROM {{D}} d WHERE d.dk = q.k) lbl \
                 FROM {{Q}} q WHERE id < {lo} ORDER BY id LIMIT 100"
            ),
        ),
        (
            "any_subquery_gt",
            "SELECT COUNT(*) FROM {Q} \
             WHERE k > ANY (SELECT dk FROM {D} WHERE w < 15.0)"
                .to_string(),
        ),
        (
            "all_subquery_lt",
            "SELECT COUNT(*) FROM {Q} \
             WHERE k < ALL (SELECT dk FROM {D} WHERE label LIKE 'lbl1%')"
                .to_string(),
        ),
        (
            "top_1_per_k_window",
            "SELECT id, k FROM ( \
               SELECT id, k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY id DESC) rn \
               FROM {Q} \
             ) t WHERE rn = 1 ORDER BY k"
                .to_string(),
        ),
        (
            "lateral_top_3_per_k",
            "SELECT d.dk, t.id FROM {D} d, \
               LATERAL (SELECT id FROM {Q} WHERE k = d.dk ORDER BY id LIMIT 3) t \
             ORDER BY d.dk, t.id"
                .to_string(),
        ),
        (
            "pivot_via_filter_agg",
            "SELECT k, \
               COUNT(*) FILTER (WHERE b IS TRUE)  t, \
               COUNT(*) FILTER (WHERE b IS FALSE) f, \
               COUNT(*) FILTER (WHERE b IS NULL)  n \
             FROM {Q} GROUP BY k ORDER BY k"
                .to_string(),
        ),
        (
            "lag_lead_window",
            format!(
                "SELECT id, k, LAG(k, 2, -1) OVER (ORDER BY id) lg, \
                              LEAD(k, 3, -1) OVER (ORDER BY id) ld \
                 FROM {{Q}} WHERE id < {lo} ORDER BY id LIMIT 50"
            ),
        ),
        (
            "named_window_multi",
            format!(
                "SELECT id, AVG(f) OVER w av, RANK() OVER w rk \
                 FROM {{Q}} WHERE id < {lo} \
                 WINDOW w AS (ORDER BY id) ORDER BY id LIMIT 50"
            ),
        ),
        (
            "nth_value_window",
            format!(
                "SELECT id, k, NTH_VALUE(k, 3) OVER (PARTITION BY b ORDER BY id) nth \
                 FROM {{Q}} WHERE id < {lo} ORDER BY id LIMIT 50"
            ),
        ),
        (
            "range_window_frame",
            format!(
                "SELECT id, AVG(f) OVER ( \
                   ORDER BY id RANGE BETWEEN 100 PRECEDING AND CURRENT ROW \
                 ) av FROM {{Q}} WHERE id < {lo} ORDER BY id LIMIT 50"
            ),
        ),
        (
            "deep_or_chain",
            "SELECT COUNT(*) FROM {Q} WHERE \
             id = 1 OR id = 7 OR id = 13 OR id = 23 OR id = 31 OR id = 47 OR \
             id = 71 OR id = 103 OR id = 137 OR id = 191 OR id = 233 OR \
             id = 311 OR id = 419 OR id = 521 OR id = 587 OR id = 647"
                .to_string(),
        ),
        (
            "in_subquery_range",
            "SELECT COUNT(*) FROM {Q} q \
             WHERE k IN (SELECT dk FROM {D} WHERE w BETWEEN 5.0 AND 25.0)"
                .to_string(),
        ),
        (
            "cube_agg",
            "SELECT k, b, COUNT(*) c, SUM(id) s FROM {Q} \
             GROUP BY CUBE (k, b) ORDER BY k NULLS LAST, b NULLS LAST"
                .to_string(),
        ),
        (
            "case_having_complex",
            "SELECT CASE WHEN k < 6 THEN 'lo' WHEN k < 12 THEN 'mid' ELSE 'hi' END g, \
                    COUNT(*) c, AVG(f) a \
             FROM {Q} GROUP BY 1 HAVING COUNT(*) > 100 ORDER BY g"
                .to_string(),
        ),
        (
            "distinct_count_case",
            format!(
                "SELECT b, COUNT(DISTINCT CASE WHEN id < {mid} THEN k END) cd \
                 FROM {{Q}} GROUP BY b ORDER BY b NULLS LAST"
            ),
        ),
        (
            "cte_self_join",
            "WITH t AS (SELECT k, COUNT(*) c FROM {Q} GROUP BY k) \
             SELECT a.k ak, b.k bk, a.c FROM t a JOIN t b ON a.c = b.c \
             WHERE a.k < b.k ORDER BY a.k, b.k"
                .to_string(),
        ),
        (
            "string_chain_funcs",
            "SELECT k, COUNT(*) c FROM {Q} \
             WHERE LENGTH(s) > 0 AND LOWER(s) LIKE 'v%' GROUP BY k ORDER BY k"
                .to_string(),
        ),
        (
            "modulo_predicate_expr",
            format!(
                "SELECT id, k * 3 + 1 e1, f / 2.0 e2 \
                 FROM {{Q}} WHERE MOD(id, 100) BETWEEN 10 AND 50 \
                 ORDER BY id LIMIT 200"
            ),
        ),
        (
            "coalesce_nullif_filter",
            "SELECT COUNT(*) FROM {Q} \
             WHERE COALESCE(s, 'X') <> 'X' AND NULLIF(k, 0) IS NOT NULL"
                .to_string(),
        ),
        // ── Harder variety ────────────────────────────────────────────────────
        (
            "recursive_cte_chain",
            "WITH RECURSIVE chain(n) AS ( \
               SELECT 0 UNION ALL SELECT n + 1 FROM chain WHERE n < 200 \
             ) SELECT COUNT(*) FROM chain"
                .to_string(),
        ),
        (
            "math_chain",
            "SELECT SUM(ROUND(SQRT(ABS(f)) + MOD(id, 7), 3)) FROM {Q}"
                .to_string(),
        ),
        (
            "abs_mod_filter",
            "SELECT COUNT(*) FROM {Q} WHERE ABS(MOD(id, 100)) BETWEEN 10 AND 50"
                .to_string(),
        ),
        (
            "stddev_grouped",
            "SELECT k, STDDEV(f) sd FROM {Q} GROUP BY k ORDER BY k"
                .to_string(),
        ),
        (
            "min_max_float",
            format!(
                "SELECT k, MIN(f) mn, MAX(f) mx FROM {{Q}} WHERE id < {hi} \
                 GROUP BY k ORDER BY k"
            ),
        ),
        (
            "intersect_all",
            "SELECT k FROM {Q} INTERSECT ALL SELECT dk FROM {D} ORDER BY k LIMIT 50"
                .to_string(),
        ),
        (
            "except_all",
            "SELECT k FROM {Q} EXCEPT ALL SELECT dk FROM {D} ORDER BY k LIMIT 50"
                .to_string(),
        ),
        (
            "order_by_expr",
            format!(
                "SELECT id, k FROM {{Q}} WHERE id < {lo} \
                 ORDER BY (id % 100), k DESC, id LIMIT 50"
            ),
        ),
        (
            "group_by_expr_mod",
            "SELECT (k % 5) g, COUNT(*) c FROM {Q} GROUP BY (k % 5) ORDER BY g"
                .to_string(),
        ),
        (
            "self_join_multi_col",
            format!(
                "SELECT a.id FROM {{Q}} a JOIN {{Q}} b \
                 ON a.k = b.k AND a.b IS NOT DISTINCT FROM b.b AND a.id < b.id \
                 WHERE a.id < {lo} ORDER BY a.id LIMIT 100"
            ),
        ),
        (
            "four_way_join",
            format!(
                "SELECT q.id, d1.label, d2.label, d3.label \
                 FROM {{Q}} q \
                 JOIN {{D}} d1 ON d1.dk = q.k \
                 JOIN {{D}} d2 ON d2.dk = q.k % 7 \
                 JOIN {{D}} d3 ON d3.dk = q.k % 3 \
                 WHERE q.id < {lo} ORDER BY q.id LIMIT 50"
            ),
        ),
        (
            "multi_having",
            "SELECT k, COUNT(*) c, AVG(f) a FROM {Q} GROUP BY k \
             HAVING COUNT(*) > 100 AND AVG(f) > 100.0 ORDER BY k"
                .to_string(),
        ),
        (
            "is_distinct_from",
            "SELECT COUNT(*) FROM {Q} WHERE b IS DISTINCT FROM TRUE"
                .to_string(),
        ),
        (
            "multi_col_distinct",
            "SELECT COUNT(*) FROM (SELECT DISTINCT k, b FROM {Q}) t"
                .to_string(),
        ),
        (
            "group_by_ordinal",
            "SELECT k, b, COUNT(*) c FROM {Q} GROUP BY 1, 2 \
             ORDER BY 1 NULLS LAST, 2 NULLS LAST"
                .to_string(),
        ),
        (
            "substring_concat",
            format!(
                "SELECT k, concat(SUBSTR(s, 1, 2), '-', CAST(k AS TEXT)) lbl \
                 FROM {{Q}} WHERE s IS NOT NULL AND id < {lo} \
                 ORDER BY id LIMIT 50"
            ),
        ),
        (
            "case_chain_nested",
            "SELECT CASE WHEN k = 0 THEN 'a' \
                         WHEN k IN (1, 2, 3) THEN \
                           CASE WHEN b IS TRUE THEN 'b' ELSE 'c' END \
                         WHEN k > 10 THEN 'd' \
                         ELSE 'e' END g, \
                    COUNT(*) c FROM {Q} GROUP BY 1 ORDER BY 1"
                .to_string(),
        ),
        (
            "limit_offset_large",
            "SELECT id, k FROM {Q} ORDER BY id LIMIT 100 OFFSET 1000"
                .to_string(),
        ),
    ]
}

// ── Core differential runner ──────────────────────────────────────────────────

/// Run the differential harness for `total_rows` rows.
///
/// Returns `(shapes_tested, shapes_skipped)` where:
/// - `shapes_tested` = shapes supported by all three modes.
/// - `shapes_skipped` = shapes unsupported in all three modes (SQL gap).
async fn run_differential(total_rows: i64) -> (usize, usize) {
    // ── Mode A: cold-only (all rows committed to Vortex files) ────────────────
    let dir_a = TempDir::new().unwrap();
    let eng_a = engine_in(&dir_a);
    let sess_a = eng_a.open_session(ProjectId::new()).await.unwrap();
    seed_cold(&sess_a, "q", "d", total_rows).await;

    // ── Mode B: hot-only (all rows in open tx, no committed files) ────────────
    let dir_b = TempDir::new().unwrap();
    let eng_b = engine_in(&dir_b);
    let sess_b = eng_b.open_session(ProjectId::new()).await.unwrap();
    seed_hot(&sess_b, "q", "d", total_rows).await;

    // ── Mode C: split (half committed, half in open tx) ───────────────────────
    let dir_c = TempDir::new().unwrap();
    let eng_c = engine_in(&dir_c);
    let sess_c = eng_c.open_session(ProjectId::new()).await.unwrap();
    seed_split(&sess_c, "q", "d", total_rows).await;

    // ── Run all shapes ────────────────────────────────────────────────────────
    let shapes = all_shapes(total_rows);
    let n_shapes = shapes.len();
    let (mut tested, mut skipped, mut diverged) = (0usize, 0usize, 0usize);

    println!(
        "\n[C6 hottier-differential — {total_rows} rows, {} shapes × 3 modes]\n\
         {:<30}{:>10}{:>10}{:>10}",
        n_shapes, "shape", "mode_a", "mode_b", "mode_c"
    );

    for (label, tmpl) in &shapes {
        let q_a = tmpl.replace("{Q}", "q").replace("{D}", "d");
        let q_b = tmpl.replace("{Q}", "q").replace("{D}", "d");
        let q_c = tmpl.replace("{Q}", "q").replace("{D}", "d");

        // Mode A: no open tx → plain query.
        let r_a = try_query(&sess_a, &q_a).await;
        // Mode B & C: open tx → use savepoint wrapper to keep tx alive.
        let r_b = try_query_in_tx(&sess_b, &q_b).await;
        let r_c = try_query_in_tx(&sess_c, &q_c).await;

        match (&r_a, &r_b, &r_c) {
            // All three succeed → assert byte-identical results.
            (Ok(rows_a), Ok(rows_b), Ok(rows_c)) => {
                tested += 1;
                let ok_ab = rows_a == rows_b;
                let ok_ac = rows_a == rows_c;
                let flag = if ok_ab && ok_ac { "" } else { "  *** DIFF ***" };
                println!(
                    "{label:<30}{:>10}{:>10}{:>10}{flag}",
                    rows_a.len(),
                    rows_b.len(),
                    rows_c.len()
                );
                if !ok_ab {
                    diverged += 1;
                    eprintln!(
                        "[C6] DIVERGENCE on shape `{label}` (mode_a vs mode_b):\n  \
                         mode_a rows: {}\n  mode_b rows: {}\n  \
                         first diff — a: {:?}\n  first diff — b: {:?}",
                        rows_a.len(),
                        rows_b.len(),
                        rows_a.iter().find(|r| !rows_b.contains(r)),
                        rows_b.iter().find(|r| !rows_a.contains(r)),
                    );
                }
                if !ok_ac {
                    diverged += 1;
                    eprintln!(
                        "[C6] DIVERGENCE on shape `{label}` (mode_a vs mode_c):\n  \
                         mode_a rows: {}\n  mode_c rows: {}\n  \
                         first diff — a: {:?}\n  first diff — c: {:?}",
                        rows_a.len(),
                        rows_c.len(),
                        rows_a.iter().find(|r| !rows_c.contains(r)),
                        rows_c.iter().find(|r| !rows_a.contains(r)),
                    );
                }
            }
            // All three fail → Basin SQL gap; skip uniformly.
            (Err(_), Err(_), Err(_)) => {
                skipped += 1;
                println!("{label:<30}{:>10}{:>10}{:>10}", "-", "-", "unsupported");
            }
            // Mixed: at least one mode errors while another succeeds → divergence.
            (ra, rb, rc) => {
                diverged += 1;
                eprintln!(
                    "[C6] ONE-SIDED ERROR on shape `{label}`:\n  \
                     mode_a={} mode_b={} mode_c={}",
                    if ra.is_ok() { "ok" } else { "err" },
                    if rb.is_ok() { "ok" } else { "err" },
                    if rc.is_ok() { "ok" } else { "err" },
                );
                println!(
                    "{label:<30}{:>10}{:>10}{:>10}  *** MIXED ERR ***",
                    if ra.is_ok() { "ok".to_string() } else { "err".to_string() },
                    if rb.is_ok() { "ok".to_string() } else { "err".to_string() },
                    if rc.is_ok() { "ok".to_string() } else { "err".to_string() },
                );
            }
        }
    }

    // Roll back the open transactions in Mode B and C so the sessions are clean.
    let _ = sess_b.execute("ROLLBACK").await;
    let _ = sess_c.execute("ROLLBACK").await;

    println!(
        "\n[C6] {tested} shapes tested ({skipped} skipped as unsupported); \
         {diverged} divergences."
    );

    assert_eq!(
        diverged,
        0,
        "[C6] {diverged} shape(s) produced different results across modes. \
         See stderr for details."
    );

    (tested, skipped)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Fast path: 10k rows, all 88 shapes, must complete in under 5 minutes.
///
/// This is the primary CI gate. Run with `--nocapture` to see the per-shape
/// comparison table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hottier_differential_10k() {
    let total = TOTAL_ROWS_FAST;
    let (tested, skipped) = run_differential(total).await;
    println!(
        "[C6] PASS — {tested} shapes × 3 modes = {} assertions, \
         {skipped} shapes skipped (SQL gap).",
        tested * 3
    );
}

/// Slow path: larger row count. Excluded from default `cargo test` run;
/// use `cargo test -- --ignored` or set `BASIN_SMOKE_ROWS_LARGE` to a value.
///
/// ```text
/// cargo test -p basin-integration-tests --test hottier_differential \
///     -- --ignored --nocapture
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "100k-row slow smoke; run with --ignored or set BASIN_SMOKE_ROWS_LARGE"]
async fn hottier_differential_100k() {
    let total: i64 = std::env::var("BASIN_SMOKE_ROWS_LARGE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);
    let (tested, skipped) = run_differential(total).await;
    println!(
        "[C6] PASS (large) — {tested} shapes × 3 modes = {} assertions, \
         {skipped} shapes skipped.",
        tested * 3
    );
}
