//! Fast Vortex-vs-Parquet smoke (#161) — the perf+correctness iterate loop.
//!
//! NOT the full benchmark: small, multi-file, no Postgres, runs in a few
//! seconds so it stays in the edit loop. It seeds the SAME data into a
//! Parquet table and a Vortex (default) table, then for a battery of
//! query SHAPES it (a) asserts the result sets are byte-identical
//! (correctness gate, same as the differential harness) and (b) times
//! Parquet vs Vortex and prints a per-shape comparison so a regression on
//! ANY shape is visible immediately — not just one.
//!
//! `ratio = parquet_ms / vortex_ms`  →  >1.0 means Vortex is faster.
//!
//! Run with `--nocapture` to see the table:
//!   cargo test -p basin-engine --test vortex_vs_parquet_smoke -- --nocapture

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{
    Array, BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch,
    StringArray, UInt32Array, UInt64Array,
};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// Tuned so the whole test is a few seconds: enough rows + files that
// timing is meaningful and file/chunk pruning is exercised, small enough
// to stay in the loop.
// Scale is env-overridable so the same harness serves both the fast loop
// (default 24k / ~12 files) and a larger comparison run, e.g.
// `BASIN_SMOKE_BATCHES=20 BASIN_SMOKE_ROWS=5000` → 100k / ~20 files.
fn n_batches() -> i64 {
    std::env::var("BASIN_SMOKE_BATCHES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(12)
}
fn rows_per_batch() -> i64 {
    std::env::var("BASIN_SMOKE_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000)
}
const REPS: usize = 5; // median of N timed runs per shape

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

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

/// Canonical, order-independent result form (sorted `|`-joined rows;
/// NULL = `\0`). Panics on an unexpected column type so a silent type
/// drift can't mask a mismatch.
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
                    // Round so a last-ULP divergence between the Vortex and
                    // Parquet compute paths can't fail an otherwise-equal row.
                    format!("{:.6}", a.value(r))
                } else if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
                    a.value(r).to_string()
                } else if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
                    // ROW_NUMBER()/window counts come back UInt64.
                    a.value(r).to_string()
                } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
                    a.value(r).to_string()
                } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
                    a.value(r).to_string()
                } else if let Some(a) = col.as_any().downcast_ref::<Int16Array>() {
                    a.value(r).to_string()
                } else {
                    panic!(
                        "normalized: extend for result col type {:?} (shape added a \
                         new output type)",
                        col.data_type()
                    );
                };
                cells.push(v);
            }
            rows.push(cells.join("|"));
        }
    }
    rows.sort();
    rows
}

/// Non-panicking query: `Err(msg)` if the engine rejects the SQL (so a
/// shape Basin doesn't support yet is *skipped*, not a harness panic) or
/// returns a non-rows result.
async fn try_query(sess: &ProjectSession, sql: &str) -> Result<Vec<String>, String> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => Ok(normalized(&batches)),
        Ok(other) => Err(format!("non-rows result: {other:?}")),
        Err(e) => Err(format!("{e:?}")),
    }
}

/// Median wall-time (ms) over REPS runs after one warm-up, or `Err` if
/// the query is rejected.
async fn try_timed(sess: &ProjectSession, sql: &str) -> Result<(f64, Vec<String>), String> {
    let result = try_query(sess, sql).await?; // warm + snapshot
    let mut s = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let t = Instant::now();
        let _ = try_query(sess, sql).await?;
        s.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Ok((s[s.len() / 2], result))
}

async fn build(sess: &ProjectSession, table: &str, with: &str) {
    exec(
        sess,
        &format!(
            "CREATE TABLE {table} (id BIGINT, k BIGINT, s TEXT, f DOUBLE, b BOOLEAN){with}"
        ),
    )
    .await;
    let rpb = rows_per_batch();
    for batch in 0..n_batches() {
        let mut vals = String::new();
        for i in 0..rpb {
            let id = batch * rpb + i;
            if !vals.is_empty() {
                vals.push(',');
            }
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
            vals.push_str(&format!("({id}, {}, {s}, {}, {bb})", id % 17, id as f64 * 1.5));
        }
        exec(
            sess,
            &format!("INSERT INTO {table} (id, k, s, f, b) VALUES {vals}"),
        )
        .await;
    }
}

/// Small dimension table (one row per distinct `k` = 0..16, the fact
/// table's `id % 17`) so JOIN / IN-subquery / correlated / anti-join
/// shapes are exercisable. Single insert → one data file.
async fn build_dim(sess: &ProjectSession, table: &str, with: &str) {
    exec(
        sess,
        &format!("CREATE TABLE {table} (dk BIGINT, label TEXT, w DOUBLE){with}"),
    )
    .await;
    let mut vals = String::new();
    for dk in 0..17i64 {
        if !vals.is_empty() {
            vals.push(',');
        }
        vals.push_str(&format!("({dk}, 'lbl{dk}', {})", dk as f64 * 2.5));
    }
    exec(
        sess,
        &format!("INSERT INTO {table} (dk, label, w) VALUES {vals}"),
    )
    .await;
}

#[tokio::test]
async fn vortex_vs_parquet_many_shapes() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    build(&sess, "tp", " WITH (basin.file_format='parquet')").await;
    build(&sess, "tv", "").await; // default = Vortex
    build_dim(&sess, "dp", " WITH (basin.file_format='parquet')").await;
    build_dim(&sess, "dv", "").await;

    let nb = n_batches();
    let total = nb * rows_per_batch();
    let lo = total / 4;
    let hi = lo + total / 2;
    let mid = total / 2 + 7;

    // `{Q}` = fact table, `{D}` = dimension table (substituted with the
    // parquet pair tp/dp or the vortex pair tv/dv). Beyond the original
    // simple shapes this adds COMPLEX shapes deliberately chosen to stress
    // the Vortex decode→Arrow→DataFusion path where a mature Parquet
    // reader is expected to win: joins, sub-queries, anti/semi-joins,
    // window functions, high-cardinality / multi-key GROUP BY, DISTINCT,
    // OR/IN/LIKE predicates, scalar-correlated sub-queries, expression
    // projections. Honest stress test — we WANT to surface Parquet wins.
    let shapes: &[(&str, String)] = &[
        // ---- simple / already-optimised ----
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
        // ---- complex: expected to favour Parquet ----
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
        // ---- very-complex PG surface (resilient loop skips any Basin
        // doesn't support; one-sided error still fails as a divergence) ----
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
    ];

    let subst = |t: &str, fact: &str, dim: &str| t.replace("{Q}", fact).replace("{D}", dim);

    println!(
        "\n[VORTEX vs PARQUET smoke — {total} rows, {nb} files/table, median of {REPS}]\n\
         {:<24}{:>12}{:>12}{:>13}",
        "shape", "parquet_ms", "vortex_ms", "ratio(p/v)"
    );
    let mut slower: Vec<String> = Vec::new();
    let (mut wins, mut parity, mut unsupported) = (0usize, 0usize, 0usize);
    for (label, tmpl) in shapes {
        let p = try_timed(&sess, &subst(tmpl, "tp", "dp")).await;
        let v = try_timed(&sess, &subst(tmpl, "tv", "dv")).await;
        match (p, v) {
            (Ok((p_ms, p_rows)), Ok((v_ms, v_rows))) => {
                assert_eq!(
                    v_rows, p_rows,
                    "CORRECTNESS: Vortex != Parquet for shape `{label}`"
                );
                let ratio = if v_ms > 0.0 { p_ms / v_ms } else { f64::NAN };
                let flag = if ratio < 0.98 {
                    slower.push(format!("{label} ({ratio:.2}x)"));
                    "  <-- slower"
                } else if ratio > 1.02 {
                    wins += 1;
                    ""
                } else {
                    parity += 1;
                    "  (~parity)"
                };
                println!("{label:<24}{p_ms:>12.3}{v_ms:>12.3}{ratio:>13.2}{flag}");
            }
            // Both formats reject it → a Basin SQL-support gap, NOT a
            // Vortex defect. Skip (don't fail the harness).
            (Err(_), Err(_)) => {
                unsupported += 1;
                println!("{label:<24}{:>12}{:>12}{:>13}", "-", "-", "unsupported");
            }
            // Exactly one format errors → a real Vortex/Parquet behaviour
            // divergence. That IS a correctness bug — fail loudly.
            (pr, vr) => {
                panic!(
                    "CORRECTNESS DIVERGENCE on `{label}`: parquet={pr:?} vortex={vr:?}"
                );
            }
        }
    }
    println!();

    // Correctness (incl. one-sided-error divergence) is asserted above.
    // Perf is reported, not hard-gated (env-sensitive); summarise so the
    // optimisation loop targets the remaining Parquet-favoured shapes.
    let comparable = shapes.len() - unsupported;
    if slower.is_empty() {
        println!(
            "[smoke] Vortex >= Parquet on ALL {comparable} comparable shapes \
             ({wins} win, {parity} parity; {unsupported} unsupported)"
        );
    } else {
        println!(
            "[smoke] Vortex slower on {}/{comparable}: {} \
             ({wins} win, {parity} parity, {unsupported} unsupported)",
            slower.len(),
            slower.join(", ")
        );
    }
}
