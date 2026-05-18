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

use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
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
                    format!("{:.6}", a.value(r))
                } else if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
                    a.value(r).to_string()
                } else {
                    panic!("unsupported result col type {:?}", col.data_type());
                };
                cells.push(v);
            }
            rows.push(cells.join("|"));
        }
    }
    rows.sort();
    rows
}

async fn query(sess: &ProjectSession, sql: &str) -> Vec<String> {
    match sess
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql:?}: {e:?}"))
    {
        ExecResult::Rows { batches, .. } => normalized(&batches),
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

/// Median wall-time (ms) over REPS runs, after one warm-up.
async fn timed(sess: &ProjectSession, sql: &str) -> (f64, Vec<String>) {
    let result = query(sess, sql).await; // warm + snapshot
    let mut s = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let t = Instant::now();
        let _ = query(sess, sql).await;
        s.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (s[s.len() / 2], result)
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

#[tokio::test]
async fn vortex_vs_parquet_many_shapes() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    build(&sess, "tp", " WITH (basin.file_format='parquet')").await;
    build(&sess, "tv", "").await; // default = Vortex

    let nb = n_batches();
    let total = nb * rows_per_batch();
    let lo = total / 4;
    let hi = lo + total / 2;
    let mid = total / 2 + 7;

    // (label, query template with {Q}). Many shapes — every one is
    // correctness-checked AND timed Parquet vs Vortex.
    let shapes: &[(&str, String)] = &[
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
    ];

    println!(
        "\n[VORTEX vs PARQUET smoke — {total} rows, {nb} files/table, median of {REPS}]\n\
         {:<22}{:>12}{:>12}{:>14}",
        "shape", "parquet_ms", "vortex_ms", "ratio(p/v)"
    );
    let mut slower: Vec<String> = Vec::new();
    for (label, tmpl) in shapes {
        let (p_ms, p_rows) = timed(&sess, &tmpl.replace("{Q}", "tp")).await;
        let (v_ms, v_rows) = timed(&sess, &tmpl.replace("{Q}", "tv")).await;
        assert_eq!(
            v_rows, p_rows,
            "CORRECTNESS: Vortex != Parquet for shape `{label}`"
        );
        let ratio = if v_ms > 0.0 { p_ms / v_ms } else { f64::NAN };
        let flag = if ratio < 1.0 { "  <-- slower" } else { "" };
        println!("{label:<22}{p_ms:>12.3}{v_ms:>12.3}{ratio:>14.2}{flag}");
        if ratio < 1.0 {
            slower.push(format!("{label} ({ratio:.2}x)"));
        }
    }
    println!();

    // Correctness is asserted per-shape above. Perf is reported, not hard-
    // gated (env-sensitive), but surface a clear summary of any shape
    // where Vortex is slower than Parquet so the loop targets it.
    if slower.is_empty() {
        println!("[smoke] Vortex >= Parquet on ALL {} shapes", shapes.len());
    } else {
        println!(
            "[smoke] Vortex SLOWER than Parquet on {}/{} shapes: {}",
            slower.len(),
            shapes.len(),
            slower.join(", ")
        );
    }
}
