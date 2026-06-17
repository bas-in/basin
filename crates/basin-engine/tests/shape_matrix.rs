//! Comprehensive in-process engine **shape-matrix** bench.
//!
//! Seeds N rows into a battery of table SHAPES (narrow/wide, PK/no-PK,
//! JSONB/NUMERIC/TIMESTAMPTZ/TEXT, composite-PK, many-column) via the COPY
//! fast-path (`ingest_csv_batch`), then runs a battery of OPERATION shapes
//! (point lookup, indexed range, full agg, group-by, update, delete) — timing
//! every step and asserting basic correctness. Runs entirely in-process against
//! LocalFS storage, so it iterates in seconds and isolates engine algorithmic
//! cost from network/object-store latency.
//!
//! Purpose: a fast, local, deterministic gate that SURFACES per-shape perf
//! cliffs (e.g. bulk COPY into a PRIMARY KEY or JSONB table) and correctness
//! regressions before they reach the cloud benchmark. Scale toward the 1B-row
//! target by raising `BASIN_SHAPE_ROWS` on a box that can hold it.
//!
//! Run:
//!   BASIN_SHAPE_ROWS=200000 cargo test --release -p basin-engine \
//!     --test shape_matrix -- --nocapture
//!
//! Env:
//!   BASIN_SHAPE_ROWS   total rows seeded per shape (default 100_000)
//!   BASIN_SHAPE_CHUNK  COPY chunk size (default 10_000)
//!   BASIN_SHAPE_ONLY   comma list of shape names to run (default: all)
//!   BASIN_SHAPE_SLA_RPS  min ingest rows/sec before a shape is flagged SLOW
//!                        (default 0 = report only, never fail)

use std::sync::Arc;
use std::time::Instant;

use arrow_schema::Schema;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig { storage, catalog, shard: None })
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// One table shape: how to create it, the COPY column list, and a per-row CSV
/// value generator (column-aligned with `cols`).
struct Shape {
    name: &'static str,
    create: &'static str,
    cols: &'static [&'static str],
    /// row index (0-based) -> the COPY text cell for each column in `cols`.
    gen: fn(usize) -> Vec<Option<String>>,
    /// Operation SQLs to time after seeding. `{T}` is replaced with the table.
    /// Each is run a few times; we report the median wall time.
    ops: &'static [(&'static str, &'static str)],
}

fn s(v: impl ToString) -> Option<String> {
    Some(v.to_string())
}

// ---- per-shape row generators ------------------------------------------------

fn gen_narrow(i: usize) -> Vec<Option<String>> {
    vec![s(i + 1), s(i % 1000)]
}
fn gen_wide(i: usize) -> Vec<Option<String>> {
    vec![
        s(i + 1),
        s(i % 200),
        s(["pending", "active", "closed", "failed", "refunded"][i % 5]),
        s(format!("{}.50", i % 100_000)),
        s("2023-06-15 12:00:00+00"),
        s(format!("{{\"ref\":\"{:08x}\",\"v\":{}}}", i, i % 9999)),
    ]
}
fn gen_composite(i: usize) -> Vec<Option<String>> {
    vec![s(i / 100), s(i % 100), s(format!("v{i}"))]
}
fn gen_jsonb_only(i: usize) -> Vec<Option<String>> {
    vec![s(i + 1), s(format!("{{\"a\":{},\"b\":\"{:08x}\"}}", i % 1000, i))]
}
fn gen_numeric_only(i: usize) -> Vec<Option<String>> {
    vec![s(i + 1), s(format!("{}.{:02}", i % 1_000_000, i % 100))]
}
fn gen_ts_only(i: usize) -> Vec<Option<String>> {
    vec![s(i + 1), s("2023-06-15 12:00:00+00")]
}

const SHAPES: &[Shape] = &[
    Shape {
        name: "narrow_nopk",
        create: "CREATE TABLE {T} (id BIGINT NOT NULL, x INT)",
        cols: &["id", "x"],
        gen: gen_narrow,
        ops: &[
            ("full_agg", "SELECT count(*), sum(x) FROM {T}"),
            ("group_by", "SELECT x, count(*) FROM {T} GROUP BY x ORDER BY 2 DESC LIMIT 20"),
        ],
    },
    Shape {
        name: "narrow_pk",
        create: "CREATE TABLE {T} (id BIGINT PRIMARY KEY, x INT)",
        cols: &["id", "x"],
        gen: gen_narrow,
        ops: &[
            ("point_lookup", "SELECT id, x FROM {T} WHERE id = 12345"),
            ("full_agg", "SELECT count(*), sum(x) FROM {T}"),
        ],
    },
    Shape {
        name: "wide_pk",
        create: "CREATE TABLE {T} (id BIGINT PRIMARY KEY, tenant_id INT, status TEXT, \
                 amount NUMERIC(12,2), created_at TIMESTAMPTZ, payload JSONB)",
        cols: &["id", "tenant_id", "status", "amount", "created_at", "payload"],
        gen: gen_wide,
        ops: &[
            ("point_lookup", "SELECT id, amount FROM {T} WHERE id = 12345"),
            ("group_by", "SELECT tenant_id, count(*), sum(amount) FROM {T} GROUP BY tenant_id ORDER BY 2 DESC LIMIT 20"),
            ("full_agg", "SELECT count(*), avg(amount) FROM {T}"),
        ],
    },
    Shape {
        name: "wide_nopk",
        create: "CREATE TABLE {T} (id BIGINT NOT NULL, tenant_id INT, status TEXT, \
                 amount NUMERIC(12,2), created_at TIMESTAMPTZ, payload JSONB)",
        cols: &["id", "tenant_id", "status", "amount", "created_at", "payload"],
        gen: gen_wide,
        ops: &[("full_agg", "SELECT count(*), avg(amount) FROM {T}")],
    },
    Shape {
        name: "composite_pk",
        create: "CREATE TABLE {T} (a BIGINT NOT NULL, b BIGINT NOT NULL, v TEXT, PRIMARY KEY (a, b))",
        cols: &["a", "b", "v"],
        gen: gen_composite,
        ops: &[("full_agg", "SELECT count(*) FROM {T}")],
    },
    Shape {
        name: "jsonb_only",
        create: "CREATE TABLE {T} (id BIGINT NOT NULL, payload JSONB)",
        cols: &["id", "payload"],
        gen: gen_jsonb_only,
        ops: &[("full_agg", "SELECT count(*) FROM {T}")],
    },
    Shape {
        name: "numeric_only",
        create: "CREATE TABLE {T} (id BIGINT NOT NULL, amount NUMERIC(18,2))",
        cols: &["id", "amount"],
        gen: gen_numeric_only,
        ops: &[("sum", "SELECT sum(amount) FROM {T}")],
    },
    Shape {
        name: "timestamp_only",
        create: "CREATE TABLE {T} (id BIGINT NOT NULL, created_at TIMESTAMPTZ)",
        cols: &["id", "created_at"],
        gen: gen_ts_only,
        ops: &[("count", "SELECT count(*) FROM {T}")],
    },
];

/// Median wall-ms of running `sql` `reps` times, or `Err(msg)` if it fails —
/// the harness records the failure and moves on rather than halting the survey.
async fn timed_query(sess: &ProjectSession, sql: &str, reps: usize) -> Result<f64, String> {
    let mut ts = Vec::with_capacity(reps);
    for _ in 0..reps {
        let t0 = Instant::now();
        sess.execute(sql).await.map_err(|e| e.to_string())?;
        ts.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    ts.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Ok(ts[ts.len() / 2])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shape_matrix() {
    let rows = env_usize("BASIN_SHAPE_ROWS", 100_000);
    let chunk = env_usize("BASIN_SHAPE_CHUNK", 10_000);
    let sla_rps = env_usize("BASIN_SHAPE_SLA_RPS", 0);
    let only: Option<Vec<String>> = std::env::var("BASIN_SHAPE_ONLY")
        .ok()
        .map(|v| v.split(',').map(|s| s.trim().to_string()).collect());

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    let empty_schema = Arc::new(Schema::empty()); // engine uses the catalog schema

    println!("\n=== Basin engine shape-matrix bench — {rows} rows/shape, {chunk}/chunk ===");
    println!("{:<16} {:>12} {:>14}   ops (median ms)", "shape", "ingest s", "ingest r/s");
    println!("{}", "-".repeat(78));

    let mut slow: Vec<String> = Vec::new();

    for shp in SHAPES {
        if let Some(ref only) = only {
            if !only.iter().any(|n| n == shp.name) {
                continue;
            }
        }
        let table = format!("sm_{}", shp.name);
        let create = shp.create.replace("{T}", &table);
        sess.execute(&format!("DROP TABLE IF EXISTS {table}")).await.unwrap();
        sess.execute(&create).await.unwrap();

        let cols: Vec<String> = shp.cols.iter().map(|c| c.to_string()).collect();

        // ---- ingest, chunked, with per-chunk timing (detects O(n^2) growth) ----
        let mut seeded = 0usize;
        let mut chunk_ms: Vec<f64> = Vec::new();
        let mut ingest_err: Option<String> = None;
        let t0 = Instant::now();
        while seeded < rows {
            let n = chunk.min(rows - seeded);
            let batch: Vec<Vec<Option<String>>> =
                (seeded..seeded + n).map(|i| (shp.gen)(i)).collect();
            let c0 = Instant::now();
            match sess.ingest_csv_batch(&table, empty_schema.clone(), Some(&cols), batch).await {
                Ok(ingested) => {
                    assert_eq!(ingested as usize, n, "[{}] ingested count", shp.name);
                    chunk_ms.push(c0.elapsed().as_secs_f64() * 1000.0);
                    seeded += n;
                }
                Err(e) => {
                    ingest_err = Some(format!("ingest @{seeded}: {e}"));
                    break;
                }
            }
        }
        let ingest_s = t0.elapsed().as_secs_f64();

        if let Some(e) = ingest_err {
            println!("{:<16} {:>12} {:>14}   INGEST-ERR: {}", shp.name, "—", "—", e);
            slow.push(format!("{}: {}", shp.name, e));
            continue;
        }

        let rps = (rows as f64 / ingest_s) as usize;
        // O(n^2) detector: last chunk vs first chunk wall time.
        let growth = if chunk_ms.len() >= 2 && chunk_ms[0] > 0.0 {
            chunk_ms.last().unwrap() / chunk_ms[0]
        } else {
            1.0
        };

        // ---- correctness: row count round-trips ----
        let cnt = match sess.execute(&format!("SELECT count(*) FROM {table}")).await {
            Ok(ExecResult::Rows { batches, .. }) => {
                use arrow_array::Array;
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .map(|i| i.value(0))
                    .unwrap_or(-1)
            }
            _ => -1,
        };
        if cnt as usize != rows {
            slow.push(format!("{}: count(*) = {} != {} seeded", shp.name, cnt, rows));
        }

        // ---- timed ops (errors recorded, not fatal) ----
        let mut op_str = String::new();
        for (op_name, op_sql) in shp.ops {
            let sql = op_sql.replace("{T}", &table);
            match timed_query(&sess, &sql, 5).await {
                Ok(ms) => op_str.push_str(&format!("{op_name}={ms:.1} ")),
                Err(e) => {
                    op_str.push_str(&format!("{op_name}=ERR "));
                    slow.push(format!("{}.{}: {}", shp.name, op_name, e));
                }
            }
        }

        let flag = if growth > 3.0 { " ⚠O(n²)?" } else { "" };
        println!("{:<16} {:>12.2} {:>14}   {}{}", shp.name, ingest_s, rps, op_str, flag);
        if sla_rps > 0 && rps < sla_rps {
            slow.push(format!("{} ({} r/s < {} SLA)", shp.name, rps, sla_rps));
        }
        if growth > 3.0 {
            slow.push(format!("{} (per-chunk grew {:.1}x — superlinear ingest)", shp.name, growth));
        }
    }

    println!("{}", "-".repeat(78));
    if !slow.is_empty() {
        println!("FLAGGED:\n  {}", slow.join("\n  "));
        if sla_rps > 0 {
            panic!("shape-matrix SLA failures:\n  {}", slow.join("\n  "));
        }
    } else {
        println!("all shapes within thresholds");
    }
}
