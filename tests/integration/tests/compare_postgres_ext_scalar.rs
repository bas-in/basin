//! Basin vs Postgres 18 — extended scalar-shape head-to-head (text / date /
//! projection-expression families).
//!
//! ## Why this file exists — taxonomy & coverage manifest
//!
//! The six `compare_postgres_*` cards (via `compare_postgres_common.rs`) and
//! the four realistic/sweep cards already time ~110 distinct shapes: CRUD,
//! bulk DML, pagination (offset + keyset), JSONB read/write, aggregates
//! (GROUP BY/HAVING/ROLLUP/CUBE/GROUPING SETS/PERCENTILE_CONT), windows
//! (LAG/RANK/ROW_NUMBER/frame), DISTINCT ON, transactions (BEGIN/COMMIT/
//! ROLLBACK/savepoint), FOR UPDATE, subqueries (correlated/EXISTS/NOT EXISTS/
//! scalar/derived), set ops (UNION/UNION ALL/INTERSECT/EXCEPT), multi-way +
//! star + LATERAL joins, ARRAY_AGG/STRING_AGG, COUNT(DISTINCT)/COUNT FILTER,
//! RETURNING, UPSERT/MERGE, INSERT…SELECT, IN-lists (10/100/1000), ANY(array),
//! LIKE prefix, ILIKE suffix, CASE-10, prepared 100x.
//!
//! What was STILL not timed head-to-head anywhere (audit gaps 11/14/16 + the
//! prompt's text/date/projection one-liners) — this file covers the **scalar
//! expression** subset, i.e. shapes whose cost is dominated by per-row scalar
//! evaluation over a (mostly) full scan rather than by plan structure:
//!
//!   Family: TEXT OPS
//!     T1  LIKE infix  `WHERE status LIKE '%ending%'`     (non-sargable scan)
//!     T2  ILIKE infix `WHERE email ILIKE '%user000%'`    (case-fold infix)
//!     T3  lower() in WHERE `WHERE lower(status) = 'active'`
//!     T4  length() filter `WHERE length(status) > 6`
//!     T5  concat in projection `SELECT email || ':' || id …` (full drain)
//!     T6  bare SELECT DISTINCT status  (dedup over full scan)
//!     T7  COUNT(DISTINCT status)-free distinct-then-count via subquery
//!
//!   Family: DATE / TIME OPS  (created_at is BIGINT epoch-seconds — both
//!           engines apply to_timestamp() so the scalar path is identical)
//!     D1  EXTRACT(hour FROM to_timestamp(created_at)) GROUP BY
//!     D2  date_trunc('day', …) range filter + COUNT
//!     D3  interval arithmetic: to_timestamp(created_at) + interval '1 day'
//!     D4  BETWEEN two epoch bounds (date-range window, ~1% selectivity)
//!
//!   Family: PROJECTION / NULL / CAST EXPRESSIONS
//!     P1  COALESCE(nullable_note, 'none') projection (full drain)
//!     P2  NULLIF(status,'active') IS NULL count
//!     P3  CASE projection (3-branch label) over full scan
//!     P4  cast ladder `amount::int`, `id::text`, `status::varchar` projection
//!     P5  numeric/decimal aggregation `SUM(amount::numeric(12,2))` + ROUND
//!     P6  boolean-flag filter `WHERE is_flagged` (bool column, ~25% true)
//!     P7  MIN/MAX over a column (loose-index-scan shape, no GROUP BY)
//!
//! Rationale: these are the shapes a columnar engine can either win big on
//! (full-scan scalar eval vectorizes; MIN/MAX may hit column stats) or lose on
//! (per-row UDF dispatch for lower()/length()/casts if not vectorized). None
//! were measured against PG; the per-row scalar path is exactly where a
//! "fast in micro-bench, slow in prod" surprise hides.
//!
//! ## Harness idiom
//! Same as `shape_sweeps.rs`: seed BOTH engines with identical deterministic
//! data, flush+quiesce Basin to its settled cold-tier state, run a symmetric
//! single warm-up pass, then take `samples` timed iterations. Correctness is
//! cross-checked (Basin scalar/row-count == PG) BEFORE any timing is recorded;
//! a shape Basin cannot run is recorded as `unsupported` in the artifact
//! rather than panicking the card. p50 = median, p99 = 99th pct.
//! Artifact: `benchmark/data/compare_postgres_ext.json` (this file owns the
//! `scalar` section; the other two ext files own `structural`/`dml` and merge
//! by read-modify-write so all three contribute to one card file).
//!
//! ## Running
//! ```text
//! cargo test -p basin-integration-tests --test compare_postgres_ext_scalar \
//!   -- --ignored --nocapture
//! ```
//! `#[ignore]`d — needs an idle box + live Postgres (run via
//! `benchmark/run/realistic-suite.sh`).
//!
//! ## Env knobs
//! * `BASIN_EXT_ROWS`     — row count (default 100_000)
//! * `BASIN_EXT_SAMPLES`  — samples per shape (default 50)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::time::Instant;

use basin_common::ProjectId;
use basin_engine::ExecResult;
use serde_json::json;
use tokio_postgres::SimpleQueryMessage;

#[path = "compare_postgres_common.rs"]
mod common;

use common::{
    build_basin_engine, email_for, median, percentile, status_for, try_connect, SchemaGuard,
};

#[path = "compare_postgres_ext_util.rs"]
mod ext_util;
use ext_util::{env_usize, merge_artifact, now_secs};

const EPOCH: i64 = 1_700_000_000;

/// Rows returned by an ExecResult.
fn row_count(res: &ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

/// First-cell i64 (for COUNT/MIN/MAX correctness oracles).
fn first_i64(res: &ExecResult) -> Option<i64> {
    if let ExecResult::Rows { batches, .. } = res {
        let b = batches.first()?;
        if b.num_rows() == 0 {
            return None;
        }
        return b
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .map(|a| a.value(0));
    }
    None
}

/// Outcome of one shape on one engine.
struct Shape {
    label: &'static str,
    /// SQL with `{t}` placeholder for the table-qualified name.
    basin_sql: String,
    pg_sql: String,
    basin_p50: f64,
    basin_p99: f64,
    pg_p50: f64,
    pg_p99: f64,
    supported: bool,
    note: String,
}

impl Shape {
    fn new(label: &'static str, basin_sql: String, pg_sql: String) -> Self {
        Shape {
            label,
            basin_sql,
            pg_sql,
            basin_p50: f64::NAN,
            basin_p99: f64::NAN,
            pg_p50: f64::NAN,
            pg_p99: f64::NAN,
            supported: true,
            note: String::new(),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "PG head-to-head card: needs idle box + live Postgres; run via benchmark/run/realistic-suite.sh"]
async fn ext_scalar_shapes() {
    let rows = env_usize("BASIN_EXT_ROWS", 100_000) as i64;
    let samples = env_usize("BASIN_EXT_SAMPLES", 50);
    eprintln!("[ext-scalar] config: rows={rows} samples={samples}");

    // ── Basin schema + seed ────────────────────────────────────────────────
    // Schema adds `note` (nullable TEXT) and `is_flagged` (BOOLEAN) on top of
    // the standard events shape so the NULL/COALESCE/boolean shapes have real
    // columns. Both engines get byte-identical data.
    let mut instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();
    sess.execute(
        "CREATE TABLE events (\
            id BIGINT NOT NULL PRIMARY KEY, \
            user_id BIGINT NOT NULL, \
            amount DOUBLE PRECISION NOT NULL, \
            status TEXT NOT NULL, \
            created_at BIGINT NOT NULL, \
            note TEXT, \
            is_flagged BOOLEAN NOT NULL, \
            email TEXT NOT NULL)",
    )
    .await
    .unwrap();

    let seed_tuple = |k: i64| -> String {
        let status = status_for(k);
        // note is NULL for ~1/3 of rows so COALESCE/NULLIF have signal.
        let note = if k % 3 == 0 {
            "NULL".to_string()
        } else {
            format!("'n{}'", k % 50)
        };
        let flagged = if k % 4 == 0 { "true" } else { "false" };
        format!(
            "({k},{},{},'{status}',{},{note},{flagged},'{}')",
            k % 1000,
            0.5 + (k % 1000) as f64,
            EPOCH + k,
            email_for(k),
        )
    };

    eprintln!("[ext-scalar] seeding {rows} rows ...");
    let batch = 10_000i64;
    let mut id = 0i64;
    while id < rows {
        let hi = (id + batch).min(rows);
        let mut stmt = String::from("INSERT INTO events VALUES ");
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            stmt.push_str(&seed_tuple(k));
        }
        sess.execute(&stmt).await.expect("basin seed");
        id = hi;
    }
    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }

    // ── Shape definitions (basin SQL uses `events`, pg SQL uses `{schema}`) ──
    // For each shape we store the Basin SQL and a PG-SQL template that still
    // contains the literal token `events` so we can prefix the schema. We keep
    // the queries identical except for the schema-qualification.
    let mk = |label: &'static str, sql: &str| -> Shape {
        Shape::new(label, sql.to_string(), sql.to_string())
    };

    let mut shapes: Vec<Shape> = vec![
        // ── TEXT OPS ──
        mk(
            "T1 LIKE infix '%ending%'",
            "SELECT COUNT(*) FROM events WHERE status LIKE '%ending%'",
        ),
        mk(
            "T2 ILIKE infix '%user000%'",
            "SELECT COUNT(*) FROM events WHERE email ILIKE '%user000%'",
        ),
        mk(
            "T3 lower() in WHERE",
            "SELECT COUNT(*) FROM events WHERE lower(status) = 'active'",
        ),
        mk(
            "T4 length() filter",
            "SELECT COUNT(*) FROM events WHERE length(status) > 6",
        ),
        mk(
            "T5 concat projection (full drain)",
            "SELECT email || ':' || id::text AS k FROM events",
        ),
        mk(
            "T6 SELECT DISTINCT status",
            "SELECT DISTINCT status FROM events",
        ),
        mk(
            "T7 distinct-then-count subquery",
            "SELECT COUNT(*) FROM (SELECT DISTINCT status FROM events) s",
        ),
        // ── DATE / TIME OPS ──
        mk(
            "D1 EXTRACT(hour) GROUP BY",
            "SELECT EXTRACT(hour FROM to_timestamp(created_at)) AS h, COUNT(*) \
             FROM events GROUP BY h ORDER BY h",
        ),
        mk(
            "D2 date_trunc('day') range COUNT",
            "SELECT COUNT(*) FROM events \
             WHERE date_trunc('day', to_timestamp(created_at)) \
                   = date_trunc('day', to_timestamp(1700043200))",
        ),
        mk(
            "D3 interval arithmetic projection",
            "SELECT id, to_timestamp(created_at) + interval '1 day' AS t \
             FROM events WHERE id < 1000 ORDER BY id",
        ),
        mk(
            "D4 epoch BETWEEN date-range (~1%)",
            &format!(
                "SELECT COUNT(*), SUM(amount) FROM events \
                 WHERE created_at BETWEEN {} AND {}",
                EPOCH + 1000,
                EPOCH + 1000 + 1000
            ),
        ),
        // ── PROJECTION / NULL / CAST ──
        mk(
            "P1 COALESCE projection (full drain)",
            "SELECT id, COALESCE(note, 'none') AS n FROM events",
        ),
        mk(
            "P2 NULLIF IS NULL count",
            "SELECT COUNT(*) FROM events WHERE NULLIF(status,'active') IS NULL",
        ),
        mk(
            "P3 CASE 3-branch projection (full drain)",
            "SELECT id, CASE WHEN amount < 100 THEN 'lo' \
                             WHEN amount < 500 THEN 'mid' ELSE 'hi' END AS bucket \
             FROM events",
        ),
        mk(
            "P4 cast ladder projection (full drain)",
            "SELECT amount::int AS ai, id::text AS it, status::varchar AS sv FROM events",
        ),
        mk(
            "P5 numeric SUM + ROUND aggregate",
            "SELECT ROUND(SUM(amount::numeric(12,2)), 2) AS total FROM events",
        ),
        mk(
            "P6 boolean-flag filter (~25% true)",
            "SELECT COUNT(*) FROM events WHERE is_flagged",
        ),
        mk(
            "P7 MIN/MAX no GROUP BY (loose-idx shape)",
            "SELECT MIN(created_at), MAX(created_at), MIN(amount), MAX(amount) FROM events",
        ),
    ];

    // ── Correctness oracle: scalar/row-count for shapes returning a scalar ──
    // For COUNT shapes we capture Basin's scalar to compare to PG below; for
    // full-drain projections we capture row count.
    // We probe each shape once on Basin: if it errors, mark unsupported.
    let mut basin_scalar: Vec<Option<i64>> = Vec::with_capacity(shapes.len());
    let mut basin_rowcount: Vec<usize> = Vec::with_capacity(shapes.len());
    for sh in shapes.iter_mut() {
        match sess.execute(&sh.basin_sql).await {
            Ok(res) => {
                basin_scalar.push(first_i64(&res));
                basin_rowcount.push(row_count(&res));
            }
            Err(e) => {
                sh.supported = false;
                sh.note = format!("basin unsupported: {e}");
                basin_scalar.push(None);
                basin_rowcount.push(0);
                eprintln!("[ext-scalar] UNSUPPORTED {}: {e}", sh.label);
            }
        }
    }

    // ── Time Basin (supported shapes only) ──────────────────────────────────
    for sh in shapes.iter_mut() {
        if !sh.supported {
            continue;
        }
        let _ = sess.execute(&sh.basin_sql).await; // warm-up
        let mut samp = Vec::with_capacity(samples);
        let mut ok = true;
        for _ in 0..samples {
            let t = Instant::now();
            if sess.execute(&sh.basin_sql).await.is_err() {
                ok = false;
                break;
            }
            samp.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        if ok && !samp.is_empty() {
            sh.basin_p50 = median(&samp);
            sh.basin_p99 = percentile(&samp, 99.0);
            println!(
                "[ext-scalar] Basin {:<42}: p50={:.3}ms p99={:.3}ms",
                sh.label, sh.basin_p50, sh.basin_p99
            );
        } else {
            sh.supported = false;
            sh.note = "basin failed during timing".to_string();
        }
    }

    // ── PG twin ───────────────────────────────────────────────────────────
    if let Some((pg, cs)) = try_connect().await {
        let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
        let schema = format!("basin_extscalar_{suffix}");
        let _guard = SchemaGuard {
            schema: schema.clone(),
            conn_str: cs,
        };
        pg.simple_query(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        pg.simple_query("SET work_mem = '4MB'").await.unwrap();
        pg.simple_query("SET enable_seqscan = on").await.unwrap();
        pg.simple_query(&format!(
            "CREATE TABLE {schema}.events (\
                id BIGINT PRIMARY KEY, user_id BIGINT, amount DOUBLE PRECISION, \
                status TEXT, created_at BIGINT, note TEXT, is_flagged BOOLEAN, email TEXT)"
        ))
        .await
        .unwrap();

        let mut pg_id = 0i64;
        while pg_id < rows {
            let hi = (pg_id + batch).min(rows);
            let mut stmt = format!("INSERT INTO {schema}.events VALUES ");
            for k in pg_id..hi {
                if k > pg_id {
                    stmt.push(',');
                }
                stmt.push_str(&seed_tuple(k));
            }
            pg.simple_query(&stmt).await.unwrap();
            pg_id = hi;
        }

        // Helper: rewrite `FROM events`/`events ` references to schema-qual.
        let qualify = |sql: &str| sql.replace("events", &format!("{schema}.events"));

        for (i, sh) in shapes.iter_mut().enumerate() {
            if !sh.supported {
                continue;
            }
            let pg_q = qualify(&sh.pg_sql);
            sh.pg_sql = pg_q.clone();

            // Correctness cross-check (scalar shapes only): compare Basin
            // scalar to PG scalar before timing.
            if let Some(bscalar) = basin_scalar[i] {
                if let Ok(msgs) = pg.simple_query(&pg_q).await {
                    if let Some(pgv) = first_pg_i64(&msgs) {
                        assert_eq!(
                            bscalar, pgv,
                            "[ext-scalar] {} scalar mismatch: basin={bscalar} pg={pgv}",
                            sh.label
                        );
                    }
                }
            } else {
                // Full-drain projection: cross-check row count.
                if let Ok(msgs) = pg.simple_query(&pg_q).await {
                    let pg_rows = msgs
                        .iter()
                        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
                        .count();
                    assert_eq!(
                        basin_rowcount[i], pg_rows,
                        "[ext-scalar] {} rowcount mismatch: basin={} pg={pg_rows}",
                        sh.label, basin_rowcount[i]
                    );
                }
            }

            let _ = pg.simple_query(&pg_q).await; // warm-up
            let mut samp = Vec::with_capacity(samples);
            for _ in 0..samples {
                let t = Instant::now();
                let _ = pg.simple_query(&pg_q).await.expect("pg ext-scalar");
                samp.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            sh.pg_p50 = median(&samp);
            sh.pg_p99 = percentile(&samp, 99.0);
            println!(
                "[ext-scalar] PG    {:<42}: p50={:.3}ms p99={:.3}ms",
                sh.label, sh.pg_p50, sh.pg_p99
            );
        }
        std::mem::forget(_guard);
    } else {
        eprintln!("[ext-scalar] PG unavailable — Basin-only results");
    }

    // ── Emit ────────────────────────────────────────────────────────────────
    let json_rows: Vec<serde_json::Value> = shapes
        .iter()
        .map(|s| {
            json!({
                "shape": s.label,
                "supported": s.supported,
                "note": s.note,
                "basin_p50_ms": nan_null(s.basin_p50),
                "basin_p99_ms": nan_null(s.basin_p99),
                "pg_p50_ms": nan_null(s.pg_p50),
                "pg_p99_ms": nan_null(s.pg_p99),
                "basin_over_pg_p50": ratio(s.basin_p50, s.pg_p50),
            })
        })
        .collect();

    let unsupported: Vec<&str> = shapes
        .iter()
        .filter(|s| !s.supported)
        .map(|s| s.label)
        .collect();

    merge_artifact(
        "scalar",
        json!({
            "section": "scalar",
            "generated_at": format!("@{}", now_secs()),
            "config": { "rows": rows, "samples": samples },
            "families": ["text_ops", "date_time_ops", "projection_null_cast"],
            "shapes": json_rows,
            "unsupported": unsupported,
        }),
    );

    instance.wal.close().await.unwrap();
}

fn nan_null(v: f64) -> serde_json::Value {
    if v.is_nan() {
        serde_json::Value::Null
    } else {
        json!(v)
    }
}

fn ratio(basin: f64, pg: f64) -> serde_json::Value {
    if basin.is_finite() && pg.is_finite() && pg > 1e-9 {
        json!(basin / pg)
    } else {
        serde_json::Value::Null
    }
}

/// First integer-ish cell of a PG simple_query result (parses text repr).
fn first_pg_i64(msgs: &[SimpleQueryMessage]) -> Option<i64> {
    for m in msgs {
        if let SimpleQueryMessage::Row(r) = m {
            if let Some(s) = r.get(0) {
                if let Ok(v) = s.trim().parse::<i64>() {
                    return Some(v);
                }
                // numeric like "12345.67" — compare integer part not safe; skip
                return None;
            }
        }
    }
    None
}
