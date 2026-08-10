//! Basin vs Postgres 18 — extended structural-shape head-to-head.
//!
//! ## Taxonomy & coverage manifest
//!
//! The core `compare_postgres_*` cards already time 2-table / 3-table / star /
//! LATERAL joins, UNION / UNION ALL / INTERSECT / EXCEPT (at small rstress
//! scale), correlated subqueries (LIMIT 100), GROUP BY user_id, and
//! GROUP BY + HAVING. This file fills the **structural** gaps the audit and
//! prompt call out that are NOT timed anywhere head-to-head:
//!
//!   Family: JOIN VARIANTS
//!     J1  LEFT JOIN with NULL-side filter (anti-join via WHERE r.id IS NULL)
//!     J2  self-join (events e1 ⋈ events e2 ON e1.user_id = e2.user_id, banded)
//!
//!   Family: GROUP-BY CARDINALITY SWEEP  (audit gap #17 — columnar advantage
//!           should grow with group count; never run head-to-head)
//!     G1  ~10 groups     (GROUP BY status×… → low card)
//!     G2  ~10k groups    (GROUP BY user_id % 10000)
//!     G3  ~500k groups   (GROUP BY id % 500000 → near-unique)
//!
//!   Family: SET OPS AT SCALE  (audit gap #18 — robustness suite runs these at
//!           ~2k rows; the sort/hash behavior only diverges at scale)
//!     S1  INTERSECT of two large filtered scans
//!     S2  EXCEPT of two large filtered scans
//!     S3  UNION (dedup) of two large overlapping scans
//!
//!   Family: JSON AGGREGATION  (audit gap #11 — the aggregation *side* of JSONB
//!           is entirely unmeasured vs PG; read side is covered by core #28-37)
//!     A1  json_agg(payload) GROUP BY user_id  (row packing)
//!     A2  jsonb_agg(status ORDER BY id) GROUP BY user_id%100
//!
//!   Family: SELECTIVITY / CONTAINMENT
//!     C1  jsonb @> AND-of-two-needles (category AND device.os)  — gap: core
//!         only times single-needle @>
//!     C2  sparse @> needle (deep value present in ~0.5% of rows) — audit gap
//!         #12 (the documented dense-needle loss is known; sparse is not)
//!     C3  correlated subquery scaled to 1000 parent rows (audit gap #16)
//!
//! Rationale: GROUP BY cardinality is the canonical OLAP axis where a columnar
//! engine should pull away from PG; set-ops at scale expose hash/sort memory
//! behavior; json_agg is the unmeasured write-shaped JSONB path; the LEFT-JOIN
//! anti-join and self-join are the two join topologies the core suite skips.
//!
//! ## Harness idiom — identical to `shape_sweeps.rs` / the scalar ext card:
//! seed both engines with byte-identical data, flush+quiesce Basin, symmetric
//! warm-up, `samples` timed iterations, correctness cross-check (scalar or row
//! count) BEFORE timing, unsupported shapes recorded not crashed. Writes the
//! `structural` section of `benchmark/data/compare_postgres_ext.json`.
//!
//! ## Running
//! ```text
//! cargo test -p basin-integration-tests --test compare_postgres_ext_structural \
//!   -- --ignored --nocapture
//! ```
//!
//! ## Env knobs
//! * `BASIN_EXT_STRUCT_ROWS`    — rows (default 200_000; G3 needs >500k groups
//!                                so id space, not row count, drives card G)
//! * `BASIN_EXT_STRUCT_SAMPLES` — samples per shape (default 30 — these are
//!                                heavier full-scan/aggregate shapes)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::time::Instant;

use basin_common::ProjectId;
use basin_engine::ExecResult;
use serde_json::json;
use tokio_postgres::SimpleQueryMessage;

#[path = "compare_postgres_common.rs"]
mod common;

use common::{
    build_basin_engine, median, payload_for, percentile, status_for, try_connect, SchemaGuard,
};

#[path = "compare_postgres_ext_util.rs"]
mod ext_util;
use ext_util::{env_usize, merge_artifact, now_secs};

const EPOCH: i64 = 1_700_000_000;

fn row_count(res: &ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

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

struct Shape {
    label: &'static str,
    sql: String,
    /// true if the shape returns a single scalar (cross-check on value);
    /// false → cross-check on row count.
    scalar: bool,
    basin_p50: f64,
    basin_p99: f64,
    pg_p50: f64,
    pg_p99: f64,
    supported: bool,
    note: String,
}

fn shape(label: &'static str, sql: &str, scalar: bool) -> Shape {
    Shape {
        label,
        sql: sql.to_string(),
        scalar,
        basin_p50: f64::NAN,
        basin_p99: f64::NAN,
        pg_p50: f64::NAN,
        pg_p99: f64::NAN,
        supported: true,
        note: String::new(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "PG head-to-head card: needs idle box + live Postgres; run via benchmark/run/realistic-suite.sh"]
async fn ext_structural_shapes() {
    let rows = env_usize("BASIN_EXT_STRUCT_ROWS", 200_000) as i64;
    let samples = env_usize("BASIN_EXT_STRUCT_SAMPLES", 30);
    eprintln!("[ext-struct] config: rows={rows} samples={samples}");

    // ── Basin seed ──────────────────────────────────────────────────────────
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
            payload JSONB)",
    )
    .await
    .unwrap();

    let seed_tuple = |k: i64| -> String {
        let status = status_for(k);
        format!(
            "({k},{},{},'{status}',{},'{}')",
            k % 1000,
            0.5 + (k % 1000) as f64,
            EPOCH + k,
            payload_for(k),
        )
    };

    eprintln!("[ext-struct] seeding {rows} rows ...");
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

    // ── Shapes ────────────────────────────────────────────────────────────
    // J1: LEFT JOIN anti-join. Self-join events against a derived "recent"
    //     subset; rows with no match are the anti-join result.
    // The half-open band keeps the self-join from being O(n^2).
    let half = rows / 2;
    let mut shapes: Vec<Shape> = vec![
        // ── JOIN VARIANTS ──
        shape(
            "J1 LEFT JOIN NULL-side anti-join",
            &format!(
                "SELECT COUNT(*) FROM events e \
                 LEFT JOIN (SELECT id FROM events WHERE id < {half}) r ON e.id = r.id \
                 WHERE r.id IS NULL"
            ),
            true,
        ),
        shape(
            "J2 self-join banded (user_id eq)",
            "SELECT COUNT(*) FROM events e1 JOIN events e2 \
             ON e1.user_id = e2.user_id AND e2.id = e1.id + 1 \
             WHERE e1.id < 50000",
            true,
        ),
        // ── GROUP BY CARDINALITY SWEEP ──
        shape(
            "G1 GROUP BY ~10 groups (status×2)",
            "SELECT status, (created_at % 2) AS p, COUNT(*), SUM(amount) \
             FROM events GROUP BY status, p ORDER BY status, p",
            false,
        ),
        shape(
            "G2 GROUP BY ~10k groups (user_id%10000)",
            "SELECT user_id % 10000 AS g, COUNT(*), SUM(amount) \
             FROM events GROUP BY g ORDER BY g LIMIT 20",
            false,
        ),
        shape(
            "G3 GROUP BY ~500k groups (id%500000)",
            "SELECT id % 500000 AS g, COUNT(*) \
             FROM events GROUP BY g ORDER BY g LIMIT 20",
            false,
        ),
        // ── SET OPS AT SCALE ──
        shape(
            "S1 INTERSECT two large scans",
            &format!(
                "SELECT COUNT(*) FROM (\
                   SELECT id FROM events WHERE id < {} \
                   INTERSECT \
                   SELECT id FROM events WHERE id >= {}) x",
                rows * 3 / 4,
                rows / 4
            ),
            true,
        ),
        shape(
            "S2 EXCEPT two large scans",
            &format!(
                "SELECT COUNT(*) FROM (\
                   SELECT id FROM events WHERE id < {} \
                   EXCEPT \
                   SELECT id FROM events WHERE id >= {}) x",
                rows * 3 / 4,
                rows / 4
            ),
            true,
        ),
        shape(
            "S3 UNION dedup two overlapping scans",
            &format!(
                "SELECT COUNT(*) FROM (\
                   SELECT id FROM events WHERE id < {} \
                   UNION \
                   SELECT id FROM events WHERE id >= {}) x",
                rows * 3 / 4,
                rows / 4
            ),
            true,
        ),
        // ── JSON AGGREGATION ──
        shape(
            "A1 json_agg(payload) GROUP BY user_id%100",
            "SELECT user_id % 100 AS g, json_agg(payload) \
             FROM events GROUP BY g ORDER BY g LIMIT 10",
            false,
        ),
        shape(
            "A2 jsonb_agg(status ORDER BY id) GROUP BY",
            "SELECT user_id % 100 AS g, jsonb_agg(status ORDER BY id) \
             FROM events GROUP BY g ORDER BY g LIMIT 10",
            false,
        ),
        // ── SELECTIVITY / CONTAINMENT ──
        shape(
            "C1 @> AND-of-two-needles",
            "SELECT COUNT(*) FROM events \
             WHERE payload @> '{\"category\":\"purchase\"}' \
               AND payload @> '{\"device\":{\"os\":\"ios\"}}'",
            true,
        ),
        shape(
            "C2 @> sparse needle (~deep value)",
            "SELECT COUNT(*) FROM events \
             WHERE payload @> '{\"metadata\":{\"campaign\":\"promo_2024\"}}'",
            true,
        ),
        shape(
            "C3 correlated subquery x1000 parents",
            "SELECT p.id, (SELECT COUNT(*) FROM events c WHERE c.user_id = p.user_id) AS cnt \
             FROM events p WHERE p.id < 1000 ORDER BY p.id",
            false,
        ),
    ];

    // ── Basin probe (support) + capture oracle values ──────────────────────
    let mut basin_scalar: Vec<Option<i64>> = Vec::with_capacity(shapes.len());
    let mut basin_rows: Vec<usize> = Vec::with_capacity(shapes.len());
    for sh in shapes.iter_mut() {
        match sess.execute(&sh.sql).await {
            Ok(res) => {
                basin_scalar.push(first_i64(&res));
                basin_rows.push(row_count(&res));
            }
            Err(e) => {
                sh.supported = false;
                sh.note = format!("basin unsupported: {e}");
                basin_scalar.push(None);
                basin_rows.push(0);
                eprintln!("[ext-struct] UNSUPPORTED {}: {e}", sh.label);
            }
        }
    }

    // ── Time Basin ──────────────────────────────────────────────────────────
    for sh in shapes.iter_mut() {
        if !sh.supported {
            continue;
        }
        let _ = sess.execute(&sh.sql).await; // warm-up
        let mut samp = Vec::with_capacity(samples);
        let mut ok = true;
        for _ in 0..samples {
            let t = Instant::now();
            if sess.execute(&sh.sql).await.is_err() {
                ok = false;
                break;
            }
            samp.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        if ok && !samp.is_empty() {
            sh.basin_p50 = median(&samp);
            sh.basin_p99 = percentile(&samp, 99.0);
            println!(
                "[ext-struct] Basin {:<40}: p50={:.3}ms p99={:.3}ms",
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
        let schema = format!("basin_extstruct_{suffix}");
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
                status TEXT, created_at BIGINT, payload JSONB)"
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

        let qualify = |sql: &str| sql.replace("events", &format!("{schema}.events"));

        for (i, sh) in shapes.iter_mut().enumerate() {
            if !sh.supported {
                continue;
            }
            let pg_q = qualify(&sh.sql);

            // Correctness cross-check before timing.
            if sh.scalar {
                if let Some(bscalar) = basin_scalar[i] {
                    if let Ok(msgs) = pg.simple_query(&pg_q).await {
                        if let Some(pgv) = first_pg_i64(&msgs) {
                            assert_eq!(
                                bscalar, pgv,
                                "[ext-struct] {} scalar mismatch: basin={bscalar} pg={pgv}",
                                sh.label
                            );
                        }
                    }
                }
            } else if let Ok(msgs) = pg.simple_query(&pg_q).await {
                let pg_rows = msgs
                    .iter()
                    .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
                    .count();
                assert_eq!(
                    basin_rows[i], pg_rows,
                    "[ext-struct] {} rowcount mismatch: basin={} pg={pg_rows}",
                    sh.label, basin_rows[i]
                );
            }

            let _ = pg.simple_query(&pg_q).await; // warm-up
            let mut samp = Vec::with_capacity(samples);
            for _ in 0..samples {
                let t = Instant::now();
                let _ = pg.simple_query(&pg_q).await.expect("pg ext-struct");
                samp.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            sh.pg_p50 = median(&samp);
            sh.pg_p99 = percentile(&samp, 99.0);
            println!(
                "[ext-struct] PG    {:<40}: p50={:.3}ms p99={:.3}ms",
                sh.label, sh.pg_p50, sh.pg_p99
            );
        }
        std::mem::forget(_guard);
    } else {
        eprintln!("[ext-struct] PG unavailable — Basin-only results");
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
        "structural",
        json!({
            "section": "structural",
            "generated_at": format!("@{}", now_secs()),
            "config": { "rows": rows, "samples": samples },
            "families": ["join_variants", "groupby_cardinality", "set_ops_scale", "json_agg", "selectivity_containment"],
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

fn first_pg_i64(msgs: &[SimpleQueryMessage]) -> Option<i64> {
    for m in msgs {
        if let SimpleQueryMessage::Row(r) = m {
            if let Some(s) = r.get(0) {
                if let Ok(v) = s.trim().parse::<i64>() {
                    return Some(v);
                }
                return None;
            }
        }
    }
    None
}
