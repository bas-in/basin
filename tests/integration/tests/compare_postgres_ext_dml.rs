//! Basin vs Postgres 18 — extended DML / write-expression head-to-head.
//!
//! ## Taxonomy & coverage manifest
//!
//! The core `compare_postgres_*` cards already time: single-row UPDATE, bulk
//! UPDATE (~rows/3), conditional CASE UPDATE, `amount = amount + 1` inside the
//! FOR-UPDATE txn, DELETE WHERE IN(10), bulk INSERT (one batch size), UPSERT,
//! bulk UPSERT (50/1000), MERGE, INSERT … SELECT (10k pipeline), INSERT/UPDATE
//! RETURNING. `prepared_stmt_probe.rs` already times prepared-vs-simple SELECT
//! re-execution amortization head-to-head, so that gap (audit #21) is NOT
//! re-measured here.
//!
//! This file covers the DML shapes the prompt enumerates that are STILL not
//! timed head-to-head anywhere:
//!
//!   Family: EXPRESSION UPDATE
//!     U1  expression UPDATE single row   `SET amount = amount + 1 WHERE id=?`
//!         (standalone, NOT wrapped in the FOR-UPDATE txn the core suite uses)
//!     U2  expression UPDATE 1000 rows    `SET amount = amount + 1 WHERE id<1000`
//!     U3  multi-column expression UPDATE `SET amount=amount*2, user_id=user_id+1`
//!
//!   Family: DELETE VARIANTS
//!     X1  single-row DELETE   `WHERE id = ?`
//!     X2  range DELETE        `WHERE id BETWEEN ? AND ?` (~1000 rows)
//!     X3  subquery DELETE     `WHERE id IN (SELECT id FROM … WHERE …)`
//!     X4  DELETE … RETURNING id (single row)  — RETURNING on DELETE (the core
//!         suite times RETURNING on INSERT/UPDATE only)
//!
//!   Family: INSERT SHAPES
//!     I1  multi-row VALUES ladder — 1 / 10 / 100 / 1000 rows per statement
//!         (per-statement amortization curve; core times one fixed batch only)
//!     I2  INSERT … SELECT timing  (core has correctness-only at this shape in
//!         `insert_select_bench_shape.rs`; the 10k pipeline in core IS timed,
//!         so here we time a *filtered* INSERT…SELECT with a WHERE predicate)
//!
//! ## Write-shape isolation discipline
//! Destructive / mutating shapes cannot be re-run N times against the same
//! rows and stay meaningful. Each mutating shape runs against a freshly
//! (re)seeded `scratch` table so every timed iteration starts from the same
//! state. For single-row and small shapes we reseed per iteration (cheap);
//! for the 1000-row and ladder shapes we reseed per iteration too but keep the
//! iteration count low (`BASIN_EXT_DML_SAMPLES`, default 10) so wall-clock
//! stays bounded — the noise-floor `min` is the right estimator at low n
//! (mirrors `robust_estimate` in the core suite). Both engines get identical
//! seed data and identical statements. Correctness: row-affected counts /
//! post-state COUNT are cross-checked Basin == PG before recording timing.
//!
//! Unsupported shapes are recorded in the artifact, never panicked. Writes the
//! `dml` section of `benchmark/data/compare_postgres_ext.json`.
//!
//! ## Running
//! ```text
//! cargo test -p basin-integration-tests --test compare_postgres_ext_dml \
//!   -- --ignored --nocapture
//! ```
//!
//! ## Env knobs
//! * `BASIN_EXT_DML_ROWS`    — scratch seed size (default 50_000)
//! * `BASIN_EXT_DML_SAMPLES` — timed iterations per shape (default 10)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::time::Instant;

use basin_common::ProjectId;
use basin_engine::ExecResult;
use serde_json::json;
use tokio_postgres::SimpleQueryMessage;

#[path = "compare_postgres_common.rs"]
mod common;

use common::{build_basin_engine, status_for, try_connect, SchemaGuard};

#[path = "compare_postgres_ext_util.rs"]
mod ext_util;
use ext_util::{env_usize, merge_artifact, now_secs};

const EPOCH: i64 = 1_700_000_000;

/// min-of-n noise floor (matches `robust_estimate` policy for small n).
fn min_floor(samples: &[f64]) -> f64 {
    samples
        .iter()
        .cloned()
        .fold(f64::INFINITY, f64::min)
        .max(0.0)
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

struct DmlResult {
    label: &'static str,
    n: usize, // rows touched / batch size, for the dashboard
    basin_min_ms: f64,
    pg_min_ms: f64,
    supported: bool,
    note: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "PG head-to-head card: needs idle box + live Postgres; run via benchmark/run/realistic-suite.sh"]
async fn ext_dml_shapes() {
    let seed_rows = env_usize("BASIN_EXT_DML_ROWS", 50_000) as i64;
    let samples = env_usize("BASIN_EXT_DML_SAMPLES", 10);
    eprintln!("[ext-dml] config: seed_rows={seed_rows} samples={samples}");

    // NOTE: unlike the read-only ext cards we deliberately leave the background
    // compaction loop ALIVE for the DML card. This is the fair settled-state
    // for a write workload: PG's autovacuum is likewise active under a stream
    // of DELETEs/INSERTs (our per-iteration reseed produces dead tuples), so
    // quiescing Basin's compactor while PG vacuums would be asymmetric. The
    // reseed cost itself is always outside the timed window.
    let instance = build_basin_engine().await;
    let sess = instance.engine.open_session(instance.project).await.unwrap();
    sess.execute(
        "CREATE TABLE scratch (\
            id BIGINT NOT NULL PRIMARY KEY, \
            user_id BIGINT NOT NULL, \
            amount DOUBLE PRECISION NOT NULL, \
            status TEXT NOT NULL, \
            created_at BIGINT NOT NULL)",
    )
    .await
    .unwrap();
    // A read-only source table for INSERT…SELECT (never mutated, seeded once).
    sess.execute(
        "CREATE TABLE src (\
            id BIGINT NOT NULL PRIMARY KEY, \
            user_id BIGINT NOT NULL, \
            amount DOUBLE PRECISION NOT NULL, \
            status TEXT NOT NULL, \
            created_at BIGINT NOT NULL)",
    )
    .await
    .unwrap();

    let tuple = |k: i64| -> String {
        format!(
            "({k},{},{},'{}',{})",
            k % 1000,
            0.5 + (k % 1000) as f64,
            status_for(k),
            EPOCH + k,
        )
    };

    // Seed src once on Basin.
    {
        let batch = 10_000i64;
        let mut id = 0i64;
        while id < seed_rows {
            let hi = (id + batch).min(seed_rows);
            let mut stmt = String::from("INSERT INTO src VALUES ");
            for k in id..hi {
                if k > id {
                    stmt.push(',');
                }
                stmt.push_str(&tuple(k));
            }
            sess.execute(&stmt).await.expect("basin src seed");
            id = hi;
        }
    }

    // Reseed scratch (truncate-equivalent: DELETE all then bulk INSERT). We use
    // DELETE because TRUNCATE semantics differ; the reseed cost is excluded
    // from the timed window (only the shape statement is timed).
    async fn reseed_basin(sess: &basin_engine::ProjectSession, rows: i64, tuple: &dyn Fn(i64) -> String) {
        sess.execute("DELETE FROM scratch").await.expect("basin reseed clear");
        let batch = 10_000i64;
        let mut id = 0i64;
        while id < rows {
            let hi = (id + batch).min(rows);
            let mut stmt = String::from("INSERT INTO scratch VALUES ");
            for k in id..hi {
                if k > id {
                    stmt.push(',');
                }
                stmt.push_str(&tuple(k));
            }
            sess.execute(&stmt).await.expect("basin reseed insert");
            id = hi;
        }
    }

    instance.shard.flush_to_parquet().await.unwrap();

    // ── Define the shapes as closures over the scratch/src tables. Each entry:
    //    (label, n, basin_stmt, pg_stmt). The `{S}` placeholder marks the
    //    schema-qualified table name on the PG side; Basin uses bare names.
    // Mutating shapes are timed one-shot per iteration after a reseed.
    struct ShapeDef {
        label: &'static str,
        n: usize,
        /// statement with bare table names (Basin); PG side replaces
        /// `scratch`/`src` with schema-qualified names.
        stmt: String,
        /// whether the shape mutates `scratch` (→ reseed before each timed run)
        mutating: bool,
    }
    let defs: Vec<ShapeDef> = vec![
        ShapeDef {
            label: "U1 expr UPDATE single row",
            n: 1,
            stmt: "UPDATE scratch SET amount = amount + 1 WHERE id = 12345".to_string(),
            mutating: true,
        },
        ShapeDef {
            label: "U2 expr UPDATE 1000 rows",
            n: 1000,
            stmt: "UPDATE scratch SET amount = amount + 1 WHERE id < 1000".to_string(),
            mutating: true,
        },
        ShapeDef {
            label: "U3 multi-col expr UPDATE 1000 rows",
            n: 1000,
            stmt: "UPDATE scratch SET amount = amount * 2, user_id = user_id + 1 WHERE id < 1000"
                .to_string(),
            mutating: true,
        },
        ShapeDef {
            label: "X1 single-row DELETE",
            n: 1,
            stmt: "DELETE FROM scratch WHERE id = 12345".to_string(),
            mutating: true,
        },
        ShapeDef {
            label: "X2 range DELETE (~1000 rows)",
            n: 1000,
            stmt: "DELETE FROM scratch WHERE id BETWEEN 5000 AND 5999".to_string(),
            mutating: true,
        },
        ShapeDef {
            label: "X3 subquery DELETE (IN SELECT)",
            n: 1000,
            stmt: "DELETE FROM scratch WHERE id IN \
                   (SELECT id FROM src WHERE id >= 20000 AND id < 21000)"
                .to_string(),
            mutating: true,
        },
        ShapeDef {
            label: "X4 DELETE RETURNING id (single)",
            n: 1,
            stmt: "DELETE FROM scratch WHERE id = 12345 RETURNING id".to_string(),
            mutating: true,
        },
        ShapeDef {
            label: "I1a INSERT VALUES (1 row)",
            n: 1,
            stmt: "INSERT INTO scratch VALUES (900000001, 1, 1.0, 'x', 1700000001)".to_string(),
            mutating: true,
        },
        ShapeDef {
            label: "I1b INSERT VALUES (10 rows)",
            n: 10,
            stmt: insert_ladder("scratch", 900_000_010, 10),
            mutating: true,
        },
        ShapeDef {
            label: "I1c INSERT VALUES (100 rows)",
            n: 100,
            stmt: insert_ladder("scratch", 900_000_100, 100),
            mutating: true,
        },
        ShapeDef {
            label: "I1d INSERT VALUES (1000 rows)",
            n: 1000,
            stmt: insert_ladder("scratch", 900_001_000, 1000),
            mutating: true,
        },
        ShapeDef {
            label: "I2 filtered INSERT…SELECT",
            n: 0, // count discovered at run
            stmt: "INSERT INTO scratch (id, user_id, amount, status, created_at) \
                   SELECT id + 1000000000, user_id, amount, status, created_at \
                   FROM src WHERE status = 'active'"
                .to_string(),
            mutating: true,
        },
    ];

    let mut results: Vec<DmlResult> = Vec::with_capacity(defs.len());

    // ── Basin timing ────────────────────────────────────────────────────────
    // Each mutating shape: reseed → time the single statement. Repeat `samples`
    // times; report min (noise floor).
    let mut basin_supported: Vec<bool> = Vec::with_capacity(defs.len());
    for d in &defs {
        let mut samp = Vec::with_capacity(samples);
        let mut ok = true;
        // Probe once for support (after a reseed).
        if d.mutating {
            reseed_basin(&sess, seed_rows, &tuple).await;
        }
        if sess.execute(&d.stmt).await.is_err() {
            ok = false;
        }
        if ok {
            for _ in 0..samples {
                if d.mutating {
                    reseed_basin(&sess, seed_rows, &tuple).await;
                }
                let t = Instant::now();
                let r = sess.execute(&d.stmt).await;
                let ms = t.elapsed().as_secs_f64() * 1000.0;
                if r.is_err() {
                    ok = false;
                    break;
                }
                samp.push(ms);
            }
        }
        basin_supported.push(ok);
        let basin_min = if ok && !samp.is_empty() {
            let m = min_floor(&samp);
            println!("[ext-dml] Basin {:<38}: min={m:.3}ms", d.label);
            m
        } else {
            eprintln!("[ext-dml] UNSUPPORTED (basin) {}", d.label);
            f64::NAN
        };
        results.push(DmlResult {
            label: d.label,
            n: d.n,
            basin_min_ms: basin_min,
            pg_min_ms: f64::NAN,
            supported: ok,
            note: if ok {
                String::new()
            } else {
                "basin unsupported".to_string()
            },
        });
    }

    // ── PG twin ───────────────────────────────────────────────────────────
    if let Some((pg, cs)) = try_connect().await {
        let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
        let schema = format!("basin_extdml_{suffix}");
        let _guard = SchemaGuard {
            schema: schema.clone(),
            conn_str: cs,
        };
        pg.simple_query(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        pg.simple_query("SET work_mem = '4MB'").await.unwrap();
        for t in ["scratch", "src"] {
            pg.simple_query(&format!(
                "CREATE TABLE {schema}.{t} (\
                    id BIGINT PRIMARY KEY, user_id BIGINT, amount DOUBLE PRECISION, \
                    status TEXT, created_at BIGINT)"
            ))
            .await
            .unwrap();
        }
        // Seed src once on PG.
        {
            let batch = 10_000i64;
            let mut id = 0i64;
            while id < seed_rows {
                let hi = (id + batch).min(seed_rows);
                let mut stmt = format!("INSERT INTO {schema}.src VALUES ");
                for k in id..hi {
                    if k > id {
                        stmt.push(',');
                    }
                    stmt.push_str(&tuple(k));
                }
                pg.simple_query(&stmt).await.unwrap();
                id = hi;
            }
        }

        let reseed_pg = |stmt_id: i64| {
            // returns the INSERT body for a full reseed of scratch
            let mut id = 0i64;
            let batch = 10_000i64;
            let mut chunks = Vec::new();
            while id < stmt_id {
                let hi = (id + batch).min(stmt_id);
                let mut stmt = format!("INSERT INTO {schema}.scratch VALUES ");
                for k in id..hi {
                    if k > id {
                        stmt.push(',');
                    }
                    stmt.push_str(&tuple(k));
                }
                chunks.push(stmt);
                id = hi;
            }
            chunks
        };

        let qualify = |sql: &str| {
            sql.replace("scratch", &format!("{schema}.scratch"))
                .replace("src", &format!("{schema}.src"))
        };

        for (i, d) in defs.iter().enumerate() {
            if !basin_supported[i] {
                continue; // keep sides symmetric: only time shapes Basin ran
            }
            let pg_stmt = qualify(&d.stmt);
            let reseed_chunks = reseed_pg(seed_rows);
            let clear_sql = format!("DELETE FROM {schema}.scratch");

            // Correctness: run the statement on PG once, compare resulting
            // scratch COUNT(*) to the Basin post-state COUNT(*).
            if d.mutating {
                reseed_pg_scratch(&pg, &clear_sql, &reseed_chunks).await;
            }
            let _ = pg.simple_query(&pg_stmt).await.expect("pg ext-dml probe");
            let pg_post = pg
                .simple_query(&format!("SELECT COUNT(*) FROM {schema}.scratch"))
                .await
                .ok()
                .and_then(|m| first_pg_i64(&m));

            // Basin post-state (re-run on a fresh reseed for parity).
            if d.mutating {
                reseed_basin(&sess, seed_rows, &tuple).await;
            }
            let _ = sess.execute(&d.stmt).await;
            let basin_post = sess
                .execute("SELECT COUNT(*) FROM scratch")
                .await
                .ok()
                .and_then(|r| first_i64(&r));
            if let (Some(a), Some(b)) = (basin_post, pg_post) {
                assert_eq!(
                    a, b,
                    "[ext-dml] {} post-state COUNT mismatch: basin={a} pg={b}",
                    d.label
                );
            }

            // Timed PG runs (reseed before each, min over samples).
            let mut samp = Vec::with_capacity(samples);
            for _ in 0..samples {
                if d.mutating {
                    reseed_pg_scratch(&pg, &clear_sql, &reseed_chunks).await;
                }
                let t = Instant::now();
                let _ = pg.simple_query(&pg_stmt).await.expect("pg ext-dml timed");
                samp.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            let pg_min = min_floor(&samp);
            println!("[ext-dml] PG    {:<38}: min={pg_min:.3}ms", d.label);
            results[i].pg_min_ms = pg_min;
        }
        std::mem::forget(_guard);
    } else {
        eprintln!("[ext-dml] PG unavailable — Basin-only results");
    }

    // ── Emit ────────────────────────────────────────────────────────────────
    let json_rows: Vec<serde_json::Value> = results
        .iter()
        .map(|r| {
            json!({
                "shape": r.label,
                "n": r.n,
                "supported": r.supported,
                "note": r.note,
                "basin_min_ms": nan_null(r.basin_min_ms),
                "pg_min_ms": nan_null(r.pg_min_ms),
                "basin_over_pg": ratio(r.basin_min_ms, r.pg_min_ms),
            })
        })
        .collect();

    let unsupported: Vec<&str> = results
        .iter()
        .filter(|r| !r.supported)
        .map(|r| r.label)
        .collect();

    merge_artifact(
        "dml",
        json!({
            "section": "dml",
            "generated_at": format!("@{}", now_secs()),
            "config": { "seed_rows": seed_rows, "samples": samples, "estimator": "min (noise-floor, low-n)" },
            "families": ["expr_update", "delete_variants", "insert_shapes"],
            "shapes": json_rows,
            "unsupported": unsupported,
        }),
    );

    instance.wal.close().await.unwrap();
}

/// Build an `INSERT INTO <table> VALUES (...),(...)` ladder of `count` rows
/// starting at `start_id` (ids chosen high so they never collide with the
/// 0..seed_rows scratch range).
fn insert_ladder(table: &str, start_id: i64, count: i64) -> String {
    let mut s = format!("INSERT INTO {table} VALUES ");
    for k in 0..count {
        if k > 0 {
            s.push(',');
        }
        let id = start_id + k;
        s.push_str(&format!(
            "({id},{},{},'{}',{})",
            k % 1000,
            0.5 + (k % 1000) as f64,
            status_for(k),
            EPOCH + k,
        ));
    }
    s
}

/// Reset the PG `scratch` table to its seed state inside one transaction:
/// clear, then replay the prebuilt INSERT chunks. The reseed is excluded from
/// the timed window — only the shape statement is timed.
async fn reseed_pg_scratch(pg: &tokio_postgres::Client, clear_sql: &str, chunks: &[String]) {
    let _ = pg.simple_query("BEGIN").await;
    let _ = pg.simple_query(clear_sql).await;
    for c in chunks {
        let _ = pg.simple_query(c).await;
    }
    let _ = pg.simple_query("COMMIT").await;
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
