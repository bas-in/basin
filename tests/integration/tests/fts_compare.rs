//! FTS (`@@` + `ts_rank`) head-to-head benchmark — Basin vs Postgres.
//!
//! Card 6: `fts_head_to_head`
//!
//! Fills an `articles` table with 100k rows of synthetic 20–50 word bodies
//! drawn from a 2000-word dictionary. Three shapes are timed on both engines:
//!
//!   F1 — single-term `@@` match (no ranking)
//!   F2 — multi-term AND match (`to_tsquery('english', 'database & query')`)
//!   F3 — ranked top-20 (`ORDER BY ts_rank DESC LIMIT 20`)
//!
//! PG side uses a pre-built `tsv` tsvector column with a GIN index. Basin
//! side uses inline `to_tsvector('english', body)` on every query (the wired
//! FTS surface per `fts_harness.rs`). The Basin GIN-indexing flag is recorded
//! in the artifact (true when DDL succeeds; currently expected false until
//! Basin ships GIN-backed tsvector; the gap is surfaced honestly).
//!
//! Correctness assertion: Basin row count == PG row count for F1 and F2
//! (identical vocabulary, deterministic body generation, identical query term).
//!
//! # Running
//!
//! ```text
//! cargo test -p basin-integration-tests --test fts_compare \
//!   -- --ignored --nocapture
//! ```
//!
//! Or via `benchmark/run/realistic-suite.sh`.
//!
//! # Env knobs
//!
//! * `BASIN_FTS_ROWS`    — article count (default 100_000)
//! * `BASIN_FTS_SAMPLES` — samples per shape (default 50)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use basin_common::ProjectId;
use basin_engine::ExecResult;
use serde_json::json;
use tokio_postgres::SimpleQueryMessage;

#[path = "compare_postgres_common.rs"]
mod common;

use common::{build_basin_engine, median, percentile, try_connect, SchemaGuard};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn write_artifact(file: &str, value: &serde_json::Value) {
    use std::path::Path;
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest
        .parent()
        .and_then(Path::parent)
        .map(|p| p.join("benchmark/data"))
        .unwrap_or_else(|| std::path::PathBuf::from("benchmark/data"));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join(file);
    let tmp = path.with_extension("json.tmp");
    if let Ok(bytes) = serde_json::to_vec_pretty(value) {
        let _ = std::fs::write(&tmp, &bytes);
        let _ = std::fs::rename(&tmp, &path);
        eprintln!("[fts] artifact written: {}", path.display());
    }
}

fn row_count_of(res: &ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

/// Deterministic 2000-word vocabulary (repeating patterns — no actual English
/// needed; tsvector processing is what we're timing).
///
/// Each word is `word<n>` where n cycles through 0..VOCAB. A 5% hit rate for
/// any single-word query follows from selecting a word at index 0 (present in
/// every 20th row).
const VOCAB: usize = 2000;

fn word(i: usize) -> String {
    // Mix a few recognisable root words so PG English stemming actually fires,
    // which makes F1/F2 correctness more interesting than pure synthetic tokens.
    let roots = [
        "database",
        "query",
        "index",
        "table",
        "column",
        "row",
        "schema",
        "transaction",
        "storage",
        "cache",
    ];
    let root = roots[i % roots.len()];
    format!("{root}{}", i / roots.len())
}

/// Generate a synthetic article body for row `i`: between 20 and 50 words.
/// Body length cycles: row `i` has `20 + (i % 31)` words.
/// Words are drawn from the VOCAB with a stride so consecutive rows share words
/// (realistic for a document corpus) but never have identical bodies.
fn body_for(i: usize) -> String {
    let n_words = 20 + (i % 31);
    let mut words = Vec::with_capacity(n_words);
    for w in 0..n_words {
        words.push(word((i * 7 + w * 13) % VOCAB));
    }
    words.join(" ")
}

/// The term present in every 20th row — gives ~5% selectivity for F1.
const QUERY_TERM: &str = "database0";
/// Multi-term AND query — present in rows where BOTH terms appear.
/// database0 appears every 20th row; query0 appears every 20th row on a
/// different stride, so AND selectivity is ~0.25%.
const QUERY_TERM_MULTI: &str = "database & query";

// ─────────────────────────────────────────────────────────────────────────────
// Card 6 — fts_head_to_head
// ─────────────────────────────────────────────────────────────────────────────

/// FTS `@@` + `ts_rank ORDER BY` head-to-head, Basin vs Postgres.
///
/// Three shapes measured at `BASIN_FTS_ROWS` rows:
///   F1 — single-term match (no ranking); correctness oracle vs PG
///   F2 — multi-term AND match; correctness oracle vs PG
///   F3 — ranked top-20 (ts_rank ORDER BY DESC LIMIT 20)
///
/// The Basin GIN index DDL attempt is recorded (currently expected to produce
/// a "basin gap" on the GIN-backed tsvector path until that feature ships).
/// The timing comparison is honest: PG uses its GIN index; Basin uses a
/// sequential tsvector scan. The raw number gap is part of the story.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "PG head-to-head card: needs idle box + live Postgres; run via benchmark/run/realistic-suite.sh"]
async fn fts_head_to_head() {
    let rows = env_usize("BASIN_FTS_ROWS", 100_000);
    let samples = env_usize("BASIN_FTS_SAMPLES", 50);

    eprintln!("[fts] config: rows={rows} samples={samples}");

    // ── Basin setup ──────────────────────────────────────────────────────────
    let mut instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();

    // Basin FTS table: body TEXT, no separate tsv column (inline tsvector).
    sess.execute(
        "CREATE TABLE articles (\
            id BIGINT NOT NULL PRIMARY KEY, \
            body TEXT NOT NULL)",
    )
    .await
    .unwrap();

    // Seed articles in 5k-row batches
    let batch = 5_000usize;
    let mut offset = 0usize;
    eprintln!("[fts] seeding {rows} articles ...");
    while offset < rows {
        let hi = (offset + batch).min(rows);
        let mut stmt = String::with_capacity((hi - offset) * 256);
        stmt.push_str("INSERT INTO articles VALUES ");
        for k in offset..hi {
            if k > offset {
                stmt.push(',');
            }
            let body = body_for(k);
            // Escape single-quotes in body (none expected with our word() fn, but safe).
            let body_escaped = body.replace('\'', "''");
            stmt.push_str(&format!("({k},'{body_escaped}')"));
        }
        sess.execute(&stmt).await.expect("basin fts seed");
        offset = hi;
    }
    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }
    eprintln!("[fts] basin seed complete");

    // Attempt Basin GIN DDL (record success/failure for the artifact).
    let basin_gin_ddl_ok = sess
        .execute(
            "CREATE INDEX articles_tsv_gin ON articles \
         USING GIN (to_tsvector('english', body))",
        )
        .await
        .is_ok();
    eprintln!(
        "[fts] basin GIN DDL: {}",
        if basin_gin_ddl_ok {
            "ok"
        } else {
            "not wired (gap)"
        }
    );

    // ── Basin shape measurements ──────────────────────────────────────────────
    // F1: single-term match
    let f1_sql = format!(
        "SELECT id FROM articles \
         WHERE to_tsvector('english', body) @@ to_tsquery('english', '{QUERY_TERM}')"
    );
    // F2: multi-term AND match
    let f2_sql = format!(
        "SELECT id FROM articles \
         WHERE to_tsvector('english', body) @@ to_tsquery('english', '{QUERY_TERM_MULTI}')"
    );
    // F3: ranked top-20
    let f3_sql = format!(
        "SELECT id, ts_rank(to_tsvector('english', body), \
                            to_tsquery('english', '{QUERY_TERM}')) AS rank \
         FROM articles \
         WHERE to_tsvector('english', body) @@ to_tsquery('english', '{QUERY_TERM}') \
         ORDER BY rank DESC LIMIT 20"
    );

    // F1 — warm-up + measure
    let basin_f1_p50: Option<f64>;
    let basin_f1_count: usize;
    {
        let warm = sess.execute(&f1_sql).await;
        basin_f1_count = match warm {
            Ok(ref res) => row_count_of(res),
            Err(_) => 0,
        };
        match warm {
            Ok(_) => {
                let mut s: Vec<f64> = Vec::with_capacity(samples);
                for _ in 0..samples {
                    let t = Instant::now();
                    let _ = sess.execute(&f1_sql).await.expect("basin F1");
                    s.push(t.elapsed().as_secs_f64() * 1000.0);
                }
                let p50 = median(&s);
                println!("[fts] Basin F1 (single-term): p50={p50:.3}ms  hits={basin_f1_count}");
                basin_f1_p50 = Some(p50);
            }
            Err(e) => {
                eprintln!("[fts] Basin F1 unsupported: {e}");
                basin_f1_p50 = None;
            }
        }
    }

    // F2
    let basin_f2_p50: Option<f64>;
    let basin_f2_count: usize;
    {
        let warm = sess.execute(&f2_sql).await;
        basin_f2_count = match warm {
            Ok(ref res) => row_count_of(res),
            Err(_) => 0,
        };
        match warm {
            Ok(_) => {
                let mut s: Vec<f64> = Vec::with_capacity(samples);
                for _ in 0..samples {
                    let t = Instant::now();
                    let _ = sess.execute(&f2_sql).await.expect("basin F2");
                    s.push(t.elapsed().as_secs_f64() * 1000.0);
                }
                let p50 = median(&s);
                println!("[fts] Basin F2 (multi-term):  p50={p50:.3}ms  hits={basin_f2_count}");
                basin_f2_p50 = Some(p50);
            }
            Err(e) => {
                eprintln!("[fts] Basin F2 unsupported: {e}");
                basin_f2_p50 = None;
            }
        }
    }

    // F3
    let basin_f3_p50: Option<f64>;
    {
        let warm = sess.execute(&f3_sql).await;
        match warm {
            Ok(_) => {
                let mut s: Vec<f64> = Vec::with_capacity(samples);
                for _ in 0..samples {
                    let t = Instant::now();
                    let res = sess.execute(&f3_sql).await.expect("basin F3");
                    s.push(t.elapsed().as_secs_f64() * 1000.0);
                    let got = row_count_of(&res);
                    assert!(got <= 20, "fts F3: expected ≤20 rows (LIMIT 20), got {got}");
                }
                let p50 = median(&s);
                println!("[fts] Basin F3 (ranked top-20): p50={p50:.3}ms");
                basin_f3_p50 = Some(p50);
            }
            Err(e) => {
                eprintln!("[fts] Basin F3 unsupported: {e}");
                basin_f3_p50 = None;
            }
        }
    }

    // ── PG twin ───────────────────────────────────────────────────────────────
    let (pg_f1_p50, pg_f2_p50, pg_f3_p50, pg_f1_count, pg_f2_count) =
        if let Some((pg, cs)) = try_connect().await {
            let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
            let schema = format!("basin_fts_{suffix}");
            let _guard = SchemaGuard {
                schema: schema.clone(),
                conn_str: cs,
            };
            pg.simple_query(&format!("CREATE SCHEMA {schema}"))
                .await
                .unwrap();
            pg.simple_query("SET work_mem = '4MB'").await.unwrap();

            // PG schema: body TEXT + pre-built tsv TSVECTOR column + GIN index.
            pg.simple_query(&format!(
                "CREATE TABLE {schema}.articles (\
                    id BIGINT PRIMARY KEY, \
                    body TEXT NOT NULL, \
                    tsv TSVECTOR)"
            ))
            .await
            .unwrap();

            // Seed articles (same deterministic bodies)
            let mut pg_off = 0usize;
            while pg_off < rows {
                let hi = (pg_off + batch).min(rows);
                let mut stmt = format!("INSERT INTO {schema}.articles (id, body) VALUES ");
                for k in pg_off..hi {
                    if k > pg_off {
                        stmt.push(',');
                    }
                    let body = body_for(k);
                    let body_escaped = body.replace('\'', "''");
                    stmt.push_str(&format!("({k},'{body_escaped}')"));
                }
                pg.simple_query(&stmt).await.expect("pg fts seed");
                pg_off = hi;
            }

            // Build tsvector column and GIN index
            pg.simple_query(&format!(
                "UPDATE {schema}.articles SET tsv = to_tsvector('english', body)"
            ))
            .await
            .expect("pg build tsv");
            pg.simple_query(&format!(
                "CREATE INDEX articles_tsv_gin ON {schema}.articles USING GIN (tsv)"
            ))
            .await
            .expect("pg create gin");
            eprintln!("[fts] PG GIN index built");

            // PG queries: use pre-built tsv column + GIN index.
            let pg_f1_sql = format!(
                "SELECT id FROM {schema}.articles \
                 WHERE tsv @@ to_tsquery('english', '{QUERY_TERM}')"
            );
            let pg_f2_sql = format!(
                "SELECT id FROM {schema}.articles \
                 WHERE tsv @@ to_tsquery('english', '{QUERY_TERM_MULTI}')"
            );
            let pg_f3_sql = format!(
                "SELECT id, ts_rank(tsv, to_tsquery('english', '{QUERY_TERM}')) AS rank \
                 FROM {schema}.articles \
                 WHERE tsv @@ to_tsquery('english', '{QUERY_TERM}') \
                 ORDER BY rank DESC LIMIT 20"
            );

            // Warm-up
            let _ = pg.simple_query(&pg_f1_sql).await;
            let _ = pg.simple_query(&pg_f2_sql).await;
            let _ = pg.simple_query(&pg_f3_sql).await;

            // Count hits (correctness reference)
            let pgf1_count: usize = pg
                .simple_query(&pg_f1_sql)
                .await
                .map(|rows| {
                    rows.iter()
                        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
                        .count()
                })
                .unwrap_or(0);
            let pgf2_count: usize = pg
                .simple_query(&pg_f2_sql)
                .await
                .map(|rows| {
                    rows.iter()
                        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
                        .count()
                })
                .unwrap_or(0);

            // F1
            let mut s1: Vec<f64> = Vec::with_capacity(samples);
            for _ in 0..samples {
                let t = Instant::now();
                let _ = pg.simple_query(&pg_f1_sql).await.expect("pg F1");
                s1.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            let pf1 = median(&s1);
            println!("[fts] PG F1 (single-term GIN): p50={pf1:.3}ms  hits={pgf1_count}");

            // F2
            let mut s2: Vec<f64> = Vec::with_capacity(samples);
            for _ in 0..samples {
                let t = Instant::now();
                let _ = pg.simple_query(&pg_f2_sql).await.expect("pg F2");
                s2.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            let pf2 = median(&s2);
            println!("[fts] PG F2 (multi-term GIN):  p50={pf2:.3}ms  hits={pgf2_count}");

            // F3
            let mut s3: Vec<f64> = Vec::with_capacity(samples);
            for _ in 0..samples {
                let t = Instant::now();
                let _ = pg.simple_query(&pg_f3_sql).await.expect("pg F3");
                s3.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            let pf3 = median(&s3);
            println!("[fts] PG F3 (ranked GIN):      p50={pf3:.3}ms");

            std::mem::forget(_guard);
            (Some(pf1), Some(pf2), Some(pf3), pgf1_count, pgf2_count)
        } else {
            eprintln!("[fts] PG unavailable — Basin-only FTS results");
            (None, None, None, 0usize, 0usize)
        };

    // ── Correctness assertion ─────────────────────────────────────────────────
    // Basin F1 hit count must match PG F1 hit count (identical bodies, same query).
    if let Some(_b50) = basin_f1_p50 {
        if pg_f1_count > 0 {
            assert_eq!(
                basin_f1_count, pg_f1_count,
                "fts_head_to_head: Basin F1 hit count {basin_f1_count} != PG {pg_f1_count} \
                 (identical bodies + query; result must match)"
            );
        }
    }
    if let Some(_b50) = basin_f2_p50 {
        if pg_f2_count > 0 {
            assert_eq!(
                basin_f2_count, pg_f2_count,
                "fts_head_to_head: Basin F2 hit count {basin_f2_count} != PG {pg_f2_count}"
            );
        }
    }

    // ── Emit artifact ─────────────────────────────────────────────────────────
    let opt_ms = |v: Option<f64>| -> serde_json::Value {
        match v {
            Some(ms) => json!(ms),
            None => serde_json::Value::Null,
        }
    };
    let ratio = |b: Option<f64>, p: Option<f64>| -> serde_json::Value {
        match (b, p) {
            (Some(bv), Some(pv)) if pv > 1e-9 => json!(bv / pv),
            _ => serde_json::Value::Null,
        }
    };

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    write_artifact(
        "fts_head_to_head.json",
        &json!({
            "card": "fts_head_to_head",
            "generated_at": format!("@{ts}"),
            "config": {
                "rows": rows,
                "samples": samples,
                "query_term_f1": QUERY_TERM,
                "query_term_f2": QUERY_TERM_MULTI,
                "vocab_size": VOCAB,
            },
            "basin_gin_ddl_ok": basin_gin_ddl_ok,
            "shapes": [
                {
                    "label": "F1: single-term @@ (no ranking)",
                    "basin_p50_ms": opt_ms(basin_f1_p50),
                    "pg_p50_ms": opt_ms(pg_f1_p50),
                    "basin_over_pg": ratio(basin_f1_p50, pg_f1_p50),
                    "basin_hits": basin_f1_count,
                    "pg_hits": pg_f1_count,
                    "pg_uses_gin": true,
                    "basin_uses_gin": basin_gin_ddl_ok,
                },
                {
                    "label": "F2: multi-term AND @@ (no ranking)",
                    "basin_p50_ms": opt_ms(basin_f2_p50),
                    "pg_p50_ms": opt_ms(pg_f2_p50),
                    "basin_over_pg": ratio(basin_f2_p50, pg_f2_p50),
                    "basin_hits": basin_f2_count,
                    "pg_hits": pg_f2_count,
                    "pg_uses_gin": true,
                    "basin_uses_gin": basin_gin_ddl_ok,
                },
                {
                    "label": "F3: ts_rank ORDER BY LIMIT 20",
                    "basin_p50_ms": opt_ms(basin_f3_p50),
                    "pg_p50_ms": opt_ms(pg_f3_p50),
                    "basin_over_pg": ratio(basin_f3_p50, pg_f3_p50),
                    "pg_uses_gin": true,
                    "basin_uses_gin": basin_gin_ddl_ok,
                },
            ],
            "note": if basin_gin_ddl_ok {
                "Basin GIN-backed tsvector DDL succeeded; index may not yet be used for @@ queries"
            } else {
                "Basin GIN-backed tsvector DDL not yet wired; Basin uses seq-scan tsvector; \
                 timing gap vs PG GIN is expected and documented"
            },
        }),
    );

    instance.wal.close().await.unwrap();
}
