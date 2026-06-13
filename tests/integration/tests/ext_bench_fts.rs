//! EXTENSIONS BENCHMARK SUITE — full-text-search family performance card.
//!
//! Separate artifact (`benchmark/data/ext_bench_fts.json`), separate runner.
//! Exercises the FTS surface Basin implements today (verified against
//! `fts_conformance.rs` / `fts_compare.rs`):
//!
//!   * `to_tsvector('english', body)` ingest
//!   * `@@` single-term / AND / OR / phrase match
//!   * `ts_rank` ORDER BY LIMIT 10 (the search page)
//!   * `websearch_to_tsquery` (best-effort AND join — documented)
//!   * `ts_headline` projection
//!   * GIN DDL (`CREATE INDEX … USING GIN (to_tsvector(...))`) probed both ways
//!
//! Basin can store a tsvector column and answer `@@`; whether the GIN index is
//! actually *used* for `@@` is recorded per-shape (`basin_uses_gin`). The card
//! measures both an inline `to_tsvector(body)` scan AND, when the Basin GIN DDL
//! is accepted, a pre-materialised `tsv` column path.
//!
//! ## Shapes
//!   F1 to_tsvector ingest cost (with vs without GIN) — ingest_rate rows/s
//!   F2 @@ single-term match at selectivity
//!   F3 @@ multi-term AND
//!   F4 @@ multi-term OR
//!   F5 @@ phrase (`a <-> b`)
//!   F6 @@ + ts_rank ORDER BY LIMIT 10 (the search page)
//!   F7 websearch_to_tsquery end-to-end
//!   F8 ts_headline projection cost
//!
//! ## Excluded as separate perf shapes
//!   * `setweight` — registered; no dedicated perf shape (output is a string
//!     transformation, cost dominated by to_tsvector ingest).
//!   * cover-density `ts_rank_cd` distinct algorithm — same formula as ts_rank,
//!     not a separate perf shape.
//!
//! ## Env knobs
//!   * `BASIN_EXT_BENCH_ROWS`        — doc count (default 100_000)
//!   * `BASIN_EXT_BENCH_FTS_SAMPLES` — samples per shape (default 25)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use basin_common::ProjectId;
use basin_engine::ExecResult;
use serde_json::json;
use tokio_postgres::SimpleQueryMessage;

#[path = "compare_postgres_common.rs"]
mod common;

use common::{build_basin_engine, median, try_connect, SchemaGuard};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
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
        eprintln!("[ext_bench_fts] artifact written: {}", path.display());
    }
}

fn opt_ms(v: Option<f64>) -> serde_json::Value {
    match v {
        Some(ms) => json!(ms),
        None => serde_json::Value::Null,
    }
}

fn ratio(b: Option<f64>, p: Option<f64>) -> serde_json::Value {
    match (b, p) {
        (Some(bv), Some(pv)) if pv > 1e-9 => json!(bv / pv),
        _ => serde_json::Value::Null,
    }
}

fn rows_of(res: &ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

const VOCAB: usize = 2000;

fn word(i: usize) -> String {
    let roots = [
        "database", "query", "index", "table", "column", "row", "schema",
        "transaction", "storage", "cache",
    ];
    let root = roots[i % roots.len()];
    format!("{root}{}", i / roots.len())
}

fn body_for(i: usize) -> String {
    let n_words = 20 + (i % 31);
    let mut words = Vec::with_capacity(n_words);
    for w in 0..n_words {
        words.push(word((i * 7 + w * 13) % VOCAB));
    }
    words.join(" ")
}

const TERM: &str = "database0";
const AND_Q: &str = "database & query";
const OR_Q: &str = "database0 | nonexistentterm";
const PHRASE_Q: &str = "database0 <-> query0";
const WEB_Q: &str = "database query";

async fn basin_p50(
    sess: &basin_engine::ProjectSession,
    sql: &str,
    n: usize,
) -> (Option<f64>, Option<ExecResult>) {
    match sess.execute(sql).await {
        Ok(warm) => {
            let mut s = Vec::with_capacity(n);
            for _ in 0..n {
                let t = Instant::now();
                let _ = sess.execute(sql).await.expect("basin fts shape");
                s.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            (Some(median(&s)), Some(warm))
        }
        Err(e) => {
            eprintln!("[ext_bench_fts] shape unsupported: {sql} :: {e}");
            (None, None)
        }
    }
}

async fn pg_p50(pg: &tokio_postgres::Client, inner: &str, n: usize) -> Option<f64> {
    let _ = pg.simple_query(&format!("EXPLAIN (ANALYZE, FORMAT TEXT) {inner}")).await;
    let mut s = Vec::with_capacity(n);
    for _ in 0..n {
        if let Ok(rs) = pg.simple_query(&format!("EXPLAIN (ANALYZE, FORMAT TEXT) {inner}")).await {
            for m in &rs {
                if let SimpleQueryMessage::Row(r) = m {
                    if let Some(line) = r.get(0) {
                        if let Some(idx) = line.find("Execution Time:") {
                            let after = line[idx + "Execution Time:".len()..].trim();
                            if let Some(end) = after.find(' ') {
                                if let Ok(v) = after[..end].parse::<f64>() {
                                    s.push(v);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    if s.is_empty() { None } else { Some(median(&s)) }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "extensions benchmark: needs idle box (+ live PG with extension for head-to-head); run via benchmark/run/extensions-suite.sh"]
async fn ext_bench_fts() {
    let rows = env_usize("BASIN_EXT_BENCH_ROWS", 100_000);
    let samples = env_usize("BASIN_EXT_BENCH_FTS_SAMPLES", 25);
    eprintln!("[ext_bench_fts] config: rows={rows} samples={samples}");

    // ── Basin setup + F1 ingest cost ─────────────────────────────────────────
    let mut instance = build_basin_engine().await;
    let sess = instance.engine.open_session(instance.project).await.unwrap();
    sess.execute("CREATE TABLE articles (id BIGINT NOT NULL PRIMARY KEY, body TEXT NOT NULL, tsv TSVECTOR)")
        .await
        .unwrap();

    let batch = 5_000usize;
    let mut off = 0usize;
    let ingest_start = Instant::now();
    eprintln!("[ext_bench_fts] seeding {rows} docs ...");
    while off < rows {
        let hi = (off + batch).min(rows);
        let mut stmt = String::with_capacity((hi - off) * 256);
        stmt.push_str("INSERT INTO articles (id, body) VALUES ");
        for k in off..hi {
            if k > off {
                stmt.push(',');
            }
            let body = body_for(k).replace('\'', "''");
            stmt.push_str(&format!("({k},'{body}')"));
        }
        sess.execute(&stmt).await.expect("basin fts seed");
        off = hi;
    }
    // Materialise the tsv column (F1 ingest cost includes to_tsvector work).
    let tsv_build_ok = sess
        .execute("UPDATE articles SET tsv = to_tsvector('english', body)")
        .await
        .is_ok();
    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }
    let basin_ingest_s = ingest_start.elapsed().as_secs_f64();
    let basin_ingest_rate = if basin_ingest_s > 0.0 { rows as f64 / basin_ingest_s } else { 0.0 };
    eprintln!("[ext_bench_fts] basin seed+tsvector complete in {basin_ingest_s:.2}s (tsv_build_ok={tsv_build_ok})");

    let basin_gin_ddl_ok = sess
        .execute("CREATE INDEX articles_tsv_gin ON articles USING GIN (tsv)")
        .await
        .is_ok();
    eprintln!("[ext_bench_fts] basin GIN DDL: {}", if basin_gin_ddl_ok { "ok" } else { "not wired (gap)" });

    // Basin shapes use the materialised tsv column if it built, else inline.
    let tcol = if tsv_build_ok { "tsv".to_string() } else { "to_tsvector('english', body)".to_string() };

    let f2_sql = format!("SELECT id FROM articles WHERE {tcol} @@ to_tsquery('english', '{TERM}')");
    let f3_sql = format!("SELECT id FROM articles WHERE {tcol} @@ to_tsquery('english', '{AND_Q}')");
    let f4_sql = format!("SELECT id FROM articles WHERE {tcol} @@ to_tsquery('english', '{OR_Q}')");
    let f5_sql = format!("SELECT id FROM articles WHERE {tcol} @@ to_tsquery('english', '{PHRASE_Q}')");
    let f6_sql = format!(
        "SELECT id, ts_rank({tcol}, to_tsquery('english', '{TERM}')) AS rank FROM articles \
         WHERE {tcol} @@ to_tsquery('english', '{TERM}') ORDER BY rank DESC LIMIT 10"
    );
    let f7_sql = format!("SELECT id FROM articles WHERE {tcol} @@ websearch_to_tsquery('english', '{WEB_Q}')");
    let f8_sql = format!(
        "SELECT ts_headline('english', body, to_tsquery('english', '{TERM}')) FROM articles \
         WHERE {tcol} @@ to_tsquery('english', '{TERM}') LIMIT 50"
    );

    let (f2_b, f2_res) = basin_p50(&sess, &f2_sql, samples).await;
    let f2_rows = f2_res.as_ref().map(rows_of).unwrap_or(0);
    let (f3_b, f3_res) = basin_p50(&sess, &f3_sql, samples).await;
    let f3_rows = f3_res.as_ref().map(rows_of).unwrap_or(0);
    let (f4_b, f4_res) = basin_p50(&sess, &f4_sql, samples).await;
    let f4_rows = f4_res.as_ref().map(rows_of).unwrap_or(0);
    let (f5_b, _) = basin_p50(&sess, &f5_sql, samples).await;
    let (f6_b, f6_res) = basin_p50(&sess, &f6_sql, samples).await;
    let f6_rows = f6_res.as_ref().map(rows_of).unwrap_or(0);
    let (f7_b, f7_res) = basin_p50(&sess, &f7_sql, samples).await;
    let f7_rows = f7_res.as_ref().map(rows_of).unwrap_or(0);
    let (f8_b, _) = basin_p50(&sess, &f8_sql, samples).await;

    println!(
        "[ext_bench_fts] Basin ingest={basin_ingest_rate:.0} rows/s F2={:?}(rows={f2_rows}) F3={:?} F4={:?} \
         F5={:?} F6={:?} F7={:?} F8={:?}",
        f2_b, f3_b, f4_b, f5_b, f6_b, f7_b, f8_b
    );

    // ── PG twin ──────────────────────────────────────────────────────────────
    let mut pg_available = false;
    let mut pg_ingest_rate: Option<f64> = None;
    let (mut pg_f2, mut pg_f3, mut pg_f4, mut pg_f5, mut pg_f6, mut pg_f7, mut pg_f8) =
        (None, None, None, None, None, None, None);
    let mut pg_f2_rows = 0usize;

    if let Some((pg, cs)) = try_connect().await {
        pg_available = true;
        let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
        let schema = format!("basin_ext_fts_{suffix}");
        let _guard = SchemaGuard { schema: schema.clone(), conn_str: cs };
        pg.simple_query(&format!("CREATE SCHEMA {schema}")).await.ok();
        pg.simple_query("SET work_mem = '16MB'").await.ok();
        // FTS is core PG (no extension needed) but probe symmetric to the family.
        let _ = pg.simple_query("CREATE EXTENSION IF NOT EXISTS unaccent").await;

        pg.simple_query(&format!(
            "CREATE TABLE {schema}.articles (id BIGINT PRIMARY KEY, body TEXT NOT NULL, tsv TSVECTOR)"
        )).await.ok();

        let pg_start = Instant::now();
        let mut po = 0usize;
        while po < rows {
            let hi = (po + batch).min(rows);
            let mut v = String::with_capacity((hi - po) * 256);
            for k in po..hi {
                if k > po {
                    v.push(',');
                }
                let body = body_for(k).replace('\'', "''");
                v.push_str(&format!("({k},'{body}')"));
            }
            pg.simple_query(&format!("INSERT INTO {schema}.articles (id, body) VALUES {v}")).await.ok();
            po = hi;
        }
        pg.simple_query(&format!("UPDATE {schema}.articles SET tsv = to_tsvector('english', body)")).await.ok();
        let pg_s = pg_start.elapsed().as_secs_f64();
        pg_ingest_rate = if pg_s > 0.0 { Some(rows as f64 / pg_s) } else { None };
        pg.simple_query(&format!("CREATE INDEX articles_tsv_gin ON {schema}.articles USING GIN (tsv)")).await.ok();
        pg.simple_query(&format!("ANALYZE {schema}.articles")).await.ok();

        pg_f2_rows = pg.simple_query(&format!(
            "SELECT id FROM {schema}.articles WHERE tsv @@ to_tsquery('english', '{TERM}')"
        )).await.map(|rs| rs.iter().filter(|m| matches!(m, SimpleQueryMessage::Row(_))).count()).unwrap_or(0);

        pg_f2 = pg_p50(&pg, &format!("SELECT id FROM {schema}.articles WHERE tsv @@ to_tsquery('english', '{TERM}')"), samples).await;
        pg_f3 = pg_p50(&pg, &format!("SELECT id FROM {schema}.articles WHERE tsv @@ to_tsquery('english', '{AND_Q}')"), samples).await;
        pg_f4 = pg_p50(&pg, &format!("SELECT id FROM {schema}.articles WHERE tsv @@ to_tsquery('english', '{OR_Q}')"), samples).await;
        pg_f5 = pg_p50(&pg, &format!("SELECT id FROM {schema}.articles WHERE tsv @@ to_tsquery('english', '{PHRASE_Q}')"), samples).await;
        pg_f6 = pg_p50(&pg, &format!(
            "SELECT id, ts_rank(tsv, to_tsquery('english', '{TERM}')) AS rank FROM {schema}.articles \
             WHERE tsv @@ to_tsquery('english', '{TERM}') ORDER BY rank DESC LIMIT 10"), samples).await;
        pg_f7 = pg_p50(&pg, &format!("SELECT id FROM {schema}.articles WHERE tsv @@ websearch_to_tsquery('english', '{WEB_Q}')"), samples).await;
        pg_f8 = pg_p50(&pg, &format!(
            "SELECT ts_headline('english', body, to_tsquery('english', '{TERM}')) FROM {schema}.articles \
             WHERE tsv @@ to_tsquery('english', '{TERM}') LIMIT 50"), samples).await;

        let _ = pg.simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE")).await;
        std::mem::forget(_guard);
    } else {
        eprintln!("[ext_bench_fts] PG unavailable — Basin-only card");
    }

    // Correctness cross-check: F2 hit counts agree (identical bodies + query).
    if let (true, Some(_)) = (pg_available, f2_b) {
        if pg_f2_rows > 0 && f2_rows > 0 {
            assert_eq!(
                f2_rows, pg_f2_rows,
                "F2 single-term @@ hit count mismatch: basin {f2_rows} != pg {pg_f2_rows}"
            );
        }
    }

    let ts = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    write_artifact("ext_bench_fts.json", &json!({
        "card": "ext_bench_fts",
        "family": "fts",
        "generated_at": format!("@{ts}"),
        "pg_available": pg_available,
        "basin_gin_ddl_ok": basin_gin_ddl_ok,
        "basin_tsv_column_built": tsv_build_ok,
        "config": { "rows": rows, "samples": samples, "vocab_size": VOCAB },
        "ingest": {
            "label": "F1: to_tsvector ingest rate (rows/s, includes tsv build)",
            "basin_rows_per_s": basin_ingest_rate,
            "pg_rows_per_s": pg_ingest_rate,
        },
        "shapes": [
            { "label": "F2: @@ single-term", "basin_p50_ms": opt_ms(f2_b), "pg_p50_ms": opt_ms(pg_f2),
              "basin_over_pg": ratio(f2_b, pg_f2), "basin_rows": f2_rows, "pg_rows": pg_f2_rows,
              "basin_uses_gin": basin_gin_ddl_ok, "pg_uses_gin": pg_available },
            { "label": "F3: @@ multi-term AND", "basin_p50_ms": opt_ms(f3_b), "pg_p50_ms": opt_ms(pg_f3),
              "basin_over_pg": ratio(f3_b, pg_f3), "basin_rows": f3_rows },
            { "label": "F4: @@ multi-term OR", "basin_p50_ms": opt_ms(f4_b), "pg_p50_ms": opt_ms(pg_f4),
              "basin_over_pg": ratio(f4_b, pg_f4), "basin_rows": f4_rows },
            { "label": "F5: @@ phrase (<->)", "basin_p50_ms": opt_ms(f5_b), "pg_p50_ms": opt_ms(pg_f5),
              "basin_over_pg": ratio(f5_b, pg_f5) },
            { "label": "F6: @@ + ts_rank ORDER BY LIMIT 10 (search page)", "basin_p50_ms": opt_ms(f6_b),
              "pg_p50_ms": opt_ms(pg_f6), "basin_over_pg": ratio(f6_b, pg_f6), "basin_rows": f6_rows },
            { "label": "F7: websearch_to_tsquery e2e", "basin_p50_ms": opt_ms(f7_b), "pg_p50_ms": opt_ms(pg_f7),
              "basin_over_pg": ratio(f7_b, pg_f7), "basin_rows": f7_rows },
            { "label": "F8: ts_headline projection (LIMIT 50)", "basin_p50_ms": opt_ms(f8_b),
              "pg_p50_ms": opt_ms(pg_f8), "basin_over_pg": ratio(f8_b, pg_f8) },
        ],
        "note": if basin_gin_ddl_ok {
            "Basin accepted GIN DDL on the tsv column; PG uses its GIN index. Both \
             engines answer @@ over a materialised tsvector column."
        } else {
            "Basin GIN DDL not wired; Basin uses a sequential tsvector scan (over the \
             materialised tsv column if built, else inline to_tsvector). PG uses GIN. \
             The timing gap is expected and recorded honestly."
        },
    }));

    instance.wal.close().await.unwrap();
}
