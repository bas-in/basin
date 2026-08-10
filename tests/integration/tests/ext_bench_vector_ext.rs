//! EXTENSIONS BENCHMARK SUITE — pgvector EXTENDED shapes (coverage expansion).
//!
//! Separate file (the main `ext_bench_vector.rs` card is being extended by the
//! ivfflat feature work — this NEW file adds the non-conflicting coverage gaps
//! identified in the shape-coverage audit without touching that body).
//! Separate artifact (`benchmark/data/ext_bench_vector_ext.json`); the runner
//! routes it to a size-suffixed name like `ext_bench_vector_ext_768d_100k.json`.
//!
//! Closes the audit's pgvector P0/P1 gaps that do NOT depend on ivfflat:
//!
//!   VE1 cosine `<=>` ORDER BY LIMIT k     (THE real metric for normalized
//!       OpenAI/sentence-transformer embeddings; the base card is L2-only)
//!   VE2 inner-product `<#>` ORDER BY LIMIT k  (dot-product retrieval)
//!   VE3 filtered-ANN selectivity sweep    (WHERE category=? AND ORDER BY <=>
//!       LIMIT k at ~20% / ~2% selectivity — the pre/post-filter cliff)
//!
//! DEFERRED to a post-merge expansion (depend on the landing ivfflat feature):
//!   * ivfflat build + kNN + recall, and the ivfflat-vs-hnsw-vs-brute crossover.
//!
//! All shapes are Basin-measured; PG head-to-head is opportunistic (records
//! `pg_available:false` and Basin-only timings when `CREATE EXTENSION vector`
//! is unavailable). The card NEVER fails just because PG lacks pgvector.
//!
//! ## Env knobs (shared with the base card via the runner)
//!   * `BASIN_EXT_BENCH_ROWS`           — vector count (default 100_000)
//!   * `BASIN_EXT_BENCH_VEC_SAMPLES`    — samples per shape (default 20)
//!   * `BASIN_EXT_BENCH_VEC_DIM`        — embedding dim (default 128)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use basin_engine::ExecResult;
use serde_json::json;
use tokio_postgres::SimpleQueryMessage;

#[path = "compare_postgres_common.rs"]
mod common;

use basin_common::ProjectId;
use common::{build_basin_engine, median, try_connect, SchemaGuard};

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
        eprintln!(
            "[ext_bench_vector_ext] artifact written: {}",
            path.display()
        );
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

/// SplitMix64-based deterministic vector — identical to ext_bench_vector.rs so
/// the two cards share the same seeded data distribution.
fn det_vec(seed: u64, dim: usize) -> Vec<f32> {
    let mut state = seed.wrapping_add(0x9E3779B97F4A7C15);
    (0..dim)
        .map(|_| {
            state = state.wrapping_add(0x9E3779B97F4A7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            z ^= z >> 31;
            ((z >> 33) as f32 / (1u32 << 31) as f32) - 0.5
        })
        .collect()
}

fn vector_lit(v: &[f32]) -> String {
    let mut s = String::from("[");
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("{:.6}", x));
    }
    s.push(']');
    s
}

const CATEGORIES: &[&str] = &["news", "blog", "docs", "forum", "wiki"];

fn rows_of(res: &ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

async fn basin_p50(sess: &basin_engine::ProjectSession, sql: &str, n: usize) -> Option<f64> {
    if sess.execute(sql).await.is_err() {
        return None;
    }
    let mut s = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        if sess.execute(sql).await.is_err() {
            return None;
        }
        s.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    Some(median(&s))
}

async fn pg_p50(pg: &tokio_postgres::Client, inner: &str, n: usize) -> Option<f64> {
    let _ = pg
        .simple_query(&format!("EXPLAIN (ANALYZE, FORMAT TEXT) {inner}"))
        .await;
    let mut s = Vec::with_capacity(n);
    for _ in 0..n {
        if let Ok(rs) = pg
            .simple_query(&format!("EXPLAIN (ANALYZE, FORMAT TEXT) {inner}"))
            .await
        {
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
    if s.is_empty() {
        None
    } else {
        Some(median(&s))
    }
}

async fn seed_vectors(sess: &basin_engine::ProjectSession, table: &str, rows: usize, dim: usize) {
    sess.execute(&format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL, category TEXT NOT NULL, embedding vector({dim}))"
    ))
    .await
    .unwrap();
    let batch = if dim >= 1024 { 1_000 } else { 4_000 };
    let mut off = 0usize;
    while off < rows {
        let hi = (off + batch).min(rows);
        let mut stmt = String::with_capacity((hi - off) * dim * 10);
        stmt.push_str(&format!("INSERT INTO {table} VALUES "));
        for k in off..hi {
            if k > off {
                stmt.push(',');
            }
            let v = det_vec(k as u64, dim);
            let cat = CATEGORIES[k % CATEGORIES.len()];
            stmt.push_str(&format!("({k}, '{cat}', '{}')", vector_lit(&v)));
        }
        sess.execute(&stmt).await.expect("basin vector seed");
        off = hi;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "extensions benchmark: needs idle box (+ live PG with pgvector for head-to-head); run via benchmark/run/extensions-suite.sh"]
async fn ext_bench_vector_ext() {
    let rows = env_usize("BASIN_EXT_BENCH_ROWS", 100_000);
    let samples = env_usize("BASIN_EXT_BENCH_VEC_SAMPLES", 20);
    let dim = env_usize("BASIN_EXT_BENCH_VEC_DIM", 128);
    let k = 10usize;
    eprintln!("[ext_bench_vector_ext] config: rows={rows} dim={dim} samples={samples}");

    let mut instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();
    seed_vectors(&sess, "docs", rows, dim).await;
    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }

    let q = vector_lit(&det_vec(42, dim));

    // VE1 cosine <=>, VE2 inner-product <#>, VE3 filtered-ANN selectivity sweep.
    let ve1_sql = format!("SELECT id FROM docs ORDER BY embedding <=> '{q}' LIMIT {k}");
    let ve2_sql = format!("SELECT id FROM docs ORDER BY embedding <#> '{q}' LIMIT {k}");
    // ~20% selectivity (one of five categories) and ~2% (a rarer literal id band).
    let ve3_wide_sql = format!(
        "SELECT id FROM docs WHERE category = 'news' ORDER BY embedding <=> '{q}' LIMIT {k}"
    );
    let ve3_narrow_sql = format!(
        "SELECT id FROM docs WHERE category = 'news' AND id < {} ORDER BY embedding <=> '{q}' LIMIT {k}",
        rows / 50
    );

    let ve1_b = basin_p50(&sess, &ve1_sql, samples).await;
    let ve1_sup = sess
        .execute(&ve1_sql)
        .await
        .map(|r| rows_of(&r))
        .unwrap_or(0)
        > 0;
    let ve2_b = basin_p50(&sess, &ve2_sql, samples).await;
    let ve2_sup = sess
        .execute(&ve2_sql)
        .await
        .map(|r| rows_of(&r))
        .unwrap_or(0)
        > 0;
    let ve3w_b = basin_p50(&sess, &ve3_wide_sql, samples).await;
    let ve3n_b = basin_p50(&sess, &ve3_narrow_sql, samples).await;

    println!(
        "[ext_bench_vector_ext] Basin: VE1(cos,sup={ve1_sup})={ve1_b:?} VE2(ip,sup={ve2_sup})={ve2_b:?} \
         VE3(wide={ve3w_b:?} narrow={ve3n_b:?})"
    );

    instance.wal.close().await.unwrap();

    // ── PG twin (opportunistic) ───────────────────────────────────────────────
    let mut pg_available = false;
    let (mut pg_ve1, mut pg_ve2, mut pg_ve3w, mut pg_ve3n) = (None, None, None, None);

    if let Some((pg, cs)) = try_connect().await {
        let ext_ok = pg
            .simple_query("CREATE EXTENSION IF NOT EXISTS vector")
            .await
            .is_ok();
        if ext_ok {
            pg_available = true;
            let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
            let schema = format!("basin_ext_vecx_{suffix}");
            let _guard = SchemaGuard {
                schema: schema.clone(),
                conn_str: cs,
            };
            pg.simple_query(&format!("CREATE SCHEMA {schema}"))
                .await
                .ok();
            pg.simple_query(&format!(
                "CREATE TABLE {schema}.docs (id BIGINT, category TEXT, embedding vector({dim}))"
            ))
            .await
            .ok();

            let batch = if dim >= 1024 { 1_000 } else { 4_000 };
            let mut off = 0usize;
            while off < rows {
                let hi = (off + batch).min(rows);
                let mut v = String::with_capacity((hi - off) * dim * 10);
                for kk in off..hi {
                    if kk > off {
                        v.push(',');
                    }
                    let vec = det_vec(kk as u64, dim);
                    let cat = CATEGORIES[kk % CATEGORIES.len()];
                    v.push_str(&format!("({kk}, '{cat}', '{}')", vector_lit(&vec)));
                }
                pg.simple_query(&format!("INSERT INTO {schema}.docs VALUES {v}"))
                    .await
                    .ok();
                off = hi;
            }
            pg.simple_query(&format!("ANALYZE {schema}.docs"))
                .await
                .ok();

            pg_ve1 = pg_p50(
                &pg,
                &format!("SELECT id FROM {schema}.docs ORDER BY embedding <=> '{q}' LIMIT {k}"),
                samples,
            )
            .await;
            pg_ve2 = pg_p50(
                &pg,
                &format!("SELECT id FROM {schema}.docs ORDER BY embedding <#> '{q}' LIMIT {k}"),
                samples,
            )
            .await;
            pg_ve3w = pg_p50(&pg, &format!("SELECT id FROM {schema}.docs WHERE category = 'news' ORDER BY embedding <=> '{q}' LIMIT {k}"), samples).await;
            pg_ve3n = pg_p50(&pg, &format!("SELECT id FROM {schema}.docs WHERE category = 'news' AND id < {} ORDER BY embedding <=> '{q}' LIMIT {k}", rows / 50), samples).await;

            let _ = pg
                .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                .await;
            std::mem::forget(_guard);
        } else {
            eprintln!("[ext_bench_vector_ext] PG lacks pgvector — Basin-only card");
        }
    } else {
        eprintln!("[ext_bench_vector_ext] PG unavailable — Basin-only card");
    }

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    write_artifact(
        "ext_bench_vector_ext.json",
        &json!({
            "card": "ext_bench_vector_ext",
            "family": "pgvector",
            "generated_at": format!("@{ts}"),
            "pg_available": pg_available,
            "pg_extension_available": pg_available,
            "config": { "rows": rows, "dim": dim, "samples": samples, "k": k },
            "shapes": [
                { "label": "VE1: cosine <=> ORDER BY LIMIT k (normalized-embedding metric)",
                  "basin_supported": ve1_sup, "basin_p50_ms": opt_ms(ve1_b), "pg_p50_ms": opt_ms(pg_ve1),
                  "basin_over_pg": ratio(ve1_b, pg_ve1) },
                { "label": "VE2: inner-product <#> ORDER BY LIMIT k (dot-product retrieval)",
                  "basin_supported": ve2_sup, "basin_p50_ms": opt_ms(ve2_b), "pg_p50_ms": opt_ms(pg_ve2),
                  "basin_over_pg": ratio(ve2_b, pg_ve2) },
                { "label": "VE3a: filtered-ANN wide (~20% category=news) cosine kNN",
                  "basin_p50_ms": opt_ms(ve3w_b), "pg_p50_ms": opt_ms(pg_ve3w), "basin_over_pg": ratio(ve3w_b, pg_ve3w) },
                { "label": "VE3b: filtered-ANN narrow (~2%) cosine kNN — pre/post-filter cliff",
                  "basin_p50_ms": opt_ms(ve3n_b), "pg_p50_ms": opt_ms(pg_ve3n), "basin_over_pg": ratio(ve3n_b, pg_ve3n) },
            ],
            "note": "Coverage-expansion card for the pgvector family: cosine (<=>) and \
                     inner-product (<#>) metrics — the dominant real metric for normalized \
                     embeddings, which the base L2-only card undercovers — plus a filtered-ANN \
                     selectivity sweep (the pre/post-filter cliff). The ivfflat-vs-hnsw-vs-brute \
                     crossover is deferred to a post-merge expansion once the ivfflat feature lands.",
        }),
    );
}
