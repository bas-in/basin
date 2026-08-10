//! EXTENSIONS BENCHMARK SUITE — pgvector family performance card.
//!
//! Separate artifact (`benchmark/data/ext_bench_vector.json`), separate runner.
//! Exercises the vector surface Basin implements today (verified against
//! `vector_smoke.rs` / `pgvector_compat_harness.rs`):
//!
//!   * `vector(N)` column type
//!   * `l2_distance` / `cosine_distance` / `dot_product` UDFs
//!   * `<->` (L2) / `<=>` (cosine) / `<#>` (inner-product) operators
//!   * `ORDER BY embedding <-> $q LIMIT k` (brute-force and HNSW-routed)
//!   * `CREATE INDEX … USING hnsw (embedding vector_l2_ops) WITH (m, ef_construction)`
//!   * programmatic `session.vector_search(table, col, q, k, Distance)` HNSW path
//!
//! RECALL is a CORRECTNESS metric in the artifact: HNSW top-10 vs brute-force
//! top-10 on the same data (recall@10).
//!
//! ## Shapes
//!   V1 ingest 100k × 128d   (rows/s)
//!   V2 ingest 100k × 1536d  (rows/s — OpenAI-embedding width)
//!   V3 brute-force <-> ORDER BY LIMIT 10   (128d)
//!   V4 HNSW build time (128d)
//!   V5 HNSW-backed kNN latency (128d) + recall@10 vs brute force
//!   V6 filtered kNN (WHERE category = ? AND ORDER BY <-> LIMIT 10)
//!   V7 distance-threshold WHERE (l2_distance < r)
//!
//! ## Excluded as unimplemented
//!   * pgvector `ivfflat` index — Basin ships HNSW only.
//!   * `halfvec` / `sparsevec` / `bit` vector types — not declared.
//!
//! ## PG head-to-head
//! `CREATE EXTENSION IF NOT EXISTS vector` inside a try; on failure record
//! `pg_available:false` and emit Basin-only timings. When available, PG builds
//! an `hnsw` index and serves the same kNN.
//!
//! ## Env knobs
//!   * `BASIN_EXT_BENCH_ROWS`           — vector count (default 100_000)
//!   * `BASIN_EXT_BENCH_VEC_SAMPLES`    — samples per shape (default 20)
//!   * `BASIN_EXT_BENCH_VEC_DIM`        — small-dim (default 128)
//!   * `BASIN_EXT_BENCH_VEC_BIGDIM`     — big-dim ingest probe (default 1536)
//!   * `BASIN_EXT_BENCH_VEC_BIGDIM_ROWS`— rows for the big-dim ingest (default 20_000)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow_array::{Array, Int64Array};
use basin_common::{ProjectId, TableName};
use basin_engine::ExecResult;
use basin_vector::Distance;
use serde_json::json;
use tokio_postgres::SimpleQueryMessage;

#[path = "compare_postgres_common.rs"]
mod common;

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
        eprintln!("[ext_bench_vector] artifact written: {}", path.display());
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

/// SplitMix64-based deterministic vector — identical to vector_smoke.rs.
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

fn ids_of(res: &ExecResult) -> Vec<i64> {
    match res {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in batches {
                if let Ok(idx) = b.schema().index_of("id") {
                    if let Some(a) = b.column(idx).as_any().downcast_ref::<Int64Array>() {
                        for i in 0..a.len() {
                            out.push(a.value(i));
                        }
                    }
                }
            }
            out
        }
        ExecResult::Empty { .. } => Vec::new(),
    }
}

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

/// Seed a `vector(dim)` table with `rows` deterministic embeddings + a category
/// column. Returns the elapsed ingest seconds (excludes flush).
async fn seed_vectors(
    sess: &basin_engine::ProjectSession,
    table: &str,
    rows: usize,
    dim: usize,
    with_category: bool,
) -> f64 {
    let cols = if with_category {
        format!("(id BIGINT NOT NULL, category TEXT NOT NULL, embedding vector({dim}))")
    } else {
        format!("(id BIGINT NOT NULL, embedding vector({dim}))")
    };
    sess.execute(&format!("CREATE TABLE {table} {cols}"))
        .await
        .unwrap();

    // Bigger dims → smaller batch to keep the INSERT statement size sane.
    let batch = if dim >= 1024 { 1_000 } else { 4_000 };
    let mut off = 0usize;
    let start = Instant::now();
    while off < rows {
        let hi = (off + batch).min(rows);
        let mut stmt = String::with_capacity((hi - off) * dim * 10);
        stmt.push_str(&format!("INSERT INTO {table} VALUES "));
        for k in off..hi {
            if k > off {
                stmt.push(',');
            }
            let v = det_vec(k as u64, dim);
            if with_category {
                let cat = CATEGORIES[k % CATEGORIES.len()];
                stmt.push_str(&format!("({k}, '{cat}', '{}')", vector_lit(&v)));
            } else {
                stmt.push_str(&format!("({k}, '{}')", vector_lit(&v)));
            }
        }
        sess.execute(&stmt).await.expect("basin vector seed");
        off = hi;
    }
    start.elapsed().as_secs_f64()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "extensions benchmark: needs idle box (+ live PG with extension for head-to-head); run via benchmark/run/extensions-suite.sh"]
async fn ext_bench_vector() {
    let rows = env_usize("BASIN_EXT_BENCH_ROWS", 100_000);
    let samples = env_usize("BASIN_EXT_BENCH_VEC_SAMPLES", 20);
    let dim = env_usize("BASIN_EXT_BENCH_VEC_DIM", 128);
    let bigdim = env_usize("BASIN_EXT_BENCH_VEC_BIGDIM", 1536);
    let bigdim_rows = env_usize("BASIN_EXT_BENCH_VEC_BIGDIM_ROWS", 20_000);
    let k = 10usize;
    eprintln!("[ext_bench_vector] config: rows={rows} dim={dim} bigdim={bigdim}x{bigdim_rows} samples={samples}");

    // ── Basin: main 128d table (V1 ingest) ───────────────────────────────────
    let mut instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();

    let v1_ingest_s = seed_vectors(&sess, "docs", rows, dim, true).await;
    let v1_rate = if v1_ingest_s > 0.0 {
        rows as f64 / v1_ingest_s
    } else {
        0.0
    };

    // Register the HNSW index BEFORE flush (the flush builds the sidecar).
    let hnsw_ddl_ok = sess
        .execute("CREATE INDEX docs_emb_hnsw ON docs USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 64)")
        .await
        .is_ok();
    eprintln!(
        "[ext_bench_vector] HNSW DDL: {}",
        if hnsw_ddl_ok { "ok" } else { "rejected" }
    );

    // V4: HNSW build time == the flush that constructs the sidecar.
    let build_start = Instant::now();
    instance.shard.flush_to_parquet().await.unwrap();
    let hnsw_build_s = build_start.elapsed().as_secs_f64();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }
    eprintln!(
        "[ext_bench_vector] V1 ingest {v1_rate:.0} rows/s; HNSW/flush build {hnsw_build_s:.2}s"
    );

    let table = TableName::new("docs").unwrap();
    let query = det_vec(424_242, dim);
    let qlit = vector_lit(&query);

    // ── V3: brute-force <-> ORDER BY LIMIT 10 ─────────────────────────────────
    let brute_sql =
        format!("SELECT id FROM docs ORDER BY l2_distance(embedding, '{qlit}') LIMIT {k}");
    let v3_brute = basin_p50(&sess, &brute_sql, samples).await;
    let brute_ids: Vec<i64> = {
        let res = sess.execute(&brute_sql).await.expect("basin brute knn");
        ids_of(&res)
    };

    // ── V5: HNSW kNN latency (programmatic) + recall@10 ───────────────────────
    let mut v5_hnsw: Option<f64> = None;
    let mut hnsw_ids: Vec<i64> = Vec::new();
    {
        // warm
        let _ = sess
            .vector_search(&table, "embedding", query.clone(), k, Distance::L2)
            .await;
        let mut s = Vec::with_capacity(samples);
        let mut ok = true;
        for _ in 0..samples {
            let t = Instant::now();
            match sess
                .vector_search(&table, "embedding", query.clone(), k, Distance::L2)
                .await
            {
                Ok(batches) => {
                    s.push(t.elapsed().as_secs_f64() * 1000.0);
                    if hnsw_ids.is_empty() {
                        for b in &batches {
                            if let Ok(idx) = b.schema().index_of("id") {
                                if let Some(a) = b.column(idx).as_any().downcast_ref::<Int64Array>()
                                {
                                    for i in 0..a.len() {
                                        hnsw_ids.push(a.value(i));
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[ext_bench_vector] vector_search failed: {e}");
                    ok = false;
                    break;
                }
            }
        }
        if ok && !s.is_empty() {
            v5_hnsw = Some(median(&s));
        }
    }
    // recall@10: fraction of brute-force ids recovered by HNSW.
    let recall_at_10: f64 = if brute_ids.is_empty() || hnsw_ids.is_empty() {
        f64::NAN
    } else {
        let brute_set: std::collections::HashSet<i64> = brute_ids.iter().copied().collect();
        let hit = hnsw_ids.iter().filter(|x| brute_set.contains(x)).count();
        hit as f64 / brute_ids.len() as f64
    };
    eprintln!("[ext_bench_vector] V5 HNSW p50={v5_hnsw:?} recall@10={recall_at_10:.3}");
    // Correctness gate: HNSW recall must be reasonable when it ran.
    if v5_hnsw.is_some() && recall_at_10.is_finite() {
        assert!(
            recall_at_10 >= 0.6,
            "HNSW recall@10 too low: {recall_at_10:.3} (expected >= 0.6 on identical data)"
        );
    }

    // ── V6: filtered kNN (WHERE category = ? AND ORDER BY <-> LIMIT 10) ───────
    let v6_sql = format!(
        "SELECT id FROM docs WHERE category = 'news' ORDER BY l2_distance(embedding, '{qlit}') LIMIT {k}"
    );
    let v6 = basin_p50(&sess, &v6_sql, samples).await;
    let v6_rows = sess
        .execute(&v6_sql)
        .await
        .map(|r| rows_of(&r))
        .unwrap_or(0);

    // ── V7: distance-threshold WHERE ──────────────────────────────────────────
    let v7_sql = format!("SELECT count(*) FROM docs WHERE l2_distance(embedding, '{qlit}') < 5.0");
    let v7 = basin_p50(&sess, &v7_sql, samples).await;

    instance.wal.close().await.unwrap();

    // ── V2: big-dim (1536) ingest probe — separate engine ────────────────────
    let v2_rate;
    {
        let mut big = build_basin_engine().await;
        let bsess = big.engine.open_session(big.project).await.unwrap();
        let v2_ingest_s = seed_vectors(&bsess, "big", bigdim_rows, bigdim, false).await;
        v2_rate = if v2_ingest_s > 0.0 {
            bigdim_rows as f64 / v2_ingest_s
        } else {
            0.0
        };
        big.shard.flush_to_parquet().await.unwrap();
        if let Some(bg) = big.bg.take() {
            bg.shutdown().await;
        }
        big.wal.close().await.unwrap();
        eprintln!(
            "[ext_bench_vector] V2 big-dim ingest {v2_rate:.0} rows/s ({bigdim}d x {bigdim_rows})"
        );
    }

    // ── PG twin (pgvector hnsw) ───────────────────────────────────────────────
    let mut pg_available = false;
    let mut pg_ext_ok = false;
    let mut pg_ingest_rate: Option<f64> = None;
    let mut pg_hnsw_build_s: Option<f64> = None;
    let (mut pg_v3, mut pg_v5, mut pg_v6, mut pg_v7) = (None, None, None, None);

    if let Some((pg, cs)) = try_connect().await {
        pg_available = true;
        let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
        let schema = format!("basin_ext_vec_{suffix}");
        let _guard = SchemaGuard {
            schema: schema.clone(),
            conn_str: cs,
        };
        pg.simple_query(&format!("CREATE SCHEMA {schema}"))
            .await
            .ok();
        pg.simple_query("SET maintenance_work_mem = '256MB'")
            .await
            .ok();

        pg_ext_ok = pg
            .simple_query("CREATE EXTENSION IF NOT EXISTS vector")
            .await
            .is_ok();
        eprintln!(
            "[ext_bench_vector] PG pgvector: {}",
            if pg_ext_ok { "ok" } else { "UNAVAILABLE" }
        );

        if pg_ext_ok {
            pg.simple_query(&format!(
                "CREATE TABLE {schema}.docs (id BIGINT PRIMARY KEY, category TEXT NOT NULL, embedding vector({dim}))"
            )).await.ok();

            let pgstart = Instant::now();
            let batch = 4_000usize;
            let mut off = 0usize;
            while off < rows {
                let hi = (off + batch).min(rows);
                let mut stmt = String::with_capacity((hi - off) * dim * 10);
                stmt.push_str(&format!("INSERT INTO {schema}.docs VALUES "));
                for kk in off..hi {
                    if kk > off {
                        stmt.push(',');
                    }
                    let v = det_vec(kk as u64, dim);
                    let cat = CATEGORIES[kk % CATEGORIES.len()];
                    stmt.push_str(&format!("({kk}, '{cat}', '{}')", vector_lit(&v)));
                }
                pg.simple_query(&stmt).await.expect("pg vector seed");
                off = hi;
            }
            let pg_s = pgstart.elapsed().as_secs_f64();
            pg_ingest_rate = if pg_s > 0.0 {
                Some(rows as f64 / pg_s)
            } else {
                None
            };

            let bstart = Instant::now();
            pg.simple_query(&format!(
                "CREATE INDEX docs_emb_hnsw ON {schema}.docs USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 64)"
            )).await.expect("pg hnsw build");
            pg_hnsw_build_s = Some(bstart.elapsed().as_secs_f64());
            pg.simple_query(&format!("ANALYZE {schema}.docs"))
                .await
                .ok();

            pg_v3 = pg_p50(
                &pg,
                &format!("SELECT id FROM {schema}.docs ORDER BY embedding <-> '{qlit}' LIMIT {k}"),
                samples,
            )
            .await;
            // V5 = same shape but the index is active; pgvector serves <-> via hnsw.
            pg_v5 = pg_v3;
            pg_v6 = pg_p50(&pg, &format!(
                "SELECT id FROM {schema}.docs WHERE category = 'news' ORDER BY embedding <-> '{qlit}' LIMIT {k}"), samples).await;
            pg_v7 = pg_p50(
                &pg,
                &format!("SELECT count(*) FROM {schema}.docs WHERE embedding <-> '{qlit}' < 5.0"),
                samples,
            )
            .await;
        }

        let _ = pg
            .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await;
        std::mem::forget(_guard);
    } else {
        eprintln!("[ext_bench_vector] PG unavailable — Basin-only card");
    }

    // ── Emit artifact ────────────────────────────────────────────────────────
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    write_artifact(
        "ext_bench_vector.json",
        &json!({
            "card": "ext_bench_vector",
            "family": "pgvector",
            "generated_at": format!("@{ts}"),
            "pg_available": pg_available,
            "pg_extension_available": pg_ext_ok,
            "basin_hnsw_ddl_ok": hnsw_ddl_ok,
            "config": {
                "rows": rows, "dim": dim, "bigdim": bigdim, "bigdim_rows": bigdim_rows,
                "samples": samples, "k": k, "hnsw_m": 16, "hnsw_ef_construction": 64,
            },
            "ingest": [
                { "label": "V1: ingest 100k x 128d (rows/s)", "basin_rows_per_s": v1_rate, "pg_rows_per_s": pg_ingest_rate },
                { "label": "V2: ingest 1536d (rows/s, OpenAI width)", "basin_rows_per_s": v2_rate, "pg_rows_per_s": serde_json::Value::Null },
            ],
            "build": {
                "label": "V4: HNSW build time (s, 128d)",
                "basin_build_s": hnsw_build_s,
                "pg_build_s": pg_hnsw_build_s,
            },
            "correctness": {
                "label": "recall@10 (HNSW vs brute-force, same data)",
                "basin_recall_at_10": if recall_at_10.is_finite() { json!(recall_at_10) } else { serde_json::Value::Null },
                "brute_ids": brute_ids,
                "hnsw_ids": hnsw_ids,
            },
            "shapes": [
                { "label": "V3: brute-force <-> ORDER BY LIMIT 10", "basin_p50_ms": opt_ms(v3_brute),
                  "pg_p50_ms": opt_ms(pg_v3), "basin_over_pg": ratio(v3_brute, pg_v3) },
                { "label": "V5: HNSW-backed kNN latency (LIMIT 10)", "basin_p50_ms": opt_ms(v5_hnsw),
                  "pg_p50_ms": opt_ms(pg_v5), "basin_over_pg": ratio(v5_hnsw, pg_v5),
                  "basin_uses_hnsw": hnsw_ddl_ok && v5_hnsw.is_some(), "pg_uses_hnsw": pg_ext_ok },
                { "label": "V6: filtered kNN (WHERE category= AND ORDER BY <->)", "basin_p50_ms": opt_ms(v6),
                  "pg_p50_ms": opt_ms(pg_v6), "basin_over_pg": ratio(v6, pg_v6), "basin_rows": v6_rows },
                { "label": "V7: distance-threshold WHERE (l2_distance < r)", "basin_p50_ms": opt_ms(v7),
                  "pg_p50_ms": opt_ms(pg_v7), "basin_over_pg": ratio(v7, pg_v7) },
            ],
            "note": "recall@10 is a correctness metric — HNSW top-10 vs brute-force top-10 on \
                     identical data. Basin HNSW build time is measured as the flush that \
                     constructs the sidecar (the real deployment hook). V5 uses the \
                     programmatic vector_search HNSW path; V3 is the brute-force SQL <-> scan. \
                     PG numbers (when pgvector is installable) use a vector_l2_ops hnsw index.",
        }),
    );
}
