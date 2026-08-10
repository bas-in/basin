//! EXTENSIONS BENCHMARK SUITE — JSONB-GIN family performance card.
//!
//! Separate artifact (`benchmark/data/ext_bench_jsonb_gin.json`), separate
//! runner. Goes BEYOND the main compare_postgres JSONB read shapes: this card
//! is about the GIN index family — `@>` containment, `?` existence, deep-path
//! filters, GIN build time, and write-with-GIN ingest overhead. Verified
//! against `viability_jsonb_posting.rs` / `jsonb_index_harness.rs` /
//! `jsonb_deep_path.rs`:
//!
//!   * `CREATE INDEX … USING gin (payload)` (posting-list row-group prune)
//!   * `payload @> '{...}'` containment at needle densities
//!   * `payload ? 'key'` existence
//!   * `jsonb_path_exists(payload, '$. ...')` deep filter
//!
//! ## Shapes
//!   J1 @> — rare needle   (low density / high selectivity)
//!   J2 @> — common needle (high density)
//!   J3 ? existence
//!   J4 jsonb_path_exists deep filter (probed)
//!   J5 GIN build time (s) — index over 100k payloads
//!   J6 write-with-GIN ingest overhead % (ingest into a GIN table vs a plain one)
//!
//! ## Excluded as unimplemented (probed, recorded if absent)
//!   * Any shape whose Basin SQL errors → recorded `basin_supported:false`,
//!     PG-only timing; the card never fails.
//!
//! ## PG head-to-head
//! JSONB-GIN is core PG (no extension). PG uses a `jsonb_ops` GIN index.
//!
//! ## Env knobs
//!   * `BASIN_EXT_BENCH_ROWS`           — payload count (default 100_000)
//!   * `BASIN_EXT_BENCH_JSONB_SAMPLES`  — samples per shape (default 25)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow_array::{Array, Int64Array};
use basin_common::ProjectId;
use basin_engine::ExecResult;
use serde_json::json;
use tokio_postgres::SimpleQueryMessage;

#[path = "compare_postgres_common.rs"]
mod common;

use common::{build_basin_engine, median, payload_for, try_connect, SchemaGuard};

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
        eprintln!("[ext_bench_jsonb_gin] artifact written: {}", path.display());
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

fn basin_count(res: &ExecResult) -> i64 {
    match res {
        ExecResult::Rows { batches, .. } => {
            let mut total = 0i64;
            for b in batches {
                if let Some(a) = b.column(0).as_any().downcast_ref::<Int64Array>() {
                    for i in 0..a.len() {
                        total = total.saturating_add(a.value(i));
                    }
                }
            }
            total
        }
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

async fn pg_count(pg: &tokio_postgres::Client, sql: &str) -> i64 {
    pg.query_one(sql, &[])
        .await
        .map(|r| r.get::<usize, i64>(0))
        .unwrap_or(-1)
}

/// Seed a `(id, payload)` JSONB table on Basin. Returns ingest seconds.
async fn seed_basin_jsonb(
    sess: &basin_engine::ProjectSession,
    table: &str,
    rows: usize,
    with_gin: bool,
) -> (f64, bool) {
    sess.execute(&format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL, payload JSONB NOT NULL)"
    ))
    .await
    .unwrap();
    let mut gin_ok = false;
    if with_gin {
        // Build GIN BEFORE the inserts so the write path pays the index cost
        // (this is what J6 "write-with-GIN overhead" measures).
        gin_ok = sess
            .execute(&format!(
                "CREATE INDEX {table}_gin ON {table} USING gin (payload)"
            ))
            .await
            .is_ok();
    }
    let batch = 5_000usize;
    let mut off = 0usize;
    let start = Instant::now();
    while off < rows {
        let hi = (off + batch).min(rows);
        let mut stmt = String::with_capacity((hi - off) * 256);
        stmt.push_str(&format!("INSERT INTO {table} VALUES "));
        for k in off..hi {
            if k > off {
                stmt.push(',');
            }
            let p = payload_for(k as i64);
            stmt.push_str(&format!("({k}, '{p}')"));
        }
        sess.execute(&stmt).await.expect("basin jsonb seed");
        off = hi;
    }
    (start.elapsed().as_secs_f64(), gin_ok)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "extensions benchmark: needs idle box (+ live PG with extension for head-to-head); run via benchmark/run/extensions-suite.sh"]
async fn ext_bench_jsonb_gin() {
    let rows = env_usize("BASIN_EXT_BENCH_ROWS", 100_000);
    let samples = env_usize("BASIN_EXT_BENCH_JSONB_SAMPLES", 25);
    eprintln!("[ext_bench_jsonb_gin] config: rows={rows} samples={samples}");

    // ── Basin: plain table (J6 baseline) ─────────────────────────────────────
    let mut instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();

    // J6 baseline: ingest WITHOUT a GIN index.
    let (plain_ingest_s, _) = seed_basin_jsonb(&sess, "docs_plain", rows, false).await;
    let basin_plain_rate = if plain_ingest_s > 0.0 {
        rows as f64 / plain_ingest_s
    } else {
        0.0
    };

    // J6 overhead: ingest WITH a GIN index from the start.
    let (gin_ingest_s, gin_ddl_ok) = seed_basin_jsonb(&sess, "docs_gin", rows, true).await;
    let basin_gin_rate = if gin_ingest_s > 0.0 {
        rows as f64 / gin_ingest_s
    } else {
        0.0
    };
    let basin_write_overhead_pct = if basin_gin_rate > 0.0 && basin_plain_rate > 0.0 {
        (basin_plain_rate / basin_gin_rate - 1.0) * 100.0
    } else {
        f64::NAN
    };

    // J5: GIN build time over the plain table (build the index after the fact).
    let j5_build_s = {
        let t = Instant::now();
        let ok = sess
            .execute("CREATE INDEX docs_plain_gin ON docs_plain USING gin (payload)")
            .await
            .is_ok();
        if ok {
            Some(t.elapsed().as_secs_f64())
        } else {
            None
        }
    };

    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }
    eprintln!(
        "[ext_bench_jsonb_gin] basin plain {basin_plain_rate:.0} rows/s, gin {basin_gin_rate:.0} rows/s, \
         write_overhead={basin_write_overhead_pct:.1}%, gin_ddl_ok={gin_ddl_ok}, j5_build={j5_build_s:?}"
    );

    // ── Basin read shapes (over the GIN-indexed table) ───────────────────────
    // payload_for: category cycles purchase/signup/click (1/3 each); campaign
    // cycles promo_2024/spring_sale/newsletter/referral (1/4 each). Combine for
    // density spread:
    //   rare    → category=purchase AND campaign=referral  (~1/12)
    //   common  → category=purchase                        (~1/3)
    let j1_sql = "SELECT count(*) FROM docs_gin WHERE payload @> '{\"category\":\"purchase\",\"metadata\":{\"campaign\":\"referral\"}}'";
    let j2_sql = "SELECT count(*) FROM docs_gin WHERE payload @> '{\"category\":\"purchase\"}'";
    let j3_sql = "SELECT count(*) FROM docs_gin WHERE payload ? 'metadata'";
    let j4_sql = "SELECT count(*) FROM docs_gin WHERE jsonb_path_exists(payload, '$.tags[*] ? (@ == \"blue\")')";

    let j1_b = basin_p50(&sess, j1_sql, samples).await;
    let j1_count = sess
        .execute(j1_sql)
        .await
        .map(|r| basin_count(&r))
        .unwrap_or(-1);
    let j2_b = basin_p50(&sess, j2_sql, samples).await;
    let j2_count = sess
        .execute(j2_sql)
        .await
        .map(|r| basin_count(&r))
        .unwrap_or(-1);
    let j3_b = basin_p50(&sess, j3_sql, samples).await;
    let j3_count = sess
        .execute(j3_sql)
        .await
        .map(|r| basin_count(&r))
        .unwrap_or(-1);
    let j4_b = basin_p50(&sess, j4_sql, samples).await;
    let j4_supported = j4_b.is_some();
    let j4_count = if j4_supported {
        sess.execute(j4_sql)
            .await
            .map(|r| basin_count(&r))
            .unwrap_or(-1)
    } else {
        -1
    };

    println!(
        "[ext_bench_jsonb_gin] Basin: J1(rare={j1_count})={:?} J2(common={j2_count})={:?} \
         J3(exists={j3_count})={:?} J4(path,sup={j4_supported},c={j4_count})={:?}",
        j1_b, j2_b, j3_b, j4_b
    );

    instance.wal.close().await.unwrap();

    // ── PG twin (jsonb_ops GIN, core PG) ──────────────────────────────────────
    let mut pg_available = false;
    let mut pg_plain_rate: Option<f64> = None;
    let mut pg_gin_rate: Option<f64> = None;
    let mut pg_write_overhead_pct: Option<f64> = None;
    let mut pg_j5_build_s: Option<f64> = None;
    let (mut pg_j1, mut pg_j2, mut pg_j3, mut pg_j4) = (None, None, None, None);
    let (mut pg_j1_count, mut pg_j2_count, mut pg_j3_count) = (-1i64, -1i64, -1i64);

    if let Some((pg, cs)) = try_connect().await {
        pg_available = true;
        let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
        let schema = format!("basin_ext_jsonb_{suffix}");
        let _guard = SchemaGuard {
            schema: schema.clone(),
            conn_str: cs,
        };
        pg.simple_query(&format!("CREATE SCHEMA {schema}"))
            .await
            .ok();
        pg.simple_query("SET work_mem = '32MB'").await.ok();
        pg.simple_query("SET maintenance_work_mem = '128MB'")
            .await
            .ok();

        // J6 baseline ingest (no index).
        pg.simple_query(&format!(
            "CREATE TABLE {schema}.docs_plain (id BIGINT, payload JSONB NOT NULL)"
        ))
        .await
        .ok();
        let seed_pg = |client_off: usize, client_hi: usize| {
            let mut stmt = String::with_capacity((client_hi - client_off) * 256);
            for k in client_off..client_hi {
                if k > client_off {
                    stmt.push(',');
                }
                let p = payload_for(k as i64);
                stmt.push_str(&format!("({k}, '{p}')"));
            }
            stmt
        };
        let batch = 5_000usize;
        let p1start = Instant::now();
        let mut po = 0usize;
        while po < rows {
            let hi = (po + batch).min(rows);
            pg.simple_query(&format!(
                "INSERT INTO {schema}.docs_plain VALUES {}",
                seed_pg(po, hi)
            ))
            .await
            .ok();
            po = hi;
        }
        let p1s = p1start.elapsed().as_secs_f64();
        pg_plain_rate = if p1s > 0.0 {
            Some(rows as f64 / p1s)
        } else {
            None
        };

        // J6 with-GIN ingest.
        pg.simple_query(&format!(
            "CREATE TABLE {schema}.docs_gin (id BIGINT, payload JSONB NOT NULL)"
        ))
        .await
        .ok();
        pg.simple_query(&format!(
            "CREATE INDEX docs_gin_gin ON {schema}.docs_gin USING gin (payload)"
        ))
        .await
        .ok();
        let p2start = Instant::now();
        let mut po = 0usize;
        while po < rows {
            let hi = (po + batch).min(rows);
            pg.simple_query(&format!(
                "INSERT INTO {schema}.docs_gin VALUES {}",
                seed_pg(po, hi)
            ))
            .await
            .ok();
            po = hi;
        }
        let p2s = p2start.elapsed().as_secs_f64();
        pg_gin_rate = if p2s > 0.0 {
            Some(rows as f64 / p2s)
        } else {
            None
        };
        if let (Some(plain), Some(gin)) = (pg_plain_rate, pg_gin_rate) {
            if gin > 0.0 {
                pg_write_overhead_pct = Some((plain / gin - 1.0) * 100.0);
            }
        }

        // J5 GIN build time over docs_plain.
        let b5 = Instant::now();
        pg.simple_query(&format!(
            "CREATE INDEX docs_plain_gin ON {schema}.docs_plain USING gin (payload)"
        ))
        .await
        .ok();
        pg_j5_build_s = Some(b5.elapsed().as_secs_f64());
        pg.simple_query(&format!("ANALYZE {schema}.docs_gin"))
            .await
            .ok();

        pg_j1_count = pg_count(&pg, &format!(
            "SELECT count(*)::bigint FROM {schema}.docs_gin WHERE payload @> '{{\"category\":\"purchase\",\"metadata\":{{\"campaign\":\"referral\"}}}}'")).await;
        pg_j2_count = pg_count(&pg, &format!(
            "SELECT count(*)::bigint FROM {schema}.docs_gin WHERE payload @> '{{\"category\":\"purchase\"}}'")).await;
        pg_j3_count = pg_count(
            &pg,
            &format!("SELECT count(*)::bigint FROM {schema}.docs_gin WHERE payload ? 'metadata'"),
        )
        .await;

        pg_j1 = pg_p50(&pg, &format!(
            "SELECT count(*) FROM {schema}.docs_gin WHERE payload @> '{{\"category\":\"purchase\",\"metadata\":{{\"campaign\":\"referral\"}}}}'"), samples).await;
        pg_j2 = pg_p50(&pg, &format!(
            "SELECT count(*) FROM {schema}.docs_gin WHERE payload @> '{{\"category\":\"purchase\"}}'"), samples).await;
        pg_j3 = pg_p50(
            &pg,
            &format!("SELECT count(*) FROM {schema}.docs_gin WHERE payload ? 'metadata'"),
            samples,
        )
        .await;
        pg_j4 = pg_p50(&pg, &format!(
            "SELECT count(*) FROM {schema}.docs_gin WHERE jsonb_path_exists(payload, '$.tags[*] ? (@ == \"blue\")')"), samples).await;

        let _ = pg
            .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await;
        std::mem::forget(_guard);
    } else {
        eprintln!("[ext_bench_jsonb_gin] PG unavailable — Basin-only card");
    }

    // ── Correctness cross-check ───────────────────────────────────────────────
    if pg_available {
        if j1_count >= 0 && pg_j1_count >= 0 {
            assert_eq!(
                j1_count, pg_j1_count,
                "J1 @> rare count mismatch: basin {j1_count} != pg {pg_j1_count}"
            );
        }
        if j2_count >= 0 && pg_j2_count >= 0 {
            assert_eq!(
                j2_count, pg_j2_count,
                "J2 @> common count mismatch: basin {j2_count} != pg {pg_j2_count}"
            );
        }
        if j3_count >= 0 && pg_j3_count >= 0 {
            assert_eq!(
                j3_count, pg_j3_count,
                "J3 ? existence count mismatch: basin {j3_count} != pg {pg_j3_count}"
            );
        }
    }

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    write_artifact(
        "ext_bench_jsonb_gin.json",
        &json!({
            "card": "ext_bench_jsonb_gin",
            "family": "jsonb_gin",
            "generated_at": format!("@{ts}"),
            "pg_available": pg_available,
            "pg_extension_available": pg_available,
            "basin_gin_ddl_ok": gin_ddl_ok,
            "config": { "rows": rows, "samples": samples },
            "build": {
                "label": "J5: GIN build time over payloads (s)",
                "basin_build_s": j5_build_s,
                "pg_build_s": pg_j5_build_s,
            },
            "write_overhead": {
                "label": "J6: write-with-GIN ingest overhead (%)",
                "basin_plain_rows_per_s": basin_plain_rate,
                "basin_gin_rows_per_s": basin_gin_rate,
                "basin_overhead_pct": if basin_write_overhead_pct.is_finite() { json!(basin_write_overhead_pct) } else { serde_json::Value::Null },
                "pg_plain_rows_per_s": pg_plain_rate,
                "pg_gin_rows_per_s": pg_gin_rate,
                "pg_overhead_pct": pg_write_overhead_pct,
            },
            "shapes": [
                { "label": "J1: @> rare needle (low density)", "basin_p50_ms": opt_ms(j1_b), "pg_p50_ms": opt_ms(pg_j1),
                  "basin_over_pg": ratio(j1_b, pg_j1), "basin_hits": j1_count, "pg_hits": pg_j1_count,
                  "basin_uses_gin": gin_ddl_ok, "pg_uses_gin": pg_available },
                { "label": "J2: @> common needle (high density)", "basin_p50_ms": opt_ms(j2_b), "pg_p50_ms": opt_ms(pg_j2),
                  "basin_over_pg": ratio(j2_b, pg_j2), "basin_hits": j2_count, "pg_hits": pg_j2_count },
                { "label": "J3: ? key existence", "basin_p50_ms": opt_ms(j3_b), "pg_p50_ms": opt_ms(pg_j3),
                  "basin_over_pg": ratio(j3_b, pg_j3), "basin_hits": j3_count, "pg_hits": pg_j3_count },
                { "label": "J4: jsonb_path_exists deep filter", "basin_supported": j4_supported,
                  "basin_p50_ms": opt_ms(j4_b), "pg_p50_ms": opt_ms(pg_j4), "basin_over_pg": ratio(j4_b, pg_j4),
                  "basin_hits": j4_count },
            ],
            "note": "JSONB-GIN is core PostgreSQL (no extension). Basin uses a per-(key,value) \
                     posting-list row-group prune behind CREATE INDEX … USING gin (payload); PG \
                     uses a jsonb_ops GIN index. @> / ? counts are hard-asserted equal across \
                     engines. J6 measures ingest-rate degradation from maintaining the GIN index \
                     (plain-table ingest vs with-GIN ingest). jsonb_path_exists is probed — if \
                     Basin cannot plan it the shape records basin_supported:false (card never fails).",
        }),
    );
}
