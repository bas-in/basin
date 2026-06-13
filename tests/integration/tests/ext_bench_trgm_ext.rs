//! EXTENSIONS BENCHMARK SUITE — pg_trgm EXTENDED shapes (coverage expansion).
//!
//! Separate file (the main `ext_bench_trgm.rs` card may be touched by a feature
//! agent — this NEW file adds the audit's non-conflicting coverage gaps without
//! editing that body). Separate artifact
//! (`benchmark/data/ext_bench_trgm_ext.json`); the runner routes it to a
//! size-suffixed name (`ext_bench_trgm_ext_100k.json`).
//!
//! Closes the audit's pg_trgm P0/P1 gaps:
//!
//!   TE1 `ILIKE '%substr%'` accelerated by the trigram GIN index — THE #1
//!       production use of pg_trgm (making substring search index-backed). The
//!       base card measures `%`/`<->`; ILIKE-via-trgm is distinct and untested.
//!   TE2 autocomplete prefix top-N (`name ILIKE 'prefix%'` ORDER BY name
//!       LIMIT 10) — the typeahead/as-you-type latency shape.
//!
//! Basin builds a `gin_trgm_ops` index (landed in base); the card records
//! whether the ILIKE path is index-backed (`basin_index_built`). PG
//! head-to-head is opportunistic (`pg_available:false` + Basin-only timings
//! when pg_trgm is unavailable); the card NEVER fails on a missing extension.
//!
//! ## Env knobs (shared with the base card via the runner)
//!   * `BASIN_EXT_BENCH_ROWS`         — string count (default 100_000)
//!   * `BASIN_EXT_BENCH_TRGM_SAMPLES` — samples per shape (default 25)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow_array::{Array, Int64Array};
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
        eprintln!("[ext_bench_trgm_ext] artifact written: {}", path.display());
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

/// Same name distribution as ext_bench_trgm.rs (shared seed).
fn name_for(i: usize) -> String {
    const FIRST: &[&str] = &[
        "alice", "alyce", "bob", "carol", "dave", "erin", "frank", "grace",
        "heidi", "ivan", "judy", "mallory", "olivia", "peggy", "trent", "victor",
    ];
    const LAST: &[&str] = &[
        "smith", "smyth", "jones", "brown", "taylor", "wilson", "davies", "evans",
    ];
    let first = FIRST[i % FIRST.len()];
    let last = LAST[(i / FIRST.len()) % LAST.len()];
    if i % 200 == 0 {
        format!("zephyrine {first} {last}{}", i)
    } else {
        format!("{first} {last}{}", i % 5000)
    }
}

// ILIKE substring (mid-string match — only an index that understands trigrams
// can accelerate this). Prefix for autocomplete.
const ILIKE_SUBSTR: &str = "%mith%";
const PREFIX: &str = "smith%";

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

async fn pg_count(pg: &tokio_postgres::Client, sql: &str) -> i64 {
    pg.query_one(sql, &[]).await.map(|r| r.get::<usize, i64>(0)).unwrap_or(-1)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "extensions benchmark: needs idle box (+ live PG with pg_trgm for head-to-head); run via benchmark/run/extensions-suite.sh"]
async fn ext_bench_trgm_ext() {
    let rows = env_usize("BASIN_EXT_BENCH_ROWS", 100_000);
    let samples = env_usize("BASIN_EXT_BENCH_TRGM_SAMPLES", 25);
    eprintln!("[ext_bench_trgm_ext] config: rows={rows} samples={samples}");

    let mut instance = build_basin_engine().await;
    let sess = instance.engine.open_session(instance.project).await.unwrap();
    sess.execute("CREATE TABLE people (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();

    let batch = 5_000usize;
    let mut off = 0usize;
    while off < rows {
        let hi = (off + batch).min(rows);
        let mut stmt = String::with_capacity((hi - off) * 48);
        stmt.push_str("INSERT INTO people VALUES ");
        for k in off..hi {
            if k > off {
                stmt.push(',');
            }
            let nm = name_for(k).replace('\'', "''");
            stmt.push_str(&format!("({k},'{nm}')"));
        }
        sess.execute(&stmt).await.expect("basin trgm seed");
        off = hi;
    }
    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }

    let basin_index_built = sess
        .execute("CREATE INDEX people_trgm ON people USING gin (name gin_trgm_ops)")
        .await
        .is_ok();

    // TE1: ILIKE substring. TE2: autocomplete prefix top-N.
    let te1_sql = format!("SELECT count(*) FROM people WHERE name ILIKE '{ILIKE_SUBSTR}'");
    let te2_sql = format!("SELECT id FROM people WHERE name ILIKE '{PREFIX}' ORDER BY name LIMIT 10");

    let te1_b = basin_p50(&sess, &te1_sql, samples).await;
    let te1_count = sess.execute(&te1_sql).await.map(|r| basin_count(&r)).unwrap_or(-1);
    let te2_b = basin_p50(&sess, &te2_sql, samples).await;
    let te2_sup = sess.execute(&te2_sql).await.is_ok();

    println!(
        "[ext_bench_trgm_ext] Basin: TE1(ilike,c={te1_count})={te1_b:?} TE2(prefix,sup={te2_sup})={te2_b:?} index={basin_index_built}"
    );

    instance.wal.close().await.unwrap();

    // ── PG twin (gin_trgm_ops indexed) ────────────────────────────────────────
    let mut pg_available = false;
    let (mut pg_te1, mut pg_te2) = (None, None);
    let mut pg_te1_count = -1i64;

    if let Some((pg, cs)) = try_connect().await {
        let ext_ok = pg
            .simple_query("CREATE EXTENSION IF NOT EXISTS pg_trgm")
            .await
            .is_ok();
        if ext_ok {
            pg_available = true;
            let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
            let schema = format!("basin_ext_trgmx_{suffix}");
            let _guard = SchemaGuard { schema: schema.clone(), conn_str: cs };
            pg.simple_query(&format!("CREATE SCHEMA {schema}")).await.ok();
            pg.simple_query(&format!(
                "CREATE TABLE {schema}.people (id BIGINT PRIMARY KEY, name TEXT NOT NULL)"
            ))
            .await
            .ok();
            let mut po = 0usize;
            while po < rows {
                let hi = (po + batch).min(rows);
                let mut v = String::with_capacity((hi - po) * 48);
                for k in po..hi {
                    if k > po {
                        v.push(',');
                    }
                    let nm = name_for(k).replace('\'', "''");
                    v.push_str(&format!("({k},'{nm}')"));
                }
                pg.simple_query(&format!("INSERT INTO {schema}.people VALUES {v}")).await.ok();
                po = hi;
            }
            // gin_trgm_ops accelerates ILIKE substring; also a btree on lower(name)
            // is irrelevant for mid-string, so only the trgm GIN is the honest comparator.
            pg.simple_query(&format!("CREATE INDEX people_trgm_gin ON {schema}.people USING gin (name gin_trgm_ops)")).await.ok();
            pg.simple_query(&format!("ANALYZE {schema}.people")).await.ok();

            pg_te1_count = pg_count(&pg, &format!("SELECT count(*)::bigint FROM {schema}.people WHERE name ILIKE '{ILIKE_SUBSTR}'")).await;
            pg_te1 = pg_p50(&pg, &format!("SELECT count(*) FROM {schema}.people WHERE name ILIKE '{ILIKE_SUBSTR}'"), samples).await;
            pg_te2 = pg_p50(&pg, &format!("SELECT id FROM {schema}.people WHERE name ILIKE '{PREFIX}' ORDER BY name LIMIT 10"), samples).await;

            let _ = pg.simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE")).await;
            std::mem::forget(_guard);
        } else {
            eprintln!("[ext_bench_trgm_ext] PG lacks pg_trgm — Basin-only card");
        }
    } else {
        eprintln!("[ext_bench_trgm_ext] PG unavailable — Basin-only card");
    }

    // Correctness cross-check on the ILIKE count.
    if pg_available && te1_count >= 0 && pg_te1_count >= 0 {
        assert_eq!(te1_count, pg_te1_count, "TE1 ILIKE count mismatch: basin {te1_count} != pg {pg_te1_count}");
    }

    let ts = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    write_artifact("ext_bench_trgm_ext.json", &json!({
        "card": "ext_bench_trgm_ext",
        "family": "pg_trgm",
        "generated_at": format!("@{ts}"),
        "pg_available": pg_available,
        "pg_extension_available": pg_available,
        "basin_index_built": basin_index_built,
        "config": { "rows": rows, "samples": samples },
        "shapes": [
            { "label": "TE1: ILIKE '%substr%' (trgm-GIN-accelerated substring search)",
              "basin_p50_ms": opt_ms(te1_b), "pg_p50_ms": opt_ms(pg_te1), "basin_over_pg": ratio(te1_b, pg_te1),
              "basin_hits": te1_count, "pg_hits": pg_te1_count },
            { "label": "TE2: autocomplete prefix top-N (ILIKE 'pre%' ORDER BY name LIMIT 10)",
              "basin_supported": te2_sup, "basin_p50_ms": opt_ms(te2_b), "pg_p50_ms": opt_ms(pg_te2),
              "basin_over_pg": ratio(te2_b, pg_te2) },
        ],
        "note": "Coverage-expansion card for pg_trgm: ILIKE '%substr%' (the #1 production \
                 use of pg_trgm — index-backed mid-string search, which the base card does \
                 not measure) and an autocomplete prefix top-N. PG gets a gin_trgm_ops index; \
                 Basin records whether its own gin_trgm_ops index was built. The ILIKE count \
                 is hard-asserted equal across engines when PG is available.",
    }));
}
