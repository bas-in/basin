//! EXTENSIONS BENCHMARK SUITE — TimescaleDB EXTENDED shapes (coverage expansion).
//!
//! Separate file (the main `ext_bench_timescale.rs` card is being extended by
//! the continuous-aggregates feature work — this NEW file adds non-conflicting
//! coverage gaps without touching that body). Separate artifact
//! (`benchmark/data/ext_bench_timescale_ext.json`); the runner routes it to a
//! size-suffixed name (`ext_bench_timescale_ext_1m.json`).
//!
//! Closes the audit's TimescaleDB P1 gaps that do NOT depend on the landing
//! continuous-aggregates / compression features:
//!
//!   TSE1 latest-value-per-series (`DISTINCT ON (device) ... ORDER BY device,
//!        ts DESC`) — the "current reading per device" realtime dashboard tile,
//!        distinct from the base card's first/last aggregate (TS4).
//!   TSE2 gap-fill (`time_bucket_gapfill` + LOCF) at scale — probed; records
//!        `basin_supported:false` + a PG-only timing if Basin can't plan it.
//!
//! DEFERRED to a post-merge expansion (depend on the landing features):
//!   * continuous-aggregate query vs equivalent raw rollup (cagg speedup)
//!   * compression ratio + post-compression query latency
//!
//! PG head-to-head is opportunistic (`pg_available:false` + Basin-only timings
//! when timescaledb is unavailable); the card NEVER fails on a missing extension.
//!
//! ## Env knobs (shared with the base card via the runner)
//!   * `BASIN_EXT_BENCH_ROWS`       — row count (default 1_000_000)
//!   * `BASIN_EXT_BENCH_TS_SAMPLES` — samples per shape (default 7)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use basin_common::ProjectId;
use basin_engine::ExecResult;
use serde_json::json;
use tokio_postgres::SimpleQueryMessage;

#[path = "compare_postgres_common.rs"]
mod common;

use common::{build_basin_engine, median, try_connect, SchemaGuard};

const EPOCH: i64 = 1_700_000_000;

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
        eprintln!("[ext_bench_timescale_ext] artifact written: {}", path.display());
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

/// Same `(id, ts, device, value)` series as ext_bench_timescale.rs (shared seed).
async fn seed_basin_series(sess: &basin_engine::ProjectSession, table: &str, rows: usize) {
    sess.execute(&format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL, ts TIMESTAMPTZ NOT NULL, device BIGINT NOT NULL, value DOUBLE PRECISION NOT NULL)"
    )).await.unwrap();
    let batch = 10_000usize;
    let mut off = 0usize;
    while off < rows {
        let hi = (off + batch).min(rows);
        let mut stmt = String::with_capacity((hi - off) * 64);
        stmt.push_str(&format!("INSERT INTO {table} VALUES "));
        for k in off..hi {
            if k > off {
                stmt.push(',');
            }
            let ts = EPOCH + k as i64;
            let device = (k % 100) as i64;
            let value = (k % 1000) as f64 * 0.5;
            stmt.push_str(&format!("({k}, to_timestamp({ts}), {device}, {value})"));
        }
        sess.execute(&stmt).await.expect("basin series seed");
        off = hi;
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
#[ignore = "extensions benchmark: needs idle box (+ live PG with timescaledb for head-to-head); run via benchmark/run/extensions-suite.sh"]
async fn ext_bench_timescale_ext() {
    let rows = env_usize("BASIN_EXT_BENCH_ROWS", 1_000_000);
    let samples = env_usize("BASIN_EXT_BENCH_TS_SAMPLES", 7);
    eprintln!("[ext_bench_timescale_ext] config: rows={rows} samples={samples}");

    let mut instance = build_basin_engine().await;
    let sess = instance.engine.open_session(instance.project).await.unwrap();
    seed_basin_series(&sess, "metrics", rows).await;
    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }

    // TSE1: latest reading per device (DISTINCT ON). TSE2: gapfill (probed).
    let tse1_sql = "SELECT DISTINCT ON (device) device, ts, value \
        FROM metrics ORDER BY device, ts DESC";
    let tse2_sql = "SELECT time_bucket_gapfill('1 hour', ts) AS b, device, \
        locf(avg(value)) AS v \
        FROM metrics GROUP BY b, device ORDER BY b, device";

    let tse1_b = basin_p50(&sess, tse1_sql, samples).await;
    let tse1_sup = sess.execute(tse1_sql).await.map(|r| rows_of(&r)).unwrap_or(0) > 0;
    let tse2_b = basin_p50(&sess, tse2_sql, samples).await;
    let tse2_sup = tse2_b.is_some();

    println!(
        "[ext_bench_timescale_ext] Basin: TSE1(latest,sup={tse1_sup})={tse1_b:?} TSE2(gapfill,sup={tse2_sup})={tse2_b:?}"
    );

    instance.wal.close().await.unwrap();

    // ── PG twin (opportunistic) ───────────────────────────────────────────────
    let mut pg_available = false;
    let mut ts_ext_available = false;
    let (mut pg_tse1, mut pg_tse2) = (None, None);

    if let Some((pg, cs)) = try_connect().await {
        pg_available = true;
        ts_ext_available = pg
            .simple_query("CREATE EXTENSION IF NOT EXISTS timescaledb")
            .await
            .is_ok();
        let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
        let schema = format!("basin_ext_tsx_{suffix}");
        let _guard = SchemaGuard { schema: schema.clone(), conn_str: cs };
        pg.simple_query(&format!("CREATE SCHEMA {schema}")).await.ok();
        pg.simple_query("SET work_mem = '64MB'").await.ok();
        pg.simple_query(&format!(
            "CREATE TABLE {schema}.metrics (id BIGINT, ts TIMESTAMPTZ NOT NULL, device BIGINT NOT NULL, value DOUBLE PRECISION NOT NULL)"
        )).await.ok();
        let batch = 10_000usize;
        let mut off = 0usize;
        while off < rows {
            let hi = (off + batch).min(rows);
            let mut v = String::with_capacity((hi - off) * 64);
            for k in off..hi {
                if k > off {
                    v.push(',');
                }
                let ts = EPOCH + k as i64;
                let device = (k % 100) as i64;
                let value = (k % 1000) as f64 * 0.5;
                v.push_str(&format!("({k}, to_timestamp({ts}), {device}, {value})"));
            }
            pg.simple_query(&format!("INSERT INTO {schema}.metrics VALUES {v}")).await.ok();
            off = hi;
        }
        pg.simple_query(&format!("CREATE INDEX metrics_dev_ts ON {schema}.metrics (device, ts DESC)")).await.ok();
        pg.simple_query(&format!("ANALYZE {schema}.metrics")).await.ok();

        pg_tse1 = pg_p50(&pg, &format!(
            "SELECT DISTINCT ON (device) device, ts, value FROM {schema}.metrics ORDER BY device, ts DESC"), samples).await;
        // gapfill needs the extension; only attempt when timescaledb loaded.
        if ts_ext_available {
            pg_tse2 = pg_p50(&pg, &format!(
                "SELECT time_bucket_gapfill('1 hour', ts) AS b, device, locf(avg(value)) AS v \
                 FROM {schema}.metrics GROUP BY b, device ORDER BY b, device"), samples).await;
        }

        let _ = pg.simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE")).await;
        std::mem::forget(_guard);
    } else {
        eprintln!("[ext_bench_timescale_ext] PG unavailable — Basin-only card");
    }

    let ts = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    write_artifact("ext_bench_timescale_ext.json", &json!({
        "card": "ext_bench_timescale_ext",
        "family": "timescaledb",
        "generated_at": format!("@{ts}"),
        "pg_available": pg_available,
        "pg_extension_available": ts_ext_available,
        "config": { "rows": rows, "samples": samples },
        "shapes": [
            { "label": "TSE1: latest-value-per-series (DISTINCT ON device, ts DESC)",
              "basin_supported": tse1_sup, "basin_p50_ms": opt_ms(tse1_b), "pg_p50_ms": opt_ms(pg_tse1),
              "basin_over_pg": ratio(tse1_b, pg_tse1) },
            { "label": "TSE2: time_bucket_gapfill + locf (sparse-series gap-fill) at scale",
              "basin_supported": tse2_sup, "basin_p50_ms": opt_ms(tse2_b), "pg_p50_ms": opt_ms(pg_tse2),
              "basin_over_pg": ratio(tse2_b, pg_tse2) },
        ],
        "note": "Coverage-expansion card for TimescaleDB: latest-value-per-series (DISTINCT \
                 ON, the realtime 'current reading per device' tile — distinct from the base \
                 card's first/last aggregate) and time_bucket_gapfill+locf at scale (probed; \
                 records basin_supported:false + a PG-only timing if Basin can't plan it). \
                 cagg-query-vs-raw and compression-ratio are deferred to a post-merge \
                 expansion once continuous aggregates + compression land.",
    }));
}
