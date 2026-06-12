//! EXTENSIONS BENCHMARK SUITE — TimescaleDB family performance card.
//!
//! Separate artifact (`benchmark/data/ext_bench_timescale.json`), separate
//! runner. Exercises the timescale surface Basin implements today (verified
//! against `timescale_conformance.rs`):
//!
//!   * `time_bucket('<interval>', ts)` GROUP BY (1m / 1h / 1d)
//!   * `create_hypertable(...)` DDL acceptance + hypertable ingest
//!   * `timescaledb_information.chunks` virtual view
//!
//! TimescaleDB-specific surfaces NOT implemented on Basin are benchmarked
//! against their PG-EQUIVALENT, and the metric LABEL says so explicitly:
//!   * `first()` / `last()` aggregates are NOT registered → both engines use a
//!     window-function / DISTINCT-ON equivalent (no Timescale, "PG-equivalent"
//!     in the label).
//!   * `drop_chunks` is NOT registered → Basin uses `DELETE WHERE ts < cutoff`;
//!     the metric is labelled "drop_chunks vs DELETE WHERE ts<cutoff".
//!
//! ## Shapes
//!   TS1 time_bucket('1 minute', ts) GROUP BY over 1M rows
//!   TS2 time_bucket('1 hour', ts)   GROUP BY
//!   TS3 time_bucket('1 day', ts)    GROUP BY
//!   TS4 first/last per group via DISTINCT-ON window (PG-equivalent, both sides)
//!   TS5 retention: Basin DELETE WHERE ts<cutoff vs PG DELETE WHERE ts<cutoff
//!       (drop_chunks not registered on Basin — labelled honestly)
//!   TS6 hypertable ingest rate vs plain-table ingest (both engines)
//!   TS7 chunks-view query overhead (Basin-only; PG timescale chunks view)
//!
//! ## Env knobs
//!   * `BASIN_EXT_BENCH_ROWS`        — row count (default 1_000_000)
//!   * `BASIN_EXT_BENCH_TS_SAMPLES`  — samples per shape (default 7)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use basin_common::ProjectId;
use basin_engine::ExecResult;
use serde_json::json;
use tokio_postgres::SimpleQueryMessage;

#[path = "compare_postgres_common.rs"]
mod common;

use common::{build_basin_engine, median, try_connect, BasinInstance, SchemaGuard};

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
        eprintln!("[ext_bench_timescale] artifact written: {}", path.display());
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

/// Seed a `(id, ts, device, value)` time-series table on Basin. `ts` for row i
/// is EPOCH + i seconds, so a 1M-row table spans ~11.5 days → meaningful 1m/1h/1d
/// bucket cardinality. Returns ingest seconds.
async fn seed_basin_series(sess: &basin_engine::ProjectSession, table: &str, rows: usize) -> f64 {
    sess.execute(&format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL, ts TIMESTAMPTZ NOT NULL, device BIGINT NOT NULL, value DOUBLE PRECISION NOT NULL)"
    )).await.unwrap();
    let batch = 10_000usize;
    let mut off = 0usize;
    let start = Instant::now();
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
    start.elapsed().as_secs_f64()
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
#[ignore = "extensions benchmark: needs idle box (+ live PG with extension for head-to-head); run via benchmark/run/extensions-suite.sh"]
async fn ext_bench_timescale() {
    let rows = env_usize("BASIN_EXT_BENCH_ROWS", 1_000_000);
    let samples = env_usize("BASIN_EXT_BENCH_TS_SAMPLES", 7);
    eprintln!("[ext_bench_timescale] config: rows={rows} samples={samples}");

    // ── Basin: plain-table ingest + bucket shapes ────────────────────────────
    let mut instance: BasinInstance = build_basin_engine().await;
    let sess = instance.engine.open_session(instance.project).await.unwrap();

    let plain_ingest_s = seed_basin_series(&sess, "metrics", rows).await;
    let basin_plain_rate = if plain_ingest_s > 0.0 { rows as f64 / plain_ingest_s } else { 0.0 };

    // Hypertable twin (TS6): create_hypertable is accepted-DDL on Basin.
    let hyper_create_ok = {
        let _ = sess.execute(
            "CREATE TABLE metrics_ht (id BIGINT NOT NULL, ts TIMESTAMPTZ NOT NULL, device BIGINT NOT NULL, value DOUBLE PRECISION NOT NULL)"
        ).await;
        sess.execute("SELECT create_hypertable('metrics_ht', 'ts', chunk_time_interval => INTERVAL '1 day')")
            .await
            .is_ok()
    };
    let basin_hyper_rate = if hyper_create_ok {
        let batch = 10_000usize;
        let mut off = 0usize;
        let start = Instant::now();
        while off < rows {
            let hi = (off + batch).min(rows);
            let mut stmt = String::with_capacity((hi - off) * 64);
            stmt.push_str("INSERT INTO metrics_ht VALUES ");
            for k in off..hi {
                if k > off {
                    stmt.push(',');
                }
                let ts = EPOCH + k as i64;
                let device = (k % 100) as i64;
                let value = (k % 1000) as f64 * 0.5;
                stmt.push_str(&format!("({k}, to_timestamp({ts}), {device}, {value})"));
            }
            sess.execute(&stmt).await.expect("basin hypertable seed");
            off = hi;
        }
        let s = start.elapsed().as_secs_f64();
        if s > 0.0 { Some(rows as f64 / s) } else { None }
    } else {
        None
    };

    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }
    eprintln!("[ext_bench_timescale] basin plain {basin_plain_rate:.0} rows/s, hypertable_ok={hyper_create_ok}");

    // time_bucket GROUP BY at 3 resolutions.
    let ts1_sql = "SELECT time_bucket('1 minute', ts) AS b, COUNT(*) AS n, SUM(value) AS s FROM metrics GROUP BY b ORDER BY b";
    let ts2_sql = "SELECT time_bucket('1 hour', ts) AS b, COUNT(*) AS n, SUM(value) AS s FROM metrics GROUP BY b ORDER BY b";
    let ts3_sql = "SELECT time_bucket('1 day', ts) AS b, COUNT(*) AS n, SUM(value) AS s FROM metrics GROUP BY b ORDER BY b";

    // TS4: first/last per device. Basin has no first()/last() aggregate, so we
    // use the PG-EQUIVALENT window form (works on both engines). The label says
    // "PG-equivalent" so nobody mistakes this for a native Timescale aggregate.
    let ts4_sql = "SELECT device, \
        (ARRAY_AGG(value ORDER BY ts ASC))[1] AS first_value, \
        (ARRAY_AGG(value ORDER BY ts DESC))[1] AS last_value \
        FROM metrics GROUP BY device ORDER BY device";

    let ts1_b = basin_p50(&sess, ts1_sql, samples).await;
    let ts2_b = basin_p50(&sess, ts2_sql, samples).await;
    let ts3_b = basin_p50(&sess, ts3_sql, samples).await;
    let ts4_b = basin_p50(&sess, ts4_sql, samples).await;
    let ts4_supported = ts4_b.is_some();

    // TS7: chunks-view query (Basin virtual view; PG timescaledb chunks).
    let ts7_sql = "SELECT count(*) FROM timescaledb_information.chunks";
    let ts7_b = basin_p50(&sess, ts7_sql, samples).await;
    let ts7_supported = ts7_b.is_some();

    // TS5: retention. drop_chunks is not registered → use DELETE WHERE ts<cutoff.
    // Measure ONCE (destructive — single-shot, no median): delete ~the first day.
    let cutoff = EPOCH + 86_400; // first day
    let ts5_b = {
        let t = Instant::now();
        match sess.execute(&format!("DELETE FROM metrics WHERE ts < to_timestamp({cutoff})")).await {
            Ok(_) => Some(t.elapsed().as_secs_f64() * 1000.0),
            Err(e) => {
                eprintln!("[ext_bench_timescale] TS5 DELETE unsupported: {e}");
                None
            }
        }
    };

    println!(
        "[ext_bench_timescale] Basin: TS1={:?} TS2={:?} TS3={:?} TS4(sup={ts4_supported})={:?} \
         TS5(delete)={:?} TS7(chunks,sup={ts7_supported})={:?}",
        ts1_b, ts2_b, ts3_b, ts4_b, ts5_b, ts7_b
    );

    instance.wal.close().await.unwrap();

    // ── PG twin ──────────────────────────────────────────────────────────────
    let mut pg_available = false;
    let mut pg_timescale_ext = false;
    let mut pg_plain_rate: Option<f64> = None;
    let mut pg_hyper_rate: Option<f64> = None;
    let (mut pg_ts1, mut pg_ts2, mut pg_ts3, mut pg_ts4, mut pg_ts5, mut pg_ts7) =
        (None, None, None, None, None, None);

    if let Some((pg, cs)) = try_connect().await {
        pg_available = true;
        let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
        let schema = format!("basin_ext_ts_{suffix}");
        let _guard = SchemaGuard { schema: schema.clone(), conn_str: cs };
        pg.simple_query(&format!("CREATE SCHEMA {schema}")).await.ok();
        pg.simple_query("SET work_mem = '64MB'").await.ok();
        // TimescaleDB may or may not be installed; the bucket/first/last
        // comparators run on plain PG either way (we use PG-equivalent SQL).
        pg_timescale_ext = pg.simple_query("CREATE EXTENSION IF NOT EXISTS timescaledb").await.is_ok();

        pg.simple_query(&format!(
            "CREATE TABLE {schema}.metrics (id BIGINT, ts TIMESTAMPTZ NOT NULL, device BIGINT NOT NULL, value DOUBLE PRECISION NOT NULL)"
        )).await.ok();

        let pgstart = Instant::now();
        let batch = 10_000usize;
        let mut po = 0usize;
        while po < rows {
            let hi = (po + batch).min(rows);
            let mut v = String::with_capacity((hi - po) * 64);
            for k in po..hi {
                if k > po {
                    v.push(',');
                }
                let ts = EPOCH + k as i64;
                let device = (k % 100) as i64;
                let value = (k % 1000) as f64 * 0.5;
                v.push_str(&format!("({k}, to_timestamp({ts}), {device}, {value})"));
            }
            pg.simple_query(&format!("INSERT INTO {schema}.metrics VALUES {v}")).await.ok();
            po = hi;
        }
        let pg_s = pgstart.elapsed().as_secs_f64();
        pg_plain_rate = if pg_s > 0.0 { Some(rows as f64 / pg_s) } else { None };
        pg.simple_query(&format!("ANALYZE {schema}.metrics")).await.ok();

        // Hypertable ingest (only when timescaledb installed) — fresh twin.
        if pg_timescale_ext {
            pg.simple_query(&format!(
                "CREATE TABLE {schema}.metrics_ht (id BIGINT, ts TIMESTAMPTZ NOT NULL, device BIGINT NOT NULL, value DOUBLE PRECISION NOT NULL)"
            )).await.ok();
            let _ = pg.simple_query(&format!(
                "SELECT create_hypertable('{schema}.metrics_ht', 'ts', chunk_time_interval => INTERVAL '1 day')"
            )).await;
            let hstart = Instant::now();
            let mut ho = 0usize;
            while ho < rows {
                let hi = (ho + batch).min(rows);
                let mut v = String::with_capacity((hi - ho) * 64);
                for k in ho..hi {
                    if k > ho {
                        v.push(',');
                    }
                    let ts = EPOCH + k as i64;
                    let device = (k % 100) as i64;
                    let value = (k % 1000) as f64 * 0.5;
                    v.push_str(&format!("({k}, to_timestamp({ts}), {device}, {value})"));
                }
                pg.simple_query(&format!("INSERT INTO {schema}.metrics_ht VALUES {v}")).await.ok();
                ho = hi;
            }
            let hs = hstart.elapsed().as_secs_f64();
            pg_hyper_rate = if hs > 0.0 { Some(rows as f64 / hs) } else { None };
        }

        // TS1/2/3: time_bucket works on PG only when timescaledb is installed;
        // otherwise fall back to date_trunc as the PG-equivalent bucket.
        let (b1, b2, b3) = if pg_timescale_ext {
            (
                "time_bucket('1 minute', ts)",
                "time_bucket('1 hour', ts)",
                "time_bucket('1 day', ts)",
            )
        } else {
            (
                "date_trunc('minute', ts)",
                "date_trunc('hour', ts)",
                "date_trunc('day', ts)",
            )
        };
        pg_ts1 = pg_p50(&pg, &format!(
            "SELECT {b1} AS b, COUNT(*) AS n, SUM(value) AS s FROM {schema}.metrics GROUP BY b ORDER BY b"), samples).await;
        pg_ts2 = pg_p50(&pg, &format!(
            "SELECT {b2} AS b, COUNT(*) AS n, SUM(value) AS s FROM {schema}.metrics GROUP BY b ORDER BY b"), samples).await;
        pg_ts3 = pg_p50(&pg, &format!(
            "SELECT {b3} AS b, COUNT(*) AS n, SUM(value) AS s FROM {schema}.metrics GROUP BY b ORDER BY b"), samples).await;

        // TS4: PG-equivalent first/last via DISTINCT ON (the idiomatic PG way).
        pg_ts4 = pg_p50(&pg, &format!(
            "SELECT device, \
             (ARRAY_AGG(value ORDER BY ts ASC))[1] AS first_value, \
             (ARRAY_AGG(value ORDER BY ts DESC))[1] AS last_value \
             FROM {schema}.metrics GROUP BY device ORDER BY device"), samples).await;

        // TS7: timescale chunks view (only when installed).
        if pg_timescale_ext {
            pg_ts7 = pg_p50(&pg, "SELECT count(*) FROM timescaledb_information.chunks", samples).await;
        }

        // TS5: DELETE WHERE ts<cutoff — single shot on a disposable copy.
        pg.simple_query(&format!("CREATE TABLE {schema}.metrics_del AS TABLE {schema}.metrics")).await.ok();
        {
            let t = Instant::now();
            let _ = pg.simple_query(&format!(
                "DELETE FROM {schema}.metrics_del WHERE ts < to_timestamp({cutoff})")).await;
            pg_ts5 = Some(t.elapsed().as_secs_f64() * 1000.0);
        }

        let _ = pg.simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE")).await;
        std::mem::forget(_guard);
    } else {
        eprintln!("[ext_bench_timescale] PG unavailable — Basin-only card");
    }

    let ts = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    write_artifact("ext_bench_timescale.json", &json!({
        "card": "ext_bench_timescale",
        "family": "timescaledb",
        "generated_at": format!("@{ts}"),
        "pg_available": pg_available,
        "pg_extension_available": pg_timescale_ext,
        "basin_hypertable_ddl_ok": hyper_create_ok,
        "config": { "rows": rows, "samples": samples },
        "ingest": [
            { "label": "TS6a: plain-table ingest (rows/s)", "basin_rows_per_s": basin_plain_rate, "pg_rows_per_s": pg_plain_rate },
            { "label": "TS6b: hypertable ingest (rows/s)", "basin_rows_per_s": basin_hyper_rate, "pg_rows_per_s": pg_hyper_rate },
        ],
        "shapes": [
            { "label": "TS1: time_bucket('1 minute') GROUP BY", "basin_p50_ms": opt_ms(ts1_b), "pg_p50_ms": opt_ms(pg_ts1),
              "basin_over_pg": ratio(ts1_b, pg_ts1), "pg_bucket": if pg_timescale_ext { "time_bucket" } else { "date_trunc (PG-equivalent)" } },
            { "label": "TS2: time_bucket('1 hour') GROUP BY", "basin_p50_ms": opt_ms(ts2_b), "pg_p50_ms": opt_ms(pg_ts2),
              "basin_over_pg": ratio(ts2_b, pg_ts2) },
            { "label": "TS3: time_bucket('1 day') GROUP BY", "basin_p50_ms": opt_ms(ts3_b), "pg_p50_ms": opt_ms(pg_ts3),
              "basin_over_pg": ratio(ts3_b, pg_ts3) },
            { "label": "TS4: first/last per device (PG-equivalent ARRAY_AGG, NOT Timescale first()/last())",
              "basin_supported": ts4_supported, "basin_p50_ms": opt_ms(ts4_b), "pg_p50_ms": opt_ms(pg_ts4),
              "basin_over_pg": ratio(ts4_b, pg_ts4) },
            { "label": "TS5: retention — Basin DELETE WHERE ts<cutoff vs PG DELETE WHERE ts<cutoff (drop_chunks not registered)",
              "basin_ms": opt_ms(ts5_b), "pg_ms": opt_ms(pg_ts5), "single_shot": true },
            { "label": "TS7: chunks-view query overhead", "basin_supported": ts7_supported,
              "basin_p50_ms": opt_ms(ts7_b), "pg_p50_ms": opt_ms(pg_ts7) },
        ],
        "note": "time_bucket GROUP BY is native on Basin. first()/last() aggregates and \
                 drop_chunks are NOT registered on Basin: TS4 uses a PG-equivalent \
                 ARRAY_AGG window on BOTH engines (label says so), and TS5 measures \
                 DELETE WHERE ts<cutoff (single-shot, destructive) as the retention \
                 equivalent. When TimescaleDB is installed PG uses time_bucket and the \
                 chunks view; otherwise PG falls back to date_trunc (PG-equivalent, \
                 labelled). Hypertable ingest is compared on both engines.",
    }));
}
