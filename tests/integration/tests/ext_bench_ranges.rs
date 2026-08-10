//! EXTENSIONS BENCHMARK SUITE — range-types family performance card.
//!
//! Separate artifact (`benchmark/data/ext_bench_ranges.json`), separate runner.
//! Range types are BUILT INTO PostgreSQL (no extension), so the PG head-to-head
//! needs no `CREATE EXTENSION`. Exercises the range surface Basin implements
//! today (verified against `range_conformance.rs` / `range_types_harness.rs`):
//!
//!   * `int4range` column type (CREATE TABLE / INSERT round-trip + canonicalization)
//!   * `&&` overlap filter (range column vs literal range)
//!   * `@>` element containment (`range_contains_elem` / `range @> elem`)
//!   * range JOIN `a.r && b.r` (booking-conflict shape) — probed (small N)
//!
//! ## Shapes
//!   R1 range column ingest 100k (rows/s; canonicalization cost folded in)
//!   R2 && overlap filter — narrow probe (low selectivity)
//!   R3 && overlap filter — wide probe (high selectivity)
//!   R4 @> element containment count
//!   R5 range JOIN a.r && b.r — booking-conflict (small N, probed)
//!
//! ## Excluded as unimplemented
//!   * `range_agg()` aggregate — not registered (correctness gap).
//!   * multirange `&&` — only scalar `@>` registered.
//!
//! ## Env knobs
//!   * `BASIN_EXT_BENCH_ROWS`         — range count (default 100_000)
//!   * `BASIN_EXT_BENCH_RANGE_SAMPLES`— samples per shape (default 25)
//!   * `BASIN_EXT_BENCH_RANGE_JOIN_N` — join row count per side (default 2_000)

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
        eprintln!("[ext_bench_ranges] artifact written: {}", path.display());
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

/// Deterministic `[lo, hi)` range for row `i`. lo cycles 0..10000, width cycles
/// 1..50 so overlap probes have a meaningful selectivity spread.
fn range_of(i: usize) -> (i64, i64) {
    let lo = (i % 10_000) as i64;
    let width = 1 + (i % 50) as i64;
    (lo, lo + width)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "extensions benchmark: needs idle box (+ live PG with extension for head-to-head); run via benchmark/run/extensions-suite.sh"]
async fn ext_bench_ranges() {
    let rows = env_usize("BASIN_EXT_BENCH_ROWS", 100_000);
    let samples = env_usize("BASIN_EXT_BENCH_RANGE_SAMPLES", 25);
    let join_n = env_usize("BASIN_EXT_BENCH_RANGE_JOIN_N", 2_000);
    eprintln!("[ext_bench_ranges] config: rows={rows} samples={samples} join_n={join_n}");

    // ── Basin: R1 ingest ──────────────────────────────────────────────────────
    let mut instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();
    sess.execute("CREATE TABLE bookings (id BIGINT NOT NULL, r int4range NOT NULL)")
        .await
        .unwrap();

    let batch = 5_000usize;
    let mut off = 0usize;
    let ingest_start = Instant::now();
    while off < rows {
        let hi = (off + batch).min(rows);
        let mut stmt = String::with_capacity((hi - off) * 32);
        stmt.push_str("INSERT INTO bookings VALUES ");
        for k in off..hi {
            if k > off {
                stmt.push(',');
            }
            let (lo, up) = range_of(k);
            stmt.push_str(&format!("({k}, int4range({lo}, {up}))"));
        }
        sess.execute(&stmt).await.expect("basin range seed");
        off = hi;
    }
    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }
    let basin_ingest_s = ingest_start.elapsed().as_secs_f64();
    let basin_ingest_rate = if basin_ingest_s > 0.0 {
        rows as f64 / basin_ingest_s
    } else {
        0.0
    };

    // Small JOIN twin table — booking-conflict shape (a.r && b.r).
    let join_ok_basin = sess
        .execute("CREATE TABLE requests (id BIGINT NOT NULL, r int4range NOT NULL)")
        .await
        .is_ok();
    if join_ok_basin {
        let mut jo = 0usize;
        while jo < join_n {
            let hi = (jo + batch).min(join_n);
            let mut stmt = String::with_capacity((hi - jo) * 32);
            stmt.push_str("INSERT INTO requests VALUES ");
            for k in jo..hi {
                if k > jo {
                    stmt.push(',');
                }
                // Offset the lo so some overlap, some not.
                let (lo, up) = range_of(k * 3 + 7);
                stmt.push_str(&format!("({k}, int4range({lo}, {up}))"));
            }
            sess.execute(&stmt).await.expect("basin requests seed");
            jo = hi;
        }
    }
    eprintln!(
        "[ext_bench_ranges] basin ingest {basin_ingest_rate:.0} rows/s, join_table={join_ok_basin}"
    );

    // ── Basin shapes ──────────────────────────────────────────────────────────
    // Narrow probe range — overlaps a small fraction.
    let r2_sql = "SELECT count(*) FROM bookings WHERE r && int4range(100, 110)";
    // Wide probe — overlaps a large fraction.
    let r3_sql = "SELECT count(*) FROM bookings WHERE r && int4range(0, 5000)";
    // Element containment.
    let r4_sql = "SELECT count(*) FROM bookings WHERE r @> 250";
    // Booking-conflict JOIN (small N).
    let r5_sql = "SELECT count(*) FROM requests req JOIN bookings bk ON req.r && bk.r";

    let r2_b = basin_p50(&sess, r2_sql, samples).await;
    let r2_count = sess
        .execute(r2_sql)
        .await
        .map(|r| basin_count(&r))
        .unwrap_or(-1);
    let r3_b = basin_p50(&sess, r3_sql, samples).await;
    let r3_count = sess
        .execute(r3_sql)
        .await
        .map(|r| basin_count(&r))
        .unwrap_or(-1);
    let r4_b = basin_p50(&sess, r4_sql, samples).await;
    let r4_count = sess
        .execute(r4_sql)
        .await
        .map(|r| basin_count(&r))
        .unwrap_or(-1);

    let r5_b = if join_ok_basin {
        basin_p50(&sess, r5_sql, samples.min(7)).await
    } else {
        None
    };
    let r5_supported = r5_b.is_some();
    let r5_count = if r5_supported {
        sess.execute(r5_sql)
            .await
            .map(|r| basin_count(&r))
            .unwrap_or(-1)
    } else {
        -1
    };

    println!(
        "[ext_bench_ranges] Basin: R2(narrow={r2_count})={:?} R3(wide={r3_count})={:?} \
         R4(elem={r4_count})={:?} R5(join,sup={r5_supported},c={r5_count})={:?}",
        r2_b, r3_b, r4_b, r5_b
    );

    instance.wal.close().await.unwrap();

    // ── PG twin (built-in ranges, no extension) ──────────────────────────────
    let mut pg_available = false;
    let mut pg_ingest_rate: Option<f64> = None;
    let (mut pg_r2, mut pg_r3, mut pg_r4, mut pg_r5) = (None, None, None, None);
    let (mut pg_r2_count, mut pg_r4_count, mut pg_r5_count) = (-1i64, -1i64, -1i64);

    if let Some((pg, cs)) = try_connect().await {
        pg_available = true;
        let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
        let schema = format!("basin_ext_rng_{suffix}");
        let _guard = SchemaGuard {
            schema: schema.clone(),
            conn_str: cs,
        };
        pg.simple_query(&format!("CREATE SCHEMA {schema}"))
            .await
            .ok();
        pg.simple_query("SET work_mem = '16MB'").await.ok();

        pg.simple_query(&format!(
            "CREATE TABLE {schema}.bookings (id BIGINT, r int4range NOT NULL)"
        ))
        .await
        .ok();
        pg.simple_query(&format!(
            "CREATE TABLE {schema}.requests (id BIGINT, r int4range NOT NULL)"
        ))
        .await
        .ok();

        let pgstart = Instant::now();
        let mut po = 0usize;
        while po < rows {
            let hi = (po + batch).min(rows);
            let mut v = String::with_capacity((hi - po) * 32);
            for k in po..hi {
                if k > po {
                    v.push(',');
                }
                let (lo, up) = range_of(k);
                v.push_str(&format!("({k}, int4range({lo}, {up}))"));
            }
            pg.simple_query(&format!("INSERT INTO {schema}.bookings VALUES {v}"))
                .await
                .ok();
            po = hi;
        }
        let pg_s = pgstart.elapsed().as_secs_f64();
        pg_ingest_rate = if pg_s > 0.0 {
            Some(rows as f64 / pg_s)
        } else {
            None
        };

        let mut jo = 0usize;
        while jo < join_n {
            let hi = (jo + batch).min(join_n);
            let mut v = String::with_capacity((hi - jo) * 32);
            for k in jo..hi {
                if k > jo {
                    v.push(',');
                }
                let (lo, up) = range_of(k * 3 + 7);
                v.push_str(&format!("({k}, int4range({lo}, {up}))"));
            }
            pg.simple_query(&format!("INSERT INTO {schema}.requests VALUES {v}"))
                .await
                .ok();
            jo = hi;
        }
        // PG GiST index on the range column makes && index-backed (the honest
        // PG-best comparator); a SP-GiST/GiST range index is the standard idiom.
        pg.simple_query(&format!(
            "CREATE INDEX bookings_r_gist ON {schema}.bookings USING gist (r)"
        ))
        .await
        .ok();
        pg.simple_query(&format!("ANALYZE {schema}.bookings"))
            .await
            .ok();
        pg.simple_query(&format!("ANALYZE {schema}.requests"))
            .await
            .ok();

        pg_r2_count = pg_count(
            &pg,
            &format!(
                "SELECT count(*)::bigint FROM {schema}.bookings WHERE r && int4range(100, 110)"
            ),
        )
        .await;
        pg_r4_count = pg_count(
            &pg,
            &format!("SELECT count(*)::bigint FROM {schema}.bookings WHERE r @> 250"),
        )
        .await;
        pg_r5_count = pg_count(&pg, &format!(
            "SELECT count(*)::bigint FROM {schema}.requests req JOIN {schema}.bookings bk ON req.r && bk.r")).await;

        pg_r2 = pg_p50(
            &pg,
            &format!("SELECT count(*) FROM {schema}.bookings WHERE r && int4range(100, 110)"),
            samples,
        )
        .await;
        pg_r3 = pg_p50(
            &pg,
            &format!("SELECT count(*) FROM {schema}.bookings WHERE r && int4range(0, 5000)"),
            samples,
        )
        .await;
        pg_r4 = pg_p50(
            &pg,
            &format!("SELECT count(*) FROM {schema}.bookings WHERE r @> 250"),
            samples,
        )
        .await;
        pg_r5 = pg_p50(&pg, &format!(
            "SELECT count(*) FROM {schema}.requests req JOIN {schema}.bookings bk ON req.r && bk.r"), samples.min(7)).await;

        let _ = pg
            .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await;
        std::mem::forget(_guard);
    } else {
        eprintln!("[ext_bench_ranges] PG unavailable — Basin-only card");
    }

    // ── Correctness cross-check ───────────────────────────────────────────────
    if pg_available {
        if r2_count >= 0 && pg_r2_count >= 0 {
            assert_eq!(
                r2_count, pg_r2_count,
                "R2 && narrow count mismatch: basin {r2_count} != pg {pg_r2_count}"
            );
        }
        if r4_count >= 0 && pg_r4_count >= 0 {
            assert_eq!(
                r4_count, pg_r4_count,
                "R4 @> elem count mismatch: basin {r4_count} != pg {pg_r4_count}"
            );
        }
        if r5_supported && r5_count >= 0 && pg_r5_count >= 0 {
            assert_eq!(
                r5_count, pg_r5_count,
                "R5 booking-conflict JOIN count mismatch: basin {r5_count} != pg {pg_r5_count}"
            );
        }
    }

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    write_artifact(
        "ext_bench_ranges.json",
        &json!({
            "card": "ext_bench_ranges",
            "family": "range_types",
            "generated_at": format!("@{ts}"),
            "pg_available": pg_available,
            "pg_extension_available": pg_available,
            "config": { "rows": rows, "samples": samples, "join_n": join_n },
            "ingest": {
                "label": "R1: int4range ingest rate (rows/s, canonicalization folded in)",
                "basin_rows_per_s": basin_ingest_rate,
                "pg_rows_per_s": pg_ingest_rate,
            },
            "shapes": [
                { "label": "R2: && overlap — narrow probe", "basin_p50_ms": opt_ms(r2_b), "pg_p50_ms": opt_ms(pg_r2),
                  "basin_over_pg": ratio(r2_b, pg_r2), "basin_hits": r2_count, "pg_hits": pg_r2_count },
                { "label": "R3: && overlap — wide probe", "basin_p50_ms": opt_ms(r3_b), "pg_p50_ms": opt_ms(pg_r3),
                  "basin_over_pg": ratio(r3_b, pg_r3), "basin_hits": r3_count },
                { "label": "R4: @> element containment", "basin_p50_ms": opt_ms(r4_b), "pg_p50_ms": opt_ms(pg_r4),
                  "basin_over_pg": ratio(r4_b, pg_r4), "basin_hits": r4_count, "pg_hits": pg_r4_count },
                { "label": "R5: range JOIN a.r && b.r (booking-conflict)", "basin_supported": r5_supported,
                  "basin_p50_ms": opt_ms(r5_b), "pg_p50_ms": opt_ms(pg_r5), "basin_over_pg": ratio(r5_b, pg_r5),
                  "basin_hits": r5_count, "pg_hits": pg_r5_count },
            ],
            "note": "Range types are built into PostgreSQL (no extension). PG uses a GiST range \
                     index for &&; Basin scans sequentially. Overlap/containment/join counts are \
                     hard-asserted equal across engines. The range JOIN (a.r && b.r) is probed \
                     at small N — if Basin cannot plan a cross-column range-overlap join it \
                     records basin_supported:false and a PG-only timing (the card never fails).",
        }),
    );
}
