//! EXTENSIONS BENCHMARK SUITE — PostGIS EXTENDED shapes (coverage expansion).
//!
//! Separate file (the main `ext_bench_postgis.rs` card is being extended by the
//! geometry-types feature work — this NEW file adds a non-conflicting coverage
//! gap without touching that body). Separate artifact
//! (`benchmark/data/ext_bench_postgis_ext.json`); the runner routes it to a
//! size-suffixed name (`ext_bench_postgis_ext_100k.json`).
//!
//! Closes the audit's PostGIS P1 gap that does NOT depend on the landing
//! geometry types (it uses the POINT type the base card already exercises):
//!
//!   GE1 ST_Distance ORDERED top-N — "sort by distance" ranked list
//!       (`SELECT id, ST_Distance(geom, q) d ... ORDER BY d LIMIT 10`). The
//!       base card has a `<->` KNN probe and a DWithin COUNT, but not the
//!       distance-PROJECTION + ordering shape every "nearest stores" UI runs.
//!
//! DEFERRED to a post-merge expansion (depend on the landing geometry types):
//!   * spatial JOIN (ST_Intersects/ST_Contains between two tables)
//!   * polygon-containment AT SCALE (many real polygons)
//!   * KNN-by-distance with a real index path
//!
//! PG head-to-head is opportunistic (`pg_available:false` + Basin-only timings
//! when postgis is unavailable); the card NEVER fails on a missing extension.
//!
//! ## Env knobs (shared with the base card via the runner)
//!   * `BASIN_EXT_BENCH_ROWS`        — point count (default 100_000)
//!   * `BASIN_EXT_BENCH_GEO_SAMPLES` — samples per shape (default 15)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use basin_common::ProjectId;
use basin_engine::ExecResult;
use serde_json::json;
use tokio_postgres::SimpleQueryMessage;

#[path = "compare_postgres_common.rs"]
mod common;

use common::{build_basin_engine, median, try_connect, SchemaGuard};

const TILES_PER_AXIS: usize = 32;
const QUERY_X: f64 = 5.0;
const QUERY_Y: f64 = 5.0;

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
            "[ext_bench_postgis_ext] artifact written: {}",
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

fn rows_of(res: &ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

struct Lcg(u64);
impl Lcg {
    fn next_unit(&mut self) -> f64 {
        self.0 = self.0.wrapping_mul(1_103_515_245).wrapping_add(12_345) & 0x7fff_ffff;
        (self.0 as f64) / (1u64 << 31) as f64
    }
}

/// Same tiled point distribution as ext_bench_postgis.rs (shared seed).
fn build_points(n: usize) -> Vec<(i64, f64, f64)> {
    let mut out = Vec::with_capacity(n);
    let mut lcg = Lcg(0xCAFEBABE);
    let tile_w = 10.0 / TILES_PER_AXIS as f64;
    let rows_per_tile = n / (TILES_PER_AXIS * TILES_PER_AXIS);
    let mut emitted = 0usize;
    let mut id: i64 = 0;
    for ti in 0..TILES_PER_AXIS {
        for tj in 0..TILES_PER_AXIS {
            let take = if ti == TILES_PER_AXIS - 1 && tj == TILES_PER_AXIS - 1 {
                n - emitted
            } else {
                rows_per_tile
            };
            let x0 = ti as f64 * tile_w;
            let y0 = tj as f64 * tile_w;
            for _ in 0..take {
                let x = x0 + lcg.next_unit() * tile_w;
                let y = y0 + lcg.next_unit() * tile_w;
                out.push((id, x, y));
                id += 1;
                emitted += 1;
            }
        }
    }
    out
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "extensions benchmark: needs idle box (+ live PG with postgis for head-to-head); run via benchmark/run/extensions-suite.sh"]
async fn ext_bench_postgis_ext() {
    let rows = env_usize("BASIN_EXT_BENCH_ROWS", 100_000);
    let samples = env_usize("BASIN_EXT_BENCH_GEO_SAMPLES", 15);
    eprintln!("[ext_bench_postgis_ext] config: rows={rows} samples={samples}");

    let points = build_points(rows);

    let mut instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();
    sess.execute("CREATE TABLE pts (id BIGINT, geom POINT) WITH (basin.file_format='parquet')")
        .await
        .unwrap();
    let batch = 5_000usize;
    let mut i = 0usize;
    while i < points.len() {
        let end = (i + batch).min(points.len());
        let mut vals = String::with_capacity((end - i) * 40);
        for (k, (id, x, y)) in points[i..end].iter().enumerate() {
            if k > 0 {
                vals.push(',');
            }
            vals.push_str(&format!("({id}, ST_MakePoint({x},{y}))"));
        }
        sess.execute(&format!("INSERT INTO pts (id, geom) VALUES {vals}"))
            .await
            .expect("basin geo seed");
        i = end;
    }
    instance.shard.flush_to_parquet().await.unwrap();
    let _ = sess
        .execute("CREATE INDEX idx_pts_geom ON pts USING gist(geom)")
        .await;
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }

    // GE1: distance-projection ordered top-N ("nearest 10, sorted by distance").
    let ge1_sql = format!(
        "SELECT id, ST_Distance(geom, ST_MakePoint({QUERY_X},{QUERY_Y})) AS d \
         FROM pts ORDER BY d LIMIT 10"
    );
    let ge1_b = basin_p50(&sess, &ge1_sql, samples).await;
    let ge1_sup = sess
        .execute(&ge1_sql)
        .await
        .map(|r| rows_of(&r))
        .unwrap_or(0)
        > 0;

    println!("[ext_bench_postgis_ext] Basin: GE1(dist-order,sup={ge1_sup})={ge1_b:?}");

    instance.wal.close().await.unwrap();

    // ── PG twin (opportunistic) ───────────────────────────────────────────────
    let mut pg_available = false;
    let mut pg_ge1 = None;

    if let Some((pg, cs)) = try_connect().await {
        let ext_ok = pg
            .simple_query("CREATE EXTENSION IF NOT EXISTS postgis")
            .await
            .is_ok();
        if ext_ok {
            pg_available = true;
            let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
            let schema = format!("basin_ext_geox_{suffix}");
            let _guard = SchemaGuard {
                schema: schema.clone(),
                conn_str: cs,
            };
            pg.simple_query(&format!("CREATE SCHEMA {schema}"))
                .await
                .ok();
            pg.simple_query(&format!(
                "CREATE TABLE {schema}.pts (id BIGINT, geom geometry(Point))"
            ))
            .await
            .ok();
            let mut p = 0usize;
            while p < points.len() {
                let end = (p + batch).min(points.len());
                let mut v = String::with_capacity((end - p) * 40);
                for (k, (id, x, y)) in points[p..end].iter().enumerate() {
                    if k > 0 {
                        v.push(',');
                    }
                    v.push_str(&format!("({id}, ST_MakePoint({x},{y}))"));
                }
                pg.simple_query(&format!("INSERT INTO {schema}.pts (id, geom) VALUES {v}"))
                    .await
                    .ok();
                p = end;
            }
            pg.simple_query(&format!(
                "CREATE INDEX pts_geom_gist ON {schema}.pts USING gist(geom)"
            ))
            .await
            .ok();
            pg.simple_query(&format!("ANALYZE {schema}.pts")).await.ok();

            pg_ge1 = pg_p50(
                &pg,
                &format!(
                    "SELECT id, ST_Distance(geom, ST_MakePoint({QUERY_X},{QUERY_Y})) AS d \
                 FROM {schema}.pts ORDER BY d LIMIT 10"
                ),
                samples,
            )
            .await;

            let _ = pg
                .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                .await;
            std::mem::forget(_guard);
        } else {
            eprintln!("[ext_bench_postgis_ext] PG lacks postgis — Basin-only card");
        }
    } else {
        eprintln!("[ext_bench_postgis_ext] PG unavailable — Basin-only card");
    }

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    write_artifact(
        "ext_bench_postgis_ext.json",
        &json!({
            "card": "ext_bench_postgis_ext",
            "family": "postgis",
            "generated_at": format!("@{ts}"),
            "pg_available": pg_available,
            "pg_extension_available": pg_available,
            "config": { "rows": rows, "samples": samples },
            "shapes": [
                { "label": "GE1: ST_Distance ORDER BY LIMIT 10 (ranked nearest, distance projection)",
                  "basin_supported": ge1_sup, "basin_p50_ms": opt_ms(ge1_b), "pg_p50_ms": opt_ms(pg_ge1),
                  "basin_over_pg": ratio(ge1_b, pg_ge1) },
            ],
            "note": "Coverage-expansion card for PostGIS: the ranked 'sort by distance' top-N \
                     (ST_Distance projection + ORDER BY), the shape every nearest-N UI runs — \
                     distinct from the base card's <-> KNN probe and DWithin COUNT. Spatial \
                     JOIN, polygon-containment-at-scale and the KNN index path are deferred to \
                     a post-merge expansion once the geometry-types feature lands.",
        }),
    );
}
