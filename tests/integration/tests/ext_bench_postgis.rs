//! EXTENSIONS BENCHMARK SUITE — PostGIS family performance card.
//!
//! Separate artifact (`benchmark/data/ext_bench_postgis.json`), separate
//! runner. Exercises the spatial surface Basin implements today (verified
//! against `compare_postgis.rs` / `spatial_knn.rs`):
//!
//!   * `POINT` column type + `ST_MakePoint(x,y)` ingest
//!   * `ST_DWithin(geom, q, r)` radius (Haversine metres on Basin)
//!   * `geom && ST_MakeEnvelope(...)` bbox (R-tree row-group prune + residual)
//!   * `ORDER BY geom <-> ST_MakePoint(x,y) LIMIT k` nearest-neighbour
//!   * `USING gist(geom)` index (drives the R-tree sidecar at flush)
//!   * point-in-polygon via `ST_Contains(polygon, geom)` — probed at runtime
//!
//! ## Shapes
//!   G1 point ingest with geo column 100k (rows/s)
//!   G2 bbox && — selective box   (low selectivity)
//!   G3 bbox && — wide box        (high selectivity)
//!   G4 radius ST_DWithin count
//!   G5 nearest-neighbour <-> ORDER BY LIMIT 10 (if implemented; probed)
//!   G6 point-in-polygon over a realistic ~50-vertex polygon (probed)
//!
//! ## Excluded as unimplemented (probed, recorded if absent)
//!   * Any shape whose Basin SQL errors at runtime → `basin_supported:false`,
//!     PG-only timing recorded, card never fails.
//!
//! ## PG head-to-head
//! `CREATE EXTENSION IF NOT EXISTS postgis` inside a try; PG uses GIST. The
//! FAIRNESS INVARIANT from compare_postgis.rs is preserved: PG `ST_DWithin`
//! operands are cast to `::geography` so both engines measure metres.
//!
//! ## Env knobs
//!   * `BASIN_EXT_BENCH_ROWS`        — point count (default 100_000)
//!   * `BASIN_EXT_BENCH_GEO_SAMPLES` — samples per shape (default 15)

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
        eprintln!("[ext_bench_postgis] artifact written: {}", path.display());
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

const TILES_PER_AXIS: usize = 32;
const QUERY_X: f64 = 5.0;
const QUERY_Y: f64 = 5.0;
const RADIUS_M: f64 = 55_000.0;
// Selective bbox: 1°×1° centred on the origin.
const BBOX_SEL: (f64, f64, f64, f64) = (4.5, 4.5, 5.5, 5.5);
// Wide bbox: 6°×6°.
const BBOX_WIDE: (f64, f64, f64, f64) = (2.0, 2.0, 8.0, 8.0);

struct Lcg(u64);
impl Lcg {
    fn next_unit(&mut self) -> f64 {
        self.0 = self.0.wrapping_mul(1_103_515_245).wrapping_add(12_345) & 0x7fff_ffff;
        (self.0 as f64) / (1u64 << 31) as f64
    }
}

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
    debug_assert_eq!(emitted, n);
    out
}

/// ~50-vertex regular polygon (a 50-gon) approximating a circle of radius 2°
/// centred on the query origin. Returns a WKT POLYGON ring string (first vertex
/// repeated to close the ring) usable in `ST_GeomFromText('POLYGON((...))')`.
fn polygon_wkt() -> String {
    let n = 50;
    let r = 2.0;
    let mut pts = Vec::with_capacity(n + 1);
    for i in 0..n {
        let theta = (i as f64) * std::f64::consts::TAU / (n as f64);
        let x = QUERY_X + r * theta.cos();
        let y = QUERY_Y + r * theta.sin();
        pts.push(format!("{x:.6} {y:.6}"));
    }
    pts.push(pts[0].clone()); // close ring
    format!("POLYGON(({}))", pts.join(","))
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

async fn pg_count(pg: &tokio_postgres::Client, sql: &str) -> i64 {
    pg.query_one(sql, &[]).await.map(|r| r.get::<usize, i64>(0)).unwrap_or(-1)
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
async fn ext_bench_postgis() {
    let rows = env_usize("BASIN_EXT_BENCH_ROWS", 100_000);
    let samples = env_usize("BASIN_EXT_BENCH_GEO_SAMPLES", 15);
    eprintln!("[ext_bench_postgis] config: rows={rows} samples={samples}");

    let points = build_points(rows);
    let poly = polygon_wkt();

    // ── Basin: G1 ingest ──────────────────────────────────────────────────────
    let mut instance = build_basin_engine().await;
    let sess = instance.engine.open_session(instance.project).await.unwrap();
    sess.execute("CREATE TABLE pts (id BIGINT, geom POINT) WITH (basin.file_format='parquet')")
        .await
        .unwrap();

    let batch = 5_000usize;
    let mut i = 0usize;
    let ingest_start = Instant::now();
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
    let basin_ingest_s = ingest_start.elapsed().as_secs_f64();
    let basin_ingest_rate = if basin_ingest_s > 0.0 { rows as f64 / basin_ingest_s } else { 0.0 };

    let gist_ok = sess
        .execute("CREATE INDEX idx_pts_geom ON pts USING gist(geom)")
        .await
        .is_ok();
    instance.shard.flush_to_parquet().await.expect("flush (builds R-tree sidecar)");
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }
    eprintln!("[ext_bench_postgis] basin ingest {basin_ingest_rate:.0} rows/s, gist_ok={gist_ok}");

    // ── Basin shapes ──────────────────────────────────────────────────────────
    let g2_sql = format!(
        "SELECT count(*) FROM pts WHERE geom && ST_MakeEnvelope({},{},{},{},4326)",
        BBOX_SEL.0, BBOX_SEL.1, BBOX_SEL.2, BBOX_SEL.3
    );
    let g3_sql = format!(
        "SELECT count(*) FROM pts WHERE geom && ST_MakeEnvelope({},{},{},{},4326)",
        BBOX_WIDE.0, BBOX_WIDE.1, BBOX_WIDE.2, BBOX_WIDE.3
    );
    let g4_sql = format!(
        "SELECT count(*) FROM pts WHERE ST_DWithin(geom, ST_MakePoint({QUERY_X},{QUERY_Y}), {RADIUS_M})"
    );
    let g5_sql = format!(
        "SELECT id FROM pts ORDER BY geom <-> ST_MakePoint({QUERY_X},{QUERY_Y}) LIMIT 10"
    );
    let g6_sql = format!(
        "SELECT count(*) FROM pts WHERE ST_Contains(ST_GeomFromText('{poly}'), geom)"
    );

    let g2_b = basin_p50(&sess, &g2_sql, samples).await;
    let g2_count = sess.execute(&g2_sql).await.map(|r| basin_count(&r)).unwrap_or(-1);
    let g3_b = basin_p50(&sess, &g3_sql, samples).await;
    let g3_count = sess.execute(&g3_sql).await.map(|r| basin_count(&r)).unwrap_or(-1);
    let g4_b = basin_p50(&sess, &g4_sql, samples).await;
    let g4_count = sess.execute(&g4_sql).await.map(|r| basin_count(&r)).unwrap_or(-1);
    let g5_b = basin_p50(&sess, &g5_sql, samples).await;
    let g5_supported = g5_b.is_some();
    let g6_b = basin_p50(&sess, &g6_sql, samples).await;
    let g6_supported = g6_b.is_some();
    let g6_count = if g6_supported {
        sess.execute(&g6_sql).await.map(|r| basin_count(&r)).unwrap_or(-1)
    } else {
        -1
    };

    println!(
        "[ext_bench_postgis] Basin: G2(sel={g2_count})={:?} G3(wide={g3_count})={:?} \
         G4(radius={g4_count})={:?} G5(nn,sup={g5_supported})={:?} G6(pip,sup={g6_supported},c={g6_count})={:?}",
        g2_b, g3_b, g4_b, g5_b, g6_b
    );

    instance.wal.close().await.unwrap();

    // ── PG twin (postgis + GIST) ──────────────────────────────────────────────
    let mut pg_available = false;
    let mut pg_ext_ok = false;
    let mut pg_ingest_rate: Option<f64> = None;
    let (mut pg_g2, mut pg_g3, mut pg_g4, mut pg_g5, mut pg_g6) = (None, None, None, None, None);
    let (mut pg_g2_count, mut pg_g4_count, mut pg_g6_count) = (-1i64, -1i64, -1i64);

    if let Some((pg, cs)) = try_connect().await {
        pg_available = true;
        let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
        let schema = format!("basin_ext_geo_{suffix}");
        let _guard = SchemaGuard { schema: schema.clone(), conn_str: cs };

        pg_ext_ok = pg.simple_query("CREATE EXTENSION IF NOT EXISTS postgis").await.is_ok();
        eprintln!("[ext_bench_postgis] PG postgis: {}", if pg_ext_ok { "ok" } else { "UNAVAILABLE" });

        if pg_ext_ok {
            pg.simple_query(&format!("CREATE SCHEMA {schema}")).await.ok();
            pg.simple_query(&format!(
                "CREATE TABLE {schema}.pts (id BIGINT, geom geometry(Point,4326))"
            )).await.ok();

            let pgstart = Instant::now();
            let mut j = 0usize;
            while j < points.len() {
                let end = (j + batch).min(points.len());
                let mut vals = String::with_capacity((end - j) * 48);
                for (kk, (id, x, y)) in points[j..end].iter().enumerate() {
                    if kk > 0 {
                        vals.push(',');
                    }
                    vals.push_str(&format!("({id}, ST_SetSRID(ST_MakePoint({x},{y}),4326))"));
                }
                pg.simple_query(&format!("INSERT INTO {schema}.pts (id, geom) VALUES {vals}")).await.ok();
                j = end;
            }
            let pg_s = pgstart.elapsed().as_secs_f64();
            pg_ingest_rate = if pg_s > 0.0 { Some(rows as f64 / pg_s) } else { None };
            pg.simple_query(&format!("CREATE INDEX ON {schema}.pts USING GIST(geom)")).await.ok();
            pg.simple_query(&format!("ANALYZE {schema}.pts")).await.ok();

            // G2 selective bbox
            pg_g2_count = pg_count(&pg, &format!(
                "SELECT count(*)::bigint FROM {schema}.pts WHERE geom && ST_MakeEnvelope({},{},{},{},4326)",
                BBOX_SEL.0, BBOX_SEL.1, BBOX_SEL.2, BBOX_SEL.3)).await;
            pg_g2 = pg_p50(&pg, &format!(
                "SELECT count(*) FROM {schema}.pts WHERE geom && ST_MakeEnvelope({},{},{},{},4326)",
                BBOX_SEL.0, BBOX_SEL.1, BBOX_SEL.2, BBOX_SEL.3), samples).await;
            // G3 wide bbox
            pg_g3 = pg_p50(&pg, &format!(
                "SELECT count(*) FROM {schema}.pts WHERE geom && ST_MakeEnvelope({},{},{},{},4326)",
                BBOX_WIDE.0, BBOX_WIDE.1, BBOX_WIDE.2, BBOX_WIDE.3), samples).await;
            // G4 radius — FAIRNESS: ::geography cast so PG measures metres.
            pg_g4_count = pg_count(&pg, &format!(
                "SELECT count(*)::bigint FROM {schema}.pts WHERE ST_DWithin(geom::geography, \
                 ST_SetSRID(ST_MakePoint({QUERY_X},{QUERY_Y}),4326)::geography, {RADIUS_M})")).await;
            pg_g4 = pg_p50(&pg, &format!(
                "SELECT count(*) FROM {schema}.pts WHERE ST_DWithin(geom::geography, \
                 ST_SetSRID(ST_MakePoint({QUERY_X},{QUERY_Y}),4326)::geography, {RADIUS_M})"), samples).await;
            // G5 nearest-neighbour
            pg_g5 = pg_p50(&pg, &format!(
                "SELECT id FROM {schema}.pts ORDER BY geom <-> ST_SetSRID(ST_MakePoint({QUERY_X},{QUERY_Y}),4326) LIMIT 10"),
                samples).await;
            // G6 point-in-polygon
            pg_g6_count = pg_count(&pg, &format!(
                "SELECT count(*)::bigint FROM {schema}.pts WHERE ST_Contains(ST_SetSRID(ST_GeomFromText('{poly}'),4326), geom)")).await;
            pg_g6 = pg_p50(&pg, &format!(
                "SELECT count(*) FROM {schema}.pts WHERE ST_Contains(ST_SetSRID(ST_GeomFromText('{poly}'),4326), geom)"),
                samples).await;

            let _ = pg.simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE")).await;
        }
        std::mem::forget(_guard);
    } else {
        eprintln!("[ext_bench_postgis] PG unavailable — Basin-only card");
    }

    // ── Correctness cross-check (counts must match when both ran) ────────────
    if pg_ext_ok {
        if g2_count >= 0 && pg_g2_count >= 0 {
            assert_eq!(g2_count, pg_g2_count, "G2 bbox count mismatch: basin {g2_count} != pg {pg_g2_count}");
        }
        if g4_count >= 0 && pg_g4_count >= 0 {
            assert_eq!(g4_count, pg_g4_count, "G4 radius count mismatch: basin {g4_count} != pg {pg_g4_count}");
        }
        if g6_supported && g6_count >= 0 && pg_g6_count >= 0 {
            assert_eq!(g6_count, pg_g6_count, "G6 point-in-polygon count mismatch: basin {g6_count} != pg {pg_g6_count}");
        }
    }

    let ts = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    write_artifact("ext_bench_postgis.json", &json!({
        "card": "ext_bench_postgis",
        "family": "postgis",
        "generated_at": format!("@{ts}"),
        "pg_available": pg_available,
        "pg_extension_available": pg_ext_ok,
        "basin_gist_ddl_ok": gist_ok,
        "config": {
            "rows": rows, "samples": samples, "tiles_per_axis": TILES_PER_AXIS,
            "radius_m": RADIUS_M, "polygon_vertices": 50,
        },
        "ingest": {
            "label": "G1: point ingest with geo column (rows/s)",
            "basin_rows_per_s": basin_ingest_rate,
            "pg_rows_per_s": pg_ingest_rate,
        },
        "shapes": [
            { "label": "G2: bbox && — selective box", "basin_p50_ms": opt_ms(g2_b), "pg_p50_ms": opt_ms(pg_g2),
              "basin_over_pg": ratio(g2_b, pg_g2), "basin_hits": g2_count, "pg_hits": pg_g2_count,
              "basin_uses_index": gist_ok, "pg_uses_gist": pg_ext_ok },
            { "label": "G3: bbox && — wide box", "basin_p50_ms": opt_ms(g3_b), "pg_p50_ms": opt_ms(pg_g3),
              "basin_over_pg": ratio(g3_b, pg_g3), "basin_hits": g3_count },
            { "label": "G4: radius ST_DWithin count (metres)", "basin_p50_ms": opt_ms(g4_b), "pg_p50_ms": opt_ms(pg_g4),
              "basin_over_pg": ratio(g4_b, pg_g4), "basin_hits": g4_count, "pg_hits": pg_g4_count },
            { "label": "G5: nearest-neighbour <-> ORDER BY LIMIT 10", "basin_supported": g5_supported,
              "basin_p50_ms": opt_ms(g5_b), "pg_p50_ms": opt_ms(pg_g5), "basin_over_pg": ratio(g5_b, pg_g5) },
            { "label": "G6: point-in-polygon (~50-vertex)", "basin_supported": g6_supported,
              "basin_p50_ms": opt_ms(g6_b), "pg_p50_ms": opt_ms(pg_g6), "basin_over_pg": ratio(g6_b, pg_g6),
              "basin_hits": g6_count, "pg_hits": pg_g6_count },
        ],
        "note": "PG uses geometry(Point,4326) + GIST; Basin uses POINT + USING gist R-tree \
                 row-group prune. FAIRNESS: PG ST_DWithin operands cast to ::geography so \
                 BOTH engines measure metres with the same radius. bbox/radius/polygon \
                 counts hard-asserted equal across engines when PostGIS is available. \
                 Unsupported Basin shapes record basin_supported:false and a PG-only timing \
                 (the card never fails because a shape is unimplemented).",
    }));
}
