//! Basin (R-tree spatial index) vs PostgreSQL + PostGIS + GIST — head-to-head.
//!
//! PG-Wave 6 (#141). Mirrors the `compare_postgres_*` family but for the
//! spatial substrate shipped in PG-Wave α (#135, UDFs + POINT FSB(21) +
//! `USING gist` DDL + compaction-time R-tree sidecar) and PG-Wave β (#139,
//! `apply_rtree_pruning_for_query` row-group prune + lazy sidecar warm-up).
//!
//! Both engines seed the IDENTICAL point set — a 32×32 tile lattice over
//! `[0,10]²` (same layout as `viability_large_spatial_dwithin`) so the
//! Parquet row-groups cluster spatially and the R-tree prune has something
//! to eliminate. Basin builds its R-tree sidecar via `flush_to_parquet()`
//! (the real compaction hook) after the gist index is registered.
//!
//! Four spatial shapes (per scoping a85de3):
//!   S1 radius      — `count(*)` within R metres of (qx,qy)        (ST_DWithin)
//!   S4 radius-id   — `id` list within R metres                    (ST_DWithin)
//!   S2 bbox        — count within an axis-aligned box             (&& ST_MakeEnvelope)
//!   S3 KNN         — 10 nearest neighbours by distance            (PG-only, <->)
//!
//! ── FAIRNESS INVARIANT (ENFORCED) ────────────────────────────────────────
//! PostGIS `ST_DWithin` on `geometry(Point,4326)` measures DEGREES unless the
//! operands are cast to `::geography`, in which case it measures METRES
//! (geodesic). Basin's `ST_DWithin` is ALWAYS Haversine metres. So the PG side
//! MUST cast both operands to `::geography` and pass the SAME radius `R`
//! (metres) that Basin uses — otherwise the two engines run different queries
//! (a degree-radius circle vs a metre-radius circle) and the comparison is
//! meaningless. Every PG `ST_DWithin` below carries the `::geography` cast.
//!
//! ── CORRECTNESS GUARD ────────────────────────────────────────────────────
//! S1's PG-side count and basin-side count are HARD-ASSERTED equal. If they
//! diverge the queries are not equivalent (broken geography cast, seeding
//! mismatch, or a basin pruning false-negative) and the whole comparison is
//! invalid — so we fail rather than publish a misleading ratio.
//!
//! ── KNN (S3) ─────────────────────────────────────────────────────────────
//! Basin ships no KNN operator (`<->`) or index-ordered nearest-neighbour
//! scan yet, so S3 is PG-only: the basin cell carries a `-1.0` sentinel
//! (honest "not shipped"), PG's real `<->` LIMIT 10 time is recorded.
//!
//! ── SCALE ────────────────────────────────────────────────────────────────
//! Default N = 10 000 (the inline/`cargo test` scale — completes in well
//! under two minutes). The orchestrator overrides via `BASIN_POSTGIS_N` for
//! the 100k / 1M dashboard cards. A 1M GIST build + spatial sweep takes
//! minutes; DO NOT run it inline.

#![allow(clippy::print_stdout)]

use std::time::Instant;

use arrow_array::{Array, Int64Array};
use basin_common::ProjectId;
use basin_engine::ExecResult;
use basin_integration_tests::benchmark::{
    report_postgres_compare, CompareMetric, WhichWins,
};

#[path = "compare_postgres_common.rs"]
mod common;

use common::{build_basin_engine, median, try_connect, SchemaGuard};

/// 32×32 spatial tiles over `[0,10]²` — each tile ≈ 0.3125° wide. Mirrors
/// `viability_large_spatial_dwithin` so the Parquet row-groups cluster and
/// the R-tree prune is exercised, not bypassed.
const TILES_PER_AXIS: usize = 32;

/// Query origin (centre of the seeded square).
const QUERY_X: f64 = 5.0;
const QUERY_Y: f64 = 5.0;

/// Radius in METRES. ~0.5° at the equator ≈ 55 km; selectivity ≈ π·0.5²/100
/// ≈ 0.79 % of the seeded area — a genuinely selective spatial predicate so
/// the index has a meaningful win to demonstrate.
const RADIUS_M: f64 = 55_000.0;

/// S2 bbox: a 1°×1° box centred on the query origin (degrees, native units
/// for the `geom && ST_MakeEnvelope(...)` overlap test — no geographic cast
/// here, the bbox is a planar envelope test on both sides).
const BBOX_X0: f64 = 4.5;
const BBOX_Y0: f64 = 4.5;
const BBOX_X1: f64 = 5.5;
const BBOX_Y1: f64 = 5.5;

/// Per-shape latency samples. The shapes are sub-second at 10k/100k; at 1M
/// the orchestrator may lower this via env, but a few samples give a stable
/// median.
fn samples_for(n: usize) -> usize {
    if n <= 100_000 {
        7
    } else {
        3
    }
}

/// Deterministic LCG unit float in [0,1). Same generator as the viability
/// test so the two harnesses produce comparable distributions.
struct Lcg(u64);
impl Lcg {
    fn next_unit(&mut self) -> f64 {
        self.0 = self.0.wrapping_mul(1_103_515_245).wrapping_add(12_345) & 0x7fff_ffff;
        (self.0 as f64) / (1u64 << 31) as f64
    }
}

/// Build the identical `(id, x, y)` point set both engines seed. Points are
/// emitted tile-by-tile so a Parquet row-group covers roughly one tile.
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

/// Median of `n` PG `EXPLAIN (ANALYZE, FORMAT TEXT)` executions of
/// `sql_inner`, in milliseconds (engine time, excludes protocol roundtrip).
async fn pg_p50(pg: &tokio_postgres::Client, sql_inner: &str, n: usize) -> f64 {
    // Symmetric warm-up: one untimed pass before the timed window.
    let _ = pg
        .simple_query(&format!("EXPLAIN (ANALYZE, FORMAT TEXT) {sql_inner}"))
        .await;
    let mut samples = Vec::with_capacity(n);
    for _ in 0..n {
        let q = format!("EXPLAIN (ANALYZE, FORMAT TEXT) {sql_inner}");
        if let Ok(r) = pg.simple_query(&q).await {
            for m in &r {
                if let tokio_postgres::SimpleQueryMessage::Row(row) = m {
                    if let Some(line) = row.get(0) {
                        if let Some(idx) = line.find("Execution Time:") {
                            let after = &line[idx + "Execution Time:".len()..];
                            let t = after.trim();
                            if let Some(end) = t.find(' ') {
                                if let Ok(v) = t[..end].parse::<f64>() {
                                    samples.push(v);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    if samples.is_empty() {
        f64::NAN
    } else {
        median(&samples)
    }
}

/// Run a basin SQL `n` times (plus one warm-up), return the median wall-clock
/// ms. The caller is responsible for any result-count assertion.
async fn basin_p50(sess: &basin_engine::ProjectSession, sql: &str, n: usize) -> f64 {
    let _ = sess.execute(sql).await;
    let mut samples = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        sess.execute(sql).await.expect("basin spatial query");
        samples.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    median(&samples)
}

/// Sum the first Int64 column over all batches — used to read a `count(*)`
/// result out of basin.
fn basin_count(res: ExecResult) -> i64 {
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return 0,
    };
    let mut total = 0i64;
    for b in &batches {
        if let Some(a) = b.column(0).as_any().downcast_ref::<Int64Array>() {
            for i in 0..a.len() {
                total = total.saturating_add(a.value(i));
            }
        }
    }
    total
}

/// Count the rows returned by a basin SELECT (for S4, the id-list shape).
fn basin_row_count(res: ExecResult) -> i64 {
    match res {
        ExecResult::Rows { batches, .. } => {
            batches.iter().map(|b| b.num_rows() as i64).sum()
        }
        ExecResult::Empty { .. } => 0,
    }
}

fn ratio_text(basin: f64, pg: f64) -> Option<String> {
    if !basin.is_finite() || !pg.is_finite() || basin <= 0.0 || pg <= 0.0 {
        return None;
    }
    if pg >= basin {
        Some(format!("basin {:.2}× faster", pg / basin))
    } else {
        Some(format!("pg {:.2}× faster", basin / pg))
    }
}

fn which_wins(basin: f64, pg: f64) -> WhichWins {
    if !basin.is_finite() {
        return WhichWins::Postgres;
    }
    if !pg.is_finite() {
        return WhichWins::Basin;
    }
    if basin < pg {
        WhichWins::Basin
    } else if basin > pg {
        WhichWins::Postgres
    } else {
        WhichWins::Tie
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compare_postgis() {
    let n: usize = std::env::var("BASIN_POSTGIS_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let samples = samples_for(n);

    let (pg, conn_str) = match try_connect().await {
        Some(v) => v,
        None => {
            println!("[compare_postgis] postgres unavailable on 127.0.0.1:5432 — skipping");
            report_postgres_compare(
                "postgis",
                "Basin R-tree vs PostgreSQL + PostGIS GIST (spatial)",
                "PostgreSQL/PostGIS unavailable on the bench host — card not generated.",
                false,
                Vec::new(),
                Some("postgres unavailable"),
            );
            return;
        }
    };

    let points = build_points(n);
    println!("[compare_postgis] N={n} samples={samples} tiles={TILES_PER_AXIS}²");

    // ── PG side ──────────────────────────────────────────────────────────
    let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
    let schema = format!("basin_postgis_{suffix}");
    let _guard = SchemaGuard {
        schema: schema.clone(),
        conn_str: conn_str.clone(),
    };

    // PostGIS may live in a dedicated schema; CREATE EXTENSION is idempotent.
    if let Err(e) = pg
        .simple_query("CREATE EXTENSION IF NOT EXISTS postgis")
        .await
    {
        println!("[compare_postgis] CREATE EXTENSION postgis failed: {e} — skipping");
        report_postgres_compare(
            "postgis",
            "Basin R-tree vs PostgreSQL + PostGIS GIST (spatial)",
            "PostGIS extension not installable on the bench host — card not generated.",
            false,
            Vec::new(),
            Some("postgis extension unavailable"),
        );
        std::mem::forget(_guard);
        return;
    }

    pg.simple_query(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("pg create schema");
    pg.simple_query(&format!(
        "CREATE TABLE {schema}.pts (id BIGINT, geom geometry(Point,4326))"
    ))
    .await
    .expect("pg create table");

    // Seed PG in batches via ST_MakePoint(x,y) → SetSRID 4326.
    let batch = if n <= 10_000 { n } else { 5_000 };
    let mut i = 0usize;
    while i < points.len() {
        let end = (i + batch).min(points.len());
        let mut vals = String::with_capacity((end - i) * 48);
        for (k, (id, x, y)) in points[i..end].iter().enumerate() {
            if k > 0 {
                vals.push(',');
            }
            vals.push_str(&format!(
                "({id}, ST_SetSRID(ST_MakePoint({x},{y}),4326))"
            ));
        }
        pg.simple_query(&format!(
            "INSERT INTO {schema}.pts (id, geom) VALUES {vals}"
        ))
        .await
        .expect("pg seed pts");
        i = end;
    }
    pg.simple_query(&format!("CREATE INDEX ON {schema}.pts USING GIST(geom)"))
        .await
        .expect("pg create gist");
    pg.simple_query(&format!("ANALYZE {schema}.pts"))
        .await
        .expect("pg analyze");

    // ── Basin side ───────────────────────────────────────────────────────
    let mut instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();
    sess.execute("CREATE TABLE pts (id BIGINT, geom POINT) WITH (basin.file_format='parquet')")
        .await
        .expect("basin create table");

    // Seed basin with the SAME points via INSERT ... ST_MakePoint(x,y).
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
            .expect("basin seed pts");
        i = end;
    }
    // Register the GIST index, THEN flush — the compaction hook builds the
    // R-tree sidecar only for tables that carry a gist index at flush time.
    sess.execute("CREATE INDEX idx_pts_geom ON pts USING gist(geom)")
        .await
        .expect("basin create gist");
    instance
        .shard
        .flush_to_parquet()
        .await
        .expect("flush_to_parquet (builds R-tree sidecar)");

    // ── S1 radius: count within R metres ─────────────────────────────────
    //
    // FAIRNESS: PG casts both operands to ::geography so ST_DWithin measures
    // METRES (geodesic), matching Basin's Haversine-metres semantics with the
    // identical radius R. WITHOUT the cast PG would measure a DEGREE radius —
    // a different query. The cast is mandatory.
    let s1_pg_sql = format!(
        "SELECT count(*) FROM {schema}.pts \
         WHERE ST_DWithin(geom::geography, \
               ST_SetSRID(ST_MakePoint({QUERY_X},{QUERY_Y}),4326)::geography, {RADIUS_M})"
    );
    let s1_basin_sql = format!(
        "SELECT count(*) FROM pts \
         WHERE ST_DWithin(geom, ST_MakePoint({QUERY_X},{QUERY_Y}), {RADIUS_M})"
    );

    // Correctness guard — counts MUST match.
    let pg_s1_count: i64 = {
        let r = pg
            .query_one(
                &format!(
                    "SELECT count(*)::bigint FROM {schema}.pts \
                     WHERE ST_DWithin(geom::geography, \
                           ST_SetSRID(ST_MakePoint({QUERY_X},{QUERY_Y}),4326)::geography, {RADIUS_M})"
                ),
                &[],
            )
            .await
            .expect("pg s1 count");
        r.get::<usize, i64>(0)
    };
    let basin_s1_count = basin_count(sess.execute(&s1_basin_sql).await.expect("basin s1 count"));
    println!(
        "[compare_postgis] S1 count guard: pg={pg_s1_count} basin={basin_s1_count}"
    );
    let count_match = pg_s1_count == basin_s1_count;
    assert!(
        pg_s1_count > 0,
        "S1 matched 0 points — degenerate dataset (radius/origin mismatch?)"
    );
    assert_eq!(
        basin_s1_count, pg_s1_count,
        "S1 COUNT MISMATCH: basin {basin_s1_count} != postgis {pg_s1_count} — \
         queries are not equivalent (geography cast / seeding / prune false-negative)"
    );

    let s1_pg = pg_p50(&pg, &s1_pg_sql, samples).await;
    let s1_basin = basin_p50(&sess, &s1_basin_sql, samples).await;

    // ── S4 radius-id: id list within R metres ────────────────────────────
    let s4_pg_sql = format!(
        "SELECT id FROM {schema}.pts \
         WHERE ST_DWithin(geom::geography, \
               ST_SetSRID(ST_MakePoint({QUERY_X},{QUERY_Y}),4326)::geography, {RADIUS_M})"
    );
    let s4_basin_sql = format!(
        "SELECT id FROM pts \
         WHERE ST_DWithin(geom, ST_MakePoint({QUERY_X},{QUERY_Y}), {RADIUS_M})"
    );
    // Sanity: S4 returns the same number of rows as the S1 count.
    let basin_s4_rows =
        basin_row_count(sess.execute(&s4_basin_sql).await.expect("basin s4"));
    assert_eq!(
        basin_s4_rows, basin_s1_count,
        "S4 row count {basin_s4_rows} != S1 count {basin_s1_count}"
    );
    let s4_pg = pg_p50(&pg, &s4_pg_sql, samples).await;
    let s4_basin = basin_p50(&sess, &s4_basin_sql, samples).await;

    // ── S2 bbox: count within an axis-aligned box ────────────────────────
    //
    // Both engines now use the IDENTICAL native PostGIS idiom:
    // `geom && ST_MakeEnvelope(...)`.  On the basin side the operator-rewrite
    // pass normalizes `geom && ST_MakeEnvelope(a,b,c,d)` →
    // `ST_Contains(ST_MakeEnvelope(a,b,c,d), geom)`, which the spatial
    // predicate detector recognizes and routes through the R-tree row-group
    // pushdown (GIST index) instead of a full scan.
    let s2_pg_sql = format!(
        "SELECT count(*) FROM {schema}.pts \
         WHERE geom && ST_MakeEnvelope({BBOX_X0},{BBOX_Y0},{BBOX_X1},{BBOX_Y1},4326)"
    );
    let s2_basin_sql = format!(
        "SELECT count(*) FROM pts \
         WHERE geom && ST_MakeEnvelope({BBOX_X0},{BBOX_Y0},{BBOX_X1},{BBOX_Y1},4326)"
    );
    let pg_s2_count: i64 = {
        let r = pg
            .query_one(
                &format!(
                    "SELECT count(*)::bigint FROM {schema}.pts \
                     WHERE geom && ST_MakeEnvelope({BBOX_X0},{BBOX_Y0},{BBOX_X1},{BBOX_Y1},4326)"
                ),
                &[],
            )
            .await
            .expect("pg s2 count");
        r.get::<usize, i64>(0)
    };
    let basin_s2_count =
        basin_count(sess.execute(&s2_basin_sql).await.expect("basin s2 count"));
    println!(
        "[compare_postgis] S2 bbox count: pg(&&)={pg_s2_count} basin(&&→ST_Contains)={basin_s2_count} \
         (identical native idiom; basin rewrites to ST_Contains + R-tree pushdown)"
    );
    // Correctness guard: the rewrite must be semantically exact, not just
    // faster — basin's bbox count must equal PG's.
    let s2_count_match = pg_s2_count == basin_s2_count;
    assert_eq!(
        basin_s2_count, pg_s2_count,
        "S2 bbox count mismatch: basin={basin_s2_count} pg={pg_s2_count} \
         (&& → ST_Contains rewrite must be semantically exact)"
    );
    let s2_pg = pg_p50(&pg, &s2_pg_sql, samples).await;
    let s2_basin = basin_p50(&sess, &s2_basin_sql, samples).await;

    // ── S3 KNN: PG-only (basin has no <-> operator / KNN scan) ────────────
    let s3_pg_sql = format!(
        "SELECT id FROM {schema}.pts \
         ORDER BY geom <-> ST_SetSRID(ST_MakePoint({QUERY_X},{QUERY_Y}),4326) LIMIT 10"
    );
    let s3_pg = pg_p50(&pg, &s3_pg_sql, samples).await;
    let s3_basin = -1.0; // sentinel: not shipped

    // ── Report ───────────────────────────────────────────────────────────
    println!(
        "\n[compare_postgis N={n}]\n\
         {:<18}{:>14}{:>14}\n\
         {:<18}{:>12.3}ms{:>12.3}ms\n\
         {:<18}{:>12.3}ms{:>12.3}ms\n\
         {:<18}{:>12.3}ms{:>12.3}ms\n\
         {:<18}{:>14}{:>12.3}ms\n\
         S1 count-match guard: {} | S2 count-match guard: {}\n",
        "shape", "basin", "postgis",
        "S1 radius count", s1_basin, s1_pg,
        "S4 radius id-list", s4_basin, s4_pg,
        "S2 bbox count", s2_basin, s2_pg,
        "S3 KNN (PG-only)", "n/a", s3_pg,
        if count_match { "PASS" } else { "FAIL" },
        if s2_count_match { "PASS" } else { "FAIL" },
    );

    let metrics = vec![
        CompareMetric {
            label: "S1 radius count (ST_DWithin)".into(),
            basin: s1_basin,
            postgres: s1_pg,
            unit: "ms".into(),
            better: which_wins(s1_basin, s1_pg),
            ratio_text: ratio_text(s1_basin, s1_pg),
        },
        CompareMetric {
            label: "S4 radius id-list (ST_DWithin)".into(),
            basin: s4_basin,
            postgres: s4_pg,
            unit: "ms".into(),
            better: which_wins(s4_basin, s4_pg),
            ratio_text: ratio_text(s4_basin, s4_pg),
        },
        CompareMetric {
            label: "S2 bbox count (&& → ST_Contains R-tree)".into(),
            basin: s2_basin,
            postgres: s2_pg,
            unit: "ms".into(),
            better: which_wins(s2_basin, s2_pg),
            ratio_text: ratio_text(s2_basin, s2_pg),
        },
        CompareMetric {
            label: "S3 KNN <-> LIMIT 10 (PG-only)".into(),
            basin: s3_basin,
            postgres: s3_pg,
            unit: "ms".into(),
            better: WhichWins::Postgres,
            ratio_text: Some("basin KNN not shipped".into()),
        },
    ];

    report_postgres_compare(
        "postgis",
        "Basin R-tree vs PostgreSQL + PostGIS GIST (spatial)",
        "Identical 32×32-tile point set on both engines. PG uses geometry(Point,4326) \
         + GIST; Basin uses POINT (FSB-21) + USING gist R-tree row-group prune. \
         ST_DWithin radius queries cast PG operands to ::geography so BOTH engines \
         measure metres with the same radius (fairness invariant). S1 result counts \
         are hard-asserted equal (correctness guard). S2 bbox uses the IDENTICAL \
         native PostGIS idiom on both sides (geom && ST_MakeEnvelope(...)); Basin \
         rewrites it to ST_Contains(envelope, geom) and routes it through the GIST \
         R-tree row-group pushdown, with the bbox count hard-asserted equal to PG. \
         S3 KNN is PG-only — Basin ships no <-> operator yet.",
        true,
        metrics,
        Some(if count_match && s2_count_match {
            "S1 + S2 count-match guards: PASS"
        } else {
            "count-match guard: FAIL"
        }),
    );

    // Cleanup: drop PG schema on the live connection then defuse the guard
    // (its Drop does a re-entrant block_on that can deadlock — see
    // perf_smoke_pg_10k).
    let _ = pg
        .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    std::mem::forget(_guard);

    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }
    instance.wal.close().await.unwrap();
}
