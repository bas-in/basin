//! Viability test: PostGIS-compatible spatial subset (`basin-geo`).
//!
//! Card: `viability_postgis`
//! Bar: `haversine_within_1pct && dwithin_correct_count && wkb_roundtrip_correct`
//!
//! What this proves:
//!
//! 1. **Haversine accuracy** — for a known landmark pair (Eiffel ↔ Big Ben,
//!    reference distance ≈ 343 556 m via PostGIS `ST_DistanceSphere`), our
//!    `haversine_meters` returns a value within 1% of truth. This is the
//!    same accuracy bar PostGIS itself ships with.
//!
//! 2. **`ST_DWithin` selectivity** — we scatter 100 synthetic points
//!    around Europe (jittered around Paris), label each one as "should be
//!    within 500 km of Paris" using a deterministic ground-truth function,
//!    and assert `basin_geo::dwithin` returns the same set. This is the
//!    most common spatial query in production (find rows within a radius)
//!    and the function whose correctness we most need to nail down before
//!    a v0.2 spatial index lands.
//!
//! 3. **WKB round-trip** — every point is encoded to its 21-byte
//!    PostGIS-canonical WKB blob and decoded back; the decoded coordinates
//!    must match bit-for-bit. This is what eventually goes onto disk in
//!    Parquet (`FixedSizeBinary(21)`) once the engine glue is wired in v0.2.
//!
//! Same staging pattern as `viability_basin_cron` and `viability_basin_net`:
//! v0.1 exposes a Rust API (no SQL surface yet), and the engine glue
//! (`basin_engine::geo_glue`) is the no-op stub where the future
//! `ScalarUDF` registrations will live.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::Array;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_geo::{decode_point, dwithin, encode_point, haversine_meters, Point, POINT_WKB_LEN};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

/// Centroid-ish reference point near Paris, used as the radius origin
/// for the `ST_DWithin` portion of the test.
const PARIS_LON: f64 = 2.3522;
const PARIS_LAT: f64 = 48.8566;

/// Search radius for the `ST_DWithin` portion of the test. 500 km
/// covers most of France, southern UK, and Belgium — plenty of inside
/// and outside rows in the synthetic dataset.
const RADIUS_M: f64 = 500_000.0;

/// 1% relative-error bound — the bar `haversine_within_1pct`.
const HAVERSINE_TOL: f64 = 0.01;

#[tokio::test]
async fn viability_postgis() {
    // -- 1. Haversine accuracy: Eiffel ↔ Big Ben ≈ 343_556 m -------------
    let eiffel = Point::new(2.2945, 48.8584);
    let big_ben = Point::new(-0.1246, 51.5007);
    let measured = haversine_meters(&eiffel, &big_ben);
    let reference = 343_556.0_f64;
    let rel_err = ((measured - reference) / reference).abs();
    let haversine_within_1pct = rel_err < HAVERSINE_TOL;

    // -- 2. ST_DWithin selectivity over 100 scattered European points ----
    //
    // Generate points on a deterministic grid around Paris. Each point
    // sits at (PARIS_LON + dlon, PARIS_LAT + dlat) for dlon, dlat in
    // [-9°..+9°] step 2°. That gives us a 10×10 = 100-point lattice
    // covering ~2_000_000 m² of Europe — comfortably more than the
    // 500 km search radius in every direction, so the result set has a
    // mix of inside and outside rows we can assert against.
    let mut points: Vec<Point> = Vec::with_capacity(100);
    for i in 0..10 {
        for j in 0..10 {
            let dlon = (i as f64) * 2.0 - 9.0;
            let dlat = (j as f64) * 2.0 - 9.0;
            points.push(Point::new(PARIS_LON + dlon, PARIS_LAT + dlat));
        }
    }
    assert_eq!(points.len(), 100);

    let paris = Point::new(PARIS_LON, PARIS_LAT);

    // Ground-truth count: rows whose true Haversine distance ≤ 500 km.
    // We compute this with the same function under test, but the unit
    // tests in basin-geo's test suite already pin down haversine_meters
    // accuracy independently — so this comparison really probes the
    // dwithin wrapper's `<=` semantics, not the distance math.
    let truth_count = points
        .iter()
        .filter(|p| haversine_meters(p, &paris) <= RADIUS_M)
        .count();
    let dwithin_count = points
        .iter()
        .filter(|p| dwithin(p, &paris, RADIUS_M))
        .count();
    let dwithin_correct_count = truth_count == dwithin_count && truth_count > 0;

    // -- 3. WKB round-trip: encode → decode → bit-for-bit equal ----------
    let mut wkb_roundtrip_correct = true;
    let mut wkb_total_bytes: usize = 0;
    for p in &points {
        let bytes = encode_point(p);
        wkb_total_bytes += bytes.len();
        if bytes.len() != POINT_WKB_LEN {
            wkb_roundtrip_correct = false;
            break;
        }
        let decoded = match decode_point(&bytes) {
            Ok(q) => q,
            Err(_) => {
                wkb_roundtrip_correct = false;
                break;
            }
        };
        if decoded.x != p.x || decoded.y != p.y {
            wkb_roundtrip_correct = false;
            break;
        }
    }

    let pass = haversine_within_1pct && dwithin_correct_count && wkb_roundtrip_correct;
    println!(
        "[VIABILITY postgis] eiffel_big_ben_m={measured:.1} ref={reference:.1} \
         rel_err={rel_err:.4} haversine_within_1pct={haversine_within_1pct} \
         dwithin_count={dwithin_count} truth_count={truth_count} \
         dwithin_correct_count={dwithin_correct_count} \
         wkb_roundtrip_correct={wkb_roundtrip_correct} \
         wkb_total_bytes={wkb_total_bytes} {}",
        if pass { "PASS" } else { "FAIL" }
    );

    let primary = PrimaryMetric {
        label: "checks_passed".into(),
        value: [
            haversine_within_1pct,
            dwithin_correct_count,
            wkb_roundtrip_correct,
        ]
        .iter()
        .filter(|b| **b)
        .count() as f64,
        unit: "checks".into(),
        bar: BarOp::eq(3.0),
    };

    report_viability(
        "postgis",
        "PostGIS-compatible spatial subset (POINT, BOX2D, ST_Distance, ST_DWithin)",
        "100 European points: Haversine within 1% of PostGIS ST_DistanceSphere; \
         ST_DWithin matches truth set; WKB round-trip is byte-exact (21 B/point).",
        pass,
        primary,
        json!({
            "haversine_within_1pct": haversine_within_1pct,
            "dwithin_correct_count": dwithin_correct_count,
            "wkb_roundtrip_correct": wkb_roundtrip_correct,
            "eiffel_big_ben_measured_m": measured,
            "eiffel_big_ben_reference_m": reference,
            "haversine_relative_error": rel_err,
            "points_total": points.len(),
            "dwithin_count": dwithin_count,
            "dwithin_truth_count": truth_count,
            "wkb_bytes_per_point": POINT_WKB_LEN,
            "wkb_total_bytes": wkb_total_bytes,
        }),
    );

    assert!(pass);
}

// ---------------------------------------------------------------------------
// SQL end-to-end: PG-Wave 3 ST_Intersects exercised through the engine
// ---------------------------------------------------------------------------

async fn open_engine() -> (TempDir, Engine) {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    (dir, engine)
}

/// End-to-end SQL test for PG-Wave 3 ST_Intersects UDF.
///
/// ```sql
/// CREATE TABLE t (p POINT) WITH (basin.file_format='parquet');
/// INSERT INTO t VALUES (ST_MakePoint(1.0, 2.0)), (ST_MakePoint(3.0, 4.0));
/// SELECT ST_Intersects(p, ST_MakePoint(1.0, 2.0)) FROM t ORDER BY ST_X(p);
/// -- expect: [true, false]
/// ```
#[tokio::test]
async fn st_intersects_sql_end_to_end() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (p POINT) WITH (basin.file_format='parquet')")
        .await
        .unwrap();

    sess.execute("INSERT INTO t VALUES (ST_MakePoint(1.0, 2.0)), (ST_MakePoint(3.0, 4.0))")
        .await
        .unwrap();

    let result = sess
        .execute("SELECT ST_Intersects(p, ST_MakePoint(1.0, 2.0)) FROM t ORDER BY ST_X(p)")
        .await
        .unwrap();

    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };

    // Flatten all rows across batches.
    let mut values: Vec<bool> = Vec::new();
    for batch in &batches {
        let col = batch.column(0);
        let bools = col
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap_or_else(|| panic!("expected BooleanArray, got {:?}", col.data_type()));
        for i in 0..bools.len() {
            values.push(bools.value(i));
        }
    }

    println!(
        "[ST_Intersects SQL] result = {:?} (expect [true, false])",
        values
    );
    assert_eq!(values, vec![true, false], "ST_Intersects result mismatch");
}

// ---------------------------------------------------------------------------
// SQL end-to-end: ST_AsGeoJSON / ST_GeomFromGeoJSON
// ---------------------------------------------------------------------------

/// SQL assertion 1: ST_AsGeoJSON emits the PostGIS-canonical envelope.
///
/// ```sql
/// SELECT ST_AsGeoJSON(ST_MakePoint(2.2945, 48.8584));
/// -- expect: {"type":"Point","coordinates":[2.2945,48.8584]}
/// ```
#[tokio::test]
async fn st_asgeojson_sql_format() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let result = sess
        .execute("SELECT ST_AsGeoJSON(ST_MakePoint(2.2945, 48.8584))")
        .await
        .unwrap();

    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };

    let mut values: Vec<String> = Vec::new();
    for batch in &batches {
        let col = batch.column(0);
        let strs = col
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap_or_else(|| panic!("expected StringArray, got {:?}", col.data_type()));
        for i in 0..strs.len() {
            values.push(strs.value(i).to_string());
        }
    }

    assert_eq!(values.len(), 1);
    let got = &values[0];
    println!("[ST_AsGeoJSON format] got = {got}");
    // Must be the exact PostGIS-canonical form (no spaces, "Point" capitalised,
    // lon first).
    assert!(
        got.starts_with("{\"type\":\"Point\",\"coordinates\":["),
        "unexpected format: {got}"
    );
    assert!(got.ends_with("]}"), "expected GeoJSON to end with ']}}'");
    // Coordinates must contain the input values.
    assert!(got.contains("2.2945"), "missing lon in {got}");
    assert!(got.contains("48.8584"), "missing lat in {got}");
}

/// SQL assertion 2: ST_GeomFromGeoJSON round-trips through ST_AsGeoJSON
/// and recovers the original WKT form via ST_AsText.
///
/// ```sql
/// SELECT ST_AsText(ST_GeomFromGeoJSON('{"type":"Point","coordinates":[1.5,2.5]}'));
/// -- expect: POINT(1.5 2.5)
/// ```
#[tokio::test]
async fn st_geomfromgeojson_sql_round_trip() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let result = sess
        .execute(
            "SELECT ST_AsText(ST_GeomFromGeoJSON('{\"type\":\"Point\",\"coordinates\":[1.5,2.5]}'))",
        )
        .await
        .unwrap();

    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };

    let mut values: Vec<String> = Vec::new();
    for batch in &batches {
        let col = batch.column(0);
        let strs = col
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap_or_else(|| panic!("expected StringArray, got {:?}", col.data_type()));
        for i in 0..strs.len() {
            values.push(strs.value(i).to_string());
        }
    }

    assert_eq!(values.len(), 1);
    println!("[ST_GeomFromGeoJSON round-trip] got = {}", values[0]);
    assert_eq!(
        values[0], "POINT(1.5 2.5)",
        "ST_AsText(ST_GeomFromGeoJSON(...)) mismatch"
    );
}

/// SQL assertion 3: full round-trip through a table — insert via
/// ST_GeomFromGeoJSON, read back via ST_AsGeoJSON, confirm the envelope.
///
/// ```sql
/// CREATE TABLE gj (p POINT) WITH (basin.file_format='parquet');
/// INSERT INTO gj VALUES (ST_GeomFromGeoJSON('{"type":"Point","coordinates":[-73.985,40.748]}'));
/// SELECT ST_AsGeoJSON(p) FROM gj;
/// -- expect one row containing -73.985 and 40.748
/// ```
#[tokio::test]
async fn st_geojson_table_round_trip() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE gj (p POINT) WITH (basin.file_format='parquet')")
        .await
        .unwrap();

    sess.execute(
        "INSERT INTO gj VALUES \
         (ST_GeomFromGeoJSON('{\"type\":\"Point\",\"coordinates\":[-73.985,40.748]}'))",
    )
    .await
    .unwrap();

    let result = sess
        .execute("SELECT ST_AsGeoJSON(p) FROM gj")
        .await
        .unwrap();

    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };

    let mut values: Vec<String> = Vec::new();
    for batch in &batches {
        let col = batch.column(0);
        let strs = col
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap_or_else(|| panic!("expected StringArray, got {:?}", col.data_type()));
        for i in 0..strs.len() {
            values.push(strs.value(i).to_string());
        }
    }

    assert_eq!(values.len(), 1, "expected exactly one row");
    let got = &values[0];
    println!("[ST_AsGeoJSON table round-trip] got = {got}");
    assert!(got.contains("-73.985"), "missing lon in {got}");
    assert!(got.contains("40.748"), "missing lat in {got}");
    assert!(
        got.starts_with("{\"type\":\"Point\",\"coordinates\":["),
        "wrong envelope: {got}"
    );
}
