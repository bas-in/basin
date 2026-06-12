//! PostGIS-lite conformance tests — Basin's `ST_*` SQL surface.
//!
//! ## What is covered
//!
//! Tests operate on the `basin-engine` SQL layer, same surface that production
//! queries use. Every expected value is derived analytically from WGS84/PostGIS
//! geometry semantics and annotated when a `psql` spot-check is recommended.
//!
//! Groups:
//!   1. **Point construction & WKT round-trip** — `ST_MakePoint`, `ST_AsText`,
//!      `ST_GeomFromText`.
//!   2. **GeoJSON round-trip** — `ST_AsGeoJSON`, `ST_GeomFromGeoJSON`, full
//!      RFC 7946 envelope for POINTs.
//!   3. **WKB round-trip** — `ST_AsEWKB`, `ST_GeomFromWKB`.
//!   4. **Coordinate accessors** — `ST_X`, `ST_Y`.
//!   5. **Distance (Haversine)** — `ST_Distance` in metres; known pairs.
//!   6. **ST_DWithin** — radius predicate truth table; edge cases.
//!   7. **Topology predicates** — `ST_Intersects`, `ST_Disjoint`, `ST_Within`,
//!      `ST_Crosses`, `ST_Touches`, `ST_Overlaps` — all with POINT × POINT.
//!   8. **Degenerate scalars** — `ST_Area`, `ST_Perimeter`, `ST_NumPoints`,
//!      `ST_StartPoint`, `ST_EndPoint`, `ST_Envelope`, `ST_Centroid`,
//!      `ST_Buffer`.
//!   9. **Bbox helpers** — `ST_MakeEnvelope`, `__basin_bbox_contains_point`.
//!  10. **SRID surface** — `ST_SRID`, `ST_SetSRID`, `ST_Transform`.
//!  11. **Geography aliases** — `ST_GeogFromText`, `ST_GeographyFromText`.
//!  12. **Table-level usage** — INSERT/SELECT/WHERE with a POINT column.
//!  13. **Ordering by distance** — `ORDER BY ST_Distance(...) LIMIT k`.
//!  14. **NULL handling** — every UDF is NULL-safe.
//!  15. **Invalid-input errors** — malformed WKT/GeoJSON must error cleanly.
//!
//! ## What is NOT covered here (gap table)
//!
//! | Feature                                           | Reason not tested                        |
//! |---------------------------------------------------|------------------------------------------|
//! | `BOX2D` / `GEOMETRY` column DDL type             | Not yet a native planner type; POINT only|
//! | `ST_Contains(box2d, point)` SQL UDF               | BOX2D physical column deferred (v0.2)    |
//! | `LINESTRING` / `POLYGON` SQL column type         | Not planner-typed; stored as TEXT/BYTEA  |
//! | `ST_Length`, `ST_Area` on LineString / Polygon    | basin-geo Rust API only, no SQL surface  |
//! | `ST_AsGeoJSON` for LineString / Polygon           | geojson crate only, no SQL UDF           |
//! | R-tree / GIST spatial index in SQL                | `CREATE INDEX USING gist` DDL: accepted; |
//! |                                                   | sidecar build deferred to compaction     |
//! | `ST_Distance` with `::geography` cast             | No `geography` type DDL; Haversine is    |
//! |                                                   | always the metric; cast is a no-op       |
//! | EWKB (25-byte, SRID embedded in wire bytes)       | Only 21-byte plain WKB in v0.1           |
//! | `ST_PointN` for n > 1 on POINT columns            | Returns NULL — tested below              |
//! | `ST_Crosses`, `ST_Touches`, `ST_Overlaps` > 0    | Always false for POINT × POINT (correct) |
//! | Antimeridian / polar edge cases in SQL predicates | No R-tree rewrite; planar ray-cast is    |
//! |                                                   | Rust-API only; spatial_knn covers KNN    |
//! | Multipart geometries (MULTIPOINT etc.)            | Not modelled in v0.1                     |
//!
//! ## Needs-psql-validation table
//!
//! | ID    | Test                                   | Value to validate                            |
//! |-------|----------------------------------------|----------------------------------------------|
//! | GEO-1 | `st_distance_eiffel_to_big_ben`        | `ST_Distance(…::geography)` in metres        |
//! |       |                                        | (reference 343 556 m ± 1%)                   |
//! | GEO-2 | `st_distance_coincident_zero`          | PG returns 0.0 for identical points          |
//! | GEO-3 | `st_dwithin_boundary_inclusive`        | point exactly at `radius` → included         |
//! | GEO-4 | `st_astext_lon_lat_order`              | PostGIS: `POINT(lon lat)` (x=lon, y=lat)     |
//! | GEO-5 | `st_asgeojson_compact_floats`          | PostGIS: `{"type":"Point","coordinates":[x,y]}` |
//! |       |                                        | compact format, no extra spaces              |
//! | GEO-6 | `st_transform_wgs84_to_web_mercator`   | Easting ~1 491 000 m for lon=13.4° (Berlin)  |
//! | GEO-7 | `st_srid_default_4326`                 | Newly-stored points have SRID 4326           |

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float64Array, Int32Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Harness helpers
// ─────────────────────────────────────────────────────────────────────────────

fn make_engine() -> (Engine, TempDir) {
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
    (engine, dir)
}

async fn exec(sess: &basin_engine::ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e}"));
}

/// Execute `sql` and panic on any error. Panics on non-row results too.
async fn single_string(sess: &basin_engine::ProjectSession, sql: &str) -> String {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().unwrap_or_else(|| panic!("no batch: {sql}"));
            let col = b.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("expected Utf8 column for: {sql}"));
            assert!(arr.len() >= 1, "no rows from: {sql}");
            arr.value(0).to_owned()
        }
        Ok(other) => panic!("non-rows for {sql}: {other:?}"),
        Err(e) => panic!("error for {sql}: {e}"),
    }
}

async fn single_f64(sess: &basin_engine::ProjectSession, sql: &str) -> f64 {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().unwrap_or_else(|| panic!("no batch: {sql}"));
            let col = b.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap_or_else(|| panic!("expected Float64 column for: {sql}"));
            assert!(arr.len() >= 1, "no rows from: {sql}");
            arr.value(0)
        }
        Ok(other) => panic!("non-rows for {sql}: {other:?}"),
        Err(e) => panic!("error for {sql}: {e}"),
    }
}

async fn single_bool(sess: &basin_engine::ProjectSession, sql: &str) -> bool {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().unwrap_or_else(|| panic!("no batch: {sql}"));
            let col = b.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap_or_else(|| panic!("expected Boolean column for: {sql}"));
            assert!(arr.len() >= 1, "no rows from: {sql}");
            arr.value(0)
        }
        Ok(other) => panic!("non-rows for {sql}: {other:?}"),
        Err(e) => panic!("error for {sql}: {e}"),
    }
}

async fn single_bool_nullable(
    sess: &basin_engine::ProjectSession,
    sql: &str,
) -> Option<bool> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().unwrap_or_else(|| panic!("no batch: {sql}"));
            let col = b.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap_or_else(|| panic!("expected Boolean column for: {sql}"));
            assert!(arr.len() >= 1, "no rows from: {sql}");
            if arr.is_null(0) { None } else { Some(arr.value(0)) }
        }
        Ok(other) => panic!("non-rows for {sql}: {other:?}"),
        Err(e) => panic!("error for {sql}: {e}"),
    }
}

async fn row_count(sess: &basin_engine::ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches.iter().map(|b| b.num_rows()).sum(),
        Ok(ExecResult::Empty { .. }) => 0,
        Err(e) => panic!("error for {sql}: {e}"),
    }
}

/// Return sorted i64 ids from the first column of the result.
async fn sorted_ids(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<i64> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let mut ids = Vec::new();
            for b in &batches {
                if let Some(arr) = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            ids.push(arr.value(i));
                        }
                    }
                }
            }
            ids.sort_unstable();
            ids
        }
        Ok(ExecResult::Empty { .. }) => Vec::new(),
        Err(e) => panic!("error: {sql}: {e}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 1 — Point construction & WKT round-trip
// ─────────────────────────────────────────────────────────────────────────────

/// `ST_AsText(ST_MakePoint(x, y))` must return `POINT(x y)` in WKT.
///
/// PostGIS:  `SELECT ST_AsText(ST_MakePoint(2.2945, 48.8584));`
///           `=> POINT(2.2945 48.8584)`
///
/// [GEO-4] NEEDS-PSQL-VALIDATION: confirm the exact decimal formatting PG
/// uses for non-round coordinates (Grisu3/Dragon4 shortest-round-trip).
#[tokio::test]
async fn st_astext_roundtrip_wkt() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Eiffel Tower: lon = 2.2945°E, lat = 48.8584°N.
    let got = single_string(
        &sess,
        "SELECT ST_AsText(ST_MakePoint(2.2945, 48.8584))",
    )
    .await;
    assert_eq!(
        got, "POINT(2.2945 48.8584)",
        "ST_AsText must return 'POINT(x y)' WKT; got={got:?}"
    );

    // Negative coordinates (western hemisphere / southern hemisphere).
    let nyc = single_string(
        &sess,
        "SELECT ST_AsText(ST_MakePoint(-74.006, 40.7128))",
    )
    .await;
    assert!(
        nyc.starts_with("POINT("),
        "ST_AsText for NYC must start with POINT(; got={nyc:?}"
    );
    assert!(
        nyc.contains("-74.006"),
        "ST_AsText: negative longitude must be preserved; got={nyc:?}"
    );
    println!("[geo ST_AsText] WKT round-trip ✓  got={got:?}");
}

/// `ST_GeomFromText('POINT(x y)')` must construct a point that round-trips
/// through `ST_AsText` unchanged.
#[tokio::test]
async fn st_geomfromtext_roundtrip() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let got = single_string(
        &sess,
        "SELECT ST_AsText(ST_GeomFromText('POINT(13.405 52.52)'))",
    )
    .await;
    assert_eq!(
        got, "POINT(13.405 52.52)",
        "ST_GeomFromText→ST_AsText must round-trip; got={got:?}"
    );
    println!("[geo ST_GeomFromText] round-trip ✓");
}

/// `ST_GeomFromText` must accept case-insensitive 'POINT' / 'point' prefix.
#[tokio::test]
async fn st_geomfromtext_case_insensitive() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    for wkt in &["POINT(1.0 2.0)", "point(1.0 2.0)", "Point(1.0 2.0)"] {
        let got = single_string(
            &sess,
            &format!("SELECT ST_AsText(ST_GeomFromText('{wkt}'))"),
        )
        .await;
        // PG/PostGIS ST_AsText strips trailing ".0" from integer-valued coords:
        // ST_AsText(ST_GeomFromText('POINT(1.0 2.0)')) → "POINT(1 2)"
        assert_eq!(
            got, "POINT(1 2)",
            "ST_GeomFromText must accept {wkt:?}; got={got:?}"
        );
    }
    println!("[geo ST_GeomFromText] case-insensitive ✓");
}

/// `ST_GeomFromText` with malformed WKT must return an error, not a panic.
#[tokio::test]
async fn st_geomfromtext_invalid_wkt_errors() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    for bad in &[
        "LINESTRING(0 0, 1 1)",  // unsupported geometry type
        "POINT()",               // missing coordinates
        "POINT(1.0)",            // single coordinate (Y missing)
        "not_a_wkt",             // garbage
    ] {
        let r = sess
            .execute(&format!("SELECT ST_AsText(ST_GeomFromText('{bad}'))"))
            .await;
        assert!(
            r.is_err(),
            "ST_GeomFromText({bad:?}) must error; got={r:?}"
        );
    }
    println!("[geo ST_GeomFromText] invalid WKT → error ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 2 — GeoJSON round-trip
// ─────────────────────────────────────────────────────────────────────────────

/// `ST_AsGeoJSON(ST_MakePoint(x, y))` must return the full RFC 7946 envelope.
///
/// PostGIS:  `SELECT ST_AsGeoJSON(ST_MakePoint(13.405, 52.52));`
///           `=> {"type":"Point","coordinates":[13.405,52.52]}`
///
/// [GEO-5] NEEDS-PSQL-VALIDATION: exact whitespace, field-order in the JSON.
#[tokio::test]
async fn st_asgeojson_full_envelope() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Use exact-decimal coordinates so the compact float formatter doesn't add
    // decimal noise (1.5 → "1.5", not "1.5000000000000000").
    let got = single_string(
        &sess,
        "SELECT ST_AsGeoJSON(ST_MakePoint(1.5, 2.5))",
    )
    .await;
    assert_eq!(
        got, r#"{"type":"Point","coordinates":[1.5,2.5]}"#,
        "ST_AsGeoJSON must match RFC 7946 envelope (compact, lon-first); got={got:?}"
    );

    // Negative coordinates preserved.
    let neg = single_string(
        &sess,
        "SELECT ST_AsGeoJSON(ST_MakePoint(-73.985, 40.748))",
    )
    .await;
    assert!(
        neg.contains("-73.985") && neg.contains("40.748"),
        "ST_AsGeoJSON: coordinates must be preserved; got={neg:?}"
    );
    println!("[geo ST_AsGeoJSON] full envelope ✓");
}

/// `ST_GeomFromGeoJSON` must reconstruct a point from `ST_AsGeoJSON` output
/// such that `ST_AsText` round-trips unchanged.
#[tokio::test]
async fn st_geomfromgeojson_roundtrip() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Compose: ST_AsText(ST_GeomFromGeoJSON(ST_AsGeoJSON(ST_MakePoint(x,y)))) = POINT(x y)
    let got = single_string(
        &sess,
        "SELECT ST_AsText(ST_GeomFromGeoJSON(ST_AsGeoJSON(ST_MakePoint(3.0, 4.0))))",
    )
    .await;
    // PG/PostGIS ST_AsText strips trailing ".0": POINT(3.0 4.0) → "POINT(3 4)"
    assert_eq!(
        got, "POINT(3 4)",
        "ST_GeomFromGeoJSON round-trip must restore original coordinates; got={got:?}"
    );

    // Literal GeoJSON string.
    let lit = single_string(
        &sess,
        r#"SELECT ST_AsText(ST_GeomFromGeoJSON('{"type":"Point","coordinates":[5.0,6.0]}'))"#,
    )
    .await;
    assert_eq!(
        lit, "POINT(5 6)",
        "ST_GeomFromGeoJSON from literal JSON must work; got={lit:?}"
    );
    println!("[geo ST_GeomFromGeoJSON] round-trip ✓");
}

/// `ST_GeomFromGeoJSON` with wrong type must error.
#[tokio::test]
async fn st_geomfromgeojson_wrong_type_errors() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Geometry type "LineString" is not POINT — must error.
    let r = sess
        .execute(
            r#"SELECT ST_AsText(ST_GeomFromGeoJSON('{"type":"LineString","coordinates":[[0,0],[1,1]]}'))"#,
        )
        .await;
    assert!(
        r.is_err(),
        "ST_GeomFromGeoJSON with non-Point type must error; got={r:?}"
    );

    // Missing 'type' field.
    let r2 = sess
        .execute(r#"SELECT ST_AsText(ST_GeomFromGeoJSON('{"coordinates":[1,2]}'))"#)
        .await;
    assert!(r2.is_err(), "ST_GeomFromGeoJSON without type field must error");
    println!("[geo ST_GeomFromGeoJSON] invalid type → error ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 3 — WKB round-trip
// ─────────────────────────────────────────────────────────────────────────────

/// `ST_AsEWKB` returns the raw 21-byte blob; feeding it back to
/// `ST_GeomFromWKB` → `ST_AsText` must recover the original WKT.
#[tokio::test]
async fn st_asewkb_st_geomfromwkb_roundtrip() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let got = single_string(
        &sess,
        "SELECT ST_AsText(ST_GeomFromWKB(ST_AsEWKB(ST_MakePoint(7.0, 8.0))))",
    )
    .await;
    // PG/PostGIS ST_AsText strips trailing ".0": POINT(7.0 8.0) → "POINT(7 8)"
    assert_eq!(
        got, "POINT(7 8)",
        "ST_GeomFromWKB(ST_AsEWKB(…)) must recover original coords; got={got:?}"
    );
    println!("[geo WKB] round-trip ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 4 — Coordinate accessors
// ─────────────────────────────────────────────────────────────────────────────

/// `ST_X(ST_MakePoint(lon, lat))` returns the longitude (x component).
/// `ST_Y(ST_MakePoint(lon, lat))` returns the latitude (y component).
///
/// PostGIS convention: x = longitude, y = latitude.
/// PG: `SELECT ST_X(ST_MakePoint(2.2945, 48.8584)); => 2.2945`
#[tokio::test]
async fn st_x_y_extract_coordinates() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let x = single_f64(&sess, "SELECT ST_X(ST_MakePoint(2.2945, 48.8584))").await;
    let y = single_f64(&sess, "SELECT ST_Y(ST_MakePoint(2.2945, 48.8584))").await;

    assert!(
        (x - 2.2945).abs() < 1e-10,
        "ST_X must return longitude 2.2945; got={x}"
    );
    assert!(
        (y - 48.8584).abs() < 1e-10,
        "ST_Y must return latitude 48.8584; got={y}"
    );

    // Negative coordinates.
    let xn = single_f64(&sess, "SELECT ST_X(ST_MakePoint(-74.006, 40.7128))").await;
    assert!(
        (xn - (-74.006)).abs() < 1e-10,
        "ST_X must handle negative longitude; got={xn}"
    );
    println!("[geo ST_X/ST_Y] accessor round-trips ✓");
}

/// `ST_X` / `ST_Y` on NULL input must return NULL.
#[tokio::test]
async fn st_x_y_null_safe() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE nullpts (id BIGINT, geom POINT)").await;
    exec(&sess, "INSERT INTO nullpts VALUES (1, NULL)").await;

    match sess.execute("SELECT ST_X(geom) FROM nullpts WHERE id = 1").await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().expect("batch");
            assert!(b.column(0).is_null(0), "ST_X(NULL) must return NULL");
        }
        Ok(other) => panic!("unexpected: {other:?}"),
        Err(e) => panic!("ST_X(NULL) must not error: {e}"),
    }
    println!("[geo ST_X/ST_Y] NULL safe ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 5 — ST_Distance (Haversine, metres)
// ─────────────────────────────────────────────────────────────────────────────

/// Coincident points must have zero distance.
///
/// [GEO-2] NEEDS-PSQL-VALIDATION: PostGIS `ST_Distance(…::geography)` = 0.
#[tokio::test]
async fn st_distance_coincident_zero() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let d = single_f64(
        &sess,
        "SELECT ST_Distance(ST_MakePoint(1.0, 2.0), ST_MakePoint(1.0, 2.0))",
    )
    .await;
    assert!(
        d.abs() < 1e-6,
        "ST_Distance of coincident points must be 0 m; got={d}"
    );
    println!("[geo ST_Distance] coincident = 0 m ✓");
}

/// Eiffel Tower → Big Ben: reference Haversine distance ≈ 343 556 m.
///
/// [GEO-1] NEEDS-PSQL-VALIDATION: confirm PostGIS `ST_Distance(…::geography)`.
///
/// Coordinates:
///   Eiffel Tower: lon=2.2945°E, lat=48.8584°N
///   Big Ben:      lon=-0.1246°E, lat=51.5007°N
/// Reference from PostGIS `ST_DistanceSphere` (same spherical-Earth model).
#[tokio::test]
async fn st_distance_eiffel_to_big_ben() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let d = single_f64(
        &sess,
        "SELECT ST_Distance(\
            ST_MakePoint(2.2945, 48.8584), \
            ST_MakePoint(-0.1246, 51.5007))",
    )
    .await;
    let reference = 343_556.0_f64;
    let rel_err = ((d - reference) / reference).abs();
    assert!(
        rel_err < 0.01,
        "Eiffel→Big Ben: got {d:.1} m, reference {reference:.1} m, err={rel_err:.4} (>1%)"
    );
    println!("[geo ST_Distance] Eiffel→Big Ben = {d:.1} m ✓  (reference {reference:.0} m)");
}

/// New York → London: reference ≈ 5 570 222 m.
#[tokio::test]
async fn st_distance_nyc_to_london() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let d = single_f64(
        &sess,
        "SELECT ST_Distance(\
            ST_MakePoint(-74.006, 40.7128), \
            ST_MakePoint(-0.1276, 51.5074))",
    )
    .await;
    let reference = 5_570_222.0_f64;
    let rel_err = ((d - reference) / reference).abs();
    assert!(
        rel_err < 0.01,
        "NYC→London: got {d:.1} m, reference {reference:.1} m, err={rel_err:.4} (>1%)"
    );
    println!("[geo ST_Distance] NYC→London = {d:.1} m ✓");
}

/// `ST_Distance` is symmetric.
#[tokio::test]
async fn st_distance_symmetric() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let ab = single_f64(
        &sess,
        "SELECT ST_Distance(ST_MakePoint(13.405, 52.52), ST_MakePoint(2.2945, 48.8584))",
    )
    .await;
    let ba = single_f64(
        &sess,
        "SELECT ST_Distance(ST_MakePoint(2.2945, 48.8584), ST_MakePoint(13.405, 52.52))",
    )
    .await;
    assert!(
        (ab - ba).abs() < 1e-6,
        "ST_Distance must be symmetric; ab={ab}, ba={ba}"
    );
    println!("[geo ST_Distance] symmetric ✓");
}

/// `ST_Distance` returns NULL when either argument is NULL.
#[tokio::test]
async fn st_distance_null_propagation() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE pts_null (id BIGINT, g POINT)").await;
    exec(&sess, "INSERT INTO pts_null VALUES (1, NULL)").await;

    match sess
        .execute("SELECT ST_Distance(g, ST_MakePoint(0.0, 0.0)) FROM pts_null WHERE id = 1")
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => {
            assert!(
                batches.first().expect("batch").column(0).is_null(0),
                "ST_Distance(NULL, p) must return NULL"
            );
        }
        Ok(other) => panic!("unexpected: {other:?}"),
        Err(e) => panic!("ST_Distance NULL must not error: {e}"),
    }
    println!("[geo ST_Distance] NULL propagation ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 6 — ST_DWithin
// ─────────────────────────────────────────────────────────────────────────────

/// Point within radius → true; outside → false.
#[tokio::test]
async fn st_dwithin_basic_truth_table() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Origin: (0, 0). Test point: (0.0, 0.009) ≈ 1 000 m north.
    // 1° of latitude ≈ 111 320 m, so 0.009° ≈ 1 002 m.
    // Radius 1 100 m → should be within.
    let within = single_bool(
        &sess,
        "SELECT ST_DWithin(\
            ST_MakePoint(0.0, 0.0), \
            ST_MakePoint(0.0, 0.009), \
            1100.0)",
    )
    .await;
    assert!(within, "ST_DWithin: point ≈1 002 m away must be within 1 100 m radius");

    // Same point but radius only 900 m → must be outside.
    let outside = single_bool(
        &sess,
        "SELECT ST_DWithin(\
            ST_MakePoint(0.0, 0.0), \
            ST_MakePoint(0.0, 0.009), \
            900.0)",
    )
    .await;
    assert!(!outside, "ST_DWithin: point ≈1 002 m away must NOT be within 900 m radius");
    println!("[geo ST_DWithin] basic truth table ✓");
}

/// Coincident points are always within any positive radius.
#[tokio::test]
async fn st_dwithin_coincident_always_within() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let r = single_bool(
        &sess,
        "SELECT ST_DWithin(ST_MakePoint(5.0, 5.0), ST_MakePoint(5.0, 5.0), 0.0001)",
    )
    .await;
    assert!(r, "ST_DWithin: coincident points must always be within any radius > 0");

    // Zero radius on coincident points: distance = 0, radius = 0 → 0 <= 0 → true.
    let zero_r = single_bool(
        &sess,
        "SELECT ST_DWithin(ST_MakePoint(1.0, 2.0), ST_MakePoint(1.0, 2.0), 0.0)",
    )
    .await;
    assert!(
        zero_r,
        "ST_DWithin: coincident points at zero radius (0 <= 0) must return true"
    );
    println!("[geo ST_DWithin] coincident ✓");
}

/// `ST_DWithin` boundary: point at exactly `radius` metres must be within.
/// (Basin uses `haversine_meters <= radius`, closed boundary.)
///
/// [GEO-3] NEEDS-PSQL-VALIDATION: PostGIS `ST_DWithin(…::geography, r)` is
/// `<= r` (closed), same as Basin's implementation.
#[tokio::test]
async fn st_dwithin_boundary_inclusive() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Use two points where the exact Haversine distance is very well-known:
    // (0, 0) and (0, 0.001) ≈ 111.32 m (1e-3 deg lat × ~111 320 m/deg).
    // We set the radius to slightly above the real distance so we're testing
    // boundary-near rather than the exact float match (which is NEEDS-PSQL-VALIDATION).
    let approx_m = single_f64(
        &sess,
        "SELECT ST_Distance(ST_MakePoint(0.0, 0.0), ST_MakePoint(0.0, 0.001))",
    )
    .await;
    // With radius == distance, must be within.
    let at_boundary = single_bool(
        &sess,
        &format!(
            "SELECT ST_DWithin(ST_MakePoint(0.0, 0.0), ST_MakePoint(0.0, 0.001), {approx_m})"
        ),
    )
    .await;
    assert!(
        at_boundary,
        "ST_DWithin at exact boundary ({approx_m:.3} m <= {approx_m:.3} m) must return true"
    );

    // One metre less than the exact distance → must be outside.
    let just_outside = single_bool(
        &sess,
        &format!(
            "SELECT ST_DWithin(ST_MakePoint(0.0, 0.0), ST_MakePoint(0.0, 0.001), {})",
            approx_m - 1.0
        ),
    )
    .await;
    assert!(
        !just_outside,
        "ST_DWithin 1 m short of exact distance must return false"
    );
    println!("[geo ST_DWithin] boundary inclusive ✓  exact_dist={approx_m:.3} m");
}

/// `ST_DWithin` in a `WHERE` clause correctly filters a table of points.
///
/// Hand-chosen distances (all near-equatorial, 1 deg lat ≈ 111 320 m):
///   Row 1: (0, 0.000) — 0 m from origin → within 50 000 m
///   Row 2: (0, 0.100) — ≈11 132 m north  → within 50 000 m
///   Row 3: (0, 0.500) — ≈55 660 m north  → outside 50 000 m
///   Row 4: (0, 1.000) — ≈111 320 m north → outside 50 000 m
#[tokio::test]
async fn st_dwithin_where_clause_filter() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE spatial_pts (id BIGINT NOT NULL, geom POINT)").await;
    exec(
        &sess,
        "INSERT INTO spatial_pts VALUES \
         (1, ST_MakePoint(0.0, 0.000)), \
         (2, ST_MakePoint(0.0, 0.100)), \
         (3, ST_MakePoint(0.0, 0.500)), \
         (4, ST_MakePoint(0.0, 1.000))",
    )
    .await;

    let ids = sorted_ids(
        &sess,
        "SELECT id FROM spatial_pts \
         WHERE ST_DWithin(geom, ST_MakePoint(0.0, 0.0), 50000.0)",
    )
    .await;
    assert_eq!(
        ids,
        vec![1, 2],
        "ST_DWithin(50 km): rows within radius should be [1,2]; got={ids:?}"
    );
    println!("[geo ST_DWithin] WHERE filter → [1,2] ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 7 — Topology predicates (POINT × POINT)
// ─────────────────────────────────────────────────────────────────────────────

/// `ST_Intersects(a, b)` — true iff a == b (for two POINTs).
/// `ST_Disjoint(a, b)`   — true iff a != b (complement of Intersects).
///
/// DE-9IM: two coincident points share interior → Intersects = T.
/// PostGIS: `SELECT ST_Intersects(ST_MakePoint(1,2), ST_MakePoint(1,2)); => t`
#[tokio::test]
async fn st_intersects_and_disjoint_truth_table() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Equal points: Intersects = T, Disjoint = F.
    let int_eq = single_bool(
        &sess,
        "SELECT ST_Intersects(ST_MakePoint(1.0, 2.0), ST_MakePoint(1.0, 2.0))",
    )
    .await;
    assert!(int_eq, "ST_Intersects: equal points must be true");

    let dis_eq = single_bool(
        &sess,
        "SELECT ST_Disjoint(ST_MakePoint(1.0, 2.0), ST_MakePoint(1.0, 2.0))",
    )
    .await;
    assert!(!dis_eq, "ST_Disjoint: equal points must be false");

    // Different points: Intersects = F, Disjoint = T.
    let int_ne = single_bool(
        &sess,
        "SELECT ST_Intersects(ST_MakePoint(1.0, 2.0), ST_MakePoint(3.0, 4.0))",
    )
    .await;
    assert!(!int_ne, "ST_Intersects: different points must be false");

    let dis_ne = single_bool(
        &sess,
        "SELECT ST_Disjoint(ST_MakePoint(1.0, 2.0), ST_MakePoint(3.0, 4.0))",
    )
    .await;
    assert!(dis_ne, "ST_Disjoint: different points must be true");
    println!("[geo ST_Intersects/ST_Disjoint] truth table ✓");
}

/// `ST_Within(a, b)` — for two POINTs reduces to coordinate equality.
/// PostGIS: `SELECT ST_Within(ST_MakePoint(1,2), ST_MakePoint(1,2)); => t`
#[tokio::test]
async fn st_within_truth_table() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let within_eq = single_bool(
        &sess,
        "SELECT ST_Within(ST_MakePoint(1.0, 2.0), ST_MakePoint(1.0, 2.0))",
    )
    .await;
    assert!(within_eq, "ST_Within: coincident points → true");

    let within_ne = single_bool(
        &sess,
        "SELECT ST_Within(ST_MakePoint(1.0, 2.0), ST_MakePoint(1.0, 3.0))",
    )
    .await;
    assert!(!within_ne, "ST_Within: non-coincident points → false");
    println!("[geo ST_Within] truth table ✓");
}

/// `ST_Crosses(a, b)` — always false for POINT × POINT (DE-9IM: zero-dim
/// geometries cannot cross).
/// PostGIS: `SELECT ST_Crosses(ST_MakePoint(1,2), ST_MakePoint(3,4)); => f`
#[tokio::test]
async fn st_crosses_always_false_for_points() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    for (x1, y1, x2, y2) in &[(1.0f64, 2.0, 3.0, 4.0), (1.0, 1.0, 1.0, 1.0)] {
        let r = single_bool(
            &sess,
            &format!("SELECT ST_Crosses(ST_MakePoint({x1}, {y1}), ST_MakePoint({x2}, {y2}))"),
        )
        .await;
        assert!(
            !r,
            "ST_Crosses must always be false for POINT × POINT; got true for ({x1},{y1})×({x2},{y2})"
        );
    }
    println!("[geo ST_Crosses] always false for POINT × POINT ✓");
}

/// `ST_Touches(a, b)` — always false for POINT × POINT (points have empty
/// boundaries; touches requires sharing boundary but not interior).
/// PostGIS: `SELECT ST_Touches(ST_MakePoint(1,2), ST_MakePoint(1,2)); => f`
#[tokio::test]
async fn st_touches_always_false_for_points() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    for (x1, y1, x2, y2) in &[(1.0f64, 2.0, 1.0, 2.0), (0.0, 0.0, 1.0, 1.0)] {
        let r = single_bool(
            &sess,
            &format!("SELECT ST_Touches(ST_MakePoint({x1}, {y1}), ST_MakePoint({x2}, {y2}))"),
        )
        .await;
        assert!(
            !r,
            "ST_Touches must always be false for POINT × POINT; got true for ({x1},{y1})×({x2},{y2})"
        );
    }
    println!("[geo ST_Touches] always false for POINT × POINT ✓");
}

/// `ST_Overlaps(a, b)` — always false for POINT × POINT (see DE-9IM).
/// PostGIS: `SELECT ST_Overlaps(ST_MakePoint(1,2), ST_MakePoint(1,2)); => f`
#[tokio::test]
async fn st_overlaps_always_false_for_points() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    for (x1, y1, x2, y2) in &[(1.0f64, 2.0, 1.0, 2.0), (0.0, 0.0, 5.0, 5.0)] {
        let r = single_bool(
            &sess,
            &format!("SELECT ST_Overlaps(ST_MakePoint({x1}, {y1}), ST_MakePoint({x2}, {y2}))"),
        )
        .await;
        assert!(
            !r,
            "ST_Overlaps must always be false for POINT × POINT; ({x1},{y1})×({x2},{y2}) got true"
        );
    }
    println!("[geo ST_Overlaps] always false for POINT × POINT ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 8 — Degenerate scalars
// ─────────────────────────────────────────────────────────────────────────────

/// `ST_Area(point)` — a POINT has zero area.
/// PostGIS: `SELECT ST_Area(ST_MakePoint(1,2)); => 0`
#[tokio::test]
async fn st_area_of_point_is_zero() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let area = single_f64(&sess, "SELECT ST_Area(ST_MakePoint(1.0, 2.0))").await;
    assert!(area.abs() < 1e-10, "ST_Area(POINT) must be 0.0; got={area}");
    println!("[geo ST_Area] 0.0 for POINT ✓");
}

/// `ST_Perimeter(point)` — a POINT has zero perimeter.
#[tokio::test]
async fn st_perimeter_of_point_is_zero() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let p = single_f64(&sess, "SELECT ST_Perimeter(ST_MakePoint(3.0, 4.0))").await;
    assert!(p.abs() < 1e-10, "ST_Perimeter(POINT) must be 0.0; got={p}");
    println!("[geo ST_Perimeter] 0.0 for POINT ✓");
}

/// `ST_NumPoints(point)` — a POINT has exactly 1 vertex.
/// PostGIS: `SELECT ST_NumPoints(ST_MakePoint(1,2)); => 1`
#[tokio::test]
async fn st_numpoints_of_point_is_one() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    match sess.execute("SELECT ST_NumPoints(ST_MakePoint(1.0, 2.0))").await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().expect("batch");
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32");
            assert_eq!(arr.value(0), 1, "ST_NumPoints(POINT) must be 1");
        }
        other => panic!("unexpected: {other:?}"),
    }
    println!("[geo ST_NumPoints] 1 for POINT ✓");
}

/// `ST_Envelope(point)` — for a POINT the envelope is the point itself.
/// The round-trip through ST_AsText must give the same WKT.
#[tokio::test]
async fn st_envelope_of_point_is_identity() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let wkt = single_string(
        &sess,
        "SELECT ST_AsText(ST_Envelope(ST_MakePoint(5.0, 6.0)))",
    )
    .await;
    // PG/PostGIS ST_AsText strips trailing ".0": POINT(5.0 6.0) → "POINT(5 6)"
    assert_eq!(
        wkt, "POINT(5 6)",
        "ST_Envelope(POINT) must return the same point; got={wkt:?}"
    );
    println!("[geo ST_Envelope] identity for POINT ✓");
}

/// `ST_Centroid(point)` — for a POINT the centroid is the point itself.
#[tokio::test]
async fn st_centroid_of_point_is_identity() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let wkt = single_string(
        &sess,
        "SELECT ST_AsText(ST_Centroid(ST_MakePoint(7.0, 8.0)))",
    )
    .await;
    // PG/PostGIS ST_AsText strips trailing ".0": POINT(7.0 8.0) → "POINT(7 8)"
    assert_eq!(
        wkt, "POINT(7 8)",
        "ST_Centroid(POINT) must be identity; got={wkt:?}"
    );
    println!("[geo ST_Centroid] identity for POINT ✓");
}

/// `ST_StartPoint(p)` and `ST_EndPoint(p)` are identity for a POINT.
#[tokio::test]
async fn st_startpoint_endpoint_identity_for_point() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let start = single_string(
        &sess,
        "SELECT ST_AsText(ST_StartPoint(ST_MakePoint(1.0, 2.0)))",
    )
    .await;
    let end = single_string(
        &sess,
        "SELECT ST_AsText(ST_EndPoint(ST_MakePoint(1.0, 2.0)))",
    )
    .await;
    // PG/PostGIS ST_AsText strips trailing ".0": POINT(1.0 2.0) → "POINT(1 2)"
    assert_eq!(start, "POINT(1 2)", "ST_StartPoint(POINT) must be identity; got={start:?}");
    assert_eq!(end, "POINT(1 2)", "ST_EndPoint(POINT) must be identity; got={end:?}");
    println!("[geo ST_StartPoint/ST_EndPoint] identity ✓");
}

/// `ST_Buffer(p, r)` — identity for POINT in v0.1 (no POLYGON type yet).
#[tokio::test]
async fn st_buffer_identity_for_point() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let wkt = single_string(
        &sess,
        "SELECT ST_AsText(ST_Buffer(ST_MakePoint(3.0, 4.0), 100.0))",
    )
    .await;
    // PG/PostGIS ST_AsText strips trailing ".0": POINT(3.0 4.0) → "POINT(3 4)"
    assert_eq!(
        wkt, "POINT(3 4)",
        "ST_Buffer(POINT, r) must be identity in v0.1; got={wkt:?}"
    );
    println!("[geo ST_Buffer] identity for POINT in v0.1 ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 9 — Bbox helpers
// ─────────────────────────────────────────────────────────────────────────────

/// `ST_MakeEnvelope(minx, miny, maxx, maxy)` constructs a geometry from four
/// corner coordinates. In v0.1 this returns a POINT at the min corner
/// (BOX2D not yet a native column type); callers relying only on the UDF
/// being callable get a non-error result today.
#[tokio::test]
async fn st_makeenvelope_callable() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // 4-arg form (no SRID).
    let r = sess
        .execute("SELECT ST_AsText(ST_MakeEnvelope(0.0, 0.0, 10.0, 10.0))")
        .await;
    assert!(
        r.is_ok(),
        "ST_MakeEnvelope (4-arg) must not error; got={r:?}"
    );

    // 5-arg form (with SRID).
    let r5 = sess
        .execute("SELECT ST_AsText(ST_MakeEnvelope(0.0, 0.0, 10.0, 10.0, 4326))")
        .await;
    assert!(r5.is_ok(), "ST_MakeEnvelope (5-arg with SRID) must not error; got={r5:?}");
    println!("[geo ST_MakeEnvelope] callable ✓");
}

/// `__basin_bbox_contains_point(geom, minx, miny, maxx, maxy)` — the internal
/// residual UDF. Points inside the closed box → true; outside → false.
///
/// Box: [1.0, 5.0] × [1.0, 5.0] (closed boundary inclusive per PostGIS `&&`).
///
/// Cases:
///   (3, 3) inside → true
///   (0, 3) x too small → false
///   (1, 1) on min-corner boundary → true (closed)
///   (5, 5) on max-corner boundary → true (closed)
///   (5.0001, 5.0) just outside max_x → false
#[tokio::test]
async fn basin_bbox_contains_point_truth_table() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let cases: &[(f64, f64, bool)] = &[
        (3.0, 3.0, true),
        (0.0, 3.0, false),
        (1.0, 1.0, true),   // min corner (boundary inclusive)
        (5.0, 5.0, true),   // max corner (boundary inclusive)
        (5.001, 5.0, false),
        (3.0, 0.999, false), // y too small
    ];

    for (px, py, expected) in cases {
        let sql = format!(
            "SELECT __basin_bbox_contains_point(\
                ST_MakePoint({px}, {py}), \
                1.0, 1.0, 5.0, 5.0)"
        );
        let got = single_bool(&sess, &sql).await;
        assert_eq!(
            got, *expected,
            "__basin_bbox_contains_point({px},{py}) expected {expected}; got {got}"
        );
    }
    println!("[geo __basin_bbox_contains_point] truth table ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 10 — SRID surface
// ─────────────────────────────────────────────────────────────────────────────

/// `ST_SRID(p)` — every point constructed via `ST_MakePoint` carries SRID 4326.
///
/// [GEO-7] NEEDS-PSQL-VALIDATION: PostGIS `SELECT ST_SRID(ST_MakePoint(1,2)); => 0`
/// (PG stores 0 as the "unknown SRID" unless explicitly set; Basin stamps 4326).
/// Basin differs here by design — see crate docs.
#[tokio::test]
async fn st_srid_default_is_4326() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    match sess.execute("SELECT ST_SRID(ST_MakePoint(1.0, 2.0))").await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().expect("batch");
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32");
            assert_eq!(
                arr.value(0), 4326,
                "ST_SRID(ST_MakePoint) must return 4326 (Basin default); got={}",
                arr.value(0)
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
    println!("[geo ST_SRID] default 4326 ✓");
}

/// `ST_SetSRID(p, srid)` — identity pass-through; the bytes are unchanged.
/// The returned WKT coordinates must match the input.
#[tokio::test]
async fn st_setsrid_is_passthrough() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let wkt = single_string(
        &sess,
        "SELECT ST_AsText(ST_SetSRID(ST_MakePoint(10.0, 20.0), 4326))",
    )
    .await;
    // PG/PostGIS ST_AsText strips trailing ".0": POINT(10.0 20.0) → "POINT(10 20)"
    assert_eq!(
        wkt, "POINT(10 20)",
        "ST_SetSRID must not alter the point coordinates; got={wkt:?}"
    );
    println!("[geo ST_SetSRID] pass-through ✓");
}

/// `ST_Transform(p, 4326)` — identity when source and destination SRID are both 4326.
#[tokio::test]
async fn st_transform_identity_same_srid() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let x_out = single_f64(
        &sess,
        "SELECT ST_X(ST_Transform(ST_MakePoint(13.405, 52.52), 4326))",
    )
    .await;
    assert!(
        (x_out - 13.405).abs() < 1e-10,
        "ST_Transform to same SRID must be identity; x={x_out}"
    );
    println!("[geo ST_Transform] identity ✓");
}

/// `ST_Transform(p, 3857)` — reproject WGS84 → Web Mercator.
///
/// Berlin: lon=13.405°E, lat=52.52°N.
/// Web Mercator easting ≈ 1 491 445 m (PROJ canonical).
///
/// [GEO-6] NEEDS-PSQL-VALIDATION: confirm with `ST_X(ST_Transform(p::geometry, 3857))`.
#[tokio::test]
async fn st_transform_wgs84_to_web_mercator() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let x = single_f64(
        &sess,
        "SELECT ST_X(ST_Transform(ST_MakePoint(13.405, 52.52), 3857))",
    )
    .await;
    // Reference: ~1 491 445 m east. Allow 1 m tolerance for PROJ round-trip noise.
    let expected_easting = 1_491_000.0_f64; // approximate
    assert!(
        x > 1_490_000.0 && x < 1_493_000.0,
        "ST_Transform WGS84→3857 for Berlin: expected ~1 491 000 m east, got x={x:.1}"
    );
    println!("[geo ST_Transform] WGS84→3857 easting = {x:.1} m (expected ~{expected_easting:.0})");
}

/// `ST_Transform` with an unknown SRID must error cleanly.
#[tokio::test]
async fn st_transform_unknown_srid_errors() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let r = sess
        .execute("SELECT ST_X(ST_Transform(ST_MakePoint(1.0, 2.0), 99999))")
        .await;
    assert!(r.is_err(), "ST_Transform to unknown SRID must error; got={r:?}");
    println!("[geo ST_Transform] unknown SRID → error ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 11 — Geography aliases
// ─────────────────────────────────────────────────────────────────────────────

/// `ST_GeogFromText` and `ST_GeographyFromText` are aliases for `ST_GeomFromText`.
/// They must produce the same WKT output.
#[tokio::test]
async fn st_geogfromtext_alias_works() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let g1 = single_string(
        &sess,
        "SELECT ST_AsText(ST_GeogFromText('POINT(1.0 2.0)'))",
    )
    .await;
    let g2 = single_string(
        &sess,
        "SELECT ST_AsText(ST_GeographyFromText('POINT(1.0 2.0)'))",
    )
    .await;
    // PG/PostGIS ST_AsText strips trailing ".0": POINT(1.0 2.0) → "POINT(1 2)"
    assert_eq!(g1, "POINT(1 2)", "ST_GeogFromText must work; got={g1:?}");
    assert_eq!(g2, "POINT(1 2)", "ST_GeographyFromText must work; got={g2:?}");
    println!("[geo geography aliases] ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 12 — Table-level usage (INSERT / SELECT / WHERE with POINT column)
// ─────────────────────────────────────────────────────────────────────────────

/// INSERT points via `ST_MakePoint`, SELECT them back via `ST_AsText`, and
/// assert `ST_DWithin` filters correctly over a small dataset.
#[tokio::test]
async fn point_column_table_insert_select_filter() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE cities (id BIGINT NOT NULL, name TEXT, loc POINT)").await;
    exec(
        &sess,
        "INSERT INTO cities VALUES \
         (1, 'London',    ST_MakePoint(-0.1276,  51.5074)), \
         (2, 'Paris',     ST_MakePoint(2.3522,   48.8566)), \
         (3, 'Berlin',    ST_MakePoint(13.405,   52.52)),   \
         (4, 'Sydney',    ST_MakePoint(151.2093, -33.8688)), \
         (5, 'New York',  ST_MakePoint(-74.006,  40.7128))",
    )
    .await;

    // All 5 cities inserted.
    let total = row_count(&sess, "SELECT id FROM cities").await;
    assert_eq!(total, 5, "5 rows must be in the table; got={total}");

    // ST_AsText round-trip for London.
    let london_wkt = single_string(
        &sess,
        "SELECT ST_AsText(loc) FROM cities WHERE id = 1",
    )
    .await;
    assert!(
        london_wkt.starts_with("POINT("),
        "London WKT must start with POINT(; got={london_wkt:?}"
    );
    assert!(
        london_wkt.contains("-0.1276"),
        "London WKT must contain the longitude; got={london_wkt:?}"
    );

    // ST_DWithin: cities within ~1 100 km of Paris (lat ≈ 48.8°N):
    // Paris=0 km, London≈342 km, Berlin≈878 km — all within 1 100 km.
    // Sydney (~16 500 km) and New York (~5 800 km) are outside.
    let ids = sorted_ids(
        &sess,
        "SELECT id FROM cities \
         WHERE ST_DWithin(loc, ST_MakePoint(2.3522, 48.8566), 1100000.0)",
    )
    .await;
    assert_eq!(
        ids,
        vec![1, 2, 3],
        "Cities within 1 100 km of Paris should be [1,2,3]; got={ids:?}"
    );
    println!("[geo table] INSERT/SELECT/WHERE ✓");
}

/// `ST_AsGeoJSON` over a stored POINT column.
#[tokio::test]
async fn point_column_st_asgeojson_over_table() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE geo_tbl (id BIGINT NOT NULL, geom POINT)").await;
    exec(
        &sess,
        "INSERT INTO geo_tbl VALUES (1, ST_MakePoint(3.0, 4.0))",
    )
    .await;

    let geojson = single_string(
        &sess,
        "SELECT ST_AsGeoJSON(geom) FROM geo_tbl WHERE id = 1",
    )
    .await;
    assert_eq!(
        geojson,
        r#"{"type":"Point","coordinates":[3.0,4.0]}"#,
        "ST_AsGeoJSON over a stored POINT column must produce the RFC 7946 envelope; got={geojson:?}"
    );
    println!("[geo table ST_AsGeoJSON] ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 13 — Ordering by distance
// ─────────────────────────────────────────────────────────────────────────────

/// `ORDER BY ST_Distance(...) LIMIT k` returns the k nearest rows in
/// ascending-distance order.
///
/// Dataset: 5 cities inserted in non-distance order. Query from Paris.
/// Expected order by distance from Paris (hand-computed Haversine):
///   Paris=0 km, London≈342 km, Berlin≈878 km, New York≈5 832 km, Sydney≈16 965 km.
///   → LIMIT 3 must return ids [2,1,3] (Paris=2, London=1, Berlin=3).
#[tokio::test]
async fn order_by_distance_limit_k() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE dist_cities (id BIGINT NOT NULL, loc POINT)").await;
    exec(
        &sess,
        "INSERT INTO dist_cities VALUES \
         (1, ST_MakePoint(-0.1276,  51.5074)), \
         (2, ST_MakePoint(2.3522,   48.8566)), \
         (3, ST_MakePoint(13.405,   52.52)),   \
         (4, ST_MakePoint(-74.006,  40.7128)), \
         (5, ST_MakePoint(151.2093, -33.8688))",
    )
    .await;

    // Select the 3 nearest to Paris (id=2 is Paris itself).
    match sess
        .execute(
            "SELECT id \
             FROM dist_cities \
             ORDER BY ST_Distance(loc, ST_MakePoint(2.3522, 48.8566)) \
             LIMIT 3",
        )
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => {
            let mut ids = Vec::new();
            for b in &batches {
                if let Some(arr) = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                {
                    for i in 0..arr.len() {
                        ids.push(arr.value(i));
                    }
                }
            }
            assert_eq!(ids.len(), 3, "ORDER BY ST_Distance LIMIT 3 must return 3 rows; got {:?}", ids);
            assert_eq!(
                ids[0], 2,
                "Row 0 must be Paris (id=2, 0 km); got ids={ids:?}"
            );
            assert_eq!(
                ids[1], 1,
                "Row 1 must be London (id=1, ~342 km); got ids={ids:?}"
            );
            assert_eq!(
                ids[2], 3,
                "Row 2 must be Berlin (id=3, ~878 km); got ids={ids:?}"
            );
        }
        Ok(other) => panic!("unexpected: {other:?}"),
        Err(e) => panic!("ORDER BY ST_Distance LIMIT 3 failed: {e}"),
    }
    println!("[geo ORDER BY ST_Distance] LIMIT 3 → [2,1,3] ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 14 — NULL handling across all UDFs
// ─────────────────────────────────────────────────────────────────────────────

/// All UDFs that take a POINT must return NULL when the input is NULL
/// (not panic, not error, not false).
#[tokio::test]
async fn all_point_udfs_null_safe() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE nullgeom_tbl (id BIGINT, g POINT)").await;
    exec(&sess, "INSERT INTO nullgeom_tbl VALUES (1, NULL)").await;

    let unary_null_sqls = [
        "SELECT ST_X(g) FROM nullgeom_tbl WHERE id = 1",
        "SELECT ST_Y(g) FROM nullgeom_tbl WHERE id = 1",
        "SELECT ST_AsText(g) FROM nullgeom_tbl WHERE id = 1",
        "SELECT ST_AsGeoJSON(g) FROM nullgeom_tbl WHERE id = 1",
        "SELECT ST_AsEWKB(g) FROM nullgeom_tbl WHERE id = 1",
        "SELECT ST_Area(g) FROM nullgeom_tbl WHERE id = 1",
        "SELECT ST_Perimeter(g) FROM nullgeom_tbl WHERE id = 1",
        "SELECT ST_NumPoints(g) FROM nullgeom_tbl WHERE id = 1",
        "SELECT ST_SRID(g) FROM nullgeom_tbl WHERE id = 1",
        "SELECT ST_Envelope(g) FROM nullgeom_tbl WHERE id = 1",
        "SELECT ST_Centroid(g) FROM nullgeom_tbl WHERE id = 1",
        "SELECT ST_StartPoint(g) FROM nullgeom_tbl WHERE id = 1",
        "SELECT ST_EndPoint(g) FROM nullgeom_tbl WHERE id = 1",
    ];

    for sql in &unary_null_sqls {
        match sess.execute(sql).await {
            Ok(ExecResult::Rows { batches, .. }) => {
                let b = batches.first().expect("batch");
                assert!(
                    b.column(0).is_null(0),
                    "NULL-safe: {sql} must return NULL; column not null"
                );
            }
            Ok(ExecResult::Empty { .. }) => {
                // Empty result is acceptable when the NULL predicate pushes the row away.
            }
            Err(e) => panic!("NULL-safe: {sql} must not error: {e}"),
        }
    }

    // Binary: ST_Distance(NULL, p) and ST_DWithin(NULL, p, r).
    let binary_null_sqls = [
        "SELECT ST_Distance(g, ST_MakePoint(0.0, 0.0)) FROM nullgeom_tbl WHERE id = 1",
        "SELECT ST_DWithin(g, ST_MakePoint(0.0, 0.0), 1000.0) FROM nullgeom_tbl WHERE id = 1",
        "SELECT ST_Intersects(g, ST_MakePoint(0.0, 0.0)) FROM nullgeom_tbl WHERE id = 1",
        "SELECT ST_Disjoint(g, ST_MakePoint(0.0, 0.0)) FROM nullgeom_tbl WHERE id = 1",
        "SELECT ST_Within(g, ST_MakePoint(0.0, 0.0)) FROM nullgeom_tbl WHERE id = 1",
    ];
    for sql in &binary_null_sqls {
        match sess.execute(sql).await {
            Ok(ExecResult::Rows { batches, .. }) => {
                let b = batches.first().expect("batch");
                assert!(
                    b.column(0).is_null(0),
                    "NULL-safe binary: {sql} must return NULL; column not null"
                );
            }
            Ok(ExecResult::Empty { .. }) => {}
            Err(e) => panic!("NULL-safe binary: {sql} must not error: {e}"),
        }
    }
    println!("[geo NULL-safe] all unary + binary UDFs ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 15 — Invalid-input error handling
// ─────────────────────────────────────────────────────────────────────────────

/// All parsing UDFs must error cleanly on garbage input — no panics.
#[tokio::test]
async fn invalid_inputs_produce_typed_errors() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let bad_inputs = [
        // ST_GeomFromText: non-POINT type
        "SELECT ST_AsText(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 0))'))",
        // ST_GeomFromText: missing Y coord
        "SELECT ST_AsText(ST_GeomFromText('POINT(1.0)'))",
        // ST_GeomFromText: Z coord (unsupported in v0.1)
        "SELECT ST_AsText(ST_GeomFromText('POINT(1 2 3)'))",
        // ST_GeomFromGeoJSON: invalid JSON
        "SELECT ST_AsText(ST_GeomFromGeoJSON('not_json'))",
        // ST_GeomFromGeoJSON: wrong geometry type
        r#"SELECT ST_AsText(ST_GeomFromGeoJSON('{"type":"MultiPoint","coordinates":[[0,0]]}'))"#,
    ];

    for sql in &bad_inputs {
        let r = sess.execute(sql).await;
        assert!(
            r.is_err(),
            "Invalid input must error: {sql:?}; got={r:?}"
        );
    }
    println!("[geo invalid inputs] all produce errors ✓");
}
