//! Integration test: end-to-end `ST_Transform` from WGS84 (EPSG:4326)
//! to UTM zone 31N (EPSG:32631) through the engine's SQL surface.
//!
//! Card: `it_geo_st_transform_wgs84_utm31n`
//!
//! Validates:
//! 1. **DDL parses `GEOMETRY(POINT, 4326)`** — the `BASIN_SRID` field
//!    metadata gets stamped on the column.
//! 2. **`ST_Transform` resolves the source SRID from column metadata** —
//!    no explicit source-SRID argument needed at the SQL site.
//! 3. **`proj4rs`'s UTM31N output matches the PROJ reference** for the
//!    Eiffel Tower (2.2945°E, 48.8584°N) → (448 252 m E, 5 411 955 m N).
//!
//! ## Why this matters
//!
//! `ST_Transform` is the most-requested PostGIS function basin didn't
//! ship pre-PG-Wave 5 — every PostGIS client emits it to project tile
//! coordinates between WGS84 and the local working CRS. Without it
//! basin was unusable for any real GIS workload.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

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

#[tokio::test]
async fn st_transform_wgs84_to_utm31n_end_to_end() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // -- 1. DDL: GEOMETRY(POINT, 4326) — engine must accept the
    //    parameterised geometry type and tag the column with BASIN_SRID
    //    metadata under the hood.
    sess.execute(
        "CREATE TABLE locs (id BIGINT, p GEOMETRY(POINT, 4326)) \
         WITH (basin.file_format='parquet')",
    )
    .await
    .expect("CREATE TABLE GEOMETRY(POINT, 4326) should parse");

    // -- 2. INSERT a WGS84 point (Eiffel Tower). ST_MakePoint produces
    //    a 21-byte WKB blob; the destination column's SRID metadata
    //    tags it as 4326 at the storage layer.
    sess.execute("INSERT INTO locs VALUES (1, ST_MakePoint(2.2945, 48.8584))")
        .await
        .expect("INSERT should succeed");

    // -- 3. ST_Transform to UTM zone 31N (EPSG:32631). The source SRID
    //    is resolved from the column's BASIN_SRID metadata (no second
    //    SRID arg required at the SQL site, unlike PostGIS's 3-arg
    //    overload).
    let result = sess
        .execute(
            "SELECT ST_X(ST_Transform(p, 32631)) AS easting, \
                    ST_Y(ST_Transform(p, 32631)) AS northing \
             FROM locs",
        )
        .await
        .expect("ST_Transform should succeed");

    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };
    assert!(!batches.is_empty(), "ST_Transform returned no batches");

    let batch = &batches[0];
    let easting = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .expect("easting column should be Float64")
        .value(0);
    let northing = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .expect("northing column should be Float64")
        .value(0);

    println!(
        "[ST_Transform SQL] Eiffel WGS84(2.2945, 48.8584) → UTM31N \
         easting={easting:.3} northing={northing:.3}"
    );

    // PROJ reference for (2.2945°E, 48.8584°N) → EPSG:32631:
    // easting  ≈ 448 252.001 m, northing ≈ 5 411 954.910 m.
    // 1-meter tolerance is well under PostGIS's own ~mm-level
    // accuracy here; we use 1 m as a defensive bound in case the
    // upstream proj4rs picks a slightly different parametrisation in
    // a future release.
    let expected_easting = 448_252.001_f64;
    let expected_northing = 5_411_954.910_f64;
    assert!(
        (easting - expected_easting).abs() < 1.0,
        "easting {easting:.3} too far from expected {expected_easting:.3}"
    );
    assert!(
        (northing - expected_northing).abs() < 1.0,
        "northing {northing:.3} too far from expected {expected_northing:.3}"
    );
}
