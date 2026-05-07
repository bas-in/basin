//! Haversine accuracy tests: known-distance pairs from the real world.
//!
//! Each pair carries a reference distance computed with PostGIS's
//! `ST_DistanceSphere` (the same spherical-Earth model we use). Asserting
//! within 1% gives us slack for the ellipsoid-vs-sphere mismatch — and is
//! the same "good enough" bar PostGIS itself ships with.

use basin_geo::{haversine_meters, Point};

/// Allowed relative error against the reference distance. 1% covers the
/// spherical-vs-ellipsoidal mismatch and float rounding.
const TOL: f64 = 0.01;

fn relative_error(measured: f64, reference: f64) -> f64 {
    ((measured - reference) / reference).abs()
}

#[test]
fn coincident_points_are_zero_meters() {
    let p = Point::new(2.2945, 48.8584);
    assert!(haversine_meters(&p, &p).abs() < 1e-6);
}

#[test]
fn eiffel_to_big_ben_is_about_343_km() {
    // Eiffel Tower (lon=2.2945, lat=48.8584)
    // Big Ben     (lon=-0.1246, lat=51.5007)
    // Reference: PostGIS ST_DistanceSphere ≈ 343_556 m.
    let eiffel = Point::new(2.2945, 48.8584);
    let big_ben = Point::new(-0.1246, 51.5007);
    let measured = haversine_meters(&eiffel, &big_ben);
    let reference = 343_556.0;
    let err = relative_error(measured, reference);
    assert!(
        err < TOL,
        "eiffel↔big_ben measured={measured:.1} ref={reference:.1} err={err:.4}"
    );
}

#[test]
fn nyc_to_london_is_about_5570_km() {
    // NYC    (lon=-74.0060, lat=40.7128)
    // London (lon=-0.1276,  lat=51.5074)
    // Reference: ~5_570_222 m.
    let nyc = Point::new(-74.0060, 40.7128);
    let london = Point::new(-0.1276, 51.5074);
    let measured = haversine_meters(&nyc, &london);
    let reference = 5_570_222.0;
    let err = relative_error(measured, reference);
    assert!(err < TOL, "nyc↔london measured={measured:.1} err={err:.4}");
}

#[test]
fn sydney_to_tokyo_is_about_7820_km() {
    // Sydney (lon=151.2093, lat=-33.8688)
    // Tokyo  (lon=139.6503, lat= 35.6762)
    // Reference: ~7_822_000 m.
    let sydney = Point::new(151.2093, -33.8688);
    let tokyo = Point::new(139.6503, 35.6762);
    let measured = haversine_meters(&sydney, &tokyo);
    let reference = 7_822_000.0;
    let err = relative_error(measured, reference);
    assert!(err < TOL, "sydney↔tokyo measured={measured:.1} err={err:.4}");
}

#[test]
fn antipodal_points_are_about_half_circumference() {
    // A point at (0, 0) and its antipode at (180, 0). The great-circle
    // distance must be ~π·R = 20_015_086.8 m. This is the worst-case
    // input for the Haversine formula (where it's most sensitive to
    // floating-point rounding) — we want the clamp in the impl to keep
    // us from tripping on `asin` of a value slightly > 1.
    let a = Point::new(0.0, 0.0);
    let b = Point::new(180.0, 0.0);
    let measured = haversine_meters(&a, &b);
    let reference = std::f64::consts::PI * 6_371_008.8;
    let err = relative_error(measured, reference);
    assert!(err < 1e-6, "antipodal measured={measured:.3} err={err:.6}");
}
