//! `ST_DWithin` semantics: positive cases (in-radius), negative cases
//! (out-of-radius), and the boundary (radius = exact distance).

use basin_geo::{dwithin, Point};

#[test]
fn close_points_are_within_one_kilometer() {
    // Two points ~50 m apart in central Paris.
    let a = Point::new(2.2945, 48.8584); // Eiffel
    let b = Point::new(2.2950, 48.8585); // ~37 m east of Eiffel
    assert!(dwithin(&a, &b, 1_000.0));
}

#[test]
fn far_points_are_not_within_one_kilometer() {
    // Eiffel (Paris) and Big Ben (London) are ~343 km apart — comfortably
    // outside a 1 km query.
    let eiffel = Point::new(2.2945, 48.8584);
    let big_ben = Point::new(-0.1246, 51.5007);
    assert!(!dwithin(&eiffel, &big_ben, 1_000.0));
}

#[test]
fn within_500km_includes_eiffel_to_big_ben() {
    let eiffel = Point::new(2.2945, 48.8584);
    let big_ben = Point::new(-0.1246, 51.5007);
    assert!(dwithin(&eiffel, &big_ben, 500_000.0));
}

#[test]
fn boundary_radius_is_inclusive() {
    // Queue up a deterministic distance, then ask for a radius equal to
    // it. v0.1 uses `<=` (closed boundary), matching PostGIS.
    let a = Point::new(0.0, 0.0);
    let b = Point::new(1.0, 1.0);
    let d = basin_geo::haversine_meters(&a, &b);
    assert!(dwithin(&a, &b, d));
    assert!(dwithin(&a, &b, d + 1.0));
    assert!(!dwithin(&a, &b, d - 1.0));
}

#[test]
fn coincident_points_are_within_zero_meters() {
    let p = Point::new(2.2945, 48.8584);
    assert!(dwithin(&p, &p, 0.0));
}
