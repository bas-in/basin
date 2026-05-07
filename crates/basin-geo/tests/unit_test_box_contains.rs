//! `ST_Contains(BOX2D, POINT)` — AABB containment with closed boundary.

use basin_geo::{contains_box, Box2d, Point};

#[test]
fn point_strictly_inside_is_contained() {
    // Bounding box around western Europe.
    let bbox = Box2d::new(-10.0, 35.0, 20.0, 60.0);
    let paris = Point::new(2.35, 48.86);
    assert!(contains_box(&bbox, &paris));
}

#[test]
fn point_outside_is_not_contained() {
    let bbox = Box2d::new(-10.0, 35.0, 20.0, 60.0);
    let nyc = Point::new(-74.0, 40.7);
    assert!(!contains_box(&bbox, &nyc));
}

#[test]
fn point_exactly_on_corner_is_contained() {
    // Closed boundary: point equal to (min_x, min_y) is contained.
    let bbox = Box2d::new(0.0, 0.0, 10.0, 10.0);
    assert!(contains_box(&bbox, &Point::new(0.0, 0.0)));
    assert!(contains_box(&bbox, &Point::new(10.0, 10.0)));
    assert!(contains_box(&bbox, &Point::new(0.0, 10.0)));
    assert!(contains_box(&bbox, &Point::new(10.0, 0.0)));
}

#[test]
fn point_just_outside_corner_is_not_contained() {
    let bbox = Box2d::new(0.0, 0.0, 10.0, 10.0);
    assert!(!contains_box(&bbox, &Point::new(-0.001, 0.0)));
    assert!(!contains_box(&bbox, &Point::new(10.001, 5.0)));
}

#[test]
fn box2d_normalises_swapped_corners() {
    // Pass the corners in any order — the constructor sorts them so the
    // resulting box still has min ≤ max on every axis.
    let bbox = Box2d::new(20.0, 60.0, -10.0, 35.0);
    assert_eq!(bbox.min_x, -10.0);
    assert_eq!(bbox.max_x, 20.0);
    assert_eq!(bbox.min_y, 35.0);
    assert_eq!(bbox.max_y, 60.0);
    let paris = Point::new(2.35, 48.86);
    assert!(contains_box(&bbox, &paris));
}
