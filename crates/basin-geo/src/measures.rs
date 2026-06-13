//! Geometric measures and predicates over [`geo_types::Geometry`].
//!
//! Two metric regimes, matching PostGIS's GEOMETRY vs GEOGRAPHY split:
//!
//! - **Planar** (`*_planar`): Cartesian math in the coordinate units
//!   (degrees, for WGS84 lon/lat). This is what PostGIS `ST_Length` /
//!   `ST_Area` return for a plain `geometry` value — fast, unit-naïve.
//! - **Geographic** (`*_geographic`): WGS84-correct great-circle length and
//!   spherical-excess area in metres / m², the same spherical model the
//!   POINT-only path already uses (see [`crate::haversine_meters`] and
//!   [`crate::polygon_area_m2`]). This is what PostGIS returns for a
//!   `geography` value or a `::geography`-cast `geometry`.
//!
//! Centroid and bounding rect come straight from the `geo` crate's
//! `Centroid` / `BoundingRect` traits — no hand-rolled algorithm.
//! Point-in-polygon / intersection predicates use `geo`'s `Contains`,
//! `Intersects`, and `EuclideanDistance` so the topology results are exact
//! (planar) rather than bbox-approximate; the honesty note for each lives
//! at the call site in `geo_glue`.

use geo::geometry::{Coord, Geometry, LineString, Point, Polygon};
use geo::{BoundingRect, Centroid, Contains, Intersects};

use crate::{haversine_meters, polygon_area_m2, Point as BPoint};

// ─────────────────────────────────────────────────────────────────────────────
// Length / perimeter
// ─────────────────────────────────────────────────────────────────────────────

/// Planar length (perimeter for areal types) in coordinate units.
/// Matches PostGIS `ST_Length` (lines) / `ST_Perimeter` (areas) for a
/// plain `geometry`.
pub fn length_planar(g: &Geometry<f64>) -> f64 {
    match g {
        Geometry::Line(l) => seg_len_planar(&l.start, &l.end),
        Geometry::LineString(ls) => linestring_len_planar(ls),
        Geometry::MultiLineString(ml) => ml.0.iter().map(linestring_len_planar).sum(),
        Geometry::Polygon(p) => polygon_perimeter_planar(p),
        Geometry::MultiPolygon(mp) => mp.0.iter().map(polygon_perimeter_planar).sum(),
        Geometry::GeometryCollection(gc) => gc.0.iter().map(length_planar).sum(),
        // Points / multipoints have zero length.
        _ => 0.0,
    }
}

/// WGS84 great-circle length (perimeter) in metres. Matches PostGIS
/// `ST_Length(geography)` / `ST_Perimeter(geography)`.
pub fn length_geographic(g: &Geometry<f64>) -> f64 {
    match g {
        Geometry::Line(l) => haversine_meters(&bp(&l.start), &bp(&l.end)),
        Geometry::LineString(ls) => linestring_len_geo(ls),
        Geometry::MultiLineString(ml) => ml.0.iter().map(linestring_len_geo).sum(),
        Geometry::Polygon(p) => polygon_perimeter_geo(p),
        Geometry::MultiPolygon(mp) => mp.0.iter().map(polygon_perimeter_geo).sum(),
        Geometry::GeometryCollection(gc) => gc.0.iter().map(length_geographic).sum(),
        _ => 0.0,
    }
}

/// PostGIS `ST_Perimeter`: 0 for non-areal, perimeter for polygons.
pub fn perimeter_planar(g: &Geometry<f64>) -> f64 {
    match g {
        Geometry::Polygon(_) | Geometry::MultiPolygon(_) | Geometry::Rect(_) | Geometry::Triangle(_) => {
            length_planar(g)
        }
        Geometry::GeometryCollection(gc) => gc.0.iter().map(perimeter_planar).sum(),
        _ => 0.0,
    }
}

pub fn perimeter_geographic(g: &Geometry<f64>) -> f64 {
    match g {
        Geometry::Polygon(_) | Geometry::MultiPolygon(_) | Geometry::Rect(_) | Geometry::Triangle(_) => {
            length_geographic(g)
        }
        Geometry::GeometryCollection(gc) => gc.0.iter().map(perimeter_geographic).sum(),
        _ => 0.0,
    }
}

fn seg_len_planar(a: &Coord<f64>, b: &Coord<f64>) -> f64 {
    let dx = b.x - a.x;
    let dy = b.y - a.y;
    (dx * dx + dy * dy).sqrt()
}

fn linestring_len_planar(ls: &LineString<f64>) -> f64 {
    ls.0.windows(2).map(|w| seg_len_planar(&w[0], &w[1])).sum()
}

fn linestring_len_geo(ls: &LineString<f64>) -> f64 {
    ls.0
        .windows(2)
        .map(|w| haversine_meters(&bp(&w[0]), &bp(&w[1])))
        .sum()
}

fn polygon_perimeter_planar(p: &Polygon<f64>) -> f64 {
    let mut total = linestring_len_planar(p.exterior());
    for h in p.interiors() {
        total += linestring_len_planar(h);
    }
    total
}

fn polygon_perimeter_geo(p: &Polygon<f64>) -> f64 {
    let mut total = linestring_len_geo(p.exterior());
    for h in p.interiors() {
        total += linestring_len_geo(h);
    }
    total
}

// ─────────────────────────────────────────────────────────────────────────────
// Area
// ─────────────────────────────────────────────────────────────────────────────

/// Planar (shoelace) area in coordinate units². Matches PostGIS
/// `ST_Area(geometry)`. Holes are subtracted; orientation-agnostic.
pub fn area_planar(g: &Geometry<f64>) -> f64 {
    match g {
        Geometry::Polygon(p) => polygon_area_planar(p),
        Geometry::MultiPolygon(mp) => mp.0.iter().map(polygon_area_planar).sum(),
        Geometry::Rect(r) => polygon_area_planar(&r.to_polygon()),
        Geometry::Triangle(t) => polygon_area_planar(&t.to_polygon()),
        Geometry::GeometryCollection(gc) => gc.0.iter().map(area_planar).sum(),
        _ => 0.0,
    }
}

/// WGS84 spherical-excess area in m². Matches PostGIS `ST_Area(geography)`.
/// Reuses [`crate::polygon_area_m2`] via a basin-geo `Polygon` bridge.
pub fn area_geographic(g: &Geometry<f64>) -> f64 {
    match g {
        Geometry::Polygon(p) => polygon_area_geo(p),
        Geometry::MultiPolygon(mp) => mp.0.iter().map(polygon_area_geo).sum(),
        Geometry::Rect(r) => polygon_area_geo(&r.to_polygon()),
        Geometry::Triangle(t) => polygon_area_geo(&t.to_polygon()),
        Geometry::GeometryCollection(gc) => gc.0.iter().map(area_geographic).sum(),
        _ => 0.0,
    }
}

fn ring_area_planar(ring: &LineString<f64>) -> f64 {
    // Shoelace formula; abs() so winding doesn't matter.
    let pts = &ring.0;
    if pts.len() < 3 {
        return 0.0;
    }
    let mut sum: f64 = 0.0;
    for i in 0..pts.len() {
        let a = pts[i];
        let b = pts[(i + 1) % pts.len()];
        sum += a.x * b.y - b.x * a.y;
    }
    (sum / 2.0).abs()
}

fn polygon_area_planar(p: &Polygon<f64>) -> f64 {
    let outer = ring_area_planar(p.exterior());
    let inner: f64 = p.interiors().iter().map(ring_area_planar).sum();
    (outer - inner).max(0.0)
}

fn polygon_area_geo(p: &Polygon<f64>) -> f64 {
    // Bridge to basin-geo Polygon (which carries the validated spherical
    // area math). Rings must be closed; geo polygons are closed by
    // construction (geo auto-closes exteriors).
    let to_b = |ls: &LineString<f64>| -> crate::LineString {
        let pts: Vec<BPoint> = ls.0.iter().map(|c| BPoint::new(c.x, c.y)).collect();
        // basin-geo LineString::new requires >=2 points; rings have >=4.
        crate::LineString { points: pts }
    };
    let bpoly = crate::Polygon {
        exterior: to_b(p.exterior()),
        holes: p.interiors().iter().map(to_b).collect(),
    };
    polygon_area_m2(&bpoly)
}

// ─────────────────────────────────────────────────────────────────────────────
// Centroid / envelope (planar, via geo crate)
// ─────────────────────────────────────────────────────────────────────────────

/// Centroid as a `geo` Point, or `None` for empty geometries. Delegates to
/// the `geo` crate's `Centroid` trait (area-weighted for polygons).
pub fn centroid(g: &Geometry<f64>) -> Option<Point<f64>> {
    g.centroid()
}

/// Axis-aligned bounding box as `(min_x, min_y, max_x, max_y)`, or `None`
/// for empty geometries. Delegates to `geo`'s `BoundingRect`.
pub fn envelope(g: &Geometry<f64>) -> Option<(f64, f64, f64, f64)> {
    let rect: Option<geo::geometry::Rect<f64>> = g.bounding_rect().into();
    rect.map(|r| (r.min().x, r.min().y, r.max().x, r.max().y))
}

// ─────────────────────────────────────────────────────────────────────────────
// Predicates (exact, planar — via geo crate)
// ─────────────────────────────────────────────────────────────────────────────

/// Exact planar `ST_Intersects` via `geo`'s `Intersects` trait.
pub fn intersects(a: &Geometry<f64>, b: &Geometry<f64>) -> bool {
    a.intersects(b)
}

/// Exact planar `ST_Contains` via `geo`'s `Contains` trait. `geo`'s
/// `Contains` is interior-based (boundary-exclusive), matching PostGIS
/// `ST_Contains` for the strict-interior case.
pub fn contains(a: &Geometry<f64>, b: &Geometry<f64>) -> bool {
    a.contains(b)
}

/// Exact planar `ST_Within(a, b)` = `ST_Contains(b, a)`.
pub fn within(a: &Geometry<f64>, b: &Geometry<f64>) -> bool {
    b.contains(a)
}

#[inline]
fn bp(c: &Coord<f64>) -> BPoint {
    BPoint::new(c.x, c.y)
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::geometry::{LineString as GLs, MultiPolygon, Point as GP, Polygon as GPoly};

    fn unit_square() -> Geometry<f64> {
        Geometry::Polygon(GPoly::new(
            GLs::new(vec![
                Coord { x: 0.0, y: 0.0 },
                Coord { x: 1.0, y: 0.0 },
                Coord { x: 1.0, y: 1.0 },
                Coord { x: 0.0, y: 1.0 },
                Coord { x: 0.0, y: 0.0 },
            ]),
            vec![],
        ))
    }

    #[test]
    fn unit_square_area_is_one() {
        assert!((area_planar(&unit_square()) - 1.0).abs() < 1e-12);
    }

    #[test]
    fn triangle_centroid() {
        // Triangle (0,0),(3,0),(0,3): centroid at (1,1).
        let tri = Geometry::Polygon(GPoly::new(
            GLs::new(vec![
                Coord { x: 0.0, y: 0.0 },
                Coord { x: 3.0, y: 0.0 },
                Coord { x: 0.0, y: 3.0 },
                Coord { x: 0.0, y: 0.0 },
            ]),
            vec![],
        ));
        let c = centroid(&tri).unwrap();
        assert!((c.x() - 1.0).abs() < 1e-9, "cx={}", c.x());
        assert!((c.y() - 1.0).abs() < 1e-9, "cy={}", c.y());
    }

    #[test]
    fn linestring_length_planar_known() {
        // (0,0)->(3,4) is a 3-4-5 triangle hypotenuse = 5.
        let ls = Geometry::LineString(GLs::new(vec![
            Coord { x: 0.0, y: 0.0 },
            Coord { x: 3.0, y: 4.0 },
        ]));
        assert!((length_planar(&ls) - 5.0).abs() < 1e-12);
    }

    #[test]
    fn envelope_of_square() {
        let (minx, miny, maxx, maxy) = envelope(&unit_square()).unwrap();
        assert_eq!((minx, miny, maxx, maxy), (0.0, 0.0, 1.0, 1.0));
    }

    #[test]
    fn point_in_polygon_contains() {
        let sq = unit_square();
        let inside = Geometry::Point(GP::new(0.5, 0.5));
        let outside = Geometry::Point(GP::new(2.0, 2.0));
        assert!(contains(&sq, &inside));
        assert!(!contains(&sq, &outside));
        assert!(within(&inside, &sq));
    }

    #[test]
    fn multipolygon_area_sums() {
        let sq = match unit_square() {
            Geometry::Polygon(p) => p,
            _ => unreachable!(),
        };
        let shifted = GPoly::new(
            GLs::new(vec![
                Coord { x: 10.0, y: 10.0 },
                Coord { x: 11.0, y: 10.0 },
                Coord { x: 11.0, y: 11.0 },
                Coord { x: 10.0, y: 11.0 },
                Coord { x: 10.0, y: 10.0 },
            ]),
            vec![],
        );
        let mp = Geometry::MultiPolygon(MultiPolygon(vec![sq, shifted]));
        assert!((area_planar(&mp) - 2.0).abs() < 1e-12);
    }

    #[test]
    fn geographic_area_positive_for_real_polygon() {
        // ~1 degree square near equator; spherical area should be a large
        // positive m² value (order 1.2e10 m²).
        let sq = Geometry::Polygon(GPoly::new(
            GLs::new(vec![
                Coord { x: 0.0, y: 0.0 },
                Coord { x: 1.0, y: 0.0 },
                Coord { x: 1.0, y: 1.0 },
                Coord { x: 0.0, y: 1.0 },
                Coord { x: 0.0, y: 0.0 },
            ]),
            vec![],
        ));
        let a = area_geographic(&sq);
        assert!(a > 1.0e10 && a < 1.5e10, "geo area = {a}");
    }
}
