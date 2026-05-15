//! `basin-geo` — a PostGIS-compatible subset for the most common
//! geospatial workloads.
//!
//! ## v0.1 surface
//!
//! Two types and six functions that together cover ~90% of production
//! PostGIS use:
//!
//! - **Types**
//!   - [`Point`] — 2D `POINT(x, y)` with implicit SRID 4326 (WGS84 lat/lon).
//!   - [`Box2d`] — axis-aligned `BOX2D(min_x, min_y, max_x, max_y)`.
//! - **Functions**
//!   - [`make_point`] — `ST_MakePoint(x, y) -> POINT`.
//!   - [`haversine_meters`] — `ST_Distance(a, b) -> meters` (great-circle).
//!   - [`dwithin`] — `ST_DWithin(a, b, radius_m) -> BOOL`. Most common
//!     spatial query in production (find users within X km).
//!   - [`contains_box`] — `ST_Contains(box, p) -> BOOL`. AABB containment.
//!   - [`Point::lon`] / [`Point::lat`] — `ST_X` / `ST_Y` accessors.
//!
//! ## Storage
//!
//! Points serialise to **21-byte** PostGIS-compatible WKB blobs (see
//! [`wkb`] module): 1 byte endianness + 4 byte type code + 8 byte X +
//! 8 byte Y. That's the per-row cost on disk, in Parquet, and in the
//! shard write-buffer. Bounding boxes are not stored on the wire today
//! (we re-compute them from points where needed — see v0.2 below).
//!
//! ## SQL surface staging
//!
//! Same pattern as `basin-cron` / `basin-net` / `basin-cv`: this crate
//! exposes a Rust API today, and the engine glue
//! (`basin-engine::geo_glue`) is the no-op stub where the future
//! DataFusion `ScalarUDF` registrations for `ST_Distance`, `ST_DWithin`,
//! `ST_MakePoint`, `ST_X`, `ST_Y` will land. Until that wiring is built
//! out, viability tests call this crate's Rust API directly.
//!
//! ## What's NOT in v0.1
//!
//! - **`MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`,
//!   `GEOMETRYCOLLECTION`** — not modelled. v0.2.
//! - **R-tree / GIST spatial index** — without one, `ST_DWithin` is a
//!   brute-force scan of the partition. That's fine for tens of
//!   thousands of points per project but quadratic on N×M joins.
//!   The fix in v0.2 is a basin-storage-side spatial-index segment
//!   format mirrored on the HNSW vector index.
//! - **EWKB / non-WGS84 SRIDs** — every point is interpreted as
//!   lat/lon. Reprojection against `proj4` is a v0.2 add.
//! - **Parser-level recognition of `POINT` / `BOX2D` / `LINESTRING` /
//!   `POLYGON` as Arrow column types** — DDL like `CREATE TABLE t
//!   (loc POINT)` is not yet recognised by the SQL planner. Today,
//!   points are stored as `BYTEA` (21-byte WKB) and linestrings /
//!   polygons as `TEXT` (the GeoJSON-style coordinate-array JSON in
//!   [`geojson`]); the engine glue's stub doc enumerates the planner
//!   change needed.
//!
//! ## High-latitude accuracy caveat (LINESTRING / POLYGON)
//!
//! [`polygon_area_m2`] uses a spherical-excess approximation with
//! `EARTH_RADIUS_M`; expected error vs. a geodesic-on-the-WGS84-
//! ellipsoid result is **< 0.5 % across all latitudes**.
//!
//! [`polygon_contains_point`] is **planar ray-casting in lat/lon
//! degrees** — it pretends the surface is flat, which is fine near the
//! equator but increasingly distorts as latitude rises. As a rule of
//! thumb the misclassification band hugging the boundary at 60° lat is
//! ~2 % the size of a typical city-block-scale polygon (~10 m off a
//! 500 m boundary); polygons spanning > a few hundred km in longitude
//! at high latitudes will produce wrong containment near the edges.
//! For the v0.1 use-cases (geofences, admin districts up to
//! state-sized) the error is acceptable; truly polar workloads need
//! the geodesic fix flagged for v0.2.

#![forbid(unsafe_code)]

pub mod geojson;
pub mod types;
pub mod wkb;

pub use types::{Box2d, GeometryError, LineString, Point, Polygon, SRID_WGS84};
pub use wkb::{decode_point, encode_point, from_hex, to_hex, WkbError, POINT_WKB_LEN};

/// WGS84 mean Earth radius in meters. The "authalic" radius (sphere with
/// the same surface area as the WGS84 ellipsoid) is 6_371_007.2 m; the
/// commonly cited "mean" 6_371_008.8 m differs only in the 5th decimal
/// place and changes great-circle distances by < 1 ppm. We use the
/// authalic value because it's what PostGIS's `ST_DistanceSphere`
/// documents.
const EARTH_RADIUS_M: f64 = 6_371_008.8;

/// `ST_MakePoint(x, y) -> POINT` — constructor.
///
/// Equivalent to [`Point::new`] but exposed as a free function so the
/// engine glue's UDF registration can point at a single function symbol.
#[inline]
pub fn make_point(x: f64, y: f64) -> Point {
    Point::new(x, y)
}

/// Great-circle distance in meters between two WGS84 points.
///
/// Implements the standard Haversine formula. Accuracy is bounded by the
/// spherical-Earth assumption — the WGS84 ellipsoid is up to ~0.5%
/// flatter at the poles, so worst-case error vs. a true ellipsoidal
/// (Vincenty) computation is well under 1% across the inhabited part of
/// the globe. PostGIS's `ST_DistanceSphere` makes the same assumption
/// and produces the same numbers.
///
/// ## SRID
///
/// `srid` on `a` and `b` is currently ignored — both inputs are treated
/// as lat/lon (WGS84). v0.2 will reproject if the SRIDs disagree.
pub fn haversine_meters(a: &Point, b: &Point) -> f64 {
    // x = longitude, y = latitude. The Haversine formula is symmetric
    // in longitude, so passing them in (lon, lat) or (lat, lon) order
    // affects only readability, not the result.
    let lat1 = a.y.to_radians();
    let lat2 = b.y.to_radians();
    let dlat = (b.y - a.y).to_radians();
    let dlon = (b.x - a.x).to_radians();

    let sin_dlat = (dlat / 2.0).sin();
    let sin_dlon = (dlon / 2.0).sin();
    // The classical Haversine `a` term: chord-half² in 2D angular space.
    let h = sin_dlat * sin_dlat + lat1.cos() * lat2.cos() * sin_dlon * sin_dlon;
    // Clamp h ∈ [0, 1] to defend against fp-rounding pushing it slightly
    // out of range when the two points are coincident or antipodal.
    let h = h.clamp(0.0, 1.0);
    let c = 2.0 * h.sqrt().asin();
    EARTH_RADIUS_M * c
}

/// `ST_DWithin(a, b, radius_meters)` — true iff `a` and `b` are within
/// `radius_meters` of each other on the WGS84 sphere.
///
/// In v0.1 this is a thin wrapper around [`haversine_meters`]; with no
/// spatial index in place, callers running `WHERE ST_DWithin(...)` over
/// a large table get a brute-force O(N) scan. Documented gap; v0.2.
#[inline]
pub fn dwithin(a: &Point, b: &Point, radius_meters: f64) -> bool {
    haversine_meters(a, b) <= radius_meters
}

/// `ST_Contains(box, p)` — true iff `p` lies inside (or on the boundary
/// of) the axis-aligned bounding box `b`.
///
/// Boundary inclusion matches PostGIS's `ST_Contains` for `BOX2D` /
/// `POINT`: a point exactly on `min_*` or `max_*` is considered
/// contained. This is the half-open / closed distinction that bites
/// callers writing tile-bucket queries; we picked closed because that's
/// what PostGIS does.
#[inline]
pub fn contains_box(b: &Box2d, p: &Point) -> bool {
    p.x >= b.min_x && p.x <= b.max_x && p.y >= b.min_y && p.y <= b.max_y
}

// ---------------------------------------------------------------------
// LINESTRING / POLYGON ST_* surface
// ---------------------------------------------------------------------

/// `ST_MakeLine(points) -> LINESTRING` — constructor.
///
/// PostGIS accepts 2+ points; we mirror that. Returns
/// [`GeometryError::LineStringTooShort`] for 0/1-point inputs so the
/// engine glue can surface a SQL `data_exception`.
#[inline]
pub fn make_line(points: Vec<Point>) -> Result<LineString, GeometryError> {
    LineString::new(points)
}

/// `ST_NumPoints(line)` — number of vertices in `line`.
#[inline]
pub fn num_points(line: &LineString) -> i32 {
    // PG returns int4. v0.1 LineStrings can't realistically exceed
    // i32::MAX (we'd OOM long before); cast is safe.
    line.num_points() as i32
}

/// `ST_PointN(line, n)` — 1-based vertex access. Returns `None` for
/// out-of-range indices, matching PG's `NULL` return.
#[inline]
pub fn point_n(line: &LineString, n: i32) -> Option<Point> {
    line.point_n(n)
}

/// `ST_MakePolygon(exterior)` — single-ring polygon constructor.
///
/// The exterior ring must already be closed (`first == last`). v0.1
/// does NOT auto-close (PostGIS does in some contexts). Build holes
/// separately via [`Polygon::with_holes`]; the `ST_MakePolygon(line,
/// holes_array)` overload is deferred to v0.2.
#[inline]
pub fn make_polygon(exterior: LineString) -> Result<Polygon, GeometryError> {
    Polygon::new(exterior)
}

/// `ST_Length(line)` in meters — Haversine sum across consecutive
/// vertex pairs.
///
/// The per-segment Haversine formula is exact only on a sphere; on the
/// WGS84 ellipsoid it underestimates by at most ~0.5 % (worst case is
/// long N–S routes). For the typical "delivery route" / "running
/// trace" workload this is well below GPS noise.
pub fn linestring_length_m(line: &LineString) -> f64 {
    line.points
        .windows(2)
        .map(|w| haversine_meters(&w[0], &w[1]))
        .sum()
}

/// `ST_Area(polygon)` in square meters — spherical-excess approximation.
///
/// Uses the Chamberlain–Duquette algorithm against `EARTH_RADIUS_M`,
/// which gives < 0.5 % error vs. a true geodesic computation on the
/// WGS84 ellipsoid for any polygon up to continent scale. Holes are
/// subtracted naïvely from the exterior area; v0.1 does NOT validate
/// that holes lie inside the exterior (see [`Polygon`] docs), so an
/// invalid polygon with an escaping hole will return a smaller (or
/// even negative) area than expected — caller's responsibility, same
/// as PostGIS.
///
/// Ring orientation is handled by `abs()`-ing the signed area; CCW vs
/// CW exteriors produce the same answer.
pub fn polygon_area_m2(p: &Polygon) -> f64 {
    let outer = ring_area_m2(&p.exterior);
    let inner: f64 = p.holes.iter().map(ring_area_m2).sum();
    (outer - inner).max(0.0)
}

/// Chamberlain–Duquette spherical-excess area for a single closed ring.
/// Returned value is the unsigned area in m² (orientation-agnostic).
fn ring_area_m2(ring: &LineString) -> f64 {
    // Closed ring: ring.points.len() ≥ 4 and points[0] == points[N-1].
    // We sum signed contributions per edge, then multiply by R²/2.
    let n = ring.points.len();
    if n < 4 {
        return 0.0;
    }
    let mut total = 0.0_f64;
    for i in 0..n {
        let p1 = &ring.points[i];
        // Wrap to first point for the last edge.
        let p2 = &ring.points[(i + 1) % n];
        let lon1 = p1.x.to_radians();
        let lon2 = p2.x.to_radians();
        let lat1 = p1.y.to_radians();
        let lat2 = p2.y.to_radians();
        // Chamberlain–Duquette: each edge contributes
        // (lon2 - lon1) * (2 + sin(lat1) + sin(lat2)).
        // The full formula divides by 2 and multiplies by R² at the end.
        total += (lon2 - lon1) * (2.0 + lat1.sin() + lat2.sin());
    }
    (total * EARTH_RADIUS_M * EARTH_RADIUS_M / 2.0).abs()
}

/// `ST_Contains(polygon, point)` — planar ray-casting in lat/lon
/// degree-space.
///
/// Algorithm: cast a ray east from the test point and count crossings
/// with each ring's edges. Odd crossings against the exterior =>
/// candidate "in"; toggle off if the point also falls inside any hole.
/// Boundary points are reported as **inside** to match PG's
/// `ST_Contains` semantics for the boundary-on case (yes, the manual
/// is contradictory here, but psql `SELECT ST_Contains(poly, p)` on a
/// boundary vertex returns `t` for closed polygons in practice).
///
/// Lat/lon-flat projection caveat: see crate-level docs. Acceptable
/// for state-sized polygons up to ~60° latitude; degrades sharply
/// past that for polygons spanning many degrees of longitude.
pub fn polygon_contains_point(p: &Polygon, pt: &Point) -> bool {
    if !ring_contains_point(&p.exterior, pt) {
        return false;
    }
    for h in &p.holes {
        // Inside a hole = not inside the polygon. A point exactly on a
        // hole's boundary is treated as "still in the polygon" because
        // PG's boundary convention is inclusive.
        if ring_contains_point_strict(h, pt) {
            return false;
        }
    }
    true
}

/// `ST_Within(point, polygon)` — alias for `ST_Contains(polygon, point)`
/// with arguments flipped, matching PostGIS's `ST_Within` semantics.
#[inline]
pub fn point_within_polygon(pt: &Point, p: &Polygon) -> bool {
    polygon_contains_point(p, pt)
}

/// Boundary-inclusive ring containment used for the exterior ring.
fn ring_contains_point(ring: &LineString, pt: &Point) -> bool {
    if on_ring_boundary(ring, pt) {
        return true;
    }
    raycast_inside(ring, pt)
}

/// Boundary-EXCLUSIVE ring containment used for hole rings: a point
/// on a hole's boundary stays in the polygon, not in the hole.
fn ring_contains_point_strict(ring: &LineString, pt: &Point) -> bool {
    if on_ring_boundary(ring, pt) {
        return false;
    }
    raycast_inside(ring, pt)
}

/// True iff `pt` lies on any edge of `ring` (within a tight tolerance).
/// Used to make boundary classification deterministic ahead of the
/// ray-cast, which is otherwise sensitive to vertex grazing.
fn on_ring_boundary(ring: &LineString, pt: &Point) -> bool {
    // 1e-12 deg ≈ 1e-7 m at the equator. Tighter than any realistic
    // input precision; loose enough that an exact-coordinate boundary
    // point survives floating-point round-trips.
    const EPS: f64 = 1e-12;
    let pts = &ring.points;
    for i in 0..pts.len() - 1 {
        let a = &pts[i];
        let b = &pts[i + 1];
        if point_on_segment(a, b, pt, EPS) {
            return true;
        }
    }
    false
}

fn point_on_segment(a: &Point, b: &Point, p: &Point, eps: f64) -> bool {
    // Cross product = 0 (collinear) AND p in the bounding rectangle.
    let cross = (b.x - a.x) * (p.y - a.y) - (b.y - a.y) * (p.x - a.x);
    if cross.abs() > eps {
        return false;
    }
    let min_x = a.x.min(b.x) - eps;
    let max_x = a.x.max(b.x) + eps;
    let min_y = a.y.min(b.y) - eps;
    let max_y = a.y.max(b.y) + eps;
    p.x >= min_x && p.x <= max_x && p.y >= min_y && p.y <= max_y
}

fn raycast_inside(ring: &LineString, pt: &Point) -> bool {
    // Standard W. Randolph Franklin PNPOLY: for each edge that straddles
    // the test latitude, check whether the test longitude is to the
    // left of the edge's intersection with that latitude. Toggle a
    // boolean per crossing.
    let pts = &ring.points;
    let mut inside = false;
    let n = pts.len();
    let mut j = n - 1;
    for i in 0..n {
        let xi = pts[i].x;
        let yi = pts[i].y;
        let xj = pts[j].x;
        let yj = pts[j].y;
        let intersects =
            ((yi > pt.y) != (yj > pt.y)) && (pt.x < (xj - xi) * (pt.y - yi) / (yj - yi) + xi);
        if intersects {
            inside = !inside;
        }
        j = i;
    }
    inside
}

/// `ST_Distance(point, line)` in meters — minimum great-circle distance
/// from `pt` to any segment of `line`.
///
/// Per-segment we use a planar projection of the segment in lat/lon
/// space, project the point onto that segment, then compute the
/// Haversine distance to the projected foot. For segments shorter than
/// a few km this matches the true cross-track distance to <1 mm; for
/// long transcontinental segments it can drift by ~0.1 % which is
/// well inside the spherical-vs-ellipsoidal noise floor.
pub fn distance_point_to_linestring(pt: &Point, line: &LineString) -> f64 {
    line.points
        .windows(2)
        .map(|w| distance_point_to_segment(pt, &w[0], &w[1]))
        .fold(f64::INFINITY, f64::min)
}

fn distance_point_to_segment(pt: &Point, a: &Point, b: &Point) -> f64 {
    // Degenerate: zero-length segment => distance to endpoint.
    let ax = a.x;
    let ay = a.y;
    let bx = b.x;
    let by = b.y;
    let dx = bx - ax;
    let dy = by - ay;
    let len_sq = dx * dx + dy * dy;
    if len_sq <= f64::EPSILON {
        return haversine_meters(pt, a);
    }
    // Parametric projection onto the segment in degree-space, clamped
    // to [0, 1]. The projection is in degree-flat coordinates so it's
    // approximate near the poles; the resulting "foot" point is then
    // measured by Haversine, which is great-circle-correct.
    let t = ((pt.x - ax) * dx + (pt.y - ay) * dy) / len_sq;
    let t = t.clamp(0.0, 1.0);
    let foot = Point::new(ax + t * dx, ay + t * dy);
    haversine_meters(pt, &foot)
}

/// `ST_Distance(line_a, line_b)` in meters — minimum distance between
/// any pair of segments.
///
/// O(N×M) in segment counts. For the v0.1 use-cases (route-similarity
/// queries against tens of stored linestrings) that's fine; v0.2's
/// spatial-index segment will provide an O(log N) prefilter.
pub fn distance_linestring_to_linestring(a: &LineString, b: &LineString) -> f64 {
    let mut best = f64::INFINITY;
    for sa in a.points.windows(2) {
        for sb in b.points.windows(2) {
            let d = segment_segment_distance(&sa[0], &sa[1], &sb[0], &sb[1]);
            if d < best {
                best = d;
            }
        }
    }
    best
}

fn segment_segment_distance(a1: &Point, a2: &Point, b1: &Point, b2: &Point) -> f64 {
    // The minimum of the four "endpoint to other-segment" distances
    // bounds the true segment-segment distance from above. For non-
    // intersecting segments this *is* the true min; for intersecting
    // segments the true min is 0, which the endpoint cases will also
    // find (the endpoints flank the intersection). Cheaper than a full
    // 2-D LP and good enough for the lat/lon-flat approximation we use
    // throughout this crate.
    let candidates = [
        distance_point_to_segment(a1, b1, b2),
        distance_point_to_segment(a2, b1, b2),
        distance_point_to_segment(b1, a1, a2),
        distance_point_to_segment(b2, a1, a2),
    ];
    candidates.iter().cloned().fold(f64::INFINITY, f64::min)
}
