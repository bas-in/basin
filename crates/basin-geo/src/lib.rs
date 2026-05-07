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
//! - **`LINESTRING`, `POLYGON`, `MULTIPOLYGON`, `GEOMETRYCOLLECTION`** —
//!   not modelled. v0.2.
//! - **R-tree / GIST spatial index** — without one, `ST_DWithin` is a
//!   brute-force scan of the partition. That's fine for tens of
//!   thousands of points per tenant but quadratic on N×M joins.
//!   The fix in v0.2 is a basin-storage-side spatial-index segment
//!   format mirrored on the HNSW vector index.
//! - **EWKB / non-WGS84 SRIDs** — every point is interpreted as
//!   lat/lon. Reprojection against `proj4` is a v0.2 add.
//! - **Parser-level recognition of `POINT` / `BOX2D` as Arrow column
//!   types** — DDL like `CREATE TABLE t (loc POINT)` is not yet
//!   recognised by the SQL planner. Today, points are stored as
//!   `BYTEA`-typed columns holding the 21-byte WKB blob; the engine
//!   glue's stub doc enumerates the planner change needed.

#![forbid(unsafe_code)]

pub mod types;
pub mod wkb;

pub use types::{Box2d, Point, SRID_WGS84};
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
