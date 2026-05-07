//! Geometry primitives — `POINT` and `BOX2D` for the v0.1 PostGIS subset.
//!
//! Both types are deliberately small (16–32 bytes in memory) and `Copy` so
//! they can be stamped into Arrow `FixedSizeBinary(21)` columns by the
//! engine glue without allocator pressure.
//!
//! ## SRID assumptions
//!
//! v0.1 hard-codes [`SRID_WGS84`] (`4326`). [`Point::new`] / [`Box2d::new`]
//! both stamp 4326 unconditionally; the explicit `with_srid` constructors
//! are reserved for v0.2 (when reprojection arrives) so callers don't bake
//! a non-WGS84 SRID into stored WKB blobs that the rest of the stack still
//! interprets as lat/lon.

use serde::{Deserialize, Serialize};

/// World Geodetic System 1984. Plain lat/lon; the assumption every
/// PostGIS `POINT` carries unless the schema says otherwise.
pub const SRID_WGS84: u32 = 4326;

/// Two-dimensional point.
///
/// Field order is `(x, y)` to match PostGIS / WKT (`POINT(x y)`).
/// In WGS84 that means `x = longitude`, `y = latitude`.
///
/// We store `f64` (8 bytes each) for parity with PostGIS's on-the-wire
/// representation; truncating to `f32` would silently lose ~2 cm of
/// precision in lat/lon, which is enough to break "within 1m" queries.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Point {
    pub x: f64,
    pub y: f64,
    pub srid: u32,
}

impl Point {
    /// Construct a WGS84 point. `x = longitude`, `y = latitude`.
    #[inline]
    pub const fn new(x: f64, y: f64) -> Self {
        Self {
            x,
            y,
            srid: SRID_WGS84,
        }
    }

    /// Construct a point with an explicit SRID. Reserved for v0.2 (when
    /// reprojection arrives); the v0.1 functions in this crate assume
    /// WGS84 unconditionally.
    #[inline]
    pub const fn with_srid(x: f64, y: f64, srid: u32) -> Self {
        Self { x, y, srid }
    }

    /// Longitude. Same as `.x` — provided for code that reads better
    /// in the lat/lon mental model.
    #[inline]
    pub fn lon(&self) -> f64 {
        self.x
    }

    /// Latitude. Same as `.y` — provided for code that reads better
    /// in the lat/lon mental model.
    #[inline]
    pub fn lat(&self) -> f64 {
        self.y
    }
}

/// Axis-aligned bounding box.
///
/// `min_x ≤ max_x` and `min_y ≤ max_y` are class invariants; the `new`
/// constructor sorts inputs so callers don't have to. PostGIS's `BOX2D`
/// is the same shape (no SRID stored on the wire), but we carry one for
/// future-proofing and because it costs nothing.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Box2d {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
    pub srid: u32,
}

impl Box2d {
    /// Construct a WGS84 bounding box. Inputs are sorted so the resulting
    /// box always satisfies `min_* ≤ max_*`; pass either corner pair in.
    #[inline]
    pub fn new(a_x: f64, a_y: f64, b_x: f64, b_y: f64) -> Self {
        Self {
            min_x: a_x.min(b_x),
            min_y: a_y.min(b_y),
            max_x: a_x.max(b_x),
            max_y: a_y.max(b_y),
            srid: SRID_WGS84,
        }
    }
}
