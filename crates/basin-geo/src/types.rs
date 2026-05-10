//! Geometry primitives — `POINT`, `BOX2D`, `LINESTRING`, `POLYGON`
//! for the v0.1 PostGIS subset.
//!
//! `Point` / `Box2d` are deliberately small (16–32 bytes in memory) and
//! `Copy` so they can be stamped into Arrow `FixedSizeBinary(21)` columns
//! by the engine glue without allocator pressure. `LineString` /
//! `Polygon` are heap-allocated `Vec`s (variable-length geometries can't
//! be `Copy`); the engine glue stores them as `Utf8` columns holding the
//! GeoJSON-style coordinate arrays this module emits.
//!
//! ## SRID assumptions
//!
//! v0.1 hard-codes [`SRID_WGS84`] (`4326`). [`Point::new`] / [`Box2d::new`]
//! both stamp 4326 unconditionally; the explicit `with_srid` constructors
//! are reserved for v0.2 (when reprojection arrives) so callers don't bake
//! a non-WGS84 SRID into stored WKB blobs that the rest of the stack still
//! interprets as lat/lon. `LineString` / `Polygon` inherit the SRID of
//! the points they're built from; v0.1 doesn't enforce uniformity (the
//! whole stack is implicit-WGS84 anyway).

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

/// Errors constructing a [`LineString`] or [`Polygon`].
///
/// Geometry construction is fallible because `LINESTRING` requires ≥2
/// points and `POLYGON` requires the exterior ring to be closed. PostGIS
/// signals these as runtime SQL errors; we return them as Rust `Result`s
/// so the engine glue can map them to `SqlState::DataException`.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum GeometryError {
    /// A `LINESTRING` was constructed with fewer than 2 points.
    /// PostGIS rejects single-point linestrings the same way.
    #[error("LineString requires at least 2 points, got {got}")]
    LineStringTooShort { got: usize },
    /// A `POLYGON`'s ring did not close on itself: `points[0] != points[N-1]`.
    /// PostGIS auto-closes in some contexts; v0.1 is strict — call
    /// [`LineString::close`] first if you need that.
    #[error("Polygon ring is not closed: first point != last point")]
    PolygonRingNotClosed,
    /// A `POLYGON` exterior ring had fewer than 4 points (3 distinct +
    /// the repeated closing point). A triangle is the smallest valid
    /// polygon; anything less collapses to a line or a point.
    #[error("Polygon ring requires at least 4 points (closed triangle), got {got}")]
    PolygonRingTooShort { got: usize },
}

/// `LINESTRING` — an ordered sequence of 2 or more points.
///
/// The `points` field is public for ergonomic iteration; mutation goes
/// through [`LineString::new`] so the ≥2 invariant survives. Reuses the
/// same SRID-implicit lat/lon convention as [`Point`].
///
/// Memory layout is a plain `Vec<Point>`; expect ~32 B/point (24 B point
/// + Vec overhead amortised). For dense routes that's the same density
/// PostGIS pays in EWKB (16 B/point + 9 B header).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LineString {
    pub points: Vec<Point>,
}

impl LineString {
    /// Build a linestring from at least 2 points. Returns
    /// [`GeometryError::LineStringTooShort`] for 0/1-point inputs.
    pub fn new(points: Vec<Point>) -> Result<Self, GeometryError> {
        if points.len() < 2 {
            return Err(GeometryError::LineStringTooShort { got: points.len() });
        }
        Ok(Self { points })
    }

    /// Convenience: construct from `(x, y)` pairs. WGS84 implicit.
    pub fn from_xy(coords: &[(f64, f64)]) -> Result<Self, GeometryError> {
        let pts: Vec<Point> = coords.iter().map(|&(x, y)| Point::new(x, y)).collect();
        Self::new(pts)
    }

    /// Number of points (PostGIS `ST_NumPoints`).
    #[inline]
    pub fn num_points(&self) -> usize {
        self.points.len()
    }

    /// 1-based point access matching PostGIS `ST_PointN(line, n)`.
    /// Returns `None` if `n < 1` or `n > num_points()`.
    pub fn point_n(&self, n: i32) -> Option<Point> {
        if n < 1 {
            return None;
        }
        let idx = (n as usize) - 1;
        self.points.get(idx).copied()
    }

    /// True iff the first and last points are bit-identical. Used by
    /// [`Polygon::new`] to validate the ring invariant.
    #[inline]
    pub fn is_closed(&self) -> bool {
        match (self.points.first(), self.points.last()) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    }

    /// Return a copy of `self` with the first point appended at the end
    /// if the ring isn't already closed. Used by callers that have an
    /// open ring and want a polygon: `LineString::close(open).unwrap()`.
    pub fn close(mut self) -> Self {
        if !self.is_closed() {
            if let Some(first) = self.points.first().copied() {
                self.points.push(first);
            }
        }
        self
    }
}

/// `POLYGON` — an exterior ring plus zero or more interior rings (holes).
///
/// Rings are stored as [`LineString`]s with the closure invariant
/// (`first == last`) enforced by [`Polygon::new`]. v0.1 ships the
/// holes-supported data model but the area / contains math treats holes
/// naïvely:
///
/// - [`crate::polygon_area_m2`] subtracts hole areas from the exterior;
///   it does NOT verify hole–exterior containment or that holes are
///   disjoint. Callers passing overlapping or escaping holes get
///   garbage area numbers, same as PostGIS would on an invalid polygon.
/// - [`crate::polygon_contains_point`] applies ray-casting against the
///   exterior, then flips per hole. A point inside a hole is NOT
///   considered contained — matching PostGIS semantics.
///
/// Ring orientation is NOT enforced (PostGIS likewise tolerates either
/// winding); the area computation takes `abs()` so CCW vs CW exterior
/// rings yield the same answer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Polygon {
    pub exterior: LineString,
    pub holes: Vec<LineString>,
}

impl Polygon {
    /// Build a polygon from an exterior ring with no holes.
    /// Validates ring closure (first point == last point) and that the
    /// ring has ≥4 points (smallest closed simple polygon = triangle).
    pub fn new(exterior: LineString) -> Result<Self, GeometryError> {
        Self::with_holes(exterior, Vec::new())
    }

    /// Build a polygon with one or more holes. Each hole must itself be
    /// a closed ring with ≥4 points; v0.1 does NOT verify the holes lie
    /// inside the exterior or are mutually disjoint (see type docs).
    pub fn with_holes(
        exterior: LineString,
        holes: Vec<LineString>,
    ) -> Result<Self, GeometryError> {
        validate_ring(&exterior)?;
        for h in &holes {
            validate_ring(h)?;
        }
        Ok(Self { exterior, holes })
    }
}

fn validate_ring(ring: &LineString) -> Result<(), GeometryError> {
    if ring.points.len() < 4 {
        return Err(GeometryError::PolygonRingTooShort {
            got: ring.points.len(),
        });
    }
    if !ring.is_closed() {
        return Err(GeometryError::PolygonRingNotClosed);
    }
    Ok(())
}
