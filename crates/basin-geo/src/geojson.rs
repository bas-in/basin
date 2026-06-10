//! GeoJSON-style coordinate-array JSON encode/decode for [`LineString`],
//! [`Polygon`], and [`Point`].
//!
//! v0.1 wire format for LineString / Polygon: a JSON array of coordinate
//! pairs (linestring) or array of rings (polygon). Deliberately *not* the
//! full GeoJSON envelope for those types — the engine column's schema already
//! carries the geometry type, so the `{"type":"…","coordinates":[…]}` wrapper
//! would be pure overhead.
//!
//! ```text
//! LineString:  [[lon, lat], [lon, lat], ...]
//! Polygon:     [ [[lon,lat], ...],            // exterior ring
//!                [[lon,lat], ...], ... ]      // 0+ holes
//! ```
//!
//! Compatible-by-truncation with GeoJSON's `coordinates` field, so a
//! caller that wants full GeoJSON can wrap our output in
//! `{"type": "LineString", "coordinates": <our_output>}` and emit it.
//!
//! ## Full GeoJSON envelope for POINT (`ST_AsGeoJSON` / `ST_GeomFromGeoJSON`)
//!
//! For POINT specifically we **do** emit the full RFC 7946 envelope because
//! PostGIS's `ST_AsGeoJSON` is the standard client-library expectation and
//! POINT is a first-class storage type in Basin.
//!
//! Output format (no spaces, lon-first per GeoJSON §3.1.2):
//! ```text
//! {"type":"Point","coordinates":[x,y]}
//! ```
//!
//! Float formatting: `serde_json::to_string` converts `f64` values through
//! Rust's Grisu3/Dragon4 implementation, which emits the shortest decimal
//! representation that round-trips via `f64` parsing — up to 17 significant
//! digits, matching or exceeding PostGIS's documented ≤15 sig-fig behaviour.
//! Values like `1.5` print as `1.5` (not `1.5000000000000000`), matching
//! PostGIS's compact output for exact-decimal inputs.
//!
//! WKT / WKB / EWKB encoding for LineString/Polygon types is a v0.2 add —
//! we don't need PostGIS-text-compat for v0.1, just a stable JSON form Basin
//! can roundtrip through `Utf8` columns.

use crate::types::{GeometryError, LineString, Point, Polygon};

/// Errors decoding a coordinate-array JSON blob.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum GeoJsonError {
    #[error("invalid JSON: {0}")]
    Json(String),
    #[error("coordinate pair must have 2 numbers, got {got}")]
    BadCoord { got: usize },
    #[error("geometry rejected: {0}")]
    Geometry(#[from] GeometryError),
    #[error("polygon must have at least one ring")]
    PolygonEmpty,
}

/// Encode a [`LineString`] as `[[lon, lat], ...]` JSON.
pub fn encode_linestring(line: &LineString) -> String {
    let coords: Vec<[f64; 2]> = line.points.iter().map(|p| [p.x, p.y]).collect();
    serde_json::to_string(&coords).expect("Vec<[f64; 2]> always serialises")
}

/// Decode `[[lon, lat], ...]` JSON into a [`LineString`]. The ≥2-point
/// constraint is enforced by [`LineString::new`] downstream.
pub fn decode_linestring(s: &str) -> Result<LineString, GeoJsonError> {
    let coords: Vec<Vec<f64>> =
        serde_json::from_str(s).map_err(|e| GeoJsonError::Json(e.to_string()))?;
    let mut pts = Vec::with_capacity(coords.len());
    for c in coords {
        if c.len() != 2 {
            return Err(GeoJsonError::BadCoord { got: c.len() });
        }
        pts.push(Point::new(c[0], c[1]));
    }
    Ok(LineString::new(pts)?)
}

/// Encode a [`Polygon`] as `[exterior_ring, hole_ring, ...]` JSON.
pub fn encode_polygon(p: &Polygon) -> String {
    let mut rings: Vec<Vec<[f64; 2]>> = Vec::with_capacity(1 + p.holes.len());
    rings.push(p.exterior.points.iter().map(|q| [q.x, q.y]).collect());
    for h in &p.holes {
        rings.push(h.points.iter().map(|q| [q.x, q.y]).collect());
    }
    serde_json::to_string(&rings).expect("Vec<Vec<[f64; 2]>> always serialises")
}

/// Decode `[exterior_ring, hole_ring, ...]` JSON into a [`Polygon`].
pub fn decode_polygon(s: &str) -> Result<Polygon, GeoJsonError> {
    let rings: Vec<Vec<Vec<f64>>> =
        serde_json::from_str(s).map_err(|e| GeoJsonError::Json(e.to_string()))?;
    if rings.is_empty() {
        return Err(GeoJsonError::PolygonEmpty);
    }
    let mut iter = rings.into_iter();
    let exterior = decode_ring(iter.next().unwrap())?;
    let mut holes = Vec::new();
    for r in iter {
        holes.push(decode_ring(r)?);
    }
    Ok(Polygon::with_holes(exterior, holes)?)
}

fn decode_ring(coords: Vec<Vec<f64>>) -> Result<LineString, GeoJsonError> {
    let mut pts = Vec::with_capacity(coords.len());
    for c in coords {
        if c.len() != 2 {
            return Err(GeoJsonError::BadCoord { got: c.len() });
        }
        pts.push(Point::new(c[0], c[1]));
    }
    Ok(LineString::new(pts)?)
}

// ── Full GeoJSON envelope for POINT ──────────────────────────────────────────
//
// PostGIS ST_AsGeoJSON output for a POINT is the RFC 7946 §3.1.2 envelope:
//
//   {"type":"Point","coordinates":[x,y]}
//
// No spaces, lon (x) first, lat (y) second.  serde_json serialises f64 with
// the shortest decimal that parses back to the same bits (Grisu3/Dragon4) —
// e.g. 1.5 → "1.5", -73.985… → "-73.985…".  This is at least as precise as
// PostGIS's documented ≤15 significant digits, and the round-trip property
// ST_GeomFromGeoJSON(ST_AsGeoJSON(g)) == g holds because we decode with the
// same f64 parser.

/// Encode a [`Point`] as a full GeoJSON `{"type":"Point","coordinates":[x,y]}`
/// string (no spaces, PostGIS-compatible format).
pub fn encode_point_geojson(p: &Point) -> String {
    // serde_json::to_string on a [f64; 2] emits compact JSON with Rust's
    // shortest-round-trip float formatter — no extra whitespace.
    let coords: [f64; 2] = [p.x, p.y];
    format!(
        "{{\"type\":\"Point\",\"coordinates\":{}}}",
        serde_json::to_string(&coords).expect("[f64; 2] always serialises")
    )
}

/// Decode a GeoJSON `{"type":"Point","coordinates":[x,y]}` string into a
/// [`Point`].  The `type` field must be present and equal to `"Point"`
/// (case-sensitive, matching PostGIS).  Extra top-level keys (e.g. `crs`,
/// `bbox`) are silently ignored so callers can round-trip PostGIS output
/// that includes those fields.
pub fn decode_point_geojson(s: &str) -> Result<Point, GeoJsonError> {
    // Parse as a generic JSON object so we tolerate extra keys.
    let v: serde_json::Value =
        serde_json::from_str(s).map_err(|e| GeoJsonError::Json(e.to_string()))?;
    let obj = v
        .as_object()
        .ok_or_else(|| GeoJsonError::Json("expected JSON object".into()))?;

    // Validate "type" == "Point".
    match obj.get("type").and_then(|t| t.as_str()) {
        Some("Point") => {}
        Some(other) => {
            return Err(GeoJsonError::Json(format!(
                "expected GeoJSON type \"Point\", got \"{other}\""
            )));
        }
        None => {
            return Err(GeoJsonError::Json(
                "GeoJSON object missing \"type\" field".into(),
            ));
        }
    }

    // Extract coordinates array.
    let coords = obj
        .get("coordinates")
        .and_then(|c| c.as_array())
        .ok_or_else(|| GeoJsonError::Json("missing or non-array \"coordinates\" field".into()))?;

    if coords.len() != 2 {
        return Err(GeoJsonError::BadCoord { got: coords.len() });
    }
    let x = coords[0]
        .as_f64()
        .ok_or_else(|| GeoJsonError::Json("coordinate x is not a number".into()))?;
    let y = coords[1]
        .as_f64()
        .ok_or_else(|| GeoJsonError::Json("coordinate y is not a number".into()))?;
    Ok(Point::new(x, y))
}
