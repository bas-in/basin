//! GeoJSON-style coordinate-array JSON encode/decode for [`LineString`]
//! and [`Polygon`].
//!
//! v0.1 wire format. This is the **simplest** structural shape that
//! Basin can roundtrip — a JSON array of coordinate pairs (linestring)
//! or array of rings (polygon). It is deliberately *not* full GeoJSON:
//! we don't emit the `{"type": "...", "coordinates": [...]}` envelope
//! because that's pure overhead for a column that already carries the
//! geometry-type information in its schema.
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
//! WKT / WKB / EWKB encoding for these types is a v0.2 add — we don't
//! need PostGIS-text-compat for v0.1, just a stable JSON form Basin can
//! roundtrip through `Utf8` columns.

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
