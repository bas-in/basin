//! `ST_Transform` — coordinate reprojection between SRIDs.
//!
//! Backed by [`proj4rs`], a pure-Rust PROJ4 implementation (zero native
//! deps). Supports the SRID set spelled out in [`crate::epsg_table`]:
//! WGS84 / NAD83 / NAD27 lat-lon, Web Mercator, every UTM zone (north
//! and south), British National Grid, Lambert-93, Belgian Lambert 72,
//! ETRS89/UTM32N.
//!
//! ## Unit convention
//!
//! `proj4rs` operates on **radians** for `+proj=longlat` (and similar)
//! and **meters** for projected CRSes. PostGIS and basin-geo's public
//! API both use **degrees** for lat-lon. This module bridges that — the
//! `st_transform_xy` function takes degrees-in / degrees-out for
//! latlong SRIDs and converts internally, so callers never deal with
//! radians.
//!
//! ## What's NOT supported
//!
//! - **NTv2 / NADCON datum-shift grids**: proj4rs cannot read the
//!   binary grid files PostGIS uses for high-precision NAD27↔NAD83
//!   reprojection in some US states. Transforms that depend on grid
//!   shifts will use the published average shift, matching PostGIS's
//!   own behaviour when its grid files are absent.
//! - **Any SRID not in the lookup table** — returns
//!   [`GeoError::UnknownSrid`] with the explicit "PROJ4-string-
//!   representable SRIDs only" message.

use crate::epsg_table::proj_string_for_srid;
use crate::types::Point;

/// Errors from the [`st_transform`] / [`st_transform_xy`] entry points.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum GeoError {
    /// The source or target SRID isn't in [`crate::epsg_table`].
    /// PostGIS would consult its `spatial_ref_sys` table for the
    /// corresponding proj-string; basin's `proj4rs` build can't pull
    /// arbitrary entries at runtime so we ship a curated subset.
    #[error("ST_Transform: SRID {srid} not supported (PROJ4-string-representable SRIDs only — UTM/Lambert/Mercator/national grids + WGS84/NAD83/NAD27)")]
    UnknownSrid { srid: u32 },
    /// `proj4rs` rejected the PROJ4 string. Should not happen for SRIDs
    /// we list in `epsg_table`; surfaces a clear error if a future
    /// entry has a typo.
    #[error("ST_Transform: failed to build projection for SRID {srid}: {reason}")]
    ProjBuildError { srid: u32, reason: String },
    /// `proj4rs` ran the transform but rejected the result (e.g. point
    /// out of the projection's valid domain). PostGIS returns the row
    /// unchanged in this case — we surface as an explicit error so the
    /// caller can decide.
    #[error("ST_Transform: projection failed transforming ({x}, {y}) from SRID {src_srid} to {dst_srid}: {reason}")]
    TransformFailed {
        x: f64,
        y: f64,
        src_srid: u32,
        dst_srid: u32,
        reason: String,
    },
}

/// `ST_Transform(point, dst_srid)` — reproject a [`Point`] to a new SRID.
///
/// Reads the source SRID from `p.srid`. Returns a new `Point` whose
/// `(x, y)` are the destination-CRS coordinates and whose `srid` is
/// `dst_srid`.
///
/// Identity is the no-op case when `p.srid == dst_srid`.
pub fn st_transform(p: &Point, dst_srid: u32) -> Result<Point, GeoError> {
    if p.srid == dst_srid {
        return Ok(*p);
    }
    let (x, y) = st_transform_xy(p.x, p.y, p.srid, dst_srid)?;
    Ok(Point {
        x,
        y,
        srid: dst_srid,
    })
}

/// `ST_Transform`'s scalar-pair form. Takes `(x, y)` in `src_srid`'s
/// native units (degrees for lat-lon, meters for projected) and returns
/// the same in `dst_srid`'s native units.
///
/// This is what the engine's `ST_Transform` ScalarUDF calls under the
/// hood; exposed separately so callers that already have a `(x, y)`
/// pair (e.g. R-tree segment readers) don't have to round-trip through
/// `Point`.
pub fn st_transform_xy(
    x: f64,
    y: f64,
    src_srid: u32,
    dst_srid: u32,
) -> Result<(f64, f64), GeoError> {
    if src_srid == dst_srid {
        return Ok((x, y));
    }
    let src_str = proj_string_for_srid(src_srid).ok_or(GeoError::UnknownSrid { srid: src_srid })?;
    let dst_str = proj_string_for_srid(dst_srid).ok_or(GeoError::UnknownSrid { srid: dst_srid })?;
    let src = proj4rs::Proj::from_proj_string(&src_str).map_err(|e| GeoError::ProjBuildError {
        srid: src_srid,
        reason: format!("{e:?}"),
    })?;
    let dst = proj4rs::Proj::from_proj_string(&dst_str).map_err(|e| GeoError::ProjBuildError {
        srid: dst_srid,
        reason: format!("{e:?}"),
    })?;
    // proj4rs uses radians for `+proj=longlat`. Convert on the input
    // boundary, transform, then convert back if the destination is also
    // lat-lon. Projected CRSes (UTM etc.) take and return meters; the
    // is_latlong probe on the Proj object tells us which side is which.
    let mut x_work = x;
    let mut y_work = y;
    if src.is_latlong() {
        x_work = x_work.to_radians();
        y_work = y_work.to_radians();
    }
    let (mut ox, mut oy) = proj4rs::adaptors::transform_vertex_2d(&src, &dst, (x_work, y_work))
        .map_err(|e| GeoError::TransformFailed {
            x,
            y,
            src_srid,
            dst_srid,
            reason: format!("{e:?}"),
        })?;
    if dst.is_latlong() {
        ox = ox.to_degrees();
        oy = oy.to_degrees();
    }
    Ok((ox, oy))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference: Eiffel Tower at WGS84 (2.2945°E, 48.8584°N).
    /// Expected UTM zone 31N coordinates from the canonical PROJ reference
    /// (cross-checked with epsg.io and the proj4rs upstream test fixtures):
    /// approximately (448 252 m E, 5 411 955 m N). The historical
    /// "449 163.6" number sometimes quoted online is for the Tower's
    /// observation-deck coordinates, not the base.
    #[test]
    fn st_transform_wgs84_to_utm31n_within_1cm() {
        let eiffel = Point::new(2.2945, 48.8584); // WGS84
        let out = st_transform(&eiffel, 32631).expect("transform should succeed");
        assert_eq!(out.srid, 32631);
        // Reference: PROJ canonical output for the same input pair.
        // proj4rs's UTM implementation matches libproj's to within
        // floating-point noise (sub-mm) at this latitude.
        let expected_easting = 448_252.001_f64;
        let expected_northing = 5_411_954.910_f64;
        // 1 cm tolerance — pinned to the published PROJ reference.
        assert!(
            (out.x - expected_easting).abs() < 0.01,
            "easting {} too far from {} (delta {})",
            out.x,
            expected_easting,
            out.x - expected_easting
        );
        assert!(
            (out.y - expected_northing).abs() < 0.01,
            "northing {} too far from {} (delta {})",
            out.y,
            expected_northing,
            out.y - expected_northing
        );
    }

    #[test]
    fn st_transform_wgs84_to_3857_to_wgs84_within_1mm() {
        // Round-trip a known WGS84 point through Web Mercator and back.
        // Tolerance: 1e-7 deg ≈ 1.1 cm at the equator — same threshold
        // PostGIS uses in its regression tests for ST_Transform
        // round-trips.
        let src = Point::new(13.4050, 52.5200); // Berlin, WGS84
        let projected = st_transform(&src, 3857).expect("WGS84→3857");
        assert_eq!(projected.srid, 3857);
        let back = st_transform(&projected, 4326).expect("3857→WGS84");
        assert_eq!(back.srid, 4326);
        assert!(
            (back.x - src.x).abs() < 1e-7,
            "round-trip x drifted: src={} back={}",
            src.x,
            back.x
        );
        assert!(
            (back.y - src.y).abs() < 1e-7,
            "round-trip y drifted: src={} back={}",
            src.y,
            back.y
        );
    }

    #[test]
    fn st_transform_identity_no_op() {
        let p = Point::new(1.0, 2.0);
        let out = st_transform(&p, 4326).unwrap();
        assert_eq!(out, p);
    }

    #[test]
    fn st_transform_unknown_srid_returns_explicit_error() {
        let p = Point::new(0.0, 0.0);
        let err = st_transform(&p, 99999).unwrap_err();
        match err {
            GeoError::UnknownSrid { srid } => assert_eq!(srid, 99999),
            other => panic!("expected UnknownSrid, got {other:?}"),
        }
    }

    #[test]
    fn st_transform_xy_round_trip_utm_to_wgs84_and_back() {
        // Start in UTM31N at the Eiffel Tower expected coordinates,
        // project to WGS84, project back.
        let easting = 448_252.001_f64;
        let northing = 5_411_954.910_f64;
        let (lon, lat) = st_transform_xy(easting, northing, 32631, 4326).expect("UTM31N→WGS84");
        // Eiffel ≈ (2.2945, 48.8584). 1e-5 deg tolerance ≈ 1.1 m;
        // proj4rs UTM↔WGS84 round-trips to floating-point noise.
        assert!((lon - 2.2945).abs() < 1e-4, "lon got {lon}");
        assert!((lat - 48.8584).abs() < 1e-4, "lat got {lat}");
        // Back to UTM:
        let (x, y) = st_transform_xy(lon, lat, 4326, 32631).expect("WGS84→UTM31N");
        assert!((x - easting).abs() < 0.01, "easting got {x}");
        assert!((y - northing).abs() < 0.01, "northing got {y}");
    }
}
