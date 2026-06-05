//! EPSG → PROJ4-string lookup table.
//!
//! Covers the SRIDs this crate's `ST_Transform` understands. Anything not
//! listed here gets a `GeoError::UnknownSrid` from
//! [`crate::transform::st_transform`] with the documented "PROJ4-string-
//! representable SRIDs only" message.
//!
//! ## Coverage
//!
//! - **WGS84** (4326) — `+proj=longlat +datum=WGS84 +no_defs`
//! - **Web Mercator** (3857) — pseudo-Mercator used by Google/OSM tiles
//! - **UTM north zones 1–60** (32601..=32660) — universal transverse mercator,
//!   northern hemisphere
//! - **UTM south zones 1–60** (32701..=32760) — UTM, southern hemisphere
//! - **OSGB36 / British National Grid** (27700)
//! - **RGF93 / Lambert-93** (2154) — French national grid
//! - **Belgian Lambert 72** (31370)
//! - **ETRS89 / UTM zone 32N** (25832) — common in central Europe
//! - **NAD83** (4269) — North American Datum 1983 lat/lon
//! - **NAD27** (4267) — North American Datum 1927 lat/lon
//!
//! All UTM proj-strings are emitted programmatically from a single template
//! at call time; the small set of named grids is in a `&'static` table.
//! Total runtime memory footprint is ~200 entries of `(u32, String)`.
//!
//! ## What this does NOT cover
//!
//! - **NTv2 / NADCON datum-shift grids** — proj4rs cannot use the binary
//!   grid files that ship separately from libproj, so transformations that
//!   PostGIS supplements with a grid shift (e.g. NAD27↔NAD83 via the
//!   `conus` grid) will be off by tens of meters in some regions. The
//!   PROJ4 string version we emit uses the published average shift, which
//!   matches PostGIS's behaviour when its grid files are not installed.
//! - **State Plane Coordinate System** (26xxx EPSG codes) — out of scope
//!   for v0.1. Add on demand.
//! - **GDA94 / MGA zones** (28349–28356), **JGD2000**, **CH1903+** etc.
//!   Add on demand.

/// Named (non-UTM-programmatic) EPSG → proj-string entries. The UTM zones
/// (32601..=32660, 32701..=32760) are produced on the fly by
/// [`proj_string_for_srid`] rather than spelling out 120 nearly-identical
/// strings here.
const NAMED_EPSG_TABLE: &[(u32, &str)] = &[
    // WGS84 lat/lon. The implicit-default SRID for every basin POINT.
    (4326, "+proj=longlat +datum=WGS84 +no_defs"),
    // Web Mercator (EPSG:3857). The "pseudo-Mercator" Google Maps /
    // OpenStreetMap uses for tile rendering. Uses a sphere of radius
    // 6378137 m even though the underlying datum is WGS84.
    (
        3857,
        "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +nadgrids=@null +wktext +no_defs",
    ),
    // OSGB36 / British National Grid. Easting/northing in metres relative
    // to a false origin SW of the Scilly Isles.
    (
        27700,
        "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84=446.448,-125.157,542.06,0.15,0.247,0.842,-20.489 +units=m +no_defs",
    ),
    // RGF93 / Lambert-93 (EPSG:2154). The French national projected CRS.
    (
        2154,
        "+proj=lcc +lat_1=49 +lat_2=44 +lat_0=46.5 +lon_0=3 +x_0=700000 +y_0=6600000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs",
    ),
    // Belgian Lambert 72 (EPSG:31370).
    (
        31370,
        "+proj=lcc +lat_1=51.16666723333333 +lat_2=49.8333339 +lat_0=90 +lon_0=4.367486666666666 +x_0=150000.013 +y_0=5400088.438 +ellps=intl +towgs84=-106.869,52.2978,-103.724,0.3366,-0.457,1.8422,-1.2747 +units=m +no_defs",
    ),
    // ETRS89 / UTM zone 32N (EPSG:25832). Common across central Europe.
    (
        25832,
        "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs",
    ),
    // NAD83 lat/lon (EPSG:4269). Equivalent to WGS84 for most practical
    // purposes (offset < 1 m in CONUS).
    (4269, "+proj=longlat +ellps=GRS80 +datum=NAD83 +no_defs"),
    // NAD27 lat/lon (EPSG:4267). Pre-1983 North American Datum.
    (4267, "+proj=longlat +ellps=clrk66 +datum=NAD27 +no_defs"),
];

/// Return the PROJ4 string for `srid`, or `None` if not supported.
///
/// UTM zones (EPSG 32601..=32660 north, 32701..=32760 south) are
/// generated on demand so the static table doesn't have to enumerate
/// 120 nearly-identical strings.
pub fn proj_string_for_srid(srid: u32) -> Option<String> {
    // UTM north zones 1..=60 → EPSG 32601..=32660.
    if (32601..=32660).contains(&srid) {
        let zone = srid - 32600;
        return Some(format!(
            "+proj=utm +zone={zone} +datum=WGS84 +units=m +no_defs"
        ));
    }
    // UTM south zones 1..=60 → EPSG 32701..=32760.
    if (32701..=32760).contains(&srid) {
        let zone = srid - 32700;
        return Some(format!(
            "+proj=utm +zone={zone} +south +datum=WGS84 +units=m +no_defs"
        ));
    }
    NAMED_EPSG_TABLE
        .iter()
        .find(|(code, _)| *code == srid)
        .map(|(_, s)| (*s).to_string())
}

/// True iff [`proj_string_for_srid`] would return `Some(_)`. Used by
/// `ST_Transform` to give a friendlier error before constructing the
/// `proj4rs::Proj`.
pub fn is_supported(srid: u32) -> bool {
    proj_string_for_srid(srid).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wgs84_is_supported() {
        assert!(is_supported(4326));
        let s = proj_string_for_srid(4326).unwrap();
        assert!(s.contains("+proj=longlat"));
        assert!(s.contains("+datum=WGS84"));
    }

    #[test]
    fn utm_north_zone_31_is_supported() {
        // UTM31N (covers France/Paris) is EPSG 32631.
        let s = proj_string_for_srid(32631).unwrap();
        assert!(s.contains("+proj=utm"));
        assert!(s.contains("+zone=31"));
        // North zone — no `+south` flag.
        assert!(!s.contains("+south"));
    }

    #[test]
    fn utm_south_zone_50_is_supported() {
        // UTM50S (covers western Australia) is EPSG 32750.
        let s = proj_string_for_srid(32750).unwrap();
        assert!(s.contains("+zone=50"));
        assert!(s.contains("+south"));
    }

    #[test]
    fn web_mercator_is_supported() {
        assert!(is_supported(3857));
    }

    #[test]
    fn unknown_srid_returns_none() {
        // Pick a code well outside any band we support.
        assert!(proj_string_for_srid(99999).is_none());
        assert!(!is_supported(99999));
    }

    #[test]
    fn british_national_grid_is_supported() {
        assert!(is_supported(27700));
    }

    #[test]
    fn french_lambert93_is_supported() {
        assert!(is_supported(2154));
    }
}
