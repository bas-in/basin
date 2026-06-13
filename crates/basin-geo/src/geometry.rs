//! General geometry codecs for the full PostGIS-lite geometry set.
//!
//! The fixed-21-byte [`crate::wkb`] path stays the fast lane for `POINT`
//! columns. This module adds **variable-length** WKB / EWKB / WKT /
//! GeoJSON encode + decode for every geometry the v0.2 surface commits to:
//!
//! - `POINT`
//! - `LINESTRING`
//! - `POLYGON` (exterior + holes)
//! - `MULTIPOINT`
//! - `MULTILINESTRING`
//! - `MULTIPOLYGON`
//! - `GEOMETRYCOLLECTION` (recursive)
//!
//! The in-memory model is [`geo_types::Geometry`] (re-exported by the `geo`
//! crate already in the workspace), so the planar algorithms — centroid,
//! bounding rect, point-in-polygon — come from `geo` directly and Basin
//! does not hand-roll them. Measures that must be WGS84-correct (length /
//! area / perimeter) reuse the spherical math in [`crate::lib`].
//!
//! ## Wire formats
//!
//! **WKB** — standard OGC layout, little-endian:
//! `01 <type:u32le> <body…>`. Bodies:
//! - Point: `X:f64 Y:f64`
//! - LineString: `npts:u32 (X Y)*`
//! - Polygon: `nrings:u32 (npts:u32 (X Y)*)*`
//! - MultiPoint: `n:u32 (point-wkb)*`
//! - MultiLineString / MultiPolygon / GeometryCollection: `n:u32 (member-wkb)*`
//!
//! **EWKB** — PostGIS extension: the type code is OR'd with `0x2000_0000`
//! and a 4-byte SRID is inserted immediately after the type code (only on
//! the top-level geometry, per the PostGIS convention). [`encode_ewkb`] /
//! [`decode_any`] both honour it; plain WKB decodes with SRID 0 (unknown).
//!
//! **WKT** — `TYPE(...)`, case-insensitive on the tag, matching PostGIS
//! `ST_AsText` / `ST_GeomFromText` for 2-D geometries.
//!
//! **GeoJSON** — RFC 7946 `{"type":…,"coordinates":…}` (and the
//! `geometries` array for GeometryCollection).

use geo::geometry::{
    Coord, Geometry, GeometryCollection, LineString as GLineString, MultiLineString, MultiPoint,
    MultiPolygon, Point as GPoint, Polygon as GPolygon,
};

use crate::SRID_WGS84;

/// OGC WKB geometry type codes (2-D, no Z/M).
const WKB_POINT: u32 = 1;
const WKB_LINESTRING: u32 = 2;
const WKB_POLYGON: u32 = 3;
const WKB_MULTIPOINT: u32 = 4;
const WKB_MULTILINESTRING: u32 = 5;
const WKB_MULTIPOLYGON: u32 = 6;
const WKB_GEOMETRYCOLLECTION: u32 = 7;

/// PostGIS EWKB flag: SRID present in the wire bytes.
const EWKB_SRID_FLAG: u32 = 0x2000_0000;
/// Mask stripping the EWKB Z/M/SRID high bits, leaving the base type code.
const EWKB_TYPE_MASK: u32 = 0x0000_00FF;

/// Errors from the general geometry codecs.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum GeomError {
    #[error("WKB truncated: needed {needed} more bytes at offset {at}")]
    Truncated { at: usize, needed: usize },
    #[error("WKB endianness byte must be 0x00 or 0x01, got 0x{got:02x}")]
    BadEndian { got: u8 },
    #[error("unsupported WKB type code {got} (only 2-D types 1..=7 are supported)")]
    BadType { got: u32 },
    #[error("EWKB Z/M dimensions are not supported in v0.2 (type word 0x{word:08x})")]
    UnsupportedDim { word: u32 },
    #[error("WKT parse error: {0}")]
    Wkt(String),
    #[error("GeoJSON parse error: {0}")]
    GeoJson(String),
    #[error("trailing bytes after geometry: {0} byte(s) unconsumed")]
    Trailing(usize),
}

/// A decoded geometry plus the SRID recovered from the wire (0 = unknown).
#[derive(Debug, Clone, PartialEq)]
pub struct Geom {
    pub geometry: Geometry<f64>,
    pub srid: u32,
}

impl Geom {
    pub fn new(geometry: Geometry<f64>) -> Self {
        Self {
            geometry,
            srid: SRID_WGS84,
        }
    }
    pub fn with_srid(geometry: Geometry<f64>, srid: u32) -> Self {
        Self { geometry, srid }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WKB / EWKB encode
// ─────────────────────────────────────────────────────────────────────────────

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_f64(buf: &mut Vec<u8>, v: f64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_coord(buf: &mut Vec<u8>, c: Coord<f64>) {
    put_f64(buf, c.x);
    put_f64(buf, c.y);
}

/// Encode `g` as plain little-endian WKB (no SRID on the wire).
pub fn encode_wkb(g: &Geometry<f64>) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_geom_into(&mut buf, g, None);
    buf
}

/// Encode `g` as PostGIS EWKB with `srid` embedded in the top-level header.
pub fn encode_ewkb(g: &Geometry<f64>, srid: u32) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_geom_into(&mut buf, g, Some(srid));
    buf
}

/// Encode one geometry. `srid` is `Some` only for the outermost call when
/// EWKB is requested; nested members never carry their own SRID (PostGIS
/// convention: SRID rides only on the top-level header).
fn encode_geom_into(buf: &mut Vec<u8>, g: &Geometry<f64>, srid: Option<u32>) {
    buf.push(0x01); // little-endian
    let base = wkb_type_code(g);
    match srid {
        Some(s) => {
            put_u32(buf, base | EWKB_SRID_FLAG);
            put_u32(buf, s);
        }
        None => put_u32(buf, base),
    }
    match g {
        Geometry::Point(p) => put_coord(buf, p.0),
        Geometry::LineString(ls) => encode_linestring_body(buf, ls),
        Geometry::Polygon(poly) => encode_polygon_body(buf, poly),
        Geometry::MultiPoint(mp) => {
            put_u32(buf, mp.0.len() as u32);
            for p in &mp.0 {
                let g: Geometry<f64> = Geometry::Point(*p);
                encode_geom_into(buf, &g, None);
            }
        }
        Geometry::MultiLineString(ml) => {
            put_u32(buf, ml.0.len() as u32);
            for ls in &ml.0 {
                let g: Geometry<f64> = Geometry::LineString(ls.clone());
                encode_geom_into(buf, &g, None);
            }
        }
        Geometry::MultiPolygon(mpoly) => {
            put_u32(buf, mpoly.0.len() as u32);
            for poly in &mpoly.0 {
                let g: Geometry<f64> = Geometry::Polygon(poly.clone());
                encode_geom_into(buf, &g, None);
            }
        }
        Geometry::GeometryCollection(gc) => {
            put_u32(buf, gc.0.len() as u32);
            for m in &gc.0 {
                encode_geom_into(buf, m, None);
            }
        }
        // geo_types has Line / Rect / Triangle convenience shapes; normalise
        // them to their polygon/linestring equivalents so the WKB stays
        // OGC-canonical.
        Geometry::Line(l) => {
            // Rewrite header type to LINESTRING (we already wrote POINT? no —
            // wkb_type_code maps Line→LINESTRING, so the header is correct).
            let ls = GLineString::new(vec![l.start, l.end]);
            encode_linestring_body(buf, &ls);
        }
        Geometry::Rect(r) => encode_polygon_body(buf, &r.to_polygon()),
        Geometry::Triangle(t) => encode_polygon_body(buf, &t.to_polygon()),
    }
}

fn encode_linestring_body(buf: &mut Vec<u8>, ls: &GLineString<f64>) {
    put_u32(buf, ls.0.len() as u32);
    for c in &ls.0 {
        put_coord(buf, *c);
    }
}

fn encode_polygon_body(buf: &mut Vec<u8>, poly: &GPolygon<f64>) {
    let nrings = 1 + poly.interiors().len();
    put_u32(buf, nrings as u32);
    encode_ring(buf, poly.exterior());
    for ring in poly.interiors() {
        encode_ring(buf, ring);
    }
}

fn encode_ring(buf: &mut Vec<u8>, ring: &GLineString<f64>) {
    put_u32(buf, ring.0.len() as u32);
    for c in &ring.0 {
        put_coord(buf, *c);
    }
}

fn wkb_type_code(g: &Geometry<f64>) -> u32 {
    match g {
        Geometry::Point(_) => WKB_POINT,
        Geometry::Line(_) | Geometry::LineString(_) => WKB_LINESTRING,
        Geometry::Polygon(_) | Geometry::Rect(_) | Geometry::Triangle(_) => WKB_POLYGON,
        Geometry::MultiPoint(_) => WKB_MULTIPOINT,
        Geometry::MultiLineString(_) => WKB_MULTILINESTRING,
        Geometry::MultiPolygon(_) => WKB_MULTIPOLYGON,
        Geometry::GeometryCollection(_) => WKB_GEOMETRYCOLLECTION,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WKB / EWKB decode
// ─────────────────────────────────────────────────────────────────────────────

/// Cursor over a WKB byte slice with bounds-checked little/big-endian reads.
struct Cur<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Cur<'a> {
    fn new(b: &'a [u8]) -> Self {
        Cur { b, i: 0 }
    }
    fn u8(&mut self) -> Result<u8, GeomError> {
        if self.i >= self.b.len() {
            return Err(GeomError::Truncated {
                at: self.i,
                needed: 1,
            });
        }
        let v = self.b[self.i];
        self.i += 1;
        Ok(v)
    }
    fn u32(&mut self, le: bool) -> Result<u32, GeomError> {
        if self.i + 4 > self.b.len() {
            return Err(GeomError::Truncated {
                at: self.i,
                needed: 4,
            });
        }
        let bytes: [u8; 4] = self.b[self.i..self.i + 4].try_into().unwrap();
        self.i += 4;
        Ok(if le {
            u32::from_le_bytes(bytes)
        } else {
            u32::from_be_bytes(bytes)
        })
    }
    fn f64(&mut self, le: bool) -> Result<f64, GeomError> {
        if self.i + 8 > self.b.len() {
            return Err(GeomError::Truncated {
                at: self.i,
                needed: 8,
            });
        }
        let bytes: [u8; 8] = self.b[self.i..self.i + 8].try_into().unwrap();
        self.i += 8;
        Ok(if le {
            f64::from_le_bytes(bytes)
        } else {
            f64::from_be_bytes(bytes)
        })
    }
    fn coord(&mut self, le: bool) -> Result<Coord<f64>, GeomError> {
        let x = self.f64(le)?;
        let y = self.f64(le)?;
        Ok(Coord { x, y })
    }
}

/// Decode any WKB or EWKB blob into a [`Geom`] (geometry + recovered SRID).
/// Rejects trailing bytes so a malformed blob can't silently truncate.
pub fn decode_any(bytes: &[u8]) -> Result<Geom, GeomError> {
    let mut cur = Cur::new(bytes);
    let (geom, srid) = decode_geom(&mut cur, None)?;
    if cur.i != bytes.len() {
        return Err(GeomError::Trailing(bytes.len() - cur.i));
    }
    Ok(Geom {
        geometry: geom,
        srid: srid.unwrap_or(SRID_WGS84 as u32),
    })
}

/// Decode one geometry from `cur`. `inherited_srid` propagates the
/// top-level EWKB SRID to nested members (so a collection's members report
/// the collection's SRID). Returns the geometry and the SRID that applies.
fn decode_geom(
    cur: &mut Cur,
    inherited_srid: Option<u32>,
) -> Result<(Geometry<f64>, Option<u32>), GeomError> {
    let endian = cur.u8()?;
    let le = match endian {
        0x01 => true,
        0x00 => false,
        other => return Err(GeomError::BadEndian { got: other }),
    };
    let type_word = cur.u32(le)?;
    // Reject Z (0x8000_0000), M (0x4000_0000) dimension flags — 2-D only.
    if type_word & 0x8000_0000 != 0 || type_word & 0x4000_0000 != 0 {
        return Err(GeomError::UnsupportedDim { word: type_word });
    }
    let srid = if type_word & EWKB_SRID_FLAG != 0 {
        Some(cur.u32(le)?)
    } else {
        inherited_srid
    };
    let base = type_word & EWKB_TYPE_MASK;
    let geom = match base {
        WKB_POINT => Geometry::Point(GPoint(cur.coord(le)?)),
        WKB_LINESTRING => Geometry::LineString(decode_linestring_body(cur, le)?),
        WKB_POLYGON => Geometry::Polygon(decode_polygon_body(cur, le)?),
        WKB_MULTIPOINT => {
            let n = cur.u32(le)?;
            let mut pts = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let (g, _) = decode_geom(cur, srid)?;
                match g {
                    Geometry::Point(p) => pts.push(p),
                    _ => return Err(GeomError::BadType { got: WKB_MULTIPOINT }),
                }
            }
            Geometry::MultiPoint(MultiPoint(pts))
        }
        WKB_MULTILINESTRING => {
            let n = cur.u32(le)?;
            let mut lss = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let (g, _) = decode_geom(cur, srid)?;
                match g {
                    Geometry::LineString(ls) => lss.push(ls),
                    _ => {
                        return Err(GeomError::BadType {
                            got: WKB_MULTILINESTRING,
                        })
                    }
                }
            }
            Geometry::MultiLineString(MultiLineString(lss))
        }
        WKB_MULTIPOLYGON => {
            let n = cur.u32(le)?;
            let mut polys = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let (g, _) = decode_geom(cur, srid)?;
                match g {
                    Geometry::Polygon(p) => polys.push(p),
                    _ => {
                        return Err(GeomError::BadType {
                            got: WKB_MULTIPOLYGON,
                        })
                    }
                }
            }
            Geometry::MultiPolygon(MultiPolygon(polys))
        }
        WKB_GEOMETRYCOLLECTION => {
            let n = cur.u32(le)?;
            let mut members = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let (g, _) = decode_geom(cur, srid)?;
                members.push(g);
            }
            Geometry::GeometryCollection(GeometryCollection(members))
        }
        other => return Err(GeomError::BadType { got: other }),
    };
    Ok((geom, srid))
}

fn decode_linestring_body(cur: &mut Cur, le: bool) -> Result<GLineString<f64>, GeomError> {
    let n = cur.u32(le)?;
    let mut coords = Vec::with_capacity(n as usize);
    for _ in 0..n {
        coords.push(cur.coord(le)?);
    }
    Ok(GLineString::new(coords))
}

fn decode_polygon_body(cur: &mut Cur, le: bool) -> Result<GPolygon<f64>, GeomError> {
    let nrings = cur.u32(le)?;
    if nrings == 0 {
        return Ok(GPolygon::new(GLineString::new(vec![]), vec![]));
    }
    let exterior = decode_linestring_body(cur, le)?;
    let mut holes = Vec::with_capacity((nrings - 1) as usize);
    for _ in 1..nrings {
        holes.push(decode_linestring_body(cur, le)?);
    }
    Ok(GPolygon::new(exterior, holes))
}

// ─────────────────────────────────────────────────────────────────────────────
// WKT
// ─────────────────────────────────────────────────────────────────────────────

/// Format an `f64` the way PostGIS `ST_AsText` does: shortest round-trip
/// decimal with no trailing `.0` (so `1.0` prints as `1`). Rust's default
/// `{}` formatter already produces shortest round-trip; we additionally
/// trim a trailing `.0` if present to match PostGIS integer-valued output.
fn fmt_num(v: f64) -> String {
    let s = format!("{v}");
    // `{}` never emits scientific notation for the magnitudes geocoords use,
    // and prints `1` for `1.0`, so no further trimming is required. Kept as a
    // single function so WKT and the legacy point formatter stay consistent.
    s
}

fn coord_wkt(c: &Coord<f64>) -> String {
    format!("{} {}", fmt_num(c.x), fmt_num(c.y))
}

fn ring_wkt(ls: &GLineString<f64>) -> String {
    let pts: Vec<String> = ls.0.iter().map(coord_wkt).collect();
    format!("({})", pts.join(", "))
}

/// Encode `g` as PostGIS WKT (`ST_AsText`).
pub fn encode_wkt(g: &Geometry<f64>) -> String {
    match g {
        Geometry::Point(p) => format!("POINT({})", coord_wkt(&p.0)),
        Geometry::Line(l) => {
            format!("LINESTRING({}, {})", coord_wkt(&l.start), coord_wkt(&l.end))
        }
        Geometry::LineString(ls) => {
            let pts: Vec<String> = ls.0.iter().map(coord_wkt).collect();
            format!("LINESTRING({})", pts.join(", "))
        }
        Geometry::Polygon(poly) => format!("POLYGON{}", polygon_rings_wkt(poly)),
        Geometry::Rect(r) => format!("POLYGON{}", polygon_rings_wkt(&r.to_polygon())),
        Geometry::Triangle(t) => format!("POLYGON{}", polygon_rings_wkt(&t.to_polygon())),
        Geometry::MultiPoint(mp) => {
            let pts: Vec<String> = mp.0.iter().map(|p| coord_wkt(&p.0)).collect();
            format!("MULTIPOINT({})", pts.join(", "))
        }
        Geometry::MultiLineString(ml) => {
            let parts: Vec<String> = ml.0.iter().map(ring_wkt).collect();
            format!("MULTILINESTRING({})", parts.join(", "))
        }
        Geometry::MultiPolygon(mpoly) => {
            let parts: Vec<String> = mpoly.0.iter().map(polygon_rings_wkt).collect();
            format!("MULTIPOLYGON({})", parts.join(", "))
        }
        Geometry::GeometryCollection(gc) => {
            let parts: Vec<String> = gc.0.iter().map(encode_wkt).collect();
            format!("GEOMETRYCOLLECTION({})", parts.join(", "))
        }
    }
}

fn polygon_rings_wkt(poly: &GPolygon<f64>) -> String {
    let mut rings = vec![ring_wkt(poly.exterior())];
    for h in poly.interiors() {
        rings.push(ring_wkt(h));
    }
    format!("({})", rings.join(", "))
}

/// Parse PostGIS WKT into a geometry. Handles the full 2-D type set.
pub fn decode_wkt(s: &str) -> Result<Geometry<f64>, GeomError> {
    let mut p = WktParser::new(s);
    let g = p.parse_geometry()?;
    p.skip_ws();
    if !p.eof() {
        return Err(GeomError::Wkt(format!("trailing input: {:?}", p.rest())));
    }
    Ok(g)
}

struct WktParser<'a> {
    s: &'a [u8],
    i: usize,
    src: &'a str,
}

impl<'a> WktParser<'a> {
    fn new(src: &'a str) -> Self {
        WktParser {
            s: src.as_bytes(),
            i: 0,
            src,
        }
    }
    fn rest(&self) -> &str {
        &self.src[self.i.min(self.src.len())..]
    }
    fn eof(&self) -> bool {
        self.i >= self.s.len()
    }
    fn skip_ws(&mut self) {
        while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }
    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }
    fn expect(&mut self, c: u8) -> Result<(), GeomError> {
        self.skip_ws();
        if self.peek() == Some(c) {
            self.i += 1;
            Ok(())
        } else {
            Err(GeomError::Wkt(format!(
                "expected {:?} at {:?}",
                c as char,
                self.rest()
            )))
        }
    }
    /// Read an alphabetic tag (geometry keyword), upper-cased.
    fn read_tag(&mut self) -> Result<String, GeomError> {
        self.skip_ws();
        let start = self.i;
        while self.i < self.s.len() && self.s[self.i].is_ascii_alphabetic() {
            self.i += 1;
        }
        if self.i == start {
            return Err(GeomError::Wkt(format!(
                "expected geometry type tag at {:?}",
                self.rest()
            )));
        }
        Ok(self.src[start..self.i].to_ascii_uppercase())
    }
    fn read_number(&mut self) -> Result<f64, GeomError> {
        self.skip_ws();
        let start = self.i;
        while self.i < self.s.len() {
            let c = self.s[self.i];
            if c.is_ascii_digit() || c == b'.' || c == b'-' || c == b'+' || c == b'e' || c == b'E' {
                self.i += 1;
            } else {
                break;
            }
        }
        if self.i == start {
            return Err(GeomError::Wkt(format!(
                "expected a number at {:?}",
                self.rest()
            )));
        }
        self.src[start..self.i]
            .parse::<f64>()
            .map_err(|e| GeomError::Wkt(format!("bad number {:?}: {e}", &self.src[start..self.i])))
    }
    fn read_coord(&mut self) -> Result<Coord<f64>, GeomError> {
        let x = self.read_number()?;
        let y = self.read_number()?;
        // Reject Z/M (a third number before the delimiter) — 2-D only.
        self.skip_ws();
        if let Some(c) = self.peek() {
            if c.is_ascii_digit() || c == b'-' || c == b'+' || c == b'.' {
                return Err(GeomError::Wkt(
                    "Z/M coordinates not supported in v0.2".into(),
                ));
            }
        }
        Ok(Coord { x, y })
    }
    /// Parse a parenthesised, comma-separated coordinate list `(x y, x y)`.
    fn read_coord_list(&mut self) -> Result<Vec<Coord<f64>>, GeomError> {
        self.expect(b'(')?;
        let mut out = Vec::new();
        loop {
            out.push(self.read_coord()?);
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.i += 1;
                }
                Some(b')') => {
                    self.i += 1;
                    break;
                }
                other => {
                    return Err(GeomError::Wkt(format!(
                        "expected ',' or ')' in coord list, got {:?}",
                        other.map(|c| c as char)
                    )))
                }
            }
        }
        Ok(out)
    }
    /// Parse `((ring), (ring), …)` for polygons.
    fn read_ring_list(&mut self) -> Result<Vec<GLineString<f64>>, GeomError> {
        self.expect(b'(')?;
        let mut rings = Vec::new();
        loop {
            rings.push(GLineString::new(self.read_coord_list()?));
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.i += 1;
                }
                Some(b')') => {
                    self.i += 1;
                    break;
                }
                other => {
                    return Err(GeomError::Wkt(format!(
                        "expected ',' or ')' in ring list, got {:?}",
                        other.map(|c| c as char)
                    )))
                }
            }
        }
        Ok(rings)
    }

    fn parse_geometry(&mut self) -> Result<Geometry<f64>, GeomError> {
        let tag = self.read_tag()?;
        match tag.as_str() {
            "POINT" => {
                let coords = self.read_coord_list()?;
                if coords.len() != 1 {
                    return Err(GeomError::Wkt(format!(
                        "POINT expects exactly one coordinate, got {}",
                        coords.len()
                    )));
                }
                Ok(Geometry::Point(GPoint(coords[0])))
            }
            "LINESTRING" => Ok(Geometry::LineString(GLineString::new(
                self.read_coord_list()?,
            ))),
            "POLYGON" => {
                let rings = self.read_ring_list()?;
                Ok(Geometry::Polygon(rings_to_polygon(rings)))
            }
            "MULTIPOINT" => {
                // PostGIS accepts both MULTIPOINT(0 0, 1 1) and
                // MULTIPOINT((0 0), (1 1)). Detect the nested form.
                self.skip_ws();
                self.expect(b'(')?;
                let mut pts = Vec::new();
                loop {
                    self.skip_ws();
                    if self.peek() == Some(b'(') {
                        let one = self.read_coord_list()?;
                        if one.len() != 1 {
                            return Err(GeomError::Wkt(
                                "MULTIPOINT member must be a single coordinate".into(),
                            ));
                        }
                        pts.push(GPoint(one[0]));
                    } else {
                        pts.push(GPoint(self.read_coord()?));
                    }
                    self.skip_ws();
                    match self.peek() {
                        Some(b',') => self.i += 1,
                        Some(b')') => {
                            self.i += 1;
                            break;
                        }
                        other => {
                            return Err(GeomError::Wkt(format!(
                                "expected ',' or ')' in MULTIPOINT, got {:?}",
                                other.map(|c| c as char)
                            )))
                        }
                    }
                }
                Ok(Geometry::MultiPoint(MultiPoint(pts)))
            }
            "MULTILINESTRING" => {
                let rings = self.read_ring_list()?;
                Ok(Geometry::MultiLineString(MultiLineString(rings)))
            }
            "MULTIPOLYGON" => {
                // ((ring,ring),(ring)) — a list of ring-lists.
                self.expect(b'(')?;
                let mut polys = Vec::new();
                loop {
                    let rings = self.read_ring_list()?;
                    polys.push(rings_to_polygon(rings));
                    self.skip_ws();
                    match self.peek() {
                        Some(b',') => self.i += 1,
                        Some(b')') => {
                            self.i += 1;
                            break;
                        }
                        other => {
                            return Err(GeomError::Wkt(format!(
                                "expected ',' or ')' in MULTIPOLYGON, got {:?}",
                                other.map(|c| c as char)
                            )))
                        }
                    }
                }
                Ok(Geometry::MultiPolygon(MultiPolygon(polys)))
            }
            "GEOMETRYCOLLECTION" => {
                self.expect(b'(')?;
                let mut members = Vec::new();
                // Empty collection: GEOMETRYCOLLECTION()
                self.skip_ws();
                if self.peek() == Some(b')') {
                    self.i += 1;
                    return Ok(Geometry::GeometryCollection(GeometryCollection(members)));
                }
                loop {
                    members.push(self.parse_geometry()?);
                    self.skip_ws();
                    match self.peek() {
                        Some(b',') => self.i += 1,
                        Some(b')') => {
                            self.i += 1;
                            break;
                        }
                        other => {
                            return Err(GeomError::Wkt(format!(
                                "expected ',' or ')' in GEOMETRYCOLLECTION, got {:?}",
                                other.map(|c| c as char)
                            )))
                        }
                    }
                }
                Ok(Geometry::GeometryCollection(GeometryCollection(members)))
            }
            other => Err(GeomError::Wkt(format!("unknown geometry type {other:?}"))),
        }
    }
}

fn rings_to_polygon(mut rings: Vec<GLineString<f64>>) -> GPolygon<f64> {
    if rings.is_empty() {
        return GPolygon::new(GLineString::new(vec![]), vec![]);
    }
    let exterior = rings.remove(0);
    GPolygon::new(exterior, rings)
}

// ─────────────────────────────────────────────────────────────────────────────
// GeoJSON
// ─────────────────────────────────────────────────────────────────────────────

/// Encode `g` as an RFC 7946 GeoJSON geometry object.
///
/// The outer object is assembled by hand so the `"type"` key always precedes
/// `"coordinates"` / `"geometries"`, matching PostGIS `ST_AsGeoJSON` (and the
/// fixed-21-byte POINT fast-path encoder). `serde_json::Value` is a `BTreeMap`
/// by default — round-tripping the object through it would re-sort the keys
/// alphabetically (`coordinates` before `type`), which diverges from PostGIS;
/// emitting the wrapper directly avoids that. The coordinate/geometry payloads
/// still go through `serde_json` so float formatting and escaping stay correct.
pub fn encode_geojson(g: &Geometry<f64>) -> String {
    use serde_json::{json, Value};
    fn coord_json(c: &Coord<f64>) -> Value {
        json!([c.x, c.y])
    }
    fn ring_json(ls: &GLineString<f64>) -> Value {
        Value::Array(ls.0.iter().map(coord_json).collect())
    }
    fn polygon_json(poly: &GPolygon<f64>) -> Value {
        let mut rings = vec![ring_json(poly.exterior())];
        for h in poly.interiors() {
            rings.push(ring_json(h));
        }
        Value::Array(rings)
    }
    // Render `{"type":"<ty>","coordinates":<payload>}` with type first.
    fn wrap(ty: &str, key: &str, payload: &Value) -> String {
        format!(
            "{{\"type\":\"{ty}\",\"{key}\":{}}}",
            serde_json::to_string(payload).expect("geometry json always serialises")
        )
    }
    match g {
        Geometry::Point(p) => wrap("Point", "coordinates", &coord_json(&p.0)),
        Geometry::Line(l) => wrap(
            "LineString",
            "coordinates",
            &json!([coord_json(&l.start), coord_json(&l.end)]),
        ),
        Geometry::LineString(ls) => wrap("LineString", "coordinates", &ring_json(ls)),
        Geometry::Polygon(poly) => wrap("Polygon", "coordinates", &polygon_json(poly)),
        Geometry::Rect(r) => {
            wrap("Polygon", "coordinates", &polygon_json(&r.to_polygon()))
        }
        Geometry::Triangle(t) => {
            wrap("Polygon", "coordinates", &polygon_json(&t.to_polygon()))
        }
        Geometry::MultiPoint(mp) => wrap(
            "MultiPoint",
            "coordinates",
            &Value::Array(mp.0.iter().map(|p| coord_json(&p.0)).collect()),
        ),
        Geometry::MultiLineString(ml) => wrap(
            "MultiLineString",
            "coordinates",
            &Value::Array(ml.0.iter().map(ring_json).collect()),
        ),
        Geometry::MultiPolygon(mpoly) => wrap(
            "MultiPolygon",
            "coordinates",
            &Value::Array(mpoly.0.iter().map(polygon_json).collect()),
        ),
        Geometry::GeometryCollection(gc) => {
            // Members are already type-first strings; join them into the array
            // verbatim so nested ordering is preserved too.
            let members: Vec<String> = gc.0.iter().map(encode_geojson).collect();
            format!(
                "{{\"type\":\"GeometryCollection\",\"geometries\":[{}]}}",
                members.join(",")
            )
        }
    }
}

/// Decode an RFC 7946 GeoJSON geometry object into a geometry.
pub fn decode_geojson(s: &str) -> Result<Geometry<f64>, GeomError> {
    let v: serde_json::Value =
        serde_json::from_str(s).map_err(|e| GeomError::GeoJson(e.to_string()))?;
    geojson_value(&v)
}

fn geojson_value(v: &serde_json::Value) -> Result<Geometry<f64>, GeomError> {
    let obj = v
        .as_object()
        .ok_or_else(|| GeomError::GeoJson("expected a JSON object".into()))?;
    let ty = obj
        .get("type")
        .and_then(|t| t.as_str())
        .ok_or_else(|| GeomError::GeoJson("missing \"type\"".into()))?;
    match ty {
        "Point" => Ok(Geometry::Point(GPoint(json_coord(coords(obj)?)?))),
        "LineString" => Ok(Geometry::LineString(json_linestring(coords(obj)?)?)),
        "Polygon" => Ok(Geometry::Polygon(json_polygon(coords(obj)?)?)),
        "MultiPoint" => {
            let arr = coords(obj)?
                .as_array()
                .ok_or_else(|| GeomError::GeoJson("MultiPoint coordinates must be array".into()))?;
            let mut pts = Vec::with_capacity(arr.len());
            for c in arr {
                pts.push(GPoint(json_coord(c)?));
            }
            Ok(Geometry::MultiPoint(MultiPoint(pts)))
        }
        "MultiLineString" => {
            let arr = coords(obj)?.as_array().ok_or_else(|| {
                GeomError::GeoJson("MultiLineString coordinates must be array".into())
            })?;
            let mut lss = Vec::with_capacity(arr.len());
            for c in arr {
                lss.push(json_linestring(c)?);
            }
            Ok(Geometry::MultiLineString(MultiLineString(lss)))
        }
        "MultiPolygon" => {
            let arr = coords(obj)?.as_array().ok_or_else(|| {
                GeomError::GeoJson("MultiPolygon coordinates must be array".into())
            })?;
            let mut polys = Vec::with_capacity(arr.len());
            for c in arr {
                polys.push(json_polygon(c)?);
            }
            Ok(Geometry::MultiPolygon(MultiPolygon(polys)))
        }
        "GeometryCollection" => {
            let arr = obj
                .get("geometries")
                .and_then(|g| g.as_array())
                .ok_or_else(|| {
                    GeomError::GeoJson("GeometryCollection missing \"geometries\" array".into())
                })?;
            let mut members = Vec::with_capacity(arr.len());
            for m in arr {
                members.push(geojson_value(m)?);
            }
            Ok(Geometry::GeometryCollection(GeometryCollection(members)))
        }
        other => Err(GeomError::GeoJson(format!("unknown geometry type {other:?}"))),
    }
}

fn coords<'a>(
    obj: &'a serde_json::Map<String, serde_json::Value>,
) -> Result<&'a serde_json::Value, GeomError> {
    obj.get("coordinates")
        .ok_or_else(|| GeomError::GeoJson("missing \"coordinates\"".into()))
}

fn json_coord(v: &serde_json::Value) -> Result<Coord<f64>, GeomError> {
    let arr = v
        .as_array()
        .ok_or_else(|| GeomError::GeoJson("coordinate must be an array".into()))?;
    if arr.len() < 2 {
        return Err(GeomError::GeoJson(format!(
            "coordinate needs at least 2 numbers, got {}",
            arr.len()
        )));
    }
    if arr.len() > 2 {
        return Err(GeomError::GeoJson(
            "Z/M coordinates not supported in v0.2".into(),
        ));
    }
    let x = arr[0]
        .as_f64()
        .ok_or_else(|| GeomError::GeoJson("coordinate x is not a number".into()))?;
    let y = arr[1]
        .as_f64()
        .ok_or_else(|| GeomError::GeoJson("coordinate y is not a number".into()))?;
    Ok(Coord { x, y })
}

fn json_linestring(v: &serde_json::Value) -> Result<GLineString<f64>, GeomError> {
    let arr = v
        .as_array()
        .ok_or_else(|| GeomError::GeoJson("LineString coordinates must be array".into()))?;
    let mut coords = Vec::with_capacity(arr.len());
    for c in arr {
        coords.push(json_coord(c)?);
    }
    Ok(GLineString::new(coords))
}

fn json_polygon(v: &serde_json::Value) -> Result<GPolygon<f64>, GeomError> {
    let arr = v
        .as_array()
        .ok_or_else(|| GeomError::GeoJson("Polygon coordinates must be array".into()))?;
    let mut rings = Vec::with_capacity(arr.len());
    for r in arr {
        rings.push(json_linestring(r)?);
    }
    Ok(rings_to_polygon(rings))
}

// ─────────────────────────────────────────────────────────────────────────────
// Type-name helper
// ─────────────────────────────────────────────────────────────────────────────

/// PostGIS `ST_GeometryType` string (e.g. `"ST_LineString"`).
pub fn geometry_type_name(g: &Geometry<f64>) -> &'static str {
    match g {
        Geometry::Point(_) => "ST_Point",
        Geometry::Line(_) | Geometry::LineString(_) => "ST_LineString",
        Geometry::Polygon(_) | Geometry::Rect(_) | Geometry::Triangle(_) => "ST_Polygon",
        Geometry::MultiPoint(_) => "ST_MultiPoint",
        Geometry::MultiLineString(_) => "ST_MultiLineString",
        Geometry::MultiPolygon(_) => "ST_MultiPolygon",
        Geometry::GeometryCollection(_) => "ST_GeometryCollection",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt_wkb(g: &Geometry<f64>) {
        let bytes = encode_wkb(g);
        let back = decode_any(&bytes).unwrap();
        assert_eq!(&back.geometry, g, "WKB round-trip mismatch");
    }
    fn rt_wkt(g: &Geometry<f64>) {
        let s = encode_wkt(g);
        let back = decode_wkt(&s).unwrap();
        assert_eq!(&back, g, "WKT round-trip mismatch via {s:?}");
    }
    fn rt_geojson(g: &Geometry<f64>) {
        let s = encode_geojson(g);
        let back = decode_geojson(&s).unwrap();
        assert_eq!(&back, g, "GeoJSON round-trip mismatch via {s:?}");
    }

    fn sample_linestring() -> Geometry<f64> {
        Geometry::LineString(GLineString::new(vec![
            Coord { x: 0.0, y: 0.0 },
            Coord { x: 3.0, y: 4.0 },
            Coord { x: 6.0, y: 4.0 },
        ]))
    }
    fn sample_polygon() -> Geometry<f64> {
        // Unit square with a hole.
        let ext = GLineString::new(vec![
            Coord { x: 0.0, y: 0.0 },
            Coord { x: 4.0, y: 0.0 },
            Coord { x: 4.0, y: 4.0 },
            Coord { x: 0.0, y: 4.0 },
            Coord { x: 0.0, y: 0.0 },
        ]);
        let hole = GLineString::new(vec![
            Coord { x: 1.0, y: 1.0 },
            Coord { x: 2.0, y: 1.0 },
            Coord { x: 2.0, y: 2.0 },
            Coord { x: 1.0, y: 2.0 },
            Coord { x: 1.0, y: 1.0 },
        ]);
        Geometry::Polygon(GPolygon::new(ext, vec![hole]))
    }

    #[test]
    fn roundtrip_all_types() {
        let geoms = vec![
            Geometry::Point(GPoint(Coord { x: 1.5, y: 2.5 })),
            sample_linestring(),
            sample_polygon(),
            Geometry::MultiPoint(MultiPoint(vec![
                GPoint(Coord { x: 0.0, y: 0.0 }),
                GPoint(Coord { x: 1.0, y: 1.0 }),
            ])),
            Geometry::MultiLineString(MultiLineString(vec![
                GLineString::new(vec![Coord { x: 0.0, y: 0.0 }, Coord { x: 1.0, y: 1.0 }]),
                GLineString::new(vec![Coord { x: 2.0, y: 2.0 }, Coord { x: 3.0, y: 3.0 }]),
            ])),
            Geometry::MultiPolygon(MultiPolygon(vec![
                match sample_polygon() {
                    Geometry::Polygon(p) => p,
                    _ => unreachable!(),
                },
            ])),
            Geometry::GeometryCollection(GeometryCollection(vec![
                Geometry::Point(GPoint(Coord { x: 9.0, y: 8.0 })),
                sample_linestring(),
            ])),
        ];
        for g in &geoms {
            rt_wkb(g);
            rt_wkt(g);
            rt_geojson(g);
        }
    }

    #[test]
    fn ewkb_carries_srid() {
        let g = sample_linestring();
        let bytes = encode_ewkb(&g, 4326);
        let back = decode_any(&bytes).unwrap();
        assert_eq!(back.srid, 4326);
        assert_eq!(back.geometry, g);
        // Plain WKB → SRID defaults to WGS84.
        let plain = encode_wkb(&g);
        assert_eq!(decode_any(&plain).unwrap().srid, SRID_WGS84);
    }

    #[test]
    fn truncated_wkb_errors() {
        let mut bytes = encode_wkb(&sample_linestring());
        bytes.truncate(bytes.len() - 4);
        assert!(matches!(decode_any(&bytes), Err(GeomError::Truncated { .. })));
    }

    #[test]
    fn trailing_bytes_rejected() {
        let mut bytes = encode_wkb(&Geometry::Point(GPoint(Coord { x: 1.0, y: 2.0 })));
        bytes.push(0xFF);
        assert!(matches!(decode_any(&bytes), Err(GeomError::Trailing(1))));
    }

    #[test]
    fn wkt_formats_integers_without_decimal() {
        let g = Geometry::Point(GPoint(Coord { x: 1.0, y: 2.0 }));
        assert_eq!(encode_wkt(&g), "POINT(1 2)");
    }

    #[test]
    fn wkt_multipoint_both_forms_parse() {
        let flat = decode_wkt("MULTIPOINT(0 0, 1 1)").unwrap();
        let nested = decode_wkt("MULTIPOINT((0 0), (1 1))").unwrap();
        assert_eq!(flat, nested);
    }

    #[test]
    fn wkt_rejects_z_coordinate() {
        assert!(matches!(
            decode_wkt("POINT(1 2 3)"),
            Err(GeomError::Wkt(_))
        ));
    }

    #[test]
    fn geojson_rejects_3d() {
        assert!(matches!(
            decode_geojson(r#"{"type":"Point","coordinates":[1,2,3]}"#),
            Err(GeomError::GeoJson(_))
        ));
    }
}
