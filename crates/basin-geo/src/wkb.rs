//! WKB (Well-Known Binary) encode/decode for [`Point`].
//!
//! PostGIS's WKB layout for a 2D `POINT` is, in order:
//!
//! | Bytes | Meaning                                          |
//! |-------|--------------------------------------------------|
//! | 1     | Endianness: `0x00` big-endian, `0x01` little-end |
//! | 4     | Type code: `0x0000_0001` for POINT (no SRID flag)|
//! | 8     | X (longitude) as IEEE-754 `f64`                  |
//! | 8     | Y (latitude) as IEEE-754 `f64`                   |
//!
//! Total: **21 bytes** per point. That's the size we pin into Arrow as
//! `FixedSizeBinary(21)` in the engine glue (see `geo_glue.rs`).
//!
//! ## EWKB and SRID
//!
//! PostGIS also speaks "EWKB" — the same layout but with the type code
//! OR'd against `0x2000_0000` and a 4-byte SRID inserted between the
//! type code and the coordinates. EWKB blobs are 25 bytes per point.
//! v0.1 of basin-geo only emits/parses the 21-byte plain WKB (with an
//! implicit SRID 4326) because every byte of per-row overhead is
//! per-tenant cost. EWKB support — needed for non-WGS84 round-trips
//! against psql clients that emit EWKB — is on the v0.2 list.
//!
//! ## Endianness
//!
//! We always write little-endian (`0x01`) on the wire — that's what
//! psql + libpq emit on every architecture we care about and what the
//! PostGIS docs use as the "canonical" form. The decoder accepts both.

use crate::types::{Point, SRID_WGS84};

/// On-the-wire size of a 2D POINT in WKB bytes. Exposed so the engine glue
/// can pin the Arrow `FixedSizeBinary` width to it without redefining the
/// constant.
pub const POINT_WKB_LEN: usize = 21;

/// WKB type code for a 2D POINT, no SRID flag.
const WKB_TYPE_POINT: u32 = 1;

/// Encode `p` as a 21-byte little-endian WKB blob. SRID is dropped on the
/// wire (it's implicit 4326 in v0.1).
pub fn encode_point(p: &Point) -> [u8; POINT_WKB_LEN] {
    let mut out = [0u8; POINT_WKB_LEN];
    out[0] = 0x01; // little-endian
    out[1..5].copy_from_slice(&WKB_TYPE_POINT.to_le_bytes());
    out[5..13].copy_from_slice(&p.x.to_le_bytes());
    out[13..21].copy_from_slice(&p.y.to_le_bytes());
    out
}

/// Errors decoding a WKB blob into a [`Point`].
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum WkbError {
    #[error("WKB blob has wrong length: got {got}, want {POINT_WKB_LEN}")]
    BadLength { got: usize },
    #[error("WKB endianness byte must be 0x00 or 0x01, got 0x{got:02x}")]
    BadEndianness { got: u8 },
    #[error("WKB type code must be 1 (POINT), got {got}")]
    BadType { got: u32 },
}

/// Decode a 21-byte WKB blob into a [`Point`]. Accepts both endianness
/// bytes; rejects EWKB (which would have the SRID bit set in the type code).
pub fn decode_point(bytes: &[u8]) -> Result<Point, WkbError> {
    if bytes.len() != POINT_WKB_LEN {
        return Err(WkbError::BadLength { got: bytes.len() });
    }
    let endian = bytes[0];
    let type_bytes: [u8; 4] = bytes[1..5].try_into().unwrap();
    let x_bytes: [u8; 8] = bytes[5..13].try_into().unwrap();
    let y_bytes: [u8; 8] = bytes[13..21].try_into().unwrap();
    let (type_code, x, y) = match endian {
        0x01 => (
            u32::from_le_bytes(type_bytes),
            f64::from_le_bytes(x_bytes),
            f64::from_le_bytes(y_bytes),
        ),
        0x00 => (
            u32::from_be_bytes(type_bytes),
            f64::from_be_bytes(x_bytes),
            f64::from_be_bytes(y_bytes),
        ),
        other => return Err(WkbError::BadEndianness { got: other }),
    };
    if type_code != WKB_TYPE_POINT {
        return Err(WkbError::BadType { got: type_code });
    }
    Ok(Point {
        x,
        y,
        srid: SRID_WGS84,
    })
}

/// Convenience: render a 21-byte WKB blob as the lower-case hex string
/// `psql` would print (PostGIS's `ST_AsEWKB(...)::text` minus the EWKB
/// SRID extension). Useful for diff-friendly snapshot tests.
pub fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Inverse of [`to_hex`]: parse a lower-or-upper-case hex string into
/// raw bytes. Returns [`WkbError::BadLength`] if the input has odd length.
pub fn from_hex(s: &str) -> Result<Vec<u8>, WkbError> {
    if s.len() % 2 != 0 {
        return Err(WkbError::BadLength { got: s.len() });
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for chunk in bytes.chunks_exact(2) {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Result<u8, WkbError> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(10 + b - b'a'),
        b'A'..=b'F' => Ok(10 + b - b'A'),
        _ => Err(WkbError::BadEndianness { got: b }),
    }
}
