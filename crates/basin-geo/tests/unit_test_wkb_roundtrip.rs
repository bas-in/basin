//! WKB encode/decode round-trip + a snapshot test against the exact
//! 21-byte hex string PostGIS would emit for `POINT(1.0, 2.0)` in
//! SRID 4326 (in plain WKB form, no EWKB SRID prefix).

use basin_geo::{decode_point, encode_point, from_hex, to_hex, Point, POINT_WKB_LEN};

/// PostGIS hex for plain WKB `POINT(1.0, 2.0)` in little-endian:
///
///   01                          -- LE
///   01000000                    -- type=1 (POINT)
///   000000000000F03F            -- 1.0 (f64 LE)
///   0000000000000040            -- 2.0 (f64 LE)
const POINT_1_2_HEX: &str = "0101000000000000000000f03f0000000000000040";

#[test]
fn point_wkb_is_exactly_21_bytes() {
    assert_eq!(POINT_WKB_LEN, 21);
    let p = Point::new(1.0, 2.0);
    assert_eq!(encode_point(&p).len(), 21);
}

#[test]
fn encode_matches_postgis_hex_for_point_1_2() {
    let p = Point::new(1.0, 2.0);
    let bytes = encode_point(&p);
    let hex = to_hex(&bytes);
    assert_eq!(
        hex, POINT_1_2_HEX,
        "WKB hex must match PostGIS canonical layout"
    );
}

#[test]
fn decode_inverts_encode() {
    let p = Point::new(2.2945, 48.8584); // Eiffel
    let bytes = encode_point(&p);
    let q = decode_point(&bytes).unwrap();
    assert_eq!(p.x, q.x);
    assert_eq!(p.y, q.y);
    assert_eq!(p.srid, q.srid);
}

#[test]
fn decode_postgis_hex_round_trips() {
    let bytes = from_hex(POINT_1_2_HEX).unwrap();
    let p = decode_point(&bytes).unwrap();
    assert_eq!(p.x, 1.0);
    assert_eq!(p.y, 2.0);
    assert_eq!(p.srid, 4326);
}

#[test]
fn decode_accepts_big_endian() {
    // Same point as POINT_1_2_HEX, but big-endian on the wire.
    let mut be = [0u8; 21];
    be[0] = 0x00;
    be[1..5].copy_from_slice(&1u32.to_be_bytes());
    be[5..13].copy_from_slice(&1.0f64.to_be_bytes());
    be[13..21].copy_from_slice(&2.0f64.to_be_bytes());
    let p = decode_point(&be).unwrap();
    assert_eq!(p.x, 1.0);
    assert_eq!(p.y, 2.0);
}

#[test]
fn decode_rejects_wrong_length() {
    let err = decode_point(&[0u8; 20]).unwrap_err();
    assert!(matches!(
        err,
        basin_geo::WkbError::BadLength { got: 20 }
    ));
}

#[test]
fn decode_rejects_unknown_type_code() {
    // Same shape but type code = 2 (LINESTRING). v0.1 only handles POINT.
    let mut bytes = [0u8; 21];
    bytes[0] = 0x01;
    bytes[1..5].copy_from_slice(&2u32.to_le_bytes());
    let err = decode_point(&bytes).unwrap_err();
    assert!(matches!(err, basin_geo::WkbError::BadType { got: 2 }));
}
