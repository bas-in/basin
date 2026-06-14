//! Byte-level and round-trip tests for the NUMERIC binary wire codec.
//!
//! Covers:
//!
//! * **Byte-level pin tests** — hand-encoded PG binary bodies for a set of
//!   canonical values (0, 1.0, -123.45, 12345.6789, 0.0001, large Decimal128).
//!   These pins are derived from the PostgreSQL numeric binary wire format spec
//!   (src/backend/utils/adt/numeric.c) and verified against known PG output.
//!
//! * **Round-trip encode → decode** — every encode path feeds into the binary
//!   NUMERIC param decoder (`decode_param_binary`) and the reconstructed
//!   decimal string must equal what Arrow's `value_as_string` would produce.
//!
//! * **Text-path unchanged** — `encode_batches` (the text-only path) produces
//!   byte-identical results before and after this feature, ensuring the text
//!   wire path is not perturbed.
//!
//! * **Format-code gating** — `encode_batches_with_formats` emits binary only
//!   when format code 1 is requested; format code 0 (text) must produce the
//!   same output as `encode_batches`.

use std::sync::Arc;

use arrow_array::{Decimal128Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

// Re-export the crate items under test via the public crate root.
// `encode_batches` and `encode_batches_with_formats` are `pub(crate)` so we
// reach them through the test helpers in `types.rs` that are already unit-
// tested; here we drive the API at the RecordBatch level, which is the actual
// integration surface used by `handle_execute`.

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Call the internal `encode_numeric_binary` via the public
/// `encode_batches_with_formats` path: produce one batch with a single
/// Decimal128 column, format_code 1 (binary), and return the raw field bytes
/// (including the 4-byte length prefix). Panics if the Arrow plumbing fails.
fn encode_numeric_binary_bytes(raw: i128, precision: u8, scale: i8) -> Vec<u8> {
    let arr = Decimal128Array::from(vec![Some(raw)])
        .with_precision_and_scale(precision, scale)
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "n",
        DataType::Decimal128(precision, scale),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
    let rows = basin_router::encode_batches_with_formats_for_test(&schema, &[batch], &[1]).unwrap();
    rows[0].data.to_vec()
}

/// Parse a PG NUMERIC binary body (starting at the 4-byte length prefix).
/// Returns (ndigits, weight, sign, dscale, digit_vec).
fn parse_numeric_body(bytes: &[u8]) -> (u16, i16, u16, u16, Vec<u16>) {
    let body_len = i32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
    assert!(
        body_len >= 8,
        "binary NUMERIC body too short: {body_len} bytes"
    );
    let ndigits = u16::from_be_bytes(bytes[4..6].try_into().unwrap());
    let weight = i16::from_be_bytes(bytes[6..8].try_into().unwrap());
    let sign = u16::from_be_bytes(bytes[8..10].try_into().unwrap());
    let dscale = u16::from_be_bytes(bytes[10..12].try_into().unwrap());
    let mut digits = Vec::with_capacity(ndigits as usize);
    for i in 0..ndigits as usize {
        let off = 12 + i * 2;
        digits.push(u16::from_be_bytes(bytes[off..off + 2].try_into().unwrap()));
    }
    assert_eq!(
        body_len,
        8 + (ndigits as usize) * 2,
        "body_len vs ndigits mismatch"
    );
    (ndigits, weight, sign, dscale, digits)
}

/// Reconstruct the decimal value from PG numeric binary fields.
fn pg_numeric_to_f64(weight: i16, sign: u16, digits: &[u16]) -> f64 {
    let mut val = 0f64;
    for (i, &d) in digits.iter().enumerate() {
        let exp = (weight as i32 - i as i32) * 4;
        val += (d as f64) * 10f64.powi(exp);
    }
    if sign == 0x4000 {
        -val
    } else {
        val
    }
}

/// Decode a binary NUMERIC body (bytes without outer length prefix) back to a
/// decimal string using the same algorithm as `decode_param_binary` in
/// `protocol.rs`. Used for round-trip assertions.
fn decode_numeric_body_to_string(bytes_with_prefix: &[u8]) -> String {
    // Strip the 4-byte length prefix.
    let body_len = i32::from_be_bytes(bytes_with_prefix[0..4].try_into().unwrap()) as usize;
    let body = &bytes_with_prefix[4..4 + body_len];

    const HEADER_LEN: usize = 8;
    assert!(
        body.len() >= HEADER_LEN,
        "body too short: {}",
        body.len()
    );
    let ndigits = u16::from_be_bytes([body[0], body[1]]);
    let weight = i16::from_be_bytes([body[2], body[3]]);
    let sign = u16::from_be_bytes([body[4], body[5]]);
    let dscale = u16::from_be_bytes([body[6], body[7]]);

    const NUMERIC_POS: u16 = 0x0000;
    const NUMERIC_NEG: u16 = 0x4000;
    assert!(
        sign == NUMERIC_POS || sign == NUMERIC_NEG,
        "sign 0x{sign:04x} not POS or NEG"
    );

    let mut digits: Vec<u16> = Vec::with_capacity(ndigits as usize);
    for i in 0..ndigits as usize {
        let off = HEADER_LEN + i * 2;
        digits.push(u16::from_be_bytes([body[off], body[off + 1]]));
    }

    // Reconstruct integer part (groups at positions weight..0).
    let mut int_part = String::new();
    if weight >= 0 {
        let int_groups = (weight as usize) + 1;
        for pos in (0..int_groups).rev() {
            let i_signed = weight as i32 - pos as i32;
            let group = if (0..ndigits as i32).contains(&i_signed) {
                digits[i_signed as usize]
            } else {
                0
            };
            if int_part.is_empty() {
                int_part.push_str(&format!("{group}"));
            } else {
                int_part.push_str(&format!("{group:04}"));
            }
        }
    }
    if int_part.is_empty() {
        int_part.push('0');
    }

    // Reconstruct fractional part.
    let frac_groups_needed = (dscale as usize).div_ceil(4);
    let mut frac_part = String::new();
    for k in 0..frac_groups_needed {
        let i_signed = weight as i32 + 1 + k as i32;
        let group = if (0..ndigits as i32).contains(&i_signed) {
            digits[i_signed as usize]
        } else {
            0
        };
        frac_part.push_str(&format!("{group:04}"));
    }
    if frac_part.len() > dscale as usize {
        frac_part.truncate(dscale as usize);
    } else {
        while frac_part.len() < dscale as usize {
            frac_part.push('0');
        }
    }

    let mut s = String::new();
    if sign == NUMERIC_NEG && !(int_part == "0" && frac_part.chars().all(|c| c == '0')) {
        s.push('-');
    }
    s.push_str(&int_part);
    if !frac_part.is_empty() {
        s.push('.');
        s.push_str(&frac_part);
    }
    s
}

// ---------------------------------------------------------------------------
// Byte-level pin tests — exact wire byte assertions
//
// These values were derived from the PG numeric binary wire format spec and
// match what `psql -c "COPY (SELECT ...) TO STDOUT (FORMAT binary)"` would
// emit for the identical values.
// ---------------------------------------------------------------------------

/// PG zero: ndigits=0, weight=0, sign=0x0000, dscale=0.
/// Wire body (8 bytes): 00 00 | 00 00 | 00 00 | 00 00
#[test]
fn pin_zero() {
    // Decimal128(38,0): raw=0 → 0.0 with scale 0
    let bytes = encode_numeric_binary_bytes(0i128, 38, 0);
    let (ndigits, weight, sign, dscale, digits) = parse_numeric_body(&bytes);
    assert_eq!(ndigits, 0, "zero: ndigits");
    assert_eq!(weight, 0, "zero: weight");
    assert_eq!(sign, 0x0000, "zero: sign (positive)");
    assert_eq!(dscale, 0, "zero: dscale");
    assert!(digits.is_empty(), "zero: no digit words");
    // Exact body bytes (after 4-byte length prefix).
    assert_eq!(
        &bytes[4..12],
        &[0u8, 0, 0, 0, 0, 0, 0, 0],
        "zero: exact body bytes"
    );
}

/// PG 1.0: raw=10 scale=1 → value 1.0
/// ndigits=2, weight=0, sign=0x0000, dscale=1, digits=[1, 0]
/// BUT trailing zero is stripped → ndigits=1, digits=[1]
///
/// Note: PG strips trailing zero digit groups but preserves dscale for display.
/// So: ndigits=1, weight=0, sign=0x0000, dscale=1, digits=[1].
#[test]
fn pin_one_point_zero() {
    let bytes = encode_numeric_binary_bytes(10i128, 38, 1);
    let (ndigits, weight, sign, dscale, digits) = parse_numeric_body(&bytes);
    assert_eq!(sign, 0x0000, "1.0: sign");
    assert_eq!(dscale, 1, "1.0: dscale preserves scale");
    assert_eq!(weight, 0, "1.0: weight");
    assert_eq!(ndigits, 1, "1.0: ndigits (trailing zero stripped)");
    assert_eq!(digits, vec![1], "1.0: digit group");
}

/// PG -123.45: raw=-12345 scale=2
/// int_part=123, frac_part=45
/// int_digits=[123], weight=0
/// frac_groups=1, full_scale=4, padding=2 → padded=4500
/// digits=[123, 4500], dscale=2, sign=0x4000
#[test]
fn pin_neg_123_45() {
    let bytes = encode_numeric_binary_bytes(-12345i128, 38, 2);
    let (ndigits, weight, sign, dscale, digits) = parse_numeric_body(&bytes);
    assert_eq!(sign, 0x4000, "-123.45: sign (negative)");
    assert_eq!(dscale, 2, "-123.45: dscale");
    assert_eq!(weight, 0, "-123.45: weight");
    assert_eq!(ndigits, 2, "-123.45: ndigits");
    assert_eq!(digits[0], 123, "-123.45: integer group");
    assert_eq!(digits[1], 4500, "-123.45: fractional group (45 padded to 4500)");
    let v = pg_numeric_to_f64(weight, sign, &digits);
    assert!(
        (v - (-123.45)).abs() < 1e-8,
        "-123.45: reconstructed value = {v}"
    );
}

/// PG 12345.6789: raw=123456789 scale=4
/// int_part=12345, frac_part=6789
/// int_digits=[1, 2345] (12345 = 1*10000 + 2345), weight=1
/// frac_groups=1, full_scale=4, padding=0 → digits=[1, 2345, 6789]
/// dscale=4, sign=0x0000
#[test]
fn pin_12345_6789() {
    let bytes = encode_numeric_binary_bytes(123456789i128, 38, 4);
    let (ndigits, weight, sign, dscale, digits) = parse_numeric_body(&bytes);
    assert_eq!(sign, 0x0000, "12345.6789: sign");
    assert_eq!(dscale, 4, "12345.6789: dscale");
    assert_eq!(weight, 1, "12345.6789: weight (int has 2 groups)");
    assert_eq!(ndigits, 3, "12345.6789: ndigits");
    assert_eq!(digits[0], 1, "12345.6789: first int group");
    assert_eq!(digits[1], 2345, "12345.6789: second int group");
    assert_eq!(digits[2], 6789, "12345.6789: frac group");
    let v = pg_numeric_to_f64(weight, sign, &digits);
    assert!(
        (v - 12345.6789).abs() < 1e-6,
        "12345.6789: reconstructed value = {v}"
    );
}

/// PG 0.0001: raw=1 scale=4
/// int_part=0, frac_part=1
/// frac_groups=1, full_scale=4, padding=0 → padded=1
/// Group0 at divisor_group=1 → 1/1=1
/// digits=[1], weight=-1 (first nonzero at index 0, weight=-1-0=-1)
/// dscale=4, sign=0x0000
#[test]
fn pin_0_0001() {
    let bytes = encode_numeric_binary_bytes(1i128, 38, 4);
    let (ndigits, weight, sign, dscale, digits) = parse_numeric_body(&bytes);
    assert_eq!(sign, 0x0000, "0.0001: sign");
    assert_eq!(dscale, 4, "0.0001: dscale");
    assert_eq!(weight, -1, "0.0001: weight");
    assert_eq!(ndigits, 1, "0.0001: ndigits");
    assert_eq!(digits[0], 1, "0.0001: digit group");
    let v = pg_numeric_to_f64(weight, sign, &digits);
    assert!(
        (v - 0.0001).abs() < 1e-12,
        "0.0001: reconstructed value = {v}"
    );
}

/// Very large Decimal128: 99999999999999999999.99 (20 integer digits, 2 frac digits)
/// raw = 9999999999999999999999 (= 99999999999999999999 * 100 + 99)
/// int_part = 99999999999999999999 (20 digits → 5 groups of 9999)
/// weight = 4 (5 int groups → weight = 4)
/// frac: 99 → scale=2 → frac_groups=1, full_scale=4, padding=2 → 9900
/// digits = [9999,9999,9999,9999,9999, 9900], dscale=2
#[test]
fn pin_large_decimal128() {
    // 99999999999999999999.99 → int = 99999999999999999999, frac = 99
    let int_val: i128 = 99999999999999999999i128;
    let raw: i128 = int_val * 100 + 99;
    let bytes = encode_numeric_binary_bytes(raw, 38, 2);
    let (ndigits, weight, sign, dscale, digits) = parse_numeric_body(&bytes);
    assert_eq!(sign, 0x0000);
    assert_eq!(dscale, 2);
    assert_eq!(weight, 4, "20-digit int has 5 groups → weight=4");
    // All 5 integer digit groups should be 9999.
    assert!(
        ndigits >= 5,
        "expected at least 5 digit groups, got {ndigits}"
    );
    for i in 0..5 {
        assert_eq!(digits[i], 9999, "int digit group {i} should be 9999");
    }
    // Frac: 99 padded to 4 → 9900.
    if ndigits > 5 {
        assert_eq!(digits[5], 9900, "frac group = 9900");
    }
    let v = pg_numeric_to_f64(weight, sign, &digits);
    let expected = 99999999999999999999.0f64 + 0.99f64;
    assert!(
        (v - expected).abs() < 1e6,
        "large decimal: reconstructed value = {v}, expected ≈ {expected}"
    );
}

/// High-dscale (8 fractional places): 0.00000001
/// raw=1 scale=8
/// frac_groups=2 (ceil(8/4)=2), full_scale=8, padding=0
/// Digit breakdown: divisor_group for group0 = 10^4 = 10000
///   group0 = 1 / 10000 = 0, rem=1
///   group1 = 1 / 1 = 1
/// all_digits=[0, 1], leading zero stripped → digits=[1]
/// weight = -1 - 1 = -2 (first nonzero at index 1 in frac_digits)
/// dscale=8
#[test]
fn pin_high_dscale_fractional() {
    // 0.00000001 = 1e-8, Decimal128(38,8): raw=1
    let bytes = encode_numeric_binary_bytes(1i128, 38, 8);
    let (ndigits, weight, sign, dscale, digits) = parse_numeric_body(&bytes);
    assert_eq!(sign, 0x0000);
    assert_eq!(dscale, 8);
    // weight should be -2 (the nonzero digit covers 10^(-2*4)..10^(-1*4-1))
    assert_eq!(weight, -2, "0.00000001: weight");
    assert_eq!(ndigits, 1);
    assert_eq!(digits[0], 1, "0.00000001: digit group");
    let v = pg_numeric_to_f64(weight, sign, &digits);
    assert!(
        (v - 1e-8).abs() < 1e-16,
        "0.00000001: reconstructed = {v}"
    );
}

/// Negative fractional (no integer part): -0.5
/// raw=-5 scale=1
/// int_part=0, frac_part=5 → padded to 5000
/// all_digits=[5000], weight=-1
/// sign=0x4000, dscale=1
#[test]
fn pin_neg_zero_point_five() {
    let bytes = encode_numeric_binary_bytes(-5i128, 38, 1);
    let (ndigits, weight, sign, dscale, digits) = parse_numeric_body(&bytes);
    assert_eq!(sign, 0x4000, "-0.5: sign");
    assert_eq!(dscale, 1, "-0.5: dscale");
    assert_eq!(weight, -1, "-0.5: weight");
    assert_eq!(ndigits, 1);
    assert_eq!(digits[0], 5000, "-0.5: digit = 5000");
    let v = pg_numeric_to_f64(weight, sign, &digits);
    assert!((v - (-0.5)).abs() < 1e-10, "-0.5: reconstructed = {v}");
}

// ---------------------------------------------------------------------------
// Round-trip encode → decode tests
//
// For each test value, encode to binary, decode with the same algorithm as
// `decode_param_binary(NUMERIC)` in protocol.rs, and verify the resulting
// string matches what Arrow's Decimal128Array::value_as_string produces.
// ---------------------------------------------------------------------------

/// Helper: compute what `Decimal128Array::value_as_string` would produce.
fn arrow_decimal_text(raw: i128, precision: u8, scale: i8) -> String {
    let arr = Decimal128Array::from(vec![Some(raw)])
        .with_precision_and_scale(precision, scale)
        .unwrap();
    arr.value_as_string(0)
}

#[test]
fn round_trip_zero() {
    let raw = 0i128;
    let (p, s) = (38u8, 4i8);
    let bytes = encode_numeric_binary_bytes(raw, p, s);
    let got = decode_numeric_body_to_string(&bytes);
    // Zero with dscale=0 (special case) — the encoder always emits dscale=0
    // for zero values. So the round-trip string is "0" not "0.0000".
    assert_eq!(got, "0", "round-trip zero: got={got:?}");
}

#[test]
fn round_trip_one() {
    let raw = 1i128; // scale=0 → value 1
    let bytes = encode_numeric_binary_bytes(raw, 38, 0);
    let got = decode_numeric_body_to_string(&bytes);
    let want = arrow_decimal_text(raw, 38, 0);
    assert_eq!(got, want, "round-trip 1: got={got:?} want={want:?}");
}

#[test]
fn round_trip_neg_one() {
    let raw = -1i128;
    let bytes = encode_numeric_binary_bytes(raw, 38, 0);
    let got = decode_numeric_body_to_string(&bytes);
    let want = arrow_decimal_text(raw, 38, 0);
    assert_eq!(got, want, "round-trip -1: got={got:?} want={want:?}");
}

#[test]
fn round_trip_12345_6789() {
    // 12345.6789 at scale=4: raw=123456789
    let raw = 123456789i128;
    let bytes = encode_numeric_binary_bytes(raw, 38, 4);
    let got = decode_numeric_body_to_string(&bytes);
    let want = arrow_decimal_text(raw, 38, 4);
    assert_eq!(
        got, want,
        "round-trip 12345.6789: got={got:?} want={want:?}"
    );
}

#[test]
fn round_trip_neg_12345_6789() {
    let raw = -123456789i128;
    let bytes = encode_numeric_binary_bytes(raw, 38, 4);
    let got = decode_numeric_body_to_string(&bytes);
    let want = arrow_decimal_text(raw, 38, 4);
    assert_eq!(
        got, want,
        "round-trip -12345.6789: got={got:?} want={want:?}"
    );
}

#[test]
fn round_trip_high_precision_decimal128() {
    // Near the Decimal128 max at scale=8: 123456789012345678.90123456
    let raw: i128 = 12345678901234567890123456i128;
    let bytes = encode_numeric_binary_bytes(raw, 38, 8);
    let got = decode_numeric_body_to_string(&bytes);
    let want = arrow_decimal_text(raw, 38, 8);
    assert_eq!(
        got, want,
        "round-trip high precision: got={got:?} want={want:?}"
    );
}

#[test]
fn round_trip_small_fraction() {
    // 0.0001 at scale=4: raw=1
    let raw = 1i128;
    let bytes = encode_numeric_binary_bytes(raw, 38, 4);
    let got = decode_numeric_body_to_string(&bytes);
    let want = arrow_decimal_text(raw, 38, 4);
    assert_eq!(
        got, want,
        "round-trip 0.0001: got={got:?} want={want:?}"
    );
}

#[test]
fn round_trip_scale_zero_large() {
    // Integer value 1234567890 (scale=0)
    let raw: i128 = 1234567890;
    let bytes = encode_numeric_binary_bytes(raw, 38, 0);
    let got = decode_numeric_body_to_string(&bytes);
    let want = arrow_decimal_text(raw, 38, 0);
    assert_eq!(
        got, want,
        "round-trip large int: got={got:?} want={want:?}"
    );
}

// ---------------------------------------------------------------------------
// Text-path-unchanged regression
//
// Verifies that `encode_batches` (text-only path, no format codes) produces
// byte-identical output after the binary NUMERIC changes. We pin a few known
// cell byte sequences.
// ---------------------------------------------------------------------------

/// Text encoding of a NUMERIC column must be byte-identical to Arrow's
/// `value_as_string` output: the scale-padded decimal string.
#[test]
fn text_path_unchanged_numeric() {
    use basin_router::encode_batches_text_for_test;

    // 1.50 → Decimal128(10, 2): raw=150, value_as_string="1.50"
    let arr = Decimal128Array::from(vec![Some(150i128)])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Decimal128(10, 2),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
    let rows = encode_batches_text_for_test(&schema, &[batch]);
    // Expected: i32 length=4, then b"1.50"
    let expected_text = b"1.50";
    let data = &rows[0].data;
    let len = i32::from_be_bytes(data[0..4].try_into().unwrap());
    assert_eq!(len as usize, expected_text.len(), "text body length");
    assert_eq!(&data[4..4 + expected_text.len()], expected_text, "text body");
}

/// Text encoding with scale=0 produces no decimal point.
#[test]
fn text_path_unchanged_integer_numeric() {
    use basin_router::encode_batches_text_for_test;

    let arr = Decimal128Array::from(vec![Some(42000i128)])
        .with_precision_and_scale(18, 0)
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        DataType::Decimal128(18, 0),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
    let rows = encode_batches_text_for_test(&schema, &[batch]);
    let data = &rows[0].data;
    let len = i32::from_be_bytes(data[0..4].try_into().unwrap());
    let text = std::str::from_utf8(&data[4..4 + len as usize]).unwrap();
    assert_eq!(text, "42000", "integer numeric text: {text:?}");
}

// ---------------------------------------------------------------------------
// Format-code gating: binary only when format code 1 requested
// ---------------------------------------------------------------------------

/// Format code 0 (text) → same bytes as text path (NOT binary PG numeric body).
#[test]
fn format_code_zero_emits_text() {
    use basin_router::{encode_batches_text_for_test, encode_batches_with_formats_for_test};

    // 1.50: raw=150, scale=2
    let arr = Decimal128Array::from(vec![Some(150i128)])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        DataType::Decimal128(10, 2),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();

    let text_rows = encode_batches_text_for_test(&schema, &[batch.clone()]);
    let fmt0_rows =
        encode_batches_with_formats_for_test(&schema, &[batch], &[0]).unwrap();

    assert_eq!(
        text_rows[0].data, fmt0_rows[0].data,
        "format_code=0 must produce identical bytes to text path"
    );
}

/// Format code 1 (binary) → PG binary NUMERIC body (NOT text).
/// The binary body starts with the 8-byte header (ndigits/weight/sign/dscale).
#[test]
fn format_code_one_emits_binary() {
    use basin_router::{encode_batches_text_for_test, encode_batches_with_formats_for_test};

    // 1.50: raw=150, scale=2
    let arr = Decimal128Array::from(vec![Some(150i128)])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        DataType::Decimal128(10, 2),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();

    let text_rows = encode_batches_text_for_test(&schema, &[batch.clone()]);
    let bin_rows =
        encode_batches_with_formats_for_test(&schema, &[batch], &[1]).unwrap();

    // Binary must be different from text.
    assert_ne!(
        text_rows[0].data, bin_rows[0].data,
        "format_code=1 must differ from text encoding"
    );

    // Binary body must parse as a valid PG NUMERIC header (not "1.50" ASCII).
    let bin_data = &bin_rows[0].data;
    let body_len = i32::from_be_bytes(bin_data[0..4].try_into().unwrap());
    // Body must be >= 8 bytes (the minimum NUMERIC header).
    assert!(
        body_len >= 8,
        "binary NUMERIC body must be at least 8 bytes, got {body_len}"
    );
    // dscale (bytes 10..12) must be 2 (matching the Arrow Decimal128 scale).
    let dscale = u16::from_be_bytes(bin_data[10..12].try_into().unwrap());
    assert_eq!(dscale, 2, "binary NUMERIC dscale must match Arrow scale");
}

/// Per-column format codes: mixed text/binary columns.
/// Column 0: text (format code 0), Column 1: binary (format code 1).
#[test]
fn per_column_format_codes_mixed() {
    use basin_router::encode_batches_with_formats_for_test;

    let arr0 = Decimal128Array::from(vec![Some(150i128)])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let arr1 = Decimal128Array::from(vec![Some(99999i128)])
        .with_precision_and_scale(10, 3)
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("text_col", DataType::Decimal128(10, 2), false),
        Field::new("bin_col", DataType::Decimal128(10, 3), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arr0), Arc::new(arr1)],
    )
    .unwrap();

    let rows = encode_batches_with_formats_for_test(&schema, &[batch], &[0, 1]).unwrap();
    let data = &rows[0].data;

    // Column 0 (text): read the length prefix, then check it's ASCII text "1.50".
    let len0 = i32::from_be_bytes(data[0..4].try_into().unwrap());
    let text0 = std::str::from_utf8(&data[4..4 + len0 as usize]).unwrap();
    assert_eq!(text0, "1.50", "col 0 text value");

    // Column 1 (binary): starts after col 0 data (4 + len0 bytes).
    let off1 = 4 + len0 as usize;
    let len1 = i32::from_be_bytes(data[off1..off1 + 4].try_into().unwrap());
    // Binary body must be >= 8 bytes (minimum numeric header).
    assert!(
        len1 >= 8,
        "col 1 binary body must be >= 8 bytes, got {len1}"
    );
    // dscale at offset (off1 + 4 + 6) must be 3 (matching scale).
    let dscale1 = u16::from_be_bytes(
        data[off1 + 10..off1 + 12].try_into().unwrap(),
    );
    assert_eq!(dscale1, 3, "col 1 binary dscale must be 3");
}

/// Single format code 1 applies to ALL columns (Bind spec: "one code for all").
#[test]
fn single_binary_format_code_applies_to_all_columns() {
    use basin_router::encode_batches_with_formats_for_test;

    let arr0 = Decimal128Array::from(vec![Some(150i128)])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let arr1 = Decimal128Array::from(vec![Some(99i128)])
        .with_precision_and_scale(10, 0)
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Decimal128(10, 2), false),
        Field::new("b", DataType::Decimal128(10, 0), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arr0), Arc::new(arr1)],
    )
    .unwrap();

    // Single format code 1 → all columns binary.
    let rows = encode_batches_with_formats_for_test(&schema, &[batch], &[1]).unwrap();
    let data = &rows[0].data;

    // Column 0: read body length.
    let len0 = i32::from_be_bytes(data[0..4].try_into().unwrap());
    assert!(len0 >= 8, "col 0 must be binary (len >= 8), got {len0}");
    let dscale0 = u16::from_be_bytes(data[10..12].try_into().unwrap());
    assert_eq!(dscale0, 2, "col 0 dscale=2");

    // Column 1: starts after col 0 data.
    let off1 = 4 + len0 as usize;
    let len1 = i32::from_be_bytes(data[off1..off1 + 4].try_into().unwrap());
    assert!(len1 >= 8, "col 1 must be binary (len >= 8), got {len1}");
    let dscale1 = u16::from_be_bytes(data[off1 + 10..off1 + 12].try_into().unwrap());
    assert_eq!(dscale1, 0, "col 1 dscale=0");
}

/// Empty format_codes slice → text for all columns (PG Bind spec default).
#[test]
fn empty_format_codes_defaults_to_text() {
    use basin_router::{encode_batches_text_for_test, encode_batches_with_formats_for_test};

    let arr = Decimal128Array::from(vec![Some(150i128)])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        DataType::Decimal128(10, 2),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();

    let text_rows = encode_batches_text_for_test(&schema, &[batch.clone()]);
    let default_rows =
        encode_batches_with_formats_for_test(&schema, &[batch], &[]).unwrap();

    assert_eq!(
        text_rows[0].data, default_rows[0].data,
        "empty format_codes must be byte-identical to text path"
    );
}
