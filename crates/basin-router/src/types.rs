//! Arrow -> Postgres type and value conversion for the simple-query path.
//!
//! The PoC encodes everything in Postgres text format (format code 0). All
//! values are UTF-8 strings; SQL NULL is signalled by a -1 length prefix.
//! `chrono` is used for the timestamp string formatting; we render every
//! timestamp variant as RFC3339 for PoC simplicity (production will need
//! distinct mappings for `timestamp` vs `timestamptz`).

use std::fmt::Write as FmtWrite;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    IntervalMonthDayNanoType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{Array, FixedSizeListArray, LargeListArray, ListArray, RecordBatch};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use basin_common::Result;
use bytes::{BufMut, BytesMut};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use pgwire::api::Type;
use pgwire::messages::data::{DataRow, FieldDescription, RowDescription};

const FORMAT_CODE_TEXT: i16 = 0;

/// Number of days between Arrow's Date32 epoch (1970-01-01) and Postgres's
/// DATE epoch (2000-01-01). Used by the binary DATE wire encoding.
const PG_EPOCH_DAYS_FROM_UNIX: i32 = 10957;

/// Microseconds between Arrow's TIMESTAMP epoch (1970-01-01 UTC) and Postgres's
/// TIMESTAMPTZ epoch (2000-01-01 UTC). Used by the binary TIMESTAMP[TZ] wire
/// encoding here and by the matching parameter DECODE in `protocol.rs`
/// (`decode_param_binary`'s TIMESTAMP / TIMESTAMPTZ arms rebase with the
/// same constant in the opposite direction).
pub(crate) const PG_EPOCH_MICROS_FROM_UNIX: i64 =
    (PG_EPOCH_DAYS_FROM_UNIX as i64) * 86_400_000_000;

/// Field-metadata key that basin-engine uses to mark logical types not
/// directly representable in Arrow (today: `JSONB`, `UUID`). Kept in sync
/// with `basin_engine::types::BASIN_TYPE_KEY` — duplicated here as a `&str`
/// constant so this crate stays free of an engine dependency cycle.
const BASIN_TYPE_KEY: &str = "BASIN_TYPE";
const BASIN_TYPE_JSONB: &str = "JSONB";
const BASIN_TYPE_UUID: &str = "UUID";

/// A tiny `fmt::Write` adaptor that formats values into a fixed-size stack
/// buffer, avoiding any heap allocation for numeric-to-text rendering.
/// 32 bytes covers i64::MIN (20 digits + sign) and f64 (at most 24 chars).
struct StackFmt {
    buf: [u8; 32],
    len: usize,
}

impl StackFmt {
    #[inline]
    fn new() -> Self {
        Self {
            buf: [0u8; 32],
            len: 0,
        }
    }

    #[inline]
    fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    #[inline]
    fn reset(&mut self) {
        self.len = 0;
    }
}

impl FmtWrite for StackFmt {
    #[inline]
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let b = s.as_bytes();
        let new_len = self.len + b.len();
        if new_len > self.buf.len() {
            return Err(std::fmt::Error);
        }
        self.buf[self.len..new_len].copy_from_slice(b);
        self.len = new_len;
        Ok(())
    }
}

fn field_is_jsonb(f: &Field) -> bool {
    f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_JSONB)
}

fn field_is_uuid(f: &Field) -> bool {
    f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_UUID)
}

/// Convert an Arrow schema to a pgwire `RowDescription`. Field names are
/// preserved verbatim. Type OIDs come from `arrow_to_pg_type`.
pub(crate) fn row_description(schema: &Schema) -> RowDescription {
    let fields = schema
        .fields()
        .iter()
        .map(|f| field_description(f.as_ref()))
        .collect();
    RowDescription::new(fields)
}

fn field_description(f: &Field) -> FieldDescription {
    let ty = arrow_to_pg_type_field(f);
    FieldDescription::new(
        f.name().clone(),
        0, // table OID — unknown at this layer
        0, // column attnum — unknown at this layer
        ty.oid(),
        type_size(&ty),
        -1,
        FORMAT_CODE_TEXT,
    )
}

/// Logical-type-aware variant of `arrow_to_pg_type`. JSONB lives on
/// `LargeBinary` (canonical-form JSON bytes) and UUID on
/// `FixedSizeBinary(16)` (RFC 4122 raw bytes); both must surface as their
/// proper Postgres OIDs (3802 / 2950) so PG-protocol clients see the
/// expected types rather than `bytea`.
pub(crate) fn arrow_to_pg_type_field(f: &Field) -> Type {
    if field_is_jsonb(f) {
        return Type::JSONB;
    }
    if field_is_uuid(f) {
        return Type::UUID;
    }
    arrow_to_pg_type(f.data_type())
}

/// Encode the value at row `idx` of `col` as one length-prefixed
/// binary-format COPY field into `buf` (NULL = `-1` length prefix).
///
/// The PG binary COPY field encoding is identical to the binary `DataRow`
/// field encoding, so this is a thin field-metadata-aware wrapper around
/// [`encode_value_binary`] — the same codec the extended-query result path
/// uses for binary result columns. Kept here so `copy.rs` doesn't have to
/// know about the JSONB/UUID field-metadata convention.
pub(crate) fn encode_copy_binary_field(
    col: &dyn Array,
    idx: usize,
    field: &Field,
    buf: &mut BytesMut,
) -> Result<()> {
    encode_value_binary(col, idx, buf, field_is_jsonb(field), field_is_uuid(field))
}

/// Map an Arrow type to the closest Postgres type for the PoC. Anything we
/// don't have a mapping for becomes TEXT — the value renderer will format it
/// via `Debug`.
///
/// # Fixed types and their Postgres OIDs
///
/// | Arrow type                        | Postgres OID | Name         |
/// |-----------------------------------|--------------|--------------|
/// | `Int8` / `UInt8`                  | 21 (INT2)    | smallint     |
/// | `Int16` / `UInt16`                | 21 (INT2)    | smallint     |
/// | `Int32`                           | 23 (INT4)    | integer      |
/// | `Int64` / `UInt32` / `UInt64`     | 20 (INT8)    | bigint       |
/// | `Timestamp(_, Some(_))`           | 1184         | timestamptz  |
/// | `Timestamp(_, None)`              | 1114         | timestamp    |
///
/// Note: `Int8` (Arrow 8-bit signed integer, aka "tinyint") and `UInt8`
/// (Arrow 8-bit unsigned integer) have no direct Postgres equivalent — the
/// smallest PG integer type is `SMALLINT` (2 bytes). Both are promoted to
/// `INT2` (OID 21) here. The type-OID mismatch this previously caused —
/// `Int8`/`UInt8` columns falling through to `TEXT` (OID 25) — produced
/// `SELECT *` failures on tables whose Arrow schema contained these types,
/// because drivers that do typed column access saw OID 25 instead of 21 and
/// rejected the binary wire data.
pub(crate) fn arrow_to_pg_type(dt: &DataType) -> Type {
    match dt {
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => Type::INT8,
        DataType::Int32 => Type::INT4,
        // Arrow Int8 / UInt8 (8-bit integers) have no direct PG equivalent.
        // Promote to INT2 (OID 21, smallint) — the smallest PG integer type —
        // rather than falling through to TEXT. This closes the type-OID
        // mismatch that caused SELECT * failures on rows that DataFusion or
        // conversion code internally typed as 8-bit.
        DataType::Int16 | DataType::Int8 | DataType::UInt8 | DataType::UInt16 => Type::INT2,
        DataType::Boolean => Type::BOOL,
        DataType::Float64 => Type::FLOAT8,
        DataType::Float32 => Type::FLOAT4,
        DataType::Utf8 | DataType::LargeUtf8 => Type::TEXT,
        DataType::Binary | DataType::LargeBinary => Type::BYTEA,
        DataType::Date32 => Type::DATE,
        DataType::Interval(IntervalUnit::MonthDayNano) => Type::INTERVAL,
        DataType::Timestamp(_, Some(_)) => Type::TIMESTAMPTZ,
        DataType::Timestamp(_, None) => Type::TIMESTAMP,
        // PG `numeric` (OID 1700) → Arrow `Decimal128(p, s)`. Binary wire
        // format is implemented: `encode_numeric_binary` emits the PG
        // base-10000 digit encoding when format code 1 is requested.
        // Text format is unchanged (default for simple-query and format code 0).
        DataType::Decimal128(_, _) => Type::NUMERIC,
        // PG one-dimensional array types.  Used by the extended-query
        // ParameterDescription path so that `WHERE col = ANY($1)` over a
        // BIGINT column advertises OID 1016 (`int8[]`), letting tokio-
        // postgres / sqlx / pgx accept a bound `Vec<i64>` instead of
        // rejecting it with `WrongType` against a TEXT slot.  Element type
        // is read from the List's inner field; anything we don't have an
        // array OID for falls through to TEXT.
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _) => match field.data_type() {
            DataType::Int64 | DataType::UInt32 | DataType::UInt64 => Type::INT8_ARRAY,
            DataType::Int32 => Type::INT4_ARRAY,
            DataType::Int16 | DataType::Int8 | DataType::UInt8 | DataType::UInt16 => {
                Type::INT2_ARRAY
            }
            DataType::Boolean => Type::BOOL_ARRAY,
            DataType::Float64 => Type::FLOAT8_ARRAY,
            DataType::Utf8 | DataType::LargeUtf8 => Type::TEXT_ARRAY,
            _ => Type::TEXT,
        },
        // Everything else gets formatted as text.
        _ => Type::TEXT,
    }
}

fn type_size(ty: &Type) -> i16 {
    match *ty {
        Type::INT8 | Type::FLOAT8 | Type::TIMESTAMPTZ | Type::TIMESTAMP => 8,
        Type::INT4 | Type::FLOAT4 | Type::DATE => 4,
        Type::INT2 => 2,
        Type::BOOL => 1,
        Type::INTERVAL => 16,
        // NUMERIC is varlena (-1) — variable-length wire payload.
        Type::NUMERIC => -1,
        _ => -1,
    }
}

/// Encode every row in `batches` as a `DataRow`. Returns the rows in batch
/// order, batch-internal row order. All values are encoded as text.
///
/// A single `BytesMut` scratch buffer is allocated once and reused across all
/// rows via `split()`: each call to `split()` transfers ownership of the
/// accumulated bytes to `DataRow` while the original `BytesMut` retains its
/// heap allocation (capacity) for the next row, amortising the per-row alloc
/// to a one-time cost.
pub(crate) fn encode_batches(schema: &Arc<Schema>, batches: &[RecordBatch]) -> Vec<DataRow> {
    let n_cols = schema.fields().len();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let mut rows = Vec::with_capacity(total_rows);
    // Pre-compute the `is_jsonb` / `is_uuid` bitmaps so the per-cell hot
    // loop doesn't redo a metadata lookup per row.
    let jsonb_cols: Vec<bool> = schema
        .fields()
        .iter()
        .map(|f| field_is_jsonb(f.as_ref()))
        .collect();
    let uuid_cols: Vec<bool> = schema
        .fields()
        .iter()
        .map(|f| field_is_uuid(f.as_ref()))
        .collect();
    // One scratch buffer reused across all rows. `split()` hands the
    // accumulated bytes off to `DataRow` while this handle retains its
    // underlying heap allocation.
    let mut scratch = BytesMut::with_capacity(256);
    // Stack-allocated numeric formatter — no heap alloc per integer cell.
    let mut sf = StackFmt::new();
    for batch in batches {
        let n_rows = batch.num_rows();
        for r in 0..n_rows {
            for c in 0..n_cols {
                let col = batch.column(c);
                encode_value(
                    col.as_ref(),
                    r,
                    &mut scratch,
                    jsonb_cols[c],
                    uuid_cols[c],
                    &mut sf,
                );
            }
            rows.push(DataRow::new(scratch.split(), n_cols as i16));
        }
    }
    rows
}

/// Encode rows respecting per-column wire-format codes. `format_codes` follows
/// the Postgres `Bind` rules: empty == all text, a single entry applies to
/// every column, otherwise one entry per column.
///
/// Used by the extended-query path. Drivers that hard-code binary result
/// columns (notably `tokio-postgres`) need this — text-only would garble
/// every numeric column.
///
/// As with `encode_batches`, a single scratch `BytesMut` is reused across
/// rows via `split()` to eliminate O(rows) heap allocations.
pub(crate) fn encode_batches_with_formats(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
    format_codes: &[i16],
) -> Result<Vec<DataRow>> {
    let n_cols = schema.fields().len();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let jsonb_cols: Vec<bool> = schema
        .fields()
        .iter()
        .map(|f| field_is_jsonb(f.as_ref()))
        .collect();
    let uuid_cols: Vec<bool> = schema
        .fields()
        .iter()
        .map(|f| field_is_uuid(f.as_ref()))
        .collect();
    // Pre-compute per-column binary flag when format_codes has one entry per
    // column; avoids the repeated match inside the hot cell loop.
    let binary_cols: Option<Vec<bool>> = if format_codes.len() > 1 {
        Some(
            (0..n_cols)
                .map(|c| format_codes.get(c).copied().unwrap_or(0) == 1)
                .collect(),
        )
    } else {
        None
    };
    let all_binary = format_codes.len() == 1 && format_codes[0] == 1;
    let mut rows = Vec::with_capacity(total_rows);
    let mut scratch = BytesMut::with_capacity(256);
    let mut sf = StackFmt::new();
    for batch in batches {
        let n_rows = batch.num_rows();
        for r in 0..n_rows {
            for c in 0..n_cols {
                let col = batch.column(c);
                let is_binary = match &binary_cols {
                    Some(v) => v[c],
                    None => all_binary,
                };
                if is_binary {
                    encode_value_binary(
                        col.as_ref(),
                        r,
                        &mut scratch,
                        jsonb_cols[c],
                        uuid_cols[c],
                    )?;
                } else {
                    encode_value(
                        col.as_ref(),
                        r,
                        &mut scratch,
                        jsonb_cols[c],
                        uuid_cols[c],
                        &mut sf,
                    );
                }
            }
            rows.push(DataRow::new(scratch.split(), n_cols as i16));
        }
    }
    Ok(rows)
}

/// Encode the value at row `idx` of `col` as a length-prefixed Postgres
/// binary-format field. NULLs get a `-1` length prefix.
///
/// Binary representations follow the wire spec:
/// <https://www.postgresql.org/docs/current/protocol-message-formats.html>.
/// We support the same scalar set the text path supports.
fn encode_value_binary(
    col: &dyn Array,
    idx: usize,
    buf: &mut BytesMut,
    is_jsonb: bool,
    is_uuid: bool,
) -> Result<()> {
    if col.is_null(idx) {
        buf.put_i32(-1);
        return Ok(());
    }
    // JSONB binary format. The Postgres wire format for JSONB is a single
    // version byte (`0x01`) followed by the JSON text. Drivers that ask for
    // binary JSONB strip the leading 1 and parse the remainder. Sending it
    // as plain text would *also* be tolerated by lenient drivers but trips
    // strict ones (notably `tokio-postgres` 0.7+). We pay one byte to be
    // correct here.
    //
    // Two storage shapes map to the JSONB logical type:
    // - `LargeBinary` / `Binary`: canonical JSONB storage (coerced at ingest via
    //   `coerce_jsonb`). Bytes are already canonical-form JSON text.
    // - `Utf8` / `LargeUtf8`: result of `json_agg` / `jsonb_agg` UDAFs, which
    //   accumulate JSON text as Utf8. Apply the same 0x01 prefix so drivers see
    //   valid binary JSONB.
    if is_jsonb
        && matches!(
            col.data_type(),
            DataType::LargeBinary | DataType::Binary | DataType::Utf8 | DataType::LargeUtf8
        )
    {
        let bytes: &[u8] = match col.data_type() {
            DataType::LargeBinary => col.as_binary::<i64>().value(idx),
            DataType::Binary => col.as_binary::<i32>().value(idx),
            DataType::Utf8 => col.as_string::<i32>().value(idx).as_bytes(),
            DataType::LargeUtf8 => col.as_string::<i64>().value(idx).as_bytes(),
            _ => unreachable!(),
        };
        buf.put_i32((bytes.len() as i32) + 1);
        buf.put_u8(1);
        buf.put_slice(bytes);
        return Ok(());
    }
    // UUID binary format: 16 raw bytes, no length-prefix tweak. The
    // Arrow column is `FixedSizeBinary(16)`; the bytes already match the
    // wire format `tokio-postgres` decodes a `Uuid` from.
    if is_uuid {
        if let DataType::FixedSizeBinary(16) = col.data_type() {
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                .expect("FixedSizeBinaryArray for UUID column");
            let bytes = arr.value(idx);
            buf.put_i32(16);
            buf.put_slice(bytes);
            return Ok(());
        }
    }
    match col.data_type() {
        DataType::Boolean => {
            let v = col.as_boolean().value(idx);
            buf.put_i32(1);
            buf.put_u8(if v { 1 } else { 0 });
        }
        // Arrow Int8 / UInt8 / UInt16 have no direct PG type; promote to INT2
        // (2-byte big-endian i16) — consistent with how arrow_to_pg_type maps them.
        DataType::Int8 => {
            let v = col.as_primitive::<Int8Type>().value(idx) as i16;
            buf.put_i32(2);
            buf.put_i16(v);
        }
        DataType::UInt8 => {
            let v = col.as_primitive::<UInt8Type>().value(idx) as i16;
            buf.put_i32(2);
            buf.put_i16(v);
        }
        DataType::UInt16 => {
            let v = col.as_primitive::<UInt16Type>().value(idx) as i16;
            buf.put_i32(2);
            buf.put_i16(v);
        }
        DataType::Int16 => {
            let v = col.as_primitive::<Int16Type>().value(idx);
            buf.put_i32(2);
            buf.put_i16(v);
        }
        DataType::Int32 => {
            let v = col.as_primitive::<Int32Type>().value(idx);
            buf.put_i32(4);
            buf.put_i32(v);
        }
        DataType::Int64 => {
            let v = col.as_primitive::<Int64Type>().value(idx);
            buf.put_i32(8);
            buf.put_i64(v);
        }
        DataType::UInt32 => {
            // Map UInt32 → INT8 binary (consistent with our type mapping).
            let v = col.as_primitive::<UInt32Type>().value(idx) as i64;
            buf.put_i32(8);
            buf.put_i64(v);
        }
        DataType::UInt64 => {
            let v = col.as_primitive::<UInt64Type>().value(idx) as i64;
            buf.put_i32(8);
            buf.put_i64(v);
        }
        DataType::Float32 => {
            let v = col.as_primitive::<Float32Type>().value(idx);
            buf.put_i32(4);
            buf.put_f32(v);
        }
        DataType::Float64 => {
            let v = col.as_primitive::<Float64Type>().value(idx);
            buf.put_i32(8);
            buf.put_f64(v);
        }
        DataType::Utf8 => {
            let s = col.as_string::<i32>().value(idx);
            buf.put_i32(s.len() as i32);
            buf.put_slice(s.as_bytes());
        }
        DataType::LargeUtf8 => {
            let s = col.as_string::<i64>().value(idx);
            buf.put_i32(s.len() as i32);
            buf.put_slice(s.as_bytes());
        }
        DataType::Binary => {
            let bytes = col.as_binary::<i32>().value(idx);
            buf.put_i32(bytes.len() as i32);
            buf.put_slice(bytes);
        }
        DataType::LargeBinary => {
            let bytes = col.as_binary::<i64>().value(idx);
            buf.put_i32(bytes.len() as i32);
            buf.put_slice(bytes);
        }
        DataType::Date32 => {
            // PG DATE binary: i32 days since 2000-01-01. Arrow Date32 counts
            // from 1970-01-01; rebase by the 10957-day offset.
            let days = col.as_primitive::<Date32Type>().value(idx);
            buf.put_i32(4);
            buf.put_i32(days - PG_EPOCH_DAYS_FROM_UNIX);
        }
        DataType::Timestamp(unit, _tz) => {
            // PG TIMESTAMP[TZ] binary: 8-byte i64 microseconds since
            // 2000-01-01 00:00:00 UTC. Arrow's epoch is 1970-01-01; rebase
            // after normalizing the granularity to microseconds.
            let raw: i64 = match unit {
                TimeUnit::Second => col.as_primitive::<TimestampSecondType>().value(idx),
                TimeUnit::Millisecond => col.as_primitive::<TimestampMillisecondType>().value(idx),
                TimeUnit::Microsecond => col.as_primitive::<TimestampMicrosecondType>().value(idx),
                TimeUnit::Nanosecond => col.as_primitive::<TimestampNanosecondType>().value(idx),
            };
            let unix_micros: i64 = match unit {
                TimeUnit::Second => raw.saturating_mul(1_000_000),
                TimeUnit::Millisecond => raw.saturating_mul(1_000),
                TimeUnit::Microsecond => raw,
                TimeUnit::Nanosecond => raw / 1_000,
            };
            buf.put_i32(8);
            buf.put_i64(unix_micros - PG_EPOCH_MICROS_FROM_UNIX);
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            // PG INTERVAL binary: i64 microseconds, i32 days, i32 months
            // (16 bytes total, big-endian). Arrow's MonthDayNano is i128
            // packed; `to_parts` returns signed components.
            let v = col.as_primitive::<IntervalMonthDayNanoType>().value(idx);
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(v);
            let micros = nanos / 1_000;
            buf.put_i32(16);
            buf.put_i64(micros);
            buf.put_i32(days);
            buf.put_i32(months);
        }
        DataType::Decimal128(_, scale) => {
            let scale = *scale as u16;
            encode_numeric_binary(
                col.as_any()
                    .downcast_ref::<arrow_array::Decimal128Array>()
                    .expect("Decimal128Array for NUMERIC column")
                    .value(idx),
                scale,
                buf,
            );
        }
        // PG array binary wire format. 1-D only; nested arrays (ListArray of
        // ListArray) would need recursive handling and are deferred until a
        // concrete production need arises.
        DataType::List(_field) => {
            let list_arr = col
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("ListArray for List column");
            let elem_array = list_arr.value(idx);
            encode_array_binary(elem_array.as_ref(), buf);
        }
        DataType::LargeList(_field) => {
            let list_arr = col
                .as_any()
                .downcast_ref::<LargeListArray>()
                .expect("LargeListArray for LargeList column");
            let elem_array = list_arr.value(idx);
            encode_array_binary(elem_array.as_ref(), buf);
        }
        DataType::FixedSizeList(_field, _size) => {
            let list_arr = col
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .expect("FixedSizeListArray for FixedSizeList column");
            let elem_array = list_arr.value(idx);
            encode_array_binary(elem_array.as_ref(), buf);
        }
        // For types we don't have a binary representation for yet, fall back
        // to text. This is a deliberate v1 trade-off: drivers requesting
        // binary for an exotic type get the text form, which is a violation
        // of the protocol but is decodable as long as the driver is lenient.
        // The alternative — erroring — breaks queries that mention any
        // unmapped column.
        other => {
            tracing::debug!(
                ?other,
                "binary format not implemented for type, emitting text"
            );
            // Fallback path: uncommon / unmapped types fall back to text.
            // A local StackFmt is fine here because this branch is not on
            // the hot path (well-typed columns never reach it).
            let mut sf = StackFmt::new();
            encode_value(col, idx, buf, is_jsonb, is_uuid, &mut sf);
        }
    }
    Ok(())
}

/// Encode an Arrow `Decimal128` value as the Postgres binary wire format for
/// the `numeric` type (OID 1700).
///
/// ## Wire layout (all big-endian)
///
/// ```text
/// u16  ndigits   — number of base-10000 "digits" in the digit array
/// i16  weight    — positional weight of the first (most-significant) digit
///                  (weight=0 means the digit represents units 1–9999,
///                   weight=1 means 10000–99999999, etc.)
/// u16  sign      — 0x0000 positive, 0x4000 negative, 0xC000 NaN
/// u16  dscale    — decimal digits after the decimal point to display
/// u16  digits…   — ndigits × base-10000 digit values (each 0–9999)
/// ```
///
/// ## Algorithm
///
/// Arrow stores `Decimal128(p, s)` as a signed 128-bit integer where the
/// true value is `i128_value / 10^scale`. We need to decompose the number
/// into PG's base-10000 representation.
///
/// 1. Extract sign; work with the absolute value.
/// 2. Separate integer and fractional parts using the scale.
/// 3. Decompose the integer part into base-10000 digits (most-significant
///    first). `weight` is the index of the most-significant digit in PG's
///    numbering (0-based from the units position).
/// 4. Decompose the fractional part into base-10000 digits (after the
///    decimal point, each group covers 4 decimal places).
/// 5. Strip trailing zero digits from the digit array (PG does this too),
///    but preserve `dscale` for display fidelity.
///
/// Zero is a special case: ndigits=0, weight=0, sign=0, dscale=0.
fn encode_numeric_binary(raw: i128, scale: u16, buf: &mut BytesMut) {
    // PG sign constants.
    const NUMERIC_POS: u16 = 0x0000;
    const NUMERIC_NEG: u16 = 0x4000;

    // --- 1. Sign + absolute value ---
    let (sign, abs_val): (u16, u128) = if raw < 0 {
        (NUMERIC_NEG, raw.unsigned_abs())
    } else {
        (NUMERIC_POS, raw as u128)
    };

    // --- 2. Separate integer and fractional parts ---
    // scale = number of decimal digits after the decimal point.
    // divisor = 10^scale
    let divisor: u128 = 10u128.pow(scale as u32);
    let int_part: u128 = abs_val / divisor;
    let frac_part: u128 = abs_val % divisor;

    // --- 3. Decompose integer part into base-10000 digits (most-significant first) ---
    // We collect digits in reverse (least-significant first), then reverse.
    let mut int_digits: Vec<u16> = Vec::with_capacity(10);
    let mut tmp = int_part;
    if tmp == 0 {
        // No integer digits — we'll still need a digit slot if frac_part is nonzero.
        // Leave int_digits empty; weight will be -1 (relative to the first frac digit).
    } else {
        while tmp > 0 {
            int_digits.push((tmp % 10000) as u16);
            tmp /= 10000;
        }
        int_digits.reverse();
    }

    // --- 4. Decompose fractional part into base-10000 digits ---
    // `scale` decimal places → ceil(scale / 4) base-10000 digit groups.
    // Each group covers exactly 4 decimal positions. The fractional integer
    // is scaled up to fill full 4-digit groups if scale is not a multiple of 4.
    let frac_groups = scale.div_ceil(4) as usize;
    let mut frac_digits: Vec<u16> = Vec::with_capacity(frac_groups);
    if frac_groups > 0 {
        // Pad frac_part to exactly frac_groups*4 decimal digits.
        let full_scale = frac_groups as u32 * 4;
        // Scale up by the difference so frac_part fills full groups.
        let padding = full_scale - scale as u32;
        let padded = frac_part * 10u128.pow(padding);

        // Extract frac_groups base-10000 digits (most-significant first).
        // The most-significant frac group represents 10^(full_scale-4)..10^(full_scale-1).
        let mut divisor_group = 10u128.pow((frac_groups as u32 - 1) * 4);
        let mut remaining = padded;
        for _ in 0..frac_groups {
            frac_digits.push((remaining / divisor_group) as u16);
            remaining %= divisor_group;
            if divisor_group > 1 {
                divisor_group /= 10000;
            }
        }
    }

    // --- 5. Strip trailing zero digit groups ---
    // PG strips trailing zeros from the digit array while preserving dscale.
    while frac_digits.last() == Some(&0) {
        frac_digits.pop();
    }

    // --- Special case: zero ---
    if int_part == 0 && frac_part == 0 {
        // PG encodes zero as ndigits=0, weight=0, sign=0x0000, dscale=0
        // regardless of the declared scale.
        let body_len: i32 = 8; // 4 × u16 header, no digit array
        buf.put_i32(body_len);
        buf.put_u16(0); // ndigits
        buf.put_i16(0); // weight
        buf.put_u16(NUMERIC_POS); // sign
        buf.put_u16(0); // dscale
        return;
    }

    // --- 6. Assemble final digit array and compute weight ---
    let all_digits: Vec<u16> = int_digits
        .iter()
        .chain(frac_digits.iter())
        .copied()
        .collect();

    // `weight` = index of first digit in PG's scheme:
    //   0 means that digit represents values 1..9999 (units group),
    //   1 means 10000..99999999, etc.
    // For a number with `int_digits.len()` integer digit groups:
    //   weight = int_digits.len() - 1  (if there are integer digits)
    //   weight = -1 (if int_part == 0, the first frac digit is weight -1,
    //               but that can recurse for leading zero frac groups)
    let weight: i16 = if !int_digits.is_empty() {
        (int_digits.len() as i16) - 1
    } else {
        // int_part == 0: find first nonzero frac digit to determine weight.
        // Each frac group represents weight -1, -2, etc.
        let first_nonzero = frac_digits.iter().position(|&d| d != 0).unwrap_or(0);
        -1 - (first_nonzero as i16)
    };

    // Strip leading zero digit groups (can appear when int_part==0 and
    // the frac_digits vector has leading zeros before the first nonzero group).
    let digits: Vec<u16> = {
        let skip = all_digits.iter().position(|&d| d != 0).unwrap_or(0);
        all_digits[skip..].to_vec()
    };

    let ndigits = digits.len() as u16;
    let body_len: i32 = 8 + (ndigits as i32) * 2;
    buf.put_i32(body_len);
    buf.put_u16(ndigits);
    buf.put_i16(weight);
    buf.put_u16(sign);
    buf.put_u16(scale); // dscale = display scale (number of decimal digits to show)
    for d in &digits {
        buf.put_u16(*d);
    }
}

/// Map an Arrow `DataType` to the Postgres element OID used inside the PG
/// array binary wire header. Mirrors `arrow_to_pg_type` but returns the raw
/// `u32` OID rather than a `pgwire::Type` because the array header needs the
/// *element* OID directly.
///
/// Falls back to `Type::TEXT.oid()` (25) for any unmapped type, consistent
/// with the text-fallback policy used elsewhere in this file.
fn oid_for_arrow_type(dt: &DataType) -> u32 {
    match dt {
        DataType::Boolean => Type::BOOL.oid(),
        // Arrow Int8 / UInt8 / UInt16 have no direct PG element type; promote
        // to INT2 (OID 21), consistent with arrow_to_pg_type.
        DataType::Int8 | DataType::UInt8 | DataType::UInt16 | DataType::Int16 => Type::INT2.oid(),
        DataType::Int32 => Type::INT4.oid(),
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => Type::INT8.oid(),
        DataType::Float32 => Type::FLOAT4.oid(),
        DataType::Float64 => Type::FLOAT8.oid(),
        DataType::Utf8 | DataType::LargeUtf8 => Type::TEXT.oid(),
        DataType::Binary | DataType::LargeBinary => Type::BYTEA.oid(),
        DataType::Date32 => Type::DATE.oid(),
        DataType::Timestamp(_, Some(_)) => Type::TIMESTAMPTZ.oid(),
        DataType::Timestamp(_, None) => Type::TIMESTAMP.oid(),
        DataType::Interval(IntervalUnit::MonthDayNano) => Type::INTERVAL.oid(),
        DataType::Decimal128(_, _) => Type::NUMERIC.oid(),
        _ => Type::TEXT.oid(),
    }
}

/// Encode a single element at index `j` of `elem_array` into `buf` as a
/// length-prefixed binary value (or -1 for NULL). This is the per-element
/// encoding used inside the PG array binary wire format.
///
/// Supports the same scalar types as `encode_value_binary`; exotic types fall
/// back to a text encoding (same lenient policy as the outer scalar path).
fn encode_element_binary(elem_array: &dyn Array, j: usize, buf: &mut BytesMut) {
    if elem_array.is_null(j) {
        buf.put_i32(-1);
        return;
    }
    match elem_array.data_type() {
        DataType::Boolean => {
            let v = elem_array.as_boolean().value(j);
            buf.put_i32(1);
            buf.put_u8(if v { 1 } else { 0 });
        }
        DataType::Int16 => {
            let v = elem_array.as_primitive::<Int16Type>().value(j);
            buf.put_i32(2);
            buf.put_i16(v);
        }
        DataType::Int32 => {
            let v = elem_array.as_primitive::<Int32Type>().value(j);
            buf.put_i32(4);
            buf.put_i32(v);
        }
        DataType::Int64 => {
            let v = elem_array.as_primitive::<Int64Type>().value(j);
            buf.put_i32(8);
            buf.put_i64(v);
        }
        DataType::UInt32 => {
            let v = elem_array.as_primitive::<UInt32Type>().value(j) as i64;
            buf.put_i32(8);
            buf.put_i64(v);
        }
        DataType::UInt64 => {
            let v = elem_array.as_primitive::<UInt64Type>().value(j) as i64;
            buf.put_i32(8);
            buf.put_i64(v);
        }
        DataType::Float32 => {
            let v = elem_array.as_primitive::<Float32Type>().value(j);
            buf.put_i32(4);
            buf.put_f32(v);
        }
        DataType::Float64 => {
            let v = elem_array.as_primitive::<Float64Type>().value(j);
            buf.put_i32(8);
            buf.put_f64(v);
        }
        DataType::Utf8 => {
            let s = elem_array.as_string::<i32>().value(j);
            buf.put_i32(s.len() as i32);
            buf.put_slice(s.as_bytes());
        }
        DataType::LargeUtf8 => {
            let s = elem_array.as_string::<i64>().value(j);
            buf.put_i32(s.len() as i32);
            buf.put_slice(s.as_bytes());
        }
        DataType::Binary => {
            let bytes = elem_array.as_binary::<i32>().value(j);
            buf.put_i32(bytes.len() as i32);
            buf.put_slice(bytes);
        }
        DataType::LargeBinary => {
            let bytes = elem_array.as_binary::<i64>().value(j);
            buf.put_i32(bytes.len() as i32);
            buf.put_slice(bytes);
        }
        DataType::Date32 => {
            let days = elem_array.as_primitive::<Date32Type>().value(j);
            buf.put_i32(4);
            buf.put_i32(days - PG_EPOCH_DAYS_FROM_UNIX);
        }
        DataType::Timestamp(unit, _tz) => {
            let raw: i64 = match unit {
                TimeUnit::Second => elem_array.as_primitive::<TimestampSecondType>().value(j),
                TimeUnit::Millisecond => elem_array
                    .as_primitive::<TimestampMillisecondType>()
                    .value(j),
                TimeUnit::Microsecond => elem_array
                    .as_primitive::<TimestampMicrosecondType>()
                    .value(j),
                TimeUnit::Nanosecond => elem_array
                    .as_primitive::<TimestampNanosecondType>()
                    .value(j),
            };
            let unix_micros: i64 = match unit {
                TimeUnit::Second => raw.saturating_mul(1_000_000),
                TimeUnit::Millisecond => raw.saturating_mul(1_000),
                TimeUnit::Microsecond => raw,
                TimeUnit::Nanosecond => raw / 1_000,
            };
            buf.put_i32(8);
            buf.put_i64(unix_micros - PG_EPOCH_MICROS_FROM_UNIX);
        }
        DataType::Decimal128(_, scale) => {
            let scale = *scale as u16;
            encode_numeric_binary(
                elem_array
                    .as_any()
                    .downcast_ref::<arrow_array::Decimal128Array>()
                    .expect("Decimal128Array for NUMERIC array element")
                    .value(j),
                scale,
                buf,
            );
        }
        other => {
            // Fallback: emit the element as text (same lenient policy as the
            // outer scalar catch-all). Strict drivers may balk, but this is
            // better than panicking or omitting the element.
            tracing::debug!(
                ?other,
                "array element binary format not implemented, emitting text"
            );
            let s = render_cell(elem_array, j);
            buf.put_i32(s.len() as i32);
            buf.put_slice(s.as_bytes());
        }
    }
}

/// Encode a 1-D Arrow list as the Postgres binary array wire format.
///
/// ## Wire layout (all big-endian, per PG docs §55.2.12 + `postgres-types`
/// `array_to_sql`):
/// ```text
/// i32  ndim         = 1
/// i32  has_nulls    = 0 or 1
/// u32  elem_oid     = OID of element type
/// i32  dim_len      = N (number of elements)
/// i32  lower_bound  = 1 (PG arrays are 1-indexed by default)
/// // N elements, each:
/// i32  elem_len     = byte-length of element, or -1 for NULL
/// [elem_len bytes]  = binary encoding of the element (absent if NULL)
/// ```
///
/// Multi-dimension arrays (ndim > 1): deferred — basin's Arrow arrays are
/// 1-D in all current production paths. Nested ListArrays would require
/// recursive handling; document this and punt until needed.
fn encode_array_binary(elem_array: &dyn Array, buf: &mut BytesMut) {
    let n = elem_array.len();
    let has_nulls: i32 = if elem_array.null_count() > 0 { 1 } else { 0 };
    let elem_oid: u32 = oid_for_arrow_type(elem_array.data_type());

    // Reserve space for the outer length prefix; we'll fill it in at the end.
    let len_pos = buf.len();
    buf.put_i32(0); // placeholder — overwritten below

    // Array header.
    buf.put_i32(1); // ndim = 1
    buf.put_i32(has_nulls);
    buf.put_u32(elem_oid);
    buf.put_i32(n as i32); // dim_len
    buf.put_i32(1); // lower_bound (1-indexed)

    // Elements.
    for j in 0..n {
        encode_element_binary(elem_array, j, buf);
    }

    // Back-fill the outer length prefix (bytes written after the 4-byte len).
    let body_len = (buf.len() - len_pos - 4) as i32;
    let bytes = body_len.to_be_bytes();
    buf[len_pos..len_pos + 4].copy_from_slice(&bytes);
}

/// Encode the value at row `idx` of `col` into `buf` as a length-prefixed
/// text-format Postgres field. NULLs get a -1 length and no body.
///
/// Common types (`Boolean`, `Utf8`, `LargeUtf8`, integer and float primitives)
/// are written directly into `buf` without a heap-allocated intermediate
/// `String`. `sf` is a reusable stack-allocated scratch buffer for integer/
/// float-to-text formatting; callers pass the same `StackFmt` across all cells
/// in all rows so no additional allocations occur on the hot path.
fn encode_value(
    col: &dyn Array,
    idx: usize,
    buf: &mut BytesMut,
    is_jsonb: bool,
    is_uuid: bool,
    sf: &mut StackFmt,
) {
    if col.is_null(idx) {
        buf.put_i32(-1);
        return;
    }

    // JSONB columns are stored as `LargeBinary` with the marker on the
    // schema's `Field`; their bytes are *already* canonical-form JSON text
    // (see `basin_engine::dml::coerce_jsonb`). Emit those bytes verbatim
    // so clients see real JSON, not a `\x...` hex blob.
    if is_jsonb && matches!(col.data_type(), DataType::LargeBinary | DataType::Binary) {
        let bytes: &[u8] = match col.data_type() {
            DataType::LargeBinary => col.as_binary::<i64>().value(idx),
            DataType::Binary => col.as_binary::<i32>().value(idx),
            _ => unreachable!(),
        };
        buf.put_i32(bytes.len() as i32);
        buf.put_slice(bytes);
        return;
    }
    // UUID columns are `FixedSizeBinary(16)`; render the canonical
    // hyphenated lowercase form (8-4-4-4-12). This is what every
    // PG-protocol client expects, and what `tokio-postgres` decodes
    // back into a `Uuid` when its column type matches OID 2950.
    if is_uuid {
        if let DataType::FixedSizeBinary(16) = col.data_type() {
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                .expect("FixedSizeBinaryArray for UUID column");
            let bytes = arr.value(idx);
            let s = render_uuid(bytes);
            buf.put_i32(s.len() as i32);
            buf.put_slice(s.as_bytes());
            return;
        }
    }

    // Fast-path: types where we can write directly into `buf` without an
    // intermediate `String` heap allocation.
    match col.data_type() {
        // Text columns: bytes are already UTF-8 in the Arrow buffer — zero
        // copy, zero heap allocation.
        DataType::Utf8 => {
            let s = col.as_string::<i32>().value(idx);
            buf.put_i32(s.len() as i32);
            buf.put_slice(s.as_bytes());
            return;
        }
        DataType::LargeUtf8 => {
            let s = col.as_string::<i64>().value(idx);
            buf.put_i32(s.len() as i32);
            buf.put_slice(s.as_bytes());
            return;
        }
        // Boolean: Postgres text is 't' or 'f' (1 byte).
        DataType::Boolean => {
            let v = col.as_boolean().value(idx);
            buf.put_i32(1);
            buf.put_u8(if v { b't' } else { b'f' });
            return;
        }
        // Integer primitives: format into the stack scratch buffer, then
        // write length + bytes. No heap allocation.
        DataType::Int8 => {
            sf.reset();
            let _ = write!(sf, "{}", col.as_primitive::<Int8Type>().value(idx));
            buf.put_i32(sf.as_bytes().len() as i32);
            buf.put_slice(sf.as_bytes());
            return;
        }
        DataType::Int16 => {
            sf.reset();
            let _ = write!(sf, "{}", col.as_primitive::<Int16Type>().value(idx));
            buf.put_i32(sf.as_bytes().len() as i32);
            buf.put_slice(sf.as_bytes());
            return;
        }
        DataType::Int32 => {
            sf.reset();
            let _ = write!(sf, "{}", col.as_primitive::<Int32Type>().value(idx));
            buf.put_i32(sf.as_bytes().len() as i32);
            buf.put_slice(sf.as_bytes());
            return;
        }
        DataType::Int64 => {
            sf.reset();
            let _ = write!(sf, "{}", col.as_primitive::<Int64Type>().value(idx));
            buf.put_i32(sf.as_bytes().len() as i32);
            buf.put_slice(sf.as_bytes());
            return;
        }
        DataType::UInt8 => {
            sf.reset();
            let _ = write!(sf, "{}", col.as_primitive::<UInt8Type>().value(idx));
            buf.put_i32(sf.as_bytes().len() as i32);
            buf.put_slice(sf.as_bytes());
            return;
        }
        DataType::UInt16 => {
            sf.reset();
            let _ = write!(sf, "{}", col.as_primitive::<UInt16Type>().value(idx));
            buf.put_i32(sf.as_bytes().len() as i32);
            buf.put_slice(sf.as_bytes());
            return;
        }
        DataType::UInt32 => {
            sf.reset();
            let _ = write!(sf, "{}", col.as_primitive::<UInt32Type>().value(idx));
            buf.put_i32(sf.as_bytes().len() as i32);
            buf.put_slice(sf.as_bytes());
            return;
        }
        DataType::UInt64 => {
            sf.reset();
            let _ = write!(sf, "{}", col.as_primitive::<UInt64Type>().value(idx));
            buf.put_i32(sf.as_bytes().len() as i32);
            buf.put_slice(sf.as_bytes());
            return;
        }
        DataType::Float32 => {
            sf.reset();
            let _ = write!(sf, "{}", col.as_primitive::<Float32Type>().value(idx));
            buf.put_i32(sf.as_bytes().len() as i32);
            buf.put_slice(sf.as_bytes());
            return;
        }
        DataType::Float64 => {
            sf.reset();
            let _ = write!(sf, "{}", col.as_primitive::<Float64Type>().value(idx));
            buf.put_i32(sf.as_bytes().len() as i32);
            buf.put_slice(sf.as_bytes());
            return;
        }
        _ => {}
    }

    // Fallback: types that still need render_cell (timestamps, dates,
    // intervals, bytea, decimal, fallback debug). These are less common on
    // the hot path in typical OLTP-style result sets.
    let s = render_cell(col, idx);
    buf.put_i32(s.len() as i32);
    buf.put_slice(s.as_bytes());
}

/// Format 16 raw bytes as the canonical hyphenated UUID text form
/// (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, lowercase). The `uuid` crate
/// would do the same job; we call it directly to avoid an `unwrap` and
/// to keep the conversion explicit.
fn render_uuid(bytes: &[u8]) -> String {
    debug_assert_eq!(bytes.len(), 16, "UUID bytes must be exactly 16");
    if bytes.len() != 16 {
        return String::new();
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(bytes);
    uuid::Uuid::from_bytes(arr).hyphenated().to_string()
}

/// Render one Arrow cell as a UTF-8 string for the `COPY TO STDOUT` CSV
/// path. Same shape as the simple-query text encoder, but JSONB / UUID
/// metadata-driven types take the same logical-type code path so the
/// CSV cell looks the way a `psql \copy` user expects (raw JSON text,
/// hyphenated UUID).
pub(crate) fn render_cell_for_copy(col: &dyn Array, idx: usize, field: &Field) -> String {
    if field_is_jsonb(field) && matches!(col.data_type(), DataType::LargeBinary | DataType::Binary)
    {
        let bytes: &[u8] = match col.data_type() {
            DataType::LargeBinary => col.as_binary::<i64>().value(idx),
            DataType::Binary => col.as_binary::<i32>().value(idx),
            _ => unreachable!(),
        };
        // JSONB cells are already canonical-form JSON text bytes.
        return String::from_utf8_lossy(bytes).into_owned();
    }
    if field_is_uuid(field) {
        if let DataType::FixedSizeBinary(16) = col.data_type() {
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                .expect("FixedSizeBinaryArray for UUID column");
            return render_uuid(arr.value(idx));
        }
    }
    render_cell(col, idx)
}

fn render_cell(col: &dyn Array, idx: usize) -> String {
    match col.data_type() {
        DataType::Boolean => {
            // Postgres text encoding of bool is 't' / 'f'.
            let v = col.as_boolean().value(idx);
            if v { "t" } else { "f" }.to_owned()
        }
        DataType::Int8 => col.as_primitive::<Int8Type>().value(idx).to_string(),
        DataType::Int16 => col.as_primitive::<Int16Type>().value(idx).to_string(),
        DataType::Int32 => col.as_primitive::<Int32Type>().value(idx).to_string(),
        DataType::Int64 => col.as_primitive::<Int64Type>().value(idx).to_string(),
        DataType::UInt8 => col.as_primitive::<UInt8Type>().value(idx).to_string(),
        DataType::UInt16 => col.as_primitive::<UInt16Type>().value(idx).to_string(),
        DataType::UInt32 => col.as_primitive::<UInt32Type>().value(idx).to_string(),
        DataType::UInt64 => col.as_primitive::<UInt64Type>().value(idx).to_string(),
        DataType::Float32 => col.as_primitive::<Float32Type>().value(idx).to_string(),
        DataType::Float64 => col.as_primitive::<Float64Type>().value(idx).to_string(),
        DataType::Utf8 => col.as_string::<i32>().value(idx).to_owned(),
        DataType::LargeUtf8 => col.as_string::<i64>().value(idx).to_owned(),
        // Postgres text encoding of bytea is `\x` + lowercase hex
        // (the default since `bytea_output = hex` was made the
        // default in PG 9.0).
        DataType::Binary => render_bytea(col.as_binary::<i32>().value(idx)),
        DataType::LargeBinary => render_bytea(col.as_binary::<i64>().value(idx)),
        DataType::Timestamp(unit, tz) => render_timestamp(col, idx, unit, tz.as_deref()),
        DataType::Date32 => {
            let days = col.as_primitive::<Date32Type>().value(idx);
            Date32Type::to_naive_date(days)
                .format("%Y-%m-%d")
                .to_string()
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let v = col.as_primitive::<IntervalMonthDayNanoType>().value(idx);
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(v);
            render_interval(months, days, nanos)
        }
        DataType::Decimal128(_, _) => {
            // PG `numeric` text form: base-10 string with `scale` digits
            // after the decimal point (or no point if scale=0). Arrow's
            // `Decimal128Array::value_as_string` already produces exactly
            // this shape (e.g. value `1234500` with scale=4 → `"123.4500"`).
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::Decimal128Array>()
                .expect("Decimal128Array for NUMERIC column");
            arr.value_as_string(idx)
        }
        // Fallback: best-effort Debug rendering.
        other => format!("{other:?}@{idx}"),
    }
}

fn render_bytea(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("\\x");
    for b in bytes {
        // write! into a &mut String uses fmt::Write, which avoids the
        // inner heap allocation that `format!("{b:02x}")` would produce.
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Render a Postgres-style interval text from `(months, days, nanos)`. Empty
/// (zero) intervals render as `00:00:00`, matching `psql` output. The time
/// component is suppressed when zero unless months/days are also zero.
fn render_interval(months: i32, days: i32, nanos: i64) -> String {
    let mut parts: Vec<String> = Vec::new();
    let years = months / 12;
    let mons = months % 12;
    if years != 0 {
        parts.push(format!(
            "{years} year{}",
            if years.abs() == 1 { "" } else { "s" }
        ));
    }
    if mons != 0 {
        parts.push(format!(
            "{mons} mon{}",
            if mons.abs() == 1 { "" } else { "s" }
        ));
    }
    if days != 0 {
        parts.push(format!(
            "{days} day{}",
            if days.abs() == 1 { "" } else { "s" }
        ));
    }
    if nanos != 0 || parts.is_empty() {
        let neg = nanos < 0;
        let abs_nanos = nanos.unsigned_abs();
        let total_secs = abs_nanos / 1_000_000_000;
        let frac_nanos = abs_nanos % 1_000_000_000;
        let hh = total_secs / 3600;
        let mm = (total_secs % 3600) / 60;
        let ss = total_secs % 60;
        let sign = if neg { "-" } else { "" };
        if frac_nanos == 0 {
            parts.push(format!("{sign}{hh:02}:{mm:02}:{ss:02}"));
        } else {
            // PG renders microseconds (6 digits), trimming trailing zeros only
            // up to the microsecond boundary. Match that by using %.6f-style
            // microseconds.
            let micros = frac_nanos / 1_000;
            parts.push(format!("{sign}{hh:02}:{mm:02}:{ss:02}.{micros:06}"));
        }
    }
    parts.join(" ")
}

fn render_timestamp(col: &dyn Array, idx: usize, unit: &TimeUnit, _tz: Option<&str>) -> String {
    // Convert the integer cell into a chrono DateTime<Utc>. We treat all
    // timestamps as UTC for the PoC; production will respect the tz field.
    let raw: i64 = match unit {
        TimeUnit::Second => col.as_primitive::<TimestampSecondType>().value(idx),
        TimeUnit::Millisecond => col.as_primitive::<TimestampMillisecondType>().value(idx),
        TimeUnit::Microsecond => col.as_primitive::<TimestampMicrosecondType>().value(idx),
        TimeUnit::Nanosecond => col.as_primitive::<TimestampNanosecondType>().value(idx),
    };
    let dt: Option<DateTime<Utc>> = match unit {
        TimeUnit::Second => Utc.timestamp_opt(raw, 0).single(),
        TimeUnit::Millisecond => Utc.timestamp_millis_opt(raw).single(),
        TimeUnit::Microsecond => {
            let secs = raw.div_euclid(1_000_000);
            let nanos = (raw.rem_euclid(1_000_000) * 1_000) as u32;
            Utc.timestamp_opt(secs, nanos).single()
        }
        TimeUnit::Nanosecond => {
            let secs = raw.div_euclid(1_000_000_000);
            let nanos = raw.rem_euclid(1_000_000_000) as u32;
            Utc.timestamp_opt(secs, nanos).single()
        }
    };
    match dt {
        Some(dt) => dt.to_rfc3339(),
        None => NaiveDateTime::default().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Int64Array, StringArray};
    use arrow_schema::{Field, Schema as ArrowSchema};

    #[test]
    fn maps_basic_types() {
        assert_eq!(arrow_to_pg_type(&DataType::Int64), Type::INT8);
        assert_eq!(arrow_to_pg_type(&DataType::Boolean), Type::BOOL);
        assert_eq!(arrow_to_pg_type(&DataType::Float64), Type::FLOAT8);
        assert_eq!(arrow_to_pg_type(&DataType::Utf8), Type::TEXT);
    }

    #[test]
    fn maps_date_and_interval() {
        // Date32 must surface as PG OID 1082 (DATE), not the TEXT fallback —
        // pgwire clients rely on this for `current_date` and friends.
        assert_eq!(arrow_to_pg_type(&DataType::Date32), Type::DATE);
        assert_eq!(Type::DATE.oid(), 1082);
        // MonthDayNano interval is what `age()` will return once the engine
        // arrow-bridge gains it; map it to PG OID 1186 (INTERVAL).
        assert_eq!(
            arrow_to_pg_type(&DataType::Interval(IntervalUnit::MonthDayNano)),
            Type::INTERVAL,
        );
        assert_eq!(Type::INTERVAL.oid(), 1186);
    }

    #[test]
    fn row_description_advertises_date_and_interval_oids() {
        let schema = ArrowSchema::new(vec![
            Field::new("d", DataType::Date32, true),
            Field::new("i", DataType::Interval(IntervalUnit::MonthDayNano), true),
        ]);
        let rd = row_description(&schema);
        assert_eq!(rd.fields.len(), 2);
        assert_eq!(rd.fields[0].name, "d");
        assert_eq!(rd.fields[0].type_id, 1082);
        assert_eq!(rd.fields[0].type_size, 4);
        assert_eq!(rd.fields[1].name, "i");
        assert_eq!(rd.fields[1].type_id, 1186);
        assert_eq!(rd.fields[1].type_size, 16);
    }

    #[test]
    fn renders_date_text() {
        use arrow_array::Date32Array;
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "d",
            DataType::Date32,
            false,
        )]));
        // 2024-03-14 = 19796 days since 1970-01-01.
        let days =
            Date32Type::from_naive_date(chrono::NaiveDate::from_ymd_opt(2024, 3, 14).unwrap());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Date32Array::from(vec![days]))],
        )
        .unwrap();
        let rows = encode_batches(&schema, &[batch]);
        // i32 length prefix = 10, then "2024-03-14".
        assert_eq!(&rows[0].data[0..4], &10i32.to_be_bytes());
        assert_eq!(&rows[0].data[4..14], b"2024-03-14");
    }

    #[test]
    fn renders_interval_text() {
        // Direct unit-test of the text formatter; building an
        // IntervalMonthDayNanoArray here would just round-trip through
        // `to_parts`. Cover the cases pgwire clients care about.
        assert_eq!(render_interval(0, 0, 0), "00:00:00");
        assert_eq!(render_interval(11, 30, 0), "11 mons 30 days");
        assert_eq!(render_interval(12, 0, 0), "1 year");
        assert_eq!(render_interval(0, 0, 9_296_000_000_000), "02:34:56");
        assert_eq!(
            render_interval(13, 1, 9_296_000_000_000),
            "1 year 1 mon 1 day 02:34:56",
        );
    }

    #[test]
    fn encodes_three_rows() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();
        let rows = encode_batches(&schema, &[batch]);
        assert_eq!(rows.len(), 3);
        for r in &rows {
            assert_eq!(r.field_count, 2);
        }
        // Row 1 (idx 1) has a NULL second column. The body should encode the
        // int "2" then a -1 length for NULL.
        let row1 = &rows[1];
        // i32 length = 1, then byte '2'. Then i32 length = -1.
        assert_eq!(&row1.data[0..4], &1i32.to_be_bytes());
        assert_eq!(row1.data[4], b'2');
        assert_eq!(&row1.data[5..9], &(-1i32).to_be_bytes());
    }

    #[test]
    fn encodes_bool() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "ok",
            DataType::Boolean,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(BooleanArray::from(vec![true, false]))],
        )
        .unwrap();
        let rows = encode_batches(&schema, &[batch]);
        // length 1, byte 't' / 'f'.
        assert_eq!(rows[0].data[4], b't');
        assert_eq!(rows[1].data[4], b'f');
    }

    #[test]
    fn maps_timestamp_oids() {
        // Tz-bearing timestamp → TIMESTAMPTZ regardless of granularity.
        assert_eq!(
            arrow_to_pg_type(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into()),
            )),
            Type::TIMESTAMPTZ,
        );
        assert_eq!(
            arrow_to_pg_type(&DataType::Timestamp(
                TimeUnit::Millisecond,
                Some("UTC".into()),
            )),
            Type::TIMESTAMPTZ,
        );
        assert_eq!(Type::TIMESTAMPTZ.oid(), 1184);
        // Tz-less timestamp → TIMESTAMP.
        assert_eq!(
            arrow_to_pg_type(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            Type::TIMESTAMP,
        );
        assert_eq!(Type::TIMESTAMP.oid(), 1114);
    }

    #[test]
    fn row_description_advertises_timestamp_oids() {
        let schema = ArrowSchema::new(vec![
            Field::new(
                "tz",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "naive",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]);
        let rd = row_description(&schema);
        assert_eq!(rd.fields.len(), 2);
        assert_eq!(rd.fields[0].type_id, 1184);
        assert_eq!(rd.fields[0].type_size, 8);
        assert_eq!(rd.fields[1].type_id, 1114);
        assert_eq!(rd.fields[1].type_size, 8);
    }

    #[test]
    fn renders_timestamp_text() {
        use arrow_array::TimestampMicrosecondArray;
        // 2024-03-14 12:34:56 UTC = 1710419696 unix-seconds.
        let unix_micros: i64 = 1_710_419_696_000_000;
        let arr = TimestampMicrosecondArray::from(vec![unix_micros]).with_timezone("UTC");
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        let rows = encode_batches(&schema, &[batch]);
        let len = i32::from_be_bytes(rows[0].data[0..4].try_into().unwrap()) as usize;
        let body = std::str::from_utf8(&rows[0].data[4..4 + len]).unwrap();
        // Existing render path emits RFC3339; just confirm the date/time
        // components round-trip — the UTC tz is what matters for clients.
        assert!(body.starts_with("2024-03-14"), "body = {body:?}");
        assert!(body.contains("12:34:56"), "body = {body:?}");
    }

    #[test]
    fn renders_timestamp_binary() {
        use arrow_array::TimestampMicrosecondArray;
        let unix_micros: i64 = 1_710_419_696_000_000;
        let arr = TimestampMicrosecondArray::from(vec![unix_micros]).with_timezone("UTC");
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        let rows = encode_batches_with_formats(&schema, &[batch], &[1]).unwrap();
        let len = i32::from_be_bytes(rows[0].data[0..4].try_into().unwrap());
        assert_eq!(len, 8);
        let got = i64::from_be_bytes(rows[0].data[4..12].try_into().unwrap());
        assert_eq!(got, unix_micros - PG_EPOCH_MICROS_FROM_UNIX);
    }

    #[test]
    fn renders_interval_binary() {
        use arrow_array::IntervalMonthDayNanoArray;
        // months=1, days=2, nanos=3_000_000 (= 3000 micros).
        let v = IntervalMonthDayNanoType::make_value(1, 2, 3_000_000);
        let arr = IntervalMonthDayNanoArray::from(vec![v]);
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "i",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        let rows = encode_batches_with_formats(&schema, &[batch], &[1]).unwrap();
        let len = i32::from_be_bytes(rows[0].data[0..4].try_into().unwrap());
        assert_eq!(len, 16);
        let micros = i64::from_be_bytes(rows[0].data[4..12].try_into().unwrap());
        let days = i32::from_be_bytes(rows[0].data[12..16].try_into().unwrap());
        let months = i32::from_be_bytes(rows[0].data[16..20].try_into().unwrap());
        assert_eq!(micros, 3_000);
        assert_eq!(days, 2);
        assert_eq!(months, 1);
    }

    #[test]
    fn renders_interval_binary_negative() {
        // age() can produce negative intervals; verify signed propagation
        // through `to_parts` and into the wire bytes (i64 micros, i32 days,
        // i32 months are all signed).
        use arrow_array::IntervalMonthDayNanoArray;
        let v = IntervalMonthDayNanoType::make_value(-1, -2, -3_000_000);
        let arr = IntervalMonthDayNanoArray::from(vec![v]);
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "i",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        let rows = encode_batches_with_formats(&schema, &[batch], &[1]).unwrap();
        let len = i32::from_be_bytes(rows[0].data[0..4].try_into().unwrap());
        assert_eq!(len, 16);
        let micros = i64::from_be_bytes(rows[0].data[4..12].try_into().unwrap());
        let days = i32::from_be_bytes(rows[0].data[12..16].try_into().unwrap());
        let months = i32::from_be_bytes(rows[0].data[16..20].try_into().unwrap());
        assert_eq!(micros, -3_000);
        assert_eq!(days, -2);
        assert_eq!(months, -1);
    }

    #[test]
    fn renders_interval_binary_round_trip_through_row_description() {
        // End-to-end check: a single Interval(MonthDayNano) column should
        // surface in row_description with type_size=16 (matching the binary
        // body length) and the binary body should match the (micros, days,
        // months) tuple emitted by the explicit binary arm.
        use arrow_array::IntervalMonthDayNanoArray;
        let v = IntervalMonthDayNanoType::make_value(7, 13, 1_000_000_000);
        let arr = IntervalMonthDayNanoArray::from(vec![v]);
        let arrow_schema = ArrowSchema::new(vec![Field::new(
            "i",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        )]);
        let rd = row_description(&arrow_schema);
        assert_eq!(rd.fields.len(), 1);
        assert_eq!(rd.fields[0].type_id, 1186);
        assert_eq!(rd.fields[0].type_size, 16);

        let schema = Arc::new(arrow_schema);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        let rows = encode_batches_with_formats(&schema, &[batch], &[1]).unwrap();
        let len = i32::from_be_bytes(rows[0].data[0..4].try_into().unwrap());
        // type_size from row_description (16) must equal the binary body
        // length the encoder emits.
        assert_eq!(len as i16, rd.fields[0].type_size);
        let micros = i64::from_be_bytes(rows[0].data[4..12].try_into().unwrap());
        let days = i32::from_be_bytes(rows[0].data[12..16].try_into().unwrap());
        let months = i32::from_be_bytes(rows[0].data[16..20].try_into().unwrap());
        // 1_000_000_000 nanos = 1_000_000 micros = 1 second.
        assert_eq!(micros, 1_000_000);
        assert_eq!(days, 13);
        assert_eq!(months, 7);
    }

    #[test]
    fn row_description_field_count() {
        let schema = ArrowSchema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let rd = row_description(&schema);
        assert_eq!(rd.fields.len(), 2);
        assert_eq!(rd.fields[0].name, "a");
        assert_eq!(rd.fields[0].type_id, Type::INT8.oid());
        assert_eq!(rd.fields[1].name, "b");
        assert_eq!(rd.fields[1].type_id, Type::TEXT.oid());
    }

    // -----------------------------------------------------------------------
    // Int8 / UInt8 / UInt16 → INT2 OID promotion tests
    // Fixes the SELECT * failure on BIGSERIAL PK + TEXT + TEXT UNIQUE +
    // TIMESTAMPTZ DEFAULT now() tables.
    // -----------------------------------------------------------------------

    /// Arrow `Int8`, `UInt8`, and `UInt16` have no direct Postgres equivalent.
    /// The smallest PG integer type is SMALLINT (OID 21, 2 bytes). Verify
    /// these three types all map to INT2 in `arrow_to_pg_type`.
    #[test]
    fn maps_int8_uint8_uint16_to_int2() {
        assert_eq!(arrow_to_pg_type(&DataType::Int8), Type::INT2);
        assert_eq!(arrow_to_pg_type(&DataType::UInt8), Type::INT2);
        assert_eq!(arrow_to_pg_type(&DataType::UInt16), Type::INT2);
        assert_eq!(Type::INT2.oid(), 21);
        // Sanity: Int16 still maps to INT2 as before.
        assert_eq!(arrow_to_pg_type(&DataType::Int16), Type::INT2);
    }

    /// `row_description` must advertise OID 21 (INT2) for Int8/UInt8/UInt16
    /// columns, not OID 25 (TEXT). type_size must be 2 (INT2 typlen).
    #[test]
    fn row_description_int8_uint8_uint16_oids() {
        let schema = ArrowSchema::new(vec![
            Field::new("i8col", DataType::Int8, true),
            Field::new("u8col", DataType::UInt8, true),
            Field::new("u16col", DataType::UInt16, true),
        ]);
        let rd = row_description(&schema);
        assert_eq!(rd.fields.len(), 3);
        for fd in &rd.fields {
            assert_eq!(
                fd.type_id, 21,
                "expected INT2 OID 21 for {}, got {}",
                fd.name, fd.type_id
            );
            assert_eq!(
                fd.type_size, 2,
                "expected INT2 type_size 2 for {}, got {}",
                fd.name, fd.type_size
            );
        }
    }

    /// Binary encoding of Int8 and UInt8 columns: each value should be emitted
    /// as a 2-byte big-endian i16 (length prefix 2, then the 2-byte value),
    /// consistent with the INT2 type OID advertised in RowDescription.
    #[test]
    fn encodes_int8_uint8_binary_as_int2() {
        use arrow_array::{Int8Array, UInt8Array};
        // Int8: value 42
        let schema_i8 = Arc::new(ArrowSchema::new(vec![Field::new(
            "v",
            DataType::Int8,
            false,
        )]));
        let arr_i8 = Arc::new(Int8Array::from(vec![42i8]));
        let batch_i8 = RecordBatch::try_new(schema_i8.clone(), vec![arr_i8]).unwrap();
        let rows = encode_batches_with_formats(&schema_i8, &[batch_i8], &[1]).unwrap();
        let len_i8 = i32::from_be_bytes(rows[0].data[0..4].try_into().unwrap());
        assert_eq!(len_i8, 2, "Int8 binary body length must be 2 (INT2)");
        let val_i8 = i16::from_be_bytes(rows[0].data[4..6].try_into().unwrap());
        assert_eq!(val_i8, 42i16);

        // UInt8: value 200
        let schema_u8 = Arc::new(ArrowSchema::new(vec![Field::new(
            "v",
            DataType::UInt8,
            false,
        )]));
        let arr_u8 = Arc::new(UInt8Array::from(vec![200u8]));
        let batch_u8 = RecordBatch::try_new(schema_u8.clone(), vec![arr_u8]).unwrap();
        let rows = encode_batches_with_formats(&schema_u8, &[batch_u8], &[1]).unwrap();
        let len_u8 = i32::from_be_bytes(rows[0].data[0..4].try_into().unwrap());
        assert_eq!(len_u8, 2, "UInt8 binary body length must be 2 (INT2)");
        let val_u8 = i16::from_be_bytes(rows[0].data[4..6].try_into().unwrap());
        assert_eq!(val_u8, 200i16);
    }

    /// End-to-end row_description for the exact table shape that triggered the
    /// SELECT * failure:
    ///   id BIGSERIAL PK   → Arrow Int64  → OID 20 (INT8)
    ///   name TEXT         → Arrow Utf8   → OID 25 (TEXT)
    ///   email TEXT UNIQUE → Arrow Utf8   → OID 25 (TEXT)
    ///   created_at TIMESTAMPTZ DEFAULT now() → Timestamp(µs, Some("UTC")) → OID 1184
    #[test]
    fn row_description_bigserial_text_text_timestamptz_shape() {
        use arrow_schema::TimeUnit as TU;
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TU::Microsecond, Some("UTC".into())),
                false,
            ),
        ]);
        let rd = row_description(&schema);
        assert_eq!(rd.fields.len(), 4);

        // id: INT8 (OID 20, 8 bytes)
        assert_eq!(rd.fields[0].name, "id");
        assert_eq!(
            rd.fields[0].type_id,
            Type::INT8.oid(),
            "BIGSERIAL PK must be OID {} (INT8), got {}",
            Type::INT8.oid(),
            rd.fields[0].type_id
        );
        assert_eq!(rd.fields[0].type_size, 8);

        // name, email: TEXT (OID 25, varlena -1)
        assert_eq!(rd.fields[1].type_id, Type::TEXT.oid());
        assert_eq!(rd.fields[1].type_size, -1);
        assert_eq!(rd.fields[2].type_id, Type::TEXT.oid());

        // created_at: TIMESTAMPTZ (OID 1184, 8 bytes)
        assert_eq!(rd.fields[3].name, "created_at");
        assert_eq!(
            rd.fields[3].type_id,
            1184,
            "TIMESTAMPTZ must be OID 1184, got {}",
            rd.fields[3].type_id
        );
        assert_eq!(rd.fields[3].type_size, 8);
    }

    // -----------------------------------------------------------------------
    // NUMERIC binary wire format tests (#141)
    // -----------------------------------------------------------------------
    //
    // Helper: call encode_numeric_binary and return the full byte slice
    // (including the 4-byte length prefix).
    fn numeric_binary_bytes(raw: i128, scale: u16) -> Vec<u8> {
        let mut buf = BytesMut::new();
        encode_numeric_binary(raw, scale, &mut buf);
        buf.to_vec()
    }

    // Parse the PG numeric binary body (after the 4-byte length prefix) into
    // (ndigits, weight, sign, dscale, digits).
    fn parse_numeric_binary(bytes: &[u8]) -> (u16, i16, u16, u16, Vec<u16>) {
        let body_len = i32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
        assert!(body_len >= 8, "body_len too small: {body_len}");
        let ndigits = u16::from_be_bytes(bytes[4..6].try_into().unwrap());
        let weight = i16::from_be_bytes(bytes[6..8].try_into().unwrap());
        let sign = u16::from_be_bytes(bytes[8..10].try_into().unwrap());
        let dscale = u16::from_be_bytes(bytes[10..12].try_into().unwrap());
        let mut digits = Vec::with_capacity(ndigits as usize);
        for i in 0..ndigits as usize {
            let off = 12 + i * 2;
            digits.push(u16::from_be_bytes(bytes[off..off + 2].try_into().unwrap()));
        }
        assert_eq!(body_len, 8 + (ndigits as usize) * 2);
        (ndigits, weight, sign, dscale, digits)
    }

    // Reconstruct the decimal value from PG numeric binary fields for
    // round-trip checking.
    fn pg_numeric_to_f64(
        ndigits: u16,
        weight: i16,
        sign: u16,
        _dscale: u16,
        digits: &[u16],
    ) -> f64 {
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

    // --- Test 1: zero ---
    // SELECT 0::numeric → ndigits=0, weight=0, sign=0x0000, dscale=0
    // Wire bytes (body 8): 00 00 00 00 00 00 00 00
    #[test]
    fn numeric_binary_zero() {
        let bytes = numeric_binary_bytes(0i128, 0);
        let (ndigits, weight, sign, dscale, digits) = parse_numeric_binary(&bytes);
        assert_eq!(ndigits, 0);
        assert_eq!(weight, 0);
        assert_eq!(sign, 0x0000);
        assert_eq!(dscale, 0);
        assert!(digits.is_empty());
        // Exact wire bytes: [0,0,0,8, 0,0, 0,0, 0,0, 0,0]
        assert_eq!(&bytes[4..12], &[0u8, 0, 0, 0, 0, 0, 0, 0]);
    }

    // --- Test 2: SELECT 1::numeric (integer, no fraction) ---
    // raw=1, scale=0 → integer_part=1, frac_part=0
    // int_digits=[1], weight=0, sign=0x0000, dscale=0, ndigits=1
    // Digit value = 1, body = 00 01  00 00  00 00  00 00  00 01
    #[test]
    fn numeric_binary_one() {
        let bytes = numeric_binary_bytes(1i128, 0);
        let (ndigits, weight, sign, dscale, digits) = parse_numeric_binary(&bytes);
        assert_eq!(ndigits, 1);
        assert_eq!(weight, 0);
        assert_eq!(sign, 0x0000);
        assert_eq!(dscale, 0);
        assert_eq!(digits, vec![1]);
    }

    // --- Test 3: SELECT 1.5::numeric(38,1) ---
    // raw=15, scale=1 → int_part=1, frac_part=5
    // int_digits=[1], frac_groups=1 (scale=1 → pad to 4 → frac=5000)
    // digits=[1, 5000], weight=0, dscale=1
    #[test]
    fn numeric_binary_one_point_five() {
        let bytes = numeric_binary_bytes(15i128, 1);
        let (ndigits, weight, sign, dscale, digits) = parse_numeric_binary(&bytes);
        assert_eq!(sign, 0x0000);
        assert_eq!(dscale, 1);
        assert_eq!(weight, 0);
        // Digit group 0 (weight=0) = 1, digit group 1 (weight=-1) = 5000
        assert_eq!(ndigits, 2);
        assert_eq!(digits[0], 1);
        assert_eq!(digits[1], 5000);
        // Round-trip check
        let v = pg_numeric_to_f64(ndigits, weight, sign, dscale, &digits);
        assert!((v - 1.5).abs() < 1e-10, "expected 1.5, got {v}");
    }

    // --- Test 4: SELECT -1::numeric ---
    // raw=-1, scale=0 → sign=0x4000, int_part=1
    #[test]
    fn numeric_binary_negative_one() {
        let bytes = numeric_binary_bytes(-1i128, 0);
        let (ndigits, weight, sign, dscale, digits) = parse_numeric_binary(&bytes);
        assert_eq!(sign, 0x4000); // NUMERIC_NEG
        assert_eq!(weight, 0);
        assert_eq!(dscale, 0);
        assert_eq!(ndigits, 1);
        assert_eq!(digits, vec![1]);
    }

    // --- Test 5: SELECT -1234.56789::numeric(38,5) ---
    // raw = -123456789, scale=5
    // int_part=1234, frac_part=56789
    // int_digits=[1234], weight=0
    // frac scale=5 → frac_groups=2 (covers 8 decimal places)
    // padded frac = 56789 * 10^3 = 56789000 → group0=5678, group1=9000
    // strip trailing zeros: group1=9000 (nonzero, keep)
    // digits=[1234, 5678, 9000], weight=0, dscale=5, sign=0x4000
    #[test]
    fn numeric_binary_neg_1234_56789() {
        let raw: i128 = -123456789i128;
        let bytes = numeric_binary_bytes(raw, 5);
        let (ndigits, weight, sign, dscale, digits) = parse_numeric_binary(&bytes);
        assert_eq!(sign, 0x4000);
        assert_eq!(dscale, 5);
        assert_eq!(weight, 0);
        assert_eq!(ndigits, 3);
        assert_eq!(digits[0], 1234);
        assert_eq!(digits[1], 5678);
        assert_eq!(digits[2], 9000);
        let v = pg_numeric_to_f64(ndigits, weight, sign, dscale, &digits);
        assert!(
            (v - (-1234.56789)).abs() < 1e-7,
            "expected -1234.56789, got {v}"
        );
    }

    // --- Test 6: SELECT 0.000001::numeric(38,10) ---
    // raw=10000, scale=10 → int_part=0, frac_part=10000
    // frac_groups=3 (covers 12 decimal places)
    // padded: 10000 * 10^2 = 1000000 → group0=0, group1=0, group2=100 (wait: let's recompute)
    // frac_groups = ceil(10/4) = 3, full_scale=12, padding=2
    // padded = 10000 * 100 = 1000000
    // divisor_group for group0 = 10^8 = 100000000 → 1000000 / 100000000 = 0
    // divisor_group for group1 = 10^4 = 10000 → 1000000 / 10000 = 100
    // remainder 0
    // So digits: group0=0, group1=100, group2=0 → strip trailing 0 → [0, 100]
    // weight = -1 (int_part=0, first nonzero is at index 1 → weight = -1 - 1 = -2)
    // After stripping leading zeros: skip one 0 → digits=[100], weight=-2
    #[test]
    fn numeric_binary_small_fraction() {
        // 0.000001 = 1e-6, stored as Decimal128(38,10): raw = 1 * 10^4 = 10000
        let raw: i128 = 10000i128; // 0.000001 with scale=10 → actual value = 10000 / 10^10 = 0.000001
        let bytes = numeric_binary_bytes(raw, 10);
        let (ndigits, weight, sign, dscale, digits) = parse_numeric_binary(&bytes);
        assert_eq!(sign, 0x0000);
        assert_eq!(dscale, 10);
        // Reconstruct as float and verify
        let v = pg_numeric_to_f64(ndigits, weight, sign, dscale, &digits);
        assert!((v - 0.000001).abs() < 1e-14, "expected 0.000001, got {v}");
    }

    // --- Test 7: SELECT 99999999999999999999.99999999::numeric(38,8) ---
    // Large value: raw = 9999999999999999999999999999 (28 digits), scale=8
    // int_part = 99999999999999999999 (20 digits → 5 base-10000 groups)
    // frac_part = 99999999 (8 decimal digits → 2 base-10000 groups of 9999,9900)
    #[test]
    fn numeric_binary_large_value() {
        // raw = 99999999999999999999_99999999 as i128
        // = 99999999999999999999 * 10^8 + 99999999
        let int: i128 = 99999999999999999999i128;
        let frac: i128 = 99999999i128;
        let raw: i128 = int * 100_000_000 + frac;
        let bytes = numeric_binary_bytes(raw, 8);
        let (ndigits, weight, sign, dscale, digits) = parse_numeric_binary(&bytes);
        assert_eq!(sign, 0x0000);
        assert_eq!(dscale, 8);
        // int_part has 20 digits → 5 groups of 4 each → weight=4
        assert_eq!(weight, 4);
        assert!(ndigits >= 5, "expected at least 5 digits, got {ndigits}");
        // All int groups should be 9999
        for i in 0..5usize {
            assert_eq!(digits[i], 9999, "int digit group {i} should be 9999");
        }
        // frac: 99999999 → padded to 8 decimal places → groups: 9999, 9900 (after strip: no trailing zero)
        // Actually 99999999 → group0=9999, group1=9900... wait:
        // scale=8, frac_groups=2, full_scale=8, padding=0
        // divisor_group for group0 = 10^4 = 10000 → 99999999 / 10000 = 9999, rem=9999
        // divisor_group for group1 = 1 → 9999 / 1 = 9999
        // So frac digits = [9999, 9999], no trailing zeros
        assert_eq!(digits[5], 9999);
        assert_eq!(digits[6], 9999);
    }

    // --- Test 8: NULL numeric binary ---
    #[test]
    fn numeric_binary_null() {
        use arrow_array::Decimal128Array;
        let arr = Decimal128Array::from(vec![None::<i128>])
            .with_precision_and_scale(38, 4)
            .unwrap();
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "n",
            DataType::Decimal128(38, 4),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        let rows = encode_batches_with_formats(&schema, &[batch], &[1]).unwrap();
        // NULL → 4-byte -1 prefix, no body
        assert_eq!(rows[0].data.len(), 4);
        assert_eq!(
            i32::from_be_bytes(rows[0].data[0..4].try_into().unwrap()),
            -1
        );
    }

    // --- Test 9: round-trip via encode_batches_with_formats ---
    // Verify the binary path is actually invoked for Decimal128 columns when
    // format_codes=[1] (all-binary).
    #[test]
    fn numeric_binary_round_trip_via_encode_batches() {
        use arrow_array::Decimal128Array;
        // 1.5 stored as Decimal128(38,1): raw=15
        let arr = Decimal128Array::from(vec![Some(15i128)])
            .with_precision_and_scale(38, 1)
            .unwrap();
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "v",
            DataType::Decimal128(38, 1),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        let rows = encode_batches_with_formats(&schema, &[batch], &[1]).unwrap();
        let (ndigits, weight, sign, dscale, digits) = parse_numeric_binary(&rows[0].data);
        assert_eq!(sign, 0x0000);
        assert_eq!(dscale, 1);
        assert_eq!(weight, 0);
        assert_eq!(ndigits, 2);
        assert_eq!(digits[0], 1);
        assert_eq!(digits[1], 5000);
    }

    // -----------------------------------------------------------------------
    // ARRAY binary wire format tests (#144)
    // -----------------------------------------------------------------------
    //
    // Helper: parse the PG array binary body from the raw wire bytes.
    // Returns (ndim, has_nulls, elem_oid, dim_len, lower_bound, element_bytes)
    // where element_bytes is the raw bytes after the per-dimension headers.
    fn parse_array_header(data: &[u8]) -> (i32, i32, u32, i32, i32, &[u8]) {
        // First 4 bytes = outer length prefix (body size).
        let body_len = i32::from_be_bytes(data[0..4].try_into().unwrap());
        assert!(
            body_len >= 0,
            "NULL array not expected in parse_array_header"
        );
        let ndim = i32::from_be_bytes(data[4..8].try_into().unwrap());
        let has_nulls = i32::from_be_bytes(data[8..12].try_into().unwrap());
        let elem_oid = u32::from_be_bytes(data[12..16].try_into().unwrap());
        let dim_len = i32::from_be_bytes(data[16..20].try_into().unwrap());
        let lower_bound = i32::from_be_bytes(data[20..24].try_into().unwrap());
        let elements = &data[24..4 + body_len as usize];
        (ndim, has_nulls, elem_oid, dim_len, lower_bound, elements)
    }

    // Parse element stream: returns (len, body_slice_or_empty_for_null) pairs.
    fn parse_elements(mut data: &[u8]) -> Vec<Option<Vec<u8>>> {
        let mut out = Vec::new();
        while !data.is_empty() {
            let elem_len = i32::from_be_bytes(data[0..4].try_into().unwrap());
            data = &data[4..];
            if elem_len == -1 {
                out.push(None);
            } else {
                let n = elem_len as usize;
                out.push(Some(data[..n].to_vec()));
                data = &data[n..];
            }
        }
        out
    }

    // --- Test A: [1, 2, 3] as Int32 LIST → ndim=1, has_nulls=0, elem_oid=23 ---
    // Each element is 4 bytes big-endian i32.
    // Wire: len=32+body, ndim=1, has_nulls=0, oid=23, dim=3, lb=1,
    //       (4,\x00\x00\x00\x01), (4,\x00\x00\x00\x02), (4,\x00\x00\x00\x03)
    #[test]
    fn array_binary_int4_simple() {
        use arrow::buffer::{OffsetBuffer, ScalarBuffer};
        use arrow_array::{Int32Array, ListArray as ArrowListArray};
        use arrow_schema::Field as ArrowField;
        let elem_field = Arc::new(ArrowField::new("item", DataType::Int32, false));
        let values = Arc::new(Int32Array::from(vec![1i32, 2, 3]));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 3]));
        let list = ArrowListArray::new(elem_field.clone(), offsets, values, None);
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "arr",
            DataType::List(elem_field),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list)]).unwrap();
        let rows = encode_batches_with_formats(&schema, &[batch], &[1]).unwrap();
        let data = &rows[0].data;
        let (ndim, has_nulls, elem_oid, dim_len, lower_bound, elems_bytes) =
            parse_array_header(data);
        assert_eq!(ndim, 1);
        assert_eq!(has_nulls, 0);
        assert_eq!(elem_oid, Type::INT4.oid());
        assert_eq!(dim_len, 3);
        assert_eq!(lower_bound, 1);
        let elems = parse_elements(elems_bytes);
        assert_eq!(elems.len(), 3);
        // Element bodies are big-endian i32.
        assert_eq!(elems[0].as_deref(), Some(&1i32.to_be_bytes()[..]));
        assert_eq!(elems[1].as_deref(), Some(&2i32.to_be_bytes()[..]));
        assert_eq!(elems[2].as_deref(), Some(&3i32.to_be_bytes()[..]));
    }

    // --- Test B: [1, NULL, 3] as Int64 LIST → has_nulls=1, elem_oid=20 ---
    #[test]
    fn array_binary_int8_with_null() {
        use arrow::buffer::{OffsetBuffer, ScalarBuffer};
        use arrow_array::{Int64Array, ListArray as ArrowListArray};
        use arrow_schema::Field as ArrowField;
        let elem_field = Arc::new(ArrowField::new("item", DataType::Int64, true));
        let values = Arc::new(Int64Array::from(vec![Some(1i64), None, Some(3)]));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 3]));
        let list = ArrowListArray::new(elem_field.clone(), offsets, values, None);
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "arr",
            DataType::List(elem_field),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list)]).unwrap();
        let rows = encode_batches_with_formats(&schema, &[batch], &[1]).unwrap();
        let data = &rows[0].data;
        let (ndim, has_nulls, elem_oid, dim_len, _lower, elems_bytes) = parse_array_header(data);
        assert_eq!(ndim, 1);
        assert_eq!(has_nulls, 1);
        assert_eq!(elem_oid, Type::INT8.oid());
        assert_eq!(dim_len, 3);
        let elems = parse_elements(elems_bytes);
        assert_eq!(elems[0].as_deref(), Some(&1i64.to_be_bytes()[..]));
        assert_eq!(elems[1], None); // NULL element
        assert_eq!(elems[2].as_deref(), Some(&3i64.to_be_bytes()[..]));
    }

    // --- Test C: [] as Int32 LIST → dim=0, no elements ---
    #[test]
    fn array_binary_empty() {
        use arrow::buffer::{OffsetBuffer, ScalarBuffer};
        use arrow_array::{Int32Array, ListArray as ArrowListArray};
        use arrow_schema::Field as ArrowField;
        let elem_field = Arc::new(ArrowField::new("item", DataType::Int32, false));
        let values = Arc::new(Int32Array::from(vec![] as Vec<i32>));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 0]));
        let list = ArrowListArray::new(elem_field.clone(), offsets, values, None);
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "arr",
            DataType::List(elem_field),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list)]).unwrap();
        let rows = encode_batches_with_formats(&schema, &[batch], &[1]).unwrap();
        let data = &rows[0].data;
        let (_ndim, has_nulls, _elem_oid, dim_len, lower_bound, elems_bytes) =
            parse_array_header(data);
        assert_eq!(has_nulls, 0);
        assert_eq!(dim_len, 0);
        assert_eq!(lower_bound, 1);
        assert_eq!(elems_bytes.len(), 0);
    }

    // --- Test D: ['hello', 'world'] as TEXT LIST → elem_oid=25 ---
    // Element bodies are UTF-8 bytes (no extra length prefix within the body).
    #[test]
    fn array_binary_text() {
        use arrow::buffer::{OffsetBuffer, ScalarBuffer};
        use arrow_array::{ListArray as ArrowListArray, StringArray};
        use arrow_schema::Field as ArrowField;
        let elem_field = Arc::new(ArrowField::new("item", DataType::Utf8, false));
        let values = Arc::new(StringArray::from(vec!["hello", "world"]));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2]));
        let list = ArrowListArray::new(elem_field.clone(), offsets, values, None);
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "arr",
            DataType::List(elem_field),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list)]).unwrap();
        let rows = encode_batches_with_formats(&schema, &[batch], &[1]).unwrap();
        let data = &rows[0].data;
        let (_ndim, has_nulls, elem_oid, dim_len, _lower, elems_bytes) = parse_array_header(data);
        assert_eq!(has_nulls, 0);
        assert_eq!(elem_oid, Type::TEXT.oid()); // 25
        assert_eq!(dim_len, 2);
        let elems = parse_elements(elems_bytes);
        assert_eq!(elems[0].as_deref(), Some(b"hello" as &[u8]));
        assert_eq!(elems[1].as_deref(), Some(b"world" as &[u8]));
    }

    // --- Test E: [NULL, NULL] as Int32 LIST → has_nulls=1, both elems NULL ---
    #[test]
    fn array_binary_all_null() {
        use arrow::buffer::{OffsetBuffer, ScalarBuffer};
        use arrow_array::{Int32Array, ListArray as ArrowListArray};
        use arrow_schema::Field as ArrowField;
        let elem_field = Arc::new(ArrowField::new("item", DataType::Int32, true));
        let values = Arc::new(Int32Array::from(vec![None::<i32>, None::<i32>]));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2]));
        let list = ArrowListArray::new(elem_field.clone(), offsets, values, None);
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "arr",
            DataType::List(elem_field),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list)]).unwrap();
        let rows = encode_batches_with_formats(&schema, &[batch], &[1]).unwrap();
        let data = &rows[0].data;
        let (_ndim, has_nulls, _elem_oid, dim_len, _lower, elems_bytes) = parse_array_header(data);
        assert_eq!(has_nulls, 1);
        assert_eq!(dim_len, 2);
        let elems = parse_elements(elems_bytes);
        assert_eq!(elems[0], None);
        assert_eq!(elems[1], None);
    }

    // --- Test F: NULL LIST row → 4-byte -1 prefix, no body ---
    // The entire list cell is NULL (not elements within it).
    #[test]
    fn array_binary_null_row() {
        use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use arrow_array::{Int32Array, ListArray as ArrowListArray};
        use arrow_schema::Field as ArrowField;
        let elem_field = Arc::new(ArrowField::new("item", DataType::Int32, true));
        let values = Arc::new(Int32Array::from(vec![] as Vec<i32>));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 0]));
        // Mark the single row NULL.
        let nulls = NullBuffer::from(vec![false]);
        let list = ArrowListArray::new(elem_field.clone(), offsets, values, Some(nulls));
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "arr",
            DataType::List(elem_field),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list)]).unwrap();
        let rows = encode_batches_with_formats(&schema, &[batch], &[1]).unwrap();
        let data = &rows[0].data;
        // NULL row: exactly 4 bytes, value -1.
        assert_eq!(data.len(), 4);
        assert_eq!(i32::from_be_bytes(data[0..4].try_into().unwrap()), -1);
    }

    // --- Test G: round-trip via encode_batches_with_formats, LargeList ---
    // Verify the LargeList arm is also exercised end-to-end.
    #[test]
    fn array_binary_round_trip_via_encode_batches() {
        use arrow::buffer::{OffsetBuffer, ScalarBuffer};
        use arrow_array::{Int32Array, LargeListArray as ArrowLargeListArray};
        use arrow_schema::Field as ArrowField;
        let elem_field = Arc::new(ArrowField::new("item", DataType::Int32, false));
        let values = Arc::new(Int32Array::from(vec![10i32, 20]));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i64, 2]));
        let list = ArrowLargeListArray::new(elem_field.clone(), offsets, values, None);
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "arr",
            DataType::LargeList(elem_field),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list)]).unwrap();
        let rows = encode_batches_with_formats(&schema, &[batch], &[1]).unwrap();
        let data = &rows[0].data;
        let (ndim, has_nulls, elem_oid, dim_len, lower_bound, elems_bytes) =
            parse_array_header(data);
        assert_eq!(ndim, 1);
        assert_eq!(has_nulls, 0);
        assert_eq!(elem_oid, Type::INT4.oid());
        assert_eq!(dim_len, 2);
        assert_eq!(lower_bound, 1);
        let elems = parse_elements(elems_bytes);
        assert_eq!(elems[0].as_deref(), Some(&10i32.to_be_bytes()[..]));
        assert_eq!(elems[1].as_deref(), Some(&20i32.to_be_bytes()[..]));
    }
}
