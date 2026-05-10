//! Arrow -> Postgres type and value conversion for the simple-query path.
//!
//! The PoC encodes everything in Postgres text format (format code 0). All
//! values are UTF-8 strings; SQL NULL is signalled by a -1 length prefix.
//! `chrono` is used for the timestamp string formatting; we render every
//! timestamp variant as RFC3339 for PoC simplicity (production will need
//! distinct mappings for `timestamp` vs `timestamptz`).

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    IntervalMonthDayNanoType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{Array, RecordBatch};
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
/// encoding.
const PG_EPOCH_MICROS_FROM_UNIX: i64 = (PG_EPOCH_DAYS_FROM_UNIX as i64) * 86_400_000_000;

/// Field-metadata key that basin-engine uses to mark logical types not
/// directly representable in Arrow (today: `JSONB`, `UUID`). Kept in sync
/// with `basin_engine::types::BASIN_TYPE_KEY` — duplicated here as a `&str`
/// constant so this crate stays free of an engine dependency cycle.
const BASIN_TYPE_KEY: &str = "BASIN_TYPE";
const BASIN_TYPE_JSONB: &str = "JSONB";
const BASIN_TYPE_UUID: &str = "UUID";

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
fn arrow_to_pg_type_field(f: &Field) -> Type {
    if field_is_jsonb(f) {
        return Type::JSONB;
    }
    if field_is_uuid(f) {
        return Type::UUID;
    }
    arrow_to_pg_type(f.data_type())
}

/// Map an Arrow type to the closest Postgres type for the PoC. Anything we
/// don't have a mapping for becomes TEXT — the value renderer will format it
/// via `Debug`.
pub(crate) fn arrow_to_pg_type(dt: &DataType) -> Type {
    match dt {
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => Type::INT8,
        DataType::Int32 => Type::INT4,
        DataType::Int16 => Type::INT2,
        DataType::Boolean => Type::BOOL,
        DataType::Float64 => Type::FLOAT8,
        DataType::Float32 => Type::FLOAT4,
        DataType::Utf8 | DataType::LargeUtf8 => Type::TEXT,
        DataType::Binary | DataType::LargeBinary => Type::BYTEA,
        DataType::Date32 => Type::DATE,
        DataType::Interval(IntervalUnit::MonthDayNano) => Type::INTERVAL,
        DataType::Timestamp(_, Some(_)) => Type::TIMESTAMPTZ,
        DataType::Timestamp(_, None) => Type::TIMESTAMP,
        // PG `numeric` (OID 1700) → Arrow `Decimal128(p, s)`. Wire format
        // is text-only for v0.1 (binary numeric encoding is varlena-shaped:
        // sign + weight + dscale + ndigits + base-10000 digit array; lots
        // of edge cases. Lenient drivers tolerate text in a binary slot.)
        DataType::Decimal128(_, _) => Type::NUMERIC,
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
pub(crate) fn encode_batches(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
) -> Vec<DataRow> {
    let mut rows = Vec::new();
    let n_cols = schema.fields().len();
    // Pre-compute the `is_jsonb` / `is_uuid` bitmaps so the per-cell hot
    // loop doesn't redo a metadata lookup per row.
    let jsonb_cols: Vec<bool> =
        schema.fields().iter().map(|f| field_is_jsonb(f.as_ref())).collect();
    let uuid_cols: Vec<bool> =
        schema.fields().iter().map(|f| field_is_uuid(f.as_ref())).collect();
    for batch in batches {
        let n_rows = batch.num_rows();
        for r in 0..n_rows {
            let mut buf = BytesMut::with_capacity(64);
            for c in 0..n_cols {
                let col = batch.column(c);
                encode_value(col.as_ref(), r, &mut buf, jsonb_cols[c], uuid_cols[c]);
            }
            rows.push(DataRow::new(buf, n_cols as i16));
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
pub(crate) fn encode_batches_with_formats(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
    format_codes: &[i16],
) -> Result<Vec<DataRow>> {
    let n_cols = schema.fields().len();
    let jsonb_cols: Vec<bool> =
        schema.fields().iter().map(|f| field_is_jsonb(f.as_ref())).collect();
    let uuid_cols: Vec<bool> =
        schema.fields().iter().map(|f| field_is_uuid(f.as_ref())).collect();
    let mut rows = Vec::new();
    for batch in batches {
        let n_rows = batch.num_rows();
        for r in 0..n_rows {
            let mut buf = BytesMut::with_capacity(64);
            for c in 0..n_cols {
                let col = batch.column(c);
                let is_binary = match format_codes.len() {
                    0 => false,
                    1 => format_codes[0] == 1,
                    _ => format_codes.get(c).copied().unwrap_or(0) == 1,
                };
                if is_binary {
                    encode_value_binary(
                        col.as_ref(),
                        r,
                        &mut buf,
                        jsonb_cols[c],
                        uuid_cols[c],
                    )?;
                } else {
                    encode_value(col.as_ref(), r, &mut buf, jsonb_cols[c], uuid_cols[c]);
                }
            }
            rows.push(DataRow::new(buf, n_cols as i16));
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
    if is_jsonb && matches!(col.data_type(), DataType::LargeBinary | DataType::Binary) {
        let bytes: &[u8] = match col.data_type() {
            DataType::LargeBinary => col.as_binary::<i64>().value(idx),
            DataType::Binary => col.as_binary::<i32>().value(idx),
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
                TimeUnit::Millisecond => {
                    col.as_primitive::<TimestampMillisecondType>().value(idx)
                }
                TimeUnit::Microsecond => {
                    col.as_primitive::<TimestampMicrosecondType>().value(idx)
                }
                TimeUnit::Nanosecond => {
                    col.as_primitive::<TimestampNanosecondType>().value(idx)
                }
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
            encode_value(col, idx, buf, is_jsonb, is_uuid);
        }
    }
    Ok(())
}

/// Encode the value at row `idx` of `col` into `buf` as a length-prefixed
/// text-format Postgres field. NULLs get a -1 length and no body.
fn encode_value(col: &dyn Array, idx: usize, buf: &mut BytesMut, is_jsonb: bool, is_uuid: bool) {
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
    // Render the cell as a UTF-8 string in `s`, then prefix with length.
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
    if field_is_jsonb(field)
        && matches!(col.data_type(), DataType::LargeBinary | DataType::Binary)
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
            Date32Type::to_naive_date(days).format("%Y-%m-%d").to_string()
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
        s.push_str(&format!("{b:02x}"));
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
        parts.push(format!("{years} year{}", if years.abs() == 1 { "" } else { "s" }));
    }
    if mons != 0 {
        parts.push(format!("{mons} mon{}", if mons.abs() == 1 { "" } else { "s" }));
    }
    if days != 0 {
        parts.push(format!("{days} day{}", if days.abs() == 1 { "" } else { "s" }));
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
        let days = Date32Type::from_naive_date(
            chrono::NaiveDate::from_ymd_opt(2024, 3, 14).unwrap(),
        );
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
        let arr = TimestampMicrosecondArray::from(vec![unix_micros])
            .with_timezone("UTC");
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
        let arr = TimestampMicrosecondArray::from(vec![unix_micros])
            .with_timezone("UTC");
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
}
