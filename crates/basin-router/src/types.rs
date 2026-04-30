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
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use basin_common::Result;
use bytes::{BufMut, BytesMut};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use pgwire::api::Type;
use pgwire::messages::data::{DataRow, FieldDescription, RowDescription};

const FORMAT_CODE_TEXT: i16 = 0;

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
    let ty = arrow_to_pg_type(f.data_type());
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
        // Everything else gets formatted as text.
        _ => Type::TEXT,
    }
}

fn type_size(ty: &Type) -> i16 {
    match *ty {
        Type::INT8 | Type::FLOAT8 => 8,
        Type::INT4 | Type::FLOAT4 => 4,
        Type::INT2 => 2,
        Type::BOOL => 1,
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
    for batch in batches {
        let n_rows = batch.num_rows();
        for r in 0..n_rows {
            let mut buf = BytesMut::with_capacity(64);
            for c in 0..n_cols {
                let col = batch.column(c);
                encode_value(col.as_ref(), r, &mut buf);
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
                    encode_value_binary(col.as_ref(), r, &mut buf)?;
                } else {
                    encode_value(col.as_ref(), r, &mut buf);
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
) -> Result<()> {
    if col.is_null(idx) {
        buf.put_i32(-1);
        return Ok(());
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
            encode_value(col, idx, buf);
        }
    }
    Ok(())
}

/// Encode the value at row `idx` of `col` into `buf` as a length-prefixed
/// text-format Postgres field. NULLs get a -1 length and no body.
fn encode_value(col: &dyn Array, idx: usize, buf: &mut BytesMut) {
    if col.is_null(idx) {
        buf.put_i32(-1);
        return;
    }

    // Render the cell as a UTF-8 string in `s`, then prefix with length.
    let s = render_cell(col, idx);
    buf.put_i32(s.len() as i32);
    buf.put_slice(s.as_bytes());
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
