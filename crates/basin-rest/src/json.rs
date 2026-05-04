//! Arrow `RecordBatch` → JSON encoding.
//!
//! Mirrors the type coverage of `basin_router::types`: int8/16/32/64,
//! float32/64, bool, utf8, large-utf8, binary (rendered hex), timestamp
//! (rendered RFC3339). Anything else falls back to a debug string so the
//! response is at least readable.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Schema, TimeUnit};
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Map, Value};

/// Encode a list of `RecordBatch`es as a top-level JSON array of objects.
/// Each batch row becomes one object keyed by the schema's field names.
pub(crate) fn batches_to_json(schema: &Arc<Schema>, batches: &[RecordBatch]) -> Value {
    let mut rows: Vec<Value> = Vec::new();
    let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    for batch in batches {
        for r in 0..batch.num_rows() {
            let mut obj = Map::with_capacity(names.len());
            for (i, name) in names.iter().enumerate() {
                obj.insert(name.clone(), encode_cell(batch.column(i).as_ref(), r));
            }
            rows.push(Value::Object(obj));
        }
    }
    Value::Array(rows)
}

fn encode_cell(col: &dyn Array, idx: usize) -> Value {
    if col.is_null(idx) {
        return Value::Null;
    }
    match col.data_type() {
        DataType::Boolean => Value::Bool(col.as_boolean().value(idx)),
        DataType::Int8 => Value::from(col.as_primitive::<Int8Type>().value(idx)),
        DataType::Int16 => Value::from(col.as_primitive::<Int16Type>().value(idx)),
        DataType::Int32 => Value::from(col.as_primitive::<Int32Type>().value(idx)),
        DataType::Int64 => Value::from(col.as_primitive::<Int64Type>().value(idx)),
        DataType::UInt8 => Value::from(col.as_primitive::<UInt8Type>().value(idx)),
        DataType::UInt16 => Value::from(col.as_primitive::<UInt16Type>().value(idx)),
        DataType::UInt32 => Value::from(col.as_primitive::<UInt32Type>().value(idx)),
        DataType::UInt64 => Value::from(col.as_primitive::<UInt64Type>().value(idx)),
        DataType::Float32 => {
            let v = col.as_primitive::<Float32Type>().value(idx);
            json_number_or_string(v as f64)
        }
        DataType::Float64 => {
            let v = col.as_primitive::<Float64Type>().value(idx);
            json_number_or_string(v)
        }
        DataType::Utf8 => Value::String(col.as_string::<i32>().value(idx).to_owned()),
        DataType::LargeUtf8 => Value::String(col.as_string::<i64>().value(idx).to_owned()),
        DataType::Binary => Value::String(hex(col.as_binary::<i32>().value(idx))),
        DataType::LargeBinary => Value::String(hex(col.as_binary::<i64>().value(idx))),
        DataType::Timestamp(unit, _tz) => Value::String(timestamp_rfc3339(col, idx, unit)),
        other => Value::String(format!("{other:?}@{idx}")),
    }
}

fn json_number_or_string(v: f64) -> Value {
    if v.is_finite() {
        // serde_json refuses non-finite floats; for finite ones from_f64
        // returns Some.
        match serde_json::Number::from_f64(v) {
            Some(n) => Value::Number(n),
            None => Value::String(v.to_string()),
        }
    } else {
        Value::String(v.to_string())
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("\\x");
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn timestamp_rfc3339(col: &dyn Array, idx: usize, unit: &TimeUnit) -> String {
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
    dt.map(|d: DateTime<Utc>| d.to_rfc3339())
        .unwrap_or_else(|| format!("{raw}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema as ArrowSchema};

    #[test]
    fn encodes_object_array() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .unwrap();
        let v = batches_to_json(&schema, &[batch]);
        assert_eq!(
            v,
            serde_json::json!([
                {"id": 1, "name": "a"},
                {"id": 2, "name": null},
            ])
        );
    }
}
