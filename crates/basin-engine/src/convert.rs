//! Cross-arrow-version glue.
//!
//! `basin-storage` and `basin-catalog` use the workspace's pinned Arrow
//! crates (v54), while the DataFusion 44 ecosystem this crate depends on is
//! still on Arrow v53. Rather than bend either side, we explicitly translate
//! at the boundary.
//!
//! The PoC only needs the small set of scalar types listed in the SQL
//! contract: Int64, Utf8, Boolean, Float64. Adding more later is a matter of
//! extending the `match` arms here.
//!
//! `ws_*` = workspace Arrow (v54), what `basin-storage` / `basin-catalog`
//! and the public engine API expose.
//! `df_*` = DataFusion's bundled Arrow (v53), what `SessionContext` speaks.

use std::sync::Arc;

use basin_common::{BasinError, Result};

// Workspace-side (v54) imports.
use arrow_array as ws_array;
use arrow_array::Array as _;
use arrow_schema as ws_schema;
use arrow_schema::IntervalUnit as WsIntervalUnit;
use arrow_schema::TimeUnit as WsTimeUnit;

// DataFusion-side (v53) imports.
use datafusion::arrow::array as df_array;
use datafusion::arrow::array::Array as _;
use datafusion::arrow::datatypes as df_schema;
use datafusion::arrow::datatypes::IntervalUnit as DfIntervalUnit;
use datafusion::arrow::datatypes::TimeUnit as DfTimeUnit;

fn timeunit_ws_to_df(u: &WsTimeUnit) -> DfTimeUnit {
    match u {
        WsTimeUnit::Second => DfTimeUnit::Second,
        WsTimeUnit::Millisecond => DfTimeUnit::Millisecond,
        WsTimeUnit::Microsecond => DfTimeUnit::Microsecond,
        WsTimeUnit::Nanosecond => DfTimeUnit::Nanosecond,
    }
}

fn timeunit_df_to_ws(u: &DfTimeUnit) -> WsTimeUnit {
    match u {
        DfTimeUnit::Second => WsTimeUnit::Second,
        DfTimeUnit::Millisecond => WsTimeUnit::Millisecond,
        DfTimeUnit::Microsecond => WsTimeUnit::Microsecond,
        DfTimeUnit::Nanosecond => WsTimeUnit::Nanosecond,
    }
}

/// Build a DataFusion-side `Schema` from a workspace-side `Schema`. Field
/// metadata (e.g. the `BASIN_TYPE=JSONB` tag on JSONB columns) is preserved
/// — DataFusion drops unknown keys silently but keeps round-trippable ones,
/// and we depend on the JSONB tag surviving the round-trip so the encoder
/// at the other end can pick it up.
pub(crate) fn schema_ws_to_df(s: &ws_schema::Schema) -> Result<df_schema::Schema> {
    let mut fields = Vec::with_capacity(s.fields().len());
    for f in s.fields() {
        let mut field = df_schema::Field::new(
            f.name().clone(),
            data_type_ws_to_df(f.data_type())?,
            f.is_nullable(),
        );
        if !f.metadata().is_empty() {
            field = field.with_metadata(f.metadata().clone());
        }
        fields.push(field);
    }
    Ok(df_schema::Schema::new(fields))
}

/// Build a workspace-side `Schema` from a DataFusion-side `Schema`. Mirror
/// of `schema_ws_to_df` — preserves field metadata.
pub(crate) fn schema_df_to_ws(s: &df_schema::Schema) -> Result<ws_schema::Schema> {
    let mut fields = Vec::with_capacity(s.fields().len());
    for f in s.fields() {
        let mut field = ws_schema::Field::new(
            f.name().clone(),
            data_type_df_to_ws(f.data_type())?,
            f.is_nullable(),
        );
        if !f.metadata().is_empty() {
            field = field.with_metadata(f.metadata().clone());
        }
        fields.push(field);
    }
    Ok(ws_schema::Schema::new(fields))
}

fn data_type_ws_to_df(dt: &ws_schema::DataType) -> Result<df_schema::DataType> {
    Ok(match dt {
        ws_schema::DataType::Null => df_schema::DataType::Null,
        ws_schema::DataType::Int16 => df_schema::DataType::Int16,
        ws_schema::DataType::Int32 => df_schema::DataType::Int32,
        ws_schema::DataType::Int64 => df_schema::DataType::Int64,
        ws_schema::DataType::Utf8 => df_schema::DataType::Utf8,
        ws_schema::DataType::Boolean => df_schema::DataType::Boolean,
        ws_schema::DataType::Float64 => df_schema::DataType::Float64,
        ws_schema::DataType::Float32 => df_schema::DataType::Float32,
        ws_schema::DataType::Binary => df_schema::DataType::Binary,
        ws_schema::DataType::LargeBinary => df_schema::DataType::LargeBinary,
        ws_schema::DataType::FixedSizeBinary(n) => df_schema::DataType::FixedSizeBinary(*n),
        ws_schema::DataType::Date32 => df_schema::DataType::Date32,
        ws_schema::DataType::Timestamp(unit, tz) => df_schema::DataType::Timestamp(
            timeunit_ws_to_df(unit),
            tz.clone(),
        ),
        ws_schema::DataType::Decimal128(p, s) => df_schema::DataType::Decimal128(*p, *s),
        ws_schema::DataType::Interval(WsIntervalUnit::MonthDayNano) => {
            df_schema::DataType::Interval(DfIntervalUnit::MonthDayNano)
        }
        ws_schema::DataType::FixedSizeList(child, n) => {
            df_schema::DataType::FixedSizeList(
                Arc::new(df_schema::Field::new(
                    child.name().clone(),
                    data_type_ws_to_df(child.data_type())?,
                    child.is_nullable(),
                )),
                *n,
            )
        }
        other => {
            return Err(BasinError::InvalidSchema(format!(
                "cannot convert workspace-arrow type to df-arrow: {other:?}"
            )));
        }
    })
}

fn data_type_df_to_ws(dt: &df_schema::DataType) -> Result<ws_schema::DataType> {
    Ok(match dt {
        df_schema::DataType::Null => ws_schema::DataType::Null,
        df_schema::DataType::Int16 => ws_schema::DataType::Int16,
        df_schema::DataType::Int32 => ws_schema::DataType::Int32,
        df_schema::DataType::Int64 => ws_schema::DataType::Int64,
        df_schema::DataType::Utf8 => ws_schema::DataType::Utf8,
        df_schema::DataType::Boolean => ws_schema::DataType::Boolean,
        df_schema::DataType::Float64 => ws_schema::DataType::Float64,
        df_schema::DataType::Float32 => ws_schema::DataType::Float32,
        df_schema::DataType::Binary => ws_schema::DataType::Binary,
        df_schema::DataType::LargeBinary => ws_schema::DataType::LargeBinary,
        df_schema::DataType::FixedSizeBinary(n) => ws_schema::DataType::FixedSizeBinary(*n),
        df_schema::DataType::Date32 => ws_schema::DataType::Date32,
        df_schema::DataType::Timestamp(unit, tz) => ws_schema::DataType::Timestamp(
            timeunit_df_to_ws(unit),
            tz.clone(),
        ),
        df_schema::DataType::Decimal128(p, s) => ws_schema::DataType::Decimal128(*p, *s),
        df_schema::DataType::Interval(DfIntervalUnit::MonthDayNano) => {
            ws_schema::DataType::Interval(WsIntervalUnit::MonthDayNano)
        }
        df_schema::DataType::FixedSizeList(child, n) => {
            ws_schema::DataType::FixedSizeList(
                Arc::new(ws_schema::Field::new(
                    child.name().clone(),
                    data_type_df_to_ws(child.data_type())?,
                    child.is_nullable(),
                )),
                *n,
            )
        }
        other => {
            return Err(BasinError::InvalidSchema(format!(
                "cannot convert df-arrow type to workspace-arrow: {other:?}"
            )));
        }
    })
}

/// Translate one workspace-side `RecordBatch` into a DataFusion-side
/// `RecordBatch`. The mirror of [`batch_df_to_ws`]; same per-column rebuild
/// strategy, opposite arrow versions. Used when the engine needs to feed
/// a workspace-arrow batch into DataFusion (e.g. registering a one-row
/// `MemTable` for generated-column expression evaluation).
pub(crate) fn batch_ws_to_df(
    batch: &ws_array::RecordBatch,
) -> Result<df_array::RecordBatch> {
    let target_schema = Arc::new(schema_ws_to_df(batch.schema().as_ref())?);
    let mut columns: Vec<Arc<dyn df_array::Array>> = Vec::with_capacity(batch.num_columns());
    for (i, field) in target_schema.fields().iter().enumerate() {
        let src = batch.column(i);
        let dst: Arc<dyn df_array::Array> = match field.data_type() {
            df_schema::DataType::Null => {
                Arc::new(df_array::NullArray::new(src.len()))
            }
            df_schema::DataType::Int16 => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::Int16Array>()
                    .ok_or_else(|| BasinError::internal(format!(
                        "expected Int16Array for {}", field.name()
                    )))?;
                let vals: Vec<Option<i16>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::Int16Array::from(vals))
            }
            df_schema::DataType::Int32 => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::Int32Array>()
                    .ok_or_else(|| BasinError::internal(format!(
                        "expected Int32Array for {}", field.name()
                    )))?;
                let vals: Vec<Option<i32>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::Int32Array::from(vals))
            }
            df_schema::DataType::Int64 => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::Int64Array>()
                    .ok_or_else(|| BasinError::internal(format!(
                        "expected Int64Array for {}", field.name()
                    )))?;
                let vals: Vec<Option<i64>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::Int64Array::from(vals))
            }
            df_schema::DataType::Date32 => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::Date32Array>()
                    .ok_or_else(|| BasinError::internal(format!(
                        "expected Date32Array for {}", field.name()
                    )))?;
                let vals: Vec<Option<i32>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::Date32Array::from(vals))
            }
            df_schema::DataType::Utf8 => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::StringArray>()
                    .ok_or_else(|| BasinError::internal(format!(
                        "expected StringArray for {}", field.name()
                    )))?;
                let vals: Vec<Option<String>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j).to_string()) })
                    .collect();
                Arc::new(df_array::StringArray::from(vals))
            }
            df_schema::DataType::Boolean => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::BooleanArray>()
                    .ok_or_else(|| BasinError::internal(format!(
                        "expected BooleanArray for {}", field.name()
                    )))?;
                let vals: Vec<Option<bool>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::BooleanArray::from(vals))
            }
            df_schema::DataType::Float32 => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::Float32Array>()
                    .ok_or_else(|| BasinError::internal(format!(
                        "expected Float32Array for {}", field.name()
                    )))?;
                let vals: Vec<Option<f32>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::Float32Array::from(vals))
            }
            df_schema::DataType::Float64 => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::Float64Array>()
                    .ok_or_else(|| BasinError::internal(format!(
                        "expected Float64Array for {}", field.name()
                    )))?;
                let vals: Vec<Option<f64>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::Float64Array::from(vals))
            }
            df_schema::DataType::Binary => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::BinaryArray>()
                    .ok_or_else(|| BasinError::internal(format!(
                        "expected BinaryArray for {}", field.name()
                    )))?;
                let vals: Vec<Option<&[u8]>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::BinaryArray::from(vals))
            }
            df_schema::DataType::LargeBinary => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::LargeBinaryArray>()
                    .ok_or_else(|| BasinError::internal(format!(
                        "expected LargeBinaryArray for {}", field.name()
                    )))?;
                let vals: Vec<Option<&[u8]>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::LargeBinaryArray::from(vals))
            }
            df_schema::DataType::FixedSizeBinary(n) => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::FixedSizeBinaryArray>()
                    .ok_or_else(|| BasinError::internal(format!(
                        "expected FixedSizeBinaryArray for {}", field.name()
                    )))?;
                let size = *n;
                let mut rows: Vec<Option<Vec<u8>>> = Vec::with_capacity(s.len());
                for j in 0..s.len() {
                    if s.is_null(j) {
                        rows.push(None);
                    } else {
                        rows.push(Some(s.value(j).to_vec()));
                    }
                }
                let arr = df_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    rows.into_iter(),
                    size,
                )
                .map_err(|e| {
                    BasinError::internal(format!(
                        "rebuilding FixedSizeBinary({size}) for column {}: {e}",
                        field.name()
                    ))
                })?;
                Arc::new(arr)
            }
            df_schema::DataType::Timestamp(unit, tz) => {
                use datafusion::arrow::array::types::{
                    TimestampMicrosecondType as DfMicros,
                    TimestampMillisecondType as DfMilli,
                    TimestampNanosecondType as DfNanos,
                    TimestampSecondType as DfSec,
                };
                let vals: Vec<Option<i64>> = match unit {
                    DfTimeUnit::Microsecond => {
                        let s = src
                            .as_any()
                            .downcast_ref::<ws_array::TimestampMicrosecondArray>()
                            .ok_or_else(|| BasinError::internal(format!(
                                "expected TimestampMicrosecondArray for {}", field.name()
                            )))?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    DfTimeUnit::Millisecond => {
                        let s = src
                            .as_any()
                            .downcast_ref::<ws_array::TimestampMillisecondArray>()
                            .ok_or_else(|| BasinError::internal(format!(
                                "expected TimestampMillisecondArray for {}", field.name()
                            )))?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    DfTimeUnit::Nanosecond => {
                        let s = src
                            .as_any()
                            .downcast_ref::<ws_array::TimestampNanosecondArray>()
                            .ok_or_else(|| BasinError::internal(format!(
                                "expected TimestampNanosecondArray for {}", field.name()
                            )))?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    DfTimeUnit::Second => {
                        let s = src
                            .as_any()
                            .downcast_ref::<ws_array::TimestampSecondArray>()
                            .ok_or_else(|| BasinError::internal(format!(
                                "expected TimestampSecondArray for {}", field.name()
                            )))?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                };
                let dt = df_schema::DataType::Timestamp(unit.clone(), tz.clone());
                let arr: Arc<dyn df_array::Array> = match unit {
                    DfTimeUnit::Microsecond => Arc::new(
                        df_array::PrimitiveArray::<DfMicros>::from(vals).with_data_type(dt),
                    ),
                    DfTimeUnit::Millisecond => Arc::new(
                        df_array::PrimitiveArray::<DfMilli>::from(vals).with_data_type(dt),
                    ),
                    DfTimeUnit::Nanosecond => Arc::new(
                        df_array::PrimitiveArray::<DfNanos>::from(vals).with_data_type(dt),
                    ),
                    DfTimeUnit::Second => Arc::new(
                        df_array::PrimitiveArray::<DfSec>::from(vals).with_data_type(dt),
                    ),
                };
                arr
            }
            df_schema::DataType::Decimal128(p, s) => {
                // PG `numeric` rides on Decimal128. Both arrow versions
                // share the i128 layout, so we walk the source array's
                // raw values and rebuild on the df side preserving the
                // column's `(precision, scale)`.
                let src_arr = src
                    .as_any()
                    .downcast_ref::<ws_array::Decimal128Array>()
                    .ok_or_else(|| BasinError::internal(format!(
                        "expected Decimal128Array for {}", field.name()
                    )))?;
                let vals: Vec<Option<i128>> = (0..src_arr.len())
                    .map(|j| if src_arr.is_null(j) { None } else { Some(src_arr.value(j)) })
                    .collect();
                let arr = df_array::Decimal128Array::from(vals)
                    .with_precision_and_scale(*p, *s)
                    .map_err(|e| {
                        BasinError::internal(format!(
                            "Decimal128 ({p},{s}) for column {}: {e}",
                            field.name()
                        ))
                    })?;
                Arc::new(arr)
            }
            other => {
                return Err(BasinError::InvalidSchema(format!(
                    "cannot translate ws column {} of type {other:?}",
                    field.name()
                )));
            }
        };
        columns.push(dst);
    }
    df_array::RecordBatch::try_new(target_schema, columns)
        .map_err(|e| BasinError::internal(format!("rebuild df batch: {e}")))
}

/// Translate one DataFusion-side `RecordBatch` into a workspace-side
/// `RecordBatch`. Since both arrow crates store the same physical layouts and
/// the PoC only exercises a handful of scalar types, we walk arrays
/// element-by-element rather than touching the FFI layer.
pub(crate) fn batch_df_to_ws(
    batch: &df_array::RecordBatch,
) -> Result<ws_array::RecordBatch> {
    let target_schema = Arc::new(schema_df_to_ws(batch.schema().as_ref())?);
    let mut columns: Vec<Arc<dyn ws_array::Array>> = Vec::with_capacity(batch.num_columns());
    for (i, field) in target_schema.fields().iter().enumerate() {
        let src = batch.column(i);
        let dst: Arc<dyn ws_array::Array> = match field.data_type() {
            ws_schema::DataType::Null => {
                Arc::new(ws_array::NullArray::new(src.len()))
            }
            ws_schema::DataType::Interval(WsIntervalUnit::MonthDayNano) => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::IntervalMonthDayNanoArray>()
                    .ok_or_else(|| {
                        BasinError::internal(format!(
                            "expected IntervalMonthDayNanoArray for {}",
                            field.name()
                        ))
                    })?;
                let vals: Vec<Option<arrow_array::types::IntervalMonthDayNano>> = (0..s.len())
                    .map(|j| {
                        if s.is_null(j) {
                            None
                        } else {
                            let v = s.value(j);
                            Some(arrow_array::types::IntervalMonthDayNano::new(
                                v.months,
                                v.days,
                                v.nanoseconds,
                            ))
                        }
                    })
                    .collect();
                Arc::new(ws_array::IntervalMonthDayNanoArray::from(vals))
            }
            ws_schema::DataType::Int16 => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::Int16Array>()
                    .ok_or_else(|| BasinError::internal(format!("expected Int16Array for {}", field.name())))?;
                let vals: Vec<Option<i16>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(ws_array::Int16Array::from(vals))
            }
            ws_schema::DataType::Int32 => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::Int32Array>()
                    .ok_or_else(|| BasinError::internal(format!("expected Int32Array for {}", field.name())))?;
                let vals: Vec<Option<i32>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(ws_array::Int32Array::from(vals))
            }
            ws_schema::DataType::Int64 => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::Int64Array>()
                    .ok_or_else(|| BasinError::internal(format!("expected Int64Array for {}", field.name())))?;
                let vals: Vec<Option<i64>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(ws_array::Int64Array::from(vals))
            }
            ws_schema::DataType::Date32 => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::Date32Array>()
                    .ok_or_else(|| BasinError::internal(format!("expected Date32Array for {}", field.name())))?;
                let vals: Vec<Option<i32>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(ws_array::Date32Array::from(vals))
            }
            ws_schema::DataType::Utf8 => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::StringArray>()
                    .ok_or_else(|| BasinError::internal(format!("expected StringArray for {}", field.name())))?;
                let vals: Vec<Option<String>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j).to_string()) })
                    .collect();
                Arc::new(ws_array::StringArray::from(vals))
            }
            ws_schema::DataType::Boolean => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::BooleanArray>()
                    .ok_or_else(|| BasinError::internal(format!("expected BooleanArray for {}", field.name())))?;
                let vals: Vec<Option<bool>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(ws_array::BooleanArray::from(vals))
            }
            ws_schema::DataType::Float64 => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::Float64Array>()
                    .ok_or_else(|| BasinError::internal(format!("expected Float64Array for {}", field.name())))?;
                let vals: Vec<Option<f64>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(ws_array::Float64Array::from(vals))
            }
            ws_schema::DataType::Float32 => {
                // pg_catalog.pg_class.reltuples surfaces as Float32 — needed
                // so the info_schema_provider's RecordBatch round-trips
                // through the executor's df→ws bridge cleanly.
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::Float32Array>()
                    .ok_or_else(|| BasinError::internal(format!("expected Float32Array for {}", field.name())))?;
                let vals: Vec<Option<f32>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(ws_array::Float32Array::from(vals))
            }
            ws_schema::DataType::Binary => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::BinaryArray>()
                    .ok_or_else(|| BasinError::internal(format!("expected BinaryArray for {}", field.name())))?;
                let vals: Vec<Option<&[u8]>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(ws_array::BinaryArray::from(vals))
            }
            ws_schema::DataType::LargeBinary => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::LargeBinaryArray>()
                    .ok_or_else(|| BasinError::internal(format!("expected LargeBinaryArray for {}", field.name())))?;
                let vals: Vec<Option<&[u8]>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(ws_array::LargeBinaryArray::from(vals))
            }
            ws_schema::DataType::FixedSizeBinary(n) => {
                // UUID columns ride on FixedSizeBinary(16). Walk the
                // null bitmap and per-row slice; rebuild on the
                // workspace side with `try_from_sparse_iter_with_size`
                // so null cells don't need a placeholder buffer.
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        BasinError::internal(format!(
                            "expected FixedSizeBinaryArray for {}",
                            field.name()
                        ))
                    })?;
                let size = *n;
                let mut rows: Vec<Option<Vec<u8>>> = Vec::with_capacity(s.len());
                for j in 0..s.len() {
                    if s.is_null(j) {
                        rows.push(None);
                    } else {
                        rows.push(Some(s.value(j).to_vec()));
                    }
                }
                let arr = ws_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    rows.into_iter(),
                    size,
                )
                .map_err(|e| {
                    BasinError::internal(format!(
                        "rebuilding FixedSizeBinary({size}) for column {}: {e}",
                        field.name()
                    ))
                })?;
                Arc::new(arr)
            }
            ws_schema::DataType::Timestamp(unit, tz) => {
                // Pass through the underlying i64 buffer; both arrow versions
                // store Timestamp as PrimitiveArray<i64> with identical
                // encoding, so we just rebuild on the workspace side using
                // the typed array matching `unit`.
                use arrow_array::types::{
                    TimestampMicrosecondType as WsMicros,
                    TimestampMillisecondType as WsMilli,
                    TimestampNanosecondType as WsNanos,
                    TimestampSecondType as WsSec,
                };
                use datafusion::arrow::array::TimestampMicrosecondArray as DfMicros;
                use datafusion::arrow::array::TimestampMillisecondArray as DfMilli;
                use datafusion::arrow::array::TimestampNanosecondArray as DfNanos;
                use datafusion::arrow::array::TimestampSecondArray as DfSec;
                let vals: Vec<Option<i64>> = match unit {
                    WsTimeUnit::Microsecond => {
                        let s = src.as_any().downcast_ref::<DfMicros>().ok_or_else(|| {
                            BasinError::internal(format!(
                                "expected TimestampMicrosecondArray for {}",
                                field.name()
                            ))
                        })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    WsTimeUnit::Millisecond => {
                        let s = src.as_any().downcast_ref::<DfMilli>().ok_or_else(|| {
                            BasinError::internal(format!(
                                "expected TimestampMillisecondArray for {}",
                                field.name()
                            ))
                        })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    WsTimeUnit::Nanosecond => {
                        let s = src.as_any().downcast_ref::<DfNanos>().ok_or_else(|| {
                            BasinError::internal(format!(
                                "expected TimestampNanosecondArray for {}",
                                field.name()
                            ))
                        })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    WsTimeUnit::Second => {
                        let s = src.as_any().downcast_ref::<DfSec>().ok_or_else(|| {
                            BasinError::internal(format!(
                                "expected TimestampSecondArray for {}",
                                field.name()
                            ))
                        })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                };
                let dt = ws_schema::DataType::Timestamp(unit.clone(), tz.clone());
                let arr: Arc<dyn ws_array::Array> = match unit {
                    WsTimeUnit::Microsecond => Arc::new(
                        ws_array::PrimitiveArray::<WsMicros>::from(vals).with_data_type(dt),
                    ),
                    WsTimeUnit::Millisecond => Arc::new(
                        ws_array::PrimitiveArray::<WsMilli>::from(vals).with_data_type(dt),
                    ),
                    WsTimeUnit::Nanosecond => Arc::new(
                        ws_array::PrimitiveArray::<WsNanos>::from(vals).with_data_type(dt),
                    ),
                    WsTimeUnit::Second => Arc::new(
                        ws_array::PrimitiveArray::<WsSec>::from(vals).with_data_type(dt),
                    ),
                };
                arr
            }
            ws_schema::DataType::Decimal128(p, s) => {
                // PG `numeric` round-trip back to the workspace side.
                // Mirror of the ws→df arm; the i128 buffer is identical
                // across arrow versions so we just walk per-row.
                let src_arr = src
                    .as_any()
                    .downcast_ref::<df_array::Decimal128Array>()
                    .ok_or_else(|| BasinError::internal(format!(
                        "expected Decimal128Array for {}", field.name()
                    )))?;
                let vals: Vec<Option<i128>> = (0..src_arr.len())
                    .map(|j| if src_arr.is_null(j) { None } else { Some(src_arr.value(j)) })
                    .collect();
                let arr = ws_array::Decimal128Array::from(vals)
                    .with_precision_and_scale(*p, *s)
                    .map_err(|e| {
                        BasinError::internal(format!(
                            "Decimal128 ({p},{s}) for column {}: {e}",
                            field.name()
                        ))
                    })?;
                Arc::new(arr)
            }
            ws_schema::DataType::FixedSizeList(child, n) => {
                // Only the FixedSizeList<Float32> shape (vector(N)) is in
                // scope for the PoC. Walk the parent's nulls and the inner
                // primitive values to rebuild on the workspace side.
                if *child.data_type() != ws_schema::DataType::Float32 {
                    return Err(BasinError::InvalidSchema(format!(
                        "FixedSizeList child must be Float32, got {:?}",
                        child.data_type()
                    )));
                }
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::FixedSizeListArray>()
                    .ok_or_else(|| {
                        BasinError::internal(format!(
                            "expected FixedSizeListArray for {}",
                            field.name()
                        ))
                    })?;
                let values = s
                    .values()
                    .as_any()
                    .downcast_ref::<df_array::Float32Array>()
                    .ok_or_else(|| {
                        BasinError::internal(format!(
                            "FixedSizeList child must be Float32Array for {}",
                            field.name()
                        ))
                    })?;
                let dim = *n as usize;
                let mut rows: Vec<Option<Vec<Option<f32>>>> = Vec::with_capacity(s.len());
                for j in 0..s.len() {
                    if s.is_null(j) {
                        rows.push(None);
                    } else {
                        let mut v = Vec::with_capacity(dim);
                        for k in 0..dim {
                            let idx = j * dim + k;
                            if values.is_null(idx) {
                                v.push(None);
                            } else {
                                v.push(Some(values.value(idx)));
                            }
                        }
                        rows.push(Some(v));
                    }
                }
                let arr = ws_array::FixedSizeListArray::from_iter_primitive::<
                    arrow_array::types::Float32Type,
                    _,
                    _,
                >(rows, *n);
                Arc::new(arr)
            }
            other => {
                return Err(BasinError::InvalidSchema(format!(
                    "cannot translate column {} of type {other:?}",
                    field.name()
                )));
            }
        };
        columns.push(dst);
    }
    ws_array::RecordBatch::try_new(target_schema, columns)
        .map_err(|e| BasinError::internal(format!("rebuild ws batch: {e}")))
}
