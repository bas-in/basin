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
// Buffer types used when constructing workspace-side ListArrays.
use arrow::array::BooleanBufferBuilder as WsBooleanBufferBuilder;
use arrow::buffer::ScalarBuffer as WsScalarBuffer;
use arrow::buffer::{NullBuffer as WsNullBuffer, OffsetBuffer as WsOffsetBuffer, ScalarBuffer};

// DataFusion-side (v53) imports.
use datafusion::arrow::array as df_array;
use datafusion::arrow::array::Array as _;
use datafusion::arrow::datatypes as df_schema;
use datafusion::arrow::datatypes::IntervalUnit as DfIntervalUnit;
use datafusion::arrow::datatypes::TimeUnit as DfTimeUnit;

fn intervalunit_ws_to_df(u: &WsIntervalUnit) -> DfIntervalUnit {
    match u {
        WsIntervalUnit::YearMonth => DfIntervalUnit::YearMonth,
        WsIntervalUnit::DayTime => DfIntervalUnit::DayTime,
        WsIntervalUnit::MonthDayNano => DfIntervalUnit::MonthDayNano,
    }
}

fn intervalunit_df_to_ws(u: &DfIntervalUnit) -> WsIntervalUnit {
    match u {
        DfIntervalUnit::YearMonth => WsIntervalUnit::YearMonth,
        DfIntervalUnit::DayTime => WsIntervalUnit::DayTime,
        DfIntervalUnit::MonthDayNano => WsIntervalUnit::MonthDayNano,
    }
}

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
        ws_schema::DataType::Time64(unit) => df_schema::DataType::Time64(timeunit_ws_to_df(unit)),
        ws_schema::DataType::Time32(unit) => df_schema::DataType::Time32(timeunit_ws_to_df(unit)),
        ws_schema::DataType::Timestamp(unit, tz) => {
            df_schema::DataType::Timestamp(timeunit_ws_to_df(unit), tz.clone())
        }
        ws_schema::DataType::Decimal128(p, s) => df_schema::DataType::Decimal128(*p, *s),
        ws_schema::DataType::UInt64 => df_schema::DataType::UInt64,
        ws_schema::DataType::Interval(u) => df_schema::DataType::Interval(intervalunit_ws_to_df(u)),
        ws_schema::DataType::Duration(u) => df_schema::DataType::Duration(timeunit_ws_to_df(u)),
        ws_schema::DataType::List(child) => {
            df_schema::DataType::List(Arc::new(df_schema::Field::new(
                child.name().clone(),
                data_type_ws_to_df(child.data_type())?,
                child.is_nullable(),
            )))
        }
        ws_schema::DataType::LargeList(child) => {
            df_schema::DataType::LargeList(Arc::new(df_schema::Field::new(
                child.name().clone(),
                data_type_ws_to_df(child.data_type())?,
                child.is_nullable(),
            )))
        }
        ws_schema::DataType::FixedSizeList(child, n) => df_schema::DataType::FixedSizeList(
            Arc::new(df_schema::Field::new(
                child.name().clone(),
                data_type_ws_to_df(child.data_type())?,
                child.is_nullable(),
            )),
            *n,
        ),
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
        // DataFusion string_agg / string_concat return LargeUtf8; map to Utf8.
        df_schema::DataType::LargeUtf8 => ws_schema::DataType::Utf8,
        // DataFusion 53 may emit Utf8View (StringView) for certain string
        // functions (e.g. initcap, substr, regexp_replace). Map to Utf8 so
        // the workspace-arrow side never sees the view type.
        df_schema::DataType::Utf8View => ws_schema::DataType::Utf8,
        // Analogous view-type for binary data.
        df_schema::DataType::BinaryView => ws_schema::DataType::Binary,
        df_schema::DataType::Boolean => ws_schema::DataType::Boolean,
        df_schema::DataType::Float64 => ws_schema::DataType::Float64,
        df_schema::DataType::Float32 => ws_schema::DataType::Float32,
        df_schema::DataType::Binary => ws_schema::DataType::Binary,
        df_schema::DataType::LargeBinary => ws_schema::DataType::LargeBinary,
        df_schema::DataType::FixedSizeBinary(n) => ws_schema::DataType::FixedSizeBinary(*n),
        df_schema::DataType::Date32 => ws_schema::DataType::Date32,
        df_schema::DataType::Time64(unit) => ws_schema::DataType::Time64(timeunit_df_to_ws(unit)),
        df_schema::DataType::Time32(unit) => ws_schema::DataType::Time32(timeunit_df_to_ws(unit)),
        df_schema::DataType::Timestamp(unit, tz) => {
            ws_schema::DataType::Timestamp(timeunit_df_to_ws(unit), tz.clone())
        }
        df_schema::DataType::Decimal128(p, s) => ws_schema::DataType::Decimal128(*p, *s),
        df_schema::DataType::UInt64 => ws_schema::DataType::UInt64,
        df_schema::DataType::Interval(u) => ws_schema::DataType::Interval(intervalunit_df_to_ws(u)),
        df_schema::DataType::Duration(u) => ws_schema::DataType::Duration(timeunit_df_to_ws(u)),
        df_schema::DataType::List(child) => {
            ws_schema::DataType::List(Arc::new(ws_schema::Field::new(
                child.name().clone(),
                data_type_df_to_ws(child.data_type())?,
                child.is_nullable(),
            )))
        }
        df_schema::DataType::LargeList(child) => {
            ws_schema::DataType::LargeList(Arc::new(ws_schema::Field::new(
                child.name().clone(),
                data_type_df_to_ws(child.data_type())?,
                child.is_nullable(),
            )))
        }
        df_schema::DataType::FixedSizeList(child, n) => ws_schema::DataType::FixedSizeList(
            Arc::new(ws_schema::Field::new(
                child.name().clone(),
                data_type_df_to_ws(child.data_type())?,
                child.is_nullable(),
            )),
            *n,
        ),
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
pub(crate) fn batch_ws_to_df(batch: &ws_array::RecordBatch) -> Result<df_array::RecordBatch> {
    let target_schema = Arc::new(schema_ws_to_df(batch.schema().as_ref())?);
    let mut columns: Vec<Arc<dyn df_array::Array>> = Vec::with_capacity(batch.num_columns());
    for (i, field) in target_schema.fields().iter().enumerate() {
        let src = batch.column(i);
        let dst: Arc<dyn df_array::Array> = match field.data_type() {
            df_schema::DataType::Null => Arc::new(df_array::NullArray::new(src.len())),
            df_schema::DataType::Int16 => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::Int16Array>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected Int16Array for {}", field.name()))
                    })?;
                let vals: Vec<Option<i16>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::Int16Array::from(vals))
            }
            df_schema::DataType::Int32 => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::Int32Array>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected Int32Array for {}", field.name()))
                    })?;
                let vals: Vec<Option<i32>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::Int32Array::from(vals))
            }
            df_schema::DataType::Int64 => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::Int64Array>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected Int64Array for {}", field.name()))
                    })?;
                let vals: Vec<Option<i64>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::Int64Array::from(vals))
            }
            df_schema::DataType::Date32 => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::Date32Array>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected Date32Array for {}", field.name()))
                    })?;
                let vals: Vec<Option<i32>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::Date32Array::from(vals))
            }
            df_schema::DataType::Utf8 => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::StringArray>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected StringArray for {}", field.name()))
                    })?;
                let vals: Vec<Option<String>> = (0..s.len())
                    .map(|j| {
                        if s.is_null(j) {
                            None
                        } else {
                            Some(s.value(j).to_string())
                        }
                    })
                    .collect();
                Arc::new(df_array::StringArray::from(vals))
            }
            df_schema::DataType::Boolean => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::BooleanArray>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected BooleanArray for {}", field.name()))
                    })?;
                let vals: Vec<Option<bool>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::BooleanArray::from(vals))
            }
            df_schema::DataType::Float32 => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::Float32Array>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected Float32Array for {}", field.name()))
                    })?;
                let vals: Vec<Option<f32>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::Float32Array::from(vals))
            }
            df_schema::DataType::Float64 => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::Float64Array>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected Float64Array for {}", field.name()))
                    })?;
                let vals: Vec<Option<f64>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::Float64Array::from(vals))
            }
            df_schema::DataType::Binary => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::BinaryArray>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected BinaryArray for {}", field.name()))
                    })?;
                let vals: Vec<Option<&[u8]>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::BinaryArray::from(vals))
            }
            df_schema::DataType::LargeBinary => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::LargeBinaryArray>()
                    .ok_or_else(|| {
                        BasinError::internal(format!(
                            "expected LargeBinaryArray for {}",
                            field.name()
                        ))
                    })?;
                let vals: Vec<Option<&[u8]>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::LargeBinaryArray::from(vals))
            }
            df_schema::DataType::FixedSizeBinary(n) => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::FixedSizeBinaryArray>()
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
                    TimestampMicrosecondType as DfMicros, TimestampMillisecondType as DfMilli,
                    TimestampNanosecondType as DfNanos, TimestampSecondType as DfSec,
                };
                let vals: Vec<Option<i64>> = match unit {
                    DfTimeUnit::Microsecond => {
                        let s = src
                            .as_any()
                            .downcast_ref::<ws_array::TimestampMicrosecondArray>()
                            .ok_or_else(|| {
                                BasinError::internal(format!(
                                    "expected TimestampMicrosecondArray for {}",
                                    field.name()
                                ))
                            })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    DfTimeUnit::Millisecond => {
                        let s = src
                            .as_any()
                            .downcast_ref::<ws_array::TimestampMillisecondArray>()
                            .ok_or_else(|| {
                                BasinError::internal(format!(
                                    "expected TimestampMillisecondArray for {}",
                                    field.name()
                                ))
                            })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    DfTimeUnit::Nanosecond => {
                        let s = src
                            .as_any()
                            .downcast_ref::<ws_array::TimestampNanosecondArray>()
                            .ok_or_else(|| {
                                BasinError::internal(format!(
                                    "expected TimestampNanosecondArray for {}",
                                    field.name()
                                ))
                            })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    DfTimeUnit::Second => {
                        let s = src
                            .as_any()
                            .downcast_ref::<ws_array::TimestampSecondArray>()
                            .ok_or_else(|| {
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
                let dt = df_schema::DataType::Timestamp(*unit, tz.clone());
                let arr: Arc<dyn df_array::Array> = match unit {
                    DfTimeUnit::Microsecond => Arc::new(
                        df_array::PrimitiveArray::<DfMicros>::from(vals).with_data_type(dt),
                    ),
                    DfTimeUnit::Millisecond => {
                        Arc::new(df_array::PrimitiveArray::<DfMilli>::from(vals).with_data_type(dt))
                    }
                    DfTimeUnit::Nanosecond => {
                        Arc::new(df_array::PrimitiveArray::<DfNanos>::from(vals).with_data_type(dt))
                    }
                    DfTimeUnit::Second => {
                        Arc::new(df_array::PrimitiveArray::<DfSec>::from(vals).with_data_type(dt))
                    }
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
                    .ok_or_else(|| {
                        BasinError::internal(format!(
                            "expected Decimal128Array for {}",
                            field.name()
                        ))
                    })?;
                let vals: Vec<Option<i128>> = (0..src_arr.len())
                    .map(|j| {
                        if src_arr.is_null(j) {
                            None
                        } else {
                            Some(src_arr.value(j))
                        }
                    })
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
            df_schema::DataType::UInt64 => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::UInt64Array>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected UInt64Array for {}", field.name()))
                    })?;
                let vals: Vec<Option<u64>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(df_array::UInt64Array::from(vals))
            }
            df_schema::DataType::Duration(unit) => {
                use datafusion::arrow::array::types::{
                    DurationMicrosecondType as DfDurMicros, DurationMillisecondType as DfDurMilli,
                    DurationNanosecondType as DfDurNanos, DurationSecondType as DfDurSec,
                };
                let vals: Vec<Option<i64>> = match unit {
                    DfTimeUnit::Second => {
                        let s = src
                            .as_any()
                            .downcast_ref::<ws_array::DurationSecondArray>()
                            .ok_or_else(|| {
                                BasinError::internal(format!(
                                    "expected DurationSecondArray for {}",
                                    field.name()
                                ))
                            })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    DfTimeUnit::Millisecond => {
                        let s = src
                            .as_any()
                            .downcast_ref::<ws_array::DurationMillisecondArray>()
                            .ok_or_else(|| {
                                BasinError::internal(format!(
                                    "expected DurationMillisecondArray for {}",
                                    field.name()
                                ))
                            })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    DfTimeUnit::Microsecond => {
                        let s = src
                            .as_any()
                            .downcast_ref::<ws_array::DurationMicrosecondArray>()
                            .ok_or_else(|| {
                                BasinError::internal(format!(
                                    "expected DurationMicrosecondArray for {}",
                                    field.name()
                                ))
                            })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    DfTimeUnit::Nanosecond => {
                        let s = src
                            .as_any()
                            .downcast_ref::<ws_array::DurationNanosecondArray>()
                            .ok_or_else(|| {
                                BasinError::internal(format!(
                                    "expected DurationNanosecondArray for {}",
                                    field.name()
                                ))
                            })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                };
                let dt = df_schema::DataType::Duration(*unit);
                let arr: Arc<dyn df_array::Array> = match unit {
                    DfTimeUnit::Second => Arc::new(
                        df_array::PrimitiveArray::<DfDurSec>::from(vals).with_data_type(dt),
                    ),
                    DfTimeUnit::Millisecond => Arc::new(
                        df_array::PrimitiveArray::<DfDurMilli>::from(vals).with_data_type(dt),
                    ),
                    DfTimeUnit::Microsecond => Arc::new(
                        df_array::PrimitiveArray::<DfDurMicros>::from(vals).with_data_type(dt),
                    ),
                    DfTimeUnit::Nanosecond => Arc::new(
                        df_array::PrimitiveArray::<DfDurNanos>::from(vals).with_data_type(dt),
                    ),
                };
                arr
            }
            df_schema::DataType::List(child_field) => {
                // src is a WS ListArray; child values are WS-typed.
                // Cross-version boundary: offsets and nulls must be rebuilt
                // on the DF side from raw values.
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::ListArray>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected ListArray for {}", field.name()))
                    })?;
                // Convert child values (WS-typed) to DF-typed via a single-column batch.
                let ws_child_type = data_type_df_to_ws(child_field.data_type())?;
                let ws_child_schema =
                    Arc::new(ws_schema::Schema::new(vec![ws_schema::Field::new(
                        child_field.name().clone(),
                        ws_child_type,
                        child_field.is_nullable(),
                    )]));
                let child_batch =
                    ws_array::RecordBatch::try_new(ws_child_schema, vec![s.values().clone()])
                        .map_err(|e| BasinError::internal(format!("ListArray child batch: {e}")))?;
                let child_df_batch = batch_ws_to_df(&child_batch)?;
                let df_child_arr = child_df_batch.column(0).clone();
                // Rebuild DF offsets from raw i32 values (cross-version safe).
                let raw_offsets: Vec<i32> = s.offsets().iter().copied().collect();
                let df_offsets = datafusion::arrow::buffer::OffsetBuffer::new(
                    datafusion::arrow::buffer::ScalarBuffer::from(raw_offsets),
                );
                // Rebuild nulls from raw bits (cross-version safe).
                let df_nulls = s.nulls().map(|nb| {
                    let bools: Vec<bool> = (0..nb.len()).map(|i| nb.is_valid(i)).collect();
                    datafusion::arrow::buffer::NullBuffer::from(bools)
                });
                let arr = df_array::ListArray::new(
                    Arc::new((**child_field).clone()),
                    df_offsets,
                    df_child_arr,
                    df_nulls,
                );
                Arc::new(arr)
            }
            df_schema::DataType::LargeList(child_field) => {
                let s = src
                    .as_any()
                    .downcast_ref::<ws_array::LargeListArray>()
                    .ok_or_else(|| {
                        BasinError::internal(format!(
                            "expected LargeListArray for {}",
                            field.name()
                        ))
                    })?;
                let ws_child_type = data_type_df_to_ws(child_field.data_type())?;
                let ws_child_schema =
                    Arc::new(ws_schema::Schema::new(vec![ws_schema::Field::new(
                        child_field.name().clone(),
                        ws_child_type,
                        child_field.is_nullable(),
                    )]));
                let child_batch =
                    ws_array::RecordBatch::try_new(ws_child_schema, vec![s.values().clone()])
                        .map_err(|e| {
                            BasinError::internal(format!("LargeListArray child batch: {e}"))
                        })?;
                let child_df_batch = batch_ws_to_df(&child_batch)?;
                let df_child_arr = child_df_batch.column(0).clone();
                let raw_offsets: Vec<i64> = s.offsets().iter().copied().collect();
                let df_offsets = datafusion::arrow::buffer::OffsetBuffer::new(
                    datafusion::arrow::buffer::ScalarBuffer::from(raw_offsets),
                );
                let df_nulls = s.nulls().map(|nb| {
                    let bools: Vec<bool> = (0..nb.len()).map(|i| nb.is_valid(i)).collect();
                    datafusion::arrow::buffer::NullBuffer::from(bools)
                });
                let arr = df_array::LargeListArray::new(
                    Arc::new((**child_field).clone()),
                    df_offsets,
                    df_child_arr,
                    df_nulls,
                );
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

/// Returns `true` when `dt` (a DataFusion-side schema type) requires actual
/// per-row conversion before it can be handed to the workspace-arrow side.
///
/// The three non-identity mappings in `data_type_df_to_ws` are:
///   • `LargeUtf8`  → `Utf8`   (different offsets width + layout)
///   • `Utf8View`   → `Utf8`   (view-format buffer layout)
///   • `BinaryView` → `Binary` (view-format buffer layout)
///
/// Everything else is structurally identical between the two "sides" (which
/// resolve to the same arrow-array 58.x crate, so the types are literally the
/// same Rust type).  Nested types are checked recursively.
fn dtype_needs_conversion(dt: &df_schema::DataType) -> bool {
    match dt {
        df_schema::DataType::LargeUtf8
        | df_schema::DataType::Utf8View
        | df_schema::DataType::BinaryView => true,
        df_schema::DataType::List(child)
        | df_schema::DataType::LargeList(child)
        | df_schema::DataType::FixedSizeList(child, _) => dtype_needs_conversion(child.data_type()),
        _ => false,
    }
}

/// Returns `true` when any column in `batch` requires conversion.
fn batch_needs_conversion(batch: &df_array::RecordBatch) -> bool {
    batch
        .schema()
        .fields()
        .iter()
        .any(|f| dtype_needs_conversion(f.data_type()))
}

/// Translate one DataFusion-side `RecordBatch` into a workspace-side
/// `RecordBatch`. Since both arrow crates store the same physical layouts and
/// the PoC only exercises a handful of scalar types, we walk arrays
/// element-by-element rather than touching the FFI layer.
///
/// **Zero-copy fast path**: `datafusion::arrow` is a re-export of the same
/// `arrow-array 58.x` crate the workspace uses, so `df_array::RecordBatch` and
/// `ws_array::RecordBatch` are literally the same Rust type.  When no column
/// requires layout conversion (`LargeUtf8`/`Utf8View`/`BinaryView`), the batch
/// is returned as-is with only an `Arc` clone of the schema pointer — no
/// per-row allocations.
pub(crate) fn batch_df_to_ws(batch: &df_array::RecordBatch) -> Result<ws_array::RecordBatch> {
    // ── Zero-copy fast path ───────────────────────────────────────────────────
    // If no column needs layout conversion the batch is already the right type.
    // Clone just bumps Arc reference counts — no Arrow buffer is copied.
    if !batch_needs_conversion(batch) {
        return Ok(batch.clone());
    }
    // ── Slow path: at least one column needs re-materialising ─────────────────
    let src_schema = batch.schema(); // keep Arc alive for the loop lifetime
    let target_schema = Arc::new(schema_df_to_ws(src_schema.as_ref())?);
    let mut columns: Vec<Arc<dyn ws_array::Array>> = Vec::with_capacity(batch.num_columns());
    for (i, field) in target_schema.fields().iter().enumerate() {
        let src = batch.column(i);
        // Per-column identity pass-through: if the source column's DataFusion
        // type maps 1-to-1 to the workspace type (no layout conversion needed),
        // clone the Arc reference — zero data copies.
        if !dtype_needs_conversion(src_schema.field(i).data_type()) {
            columns.push(src.clone());
            continue;
        }
        let dst: Arc<dyn ws_array::Array> = match field.data_type() {
            ws_schema::DataType::Null => Arc::new(ws_array::NullArray::new(src.len())),
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
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected Int16Array for {}", field.name()))
                    })?;
                let vals: Vec<Option<i16>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(ws_array::Int16Array::from(vals))
            }
            ws_schema::DataType::Int32 => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::Int32Array>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected Int32Array for {}", field.name()))
                    })?;
                let vals: Vec<Option<i32>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(ws_array::Int32Array::from(vals))
            }
            ws_schema::DataType::Int64 => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::Int64Array>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected Int64Array for {}", field.name()))
                    })?;
                let vals: Vec<Option<i64>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(ws_array::Int64Array::from(vals))
            }
            ws_schema::DataType::Date32 => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::Date32Array>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected Date32Array for {}", field.name()))
                    })?;
                let vals: Vec<Option<i32>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(ws_array::Date32Array::from(vals))
            }
            ws_schema::DataType::Utf8 => {
                // src may be StringArray (Utf8), LargeStringArray (LargeUtf8,
                // mapped to Utf8 in data_type_df_to_ws for string_agg output),
                // or StringViewArray (Utf8View, emitted by DataFusion 53 for
                // functions like initcap/substr/regexp_replace — Bug #40 fix).
                if let Some(s) = src.as_any().downcast_ref::<df_array::LargeStringArray>() {
                    let vals: Vec<Option<String>> = (0..s.len())
                        .map(|j| {
                            if s.is_null(j) {
                                None
                            } else {
                                Some(s.value(j).to_string())
                            }
                        })
                        .collect();
                    Arc::new(ws_array::StringArray::from(vals))
                } else if let Some(s) = src.as_any().downcast_ref::<df_array::StringViewArray>() {
                    let vals: Vec<Option<String>> = (0..s.len())
                        .map(|j| {
                            if s.is_null(j) {
                                None
                            } else {
                                Some(s.value(j).to_string())
                            }
                        })
                        .collect();
                    Arc::new(ws_array::StringArray::from(vals))
                } else {
                    let s = src
                        .as_any()
                        .downcast_ref::<df_array::StringArray>()
                        .ok_or_else(|| {
                            BasinError::internal(format!(
                                "expected StringArray for {}",
                                field.name()
                            ))
                        })?;
                    let vals: Vec<Option<String>> = (0..s.len())
                        .map(|j| {
                            if s.is_null(j) {
                                None
                            } else {
                                Some(s.value(j).to_string())
                            }
                        })
                        .collect();
                    Arc::new(ws_array::StringArray::from(vals))
                }
            }
            ws_schema::DataType::Boolean => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::BooleanArray>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected BooleanArray for {}", field.name()))
                    })?;
                let vals: Vec<Option<bool>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(ws_array::BooleanArray::from(vals))
            }
            ws_schema::DataType::Float64 => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::Float64Array>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected Float64Array for {}", field.name()))
                    })?;
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
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected Float32Array for {}", field.name()))
                    })?;
                let vals: Vec<Option<f32>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(ws_array::Float32Array::from(vals))
            }
            ws_schema::DataType::Binary => {
                // src may be BinaryArray (Binary) or BinaryViewArray (BinaryView,
                // analogous to the Utf8View → Utf8 mapping added for Bug #40).
                if let Some(s) = src.as_any().downcast_ref::<df_array::BinaryViewArray>() {
                    let vals: Vec<Option<Vec<u8>>> = (0..s.len())
                        .map(|j| {
                            if s.is_null(j) {
                                None
                            } else {
                                Some(s.value(j).to_vec())
                            }
                        })
                        .collect();
                    let refs: Vec<Option<&[u8]>> = vals.iter().map(|v| v.as_deref()).collect();
                    Arc::new(ws_array::BinaryArray::from(refs))
                } else {
                    let s = src
                        .as_any()
                        .downcast_ref::<df_array::BinaryArray>()
                        .ok_or_else(|| {
                            BasinError::internal(format!(
                                "expected BinaryArray for {}",
                                field.name()
                            ))
                        })?;
                    let vals: Vec<Option<&[u8]>> = (0..s.len())
                        .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                        .collect();
                    Arc::new(ws_array::BinaryArray::from(vals))
                }
            }
            ws_schema::DataType::LargeBinary => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::LargeBinaryArray>()
                    .ok_or_else(|| {
                        BasinError::internal(format!(
                            "expected LargeBinaryArray for {}",
                            field.name()
                        ))
                    })?;
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
                    TimestampMicrosecondType as WsMicros, TimestampMillisecondType as WsMilli,
                    TimestampNanosecondType as WsNanos, TimestampSecondType as WsSec,
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
                let dt = ws_schema::DataType::Timestamp(*unit, tz.clone());
                let arr: Arc<dyn ws_array::Array> = match unit {
                    WsTimeUnit::Microsecond => Arc::new(
                        ws_array::PrimitiveArray::<WsMicros>::from(vals).with_data_type(dt),
                    ),
                    WsTimeUnit::Millisecond => {
                        Arc::new(ws_array::PrimitiveArray::<WsMilli>::from(vals).with_data_type(dt))
                    }
                    WsTimeUnit::Nanosecond => {
                        Arc::new(ws_array::PrimitiveArray::<WsNanos>::from(vals).with_data_type(dt))
                    }
                    WsTimeUnit::Second => {
                        Arc::new(ws_array::PrimitiveArray::<WsSec>::from(vals).with_data_type(dt))
                    }
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
                    .ok_or_else(|| {
                        BasinError::internal(format!(
                            "expected Decimal128Array for {}",
                            field.name()
                        ))
                    })?;
                let vals: Vec<Option<i128>> = (0..src_arr.len())
                    .map(|j| {
                        if src_arr.is_null(j) {
                            None
                        } else {
                            Some(src_arr.value(j))
                        }
                    })
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
            // Variable-length list (1-D or N-D arrays). We recursively
            // translate the child values array and rebuild the ListArray on
            // the workspace side with the same offsets + null bitmap.
            ws_schema::DataType::List(ws_child_field) => {
                let df_list = src
                    .as_any()
                    .downcast_ref::<df_array::ListArray>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected ListArray for {}", field.name()))
                    })?;

                // Translate the flat child values array recursively.
                // Build a 1-column RecordBatch for the child, translate it,
                // then extract the column back out.
                let child_df_schema =
                    Arc::new(df_schema::Schema::new(vec![df_schema::Field::new(
                        ws_child_field.name(),
                        df_list.values().data_type().clone(),
                        ws_child_field.is_nullable(),
                    )]));
                let child_batch =
                    df_array::RecordBatch::try_new(child_df_schema, vec![df_list.values().clone()])
                        .map_err(|e| BasinError::internal(format!("list child batch: {e}")))?;
                let translated_child_batch = batch_df_to_ws(&child_batch)?;
                let ws_child_values = translated_child_batch.column(0).clone();

                // Rebuild the ListArray on the workspace side reusing the
                // same offsets and null bitmap.
                let offsets: Vec<i32> = (0..=df_list.len())
                    .map(|k| df_list.value_offsets()[k])
                    .collect();
                let ws_child_field_arc: Arc<ws_schema::Field> = Arc::new(ws_schema::Field::new(
                    ws_child_field.name(),
                    ws_child_values.data_type().clone(),
                    ws_child_field.is_nullable(),
                ));
                let null_buffer: Option<WsNullBuffer> = if df_list.nulls().is_some() {
                    let mut b = WsBooleanBufferBuilder::new(df_list.len());
                    for k in 0..df_list.len() {
                        b.append(!df_list.is_null(k));
                    }
                    Some(WsNullBuffer::new(b.finish()))
                } else {
                    None
                };
                let ws_list = ws_array::ListArray::new(
                    ws_child_field_arc,
                    WsOffsetBuffer::new(ScalarBuffer::from(offsets)),
                    ws_child_values,
                    null_buffer,
                );
                Arc::new(ws_list)
            }
            ws_schema::DataType::UInt64 => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::UInt64Array>()
                    .ok_or_else(|| {
                        BasinError::internal(format!("expected UInt64Array for {}", field.name()))
                    })?;
                let vals: Vec<Option<u64>> = (0..s.len())
                    .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                    .collect();
                Arc::new(ws_array::UInt64Array::from(vals))
            }
            ws_schema::DataType::Duration(unit) => {
                use arrow_array::types::{
                    DurationMicrosecondType as WsDurMicros, DurationMillisecondType as WsDurMilli,
                    DurationNanosecondType as WsDurNanos, DurationSecondType as WsDurSec,
                };
                use datafusion::arrow::array::DurationMicrosecondArray as DfDurMicros;
                use datafusion::arrow::array::DurationMillisecondArray as DfDurMilli;
                use datafusion::arrow::array::DurationNanosecondArray as DfDurNanos;
                use datafusion::arrow::array::DurationSecondArray as DfDurSec;
                let vals: Vec<Option<i64>> = match unit {
                    WsTimeUnit::Second => {
                        let s = src.as_any().downcast_ref::<DfDurSec>().ok_or_else(|| {
                            BasinError::internal(format!(
                                "expected DurationSecondArray for {}",
                                field.name()
                            ))
                        })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    WsTimeUnit::Millisecond => {
                        let s = src.as_any().downcast_ref::<DfDurMilli>().ok_or_else(|| {
                            BasinError::internal(format!(
                                "expected DurationMillisecondArray for {}",
                                field.name()
                            ))
                        })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    WsTimeUnit::Microsecond => {
                        let s = src.as_any().downcast_ref::<DfDurMicros>().ok_or_else(|| {
                            BasinError::internal(format!(
                                "expected DurationMicrosecondArray for {}",
                                field.name()
                            ))
                        })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    WsTimeUnit::Nanosecond => {
                        let s = src.as_any().downcast_ref::<DfDurNanos>().ok_or_else(|| {
                            BasinError::internal(format!(
                                "expected DurationNanosecondArray for {}",
                                field.name()
                            ))
                        })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                };
                let dt = ws_schema::DataType::Duration(*unit);
                let arr: Arc<dyn ws_array::Array> = match unit {
                    WsTimeUnit::Second => Arc::new(
                        ws_array::PrimitiveArray::<WsDurSec>::from(vals).with_data_type(dt),
                    ),
                    WsTimeUnit::Millisecond => Arc::new(
                        ws_array::PrimitiveArray::<WsDurMilli>::from(vals).with_data_type(dt),
                    ),
                    WsTimeUnit::Microsecond => Arc::new(
                        ws_array::PrimitiveArray::<WsDurMicros>::from(vals).with_data_type(dt),
                    ),
                    WsTimeUnit::Nanosecond => Arc::new(
                        ws_array::PrimitiveArray::<WsDurNanos>::from(vals).with_data_type(dt),
                    ),
                };
                arr
            }
            ws_schema::DataType::LargeList(ws_child_field) => {
                let s = src
                    .as_any()
                    .downcast_ref::<df_array::LargeListArray>()
                    .ok_or_else(|| {
                        BasinError::internal(format!(
                            "expected LargeListArray for {}",
                            field.name()
                        ))
                    })?;
                let df_child_type = data_type_ws_to_df(ws_child_field.data_type())?;
                let df_child_schema =
                    Arc::new(df_schema::Schema::new(vec![df_schema::Field::new(
                        ws_child_field.name().clone(),
                        df_child_type,
                        ws_child_field.is_nullable(),
                    )]));
                let child_df_batch =
                    df_array::RecordBatch::try_new(df_child_schema, vec![s.values().clone()])
                        .map_err(|e| {
                            BasinError::internal(format!("LargeListArray child batch: {e}"))
                        })?;
                let child_ws_batch = batch_df_to_ws(&child_df_batch)?;
                let ws_child_arr = child_ws_batch.column(0).clone();
                let raw_offsets: Vec<i64> = s.offsets().iter().copied().collect();
                let ws_offsets = WsOffsetBuffer::new(WsScalarBuffer::from(raw_offsets));
                let ws_nulls = s.nulls().map(|nb| {
                    let bools: Vec<bool> = (0..nb.len()).map(|i| nb.is_valid(i)).collect();
                    WsNullBuffer::from(bools)
                });
                let arr = ws_array::LargeListArray::new(
                    Arc::new((**ws_child_field).clone()),
                    ws_offsets,
                    ws_child_arr,
                    ws_nulls,
                );
                Arc::new(arr)
            }
            // ── Time64: time-of-day (microsecond or nanosecond precision) ──────
            // DataFusion emits Time64(Nanosecond) for `::TIME` casts. Walk the
            // underlying i64 buffer and rebuild on the workspace side. Both
            // Time64(Microsecond) and Time64(Nanosecond) ride on PrimitiveArray<i64>
            // with identical encoding; we preserve the time unit exactly so
            // downstream pgwire encoding sees the right tick scale.
            ws_schema::DataType::Time64(ws_unit) => {
                use arrow_array::types::{
                    Time64MicrosecondType as WsTime64Micros, Time64NanosecondType as WsTime64Nanos,
                };
                use datafusion::arrow::array::Time64MicrosecondArray as DfTime64Micros;
                use datafusion::arrow::array::Time64NanosecondArray as DfTime64Nanos;
                let vals: Vec<Option<i64>> = match ws_unit {
                    WsTimeUnit::Microsecond => {
                        let s = src
                            .as_any()
                            .downcast_ref::<DfTime64Micros>()
                            .ok_or_else(|| {
                                BasinError::internal(format!(
                                    "expected Time64MicrosecondArray for {}",
                                    field.name()
                                ))
                            })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    WsTimeUnit::Nanosecond => {
                        let s = src
                            .as_any()
                            .downcast_ref::<DfTime64Nanos>()
                            .ok_or_else(|| {
                                BasinError::internal(format!(
                                    "expected Time64NanosecondArray for {}",
                                    field.name()
                                ))
                            })?;
                        (0..s.len())
                            .map(|j| if s.is_null(j) { None } else { Some(s.value(j)) })
                            .collect()
                    }
                    _ => {
                        return Err(BasinError::InvalidSchema(format!(
                            "Time64 unit {:?} not supported in df→ws bridge",
                            ws_unit
                        )));
                    }
                };
                let dt = ws_schema::DataType::Time64(*ws_unit);
                let arr: Arc<dyn ws_array::Array> = match ws_unit {
                    WsTimeUnit::Microsecond => Arc::new(
                        ws_array::PrimitiveArray::<WsTime64Micros>::from(vals).with_data_type(dt),
                    ),
                    WsTimeUnit::Nanosecond => Arc::new(
                        ws_array::PrimitiveArray::<WsTime64Nanos>::from(vals).with_data_type(dt),
                    ),
                    _ => unreachable!("filtered above"),
                };
                arr
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

// ─── Unit tests ──────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    // Helper: build a WS schema + batch with one column of the given type.
    fn ws_batch_one_col(
        name: &str,
        dt: DataType,
        arr: Arc<dyn arrow_array::Array>,
    ) -> ws_array::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, dt, true)]));
        ws_array::RecordBatch::try_new(schema, vec![arr]).expect("ws batch")
    }

    // ── schema conversion: UInt64 ─────────────────────────────────────────────

    #[test]
    fn test_schema_uint64_roundtrip() {
        let ws_schema = ws_schema::Schema::new(vec![ws_schema::Field::new(
            "n",
            ws_schema::DataType::UInt64,
            true,
        )]);
        let df_schema = schema_ws_to_df(&ws_schema).expect("ws→df");
        assert_eq!(
            df_schema.field(0).data_type(),
            &datafusion::arrow::datatypes::DataType::UInt64
        );
        let ws2 = schema_df_to_ws(&df_schema).expect("df→ws");
        assert_eq!(ws2.field(0).data_type(), &ws_schema::DataType::UInt64);
    }

    // ── schema conversion: Duration(Second) ──────────────────────────────────

    #[test]
    fn test_schema_duration_roundtrip() {
        let ws_schema = ws_schema::Schema::new(vec![ws_schema::Field::new(
            "d",
            ws_schema::DataType::Duration(ws_schema::TimeUnit::Second),
            true,
        )]);
        let df_schema = schema_ws_to_df(&ws_schema).expect("ws→df");
        assert_eq!(
            df_schema.field(0).data_type(),
            &datafusion::arrow::datatypes::DataType::Duration(
                datafusion::arrow::datatypes::TimeUnit::Second
            )
        );
        let ws2 = schema_df_to_ws(&df_schema).expect("df→ws");
        assert_eq!(
            ws2.field(0).data_type(),
            &ws_schema::DataType::Duration(ws_schema::TimeUnit::Second)
        );
    }

    // ── schema conversion: List<Int32> ────────────────────────────────────────

    #[test]
    fn test_schema_list_int32_roundtrip() {
        let inner = Arc::new(ws_schema::Field::new(
            "item",
            ws_schema::DataType::Int32,
            true,
        ));
        let ws_schema = ws_schema::Schema::new(vec![ws_schema::Field::new(
            "arr",
            ws_schema::DataType::List(inner),
            true,
        )]);
        let df_schema = schema_ws_to_df(&ws_schema).expect("ws→df");
        let ws2 = schema_df_to_ws(&df_schema).expect("df→ws");
        assert_eq!(ws_schema, ws2);
    }

    // ── schema conversion: List<Utf8> ─────────────────────────────────────────

    #[test]
    fn test_schema_list_utf8_roundtrip() {
        let inner = Arc::new(ws_schema::Field::new(
            "item",
            ws_schema::DataType::Utf8,
            true,
        ));
        let ws_schema = ws_schema::Schema::new(vec![ws_schema::Field::new(
            "tags",
            ws_schema::DataType::List(inner),
            true,
        )]);
        let df_schema = schema_ws_to_df(&ws_schema).expect("ws→df");
        let ws2 = schema_df_to_ws(&df_schema).expect("df→ws");
        assert_eq!(ws_schema, ws2);
    }

    // ── batch roundtrip: UInt64 ───────────────────────────────────────────────

    #[test]
    fn test_batch_uint64_roundtrip() {
        let vals: Vec<Option<u64>> = vec![Some(0), Some(u64::MAX), None, Some(42)];
        let arr = Arc::new(ws_array::UInt64Array::from(vals.clone()));
        let batch = ws_batch_one_col("n", ws_schema::DataType::UInt64, arr);

        let df_batch = batch_ws_to_df(&batch).expect("ws→df");
        let ws_batch2 = batch_df_to_ws(&df_batch).expect("df→ws");

        let out = ws_batch2
            .column(0)
            .as_any()
            .downcast_ref::<ws_array::UInt64Array>()
            .expect("UInt64Array");
        assert_eq!(out.len(), 4);
        assert!(out.is_null(2));
        assert_eq!(out.value(0), 0);
        assert_eq!(out.value(1), u64::MAX);
        assert_eq!(out.value(3), 42);
    }

    // ── batch roundtrip: Duration(Second) ────────────────────────────────────

    #[test]
    fn test_batch_duration_second_roundtrip() {
        let vals: Vec<Option<i64>> = vec![Some(86400), None, Some(-3600)];
        let arr: Arc<dyn ws_array::Array> = Arc::new(ws_array::DurationSecondArray::from(vals));
        let batch = ws_batch_one_col(
            "d",
            ws_schema::DataType::Duration(ws_schema::TimeUnit::Second),
            arr,
        );

        let df_batch = batch_ws_to_df(&batch).expect("ws→df");
        let ws_batch2 = batch_df_to_ws(&df_batch).expect("df→ws");

        let out = ws_batch2
            .column(0)
            .as_any()
            .downcast_ref::<ws_array::DurationSecondArray>()
            .expect("DurationSecondArray");
        assert_eq!(out.len(), 3);
        assert!(out.is_null(1));
        assert_eq!(out.value(0), 86400);
        assert_eq!(out.value(2), -3600);
    }

    // ── batch roundtrip: List<Int32> ─────────────────────────────────────────

    #[test]
    fn test_batch_list_int32_roundtrip() {
        use arrow_array::builder::{Int32Builder, ListBuilder};
        let mut builder = ListBuilder::new(Int32Builder::new());
        // row 0: [1, 2, 3]
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.values().append_value(3);
        builder.append(true);
        // row 1: null list
        builder.append(false);
        // row 2: [42]
        builder.values().append_value(42);
        builder.append(true);
        let arr = Arc::new(builder.finish());

        let inner = Arc::new(ws_schema::Field::new(
            "item",
            ws_schema::DataType::Int32,
            true,
        ));
        let batch = ws_batch_one_col("arr", ws_schema::DataType::List(inner), arr);

        let df_batch = batch_ws_to_df(&batch).expect("ws→df");
        let ws_batch2 = batch_df_to_ws(&df_batch).expect("df→ws");

        let out = ws_batch2
            .column(0)
            .as_any()
            .downcast_ref::<ws_array::ListArray>()
            .expect("ListArray");
        assert_eq!(out.len(), 3);
        assert!(out.is_null(1));
        let row0 = out.value(0);
        let row0_ints = row0.as_any().downcast_ref::<Int32Array>().expect("i32");
        assert_eq!(row0_ints.values(), &[1, 2, 3]);
        let row2 = out.value(2);
        let row2_ints = row2.as_any().downcast_ref::<Int32Array>().expect("i32");
        assert_eq!(row2_ints.values(), &[42]);
    }

    // ── batch roundtrip: List<Utf8> ──────────────────────────────────────────

    #[test]
    fn test_batch_list_utf8_roundtrip() {
        use arrow_array::builder::{ListBuilder, StringBuilder};
        let mut builder = ListBuilder::new(StringBuilder::new());
        // row 0: ["a", "b"]
        builder.values().append_value("a");
        builder.values().append_value("b");
        builder.append(true);
        // row 1: []
        builder.append(true);
        // row 2: ["hello"]
        builder.values().append_value("hello");
        builder.append(true);
        let arr = Arc::new(builder.finish());

        let inner = Arc::new(ws_schema::Field::new(
            "item",
            ws_schema::DataType::Utf8,
            true,
        ));
        let batch = ws_batch_one_col("tags", ws_schema::DataType::List(inner), arr);

        let df_batch = batch_ws_to_df(&batch).expect("ws→df");
        let ws_batch2 = batch_df_to_ws(&df_batch).expect("df→ws");

        let out = ws_batch2
            .column(0)
            .as_any()
            .downcast_ref::<ws_array::ListArray>()
            .expect("ListArray");
        assert_eq!(out.len(), 3);
        let row0 = out.value(0);
        let row0_strs = row0.as_any().downcast_ref::<StringArray>().expect("str");
        assert_eq!(row0_strs.value(0), "a");
        assert_eq!(row0_strs.value(1), "b");
        let row2 = out.value(2);
        let row2_strs = row2.as_any().downcast_ref::<StringArray>().expect("str");
        assert_eq!(row2_strs.value(0), "hello");
    }

    // ── batch roundtrip: df→ws direction for UInt64 ───────────────────────────
    // (Most tests above go ws→df→ws; this one starts from the df side.)

    #[test]
    fn test_batch_df_to_ws_uint64() {
        use datafusion::arrow::array::UInt64Array as DfUInt64Array;
        use datafusion::arrow::datatypes::{
            DataType as DfDataType, Field as DfField, Schema as DfSchema,
        };

        let df_schema = Arc::new(DfSchema::new(vec![DfField::new(
            "n",
            DfDataType::UInt64,
            true,
        )]));
        let vals: Vec<Option<u64>> = vec![Some(1), Some(2), None];
        let arr = Arc::new(DfUInt64Array::from(vals));
        let df_batch = datafusion::arrow::array::RecordBatch::try_new(df_schema, vec![arr as _])
            .expect("df batch");

        let ws_batch = batch_df_to_ws(&df_batch).expect("df→ws");
        let out = ws_batch
            .column(0)
            .as_any()
            .downcast_ref::<ws_array::UInt64Array>()
            .expect("UInt64Array");
        assert_eq!(out.len(), 3);
        assert!(out.is_null(2));
        assert_eq!(out.value(0), 1);
        assert_eq!(out.value(1), 2);
    }

    // ── batch roundtrip: df→ws direction for Duration(Second) ─────────────────

    #[test]
    fn test_batch_df_to_ws_duration_second() {
        use datafusion::arrow::array::{
            types::DurationSecondType, PrimitiveArray as DfPrimArray, RecordBatch as DfRecordBatch,
        };
        use datafusion::arrow::datatypes::{
            DataType as DfDataType, Field as DfField, Schema as DfSchema, TimeUnit as DfTimeUnit,
        };

        let df_schema = Arc::new(DfSchema::new(vec![DfField::new(
            "d",
            DfDataType::Duration(DfTimeUnit::Second),
            true,
        )]));
        let vals: Vec<Option<i64>> = vec![Some(3600), None, Some(-60)];
        let arr: Arc<dyn datafusion::arrow::array::Array> = Arc::new(
            DfPrimArray::<DurationSecondType>::from(vals)
                .with_data_type(DfDataType::Duration(DfTimeUnit::Second)),
        );
        let df_batch = DfRecordBatch::try_new(df_schema, vec![arr]).expect("df batch");

        let ws_batch = batch_df_to_ws(&df_batch).expect("df→ws");
        let out = ws_batch
            .column(0)
            .as_any()
            .downcast_ref::<ws_array::DurationSecondArray>()
            .expect("DurationSecondArray");
        assert_eq!(out.len(), 3);
        assert!(out.is_null(1));
        assert_eq!(out.value(0), 3600);
        assert_eq!(out.value(2), -60);
    }

    // ── Bug #40 fix: Utf8View (StringViewArray) → workspace Utf8 ─────────────
    // DataFusion 53 emits StringViewArray for certain string functions (e.g.
    // initcap, substr). data_type_df_to_ws now maps Utf8View → Utf8, and
    // batch_df_to_ws downcasts StringViewArray in the Utf8 arm.

    #[test]
    fn test_batch_df_to_ws_utf8view() {
        use datafusion::arrow::array::{
            RecordBatch as DfRecordBatch, StringViewArray as DfStringViewArray,
        };
        use datafusion::arrow::datatypes::{
            DataType as DfDataType, Field as DfField, Schema as DfSchema,
        };

        let df_schema = Arc::new(DfSchema::new(vec![DfField::new(
            "s",
            DfDataType::Utf8View,
            true,
        )]));
        let arr: Arc<dyn datafusion::arrow::array::Array> =
            Arc::new(DfStringViewArray::from(vec![
                Some("hello"),
                None,
                Some("world"),
            ]));
        let df_batch = DfRecordBatch::try_new(df_schema, vec![arr]).expect("df batch");

        let ws_batch = batch_df_to_ws(&df_batch).expect("df→ws Utf8View should succeed");
        assert_eq!(
            ws_batch.schema().field(0).data_type(),
            &ws_schema::DataType::Utf8
        );
        let out = ws_batch
            .column(0)
            .as_any()
            .downcast_ref::<ws_array::StringArray>()
            .expect("StringArray");
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), "hello");
        assert!(out.is_null(1));
        assert_eq!(out.value(2), "world");
    }

    // ── Bug #40 fix: BinaryView (BinaryViewArray) → workspace Binary ──────────

    #[test]
    fn test_batch_df_to_ws_binaryview() {
        use datafusion::arrow::array::{
            BinaryViewArray as DfBinaryViewArray, RecordBatch as DfRecordBatch,
        };
        use datafusion::arrow::datatypes::{
            DataType as DfDataType, Field as DfField, Schema as DfSchema,
        };

        let df_schema = Arc::new(DfSchema::new(vec![DfField::new(
            "b",
            DfDataType::BinaryView,
            true,
        )]));
        let arr: Arc<dyn datafusion::arrow::array::Array> =
            Arc::new(DfBinaryViewArray::from(vec![
                Some(b"foo" as &[u8]),
                None,
                Some(b"bar"),
            ]));
        let df_batch = DfRecordBatch::try_new(df_schema, vec![arr]).expect("df batch");

        let ws_batch = batch_df_to_ws(&df_batch).expect("df→ws BinaryView should succeed");
        assert_eq!(
            ws_batch.schema().field(0).data_type(),
            &ws_schema::DataType::Binary
        );
        let out = ws_batch
            .column(0)
            .as_any()
            .downcast_ref::<ws_array::BinaryArray>()
            .expect("BinaryArray");
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), b"foo");
        assert!(out.is_null(1));
        assert_eq!(out.value(2), b"bar");
    }
}
