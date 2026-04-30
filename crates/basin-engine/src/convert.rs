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
use arrow_schema as ws_schema;

// DataFusion-side (v53) imports.
use datafusion::arrow::array as df_array;
use datafusion::arrow::array::Array as _;
use datafusion::arrow::datatypes as df_schema;

/// Build a DataFusion-side `Schema` from a workspace-side `Schema`.
pub(crate) fn schema_ws_to_df(s: &ws_schema::Schema) -> Result<df_schema::Schema> {
    let mut fields = Vec::with_capacity(s.fields().len());
    for f in s.fields() {
        fields.push(df_schema::Field::new(
            f.name().clone(),
            data_type_ws_to_df(f.data_type())?,
            f.is_nullable(),
        ));
    }
    Ok(df_schema::Schema::new(fields))
}

/// Build a workspace-side `Schema` from a DataFusion-side `Schema`.
pub(crate) fn schema_df_to_ws(s: &df_schema::Schema) -> Result<ws_schema::Schema> {
    let mut fields = Vec::with_capacity(s.fields().len());
    for f in s.fields() {
        fields.push(ws_schema::Field::new(
            f.name().clone(),
            data_type_df_to_ws(f.data_type())?,
            f.is_nullable(),
        ));
    }
    Ok(ws_schema::Schema::new(fields))
}

fn data_type_ws_to_df(dt: &ws_schema::DataType) -> Result<df_schema::DataType> {
    Ok(match dt {
        ws_schema::DataType::Int64 => df_schema::DataType::Int64,
        ws_schema::DataType::Utf8 => df_schema::DataType::Utf8,
        ws_schema::DataType::Boolean => df_schema::DataType::Boolean,
        ws_schema::DataType::Float64 => df_schema::DataType::Float64,
        ws_schema::DataType::Float32 => df_schema::DataType::Float32,
        ws_schema::DataType::Binary => df_schema::DataType::Binary,
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
        df_schema::DataType::Int64 => ws_schema::DataType::Int64,
        df_schema::DataType::Utf8 => ws_schema::DataType::Utf8,
        df_schema::DataType::Boolean => ws_schema::DataType::Boolean,
        df_schema::DataType::Float64 => ws_schema::DataType::Float64,
        df_schema::DataType::Float32 => ws_schema::DataType::Float32,
        df_schema::DataType::Binary => ws_schema::DataType::Binary,
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
