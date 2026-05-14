//! PostgreSQL JSON aggregate UDAFs.
//!
//! Implements `json_agg`, `jsonb_agg`, `json_object_agg`, and
//! `jsonb_object_agg` as DataFusion UDAFs.  These accumulate column values
//! during aggregation and emit a JSON string.
//!
//! - `json_agg(col)` / `jsonb_agg(col)` → JSON array of all non-null values
//! - `json_object_agg(key, value)` / `jsonb_object_agg(key, value)` → JSON
//!   object mapping each key to the corresponding value
//!
//! The output type is `Utf8` (TEXT) — Basin stores both JSON and JSONB as
//! UTF-8 text internally.  The PG wire-level difference (OID 114 vs 3802) is
//! handled at the pgwire layer, not here.

use std::any::Any;

use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{exec_err, Result as DFResult};
use datafusion::logical_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    AggregateUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use serde_json::Value as JValue;

// ── json_agg ──────────────────────────────────────────────────────────────────

/// UDAF that accumulates any column into a JSON array string.
/// Used for both `json_agg` and `jsonb_agg`.
#[derive(Debug, Clone)]
pub(crate) struct JsonAggUdaf {
    pub name: &'static str,
    signature: Signature,
}

impl JsonAggUdaf {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            // Accept any single argument type.
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for JsonAggUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> DFResult<Box<dyn datafusion::logical_expr::Accumulator>> {
        Ok(Box::new(JsonAggAccumulator { values: vec![] }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<Field>> {
        Ok(vec![Field::new(
            format!("{}_state", args.name),
            DataType::Utf8,
            true,
        )])
    }
}

#[derive(Debug, Default)]
struct JsonAggAccumulator {
    values: Vec<JValue>,
}

impl datafusion::logical_expr::Accumulator for JsonAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let arr = &values[0];
        for i in 0..arr.len() {
            if arr.is_null(i) {
                self.values.push(JValue::Null);
            } else {
                self.values.push(arrow_scalar_to_json(arr.as_ref(), i));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let arr = JValue::Array(self.values.clone());
        Ok(ScalarValue::Utf8(Some(arr.to_string())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.len() * 64
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let arr = JValue::Array(self.values.clone());
        Ok(vec![ScalarValue::Utf8(Some(arr.to_string()))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        // Each state is a JSON array string; parse and extend.
        if states.is_empty() {
            return Ok(());
        }
        let Some(arr) = states[0].as_any().downcast_ref::<StringArray>() else {
            return Ok(());
        };
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let s = arr.value(i);
            if let Ok(JValue::Array(inner)) = serde_json::from_str::<JValue>(s) {
                self.values.extend(inner);
            }
        }
        Ok(())
    }
}

// ── json_object_agg ───────────────────────────────────────────────────────────

/// UDAF that accumulates (key, value) pairs into a JSON object string.
/// Used for both `json_object_agg` and `jsonb_object_agg`.
#[derive(Debug, Clone)]
pub(crate) struct JsonObjectAggUdaf {
    pub name: &'static str,
    signature: Signature,
}

impl JsonObjectAggUdaf {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            // Two arguments: key (must be text-able) and value (any).
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for JsonObjectAggUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> DFResult<Box<dyn datafusion::logical_expr::Accumulator>> {
        Ok(Box::new(JsonObjectAggAccumulator {
            pairs: vec![],
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<Field>> {
        Ok(vec![Field::new(
            format!("{}_state", args.name),
            DataType::Utf8,
            true,
        )])
    }
}

#[derive(Debug, Default)]
struct JsonObjectAggAccumulator {
    pairs: Vec<(String, JValue)>,
}

impl datafusion::logical_expr::Accumulator for JsonObjectAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        if values.len() < 2 {
            return exec_err!("json_object_agg requires 2 arguments");
        }
        let keys = &values[0];
        let vals = &values[1];
        for i in 0..keys.len() {
            if keys.is_null(i) {
                return exec_err!("json_object_agg: key must not be NULL");
            }
            let k = arrow_scalar_to_string(keys.as_ref(), i);
            let v = if vals.is_null(i) {
                JValue::Null
            } else {
                arrow_scalar_to_json(vals.as_ref(), i)
            };
            self.pairs.push((k, v));
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let obj: serde_json::Map<String, JValue> = self.pairs.iter().cloned().collect();
        Ok(ScalarValue::Utf8(Some(JValue::Object(obj).to_string())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.pairs.len() * 128
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let obj: serde_json::Map<String, JValue> = self.pairs.iter().cloned().collect();
        Ok(vec![ScalarValue::Utf8(Some(
            JValue::Object(obj).to_string(),
        ))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states.is_empty() {
            return Ok(());
        }
        let Some(arr) = states[0].as_any().downcast_ref::<StringArray>() else {
            return Ok(());
        };
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let s = arr.value(i);
            if let Ok(JValue::Object(map)) = serde_json::from_str::<JValue>(s) {
                self.pairs.extend(map.into_iter());
            }
        }
        Ok(())
    }
}

// ── helper: Arrow scalar → serde_json::Value ──────────────────────────────────

fn arrow_scalar_to_json(arr: &dyn Array, i: usize) -> JValue {
    use datafusion::arrow::array::*;
    match arr.data_type() {
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| JValue::String(a.value(i).to_string()))
            .unwrap_or(JValue::Null),
        DataType::LargeUtf8 => arr
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| JValue::String(a.value(i).to_string()))
            .unwrap_or(JValue::Null),
        DataType::Boolean => arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| JValue::Bool(a.value(i)))
            .unwrap_or(JValue::Null),
        DataType::Int8 => arr
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|a| JValue::Number(a.value(i).into()))
            .unwrap_or(JValue::Null),
        DataType::Int16 => arr
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|a| JValue::Number(a.value(i).into()))
            .unwrap_or(JValue::Null),
        DataType::Int32 => arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| JValue::Number(a.value(i).into()))
            .unwrap_or(JValue::Null),
        DataType::Int64 => arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| JValue::Number(a.value(i).into()))
            .unwrap_or(JValue::Null),
        DataType::UInt8 => arr
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|a| JValue::Number(a.value(i).into()))
            .unwrap_or(JValue::Null),
        DataType::UInt16 => arr
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|a| JValue::Number(a.value(i).into()))
            .unwrap_or(JValue::Null),
        DataType::UInt32 => arr
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| JValue::Number(a.value(i).into()))
            .unwrap_or(JValue::Null),
        DataType::UInt64 => arr
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| JValue::Number(a.value(i).into()))
            .unwrap_or(JValue::Null),
        DataType::Float32 => arr
            .as_any()
            .downcast_ref::<Float32Array>()
            .and_then(|a| {
                serde_json::Number::from_f64(a.value(i) as f64).map(JValue::Number)
            })
            .unwrap_or(JValue::Null),
        DataType::Float64 => arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .and_then(|a| {
                serde_json::Number::from_f64(a.value(i)).map(JValue::Number)
            })
            .unwrap_or(JValue::Null),
        _ => {
            // Fallback: render as a string using Display-style
            JValue::String(format!("{:?}[{i}]", arr.data_type()))
        }
    }
}

fn arrow_scalar_to_string(arr: &dyn Array, i: usize) -> String {
    use datafusion::arrow::array::*;
    match arr.data_type() {
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(i).to_string())
            .unwrap_or_default(),
        DataType::LargeUtf8 => arr
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| a.value(i).to_string())
            .unwrap_or_default(),
        DataType::Int32 => arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(i).to_string())
            .unwrap_or_default(),
        DataType::Int64 => arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(i).to_string())
            .unwrap_or_default(),
        _ => format!("{:?}[{i}]", arr.data_type()),
    }
}

// ── registration helper ───────────────────────────────────────────────────────

use datafusion::logical_expr::AggregateUDF;
use datafusion::prelude::SessionContext;

/// Register all PG JSON aggregate UDAFs on `ctx`.
///
/// Registers four functions:
/// - `json_agg(col)`
/// - `jsonb_agg(col)`
/// - `json_object_agg(key, value)`
/// - `jsonb_object_agg(key, value)`
pub(crate) fn register_json_agg_udafs(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(JsonAggUdaf::new("json_agg")));
    ctx.register_udaf(AggregateUDF::from(JsonAggUdaf::new("jsonb_agg")));
    ctx.register_udaf(AggregateUDF::from(JsonObjectAggUdaf::new(
        "json_object_agg",
    )));
    ctx.register_udaf(AggregateUDF::from(JsonObjectAggUdaf::new(
        "jsonb_object_agg",
    )));
}
