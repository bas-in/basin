//! PostgreSQL aggregate UDAFs.
//!
//! Implements JSON aggregates, ordered-set aggregates, and related UDAFs:
//!
//! ## JSON aggregates
//! - `json_agg(col)` / `jsonb_agg(col)` → JSON array of all non-null values
//! - `json_object_agg(key, value)` / `jsonb_object_agg(key, value)` → JSON
//!   object mapping each key to the corresponding value
//!
//! ## Ordered-set aggregates (exact, Postgres-compatible)
//! - `percentile_disc(f) WITHIN GROUP (ORDER BY expr)` — exact discrete
//!   percentile. Collects all non-NULL values of `expr`, sorts them, returns
//!   the value at 1-based position `k = ceil(f * N)` (clamped to [1, N]).
//!   For `f = 0` returns the minimum; for `f = 1` returns the maximum.
//!   Also supports array input: `percentile_disc(ARRAY[f1,f2,...]) WITHIN GROUP (ORDER BY expr)`.
//! - `mode() WITHIN GROUP (ORDER BY expr)` — most frequent non-NULL value of
//!   `expr`; ties broken by the first value in ascending sort order.
//!
//! Both ordered-set aggregates buffer all group values in memory (O(N) space),
//! which is the correct and unavoidable behaviour for exact computation of
//! order-dependent statistics.  They implement `state()`/`merge_batch()` so
//! DataFusion's partitioned aggregation works correctly.
//!
//! The output type is `Utf8` (TEXT) for JSON — Basin stores both JSON and JSONB
//! as UTF-8 text internally.  The PG wire-level difference (OID 114 vs 3802)
//! is handled at the pgwire layer, not here.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, LargeBinaryArray, ListArray, StringArray, StructArray,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{exec_err, plan_err, Result as DFResult};
use datafusion::logical_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    AggregateUDFImpl, ColumnarValue, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use serde_json::Value as JValue;

/// Build a `Field` with `BASIN_TYPE=JSONB` metadata so that the pgwire layer
/// emits OID 3802 (JSONB) for `json_agg` / `jsonb_agg` result columns.
/// `tokio-postgres` (and every other PG driver) uses the OID to pick the
/// right deserializer; without this marker the column is advertised as TEXT
/// and the client refuses to deserialize it as `serde_json::Value`.
fn json_agg_return_field(name: &str) -> FieldRef {
    let mut meta = HashMap::new();
    meta.insert(
        crate::types::BASIN_TYPE_KEY.to_string(),
        crate::types::BASIN_TYPE_JSONB.to_string(),
    );
    Arc::new(Field::new(name, DataType::Utf8, true).with_metadata(meta))
}

// ── json_agg ──────────────────────────────────────────────────────────────────

/// UDAF that accumulates any column into a JSON array string.
/// Used for both `json_agg` and `jsonb_agg`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

    /// Override `return_field` to attach `BASIN_TYPE=JSONB` metadata so the
    /// pgwire router advertises OID 3802 for this column, enabling clients to
    /// deserialize the result as a JSON value instead of plain text.
    fn return_field(&self, _arg_fields: &[FieldRef]) -> DFResult<FieldRef> {
        Ok(json_agg_return_field(self.name))
    }

    fn accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> DFResult<Box<dyn datafusion::logical_expr::Accumulator>> {
        Ok(Box::new(JsonAggAccumulator { values: vec![] }))
    }

    fn state_fields(
        &self,
        args: StateFieldsArgs,
    ) -> DFResult<Vec<datafusion::arrow::datatypes::FieldRef>> {
        Ok(vec![std::sync::Arc::new(Field::new(
            format!("{}_state", args.name),
            DataType::Utf8,
            true,
        ))])
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
        if self.values.is_empty() {
            // PostgreSQL json_agg returns NULL for zero rows.
            return Ok(ScalarValue::Utf8(None));
        }
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

    /// Attach `BASIN_TYPE=JSONB` metadata so the pgwire layer emits OID 3802.
    fn return_field(&self, _arg_fields: &[FieldRef]) -> DFResult<FieldRef> {
        Ok(json_agg_return_field(self.name))
    }

    fn accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> DFResult<Box<dyn datafusion::logical_expr::Accumulator>> {
        Ok(Box::new(JsonObjectAggAccumulator { pairs: vec![] }))
    }

    fn state_fields(
        &self,
        args: StateFieldsArgs,
    ) -> DFResult<Vec<datafusion::arrow::datatypes::FieldRef>> {
        Ok(vec![std::sync::Arc::new(Field::new(
            format!("{}_state", args.name),
            DataType::Utf8,
            true,
        ))])
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
            .and_then(|a| serde_json::Number::from_f64(a.value(i) as f64).map(JValue::Number))
            .unwrap_or(JValue::Null),
        DataType::Float64 => arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .and_then(|a| serde_json::Number::from_f64(a.value(i)).map(JValue::Number))
            .unwrap_or(JValue::Null),
        // Struct arrays — produced by `named_struct(...)` calls.
        // Convert each field to a JSON object key→value pair.
        DataType::Struct(_) => {
            if let Some(sa) = arr.as_any().downcast_ref::<StructArray>() {
                if sa.is_null(i) {
                    return JValue::Null;
                }
                let mut map = serde_json::Map::new();
                for col_idx in 0..sa.num_columns() {
                    let col_arr = sa.column(col_idx);
                    let col_name = sa.fields()[col_idx].name().clone();
                    let val = if col_arr.is_null(i) {
                        JValue::Null
                    } else {
                        arrow_scalar_to_json(col_arr.as_ref(), i)
                    };
                    map.insert(col_name, val);
                }
                JValue::Object(map)
            } else {
                JValue::Null
            }
        }
        // Large Binary (JSONB stored as bytes) — parse and re-emit as JSON.
        DataType::LargeBinary => {
            if let Some(a) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
                let bytes = a.value(i);
                serde_json::from_slice(bytes).unwrap_or(JValue::Null)
            } else {
                JValue::Null
            }
        }
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

// ── unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray as ArrowStringArray, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
    use datafusion::logical_expr::Accumulator;
    use std::sync::Arc;

    // ── struct→JSON conversion ──────────────────────────────────────────────

    /// `named_struct('id', 1, 'name', 'alice')` produces a StructArray; verify
    /// that `arrow_scalar_to_json` returns the correct JSON object.
    #[test]
    fn struct_to_json_basic() {
        let fields = Fields::from(vec![
            Arc::new(Field::new("id", DataType::Int32, true)),
            Arc::new(Field::new("name", DataType::Utf8, true)),
        ]);
        let id_arr: ArrayRef = Arc::new(Int32Array::from(vec![1i32, 2i32]));
        let name_arr: ArrayRef = Arc::new(ArrowStringArray::from(vec!["alice", "bob"]));
        let struct_arr = StructArray::new(fields, vec![id_arr, name_arr], None);
        let arr_ref: ArrayRef = Arc::new(struct_arr);

        let v0 = arrow_scalar_to_json(arr_ref.as_ref(), 0);
        let v1 = arrow_scalar_to_json(arr_ref.as_ref(), 1);

        // row 0: {"id":1,"name":"alice"}
        assert!(matches!(&v0, JValue::Object(m) if m.len() == 2));
        if let JValue::Object(m) = &v0 {
            assert_eq!(m["id"], JValue::Number(1.into()));
            assert_eq!(m["name"], JValue::String("alice".into()));
        }
        // row 1: {"id":2,"name":"bob"}
        if let JValue::Object(m) = &v1 {
            assert_eq!(m["id"], JValue::Number(2.into()));
            assert_eq!(m["name"], JValue::String("bob".into()));
        }
    }

    /// Struct with a null row should produce `null`.
    #[test]
    fn struct_to_json_null_row() {
        let fields = Fields::from(vec![Arc::new(Field::new("x", DataType::Int32, true))]);
        let x_arr: ArrayRef = Arc::new(Int32Array::from(vec![Some(42i32), None]));
        // Mark the first struct element as null.
        let validity = datafusion::arrow::buffer::NullBuffer::from(vec![false, true]);
        let struct_arr = StructArray::new(fields, vec![x_arr], Some(validity));
        let arr_ref: ArrayRef = Arc::new(struct_arr);

        let v0 = arrow_scalar_to_json(arr_ref.as_ref(), 0);
        assert_eq!(v0, JValue::Null, "null struct row should yield null");

        let v1 = arrow_scalar_to_json(arr_ref.as_ref(), 1);
        assert!(
            matches!(&v1, JValue::Object(_)),
            "non-null struct row should be object"
        );
    }

    // ── json_agg accumulator ────────────────────────────────────────────────

    /// `json_agg` over zero rows must return NULL (PostgreSQL-compatible).
    #[test]
    fn json_agg_empty_returns_null() {
        let mut acc = JsonAggAccumulator { values: vec![] };
        let result = acc.evaluate().unwrap();
        assert_eq!(
            result,
            ScalarValue::Utf8(None),
            "json_agg over 0 rows should be NULL"
        );
    }

    /// `json_agg` over two integer rows must produce a JSON array.
    #[test]
    fn json_agg_int_rows() {
        let mut acc = JsonAggAccumulator { values: vec![] };
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1i32, 2i32]));
        acc.update_batch(&[arr]).unwrap();
        let result = acc.evaluate().unwrap();
        match result {
            ScalarValue::Utf8(Some(s)) => {
                let v: JValue = serde_json::from_str(&s).expect("must be valid JSON");
                assert_eq!(
                    v,
                    JValue::Array(vec![JValue::Number(1.into()), JValue::Number(2.into())])
                );
            }
            other => panic!("expected Utf8(Some(...)), got {other:?}"),
        }
    }

    /// `json_agg` over struct rows (simulating `json_agg(named_struct(...))`)
    /// must produce a JSON array of objects with correct keys.
    #[test]
    fn json_agg_struct_rows() {
        let fields = Fields::from(vec![
            Arc::new(Field::new("id", DataType::Int32, true)),
            Arc::new(Field::new("val", DataType::Utf8, true)),
        ]);
        let id_arr: ArrayRef = Arc::new(Int32Array::from(vec![10i32, 20i32]));
        let val_arr: ArrayRef = Arc::new(ArrowStringArray::from(vec!["x", "y"]));
        let struct_arr: ArrayRef = Arc::new(StructArray::new(fields, vec![id_arr, val_arr], None));

        let mut acc = JsonAggAccumulator { values: vec![] };
        acc.update_batch(&[struct_arr]).unwrap();
        let result = acc.evaluate().unwrap();
        match result {
            ScalarValue::Utf8(Some(s)) => {
                let v: JValue = serde_json::from_str(&s).expect("must be valid JSON");
                let JValue::Array(arr) = v else {
                    panic!("expected array, got {s}")
                };
                assert_eq!(arr.len(), 2, "expected 2 elements");
                assert!(
                    matches!(&arr[0], JValue::Object(m) if m.len() == 2),
                    "first element should be a 2-key object, got {:?}",
                    arr[0]
                );
                assert!(
                    matches!(&arr[1], JValue::Object(m) if m.len() == 2),
                    "second element should be a 2-key object, got {:?}",
                    arr[1]
                );
                if let JValue::Object(m) = &arr[0] {
                    assert_eq!(m["id"], JValue::Number(10.into()));
                    assert_eq!(m["val"], JValue::String("x".into()));
                }
                if let JValue::Object(m) = &arr[1] {
                    assert_eq!(m["id"], JValue::Number(20.into()));
                    assert_eq!(m["val"], JValue::String("y".into()));
                }
            }
            other => panic!("expected Utf8(Some(...)), got {other:?}"),
        }
    }
}

// ── unit tests: ordered-set aggregates ────────────────────────────────────────

#[cfg(test)]
mod ordered_set_tests {
    use super::*;
    use datafusion::arrow::array::{Array, Float64Array, Int32Array, Int64Array};
    use datafusion::logical_expr::Accumulator;
    use std::sync::Arc;

    // ── percentile_disc_index formula ──────────────────────────────────────

    /// Verify the exact discrete percentile index formula against hand-computed
    /// Postgres-spec values.
    #[test]
    fn percentile_disc_index_formula() {
        // N=4: k = ceil(f*4), 0-based = k-1
        assert_eq!(percentile_disc_index(4, 0.5), 1, "ceil(0.5*4)=2 → idx 1");
        assert_eq!(percentile_disc_index(4, 0.25), 0, "ceil(0.25*4)=1 → idx 0");
        assert_eq!(percentile_disc_index(4, 0.75), 2, "ceil(0.75*4)=3 → idx 2");
        assert_eq!(percentile_disc_index(4, 1.0), 3, "ceil(1.0*4)=4 → idx 3");
        assert_eq!(percentile_disc_index(4, 0.0), 0, "f=0 → idx 0 (min)");
        // N=5: median
        assert_eq!(percentile_disc_index(5, 0.5), 2, "ceil(0.5*5)=3 → idx 2");
        // N=1: always idx 0
        assert_eq!(percentile_disc_index(1, 0.5), 0);
        assert_eq!(percentile_disc_index(1, 0.0), 0);
        assert_eq!(percentile_disc_index(1, 1.0), 0);
    }

    // ── helper: build a PercentileDiscAccumulator with given fractions ──────

    fn make_pd_acc(fractions: Vec<f64>) -> PercentileDiscAccumulator {
        PercentileDiscAccumulator {
            values: vec![],
            fractions,
            data_type: DataType::Int32,
            is_array_mode: false,
        }
    }

    fn make_pd_arr_acc(fractions: Vec<f64>) -> PercentileDiscAccumulator {
        PercentileDiscAccumulator {
            values: vec![],
            fractions,
            data_type: DataType::Int32,
            is_array_mode: true,
        }
    }

    // ── percentile_disc scalar, N=4 ────────────────────────────────────────

    /// `percentile_disc(0.5) WITHIN GROUP (ORDER BY x)` over {1,2,3,4} → 2
    /// k = ceil(0.5 * 4) = 2 → 2nd smallest = 2.
    #[test]
    fn percentile_disc_p50_n4() {
        let mut acc = make_pd_acc(vec![0.5]);
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1i32, 2, 3, 4]));
        acc.update_batch(&[arr]).unwrap();
        let result = acc.evaluate().unwrap();
        assert_eq!(
            result,
            ScalarValue::Int32(Some(2)),
            "p50 of {{1,2,3,4}} must be 2"
        );
    }

    /// `percentile_disc(0.5) WITHIN GROUP (ORDER BY x)` over {1,2,3,4,5} → 3
    /// k = ceil(0.5 * 5) = 3 → 3rd smallest = 3.
    #[test]
    fn percentile_disc_p50_n5() {
        let mut acc = make_pd_acc(vec![0.5]);
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![3i32, 1, 5, 2, 4])); // unsorted input
        acc.update_batch(&[arr]).unwrap();
        let result = acc.evaluate().unwrap();
        assert_eq!(
            result,
            ScalarValue::Int32(Some(3)),
            "p50 of {{1,2,3,4,5}} must be 3"
        );
    }

    /// Single element: always returns that element.
    #[test]
    fn percentile_disc_single_element() {
        for &f in &[0.0f64, 0.5, 1.0] {
            let mut acc = make_pd_acc(vec![f]);
            let arr: ArrayRef = Arc::new(Int32Array::from(vec![10i32]));
            acc.update_batch(&[arr]).unwrap();
            let result = acc.evaluate().unwrap();
            assert_eq!(result, ScalarValue::Int32(Some(10)), "single element {f}");
        }
    }

    /// fraction=0 → minimum; fraction=1 → maximum.
    #[test]
    fn percentile_disc_min_max() {
        let data: ArrayRef = Arc::new(Int32Array::from(vec![3i32, 1, 4, 1, 5, 9, 2, 6]));

        let mut acc_min = make_pd_acc(vec![0.0]);
        acc_min.update_batch(&[data.clone()]).unwrap();
        assert_eq!(
            acc_min.evaluate().unwrap(),
            ScalarValue::Int32(Some(1)),
            "fraction=0 → min"
        );

        let mut acc_max = make_pd_acc(vec![1.0]);
        acc_max.update_batch(&[data.clone()]).unwrap();
        assert_eq!(
            acc_max.evaluate().unwrap(),
            ScalarValue::Int32(Some(9)),
            "fraction=1 → max"
        );
    }

    /// Empty group (no rows) → NULL.
    #[test]
    fn percentile_disc_empty_group_returns_null() {
        let mut acc = make_pd_acc(vec![0.5]);
        let result = acc.evaluate().unwrap();
        assert_eq!(result, ScalarValue::Int32(None), "empty group must be NULL");
    }

    /// All-NULL input → NULL.
    #[test]
    fn percentile_disc_all_null_returns_null() {
        let mut acc = make_pd_acc(vec![0.5]);
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![None::<i32>, None, None]));
        acc.update_batch(&[arr]).unwrap();
        let result = acc.evaluate().unwrap();
        assert_eq!(result, ScalarValue::Int32(None), "all-NULL must be NULL");
    }

    /// NULLs mixed with values are excluded; rest computed correctly.
    #[test]
    fn percentile_disc_excludes_nulls() {
        let mut acc = make_pd_acc(vec![0.5]);
        // Non-NULL values: {5, 7} (2 values); p50 = ceil(0.5*2)=1 → 1st = 5
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![
            None,
            Some(5i32),
            None,
            Some(7),
            None,
        ]));
        acc.update_batch(&[arr]).unwrap();
        let result = acc.evaluate().unwrap();
        assert_eq!(
            result,
            ScalarValue::Int32(Some(5)),
            "NULLs excluded; p50 of {{5,7}} = 5"
        );
    }

    // ── array variant ───────────────────────────────────────────────────────

    /// `percentile_disc(ARRAY[0.25,0.5,0.75]) WITHIN GROUP (ORDER BY id)` over id=1..4
    /// → [1, 2, 3]
    /// ceil(0.25*4)=1→idx 0=1; ceil(0.5*4)=2→idx 1=2; ceil(0.75*4)=3→idx 2=3
    #[test]
    fn percentile_disc_array_variant() {
        let mut acc = make_pd_arr_acc(vec![0.25, 0.5, 0.75]);
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1i32, 2, 3, 4]));
        acc.update_batch(&[arr]).unwrap();
        let result = acc.evaluate().unwrap();
        // Should be ScalarValue::List containing [1, 2, 3].
        match result {
            ScalarValue::List(list_arr) => {
                assert_eq!(list_arr.len(), 1, "outer list must have 1 element");
                let inner = list_arr.value(0);
                assert_eq!(inner.len(), 3, "must have 3 percentile values");
                let sv0 = ScalarValue::try_from_array(&inner, 0).unwrap();
                let sv1 = ScalarValue::try_from_array(&inner, 1).unwrap();
                let sv2 = ScalarValue::try_from_array(&inner, 2).unwrap();
                assert_eq!(sv0, ScalarValue::Int32(Some(1)), "p25 of {{1,2,3,4}} = 1");
                assert_eq!(sv1, ScalarValue::Int32(Some(2)), "p50 of {{1,2,3,4}} = 2");
                assert_eq!(sv2, ScalarValue::Int32(Some(3)), "p75 of {{1,2,3,4}} = 3");
            }
            other => panic!("expected List, got {other:?}"),
        }
    }

    // ── state/merge round-trip ──────────────────────────────────────────────

    /// state() + merge_batch() must produce the same result as direct update.
    #[test]
    fn percentile_disc_state_merge_round_trip() {
        // Two partial accumulators, each sees half the data.
        let mut acc1 = make_pd_acc(vec![0.5]);
        let arr1: ArrayRef = Arc::new(Int32Array::from(vec![1i32, 3, 5]));
        acc1.update_batch(&[arr1]).unwrap();
        let state1 = acc1.state().unwrap();

        let mut acc2 = make_pd_acc(vec![0.5]);
        let arr2: ArrayRef = Arc::new(Int32Array::from(vec![2i32, 4]));
        acc2.update_batch(&[arr2]).unwrap();
        let state2 = acc2.state().unwrap();

        // Merge into a fresh accumulator.
        let mut merged = make_pd_acc(vec![0.5]);
        // Build a StringArray from both state values.
        let s1 = match &state1[0] {
            ScalarValue::Utf8(Some(s)) => s.clone(),
            _ => panic!(),
        };
        let s2 = match &state2[0] {
            ScalarValue::Utf8(Some(s)) => s.clone(),
            _ => panic!(),
        };
        let states_arr: ArrayRef = Arc::new(datafusion::arrow::array::StringArray::from(vec![
            s1.as_str(),
            s2.as_str(),
        ]));
        merged.merge_batch(&[states_arr]).unwrap();

        // p50 of {1,2,3,4,5} = 3.
        let result = merged.evaluate().unwrap();
        assert_eq!(
            result,
            ScalarValue::Int32(Some(3)),
            "merge: p50 of {{1..5}} = 3"
        );
    }

    // ── mode ───────────────────────────────────────────────────────────────

    fn make_mode_acc() -> ModeAccumulator {
        ModeAccumulator {
            values: vec![],
            data_type: DataType::Int32,
        }
    }

    /// `mode()` over {1,1,2,2,2,3} → 2 (highest frequency).
    #[test]
    fn mode_most_frequent() {
        let mut acc = make_mode_acc();
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1i32, 1, 2, 2, 2, 3]));
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int32(Some(2)));
    }

    /// Tie {1,1,2,2} → 1 (first in sort order).
    #[test]
    fn mode_tie_broken_by_sort_order() {
        let mut acc = make_mode_acc();
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![2i32, 1, 2, 1])); // unsorted input
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Int32(Some(1)),
            "tie between 1 and 2: first in sort order (1) wins"
        );
    }

    /// NULLs are excluded; {NULL, 5, 5, NULL, 7} → 5.
    #[test]
    fn mode_excludes_nulls() {
        let mut acc = make_mode_acc();
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![
            None,
            Some(5i32),
            Some(5),
            None,
            Some(7),
        ]));
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int32(Some(5)));
    }

    /// Empty group → NULL.
    #[test]
    fn mode_empty_group_returns_null() {
        let mut acc = make_mode_acc();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int32(None));
    }

    /// All-NULL → NULL.
    #[test]
    fn mode_all_null_returns_null() {
        let mut acc = make_mode_acc();
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![None::<i32>, None, None]));
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int32(None));
    }

    /// Single value → that value.
    #[test]
    fn mode_single_value() {
        let mut acc = make_mode_acc();
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![42i32]));
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int32(Some(42)));
    }

    /// mode() state/merge round-trip.
    #[test]
    fn mode_state_merge_round_trip() {
        let mut acc1 = make_mode_acc();
        let arr1: ArrayRef = Arc::new(Int32Array::from(vec![1i32, 2, 2]));
        acc1.update_batch(&[arr1]).unwrap();
        let state1 = acc1.state().unwrap();

        let mut acc2 = make_mode_acc();
        let arr2: ArrayRef = Arc::new(Int32Array::from(vec![2i32, 3, 3]));
        acc2.update_batch(&[arr2]).unwrap();
        let state2 = acc2.state().unwrap();

        let mut merged = make_mode_acc();
        let s1 = match &state1[0] {
            ScalarValue::Utf8(Some(s)) => s.clone(),
            _ => panic!(),
        };
        let s2 = match &state2[0] {
            ScalarValue::Utf8(Some(s)) => s.clone(),
            _ => panic!(),
        };
        let states_arr: ArrayRef = Arc::new(datafusion::arrow::array::StringArray::from(vec![
            s1.as_str(),
            s2.as_str(),
        ]));
        merged.merge_batch(&[states_arr]).unwrap();

        // Combined: {1, 2, 2, 2, 3, 3} → mode = 2
        assert_eq!(merged.evaluate().unwrap(), ScalarValue::Int32(Some(2)));
    }

    // ── GROUP BY correctness ────────────────────────────────────────────────

    /// Verify that per-group computation is correct by simulating two groups.
    #[test]
    fn percentile_disc_per_group_simulation() {
        // Group A: {10, 20} → p50 = ceil(0.5*2)=1 → 10
        let mut acc_a = make_pd_acc(vec![0.5]);
        let arr_a: ArrayRef = Arc::new(Int32Array::from(vec![20i32, 10]));
        acc_a.update_batch(&[arr_a]).unwrap();
        assert_eq!(acc_a.evaluate().unwrap(), ScalarValue::Int32(Some(10)));

        // Group B: {100, 200, 300} → p50 = ceil(0.5*3)=2 → 200
        let mut acc_b = make_pd_acc(vec![0.5]);
        let arr_b: ArrayRef = Arc::new(Int32Array::from(vec![300i32, 100, 200]));
        acc_b.update_batch(&[arr_b]).unwrap();
        assert_eq!(acc_b.evaluate().unwrap(), ScalarValue::Int32(Some(200)));
    }

    /// Verify mode per-group correctness.
    #[test]
    fn mode_per_group_simulation() {
        // Group A: {1, 1, 2} → mode = 1
        let mut acc_a = make_mode_acc();
        let arr_a: ArrayRef = Arc::new(Int32Array::from(vec![1i32, 1, 2]));
        acc_a.update_batch(&[arr_a]).unwrap();
        assert_eq!(acc_a.evaluate().unwrap(), ScalarValue::Int32(Some(1)));

        // Group B: {3, 3, 3, 2} → mode = 3
        let mut acc_b = make_mode_acc();
        let arr_b: ArrayRef = Arc::new(Int32Array::from(vec![2i32, 3, 3, 3]));
        acc_b.update_batch(&[arr_b]).unwrap();
        assert_eq!(acc_b.evaluate().unwrap(), ScalarValue::Int32(Some(3)));
    }
}

// ── percentile_disc ───────────────────────────────────────────────────────────

/// Exact `percentile_disc(f) WITHIN GROUP (ORDER BY expr)` UDAF.
///
/// # Postgres semantics (exact)
/// Collect all non-NULL values of `expr`, sort ascending.  With N values,
/// return the value at 1-based position `k = ceil(f * N)` (clamped to [1, N]).
/// For `f = 0` return the minimum; for `f = 1` return the maximum.
///
/// Also supports the array-of-fractions variant:
/// `percentile_disc(ARRAY[0.25, 0.5, 0.75]) WITHIN GROUP (ORDER BY expr)` →
/// returns a `List` of the discrete percentile values.
///
/// # DataFusion integration
/// `supports_within_group_clause()` returns `true`.  DataFusion's SQL planner
/// then prepends the ORDER BY expression as `args.exprs[0]`; the direct arg
/// (the fraction literal or array) becomes `args.exprs[1]`.  `update_batch`
/// receives `values[0]` = the data column.
///
/// # Memory
/// All non-NULL group values are buffered in an `Arc<[ScalarValue]>` list
/// serialised via a JSON state string.  This is O(N) per group — unavoidable
/// for exact ordered-set semantics.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PercentileDiscUdaf {
    signature: Signature,
}

impl PercentileDiscUdaf {
    pub fn new() -> Self {
        // Accepts (any_data_expr, Float64_or_List<Float64>) — DataFusion
        // coerces the caller's literal before handing us exprs.
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for PercentileDiscUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "percentile_disc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn supports_within_group_clause(&self) -> bool {
        true
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        // arg_types[0] = data expr type (ORDER BY column)
        // arg_types[1] = fraction type (Float64 scalar or List<Float64>)
        let data_type = arg_types.first().cloned().unwrap_or(DataType::Null);
        match arg_types.get(1) {
            // Array variant → return List<data_type>
            Some(DataType::List(_)) | Some(DataType::LargeList(_)) => Ok(DataType::List(Arc::new(
                Field::new_list_field(data_type, true),
            ))),
            // Scalar variant → return data_type unchanged (discrete means same type)
            _ => Ok(data_type),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<Arc<Field>>> {
        // State: a JSON string encoding the buffered values list.
        Ok(vec![Arc::new(Field::new(
            format!("{}_state", args.name),
            DataType::Utf8,
            true,
        ))])
    }

    fn accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DFResult<Box<dyn datafusion::logical_expr::Accumulator>> {
        let fractions = extract_fractions(args.exprs.get(1))?;
        let data_type = args
            .expr_fields
            .first()
            .map(|f| f.data_type().clone())
            .unwrap_or(DataType::Null);
        let is_array_mode = matches!(
            args.expr_fields.get(1).map(|f| f.data_type()),
            Some(DataType::List(_)) | Some(DataType::LargeList(_))
        );
        Ok(Box::new(PercentileDiscAccumulator {
            values: vec![],
            fractions,
            data_type,
            is_array_mode,
        }))
    }
}

/// Extract fraction(s) from the second physical expression (must be a literal).
fn extract_fractions(
    expr: Option<&Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
) -> DFResult<Vec<f64>> {
    let Some(expr) = expr else {
        return plan_err!("percentile_disc requires a fraction argument");
    };
    let empty_schema = Arc::new(Schema::empty());
    let batch = RecordBatch::new_empty(Arc::clone(&empty_schema));
    let val = match expr.evaluate(&batch)? {
        ColumnarValue::Scalar(s) => s,
        ColumnarValue::Array(_) => {
            return plan_err!("percentile_disc fraction must be a literal scalar or literal array");
        }
    };
    match val {
        ScalarValue::Float64(Some(f)) => Ok(vec![f]),
        ScalarValue::Float32(Some(f)) => Ok(vec![f as f64]),
        ScalarValue::List(list_arr) => {
            // Extract each element as f64.
            let mut fracs = vec![];
            if list_arr.len() == 0 {
                return Ok(fracs);
            }
            let values = list_arr.value(0);
            for i in 0..values.len() {
                if values.is_null(i) {
                    return plan_err!("percentile_disc: fraction array must not contain NULLs");
                }
                let sv = ScalarValue::try_from_array(&values, i)?;
                let f = scalar_to_f64(&sv)?;
                if !(0.0..=1.0).contains(&f) {
                    return plan_err!(
                        "percentile_disc: fraction must be between 0.0 and 1.0, got {f}"
                    );
                }
                fracs.push(f);
            }
            Ok(fracs)
        }
        ScalarValue::Float64(None) | ScalarValue::Float32(None) => {
            plan_err!("percentile_disc: fraction must not be NULL")
        }
        other => {
            // Try to coerce integer literals.
            match other {
                ScalarValue::Int8(Some(v)) => Ok(vec![v as f64]),
                ScalarValue::Int16(Some(v)) => Ok(vec![v as f64]),
                ScalarValue::Int32(Some(v)) => Ok(vec![v as f64]),
                ScalarValue::Int64(Some(v)) => Ok(vec![v as f64]),
                ScalarValue::UInt8(Some(v)) => Ok(vec![v as f64]),
                ScalarValue::UInt16(Some(v)) => Ok(vec![v as f64]),
                ScalarValue::UInt32(Some(v)) => Ok(vec![v as f64]),
                ScalarValue::UInt64(Some(v)) => Ok(vec![v as f64]),
                _ => plan_err!("percentile_disc: fraction must be a float literal between 0 and 1"),
            }
        }
    }
}

fn scalar_to_f64(sv: &ScalarValue) -> DFResult<f64> {
    Ok(match sv {
        ScalarValue::Float32(Some(v)) => *v as f64,
        ScalarValue::Float64(Some(v)) => *v,
        ScalarValue::Int8(Some(v)) => *v as f64,
        ScalarValue::Int16(Some(v)) => *v as f64,
        ScalarValue::Int32(Some(v)) => *v as f64,
        ScalarValue::Int64(Some(v)) => *v as f64,
        ScalarValue::UInt8(Some(v)) => *v as f64,
        ScalarValue::UInt16(Some(v)) => *v as f64,
        ScalarValue::UInt32(Some(v)) => *v as f64,
        ScalarValue::UInt64(Some(v)) => *v as f64,
        other => return exec_err!("Cannot convert {other:?} to f64"),
    })
}

/// Compute the exact discrete percentile index (1-based, Postgres spec).
///
/// Given N values (sorted ascending), fraction f:
/// - k = ceil(f * N), clamped to [1, N].
/// - For f = 0: k = 1 (minimum).
/// - Returns 0-based index k - 1.
fn percentile_disc_index(n: usize, fraction: f64) -> usize {
    if n == 0 {
        return 0;
    }
    if fraction <= 0.0 {
        return 0;
    }
    let k = (fraction * n as f64).ceil() as usize;
    k.clamp(1, n) - 1
}

#[derive(Debug)]
struct PercentileDiscAccumulator {
    /// Non-NULL values buffered so far (in arrival order; sorted at evaluate time).
    values: Vec<ScalarValue>,
    /// The fraction(s) to compute.
    fractions: Vec<f64>,
    /// Data type of the accumulated column.
    data_type: DataType,
    /// True iff the caller passed ARRAY[...] of fractions → result is a List.
    is_array_mode: bool,
}

impl datafusion::logical_expr::Accumulator for PercentileDiscAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let arr = &values[0];
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue; // Exclude NULLs per Postgres spec.
            }
            self.values
                .push(ScalarValue::try_from_array(arr.as_ref(), i)?);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let n = self.values.len();
        if n == 0 {
            // Empty / all-NULL group → NULL.
            if self.is_array_mode {
                return Ok(ScalarValue::List(Arc::new(ListArray::new_null(
                    Arc::new(Field::new_list_field(self.data_type.clone(), true)),
                    1,
                ))));
            }
            return Ok(ScalarValue::try_from(&self.data_type)?);
        }

        // Sort the values ascending (NULL-safe: we excluded NULLs above).
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        if self.is_array_mode {
            // Array variant: one result per fraction.
            let results: Vec<ScalarValue> = self
                .fractions
                .iter()
                .map(|&f| sorted[percentile_disc_index(n, f)].clone())
                .collect();
            // Build a ListArray with one list element containing all results.
            let element_array = ScalarValue::iter_to_array(results.into_iter())?;
            let offsets =
                OffsetBuffer::new(ScalarBuffer::from(vec![0i32, element_array.len() as i32]));
            let list_array = ListArray::new(
                Arc::new(Field::new_list_field(self.data_type.clone(), true)),
                offsets,
                element_array,
                None,
            );
            Ok(ScalarValue::List(Arc::new(list_array)))
        } else {
            // Scalar variant: single fraction.
            let f = self.fractions.first().copied().unwrap_or(0.5);
            Ok(sorted[percentile_disc_index(n, f)].clone())
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.len() * 64
    }

    /// Serialise state as a JSON string of scalar values.
    ///
    /// State layout: single `Utf8` column containing a JSON array of the
    /// buffered scalar values (serialised via `arrow_scalar_to_json`).  This
    /// matches the design used by `JsonAggAccumulator` so that `merge_batch`
    /// can simply deserialise and concatenate.
    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let json_vals: Vec<JValue> = self.values.iter().map(scalar_value_to_json).collect();
        let s = JValue::Array(json_vals).to_string();
        Ok(vec![ScalarValue::Utf8(Some(s))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let Some(arr) = states[0].as_any().downcast_ref::<StringArray>() else {
            return Ok(());
        };
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let s = arr.value(i);
            if let Ok(JValue::Array(elems)) = serde_json::from_str::<JValue>(s) {
                for elem in elems {
                    let sv = json_value_to_scalar(&elem, &self.data_type)?;
                    self.values.push(sv);
                }
            }
        }
        Ok(())
    }
}

// ── mode ──────────────────────────────────────────────────────────────────────

/// Exact `mode() WITHIN GROUP (ORDER BY expr)` UDAF.
///
/// # Postgres semantics (exact)
/// The most frequent non-NULL value of `expr`.  Ties are broken by the first
/// value in ascending sort order.  Returns the same type as `expr`.
///
/// # DataFusion integration
/// `supports_within_group_clause()` returns `true`.  DataFusion prepends the
/// ORDER BY expression as `args.exprs[0]`.  `update_batch` receives `values[0]`.
///
/// # Memory: O(N) per group.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ModeUdaf {
    signature: Signature,
}

impl ModeUdaf {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ModeUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "mode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn supports_within_group_clause(&self) -> bool {
        true
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<Arc<Field>>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}_state", args.name),
            DataType::Utf8,
            true,
        ))])
    }

    fn accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DFResult<Box<dyn datafusion::logical_expr::Accumulator>> {
        let data_type = args
            .expr_fields
            .first()
            .map(|f| f.data_type().clone())
            .unwrap_or(DataType::Null);
        Ok(Box::new(ModeAccumulator {
            values: vec![],
            data_type,
        }))
    }
}

#[derive(Debug)]
struct ModeAccumulator {
    values: Vec<ScalarValue>,
    data_type: DataType,
}

impl datafusion::logical_expr::Accumulator for ModeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let arr = &values[0];
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            self.values
                .push(ScalarValue::try_from_array(arr.as_ref(), i)?);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.values.is_empty() {
            return Ok(ScalarValue::try_from(&self.data_type)?);
        }

        // Sort all values ascending (for tie-break: first in sort order wins).
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        // Count frequencies using the sorted list.  Because `sorted` is
        // ascending, the first occurrence of each run determines sort-order
        // position.  We want the value with the highest count; on ties, the
        // smallest (earliest in sort order).
        let mut best_val: Option<&ScalarValue> = None;
        let mut best_count = 0usize;
        let mut run_count = 0usize;
        let mut prev: Option<&ScalarValue> = None;

        for v in &sorted {
            if prev.map_or(false, |p| p == v) {
                run_count += 1;
            } else {
                // New value; update best if previous run was longer.
                if run_count > best_count {
                    best_count = run_count;
                    best_val = prev;
                }
                run_count = 1;
                prev = Some(v);
            }
        }
        // Final run.
        if run_count > best_count {
            best_count = run_count;
            best_val = prev;
        }
        let _ = best_count;

        Ok(best_val
            .cloned()
            .unwrap_or_else(|| ScalarValue::try_from(&self.data_type).unwrap_or(ScalarValue::Null)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.len() * 64
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let json_vals: Vec<JValue> = self.values.iter().map(scalar_value_to_json).collect();
        let s = JValue::Array(json_vals).to_string();
        Ok(vec![ScalarValue::Utf8(Some(s))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let Some(arr) = states[0].as_any().downcast_ref::<StringArray>() else {
            return Ok(());
        };
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let s = arr.value(i);
            if let Ok(JValue::Array(elems)) = serde_json::from_str::<JValue>(s) {
                for elem in elems {
                    let sv = json_value_to_scalar(&elem, &self.data_type)?;
                    self.values.push(sv);
                }
            }
        }
        Ok(())
    }
}

// ── JSON ↔ ScalarValue helpers for ordered-set state serialisation ─────────

/// Convert a `ScalarValue` to a `serde_json::Value` for state serialisation.
fn scalar_value_to_json(sv: &ScalarValue) -> JValue {
    match sv {
        ScalarValue::Int8(Some(v)) => JValue::Number((*v).into()),
        ScalarValue::Int16(Some(v)) => JValue::Number((*v).into()),
        ScalarValue::Int32(Some(v)) => JValue::Number((*v).into()),
        ScalarValue::Int64(Some(v)) => JValue::Number((*v).into()),
        ScalarValue::UInt8(Some(v)) => JValue::Number((*v).into()),
        ScalarValue::UInt16(Some(v)) => JValue::Number((*v).into()),
        ScalarValue::UInt32(Some(v)) => JValue::Number((*v).into()),
        ScalarValue::UInt64(Some(v)) => JValue::Number((*v).into()),
        ScalarValue::Float32(Some(v)) => serde_json::Number::from_f64(*v as f64)
            .map(JValue::Number)
            .unwrap_or(JValue::Null),
        ScalarValue::Float64(Some(v)) => serde_json::Number::from_f64(*v)
            .map(JValue::Number)
            .unwrap_or(JValue::Null),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => JValue::String(s.clone()),
        ScalarValue::Boolean(Some(b)) => JValue::Bool(*b),
        ScalarValue::Date32(Some(d)) => JValue::Number((*d).into()),
        ScalarValue::TimestampMicrosecond(Some(ts), _) => JValue::Number((*ts).into()),
        ScalarValue::TimestampMillisecond(Some(ts), _) => JValue::Number((*ts).into()),
        ScalarValue::TimestampSecond(Some(ts), _) => JValue::Number((*ts).into()),
        ScalarValue::TimestampNanosecond(Some(ts), _) => JValue::Number((*ts).into()),
        _ => JValue::Null,
    }
}

/// Deserialise a `serde_json::Value` back into a `ScalarValue` of the given type.
fn json_value_to_scalar(v: &JValue, dt: &DataType) -> DFResult<ScalarValue> {
    Ok(match (v, dt) {
        (JValue::Number(n), DataType::Int8) => ScalarValue::Int8(n.as_i64().map(|x| x as i8)),
        (JValue::Number(n), DataType::Int16) => ScalarValue::Int16(n.as_i64().map(|x| x as i16)),
        (JValue::Number(n), DataType::Int32) => ScalarValue::Int32(n.as_i64().map(|x| x as i32)),
        (JValue::Number(n), DataType::Int64) => ScalarValue::Int64(n.as_i64()),
        (JValue::Number(n), DataType::UInt8) => ScalarValue::UInt8(n.as_u64().map(|x| x as u8)),
        (JValue::Number(n), DataType::UInt16) => ScalarValue::UInt16(n.as_u64().map(|x| x as u16)),
        (JValue::Number(n), DataType::UInt32) => ScalarValue::UInt32(n.as_u64().map(|x| x as u32)),
        (JValue::Number(n), DataType::UInt64) => ScalarValue::UInt64(n.as_u64()),
        (JValue::Number(n), DataType::Float32) => {
            ScalarValue::Float32(n.as_f64().map(|x| x as f32))
        }
        (JValue::Number(n), DataType::Float64) => ScalarValue::Float64(n.as_f64()),
        (JValue::String(s), DataType::Utf8) => ScalarValue::Utf8(Some(s.clone())),
        (JValue::String(s), DataType::LargeUtf8) => ScalarValue::LargeUtf8(Some(s.clone())),
        (JValue::Bool(b), DataType::Boolean) => ScalarValue::Boolean(Some(*b)),
        (JValue::Number(n), DataType::Date32) => ScalarValue::Date32(n.as_i64().map(|x| x as i32)),
        (
            JValue::Number(n),
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, tz),
        ) => ScalarValue::TimestampMicrosecond(n.as_i64(), tz.clone()),
        (
            JValue::Number(n),
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, tz),
        ) => ScalarValue::TimestampMillisecond(n.as_i64(), tz.clone()),
        (
            JValue::Number(n),
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Second, tz),
        ) => ScalarValue::TimestampSecond(n.as_i64(), tz.clone()),
        (
            JValue::Number(n),
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, tz),
        ) => ScalarValue::TimestampNanosecond(n.as_i64(), tz.clone()),
        (JValue::Null, _) => ScalarValue::try_from(dt)?,
        _ => {
            return exec_err!("json_value_to_scalar: cannot convert {v:?} to {dt:?}");
        }
    })
}

// ── registration helper ───────────────────────────────────────────────────────

use datafusion::logical_expr::AggregateUDF;
use datafusion::prelude::SessionContext;

/// Register all PG JSON aggregate UDAFs on `ctx`.
///
/// Registers:
/// - `json_agg(col)`
/// - `jsonb_agg(col)`
/// - `json_object_agg(key, value)`
/// - `jsonb_object_agg(key, value)`
/// - `percentile_disc(f) WITHIN GROUP (ORDER BY expr)` (exact, discrete)
/// - `mode() WITHIN GROUP (ORDER BY expr)` (exact, sort-order tie-break)
pub(crate) fn register_json_agg_udafs(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(JsonAggUdaf::new("json_agg")));
    ctx.register_udaf(AggregateUDF::from(JsonAggUdaf::new("jsonb_agg")));
    ctx.register_udaf(AggregateUDF::from(JsonObjectAggUdaf::new(
        "json_object_agg",
    )));
    ctx.register_udaf(AggregateUDF::from(JsonObjectAggUdaf::new(
        "jsonb_object_agg",
    )));
    ctx.register_udaf(AggregateUDF::from(PercentileDiscUdaf::new()));
    ctx.register_udaf(AggregateUDF::from(ModeUdaf::new()));
}
