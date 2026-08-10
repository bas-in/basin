//! PostgreSQL aggregate UDAFs.
//!
//! Implements JSON aggregates, ordered-set aggregates, and related UDAFs:
//!
//! ## JSON aggregates
//! - `json_agg(col)` / `jsonb_agg(col)` → JSON array of all non-null values
//! - `json_object_agg(key, value)` / `jsonb_object_agg(key, value)` → JSON
//!   object mapping each key to the corresponding value
//!
//! ## array_agg (ordered fast path)
//! - `array_agg(expr ORDER BY sortkey [ASC|DESC])` — Basin overrides
//!   DataFusion's builtin `array_agg` with [`PgArrayAggUdaf`], a thin wrapper
//!   that delegates every path to the builtin **except** the non-DISTINCT
//!   `ORDER BY` path.  The builtin `OrderSensitiveArrayAggAccumulator`
//!   materialises one `ScalarValue` per row plus a `Vec<ScalarValue>` of sort
//!   keys per row and sorts via row-at-a-time `compare_rows`; at 1M rows that
//!   per-row scalar-tree work made `ARRAY_AGG(x ORDER BY y)` ~6.5x slower than
//!   Postgres.
//!
//!   This path is served two ways, both producing the byte-identical output
//!   (a single-row `List` scalar with a nullable `item` field) and the same
//!   `[List<item>, List<Struct<ordering>>]` partial state as the builtin:
//!   * **Grouped scan (the benchmark shape, `GROUP BY ...`)** —
//!     [`OrderedArrayAggGroupsAccumulator`], a true vectorized
//!     [`GroupsAccumulator`].  DataFusion would otherwise wrap a per-group
//!     `Accumulator` in `GroupsAccumulatorAdapter`, which slices every input
//!     batch once per group and dispatches `update_batch` per group — the
//!     dominant cost at 1M rows / many groups.  The GroupsAccumulator instead
//!     buffers whole Arrow chunks plus `(group, row)` entries (O(1) per batch)
//!     and defers all work to `evaluate`: one global `lexsort_to_indices` keyed
//!     on `[group_index, sortkeys...]` (rows come out grouped *and* ordered in
//!     a single vectorized sort), a counting pass for list offsets, and one
//!     `take`.  `groups_accumulator_supported` returns this path only for
//!     non-DISTINCT, non-IGNORE-NULLS, non-nested sort keys; everything else
//!     falls back below.
//!   * **Single group / no GROUP BY** — [`OrderedArrayAggAccumulator`], the
//!     equivalent per-group `Accumulator` (`concat` + `lexsort_to_indices` +
//!     `take` at evaluate).  Also the fallback for exotic shapes the
//!     GroupsAccumulator opts out of.
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
    new_empty_array, Array, ArrayRef, BooleanArray, LargeBinaryArray, ListArray, NullBufferBuilder,
    StringArray, StructArray, UInt32Array,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::compute::{concat, lexsort_to_indices, take, SortColumn, SortOptions};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{exec_err, plan_err, Result as DFResult};
use datafusion::functions_aggregate::array_agg::ArrayAgg;
use datafusion::logical_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    AggregateUDFImpl, ColumnarValue, EmitTo, GroupsAccumulator, Signature, Volatility,
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

// ── array_agg (PG override: vectorized ORDER BY path) ────────────────────────

/// Basin's `array_agg` UDAF: a wrapper around DataFusion's builtin
/// [`ArrayAgg`] that replaces only the non-DISTINCT `ORDER BY` accumulator.
///
/// # Why override at all
/// The builtin serves `array_agg(x ORDER BY y)` with
/// `OrderSensitiveArrayAggAccumulator`, which buffers a `ScalarValue` per row
/// *and* a `Vec<ScalarValue>` of sort keys per row, then sorts with
/// row-at-a-time `compare_rows` and re-materialises through
/// `ScalarValue::iter_to_array`.  All of that is per-row heap work; at 1M rows
/// the benchmark measured ~6.5x slower than Postgres.
///
/// # What stays delegated (zero behaviour change)
/// - `array_agg(x)` without ORDER BY — including the fast vectorized
///   `GroupsAccumulator` path (`groups_accumulator_supported` /
///   `create_groups_accumulator` delegate).
/// - `array_agg(DISTINCT x [ORDER BY x])` — builtin
///   `DistinctArrayAggAccumulator`.
/// - `IGNORE NULLS` — builtin ordered accumulator (rare; not worth a second
///   fast path).
/// - `return_type` / `state_fields` — delegated, so the plan-level schema and
///   the partial-state schema are byte-identical to the builtin's.
///
/// # The fast path
/// [`OrderedArrayAggAccumulator`] buffers the incoming Arrow array chunks
/// as-is (a couple of `Arc` clones per `update_batch`) and defers all work to
/// evaluation: one `concat`, one `lexsort_to_indices` (typed, vectorized;
/// single-key sorts take the `sort_to_indices` fast path), one `take`.
/// Partial state is emitted in arrival order — the final-phase accumulator
/// sorts once after merging, which is strictly less work than the builtin's
/// sort-per-partial + k-way scalar merge.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct PgArrayAggUdaf {
    inner: ArrayAgg,
}

impl PgArrayAggUdaf {
    pub fn new() -> Self {
        Self {
            inner: ArrayAgg::default(),
        }
    }
}

impl AggregateUDFImpl for PgArrayAggUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_agg"
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        self.inner.return_type(arg_types)
    }

    /// Delegated: the partial-aggregation state schema must stay identical to
    /// the builtin's (`[List<item>, List<Struct<ordering cols>>]` when an
    /// ORDER BY is present) because [`OrderedArrayAggAccumulator`] emits
    /// exactly that layout.
    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }

    /// Same as the builtin (`SoftRequirement`): the planner may pre-sort the
    /// input but is never *required* to — the accumulator sorts itself.
    fn order_sensitivity(&self) -> datafusion::logical_expr::utils::AggregateOrderSensitivity {
        self.inner.order_sensitivity()
    }

    /// The fast accumulator always sorts at evaluation time, so a beneficial
    /// input ordering changes nothing; keep the same UDAF instance.
    fn with_beneficial_ordering(
        self: Arc<Self>,
        _beneficial_ordering: bool,
    ) -> DFResult<Option<Arc<dyn AggregateUDFImpl>>> {
        Ok(Some(self))
    }

    fn supports_null_handling_clause(&self) -> bool {
        self.inner.supports_null_handling_clause()
    }

    /// The fast `ORDER BY` path supports a true vectorized
    /// [`GroupsAccumulator`] for the same shapes the per-group
    /// [`OrderedArrayAggAccumulator`] handles: non-DISTINCT, non-IGNORE-NULLS,
    /// with non-nested (lexsort-able) sort keys.  For the plain unordered /
    /// DISTINCT shapes we defer to the builtin's decision (it ships its own
    /// `ArrayAggGroupsAccumulator` for the unordered case).
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        if ordered_fast_path_fields(&args).is_some() {
            return true;
        }
        self.inner.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DFResult<Box<dyn datafusion::logical_expr::GroupsAccumulator>> {
        if let Some((ordering_fields, sort_options)) = ordered_fast_path_fields(&args) {
            return Ok(Box::new(OrderedArrayAggGroupsAccumulator::new(
                args.expr_fields[0].data_type().clone(),
                ordering_fields,
                sort_options,
            )));
        }
        self.inner.create_groups_accumulator(args)
    }

    fn accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DFResult<Box<dyn datafusion::logical_expr::Accumulator>> {
        let field = &args.expr_fields[0];
        match ordered_fast_path_fields(&args) {
            Some((ordering_fields, sort_options)) => Ok(Box::new(OrderedArrayAggAccumulator::new(
                field.data_type().clone(),
                ordering_fields,
                sort_options,
            ))),
            // DISTINCT / unordered / IGNORE NULLS / nested sort keys: builtin
            // accumulators are either already vectorized or rare enough not to
            // matter.  The state layout matches because `state_fields` is
            // delegated too.
            None => self.inner.accumulator(args),
        }
    }
}

/// Returns `Some((ordering_fields, sort_options))` when `args` describes the
/// fast non-DISTINCT `ORDER BY` `array_agg` path that Basin serves with its
/// own vectorized accumulators, or `None` to defer to the DataFusion builtin.
///
/// `None` is returned for: no ORDER BY, DISTINCT, IGNORE NULLS, or any nested
/// (`List`/`Struct`/…) sort key — `lexsort_to_indices` cannot order nested
/// keys, but the builtin's row-wise comparator can.
///
/// `ordering_fields` are named exactly like DataFusion's `ordering_fields`
/// helper (`expr.to_string()`, nullable) so the emitted partial-state struct
/// columns match the schema declared by the delegated `state_fields`.
fn ordered_fast_path_fields(args: &AccumulatorArgs) -> Option<(Vec<FieldRef>, Vec<SortOptions>)> {
    let field = &args.expr_fields[0];
    let ignore_nulls = args.ignore_nulls && field.is_nullable();
    if args.is_distinct || args.order_bys.is_empty() || ignore_nulls {
        return None;
    }
    let mut sort_options = Vec::with_capacity(args.order_bys.len());
    let mut ordering_fields: Vec<FieldRef> = Vec::with_capacity(args.order_bys.len());
    for sort_expr in args.order_bys {
        let dt = sort_expr.expr.data_type(args.schema).ok()?;
        if dt.is_nested() {
            return None;
        }
        sort_options.push(sort_expr.options);
        ordering_fields.push(Arc::new(Field::new(sort_expr.expr.to_string(), dt, true)));
    }
    Some((ordering_fields, sort_options))
}

/// Build the single-row `List` scalar shape that `array_agg` results and
/// partial states use: `List<Field("item", item_type, nullable)>` with one
/// list element containing `values`.  This is the same shape
/// `ScalarValue::new_list_from_iter` / `SingleRowListArrayBuilder` produce,
/// so results compare equal to the builtin's byte-for-byte.
fn single_row_list(item_type: DataType, values: ArrayRef) -> ScalarValue {
    let field = Arc::new(Field::new_list_field(item_type, true));
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, values.len() as i32]));
    ScalarValue::List(Arc::new(ListArray::new(field, offsets, values, None)))
}

/// Vectorized accumulator for `array_agg(expr ORDER BY keys...)`.
///
/// `update_batch` is O(1) per batch (Arc clones); all real work happens once
/// per group at evaluation: `concat` the chunks, `lexsort_to_indices` on the
/// typed key columns (honouring per-key `SortOptions`, i.e. ASC/DESC and
/// NULLS FIRST/LAST), then `take` the value column through the permutation.
#[derive(Debug)]
struct OrderedArrayAggAccumulator {
    /// Element type of the aggregated column.
    value_type: DataType,
    /// Struct fields for the ordering columns in the partial state
    /// (names/types must match the delegated `state_fields` declaration).
    ordering_fields: Vec<FieldRef>,
    /// Sort direction / null placement per ordering column.
    sort_options: Vec<SortOptions>,
    /// Buffered value chunks, in arrival order.
    value_chunks: Vec<ArrayRef>,
    /// Buffered ordering-column chunks; `ordering_chunks[k][j]` is ordering
    /// column `j` of chunk `k` (same row count as `value_chunks[k]`).
    ordering_chunks: Vec<Vec<ArrayRef>>,
    /// Incrementally tracked slice memory, for `size()` accounting.
    approx_bytes: usize,
}

impl OrderedArrayAggAccumulator {
    fn new(
        value_type: DataType,
        ordering_fields: Vec<FieldRef>,
        sort_options: Vec<SortOptions>,
    ) -> Self {
        Self {
            value_type,
            ordering_fields,
            sort_options,
            value_chunks: vec![],
            ordering_chunks: vec![],
            approx_bytes: 0,
        }
    }

    fn num_rows(&self) -> usize {
        self.value_chunks.iter().map(|c| c.len()).sum()
    }

    fn push_chunk(&mut self, values: ArrayRef, ordering: Vec<ArrayRef>) {
        // `get_slice_memory_size` counts only this slice's rows, so sliced
        // per-group batches (GroupsAccumulatorAdapter) don't multiply-count
        // the shared parent buffers.
        self.approx_bytes += values.to_data().get_slice_memory_size().unwrap_or(0);
        for col in &ordering {
            self.approx_bytes += col.to_data().get_slice_memory_size().unwrap_or(0);
        }
        self.value_chunks.push(values);
        self.ordering_chunks.push(ordering);
    }

    /// Concatenate the buffered chunks into one values array plus one array
    /// per ordering column.  Caller must ensure at least one chunk exists.
    fn concat_columns(&self) -> DFResult<(ArrayRef, Vec<ArrayRef>)> {
        let values = if self.value_chunks.len() == 1 {
            Arc::clone(&self.value_chunks[0])
        } else {
            let refs: Vec<&dyn Array> = self.value_chunks.iter().map(|c| c.as_ref()).collect();
            concat(&refs)?
        };
        let mut ord_cols = Vec::with_capacity(self.sort_options.len());
        for j in 0..self.sort_options.len() {
            let col = if self.ordering_chunks.len() == 1 {
                Arc::clone(&self.ordering_chunks[0][j])
            } else {
                let refs: Vec<&dyn Array> =
                    self.ordering_chunks.iter().map(|c| c[j].as_ref()).collect();
                concat(&refs)?
            };
            ord_cols.push(col);
        }
        Ok((values, ord_cols))
    }

    /// Compute the sort permutation for the given ordering columns.
    fn sort_indices(&self, ord_cols: &[ArrayRef]) -> DFResult<UInt32Array> {
        let sort_cols: Vec<SortColumn> = ord_cols
            .iter()
            .zip(self.sort_options.iter())
            .map(|(col, opts)| SortColumn {
                values: Arc::clone(col),
                options: Some(*opts),
            })
            .collect();
        Ok(lexsort_to_indices(&sort_cols, None)?)
    }
}

impl datafusion::logical_expr::Accumulator for OrderedArrayAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let n_ord = self.sort_options.len();
        if values.len() < 1 + n_ord {
            return exec_err!(
                "array_agg ORDER BY: expected {} input columns, got {}",
                1 + n_ord,
                values.len()
            );
        }
        if values[0].is_empty() {
            return Ok(());
        }
        self.push_chunk(Arc::clone(&values[0]), values[1..1 + n_ord].to_vec());
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.num_rows() == 0 {
            // Matches the builtin (and PG): zero rows → NULL, not '{}'.
            return Ok(ScalarValue::new_null_list(self.value_type.clone(), true, 1));
        }
        let (values, ord_cols) = self.concat_columns()?;
        let indices = self.sort_indices(&ord_cols)?;
        let sorted = take(values.as_ref(), &indices, None)?;
        Ok(single_row_list(self.value_type.clone(), sorted))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.approx_bytes
            + self.value_chunks.capacity() * std::mem::size_of::<ArrayRef>()
            + self
                .ordering_chunks
                .iter()
                .map(|c| c.capacity() * std::mem::size_of::<ArrayRef>())
                .sum::<usize>()
    }

    /// State layout (matching the builtin's declaration, which the delegated
    /// `state_fields` emits):
    /// - column 0: single-row `List<item>` of the buffered values
    /// - column 1: single-row `List<Struct<ordering cols>>`, one struct row
    ///   per buffered value
    ///
    /// Rows are emitted in arrival order — unlike the builtin we do *not*
    /// pre-sort partial states, because the merging accumulator (always this
    /// same type; both plan phases come from the same UDAF) sorts once at
    /// final evaluation.
    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let struct_fields = Fields::from(self.ordering_fields.clone());
        let struct_dt = DataType::Struct(struct_fields.clone());
        if self.num_rows() == 0 {
            let empty_cols: Vec<ArrayRef> = self
                .ordering_fields
                .iter()
                .map(|f| new_empty_array(f.data_type()))
                .collect();
            let empty_struct: ArrayRef =
                Arc::new(StructArray::try_new(struct_fields, empty_cols, None)?);
            return Ok(vec![
                ScalarValue::new_null_list(self.value_type.clone(), true, 1),
                single_row_list(struct_dt, empty_struct),
            ]);
        }
        let (values, ord_cols) = self.concat_columns()?;
        let struct_arr: ArrayRef = Arc::new(StructArray::try_new(struct_fields, ord_cols, None)?);
        Ok(vec![
            single_row_list(self.value_type.clone(), values),
            single_row_list(struct_dt, struct_arr),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states.len() < 2 {
            return exec_err!(
                "array_agg ORDER BY merge: expected 2 state columns, got {}",
                states.len()
            );
        }
        let Some(vals_list) = states[0].as_any().downcast_ref::<ListArray>() else {
            return exec_err!("array_agg ORDER BY merge: state[0] must be a List array");
        };
        let Some(ords_list) = states[1].as_any().downcast_ref::<ListArray>() else {
            return exec_err!("array_agg ORDER BY merge: state[1] must be a List array");
        };
        for i in 0..vals_list.len() {
            if vals_list.is_null(i) {
                continue; // Empty partial (NULL values list).
            }
            let vals = vals_list.value(i);
            if vals.is_empty() {
                continue;
            }
            let ords = ords_list.value(i);
            let Some(structs) = ords.as_any().downcast_ref::<StructArray>() else {
                return exec_err!("array_agg ORDER BY merge: orderings must be Struct rows");
            };
            if structs.len() != vals.len() {
                return exec_err!(
                    "array_agg ORDER BY merge: {} values but {} ordering rows",
                    vals.len(),
                    structs.len()
                );
            }
            let cols: Vec<ArrayRef> = structs.columns().to_vec();
            self.push_chunk(vals, cols);
        }
        Ok(())
    }
}

/// Vectorized [`GroupsAccumulator`] for `array_agg(expr ORDER BY keys...)` in a
/// grouped (`GROUP BY`) scan.
///
/// # Why this exists
/// Without a `GroupsAccumulator`, DataFusion serves a grouped ordered
/// `array_agg` by wrapping [`OrderedArrayAggAccumulator`] in
/// `GroupsAccumulatorAdapter`.  The adapter, for every input batch, slices the
/// batch once per group it touches and calls `update_batch` per group — so the
/// per-row/per-group dispatch cost grows with both row count and group count.
/// At the 1M-row benchmark shape (`ARRAY_AGG(status ORDER BY created_at DESC)
/// GROUP BY user_id`) that adapter overhead dominated.
///
/// # Strategy (mirrors the builtin `ArrayAggGroupsAccumulator`, plus ordering)
/// `update_batch` is O(1) Arc clones plus one `(group, row)` entry per row.
/// All real work is deferred to `evaluate`/`state`:
/// 1. `interleave` the buffered value column and each ordering column down to
///    just the rows of the groups being emitted, in entry order, building a
///    parallel `group_id` column.
/// 2. one global `lexsort_to_indices` keyed on `[group_id ASC, sortkeys...]`
///    (NULLS-first defaults for the synthetic group key never matter — group
///    ids are non-null) — a single vectorized sort yields rows that are both
///    grouped by `group_id` *and* ordered within each group.
/// 3. a counting pass over `group_id` builds the `ListArray` offsets + null
///    buffer (empty groups → SQL NULL, matching PG and the builtin).
/// 4. one `take` materialises the flat values backing array.
///
/// Output and partial-state layouts are byte-identical to
/// [`OrderedArrayAggAccumulator`] / the builtin: result is `List<item>`;
/// partial state is `[List<item>, List<Struct<ordering cols>>]` with values in
/// arrival order (the final phase sorts once after merging).
#[derive(Debug)]
struct OrderedArrayAggGroupsAccumulator {
    /// Element type of the aggregated column.
    value_type: DataType,
    /// Struct fields for the ordering columns in the partial state
    /// (names/types must match the delegated `state_fields` declaration).
    ordering_fields: Vec<FieldRef>,
    /// Sort direction / null placement per ordering column.
    sort_options: Vec<SortOptions>,
    /// Buffered value chunks, in arrival order.
    value_chunks: Vec<ArrayRef>,
    /// Buffered ordering-column chunks; `ordering_chunks[k][j]` is ordering
    /// column `j` of chunk `k` (same row count as `value_chunks[k]`).
    ordering_chunks: Vec<Vec<ArrayRef>>,
    /// `entries[k]` are the `(group_idx, row_idx)` pairs contributed by chunk
    /// `k` (already filtered for `opt_filter`); the chunk index is implicit.
    entries: Vec<Vec<(u32, u32)>>,
    /// Largest `total_num_groups` seen so far.
    num_groups: usize,
    /// Incrementally tracked slice memory, for `size()` accounting.
    approx_bytes: usize,
}

impl OrderedArrayAggGroupsAccumulator {
    fn new(
        value_type: DataType,
        ordering_fields: Vec<FieldRef>,
        sort_options: Vec<SortOptions>,
    ) -> Self {
        Self {
            value_type,
            ordering_fields,
            sort_options,
            value_chunks: vec![],
            ordering_chunks: vec![],
            entries: vec![],
            num_groups: 0,
            approx_bytes: 0,
        }
    }

    fn clear_state(&mut self) {
        // `size()` measures Vec capacity, so allocate fresh buffers rather than
        // `clear()` so emitted memory is actually released back.
        self.value_chunks = vec![];
        self.ordering_chunks = vec![];
        self.entries = vec![];
        self.num_groups = 0;
        self.approx_bytes = 0;
    }

    /// Record one buffered chunk plus its `(group, row)` entries.
    fn push_chunk(&mut self, values: ArrayRef, ordering: Vec<ArrayRef>, entries: Vec<(u32, u32)>) {
        // `get_slice_memory_size` counts only this slice's rows so shared
        // parent buffers aren't multiply-counted.
        self.approx_bytes += values.to_data().get_slice_memory_size().unwrap_or(0);
        for col in &ordering {
            self.approx_bytes += col.to_data().get_slice_memory_size().unwrap_or(0);
        }
        self.approx_bytes += entries.capacity() * std::mem::size_of::<(u32, u32)>();
        self.value_chunks.push(values);
        self.ordering_chunks.push(ordering);
        self.entries.push(entries);
    }

    /// The empty `List<Struct<ordering>>` partial-state column shape used when
    /// a group has no rows.
    fn empty_orderings_list(&self, len: usize) -> DFResult<ArrayRef> {
        let struct_fields = Fields::from(self.ordering_fields.clone());
        let field = Arc::new(Field::new_list_field(DataType::Struct(struct_fields), true));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32; len + 1]));
        let empty_cols: Vec<ArrayRef> = self
            .ordering_fields
            .iter()
            .map(|f| new_empty_array(f.data_type()))
            .collect();
        let values: ArrayRef = Arc::new(StructArray::try_new(
            Fields::from(self.ordering_fields.clone()),
            empty_cols,
            None,
        )?);
        Ok(Arc::new(ListArray::new(field, offsets, values, None)))
    }

    /// Number of groups to emit, given `emit_to`.
    fn emit_groups(&self, emit_to: EmitTo) -> usize {
        match emit_to {
            EmitTo::All => self.num_groups,
            EmitTo::First(n) => n.min(self.num_groups),
        }
    }

    /// Gather, for the entries belonging to groups `0..emit_groups`, the value
    /// column, each ordering column, and a parallel `group_id` column — all in
    /// entry order.  Returns `(group_ids, values, ordering_cols)`.
    fn gather_emitted(
        &self,
        emit_groups: usize,
    ) -> DFResult<(UInt32Array, ArrayRef, Vec<ArrayRef>)> {
        let mut group_ids: Vec<u32> = Vec::new();
        let mut interleave_idx: Vec<(usize, usize)> = Vec::new();
        for (chunk_idx, ents) in self.entries.iter().enumerate() {
            for &(g, r) in ents {
                if (g as usize) < emit_groups {
                    group_ids.push(g);
                    interleave_idx.push((chunk_idx, r as usize));
                }
            }
        }
        let values = self
            .interleave_chunk_col(&interleave_idx, &self.value_type, |k| &self.value_chunks[k])?;
        let mut ord_cols = Vec::with_capacity(self.sort_options.len());
        for j in 0..self.sort_options.len() {
            let col = self.interleave_chunk_col(
                &interleave_idx,
                self.ordering_fields[j].data_type(),
                |k| &self.ordering_chunks[k][j],
            )?;
            ord_cols.push(col);
        }
        Ok((UInt32Array::from(group_ids), values, ord_cols))
    }

    /// `arrow::compute::interleave` over per-chunk source arrays selected by
    /// `pick`, using `(chunk_idx, row_idx)` pairs.  When there are no indices,
    /// returns an empty array of `fallback_type` (used when no chunk exists to
    /// infer the type from — `interleave` requires ≥1 source array).
    fn interleave_chunk_col<'a>(
        &'a self,
        indices: &[(usize, usize)],
        fallback_type: &DataType,
        pick: impl Fn(usize) -> &'a ArrayRef,
    ) -> DFResult<ArrayRef> {
        if indices.is_empty() || self.value_chunks.is_empty() {
            return Ok(new_empty_array(fallback_type));
        }
        let sources: Vec<&dyn Array> = (0..self.value_chunks.len())
            .map(|k| pick(k).as_ref())
            .collect();
        Ok(datafusion::arrow::compute::interleave(&sources, indices)?)
    }
}

impl GroupsAccumulator for OrderedArrayAggGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        let n_ord = self.sort_options.len();
        if values.len() < 1 + n_ord {
            return exec_err!(
                "array_agg ORDER BY: expected {} input columns, got {}",
                1 + n_ord,
                values.len()
            );
        }
        self.num_groups = self.num_groups.max(total_num_groups);
        let input = &values[0];
        if input.is_empty() {
            return Ok(());
        }
        // PG semantics: NULL *values* are kept (array_agg does not drop NULLs),
        // so we never consult the value's null buffer — only `opt_filter`.
        let mut entries = Vec::with_capacity(group_indices.len());
        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            if let Some(filter) = opt_filter {
                if filter.is_null(row_idx) || !filter.value(row_idx) {
                    continue;
                }
            }
            entries.push((group_idx as u32, row_idx as u32));
        }
        if entries.is_empty() {
            return Ok(());
        }
        self.push_chunk(Arc::clone(input), values[1..1 + n_ord].to_vec(), entries);
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        let emit_groups = self.emit_groups(emit_to);
        let (group_ids, values, ord_cols) = self.gather_emitted(emit_groups)?;
        let total_rows = group_ids.len();

        // Sort all gathered rows by [group_id, sortkeys...] in one shot: rows
        // come out grouped by group and ordered within each group.
        let flat_values = if total_rows == 0 {
            new_empty_array(&self.value_type)
        } else {
            let perm = self.lexsort_grouped(&group_ids, &ord_cols)?;
            take(values.as_ref(), &perm, None)?
        };

        // Count rows per emitted group (group_ids are unsorted here, which is
        // fine — counting is order-independent) to build list offsets + nulls.
        let mut counts = vec![0u32; emit_groups];
        for i in 0..total_rows {
            counts[group_ids.value(i) as usize] += 1;
        }
        let (offsets, nulls) = build_list_offsets(&counts);

        self.advance_state(emit_to, emit_groups);

        let field = Arc::new(Field::new_list_field(self.value_type.clone(), true));
        Ok(Arc::new(ListArray::new(field, offsets, flat_values, nulls)))
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let emit_groups = self.emit_groups(emit_to);
        // Partial state emits values in *arrival* order (not sorted): the final
        // phase's accumulator sorts once after merging.  So we do NOT lexsort
        // here — we only counting-sort into per-group ranges.
        let emit_g = emit_groups as u32;
        let mut counts = vec![0u32; emit_groups];
        for ents in &self.entries {
            for &(g, _) in ents {
                if g < emit_g {
                    counts[g as usize] += 1;
                }
            }
        }
        // Prefix sum → per-group write positions (mutated as we scatter).
        let mut wp = Vec::with_capacity(emit_groups);
        let mut cur = 0u32;
        for &c in &counts {
            wp.push(cur);
            cur += c;
        }
        let total_rows = cur as usize;

        // Scatter entries into group order.
        let mut interleave_idx = vec![(0usize, 0usize); total_rows];
        for (chunk_idx, ents) in self.entries.iter().enumerate() {
            for &(g, r) in ents {
                if g < emit_g {
                    let p = wp[g as usize] as usize;
                    interleave_idx[p] = (chunk_idx, r as usize);
                    wp[g as usize] += 1;
                }
            }
        }

        let (offsets, nulls) = build_list_offsets(&counts);

        // Values list column.
        let values_flat = self
            .interleave_chunk_col(&interleave_idx, &self.value_type, |k| &self.value_chunks[k])?;
        let vfield = Arc::new(Field::new_list_field(self.value_type.clone(), true));
        let values_list: ArrayRef = Arc::new(ListArray::new(
            vfield,
            offsets.clone(),
            values_flat,
            nulls.clone(),
        ));

        // Orderings list column: one Struct row per value, columns in the same
        // group order; the list null buffer matches the values list so empty
        // groups become NULL ordering lists too.
        let orderings_list: ArrayRef = if self.sort_options.is_empty() {
            self.empty_orderings_list(emit_groups)?
        } else {
            let mut struct_cols = Vec::with_capacity(self.sort_options.len());
            for j in 0..self.sort_options.len() {
                let col = self.interleave_chunk_col(
                    &interleave_idx,
                    self.ordering_fields[j].data_type(),
                    |k| &self.ordering_chunks[k][j],
                )?;
                struct_cols.push(col);
            }
            let struct_fields = Fields::from(self.ordering_fields.clone());
            let struct_arr: ArrayRef = Arc::new(StructArray::try_new(
                struct_fields.clone(),
                struct_cols,
                None,
            )?);
            let sfield = Arc::new(Field::new_list_field(DataType::Struct(struct_fields), true));
            Arc::new(ListArray::new(sfield, offsets, struct_arr, nulls))
        };

        self.advance_state(emit_to, emit_groups);
        Ok(vec![values_list, orderings_list])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        if values.len() < 2 {
            return exec_err!(
                "array_agg ORDER BY merge: expected 2 state columns, got {}",
                values.len()
            );
        }
        self.num_groups = self.num_groups.max(total_num_groups);
        let Some(vals_list) = values[0].as_any().downcast_ref::<ListArray>() else {
            return exec_err!("array_agg ORDER BY merge: state[0] must be a List array");
        };
        let Some(ords_list) = values[1].as_any().downcast_ref::<ListArray>() else {
            return exec_err!("array_agg ORDER BY merge: state[1] must be a List array");
        };
        // Each input row is one partial group.  Its list element holds that
        // group's buffered values + ordering structs (in arrival order); we
        // push them as one chunk with entries pointing at the current group.
        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            if vals_list.is_null(row_idx) {
                continue;
            }
            let vals = vals_list.value(row_idx);
            let len = vals.len();
            if len == 0 {
                continue;
            }
            let ords = ords_list.value(row_idx);
            let Some(structs) = ords.as_any().downcast_ref::<StructArray>() else {
                return exec_err!("array_agg ORDER BY merge: orderings must be Struct rows");
            };
            if structs.len() != len {
                return exec_err!(
                    "array_agg ORDER BY merge: {} values but {} ordering rows",
                    len,
                    structs.len()
                );
            }
            let entries: Vec<(u32, u32)> = (0..len as u32).map(|r| (group_idx as u32, r)).collect();
            self.push_chunk(vals, structs.columns().to_vec(), entries);
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.approx_bytes
            + self.value_chunks.capacity() * std::mem::size_of::<ArrayRef>()
            + self
                .ordering_chunks
                .iter()
                .map(|c| c.capacity() * std::mem::size_of::<ArrayRef>())
                .sum::<usize>()
            + self.entries.capacity() * std::mem::size_of::<Vec<(u32, u32)>>()
    }
}

impl OrderedArrayAggGroupsAccumulator {
    /// One global lexsort keyed on `[group_id ASC, sortkeys...]`.  Group ids are
    /// never null, so the synthetic leading key needs no special null handling;
    /// the trailing keys honour their per-column `SortOptions` (ASC/DESC,
    /// NULLS FIRST/LAST), exactly like the per-group accumulator.
    fn lexsort_grouped(
        &self,
        group_ids: &UInt32Array,
        ord_cols: &[ArrayRef],
    ) -> DFResult<UInt32Array> {
        let mut sort_cols: Vec<SortColumn> = Vec::with_capacity(1 + ord_cols.len());
        sort_cols.push(SortColumn {
            values: Arc::new(group_ids.clone()) as ArrayRef,
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        });
        for (col, opts) in ord_cols.iter().zip(self.sort_options.iter()) {
            sort_cols.push(SortColumn {
                values: Arc::clone(col),
                options: Some(*opts),
            });
        }
        Ok(lexsort_to_indices(&sort_cols, None)?)
    }

    /// Release/renumber state after an emit.  `EmitTo::All` resets everything;
    /// `EmitTo::First(n)` drops emitted groups, renumbers the rest down by `n`,
    /// and rebuilds chunks so emitted rows no longer pin buffers.
    fn advance_state(&mut self, emit_to: EmitTo, emit_groups: usize) {
        match emit_to {
            EmitTo::All => self.clear_state(),
            EmitTo::First(_) => {
                let emit_g = emit_groups as u32;
                let old_values = std::mem::take(&mut self.value_chunks);
                let old_orderings = std::mem::take(&mut self.ordering_chunks);
                let old_entries = std::mem::take(&mut self.entries);
                self.approx_bytes = 0;
                for ((vals, ords), ents) in
                    old_values.into_iter().zip(old_orderings).zip(old_entries)
                {
                    let retained: Vec<(u32, u32)> = ents
                        .into_iter()
                        .filter(|(g, _)| *g >= emit_g)
                        .map(|(g, r)| (g - emit_g, r))
                        .collect();
                    if !retained.is_empty() {
                        // Keep the original (possibly sliced) chunk; entries
                        // still index into it by row. Chunks fully drained are
                        // dropped, freeing their buffers.
                        self.push_chunk(vals, ords, retained);
                    }
                }
                self.num_groups = self.num_groups.saturating_sub(emit_groups);
            }
        }
    }
}

/// Build `ListArray` offsets + null buffer from per-group counts: a group with
/// zero rows becomes a NULL list element (PG: `array_agg` over no rows is NULL,
/// not `'{}'`), a non-empty group a non-null list of that length.
fn build_list_offsets(counts: &[u32]) -> (OffsetBuffer<i32>, Option<NullBuffer>) {
    let mut offsets = Vec::<i32>::with_capacity(counts.len() + 1);
    offsets.push(0);
    let mut nulls = NullBufferBuilder::new(counts.len());
    let mut cur = 0i32;
    for &c in counts {
        if c == 0 {
            nulls.append_null();
        } else {
            nulls.append_non_null();
        }
        cur += c as i32;
        offsets.push(cur);
    }
    (
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        nulls.finish(),
    )
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

// ── unit tests: ordered array_agg ─────────────────────────────────────────────

#[cfg(test)]
mod ordered_array_agg_tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray as ArrowStringArray};
    use datafusion::logical_expr::Accumulator;
    use std::sync::Arc;

    fn opts(descending: bool, nulls_first: bool) -> SortOptions {
        SortOptions {
            descending,
            nulls_first,
        }
    }

    /// Single Int64 sort key over Utf8 values — the benchmark shape.
    fn make_acc(o: SortOptions) -> OrderedArrayAggAccumulator {
        OrderedArrayAggAccumulator::new(
            DataType::Utf8,
            vec![Arc::new(Field::new("created_at", DataType::Int64, true))],
            vec![o],
        )
    }

    /// Build the expected result through `ScalarValue::new_list_from_iter`,
    /// the exact constructor DataFusion's builtin ordered array_agg uses for
    /// its results — full `ScalarValue` equality therefore proves the output
    /// format (field name, nullability, type, data) is unchanged.
    fn expected_list(vals: Vec<ScalarValue>, dt: &DataType) -> ScalarValue {
        ScalarValue::List(ScalarValue::new_list_from_iter(vals.into_iter(), dt, true))
    }

    fn utf8(s: &str) -> ScalarValue {
        ScalarValue::Utf8(Some(s.to_string()))
    }

    fn update(
        acc: &mut OrderedArrayAggAccumulator,
        vals: Vec<Option<&str>>,
        keys: Vec<Option<i64>>,
    ) {
        let v: ArrayRef = Arc::new(ArrowStringArray::from(vals));
        let k: ArrayRef = Arc::new(Int64Array::from(keys));
        acc.update_batch(&[v, k]).unwrap();
    }

    /// `ARRAY_AGG(status ORDER BY created_at DESC)` — PG default for DESC is
    /// NULLS FIRST, hence `nulls_first: true`.
    #[test]
    fn array_agg_order_by_desc() {
        let mut acc = make_acc(opts(true, true));
        update(
            &mut acc,
            vec![Some("a"), Some("b"), Some("c"), Some("d")],
            vec![Some(2), Some(4), Some(1), Some(3)],
        );
        let result = acc.evaluate().unwrap();
        // Keys desc: 4, 3, 2, 1 → values b, d, a, c.
        let expected = expected_list(
            vec![utf8("b"), utf8("d"), utf8("a"), utf8("c")],
            &DataType::Utf8,
        );
        assert_eq!(result, expected, "DESC sort by key");
    }

    /// `ARRAY_AGG(status ORDER BY created_at ASC)` — PG default for ASC is
    /// NULLS LAST, hence `nulls_first: false`.
    #[test]
    fn array_agg_order_by_asc() {
        let mut acc = make_acc(opts(false, false));
        update(
            &mut acc,
            vec![Some("a"), Some("b"), Some("c"), Some("d")],
            vec![Some(2), Some(4), Some(1), Some(3)],
        );
        let result = acc.evaluate().unwrap();
        // Keys asc: 1, 2, 3, 4 → values c, a, d, b.
        let expected = expected_list(
            vec![utf8("c"), utf8("a"), utf8("d"), utf8("b")],
            &DataType::Utf8,
        );
        assert_eq!(result, expected, "ASC sort by key");
    }

    /// NULL sort keys follow `SortOptions.nulls_first`, matching what the
    /// SQL planner passes (PG defaults: ASC → NULLS LAST, DESC → NULLS FIRST).
    #[test]
    fn array_agg_null_sort_keys() {
        // ASC NULLS LAST: the NULL-keyed row goes to the end.
        let mut acc = make_acc(opts(false, false));
        update(
            &mut acc,
            vec![Some("x"), Some("y"), Some("z")],
            vec![Some(2), None, Some(1)],
        );
        let expected = expected_list(vec![utf8("z"), utf8("x"), utf8("y")], &DataType::Utf8);
        assert_eq!(acc.evaluate().unwrap(), expected, "ASC NULLS LAST");

        // DESC NULLS FIRST: the NULL-keyed row goes to the front.
        let mut acc = make_acc(opts(true, true));
        update(
            &mut acc,
            vec![Some("x"), Some("y"), Some("z")],
            vec![Some(2), None, Some(1)],
        );
        let expected = expected_list(vec![utf8("y"), utf8("x"), utf8("z")], &DataType::Utf8);
        assert_eq!(acc.evaluate().unwrap(), expected, "DESC NULLS FIRST");
    }

    /// NULL *values* are kept (PG array_agg does not drop NULLs) and travel
    /// with their sort key.
    #[test]
    fn array_agg_null_values_preserved() {
        let mut acc = make_acc(opts(false, false));
        update(
            &mut acc,
            vec![Some("a"), None, Some("b")],
            vec![Some(3), Some(1), Some(2)],
        );
        let expected = expected_list(
            vec![ScalarValue::Utf8(None), utf8("b"), utf8("a")],
            &DataType::Utf8,
        );
        assert_eq!(
            acc.evaluate().unwrap(),
            expected,
            "NULL value kept at key 1"
        );
    }

    /// Multiple `update_batch` calls (large group spanning many record
    /// batches) must produce one globally sorted array.
    #[test]
    fn array_agg_multi_batch_group() {
        let mut acc = OrderedArrayAggAccumulator::new(
            DataType::Int64,
            vec![Arc::new(Field::new("k", DataType::Int64, true))],
            vec![opts(false, false)],
        );
        // 3 batches of 500 rows each; value i carries key 1500 - i, so the
        // ascending-by-key result is the values in reverse insertion order.
        for chunk in 0..3i64 {
            let vals: Vec<i64> = (chunk * 500..(chunk + 1) * 500).collect();
            let keys: Vec<i64> = vals.iter().map(|i| 1500 - i).collect();
            let v: ArrayRef = Arc::new(Int64Array::from(vals));
            let k: ArrayRef = Arc::new(Int64Array::from(keys));
            acc.update_batch(&[v, k]).unwrap();
        }
        let expected = expected_list(
            (0..1500i64)
                .rev()
                .map(|i| ScalarValue::Int64(Some(i)))
                .collect(),
            &DataType::Int64,
        );
        assert_eq!(
            acc.evaluate().unwrap(),
            expected,
            "1500 rows over 3 batches"
        );
    }

    /// Two sort keys: primary ASC, secondary DESC (exercises the multi-column
    /// lexsort path).
    #[test]
    fn array_agg_two_sort_keys() {
        let mut acc = OrderedArrayAggAccumulator::new(
            DataType::Utf8,
            vec![
                Arc::new(Field::new("k1", DataType::Int64, true)),
                Arc::new(Field::new("k2", DataType::Int64, true)),
            ],
            vec![opts(false, false), opts(true, true)],
        );
        let v: ArrayRef = Arc::new(ArrowStringArray::from(vec!["a", "b", "c", "d"]));
        let k1: ArrayRef = Arc::new(Int64Array::from(vec![2i64, 1, 2, 1]));
        let k2: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 2, 2]));
        acc.update_batch(&[v, k1, k2]).unwrap();
        // (k1 asc, k2 desc): (1,2)=d, (1,1)=b, (2,2)=c, (2,1)=a.
        let expected = expected_list(
            vec![utf8("d"), utf8("b"), utf8("c"), utf8("a")],
            &DataType::Utf8,
        );
        assert_eq!(acc.evaluate().unwrap(), expected, "two-key lexsort");
    }

    /// Zero rows → NULL (PG: array_agg over no rows is NULL, not '{}'),
    /// byte-identical to the builtin's `new_null_list` result.
    #[test]
    fn array_agg_empty_returns_null_list() {
        let mut acc = make_acc(opts(true, true));
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::new_null_list(DataType::Utf8, true, 1),
            "empty group must be NULL list"
        );
    }

    /// state() + merge_batch() across two partials must yield the same fully
    /// sorted result as a single accumulator seeing all rows.
    #[test]
    fn array_agg_state_merge_round_trip() {
        let mut acc1 = make_acc(opts(false, false));
        update(
            &mut acc1,
            vec![Some("x"), Some("z")],
            vec![Some(1), Some(3)],
        );
        let state1 = acc1.state().unwrap();

        let mut acc2 = make_acc(opts(false, false));
        update(&mut acc2, vec![Some("y")], vec![Some(2)]);
        let state2 = acc2.state().unwrap();

        // Assemble the two partial states into the state arrays the final
        // phase receives (one row per partial).
        let col0 = ScalarValue::iter_to_array(vec![state1[0].clone(), state2[0].clone()]).unwrap();
        let col1 = ScalarValue::iter_to_array(vec![state1[1].clone(), state2[1].clone()]).unwrap();

        let mut merged = make_acc(opts(false, false));
        merged.merge_batch(&[col0, col1]).unwrap();
        let expected = expected_list(vec![utf8("x"), utf8("y"), utf8("z")], &DataType::Utf8);
        assert_eq!(merged.evaluate().unwrap(), expected, "merged partials");
    }

    /// Merging an empty partial (NULL values list) is a no-op.
    #[test]
    fn array_agg_merge_skips_empty_partial() {
        let mut empty = make_acc(opts(false, false));
        let empty_state = empty.state().unwrap();

        let mut acc = make_acc(opts(false, false));
        update(&mut acc, vec![Some("a")], vec![Some(1)]);
        let full_state = acc.state().unwrap();

        let col0 = ScalarValue::iter_to_array(vec![empty_state[0].clone(), full_state[0].clone()])
            .unwrap();
        let col1 = ScalarValue::iter_to_array(vec![empty_state[1].clone(), full_state[1].clone()])
            .unwrap();

        let mut merged = make_acc(opts(false, false));
        merged.merge_batch(&[col0, col1]).unwrap();
        let expected = expected_list(vec![utf8("a")], &DataType::Utf8);
        assert_eq!(
            merged.evaluate().unwrap(),
            expected,
            "empty partial skipped"
        );
    }

    /// The accumulator's emitted state must match the schema declared by
    /// `PgArrayAggUdaf::state_fields` (delegated to the builtin) — this is
    /// the contract that keeps partial aggregation plans schema-valid.
    #[test]
    fn array_agg_state_layout_matches_declared_state_fields() {
        let ordering_fields: Vec<FieldRef> =
            vec![Arc::new(Field::new("created_at", DataType::Int64, true))];
        let udaf = PgArrayAggUdaf::new();
        let input_fields: Vec<FieldRef> =
            vec![Arc::new(Field::new("status", DataType::Utf8, true))];
        let return_field: FieldRef = Arc::new(Field::new(
            "array_agg",
            udaf.return_type(&[DataType::Utf8]).unwrap(),
            true,
        ));
        let declared = udaf
            .state_fields(StateFieldsArgs {
                name: "array_agg",
                input_fields: &input_fields,
                return_field,
                ordering_fields: &ordering_fields,
                is_distinct: false,
            })
            .unwrap();

        let mut acc = OrderedArrayAggAccumulator::new(
            DataType::Utf8,
            ordering_fields,
            vec![opts(true, true)],
        );
        update(&mut acc, vec![Some("a")], vec![Some(1)]);
        let state = acc.state().unwrap();

        assert_eq!(state.len(), declared.len(), "state column count");
        for (sv, field) in state.iter().zip(declared.iter()) {
            assert_eq!(
                &sv.data_type(),
                field.data_type(),
                "state column {} type must match declared schema",
                field.name()
            );
        }

        // Empty-group state must use the identical layout too.
        let mut empty = OrderedArrayAggAccumulator::new(
            DataType::Utf8,
            vec![Arc::new(Field::new("created_at", DataType::Int64, true))],
            vec![opts(true, true)],
        );
        let state = empty.state().unwrap();
        for (sv, field) in state.iter().zip(declared.iter()) {
            assert_eq!(
                &sv.data_type(),
                field.data_type(),
                "empty state column {} type must match declared schema",
                field.name()
            );
        }
    }

    // ── GroupsAccumulator (vectorized grouped ORDER BY path) ────────────────
    //
    // These pin the new fast path that serves the 1M benchmark shape
    // (`ARRAY_AGG(status ORDER BY created_at DESC) GROUP BY user_id`).  The
    // primary correctness oracle is *differential*: the GroupsAccumulator must
    // produce, for every group, the byte-identical `List` the per-group
    // `OrderedArrayAggAccumulator` produces for that group's rows.

    use datafusion::arrow::array::BooleanArray as ArrowBooleanArray;
    // `EmitTo` and `GroupsAccumulator` come in via `use super::*`.

    fn make_groups_acc(value_type: DataType, o: SortOptions) -> OrderedArrayAggGroupsAccumulator {
        OrderedArrayAggGroupsAccumulator::new(
            value_type,
            vec![Arc::new(Field::new("created_at", DataType::Int64, true))],
            vec![o],
        )
    }

    /// Extract per-group `Vec<ScalarValue>` (or `None` for a NULL group) from a
    /// `List<item>` result array.
    fn list_groups(arr: &ArrayRef) -> Vec<Option<Vec<ScalarValue>>> {
        let list = arr
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("List result");
        (0..list.len())
            .map(|i| {
                if list.is_null(i) {
                    None
                } else {
                    let inner = list.value(i);
                    Some(
                        (0..inner.len())
                            .map(|j| ScalarValue::try_from_array(inner.as_ref(), j).unwrap())
                            .collect(),
                    )
                }
            })
            .collect()
    }

    /// Run the per-group accumulator on one group's rows and return its
    /// `Vec<ScalarValue>` (or `None` for the zero-row/NULL case).
    fn ref_group(
        value_type: DataType,
        o: SortOptions,
        vals: ArrayRef,
        keys: ArrayRef,
    ) -> Option<Vec<ScalarValue>> {
        let mut acc = OrderedArrayAggAccumulator::new(
            value_type,
            vec![Arc::new(Field::new("created_at", DataType::Int64, true))],
            vec![o],
        );
        if !vals.is_empty() {
            acc.update_batch(&[vals, keys]).unwrap();
        }
        match acc.evaluate().unwrap() {
            ScalarValue::List(l) => {
                if l.is_null(0) {
                    None
                } else {
                    let inner = l.value(0);
                    Some(
                        (0..inner.len())
                            .map(|j| ScalarValue::try_from_array(inner.as_ref(), j).unwrap())
                            .collect(),
                    )
                }
            }
            other => panic!("unexpected scalar {other:?}"),
        }
    }

    /// Differential: one batch, two groups, DESC — GroupsAccumulator output
    /// must equal the per-group accumulator output for each group.
    #[test]
    fn groups_acc_two_groups_differential() {
        let o = opts(true, true);
        let mut acc = make_groups_acc(DataType::Utf8, o);
        // group 0: ("a",2) ("c",1) ; group 1: ("b",4) ("d",3)
        let vals: ArrayRef = Arc::new(ArrowStringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
        ]));
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![2i64, 4, 1, 3]));
        acc.update_batch(&[vals, keys], &[0, 1, 0, 1], None, 2)
            .unwrap();
        let out = acc.evaluate(EmitTo::All).unwrap();
        let got = list_groups(&out);

        let g0 = ref_group(
            DataType::Utf8,
            o,
            Arc::new(ArrowStringArray::from(vec![Some("a"), Some("c")])),
            Arc::new(Int64Array::from(vec![2i64, 1])),
        );
        let g1 = ref_group(
            DataType::Utf8,
            o,
            Arc::new(ArrowStringArray::from(vec![Some("b"), Some("d")])),
            Arc::new(Int64Array::from(vec![4i64, 3])),
        );
        assert_eq!(
            got,
            vec![g0, g1],
            "grouped output must match per-group oracle"
        );
    }

    /// NULL *values* are kept (PG semantics) and NULL sort keys honour
    /// `nulls_first`; differential against the per-group accumulator.
    #[test]
    fn groups_acc_nulls_kept() {
        let o = opts(false, false); // ASC NULLS LAST
        let mut acc = make_groups_acc(DataType::Utf8, o);
        // single group 0: ("x",NULL) (NULL,1) ("y",2)
        let vals: ArrayRef = Arc::new(ArrowStringArray::from(vec![Some("x"), None, Some("y")]));
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(1i64), Some(2)]));
        acc.update_batch(&[Arc::clone(&vals), Arc::clone(&keys)], &[0, 0, 0], None, 1)
            .unwrap();
        let got = list_groups(&acc.evaluate(EmitTo::All).unwrap());
        let oracle = ref_group(DataType::Utf8, o, vals, keys);
        assert_eq!(got, vec![oracle]);
        // The NULL value must still be present in the aggregate.
        assert!(
            got[0]
                .as_ref()
                .unwrap()
                .iter()
                .any(|s| matches!(s, ScalarValue::Utf8(None))),
            "NULL value dropped — violates PG array_agg semantics"
        );
    }

    /// An empty group (no rows scattered to it) must emit SQL NULL, not `'{}'`.
    #[test]
    fn groups_acc_empty_group_is_null() {
        let mut acc = make_groups_acc(DataType::Int64, opts(false, false));
        // total_num_groups=3 but only groups 0 and 2 get rows; group 1 empty.
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 30]));
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1]));
        acc.update_batch(&[vals, keys], &[0, 2], None, 3).unwrap();
        let got = list_groups(&acc.evaluate(EmitTo::All).unwrap());
        assert_eq!(got.len(), 3);
        assert!(got[0].is_some());
        assert_eq!(got[1], None, "empty group 1 must be NULL");
        assert!(got[2].is_some());
    }

    /// `opt_filter` skips rows (FILTER (WHERE ...)); Int64 values.
    #[test]
    fn groups_acc_opt_filter() {
        let o = opts(false, false);
        let mut acc = make_groups_acc(DataType::Int64, o);
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30, 40]));
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 3, 4]));
        let filter = ArrowBooleanArray::from(vec![true, false, true, false]);
        acc.update_batch(&[vals, keys], &[0, 0, 0, 0], Some(&filter), 1)
            .unwrap();
        let got = list_groups(&acc.evaluate(EmitTo::All).unwrap());
        // Only rows 0 and 2 survive: values 10, 30 (keys 1, 3 ASC).
        assert_eq!(
            got,
            vec![Some(vec![
                ScalarValue::Int64(Some(10)),
                ScalarValue::Int64(Some(30))
            ])]
        );
    }

    /// Multiple update_batch calls (group spans batches) merge into one group.
    #[test]
    fn groups_acc_multi_batch() {
        let o = opts(true, true); // DESC
        let mut acc = make_groups_acc(DataType::Int64, o);
        acc.update_batch(
            &[
                Arc::new(Int64Array::from(vec![1i64, 2])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1i64, 2])) as ArrayRef,
            ],
            &[0, 0],
            None,
            1,
        )
        .unwrap();
        acc.update_batch(
            &[
                Arc::new(Int64Array::from(vec![3i64])) as ArrayRef,
                Arc::new(Int64Array::from(vec![3i64])) as ArrayRef,
            ],
            &[0],
            None,
            1,
        )
        .unwrap();
        let got = list_groups(&acc.evaluate(EmitTo::All).unwrap());
        // keys desc 3,2,1 → values 3,2,1
        assert_eq!(
            got,
            vec![Some(vec![
                ScalarValue::Int64(Some(3)),
                ScalarValue::Int64(Some(2)),
                ScalarValue::Int64(Some(1)),
            ])]
        );
    }

    /// Partial-aggregation round trip: `state()` of a partial accumulator fed
    /// into `merge_batch()` of a final accumulator must reproduce the direct
    /// result.  Exercises the `[List<item>, List<Struct<ord>>]` state layout.
    #[test]
    fn groups_acc_state_merge_roundtrip() {
        let o = opts(true, true); // DESC
                                  // Direct (single-phase) reference.
        let mut direct = make_groups_acc(DataType::Int64, o);
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![5i64, 9, 1, 7]));
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![5i64, 9, 1, 7]));
        direct
            .update_batch(
                &[Arc::clone(&vals), Arc::clone(&keys)],
                &[0, 1, 0, 1],
                None,
                2,
            )
            .unwrap();
        let direct_out = list_groups(&direct.evaluate(EmitTo::All).unwrap());

        // Two-phase: partial accumulates, emits state; final merges, evaluates.
        let mut partial = make_groups_acc(DataType::Int64, o);
        partial
            .update_batch(&[vals, keys], &[0, 1, 0, 1], None, 2)
            .unwrap();
        let state = partial.state(EmitTo::All).unwrap();
        assert_eq!(state.len(), 2, "state must be [List<item>, List<Struct>]");
        let mut final_acc = make_groups_acc(DataType::Int64, o);
        final_acc.merge_batch(&state, &[0, 1], None, 2).unwrap();
        let merged_out = list_groups(&final_acc.evaluate(EmitTo::All).unwrap());

        assert_eq!(
            merged_out, direct_out,
            "two-phase result must match single-phase"
        );
    }

    /// `EmitTo::First(n)` emits the first n groups, renumbers the rest down by
    /// n, and keeps their state for a later emit.
    #[test]
    fn groups_acc_emit_first() {
        let o = opts(false, false);
        let mut acc = make_groups_acc(DataType::Int64, o);
        // groups 0,1,2
        acc.update_batch(
            &[
                Arc::new(Int64Array::from(vec![10i64, 20, 30])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1i64, 1, 1])) as ArrayRef,
            ],
            &[0, 1, 2],
            None,
            3,
        )
        .unwrap();
        let first = list_groups(&acc.evaluate(EmitTo::First(2)).unwrap());
        assert_eq!(first.len(), 2, "First(2) emits exactly 2 groups");
        assert_eq!(first[0], Some(vec![ScalarValue::Int64(Some(10))]));
        assert_eq!(first[1], Some(vec![ScalarValue::Int64(Some(20))]));
        // Remaining group (old index 2 → new index 0) survives.
        let rest = list_groups(&acc.evaluate(EmitTo::All).unwrap());
        assert_eq!(rest, vec![Some(vec![ScalarValue::Int64(Some(30))])]);
    }

    /// `size()` must be > 0 after buffering and account for the buffered slice
    /// memory (so DataFusion's memory accounting/spill thresholds work).
    #[test]
    fn groups_acc_size_accounts_buffers() {
        let mut acc = make_groups_acc(DataType::Int64, opts(false, false));
        let empty_size = acc.size();
        acc.update_batch(
            &[
                Arc::new(Int64Array::from((0..1000).collect::<Vec<i64>>())) as ArrayRef,
                Arc::new(Int64Array::from((0..1000).collect::<Vec<i64>>())) as ArrayRef,
            ],
            &(0..1000).map(|i| (i % 4) as usize).collect::<Vec<_>>(),
            None,
            4,
        )
        .unwrap();
        assert!(
            acc.size() > empty_size,
            "size() must grow after buffering 1000 rows"
        );
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
/// - `array_agg(...)` — replaces the DataFusion builtin by name with
///   [`PgArrayAggUdaf`], whose only behavioural difference is a vectorized
///   accumulator for the `ORDER BY` (non-DISTINCT) path.
pub(crate) fn register_json_agg_udafs(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(PgArrayAggUdaf::new()));
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
    ctx.register_udaf(AggregateUDF::from(FirstAggUdaf::new()));
    ctx.register_udaf(AggregateUDF::from(LastAggUdaf::new()));
}

// ── first(value, ts) / last(value, ts) — Timescale-compatible aggregates ─────
//
// Timescale semantics:
//   first(value_col, ts_col) → the value whose ts is the MINIMUM across the group
//   last(value_col,  ts_col) → the value whose ts is the MAXIMUM across the group
//
// NULL ts rows are ignored (Timescale spec). Ties (identical ts values in the
// same group) are broken by taking the lowest physical row index seen first —
// i.e. the first occurrence in scan order wins. This is deterministic for any
// given storage layout and is documented as the tie-breaking rule.
//
// State layout: two parallel JSON arrays: the running best_value and best_ts
// encoded as strings. We use a compact binary layout in state() to support
// partial aggregation: [best_value_json, best_ts_i64_str].
//
// GroupsAccumulator: not implemented. The existing UDAFs in this file (mode,
// percentile_disc) also do not implement GroupsAccumulator, so this matches
// the codebase idiom.

/// UDAF for `first(value, ts)` — returns value at minimum ts per group.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FirstAggUdaf {
    signature: Signature,
}

/// UDAF for `last(value, ts)` — returns value at maximum ts per group.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct LastAggUdaf {
    signature: Signature,
}

impl FirstAggUdaf {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl LastAggUdaf {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for FirstAggUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "first"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        // Return type = type of the first (value) argument.
        Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<Arc<Field>>> {
        // State: two UTF-8 fields — best_value (JSON-encoded) and best_ts (i64 µs as string).
        Ok(vec![
            Arc::new(Field::new(
                format!("{}_val", args.name),
                DataType::Utf8,
                true,
            )),
            Arc::new(Field::new(
                format!("{}_ts", args.name),
                DataType::Int64,
                true,
            )),
        ])
    }

    fn accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DFResult<Box<dyn datafusion::logical_expr::Accumulator>> {
        let value_type = args
            .expr_fields
            .first()
            .map(|f| f.data_type().clone())
            .unwrap_or(DataType::Null);
        Ok(Box::new(FirstLastAccumulator {
            value_type,
            best_value: None,
            best_ts: None,
            want_min: true,
        }))
    }
}

impl AggregateUDFImpl for LastAggUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "last"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<Arc<Field>>> {
        Ok(vec![
            Arc::new(Field::new(
                format!("{}_val", args.name),
                DataType::Utf8,
                true,
            )),
            Arc::new(Field::new(
                format!("{}_ts", args.name),
                DataType::Int64,
                true,
            )),
        ])
    }

    fn accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DFResult<Box<dyn datafusion::logical_expr::Accumulator>> {
        let value_type = args
            .expr_fields
            .first()
            .map(|f| f.data_type().clone())
            .unwrap_or(DataType::Null);
        Ok(Box::new(FirstLastAccumulator {
            value_type,
            best_value: None,
            best_ts: None,
            want_min: false,
        }))
    }
}

// ── shared accumulator ────────────────────────────────────────────────────────

/// Shared accumulator backing both `first` and `last`.
///
/// `want_min = true`  → keep the value at the **minimum** ts (first).
/// `want_min = false` → keep the value at the **maximum** ts (last).
///
/// Tie-breaking: when two rows share the identical ts µs value we keep the
/// value already stored (i.e. the first arrival in scan order), since the
/// comparison uses strict `<` / `>`. This is deterministic and documented.
#[derive(Debug)]
struct FirstLastAccumulator {
    value_type: DataType,
    /// The running best value, or None if no non-NULL ts row has been seen.
    best_value: Option<ScalarValue>,
    /// The running best ts (µs since epoch), or None if no qualifying row seen.
    best_ts: Option<i64>,
    /// true → first (min ts); false → last (max ts).
    want_min: bool,
}

impl FirstLastAccumulator {
    /// Extract microseconds since epoch from a single array slot.
    /// Returns None if the slot is null or the type is unsupported.
    fn extract_ts_us(arr: &dyn Array, i: usize) -> Option<i64> {
        use datafusion::arrow::array::*;
        if arr.is_null(i) {
            return None;
        }
        match arr.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .map(|a| a.value(i)),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => arr
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .map(|a| a.value(i) / 1_000),
            DataType::Timestamp(TimeUnit::Millisecond, _) => arr
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .map(|a| a.value(i) * 1_000),
            DataType::Timestamp(TimeUnit::Second, _) => arr
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .map(|a| a.value(i) * 1_000_000),
            DataType::Int64 => arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(i)),
            _ => None,
        }
    }

    /// Update the running best from one row.
    fn consider(&mut self, value: ScalarValue, ts_us: i64) {
        let is_better = match self.best_ts {
            None => true,
            Some(cur) => {
                if self.want_min {
                    ts_us < cur
                } else {
                    ts_us > cur
                }
            }
        };
        if is_better {
            self.best_value = Some(value);
            self.best_ts = Some(ts_us);
        }
    }
}

impl datafusion::logical_expr::Accumulator for FirstLastAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        if values.len() < 2 {
            return exec_err!("first/last requires 2 arguments (value, ts)");
        }
        let val_arr = &values[0];
        let ts_arr = &values[1];
        for i in 0..val_arr.len() {
            // Ignore rows where ts is NULL (Timescale spec).
            let Some(ts_us) = Self::extract_ts_us(ts_arr.as_ref(), i) else {
                continue;
            };
            let sv = ScalarValue::try_from_array(val_arr.as_ref(), i)?;
            self.consider(sv, ts_us);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(match &self.best_value {
            Some(sv) => sv.clone(),
            None => ScalarValue::try_from(&self.value_type).unwrap_or(ScalarValue::Null),
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.best_value.as_ref().map(|sv| sv.size()).unwrap_or(0)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        // State: [value_json_string, ts_i64_micros]
        let val_str = match &self.best_value {
            Some(sv) => {
                let j = scalar_value_to_json_string(sv);
                ScalarValue::Utf8(Some(j))
            }
            None => ScalarValue::Utf8(None),
        };
        let ts_sv = ScalarValue::Int64(self.best_ts);
        Ok(vec![val_str, ts_sv])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        use datafusion::arrow::array::{Int64Array, StringArray as StrArr};
        // states[0] = Utf8 (JSON-encoded value or NULL)
        // states[1] = Int64 (ts µs or NULL)
        if states.len() < 2 {
            return Ok(());
        }
        let val_arr = states[0].as_any().downcast_ref::<StrArr>();
        let ts_arr = states[1].as_any().downcast_ref::<Int64Array>();
        let (Some(val_arr), Some(ts_arr)) = (val_arr, ts_arr) else {
            return Ok(());
        };
        for i in 0..val_arr.len() {
            if ts_arr.is_null(i) {
                continue;
            }
            let ts_us = ts_arr.value(i);
            let sv = if val_arr.is_null(i) {
                ScalarValue::try_from(&self.value_type).unwrap_or(ScalarValue::Null)
            } else {
                json_to_scalar_value(val_arr.value(i), &self.value_type)
            };
            self.consider(sv, ts_us);
        }
        Ok(())
    }
}

// ── JSON encode/decode helpers for first/last state ───────────────────────────

// Encode a ScalarValue as a JSON *string* for the first/last UDAF partial
// state (state column is Utf8). Distinct from `scalar_value_to_json` above,
// which returns a `serde_json::Value` for the ordered-set aggregates' state.
fn scalar_value_to_json_string(sv: &ScalarValue) -> String {
    match sv {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            serde_json::to_string(s).unwrap_or_else(|_| "null".to_string())
        }
        ScalarValue::Boolean(Some(b)) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        ScalarValue::Int8(Some(v)) => v.to_string(),
        ScalarValue::Int16(Some(v)) => v.to_string(),
        ScalarValue::Int32(Some(v)) => v.to_string(),
        ScalarValue::Int64(Some(v)) => v.to_string(),
        ScalarValue::UInt8(Some(v)) => v.to_string(),
        ScalarValue::UInt16(Some(v)) => v.to_string(),
        ScalarValue::UInt32(Some(v)) => v.to_string(),
        ScalarValue::UInt64(Some(v)) => v.to_string(),
        ScalarValue::Float32(Some(v)) => v.to_string(),
        ScalarValue::Float64(Some(v)) => v.to_string(),
        ScalarValue::TimestampMicrosecond(Some(v), _) => v.to_string(),
        ScalarValue::TimestampNanosecond(Some(v), _) => v.to_string(),
        ScalarValue::TimestampMillisecond(Some(v), _) => v.to_string(),
        ScalarValue::TimestampSecond(Some(v), _) => v.to_string(),
        _ => "null".to_string(),
    }
}

fn json_to_scalar_value(s: &str, target_type: &DataType) -> ScalarValue {
    use std::str::FromStr;
    match target_type {
        DataType::Utf8 | DataType::LargeUtf8 => {
            // The JSON string is a quoted string; strip the quotes.
            let inner = serde_json::from_str::<serde_json::Value>(s)
                .ok()
                .and_then(|v| v.as_str().map(|s| s.to_owned()))
                .unwrap_or_else(|| s.to_owned());
            ScalarValue::Utf8(Some(inner))
        }
        DataType::Boolean => ScalarValue::Boolean(Some(s == "true")),
        DataType::Int8 => ScalarValue::Int8(s.parse().ok()),
        DataType::Int16 => ScalarValue::Int16(s.parse().ok()),
        DataType::Int32 => ScalarValue::Int32(s.parse().ok()),
        DataType::Int64 => ScalarValue::Int64(s.parse().ok()),
        DataType::UInt8 => ScalarValue::UInt8(s.parse().ok()),
        DataType::UInt16 => ScalarValue::UInt16(s.parse().ok()),
        DataType::UInt32 => ScalarValue::UInt32(s.parse().ok()),
        DataType::UInt64 => ScalarValue::UInt64(s.parse().ok()),
        DataType::Float32 => ScalarValue::Float32(s.parse::<f32>().ok()),
        DataType::Float64 => ScalarValue::Float64(s.parse::<f64>().ok()),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            ScalarValue::TimestampMicrosecond(s.parse::<i64>().ok(), tz.clone())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            ScalarValue::TimestampNanosecond(s.parse::<i64>().ok(), tz.clone())
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            ScalarValue::TimestampMillisecond(s.parse::<i64>().ok(), tz.clone())
        }
        DataType::Timestamp(TimeUnit::Second, tz) => {
            ScalarValue::TimestampSecond(s.parse::<i64>().ok(), tz.clone())
        }
        _ => ScalarValue::try_from(target_type).unwrap_or(ScalarValue::Null),
    }
}

// TimeUnit is needed for the DataType::Timestamp pattern matches in
// FirstLastAccumulator::extract_ts_us and merge_batch.
use datafusion::arrow::datatypes::TimeUnit;

// ── unit tests: first / last ──────────────────────────────────────────────────

#[cfg(test)]
mod first_last_tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, TimestampMicrosecondArray};
    use datafusion::logical_expr::Accumulator;
    use std::sync::Arc;

    fn make_first_acc(value_type: DataType) -> FirstLastAccumulator {
        FirstLastAccumulator {
            value_type,
            best_value: None,
            best_ts: None,
            want_min: true,
        }
    }

    fn make_last_acc(value_type: DataType) -> FirstLastAccumulator {
        FirstLastAccumulator {
            value_type,
            best_value: None,
            best_ts: None,
            want_min: false,
        }
    }

    /// first() over {(v=10, ts=100), (v=20, ts=50)} → 20 (ts=50 is minimum).
    #[test]
    fn first_returns_min_ts_value() {
        let mut acc = make_first_acc(DataType::Int32);
        let vals: ArrayRef = Arc::new(Int32Array::from(vec![10i32, 20]));
        let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![100i64, 50]));
        acc.update_batch(&[vals, ts]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int32(Some(20)));
    }

    /// last() over {(v=10, ts=100), (v=20, ts=50)} → 10 (ts=100 is maximum).
    #[test]
    fn last_returns_max_ts_value() {
        let mut acc = make_last_acc(DataType::Int32);
        let vals: ArrayRef = Arc::new(Int32Array::from(vec![10i32, 20]));
        let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![100i64, 50]));
        acc.update_batch(&[vals, ts]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int32(Some(10)));
    }

    /// NULL ts rows are ignored.
    #[test]
    fn null_ts_ignored() {
        let mut acc = make_first_acc(DataType::Int32);
        let vals: ArrayRef = Arc::new(Int32Array::from(vec![Some(1i32), Some(2), Some(3)]));
        // Row 0 has NULL ts → should be ignored; min ts is row 1 (ts=10).
        let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            None,
            Some(10i64),
            Some(20),
        ]));
        acc.update_batch(&[vals, ts]).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Int32(Some(2)),
            "row with NULL ts must be ignored; first = value at ts=10"
        );
    }

    /// Empty group → NULL.
    #[test]
    fn empty_group_returns_null() {
        let mut acc = make_first_acc(DataType::Int32);
        let result = acc.evaluate().unwrap();
        // ScalarValue::try_from(DataType::Int32) = Int32(None) which is NULL.
        assert_eq!(result, ScalarValue::Int32(None));
    }

    /// All-NULL ts → NULL result.
    #[test]
    fn all_null_ts_returns_null() {
        let mut acc = make_last_acc(DataType::Int32);
        let vals: ArrayRef = Arc::new(Int32Array::from(vec![Some(5i32), Some(6)]));
        let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>, None]));
        acc.update_batch(&[vals, ts]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int32(None));
    }

    /// Tie-breaking: two rows with identical ts — first arrival (lowest index) wins.
    #[test]
    fn tie_broken_by_first_arrival() {
        let mut acc = make_first_acc(DataType::Int32);
        let vals: ArrayRef = Arc::new(Int32Array::from(vec![99i32, 42]));
        // Both rows have the same ts; first in batch order (99) should win.
        let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![1000i64, 1000]));
        acc.update_batch(&[vals, ts]).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Int32(Some(99)),
            "tie: first arrival (index 0 = 99) must win"
        );
    }

    /// State/merge round-trip for first().
    #[test]
    fn first_state_merge_round_trip() {
        // Partial 1: (v=5, ts=200)
        let mut p1 = make_first_acc(DataType::Int32);
        let v1: ArrayRef = Arc::new(Int32Array::from(vec![5i32]));
        let t1: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![200i64]));
        p1.update_batch(&[v1, t1]).unwrap();
        let state1 = p1.state().unwrap();

        // Partial 2: (v=7, ts=100) — lower ts, should win in first()
        let mut p2 = make_first_acc(DataType::Int32);
        let v2: ArrayRef = Arc::new(Int32Array::from(vec![7i32]));
        let t2: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![100i64]));
        p2.update_batch(&[v2, t2]).unwrap();
        let state2 = p2.state().unwrap();

        // Merge states.
        let val_state: ArrayRef = Arc::new(datafusion::arrow::array::StringArray::from(vec![
            match &state1[0] {
                ScalarValue::Utf8(Some(s)) => s.as_str(),
                _ => "",
            },
            match &state2[0] {
                ScalarValue::Utf8(Some(s)) => s.as_str(),
                _ => "",
            },
        ]));
        let ts_state: ArrayRef = Arc::new(datafusion::arrow::array::Int64Array::from(vec![
            match &state1[1] {
                ScalarValue::Int64(Some(v)) => *v,
                _ => 0,
            },
            match &state2[1] {
                ScalarValue::Int64(Some(v)) => *v,
                _ => 0,
            },
        ]));
        let mut merged = make_first_acc(DataType::Int32);
        merged.merge_batch(&[val_state, ts_state]).unwrap();
        // first() should pick v=7 (ts=100 is minimum).
        assert_eq!(
            merged.evaluate().unwrap(),
            ScalarValue::Int32(Some(7)),
            "merge: first should pick the value at minimum ts"
        );
    }
}
