//! `APPROX_COUNT_DISTINCT(col)` — approximate distinct-count aggregate UDF.
//!
//! Uses the HyperLogLog (HLL) implementation from `basin-sketch` (hoisted
//! in Phase 5.14.B1 from the inline definition that lived here).
//!
//! ## Algorithm
//! - 2^14 = 16 384 u8 registers (b=14 → relative std-error ≈ 0.8 %)
//! - MurmurHash3-like mixing for speed and uniformity
//! - LogLog-Beta bias correction from Deudon 2017 (replaces HLL++ correction)
//! - Serialization: raw 16 384-byte `Vec<u8>` stored as `ScalarValue::Binary`
//!
//! ## Merge
//! Each partition serializes its HLL state. The combiner deserializes all
//! partial states and applies the standard "component-wise maximum" merge.
//!
//! ## Accuracy
//! On 17 distinct values the estimator returns 17 (exact — small-cardinality
//! correction dominates). On 1 000 000 distinct values the error is < 1 %.

use std::any::Any;
use std::sync::Arc;

use basin_sketch::hll::Hll;
use datafusion::arrow::array::{Array, ArrayRef, BinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, Result as DFResult};
use datafusion::logical_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

// HLL_M is needed for the size() estimate in the accumulator.
const HLL_M: usize = 1 << 14; // 16 384

// ── Accumulator ───────────────────────────────────────────────────────────────

#[derive(Debug)]
struct ApproxCountDistinctAccumulator {
    hll: Hll,
}

impl ApproxCountDistinctAccumulator {
    fn new() -> Self {
        Self { hll: Hll::new() }
    }
}

impl datafusion::logical_expr::Accumulator for ApproxCountDistinctAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let arr = &values[0];
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            hash_and_insert(arr.as_ref(), i, &mut self.hll)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.hll.cardinality() as i64)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + HLL_M
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.hll.to_bytes()))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states.is_empty() {
            return Ok(());
        }
        let arr = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "approx_count_distinct: expected Binary state array".into(),
                )
            })?;
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let bytes = arr.value(i);
            if let Some(other) = Hll::from_bytes(bytes) {
                self.hll.merge(&other);
            }
        }
        Ok(())
    }
}

/// Hash a single row in an Arrow array and insert it into the HLL.
fn hash_and_insert(arr: &dyn Array, idx: usize, hll: &mut Hll) -> DFResult<()> {
    use datafusion::arrow::array::{
        BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int8Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    };
    use datafusion::arrow::datatypes::DataType::*;

    macro_rules! hash_primitive {
        ($array_type:ty, $val:expr) => {{
            if let Some(a) = arr.as_any().downcast_ref::<$array_type>() {
                hll.insert(&a.value(idx).to_le_bytes());
                return Ok(());
            }
        }};
    }

    match arr.data_type() {
        Int8 => hash_primitive!(Int8Array, i8),
        Int16 => hash_primitive!(Int16Array, i16),
        Int32 => hash_primitive!(Int32Array, i32),
        Int64 => hash_primitive!(Int64Array, i64),
        UInt8 => hash_primitive!(UInt8Array, u8),
        UInt16 => hash_primitive!(UInt16Array, u16),
        UInt32 => hash_primitive!(UInt32Array, u32),
        UInt64 => hash_primitive!(UInt64Array, u64),
        Float32 => hash_primitive!(Float32Array, f32),
        Float64 => hash_primitive!(Float64Array, f64),
        Date32 => hash_primitive!(Date32Array, i32),
        Date64 => hash_primitive!(Date64Array, i64),
        Timestamp(_, _) => {
            if let Some(a) = arr.as_any().downcast_ref::<TimestampSecondArray>() {
                hll.insert(&a.value(idx).to_le_bytes());
            } else if let Some(a) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
                hll.insert(&a.value(idx).to_le_bytes());
            } else if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                hll.insert(&a.value(idx).to_le_bytes());
            } else if let Some(a) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
                hll.insert(&a.value(idx).to_le_bytes());
            }
        }
        Boolean => {
            if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
                hll.insert(&[a.value(idx) as u8]);
            }
        }
        Utf8 => {
            if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                hll.insert(a.value(idx).as_bytes());
            }
        }
        LargeUtf8 => {
            if let Some(a) = arr
                .as_any()
                .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
            {
                hll.insert(a.value(idx).as_bytes());
            }
        }
        Binary => {
            if let Some(a) = arr.as_any().downcast_ref::<BinaryArray>() {
                hll.insert(a.value(idx));
            }
        }
        LargeBinary => {
            if let Some(a) = arr
                .as_any()
                .downcast_ref::<datafusion::arrow::array::LargeBinaryArray>()
            {
                hll.insert(a.value(idx));
            }
        }
        other => {
            return exec_err!("APPROX_COUNT_DISTINCT: unsupported column type {:?}", other);
        }
    }
    Ok(())
}

// ── UDF impl ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ApproxCountDistinctUdaf {
    signature: Signature,
}

impl ApproxCountDistinctUdaf {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ApproxCountDistinctUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "approx_count_distinct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> DFResult<Box<dyn datafusion::logical_expr::Accumulator>> {
        Ok(Box::new(ApproxCountDistinctAccumulator::new()))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}_hll_state", args.name),
            DataType::Binary,
            true,
        ))])
    }
}

/// Register `APPROX_COUNT_DISTINCT` into a DataFusion `SessionContext`.
pub(crate) fn register_approx_count_distinct(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(ApproxCountDistinctUdaf::new()));
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn check_estimate(n: usize, tolerance: f64) {
        let mut hll = Hll::new();
        for i in 0..n {
            hll.insert(&(i as u64).to_le_bytes());
        }
        let est = hll.cardinality() as f64;
        let err = (est - n as f64).abs() / n as f64;
        assert!(
            err <= tolerance,
            "n={n}: estimate={est}, error={:.2} > {:.2}",
            err * 100.0,
            tolerance * 100.0
        );
    }

    #[test]
    fn hll_small_cardinality_exact() {
        check_estimate(17, 0.05);
    }

    #[test]
    fn hll_medium_cardinality() {
        check_estimate(1_000, 0.05);
    }

    #[test]
    fn hll_large_cardinality() {
        check_estimate(100_000, 0.05);
    }

    #[test]
    fn hll_merge_is_union() {
        let mut a = Hll::new();
        let mut b = Hll::new();
        for i in 0u64..500 {
            a.insert(&i.to_le_bytes());
        }
        for i in 500u64..1000 {
            b.insert(&i.to_le_bytes());
        }
        a.merge(&b);
        let est = a.cardinality() as f64;
        let err = (est - 1000.0).abs() / 1000.0;
        assert!(
            err <= 0.05,
            "merged estimate={est}, error={:.2}",
            err * 100.0
        );
    }

    #[test]
    fn hll_serialization_roundtrip() {
        let mut hll = Hll::new();
        for i in 0u64..200 {
            hll.insert(&i.to_le_bytes());
        }
        let bytes = hll.to_bytes();
        assert_eq!(bytes.len(), HLL_M);
        let restored = Hll::from_bytes(&bytes).expect("deserialize failed");
        assert_eq!(hll.cardinality(), restored.cardinality());
    }

    #[test]
    fn hll_empty_returns_zero() {
        let hll = Hll::new();
        assert_eq!(hll.cardinality(), 0);
    }
}
