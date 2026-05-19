//! `APPROX_PERCENTILE(col, p)` — approximate percentile aggregate UDF.
//!
//! Uses the t-digest implementation from `basin-sketch` (hoisted in Phase
//! 5.14.B1 from the inline definition that lived here).
//!
//! ## Algorithm
//! - Merging t-digest with fixed compression factor `delta = 100`.
//! - Serialization: 8-byte count + 16 bytes per centroid in `ScalarValue::Binary`.

use std::any::Any;
use std::sync::Arc;

use basin_sketch::tdigest::{Centroid, TDigest};
use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{exec_err, Result as DFResult};
use datafusion::logical_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    AggregateUDF, AggregateUDFImpl, ColumnarValue, Signature, TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

// ── Accumulator ───────────────────────────────────────────────────────────────

#[derive(Debug)]
struct ApproxPercentileAccumulator {
    digest: TDigest,
    /// The target percentile [0, 1]. Captured from the literal argument at
    /// accumulator construction time and stored in state so the combiner can
    /// reconstruct it.
    p: f64,
}

impl ApproxPercentileAccumulator {
    fn new(p: f64) -> Self {
        Self {
            digest: TDigest::new(),
            p,
        }
    }
}

impl datafusion::logical_expr::Accumulator for ApproxPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        // values[0] = col (numeric), values[1] = p (constant float)
        let col = &values[0];
        for i in 0..col.len() {
            if col.is_null(i) {
                continue;
            }
            let v = numeric_to_f64(col.as_ref(), i)?;
            self.digest.add(v);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.digest.total_weight == 0.0 {
            return Ok(ScalarValue::Float64(None));
        }
        let result = self.digest.quantile(self.p);
        Ok(ScalarValue::Float64(Some(result)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.digest.centroids.len() * std::mem::size_of::<Centroid>()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Binary(Some(self.digest.to_bytes())),
            ScalarValue::Float64(Some(self.p)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states.is_empty() {
            return Ok(());
        }
        let digest_arr = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "approx_percentile: expected Binary state[0] array".into(),
                )
            })?;
        let p_arr = states[1]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "approx_percentile: expected Float64 state[1] array".into(),
                )
            })?;

        for i in 0..digest_arr.len() {
            if digest_arr.is_null(i) {
                continue;
            }
            let bytes = digest_arr.value(i);
            if let Some(other) = TDigest::from_bytes(bytes) {
                self.digest.merge(&other);
            }
            // Update p from the first non-null partition (they must all agree).
            if !p_arr.is_null(i) {
                self.p = p_arr.value(i);
            }
        }
        Ok(())
    }
}

/// Convert a single numeric row at `idx` in `arr` to `f64`.
fn numeric_to_f64(arr: &dyn Array, idx: usize) -> DFResult<f64> {
    use datafusion::arrow::array::{
        Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array,
        UInt32Array, UInt64Array, UInt8Array,
    };
    use datafusion::arrow::datatypes::DataType::*;

    macro_rules! cast_prim {
        ($array_type:ty) => {{
            if let Some(a) = arr.as_any().downcast_ref::<$array_type>() {
                return Ok(a.value(idx) as f64);
            }
        }};
    }

    match arr.data_type() {
        Int8 => cast_prim!(Int8Array),
        Int16 => cast_prim!(Int16Array),
        Int32 => cast_prim!(Int32Array),
        Int64 => cast_prim!(Int64Array),
        UInt8 => cast_prim!(UInt8Array),
        UInt16 => cast_prim!(UInt16Array),
        UInt32 => cast_prim!(UInt32Array),
        UInt64 => cast_prim!(UInt64Array),
        Float32 => cast_prim!(Float32Array),
        Float64 => cast_prim!(Float64Array),
        other => {
            return exec_err!(
                "APPROX_PERCENTILE: unsupported column type {:?}",
                other
            );
        }
    }

    exec_err!(
        "APPROX_PERCENTILE: internal downcast failed for type {:?}",
        arr.data_type()
    )
}

// ── UDF impl ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ApproxPercentileUdaf {
    signature: Signature,
}

impl ApproxPercentileUdaf {
    pub fn new() -> Self {
        // Accepts (numeric_col, float_percentile) — the second arg is always a
        // Float64 literal.  We use a custom TypeSignature to document the
        // intent even though at runtime we validate the first arg ourselves.
        Self {
            signature: Signature::new(
                TypeSignature::Any(2),
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for ApproxPercentileUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "approx_percentile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> DFResult<Box<dyn datafusion::logical_expr::Accumulator>> {
        // The second argument must be a Float64 literal.  Extract it by
        // evaluating the PhysicalExpr against an empty batch so the accumulator
        // captures `p` at plan time.
        let p = if let Some(expr) = args.exprs.get(1) {
            let empty_schema = Arc::new(Schema::empty());
            let empty_batch = RecordBatch::new_empty(Arc::clone(&empty_schema));
            let val = match expr.evaluate(&empty_batch)? {
                ColumnarValue::Scalar(s) => s,
                ColumnarValue::Array(_) => {
                    return exec_err!(
                        "APPROX_PERCENTILE: second argument must be a constant literal, not an array"
                    );
                }
            };
            match val {
                ScalarValue::Float64(Some(v)) => v,
                ScalarValue::Float32(Some(v)) => v as f64,
                ScalarValue::Int8(Some(v)) => v as f64,
                ScalarValue::Int16(Some(v)) => v as f64,
                ScalarValue::Int32(Some(v)) => v as f64,
                ScalarValue::Int64(Some(v)) => v as f64,
                ScalarValue::UInt8(Some(v)) => v as f64,
                ScalarValue::UInt16(Some(v)) => v as f64,
                ScalarValue::UInt32(Some(v)) => v as f64,
                ScalarValue::UInt64(Some(v)) => v as f64,
                other => {
                    return exec_err!(
                        "APPROX_PERCENTILE: second argument must be a numeric literal, got {:?}",
                        other
                    );
                }
            }
        } else {
            return exec_err!(
                "APPROX_PERCENTILE: requires two arguments: (col, percentile)"
            );
        };

        if !(0.0..=1.0).contains(&p) {
            return exec_err!(
                "APPROX_PERCENTILE: percentile p={p} must be in [0.0, 1.0]"
            );
        }

        Ok(Box::new(ApproxPercentileAccumulator::new(p)))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format!("{}_tdigest_state", args.name),
                DataType::Binary,
                true,
            )),
            Arc::new(Field::new(
                format!("{}_p", args.name),
                DataType::Float64,
                true,
            )),
        ])
    }
}

// ── Alias: approx_percentile_cont ────────────────────────────────────────────

/// SQL-standard alias: `APPROX_PERCENTILE_CONT` behaves identically.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ApproxPercentileContUdaf {
    inner: ApproxPercentileUdaf,
}

impl ApproxPercentileContUdaf {
    pub fn new() -> Self {
        Self {
            inner: ApproxPercentileUdaf::new(),
        }
    }
}

impl AggregateUDFImpl for ApproxPercentileContUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "approx_percentile_cont"
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        self.inner.return_type(arg_types)
    }
    fn accumulator(&self, args: AccumulatorArgs) -> DFResult<Box<dyn datafusion::logical_expr::Accumulator>> {
        self.inner.accumulator(args)
    }
    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }
}

/// Register `APPROX_PERCENTILE` (and alias `APPROX_PERCENTILE_CONT`) into a
/// DataFusion `SessionContext`.
pub(crate) fn register_approx_percentile(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(ApproxPercentileUdaf::new()));
    ctx.register_udaf(AggregateUDF::from(ApproxPercentileContUdaf::new()));
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Feed `n` sequential integers (0..n) and verify that the quantile
    /// estimate is within `tol` (fraction) of the true value.
    fn check_quantile(n: usize, p: f64, tol: f64) {
        let mut digest = TDigest::new();
        for i in 0..n {
            digest.add(i as f64);
        }
        let estimated = digest.quantile(p);
        let true_val = p * (n as f64 - 1.0); // for 0-indexed uniform: true quantile
        let err = (estimated - true_val).abs() / (n as f64);
        assert!(
            err <= tol,
            "n={n} p={p}: estimated={estimated:.2} true={true_val:.2} rel_err={:.4} > tol={tol}",
            err
        );
    }

    #[test]
    fn tdigest_median_100k() {
        // 100k samples — median should be within 0.5% of n/2.
        check_quantile(100_000, 0.5, 0.005);
    }

    #[test]
    fn tdigest_p95_100k() {
        // p=0.95 within 1% of true value on 100k samples.
        check_quantile(100_000, 0.95, 0.01);
    }

    #[test]
    fn tdigest_p05_100k() {
        // p=0.05 within 1% of true value on 100k samples.
        check_quantile(100_000, 0.05, 0.01);
    }

    #[test]
    fn tdigest_edge_min() {
        let mut d = TDigest::new();
        for i in 0..1000 {
            d.add(i as f64);
        }
        let min = d.quantile(0.0);
        assert!((min - 0.0).abs() < 1.0, "min estimate={min}");
    }

    #[test]
    fn tdigest_edge_max() {
        let mut d = TDigest::new();
        for i in 0..1000 {
            d.add(i as f64);
        }
        let max = d.quantile(1.0);
        assert!((max - 999.0).abs() < 1.0, "max estimate={max}");
    }

    #[test]
    fn tdigest_serialization_roundtrip() {
        let mut d = TDigest::new();
        for i in 0..5000_u64 {
            d.add(i as f64);
        }
        d.compress();
        let bytes = d.to_bytes();
        let restored = TDigest::from_bytes(&bytes).expect("deserialize failed");
        let orig_q = d.quantile(0.5);
        let rest_q = {
            let mut r = restored;
            r.quantile(0.5)
        };
        assert!(
            (orig_q - rest_q).abs() < 1.0,
            "roundtrip diverged: orig={orig_q} restored={rest_q}"
        );
    }

    #[test]
    fn tdigest_merge_two_halves() {
        // Merge two digests covering [0, 5000) and [5000, 10000).
        let mut a = TDigest::new();
        let mut b = TDigest::new();
        for i in 0..5000 {
            a.add(i as f64);
        }
        for i in 5000..10000 {
            b.add(i as f64);
        }
        a.merge(&b);
        let median = a.quantile(0.5);
        // True median of 0..10000 is ~4999.5
        let err = (median - 4999.5).abs() / 10000.0;
        assert!(err <= 0.01, "merged median={median:.2} rel_err={err:.4}");
    }

    #[test]
    fn tdigest_empty_returns_nan() {
        let mut d = TDigest::new();
        assert!(d.quantile(0.5).is_nan());
    }
}
