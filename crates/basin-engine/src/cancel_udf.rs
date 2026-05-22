//! Per-session `pg_cancel_backend(pid integer) → boolean` UDF.
//!
//! Phase 5.31.A: `pg_cancel_backend(pid)` is registered as a per-session
//! ScalarUDF so that calling `SELECT pg_cancel_backend(N)` from any session
//! routes through the engine's cancel path:
//!
//!   1. Calls `engine.cancel_backend(pid)` which fires the target session's
//!      `tokio::sync::Notify` handle.
//!   2. The target session's `exec_select` loop wakes from its `tokio::select!`
//!      branch, drops the DataFusion collect future, and returns
//!      `BasinError::QueryCanceled` (SQLSTATE 57014).
//!   3. Returns `true` to the caller if the pid was found, `false` otherwise
//!      (matching PostgreSQL semantics).
//!
//! The UDF is registered twice — bare name (`pg_cancel_backend`) and
//! schema-qualified (`pg_catalog.pg_cancel_backend`) — so clients that
//! quote the catalog prefix also resolve it.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

use crate::Engine;

/// Register the `pg_cancel_backend` UDF on `ctx`. Captures `engine` so
/// calls from any session can reach the process-wide cancel_backend path.
pub(crate) fn register_pg_cancel_backend_udf(ctx: &SessionContext, engine: Engine) {
    let eng = Arc::new(engine);
    let sig = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Int32]),
            TypeSignature::Exact(vec![DataType::Int64]),
        ],
        Volatility::Volatile,
    );
    ctx.register_udf(ScalarUDF::from(PgCancelBackendUdf {
        name: "pg_cancel_backend".into(),
        engine: eng.clone(),
        signature: sig.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(PgCancelBackendUdf {
        name: "pg_catalog.pg_cancel_backend".into(),
        engine: eng,
        signature: sig,
    }));
}

// ---------------------------------------------------------------------------
// UDF implementation
// ---------------------------------------------------------------------------

struct PgCancelBackendUdf {
    name: String,
    engine: Arc<Engine>,
    signature: Signature,
}

impl std::fmt::Debug for PgCancelBackendUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgCancelBackendUdf")
            .field("name", &self.name)
            .finish()
    }
}

impl PartialEq for PgCancelBackendUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && Arc::ptr_eq(&self.engine, &other.engine)
    }
}
impl Eq for PgCancelBackendUdf {}
impl std::hash::Hash for PgCancelBackendUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for PgCancelBackendUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "pg_cancel_backend: expected 1 argument, got {}",
                args.len()
            )));
        }
        // Extract the pid as i32 from the first argument.
        let pid: Option<i32> = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => Some(*v),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => {
                Some(i32::try_from(*v).unwrap_or(-1))
            }
            ColumnarValue::Scalar(ScalarValue::Int32(None))
            | ColumnarValue::Scalar(ScalarValue::Int64(None)) => None,
            ColumnarValue::Array(arr) => {
                // Vectorised call — iterate and cancel each pid.
                use datafusion::arrow::array::PrimitiveArray;
                use datafusion::arrow::datatypes::{Int32Type, Int64Type};
                let mut results: Vec<bool> = Vec::with_capacity(arr.len());
                if let Some(int32_arr) = arr.as_any().downcast_ref::<PrimitiveArray<Int32Type>>() {
                    for i in 0..int32_arr.len() {
                        let pid: Option<i32> = if Array::is_null(int32_arr, i) {
                            None
                        } else {
                            Some(int32_arr.value(i))
                        };
                        results.push(pid.map(|p| self.engine.cancel_backend(p)).unwrap_or(false));
                    }
                } else if let Some(int64_arr) =
                    arr.as_any().downcast_ref::<PrimitiveArray<Int64Type>>()
                {
                    for i in 0..int64_arr.len() {
                        let pid: Option<i32> = if Array::is_null(int64_arr, i) {
                            None
                        } else {
                            i32::try_from(int64_arr.value(i)).ok()
                        };
                        results.push(pid.map(|p| self.engine.cancel_backend(p)).unwrap_or(false));
                    }
                } else {
                    // Unknown array type — return all false.
                    results.resize(arr.len(), false);
                }
                let out: ArrayRef = Arc::new(BooleanArray::from(results));
                return Ok(ColumnarValue::Array(out));
            }
            other => {
                return Err(DataFusionError::Execution(format!(
                    "pg_cancel_backend: unexpected argument type {:?}",
                    other
                )));
            }
        };
        let cancelled = pid.map(|p| self.engine.cancel_backend(p)).unwrap_or(false);
        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(cancelled))))
    }
}
