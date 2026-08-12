//! The operator interface every physical node implements.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

/// Why execution failed.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecError {
    /// The query exceeded its memory budget. Distinct from an internal error
    /// because it is an expected outcome of a bounded multi-tenant pool, and
    /// the caller maps it to SQLSTATE 53200 (`out_of_memory`) rather than
    /// treating it as a bug.
    OutOfMemory {
        requested: usize,
        budget: usize,
    },
    /// The statement was cancelled — `statement_timeout` or a client request.
    Cancelled,
    /// A type mismatch that should have been caught during planning. Reaching
    /// one at runtime is a planner bug, not user error.
    TypeMismatch(String),
    /// Arithmetic overflow. Postgres ERRORS on integer overflow where Arrow's
    /// kernels may wrap, so this must be raised explicitly rather than
    /// inherited from the kernel.
    Overflow(&'static str),
    DivisionByZero,
    Internal(String),
}

impl std::fmt::Display for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecError::OutOfMemory { requested, budget } => {
                write!(f, "out of memory: wanted {requested} bytes of {budget}")
            }
            ExecError::Cancelled => write!(f, "canceling statement due to user request"),
            ExecError::TypeMismatch(m) => write!(f, "type mismatch: {m}"),
            ExecError::Overflow(op) => write!(f, "{op} out of range"),
            ExecError::DivisionByZero => write!(f, "division by zero"),
            ExecError::Internal(m) => write!(f, "internal error: {m}"),
        }
    }
}

impl std::error::Error for ExecError {}

/// A pull-based physical operator.
///
/// `next_batch` returns `Ok(None)` at end of stream. Implementations must
/// return control between batches so the caller can check cancellation — an
/// operator that loops internally until exhaustion defeats `statement_timeout`.
pub trait Operator {
    /// The Arrow schema of the batches this operator yields.
    fn schema(&self) -> SchemaRef;

    /// Produce the next batch, or `None` when exhausted.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError>;

    /// Bytes currently held. Used by the memory accountant; an operator that
    /// buffers (hash join build side, sort run, hash aggregate table) must
    /// report honestly or the budget means nothing.
    fn memory_used(&self) -> usize {
        0
    }
}
