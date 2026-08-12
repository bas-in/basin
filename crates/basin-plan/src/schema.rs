//! Output-schema inference for a [`LogicalPlan`].
//!
//! Every rewrite has to preserve a plan's output schema, so schema inference is
//! the contract the optimizer is checked against rather than a convenience.
//! STUB — implemented in a following increment.

use crate::{LogicalPlan, Schema};

/// Why a plan's schema could not be inferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaError {
    /// A column reference points outside its input's schema.
    ColumnOutOfRange { relation: u16, index: u16 },
    /// The two sides of a set operation have incompatible schemas.
    SetOpMismatch,
    /// Not yet implemented for this plan shape.
    Unimplemented(&'static str),
}

/// Infer the output schema of `plan`.
pub fn output_schema(plan: &LogicalPlan) -> Result<Schema, SchemaError> {
    match plan {
        LogicalPlan::Values { schema, .. }
        | LogicalPlan::Empty { schema, .. }
        | LogicalPlan::CteRef { schema, .. } => Ok(schema.clone()),
        _ => Err(SchemaError::Unimplemented("output_schema")),
    }
}
