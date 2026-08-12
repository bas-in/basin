//! Lowering: `pg_query` parse tree to Basin's IR.
//!
//! This is what deletes `pg_operators.rs`. That file is 9,546 lines of string
//! rewriting whose own header concedes it cannot handle dollar-quoted strings,
//! comments, or quoted identifiers — it exists only because DataFusion's SQL
//! surface differs from Postgres's. Lowering from the parse tree directly, with
//! operators resolved against a catalog by argument type, removes the reason
//! for it to exist.

pub mod dml;
pub mod expr;
pub mod select;

/// Why lowering failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LowerError {
    /// A construct Basin does not support. Surfaces as SQLSTATE 0A000.
    Unsupported(String),
    /// A name that does not resolve.
    UnknownName(String),
    /// No operator or function matches these argument types.
    NoMatchingOperator(String),
    /// The parse tree was not the shape the caller expected.
    Malformed(&'static str),
}
