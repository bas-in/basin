//! The error type every [`crate::SystemView::scan`] returns.

use std::fmt;

/// Something went wrong building a catalog relation.
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// Arrow refused to build the `RecordBatch` — almost always a
    /// column-length mismatch introduced by a bug in a `scan()`
    /// implementation, not something a caller can recover from.
    Arrow(String),
    /// A [`crate::predicate::Predicate`] named a column the relation does not
    /// have. Surfaced rather than silently ignored, because a typo'd column
    /// name silently *not* narrowing the result set is exactly the bug this
    /// crate exists to prevent — see the pushdown discussion in the crate
    /// docs.
    UnknownColumn {
        relation: &'static str,
        column: String,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Arrow(msg) => write!(f, "failed to build catalog relation: {msg}"),
            Error::UnknownColumn { relation, column } => {
                write!(f, "{relation} has no column named {column:?}")
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<arrow_schema::ArrowError> for Error {
    fn from(e: arrow_schema::ArrowError) -> Self {
        Error::Arrow(e.to_string())
    }
}
