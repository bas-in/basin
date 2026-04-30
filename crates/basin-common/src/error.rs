//! Cross-crate error type. Every `basin-*` crate should map its errors into
//! [`BasinError`] at its public boundary so the router can render consistent
//! Postgres-flavored error responses.

use std::io;

use thiserror::Error;

pub type Result<T, E = BasinError> = std::result::Result<T, E>;

/// Top-level error.
///
/// Variants are deliberately coarse: they describe *what kind of failure
/// happened* from the caller's perspective, not which subsystem produced it.
/// The `source` chain (via `#[source]`) carries the gory detail.
#[derive(Debug, Error)]
pub enum BasinError {
    /// User-supplied identifier failed validation.
    #[error("invalid identifier: {0}")]
    InvalidIdent(String),

    /// User-supplied schema is malformed (column types, constraints, etc.).
    #[error("invalid schema: {0}")]
    InvalidSchema(String),

    /// The named tenant / table / partition / object does not exist.
    #[error("not found: {0}")]
    NotFound(String),

    /// Concurrent commit lost a race; caller should retry from a fresh read.
    #[error("commit conflict: {0}")]
    CommitConflict(String),

    /// Storage-layer failure (object_store, parquet, arrow).
    #[error("storage error: {0}")]
    Storage(String),

    /// Catalog-layer failure (Iceberg, Postgres metadata store).
    #[error("catalog error: {0}")]
    Catalog(String),

    /// WAL-layer failure (Raft, replication, flush).
    #[error("wal error: {0}")]
    Wal(String),

    /// Tenant isolation invariant was about to be violated. **This is fatal.**
    /// Treat any occurrence as a P0 incident: dump context, page on-call.
    #[error("ISOLATION VIOLATION: {0}")]
    IsolationViolation(String),

    /// Local filesystem / network IO problem we couldn't categorize further.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// JSON ser/de.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// Catch-all for sources without a dedicated variant.
    #[error("internal: {0}")]
    Internal(String),
}

impl BasinError {
    pub fn storage(msg: impl Into<String>) -> Self {
        Self::Storage(msg.into())
    }
    pub fn catalog(msg: impl Into<String>) -> Self {
        Self::Catalog(msg.into())
    }
    pub fn wal(msg: impl Into<String>) -> Self {
        Self::Wal(msg.into())
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound(msg.into())
    }
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }

    /// Convenience for the load-bearing safety check. The router and shard
    /// owner both call this; if either ever sees it fire, something is very
    /// wrong and we fail closed.
    pub fn isolation(msg: impl Into<String>) -> Self {
        Self::IsolationViolation(msg.into())
    }
}
