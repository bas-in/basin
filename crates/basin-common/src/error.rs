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

    /// The named project / table / partition / object does not exist.
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

    /// Project isolation invariant was about to be violated. **This is fatal.**
    /// Treat any occurrence as a P0 incident: dump context, page on-call.
    #[error("ISOLATION VIOLATION: {0}")]
    IsolationViolation(String),

    /// Local filesystem / network IO problem we couldn't categorize further.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// JSON ser/de.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// User's query was rejected at planning time because its estimated
    /// cost (rows scanned, bytes read, etc.) exceeds the configured
    /// per-project cap. The router maps this to Postgres SQLSTATE 54000
    /// (`program_limit_exceeded`) so drivers surface a distinct
    /// retry-with-backoff exception class. Configured via
    /// `BASIN_QUERY_COST_LIMIT_ROWS` (or future per-project overrides);
    /// unset / `0` disables the check entirely.
    #[error("query cost exceeded: {0}")]
    QueryCostExceeded(String),

    /// Statement exceeded its wall-clock budget and was cancelled. The router
    /// maps this to Postgres SQLSTATE `57014` (`query_canceled`) — exactly the
    /// code PostgreSQL raises when `statement_timeout` fires — so drivers
    /// surface a distinct, non-retryable-without-rework exception class.
    /// Configured via `BASIN_STATEMENT_TIMEOUT_MS`; `0` disables the timeout.
    #[error("canceling statement due to statement timeout: {0}")]
    QueryCanceled(String),

    /// User asked for a feature that's known but not yet implemented (e.g.
    /// `GENERATED ALWAYS AS ... VIRTUAL`). The router maps this to
    /// Postgres SQLSTATE `0A000` (`feature_not_supported`) so drivers can
    /// distinguish "we don't ship it" from a parse / schema error.
    #[error("feature not supported: {0}")]
    FeatureNotSupported(String),

    /// PRIMARY KEY (or UNIQUE) violation. Router maps to SQLSTATE
    /// `23505` (`unique_violation`).
    #[error("{0}")]
    UniqueViolation(String),

    /// CHECK constraint violation. Router maps to SQLSTATE `23514`
    /// (`check_violation`).
    #[error("{0}")]
    CheckViolation(String),

    /// FOREIGN KEY violation. Router maps to SQLSTATE `23503`
    /// (`foreign_key_violation`).
    #[error("{0}")]
    ForeignKeyViolation(String),

    /// Row-level-security policy violation: an INSERT/UPDATE produced a
    /// row that fails an applicable policy's WITH CHECK (or USING when no
    /// WITH CHECK is declared) expression. Router maps to SQLSTATE
    /// `42501` (`insufficient_privilege`) — exactly what PostgreSQL
    /// raises for `new row violates row-level security policy`.
    #[error("{0}")]
    RlsViolation(String),

    /// Caller attempted a privileged operation that the current session is
    /// not allowed to perform — e.g. user DDL targeting a reserved system
    /// schema (`auth`, `storage`, `cron`, `net`, `realtime`, `pg_catalog`,
    /// `information_schema`). Router maps to SQLSTATE `42501`
    /// (`insufficient_privilege`), matching PostgreSQL's
    /// `permission denied for schema "<name>"`.
    #[error("{0}")]
    PermissionDenied(String),

    /// A text value exceeded a declared `VARCHAR(n)` / `CHAR(n)` length.
    /// Router maps to SQLSTATE `22001` (`string_data_right_truncation`),
    /// matching PostgreSQL's `value too long for type ...` error.
    #[error("{0}")]
    StringTooLong(String),

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
    pub fn feature_not_supported(msg: impl Into<String>) -> Self {
        Self::FeatureNotSupported(msg.into())
    }
    pub fn query_canceled(msg: impl Into<String>) -> Self {
        Self::QueryCanceled(msg.into())
    }

    /// Convenience for the load-bearing safety check. The router and shard
    /// owner both call this; if either ever sees it fire, something is very
    /// wrong and we fail closed.
    pub fn isolation(msg: impl Into<String>) -> Self {
        Self::IsolationViolation(msg.into())
    }
}
