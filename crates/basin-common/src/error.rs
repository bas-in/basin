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

    /// Phase 6.X.C (ADR 0023) — a voluntary lease handoff is in progress for
    /// this `(project, partition)` and the leaseholder is rejecting new writes
    /// so the memtable can drain to the cold tier before ownership transfers.
    /// Routers and tests treat this as **retryable**: the router invalidates
    /// its lease cache entry and retries against the new owner once the
    /// handoff completes. The accompanying message names the partition for
    /// observability. Reads are not blocked during a handoff; only new writes
    /// are rejected.
    #[error("lease handoff in progress: {0}")]
    LeaseHandoffInProgress(String),

    /// Multi-node phase 1 (ADR 0023) — `BASIN_LEASE_MODE=required`: this
    /// replica does not (or no longer) hold the writer lease for the target
    /// `(project, partition)`, so the write is refused before it reaches the
    /// WAL. Raised when the lease was never granted (another replica owns
    /// it), when the heartbeat lost the lease (renew failed / CAS stolen),
    /// or when the lease coordinator is unreachable — fail-closed for
    /// writes. Reads are never blocked by this error. Routers treat it as
    /// **retryable** (same class as a commit conflict): re-resolve the
    /// owner and retry there. The message names the partition.
    #[error("writer lease not held: {0}")]
    LeaseNotHeld(String),

    /// Multi-node commit 4 (`BASIN_WAL_MODE=raft`) — the raft-backed WAL could
    /// not durably commit the write because consensus did not reach quorum:
    /// no leader, lost leadership, replication timed out, or not enough
    /// replicas are reachable to form a majority. In raft mode "durable" means
    /// "quorum-replicated", so a write that cannot reach quorum is **not**
    /// acked — it blocks (waiting for the proposal to commit) and then fails
    /// with this error rather than silently dropping. Fail-closed for writes;
    /// reads are never rejected with it. Routers treat it as **retryable**
    /// (same 40001 serialization-failure class as [`Self::LeaseNotHeld`]): the
    /// caller should re-resolve the leader and retry. The message names the
    /// partition / cause.
    ///
    /// SQLSTATE choice: 40001 keeps it in the retryable class drivers already
    /// back off on (matching LeaseNotHeld / commit conflict). 57P03
    /// (`cannot_connect_now`) was the alternative — it reads as "the durable
    /// backend is temporarily unavailable, retry" — but mixing a connection-
    /// state code into a per-statement retry confuses driver retry loops that
    /// special-case 57P03 as "reconnect". See APPLY.md.
    #[error("raft WAL could not reach quorum: {0}")]
    RaftNoQuorum(String),

    /// A statement waited for a row or table lock longer than
    /// `lock_timeout` allows. Router maps to SQLSTATE `55P03`
    /// (`lock_not_available`), matching PostgreSQL's behaviour when
    /// `lock_timeout` fires.
    #[error("canceling statement due to lock timeout: {0}")]
    LockNotAvailable(String),

    /// An idle-in-transaction session exceeded `idle_in_transaction_session_timeout`.
    /// The session is terminated by the reaper; the client sees a connection-
    /// closed error on the next attempt. Router maps to SQLSTATE `25P03`
    /// (`idle_in_transaction_session_timeout`).
    #[error("terminating connection due to idle-in-transaction timeout: {0}")]
    IdleInTransactionTimeout(String),

    /// Referenced relation (table / view) does not exist at planning time.
    /// Router maps to SQLSTATE `42P01` (`undefined_table`) with the exact
    /// PG-shaped message `relation "<name>" does not exist`. Both the code
    /// and the message shape are load-bearing: ORM migration flows branch
    /// on 42P01 (Diesel / TypeORM / Django all decide "tracker table is
    /// missing → create it" when the version probe raises undefined_table).
    #[error("relation \"{0}\" does not exist")]
    UndefinedTable(String),

    /// Referenced function does not exist at planning time. Router maps to
    /// SQLSTATE `42883` (`undefined_function`) with the PG-shaped message
    /// `function <name> does not exist`. PG raises the same code both for an
    /// unknown function name and for a known name with no matching argument
    /// types; Basin only detects the former (DataFusion's planner reports
    /// `Invalid function '<name>'` / `table function '<name>' not found`).
    /// Drivers (psycopg `UndefinedFunction`, asyncpg) branch on the code.
    #[error("function {0} does not exist")]
    UndefinedFunction(String),

    /// Referenced column does not exist at planning time. Router maps to
    /// SQLSTATE `42703` (`undefined_column`) with the PG-shaped message
    /// `column "<name>" does not exist` — the code drivers surface as a
    /// dedicated exception class (psycopg `UndefinedColumn`).
    #[error("column \"{0}\" does not exist")]
    UndefinedColumn(String),

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
    /// Typed constructor for a missing relation. `name` is the relation
    /// name as the user wrote it (qualified if they qualified it) — it is
    /// interpolated into the PG-shaped `relation "<name>" does not exist`
    /// message and surfaces as SQLSTATE 42P01.
    pub fn undefined_table(name: impl Into<String>) -> Self {
        Self::UndefinedTable(name.into())
    }
    /// Typed constructor for a missing function. `name` is the function name
    /// as the user wrote it — interpolated into the PG-shaped
    /// `function <name> does not exist` message and surfaces as SQLSTATE 42883.
    pub fn undefined_function(name: impl Into<String>) -> Self {
        Self::UndefinedFunction(name.into())
    }
    /// Typed constructor for a missing column. `name` is the column reference
    /// as the planner reported it — interpolated into the PG-shaped
    /// `column "<name>" does not exist` message and surfaces as SQLSTATE 42703.
    pub fn undefined_column(name: impl Into<String>) -> Self {
        Self::UndefinedColumn(name.into())
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
    /// Phase 6.X.C — typed constructor for the lease-handoff rejection.
    /// Callers should set `msg` to `"{project}/{partition}"` so the router
    /// can extract the affected partition on retry.
    pub fn lease_handoff_in_progress(msg: impl Into<String>) -> Self {
        Self::LeaseHandoffInProgress(msg.into())
    }

    /// Multi-node phase 1 — typed constructor for the
    /// `BASIN_LEASE_MODE=required` write refusal. Callers should set `msg`
    /// to `"{project}/{partition}"` (same convention as
    /// [`Self::lease_handoff_in_progress`]) so the router can re-resolve
    /// the owner on retry.
    pub fn lease_not_held(msg: impl Into<String>) -> Self {
        Self::LeaseNotHeld(msg.into())
    }

    /// Multi-node raft (commit 6) — typed constructor for a write that
    /// arrived at a node which is **not the raft leader**. In `BASIN_WAL_MODE
    /// =raft` raft leadership is the write fence (it supersedes the writer
    /// lease — see `RaftWal`), so a non-leader write is refused before it
    /// reaches the raft log. Reuses the [`Self::LeaseNotHeld`] variant on
    /// purpose: it is the same retryable class (SQLSTATE 40001) the router
    /// already handles for lease refusals, so no new error variant and no new
    /// exhaustive-match arm are needed. The message embeds an optional leader
    /// hint so the caller (and the router/proxy on retry) can be redirected
    /// to the current leader: `not raft leader (leader hint: <addr-or-id>)`
    /// when known, else `not raft leader (leader unknown)`. `hint` is the
    /// leader's advertised address (or node id) as the cluster knows it.
    pub fn not_leader(hint: Option<impl Into<String>>) -> Self {
        let msg = match hint {
            Some(h) => format!("not raft leader (leader hint: {})", h.into()),
            None => "not raft leader (leader unknown)".to_string(),
        };
        Self::LeaseNotHeld(msg)
    }

    /// Multi-node commit 4 — typed constructor for the raft-WAL no-quorum
    /// write failure (`BASIN_WAL_MODE=raft`). Retryable (SQLSTATE 40001).
    pub fn raft_no_quorum(msg: impl Into<String>) -> Self {
        Self::RaftNoQuorum(msg.into())
    }

    /// Phase 5.28.B — typed constructor for `lock_timeout` expiry.
    /// Produces SQLSTATE 55P03 (`lock_not_available`).
    pub fn lock_not_available(msg: impl Into<String>) -> Self {
        Self::LockNotAvailable(msg.into())
    }

    /// Phase 5.28.C — typed constructor for `idle_in_transaction_session_timeout` expiry.
    /// Produces SQLSTATE 25P03.
    pub fn idle_in_transaction_timeout(msg: impl Into<String>) -> Self {
        Self::IdleInTransactionTimeout(msg.into())
    }
}
