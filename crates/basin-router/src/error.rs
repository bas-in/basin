//! Map between `BasinError` and pgwire's `ErrorResponse`.
//!
//! Postgres error codes are 5-character strings categorized in
//! <https://www.postgresql.org/docs/current/errcodes-appendix.html>. We map a
//! handful of obvious cases and fall back to `XX000` (internal error) for the
//! rest. The string body is always the `Display` form of the error so the
//! client gets enough context to debug.

use basin_common::BasinError;
use pgwire::error::ErrorInfo;
use pgwire::messages::response::ErrorResponse;

/// Build an `ErrorResponse` from a `BasinError`.
pub(crate) fn error_response(err: &BasinError) -> ErrorResponse {
    let (severity, code) = classify(err);
    let info = ErrorInfo::new(severity.to_owned(), code.to_owned(), err.to_string());
    info.into()
}

/// Pgwire `ErrorResponse` for the per-project rate-limiter (SQLSTATE
/// `53400` — `configuration_limit_exceeded`). Same code Postgres itself
/// raises for connection / statement quota breaches; drivers map it to a
/// dedicated exception class so app code can retry-with-backoff distinct
/// from a parse / permission error.
pub(crate) fn rate_limit_exceeded_response() -> ErrorResponse {
    let info = ErrorInfo::new(
        "ERROR".to_owned(),
        "53400".to_owned(),
        "project pgwire rate limit exceeded; retry after a short backoff".to_owned(),
    );
    info.into()
}

/// Pgwire `ErrorResponse` for the per-project connection ceiling (SQLSTATE
/// `53300` — `too_many_connections`). Sent during startup handshake when the
/// project has reached its `max_connections` limit; the connection is closed
/// immediately after this response.
pub(crate) fn connection_limit_reached_response() -> ErrorResponse {
    let info = ErrorInfo::new(
        "FATAL".to_owned(),
        "53300".to_owned(),
        "too many connections for project (ceiling reached)".to_owned(),
    );
    info.into()
}

fn classify(err: &BasinError) -> (&'static str, &'static str) {
    match err {
        BasinError::InvalidIdent(_) | BasinError::InvalidSchema(_) => ("ERROR", "42601"), // syntax_error
        BasinError::NotFound(_) => ("ERROR", "42704"), // undefined_object
        // Missing relation at planning time. PG raises 42P01
        // (undefined_table) and ORM migration flows BRANCH on this exact
        // code (Diesel / TypeORM / Django treat it as "tracker table
        // missing → create it"), so it must not collapse into XX000/42704.
        BasinError::UndefinedTable(_) => ("ERROR", "42P01"), // undefined_table
        // Missing function at planning time. PG raises 42883
        // (undefined_function); drivers expose it as a dedicated class
        // (psycopg UndefinedFunction) so it must not collapse into XX000.
        BasinError::UndefinedFunction(_) => ("ERROR", "42883"), // undefined_function
        // Missing column at planning time. PG raises 42703
        // (undefined_column); same driver-class rationale as above.
        BasinError::UndefinedColumn(_) => ("ERROR", "42703"), // undefined_column
        BasinError::CommitConflict(_) => ("ERROR", "40001"), // serialization_failure
        BasinError::QueryCostExceeded(_) => ("ERROR", "54000"), // program_limit_exceeded
        BasinError::QueryCanceled(_) => ("ERROR", "57014"),     // query_canceled
        BasinError::FeatureNotSupported(_) => ("ERROR", "0A000"), // feature_not_supported
        BasinError::UniqueViolation(_) => ("ERROR", "23505"), // unique_violation
        BasinError::CheckViolation(_) => ("ERROR", "23514"), // check_violation
        BasinError::ForeignKeyViolation(_) => ("ERROR", "23503"), // foreign_key_violation
        BasinError::RlsViolation(_) => ("ERROR", "42501"), // insufficient_privilege (RLS)
        BasinError::PermissionDenied(_) => ("ERROR", "42501"), // insufficient_privilege
        BasinError::StringTooLong(_) => ("ERROR", "22001"), // string_data_right_truncation
        // Phase 6.X.C (ADR 0023): voluntary lease handoff in progress —
        // retryable (same SQLSTATE class as commit conflict, since the
        // caller should retry from a fresh route + re-resolved owner).
        BasinError::LeaseHandoffInProgress(_) => ("ERROR", "40001"), // serialization_failure
        // Multi-node phase 1 (BASIN_LEASE_MODE=required): this replica does
        // not hold the writer lease — retryable for the same reason as the
        // handoff rejection above (the caller should re-resolve the owner
        // and retry there). Reads are never rejected with this code.
        BasinError::LeaseNotHeld(_) => ("ERROR", "40001"), // serialization_failure
        // Multi-node commit 4 (BASIN_WAL_MODE=raft): the raft WAL could not
        // reach quorum to durably commit the write — retryable, same class as
        // LeaseNotHeld above (re-resolve the leader and retry). 57P03 was the
        // alternative but it confuses drivers that special-case it as
        // "reconnect"; see basin-common error docs.
        BasinError::RaftNoQuorum(_) => ("ERROR", "40001"), // serialization_failure
        // Phase 5.28.B: lock_timeout expiry — PostgreSQL raises 55P03.
        BasinError::LockNotAvailable(_) => ("ERROR", "55P03"), // lock_not_available
        // Phase 5.28.C: idle_in_transaction_session_timeout — PostgreSQL
        // terminates the session as FATAL with 25P03.
        BasinError::IdleInTransactionTimeout(_) => ("FATAL", "25P03"), // idle_in_transaction_session_timeout
        BasinError::IsolationViolation(_) => ("FATAL", "XX000"),
        // Coarse-grained internal categories all collapse to XX000.
        BasinError::Storage(_)
        | BasinError::Catalog(_)
        | BasinError::Wal(_)
        | BasinError::Io(_)
        | BasinError::Json(_)
        | BasinError::Internal(_) => ("ERROR", "XX000"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_internal() {
        let er = error_response(&BasinError::Internal("boom".into()));
        // First field is severity 'S', second is code 'C', third is message 'M'.
        assert_eq!(er.fields[0], (b'S', "ERROR".to_owned()));
        assert_eq!(er.fields[1], (b'C', "XX000".to_owned()));
        assert!(er.fields[2].1.contains("boom"));
    }

    #[test]
    fn classifies_not_found() {
        let er = error_response(&BasinError::NotFound("table x".into()));
        assert_eq!(er.fields[1], (b'C', "42704".to_owned()));
    }

    #[test]
    fn classifies_undefined_table() {
        // Missing relations must surface as SQLSTATE 42P01 with the exact
        // PG message shape — ORM migration flows (Diesel / TypeORM /
        // Django) branch on this code to decide "create the tracker".
        let er = error_response(&BasinError::UndefinedTable("django_migrations".into()));
        assert_eq!(er.fields[0], (b'S', "ERROR".to_owned()));
        assert_eq!(er.fields[1], (b'C', "42P01".to_owned()));
        assert_eq!(
            er.fields[2],
            (b'M', "relation \"django_migrations\" does not exist".to_owned())
        );
    }

    #[test]
    fn classifies_undefined_function() {
        // Missing functions must surface as SQLSTATE 42883 with the
        // PG message shape `function <name> does not exist`.
        let er = error_response(&BasinError::UndefinedFunction("nosuch_fn".into()));
        assert_eq!(er.fields[0], (b'S', "ERROR".to_owned()));
        assert_eq!(er.fields[1], (b'C', "42883".to_owned()));
        assert_eq!(
            er.fields[2],
            (b'M', "function nosuch_fn does not exist".to_owned())
        );
    }

    #[test]
    fn classifies_undefined_column() {
        // Missing columns must surface as SQLSTATE 42703 with the
        // PG message shape `column "<name>" does not exist`.
        let er = error_response(&BasinError::UndefinedColumn("nosuch_col".into()));
        assert_eq!(er.fields[0], (b'S', "ERROR".to_owned()));
        assert_eq!(er.fields[1], (b'C', "42703".to_owned()));
        assert_eq!(
            er.fields[2],
            (b'M', "column \"nosuch_col\" does not exist".to_owned())
        );
    }

    #[test]
    fn classifies_lease_not_held() {
        // BASIN_LEASE_MODE=required write refusals must surface as SQLSTATE
        // 40001 (serialization_failure) — the retryable class drivers and
        // routers already handle for commit conflicts and lease handoffs.
        let er = error_response(&BasinError::lease_not_held("proj/part"));
        assert_eq!(er.fields[0], (b'S', "ERROR".to_owned()));
        assert_eq!(er.fields[1], (b'C', "40001".to_owned()));
        assert!(er.fields[2].1.contains("writer lease not held"));
    }

    #[test]
    fn classifies_raft_no_quorum() {
        // BASIN_WAL_MODE=raft no-quorum write failures must surface as
        // SQLSTATE 40001 (serialization_failure) — same retryable class as
        // LeaseNotHeld, so existing driver/router retry loops pick it up.
        let er = error_response(&BasinError::raft_no_quorum("proj/part: no leader"));
        assert_eq!(er.fields[0], (b'S', "ERROR".to_owned()));
        assert_eq!(er.fields[1], (b'C', "40001".to_owned()));
        assert!(er.fields[2].1.contains("could not reach quorum"));
    }

    #[test]
    fn classifies_query_canceled() {
        // Statement-timeout cancellation must surface as SQLSTATE 57014
        // (query_canceled) — the exact code PostgreSQL raises for
        // statement_timeout — so drivers map it to a dedicated class.
        let er = error_response(&BasinError::QueryCanceled("timed out".into()));
        assert_eq!(er.fields[0], (b'S', "ERROR".to_owned()));
        assert_eq!(er.fields[1], (b'C', "57014".to_owned()));
    }
}
