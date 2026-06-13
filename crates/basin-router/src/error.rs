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

/// Multi-region v0.2: if `err` is the [`BasinError::ForwardReplay`] control
/// signal, return the value of the Fly `fly-replay` response header that the
/// edge should emit (`region=<home_region>;...`), so Fly re-fires the request
/// against the project's home region. Returns `None` for any other error (the
/// caller then renders a normal `ErrorResponse` via [`error_response`]).
///
/// The directive carries the replay TOKEN in the `instance`-style state field
/// so the home region (and Fly's own loop detection) can observe it; the
/// `region=` key is the part Fly acts on. Header shape per Fly's
/// `fly-replay` docs: `region=<code>;state=<opaque>`.
///
/// This is the ONE place the engine binary's edge needs to call to honour
/// `BASIN_WRITE_FORWARD_MODE=fly-replay`; the pgwire edge cannot carry a
/// mid-session replay (no HTTP response to attach a header to), so the
/// pgwire path falls back to the `http-forward` mode or the 08006 render.
pub fn fly_replay_directive(err: &BasinError) -> Option<String> {
    match err {
        BasinError::ForwardReplay { home_region, token } => {
            Some(format!("region={home_region};state={token}"))
        }
        _ => None,
    }
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
        // Named cursor not found. PG raises 34000 (invalid_cursor_name);
        // psycopg exposes it as `InvalidCursorName`.
        BasinError::CursorNotFound(_) => ("ERROR", "34000"), // invalid_cursor_name
        // ON CONFLICT target unresolvable. PG raises 42P10
        // (invalid_column_reference — "there is no unique or exclusion
        // constraint matching the ON CONFLICT specification").
        BasinError::AmbiguousConflictTarget(_) => ("ERROR", "42P10"), // invalid_column_reference
        BasinError::CommitConflict(_) => ("ERROR", "40001"), // serialization_failure
        BasinError::QueryCostExceeded(_) => ("ERROR", "54000"), // program_limit_exceeded
        BasinError::QueryCanceled(_) => ("ERROR", "57014"),     // query_canceled
        BasinError::FeatureNotSupported(_) => ("ERROR", "0A000"), // feature_not_supported
        // MERGE matched the same target row from two source rows. PostgreSQL
        // raises 21000 (cardinality_violation) with the message
        // "MERGE command cannot affect row a second time".
        BasinError::CardinalityViolation(_) => ("ERROR", "21000"), // cardinality_violation
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
        // Multi-region phase 6 (ADR 0009): the statement reached the wrong
        // region for its project — a write, or a primary-tier read, against a
        // project homed elsewhere. This is NOT retryable in place: retrying on
        // the same connection hits the same region and fails again. We map it
        // to SQLSTATE 08006 (`connection_failure`, class 08 =
        // connection_exception): the message names the home region so a
        // region-aware client / pgwire pooler reconnects to the home endpoint
        // and retries THERE. (When a WriteForwarder is registered the engine
        // forwards instead of raising this, so the router only sees the error
        // in the fail-loud / forwarding-disabled deployment.)
        BasinError::WrongRegion(_) => ("ERROR", "08006"), // connection_failure
        // Multi-region v0.2: the fly-replay control signal. The Fly-aware edge
        // (see `fly_replay_directive`) intercepts this BEFORE classify() and
        // emits a `fly-replay: region=<home>` header instead of an
        // ErrorResponse. If it reaches here, the deployment's edge does not
        // understand replay, so we degrade to the same fail-loud 08006 a
        // no-forwarder deployment raises — the Display form already names the
        // home region, so a region-aware client reconnects correctly.
        BasinError::ForwardReplay { .. } => ("ERROR", "08006"), // connection_failure
        // Phase 5.28.B: lock_timeout expiry — PostgreSQL raises 55P03.
        BasinError::LockNotAvailable(_) => ("ERROR", "55P03"), // lock_not_available
        // Advisory-lock deadlock detection — PostgreSQL raises 40P01
        // (deadlock_detected) the instant it breaks a waiter cycle. Drivers
        // expose it as a dedicated exception class (psycopg DeadlockDetected)
        // and applications retry the losing transaction.
        BasinError::DeadlockDetected(_) => ("ERROR", "40P01"), // deadlock_detected
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
    fn classifies_wrong_region() {
        // A write / primary-read against a non-home project must surface as
        // SQLSTATE 08006 (connection_failure, class 08) with BOTH region
        // names in the message so a region-aware client reconnects to home.
        let er = error_response(&BasinError::wrong_region_for(
            "proj_abc",
            "write",
            "us-east-1",
            "eu-west-1",
        ));
        assert_eq!(er.fields[0], (b'S', "ERROR".to_owned()));
        assert_eq!(er.fields[1], (b'C', "08006".to_owned()));
        // Message names the home region AND the local region.
        assert!(er.fields[2].1.contains("us-east-1"));
        assert!(er.fields[2].1.contains("eu-west-1"));
        assert!(er.fields[2].1.contains("proj_abc"));
    }

    #[test]
    fn classifies_deadlock_detected() {
        // Advisory-lock deadlock detection must surface as SQLSTATE 40P01
        // (deadlock_detected) — the exact code PostgreSQL raises — so drivers
        // map it to the dedicated DeadlockDetected exception class.
        let er = error_response(&BasinError::deadlock_detected("advisory lock cycle on key 7"));
        assert_eq!(er.fields[0], (b'S', "ERROR".to_owned()));
        assert_eq!(er.fields[1], (b'C', "40P01".to_owned()));
        assert!(er.fields[2].1.contains("deadlock detected"));
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

    #[test]
    fn classifies_cursor_not_found() {
        // Cursor-not-found must surface as SQLSTATE 34000 (invalid_cursor_name)
        // with the PG message shape `cursor "<name>" does not exist`.
        let er = error_response(&BasinError::CursorNotFound("my_cursor".into()));
        assert_eq!(er.fields[0], (b'S', "ERROR".to_owned()));
        assert_eq!(er.fields[1], (b'C', "34000".to_owned()));
        assert_eq!(
            er.fields[2],
            (b'M', "cursor \"my_cursor\" does not exist".to_owned())
        );
    }

    #[test]
    fn classifies_ambiguous_conflict_target() {
        // ON CONFLICT target mismatch must surface as SQLSTATE 42P10
        // (invalid_column_reference).
        let er = error_response(&BasinError::AmbiguousConflictTarget("a, b".into()));
        assert_eq!(er.fields[0], (b'S', "ERROR".to_owned()));
        assert_eq!(er.fields[1], (b'C', "42P10".to_owned()));
        assert!(er.fields[2].1.contains("no unique or exclusion constraint"));
    }
}
