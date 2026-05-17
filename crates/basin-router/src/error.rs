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
        "connection limit reached".to_owned(),
    );
    info.into()
}

fn classify(err: &BasinError) -> (&'static str, &'static str) {
    match err {
        BasinError::InvalidIdent(_) | BasinError::InvalidSchema(_) => ("ERROR", "42601"), // syntax_error
        BasinError::NotFound(_) => ("ERROR", "42704"), // undefined_object
        BasinError::CommitConflict(_) => ("ERROR", "40001"), // serialization_failure
        BasinError::QueryCostExceeded(_) => ("ERROR", "54000"), // program_limit_exceeded
        BasinError::FeatureNotSupported(_) => ("ERROR", "0A000"), // feature_not_supported
        BasinError::UniqueViolation(_) => ("ERROR", "23505"), // unique_violation
        BasinError::CheckViolation(_) => ("ERROR", "23514"), // check_violation
        BasinError::ForeignKeyViolation(_) => ("ERROR", "23503"), // foreign_key_violation
        BasinError::StringTooLong(_) => ("ERROR", "22001"), // string_data_right_truncation
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
}
