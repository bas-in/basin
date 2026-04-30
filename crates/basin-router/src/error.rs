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

fn classify(err: &BasinError) -> (&'static str, &'static str) {
    match err {
        BasinError::InvalidIdent(_) | BasinError::InvalidSchema(_) => ("ERROR", "42601"), // syntax_error
        BasinError::NotFound(_) => ("ERROR", "42704"), // undefined_object
        BasinError::CommitConflict(_) => ("ERROR", "40001"), // serialization_failure
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
