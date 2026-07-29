//! Typed errors mirroring basin-rest's error envelope.
//!
//! Every failed request returns `{"code": "...", "message": "..."}` with an
//! appropriate HTTP status (`crates/basin-rest/src/errors.rs`). `code` is the
//! stable contract; `message` is human-readable and **not** promised to be
//! stable — match on [`BasinError::code`], never on the message text.

/// Stable error codes emitted by basin-rest (`errors.rs ErrorCode::as_str`).
///
/// A newer server may emit a code not listed here; such codes pass through as
/// the raw string in [`ApiError::code`] so an older SDK keeps working.
pub const ERROR_CODES: &[&str] = &[
    "E_UNAUTHENTICATED",
    "E_FORBIDDEN",
    "E_NOT_FOUND",
    "E_INVALID_REQUEST",
    "E_RATE_LIMITED",
    "E_ENGINE_UNSUPPORTED",
    "E_INTERNAL",
    "E_EMAIL_DISABLED",
    "E_REVOKED_TOKEN",
];

/// A non-2xx response decoded from a Basin server's error envelope.
///
/// Carries the stable [`code`](ApiError::code), the human-readable
/// [`message`](ApiError::message), the HTTP [`status`](ApiError::status), and —
/// when present — the Postgres [`sqlstate`](ApiError::sqlstate) five-character
/// class code surfaced by the engine for SQL-level failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("[{code} HTTP {status}] {message}")]
pub struct ApiError {
    /// Stable error code (`E_*`). Unknown codes pass through verbatim.
    pub code: String,
    /// Human-readable detail. Do **not** match on this; use [`code`](Self::code).
    pub message: String,
    /// HTTP status of the response (`0` when synthesised client-side without a
    /// round-trip, e.g. a missing project id).
    pub status: u16,
    /// Postgres `SQLSTATE` class code (e.g. `"23505"` for unique violation),
    /// when the server included one in the error envelope.
    pub sqlstate: Option<String>,
}

impl ApiError {
    /// Construct an [`ApiError`] from its parts.
    pub fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        status: u16,
    ) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            status,
            sqlstate: None,
        }
    }

    /// Returns `true` when [`code`](Self::code) is one of the documented
    /// [`ERROR_CODES`]; `false` for pass-through codes from a newer server.
    pub fn is_known_code(&self) -> bool {
        ERROR_CODES.contains(&self.code.as_str())
    }
}

/// The error type returned by every fallible SDK operation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BasinError {
    /// A non-2xx response carrying Basin's error envelope.
    #[error(transparent)]
    Api(#[from] ApiError),

    /// The transport layer failed before a server response was received
    /// (connection refused, DNS failure, timeout, TLS error, …).
    #[error("network error: {0}")]
    Network(String),

    /// The response body could not be deserialised into the expected type.
    #[error("failed to decode response: {0}")]
    Decode(String),

    /// A precondition required by the SDK was not met (e.g. a project id is
    /// required but none was supplied). Carries `status == 0`.
    #[error("{0}")]
    InvalidRequest(String),

    /// A realtime / WebSocket transport error (only with the `realtime`
    /// feature).
    #[cfg(feature = "realtime")]
    #[error("realtime error: {0}")]
    Realtime(String),
}

impl BasinError {
    /// Convenience constructor for a client-side invalid-request error.
    pub(crate) fn invalid_request(msg: impl Into<String>) -> Self {
        BasinError::InvalidRequest(msg.into())
    }

    /// If this is an [`BasinError::Api`] error, return its stable code.
    pub fn code(&self) -> Option<&str> {
        match self {
            BasinError::Api(e) => Some(&e.code),
            _ => None,
        }
    }

    /// If this is an [`BasinError::Api`] error, return its HTTP status.
    pub fn status(&self) -> Option<u16> {
        match self {
            BasinError::Api(e) => Some(e.status),
            _ => None,
        }
    }

    /// If this is an [`BasinError::Api`] error, return its `SQLSTATE`, if any.
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            BasinError::Api(e) => e.sqlstate.as_deref(),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for BasinError {
    fn from(e: reqwest::Error) -> Self {
        if e.is_decode() {
            BasinError::Decode(e.to_string())
        } else {
            BasinError::Network(e.to_string())
        }
    }
}

/// Decode a JSON error envelope `{code, message, sqlstate?}` into an
/// [`ApiError`]. Falls back to sensible defaults when fields are missing or
/// the body is not the expected shape.
pub(crate) fn decode_error_envelope(status: u16, body: &str) -> ApiError {
    let mut code = "E_UNKNOWN".to_string();
    let mut message = format!("HTTP {status}");
    let mut sqlstate = None;

    if !body.is_empty() {
        match serde_json::from_str::<serde_json::Value>(body) {
            Ok(serde_json::Value::Object(map)) => {
                if let Some(serde_json::Value::String(c)) = map.get("code") {
                    code = c.clone();
                }
                match map.get("message") {
                    Some(serde_json::Value::String(m)) => message = m.clone(),
                    _ => message = body.to_string(),
                }
                // The engine surfaces SQLSTATE either as a top-level field or
                // nested under `details.sqlstate` depending on the route.
                if let Some(serde_json::Value::String(s)) = map.get("sqlstate") {
                    sqlstate = Some(s.clone());
                } else if let Some(serde_json::Value::Object(d)) = map.get("details") {
                    if let Some(serde_json::Value::String(s)) = d.get("sqlstate") {
                        sqlstate = Some(s.clone());
                    }
                }
            }
            // Body is valid JSON but not an object, or invalid JSON: keep the
            // raw text as the message.
            _ => message = body.to_string(),
        }
    }

    ApiError {
        code,
        message,
        status,
        sqlstate,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_full_envelope() {
        let e = decode_error_envelope(
            409,
            r#"{"code":"E_INVALID_REQUEST","message":"dup","sqlstate":"23505"}"#,
        );
        assert_eq!(e.code, "E_INVALID_REQUEST");
        assert_eq!(e.message, "dup");
        assert_eq!(e.status, 409);
        assert_eq!(e.sqlstate.as_deref(), Some("23505"));
        assert!(e.is_known_code());
    }

    #[test]
    fn decodes_nested_sqlstate() {
        let e = decode_error_envelope(
            400,
            r#"{"code":"E_INTERNAL","message":"x","details":{"sqlstate":"42P01"}}"#,
        );
        assert_eq!(e.sqlstate.as_deref(), Some("42P01"));
    }

    #[test]
    fn unknown_code_passes_through() {
        let e = decode_error_envelope(418, r#"{"code":"E_TEAPOT","message":"brew"}"#);
        assert_eq!(e.code, "E_TEAPOT");
        assert!(!e.is_known_code());
    }

    #[test]
    fn non_object_body_becomes_message() {
        let e = decode_error_envelope(500, "upstream exploded");
        assert_eq!(e.code, "E_UNKNOWN");
        assert_eq!(e.message, "upstream exploded");
    }

    #[test]
    fn display_includes_code_and_status() {
        let e = ApiError {
            code: "E_INVALID_REQUEST".into(),
            message: "dup".into(),
            status: 409,
            sqlstate: Some("23505".into()),
        };
        let s = e.to_string();
        assert!(s.contains("E_INVALID_REQUEST"));
        assert!(s.contains("409"));
    }
}
