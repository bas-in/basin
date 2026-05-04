//! Error responses.
//!
//! Every failed request comes back as `{ "code": "...", "message": "..." }`
//! with the appropriate HTTP status. The `code` field is a stable string a
//! client SDK can match on; `message` is human-readable and not promised to
//! be stable.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_common::BasinError;
use serde::Serialize;

/// Stable error codes. Surfaces in the JSON `code` field. Match these on the
/// client side; do not pattern-match on `message`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    Unauthenticated,
    Forbidden,
    NotFound,
    InvalidRequest,
    RateLimited,
    EngineUnsupported,
    PayloadTooLarge,
    Internal,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::Unauthenticated => "E_UNAUTHENTICATED",
            ErrorCode::Forbidden => "E_FORBIDDEN",
            ErrorCode::NotFound => "E_NOT_FOUND",
            ErrorCode::InvalidRequest => "E_INVALID_REQUEST",
            ErrorCode::RateLimited => "E_RATE_LIMITED",
            ErrorCode::EngineUnsupported => "E_ENGINE_UNSUPPORTED",
            ErrorCode::PayloadTooLarge => "E_INVALID_REQUEST",
            ErrorCode::Internal => "E_INTERNAL",
        }
    }

    pub fn status(self) -> StatusCode {
        match self {
            ErrorCode::Unauthenticated => StatusCode::UNAUTHORIZED,
            ErrorCode::Forbidden => StatusCode::FORBIDDEN,
            ErrorCode::NotFound => StatusCode::NOT_FOUND,
            ErrorCode::InvalidRequest => StatusCode::BAD_REQUEST,
            ErrorCode::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            ErrorCode::EngineUnsupported => StatusCode::NOT_IMPLEMENTED,
            ErrorCode::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[derive(Debug, Serialize)]
struct ErrorBody<'a> {
    code: &'a str,
    message: String,
}

/// Internal error that knows its own status. Implements `IntoResponse` so
/// handlers can `?` and the right thing happens.
#[derive(Debug)]
pub(crate) struct ApiError {
    pub code: ErrorCode,
    pub message: String,
}

impl ApiError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn unauthenticated(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unauthenticated, msg)
    }
    pub fn invalid(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidRequest, msg)
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, msg)
    }
    pub fn rate_limited(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::RateLimited, msg)
    }
    pub fn unsupported(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::EngineUnsupported, msg)
    }
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, msg)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            code: self.code.as_str(),
            message: self.message,
        };
        (self.code.status(), Json(body)).into_response()
    }
}

/// Map a [`BasinError`] coming back from the engine / auth layer to an
/// [`ApiError`]. The mapping is deliberately conservative: anything we can't
/// classify is a 500 with code `E_INTERNAL`. Errors with embedded user-facing
/// detail are passed through verbatim (e.g. `InvalidIdent`).
impl From<BasinError> for ApiError {
    fn from(err: BasinError) -> Self {
        match &err {
            BasinError::InvalidIdent(_) | BasinError::InvalidSchema(_) => {
                ApiError::invalid(err.to_string())
            }
            BasinError::NotFound(_) => ApiError::not_found(err.to_string()),
            BasinError::IsolationViolation(_) => {
                // Don't leak the message — fail-closed.
                tracing::error!(?err, "ISOLATION VIOLATION surfaced at REST layer");
                ApiError::internal("internal error")
            }
            _ => {
                // Engine errors (DataFusion, parser) often surface as Internal;
                // most are user-facing SQL problems. Surface the message but
                // keep the 400 shape unless the message contains "rate limit".
                let msg = err.to_string();
                if msg.contains("rate limited") {
                    ApiError::rate_limited(msg)
                } else if matches!(err, BasinError::Internal(_)) {
                    // The engine bundles a lot of "user wrote bad SQL" errors
                    // under Internal. Treat anything that looks like a parse /
                    // plan error as a 400 so clients get the message; fall back
                    // to 500 only for genuinely-opaque errors.
                    let lower = msg.to_ascii_lowercase();
                    if lower.contains("parse")
                        || lower.contains("plan")
                        || lower.contains("schema")
                        || lower.contains("type")
                        || lower.contains("expected")
                        || lower.contains("not supported")
                        || lower.contains("invalid")
                        || lower.contains("no field")
                        || lower.contains("unknown")
                    {
                        ApiError::invalid(msg)
                    } else {
                        ApiError::internal(msg)
                    }
                } else {
                    ApiError::internal(msg)
                }
            }
        }
    }
}
