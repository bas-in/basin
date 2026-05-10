//! HTTP status code mapping for [`basin_common::BasinError`] and friends.
//!
//! Iceberg REST clients (pyiceberg / Spark / Trino) inspect the HTTP
//! status to decide retry / surface; the JSON body carries an `error`
//! envelope per the open-api spec:
//!
//! ```json
//! { "error": { "message": "...", "type": "NoSuchTableException", "code": 404 } }
//! ```
//!
//! That envelope is what pyiceberg parses; we emit it on every failure
//! so error handling is interoperable.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_common::BasinError;
use serde::Serialize;
use thiserror::Error;

/// Error type returned by every Iceberg REST handler. Implements
/// `IntoResponse` so handlers can `?` cleanly.
#[derive(Debug, Error)]
pub enum IcebergRestError {
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("internal: {0}")]
    Internal(String),
}

impl IcebergRestError {
    fn status(&self) -> StatusCode {
        match self {
            IcebergRestError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            IcebergRestError::BadRequest(_) => StatusCode::BAD_REQUEST,
            IcebergRestError::NotFound(_) => StatusCode::NOT_FOUND,
            IcebergRestError::Conflict(_) => StatusCode::CONFLICT,
            IcebergRestError::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            IcebergRestError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Iceberg-spec exception type names. pyiceberg matches on these
    /// strings to translate REST errors into client-side exception
    /// classes (`NoSuchNamespaceException`, `NoSuchTableException`,
    /// etc).
    fn kind(&self) -> &'static str {
        match self {
            IcebergRestError::Unauthorized(_) => "NotAuthorizedException",
            IcebergRestError::BadRequest(_) => "BadRequestException",
            IcebergRestError::NotFound(_) => "NoSuchTableException",
            IcebergRestError::Conflict(_) => "AlreadyExistsException",
            IcebergRestError::NotImplemented(_) => "NotImplementedException",
            IcebergRestError::Internal(_) => "ServiceUnavailableException",
        }
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: ErrorInner,
}

#[derive(Serialize)]
struct ErrorInner {
    message: String,
    #[serde(rename = "type")]
    kind: String,
    code: u16,
}

impl IntoResponse for IcebergRestError {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = ErrorBody {
            error: ErrorInner {
                message: self.to_string(),
                kind: self.kind().to_string(),
                code: status.as_u16(),
            },
        };
        (status, Json(body)).into_response()
    }
}

impl From<BasinError> for IcebergRestError {
    fn from(err: BasinError) -> Self {
        match &err {
            BasinError::NotFound(_) => IcebergRestError::NotFound(err.to_string()),
            BasinError::InvalidIdent(_) | BasinError::InvalidSchema(_) => {
                IcebergRestError::BadRequest(err.to_string())
            }
            BasinError::CommitConflict(_) => IcebergRestError::Conflict(err.to_string()),
            BasinError::IsolationViolation(_) => {
                tracing::error!(?err, "ISOLATION VIOLATION at iceberg-rest layer");
                IcebergRestError::Internal("internal error".to_string())
            }
            BasinError::FeatureNotSupported(_) => IcebergRestError::NotImplemented(err.to_string()),
            _ => IcebergRestError::Internal(err.to_string()),
        }
    }
}
