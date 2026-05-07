//! Shared types for `basin-net`.
//!
//! [`HttpRequest`] / [`HttpResponse`] mirror the columns the Postgres `http`
//! extension surfaces (`status`, `content`, headers); [`ResponseRow`] mirrors
//! the `net._http_response` row in `pg_net`.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// Stable surrogate id for an enqueued async request. The same id is the
/// primary key of the resulting `_net_http_response` row, so callers
/// correlate request → response by it.
pub type RequestId = Uuid;

/// One outbound HTTP request. Built either by [`crate::HttpClient`] (sync)
/// or [`crate::RequestQueue`] (async).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpRequest {
    /// Absolute URL. Validated against the per-tenant allowlist before the
    /// outbound call.
    pub url: String,
    /// Always `"GET"` or `"POST"` in v0.1. PUT/DELETE/PATCH are TODO.
    pub method: String,
    /// Header set in deterministic order. We deliberately use `BTreeMap` so
    /// the persisted response row's `headers` JSON is stable across runs;
    /// pg_net does the same.
    pub headers: BTreeMap<String, String>,
    /// Optional body. Bounded by [`crate::GuardConfig::max_body_bytes`].
    pub body: Option<Vec<u8>>,
    /// Per-request timeout. Defaults to [`crate::GuardConfig::timeout`].
    pub timeout: Option<Duration>,
}

impl HttpRequest {
    pub fn get(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            method: "GET".into(),
            headers: BTreeMap::new(),
            body: None,
            timeout: None,
        }
    }

    pub fn post(
        url: impl Into<String>,
        body: impl Into<Vec<u8>>,
        content_type: impl Into<String>,
    ) -> Self {
        let mut headers = BTreeMap::new();
        headers.insert("content-type".into(), content_type.into());
        Self {
            url: url.into(),
            method: "POST".into(),
            headers,
            body: Some(body.into()),
            timeout: None,
        }
    }

    /// Add or override a header. Returns the same request for chaining.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into().to_lowercase(), value.into());
        self
    }
}

/// One inbound HTTP response. The `error` field is populated when the request
/// did not produce a response at all (DNS failure, connect timeout, allowlist
/// rejection, body cap exceeded). When `error` is `Some`, `status` is `0` and
/// `body` is empty — same shape pg_net uses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpResponse {
    /// HTTP status code, or 0 if `error.is_some()`.
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    /// Response body bytes. Truncated at [`crate::GuardConfig::max_body_bytes`]
    /// — beyond that we surface an error rather than a silent truncation.
    pub body: Vec<u8>,
    /// Reason the request failed before producing a response. Mutually
    /// exclusive with a non-zero status.
    pub error: Option<String>,
}

impl HttpResponse {
    /// Best-effort UTF-8 view of the body; `String::from_utf8_lossy` so
    /// binary payloads don't panic, matching what the `http` extension
    /// puts in its `content TEXT` column.
    pub fn body_as_text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// One row of `net._http_response`. Compatible with pg_net's column set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseRow {
    /// Same uuid the queue handed back at submit time.
    pub id: RequestId,
    /// Convenience alias used by some pg_net consumers; equal to `id` in
    /// v0.1.
    pub request_id: RequestId,
    pub status: u16,
    /// JSON object of header name → value pairs. Stored as TEXT in v0.1
    /// (the column type system doesn't yet take `JSONB` from a UDF arg —
    /// see crate-level TODOs).
    pub headers: String,
    pub body: String,
    pub error: Option<String>,
    pub completed_at: DateTime<Utc>,
}

/// Local error type. Most call sites bubble these up through
/// `basin_common::BasinError::Internal` — keeping the original variant
/// helps tests assert on the failure mode without grepping strings.
#[derive(Debug, Error)]
pub enum HttpError {
    #[error("host not on allowlist: {host}")]
    HostDenied { host: String },
    #[error("invalid url: {0}")]
    InvalidUrl(String),
    #[error("body exceeds cap of {cap} bytes (was {actual})")]
    BodyTooLarge { cap: usize, actual: usize },
    #[error("rate limited (basin_net)")]
    RateLimited,
    #[error("request timed out after {0:?}")]
    Timeout(Duration),
    #[error("transport: {0}")]
    Transport(String),
}

impl HttpError {
    /// Render to a `basin_common::BasinError::Internal`. Most call sites
    /// only care about the message body and the fact that the call failed,
    /// not the variant — except tests, which match on variants directly.
    pub fn to_basin(self) -> basin_common::BasinError {
        basin_common::BasinError::internal(format!("{self}"))
    }
}
