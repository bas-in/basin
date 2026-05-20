//! `POST /in/:project/:name` — inbound webhook receiver (5.11.N, ADR 0019).
//!
//! ## Contract (ADR 0019)
//!
//! 1. Resolve `(project_slug, name)` → `InboundWebhookDef` from the catalog.
//!    Project slug in the URL path is the `ProjectId` string; v0.1 does not
//!    add a human-readable slug layer — the ULID is the identifier.
//! 2. Buffer the raw POST body (body cap enforced by the global
//!    `DefaultBodyLimit` middleware in `server.rs`).
//! 3. Execute the registered SQL body against an engine session opened for the
//!    project, substituting the raw body as the `payload` jsonb bind parameter.
//!    v0.1 performs the substitution via a literal embed — the body is already
//!    a buffer of bytes we pass as a PostgreSQL `jsonb` literal after parsing
//!    and re-serialising through `serde_json` (parse validates UTF-8 / JSON
//!    well-formedness; re-serialise normalises whitespace).
//! 4. Return HTTP 200 `{"ok": true}` on commit, 4xx / 5xx on error.
//!
//! ## Auth
//!
//! Inbound webhooks are HMAC-authenticated by the caller (e.g. Stripe). v0.1
//! ships **without** HMAC verification — the ADR marks the full signature
//! scheme as v0.2. The endpoint is accessible without a bearer token; ADR 0019
//! § security explains that CSRF does not apply and no browser session is
//! involved. Operators requiring access control before v0.2 can gate this
//! behind a reverse-proxy rule.
//!
//! ## SQL injection defence
//!
//! The payload is parsed through `serde_json`, re-serialised to a canonical
//! JSON string, and embedded as a PostgreSQL `jsonb` quoted literal
//! (`'…'::jsonb`). No raw user bytes are spliced into the SQL text. The body
//! text is the registered SQL body from the catalog (trusted DDL, not
//! user-supplied at request time).

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_catalog::Catalog as _;
use basin_common::ProjectId;

use crate::errors::ApiError;
use crate::server::Inner;

/// `POST /in/:project_id/:name`
///
/// Receives a raw JSON POST body, looks up the registered inbound webhook,
/// and executes its SQL body with `payload` bound to the JSON body.
#[axum::debug_handler]
pub(crate) async fn post_inbound_webhook(
    State(state): State<Arc<Inner>>,
    Path((project_str, name)): Path<(String, String)>,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Parse project_id from the URL segment.
    let project_id: ProjectId = project_str
        .parse()
        .map_err(|_| ApiError::not_found(format!("project {project_str:?} not found")))?;

    // Validate webhook name as a SQL identifier.
    let name = crate::parser::validate_ident(&name)?;

    // Catalog lookup.
    let def = state
        .cfg
        .engine
        .config()
        .catalog
        .lookup_inbound_webhook(&project_id, &name)
        .await
        .ok_or_else(|| {
            ApiError::not_found(format!("inbound webhook {name:?} not found for project"))
        })?;

    // Parse body as JSON (validates well-formedness + UTF-8). Empty body is
    // treated as `null`.
    let payload_value: serde_json::Value = if body.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| ApiError::invalid(format!("request body is not valid JSON: {e}")))?
    };

    // Embed payload as a literal jsonb argument in the SQL body. Re-serialise
    // through serde_json to canonicalise (no raw user bytes in SQL text).
    let payload_literal = build_jsonb_literal(&payload_value);

    // Rewrite `payload` → the literal. We use a conservative word-boundary
    // replacement: replace occurrences of the standalone keyword `payload`
    // (not part of a longer identifier) with the literal. This is safe
    // because `def.body` is DDL-time SQL registered by the project owner, not
    // supplied at request time.
    let sql = replace_payload_identifier(&def.body, &payload_literal);

    // Execute against the project session.
    let session = state
        .cfg
        .engine
        .open_session(project_id)
        .await
        .map_err(ApiError::from)?;
    session.execute(&sql).await.map_err(ApiError::from)?;

    Ok(Json(serde_json::json!({ "ok": true })).into_response())
}

/// Render a `serde_json::Value` as a PostgreSQL `'…'::jsonb` literal.
fn build_jsonb_literal(v: &serde_json::Value) -> String {
    // serde_json's Display escapes single-quotes inside strings as `\'` but PG
    // uses `''` (doubled). We go through `to_string()` (which gives us
    // standard JSON with `\"` for double-quotes, no single-quotes inside JSON
    // values by construction for scalar types). JSON strings never contain bare
    // single-quotes because JSON uses double-quote delimiters — the only risk
    // is if a JSON string *value* contains a single-quote character. We escape
    // it by doubling.
    let json_str = v.to_string();
    let escaped = json_str.replace('\'', "''");
    format!("'{escaped}'::jsonb")
}

/// Replace standalone occurrences of the identifier `payload` in `sql` with
/// `replacement`. Word boundaries: not preceded or followed by `[A-Za-z0-9_]`.
fn replace_payload_identifier(sql: &str, replacement: &str) -> String {
    let needle = "payload";
    let bytes = sql.as_bytes();
    let needle_bytes = needle.as_bytes();
    let nlen = needle_bytes.len();
    let mut out = String::with_capacity(sql.len() + replacement.len());
    let mut i = 0;
    while i < bytes.len() {
        // Check if we match `payload` at position i with word boundaries.
        if i + nlen <= bytes.len()
            && bytes[i..i + nlen].eq_ignore_ascii_case(needle_bytes)
        {
            let before_ok = i == 0
                || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
            let after_ok = i + nlen == bytes.len()
                || !(bytes[i + nlen].is_ascii_alphanumeric() || bytes[i + nlen] == b'_');
            if before_ok && after_ok {
                out.push_str(replacement);
                i += nlen;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replace_payload_simple() {
        let sql = "INSERT INTO events (data) VALUES (payload)";
        let result = replace_payload_identifier(sql, "'{}' ::jsonb");
        assert_eq!(result, "INSERT INTO events (data) VALUES ('{}' ::jsonb)");
    }

    #[test]
    fn replace_payload_does_not_touch_payload_id() {
        // `payload_id` must NOT be replaced.
        let sql = "INSERT INTO t (payload_id, data) VALUES (payload_id, payload)";
        let result = replace_payload_identifier(sql, "X");
        assert_eq!(result, "INSERT INTO t (payload_id, data) VALUES (payload_id, X)");
    }

    #[test]
    fn build_jsonb_literal_escapes_single_quotes() {
        // JSON string values CAN contain single-quotes (rare but possible via
        // embedding raw chars). Verify doubling.
        let v: serde_json::Value = serde_json::json!({"key": "it's"});
        let lit = build_jsonb_literal(&v);
        // The literal must be valid SQL: single-quote inside → doubled.
        assert!(lit.contains("it''s"), "got: {lit}");
        assert!(lit.ends_with("::jsonb"));
    }
}
