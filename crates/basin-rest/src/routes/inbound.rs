//! `POST /in/:project/:name` — inbound webhook receiver
//! (5.11.N, ADR 0019, Phase 6.SEC.P0.3).
//!
//! ## Contract (ADR 0019 + Phase 6.SEC.P0.3)
//!
//! 1. Resolve `(project_slug, name)` → `InboundWebhookDef` from the catalog.
//!    Project slug in the URL path is the `ProjectId` string; v0.1 does not
//!    add a human-readable slug layer — the ULID is the identifier.
//! 2. **Default-secure HMAC check.** The caller MUST send
//!    `X-Basin-Signature: <hex>` where `<hex>` is the lowercase hex of
//!    HMAC-SHA256(body, secret). The secret is the value returned by
//!    `CREATE INBOUND WEBHOOK …` (the `secret` column of the result set),
//!    stored verbatim on the catalog row. Verification uses
//!    [`subtle::ConstantTimeEq`] — no early-exit on mismatch.
//!    Missing header / wrong length / invalid hex / wrong MAC → 401 with
//!    `code: "E_UNAUTHENTICATED"`.
//! 3. Buffer the raw POST body (body cap enforced by the global
//!    `DefaultBodyLimit` middleware in `server.rs`).
//! 4. Execute the registered SQL body against an engine session opened for the
//!    project, substituting the raw body as the `payload` jsonb bind parameter.
//!    The body is parsed and re-serialised through `serde_json` (validates
//!    UTF-8 / JSON well-formedness; canonicalises whitespace).
//! 5. Return HTTP 200 `{"ok": true}` on commit, 4xx / 5xx on error.
//!
//! ## Debug bypass
//!
//! When `BASIN_NET_ALLOW_PLAINTEXT_WEBHOOKS=1` is set in the server's env,
//! requests without `X-Basin-Signature` are accepted (debug/dev only —
//! never set in prod; see ADR 0019 § "TLS downgrade"). Requests that DO
//! supply a signature still go through full verification even with the
//! bypass active — the bypass widens the unsigned path, it does not weaken
//! the signed path. A `tracing::warn!` line fires on bypass so operators
//! can spot the misconfiguration in logs.
//!
//! ## SQL-injection defence
//!
//! The payload is parsed through `serde_json`, re-serialised to a canonical
//! JSON string, and embedded as a PostgreSQL `jsonb` quoted literal
//! (`'…'::jsonb`). No raw user bytes are spliced into the SQL text. The body
//! text is the registered SQL body from the catalog (trusted DDL, not
//! user-supplied at request time).

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_catalog::Catalog as _;
use basin_common::ProjectId;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::errors::ApiError;
use crate::server::Inner;

/// HMAC header name. Lowercase to match HTTP/2 normalisation; `HeaderMap`
/// lookup is case-insensitive regardless.
const SIG_HEADER: &str = "x-basin-signature";

/// Env var that opens the unsigned-request path. Set to `"1"` to enable
/// (debug/dev only). Any other value (or unset) keeps the secure default.
const PLAINTEXT_BYPASS_ENV: &str = "BASIN_NET_ALLOW_PLAINTEXT_WEBHOOKS";

/// `POST /in/:project_id/:name`
///
/// Receives a raw JSON POST body, verifies the `X-Basin-Signature` header
/// against the registered webhook secret, looks up the registered inbound
/// webhook, and executes its SQL body with `payload` bound to the JSON body.
#[axum::debug_handler]
pub(crate) async fn post_inbound_webhook(
    State(state): State<Arc<Inner>>,
    Path((project_str, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Parse project_id from the URL segment.
    let project_id: ProjectId = project_str
        .parse()
        .map_err(|_| ApiError::not_found(format!("project {project_str:?} not found")))?;

    // Validate webhook name as a SQL identifier.
    let name = crate::parser::validate_ident(&name)?;

    // Catalog lookup. We always look up the def first so that an attacker
    // who guesses the URL path but provides no signature gets the same
    // 401-shaped failure regardless of whether the webhook exists — this
    // prevents using the 404 vs 401 distinction to enumerate webhook
    // names. (Enumeration of project ULIDs is already hard: 26 chars of
    // crockford32 = ~125 bits.)
    let def = state
        .cfg
        .engine
        .config()
        .catalog
        .lookup_inbound_webhook(&project_id, &name)
        .await;

    // HMAC verification (Phase 6.SEC.P0.3). Default-secure: require the
    // header unless the env-gated debug bypass is on AND the caller
    // omitted the header entirely.
    let sig_header = headers.get(SIG_HEADER).and_then(|v| v.to_str().ok());
    let bypass = plaintext_bypass_enabled();
    match sig_header {
        Some(provided_hex) => {
            // Header present → MUST verify, even with bypass on.
            let def = def.as_ref().ok_or_else(|| {
                // Webhook does not exist; reject with 401 (not 404) so an
                // attacker can't distinguish "wrong sig" from "no such
                // hook" by header presence alone.
                ApiError::unauthenticated("invalid webhook signature")
            })?;
            verify_signature(&def.secret_hex, &body, provided_hex)
                .map_err(|_| ApiError::unauthenticated("invalid webhook signature"))?;
        }
        None => {
            if !bypass {
                return Err(ApiError::unauthenticated(format!(
                    "missing required header {SIG_HEADER:?} (HMAC-SHA256 of body, hex-encoded)"
                )));
            }
            // Bypass on + no header → accept, but require the webhook to
            // exist (404 on miss is fine on the bypass path).
            if def.is_none() {
                return Err(ApiError::not_found(format!(
                    "inbound webhook {name:?} not found for project"
                )));
            }
            tracing::warn!(
                project = %project_id,
                webhook = %name,
                "inbound webhook accepted without {SIG_HEADER} ({PLAINTEXT_BYPASS_ENV}=1)"
            );
        }
    }
    // Past the auth gate: def is guaranteed Some by either branch above.
    let def = def.expect("def present after auth gate");

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

/// Verify an `X-Basin-Signature` header value against the registered
/// webhook secret. Returns `Ok(())` on match, `Err(())` on any failure
/// mode (length mismatch, non-hex char, MAC mismatch). The MAC compare
/// is constant-time via [`subtle::ConstantTimeEq`], and length-mismatch
/// rejection still pays the full HMAC compute so callers can't time the
/// failure mode (the early-return on hex-decode failure is acceptable
/// because hex validity is not secret).
fn verify_signature(secret_hex: &str, body: &[u8], provided_hex: &str) -> Result<(), ()> {
    // Decode the registered secret. Stored as hex on the catalog row
    // (DDL guarantees ≥ 32 even-length hex), but be defensive in case of
    // a manually-edited row.
    let secret_bytes = hex::decode(secret_hex).map_err(|_| ())?;

    // Compute MAC over the body.
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&secret_bytes).map_err(|_| ())?;
    mac.update(body);
    let expected = mac.finalize().into_bytes(); // 32-byte SHA-256 output.

    // Decode the provided header.
    let provided = match hex::decode(provided_hex.trim()) {
        Ok(b) => b,
        Err(_) => return Err(()),
    };

    // Both vectors must be the same length for ConstantTimeEq; a
    // length mismatch still falls through with a constant-time compare
    // against the (longer) expected value so timing observers can't
    // distinguish a length mismatch from a value mismatch any easier
    // than a value mismatch alone.
    if provided.len() != expected.len() {
        // Burn the time anyway: compare against a zero vector of the
        // same length to keep the path uniform-ish, then reject.
        let _ = expected.ct_eq(&vec![0u8; expected.len()]);
        return Err(());
    }
    if provided.ct_eq(&expected).into() {
        Ok(())
    } else {
        Err(())
    }
}

/// Is the env-gated unsigned-request bypass active? Reads
/// `BASIN_NET_ALLOW_PLAINTEXT_WEBHOOKS` on every call so test runs can
/// flip it via `std::env::set_var`.
fn plaintext_bypass_enabled() -> bool {
    std::env::var(PLAINTEXT_BYPASS_ENV).as_deref() == Ok("1")
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
        if i + nlen <= bytes.len() && bytes[i..i + nlen].eq_ignore_ascii_case(needle_bytes) {
            let before_ok =
                i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
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
        assert_eq!(
            result,
            "INSERT INTO t (payload_id, data) VALUES (payload_id, X)"
        );
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

    // ---- Phase 6.SEC.P0.3: HMAC verification ----------------------------

    /// Compute the canonical signature a caller would send for a body.
    /// Mirrors what an SDK would do: hex(HMAC-SHA256(body, secret)).
    fn canonical_sig(secret_hex: &str, body: &[u8]) -> String {
        let secret = hex::decode(secret_hex).unwrap();
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&secret).unwrap();
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
    }

    #[test]
    fn verify_signature_accepts_valid_mac() {
        let secret = "a".repeat(64);
        let body = br#"{"id":"evt_001","amount":99}"#;
        let sig = canonical_sig(&secret, body);
        assert!(verify_signature(&secret, body, &sig).is_ok());
    }

    #[test]
    fn verify_signature_rejects_wrong_mac() {
        let secret = "a".repeat(64);
        let body = br#"{"id":"evt_001"}"#;
        let bad = "f".repeat(64); // 32 bytes of `0xff` — wrong MAC.
        assert!(verify_signature(&secret, body, &bad).is_err());
    }

    #[test]
    fn verify_signature_rejects_short_hex() {
        let secret = "a".repeat(64);
        let body = b"{}";
        // 31 bytes worth — length mismatch.
        let short = "ab".repeat(31);
        assert!(verify_signature(&secret, body, &short).is_err());
    }

    #[test]
    fn verify_signature_rejects_non_hex() {
        let secret = "a".repeat(64);
        let body = b"{}";
        let garbage = "z".repeat(64);
        assert!(verify_signature(&secret, body, &garbage).is_err());
    }

    #[test]
    fn verify_signature_uses_constant_time_compare() {
        // Smoke-test the ct path: equal-length but differing-first-byte
        // vs differing-last-byte should both reject without panic.
        let secret = "a".repeat(64);
        let body = b"abc";
        let good = canonical_sig(&secret, body);
        let mut diff_first = good.clone().into_bytes();
        diff_first[0] = if diff_first[0] == b'a' { b'b' } else { b'a' };
        let mut diff_last = good.clone().into_bytes();
        let last = diff_last.len() - 1;
        diff_last[last] = if diff_last[last] == b'a' { b'b' } else { b'a' };
        assert!(
            verify_signature(&secret, body, std::str::from_utf8(&diff_first).unwrap()).is_err()
        );
        assert!(verify_signature(&secret, body, std::str::from_utf8(&diff_last).unwrap()).is_err());
        // Sanity: the unmodified MAC must still verify.
        assert!(verify_signature(&secret, body, &good).is_ok());
    }

    #[test]
    fn verify_signature_with_empty_body() {
        // An empty body is a valid input — MAC is still defined.
        let secret = "1".repeat(64);
        let sig = canonical_sig(&secret, b"");
        assert!(verify_signature(&secret, b"", &sig).is_ok());
    }
}
