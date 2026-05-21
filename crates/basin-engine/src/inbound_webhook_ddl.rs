//! `CREATE INBOUND WEBHOOK <name> [WITH SECRET '<hex>'] EXECUTE <body>` SQL
//! surface (5.11.N, ADR 0019, Phase 6.SEC.P0.3).
//!
//! sqlparser 0.52 has no `CREATE INBOUND WEBHOOK` AST node, so we recognise
//! the shape textually before sqlparser sees the statement (mirrors
//! `webhook_ddl.rs`, `reactor_ddl.rs`, `function_ddl.rs`).
//!
//! ## Syntax
//!
//! ```sql
//! CREATE INBOUND WEBHOOK <name>
//!   [WITH SECRET '<hex-token>']
//!   EXECUTE <single-SQL-statement>
//! ```
//!
//! `<name>` — a plain SQL identifier, unique per project.
//! `WITH SECRET '<hex>'` — optional. A hex-encoded HMAC secret (≥ 32 hex
//! chars, even length). When omitted, the engine generates a
//! 32-random-byte / 64-hex-char token via `getrandom` and surfaces it as a
//! single `secret` column in the result set — that is the only time the
//! plaintext secret leaves the server.
//! `EXECUTE <body>` — exactly one INSERT / UPDATE / DELETE / SELECT statement.
//! The special identifier `payload` inside `<body>` is a `jsonb` bind
//! parameter bound to the raw POST body at dispatch time.
//!
//! ## Authentication contract (Phase 6.SEC.P0.3)
//!
//! `POST /in/<project>/<name>` requires `X-Basin-Signature: <hex>` where
//! the hex value is HMAC-SHA256(body, secret). The dispatcher in
//! `basin-rest::routes::inbound` performs the verification in constant
//! time via `subtle::ConstantTimeEq` before executing the registered
//! SQL body. The legacy plaintext path is gated behind
//! `BASIN_NET_ALLOW_PLAINTEXT_WEBHOOKS=1` (debug-only env, ADR 0019
//! § "TLS downgrade").
//!
//! ## Drop syntax
//!
//! ```sql
//! DROP INBOUND WEBHOOK [IF EXISTS] <name>
//! ```

use basin_catalog::{Catalog as _, InboundWebhookDef};
use basin_common::{BasinError, ProjectId, Result};

/// Parsed `CREATE INBOUND WEBHOOK …` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateInboundWebhookIntent {
    pub name: String,
    pub body: String,
    /// Caller-supplied `WITH SECRET '<hex>'` token, when present. When
    /// `None`, [`exec_create_inbound_webhook`] generates a 32-byte random
    /// hex token.
    pub explicit_secret_hex: Option<String>,
}

/// Parsed `DROP INBOUND WEBHOOK …` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropInboundWebhookIntent {
    pub name: String,
    pub if_exists: bool,
}

/// Recognise `CREATE INBOUND WEBHOOK <name> [WITH SECRET '<hex>'] EXECUTE
/// <body>`. Returns the parsed intent on a match, `None` when the
/// statement is something else, or `Err` on a malformed match.
pub fn match_create_inbound_webhook(sql: &str) -> Result<Option<CreateInboundWebhookIntent>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !starts_with_kw(trimmed, "CREATE") {
        return Ok(None);
    }
    let after_create = skip_word(trimmed).trim_start();
    if !starts_with_kw(after_create, "INBOUND") {
        return Ok(None);
    }
    let after_inbound = skip_word(after_create).trim_start();
    if !starts_with_kw(after_inbound, "WEBHOOK") {
        return Ok(None);
    }
    let after_webhook = skip_word(after_inbound).trim_start();
    let (name, after_name) = read_simple_identifier(after_webhook)?;
    let mut cursor = after_name.trim_start();

    // Optional `WITH SECRET '<hex>'` clause (Phase 6.SEC.P0.3).
    let mut explicit_secret_hex: Option<String> = None;
    if starts_with_kw(cursor, "WITH") {
        let after_with = skip_word(cursor).trim_start();
        if !starts_with_kw(after_with, "SECRET") {
            return Err(BasinError::InvalidSchema(
                "CREATE INBOUND WEBHOOK: expected SECRET '<hex>' after WITH".into(),
            ));
        }
        let after_secret = skip_word(after_with).trim_start();
        let (secret, after_lit) = read_single_quoted_literal(after_secret)?;
        if secret.len() < 32 || secret.len() % 2 != 0 {
            return Err(BasinError::InvalidSchema(format!(
                "CREATE INBOUND WEBHOOK: WITH SECRET must be ≥ 32 even-length hex chars, got {} chars",
                secret.len()
            )));
        }
        if !secret.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(BasinError::InvalidSchema(
                "CREATE INBOUND WEBHOOK: WITH SECRET must be hex-encoded (0-9, a-f, A-F)".into(),
            ));
        }
        explicit_secret_hex = Some(secret.to_ascii_lowercase());
        cursor = after_lit.trim_start();
    }

    if !starts_with_kw(cursor, "EXECUTE") {
        return Err(BasinError::InvalidSchema(
            "CREATE INBOUND WEBHOOK: expected EXECUTE <body>".into(),
        ));
    }
    let body = skip_word(cursor).trim_start().to_string();
    if body.is_empty() {
        return Err(BasinError::InvalidSchema(
            "CREATE INBOUND WEBHOOK: EXECUTE body must not be empty".into(),
        ));
    }

    Ok(Some(CreateInboundWebhookIntent {
        name,
        body,
        explicit_secret_hex,
    }))
}

/// Recognise `DROP INBOUND WEBHOOK [IF EXISTS] <name>`.
pub fn match_drop_inbound_webhook(sql: &str) -> Result<Option<DropInboundWebhookIntent>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !starts_with_kw(trimmed, "DROP") {
        return Ok(None);
    }
    let after_drop = skip_word(trimmed).trim_start();
    if !starts_with_kw(after_drop, "INBOUND") {
        return Ok(None);
    }
    let after_inbound = skip_word(after_drop).trim_start();
    if !starts_with_kw(after_inbound, "WEBHOOK") {
        return Ok(None);
    }
    let after_webhook = skip_word(after_inbound).trim_start();

    let (if_exists, after_if_exists) = if starts_with_kw(after_webhook, "IF") {
        let after_if = skip_word(after_webhook).trim_start();
        if !starts_with_kw(after_if, "EXISTS") {
            return Err(BasinError::InvalidSchema(
                "DROP INBOUND WEBHOOK: expected EXISTS after IF".into(),
            ));
        }
        (true, skip_word(after_if).trim_start())
    } else {
        (false, after_webhook)
    };

    let (name, rest) = read_simple_identifier(after_if_exists)?;
    let rest = rest.trim();
    if !rest.is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "DROP INBOUND WEBHOOK: unexpected trailing input {rest:?}"
        )));
    }

    Ok(Some(DropInboundWebhookIntent { name, if_exists }))
}

/// Register a `CreateInboundWebhookIntent` in the catalog. Returns the
/// hex-encoded HMAC secret (32 bytes, 64 hex chars) — either the
/// caller-supplied `WITH SECRET '<hex>'` value or a freshly-generated one
/// — so the executor can surface it to the creator exactly once via the
/// result set's `secret` column.
pub async fn exec_create_inbound_webhook(
    intent: CreateInboundWebhookIntent,
    project: &ProjectId,
    catalog: &dyn basin_catalog::Catalog,
) -> Result<String> {
    let secret_hex = intent
        .explicit_secret_hex
        .unwrap_or_else(generate_secret_hex);
    catalog
        .register_inbound_webhook(InboundWebhookDef {
            project: *project,
            name: intent.name,
            body: intent.body,
            secret_hex: secret_hex.clone(),
        })
        .await?;
    Ok(secret_hex)
}

/// Generate a 32-byte cryptographically-random secret, hex-encoded
/// (64 lowercase chars). Reuses the same `getrandom`-via-uuid pattern as
/// `udf::getrandom_fill` to avoid adding a fresh workspace dep.
fn generate_secret_hex() -> String {
    let mut raw = [0u8; 32];
    let mut idx = 0;
    while idx < raw.len() {
        let bytes = *uuid::Uuid::new_v4().as_bytes();
        let take = bytes.len().min(raw.len() - idx);
        raw[idx..idx + take].copy_from_slice(&bytes[..take]);
        idx += take;
    }
    hex::encode(raw)
}

/// Drop an inbound webhook from the catalog.
pub async fn exec_drop_inbound_webhook(
    intent: DropInboundWebhookIntent,
    project: &ProjectId,
    catalog: &dyn basin_catalog::Catalog,
) -> Result<()> {
    match catalog.drop_inbound_webhook(project, &intent.name).await {
        Ok(()) => Ok(()),
        Err(BasinError::NotFound(_)) if intent.if_exists => Ok(()),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// Private helpers (mirrors webhook_ddl.rs / reactor_ddl.rs helpers)
// ---------------------------------------------------------------------------

fn starts_with_kw(s: &str, kw: &str) -> bool {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let kw = kw.as_bytes();
    if bytes.len() < kw.len() {
        return false;
    }
    for (a, b) in bytes.iter().zip(kw.iter()) {
        if !a.eq_ignore_ascii_case(b) {
            return false;
        }
    }
    if bytes.len() == kw.len() {
        return true;
    }
    let next = bytes[kw.len()];
    !(next.is_ascii_alphanumeric() || next == b'_')
}

fn skip_word(s: &str) -> &str {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    &s[i..]
}

/// Read a single-quoted SQL string literal (`'…'`). Supports doubled
/// single-quote escapes (`''` → `'`). Returns the unquoted body + the
/// remainder. Used for the `WITH SECRET '<hex>'` clause.
fn read_single_quoted_literal(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes[0] != b'\'' {
        return Err(BasinError::InvalidSchema(
            "expected a single-quoted string literal".into(),
        ));
    }
    let mut out = String::new();
    let mut i = 1;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                out.push('\'');
                i += 2;
                continue;
            }
            return Ok((out, &s[i + 1..]));
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    Err(BasinError::InvalidSchema(
        "unterminated single-quoted string literal".into(),
    ))
}

fn read_simple_identifier(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return Err(BasinError::InvalidIdent(
            "expected an identifier, got end of statement".into(),
        ));
    }
    if !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_') {
        return Err(BasinError::InvalidIdent(format!(
            "expected an identifier at {:?}",
            s.chars().take(8).collect::<String>()
        )));
    }
    let mut i = 1;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    Ok((s[..i].to_string(), &s[i..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_create_minimal() {
        let intent = match_create_inbound_webhook(
            "CREATE INBOUND WEBHOOK stripe_events EXECUTE INSERT INTO events (payload) VALUES ($1)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(intent.name, "stripe_events");
        assert!(intent.body.starts_with("INSERT"));
        assert!(intent.explicit_secret_hex.is_none());
    }

    #[test]
    fn match_create_with_semicolon() {
        let intent = match_create_inbound_webhook("CREATE INBOUND WEBHOOK wh EXECUTE SELECT 1;")
            .unwrap()
            .unwrap();
        assert_eq!(intent.name, "wh");
    }

    #[test]
    fn match_create_with_explicit_secret() {
        let secret = "deadbeefcafef00d".repeat(4); // 64 hex chars
        let sql = format!(
            "CREATE INBOUND WEBHOOK stripe_events WITH SECRET '{secret}' \
             EXECUTE INSERT INTO events (payload) VALUES ($1)"
        );
        let intent = match_create_inbound_webhook(&sql).unwrap().unwrap();
        assert_eq!(intent.name, "stripe_events");
        assert_eq!(intent.explicit_secret_hex.as_deref(), Some(secret.as_str()));
        assert!(intent.body.starts_with("INSERT"));
    }

    #[test]
    fn match_create_with_secret_rejects_short_token() {
        let err = match_create_inbound_webhook(
            "CREATE INBOUND WEBHOOK wh WITH SECRET 'deadbeef' EXECUTE SELECT 1",
        )
        .unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_create_with_secret_rejects_non_hex() {
        // 64 chars, but contains non-hex.
        let bad = "z".repeat(64);
        let sql = format!("CREATE INBOUND WEBHOOK wh WITH SECRET '{bad}' EXECUTE SELECT 1");
        let err = match_create_inbound_webhook(&sql).unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_create_with_missing_execute_after_secret() {
        let secret = "a".repeat(64);
        let sql = format!("CREATE INBOUND WEBHOOK wh WITH SECRET '{secret}'");
        let err = match_create_inbound_webhook(&sql).unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn generate_secret_hex_is_64_chars_and_unique() {
        let a = generate_secret_hex();
        let b = generate_secret_hex();
        assert_eq!(a.len(), 64, "expected 64 hex chars, got {}", a.len());
        assert!(a.bytes().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b, "two consecutive secrets must differ");
    }

    #[test]
    fn match_create_missing_execute() {
        let err =
            match_create_inbound_webhook("CREATE INBOUND WEBHOOK wh ON POST '/x'").unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_create_returns_none_for_other() {
        assert!(match_create_inbound_webhook("SELECT 1").unwrap().is_none());
        assert!(match_create_inbound_webhook("CREATE TABLE t (id INT)")
            .unwrap()
            .is_none());
    }

    #[test]
    fn match_drop_simple() {
        let intent = match_drop_inbound_webhook("DROP INBOUND WEBHOOK my_hook")
            .unwrap()
            .unwrap();
        assert_eq!(intent.name, "my_hook");
        assert!(!intent.if_exists);
    }

    #[test]
    fn match_drop_if_exists() {
        let intent = match_drop_inbound_webhook("DROP INBOUND WEBHOOK IF EXISTS my_hook;")
            .unwrap()
            .unwrap();
        assert_eq!(intent.name, "my_hook");
        assert!(intent.if_exists);
    }

    #[test]
    fn match_drop_returns_none_for_other() {
        assert!(match_drop_inbound_webhook("SELECT 1").unwrap().is_none());
    }
}
