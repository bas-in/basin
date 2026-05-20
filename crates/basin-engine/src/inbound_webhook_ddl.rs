//! `CREATE INBOUND WEBHOOK <name> EXECUTE <body>` SQL surface (5.11.N, ADR 0019).
//!
//! sqlparser 0.52 has no `CREATE INBOUND WEBHOOK` AST node, so we recognise
//! the shape textually before sqlparser sees the statement (mirrors
//! `webhook_ddl.rs`, `reactor_ddl.rs`, `function_ddl.rs`).
//!
//! ## Syntax (minimal v0.1)
//!
//! ```sql
//! CREATE INBOUND WEBHOOK <name>
//!   EXECUTE <single-SQL-statement>
//! ```
//!
//! `<name>` — a plain SQL identifier, unique per project.
//! `EXECUTE <body>` — exactly one INSERT / UPDATE / DELETE / SELECT statement.
//! The special identifier `payload` inside `<body>` is a `jsonb` bind
//! parameter bound to the raw POST body at dispatch time.
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
}

/// Parsed `DROP INBOUND WEBHOOK …` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropInboundWebhookIntent {
    pub name: String,
    pub if_exists: bool,
}

/// Recognise `CREATE INBOUND WEBHOOK <name> EXECUTE <body>`. Returns the
/// parsed intent on a match, `None` when the statement is something else,
/// or `Err` on a malformed match.
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
    let after_name = after_name.trim_start();

    if !starts_with_kw(after_name, "EXECUTE") {
        return Err(BasinError::InvalidSchema(
            "CREATE INBOUND WEBHOOK: expected EXECUTE <body>".into(),
        ));
    }
    let body = skip_word(after_name).trim_start().to_string();
    if body.is_empty() {
        return Err(BasinError::InvalidSchema(
            "CREATE INBOUND WEBHOOK: EXECUTE body must not be empty".into(),
        ));
    }

    Ok(Some(CreateInboundWebhookIntent { name, body }))
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

/// Register a `CreateInboundWebhookIntent` in the catalog.
pub async fn exec_create_inbound_webhook(
    intent: CreateInboundWebhookIntent,
    project: &ProjectId,
    catalog: &dyn basin_catalog::Catalog,
) -> Result<()> {
    catalog
        .register_inbound_webhook(InboundWebhookDef {
            project: *project,
            name: intent.name,
            body: intent.body,
        })
        .await
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
    }

    #[test]
    fn match_create_with_semicolon() {
        let intent =
            match_create_inbound_webhook("CREATE INBOUND WEBHOOK wh EXECUTE SELECT 1;")
                .unwrap()
                .unwrap();
        assert_eq!(intent.name, "wh");
    }

    #[test]
    fn match_create_missing_execute() {
        let err = match_create_inbound_webhook("CREATE INBOUND WEBHOOK wh ON POST '/x'")
            .unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_create_returns_none_for_other() {
        assert!(match_create_inbound_webhook("SELECT 1").unwrap().is_none());
        assert!(
            match_create_inbound_webhook("CREATE TABLE t (id INT)")
                .unwrap()
                .is_none()
        );
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
