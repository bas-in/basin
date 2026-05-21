//! Per-project inbound-webhook catalog.
//!
//! `CREATE INBOUND WEBHOOK <name> EXECUTE <body>` registers an endpoint;
//! incoming POSTs to `/in/<project>/<name>` are validated and translated
//! into a row insert (or any single SQL DML statement) with the raw JSON
//! body bound as the `payload` jsonb parameter.
//!
//! The catalog is a single shared `HashMap<(ProjectId, name), InboundWebhookDef>`
//! so per-project cost stays `O(bytes)` — no per-project heavy resource.

use basin_common::ProjectId;
use serde::{Deserialize, Serialize};

/// Catalog row for a registered inbound webhook.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboundWebhookDef {
    pub project: ProjectId,
    /// Unqualified webhook name, unique per project. Validated as a SQL
    /// identifier.
    pub name: String,
    /// Raw SQL text of the `EXECUTE` statement. Exactly one DML statement
    /// (INSERT / UPDATE / DELETE / SELECT). The special identifier `payload`
    /// inside the statement refers to the incoming POST body as `jsonb`.
    pub body: String,
    /// Hex-encoded HMAC secret (32 raw bytes → 64 hex chars). Generated at
    /// `CREATE INBOUND WEBHOOK` time and returned to the creator exactly
    /// once. The dispatcher verifies the `X-Basin-Signature` header
    /// (hex-encoded HMAC-SHA256 of the request body under this secret) in
    /// constant time before executing the registered SQL body
    /// (Phase 6.SEC.P0.3, ADR 0019). Stored verbatim in the catalog row so
    /// the dispatcher can recompute the MAC.
    pub secret_hex: String,
}

/// Errors specific to inbound-webhook registration.
#[derive(Debug)]
pub enum InboundWebhookError {
    /// Name collides with an existing webhook for the same project.
    Duplicate,
    /// Body failed to parse as a single SQL DML statement.
    InvalidBody(String),
    /// Body parsed as multiple statements; v0.1 accepts exactly one.
    MultiStatementBody,
    /// Webhook named in a `drop_inbound_webhook` call doesn't exist.
    NotFound,
}

/// Parse + validate the body. Returns `Ok` if it is exactly one acceptable SQL
/// statement (INSERT / UPDATE / DELETE / SELECT). Used by the catalog at
/// registration time.
pub fn validate_body(body: &str) -> Result<(), InboundWebhookError> {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, body)
        .map_err(|e| InboundWebhookError::InvalidBody(format!("parse error: {e}")))?;
    if stmts.is_empty() {
        return Err(InboundWebhookError::InvalidBody("body is empty".into()));
    }
    if stmts.len() > 1 {
        return Err(InboundWebhookError::MultiStatementBody);
    }
    use sqlparser::ast::Statement;
    match &stmts[0] {
        Statement::Insert(_)
        | Statement::Update { .. }
        | Statement::Delete(_)
        | Statement::Query(_) => Ok(()),
        other => Err(InboundWebhookError::InvalidBody(format!(
            "inbound webhook body must be INSERT / UPDATE / DELETE / SELECT, got: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_body_accepts_insert() {
        assert!(validate_body("INSERT INTO events (payload) VALUES ($1)").is_ok());
    }

    #[test]
    fn validate_body_accepts_select() {
        assert!(validate_body("SELECT 1").is_ok());
    }

    #[test]
    fn validate_body_rejects_multi_statement() {
        assert!(matches!(
            validate_body("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)"),
            Err(InboundWebhookError::MultiStatementBody)
        ));
    }

    #[test]
    fn validate_body_rejects_garbage() {
        assert!(matches!(
            validate_body("not sql"),
            Err(InboundWebhookError::InvalidBody(_))
        ));
    }
}
