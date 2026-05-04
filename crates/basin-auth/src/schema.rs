//! Postgres schema for the auth tables.
//!
//! Mirrors the `PostgresCatalog` migration pattern: idempotent
//! `CREATE TABLE IF NOT EXISTS` against a configurable schema. Tests use a
//! unique schema per run with a `Drop`-guard cleanup.

use basin_common::{BasinError, Result};
use tokio_postgres::Client;

/// Validate a Postgres identifier we will interpolate into DDL. Mirrors
/// `basin-catalog::postgres::validate_schema_ident` but kept local so this
/// crate has no dependency on a private function from another crate.
pub(crate) fn validate_schema_ident(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(BasinError::catalog("auth schema name is empty"));
    }
    if s.len() > 63 {
        return Err(BasinError::catalog(
            "auth schema name longer than 63 chars",
        ));
    }
    let mut chars = s.chars();
    let first = chars.next().unwrap();
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(BasinError::catalog(format!(
            "auth schema must start with [A-Za-z_]: {s:?}"
        )));
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '_') {
            return Err(BasinError::catalog(format!(
                "auth schema has invalid char {c:?}: {s:?}"
            )));
        }
    }
    Ok(())
}

/// Run every `CREATE SCHEMA / CREATE TABLE / CREATE INDEX IF NOT EXISTS`
/// statement. Safe to call repeatedly; schema name is validated before
/// interpolation by [`validate_schema_ident`].
pub async fn run_migrations(client: &Client, schema: &str) -> Result<()> {
    validate_schema_ident(schema)?;

    let stmts = [
        format!("CREATE SCHEMA IF NOT EXISTS {schema}"),
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}.users (
                user_id           UUID PRIMARY KEY,
                tenant_id         TEXT NOT NULL,
                email             TEXT NOT NULL,
                password_hash     TEXT NOT NULL,
                email_verified_at TIMESTAMPTZ,
                created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (tenant_id, email)
            )"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS users_tenant_email
             ON {schema}.users (tenant_id, email)"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}.refresh_tokens (
                token_hash   TEXT PRIMARY KEY,
                user_id      UUID NOT NULL REFERENCES {schema}.users(user_id) ON DELETE CASCADE,
                tenant_id    TEXT NOT NULL,
                expires_at   TIMESTAMPTZ NOT NULL,
                revoked_at   TIMESTAMPTZ,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}.email_tokens (
                token_hash   TEXT PRIMARY KEY,
                user_id      UUID NOT NULL REFERENCES {schema}.users(user_id) ON DELETE CASCADE,
                tenant_id    TEXT NOT NULL,
                purpose      TEXT NOT NULL,
                expires_at   TIMESTAMPTZ NOT NULL,
                consumed_at  TIMESTAMPTZ,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            )"
        ),
    ];

    for stmt in stmts {
        client
            .batch_execute(&stmt)
            .await
            .map_err(|e| BasinError::catalog(format!("auth migrate: {e}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_idents() {
        validate_schema_ident("basin_auth").unwrap();
        validate_schema_ident("_x").unwrap();
        assert!(validate_schema_ident("").is_err());
        assert!(validate_schema_ident("1bad").is_err());
        assert!(validate_schema_ident("with-dash").is_err());
        assert!(validate_schema_ident("with space").is_err());
    }
}
