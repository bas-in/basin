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
        // Long-lived per-tenant API keys. `key_hash` is the sha256 of the
        // bearer secret (used as a fast-path lookup column); `key_bcrypt` is
        // the bcrypt of the same secret and is what we actually verify
        // against, defence-in-depth if the hash column ever leaks alone.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}.api_keys (
                id            BIGSERIAL PRIMARY KEY,
                tenant_id     TEXT NOT NULL,
                user_id       UUID NOT NULL REFERENCES {schema}.users(user_id) ON DELETE CASCADE,
                name          TEXT NOT NULL,
                key_hash      TEXT NOT NULL,
                key_bcrypt    TEXT NOT NULL,
                created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_used_at  TIMESTAMPTZ,
                revoked_at    TIMESTAMPTZ,
                UNIQUE (tenant_id, user_id, name)
            )"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS api_keys_key_hash
             ON {schema}.api_keys (key_hash)"
        ),
        // Per-user `current_setting()` overrides. Hard-coded allowlist of
        // keys lives in the service layer; the DB doesn't constrain the
        // value further beyond TEXT.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}.user_session_settings (
                tenant_id   TEXT NOT NULL,
                user_id     UUID NOT NULL REFERENCES {schema}.users(user_id) ON DELETE CASCADE,
                key         TEXT NOT NULL,
                value       TEXT NOT NULL,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (tenant_id, user_id, key)
            )"
        ),
        // Tenant-agnostic email-link login. Distinct from the existing
        // `email_tokens` flow (which is per-tenant + bound to a known user
        // at issue time): the consumer POSTs only an email, and we resolve
        // the user at consume time. `token_hash` is bcrypt of the raw token.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}.auth_magic_links (
                id           BIGSERIAL PRIMARY KEY,
                email        TEXT NOT NULL,
                token_hash   TEXT NOT NULL,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                expires_at   TIMESTAMPTZ NOT NULL,
                consumed_at  TIMESTAMPTZ
            )"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS auth_magic_links_token_hash
             ON {schema}.auth_magic_links (token_hash)"
        ),
        // Refresh-JWT revocation list. Keyed on the JWT `jti`. Reuse-detection
        // sentinel rows use the well-known `token_hash` prefix `BLANKET:<uuid>`
        // — see `flows::refresh` for the protocol.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}.auth_revoked_refresh_tokens (
                token_hash   TEXT PRIMARY KEY,
                user_id      UUID NOT NULL REFERENCES {schema}.users(user_id) ON DELETE CASCADE,
                revoked_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                expires_at   TIMESTAMPTZ NOT NULL
            )"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS auth_revoked_refresh_tokens_user
             ON {schema}.auth_revoked_refresh_tokens (user_id)"
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
