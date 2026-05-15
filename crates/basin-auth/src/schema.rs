//! Schema for the auth tables.
//!
//! Folded 2026-05-12 per ADR 0013: all intermediate ALTER/ADD/DROP steps
//! and provisional v0.1 capability hedges collapsed into the authoritative
//! CREATE TABLE definitions below.
//!
//! Mirrors the `PostgresCatalog` migration pattern: idempotent
//! `CREATE TABLE IF NOT EXISTS` statements. Tests use a unique schema-prefix
//! per run with a `Drop`-guard cleanup.
//!
//! # Dialect note: basin engine vs upstream Postgres
//!
//! This module historically targeted an external Postgres (Neon). The
//! parallel basin-server boot-order change (v0.1.5) points it at basin
//! engine's own pgwire on loopback. Basin engine's DDL surface differs from
//! upstream Postgres in the following ways that affect this file:
//!
//! - No `CREATE SCHEMA` — the catalog is single-namespace per project. The
//!   schema name is collapsed into a `<schema>_<table>` table-name prefix
//!   so different deployments (prod / test / unit) can still
//!   namespace-isolate. `auth_loopback_smoke.rs` is the safety net.
//! - No schema-qualified table references — `single_part_name` in
//!   `crates/basin-engine/src/executor.rs` rejects multi-part `ObjectName`s.
//!   Every DDL identifier here is a flat single-part name; callers emit
//!   `{sch}_<table>` instead of `{sch}.<table>`.
//! - `CREATE INDEX [IF NOT EXISTS]`, table-level `UNIQUE`, and column-level
//!   `UNIQUE` — all supported as of Phase 5.7 B1. See
//!   `crates/basin-engine/tests/create_index.rs` and
//!   `crates/basin-engine/tests/unique_constraint.rs`.
//! - `BIGSERIAL` / `SERIAL` / `SMALLSERIAL` — supported (see
//!   `crates/basin-engine/tests/serial_type.rs`).
//! - `UUID`, `TEXT`, `TIMESTAMPTZ`, `BIGINT`, `JSONB`, `BYTEA`, `BOOLEAN`,
//!   `NUMERIC` — supported (see `crates/basin-engine/src/types.rs` +
//!   `tests/numeric_type.rs`).
//! - `PRIMARY KEY (col …)`, `REFERENCES <t>(<c>) ON DELETE CASCADE`,
//!   `CHECK (<expr>)`, `DEFAULT now()`, `DEFAULT '<literal>'`,
//!   `DEFAULT nextval('<seq>')` (implicit via BIGSERIAL) — supported (see
//!   `crates/basin-engine/src/ddl.rs` + `tests/constraints.rs`).

use basin_common::{BasinError, Result};
use tokio_postgres::Client;

/// Validate an identifier we will interpolate into DDL. The interpolated
/// string is used as the *prefix* of every table name (e.g. `basin_auth`
/// becomes the prefix in `basin_auth_users`), so the same character rules a
/// Postgres schema name has are what we want.
///
/// Mirrors `basin-catalog::postgres::validate_schema_ident` but kept local
/// so this crate has no dependency on a private function from another
/// crate.
pub(crate) fn validate_schema_ident(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(BasinError::catalog("auth schema name is empty"));
    }
    // Leave headroom for the longest suffix we append below
    // (`_auth_revoked_refresh_tokens` = 28 chars) so the resulting flat
    // table name still fits Postgres's 63-char identifier limit. 63 - 28 =
    // 35; round down to 32 for safety.
    if s.len() > 32 {
        return Err(BasinError::catalog(
            "auth schema name longer than 32 chars (table-prefix budget)",
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

/// Run every `CREATE TABLE IF NOT EXISTS` statement. Safe to call
/// repeatedly; the prefix is validated before interpolation by
/// [`validate_schema_ident`].
///
/// The `schema` argument is *not* emitted as a Postgres schema (basin
/// engine doesn't support `CREATE SCHEMA`); instead it becomes the prefix
/// of every table name. See the module docs for the rationale.
pub async fn run_migrations(client: &Client, schema: &str) -> Result<()> {
    validate_schema_ident(schema)?;

    let stmts = [
        // basin_auth_users: composite UNIQUE on (project_id, email) is the
        // login-identity invariant — one row per (project, email) pair so
        // `WHERE project_id = $1 AND email = $2` is a single hit.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_users (
                user_id           UUID PRIMARY KEY,
                project_id         TEXT NOT NULL,
                email             TEXT NOT NULL,
                password_hash     TEXT NOT NULL,
                email_verified_at TIMESTAMPTZ,
                created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (project_id, email)
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_refresh_tokens (
                token_hash   TEXT PRIMARY KEY,
                user_id      UUID NOT NULL REFERENCES {schema}_users(user_id) ON DELETE CASCADE,
                project_id    TEXT NOT NULL,
                expires_at   TIMESTAMPTZ NOT NULL,
                revoked_at   TIMESTAMPTZ,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_email_tokens (
                token_hash   TEXT PRIMARY KEY,
                user_id      UUID NOT NULL REFERENCES {schema}_users(user_id) ON DELETE CASCADE,
                project_id    TEXT NOT NULL,
                purpose      TEXT NOT NULL,
                expires_at   TIMESTAMPTZ NOT NULL,
                consumed_at  TIMESTAMPTZ,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            )"
        ),
        // Long-lived per-project API keys. `key_hash` is the sha256 of the
        // bearer secret (used as a fast-path lookup column); `key_bcrypt`
        // is the bcrypt of the same secret and is what we actually verify
        // against, defence-in-depth if the hash column ever leaks alone.
        //
        // Composite UNIQUE (project_id, user_id, name) keeps key labels
        // disambiguated per-(project, user). The `id` surrogate stays
        // because `api_keys.rs` returns it as a stable handle.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_api_keys (
                id            BIGSERIAL PRIMARY KEY,
                project_id     TEXT NOT NULL,
                user_id       UUID NOT NULL REFERENCES {schema}_users(user_id) ON DELETE CASCADE,
                name          TEXT NOT NULL,
                key_hash      TEXT NOT NULL,
                key_bcrypt    TEXT NOT NULL,
                created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_used_at  TIMESTAMPTZ,
                revoked_at    TIMESTAMPTZ,
                UNIQUE (project_id, user_id, name)
            )"
        ),
        // Per-user `current_setting()` overrides. Hard-coded allowlist of
        // keys lives in the service layer; the DB doesn't constrain the
        // value further beyond TEXT. The 3-col composite PK gives both the
        // primary-key uniqueness and the lookup path.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_user_session_settings (
                project_id   TEXT NOT NULL,
                user_id     UUID NOT NULL REFERENCES {schema}_users(user_id) ON DELETE CASCADE,
                key         TEXT NOT NULL,
                value       TEXT NOT NULL,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (project_id, user_id, key)
            )"
        ),
        // Project-agnostic email-link login. Distinct from the existing
        // `email_tokens` flow (which is per-project + bound to a known user
        // at issue time): the consumer POSTs only an email, and we resolve
        // the user at consume time. `token_hash` is bcrypt of the raw token.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_auth_magic_links (
                id           BIGSERIAL PRIMARY KEY,
                email        TEXT NOT NULL,
                token_hash   TEXT NOT NULL,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                expires_at   TIMESTAMPTZ NOT NULL,
                consumed_at  TIMESTAMPTZ
            )"
        ),
        // Refresh-JWT revocation list. Keyed on the JWT `jti`. Reuse-
        // detection sentinel rows use the well-known `token_hash` prefix
        // `BLANKET:<uuid>` — see `flows::refresh` for the protocol.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_auth_revoked_refresh_tokens (
                token_hash   TEXT PRIMARY KEY,
                user_id      UUID NOT NULL REFERENCES {schema}_users(user_id) ON DELETE CASCADE,
                revoked_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                expires_at   TIMESTAMPTZ NOT NULL
            )"
        ),
        // Per-project pgwire credentials — one row maps a public
        // `pgwire_user` (e.g. `project_a1b2c3d4`) to the project id, a
        // bcrypt'd password, and a `dbname`. `validate_pgwire_credentials`
        // bcrypt-verifies; `rotate_pgwire_password` rolls the row.
        //
        // Column-level UNIQUE on `pgwire_user` enforces the wire-login
        // namespace invariant: one identity per public username.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_auth_project_credentials (
                id            BIGSERIAL PRIMARY KEY,
                project_id     TEXT NOT NULL,
                pgwire_user   TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                dbname        TEXT NOT NULL DEFAULT 'basin',
                created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                rotated_at    TIMESTAMPTZ
            )"
        ),
        // Secondary indexes — one per hot lookup path:
        //   - api_keys (key_hash): bearer-token verification
        //   - auth_magic_links (token_hash): consume-flow lookup
        //   - auth_revoked_refresh_tokens (user_id): reuse-detection sweep
        //   - auth_project_credentials (pgwire_user): pgwire startup auth
        //
        // Note: users (project_id, email) is covered by the UNIQUE constraint
        // on that table, which implies a unique index — no separate entry here.
        format!(
            "CREATE INDEX IF NOT EXISTS {schema}_api_keys_key_hash \
             ON {schema}_api_keys (key_hash)"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {schema}_auth_magic_links_token_hash \
             ON {schema}_auth_magic_links (token_hash)"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {schema}_auth_revoked_refresh_tokens_user_id \
             ON {schema}_auth_revoked_refresh_tokens (user_id)"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {schema}_auth_project_credentials_pgwire_user \
             ON {schema}_auth_project_credentials (pgwire_user)"
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

    #[test]
    fn rejects_overlong_prefix() {
        // Anything longer than 32 chars busts the table-name budget
        // (longest suffix = `_auth_revoked_refresh_tokens` = 28 chars,
        // and we keep 3 chars of headroom under the 63-char PG identifier
        // limit).
        let too_long = "a".repeat(33);
        assert!(validate_schema_ident(&too_long).is_err());
        let just_fits = "a".repeat(32);
        validate_schema_ident(&just_fits).unwrap();
    }
}
