//! Schema for the auth tables.
//!
//! Mirrors the `PostgresCatalog` migration pattern: idempotent
//! `CREATE TABLE IF NOT EXISTS` statements. Tests use a unique schema-prefix
//! per run with a `Drop`-guard cleanup.
//!
//! # Dialect note: basin engine vs upstream Postgres
//!
//! This module historically targeted an external Postgres (Neon). The
//! parallel basin-server boot-order change points it at basin engine's own
//! pgwire on loopback. Basin engine claims Postgres-compatibility but its
//! DDL surface (`basin-engine::executor`) is a strict subset:
//!
//! - No `CREATE SCHEMA` — there is no `Statement::CreateSchema` arm in the
//!   executor; the catalog is single-namespace per tenant. We collapse the
//!   notional schema into a `<schema>_<table>` table-name prefix instead so
//!   different deployments (prod / test / unit) can still namespace-isolate.
//! - No schema-qualified table references — `single_part_name` in
//!   `crates/basin-engine/src/executor.rs` rejects multi-part `ObjectName`s.
//!   Every DDL identifier here is therefore a flat single-part name; callers
//!   (`flows/*.rs`, `api_keys.rs`, `tokens.rs`, …) still emit
//!   `{sch}.<table>` SQL and need a parallel rewrite to `{sch}_<table>` to
//!   actually run. See // TODO markers below.
//! - No `CREATE INDEX` — secondary indexes are out of v0.1. We rely on
//!   PK-as-index for the columns we previously indexed. See // TODO markers.
//! - No table-level `UNIQUE` / non-PK column-level `UNIQUE` — only single-
//!   column `PRIMARY KEY` and composite `PRIMARY KEY (a, b)` give
//!   uniqueness. Composite uniqueness without a PK is folded into the PK
//!   where possible; standalone secondary uniqueness is left to
//!   application-level upsert checks. See // TODO markers.
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

    // TODO: revisit when basin supports `CREATE SCHEMA` — restore the
    // explicit schema-create at the top so the catalog actually groups
    // these tables under a namespace instead of leaning on a name prefix.

    let stmts = [
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_users (
                user_id           UUID PRIMARY KEY,
                tenant_id         TEXT NOT NULL,
                email             TEXT NOT NULL,
                password_hash     TEXT NOT NULL,
                email_verified_at TIMESTAMPTZ,
                created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
            )"
        ),
        // TODO: revisit when basin supports composite UNIQUE / CREATE INDEX
        // — the original schema had `UNIQUE (tenant_id, email)` plus an
        // index on `(tenant_id, email)`. v0.1 engine rejects both, so we
        // currently lean on application-side uniqueness (the signup flow
        // already does a SELECT-then-INSERT inside one transaction; the
        // ON CONFLICT shape it uses today is also engine-ignored — that's
        // a separate caller-layer fix). The lookup query is still
        // `WHERE tenant_id = $1 AND email = $2`, which scans the table;
        // acceptable until the auth table grows beyond ~10k rows.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_refresh_tokens (
                token_hash   TEXT PRIMARY KEY,
                user_id      UUID NOT NULL REFERENCES {schema}_users(user_id) ON DELETE CASCADE,
                tenant_id    TEXT NOT NULL,
                expires_at   TIMESTAMPTZ NOT NULL,
                revoked_at   TIMESTAMPTZ,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_email_tokens (
                token_hash   TEXT PRIMARY KEY,
                user_id      UUID NOT NULL REFERENCES {schema}_users(user_id) ON DELETE CASCADE,
                tenant_id    TEXT NOT NULL,
                purpose      TEXT NOT NULL,
                expires_at   TIMESTAMPTZ NOT NULL,
                consumed_at  TIMESTAMPTZ,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            )"
        ),
        // Long-lived per-tenant API keys. `key_hash` is the sha256 of the
        // bearer secret (used as a fast-path lookup column); `key_bcrypt`
        // is the bcrypt of the same secret and is what we actually verify
        // against, defence-in-depth if the hash column ever leaks alone.
        //
        // TODO: revisit when basin supports composite UNIQUE / CREATE INDEX
        // — the original schema had `UNIQUE (tenant_id, user_id, name)`
        // plus an index on `(key_hash)`. We could hoist the composite
        // uniqueness into the PK (replacing `BIGSERIAL id` with a 3-col
        // PK), but the `id` column is referenced by callers as a stable
        // surrogate (api_keys.rs returns it), so flipping that shape is a
        // caller-layer change. Until then, uniqueness is application-side.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_api_keys (
                id            BIGSERIAL PRIMARY KEY,
                tenant_id     TEXT NOT NULL,
                user_id       UUID NOT NULL REFERENCES {schema}_users(user_id) ON DELETE CASCADE,
                name          TEXT NOT NULL,
                key_hash      TEXT NOT NULL,
                key_bcrypt    TEXT NOT NULL,
                created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_used_at  TIMESTAMPTZ,
                revoked_at    TIMESTAMPTZ
            )"
        ),
        // Per-user `current_setting()` overrides. Hard-coded allowlist of
        // keys lives in the service layer; the DB doesn't constrain the
        // value further beyond TEXT. The 3-col composite PK gives both the
        // primary-key uniqueness and the lookup path the original schema
        // wanted from a composite UNIQUE; this one *did* compose cleanly
        // into a PK so we kept it.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_user_session_settings (
                tenant_id   TEXT NOT NULL,
                user_id     UUID NOT NULL REFERENCES {schema}_users(user_id) ON DELETE CASCADE,
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
        //
        // TODO: revisit when basin supports `CREATE INDEX` — the original
        // schema had an index on `(token_hash)` to fast-path the consume-
        // flow lookup. v0.1 scans the table; auth_magic_links is short-
        // lived (expires_at is ~15min out, consumed rows are pruned) so
        // the table stays small enough that a scan is acceptable.
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
        //
        // TODO: revisit when basin supports `CREATE INDEX` — the original
        // schema had an index on `(user_id)` so the reuse-detection
        // BLANKET sweep could fast-path delete the user's outstanding
        // tokens. v0.1 scans the table; the revocation list churns
        // (rows past expires_at are GC'd) so it stays small.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_auth_revoked_refresh_tokens (
                token_hash   TEXT PRIMARY KEY,
                user_id      UUID NOT NULL REFERENCES {schema}_users(user_id) ON DELETE CASCADE,
                revoked_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                expires_at   TIMESTAMPTZ NOT NULL
            )"
        ),
        // Per-tenant pgwire credentials — one row maps a public
        // `pgwire_user` (e.g. `tenant_a1b2c3d4`) to the tenant id, a
        // bcrypt'd password, and a `dbname`. `validate_pgwire_credentials`
        // bcrypt-verifies; `rotate_pgwire_password` rolls the row.
        //
        // TODO: revisit when basin supports column-level UNIQUE on a non-
        // PK column / `CREATE INDEX` — the original schema had
        // `pgwire_user TEXT NOT NULL UNIQUE` + an index on
        // `(pgwire_user)`. We currently rely on the `tenant_credentials.rs`
        // upsert path to enforce uniqueness; the lookup is a `WHERE
        // pgwire_user = $1` scan. If we ever want a stable wire-level
        // login namespace, we could promote `pgwire_user` to the PK and
        // drop the `id` surrogate — but callers index by `id`, so that's
        // a caller-layer change.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_auth_tenant_credentials (
                id            BIGSERIAL PRIMARY KEY,
                tenant_id     TEXT NOT NULL,
                pgwire_user   TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                dbname        TEXT NOT NULL DEFAULT 'basin',
                created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                rotated_at    TIMESTAMPTZ
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
