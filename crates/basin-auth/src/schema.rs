//! Schema for the auth tables.
//!
//! Folded 2026-05-12 per ADR 0013: all intermediate ALTER/ADD/DROP steps
//! and provisional v0.1 capability hedges collapsed into the authoritative
//! CREATE TABLE definitions below.
//!
//! # Phase 5.18.C — `auth` reserved schema migration
//!
//! The canonical namespace for auth tables is now `auth` (a `ReservedSchema`
//! in basin-catalog). Table names under the auth schema are the bare table
//! names without the `basin_auth_` prefix:
//!
//! | Canonical (`auth.<table>`)    | Legacy flat name (`{prefix}_<table>`)      |
//! |-------------------------------|---------------------------------------------|
//! | `auth.users`                  | `{schema}_users` (e.g. `basin_auth_users`)  |
//! | `auth.refresh_tokens`         | `{schema}_refresh_tokens`                   |
//! | `auth.email_tokens`           | `{schema}_email_tokens`                     |
//! | `auth.api_keys`               | `{schema}_api_keys`                         |
//! | `auth.user_session_settings`  | `{schema}_user_session_settings`            |
//! | `auth.magic_links`            | `{schema}_auth_magic_links`                 |
//! | `auth.revoked_refresh_tokens` | `{schema}_auth_revoked_refresh_tokens`      |
//! | `auth.project_credentials`    | `{schema}_auth_project_credentials`         |
//! | `auth.oauth_providers`        | `{schema}_oauth_providers`                  |
//! | `auth.identities`             | `{schema}_identities`                       |
//! | `auth.oauth_states`           | `{schema}_oauth_states`                     |
//!
//! **Back-compat**: [`legacy_suffix_for`] maps a canonical table name
//! (e.g. `"users"`) to the legacy flat-prefix form so callers can fall back
//! to the old physical name when the canonical `auth.<table>` entry is absent.
//! [`canonical_auth_table_name`] maps the reverse direction.
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

// ─────────────────────────────────────────────────────────────────────────────
// Phase 5.18.C — canonical reserved-schema constants + back-compat helpers
// ─────────────────────────────────────────────────────────────────────────────

/// The reserved schema name for all auth system tables (ADR 0022 / 5.18.C).
///
/// Canonical table identifiers are `auth.<table>` — see the module-level
/// table for the full mapping from canonical to legacy names.
pub const RESERVED_SCHEMA: &str = "auth";

/// Bare table names within the `auth` schema (canonical, post-5.18.C form).
pub mod table_names {
    pub const USERS: &str = "users";
    pub const REFRESH_TOKENS: &str = "refresh_tokens";
    pub const EMAIL_TOKENS: &str = "email_tokens";
    pub const API_KEYS: &str = "api_keys";
    pub const USER_SESSION_SETTINGS: &str = "user_session_settings";
    pub const MAGIC_LINKS: &str = "magic_links";
    pub const REVOKED_REFRESH_TOKENS: &str = "revoked_refresh_tokens";
    pub const PROJECT_CREDENTIALS: &str = "project_credentials";
    pub const OAUTH_PROVIDERS: &str = "oauth_providers";
    pub const IDENTITIES: &str = "identities";
    pub const OAUTH_STATES: &str = "oauth_states";
}

/// Given a canonical bare table name within the `auth` schema (e.g.
/// `"users"`), return the legacy flat prefix used in the old
/// `<schema>_<table>` scheme (e.g. `"users"` → `"users"` suffix, where the
/// full old name was `"{prefix}_users"`).
///
/// Returns `None` for unknown table names (not managed by this module).
///
/// Use this for back-compat read paths: if the new `auth.<table>` catalog
/// entry is absent, fall back to looking for `{schema}_{suffix}` in the
/// legacy flat namespace.
pub fn legacy_suffix_for(canonical_table: &str) -> Option<&'static str> {
    match canonical_table {
        table_names::USERS => Some("users"),
        table_names::REFRESH_TOKENS => Some("refresh_tokens"),
        table_names::EMAIL_TOKENS => Some("email_tokens"),
        table_names::API_KEYS => Some("api_keys"),
        table_names::USER_SESSION_SETTINGS => Some("user_session_settings"),
        // magic_links and revoked_refresh_tokens had an extra `auth_` infix
        table_names::MAGIC_LINKS => Some("auth_magic_links"),
        table_names::REVOKED_REFRESH_TOKENS => Some("auth_revoked_refresh_tokens"),
        table_names::PROJECT_CREDENTIALS => Some("auth_project_credentials"),
        table_names::OAUTH_PROVIDERS => Some("oauth_providers"),
        table_names::IDENTITIES => Some("identities"),
        table_names::OAUTH_STATES => Some("oauth_states"),
        _ => None,
    }
}

/// Given a legacy flat table name (the full name including the schema prefix,
/// e.g. `"basin_auth_users"`), return the canonical `auth.<table>` form.
///
/// Returns `None` if the name doesn't match any known auth table pattern.
///
/// This is the reverse of the legacy prefix + [`legacy_suffix_for`] scheme.
pub fn canonical_auth_table_name(schema_prefix: &str, legacy_name: &str) -> Option<String> {
    // Try stripping the schema prefix + underscore from the start.
    let prefix_sep = format!("{schema_prefix}_");
    let suffix = legacy_name.strip_prefix(prefix_sep.as_str())?;
    // Map legacy suffixes back to canonical bare names.
    let canonical = match suffix {
        "users" => table_names::USERS,
        "refresh_tokens" => table_names::REFRESH_TOKENS,
        "email_tokens" => table_names::EMAIL_TOKENS,
        "api_keys" => table_names::API_KEYS,
        "user_session_settings" => table_names::USER_SESSION_SETTINGS,
        "auth_magic_links" => table_names::MAGIC_LINKS,
        "auth_revoked_refresh_tokens" => table_names::REVOKED_REFRESH_TOKENS,
        "auth_project_credentials" => table_names::PROJECT_CREDENTIALS,
        "oauth_providers" => table_names::OAUTH_PROVIDERS,
        "identities" => table_names::IDENTITIES,
        "oauth_states" => table_names::OAUTH_STATES,
        _ => return None,
    };
    Some(format!("{RESERVED_SCHEMA}.{canonical}"))
}

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
        // --- OAuth provider config (Phase 5.10.O / ADR 0020) ---
        //
        // Stores per-project OAuth 2.0 / OIDC provider configuration.
        // `client_secret` is AES-GCM encrypted by the EncryptionProvider
        // before being stored. For preset providers (google, github, apple)
        // the endpoint/scope columns are populated from defaults; only
        // `client_id` + `client_secret` are required. Generic OIDC sets
        // `discovery_url` and the explicit endpoint columns are filled in at
        // runtime via RFC 8414 discovery.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_oauth_providers (
                id               BIGSERIAL PRIMARY KEY,
                project_id        TEXT NOT NULL,
                provider         TEXT NOT NULL,
                client_id        TEXT NOT NULL,
                client_secret    TEXT NOT NULL,
                scopes           TEXT NOT NULL DEFAULT '',
                redirect_uri     TEXT NOT NULL DEFAULT '',
                discovery_url    TEXT NOT NULL DEFAULT '',
                authorize_url    TEXT NOT NULL DEFAULT '',
                token_url        TEXT NOT NULL DEFAULT '',
                userinfo_url     TEXT NOT NULL DEFAULT '',
                enabled          BOOLEAN NOT NULL DEFAULT TRUE,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (project_id, provider)
            )"
        ),
        // OAuth identity links — one row per (user, provider) pair.
        // `provider_user_id` is the sub/id from the provider's userinfo.
        // `email_verified` records whether the provider asserted email
        // verification at the time of the last sign-in.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_identities (
                id                BIGSERIAL PRIMARY KEY,
                user_id           UUID NOT NULL REFERENCES {schema}_users(user_id) ON DELETE CASCADE,
                project_id         TEXT NOT NULL,
                provider          TEXT NOT NULL,
                provider_user_id  TEXT NOT NULL,
                email             TEXT NOT NULL,
                email_verified    BOOLEAN NOT NULL DEFAULT FALSE,
                created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_sign_in_at   TIMESTAMPTZ,
                UNIQUE (project_id, provider, provider_user_id)
            )"
        ),
        // OAuth PKCE / state store — short-lived rows tracking in-flight
        // authorization code flows. The `state` value is HMAC-signed and also
        // stored here so we can validate the callback quickly. Each row is
        // deleted on successful callback or on expiry.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_oauth_states (
                state_hash   TEXT PRIMARY KEY,
                project_id    TEXT NOT NULL,
                provider     TEXT NOT NULL,
                pkce_verifier TEXT NOT NULL,
                redirect_to  TEXT NOT NULL DEFAULT '',
                expires_at   TIMESTAMPTZ NOT NULL,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            )"
        ),
        // MFA factors (Phase 5.10.M / 6.SEC.P0). `secret_enc` carries the
        // encrypted TOTP base32 secret, or — for WebAuthn — the serialised
        // passkey credential (credential id + COSE public key + sign-counter).
        // `last_used_step` is the replay/clone guard: the highest accepted TOTP
        // 30-second step (RFC 6238 §5.2) or the highest WebAuthn sign-counter.
        // A code/assertion at or below this value is a replay and is rejected.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_mfa_factors (
                id              UUID PRIMARY KEY,
                user_id         UUID NOT NULL REFERENCES {schema}_users(user_id) ON DELETE CASCADE,
                project_id      TEXT NOT NULL,
                factor_type     TEXT NOT NULL,
                status          TEXT NOT NULL,
                secret_enc      TEXT NOT NULL,
                friendly_name   TEXT NOT NULL,
                last_used_step  BIGINT NOT NULL DEFAULT 0,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
            )"
        ),
        // Defensive add-column for schemas migrated before 6.SEC.P0 landed the
        // replay guard. Idempotent on Postgres ≥ 9.6.
        format!(
            "ALTER TABLE {schema}_mfa_factors \
             ADD COLUMN IF NOT EXISTS last_used_step BIGINT NOT NULL DEFAULT 0"
        ),
        // Short-lived MFA challenges (step-up + WebAuthn registration/assertion).
        // `challenge_data` carries the base64url challenge nonce.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_mfa_challenges (
                id             UUID PRIMARY KEY,
                factor_id      UUID NOT NULL,
                user_id        UUID NOT NULL REFERENCES {schema}_users(user_id) ON DELETE CASCADE,
                project_id     TEXT NOT NULL,
                expires_at     TIMESTAMPTZ NOT NULL,
                challenge_data TEXT NOT NULL DEFAULT ''
            )"
        ),
        // Single-use recovery codes (bcrypt-hashed). `consumed_at` non-null ⇒ spent.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}_mfa_recovery_codes (
                id           BIGSERIAL PRIMARY KEY,
                user_id      UUID NOT NULL REFERENCES {schema}_users(user_id) ON DELETE CASCADE,
                project_id   TEXT NOT NULL,
                code_hash    TEXT NOT NULL,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                consumed_at  TIMESTAMPTZ
            )"
        ),
        // Secondary indexes — one per hot lookup path:
        //   - api_keys (key_hash): bearer-token verification
        //   - auth_magic_links (token_hash): consume-flow lookup
        //   - auth_revoked_refresh_tokens (user_id): reuse-detection sweep
        //   - auth_project_credentials (pgwire_user): pgwire startup auth
        //   - identities (project_id, provider, email): email-link lookup
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
        format!(
            "CREATE INDEX IF NOT EXISTS {schema}_identities_project_provider_email \
             ON {schema}_identities (project_id, provider, email)"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {schema}_oauth_states_expires_at \
             ON {schema}_oauth_states (expires_at)"
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

    // ── Phase 5.18.C back-compat alias tests ──────────────────────────────

    #[test]
    fn reserved_schema_is_auth() {
        assert_eq!(RESERVED_SCHEMA, "auth");
    }

    #[test]
    fn legacy_suffix_known_tables() {
        // Primary tables map to bare suffix (no schema infix).
        assert_eq!(legacy_suffix_for(table_names::USERS), Some("users"));
        assert_eq!(
            legacy_suffix_for(table_names::REFRESH_TOKENS),
            Some("refresh_tokens")
        );
        assert_eq!(
            legacy_suffix_for(table_names::EMAIL_TOKENS),
            Some("email_tokens")
        );
        assert_eq!(legacy_suffix_for(table_names::API_KEYS), Some("api_keys"));
        assert_eq!(
            legacy_suffix_for(table_names::USER_SESSION_SETTINGS),
            Some("user_session_settings")
        );
        // These had an extra `auth_` infix in the old flat name.
        assert_eq!(
            legacy_suffix_for(table_names::MAGIC_LINKS),
            Some("auth_magic_links")
        );
        assert_eq!(
            legacy_suffix_for(table_names::REVOKED_REFRESH_TOKENS),
            Some("auth_revoked_refresh_tokens")
        );
        assert_eq!(
            legacy_suffix_for(table_names::PROJECT_CREDENTIALS),
            Some("auth_project_credentials")
        );
        assert_eq!(
            legacy_suffix_for(table_names::OAUTH_PROVIDERS),
            Some("oauth_providers")
        );
        assert_eq!(
            legacy_suffix_for(table_names::IDENTITIES),
            Some("identities")
        );
        assert_eq!(
            legacy_suffix_for(table_names::OAUTH_STATES),
            Some("oauth_states")
        );
    }

    #[test]
    fn legacy_suffix_unknown_returns_none() {
        assert_eq!(legacy_suffix_for("nonexistent"), None);
        assert_eq!(legacy_suffix_for(""), None);
    }

    #[test]
    fn canonical_auth_table_name_round_trip() {
        let schema = "basin_auth";
        // Users: basin_auth_users → auth.users
        assert_eq!(
            canonical_auth_table_name(schema, "basin_auth_users"),
            Some("auth.users".to_string())
        );
        // Revoked refresh tokens has an auth_ infix.
        assert_eq!(
            canonical_auth_table_name(schema, "basin_auth_auth_revoked_refresh_tokens"),
            Some("auth.revoked_refresh_tokens".to_string())
        );
        // Magic links.
        assert_eq!(
            canonical_auth_table_name(schema, "basin_auth_auth_magic_links"),
            Some("auth.magic_links".to_string())
        );
        // Project credentials.
        assert_eq!(
            canonical_auth_table_name(schema, "basin_auth_auth_project_credentials"),
            Some("auth.project_credentials".to_string())
        );
        // OAuth providers.
        assert_eq!(
            canonical_auth_table_name(schema, "basin_auth_oauth_providers"),
            Some("auth.oauth_providers".to_string())
        );
        // Unknown suffix: returns None.
        assert_eq!(
            canonical_auth_table_name(schema, "basin_auth_unknown_table"),
            None
        );
        // Does not match a different schema prefix.
        assert_eq!(
            canonical_auth_table_name("other_prefix", "basin_auth_users"),
            None
        );
    }

    #[test]
    fn legacy_flat_names_resolve_via_suffix_helper() {
        // Verify that the legacy flat name `basin_auth_<suffix>` is what
        // you get when using the schema prefix + the suffix from
        // `legacy_suffix_for`.
        let prefix = "basin_auth";
        for name in [
            table_names::USERS,
            table_names::REFRESH_TOKENS,
            table_names::EMAIL_TOKENS,
            table_names::API_KEYS,
            table_names::USER_SESSION_SETTINGS,
            table_names::MAGIC_LINKS,
            table_names::REVOKED_REFRESH_TOKENS,
            table_names::PROJECT_CREDENTIALS,
            table_names::OAUTH_PROVIDERS,
            table_names::IDENTITIES,
            table_names::OAUTH_STATES,
        ] {
            let suffix = legacy_suffix_for(name).expect("every table_names:: has a suffix");
            let legacy = format!("{prefix}_{suffix}");
            // canonical_auth_table_name must recover the original table name.
            let canonical = canonical_auth_table_name(prefix, &legacy)
                .unwrap_or_else(|| panic!("no canonical for {legacy}"));
            assert_eq!(
                canonical,
                format!("auth.{name}"),
                "round-trip failed for {name}"
            );
        }
    }
}
