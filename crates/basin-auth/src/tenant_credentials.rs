//! Per-tenant pgwire credentials.
//!
//! One row per tenant in `auth_tenant_credentials`. Each row maps a public
//! `pgwire_user` (the literal string a customer pastes into a Postgres URL,
//! e.g. `01JBAS1NAVTH00000000000000_a1b2c3d4`) to a tenant id, a
//! bcrypt-hashed password, and a `dbname`. Validation is bcrypt-only — no
//! second sha256 fast path — because the authentication boundary on a pgwire
//! connection is per-connection (cheap enough at 12-cost bcrypt for the
//! wedge).
//!
//! Plaintext passwords leave this module exactly twice: from
//! `provision_tenant_db` and from `rotate_pgwire_password`. After that, the
//! plaintext is permanently gone — only the bcrypt hash and the public
//! descriptor survive.
//!
//! ## pgwire_user format
//!
//! `{tenant_id}_{8 hex chars}` where tenant_id is the full 26-char ULID.
//! Example: `01JBAS1NAVTH00000000000000_a1b2c3d4`.
//! The tenant_id is embedded so callers can extract it without a DB round-trip
//! using [`parse_tenant_from_pgwire_user`].

use base64::Engine;
use basin_common::{BasinError, Result, TenantId};
use chrono::{DateTime, Utc};
use rand::RngCore;

use crate::{password, Inner};

/// 24 random bytes -> ~32 chars of `URL_SAFE_NO_PAD` base64. The character
/// set is `A-Za-z0-9-_`, which means the password pastes cleanly into a
/// `postgres://user:password@host/db` URL without percent-encoding — that's
/// the whole point of choosing this alphabet over the standard one.
const PASSWORD_BYTES: usize = 24;

/// Public-facing descriptor for `list_tenant_credentials`. Carries the
/// columns an operator needs to render a credentials page; never the hash,
/// never the plaintext.
#[derive(Debug, Clone)]
pub struct TenantCredentialDescriptor {
    pub id: i64,
    pub tenant_id: TenantId,
    pub pgwire_user: String,
    pub dbname: String,
    pub created_at: DateTime<Utc>,
    pub rotated_at: Option<DateTime<Utc>>,
}

/// Returned exactly once from `provision_tenant_db` and `rotate_pgwire_password`.
/// The `password_secret` field is the only place the plaintext exists outside
/// the customer's clipboard; after the response goes out, it's gone.
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub tenant_id: TenantId,
    pub pgwire_user: String,
    pub dbname: String,
    /// Plaintext password — present only on the response from
    /// `provision_tenant_db` / `rotate_pgwire_password`, never returned again.
    pub password_secret: String,
    /// Convenience: a complete `postgres://user:password@host/dbname` URL
    /// the customer can paste into psql / tokio-postgres / asyncpg / JDBC.
    pub connection_url: String,
}

/// Generate a pgwire username in the new ADR-0013 format:
/// `{26-char-tenant-ulid}_{8 hex chars}`. The tenant ULID embedded in the
/// username makes it self-routing — the pgwire auth handler can call
/// [`parse_tenant_from_pgwire_user`] to resolve the tenant without a global
/// DB lookup, even before the password has been verified.
fn generate_pgwire_user(tenant: &TenantId) -> String {
    let mut buf = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut buf);
    format!("{}_{}", tenant.to_string(), hex::encode(buf))
}

fn generate_password() -> String {
    let mut buf = [0u8; PASSWORD_BYTES];
    rand::thread_rng().fill_bytes(&mut buf);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

/// Returns `true` if `pgwire_user` is in the pre-ADR-0013 `tenant_<hex>`
/// format, which does not embed the tenant ULID and therefore requires a DB
/// lookup to resolve the tenant. Credentials in this format must be migrated
/// to the new `{26-char-ulid}_{hex}` format via
/// [`migrate_legacy_credential`] before the self-routing path can be used.
///
/// The check is deliberately conservative: only strings that start with
/// `"tenant_"` AND are shorter than 20 chars are treated as legacy (the old
/// format is `tenant_` + 8 hex chars = 15 chars; the new format is at least
/// 28 chars).
pub fn is_legacy_pgwire_user(pgwire_user: &str) -> bool {
    pgwire_user.starts_with("tenant_") && pgwire_user.len() < 20
}

/// Extract the 26-character tenant ULID from a new-format pgwire username
/// (`{26-char-ulid}_{suffix}`). This makes credential validation self-routing:
/// the pgwire handler can identify the tenant from the username alone without a
/// global DB lookup, enabling efficient per-tenant auth table access. Returns
/// `None` for usernames that don't match the format (e.g. legacy `tenant_<hex>`
/// credentials).
pub fn parse_tenant_from_pgwire_user(pgwire_user: &str) -> Option<&str> {
    // ULID is 26 chars, followed by '_'
    if pgwire_user.len() > 27 && pgwire_user.as_bytes()[26] == b'_' {
        Some(&pgwire_user[..26])
    } else {
        None
    }
}

/// Validate that a pgwire_user matches the new `{26-char-ulid}_{hex}` format.
/// Also accepts the legacy `tenant_{hex}` format for backwards-compat reads.
fn validate_pgwire_user_format(s: &str) -> Result<()> {
    // New format: 26-char ULID + '_' + hex suffix
    if s.len() > 27 && s.as_bytes()[26] == b'_' {
        let suffix = &s[27..];
        if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_hexdigit()) {
            return Ok(());
        }
    }
    // Legacy format: `tenant_<hex>`
    if s.starts_with("tenant_") {
        let suffix = &s[7..];
        if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_hexdigit()) {
            return Ok(());
        }
    }
    Err(BasinError::InvalidIdent(format!(
        "pgwire_user has invalid format: {s:?}"
    )))
}

fn validate_dbname(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(BasinError::InvalidIdent("dbname is empty".into()));
    }
    if s.len() > 63 {
        return Err(BasinError::InvalidIdent(
            "dbname longer than 63 chars".into(),
        ));
    }
    if !s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(BasinError::InvalidIdent(format!(
            "dbname has invalid char: {s:?}"
        )));
    }
    Ok(())
}

fn build_connection_url(host: &str, user: &str, password: &str, dbname: &str) -> String {
    // `user` is `{ulid}_{hex}` and the password is `URL_SAFE_NO_PAD` base64,
    // so neither needs percent-encoding. Same goes for dbname (validated
    // above as alphanumeric/_/-). If we ever broaden any of those, this
    // helper grows a real percent-encoder.
    format!("postgres://{user}:{password}@{host}/{dbname}")
}

pub(crate) async fn provision(
    inner: &Inner,
    tenant: &TenantId,
    dbname: Option<&str>,
) -> Result<ConnectionInfo> {
    let dbname = dbname.unwrap_or("basin");
    validate_dbname(dbname)?;

    // Retry on the rare username collision; 32 bits of entropy is plenty
    // for v0.1, but the UNIQUE index can still kick on a duplicate.
    let mut last_err = None;
    for _ in 0..4 {
        let user = generate_pgwire_user(tenant);
        validate_pgwire_user_format(&user)?;
        let secret = generate_password();
        let hash = password::hash(&secret, inner.cfg.bcrypt_cost)?;
        let inserted = inner
            .store
            .insert_tenant_credential(tenant, &user, &hash, dbname)
            .await?;
        if inserted {
            let connection_url =
                build_connection_url(&inner.cfg.pgwire_public_host, &user, &secret, dbname);
            return Ok(ConnectionInfo {
                tenant_id: *tenant,
                pgwire_user: user,
                dbname: dbname.to_owned(),
                password_secret: secret,
                connection_url,
            });
        } else {
            last_err = Some(BasinError::CommitConflict(format!(
                "pgwire_user {user:?} collided"
            )));
        }
    }
    Err(last_err
        .unwrap_or_else(|| BasinError::internal("provision_tenant_db: exhausted user retries")))
}

pub(crate) async fn validate(inner: &Inner, user: &str, password_plain: &str) -> Result<TenantId> {
    // Uniform error for both "no such user" and "user exists but wrong
    // password" — keeps the SQLSTATE 28P01 surface minimal and avoids
    // leaking enumeration information. bcrypt's constant-ish work factor
    // does the timing-attack hardening; we add a single error string here.
    let unknown = || BasinError::InvalidIdent("invalid pgwire credentials".into());
    if user.is_empty() {
        return Err(unknown());
    }
    let row = inner.store.find_tenant_credential(user).await?;
    let Some(row) = row else {
        return Err(unknown());
    };
    if !password::verify(password_plain, &row.password_hash)? {
        return Err(unknown());
    }
    let tenant: TenantId = row
        .tenant_id
        .parse()
        .map_err(|e| BasinError::internal(format!("tenant_credentials tenant parse: {e}")))?;
    Ok(tenant)
}

pub(crate) async fn rotate(inner: &Inner, pgwire_user: &str) -> Result<ConnectionInfo> {
    validate_pgwire_user_format(pgwire_user)?;
    let secret = generate_password();
    let hash = password::hash(&secret, inner.cfg.bcrypt_cost)?;
    let row = inner
        .store
        .rotate_tenant_credential(pgwire_user, &hash)
        .await?;
    let Some(row) = row else {
        return Err(BasinError::not_found(format!(
            "pgwire_user {pgwire_user:?}"
        )));
    };
    let tenant: TenantId = row
        .tenant_id
        .parse()
        .map_err(|e| BasinError::internal(format!("tenant_credentials tenant parse: {e}")))?;
    let connection_url =
        build_connection_url(&inner.cfg.pgwire_public_host, pgwire_user, &secret, &row.dbname);
    Ok(ConnectionInfo {
        tenant_id: tenant,
        pgwire_user: pgwire_user.to_owned(),
        dbname: row.dbname,
        password_secret: secret,
        connection_url,
    })
}

pub(crate) async fn list(
    inner: &Inner,
    tenant: &TenantId,
) -> Result<Vec<TenantCredentialDescriptor>> {
    inner.store.list_tenant_credentials(tenant).await
}

/// Returns every credential row whose `pgwire_user` is in the old
/// `tenant_<hex>` format. Used by the upgrade migration to discover rows
/// that need to be rotated to the new `{tenant_id}_{hex}` format.
///
/// Returns a list of `(tenant_id, old_pgwire_user)` pairs.
pub(crate) async fn list_legacy(inner: &Inner) -> Result<Vec<(TenantId, String)>> {
    inner.store.list_legacy_tenant_credentials().await
}

/// Rotates a single legacy credential in place:
///
/// 1. Generates a new pgwire_user in the new `{tenant_id}_{hex}` format.
/// 2. Generates a new random password.
/// 3. Inserts the new credential row.
/// 4. Deletes the old credential row (only after the insert succeeds).
///
/// Returns `(new_pgwire_user, plaintext_password)`. The plaintext password
/// is returned so the caller (e.g. basin-cloud's startup migration job) can
/// update the corresponding row in cloud's `project_pgwire_credentials` table.
///
/// The function is idempotent with respect to the *old* credential: if the
/// old row is already gone (migrated by a previous run), it returns a
/// `BasinError::NotFound` which the caller should treat as "already done".
pub(crate) async fn migrate_legacy_credential(
    inner: &Inner,
    tenant: &TenantId,
    old_pgwire_user: &str,
) -> Result<(String, String)> {
    // Sanity-check: only migrate credentials that actually need it.
    if !is_legacy_pgwire_user(old_pgwire_user) {
        return Err(BasinError::InvalidIdent(format!(
            "migrate_legacy_credential: {old_pgwire_user:?} is not a legacy pgwire_user"
        )));
    }

    // Try up to 4 times in case of a username collision (same as provision).
    let mut last_err = None;
    for _ in 0..4 {
        let new_user = generate_pgwire_user(tenant);
        let secret = generate_password();
        let hash = password::hash(&secret, inner.cfg.bcrypt_cost)?;

        // INSERT new row first — keep the old row alive until we succeed.
        let inserted = inner
            .store
            .insert_tenant_credential(tenant, &new_user, &hash, "basin")
            .await?;

        if inserted {
            // New row is in place; now remove the old one.
            inner
                .store
                .delete_tenant_credential(old_pgwire_user)
                .await?;
            return Ok((new_user, secret));
        } else {
            last_err = Some(BasinError::CommitConflict(format!(
                "new pgwire_user {new_user:?} collided during migration"
            )));
        }
    }
    Err(last_err
        .unwrap_or_else(|| BasinError::internal("migrate_legacy_credential: exhausted user retries")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::TenantId;

    #[test]
    fn pgwire_user_format_round_trip() {
        let tenant = TenantId::new();
        let u = generate_pgwire_user(&tenant);
        validate_pgwire_user_format(&u).unwrap();
        // 26-char ULID + '_' + 8 hex chars = 35 chars
        assert_eq!(u.len(), 26 + 1 + 8);
        assert_eq!(&u[..26], tenant.to_string().as_str());
    }

    #[test]
    fn parse_tenant_round_trip() {
        let tenant = TenantId::new();
        let u = generate_pgwire_user(&tenant);
        let parsed = parse_tenant_from_pgwire_user(&u).expect("should parse");
        assert_eq!(parsed, tenant.to_string().as_str());
    }

    #[test]
    fn parse_tenant_rejects_short() {
        assert!(parse_tenant_from_pgwire_user("tenant_a1b2c3d4").is_none());
        assert!(parse_tenant_from_pgwire_user("short").is_none());
    }

    #[test]
    fn pgwire_user_format_rejects_path_injection() {
        assert!(validate_pgwire_user_format("tenant_../etc").is_err());
        assert!(validate_pgwire_user_format("../etc").is_err());
        assert!(validate_pgwire_user_format("tenant_").is_err());
    }

    #[test]
    fn dbname_validation() {
        validate_dbname("basin").unwrap();
        validate_dbname("basin_2").unwrap();
        validate_dbname("a-b").unwrap();
        assert!(validate_dbname("").is_err());
        assert!(validate_dbname("../etc").is_err());
        assert!(validate_dbname(&"a".repeat(64)).is_err());
    }

    #[test]
    fn password_is_url_safe() {
        // Generated passwords must not require percent-encoding when pasted
        // into a postgres:// URL — i.e. only [A-Za-z0-9-_].
        let p = generate_password();
        for c in p.chars() {
            assert!(
                c.is_ascii_alphanumeric() || c == '-' || c == '_',
                "password contains URL-unsafe char {c:?}"
            );
        }
    }

    #[test]
    fn connection_url_shape() {
        let tenant = TenantId::new();
        let user = generate_pgwire_user(&tenant);
        let u = build_connection_url("db.example.com:5433", &user, "supersecret", "basin");
        assert!(u.starts_with("postgres://"));
        assert!(u.contains("supersecret@db.example.com:5433/basin"));
    }

    // --- parse_tenant_from_pgwire_user: new comprehensive cases ---

    #[test]
    fn parse_tenant_valid_new_format() {
        // Valid new format: exactly 26-char ULID + '_' + any suffix
        let tenant = TenantId::new();
        let tenant_str = tenant.to_string();
        assert_eq!(tenant_str.len(), 26, "TenantId must be 26 chars");
        let pgwire_user = format!("{tenant_str}_a1b2c3d4");
        let parsed = parse_tenant_from_pgwire_user(&pgwire_user).expect("must parse new format");
        assert_eq!(parsed, tenant_str.as_str());
    }

    #[test]
    fn parse_tenant_requires_more_than_27_chars() {
        // Exactly 27 chars (26 + '_' + nothing) → len == 27, not > 27 → None.
        let tenant = TenantId::new();
        let user_no_suffix = format!("{}_", tenant);
        assert_eq!(user_no_suffix.len(), 27);
        assert!(
            parse_tenant_from_pgwire_user(&user_no_suffix).is_none(),
            "need at least one char after the underscore"
        );
    }

    #[test]
    fn parse_tenant_old_legacy_format_returns_none() {
        // Old `tenant_<hex>` format does not embed a 26-char ULID → None.
        assert!(parse_tenant_from_pgwire_user("tenant_a1b2c3d4").is_none());
    }

    #[test]
    fn parse_tenant_no_underscore_at_position_26_returns_none() {
        // 26 chars with no underscore at all.
        assert!(parse_tenant_from_pgwire_user("01JBAS1NAVTH00000000000000").is_none());
        // 26 chars + non-underscore separator.
        assert!(parse_tenant_from_pgwire_user("01JBAS1NAVTH00000000000000-suffix").is_none());
    }

    #[test]
    fn parse_tenant_too_short_returns_none() {
        assert!(parse_tenant_from_pgwire_user("abc_def").is_none());
        assert!(parse_tenant_from_pgwire_user("").is_none());
        assert!(parse_tenant_from_pgwire_user("_").is_none());
    }

    #[test]
    fn parse_tenant_self_routing_invariant() {
        // Every username generated by `generate_pgwire_user` must parse back
        // to the exact tenant_id it was generated for.
        for _ in 0..20 {
            let tenant = TenantId::new();
            let u = generate_pgwire_user(&tenant);
            let parsed = parse_tenant_from_pgwire_user(&u).expect("generated user must parse");
            assert_eq!(
                parsed,
                tenant.to_string().as_str(),
                "self-routing invariant broken: generated={u:?} parsed={parsed:?}"
            );
        }
    }

    // --- is_legacy_pgwire_user ---

    #[test]
    fn is_legacy_true_for_tenant_hex_format() {
        assert!(is_legacy_pgwire_user("tenant_a1b2c3d4"));
        assert!(is_legacy_pgwire_user("tenant_deadbeef"));
        // 15 chars total (7 + 8): shorter than 20 → legacy.
        assert!(is_legacy_pgwire_user("tenant_00000000"));
    }

    #[test]
    fn is_legacy_false_for_new_format() {
        let tenant = TenantId::new();
        let u = generate_pgwire_user(&tenant);
        assert!(
            !is_legacy_pgwire_user(&u),
            "new-format user must not be flagged legacy: {u:?}"
        );
    }

    #[test]
    fn is_legacy_false_for_empty_string() {
        assert!(!is_legacy_pgwire_user(""));
    }

    #[test]
    fn is_legacy_false_for_long_tenant_prefix() {
        // Starts with "tenant_" but is >= 20 chars (new-format collision unlikely
        // but the boundary is what matters for migration correctness).
        let long = "tenant_0000000000000"; // 7 + 13 = 20 chars
        assert!(!is_legacy_pgwire_user(long), "length 20 is NOT legacy");
    }

    #[test]
    fn is_legacy_false_for_unrelated_string() {
        assert!(!is_legacy_pgwire_user("01JBAS1NAVTH00000000000000_a1b2c3d4"));
        assert!(!is_legacy_pgwire_user("notatenantatall"));
    }
}
