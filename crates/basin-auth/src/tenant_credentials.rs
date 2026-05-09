//! Per-tenant pgwire credentials.
//!
//! One row per tenant in `auth_tenant_credentials`. Each row maps a public
//! `pgwire_user` (the literal string a customer pastes into a Postgres URL,
//! e.g. `tenant_a1b2c3d4`) to a tenant id, a bcrypt-hashed password, and a
//! `dbname`. Validation is bcrypt-only — no second sha256 fast path — because
//! the authentication boundary on a pgwire connection is per-connection
//! (cheap enough at 12-cost bcrypt for the wedge).
//!
//! Plaintext passwords leave this module exactly twice: from
//! `provision_tenant_db` and from `rotate_pgwire_password`. After that, the
//! plaintext is permanently gone — only the bcrypt hash and the public
//! descriptor survive.

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

/// Build the canonical pgwire username form: `tenant_<8 hex chars>`. We
/// validate this prefix on consume so a malicious provisioner can't smuggle
/// `../` or other path-y junk into the URL.
fn generate_pgwire_user() -> String {
    let mut buf = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut buf);
    format!("tenant_{}", hex::encode(buf))
}

fn generate_password() -> String {
    let mut buf = [0u8; PASSWORD_BYTES];
    rand::thread_rng().fill_bytes(&mut buf);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

/// Reject anything that doesn't match `tenant_<hex>+`. Belt-and-braces:
/// `pgwire_user` flows into the connection URL we hand to operators, so
/// even though it's generated server-side we sanity-check it on every
/// boundary. Mismatched forms are a corrupted-row error, not a normal
/// auth-failure path.
fn validate_pgwire_user_format(s: &str) -> Result<()> {
    if !s.starts_with("tenant_") {
        return Err(BasinError::InvalidIdent(format!(
            "pgwire_user must start with 'tenant_': {s:?}"
        )));
    }
    let suffix = &s[7..];
    if suffix.is_empty() || !suffix.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(BasinError::InvalidIdent(format!(
            "pgwire_user suffix must be non-empty hex: {s:?}"
        )));
    }
    Ok(())
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
    // `user` is `tenant_<hex>` and the password is `URL_SAFE_NO_PAD` base64,
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
        let user = generate_pgwire_user();
        validate_pgwire_user_format(&user)?;
        let secret = generate_password();
        let hash = password::hash(&secret, inner.cfg.bcrypt_cost)?;
        let tenant_str = tenant.to_string();
        let client = inner.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "INSERT INTO {sch}.auth_tenant_credentials
                       (tenant_id, pgwire_user, password_hash, dbname)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT (pgwire_user) DO NOTHING
                     RETURNING id",
                    sch = inner.schema()
                ),
                &[&tenant_str, &user, &hash, &dbname],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("tenant_credentials insert: {e}")))?;
        drop(client);
        match row {
            Some(_) => {
                let connection_url = build_connection_url(
                    &inner.cfg.pgwire_public_host,
                    &user,
                    &secret,
                    dbname,
                );
                return Ok(ConnectionInfo {
                    tenant_id: *tenant,
                    pgwire_user: user,
                    dbname: dbname.to_owned(),
                    password_secret: secret,
                    connection_url,
                });
            }
            None => {
                last_err = Some(BasinError::CommitConflict(format!(
                    "pgwire_user {user:?} collided"
                )));
                continue;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| {
        BasinError::internal("provision_tenant_db: exhausted user retries")
    }))
}

pub(crate) async fn validate(
    inner: &Inner,
    user: &str,
    password_plain: &str,
) -> Result<TenantId> {
    // Uniform error for both "no such user" and "user exists but wrong
    // password" — keeps the SQLSTATE 28P01 surface minimal and avoids
    // leaking enumeration information. bcrypt's constant-ish work factor
    // does the timing-attack hardening; we add a single error string here.
    let unknown = || BasinError::InvalidIdent("invalid pgwire credentials".into());
    if user.is_empty() {
        return Err(unknown());
    }
    let client = inner.client.lock().await;
    let row = client
        .query_opt(
            &format!(
                "SELECT tenant_id, password_hash FROM {sch}.auth_tenant_credentials
                 WHERE pgwire_user = $1",
                sch = inner.schema()
            ),
            &[&user],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("tenant_credentials lookup: {e}")))?;
    drop(client);
    let Some(row) = row else {
        return Err(unknown());
    };
    let tenant_str: String = row.get(0);
    let hash: String = row.get(1);
    if !password::verify(password_plain, &hash)? {
        return Err(unknown());
    }
    let tenant: TenantId = tenant_str
        .parse()
        .map_err(|e| BasinError::internal(format!("tenant_credentials tenant parse: {e}")))?;
    Ok(tenant)
}

pub(crate) async fn rotate(inner: &Inner, pgwire_user: &str) -> Result<ConnectionInfo> {
    validate_pgwire_user_format(pgwire_user)?;
    let secret = generate_password();
    let hash = password::hash(&secret, inner.cfg.bcrypt_cost)?;
    let client = inner.client.lock().await;
    let row = client
        .query_opt(
            &format!(
                "UPDATE {sch}.auth_tenant_credentials
                   SET password_hash = $1, rotated_at = now()
                 WHERE pgwire_user = $2
                 RETURNING tenant_id, dbname",
                sch = inner.schema()
            ),
            &[&hash, &pgwire_user],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("tenant_credentials rotate: {e}")))?;
    drop(client);
    let Some(row) = row else {
        return Err(BasinError::not_found(format!(
            "pgwire_user {pgwire_user:?}"
        )));
    };
    let tenant_str: String = row.get(0);
    let dbname: String = row.get(1);
    let tenant: TenantId = tenant_str
        .parse()
        .map_err(|e| BasinError::internal(format!("tenant_credentials tenant parse: {e}")))?;
    let connection_url = build_connection_url(
        &inner.cfg.pgwire_public_host,
        pgwire_user,
        &secret,
        &dbname,
    );
    Ok(ConnectionInfo {
        tenant_id: tenant,
        pgwire_user: pgwire_user.to_owned(),
        dbname,
        password_secret: secret,
        connection_url,
    })
}

pub(crate) async fn list(
    inner: &Inner,
    tenant: &TenantId,
) -> Result<Vec<TenantCredentialDescriptor>> {
    let tenant_str = tenant.to_string();
    let client = inner.client.lock().await;
    let rows = client
        .query(
            &format!(
                "SELECT id, tenant_id, pgwire_user, dbname, created_at, rotated_at
                 FROM {sch}.auth_tenant_credentials
                 WHERE tenant_id = $1
                 ORDER BY id ASC",
                sch = inner.schema()
            ),
            &[&tenant_str],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("tenant_credentials list: {e}")))?;
    drop(client);
    rows.into_iter()
        .map(|row| {
            let tenant_str: String = row.get(1);
            let tenant: TenantId = tenant_str.parse().map_err(|e| {
                BasinError::internal(format!("tenant_credentials list tenant parse: {e}"))
            })?;
            Ok(TenantCredentialDescriptor {
                id: row.get(0),
                tenant_id: tenant,
                pgwire_user: row.get(2),
                dbname: row.get(3),
                created_at: row.get(4),
                rotated_at: row.get(5),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pgwire_user_format_round_trip() {
        let u = generate_pgwire_user();
        validate_pgwire_user_format(&u).unwrap();
        assert!(u.starts_with("tenant_"));
        // 4 bytes -> 8 hex chars.
        assert_eq!(u.len(), "tenant_".len() + 8);
    }

    #[test]
    fn pgwire_user_format_rejects_path_injection() {
        assert!(validate_pgwire_user_format("tenant_../etc").is_err());
        assert!(validate_pgwire_user_format("../etc").is_err());
        assert!(validate_pgwire_user_format("tenant_").is_err());
        assert!(validate_pgwire_user_format("tenant_GZX").is_err());
        // Embedded URL specials must also be rejected.
        assert!(validate_pgwire_user_format("tenant_aa@bb").is_err());
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
        let u = build_connection_url(
            "db.example.com:5433",
            "tenant_a1b2c3d4",
            "supersecret",
            "basin",
        );
        assert_eq!(
            u,
            "postgres://tenant_a1b2c3d4:supersecret@db.example.com:5433/basin"
        );
    }
}
