//! Per-tenant long-lived API keys.
//!
//! An API key is an opaque, server-issued bearer credential. Unlike a JWT
//! it has no embedded claims: every request resolves the owning tenant +
//! user via a DB lookup against [`schema::api_keys`]. Keys are returned
//! once at issuance time and are bcrypt-hashed at rest; a database leak
//! does not directly leak usable credentials.
//!
//! Lookup is two-stage to keep the hot path off bcrypt: we index on a
//! sha256 prefix-style `key_hash` column, then `bcrypt::verify` the row
//! we found against the presented secret. The sha256 column is *not* the
//! authentication boundary — bcrypt is — but it lets the lookup go
//! through an index instead of a full-table scan.

use base64::Engine;
use basin_common::{BasinError, Result, TenantId};
use chrono::{DateTime, Utc};
use rand::RngCore;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{password, Inner, UserId};

/// 32 random bytes is plenty (~256 bits of entropy); url-safe base64
/// makes the resulting string copy/paste-friendly in shell snippets.
const API_KEY_BYTES: usize = 32;

/// Plaintext secret returned only at issuance. Treat as opaque — the
/// caller must surface it to the user once and never again.
pub type ApiKeySecret = String;

/// Public-facing descriptor for `list_api_keys`. Never carries the
/// secret or any hash form.
#[derive(Debug, Clone)]
pub struct ApiKeyDescriptor {
    pub id: i64,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// Returned by `issue_api_key`. The `secret` field is the only place the
/// plaintext token is ever exposed.
#[derive(Debug, Clone)]
pub struct IssuedApiKey {
    pub id: i64,
    pub name: String,
    pub secret: ApiKeySecret,
    pub created_at: DateTime<Utc>,
}

fn generate_secret() -> String {
    let mut buf = [0u8; API_KEY_BYTES];
    rand::thread_rng().fill_bytes(&mut buf);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

fn key_hash(raw: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    hex::encode(hasher.finalize())
}

pub(crate) async fn issue(
    inner: &Inner,
    tenant: &TenantId,
    user: UserId,
    name: &str,
) -> Result<IssuedApiKey> {
    let name = name.trim();
    if name.is_empty() {
        return Err(BasinError::InvalidIdent("api key name is empty".into()));
    }
    if name.len() > 128 {
        return Err(BasinError::InvalidIdent(
            "api key name longer than 128 chars".into(),
        ));
    }
    let secret = generate_secret();
    let h = key_hash(&secret);
    let bcrypted = password::hash(&secret, inner.cfg.bcrypt_cost)?;
    let tenant_str = tenant.to_string();

    let client = inner.client.lock().await;
    let row = client
        .query_opt(
            &format!(
                "INSERT INTO {sch}.api_keys
                   (tenant_id, user_id, name, key_hash, key_bcrypt)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (tenant_id, user_id, name) DO NOTHING
                 RETURNING id, created_at",
                sch = inner.schema()
            ),
            &[&tenant_str, &user, &name, &h, &bcrypted],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("api_key insert: {e}")))?;
    let Some(row) = row else {
        return Err(BasinError::CommitConflict(format!(
            "api key {name:?} already exists for user"
        )));
    };
    let id: i64 = row.get(0);
    let created_at: DateTime<Utc> = row.get(1);
    Ok(IssuedApiKey {
        id,
        name: name.to_owned(),
        secret,
        created_at,
    })
}

pub(crate) async fn validate(inner: &Inner, raw: &str) -> Result<(TenantId, UserId)> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(BasinError::InvalidIdent("api key is empty".into()));
    }
    let h = key_hash(raw);
    let client = inner.client.lock().await;
    // The sha256 hash is unique enough in practice that we'd be surprised
    // by more than one row; treat all matches as candidates and bcrypt-
    // verify each.
    let rows = client
        .query(
            &format!(
                "SELECT id, tenant_id, user_id, key_bcrypt, revoked_at
                 FROM {sch}.api_keys
                 WHERE key_hash = $1",
                sch = inner.schema()
            ),
            &[&h],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("api_key lookup: {e}")))?;
    for row in rows {
        let id: i64 = row.get(0);
        let tenant_str: String = row.get(1);
        let user_id: Uuid = row.get(2);
        let bcrypted: String = row.get(3);
        let revoked_at: Option<DateTime<Utc>> = row.get(4);
        if revoked_at.is_some() {
            continue;
        }
        if password::verify(raw, &bcrypted)? {
            // Best-effort touch; a failed UPDATE here must not deny auth.
            let _ = client
                .execute(
                    &format!(
                        "UPDATE {sch}.api_keys SET last_used_at = now()
                         WHERE id = $1",
                        sch = inner.schema()
                    ),
                    &[&id],
                )
                .await;
            let tenant: TenantId = tenant_str
                .parse()
                .map_err(|e| BasinError::internal(format!("api_key tenant parse: {e}")))?;
            return Ok((tenant, user_id));
        }
    }
    Err(BasinError::InvalidIdent("invalid api key".into()))
}

pub(crate) async fn revoke(inner: &Inner, key_id: i64, tenant: &TenantId) -> Result<()> {
    let tenant_str = tenant.to_string();
    let client = inner.client.lock().await;
    let n = client
        .execute(
            &format!(
                "UPDATE {sch}.api_keys SET revoked_at = now()
                 WHERE id = $1 AND tenant_id = $2 AND revoked_at IS NULL",
                sch = inner.schema()
            ),
            &[&key_id, &tenant_str],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("api_key revoke: {e}")))?;
    if n == 0 {
        // Either unknown id, wrong tenant, or already revoked. Disambiguate
        // so callers can return 404 vs. 200-no-op cleanly.
        let exists = client
            .query_opt(
                &format!(
                    "SELECT 1 FROM {sch}.api_keys
                     WHERE id = $1 AND tenant_id = $2",
                    sch = inner.schema()
                ),
                &[&key_id, &tenant_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("api_key revoke check: {e}")))?;
        if exists.is_none() {
            return Err(BasinError::not_found(format!("api key {key_id}")));
        }
        // Already revoked — idempotent success.
    }
    Ok(())
}

pub(crate) async fn list(
    inner: &Inner,
    tenant: &TenantId,
    user: UserId,
) -> Result<Vec<ApiKeyDescriptor>> {
    let tenant_str = tenant.to_string();
    let client = inner.client.lock().await;
    let rows = client
        .query(
            &format!(
                "SELECT id, name, created_at, last_used_at, revoked_at
                 FROM {sch}.api_keys
                 WHERE tenant_id = $1 AND user_id = $2
                 ORDER BY id ASC",
                sch = inner.schema()
            ),
            &[&tenant_str, &user],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("api_key list: {e}")))?;
    Ok(rows
        .into_iter()
        .map(|row| ApiKeyDescriptor {
            id: row.get(0),
            name: row.get(1),
            created_at: row.get(2),
            last_used_at: row.get(3),
            revoked_at: row.get(4),
        })
        .collect())
}
