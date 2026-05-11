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
//!
//! ## Key format
//!
//! Keys are issued in the format `basin_{tenant_id}_{random_urlsafe_base64}`,
//! where `tenant_id` is the 26-char ULID. This makes the key self-routing:
//! callers can extract the tenant from the key without a DB round-trip.
//!
//! The sha256 lookup hash is still computed from the full key string.

use base64::Engine;
use basin_common::{BasinError, Result, TenantId};
use chrono::{DateTime, Utc};
use rand::RngCore;
use sha2::{Digest, Sha256};

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

fn generate_secret(tenant: &TenantId) -> String {
    let mut buf = [0u8; API_KEY_BYTES];
    rand::thread_rng().fill_bytes(&mut buf);
    let random_part = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf);
    format!("basin_{}_{}", tenant.to_string(), random_part)
}

fn key_hash(raw: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    hex::encode(hasher.finalize())
}

/// Extract the 26-character tenant ULID from an API key of the form
/// `basin_{26-char-ulid}_{url-safe-base64}`. The embedded tenant makes keys
/// self-routing: callers can identify the tenant without a DB round-trip before
/// the bcrypt verify step. Returns `None` if the key doesn't match the expected
/// format (e.g. old opaque random-byte keys that predate this format).
pub fn parse_tenant_from_api_key(raw_key: &str) -> Option<&str> {
    let rest = raw_key.strip_prefix("basin_")?;
    // ULID is 26 chars, followed by '_'
    if rest.len() > 27 && rest.as_bytes()[26] == b'_' {
        Some(&rest[..26])
    } else {
        None
    }
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
    let secret = generate_secret(tenant);
    let h = key_hash(&secret);
    let bcrypted = password::hash(&secret, inner.cfg.bcrypt_cost)?;

    let (id, created_at) = inner
        .store
        .insert_api_key(tenant, user, name, &h, &bcrypted)
        .await?;

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

    // The sha256 hash is unique enough in practice that we'd be surprised
    // by more than one row; treat all matches as candidates and bcrypt-
    // verify each.
    let rows = inner.store.find_api_keys_by_hash(&h).await?;

    for row in rows {
        if row.revoked_at.is_some() {
            continue;
        }
        if password::verify(raw, &row.key_bcrypt)? {
            // Best-effort touch; a failed UPDATE here must not deny auth.
            let _ = inner.store.touch_api_key(row.id).await;
            let tenant: TenantId = row
                .tenant_id
                .parse()
                .map_err(|e| BasinError::internal(format!("api_key tenant parse: {e}")))?;
            return Ok((tenant, row.user_id));
        }
    }
    Err(BasinError::InvalidIdent("invalid api key".into()))
}

pub(crate) async fn revoke(inner: &Inner, key_id: i64, tenant: &TenantId) -> Result<()> {
    inner.store.revoke_api_key(tenant, key_id).await
}

pub(crate) async fn list(
    inner: &Inner,
    tenant: &TenantId,
    user: UserId,
) -> Result<Vec<ApiKeyDescriptor>> {
    inner.store.list_api_keys(tenant, user).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::TenantId;

    #[test]
    fn key_format_contains_tenant() {
        let tenant = TenantId::new();
        let secret = generate_secret(&tenant);
        assert!(secret.starts_with("basin_"));
        let tenant_str = tenant.to_string();
        assert!(secret.contains(&tenant_str));
    }

    #[test]
    fn parse_tenant_from_api_key_round_trips() {
        let tenant = TenantId::new();
        let secret = generate_secret(&tenant);
        let parsed = parse_tenant_from_api_key(&secret).expect("should parse");
        assert_eq!(parsed, tenant.to_string());
    }

    #[test]
    fn parse_tenant_from_api_key_rejects_wrong_format() {
        assert!(parse_tenant_from_api_key("notakey").is_none());
        assert!(parse_tenant_from_api_key("basin_short").is_none());
        assert!(parse_tenant_from_api_key("").is_none());
    }

    // --- parse_tenant_from_api_key: comprehensive cases ---------------------

    #[test]
    fn parse_tenant_valid_full_key() {
        // basin_{26-char ulid}_{base64 random} → Some(ulid)
        let tenant = TenantId::new();
        let tenant_str = tenant.to_string();
        let secret = format!("basin_{tenant_str}_somebase64stuff");
        let parsed = parse_tenant_from_api_key(&secret).expect("must parse");
        assert_eq!(parsed, tenant_str.as_str());
    }

    #[test]
    fn parse_tenant_wrong_prefix_returns_none() {
        let tenant = TenantId::new();
        let tenant_str = tenant.to_string();
        // Prefix is "notbasin_" instead of "basin_"
        let bad = format!("notbasin_{tenant_str}_extra");
        assert!(parse_tenant_from_api_key(&bad).is_none());
        // No prefix at all
        assert!(parse_tenant_from_api_key(&format!("{tenant_str}_extra")).is_none());
    }

    #[test]
    fn parse_tenant_missing_random_suffix_returns_none() {
        let tenant = TenantId::new();
        // basin_ + 26-char ulid + _ but no payload → len == 6+26+1 = 33; rest.len() == 27
        // rest[26] == '_', but rest.len() is exactly 27 so rest.len() > 27 is false → None
        let no_payload = format!("basin_{}_", tenant);
        assert_eq!(no_payload.len(), 6 + 26 + 1);
        // rest = "{tenant}_" has len 27, which is not > 27 → None
        assert!(parse_tenant_from_api_key(&no_payload).is_none());
    }

    #[test]
    fn parse_tenant_short_after_basin_prefix_returns_none() {
        assert!(parse_tenant_from_api_key("basin_shortkey").is_none());
        assert!(parse_tenant_from_api_key("basin_").is_none());
    }

    #[test]
    fn parse_tenant_empty_string_returns_none() {
        assert!(parse_tenant_from_api_key("").is_none());
    }

    #[test]
    fn parse_tenant_self_routing_invariant() {
        // Every key generated by `generate_secret` must parse back to the
        // tenant it was generated for, regardless of the random payload.
        for _ in 0..20 {
            let tenant = TenantId::new();
            let secret = generate_secret(&tenant);
            let parsed =
                parse_tenant_from_api_key(&secret).expect("generated secret must always parse");
            assert_eq!(
                parsed,
                tenant.to_string().as_str(),
                "self-routing invariant: secret={secret:?} parsed={parsed:?}"
            );
        }
    }

    #[test]
    fn key_hash_is_deterministic() {
        let raw = "basin_01JBAS1NAVTH00000000000000_abcdef";
        assert_eq!(key_hash(raw), key_hash(raw));
    }

    #[test]
    fn key_hash_differs_for_different_inputs() {
        let h1 = key_hash("basin_01JBAS1NAVTH00000000000000_aaa");
        let h2 = key_hash("basin_01JBAS1NAVTH00000000000000_bbb");
        assert_ne!(h1, h2);
    }

    #[test]
    fn generate_secret_random_suffix_is_url_safe() {
        // The random part is URL_SAFE_NO_PAD base64: [A-Za-z0-9\-_] only.
        let tenant = TenantId::new();
        let secret = generate_secret(&tenant);
        // Skip "basin_{tenant}_" prefix (6 + 26 + 1 = 33 chars)
        let random_part = &secret[33..];
        assert!(!random_part.is_empty(), "random suffix must not be empty");
        for c in random_part.chars() {
            assert!(
                c.is_ascii_alphanumeric() || c == '-' || c == '_',
                "random part contains non-URL-safe char {c:?}"
            );
        }
    }
}
