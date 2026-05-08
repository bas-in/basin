//! Tenant-agnostic email-link login.
//!
//! Distinct from the per-tenant `flows::magic` flow: the request body has
//! only `email`, and we resolve the user at consume time. The bcrypt-hashed
//! token row sits in `auth_magic_links` with no `user_id` — the spec keeps
//! the table tenant- and user-blind.
//!
//! Flow:
//!   1. POST `/auth/v1/magic-link` body `{"email"}` → 204 always (no user-
//!      enumeration leak). If a user exists, we insert a row + send mail.
//!   2. POST `/auth/v1/magic-link/consume` body `{"token"}` → tokens.
//!
//! Multi-tenant note: `users` is unique on `(tenant_id, email)`, so the same
//! address can exist in many tenants. v0.1 picks the most-recently-created
//! user matching the email at consume time. This is good enough for the
//! single-tenant-per-user wedge; multi-tenant disambiguation is a follow-up.

use std::time::Duration;

use basin_common::{BasinError, Result};
use chrono::Utc;
use rand::RngCore;
use uuid::Uuid;

use crate::email::{magic_link_template, Outbound};
use crate::flows::signin::issue_tokens_for;
use crate::{password, Inner, Tokens};

const MAGIC_LINK_TTL: Duration = Duration::from_secs(60 * 15);

/// 32 bytes = 256 bits of entropy, hex-encoded for copy/paste.
const MAGIC_LINK_BYTES: usize = 32;

fn generate_raw() -> String {
    let mut buf = [0u8; MAGIC_LINK_BYTES];
    rand::thread_rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

/// Issue a new magic link for `email`. Always returns Ok — a missing email
/// is silently dropped to defeat user-enumeration probes.
pub(crate) async fn request(inner: &Inner, email_raw: &str) -> Result<()> {
    if !inner.cfg.is_email_enabled() {
        return Err(BasinError::Internal("E_EMAIL_DISABLED".into()));
    }

    let email = crate::normalise_email(email_raw)?;
    inner.ip_limiter.check(&format!("magic_link:{email}"))?;
    inner.email_limiter.check(&format!("magic_link:{email}"))?;

    let raw = generate_raw();
    let bcrypted = password::hash(&raw, inner.cfg.bcrypt_cost)?;
    let expires_at = Utc::now() + crate::ttl_or_default(MAGIC_LINK_TTL);

    let client = inner.client.lock().await;
    // Only insert if a user with this email actually exists somewhere — but
    // we never tell the caller either way (204 + silence per OWASP).
    let any_user = client
        .query_opt(
            &format!(
                "SELECT 1 FROM {sch}.users WHERE email = $1 LIMIT 1",
                sch = inner.schema()
            ),
            &[&email],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("magic_link user check: {e}")))?;
    if any_user.is_none() {
        return Ok(());
    }

    client
        .execute(
            &format!(
                "INSERT INTO {sch}.auth_magic_links (email, token_hash, expires_at)
                 VALUES ($1, $2, $3)",
                sch = inner.schema()
            ),
            &[&email, &bcrypted, &expires_at],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("magic_link insert: {e}")))?;
    drop(client);

    let mut out: Outbound = magic_link_template(&raw);
    out.to = email;
    inner.mailer.send(out).await?;
    Ok(())
}

/// Consume a magic-link token. Single-use: this marks the row consumed and
/// returns a fresh token pair for the user we resolve from `email`.
pub(crate) async fn consume(inner: &Inner, raw_token: &str) -> Result<Tokens> {
    let raw_token = raw_token.trim();
    if raw_token.is_empty() {
        return Err(BasinError::InvalidIdent("magic-link token is empty".into()));
    }

    let mut client = inner.client.lock().await;
    let tx = client
        .transaction()
        .await
        .map_err(|e| BasinError::catalog(format!("magic_link begin: {e}")))?;

    // bcrypt-verify is too expensive for index lookup. Instead: scan the
    // small set of unconsumed, unexpired rows and bcrypt-verify each.
    // 15-minute TTL keeps the working set tiny for normal traffic.
    let candidates = tx
        .query(
            &format!(
                "SELECT id, email, token_hash, expires_at, consumed_at
                 FROM {sch}.auth_magic_links
                 WHERE consumed_at IS NULL
                   AND expires_at > now()
                 FOR UPDATE",
                sch = inner.schema()
            ),
            &[],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("magic_link scan: {e}")))?;

    let mut hit: Option<(i64, String)> = None;
    for row in candidates {
        let id: i64 = row.get(0);
        let email: String = row.get(1);
        let h: String = row.get(2);
        if password::verify(raw_token, &h)? {
            hit = Some((id, email));
            break;
        }
    }
    let Some((id, email)) = hit else {
        return Err(BasinError::InvalidIdent("invalid magic-link token".into()));
    };

    tx.execute(
        &format!(
            "UPDATE {sch}.auth_magic_links SET consumed_at = now() WHERE id = $1",
            sch = inner.schema()
        ),
        &[&id],
    )
    .await
    .map_err(|e| BasinError::catalog(format!("magic_link consume: {e}")))?;

    // v0.1: pick the most-recently-created user with this email. See the
    // module comment on the multi-tenant follow-up.
    let user_row = tx
        .query_opt(
            &format!(
                "SELECT user_id, tenant_id FROM {sch}.users
                 WHERE email = $1
                 ORDER BY created_at DESC
                 LIMIT 1",
                sch = inner.schema()
            ),
            &[&email],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("magic_link user lookup: {e}")))?;
    let Some(row) = user_row else {
        return Err(BasinError::not_found("user for magic link"));
    };
    let user_id: Uuid = row.get(0);
    let tenant_str: String = row.get(1);

    // Possessing the inbox is also proof of email control — fold the
    // verification timestamp in.
    tx.execute(
        &format!(
            "UPDATE {sch}.users
             SET email_verified_at = COALESCE(email_verified_at, now())
             WHERE user_id = $1",
            sch = inner.schema()
        ),
        &[&user_id],
    )
    .await
    .map_err(|e| BasinError::catalog(format!("magic_link mark verified: {e}")))?;

    tx.commit()
        .await
        .map_err(|e| BasinError::catalog(format!("magic_link commit: {e}")))?;
    drop(client);

    let tenant = tenant_str
        .parse()
        .map_err(|e| BasinError::internal(format!("magic_link tenant parse: {e}")))?;
    issue_tokens_for(inner, &tenant, user_id, &email).await
}

/// Helper used by tests to drain the most recently-issued raw token from the
/// stub mailer. Lives here so the caller doesn't have to know the email
/// template's wire format.
#[cfg(test)]
pub(crate) fn extract_raw_token_from_body(body: &str) -> Option<String> {
    let needle = "token=";
    let start = body.find(needle)? + needle.len();
    let tail = &body[start..];
    let end = tail
        .find(|c: char| !c.is_ascii_hexdigit())
        .unwrap_or(tail.len());
    Some(tail[..end].to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_raw_token_pulls_hex() {
        let body = "Click https://x/auth/magic?token=deadbeef\nbla";
        assert_eq!(
            extract_raw_token_from_body(body).as_deref(),
            Some("deadbeef")
        );
    }

    #[test]
    fn generate_raw_is_hex() {
        let raw = generate_raw();
        assert_eq!(raw.len(), MAGIC_LINK_BYTES * 2);
        assert!(raw.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
