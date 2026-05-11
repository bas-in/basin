//! Refresh-token issue, rotation, sign-out, and reuse detection.
//!
//! Refresh tokens are JWTs with `aud=basin-refresh` and a fresh `jti` per
//! issuance. The plaintext token is never stored — verification is by
//! signature, and revocation is a simple `(jti, user_id, revoked_at,
//! expires_at)` row in `auth_revoked_refresh_tokens`.
//!
//! ## Rotation
//!
//! `refresh()` revokes the presented `jti` and returns a fresh access +
//! refresh pair. A second use of the same refresh JWT therefore lands on
//! a row in the revocation table → 401 with `E_REVOKED_TOKEN`.
//!
//! ## Reuse detection (the security path)
//!
//! If the presented JWT *and* the user has had a *newer* refresh issued
//! since (i.e. there's another revoked-jti row for this user with
//! `revoked_at > self.revoked_at`), the old refresh being re-presented is
//! a strong signal the prior token leaked. We respond by writing a
//! `BLANKET:<user_id>` sentinel row whose `revoked_at = now()`; from then
//! on, `refresh()` rejects any refresh JWT whose `iat` is before this
//! sentinel timestamp. Net effect: every outstanding refresh for the user
//! is invalidated.
//!
//! Sign-out (`signout()`) revokes a single jti without writing a sentinel
//! — there's no peer "newer" jti to imply leak.

use basin_common::{BasinError, Result, TenantId};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::flows::signin::issue_tokens_for;
use crate::{Inner, Tokens};

/// Token-hash prefix for blanket-revoke sentinels. One row per user.
fn blanket_key(user_id: Uuid) -> String {
    format!("BLANKET:{user_id}")
}

/// Insert a fresh refresh-JWT row and return `(jwt, expires_at)`. The
/// service stores nothing at issuance — the only DB writes happen on
/// rotation / signout / blanket-revoke.
pub(crate) async fn issue_refresh(
    inner: &Inner,
    tenant: &TenantId,
    user_id: Uuid,
    email: &str,
    now: DateTime<Utc>,
) -> Result<(String, DateTime<Utc>)> {
    let (jwt, _jti, expires_at) =
        inner
            .jwt
            .issue_refresh(tenant, user_id, email, now, inner.cfg.refresh_ttl)?;
    Ok((jwt, expires_at))
}

pub(crate) async fn refresh(inner: &Inner, raw: &str) -> Result<Tokens> {
    // First, full verify — including the revocation list. A token that's
    // already on the list is the reuse-detection trigger, not a plain 401.
    let claims = match inner.jwt.verify_refresh(raw) {
        Ok(c) => c,
        Err(_) => return Err(BasinError::InvalidIdent("invalid refresh token".into())),
    };

    let exp_dt = DateTime::<Utc>::from_timestamp(claims.exp, 0)
        .ok_or_else(|| BasinError::internal("refresh exp out of range".to_string()))?;
    if exp_dt < Utc::now() {
        return Err(BasinError::InvalidIdent("refresh token expired".into()));
    }

    let blanket = blanket_key(claims.user_id);

    // Pull every revocation row for this user that's still relevant. Two
    // possible hits: an exact-jti revoke (this token was rotated already →
    // reuse), or a blanket sentinel (whole-user revoked).
    let rows = inner.store.list_refresh_revocations(claims.user_id).await?;

    let mut self_revoked_at: Option<DateTime<Utc>> = None;
    let mut blanket_revoked_at: Option<DateTime<Utc>> = None;
    let mut newest_revoked_at: Option<DateTime<Utc>> = None;
    for row in &rows {
        if row.token_hash == claims.jti {
            self_revoked_at = Some(row.revoked_at);
        } else if row.token_hash == blanket {
            blanket_revoked_at = Some(row.revoked_at);
        } else {
            newest_revoked_at = Some(match newest_revoked_at {
                Some(prev) if prev > row.revoked_at => prev,
                _ => row.revoked_at,
            });
        }
    }

    // Blanket revoke active and post-dates this token → revoked.
    if let Some(b) = blanket_revoked_at {
        if claims.iat <= b.timestamp() {
            return Err(BasinError::InvalidIdent("refresh token revoked".into()));
        }
    }

    if let Some(self_ts) = self_revoked_at {
        // This jti was already rotated. Check for *another* revoked row
        // newer than this rotation — implies the legit user has rotated
        // again since, and we're seeing an attacker reuse the leaked old
        // token. Revoke everything.
        let is_reuse = newest_revoked_at.is_some_and(|n| n > self_ts);
        if is_reuse {
            inner
                .store
                .upsert_blanket_revocation(&blanket, claims.user_id, exp_dt)
                .await?;
            tracing::warn!(
                user_id = %claims.user_id,
                "refresh-token reuse detected; blanket-revoking user"
            );
            return Err(BasinError::InvalidIdent("refresh token revoked".into()));
        }
        return Err(BasinError::InvalidIdent("refresh token revoked".into()));
    }

    // Fresh, valid refresh → record this jti as rotated and issue a new pair.
    let inserted = inner
        .store
        .insert_refresh_revocation(&claims.jti, claims.user_id, exp_dt)
        .await?;
    if inserted == 0 {
        return Err(BasinError::InvalidIdent("refresh token revoked".into()));
    }

    issue_tokens_for(inner, &claims.tenant_id, claims.user_id, &claims.email).await
}

pub(crate) async fn signout(inner: &Inner, raw: &str) -> Result<()> {
    // Tolerate a malformed token: the user-visible behaviour is the same
    // either way ("you're signed out"), and we don't want a typo to 500.
    let Ok(claims) = inner.jwt.verify_refresh(raw) else {
        return Ok(());
    };
    let exp_dt = DateTime::<Utc>::from_timestamp(claims.exp, 0)
        .unwrap_or_else(|| Utc::now() + chrono::Duration::days(30));

    inner
        .store
        .upsert_refresh_revocation(&claims.jti, claims.user_id, exp_dt)
        .await?;
    Ok(())
}

/// Rarely-needed: a periodic GC daemon would run this in the background to
/// drop expired-and-no-longer-relevant rows. v0.1 leaves it as a manual
/// admin verb; runtime correctness is unaffected because every read filters
/// `expires_at > now()`.
#[allow(dead_code)]
pub(crate) async fn purge_expired(_inner: &Inner) -> Result<u64> {
    // TODO: expose a purge method on AuthStore when GC is wired up
    Ok(0)
}
