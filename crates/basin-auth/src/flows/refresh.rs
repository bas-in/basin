//! Refresh-token issue, rotation, and sign-out (revocation).
//!
//! Rotation: every successful `refresh` revokes the presented refresh token
//! and issues a new one. A second use of the same refresh fails — the
//! original `revoked_at` is set, so the row is still in the table but won't
//! verify.

use basin_common::{BasinError, Result, TenantId};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::flows::signin::issue_tokens_for;
use crate::tokens::{generate, hash_token};
use crate::{Inner, Tokens};

/// Insert a fresh refresh-token row and return `(raw, expires_at)`. The DB
/// stores only the sha256 hash; the raw goes back to the user.
pub(crate) async fn issue_refresh(
    inner: &Inner,
    tenant: &TenantId,
    user_id: Uuid,
    now: DateTime<Utc>,
) -> Result<(String, DateTime<Utc>)> {
    let (raw, h) = generate();
    let expires_at = now + crate::ttl_or_default(inner.cfg.refresh_ttl);
    let tenant_str = tenant.to_string();

    let client = inner.client.lock().await;
    client
        .execute(
            &format!(
                "INSERT INTO {sch}.refresh_tokens
                   (token_hash, user_id, tenant_id, expires_at)
                 VALUES ($1, $2, $3, $4)",
                sch = inner.schema()
            ),
            &[&h, &user_id, &tenant_str, &expires_at],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("refresh insert: {e}")))?;
    Ok((raw, expires_at))
}

pub(crate) async fn refresh(inner: &Inner, raw: &str) -> Result<Tokens> {
    let h = hash_token(raw);

    let mut client = inner.client.lock().await;
    let tx = client
        .transaction()
        .await
        .map_err(|e| BasinError::catalog(format!("refresh begin: {e}")))?;

    let row = tx
        .query_opt(
            &format!(
                "SELECT user_id, tenant_id, expires_at, revoked_at
                 FROM {sch}.refresh_tokens
                 WHERE token_hash = $1
                 FOR UPDATE",
                sch = inner.schema()
            ),
            &[&h],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("refresh select: {e}")))?;
    let Some(row) = row else {
        return Err(BasinError::InvalidIdent("invalid refresh token".into()));
    };
    let user_id: Uuid = row.get(0);
    let tenant_str: String = row.get(1);
    let expires_at: DateTime<Utc> = row.get(2);
    let revoked_at: Option<DateTime<Utc>> = row.get(3);

    if revoked_at.is_some() {
        return Err(BasinError::InvalidIdent(
            "refresh token revoked".into(),
        ));
    }
    if expires_at < Utc::now() {
        return Err(BasinError::InvalidIdent("refresh token expired".into()));
    }

    // Mark this row revoked so a replay is caught even if the new pair is
    // never used.
    tx.execute(
        &format!(
            "UPDATE {sch}.refresh_tokens SET revoked_at = now()
             WHERE token_hash = $1",
            sch = inner.schema()
        ),
        &[&h],
    )
    .await
    .map_err(|e| BasinError::catalog(format!("refresh revoke old: {e}")))?;

    // Look up the email for the JWT claims.
    let email_row = tx
        .query_one(
            &format!(
                "SELECT email FROM {sch}.users WHERE user_id = $1",
                sch = inner.schema()
            ),
            &[&user_id],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("refresh user fetch: {e}")))?;
    let email: String = email_row.get(0);

    tx.commit()
        .await
        .map_err(|e| BasinError::catalog(format!("refresh commit: {e}")))?;
    drop(client);

    let tenant: TenantId = tenant_str
        .parse()
        .map_err(|e| BasinError::internal(format!("refresh parse tenant: {e}")))?;
    issue_tokens_for(inner, &tenant, user_id, &email).await
}

pub(crate) async fn signout(inner: &Inner, raw: &str) -> Result<()> {
    let h = hash_token(raw);
    let client = inner.client.lock().await;
    let n = client
        .execute(
            &format!(
                "UPDATE {sch}.refresh_tokens SET revoked_at = now()
                 WHERE token_hash = $1 AND revoked_at IS NULL",
                sch = inner.schema()
            ),
            &[&h],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("signout update: {e}")))?;
    if n == 0 {
        // Per-OWASP: don't tell the caller whether the token existed; either
        // way the user is signed out from this client's perspective.
        tracing::debug!("signout no-op (already revoked or unknown token)");
    }
    Ok(())
}
