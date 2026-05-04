//! Password-reset request + consume.

use std::time::Duration;

use basin_common::{BasinError, Result, TenantId};
use chrono::Utc;
use uuid::Uuid;

use crate::email::{reset_template, Outbound};
use crate::tokens::{generate, hash_token, EmailTokenPurpose};
use crate::{password, Inner};

const RESET_TTL: Duration = Duration::from_secs(60 * 60);

pub(crate) async fn request_password_reset(
    inner: &Inner,
    tenant: &TenantId,
    email: &str,
) -> Result<()> {
    inner.ip_limiter.check(&format!("reset:{tenant}"))?;
    inner.email_limiter.check(&format!("reset:{email}"))?;

    let email = crate::normalise_email(email)?;
    let tenant_str = tenant.to_string();

    let (raw, h) = generate();
    let expires_at = Utc::now() + crate::ttl_or_default(RESET_TTL);

    let client = inner.client.lock().await;
    let row = client
        .query_opt(
            &format!(
                "SELECT user_id FROM {sch}.users
                 WHERE tenant_id = $1 AND email = $2",
                sch = inner.schema()
            ),
            &[&tenant_str, &email],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("reset lookup: {e}")))?;

    // Per-OWASP: don't leak whether the email exists. We always return Ok,
    // but only insert a token + send the email if the user is real.
    let Some(row) = row else {
        return Ok(());
    };
    let user_id: Uuid = row.get(0);

    client
        .execute(
            &format!(
                "INSERT INTO {sch}.email_tokens
                   (token_hash, user_id, tenant_id, purpose, expires_at)
                 VALUES ($1, $2, $3, $4, $5)",
                sch = inner.schema()
            ),
            &[
                &h,
                &user_id,
                &tenant_str,
                &EmailTokenPurpose::Reset.as_str(),
                &expires_at,
            ],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("reset insert: {e}")))?;
    drop(client);

    let mut out: Outbound = reset_template(&raw);
    out.to = email;
    inner.mailer.send(out).await?;
    Ok(())
}

pub(crate) async fn reset_password(
    inner: &Inner,
    tenant: &TenantId,
    raw_token: &str,
    new_password: &str,
) -> Result<()> {
    password::check_length(new_password, inner.cfg.password_min_len)?;
    let h = hash_token(raw_token);
    let tenant_str = tenant.to_string();
    let new_hash = password::hash(new_password, inner.cfg.bcrypt_cost)?;

    let mut client = inner.client.lock().await;
    let tx = client
        .transaction()
        .await
        .map_err(|e| BasinError::catalog(format!("reset begin: {e}")))?;

    let row = tx
        .query_opt(
            &format!(
                "SELECT user_id, expires_at, consumed_at, purpose
                 FROM {sch}.email_tokens
                 WHERE token_hash = $1 AND tenant_id = $2
                 FOR UPDATE",
                sch = inner.schema()
            ),
            &[&h, &tenant_str],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("reset select: {e}")))?;
    let Some(row) = row else {
        return Err(BasinError::not_found("invalid reset token"));
    };
    let user_id: Uuid = row.get(0);
    let expires_at: chrono::DateTime<Utc> = row.get(1);
    let consumed_at: Option<chrono::DateTime<Utc>> = row.get(2);
    let purpose: String = row.get(3);

    if purpose != EmailTokenPurpose::Reset.as_str() {
        return Err(BasinError::InvalidIdent("token has wrong purpose".into()));
    }
    if consumed_at.is_some() {
        return Err(BasinError::InvalidIdent("reset token already consumed".into()));
    }
    if expires_at < Utc::now() {
        return Err(BasinError::InvalidIdent("reset token expired".into()));
    }

    tx.execute(
        &format!(
            "UPDATE {sch}.email_tokens SET consumed_at = now()
             WHERE token_hash = $1",
            sch = inner.schema()
        ),
        &[&h],
    )
    .await
    .map_err(|e| BasinError::catalog(format!("reset mark consumed: {e}")))?;

    tx.execute(
        &format!(
            "UPDATE {sch}.users SET password_hash = $1
             WHERE user_id = $2 AND tenant_id = $3",
            sch = inner.schema()
        ),
        &[&new_hash, &user_id, &tenant_str],
    )
    .await
    .map_err(|e| BasinError::catalog(format!("reset update password: {e}")))?;

    // Invalidate every active refresh token for this user — a forgotten
    // password is a credentials-changed event; existing sessions should die.
    tx.execute(
        &format!(
            "UPDATE {sch}.refresh_tokens SET revoked_at = now()
             WHERE user_id = $1 AND revoked_at IS NULL",
            sch = inner.schema()
        ),
        &[&user_id],
    )
    .await
    .map_err(|e| BasinError::catalog(format!("reset revoke refresh: {e}")))?;

    tx.commit()
        .await
        .map_err(|e| BasinError::catalog(format!("reset commit: {e}")))?;
    Ok(())
}
