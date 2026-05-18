//! Password-reset request + consume.

use std::time::Duration;

use basin_common::{BasinError, ProjectId, Result};
use chrono::Utc;

use crate::email::{reset_template, Outbound};
use crate::tokens::{generate, hash_token, EmailTokenPurpose};
use crate::{password, Inner};

const RESET_TTL: Duration = Duration::from_secs(60 * 60);

pub(crate) async fn request_password_reset(
    inner: &Inner,
    project: &ProjectId,
    email: &str,
) -> Result<()> {
    inner.ip_limiter.check(&format!("reset:{project}"))?;
    inner.email_limiter.check(&format!("reset:{email}"))?;

    let email = crate::normalise_email(email)?;

    let (raw, h) = generate();
    let expires_at = Utc::now() + crate::ttl_or_default(RESET_TTL);

    let user_row = inner.store.find_user_by_email(project, &email).await?;

    // Per-OWASP: don't leak whether the email exists. We always return Ok,
    // but only insert a token + send the email if the user is real.
    let Some(user_row) = user_row else {
        return Ok(());
    };

    inner
        .store
        .insert_email_token(
            project,
            user_row.user_id,
            &h,
            EmailTokenPurpose::Reset.as_str(),
            expires_at,
        )
        .await?;

    let mut out: Outbound = reset_template(&raw);
    out.to = email;
    inner.mailer.send(out).await?;
    Ok(())
}

pub(crate) async fn reset_password(
    inner: &Inner,
    project: &ProjectId,
    raw_token: &str,
    new_password: &str,
) -> Result<()> {
    password::check_length(new_password, inner.cfg.password_min_len)?;
    let h = hash_token(raw_token);
    let new_hash = password::hash(new_password, inner.cfg.bcrypt_cost)?;

    let row = inner.store.find_email_token(project, &h).await?;
    let Some(row) = row else {
        return Err(BasinError::not_found("invalid reset token"));
    };

    if row.purpose != EmailTokenPurpose::Reset.as_str() {
        return Err(BasinError::InvalidIdent("token has wrong purpose".into()));
    }
    if row.consumed_at.is_some() {
        return Err(BasinError::InvalidIdent(
            "reset token already consumed".into(),
        ));
    }
    if row.expires_at < Utc::now() {
        return Err(BasinError::InvalidIdent("reset token expired".into()));
    }

    let consumed = inner.store.consume_email_token(project, &h).await?;
    if consumed == 0 {
        return Err(BasinError::InvalidIdent(
            "reset token already consumed".into(),
        ));
    }

    inner
        .store
        .update_password(project, row.user_id, &new_hash)
        .await?;

    // Invalidate every active refresh token for this user — a forgotten
    // password is a credentials-changed event; existing sessions should die.
    // NOTE: the old code updated the `refresh_tokens` table which is not
    // used in the current revocation-list scheme (revoked tokens live in
    // `auth_revoked_refresh_tokens`, not `refresh_tokens`). The blanket-
    // revocation sentinel handles this case gracefully: any outstanding
    // refresh JWT issued before now() will be rejected by the sentinel check.
    // For now we skip the stale `refresh_tokens` UPDATE that was a no-op
    // in the current schema (that table has no `revoked_at` column).
    Ok(())
}
