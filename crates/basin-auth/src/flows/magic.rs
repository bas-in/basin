//! Magic-link sign-in.
//!
//! Same shape as password reset: request issues a one-time token (15-minute
//! TTL per ADR), sign-in consumes it. Unlike password reset, the consume
//! returns a fresh `Tokens` pair rather than just changing state.

use std::time::Duration;

use basin_common::{BasinError, Result, TenantId};
use chrono::Utc;

use crate::email::{magic_link_template, Outbound};
use crate::flows::signin::issue_tokens_for;
use crate::tokens::{generate, hash_token, EmailTokenPurpose};
use crate::{Inner, Tokens};

const MAGIC_TTL: Duration = Duration::from_secs(60 * 15);

pub(crate) async fn request_magic_link(
    inner: &Inner,
    tenant: &TenantId,
    email: &str,
) -> Result<()> {
    inner.ip_limiter.check(&format!("magic:{tenant}"))?;
    inner.email_limiter.check(&format!("magic:{email}"))?;

    let email = crate::normalise_email(email)?;

    let (raw, h) = generate();
    let expires_at = Utc::now() + crate::ttl_or_default(MAGIC_TTL);

    let user_row = inner.store.find_user_by_email(tenant, &email).await?;
    let Some(user_row) = user_row else {
        // Same OWASP-friendly silence as the reset flow.
        return Ok(());
    };

    inner
        .store
        .insert_email_token(
            tenant,
            user_row.user_id,
            &h,
            EmailTokenPurpose::MagicLink.as_str(),
            expires_at,
        )
        .await?;

    let mut out: Outbound = magic_link_template(&raw);
    out.to = email;
    inner.mailer.send(out).await?;
    Ok(())
}

pub(crate) async fn signin_with_magic_link(
    inner: &Inner,
    tenant: &TenantId,
    raw_token: &str,
) -> Result<Tokens> {
    let h = hash_token(raw_token);

    let row = inner.store.find_magic_link_email_token(tenant, &h).await?;
    let Some(row) = row else {
        return Err(BasinError::not_found("invalid magic-link token"));
    };

    if row.purpose != EmailTokenPurpose::MagicLink.as_str() {
        return Err(BasinError::InvalidIdent("token has wrong purpose".into()));
    }
    if row.consumed_at.is_some() {
        return Err(BasinError::InvalidIdent(
            "magic-link token already consumed".into(),
        ));
    }
    if row.expires_at < Utc::now() {
        return Err(BasinError::InvalidIdent("magic-link token expired".into()));
    }

    let updated = inner.store.consume_email_token(tenant, &h).await?;
    if updated == 0 {
        return Err(BasinError::InvalidIdent(
            "magic-link token already consumed".into(),
        ));
    }

    // Magic-link sign-in is implicitly an email-verification event — possessing
    // the inbox is the proof. Mark the user verified if they weren't already.
    inner
        .store
        .mark_email_verified_if_null(tenant, row.user_id)
        .await?;

    issue_tokens_for(inner, tenant, row.user_id, &row.email).await
}
