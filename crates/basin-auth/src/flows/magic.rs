//! Magic-link sign-in.
//!
//! Same shape as password reset: request issues a one-time token (15-minute
//! TTL per ADR), sign-in consumes it. Unlike password reset, the consume
//! returns a fresh `Tokens` pair rather than just changing state.

use std::time::Duration;

use basin_common::{BasinError, Result, TenantId};
use chrono::Utc;
use uuid::Uuid;

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
    let tenant_str = tenant.to_string();

    let (raw, h) = generate();
    let expires_at = Utc::now() + crate::ttl_or_default(MAGIC_TTL);

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
        .map_err(|e| BasinError::catalog(format!("magic lookup: {e}")))?;
    let Some(row) = row else {
        // Same OWASP-friendly silence as the reset flow.
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
                &EmailTokenPurpose::MagicLink.as_str(),
                &expires_at,
            ],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("magic insert: {e}")))?;
    drop(client);

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
    let tenant_str = tenant.to_string();

    let mut client = inner.client.lock().await;
    let tx = client
        .transaction()
        .await
        .map_err(|e| BasinError::catalog(format!("magic begin: {e}")))?;

    let row = tx
        .query_opt(
            &format!(
                "SELECT et.user_id, et.expires_at, et.consumed_at, et.purpose, u.email
                 FROM {sch}.email_tokens et
                 JOIN {sch}.users u
                   ON u.user_id = et.user_id AND u.tenant_id = et.tenant_id
                 WHERE et.token_hash = $1 AND et.tenant_id = $2
                 FOR UPDATE OF et",
                sch = inner.schema()
            ),
            &[&h, &tenant_str],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("magic select: {e}")))?;
    let Some(row) = row else {
        return Err(BasinError::not_found("invalid magic-link token"));
    };
    let user_id: Uuid = row.get(0);
    let expires_at: chrono::DateTime<Utc> = row.get(1);
    let consumed_at: Option<chrono::DateTime<Utc>> = row.get(2);
    let purpose: String = row.get(3);
    let email: String = row.get(4);

    if purpose != EmailTokenPurpose::MagicLink.as_str() {
        return Err(BasinError::InvalidIdent("token has wrong purpose".into()));
    }
    if consumed_at.is_some() {
        return Err(BasinError::InvalidIdent(
            "magic-link token already consumed".into(),
        ));
    }
    if expires_at < Utc::now() {
        return Err(BasinError::InvalidIdent("magic-link token expired".into()));
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
    .map_err(|e| BasinError::catalog(format!("magic mark consumed: {e}")))?;

    // Magic-link sign-in is implicitly an email-verification event — possessing
    // the inbox is the proof. Mark the user verified if they weren't already.
    tx.execute(
        &format!(
            "UPDATE {sch}.users SET email_verified_at = COALESCE(email_verified_at, now())
             WHERE user_id = $1 AND tenant_id = $2",
            sch = inner.schema()
        ),
        &[&user_id, &tenant_str],
    )
    .await
    .map_err(|e| BasinError::catalog(format!("magic mark verified: {e}")))?;

    tx.commit()
        .await
        .map_err(|e| BasinError::catalog(format!("magic commit: {e}")))?;
    drop(client);

    issue_tokens_for(inner, tenant, user_id, &email).await
}
