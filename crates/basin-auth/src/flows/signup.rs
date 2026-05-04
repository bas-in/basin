//! Signup, email-verification request, email-verification consume.
//!
//! State machine:
//!
//! 1. `signup` — insert into `users` with `email_verified_at = NULL`.
//! 2. `request_email_verification` — generate token, persist hash, send email.
//! 3. `verify_email` — look up by hash, mark `consumed_at`, set
//!    `email_verified_at` on the user row.
//!
//! Sign-in is gated on `email_verified_at IS NOT NULL` — see `signin.rs`.

use std::time::Duration;

use basin_common::{BasinError, Result, TenantId};
use chrono::Utc;
use uuid::Uuid;

use crate::email::{verify_template, Outbound};
use crate::tokens::{generate, hash_token, EmailTokenPurpose};
use crate::{password, Inner, UserId};

const VERIFY_TTL: Duration = Duration::from_secs(60 * 60 * 24);

pub(crate) async fn signup(
    inner: &Inner,
    tenant: &TenantId,
    email: &str,
    password_raw: &str,
) -> Result<UserId> {
    inner.ip_limiter.check(&format!("signup:{tenant}"))?;
    inner.email_limiter.check(&format!("signup:{email}"))?;

    let email = crate::normalise_email(email)?;
    password::check_length(password_raw, inner.cfg.password_min_len)?;
    let hashed = password::hash(password_raw, inner.cfg.bcrypt_cost)?;

    let user_id = Uuid::new_v4();
    let tenant_str = tenant.to_string();

    let client = inner.client.lock().await;
    let inserted = client
        .execute(
            &format!(
                "INSERT INTO {sch}.users (user_id, tenant_id, email, password_hash)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (tenant_id, email) DO NOTHING",
                sch = inner.schema()
            ),
            &[&user_id, &tenant_str, &email, &hashed],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("signup insert: {e}")))?;
    if inserted == 0 {
        return Err(BasinError::CommitConflict(format!(
            "user {email} already exists for tenant {tenant}"
        )));
    }
    Ok(user_id)
}

pub(crate) async fn request_email_verification(
    inner: &Inner,
    tenant: &TenantId,
    user: UserId,
) -> Result<()> {
    let (raw, hash) = generate();
    let expires_at = Utc::now() + crate::ttl_or_default(VERIFY_TTL);
    let tenant_str = tenant.to_string();

    let client = inner.client.lock().await;
    // Pull email so we know where to send it. Fail closed if user is missing.
    let row = client
        .query_opt(
            &format!(
                "SELECT email FROM {sch}.users
                 WHERE user_id = $1 AND tenant_id = $2",
                sch = inner.schema()
            ),
            &[&user, &tenant_str],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("verify lookup: {e}")))?;
    let Some(row) = row else {
        return Err(BasinError::not_found(format!("user {user}")));
    };
    let email: String = row.get(0);

    client
        .execute(
            &format!(
                "INSERT INTO {sch}.email_tokens
                   (token_hash, user_id, tenant_id, purpose, expires_at)
                 VALUES ($1, $2, $3, $4, $5)",
                sch = inner.schema()
            ),
            &[
                &hash,
                &user,
                &tenant_str,
                &EmailTokenPurpose::Verify.as_str(),
                &expires_at,
            ],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("verify insert: {e}")))?;
    drop(client);

    let mut out: Outbound = verify_template(&raw);
    out.to = email;
    inner.mailer.send(out).await?;
    Ok(())
}

pub(crate) async fn verify_email(
    inner: &Inner,
    tenant: &TenantId,
    raw_token: &str,
) -> Result<()> {
    let h = hash_token(raw_token);
    let tenant_str = tenant.to_string();

    let mut client = inner.client.lock().await;
    let tx = client
        .transaction()
        .await
        .map_err(|e| BasinError::catalog(format!("verify begin: {e}")))?;

    // Single round-trip: fetch the row and lock it. If consumed, expired, or
    // wrong purpose / tenant, fail without touching anything.
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
        .map_err(|e| BasinError::catalog(format!("verify select: {e}")))?;
    let Some(row) = row else {
        return Err(BasinError::not_found("invalid verification token"));
    };
    let user_id: Uuid = row.get(0);
    let expires_at: chrono::DateTime<Utc> = row.get(1);
    let consumed_at: Option<chrono::DateTime<Utc>> = row.get(2);
    let purpose: String = row.get(3);

    if purpose != EmailTokenPurpose::Verify.as_str() {
        return Err(BasinError::InvalidIdent(
            "token has wrong purpose".into(),
        ));
    }
    if consumed_at.is_some() {
        return Err(BasinError::InvalidIdent(
            "verification token already consumed".into(),
        ));
    }
    if expires_at < Utc::now() {
        return Err(BasinError::InvalidIdent(
            "verification token expired".into(),
        ));
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
    .map_err(|e| BasinError::catalog(format!("verify mark consumed: {e}")))?;

    tx.execute(
        &format!(
            "UPDATE {sch}.users SET email_verified_at = now()
             WHERE user_id = $1 AND tenant_id = $2",
            sch = inner.schema()
        ),
        &[&user_id, &tenant_str],
    )
    .await
    .map_err(|e| BasinError::catalog(format!("verify mark user: {e}")))?;

    tx.commit()
        .await
        .map_err(|e| BasinError::catalog(format!("verify commit: {e}")))?;
    Ok(())
}
