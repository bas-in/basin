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

use basin_common::{BasinError, Result, ProjectId};
use chrono::Utc;
use uuid::Uuid;

use crate::email::{verify_template, Outbound};
use crate::tokens::{generate, hash_token, EmailTokenPurpose};
use crate::{password, Inner, UserId};

const VERIFY_TTL: Duration = Duration::from_secs(60 * 60 * 24);

pub(crate) async fn signup(
    inner: &Inner,
    project: &ProjectId,
    email: &str,
    password_raw: &str,
) -> Result<UserId> {
    inner.ip_limiter.check(&format!("signup:{project}"))?;
    inner.email_limiter.check(&format!("signup:{email}"))?;

    let email = crate::normalise_email(email)?;
    password::check_length(password_raw, inner.cfg.password_min_len)?;
    let hashed = password::hash(password_raw, inner.cfg.bcrypt_cost)?;

    let user_id = Uuid::new_v4();

    inner
        .store
        .create_user(project, &email, &hashed, user_id)
        .await
}

pub(crate) async fn request_email_verification(
    inner: &Inner,
    project: &ProjectId,
    user: UserId,
) -> Result<()> {
    let (raw, hash) = generate();
    let expires_at = Utc::now() + crate::ttl_or_default(VERIFY_TTL);

    // Pull email so we know where to send it. Fail closed if user is missing.
    let user_row = inner.store.find_user_by_id(project, user).await?;
    let Some(user_row) = user_row else {
        return Err(BasinError::not_found(format!("user {user}")));
    };

    inner
        .store
        .insert_email_token(
            project,
            user,
            &hash,
            EmailTokenPurpose::Verify.as_str(),
            expires_at,
        )
        .await?;

    let mut out: Outbound = verify_template(&raw);
    out.to = user_row.email;
    inner.mailer.send(out).await?;
    Ok(())
}

pub(crate) async fn verify_email(inner: &Inner, project: &ProjectId, raw_token: &str) -> Result<()> {
    let h = hash_token(raw_token);

    let row = inner.store.find_email_token(project, &h).await?;
    let Some(row) = row else {
        return Err(BasinError::not_found("invalid verification token"));
    };

    if row.purpose != EmailTokenPurpose::Verify.as_str() {
        return Err(BasinError::InvalidIdent("token has wrong purpose".into()));
    }
    if row.consumed_at.is_some() {
        return Err(BasinError::InvalidIdent(
            "verification token already consumed".into(),
        ));
    }
    if row.expires_at < Utc::now() {
        return Err(BasinError::InvalidIdent(
            "verification token expired".into(),
        ));
    }

    let consumed = inner.store.consume_email_token(project, &h).await?;
    if consumed == 0 {
        return Err(BasinError::InvalidIdent(
            "verification token already consumed".into(),
        ));
    }

    inner.store.mark_email_verified(project, row.user_id).await?;
    Ok(())
}
