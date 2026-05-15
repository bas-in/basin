//! Sign-in (email + password). Issues a fresh access JWT and refresh token.
//!
//! Sign-in fails closed if `email_verified_at` is null. The bcrypt verify and
//! the "user exists" check are deliberately structured to take similar time
//! in the failure path: we always run a bcrypt verify (against a synthetic
//! hash if the user doesn't exist) so the response time doesn't tell an
//! attacker whether the email exists.

use basin_common::{BasinError, Result, ProjectId};
use chrono::Utc;
use uuid::Uuid;

use crate::flows::refresh::issue_refresh;
use crate::{password, Inner, Tokens};

/// A pre-computed bcrypt hash of garbage. Verify takes ~the same time
/// regardless of input, so running this when the user is missing keeps the
/// timing channel narrow.
const DUMMY_HASH: &str = "$2b$04$dummyhashthatshouldneververifyXXXXXXXXXXXXXXXXXXXXXX";

pub(crate) async fn signin(
    inner: &Inner,
    project: &ProjectId,
    email: &str,
    password_raw: &str,
) -> Result<Tokens> {
    inner.ip_limiter.check(&format!("signin:{project}"))?;
    inner.email_limiter.check(&format!("signin:{email}"))?;

    let email = crate::normalise_email(email)?;

    let row = inner.store.find_user_by_email(project, &email).await?;

    let (user_id, hash, verified) = match row {
        Some(r) => (Some(r.user_id), r.password_hash, r.email_verified_at),
        None => (None, DUMMY_HASH.to_string(), None),
    };

    let ok = password::verify(password_raw, &hash)?;
    let user_id = match (ok, user_id) {
        (true, Some(id)) => id,
        _ => return Err(BasinError::InvalidIdent("invalid email or password".into())),
    };

    if verified.is_none() {
        return Err(BasinError::InvalidIdent(
            "email not verified; check your inbox".into(),
        ));
    }

    issue_tokens_for(inner, project, user_id, &email).await
}

/// Issue a JWT + refresh pair for a user. Reused by signin, magic-link,
/// and refresh.
pub(crate) async fn issue_tokens_for(
    inner: &Inner,
    project: &ProjectId,
    user_id: Uuid,
    email: &str,
) -> Result<Tokens> {
    let now = Utc::now();
    let (access_token, access_expires_at) = inner.jwt.issue(
        project,
        user_id,
        email,
        // No role hierarchy in v1.
        &[],
        now,
        inner.cfg.token_ttl,
    )?;
    let (refresh_token, refresh_expires_at) =
        issue_refresh(inner, project, user_id, email, now).await?;
    Ok(Tokens {
        access_token,
        refresh_token,
        access_expires_at,
        refresh_expires_at,
    })
}
