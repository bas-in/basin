//! Auth client — bindings for `/auth/v1/*`.
//!
//! Routes verified against `crates/basin-rest/src/server.rs`:
//! - `POST /auth/v1/signup`                         server.rs:302
//! - `POST /auth/v1/signin`                         server.rs:303
//! - `POST /auth/v1/refresh`                        server.rs:304
//! - `POST /auth/v1/signout`                        server.rs:305
//! - `POST /auth/v1/verify-email`                   server.rs:306
//! - `POST /auth/v1/reset-password`                 server.rs:307
//! - `POST /auth/v1/request-password-reset`         server.rs:309
//! - `POST /auth/v1/magic-link`                     server.rs:315
//! - `POST /auth/v1/magic-link/consume`             server.rs:317
//! - `POST|GET /auth/v1/api-keys`                   server.rs:321
//! - `DELETE /auth/v1/api-keys/:id`                 server.rs:325
//! - `GET /auth/v1/oauth/:provider/authorize`       server.rs:330
//! - `POST|GET /auth/v1/factors`                    server.rs:339
//! - `POST /auth/v1/factors/:id/verify`             server.rs:343
//! - `POST /auth/v1/factors/:id/challenge`          server.rs:347
//! - `POST /auth/v1/factors/:id/challenge/verify`   server.rs:351
//! - `DELETE /auth/v1/factors/:id`                  (server.rs factors delete)
//!
//! Auth is per-project: `signup`/`signin` take a `project_id` in the body.

use std::sync::Arc;

use base64::Engine;
use reqwest::Method;
use serde_json::{json, Value};
use tokio::sync::RwLock;

use crate::error::BasinError;
use crate::http::Transport;
use crate::types::{
    ApiKeyDescriptor, ApiKeyIssued, FactorDescriptor, MfaChallengeResult, MfaEnrollResult,
    OAuthAuthorizeResult, Session, SignUpResult, VerifyFactorResult,
};

/// Refresh this many seconds before the access token's stated expiry.
const EXPIRY_SKEW_SECS: i64 = 10;

/// Decode the `project_id` claim from a Basin JWT payload segment.
///
/// Returns `None` for non-JWT tokens (e.g. a raw API key).
pub fn project_id_from_jwt(token: &str) -> Option<String> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let payload = parts[1];
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(payload))
        .ok()?;
    let v: Value = serde_json::from_slice(&decoded).ok()?;
    v.get("project_id")
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Auth client for `/auth/v1/*`. Holds the live [`Session`] behind an async
/// [`RwLock`] so concurrent requests share one auto-refresh.
#[derive(Clone)]
pub struct AuthClient {
    transport: Transport,
    default_project_id: Option<String>,
    session: Arc<RwLock<Option<Session>>>,
    /// Serialises the auto-refresh path so a burst of concurrent callers
    /// performs at most one refresh round-trip.
    refresh_lock: Arc<tokio::sync::Mutex<()>>,
}

impl AuthClient {
    pub(crate) fn new(transport: Transport, default_project_id: Option<String>) -> Self {
        Self {
            transport,
            default_project_id,
            session: Arc::new(RwLock::new(None)),
            refresh_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    // -- session management -------------------------------------------------

    /// Return a clone of the current session, or `None` when signed out.
    pub async fn get_session(&self) -> Option<Session> {
        self.session.read().await.clone()
    }

    /// Adopt an externally obtained token pair (e.g. restored from storage).
    pub async fn set_session(&self, session: Option<Session>) {
        *self.session.write().await = session;
    }

    fn resolve_project_id(&self, project_id: Option<&str>) -> Result<String, BasinError> {
        if let Some(p) = project_id {
            return Ok(p.to_string());
        }
        if let Some(p) = &self.default_project_id {
            return Ok(p.clone());
        }
        if let Some(key) = &self.transport.key {
            if let Some(p) = project_id_from_jwt(key) {
                return Ok(p);
            }
        }
        Err(BasinError::invalid_request(
            "project id required: pass project_id or set it on the client builder",
        ))
    }

    /// Return a valid access token, auto-refreshing if at/near expiry.
    ///
    /// Returns `None` when there is no session (callers fall back to the static
    /// key). Refresh is guarded by the session write lock, so a burst of
    /// concurrent callers performs at most one refresh.
    pub async fn access_token(&self) -> Result<Option<String>, BasinError> {
        // Fast path: a fresh (or absent) session needs no refresh and no lock.
        {
            let guard = self.session.read().await;
            match guard.as_ref() {
                None => return Ok(None),
                Some(s) if !near_expiry(&s.access_expires_at) => {
                    return Ok(Some(s.access_token.clone()));
                }
                Some(_) => {}
            }
        }
        // Slow path: serialise the refresh so concurrent callers coalesce into
        // one round-trip. Re-check under the lock — another task may have
        // refreshed while we waited for it.
        let _refresh_guard = self.refresh_lock.lock().await;
        {
            let guard = self.session.read().await;
            match guard.as_ref() {
                None => return Ok(None),
                Some(s) if !near_expiry(&s.access_expires_at) => {
                    return Ok(Some(s.access_token.clone()));
                }
                Some(_) => {}
            }
        }
        let refreshed = self.refresh_session().await?;
        Ok(Some(refreshed.access_token))
    }

    // -- core flows ---------------------------------------------------------

    /// `POST /auth/v1/signup` → `{ ok, user_id }`.
    pub async fn sign_up(
        &self,
        email: &str,
        password: &str,
        project_id: Option<&str>,
    ) -> Result<SignUpResult, BasinError> {
        let body = json!({
            "project_id": self.resolve_project_id(project_id)?,
            "email": email,
            "password": password,
        });
        self.transport
            .request_json(Method::POST, "/auth/v1/signup", &[], Some(&body), false)
            .await
    }

    /// `POST /auth/v1/signin` → token body; stored as the live session.
    pub async fn sign_in(
        &self,
        email: &str,
        password: &str,
        project_id: Option<&str>,
    ) -> Result<Session, BasinError> {
        let body = json!({
            "project_id": self.resolve_project_id(project_id)?,
            "email": email,
            "password": password,
        });
        let session: Session = self
            .transport
            .request_json(Method::POST, "/auth/v1/signin", &[], Some(&body), false)
            .await?;
        *self.session.write().await = Some(session.clone());
        Ok(session)
    }

    /// Revoke the current refresh token server-side, then clear the local
    /// session.
    ///
    /// - No-op when already signed out.
    /// - A 404 (older server without the route) is tolerated silently.
    /// - Network errors are tolerated silently — the local session is always
    ///   cleared, so the client always ends up signed out.
    pub async fn sign_out(&self) -> Result<(), BasinError> {
        let current = self.session.write().await.take();
        let current = match current {
            Some(s) => s,
            None => return Ok(()),
        };
        let body = json!({ "refresh_token": current.refresh_token });
        match self
            .transport
            .request_discard(Method::POST, "/auth/v1/signout", &[], Some(&body), false)
            .await
        {
            Ok(()) => Ok(()),
            Err(BasinError::Api(e)) if e.status == 404 => Ok(()),
            Err(BasinError::Network(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// `POST /auth/v1/refresh` — rotates both tokens. Errors with
    /// `E_REVOKED_TOKEN` if the refresh token was already rotated.
    pub async fn refresh_session(&self) -> Result<Session, BasinError> {
        let refresh_token = {
            let guard = self.session.read().await;
            match guard.as_ref() {
                Some(s) => s.refresh_token.clone(),
                None => {
                    return Err(BasinError::Api(crate::error::ApiError::new(
                        "E_UNAUTHENTICATED",
                        "no session to refresh",
                        0,
                    )))
                }
            }
        };
        let body = json!({ "refresh_token": refresh_token });
        let session: Session = self
            .transport
            .request_json(Method::POST, "/auth/v1/refresh", &[], Some(&body), false)
            .await?;
        *self.session.write().await = Some(session.clone());
        Ok(session)
    }

    /// `POST /auth/v1/verify-email` → `{ ok: true }`.
    pub async fn verify_email(
        &self,
        token: &str,
        project_id: Option<&str>,
    ) -> Result<(), BasinError> {
        let body = json!({
            "project_id": self.resolve_project_id(project_id)?,
            "token": token,
        });
        self.transport
            .request_discard(Method::POST, "/auth/v1/verify-email", &[], Some(&body), false)
            .await
    }

    /// `POST /auth/v1/request-password-reset` → `{ ok: true }`.
    pub async fn request_password_reset(
        &self,
        email: &str,
        project_id: Option<&str>,
    ) -> Result<(), BasinError> {
        let body = json!({
            "project_id": self.resolve_project_id(project_id)?,
            "email": email,
        });
        self.transport
            .request_discard(
                Method::POST,
                "/auth/v1/request-password-reset",
                &[],
                Some(&body),
                false,
            )
            .await
    }

    /// `POST /auth/v1/reset-password` → `{ ok: true }`.
    pub async fn reset_password(
        &self,
        token: &str,
        new_password: &str,
        project_id: Option<&str>,
    ) -> Result<(), BasinError> {
        let body = json!({
            "project_id": self.resolve_project_id(project_id)?,
            "token": token,
            "new_password": new_password,
        });
        self.transport
            .request_discard(Method::POST, "/auth/v1/reset-password", &[], Some(&body), false)
            .await
    }

    /// `POST /auth/v1/magic-link` — 204 always (never confirms the address).
    pub async fn request_magic_link(&self, email: &str) -> Result<(), BasinError> {
        let body = json!({ "email": email });
        self.transport
            .request_discard(Method::POST, "/auth/v1/magic-link", &[], Some(&body), false)
            .await
    }

    /// `POST /auth/v1/magic-link/consume` → token body; stored as session.
    pub async fn consume_magic_link(&self, token: &str) -> Result<Session, BasinError> {
        let body = json!({ "token": token });
        let session: Session = self
            .transport
            .request_json(
                Method::POST,
                "/auth/v1/magic-link/consume",
                &[],
                Some(&body),
                false,
            )
            .await?;
        *self.session.write().await = Some(session.clone());
        Ok(session)
    }

    // -- API keys (JWT-gated) ----------------------------------------------

    /// `POST /auth/v1/api-keys` → issued key incl. the one-time secret.
    pub async fn create_api_key(&self, name: &str) -> Result<ApiKeyIssued, BasinError> {
        let body = json!({ "name": name });
        self.transport
            .request_json(Method::POST, "/auth/v1/api-keys", &[], Some(&body), true)
            .await
    }

    /// `GET /auth/v1/api-keys`.
    pub async fn list_api_keys(&self) -> Result<Vec<ApiKeyDescriptor>, BasinError> {
        Ok(self
            .transport
            .request_json_opt(Method::GET, "/auth/v1/api-keys", &[], None, true)
            .await?
            .unwrap_or_default())
    }

    /// `DELETE /auth/v1/api-keys/:id` → `{ ok: true }`.
    pub async fn delete_api_key(&self, key_id: i64) -> Result<(), BasinError> {
        let path = format!("/auth/v1/api-keys/{key_id}");
        self.transport
            .request_discard(Method::DELETE, &path, &[], None, true)
            .await
    }

    // -- OAuth --------------------------------------------------------------

    /// `GET /auth/v1/oauth/:provider/authorize` → `{ redirect_url, state }`.
    ///
    /// Builds the provider authorize URL server-side (PKCE + signed CSRF
    /// state). The SDK cannot perform the browser redirect itself — send the
    /// user's browser to `result.redirect_url`.
    pub async fn get_oauth_authorize_url(
        &self,
        provider: &str,
        redirect_to: &str,
        project_id: Option<&str>,
    ) -> Result<OAuthAuthorizeResult, BasinError> {
        let pid = self.resolve_project_id(project_id)?;
        let path = format!("/auth/v1/oauth/{}/authorize", crate::http::urlencode(provider));
        let query = vec![
            ("project_id".to_string(), pid),
            ("redirect_to".to_string(), redirect_to.to_string()),
        ];
        self.transport
            .request_json(Method::GET, &path, &query, None, false)
            .await
    }

    // -- MFA ----------------------------------------------------------------

    /// `POST /auth/v1/factors` — begin MFA factor enrollment (JWT-gated).
    ///
    /// `factor_type` is `"totp"` or `"webauthn"`. The returned
    /// [`MfaEnrollResult`] discriminates the two shapes. The factor is
    /// `unverified` until [`verify_factor`](Self::verify_factor) succeeds.
    pub async fn enroll_factor(
        &self,
        factor_type: &str,
        friendly_name: &str,
    ) -> Result<MfaEnrollResult, BasinError> {
        let body = json!({ "factor_type": factor_type, "friendly_name": friendly_name });
        self.transport
            .request_json(Method::POST, "/auth/v1/factors", &[], Some(&body), true)
            .await
    }

    /// `GET /auth/v1/factors` — list MFA factors for the current user.
    pub async fn list_factors(&self) -> Result<Vec<FactorDescriptor>, BasinError> {
        Ok(self
            .transport
            .request_json_opt(Method::GET, "/auth/v1/factors", &[], None, true)
            .await?
            .unwrap_or_default())
    }

    /// `POST /auth/v1/factors/:id/verify` — confirm enrollment.
    ///
    /// TOTP path: pass `code`. WebAuthn path: pass `attestation` +
    /// `challenge_id`. On the **first** verified factor the response carries
    /// `recovery_codes` (shown exactly once).
    pub async fn verify_factor(
        &self,
        factor_id: &str,
        code: Option<&str>,
        attestation: Option<&str>,
        challenge_id: Option<&str>,
    ) -> Result<VerifyFactorResult, BasinError> {
        let mut body = serde_json::Map::new();
        if let Some(c) = code {
            body.insert("code".into(), json!(c));
        }
        if let Some(a) = attestation {
            body.insert("attestation".into(), json!(a));
        }
        if let Some(ch) = challenge_id {
            body.insert("challenge_id".into(), json!(ch));
        }
        let path = format!("/auth/v1/factors/{}/verify", crate::http::urlencode(factor_id));
        self.transport
            .request_json(Method::POST, &path, &[], Some(&Value::Object(body)), true)
            .await
    }

    /// `POST /auth/v1/factors/:id/challenge` — begin a step-up challenge.
    ///
    /// The server auto-detects factor type; the returned
    /// [`MfaChallengeResult`] carries `challenge_id` (and
    /// `request_options_json` for WebAuthn).
    pub async fn challenge_factor(
        &self,
        factor_id: &str,
    ) -> Result<MfaChallengeResult, BasinError> {
        let path = format!("/auth/v1/factors/{}/challenge", crate::http::urlencode(factor_id));
        let body = json!({});
        self.transport
            .request_json(Method::POST, &path, &[], Some(&body), true)
            .await
    }

    /// `POST /auth/v1/factors/:id/challenge/verify` — complete the step-up.
    ///
    /// On success returns a new [`Session`] whose JWT carries `aal2`; it is
    /// stored as the live session. TOTP path: pass `code`. WebAuthn path: pass
    /// `assertion`.
    pub async fn verify_challenge(
        &self,
        factor_id: &str,
        challenge_id: &str,
        code: Option<&str>,
        assertion: Option<&str>,
    ) -> Result<Session, BasinError> {
        let mut body = serde_json::Map::new();
        body.insert("challenge_id".into(), json!(challenge_id));
        if let Some(c) = code {
            body.insert("code".into(), json!(c));
        }
        if let Some(a) = assertion {
            body.insert("assertion".into(), json!(a));
        }
        let path = format!(
            "/auth/v1/factors/{}/challenge/verify",
            crate::http::urlencode(factor_id)
        );
        let session: Session = self
            .transport
            .request_json(Method::POST, &path, &[], Some(&Value::Object(body)), true)
            .await?;
        *self.session.write().await = Some(session.clone());
        Ok(session)
    }

    /// `DELETE /auth/v1/factors/:id` — unenroll a factor (requires an `aal2`
    /// JWT; the server returns `E_FORBIDDEN` for an `aal1`-only session).
    pub async fn unenroll_factor(&self, factor_id: &str) -> Result<(), BasinError> {
        let path = format!("/auth/v1/factors/{}", crate::http::urlencode(factor_id));
        self.transport
            .request_discard(Method::DELETE, &path, &[], None, true)
            .await
    }
}

/// Returns `true` when an RFC 3339 expiry is within [`EXPIRY_SKEW_SECS`] of now
/// (or unparseable / already past). Parsing is intentionally minimal so the SDK
/// pulls in no date-time dependency.
fn near_expiry(rfc3339: &str) -> bool {
    match parse_rfc3339_epoch(rfc3339) {
        Some(expiry) => expiry - EXPIRY_SKEW_SECS <= now_unix(),
        // Unparseable expiry → conservatively refresh.
        None => true,
    }
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Parse an RFC 3339 / ISO-8601 UTC timestamp into a Unix epoch second.
///
/// Supports `YYYY-MM-DDThh:mm:ss[.fff]Z` and `+00:00` offsets — the shapes
/// basin-rest emits. Returns `None` for anything else.
fn parse_rfc3339_epoch(s: &str) -> Option<i64> {
    // Split date and time on 'T'.
    let (date, rest) = s.split_once('T')?;
    let mut d = date.split('-');
    let year: i64 = d.next()?.parse().ok()?;
    let month: i64 = d.next()?.parse().ok()?;
    let day: i64 = d.next()?.parse().ok()?;

    // Strip timezone suffix; we treat the timestamp as UTC.
    let time = rest
        .trim_end_matches('Z')
        .split(['+', '-'])
        .next()
        .unwrap_or(rest);
    let mut t = time.split(':');
    let hour: i64 = t.next()?.parse().ok()?;
    let minute: i64 = t.next()?.parse().ok()?;
    let sec_part = t.next()?;
    let second: i64 = sec_part.split('.').next()?.parse().ok()?;

    // Days since Unix epoch via a civil-to-days algorithm (Howard Hinnant).
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    Some(days * 86400 + hour * 3600 + minute * 60 + second)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jwt_project_id_extraction() {
        // header.payload.signature where payload = {"project_id":"01J"}
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"project_id":"01J","sub":"u"}"#);
        let jwt = format!("h.{payload}.s");
        assert_eq!(project_id_from_jwt(&jwt).as_deref(), Some("01J"));
    }

    #[test]
    fn jwt_project_id_none_for_api_key() {
        assert_eq!(project_id_from_jwt("sbk_rawapikey"), None);
    }

    #[test]
    fn rfc3339_epoch_known_value() {
        // 2021-01-01T00:00:00Z = 1609459200
        assert_eq!(parse_rfc3339_epoch("2021-01-01T00:00:00Z"), Some(1609459200));
    }

    #[test]
    fn rfc3339_epoch_handles_fractional_and_offset() {
        assert_eq!(
            parse_rfc3339_epoch("2021-01-01T00:00:00.123+00:00"),
            Some(1609459200)
        );
    }

    #[test]
    fn near_expiry_far_future_is_false() {
        assert!(!near_expiry("2999-01-01T00:00:00Z"));
    }

    #[test]
    fn near_expiry_past_is_true() {
        assert!(near_expiry("2000-01-01T00:00:00Z"));
    }

    #[test]
    fn near_expiry_garbage_is_true() {
        assert!(near_expiry("not-a-date"));
    }
}
