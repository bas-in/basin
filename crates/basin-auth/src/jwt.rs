//! JWT issue + verify (HS256).
//!
//! Claims layout matches ADR 0005: `project_id`, `user_id`, `email`, `roles`,
//! `iat`, `exp`. Signing is HS256 with the platform-level secret loaded by
//! `AuthConfig::from_env`.

use std::time::Duration;

use basin_common::{BasinError, ProjectId, Result};
use chrono::{DateTime, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Decoded JWT claims. The wire format uses string forms for ULID/UUID so the
/// token survives any JSON tooling on the way to a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    pub project_id: ProjectId,
    pub user_id: Uuid,
    pub email: String,
    pub roles: Vec<String>,
    pub exp: i64,
    pub iat: i64,
    /// True for operator-grade tokens that gate `/admin/v1/*` endpoints.
    /// Defaults to `false` (i.e. the wire claim is omitted by `issue` and
    /// `WireClaims` decodes a missing field as `false`). The wedge customer's
    /// control plane mints one admin-true token at deploy time and uses it to
    /// provision projects.
    pub is_admin: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct WireClaims {
    project_id: String,
    user_id: String,
    email: String,
    roles: Vec<String>,
    exp: i64,
    iat: i64,
    #[serde(default)]
    is_admin: bool,
}

/// Refresh-token claims. Distinct `aud` keeps a stolen access token from
/// being passed to `/refresh`; `jti` keys the per-token revocation row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshClaims {
    pub project_id: ProjectId,
    pub user_id: Uuid,
    pub email: String,
    pub jti: String,
    pub exp: i64,
    pub iat: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct RefreshWireClaims {
    project_id: String,
    user_id: String,
    email: String,
    jti: String,
    aud: String,
    exp: i64,
    iat: i64,
}

/// Audience claim for refresh JWTs. Access tokens have no `aud`.
pub const REFRESH_AUDIENCE: &str = "basin-refresh";

/// Wraps the HS256 keys and validation parameters shared between issue
/// and verify. Cheap to clone (key material is `Arc`'d inside jsonwebtoken).
#[derive(Clone)]
pub struct JwtKeys {
    encoding: EncodingKey,
    decoding: DecodingKey,
    validation: Validation,
    refresh_validation: Validation,
}

impl JwtKeys {
    pub fn new(secret: &[u8]) -> Self {
        let mut validation = Validation::new(Algorithm::HS256);
        // Access tokens have no `aud`; refresh validation requires it below.
        validation.required_spec_claims = ["exp"].iter().map(|s| s.to_string()).collect();
        let mut refresh_validation = Validation::new(Algorithm::HS256);
        refresh_validation.required_spec_claims =
            ["exp", "aud"].iter().map(|s| s.to_string()).collect();
        refresh_validation.set_audience(&[REFRESH_AUDIENCE]);
        Self {
            encoding: EncodingKey::from_secret(secret),
            decoding: DecodingKey::from_secret(secret),
            validation,
            refresh_validation,
        }
    }

    /// Issue a fresh access token. `now` lets tests be deterministic.
    pub fn issue(
        &self,
        project: &ProjectId,
        user: Uuid,
        email: &str,
        roles: &[String],
        now: DateTime<Utc>,
        ttl: Duration,
    ) -> Result<(String, DateTime<Utc>)> {
        self.issue_with_admin(project, user, email, roles, false, now, ttl)
    }

    /// Issue an access token with the `is_admin` claim set explicitly. Tokens
    /// with `is_admin = true` gate the `/admin/v1/*` operator endpoints.
    /// Most callers should use [`Self::issue`]; this is the single
    /// admin-grade entry point.
    pub fn issue_with_admin(
        &self,
        project: &ProjectId,
        user: Uuid,
        email: &str,
        roles: &[String],
        is_admin: bool,
        now: DateTime<Utc>,
        ttl: Duration,
    ) -> Result<(String, DateTime<Utc>)> {
        let exp_dt = now
            + chrono::Duration::from_std(ttl).map_err(|e| {
                BasinError::internal(format!("token_ttl out of range for chrono: {e}"))
            })?;
        let wire = WireClaims {
            project_id: project.to_string(),
            user_id: user.to_string(),
            email: email.to_owned(),
            roles: roles.to_vec(),
            exp: exp_dt.timestamp(),
            iat: now.timestamp(),
            is_admin,
        };
        let token = encode(&Header::new(Algorithm::HS256), &wire, &self.encoding)
            .map_err(|e| BasinError::internal(format!("jwt encode: {e}")))?;
        Ok((token, exp_dt))
    }

    /// Verify signature + expiry, return parsed claims.
    pub fn verify(&self, token: &str) -> Result<Claims> {
        let data = decode::<WireClaims>(token, &self.decoding, &self.validation)
            .map_err(|e| BasinError::internal(format!("jwt verify: {e}")))?;
        let w = data.claims;
        let project: ProjectId = w
            .project_id
            .parse()
            .map_err(|e| BasinError::internal(format!("jwt project_id parse: {e}")))?;
        let user: Uuid = w
            .user_id
            .parse()
            .map_err(|e| BasinError::internal(format!("jwt user_id parse: {e}")))?;
        Ok(Claims {
            project_id: project,
            user_id: user,
            email: w.email,
            roles: w.roles,
            exp: w.exp,
            iat: w.iat,
            is_admin: w.is_admin,
        })
    }

    /// Issue a refresh JWT. The `jti` is a fresh UUID; the audience is
    /// fixed to [`REFRESH_AUDIENCE`] so an access token can't be presented
    /// to `/refresh` and vice versa.
    pub fn issue_refresh(
        &self,
        project: &ProjectId,
        user: Uuid,
        email: &str,
        now: DateTime<Utc>,
        ttl: Duration,
    ) -> Result<(String, String, DateTime<Utc>)> {
        let exp_dt = now
            + chrono::Duration::from_std(ttl).map_err(|e| {
                BasinError::internal(format!("refresh_ttl out of range for chrono: {e}"))
            })?;
        let jti = Uuid::new_v4().to_string();
        let wire = RefreshWireClaims {
            project_id: project.to_string(),
            user_id: user.to_string(),
            email: email.to_owned(),
            jti: jti.clone(),
            aud: REFRESH_AUDIENCE.to_owned(),
            exp: exp_dt.timestamp(),
            iat: now.timestamp(),
        };
        let token = encode(&Header::new(Algorithm::HS256), &wire, &self.encoding)
            .map_err(|e| BasinError::internal(format!("refresh jwt encode: {e}")))?;
        Ok((token, jti, exp_dt))
    }

    /// Verify a refresh JWT (signature + expiry + audience).
    pub fn verify_refresh(&self, token: &str) -> Result<RefreshClaims> {
        let data = decode::<RefreshWireClaims>(token, &self.decoding, &self.refresh_validation)
            .map_err(|e| BasinError::internal(format!("refresh jwt verify: {e}")))?;
        let w = data.claims;
        let project: ProjectId = w
            .project_id
            .parse()
            .map_err(|e| BasinError::internal(format!("refresh jwt project_id parse: {e}")))?;
        let user: Uuid = w
            .user_id
            .parse()
            .map_err(|e| BasinError::internal(format!("refresh jwt user_id parse: {e}")))?;
        Ok(RefreshClaims {
            project_id: project,
            user_id: user,
            email: w.email,
            jti: w.jti,
            exp: w.exp,
            iat: w.iat,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issue_then_verify_round_trips() {
        let keys = JwtKeys::new(&[7u8; 32]);
        let project = ProjectId::new();
        let user = Uuid::new_v4();
        let now = Utc::now();
        let (jwt, exp) = keys
            .issue(
                &project,
                user,
                "alice@example.com",
                &["admin".to_string()],
                now,
                Duration::from_secs(60),
            )
            .unwrap();
        let claims = keys.verify(&jwt).unwrap();
        assert_eq!(claims.project_id, project);
        assert_eq!(claims.user_id, user);
        assert_eq!(claims.email, "alice@example.com");
        assert_eq!(claims.roles, vec!["admin".to_string()]);
        assert_eq!(claims.exp, exp.timestamp());
        assert_eq!(claims.iat, now.timestamp());
    }

    #[test]
    fn tampered_signature_rejected() {
        let keys = JwtKeys::new(&[7u8; 32]);
        let (jwt, _) = keys
            .issue(
                &ProjectId::new(),
                Uuid::new_v4(),
                "x@y.z",
                &[],
                Utc::now(),
                Duration::from_secs(60),
            )
            .unwrap();
        // Flip the last char of the signature (last segment after the final '.').
        let mut bytes = jwt.into_bytes();
        let last = bytes.last_mut().unwrap();
        *last = if *last == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(bytes).unwrap();
        assert!(keys.verify(&tampered).is_err());
    }

    #[test]
    fn refresh_token_round_trips() {
        let keys = JwtKeys::new(&[7u8; 32]);
        let project = ProjectId::new();
        let user = Uuid::new_v4();
        let now = Utc::now();
        let (jwt, jti, exp) = keys
            .issue_refresh(
                &project,
                user,
                "alice@example.com",
                now,
                Duration::from_secs(60),
            )
            .unwrap();
        let claims = keys.verify_refresh(&jwt).unwrap();
        assert_eq!(claims.project_id, project);
        assert_eq!(claims.user_id, user);
        assert_eq!(claims.jti, jti);
        assert_eq!(claims.exp, exp.timestamp());
    }

    #[test]
    fn refresh_jwt_unique_jti_per_issue() {
        let keys = JwtKeys::new(&[7u8; 32]);
        let project = ProjectId::new();
        let user = Uuid::new_v4();
        let now = Utc::now();
        let (_, jti_a, _) = keys
            .issue_refresh(&project, user, "x@y.z", now, Duration::from_secs(60))
            .unwrap();
        let (_, jti_b, _) = keys
            .issue_refresh(&project, user, "x@y.z", now, Duration::from_secs(60))
            .unwrap();
        assert_ne!(jti_a, jti_b, "jti must be unique per refresh issuance");
    }

    #[test]
    fn access_token_rejected_by_refresh_verify() {
        // Cross-audience replay protection: access tokens have no `aud`,
        // refresh-verify requires `aud=basin-refresh`.
        let keys = JwtKeys::new(&[7u8; 32]);
        let (access, _) = keys
            .issue(
                &ProjectId::new(),
                Uuid::new_v4(),
                "x@y.z",
                &[],
                Utc::now(),
                Duration::from_secs(60),
            )
            .unwrap();
        assert!(keys.verify_refresh(&access).is_err());
    }

    #[test]
    fn refresh_token_rejected_by_access_verify() {
        let keys = JwtKeys::new(&[7u8; 32]);
        let (refresh, _, _) = keys
            .issue_refresh(
                &ProjectId::new(),
                Uuid::new_v4(),
                "x@y.z",
                Utc::now(),
                Duration::from_secs(60),
            )
            .unwrap();
        // Access verify rejects because the refresh wire doesn't have `roles`.
        // (Either way, reusing a refresh as an access token must fail.)
        assert!(keys.verify(&refresh).is_err());
    }

    #[test]
    fn expired_token_rejected() {
        let keys = JwtKeys::new(&[7u8; 32]);
        // jsonwebtoken's `Validation::default()` has a 60-second leeway for
        // clock skew. Push the issue point well past it so the token is
        // genuinely outside the window.
        let past = Utc::now() - chrono::Duration::seconds(600);
        let (jwt, _) = keys
            .issue(
                &ProjectId::new(),
                Uuid::new_v4(),
                "x@y.z",
                &[],
                past,
                Duration::from_secs(5),
            )
            .unwrap();
        assert!(keys.verify(&jwt).is_err(), "expired jwt must not verify");
    }
}
