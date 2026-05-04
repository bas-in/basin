//! JWT issue + verify (HS256).
//!
//! Claims layout matches ADR 0005: `tenant_id`, `user_id`, `email`, `roles`,
//! `iat`, `exp`. Signing is HS256 with the platform-level secret loaded by
//! `AuthConfig::from_env`.

use std::time::Duration;

use basin_common::{BasinError, Result, TenantId};
use chrono::{DateTime, Utc};
use jsonwebtoken::{
    decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Decoded JWT claims. The wire format uses string forms for ULID/UUID so the
/// token survives any JSON tooling on the way to a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    pub tenant_id: TenantId,
    pub user_id: Uuid,
    pub email: String,
    pub roles: Vec<String>,
    pub exp: i64,
    pub iat: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct WireClaims {
    tenant_id: String,
    user_id: String,
    email: String,
    roles: Vec<String>,
    exp: i64,
    iat: i64,
}

/// Wraps the HS256 keys and validation parameters shared between issue
/// and verify. Cheap to clone (key material is `Arc`'d inside jsonwebtoken).
#[derive(Clone)]
pub struct JwtKeys {
    encoding: EncodingKey,
    decoding: DecodingKey,
    validation: Validation,
}

impl JwtKeys {
    pub fn new(secret: &[u8]) -> Self {
        let mut validation = Validation::new(Algorithm::HS256);
        // We don't issue `aud` or `iss` claims yet (per-tenant secrets are a
        // v2 concern), so don't require them on verify either.
        validation.required_spec_claims = ["exp"].iter().map(|s| s.to_string()).collect();
        Self {
            encoding: EncodingKey::from_secret(secret),
            decoding: DecodingKey::from_secret(secret),
            validation,
        }
    }

    /// Issue a fresh access token. `now` lets tests be deterministic.
    pub fn issue(
        &self,
        tenant: &TenantId,
        user: Uuid,
        email: &str,
        roles: &[String],
        now: DateTime<Utc>,
        ttl: Duration,
    ) -> Result<(String, DateTime<Utc>)> {
        let exp_dt = now + chrono::Duration::from_std(ttl).map_err(|e| {
            BasinError::internal(format!("token_ttl out of range for chrono: {e}"))
        })?;
        let wire = WireClaims {
            tenant_id: tenant.to_string(),
            user_id: user.to_string(),
            email: email.to_owned(),
            roles: roles.to_vec(),
            exp: exp_dt.timestamp(),
            iat: now.timestamp(),
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
        let tenant: TenantId = w
            .tenant_id
            .parse()
            .map_err(|e| BasinError::internal(format!("jwt tenant_id parse: {e}")))?;
        let user: Uuid = w
            .user_id
            .parse()
            .map_err(|e| BasinError::internal(format!("jwt user_id parse: {e}")))?;
        Ok(Claims {
            tenant_id: tenant,
            user_id: user,
            email: w.email,
            roles: w.roles,
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
        let tenant = TenantId::new();
        let user = Uuid::new_v4();
        let now = Utc::now();
        let (jwt, exp) = keys
            .issue(
                &tenant,
                user,
                "alice@example.com",
                &["admin".to_string()],
                now,
                Duration::from_secs(60),
            )
            .unwrap();
        let claims = keys.verify(&jwt).unwrap();
        assert_eq!(claims.tenant_id, tenant);
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
                &TenantId::new(),
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
    fn expired_token_rejected() {
        let keys = JwtKeys::new(&[7u8; 32]);
        // jsonwebtoken's `Validation::default()` has a 60-second leeway for
        // clock skew. Push the issue point well past it so the token is
        // genuinely outside the window.
        let past = Utc::now() - chrono::Duration::seconds(600);
        let (jwt, _) = keys
            .issue(
                &TenantId::new(),
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
