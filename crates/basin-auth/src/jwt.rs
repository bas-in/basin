//! JWT issue + verify (HS256).
//!
//! Claims layout matches ADR 0005: `project_id`, `user_id`, `email`, `roles`,
//! `iat`, `exp`. Signing is HS256 with the platform-level secret loaded by
//! `AuthConfig::from_env`.
//!
//! Security invariants enforced on every `verify` call (fixes #53 P1):
//! - `exp` required and validated (token not expired).
//! - `nbf` required and validated (token not used before it is valid).
//! - `iat` required and must not be more than `IAT_MAX_SKEW_SECS` seconds in
//!   the future (prevents pre-issued tokens with `iat = year 3000`).
//! - 60-second leeway on `exp`/`nbf` to tolerate minor clock skew.

use std::time::Duration;

use basin_common::{BasinError, ProjectId, Result};
use chrono::{DateTime, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Maximum number of seconds that `iat` may exceed the current wall clock.
/// Tokens issued more than this far in the future are rejected even if the
/// signature is valid (prevents attacker with leaked secret from minting tokens
/// with `iat = year 3000` that would otherwise slip through).
const IAT_MAX_SKEW_SECS: i64 = 60;

/// Authenticator Assurance Level (RFC 8176 / Supabase AAL).
/// `aal1` = single-factor (password, magic-link, OAuth).
/// `aal2` = multi-factor (TOTP or WebAuthn challenge verified this session).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Aal {
    Aal1,
    Aal2,
}

impl Default for Aal {
    fn default() -> Self {
        Self::Aal1
    }
}

impl std::fmt::Display for Aal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Aal::Aal1 => f.write_str("aal1"),
            Aal::Aal2 => f.write_str("aal2"),
        }
    }
}

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
    /// Authenticator Assurance Level. `aal1` until an MFA challenge succeeds.
    pub aal: Aal,
    /// Authentication Method References (RFC 8176): ordered list of methods
    /// used this session (e.g. `["pwd"]`, `["pwd","totp"]`).
    pub amr: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WireClaims {
    project_id: String,
    user_id: String,
    email: String,
    roles: Vec<String>,
    exp: i64,
    /// `iat` is deserialized as `Option` so that a missing claim is
    /// distinguishable from `iat = 0`. The `verify` method rejects `None`.
    #[serde(default)]
    iat: Option<i64>,
    /// `nbf` (not-before) is set equal to `iat` on issue so that the token is
    /// not usable before the moment it was minted. Required by validation.
    nbf: i64,
    #[serde(default)]
    is_admin: bool,
    #[serde(default)]
    aal: Aal,
    #[serde(default)]
    amr: Vec<String>,
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
    /// `nbf` = `iat`: refresh token not usable before issuance moment.
    nbf: i64,
}

/// Audience claim for refresh JWTs. Access tokens have no `aud`.
pub const REFRESH_AUDIENCE: &str = "basin-refresh";

/// Wraps the HS256 keys and validation parameters shared between issue
/// and verify. Cheap to clone (key material is `Arc`'d inside jsonwebtoken).
///
/// # Secret rotation
///
/// Construct a fresh `JwtKeys` with the new secret to rotate. Tokens signed
/// with the old key will be rejected immediately (hard rotation). For a
/// graceful rotation that gives in-flight tokens time to expire without forcing
/// all users to re-authenticate, supply the previous secret via
/// [`JwtKeys::with_previous_secret`]: tokens that fail verification against the
/// current key are re-tried against the previous key. Remove `previous_secret`
/// once its TTL has elapsed (typically one access-token lifetime, e.g. 1 hour).
///
/// # Issuance-floor / key-rotation window bounding (audit P2-7)
///
/// An **issuance floor** is a Unix timestamp `t₀` below which any token whose
/// `iat < t₀` is rejected even if the signature is otherwise valid. This bounds
/// the refresh-token compromise window after a key rotation: by setting the
/// floor to the rotation instant, all tokens that were issued before the
/// rotation (and could be stolen refresh tokens) are immediately invalidated,
/// rather than remaining valid for the full refresh TTL (up to 30 days).
///
/// Set the floor with [`JwtKeys::with_issuance_floor`]. Tokens issued by
/// `JwtKeys` itself always set `iat = now`, so freshly minted tokens are never
/// below the floor.
#[derive(Clone)]
pub struct JwtKeys {
    encoding: EncodingKey,
    decoding: DecodingKey,
    /// Optional previous decoding key retained for grace-window rotation.
    /// Access tokens are first verified with `decoding`; on failure they are
    /// re-tried with this key. New tokens are always signed with `encoding`
    /// (current secret), so the previous key is read-only.
    previous_decoding: Option<DecodingKey>,
    validation: Validation,
    refresh_validation: Validation,
    /// Issuance floor (audit P2-7). When `Some(t₀)`, any token whose `iat`
    /// claim is strictly less than `t₀` is rejected regardless of signature
    /// validity. `None` means "no floor" (default — backward compatible).
    issuance_floor: Option<i64>,
}

impl JwtKeys {
    pub fn new(secret: &[u8]) -> Self {
        let mut validation = Validation::new(Algorithm::HS256);
        // Access tokens have no `aud`; refresh validation requires it below.
        // Require exp + nbf; iat is validated manually in `verify` because
        // jsonwebtoken's required_spec_claims only enforces exp/nbf/aud/iss/sub.
        validation.required_spec_claims = ["exp", "nbf"].iter().map(|s| s.to_string()).collect();
        // Enable nbf enforcement (defaults to false in jsonwebtoken).
        validation.validate_nbf = true;
        // 60-second leeway for exp + nbf to absorb minor clock skew.
        validation.leeway = 60;
        let mut refresh_validation = Validation::new(Algorithm::HS256);
        refresh_validation.required_spec_claims = ["exp", "nbf", "aud"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        refresh_validation.validate_nbf = true;
        refresh_validation.leeway = 60;
        refresh_validation.set_audience(&[REFRESH_AUDIENCE]);
        Self {
            encoding: EncodingKey::from_secret(secret),
            decoding: DecodingKey::from_secret(secret),
            previous_decoding: None,
            validation,
            refresh_validation,
            issuance_floor: None,
        }
    }

    /// Construct a `JwtKeys` that accepts tokens signed with either `secret`
    /// (current) **or** `previous_secret` (grace-window rotation).
    ///
    /// New tokens are always issued with `secret`. Tokens signed with
    /// `previous_secret` will verify successfully until the caller rebuilds
    /// `JwtKeys` without a `previous_secret` (typically after one
    /// access-token TTL has elapsed). This avoids force-logging-out every user
    /// the moment the secret is rotated in the environment.
    ///
    /// # Rotation procedure
    /// 1. Set `BASIN_AUTH_JWT_SECRET` to the new value and
    ///    `BASIN_AUTH_JWT_SECRET_PREVIOUS` to the old value; restart.
    /// 2. Wait one access-token TTL (default 1 h) for old tokens to expire.
    /// 3. Unset `BASIN_AUTH_JWT_SECRET_PREVIOUS`; restart.
    pub fn with_previous_secret(secret: &[u8], previous_secret: &[u8]) -> Self {
        let mut keys = Self::new(secret);
        keys.previous_decoding = Some(DecodingKey::from_secret(previous_secret));
        keys
    }

    /// Set an **issuance floor**: any token whose `iat` is strictly before
    /// `floor_unix_secs` is rejected, bounding the window during which a
    /// previously-stolen refresh token can be used (audit P2-7).
    ///
    /// Typically `floor_unix_secs` is set to `Utc::now().timestamp()` at the
    /// moment a key rotation is performed. After setting the floor, the caller
    /// should also rotate to a new secret (or keep the existing secret —
    /// the floor alone is sufficient to invalidate pre-rotation tokens without
    /// forcing a full secret change).
    ///
    /// # Example
    /// ```rust
    /// use basin_auth::jwt::JwtKeys;
    /// use chrono::Utc;
    ///
    /// // At rotation time:
    /// let floor = Utc::now().timestamp();
    /// let keys = JwtKeys::new(&[0u8; 32]).with_issuance_floor(floor);
    /// // Tokens issued before `floor` are now rejected; new tokens are fine.
    /// ```
    pub fn with_issuance_floor(mut self, floor_unix_secs: i64) -> Self {
        self.issuance_floor = Some(floor_unix_secs);
        self
    }

    /// Return the current issuance floor, if any.
    pub fn issuance_floor(&self) -> Option<i64> {
        self.issuance_floor
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
        self.issue_full(
            project,
            user,
            email,
            roles,
            is_admin,
            Aal::Aal1,
            vec![],
            now,
            ttl,
        )
    }

    /// Issue an access token with full AAL/AMR control (used by MFA challenge
    /// step-up flow). `aal2` is emitted when the user has passed an MFA challenge
    /// in this session; `amr` lists the methods used.
    pub fn issue_full(
        &self,
        project: &ProjectId,
        user: Uuid,
        email: &str,
        roles: &[String],
        is_admin: bool,
        aal: Aal,
        amr: Vec<String>,
        now: DateTime<Utc>,
        ttl: Duration,
    ) -> Result<(String, DateTime<Utc>)> {
        let exp_dt = now
            + chrono::Duration::from_std(ttl).map_err(|e| {
                BasinError::internal(format!("token_ttl out of range for chrono: {e}"))
            })?;
        let now_ts = now.timestamp();
        let wire = WireClaims {
            project_id: project.to_string(),
            user_id: user.to_string(),
            email: email.to_owned(),
            roles: roles.to_vec(),
            exp: exp_dt.timestamp(),
            iat: Some(now_ts),
            // nbf = iat: token is not valid before the moment it was issued.
            nbf: now_ts,
            is_admin,
            aal,
            amr,
        };
        let token = encode(&Header::new(Algorithm::HS256), &wire, &self.encoding)
            .map_err(|e| BasinError::internal(format!("jwt encode: {e}")))?;
        Ok((token, exp_dt))
    }

    /// Verify signature + expiry, return parsed claims.
    ///
    /// In addition to the checks performed by `jsonwebtoken` (signature, `exp`,
    /// `nbf`), this method enforces `iat` validity:
    /// - `iat` must be present in the token.
    /// - `iat` must not be more than `IAT_MAX_SKEW_SECS` seconds in the
    ///   future; a token issued with `iat = year 3000` is rejected here.
    ///
    /// If a `previous_decoding` key is present (grace-window rotation), tokens
    /// that fail verification against the current key are automatically retried
    /// against the previous key. The iat checks still apply in both cases.
    pub fn verify(&self, token: &str) -> Result<Claims> {
        let decode_result = decode::<WireClaims>(token, &self.decoding, &self.validation);
        let data = match decode_result {
            Ok(d) => d,
            Err(primary_err) => {
                // On primary failure, try the previous key if one is configured.
                match &self.previous_decoding {
                    Some(prev_key) => {
                        decode::<WireClaims>(token, prev_key, &self.validation).map_err(|_| {
                            // Surface the primary error so callers see a consistent
                            // message rather than a potentially confusing "previous
                            // key also failed" message.
                            BasinError::internal(format!("jwt verify: {primary_err}"))
                        })?
                    }
                    None => {
                        return Err(BasinError::internal(format!("jwt verify: {primary_err}")));
                    }
                }
            }
        };
        let w = data.claims;

        // --- iat validation (jsonwebtoken does not enforce this natively) ---
        // 1. iat must be present.
        let iat = w.iat.ok_or_else(|| {
            BasinError::internal("jwt verify: missing required claim: iat".to_string())
        })?;
        // 2. Reject tokens whose iat is more than IAT_MAX_SKEW_SECS in the future.
        //    This closes the attack where an adversary with a stolen secret mints a
        //    token with iat = year 3000 to confuse any time-based audit trail.
        let now_ts = Utc::now().timestamp();
        if iat > now_ts + IAT_MAX_SKEW_SECS {
            return Err(BasinError::internal(format!(
                "jwt verify: iat ({iat}) is too far in the future \
                 (now={now_ts}, max_skew={IAT_MAX_SKEW_SECS}s)"
            )));
        }
        // 3. Issuance-floor check (audit P2-7): reject tokens issued before the
        //    rotation floor. This bounds the refresh-token compromise window to
        //    the interval [floor, now] after a secret/key rotation.
        if let Some(floor) = self.issuance_floor {
            if iat < floor {
                return Err(BasinError::internal(format!(
                    "jwt verify: token issued before issuance floor \
                     (iat={iat}, floor={floor})"
                )));
            }
        }

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
            iat,
            is_admin: w.is_admin,
            aal: w.aal,
            amr: w.amr,
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
        let now_ts = now.timestamp();
        let wire = RefreshWireClaims {
            project_id: project.to_string(),
            user_id: user.to_string(),
            email: email.to_owned(),
            jti: jti.clone(),
            aud: REFRESH_AUDIENCE.to_owned(),
            exp: exp_dt.timestamp(),
            iat: now_ts,
            nbf: now_ts,
        };
        let token = encode(&Header::new(Algorithm::HS256), &wire, &self.encoding)
            .map_err(|e| BasinError::internal(format!("refresh jwt encode: {e}")))?;
        Ok((token, jti, exp_dt))
    }

    /// Verify a refresh JWT (signature + expiry + audience).
    ///
    /// Verify a refresh token's **signature and audience** only — expiry is
    /// intentionally not checked. Used by the sign-out handler so that a user
    /// holding an already-expired refresh token can still call
    /// `POST /auth/v1/signout` and get a clean revocation row written.
    ///
    /// Returns `Ok(RefreshClaims)` when the signature is valid and the
    /// `aud=basin-refresh` claim is present. Returns an error when the token
    /// is structurally malformed, signed with an unknown key, or missing the
    /// `aud` claim — all of which mean the token was never validly issued.
    pub fn verify_refresh_structural(&self, token: &str) -> Result<RefreshClaims> {
        let mut v = Validation::new(Algorithm::HS256);
        // Require audience so an access token cannot be passed here.
        v.required_spec_claims = ["aud"].iter().map(|s| s.to_string()).collect();
        v.set_audience(&[REFRESH_AUDIENCE]);
        // Do NOT validate exp/nbf — the token may be expired but the caller
        // still wants to record the revocation.
        v.validate_exp = false;
        v.validate_nbf = false;

        let decode_result = decode::<RefreshWireClaims>(token, &self.decoding, &v);
        let data = match decode_result {
            Ok(d) => d,
            Err(primary_err) => match &self.previous_decoding {
                Some(prev_key) => {
                    decode::<RefreshWireClaims>(token, prev_key, &v).map_err(|_| {
                        BasinError::internal(format!(
                            "refresh jwt structural verify: {primary_err}"
                        ))
                    })?
                }
                None => {
                    return Err(BasinError::internal(format!(
                        "refresh jwt structural verify: {primary_err}"
                    )));
                }
            },
        };
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

    /// Like [`Self::verify`], falls back to `previous_decoding` if present and
    /// the primary key rejects the token.
    pub fn verify_refresh(&self, token: &str) -> Result<RefreshClaims> {
        let decode_result =
            decode::<RefreshWireClaims>(token, &self.decoding, &self.refresh_validation);
        let data = match decode_result {
            Ok(d) => d,
            Err(primary_err) => match &self.previous_decoding {
                Some(prev_key) => {
                    decode::<RefreshWireClaims>(token, prev_key, &self.refresh_validation).map_err(
                        |_| BasinError::internal(format!("refresh jwt verify: {primary_err}")),
                    )?
                }
                None => {
                    return Err(BasinError::internal(format!(
                        "refresh jwt verify: {primary_err}"
                    )));
                }
            },
        };
        let w = data.claims;

        // Issuance-floor check (audit P2-7): a stolen refresh token issued
        // before a rotation event is rejected here, bounding the compromise
        // window even when the full 30-day TTL has not yet elapsed.
        if let Some(floor) = self.issuance_floor {
            if w.iat < floor {
                return Err(BasinError::internal(format!(
                    "refresh jwt verify: token issued before issuance floor \
                     (iat={}, floor={floor})",
                    w.iat
                )));
            }
        }

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

    // -----------------------------------------------------------------------
    // Helper: encode a raw JWT with arbitrary claims (for adversarial tests).
    // -----------------------------------------------------------------------

    /// Minimal wire shape for crafting adversarial tokens in tests.
    #[derive(Debug, Serialize, Deserialize)]
    struct RawClaims {
        project_id: String,
        user_id: String,
        email: String,
        roles: Vec<String>,
        exp: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        iat: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        nbf: Option<i64>,
    }

    fn encode_raw(secret: &[u8], raw: &RawClaims) -> String {
        let key = EncodingKey::from_secret(secret);
        encode(&Header::new(Algorithm::HS256), raw, &key).unwrap()
    }

    fn default_raw(now_ts: i64, iat: Option<i64>, nbf: Option<i64>) -> RawClaims {
        RawClaims {
            project_id: ProjectId::new().to_string(),
            user_id: Uuid::new_v4().to_string(),
            email: "test@example.com".to_owned(),
            roles: vec![],
            // exp safely in the future
            exp: now_ts + 3600,
            iat,
            nbf,
        }
    }

    // -----------------------------------------------------------------------
    // iat / nbf security tests (fixes #53 P1)
    // -----------------------------------------------------------------------

    /// A token with `iat` one year in the future must be rejected, even though
    /// the signature is valid (attacker with stolen secret scenario).
    #[test]
    fn jwt_iat_in_future_rejected() {
        let secret = [42u8; 32];
        let keys = JwtKeys::new(&secret);
        let now_ts = Utc::now().timestamp();
        let future_iat = now_ts + 3600 * 24 * 365; // ~1 year ahead
        let raw = default_raw(now_ts, Some(future_iat), Some(now_ts));
        let token = encode_raw(&secret, &raw);
        let err = keys.verify(&token).unwrap_err();
        assert!(
            err.to_string().contains("iat") || err.to_string().contains("future"),
            "expected iat-future error, got: {err}"
        );
    }

    /// A token with `nbf` one hour in the future must be rejected.
    #[test]
    fn jwt_nbf_in_future_rejected() {
        let secret = [43u8; 32];
        let keys = JwtKeys::new(&secret);
        let now_ts = Utc::now().timestamp();
        let future_nbf = now_ts + 3600; // 1 hour ahead
        let raw = default_raw(now_ts, Some(now_ts), Some(future_nbf));
        let token = encode_raw(&secret, &raw);
        // The library should reject via ImmatureSignature.
        assert!(
            keys.verify(&token).is_err(),
            "token with nbf in the future must be rejected"
        );
    }

    /// A token with `iat` only 30 seconds ahead is within the 60-second leeway
    /// and must be accepted.
    #[test]
    fn jwt_iat_within_leeway_accepted() {
        let secret = [44u8; 32];
        let keys = JwtKeys::new(&secret);
        let now_ts = Utc::now().timestamp();
        let slightly_future_iat = now_ts + 30; // within IAT_MAX_SKEW_SECS (60)
        let raw = default_raw(now_ts, Some(slightly_future_iat), Some(now_ts));
        let token = encode_raw(&secret, &raw);
        assert!(
            keys.verify(&token).is_ok(),
            "token with iat 30s ahead should be accepted (within 60s leeway)"
        );
    }

    /// A token missing `iat` must be rejected because our validation now
    /// requires `iat` to be present (validated manually in `verify`).
    #[test]
    fn jwt_missing_iat_rejected() {
        let secret = [45u8; 32];
        let keys = JwtKeys::new(&secret);
        let now_ts = Utc::now().timestamp();
        // iat = None means the field is omitted from the token payload.
        let raw = default_raw(now_ts, None, Some(now_ts));
        let token = encode_raw(&secret, &raw);
        // Without iat the iat > now + skew check treats w.iat as the serde
        // default (0), which is safely in the past — so we need the explicit
        // missing-iat guard. Tokens without iat must be rejected.
        let err = keys.verify(&token).unwrap_err();
        assert!(
            err.to_string().contains("iat") || err.to_string().contains("missing"),
            "expected missing-iat error, got: {err}"
        );
    }

    /// A token missing `nbf` must be rejected because nbf is now required.
    #[test]
    fn jwt_missing_nbf_rejected() {
        let secret = [46u8; 32];
        let keys = JwtKeys::new(&secret);
        let now_ts = Utc::now().timestamp();
        // nbf = None → field omitted; required_spec_claims includes "nbf".
        let raw = default_raw(now_ts, Some(now_ts), None);
        let token = encode_raw(&secret, &raw);
        assert!(
            keys.verify(&token).is_err(),
            "token missing nbf must be rejected (nbf is required)"
        );
    }

    // -----------------------------------------------------------------------
    // Pre-existing tests (must still pass)
    // -----------------------------------------------------------------------

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
        assert_eq!(claims.aal, Aal::Aal1);
        assert!(claims.amr.is_empty());
    }

    #[test]
    fn issue_full_aal2_round_trips() {
        let keys = JwtKeys::new(&[7u8; 32]);
        let project = ProjectId::new();
        let user = Uuid::new_v4();
        let now = Utc::now();
        let (jwt, _) = keys
            .issue_full(
                &project,
                user,
                "alice@example.com",
                &["user".to_string()],
                false,
                Aal::Aal2,
                vec!["pwd".to_string(), "totp".to_string()],
                now,
                Duration::from_secs(60),
            )
            .unwrap();
        let claims = keys.verify(&jwt).unwrap();
        assert_eq!(claims.aal, Aal::Aal2);
        assert_eq!(claims.amr, vec!["pwd", "totp"]);
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

    // -----------------------------------------------------------------------
    // Secret rotation tests (fixes #53 P1 JWT rotation)
    // -----------------------------------------------------------------------

    /// Hard rotation: a token signed with secret-A must be rejected once keys
    /// are rebuilt with secret-B only (no grace window).
    ///
    /// This validates that `JwtKeys` holds no stale key material and that
    /// constructing a new `JwtKeys` with a different secret is sufficient to
    /// invalidate all previously-issued tokens.
    #[test]
    fn jwt_secret_rotation_invalidates_old_tokens() {
        let secret_a = [0xAAu8; 32];
        let secret_b = [0xBBu8; 32];

        // Issue with secret-A.
        let keys_a = JwtKeys::new(&secret_a);
        let (token_signed_with_a, _) = keys_a
            .issue(
                &ProjectId::new(),
                Uuid::new_v4(),
                "alice@example.com",
                &["user".to_string()],
                Utc::now(),
                Duration::from_secs(3600),
            )
            .unwrap();

        // Rotate to secret-B (hard rotation — no previous key).
        let keys_b = JwtKeys::new(&secret_b);

        // The token issued with A must fail verification against B.
        let result = keys_b.verify(&token_signed_with_a);
        assert!(
            result.is_err(),
            "token signed with secret-A must be rejected after hard rotation to secret-B"
        );

        // Sanity-check: the new keys can still issue and verify their own tokens.
        let (token_b, _) = keys_b
            .issue(
                &ProjectId::new(),
                Uuid::new_v4(),
                "bob@example.com",
                &[],
                Utc::now(),
                Duration::from_secs(3600),
            )
            .unwrap();
        assert!(
            keys_b.verify(&token_b).is_ok(),
            "keys_b must verify tokens it signed itself"
        );

        // And old keys cannot verify new tokens (different secret, different sig).
        assert!(
            keys_a.verify(&token_b).is_err(),
            "keys_a must not verify tokens signed with secret-B"
        );
    }

    /// Refresh-token hard rotation: a refresh token issued with secret-A must be
    /// rejected after rotation to secret-B (no grace window).
    #[test]
    fn jwt_refresh_secret_rotation_invalidates_old_tokens() {
        let secret_a = [0xCCu8; 32];
        let secret_b = [0xDDu8; 32];

        let keys_a = JwtKeys::new(&secret_a);
        let (refresh_token, _jti, _exp) = keys_a
            .issue_refresh(
                &ProjectId::new(),
                Uuid::new_v4(),
                "carol@example.com",
                Utc::now(),
                Duration::from_secs(3600),
            )
            .unwrap();

        // Hard rotate to secret-B.
        let keys_b = JwtKeys::new(&secret_b);

        assert!(
            keys_b.verify_refresh(&refresh_token).is_err(),
            "refresh token signed with secret-A must be rejected after rotation to secret-B"
        );
    }

    /// Grace-window rotation: a token signed with the *previous* secret must
    /// still verify during the grace window when `with_previous_secret` is used.
    ///
    /// Rotation procedure:
    ///   1. `JwtKeys::with_previous_secret(new, old)` — accepts both.
    ///   2. After one TTL window: `JwtKeys::new(new)` — old tokens expire.
    #[test]
    fn jwt_secret_rotation_grace_window_accepts_previous() {
        let secret_old = [0x11u8; 32];
        let secret_new = [0x22u8; 32];

        // Issue with the old secret.
        let keys_old = JwtKeys::new(&secret_old);
        let (token_signed_with_old, _) = keys_old
            .issue(
                &ProjectId::new(),
                Uuid::new_v4(),
                "dave@example.com",
                &["viewer".to_string()],
                Utc::now(),
                Duration::from_secs(3600),
            )
            .unwrap();

        // Rotate: current=new, previous=old (grace window open).
        let keys_grace = JwtKeys::with_previous_secret(&secret_new, &secret_old);

        // Token signed with old secret must still verify during grace window.
        let claims = keys_grace.verify(&token_signed_with_old);
        assert!(
            claims.is_ok(),
            "token signed with previous secret must verify during grace window; \
             got: {:?}",
            claims.err()
        );
        assert_eq!(
            claims.unwrap().email,
            "dave@example.com",
            "claims must decode correctly through the grace-window path"
        );

        // New tokens are issued with the new secret.
        let (token_signed_with_new, _) = keys_grace
            .issue(
                &ProjectId::new(),
                Uuid::new_v4(),
                "eve@example.com",
                &[],
                Utc::now(),
                Duration::from_secs(3600),
            )
            .unwrap();

        // New tokens verify fine against keys_grace.
        assert!(
            keys_grace.verify(&token_signed_with_new).is_ok(),
            "new tokens signed with current secret must verify against grace-window keys"
        );

        // Once the grace window closes (new JwtKeys with only the new secret),
        // old tokens must be rejected.
        let keys_final = JwtKeys::new(&secret_new);
        assert!(
            keys_final.verify(&token_signed_with_old).is_err(),
            "old token must be rejected once grace window closes"
        );
        // New tokens must still verify after grace window closes.
        assert!(
            keys_final.verify(&token_signed_with_new).is_ok(),
            "new tokens must continue to verify after grace window closes"
        );
    }

    /// Grace-window rotation for refresh tokens.
    #[test]
    fn jwt_refresh_grace_window_accepts_previous() {
        let secret_old = [0x33u8; 32];
        let secret_new = [0x44u8; 32];

        let keys_old = JwtKeys::new(&secret_old);
        let (refresh_token, _jti, _exp) = keys_old
            .issue_refresh(
                &ProjectId::new(),
                Uuid::new_v4(),
                "frank@example.com",
                Utc::now(),
                Duration::from_secs(3600),
            )
            .unwrap();

        // Rotate with grace window.
        let keys_grace = JwtKeys::with_previous_secret(&secret_new, &secret_old);

        assert!(
            keys_grace.verify_refresh(&refresh_token).is_ok(),
            "refresh token signed with previous secret must verify during grace window"
        );

        // After grace window closes, the old refresh token must fail.
        let keys_final = JwtKeys::new(&secret_new);
        assert!(
            keys_final.verify_refresh(&refresh_token).is_err(),
            "refresh token must be rejected once grace window closes"
        );
    }

    // -----------------------------------------------------------------------
    // Issuance-floor tests (audit P2-7)
    // -----------------------------------------------------------------------

    /// Tokens issued BEFORE the floor are rejected, even with a valid signature.
    #[test]
    fn jwt_issuance_floor_rejects_pre_rotation_access_token() {
        let secret = [0xA1u8; 32];
        let keys = JwtKeys::new(&secret);
        let now = Utc::now();

        // Issue a token whose iat is in the "past" relative to the floor.
        let (token, _) = keys
            .issue(
                &ProjectId::new(),
                Uuid::new_v4(),
                "victim@example.com",
                &["user".to_string()],
                now - chrono::Duration::seconds(120), // iat = 2 minutes ago
                Duration::from_secs(3600),
            )
            .unwrap();

        // Install a floor at (now - 60s): the token's iat is before the floor.
        let floor = (now - chrono::Duration::seconds(60)).timestamp();
        let keys_with_floor = JwtKeys::new(&secret).with_issuance_floor(floor);

        let result = keys_with_floor.verify(&token);
        assert!(
            result.is_err(),
            "SECURITY P2-7: access token issued before issuance floor MUST be rejected"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("floor") || msg.contains("issuance"),
            "error must mention floor/issuance, got: {msg}"
        );
    }

    /// Tokens issued AFTER the floor are still accepted.
    #[test]
    fn jwt_issuance_floor_accepts_post_rotation_access_token() {
        let secret = [0xA2u8; 32];
        let now = Utc::now();

        // Floor = 10 minutes ago; token iat = now → after the floor.
        let floor = (now - chrono::Duration::seconds(600)).timestamp();
        let keys = JwtKeys::new(&secret).with_issuance_floor(floor);

        let (token, _) = keys
            .issue(
                &ProjectId::new(),
                Uuid::new_v4(),
                "ok@example.com",
                &[],
                now,
                Duration::from_secs(3600),
            )
            .unwrap();

        assert!(
            keys.verify(&token).is_ok(),
            "token issued after the floor must still verify"
        );
    }

    /// Refresh token issued before the floor is rejected (audit P2-7).
    #[test]
    fn jwt_issuance_floor_rejects_pre_rotation_refresh_token() {
        let secret = [0xA3u8; 32];
        let keys = JwtKeys::new(&secret);
        let now = Utc::now();

        // Issue refresh token with iat 2 minutes in the past.
        let (refresh, _jti, _exp) = keys
            .issue_refresh(
                &ProjectId::new(),
                Uuid::new_v4(),
                "stolen@example.com",
                now - chrono::Duration::seconds(120),
                Duration::from_secs(86_400 * 30), // 30-day TTL
            )
            .unwrap();

        // Install floor at (now - 60s).
        let floor = (now - chrono::Duration::seconds(60)).timestamp();
        let keys_with_floor = JwtKeys::new(&secret).with_issuance_floor(floor);

        let result = keys_with_floor.verify_refresh(&refresh);
        assert!(
            result.is_err(),
            "SECURITY P2-7: stolen refresh token issued before floor MUST be rejected"
        );
    }

    /// Refresh token issued after the floor is still accepted (audit P2-7).
    #[test]
    fn jwt_issuance_floor_accepts_post_rotation_refresh_token() {
        let secret = [0xA4u8; 32];
        let now = Utc::now();

        let floor = (now - chrono::Duration::seconds(600)).timestamp();
        let keys = JwtKeys::new(&secret).with_issuance_floor(floor);

        let (refresh, _jti, _exp) = keys
            .issue_refresh(
                &ProjectId::new(),
                Uuid::new_v4(),
                "fresh@example.com",
                now,
                Duration::from_secs(3600),
            )
            .unwrap();

        assert!(
            keys.verify_refresh(&refresh).is_ok(),
            "refresh token issued after the floor must still verify"
        );
    }

    /// No-floor keys (the default) still accept all valid tokens (backward compat).
    #[test]
    fn jwt_no_floor_accepts_old_tokens() {
        let secret = [0xA5u8; 32];
        let keys = JwtKeys::new(&secret); // no floor
        let past = Utc::now() - chrono::Duration::seconds(7200);

        let (token, _) = keys
            .issue(
                &ProjectId::new(),
                Uuid::new_v4(),
                "nofloor@example.com",
                &[],
                past,
                // TTL of 3 hours so the token hasn't expired yet even issued
                // 2 hours ago (exp = past + 3h = now + 1h).
                Duration::from_secs(3 * 3600),
            )
            .unwrap();

        assert!(
            keys.verify(&token).is_ok(),
            "without a floor, tokens issued in the past must still verify (backward compat)"
        );
    }
}
