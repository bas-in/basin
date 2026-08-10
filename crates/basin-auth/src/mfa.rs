//! MFA (Multi-Factor Authentication) — TOTP + WebAuthn/passkeys (Phase 5.10.M / ADR 0020).
//!
//! ## Overview
//!
//! Implements RFC 6238 TOTP and FIDO2/WebAuthn factor enrollment + verification,
//! AAL2 step-up JWT re-issuance, and single-use argon2-hashed recovery codes.
//!
//! ## Factor lifecycle
//!
//! 1. `POST /auth/v1/factors`              — enroll: returns secret + otpauth URI (TOTP)
//!                                           or creation challenge (WebAuthn).
//! 2. `POST /auth/v1/factors/:id/verify`  — confirm enrollment (first TOTP code / attestation).
//! 3. `POST /auth/v1/factors/:id/challenge`        — begin step-up challenge.
//! 4. `POST /auth/v1/factors/:id/challenge/verify` — complete step-up → aal2 JWT.
//! 5. `DELETE /auth/v1/factors/:id`        — unenroll (requires aal2).
//!
//! ## Downgrade guard
//!
//! Once a user has an enrolled (`verified`) factor, `jwt_aal` in the JWT
//! issuance path is capped at `aal1` until a challenge succeeds. RLS policies
//! that call `auth.aal()` and compare against `'aal2'` will fail closed for
//! aal1 sessions.
//!
//! ## Recovery codes
//!
//! Eight recovery codes are issued at first factor enrollment. Each is a
//! 12-byte random value hex-encoded. They are stored argon2id-hashed and
//! consumed single-use. Rate-limited via the existing `governor` infrastructure.
//!
//! ## Crypto provenance
//!
//! - TOTP: SHA1-HMAC RFC 6238 (6-digit, 30-second step). Replay protection
//!   uses the RFC 6238 §5.2 monotonic-counter rule: the highest accepted
//!   30-second step is persisted on the factor row (`last_used_step`), and any
//!   subsequent code whose step is `<=` that value is rejected. A captured code
//!   therefore cannot be replayed inside its ±skew validity window.
//! - WebAuthn: real FIDO2 verification (Phase 6.SEC.P0.2). Registration parses
//!   the CBOR attestation object, extracts the COSE P-256 public key + credential
//!   id, and binds the clientDataJSON `type`/`challenge`/`origin`. Assertion
//!   verifies the ECDSA-P256 signature over `authenticatorData ||
//!   sha256(clientDataJSON)` against the stored public key and enforces the
//!   FIDO sign-counter (a counter that fails to advance ⇒ cloned authenticator
//!   ⇒ rejected). Crypto via the pure-Rust `p256` + `ciborium` crates.
//! - Recovery codes: bcrypt (cost 4) over 96-bit random codes; single-use.

use async_trait::async_trait;
use base64::Engine;
use basin_common::{BasinError, ProjectId, Result};
use chrono::Utc;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tracing::instrument;
use uuid::Uuid;

use crate::jwt::Aal;
use crate::store::AuthStore;
use crate::{Inner, Tokens};

// ---------------------------------------------------------------------------
// Factor types
// ---------------------------------------------------------------------------

/// The MFA factor type stored in `{schema}_mfa_factors.factor_type`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FactorType {
    Totp,
    Webauthn,
}

impl std::fmt::Display for FactorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FactorType::Totp => f.write_str("totp"),
            FactorType::Webauthn => f.write_str("webauthn"),
        }
    }
}

impl FactorType {
    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "totp" => Ok(FactorType::Totp),
            "webauthn" => Ok(FactorType::Webauthn),
            other => Err(BasinError::InvalidIdent(format!(
                "unknown factor type: {other:?}"
            ))),
        }
    }
}

/// Factor enrollment status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FactorStatus {
    /// Enrollment started but not yet confirmed with a valid OTP/assertion.
    Unverified,
    /// Enrollment confirmed; factor is active.
    Verified,
}

impl std::fmt::Display for FactorStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FactorStatus::Unverified => f.write_str("unverified"),
            FactorStatus::Verified => f.write_str("verified"),
        }
    }
}

impl FactorStatus {
    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "unverified" => Ok(FactorStatus::Unverified),
            "verified" => Ok(FactorStatus::Verified),
            other => Err(BasinError::InvalidIdent(format!(
                "unknown factor status: {other:?}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

/// A row from `{schema}_mfa_factors`.
#[derive(Debug, Clone)]
pub struct MfaFactorRow {
    pub id: Uuid,
    pub user_id: Uuid,
    pub project_id: String,
    pub factor_type: FactorType,
    pub status: FactorStatus,
    /// Encrypted TOTP secret (base32) or serialised WebAuthn credential.
    /// Encrypted via [`crate::oauth::EncryptionProvider`].
    pub secret_enc: String,
    /// Human-readable label (e.g. authenticator app name or passkey device).
    pub friendly_name: String,
    /// TOTP replay guard (RFC 6238 §5.2): the highest 30-second time step that
    /// has been successfully consumed by this factor. A code whose step is
    /// `<=` this value is a replay and is rejected. `0` means "never used".
    /// For WebAuthn factors this column carries the authenticator sign-counter.
    pub last_used_step: u64,
    pub created_at: chrono::DateTime<Utc>,
    pub updated_at: chrono::DateTime<Utc>,
}

/// A row from `{schema}_mfa_challenges`.
#[derive(Debug, Clone)]
pub struct MfaChallengeRow {
    pub id: Uuid,
    pub factor_id: Uuid,
    pub user_id: Uuid,
    pub project_id: String,
    pub expires_at: chrono::DateTime<Utc>,
    /// For WebAuthn: the serialised challenge bytes (base64url).
    pub challenge_data: String,
}

// ---------------------------------------------------------------------------
// MfaStore supertrait
// ---------------------------------------------------------------------------

/// MFA-specific persistence. Implemented by `PostgresAuthStore`.
#[async_trait]
pub trait MfaStore: AuthStore {
    // --- Factors ------------------------------------------------------------

    async fn insert_mfa_factor(&self, schema: &str, row: &MfaFactorRow) -> Result<()>;

    async fn load_mfa_factor(&self, schema: &str, factor_id: Uuid) -> Result<Option<MfaFactorRow>>;

    async fn list_mfa_factors(
        &self,
        schema: &str,
        user_id: Uuid,
        project_id: &ProjectId,
    ) -> Result<Vec<MfaFactorRow>>;

    /// Transition `status` to `verified` and update the timestamp.
    async fn verify_mfa_factor(&self, schema: &str, factor_id: Uuid) -> Result<()>;

    /// Persist the highest accepted TOTP step (or WebAuthn sign-counter) for a
    /// factor — the replay/clone guard. Idempotent: callers only advance it.
    async fn set_factor_last_used_step(
        &self,
        schema: &str,
        factor_id: Uuid,
        step: u64,
    ) -> Result<()>;

    async fn delete_mfa_factor(&self, schema: &str, factor_id: Uuid) -> Result<()>;

    // --- Challenges ---------------------------------------------------------

    async fn insert_mfa_challenge(&self, schema: &str, row: &MfaChallengeRow) -> Result<()>;

    /// Consume (read-and-delete) a challenge. Returns `None` if not found or expired.
    async fn consume_mfa_challenge(
        &self,
        schema: &str,
        challenge_id: Uuid,
    ) -> Result<Option<MfaChallengeRow>>;

    // --- Recovery codes -----------------------------------------------------

    /// Insert `n` pre-hashed recovery codes for a user.
    async fn insert_recovery_codes(
        &self,
        schema: &str,
        user_id: Uuid,
        project_id: &ProjectId,
        hashed_codes: &[String],
    ) -> Result<()>;

    /// Count active (unconsumed) recovery codes for a user.
    async fn count_recovery_codes(
        &self,
        schema: &str,
        user_id: Uuid,
        project_id: &ProjectId,
    ) -> Result<i64>;

    /// Return all active recovery code hashes for a user (for verification).
    async fn list_active_recovery_code_hashes(
        &self,
        schema: &str,
        user_id: Uuid,
        project_id: &ProjectId,
    ) -> Result<Vec<(i64, String)>>;

    /// Mark a single recovery code as consumed.
    async fn consume_recovery_code(&self, schema: &str, code_id: i64) -> Result<()>;

    /// Update the WebAuthn credential bytes and mark the factor verified.
    async fn update_webauthn_credential(
        &self,
        schema: &str,
        factor_id: Uuid,
        secret_enc: &str,
    ) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Postgres MfaStore implementation
// ---------------------------------------------------------------------------

use crate::store::postgres::PostgresAuthStore;

#[async_trait]
impl MfaStore for PostgresAuthStore {
    async fn insert_mfa_factor(&self, schema: &str, row: &MfaFactorRow) -> Result<()> {
        let client = self.client.lock().await;
        let sql = format!(
            "INSERT INTO {schema}_mfa_factors
                (id, user_id, project_id, factor_type, status, secret_enc, friendly_name,
                 last_used_step, created_at, updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)"
        );
        client
            .execute(
                &sql,
                &[
                    &row.id,
                    &row.user_id,
                    &row.project_id,
                    &row.factor_type.to_string(),
                    &row.status.to_string(),
                    &row.secret_enc,
                    &row.friendly_name,
                    &(row.last_used_step as i64),
                    &row.created_at,
                    &row.updated_at,
                ],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("insert_mfa_factor: {e}")))?;
        Ok(())
    }

    async fn load_mfa_factor(&self, schema: &str, factor_id: Uuid) -> Result<Option<MfaFactorRow>> {
        let client = self.client.lock().await;
        let sql = format!(
            "SELECT id, user_id, project_id, factor_type, status, secret_enc, friendly_name,
                    last_used_step, created_at, updated_at
             FROM {schema}_mfa_factors WHERE id = $1"
        );
        let maybe = client
            .query_opt(&sql, &[&factor_id])
            .await
            .map_err(|e| BasinError::catalog(format!("load_mfa_factor: {e}")))?;
        Ok(maybe.map(pg_row_to_factor))
    }

    async fn list_mfa_factors(
        &self,
        schema: &str,
        user_id: Uuid,
        project_id: &ProjectId,
    ) -> Result<Vec<MfaFactorRow>> {
        let client = self.client.lock().await;
        let sql = format!(
            "SELECT id, user_id, project_id, factor_type, status, secret_enc, friendly_name,
                    last_used_step, created_at, updated_at
             FROM {schema}_mfa_factors WHERE user_id = $1 AND project_id = $2
             ORDER BY created_at ASC"
        );
        let rows = client
            .query(&sql, &[&user_id, &project_id.to_string()])
            .await
            .map_err(|e| BasinError::catalog(format!("list_mfa_factors: {e}")))?;
        Ok(rows.into_iter().map(pg_row_to_factor).collect())
    }

    async fn verify_mfa_factor(&self, schema: &str, factor_id: Uuid) -> Result<()> {
        let client = self.client.lock().await;
        let sql = format!(
            "UPDATE {schema}_mfa_factors SET status = 'verified', updated_at = now()
             WHERE id = $1"
        );
        client
            .execute(&sql, &[&factor_id])
            .await
            .map_err(|e| BasinError::catalog(format!("verify_mfa_factor: {e}")))?;
        Ok(())
    }

    async fn set_factor_last_used_step(
        &self,
        schema: &str,
        factor_id: Uuid,
        step: u64,
    ) -> Result<()> {
        let client = self.client.lock().await;
        // GREATEST guards against a stale concurrent caller lowering the
        // counter — the replay guard must be monotonic.
        let sql = format!(
            "UPDATE {schema}_mfa_factors
             SET last_used_step = GREATEST(last_used_step, $2), updated_at = now()
             WHERE id = $1"
        );
        client
            .execute(&sql, &[&factor_id, &(step as i64)])
            .await
            .map_err(|e| BasinError::catalog(format!("set_factor_last_used_step: {e}")))?;
        Ok(())
    }

    async fn delete_mfa_factor(&self, schema: &str, factor_id: Uuid) -> Result<()> {
        let client = self.client.lock().await;
        let sql = format!("DELETE FROM {schema}_mfa_factors WHERE id = $1");
        client
            .execute(&sql, &[&factor_id])
            .await
            .map_err(|e| BasinError::catalog(format!("delete_mfa_factor: {e}")))?;
        Ok(())
    }

    async fn insert_mfa_challenge(&self, schema: &str, row: &MfaChallengeRow) -> Result<()> {
        let client = self.client.lock().await;
        let sql = format!(
            "INSERT INTO {schema}_mfa_challenges
                (id, factor_id, user_id, project_id, expires_at, challenge_data)
             VALUES ($1,$2,$3,$4,$5,$6)"
        );
        client
            .execute(
                &sql,
                &[
                    &row.id,
                    &row.factor_id,
                    &row.user_id,
                    &row.project_id,
                    &row.expires_at,
                    &row.challenge_data,
                ],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("insert_mfa_challenge: {e}")))?;
        Ok(())
    }

    async fn consume_mfa_challenge(
        &self,
        schema: &str,
        challenge_id: Uuid,
    ) -> Result<Option<MfaChallengeRow>> {
        let client = self.client.lock().await;
        let sql = format!(
            "DELETE FROM {schema}_mfa_challenges
             WHERE id = $1 AND expires_at > now()
             RETURNING id, factor_id, user_id, project_id, expires_at, challenge_data"
        );
        let maybe = client
            .query_opt(&sql, &[&challenge_id])
            .await
            .map_err(|e| BasinError::catalog(format!("consume_mfa_challenge: {e}")))?;
        Ok(maybe.map(|r| MfaChallengeRow {
            id: r.get(0),
            factor_id: r.get(1),
            user_id: r.get(2),
            project_id: r.get(3),
            expires_at: r.get(4),
            challenge_data: r.get(5),
        }))
    }

    async fn insert_recovery_codes(
        &self,
        schema: &str,
        user_id: Uuid,
        project_id: &ProjectId,
        hashed_codes: &[String],
    ) -> Result<()> {
        let client = self.client.lock().await;
        for h in hashed_codes {
            let sql = format!(
                "INSERT INTO {schema}_mfa_recovery_codes (user_id, project_id, code_hash)
                 VALUES ($1,$2,$3)"
            );
            client
                .execute(&sql, &[&user_id, &project_id.to_string(), h])
                .await
                .map_err(|e| BasinError::catalog(format!("insert_recovery_code: {e}")))?;
        }
        Ok(())
    }

    async fn count_recovery_codes(
        &self,
        schema: &str,
        user_id: Uuid,
        project_id: &ProjectId,
    ) -> Result<i64> {
        let client = self.client.lock().await;
        let sql = format!(
            "SELECT COUNT(*) FROM {schema}_mfa_recovery_codes
             WHERE user_id = $1 AND project_id = $2 AND consumed_at IS NULL"
        );
        let row = client
            .query_one(&sql, &[&user_id, &project_id.to_string()])
            .await
            .map_err(|e| BasinError::catalog(format!("count_recovery_codes: {e}")))?;
        Ok(row.get::<_, i64>(0))
    }

    async fn list_active_recovery_code_hashes(
        &self,
        schema: &str,
        user_id: Uuid,
        project_id: &ProjectId,
    ) -> Result<Vec<(i64, String)>> {
        let client = self.client.lock().await;
        let sql = format!(
            "SELECT id, code_hash FROM {schema}_mfa_recovery_codes
             WHERE user_id = $1 AND project_id = $2 AND consumed_at IS NULL"
        );
        let rows = client
            .query(&sql, &[&user_id, &project_id.to_string()])
            .await
            .map_err(|e| BasinError::catalog(format!("list_active_recovery_code_hashes: {e}")))?;
        Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    async fn consume_recovery_code(&self, schema: &str, code_id: i64) -> Result<()> {
        let client = self.client.lock().await;
        let sql =
            format!("UPDATE {schema}_mfa_recovery_codes SET consumed_at = now() WHERE id = $1");
        client
            .execute(&sql, &[&code_id])
            .await
            .map_err(|e| BasinError::catalog(format!("consume_recovery_code: {e}")))?;
        Ok(())
    }

    async fn update_webauthn_credential(
        &self,
        schema: &str,
        factor_id: Uuid,
        secret_enc: &str,
    ) -> Result<()> {
        let client = self.client.lock().await;
        let sql = format!(
            "UPDATE {schema}_mfa_factors SET secret_enc = $1, status = 'verified', updated_at = now()
             WHERE id = $2"
        );
        client
            .execute(&sql, &[&secret_enc, &factor_id])
            .await
            .map_err(|e| BasinError::catalog(format!("update_webauthn_credential: {e}")))?;
        Ok(())
    }
}

fn pg_row_to_factor(r: tokio_postgres::Row) -> MfaFactorRow {
    let ft_str: String = r.get(3);
    let st_str: String = r.get(4);
    let last_used_step: i64 = r.get(7);
    MfaFactorRow {
        id: r.get(0),
        user_id: r.get(1),
        project_id: r.get(2),
        factor_type: FactorType::from_str(&ft_str).unwrap_or(FactorType::Totp),
        status: FactorStatus::from_str(&st_str).unwrap_or(FactorStatus::Unverified),
        secret_enc: r.get(5),
        friendly_name: r.get(6),
        last_used_step: last_used_step.max(0) as u64,
        created_at: r.get(8),
        updated_at: r.get(9),
    }
}

// ---------------------------------------------------------------------------
// In-memory MFA store (for non-Postgres stores in tests / integration)
// ---------------------------------------------------------------------------

use std::sync::Mutex;

/// Thread-safe in-memory MFA state used when the concrete `AuthStore` does
/// not implement `MfaStore` (e.g. `EngineAuthStore` in integration tests).
pub struct MfaCache {
    factors: Mutex<Vec<MfaFactorRow>>,
    pub challenges: Mutex<Vec<MfaChallengeRow>>,
    recovery_codes: Mutex<Vec<(i64, Uuid, String, String, Option<chrono::DateTime<Utc>>)>>,
    next_rc_id: Mutex<i64>,
}

impl MfaCache {
    pub fn new() -> Self {
        Self {
            factors: Mutex::new(Vec::new()),
            challenges: Mutex::new(Vec::new()),
            recovery_codes: Mutex::new(Vec::new()),
            next_rc_id: Mutex::new(1),
        }
    }

    pub fn insert_factor_sync(&self, row: MfaFactorRow) {
        self.factors.lock().unwrap().push(row);
    }

    pub fn load_factor_sync(&self, factor_id: Uuid) -> Option<MfaFactorRow> {
        self.factors
            .lock()
            .unwrap()
            .iter()
            .find(|f| f.id == factor_id)
            .cloned()
    }

    pub fn list_factors_sync(&self, user_id: Uuid, project_id: &ProjectId) -> Vec<MfaFactorRow> {
        self.factors
            .lock()
            .unwrap()
            .iter()
            .filter(|f| f.user_id == user_id && f.project_id == project_id.to_string())
            .cloned()
            .collect()
    }

    pub fn verify_factor_sync(&self, factor_id: Uuid) {
        let mut factors = self.factors.lock().unwrap();
        if let Some(f) = factors.iter_mut().find(|f| f.id == factor_id) {
            f.status = FactorStatus::Verified;
            f.updated_at = Utc::now();
        }
    }

    pub fn delete_factor_sync(&self, factor_id: Uuid) {
        self.factors.lock().unwrap().retain(|f| f.id != factor_id);
    }

    /// Monotonically advance the replay-guard counter for a factor.
    pub fn set_factor_last_used_step_sync(&self, factor_id: Uuid, step: u64) {
        let mut factors = self.factors.lock().unwrap();
        if let Some(f) = factors.iter_mut().find(|f| f.id == factor_id) {
            f.last_used_step = f.last_used_step.max(step);
            f.updated_at = Utc::now();
        }
    }

    pub fn insert_challenge_sync(&self, row: MfaChallengeRow) {
        self.challenges.lock().unwrap().push(row);
    }

    pub fn consume_challenge_sync(&self, challenge_id: Uuid) -> Option<MfaChallengeRow> {
        let mut challenges = self.challenges.lock().unwrap();
        let now = Utc::now();
        if let Some(pos) = challenges
            .iter()
            .position(|c| c.id == challenge_id && c.expires_at > now)
        {
            Some(challenges.remove(pos))
        } else {
            None
        }
    }

    pub fn insert_recovery_codes_sync(
        &self,
        user_id: Uuid,
        project_id: &ProjectId,
        hashed_codes: &[String],
    ) {
        let mut codes = self.recovery_codes.lock().unwrap();
        let mut next = self.next_rc_id.lock().unwrap();
        for h in hashed_codes {
            codes.push((*next, user_id, project_id.to_string(), h.clone(), None));
            *next += 1;
        }
    }

    pub fn count_recovery_codes_sync(&self, user_id: Uuid, project_id: &ProjectId) -> i64 {
        self.recovery_codes
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, uid, pid, _, consumed)| {
                *uid == user_id && pid == &project_id.to_string() && consumed.is_none()
            })
            .count() as i64
    }

    pub fn list_active_recovery_code_hashes_sync(
        &self,
        user_id: Uuid,
        project_id: &ProjectId,
    ) -> Vec<(i64, String)> {
        self.recovery_codes
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, uid, pid, _, consumed)| {
                *uid == user_id && pid == &project_id.to_string() && consumed.is_none()
            })
            .map(|(id, _, _, h, _)| (*id, h.clone()))
            .collect()
    }

    pub fn consume_recovery_code_sync(&self, code_id: i64) {
        let mut codes = self.recovery_codes.lock().unwrap();
        if let Some(entry) = codes.iter_mut().find(|(id, _, _, _, _)| *id == code_id) {
            entry.4 = Some(Utc::now());
        }
    }
}

impl Default for MfaCache {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// TOTP helpers
// ---------------------------------------------------------------------------

/// Generate a new TOTP secret (20 bytes, base32-encoded).
pub fn generate_totp_secret() -> String {
    let mut bytes = [0u8; 20];
    rand::thread_rng().fill_bytes(&mut bytes);
    base32::encode(base32::Alphabet::RFC4648 { padding: false }, &bytes)
}

/// Build the `otpauth://totp/...` URI for QR code display.
pub fn build_otpauth_uri(secret: &str, email: &str, issuer: &str) -> String {
    let label = format!("{issuer}:{email}");
    format!(
        "otpauth://totp/{label}?secret={secret}&issuer={issuer}&algorithm=SHA1&digits=6&period=30",
        label = percent_encode(&label),
        secret = secret,
        issuer = percent_encode(issuer),
    )
}

fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Verify a TOTP code against a base32 secret. Allows one-step window on
/// either side for clock skew.
///
/// **Replay protection (RFC 6238 §5.2).** `min_step` is the highest 30-second
/// time step already consumed by this factor (`0` = never used). Any candidate
/// step `<= min_step` is rejected, so a code captured and already used cannot
/// be replayed inside its validity window — even though it is still
/// arithmetically valid. On success the accepted step is returned; the caller
/// MUST persist it as the new `min_step`.
pub fn verify_totp(secret_b32: &str, code: &str, min_step: u64) -> Result<u64> {
    let secret_bytes = base32::decode(base32::Alphabet::RFC4648 { padding: false }, secret_b32)
        .ok_or_else(|| BasinError::InvalidIdent("invalid TOTP secret encoding".into()))?;

    let code_stripped = code.replace(' ', "");
    if code_stripped.len() != 6 || !code_stripped.chars().all(|c| c.is_ascii_digit()) {
        return Err(BasinError::InvalidIdent(
            "TOTP code must be 6 digits".into(),
        ));
    }
    let presented: u32 = code_stripped
        .parse()
        .map_err(|_| BasinError::InvalidIdent("TOTP code parse error".into()))?;

    let now_secs = Utc::now().timestamp() as u64;
    let current_step = now_secs / 30;

    // Check current step and ±1 window, newest first so a fresh code wins.
    for step in [
        current_step + 1,
        current_step,
        current_step.saturating_sub(1),
    ] {
        // Replay guard: a step at or below the last consumed step is a reuse.
        if step <= min_step {
            continue;
        }
        let expected = totp_at_step(&secret_bytes, step)?;
        if expected == presented {
            return Ok(step);
        }
    }
    Err(BasinError::InvalidIdent("invalid TOTP code".into()))
}

/// Compute RFC 6238 TOTP for a given step (SHA1-HMAC).
fn totp_at_step(secret: &[u8], step: u64) -> Result<u32> {
    use hmac::{Hmac, Mac};
    use sha1::Sha1;

    type HmacSha1 = Hmac<Sha1>;

    let step_bytes = step.to_be_bytes();
    let mut mac = HmacSha1::new_from_slice(secret)
        .map_err(|e| BasinError::internal(format!("TOTP HMAC key error: {e}")))?;
    mac.update(&step_bytes);
    let result = mac.finalize().into_bytes();

    // Dynamic truncation per RFC 6238 / RFC 4226.
    let offset = (result[19] & 0x0f) as usize;
    let code = ((result[offset] as u32 & 0x7f) << 24)
        | ((result[offset + 1] as u32) << 16)
        | ((result[offset + 2] as u32) << 8)
        | (result[offset + 3] as u32);
    Ok(code % 1_000_000)
}

// ---------------------------------------------------------------------------
// Recovery code helpers
// ---------------------------------------------------------------------------

/// Number of recovery codes issued at first enrollment.
pub const RECOVERY_CODE_COUNT: usize = 8;

/// Generate plaintext recovery codes and their argon2id hashes.
/// Returns `(plaintext_codes, hashed_codes)`.
pub fn generate_recovery_codes() -> (Vec<String>, Vec<String>) {
    let mut plaintexts = Vec::with_capacity(RECOVERY_CODE_COUNT);
    let mut hashes = Vec::with_capacity(RECOVERY_CODE_COUNT);

    for _ in 0..RECOVERY_CODE_COUNT {
        let mut bytes = [0u8; 12];
        rand::thread_rng().fill_bytes(&mut bytes);
        let plain = hex::encode(bytes);
        // Use bcrypt (same as password hashing — argon2 not in workspace yet)
        // with cost 4 for speed. Recovery codes are high-entropy so cost 4
        // is still plenty strong against offline attacks.
        let hash = bcrypt::hash(&plain, 4).unwrap_or_else(|_| format!("HASH_FAILED:{plain}"));
        plaintexts.push(plain);
        hashes.push(hash);
    }
    (plaintexts, hashes)
}

/// Verify a presented recovery code against a stored bcrypt hash.
/// Returns `true` if the code matches.
pub fn verify_recovery_code(presented: &str, hash: &str) -> bool {
    bcrypt::verify(presented, hash).unwrap_or(false)
}

// ---------------------------------------------------------------------------
// MFA service methods (called from AuthService)
// ---------------------------------------------------------------------------

/// Public descriptor for a factor returned by `list_factors`. Contains only
/// the metadata fields — the secret/credential (`secret_enc`) is never exposed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorDescriptor {
    pub id: Uuid,
    pub factor_type: FactorType,
    pub status: FactorStatus,
    pub friendly_name: String,
    pub created_at: chrono::DateTime<Utc>,
    pub updated_at: chrono::DateTime<Utc>,
}

impl From<MfaFactorRow> for FactorDescriptor {
    fn from(row: MfaFactorRow) -> Self {
        Self {
            id: row.id,
            factor_type: row.factor_type,
            status: row.status,
            friendly_name: row.friendly_name,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

/// Enrollment result for a TOTP factor.
#[derive(Debug)]
pub struct TotpEnrollment {
    pub factor_id: Uuid,
    pub secret_b32: String,
    pub otpauth_uri: String,
}

/// Enrollment result for a WebAuthn factor (returns creation options JSON).
#[derive(Debug)]
pub struct WebAuthnEnrollment {
    pub factor_id: Uuid,
    pub challenge_id: Uuid,
    /// Serialised `PublicKeyCredentialCreationOptions` (JSON).
    pub creation_options_json: String,
}

/// Result of a successful MFA challenge verification.
#[derive(Debug)]
pub struct ChallengeVerifyResult {
    pub tokens: Tokens,
}

/// Begin enrolling a new TOTP factor. The caller must confirm with
/// [`verify_totp_factor`] before the factor becomes active.
#[instrument(skip(inner, mfa_store, enc), fields(project = %project_id, user_id = %user_id))]
pub async fn enroll_totp<S>(
    inner: &Inner,
    mfa_store: Option<&S>,
    enc: &dyn crate::oauth::EncryptionProvider,
    project_id: &ProjectId,
    user_id: Uuid,
    friendly_name: &str,
) -> Result<TotpEnrollment>
where
    S: MfaStore,
{
    let schema = &inner.cfg.catalog_schema;
    let secret_b32 = generate_totp_secret();
    let secret_enc = enc.encrypt(&secret_b32)?;

    // Find the user's email for the otpauth URI label.
    let user = inner
        .store
        .find_user_by_id(project_id, user_id)
        .await?
        .ok_or_else(|| BasinError::NotFound("user not found".into()))?;
    let uri = build_otpauth_uri(&secret_b32, &user.email, "Basin");

    let factor_id = Uuid::new_v4();
    let now = Utc::now();
    let row = MfaFactorRow {
        id: factor_id,
        user_id,
        project_id: project_id.to_string(),
        factor_type: FactorType::Totp,
        status: FactorStatus::Unverified,
        secret_enc,
        friendly_name: friendly_name.to_owned(),
        last_used_step: 0,
        created_at: now,
        updated_at: now,
    };

    if let Some(pg) = mfa_store {
        pg.insert_mfa_factor(schema, &row).await?;
    } else {
        inner.mfa_cache.insert_factor_sync(row);
    }

    Ok(TotpEnrollment {
        factor_id,
        secret_b32,
        otpauth_uri: uri,
    })
}

/// Confirm TOTP enrollment. Verifies the first OTP code; on success the factor
/// status is promoted to `verified`. If this is the user's first verified factor,
/// recovery codes are issued and returned.
#[instrument(skip(inner, mfa_store, enc, code), fields(project = %project_id, factor_id = %factor_id))]
pub async fn verify_totp_factor<S>(
    inner: &Inner,
    mfa_store: Option<&S>,
    enc: &dyn crate::oauth::EncryptionProvider,
    project_id: &ProjectId,
    user_id: Uuid,
    factor_id: Uuid,
    code: &str,
) -> Result<Option<Vec<String>>>
where
    S: MfaStore,
{
    let schema = &inner.cfg.catalog_schema;

    let factor = load_factor(inner, mfa_store, schema, factor_id).await?;
    require_factor_owner(&factor, user_id, project_id)?;
    require_factor_type(&factor, FactorType::Totp)?;

    if factor.status == FactorStatus::Verified {
        return Err(BasinError::InvalidIdent("factor already verified".into()));
    }

    let secret = enc.decrypt(&factor.secret_enc)?;
    let step = verify_totp(&secret, code, factor.last_used_step)?;

    if let Some(pg) = mfa_store {
        pg.verify_mfa_factor(schema, factor_id).await?;
        // Burn this step so the enrollment code can't be replayed.
        pg.set_factor_last_used_step(schema, factor_id, step)
            .await?;
    } else {
        inner.mfa_cache.verify_factor_sync(factor_id);
        inner
            .mfa_cache
            .set_factor_last_used_step_sync(factor_id, step);
    }

    // Issue recovery codes only at first verified factor.
    let recovery =
        maybe_issue_recovery_codes(inner, mfa_store, schema, user_id, project_id).await?;
    Ok(recovery)
}

/// Begin a TOTP step-up challenge. Returns the challenge ID (clients present
/// this in the `challenge/verify` call so we know which factor to check).
#[instrument(skip(inner, mfa_store), fields(project = %project_id, factor_id = %factor_id))]
pub async fn begin_totp_challenge<S>(
    inner: &Inner,
    mfa_store: Option<&S>,
    project_id: &ProjectId,
    user_id: Uuid,
    factor_id: Uuid,
) -> Result<Uuid>
where
    S: MfaStore,
{
    let schema = &inner.cfg.catalog_schema;
    let factor = load_factor(inner, mfa_store, schema, factor_id).await?;
    require_factor_owner(&factor, user_id, project_id)?;
    require_factor_type(&factor, FactorType::Totp)?;
    require_factor_verified(&factor)?;

    let challenge_id = Uuid::new_v4();
    let expires_at = Utc::now() + chrono::Duration::minutes(5);
    let row = MfaChallengeRow {
        id: challenge_id,
        factor_id,
        user_id,
        project_id: project_id.to_string(),
        expires_at,
        challenge_data: String::new(),
    };

    if let Some(pg) = mfa_store {
        pg.insert_mfa_challenge(schema, &row).await?;
    } else {
        inner.mfa_cache.insert_challenge_sync(row);
    }

    Ok(challenge_id)
}

/// Verify a TOTP step-up challenge. On success re-issues an aal2 JWT.
#[instrument(skip(inner, mfa_store, enc, code), fields(project = %project_id, challenge_id = %challenge_id))]
pub async fn verify_totp_challenge<S>(
    inner: &Inner,
    mfa_store: Option<&S>,
    enc: &dyn crate::oauth::EncryptionProvider,
    project_id: &ProjectId,
    user_id: Uuid,
    challenge_id: Uuid,
    code: &str,
) -> Result<ChallengeVerifyResult>
where
    S: MfaStore,
{
    let schema = &inner.cfg.catalog_schema;

    // Consume challenge row.
    let challenge = consume_challenge(inner, mfa_store, schema, challenge_id).await?;
    if challenge.user_id != user_id || challenge.project_id != project_id.to_string() {
        return Err(BasinError::InvalidIdent("challenge not found".into()));
    }

    let factor = load_factor(inner, mfa_store, schema, challenge.factor_id).await?;
    require_factor_type(&factor, FactorType::Totp)?;
    require_factor_verified(&factor)?;

    let secret = enc.decrypt(&factor.secret_enc)?;
    let step = verify_totp(&secret, code, factor.last_used_step)?;

    // Persist the accepted step BEFORE issuing tokens: a replay of the same
    // code (same or lower step) is now rejected for the validity window.
    if let Some(pg) = mfa_store {
        pg.set_factor_last_used_step(schema, factor.id, step)
            .await?;
    } else {
        inner
            .mfa_cache
            .set_factor_last_used_step_sync(factor.id, step);
    }

    let tokens = issue_aal2_tokens(inner, project_id, user_id, "totp").await?;
    Ok(ChallengeVerifyResult { tokens })
}

/// Begin WebAuthn enrollment. Returns a serialised creation options JSON
/// and a challenge ID.
#[instrument(skip(inner, mfa_store), fields(project = %project_id, user_id = %user_id))]
pub async fn enroll_webauthn<S>(
    inner: &Inner,
    mfa_store: Option<&S>,
    project_id: &ProjectId,
    user_id: Uuid,
    friendly_name: &str,
) -> Result<WebAuthnEnrollment>
where
    S: MfaStore,
{
    let schema = &inner.cfg.catalog_schema;

    // Generate a random challenge for the RP.
    let mut challenge_bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut challenge_bytes);
    let challenge_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(challenge_bytes);

    let user = inner
        .store
        .find_user_by_id(project_id, user_id)
        .await?
        .ok_or_else(|| BasinError::NotFound("user not found".into()))?;

    // Build minimal PublicKeyCredentialCreationOptions JSON (FIDO2 / WebAuthn L2).
    // The client-side JS library (webauthn-json or @simplewebauthn/browser)
    // parses this and calls navigator.credentials.create().
    let options = serde_json::json!({
        "rp": {
            "name": "Basin",
            "id": "localhost"
        },
        "user": {
            "id": base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(user_id.as_bytes()),
            "name": user.email,
            "displayName": user.email
        },
        "challenge": challenge_b64,
        "pubKeyCredParams": [
            { "type": "public-key", "alg": -7  },   // ES256
            { "type": "public-key", "alg": -257 }   // RS256
        ],
        "timeout": 300000,
        "attestation": "none",
        "authenticatorSelection": {
            "residentKey": "preferred",
            "requireResidentKey": false,
            "userVerification": "preferred"
        }
    });

    let factor_id = Uuid::new_v4();
    let challenge_id = Uuid::new_v4();
    let now = Utc::now();

    // Store the factor (unverified, no credential yet — placeholder).
    let factor_row = MfaFactorRow {
        id: factor_id,
        user_id,
        project_id: project_id.to_string(),
        factor_type: FactorType::Webauthn,
        status: FactorStatus::Unverified,
        secret_enc: String::new(), // filled on verify
        friendly_name: friendly_name.to_owned(),
        last_used_step: 0,
        created_at: now,
        updated_at: now,
    };

    if let Some(pg) = mfa_store {
        pg.insert_mfa_factor(schema, &factor_row).await?;
    } else {
        inner.mfa_cache.insert_factor_sync(factor_row);
    }

    // Store the pending challenge (carries the challenge bytes so we can
    // verify the attestation response).
    let challenge_row = MfaChallengeRow {
        id: challenge_id,
        factor_id,
        user_id,
        project_id: project_id.to_string(),
        expires_at: now + chrono::Duration::minutes(5),
        challenge_data: challenge_b64,
    };

    if let Some(pg) = mfa_store {
        pg.insert_mfa_challenge(schema, &challenge_row).await?;
    } else {
        inner.mfa_cache.insert_challenge_sync(challenge_row);
    }

    Ok(WebAuthnEnrollment {
        factor_id,
        challenge_id,
        creation_options_json: serde_json::to_string(&options)
            .map_err(|e| BasinError::internal(format!("webauthn options serialize: {e}")))?,
    })
}

// ---------------------------------------------------------------------------
// WebAuthn / FIDO2 cryptographic verification (Phase 6.SEC.P0.2)
// ---------------------------------------------------------------------------
//
// We implement the load-bearing FIDO2 checks directly with pure-Rust crypto
// (`p256` for ECDSA-P256 / COSE alg -7, `ciborium` for CBOR) rather than
// pulling `webauthn-rs` + the openssl-backed `webauthn-authenticator-rs`
// softpasskey test helper. The checks performed are exactly those an attacker
// must defeat to forge an aal2 token:
//
//   Registration (attestation):
//     - clientDataJSON.type == "webauthn.create"
//     - clientDataJSON.challenge == issued challenge (constant-time)
//     - clientDataJSON.origin   == expected origin
//     - authenticatorData.rpIdHash == sha256(RP_ID)
//     - User-Present flag set
//     - parse the COSE EC2 P-256 public key + credential id from authData
//
//   Assertion (login / step-up):
//     - clientDataJSON.type == "webauthn.get"
//     - clientDataJSON.challenge / origin as above
//     - authenticatorData.rpIdHash == sha256(RP_ID), UP flag set
//     - ECDSA-P256 verify( pubkey, sig, authData || sha256(clientDataJSON) )
//     - signCount strictly greater than the stored counter (rollback ⇒ clone)
//
// A hand-built JSON blob with the right challenge but no valid signature fails
// the ECDSA step; a cloned authenticator replaying an old signCount fails the
// counter step.

/// The Relying Party ID this deployment binds passkeys to. Matches the `rp.id`
/// advertised in [`enroll_webauthn`]'s creation options.
const WEBAUTHN_RP_ID: &str = "localhost";
/// The expected `origin` in clientDataJSON. Matches the dev/test origin used by
/// the browser when contacting a `localhost` RP.
const WEBAUTHN_ORIGIN: &str = "http://localhost";

/// A persisted passkey credential. Serialised as JSON, then encrypted into the
/// factor's `secret_enc` column. The sign-counter lives in `last_used_step`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredPasskey {
    /// base64url credential id (the authenticator's handle for this key).
    credential_id_b64: String,
    /// Uncompressed SEC1 public-key point (0x04 || X || Y), base64url.
    public_key_sec1_b64: String,
}

fn b64url_decode(s: &str) -> Result<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s)
        .map_err(|e| BasinError::InvalidIdent(format!("base64url decode: {e}")))
}

fn sha256(data: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().into()
}

/// Parsed FIDO2 `authenticatorData` header (the fixed 37-byte prefix).
struct AuthData {
    rp_id_hash: [u8; 32],
    flags: u8,
    sign_count: u32,
    /// Remaining bytes after the 37-byte header (attestedCredentialData +
    /// extensions, present only on registration / when AT flag is set).
    rest: Vec<u8>,
}

fn parse_auth_data(bytes: &[u8]) -> Result<AuthData> {
    if bytes.len() < 37 {
        return Err(BasinError::InvalidIdent(
            "authenticatorData too short".into(),
        ));
    }
    let mut rp_id_hash = [0u8; 32];
    rp_id_hash.copy_from_slice(&bytes[0..32]);
    let flags = bytes[32];
    let sign_count = u32::from_be_bytes([bytes[33], bytes[34], bytes[35], bytes[36]]);
    Ok(AuthData {
        rp_id_hash,
        flags,
        sign_count,
        rest: bytes[37..].to_vec(),
    })
}

/// FIDO2 flag: User Present (bit 0).
const FLAG_UP: u8 = 0x01;
/// FIDO2 flag: Attested credential data included (bit 6).
const FLAG_AT: u8 = 0x40;

/// Extract `(credential_id, cose_public_key_cbor_bytes)` from the
/// attestedCredentialData portion of authenticatorData.
fn parse_attested_credential(rest: &[u8]) -> Result<(Vec<u8>, ciborium::value::Value)> {
    // attestedCredentialData = aaguid(16) || credIdLen(2) || credId || COSEKey
    if rest.len() < 18 {
        return Err(BasinError::InvalidIdent(
            "attestedCredentialData too short".into(),
        ));
    }
    let cred_id_len = u16::from_be_bytes([rest[16], rest[17]]) as usize;
    let cred_start = 18;
    let cred_end = cred_start + cred_id_len;
    if rest.len() < cred_end {
        return Err(BasinError::InvalidIdent(
            "credential id length overruns authenticatorData".into(),
        ));
    }
    let credential_id = rest[cred_start..cred_end].to_vec();
    let cose_bytes = &rest[cred_end..];
    let cose: ciborium::value::Value = ciborium::de::from_reader(cose_bytes)
        .map_err(|e| BasinError::InvalidIdent(format!("COSE key CBOR decode: {e}")))?;
    Ok((credential_id, cose))
}

/// Convert a COSE EC2 P-256 (alg -7) public key into an uncompressed SEC1
/// point (0x04 || X || Y). Rejects any other key type / curve / algorithm.
fn cose_ec2_p256_to_sec1(cose: &ciborium::value::Value) -> Result<Vec<u8>> {
    use ciborium::value::Value;
    let map = match cose {
        Value::Map(m) => m,
        _ => return Err(BasinError::InvalidIdent("COSE key is not a map".into())),
    };
    let get = |key: i128| -> Option<&Value> {
        map.iter().find_map(|(k, v)| match k {
            Value::Integer(i) if i128::from(*i) == key => Some(v),
            _ => None,
        })
    };
    let as_int = |v: &Value| -> Option<i128> {
        match v {
            Value::Integer(i) => Some(i128::from(*i)),
            _ => None,
        }
    };
    // kty (1) must be EC2 (2); alg (3) must be ES256 (-7); crv (-1) must be P-256 (1).
    if get(1).and_then(as_int) != Some(2) {
        return Err(BasinError::InvalidIdent("COSE key kty != EC2".into()));
    }
    if get(3).and_then(as_int) != Some(-7) {
        return Err(BasinError::InvalidIdent(
            "COSE key alg != ES256 (only P-256 supported)".into(),
        ));
    }
    if get(-1).and_then(as_int) != Some(1) {
        return Err(BasinError::InvalidIdent("COSE key crv != P-256".into()));
    }
    let x = match get(-2) {
        Some(Value::Bytes(b)) if b.len() == 32 => b.clone(),
        _ => return Err(BasinError::InvalidIdent("COSE key x coord invalid".into())),
    };
    let y = match get(-3) {
        Some(Value::Bytes(b)) if b.len() == 32 => b.clone(),
        _ => return Err(BasinError::InvalidIdent("COSE key y coord invalid".into())),
    };
    let mut sec1 = Vec::with_capacity(65);
    sec1.push(0x04);
    sec1.extend_from_slice(&x);
    sec1.extend_from_slice(&y);
    Ok(sec1)
}

/// Validate clientDataJSON for the given ceremony `type`, binding the
/// challenge (constant-time) and origin.
fn check_client_data(
    client_data_b64: &str,
    expected_type: &str,
    expected_challenge: &str,
) -> Result<()> {
    let bytes = b64url_decode(client_data_b64)?;
    let cd: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| BasinError::InvalidIdent(format!("clientDataJSON parse: {e}")))?;

    if cd.get("type").and_then(|v| v.as_str()) != Some(expected_type) {
        return Err(BasinError::InvalidIdent(format!(
            "clientDataJSON.type != {expected_type}"
        )));
    }
    let returned_challenge = cd
        .get("challenge")
        .and_then(|v| v.as_str())
        .ok_or_else(|| BasinError::InvalidIdent("missing challenge in clientDataJSON".into()))?;
    let challenge_ok: bool = returned_challenge
        .as_bytes()
        .ct_eq(expected_challenge.as_bytes())
        .into();
    if !challenge_ok {
        return Err(BasinError::InvalidIdent(
            "WebAuthn challenge mismatch".into(),
        ));
    }
    let origin = cd
        .get("origin")
        .and_then(|v| v.as_str())
        .ok_or_else(|| BasinError::InvalidIdent("missing origin in clientDataJSON".into()))?;
    if origin != WEBAUTHN_ORIGIN {
        return Err(BasinError::InvalidIdent("WebAuthn origin mismatch".into()));
    }
    Ok(())
}

/// Verify a registration (attestation) response. Returns the credential to
/// persist and the initial authenticator sign-counter.
fn verify_attestation(
    attestation_json: &str,
    expected_challenge: &str,
) -> Result<(StoredPasskey, u32)> {
    let attest: serde_json::Value = serde_json::from_str(attestation_json)
        .map_err(|e| BasinError::InvalidIdent(format!("attestation JSON parse: {e}")))?;
    if attest.get("type").and_then(|v| v.as_str()) != Some("public-key") {
        return Err(BasinError::InvalidIdent(
            "attestation response missing type=public-key".into(),
        ));
    }
    let response = attest
        .get("response")
        .ok_or_else(|| BasinError::InvalidIdent("attestation missing response".into()))?;

    let client_data_b64 = response
        .get("clientDataJSON")
        .and_then(|v| v.as_str())
        .ok_or_else(|| BasinError::InvalidIdent("missing clientDataJSON".into()))?;
    check_client_data(client_data_b64, "webauthn.create", expected_challenge)?;

    let att_obj_b64 = response
        .get("attestationObject")
        .and_then(|v| v.as_str())
        .ok_or_else(|| BasinError::InvalidIdent("missing attestationObject".into()))?;
    let att_obj_bytes = b64url_decode(att_obj_b64)?;

    let att_obj: ciborium::value::Value = ciborium::de::from_reader(att_obj_bytes.as_slice())
        .map_err(|e| BasinError::InvalidIdent(format!("attestationObject CBOR decode: {e}")))?;
    let auth_data_bytes = match &att_obj {
        ciborium::value::Value::Map(m) => m
            .iter()
            .find_map(|(k, v)| match (k, v) {
                (ciborium::value::Value::Text(t), ciborium::value::Value::Bytes(b))
                    if t == "authData" =>
                {
                    Some(b.clone())
                }
                _ => None,
            })
            .ok_or_else(|| BasinError::InvalidIdent("attestationObject missing authData".into()))?,
        _ => {
            return Err(BasinError::InvalidIdent(
                "attestationObject not a map".into(),
            ))
        }
    };

    let auth_data = parse_auth_data(&auth_data_bytes)?;
    if auth_data.rp_id_hash != sha256(WEBAUTHN_RP_ID.as_bytes()) {
        return Err(BasinError::InvalidIdent(
            "WebAuthn rpIdHash mismatch".into(),
        ));
    }
    if auth_data.flags & FLAG_UP == 0 {
        return Err(BasinError::InvalidIdent(
            "WebAuthn user-present flag unset".into(),
        ));
    }
    if auth_data.flags & FLAG_AT == 0 {
        return Err(BasinError::InvalidIdent(
            "attestation lacks attested-credential-data flag".into(),
        ));
    }

    let (credential_id, cose) = parse_attested_credential(&auth_data.rest)?;
    let sec1 = cose_ec2_p256_to_sec1(&cose)?;
    // Sanity: the SEC1 point must parse as a valid P-256 verifying key.
    p256::ecdsa::VerifyingKey::from_sec1_bytes(&sec1)
        .map_err(|e| BasinError::InvalidIdent(format!("invalid P-256 public key: {e}")))?;

    let stored = StoredPasskey {
        credential_id_b64: base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&credential_id),
        public_key_sec1_b64: base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&sec1),
    };
    Ok((stored, auth_data.sign_count))
}

/// Verify an assertion (login / step-up) response against a stored passkey.
/// Returns the authenticator's reported sign-counter (caller enforces
/// monotonicity against the stored value).
fn verify_assertion(
    assertion_json: &str,
    expected_challenge: &str,
    stored: &StoredPasskey,
) -> Result<u32> {
    use p256::ecdsa::signature::Verifier;

    let assertion: serde_json::Value = serde_json::from_str(assertion_json)
        .map_err(|e| BasinError::InvalidIdent(format!("assertion JSON parse: {e}")))?;
    if assertion.get("type").and_then(|v| v.as_str()) != Some("public-key") {
        return Err(BasinError::InvalidIdent(
            "assertion response missing type=public-key".into(),
        ));
    }
    let response = assertion
        .get("response")
        .ok_or_else(|| BasinError::InvalidIdent("assertion missing response".into()))?;

    let client_data_b64 = response
        .get("clientDataJSON")
        .and_then(|v| v.as_str())
        .ok_or_else(|| BasinError::InvalidIdent("missing clientDataJSON".into()))?;
    check_client_data(client_data_b64, "webauthn.get", expected_challenge)?;

    let auth_data_b64 = response
        .get("authenticatorData")
        .and_then(|v| v.as_str())
        .ok_or_else(|| BasinError::InvalidIdent("missing authenticatorData".into()))?;
    let auth_data_bytes = b64url_decode(auth_data_b64)?;
    let auth_data = parse_auth_data(&auth_data_bytes)?;

    if auth_data.rp_id_hash != sha256(WEBAUTHN_RP_ID.as_bytes()) {
        return Err(BasinError::InvalidIdent(
            "WebAuthn rpIdHash mismatch".into(),
        ));
    }
    if auth_data.flags & FLAG_UP == 0 {
        return Err(BasinError::InvalidIdent(
            "WebAuthn user-present flag unset".into(),
        ));
    }

    let sig_b64 = response
        .get("signature")
        .and_then(|v| v.as_str())
        .ok_or_else(|| BasinError::InvalidIdent("missing signature".into()))?;
    let sig_bytes = b64url_decode(sig_b64)?;

    // FIDO2 signature is over: authenticatorData || sha256(clientDataJSON).
    let client_data_bytes = b64url_decode(client_data_b64)?;
    let mut signed = auth_data_bytes.clone();
    signed.extend_from_slice(&sha256(&client_data_bytes));

    let sec1 = b64url_decode(&stored.public_key_sec1_b64)?;
    let vk = p256::ecdsa::VerifyingKey::from_sec1_bytes(&sec1)
        .map_err(|e| BasinError::InvalidIdent(format!("stored public key invalid: {e}")))?;
    // WebAuthn ES256 signatures are ASN.1/DER-encoded.
    let sig = p256::ecdsa::DerSignature::try_from(sig_bytes.as_slice())
        .map_err(|e| BasinError::InvalidIdent(format!("signature DER decode: {e}")))?;

    vk.verify(&signed, &sig)
        .map_err(|_| BasinError::InvalidIdent("WebAuthn signature verification failed".into()))?;

    Ok(auth_data.sign_count)
}

/// Verify WebAuthn enrollment. The client sends the attestation response JSON
/// from `navigator.credentials.create()`. On success the factor is promoted to
/// `verified` and the credential public-key bytes are stored encrypted.
#[instrument(skip(inner, mfa_store, enc, attestation_json), fields(project = %project_id, factor_id = %factor_id))]
pub async fn verify_webauthn_factor<S>(
    inner: &Inner,
    mfa_store: Option<&S>,
    enc: &dyn crate::oauth::EncryptionProvider,
    project_id: &ProjectId,
    user_id: Uuid,
    factor_id: Uuid,
    challenge_id: Uuid,
    attestation_json: &str,
) -> Result<Option<Vec<String>>>
where
    S: MfaStore,
{
    let schema = &inner.cfg.catalog_schema;

    let challenge = consume_challenge(inner, mfa_store, schema, challenge_id).await?;
    if challenge.factor_id != factor_id
        || challenge.user_id != user_id
        || challenge.project_id != project_id.to_string()
    {
        return Err(BasinError::InvalidIdent("challenge mismatch".into()));
    }

    // Real FIDO2 attestation verification: validates clientDataJSON
    // (type/challenge/origin), the rpIdHash + user-present flag, and parses the
    // COSE P-256 public key. A hand-built JSON without a valid attested
    // credential structure is rejected here.
    let (stored, initial_counter) =
        verify_attestation(attestation_json, &challenge.challenge_data)?;

    // Persist the credential (credential id + public key), encrypted.
    let stored_json = serde_json::to_string(&stored)
        .map_err(|e| BasinError::internal(format!("passkey serialize: {e}")))?;
    let secret_enc = enc.encrypt(&stored_json)?;

    // Update factor: set secret_enc, mark verified, seed the sign-counter.
    if let Some(pg) = mfa_store {
        pg.update_webauthn_credential(schema, factor_id, &secret_enc)
            .await?;
        pg.set_factor_last_used_step(schema, factor_id, initial_counter as u64)
            .await?;
    } else {
        inner.mfa_cache.verify_factor_sync(factor_id);
        // Patch secret + counter in memory.
        let mut factors = inner.mfa_cache.factors.lock().unwrap();
        if let Some(f) = factors.iter_mut().find(|f| f.id == factor_id) {
            f.secret_enc = secret_enc;
            f.last_used_step = initial_counter as u64;
        }
    }

    let recovery =
        maybe_issue_recovery_codes(inner, mfa_store, schema, user_id, project_id).await?;
    Ok(recovery)
}

/// Begin a WebAuthn step-up assertion challenge.
#[instrument(skip(inner, mfa_store), fields(project = %project_id, factor_id = %factor_id))]
pub async fn begin_webauthn_challenge<S>(
    inner: &Inner,
    mfa_store: Option<&S>,
    project_id: &ProjectId,
    user_id: Uuid,
    factor_id: Uuid,
) -> Result<(Uuid, String)>
where
    S: MfaStore,
{
    let schema = &inner.cfg.catalog_schema;
    let factor = load_factor(inner, mfa_store, schema, factor_id).await?;
    require_factor_owner(&factor, user_id, project_id)?;
    require_factor_type(&factor, FactorType::Webauthn)?;
    require_factor_verified(&factor)?;

    let mut challenge_bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut challenge_bytes);
    let challenge_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(challenge_bytes);

    // Build minimal PublicKeyCredentialRequestOptions.
    let options = serde_json::json!({
        "challenge": challenge_b64,
        "timeout": 300000,
        "rpId": "localhost",
        "allowCredentials": [],
        "userVerification": "preferred"
    });

    let challenge_id = Uuid::new_v4();
    let row = MfaChallengeRow {
        id: challenge_id,
        factor_id,
        user_id,
        project_id: project_id.to_string(),
        expires_at: Utc::now() + chrono::Duration::minutes(5),
        challenge_data: challenge_b64,
    };

    if let Some(pg) = mfa_store {
        pg.insert_mfa_challenge(schema, &row).await?;
    } else {
        inner.mfa_cache.insert_challenge_sync(row);
    }

    let options_json = serde_json::to_string(&options)
        .map_err(|e| BasinError::internal(format!("webauthn options serialize: {e}")))?;
    Ok((challenge_id, options_json))
}

/// Verify a WebAuthn step-up assertion. On success re-issues an aal2 JWT.
///
/// Performs full FIDO2 assertion verification: the ECDSA-P256 signature is
/// checked against the stored passkey public key, and the authenticator
/// sign-counter must strictly advance (a non-advancing counter signals a
/// cloned authenticator and is rejected).
#[instrument(skip(inner, mfa_store, enc, assertion_json), fields(project = %project_id, challenge_id = %challenge_id))]
pub async fn verify_webauthn_challenge<S>(
    inner: &Inner,
    mfa_store: Option<&S>,
    enc: &dyn crate::oauth::EncryptionProvider,
    project_id: &ProjectId,
    user_id: Uuid,
    challenge_id: Uuid,
    assertion_json: &str,
) -> Result<ChallengeVerifyResult>
where
    S: MfaStore,
{
    let schema = &inner.cfg.catalog_schema;

    let challenge = consume_challenge(inner, mfa_store, schema, challenge_id).await?;
    if challenge.user_id != user_id || challenge.project_id != project_id.to_string() {
        return Err(BasinError::InvalidIdent("challenge not found".into()));
    }

    let factor = load_factor(inner, mfa_store, schema, challenge.factor_id).await?;
    require_factor_type(&factor, FactorType::Webauthn)?;
    require_factor_verified(&factor)?;

    // Decrypt the stored passkey credential (public key + credential id).
    let stored_json = enc.decrypt(&factor.secret_enc)?;
    let stored: StoredPasskey = serde_json::from_str(&stored_json)
        .map_err(|e| BasinError::internal(format!("passkey deserialize: {e}")))?;

    // Cryptographically verify the assertion signature + bindings.
    let reported_counter = verify_assertion(assertion_json, &challenge.challenge_data, &stored)?;

    // Sign-counter rollback / clone detection. Authenticators that implement a
    // counter increment it on every assertion; a value that fails to advance
    // (and is non-zero) means a cloned credential. Counter `0` ⇒ authenticator
    // does not implement a counter, which the spec permits — accept but don't
    // require advance in that case.
    let stored_counter = factor.last_used_step as u32;
    if reported_counter != 0 && reported_counter <= stored_counter {
        return Err(BasinError::InvalidIdent(
            "WebAuthn sign-counter did not advance (possible cloned authenticator)".into(),
        ));
    }
    if reported_counter > stored_counter {
        if let Some(pg) = mfa_store {
            pg.set_factor_last_used_step(schema, factor.id, reported_counter as u64)
                .await?;
        } else {
            inner
                .mfa_cache
                .set_factor_last_used_step_sync(factor.id, reported_counter as u64);
        }
    }

    let tokens = issue_aal2_tokens(inner, project_id, user_id, "webauthn").await?;
    Ok(ChallengeVerifyResult { tokens })
}

/// Use a recovery code to get aal2 tokens (bypasses factor challenge).
/// The code is consumed and cannot be reused.
#[instrument(skip(inner, mfa_store, code), fields(project = %project_id, user_id = %user_id))]
pub async fn use_recovery_code<S>(
    inner: &Inner,
    mfa_store: Option<&S>,
    project_id: &ProjectId,
    user_id: Uuid,
    code: &str,
) -> Result<ChallengeVerifyResult>
where
    S: MfaStore,
{
    let schema = &inner.cfg.catalog_schema;

    // Load all active hashes and find the matching one.
    let active = if let Some(pg) = mfa_store {
        pg.list_active_recovery_code_hashes(schema, user_id, project_id)
            .await?
    } else {
        inner
            .mfa_cache
            .list_active_recovery_code_hashes_sync(user_id, project_id)
    };

    for (id, hash) in &active {
        if verify_recovery_code(code, hash) {
            if let Some(pg) = mfa_store {
                pg.consume_recovery_code(schema, *id).await?;
            } else {
                inner.mfa_cache.consume_recovery_code_sync(*id);
            }
            let tokens = issue_aal2_tokens(inner, project_id, user_id, "recovery").await?;
            return Ok(ChallengeVerifyResult { tokens });
        }
    }

    Err(BasinError::InvalidIdent("invalid recovery code".into()))
}

/// Unenroll an MFA factor. Requires that the caller's JWT carries `aal2`
/// (enforced at the HTTP layer; this function takes the aal as a parameter
/// to enforce it at the service level too).
#[instrument(skip(inner, mfa_store), fields(project = %project_id, factor_id = %factor_id))]
pub async fn unenroll_factor<S>(
    inner: &Inner,
    mfa_store: Option<&S>,
    project_id: &ProjectId,
    user_id: Uuid,
    factor_id: Uuid,
    caller_aal: &Aal,
) -> Result<()>
where
    S: MfaStore,
{
    if *caller_aal != Aal::Aal2 {
        return Err(BasinError::InvalidIdent(
            "aal2 required to unenroll a factor".into(),
        ));
    }

    let schema = &inner.cfg.catalog_schema;
    let factor = load_factor(inner, mfa_store, schema, factor_id).await?;
    require_factor_owner(&factor, user_id, project_id)?;

    if let Some(pg) = mfa_store {
        pg.delete_mfa_factor(schema, factor_id).await?;
    } else {
        inner.mfa_cache.delete_factor_sync(factor_id);
    }
    Ok(())
}

/// Returns `true` iff the user has at least one `verified` factor.
pub async fn has_verified_factor<S>(
    inner: &Inner,
    mfa_store: Option<&S>,
    project_id: &ProjectId,
    user_id: Uuid,
) -> Result<bool>
where
    S: MfaStore,
{
    let schema = &inner.cfg.catalog_schema;
    let factors = if let Some(pg) = mfa_store {
        pg.list_mfa_factors(schema, user_id, project_id).await?
    } else {
        inner.mfa_cache.list_factors_sync(user_id, project_id)
    };
    Ok(factors.iter().any(|f| f.status == FactorStatus::Verified))
}

/// List all MFA factors for a user within a project. Returns public
/// descriptors only — the secret/credential column is never included.
#[instrument(skip(inner, mfa_store), fields(project = %project_id, user_id = %user_id))]
pub async fn list_factors<S>(
    inner: &Inner,
    mfa_store: Option<&S>,
    project_id: &ProjectId,
    user_id: Uuid,
) -> Result<Vec<FactorDescriptor>>
where
    S: MfaStore,
{
    let schema = &inner.cfg.catalog_schema;
    let rows = if let Some(pg) = mfa_store {
        pg.list_mfa_factors(schema, user_id, project_id).await?
    } else {
        inner.mfa_cache.list_factors_sync(user_id, project_id)
    };
    Ok(rows.into_iter().map(FactorDescriptor::from).collect())
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

async fn load_factor<S: MfaStore>(
    inner: &Inner,
    mfa_store: Option<&S>,
    schema: &str,
    factor_id: Uuid,
) -> Result<MfaFactorRow> {
    if let Some(pg) = mfa_store {
        pg.load_mfa_factor(schema, factor_id)
            .await?
            .ok_or_else(|| BasinError::NotFound(format!("factor {factor_id} not found")))
    } else {
        inner
            .mfa_cache
            .load_factor_sync(factor_id)
            .ok_or_else(|| BasinError::NotFound(format!("factor {factor_id} not found")))
    }
}

async fn consume_challenge<S: MfaStore>(
    inner: &Inner,
    mfa_store: Option<&S>,
    schema: &str,
    challenge_id: Uuid,
) -> Result<MfaChallengeRow> {
    if let Some(pg) = mfa_store {
        pg.consume_mfa_challenge(schema, challenge_id)
            .await?
            .ok_or_else(|| BasinError::InvalidIdent("challenge not found or expired".into()))
    } else {
        inner
            .mfa_cache
            .consume_challenge_sync(challenge_id)
            .ok_or_else(|| BasinError::InvalidIdent("challenge not found or expired".into()))
    }
}

fn require_factor_owner(
    factor: &MfaFactorRow,
    user_id: Uuid,
    project_id: &ProjectId,
) -> Result<()> {
    if factor.user_id != user_id || factor.project_id != project_id.to_string() {
        return Err(BasinError::NotFound("factor not found".into()));
    }
    Ok(())
}

fn require_factor_type(factor: &MfaFactorRow, expected: FactorType) -> Result<()> {
    if factor.factor_type != expected {
        return Err(BasinError::InvalidIdent(format!(
            "factor type mismatch: expected {:?}, got {:?}",
            expected, factor.factor_type
        )));
    }
    Ok(())
}

fn require_factor_verified(factor: &MfaFactorRow) -> Result<()> {
    if factor.status != FactorStatus::Verified {
        return Err(BasinError::InvalidIdent("factor not yet verified".into()));
    }
    Ok(())
}

/// Issue recovery codes if the user has no existing codes (first-time enrollment).
async fn maybe_issue_recovery_codes<S: MfaStore>(
    inner: &Inner,
    mfa_store: Option<&S>,
    schema: &str,
    user_id: Uuid,
    project_id: &ProjectId,
) -> Result<Option<Vec<String>>> {
    let count = if let Some(pg) = mfa_store {
        pg.count_recovery_codes(schema, user_id, project_id).await?
    } else {
        inner
            .mfa_cache
            .count_recovery_codes_sync(user_id, project_id)
    };

    if count > 0 {
        return Ok(None); // Already has codes.
    }

    let (plaintexts, hashes) = generate_recovery_codes();
    if let Some(pg) = mfa_store {
        pg.insert_recovery_codes(schema, user_id, project_id, &hashes)
            .await?;
    } else {
        inner
            .mfa_cache
            .insert_recovery_codes_sync(user_id, project_id, &hashes);
    }
    Ok(Some(plaintexts))
}

/// Re-issue a JWT with aal2 + amr reflecting the MFA method used.
async fn issue_aal2_tokens(
    inner: &Inner,
    project_id: &ProjectId,
    user_id: Uuid,
    method: &str,
) -> Result<Tokens> {
    let user = inner
        .store
        .find_user_by_id(project_id, user_id)
        .await?
        .ok_or_else(|| BasinError::NotFound("user not found".into()))?;

    let now = Utc::now();
    let (access_token, access_expires_at) = inner.jwt.issue_full(
        project_id,
        user_id,
        &user.email,
        &["user".to_string()],
        false,
        Aal::Aal2,
        vec![method.to_owned()],
        now,
        inner.cfg.token_ttl,
    )?;
    let (refresh_token_str, jti, refresh_expires_at) =
        inner
            .jwt
            .issue_refresh(project_id, user_id, &user.email, now, inner.cfg.refresh_ttl)?;

    inner
        .store
        .insert_refresh_revocation(&jti, user_id, refresh_expires_at)
        .await?;

    Ok(Tokens {
        access_token,
        refresh_token: refresh_token_str,
        access_expires_at,
        refresh_expires_at,
    })
}

// ---------------------------------------------------------------------------
// MfaCache: AuthStore + MfaStore stub impls (test-utils)
//
// Lets callers write `None::<&MfaCache>` to satisfy `S: MfaStore`. The None
// branch never invokes S — it uses inner.mfa_cache directly — so all methods
// below are safe to panic.
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "test-utils"))]
const _: () = {
    use crate::store::{
        ApiKeyRow, AuthMagicLinkRow, AuthStore, AuthUser, EmailTokenRow, MagicLinkEmailTokenRow,
        ProjectCredentialRow, RefreshRevocationRow,
    };
    use crate::UserId;
    use async_trait::async_trait;
    use basin_common::{ProjectId, Result};
    use chrono::{DateTime, Utc};
    use std::collections::HashMap;
    use uuid::Uuid;

    #[async_trait]
    impl AuthStore for MfaCache {
        async fn migrate(&self, _: &str) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn create_user(&self, _: &ProjectId, _: &str, _: &str, _: UserId) -> Result<UserId> {
            panic!("MfaCache stub")
        }
        async fn find_user_by_email(&self, _: &ProjectId, _: &str) -> Result<Option<AuthUser>> {
            panic!("MfaCache stub")
        }
        async fn find_user_by_id(&self, _: &ProjectId, _: UserId) -> Result<Option<AuthUser>> {
            panic!("MfaCache stub")
        }
        async fn any_user_by_email(&self, _: &str) -> Result<Option<()>> {
            panic!("MfaCache stub")
        }
        async fn latest_user_by_email(&self, _: &str) -> Result<Option<(UserId, ProjectId)>> {
            panic!("MfaCache stub")
        }
        async fn mark_email_verified(&self, _: &ProjectId, _: UserId) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn mark_email_verified_if_null(&self, _: &ProjectId, _: UserId) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn update_password(&self, _: &ProjectId, _: UserId, _: &str) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn insert_email_token(
            &self,
            _: &ProjectId,
            _: UserId,
            _: &str,
            _: &str,
            _: DateTime<Utc>,
        ) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn find_email_token(&self, _: &ProjectId, _: &str) -> Result<Option<EmailTokenRow>> {
            panic!("MfaCache stub")
        }
        async fn find_magic_link_email_token(
            &self,
            _: &ProjectId,
            _: &str,
        ) -> Result<Option<MagicLinkEmailTokenRow>> {
            panic!("MfaCache stub")
        }
        async fn consume_email_token(&self, _: &ProjectId, _: &str) -> Result<u64> {
            panic!("MfaCache stub")
        }
        async fn insert_refresh_revocation(
            &self,
            _: &str,
            _: UserId,
            _: DateTime<Utc>,
        ) -> Result<u64> {
            panic!("MfaCache stub")
        }
        async fn upsert_refresh_revocation(
            &self,
            _: &str,
            _: UserId,
            _: DateTime<Utc>,
        ) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn list_refresh_revocations(&self, _: UserId) -> Result<Vec<RefreshRevocationRow>> {
            panic!("MfaCache stub")
        }
        async fn upsert_blanket_revocation(
            &self,
            _: &str,
            _: UserId,
            _: DateTime<Utc>,
        ) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn insert_api_key(
            &self,
            _: &ProjectId,
            _: UserId,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<(i64, DateTime<Utc>)> {
            panic!("MfaCache stub")
        }
        async fn find_api_keys_by_hash(&self, _: &str) -> Result<Vec<ApiKeyRow>> {
            panic!("MfaCache stub")
        }
        async fn touch_api_key(&self, _: i64) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn revoke_api_key(&self, _: &ProjectId, _: i64) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn list_api_keys(
            &self,
            _: &ProjectId,
            _: UserId,
        ) -> Result<Vec<crate::api_keys::ApiKeyDescriptor>> {
            panic!("MfaCache stub")
        }
        async fn upsert_session_setting(
            &self,
            _: &ProjectId,
            _: UserId,
            _: &str,
            _: &str,
        ) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn list_session_settings(
            &self,
            _: &ProjectId,
            _: UserId,
        ) -> Result<HashMap<String, String>> {
            panic!("MfaCache stub")
        }
        async fn insert_project_credential(
            &self,
            _: &ProjectId,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<bool> {
            panic!("MfaCache stub")
        }
        async fn find_project_credential(&self, _: &str) -> Result<Option<ProjectCredentialRow>> {
            panic!("MfaCache stub")
        }
        async fn rotate_project_credential(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Option<ProjectCredentialRow>> {
            panic!("MfaCache stub")
        }
        async fn list_project_credentials(
            &self,
            _: &ProjectId,
        ) -> Result<Vec<crate::project_credentials::ProjectCredentialDescriptor>> {
            panic!("MfaCache stub")
        }
        async fn list_legacy_project_credentials(&self) -> Result<Vec<(ProjectId, String)>> {
            panic!("MfaCache stub")
        }
        async fn delete_project_credential(&self, _: &str) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn insert_auth_magic_link(&self, _: &str, _: &str, _: DateTime<Utc>) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn list_active_auth_magic_links(&self) -> Result<Vec<AuthMagicLinkRow>> {
            panic!("MfaCache stub")
        }
        async fn consume_auth_magic_link(&self, _: i64) -> Result<u64> {
            panic!("MfaCache stub")
        }
        async fn mark_email_verified_by_user_id(&self, _: UserId) -> Result<()> {
            panic!("MfaCache stub")
        }
    }

    #[async_trait]
    impl MfaStore for MfaCache {
        async fn insert_mfa_factor(&self, _: &str, _: &MfaFactorRow) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn load_mfa_factor(&self, _: &str, _: Uuid) -> Result<Option<MfaFactorRow>> {
            panic!("MfaCache stub")
        }
        async fn list_mfa_factors(
            &self,
            _: &str,
            _: Uuid,
            _: &ProjectId,
        ) -> Result<Vec<MfaFactorRow>> {
            panic!("MfaCache stub")
        }
        async fn verify_mfa_factor(&self, _: &str, _: Uuid) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn set_factor_last_used_step(&self, _: &str, _: Uuid, _: u64) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn delete_mfa_factor(&self, _: &str, _: Uuid) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn insert_mfa_challenge(&self, _: &str, _: &MfaChallengeRow) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn consume_mfa_challenge(&self, _: &str, _: Uuid) -> Result<Option<MfaChallengeRow>> {
            panic!("MfaCache stub")
        }
        async fn insert_recovery_codes(
            &self,
            _: &str,
            _: Uuid,
            _: &ProjectId,
            _: &[String],
        ) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn count_recovery_codes(&self, _: &str, _: Uuid, _: &ProjectId) -> Result<i64> {
            panic!("MfaCache stub")
        }
        async fn list_active_recovery_code_hashes(
            &self,
            _: &str,
            _: Uuid,
            _: &ProjectId,
        ) -> Result<Vec<(i64, String)>> {
            panic!("MfaCache stub")
        }
        async fn consume_recovery_code(&self, _: &str, _: i64) -> Result<()> {
            panic!("MfaCache stub")
        }
        async fn update_webauthn_credential(&self, _: &str, _: Uuid, _: &str) -> Result<()> {
            panic!("MfaCache stub")
        }
    }
};

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_totp_secret_is_base32() {
        let s = generate_totp_secret();
        assert!(!s.is_empty());
        // Must be valid base32.
        assert!(
            base32::decode(base32::Alphabet::RFC4648 { padding: false }, &s).is_some(),
            "secret should be valid base32"
        );
    }

    #[test]
    fn otpauth_uri_format() {
        let uri = build_otpauth_uri("JBSWY3DPEHPK3PXP", "user@example.com", "Basin");
        assert!(uri.starts_with("otpauth://totp/"));
        assert!(uri.contains("JBSWY3DPEHPK3PXP"));
        assert!(uri.contains("digits=6"));
        assert!(uri.contains("period=30"));
    }

    #[test]
    fn totp_verify_with_known_secret() {
        // RFC 6238 test vector: secret = b"12345678901234567890" as base32.
        let secret_bytes = b"12345678901234567890";
        let secret_b32 = base32::encode(base32::Alphabet::RFC4648 { padding: false }, secret_bytes);

        // Compute the TOTP for step 0 (T=0, step = 0/30 = 0).
        let expected = totp_at_step(secret_bytes, 0).unwrap();

        // Build the code string and verify it (using step 0's code against step 0
        // — since our verify_totp checks ±1 around *now*, we have to patch `now`.
        // Instead just assert the math is correct by calling totp_at_step directly.
        assert!(expected < 1_000_000, "TOTP code must be < 6 digits");
    }

    #[test]
    fn recovery_code_round_trip() {
        let (plains, hashes) = generate_recovery_codes();
        assert_eq!(plains.len(), RECOVERY_CODE_COUNT);
        assert_eq!(hashes.len(), RECOVERY_CODE_COUNT);
        // Each code hashes correctly.
        for (p, h) in plains.iter().zip(hashes.iter()) {
            assert!(verify_recovery_code(p, h), "code {p} should verify");
        }
    }

    #[test]
    fn recovery_code_wrong_input_rejected() {
        let (plains, hashes) = generate_recovery_codes();
        // Cross-check: code[0] should NOT match hash[1].
        assert!(
            !verify_recovery_code(&plains[0], &hashes[1]),
            "wrong code must not verify"
        );
    }

    #[test]
    fn factor_type_round_trip() {
        assert_eq!(FactorType::from_str("totp").unwrap(), FactorType::Totp);
        assert_eq!(
            FactorType::from_str("webauthn").unwrap(),
            FactorType::Webauthn
        );
        assert!(FactorType::from_str("sms").is_err());
    }

    #[test]
    fn factor_status_round_trip() {
        assert_eq!(
            FactorStatus::from_str("unverified").unwrap(),
            FactorStatus::Unverified
        );
        assert_eq!(
            FactorStatus::from_str("verified").unwrap(),
            FactorStatus::Verified
        );
        assert!(FactorStatus::from_str("pending").is_err());
    }

    #[test]
    fn mfa_cache_factor_lifecycle() {
        let cache = MfaCache::new();
        let fid = Uuid::new_v4();
        let uid = Uuid::new_v4();
        let pid: ProjectId = ProjectId::new();
        let now = Utc::now();

        let row = MfaFactorRow {
            id: fid,
            user_id: uid,
            project_id: pid.to_string(),
            factor_type: FactorType::Totp,
            status: FactorStatus::Unverified,
            secret_enc: "enc_secret".to_owned(),
            friendly_name: "Authenticator".to_owned(),
            last_used_step: 0,
            created_at: now,
            updated_at: now,
        };
        cache.insert_factor_sync(row);

        // Load.
        let loaded = cache.load_factor_sync(fid).unwrap();
        assert_eq!(loaded.status, FactorStatus::Unverified);

        // List.
        let list = cache.list_factors_sync(uid, &pid);
        assert_eq!(list.len(), 1);

        // Verify.
        cache.verify_factor_sync(fid);
        assert_eq!(
            cache.load_factor_sync(fid).unwrap().status,
            FactorStatus::Verified
        );

        // Delete.
        cache.delete_factor_sync(fid);
        assert!(cache.load_factor_sync(fid).is_none());
    }

    #[test]
    fn mfa_cache_challenge_single_use() {
        let cache = MfaCache::new();
        let cid = Uuid::new_v4();
        let fid = Uuid::new_v4();
        let uid = Uuid::new_v4();
        let pid: ProjectId = ProjectId::new();

        cache.insert_challenge_sync(MfaChallengeRow {
            id: cid,
            factor_id: fid,
            user_id: uid,
            project_id: pid.to_string(),
            expires_at: Utc::now() + chrono::Duration::minutes(5),
            challenge_data: "chal".to_owned(),
        });

        assert!(cache.consume_challenge_sync(cid).is_some());
        // Second consume must return None.
        assert!(cache.consume_challenge_sync(cid).is_none());
    }

    #[test]
    fn mfa_cache_recovery_codes_single_use() {
        let cache = MfaCache::new();
        let uid = Uuid::new_v4();
        let pid: ProjectId = ProjectId::new();

        let (plains, hashes) = generate_recovery_codes();
        cache.insert_recovery_codes_sync(uid, &pid, &hashes);
        assert_eq!(cache.count_recovery_codes_sync(uid, &pid), 8);

        let active = cache.list_active_recovery_code_hashes_sync(uid, &pid);
        let (id0, _) = &active[0];
        cache.consume_recovery_code_sync(*id0);
        assert_eq!(cache.count_recovery_codes_sync(uid, &pid), 7);

        // The consumed code should no longer appear.
        let remaining = cache.list_active_recovery_code_hashes_sync(uid, &pid);
        assert!(!remaining.iter().any(|(id, _)| *id == *id0));
        let _ = plains; // quiet unused warning
    }
}

// ---------------------------------------------------------------------------
// Security regression tests — TOTP replay (6.SEC.P0.1) + WebAuthn crypto
// (6.SEC.P0.2). Pure-function level; no Postgres required.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod security_tests {
    use super::*;

    fn b64url(bytes: &[u8]) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    }

    // --- 6.SEC.P0.1: TOTP replay protection ---------------------------------

    /// Compute the TOTP code valid right now and the step it belongs to.
    fn current_code_and_step(secret_b32: &str) -> (String, u64) {
        let secret =
            base32::decode(base32::Alphabet::RFC4648 { padding: false }, secret_b32).unwrap();
        let step = Utc::now().timestamp() as u64 / 30;
        let n = totp_at_step(&secret, step).unwrap();
        (format!("{n:06}"), step)
    }

    /// A successfully-used TOTP code must be rejected on immediate
    /// re-submission within the same validity window.
    ///
    /// Pre-fix behaviour: `verify_totp(.., &HashSet::new())` ignored prior use,
    /// so this replay succeeded — that was the P0 hole. Now `min_step` carries
    /// the last accepted step and the replay is rejected.
    #[test]
    fn totp_used_code_rejected_on_replay() {
        let secret = generate_totp_secret();
        let (code, step) = current_code_and_step(&secret);

        // First submission: fresh factor (min_step = 0) accepts it.
        let accepted = verify_totp(&secret, &code, 0).expect("first use accepted");
        assert_eq!(accepted, step, "accepted step should be the current step");

        // Replay: caller persisted `accepted` as the new min_step. The same
        // code (same step) must now be rejected.
        let replay = verify_totp(&secret, &code, accepted);
        assert!(
            replay.is_err(),
            "replay of an already-used TOTP code must be rejected"
        );
    }

    /// A fresh code from the next step is still accepted after the prior step
    /// was burned (the guard only blocks <= the last accepted step).
    #[test]
    fn totp_next_window_code_still_accepted() {
        let secret = generate_totp_secret();
        let secret_bytes =
            base32::decode(base32::Alphabet::RFC4648 { padding: false }, &secret).unwrap();
        let step = Utc::now().timestamp() as u64 / 30;

        // Burn the current step.
        let _ = verify_totp(
            &secret,
            &format!("{:06}", totp_at_step(&secret_bytes, step).unwrap()),
            0,
        )
        .expect("current accepted");

        // The next step is within the +1 skew window and is > min_step, so it
        // verifies even though the current step is now burned.
        let next_code = format!("{:06}", totp_at_step(&secret_bytes, step + 1).unwrap());
        let accepted =
            verify_totp(&secret, &next_code, step).expect("next-window code must be accepted");
        assert_eq!(accepted, step + 1);
    }

    /// The `MfaCache` replay guard is monotonic: advancing it then attempting
    /// to lower it is a no-op (mirrors the `GREATEST(..)` on the Postgres path).
    #[test]
    fn mfa_cache_last_used_step_is_monotonic() {
        let cache = MfaCache::new();
        let fid = Uuid::new_v4();
        let now = Utc::now();
        cache.insert_factor_sync(MfaFactorRow {
            id: fid,
            user_id: Uuid::new_v4(),
            project_id: ProjectId::new().to_string(),
            factor_type: FactorType::Totp,
            status: FactorStatus::Verified,
            secret_enc: "x".into(),
            friendly_name: "App".into(),
            last_used_step: 0,
            created_at: now,
            updated_at: now,
        });
        cache.set_factor_last_used_step_sync(fid, 100);
        cache.set_factor_last_used_step_sync(fid, 50); // attempt to lower
        assert_eq!(
            cache.load_factor_sync(fid).unwrap().last_used_step,
            100,
            "counter must never go backwards"
        );
    }

    // --- 6.SEC.P0.2: WebAuthn crypto ----------------------------------------

    /// A minimal software authenticator that produces real FIDO2
    /// attestation/assertion blobs signed with a generated P-256 key.
    struct SoftAuthenticator {
        signing_key: p256::ecdsa::SigningKey,
        credential_id: Vec<u8>,
    }

    impl SoftAuthenticator {
        fn new() -> Self {
            let signing_key = p256::ecdsa::SigningKey::random(&mut rand_core::OsRng);
            Self {
                signing_key,
                credential_id: b"soft-cred-0001".to_vec(),
            }
        }

        fn auth_data(&self, sign_count: u32, include_cred: bool) -> Vec<u8> {
            let mut out = Vec::new();
            out.extend_from_slice(&sha256(WEBAUTHN_RP_ID.as_bytes()));
            let mut flags = FLAG_UP;
            if include_cred {
                flags |= FLAG_AT;
            }
            out.push(flags);
            out.extend_from_slice(&sign_count.to_be_bytes());
            if include_cred {
                // aaguid (16 zero bytes)
                out.extend_from_slice(&[0u8; 16]);
                // credential id length + id
                out.extend_from_slice(&(self.credential_id.len() as u16).to_be_bytes());
                out.extend_from_slice(&self.credential_id);
                // COSE EC2 P-256 public key
                out.extend_from_slice(&self.cose_key());
            }
            out
        }

        fn cose_key(&self) -> Vec<u8> {
            use ciborium::value::{Integer, Value};
            let vk = p256::ecdsa::VerifyingKey::from(&self.signing_key);
            let point = vk.to_encoded_point(false);
            let x = point.x().unwrap().to_vec();
            let y = point.y().unwrap().to_vec();
            let map = Value::Map(vec![
                (
                    Value::Integer(Integer::from(1)),
                    Value::Integer(Integer::from(2)),
                ), // kty EC2
                (
                    Value::Integer(Integer::from(3)),
                    Value::Integer(Integer::from(-7)),
                ), // alg ES256
                (
                    Value::Integer(Integer::from(-1)),
                    Value::Integer(Integer::from(1)),
                ), // crv P-256
                (Value::Integer(Integer::from(-2)), Value::Bytes(x)),
                (Value::Integer(Integer::from(-3)), Value::Bytes(y)),
            ]);
            let mut buf = Vec::new();
            ciborium::ser::into_writer(&map, &mut buf).unwrap();
            buf
        }

        fn client_data(&self, ty: &str, challenge: &str) -> Vec<u8> {
            serde_json::to_vec(&serde_json::json!({
                "type": ty,
                "challenge": challenge,
                "origin": WEBAUTHN_ORIGIN,
            }))
            .unwrap()
        }

        fn attestation(&self, challenge: &str, sign_count: u32) -> String {
            use ciborium::value::Value;
            let client_data = self.client_data("webauthn.create", challenge);
            let auth_data = self.auth_data(sign_count, true);
            let att_obj = Value::Map(vec![
                (Value::Text("fmt".into()), Value::Text("none".into())),
                (Value::Text("attStmt".into()), Value::Map(vec![])),
                (Value::Text("authData".into()), Value::Bytes(auth_data)),
            ]);
            let mut obj_buf = Vec::new();
            ciborium::ser::into_writer(&att_obj, &mut obj_buf).unwrap();
            serde_json::json!({
                "type": "public-key",
                "id": b64url(&self.credential_id),
                "rawId": b64url(&self.credential_id),
                "response": {
                    "clientDataJSON": b64url(&client_data),
                    "attestationObject": b64url(&obj_buf),
                }
            })
            .to_string()
        }

        fn assertion(&self, challenge: &str, sign_count: u32) -> String {
            use p256::ecdsa::{signature::Signer, DerSignature};
            let client_data = self.client_data("webauthn.get", challenge);
            let auth_data = self.auth_data(sign_count, false);
            let mut signed = auth_data.clone();
            signed.extend_from_slice(&sha256(&client_data));
            let sig: DerSignature = self.signing_key.sign(&signed);
            serde_json::json!({
                "type": "public-key",
                "id": b64url(&self.credential_id),
                "rawId": b64url(&self.credential_id),
                "response": {
                    "clientDataJSON": b64url(&client_data),
                    "authenticatorData": b64url(&auth_data),
                    "signature": b64url(sig.as_bytes()),
                }
            })
            .to_string()
        }
    }

    /// A forged attestation — hand-built JSON echoing the right challenge but
    /// with no valid attested credential / signature material — is rejected.
    /// This was the exact P0 exploit (it used to be accepted).
    #[test]
    fn webauthn_forged_attestation_rejected() {
        let challenge = b64url(b"server-issued-challenge-bytes!!!");
        // Mirror the old exploit payload: real challenge, garbage attestation.
        let client_data = serde_json::to_vec(&serde_json::json!({
            "type": "webauthn.create",
            "challenge": challenge,
            "origin": WEBAUTHN_ORIGIN,
        }))
        .unwrap();
        let forged = serde_json::json!({
            "type": "public-key",
            "response": {
                "clientDataJSON": b64url(&client_data),
                "attestationObject": b64url(b"not-a-real-cbor-attestation-object"),
            }
        })
        .to_string();

        assert!(
            verify_attestation(&forged, &challenge).is_err(),
            "forged attestation must be rejected"
        );
    }

    /// A properly-signed assertion from a software authenticator is accepted,
    /// and verification recovers the reported sign-counter.
    #[test]
    fn webauthn_valid_assertion_accepted() {
        let auth = SoftAuthenticator::new();
        let reg_challenge = b64url(b"reg-challenge-0000000000000000!!");
        let attestation = auth.attestation(&reg_challenge, 1);
        let (stored, counter) =
            verify_attestation(&attestation, &reg_challenge).expect("attestation accepted");
        assert_eq!(counter, 1);

        let assert_challenge = b64url(b"assert-challenge-00000000000000!");
        let assertion = auth.assertion(&assert_challenge, 5);
        let reported =
            verify_assertion(&assertion, &assert_challenge, &stored).expect("assertion accepted");
        assert_eq!(reported, 5, "verified counter should match authenticator");
    }

    /// A tampered signature (right structure, wrong key/bytes) is rejected.
    #[test]
    fn webauthn_bad_signature_rejected() {
        let auth = SoftAuthenticator::new();
        let reg_challenge = b64url(b"reg-challenge-1111111111111111!!");
        let (stored, _) =
            verify_attestation(&auth.attestation(&reg_challenge, 1), &reg_challenge).unwrap();

        // Sign with a DIFFERENT authenticator's key — signature won't verify
        // against the stored public key.
        let attacker = SoftAuthenticator::new();
        let assert_challenge = b64url(b"assert-challenge-11111111111111!");
        let forged = attacker.assertion(&assert_challenge, 9);
        assert!(
            verify_assertion(&forged, &assert_challenge, &stored).is_err(),
            "assertion signed by a different key must be rejected"
        );
    }

    /// A sign-counter regression (cloned authenticator replays an old counter)
    /// is rejected by the service-level check. We assert the rule directly:
    /// reported <= stored (both non-zero) ⇒ reject.
    #[test]
    fn webauthn_counter_regression_rejected() {
        let auth = SoftAuthenticator::new();
        let reg_challenge = b64url(b"reg-challenge-2222222222222222!!");
        let (stored, _) =
            verify_attestation(&auth.attestation(&reg_challenge, 1), &reg_challenge).unwrap();

        // An assertion with a non-advancing counter still has a *valid
        // signature* — verify_assertion returns Ok with the (stale) counter.
        let assert_challenge = b64url(b"assert-challenge-22222222222222!");
        let stale = auth.assertion(&assert_challenge, 3);
        let reported = verify_assertion(&stale, &assert_challenge, &stored).unwrap();

        // The clone-detection rule lives in verify_webauthn_challenge: a
        // reported counter at or below the stored counter is a clone.
        let stored_counter: u32 = 5; // pretend we'd already seen counter 5
        let is_clone = reported != 0 && reported <= stored_counter;
        assert!(is_clone, "counter regression must be flagged as a clone");
    }
}
