//! Shared wire types mirrored from the Rust handlers they bind to.
//!
//! Field shapes follow `sdk/basin-python/basin/types.py`, which in turn cites
//! the basin-rest handlers. JSON field names match the server exactly.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A single data row — an arbitrary JSON object.
pub type Row = serde_json::Map<String, Value>;

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

/// Token body returned by `/auth/v1/signin`, `/auth/v1/refresh`, and
/// `/auth/v1/magic-link/consume` (basin-rest `auth.rs token_body`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    /// Short-lived access token (JWT) presented as `Authorization: Bearer`.
    pub access_token: String,
    /// Long-lived refresh token used to mint a new [`Session`].
    pub refresh_token: String,
    /// RFC 3339 timestamp at which `access_token` expires.
    pub access_expires_at: String,
    /// RFC 3339 timestamp at which `refresh_token` expires.
    pub refresh_expires_at: String,
}

/// Response of `POST /auth/v1/signup`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignUpResult {
    /// Always `true` on success.
    pub ok: bool,
    /// ULID of the newly created user.
    pub user_id: String,
}

/// Response of `POST /auth/v1/api-keys` — the secret is shown exactly once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKeyIssued {
    /// Numeric key id (used by `delete_api_key`).
    pub id: i64,
    /// Human-readable key name.
    pub name: String,
    /// The raw secret — store it now, it is never returned again.
    pub secret: String,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
}

/// An element of `GET /auth/v1/api-keys`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKeyDescriptor {
    /// Numeric key id.
    pub id: i64,
    /// Human-readable key name.
    pub name: String,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
    /// RFC 3339 timestamp of last use, if ever used.
    #[serde(default)]
    pub last_used_at: Option<String>,
    /// RFC 3339 revocation timestamp, if revoked.
    #[serde(default)]
    pub revoked_at: Option<String>,
}

/// Response of `GET /auth/v1/oauth/:provider/authorize`.
///
/// The SDK cannot perform the browser redirect itself: send the user's browser
/// to [`redirect_url`](Self::redirect_url).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OAuthAuthorizeResult {
    /// Full provider authorize URL — redirect the browser here.
    pub redirect_url: String,
    /// CSRF state value embedded in `redirect_url`; echoed back on callback.
    pub state: String,
}

// ---------------------------------------------------------------------------
// MFA
// ---------------------------------------------------------------------------

/// Result of `enroll_factor` — TOTP or WebAuthn depending on `factor_type`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MfaEnrollResult {
    /// WebAuthn enrollment (matched first; it carries `creation_options_json`).
    WebAuthn(WebAuthnEnrollResult),
    /// TOTP enrollment.
    Totp(TotpEnrollResult),
}

/// `POST /auth/v1/factors` response for `factor_type = "totp"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TotpEnrollResult {
    /// Factor UUID (used to verify / challenge / unenroll).
    pub factor_id: String,
    /// Always `"totp"`.
    pub factor_type: String,
    /// Base-32 TOTP secret — display in a QR code or hand to an auth app.
    pub secret_b32: String,
    /// `otpauth://totp/...` URI suitable for QR display.
    pub otpauth_uri: String,
}

/// `POST /auth/v1/factors` response for `factor_type = "webauthn"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebAuthnEnrollResult {
    /// Factor UUID.
    pub factor_id: String,
    /// Always `"webauthn"`.
    pub factor_type: String,
    /// Challenge id to pass back to `verify_factor`.
    pub challenge_id: String,
    /// Serialised `PublicKeyCredentialCreationOptions` JSON for
    /// `navigator.credentials.create()`.
    pub creation_options_json: String,
}

/// An element of `GET /auth/v1/factors`. Secret bytes are never included.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorDescriptor {
    /// Factor UUID.
    pub id: String,
    /// `"totp"` or `"webauthn"`.
    pub factor_type: String,
    /// `"unverified"` or `"verified"`.
    pub status: String,
    /// Human-readable label.
    pub friendly_name: String,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
    /// RFC 3339 last-updated timestamp.
    pub updated_at: String,
}

/// `POST /auth/v1/factors/:id/verify` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifyFactorResult {
    /// Always `true` on success.
    #[serde(default = "default_true")]
    pub ok: bool,
    /// Recovery codes — present only for the **first** verified factor; store
    /// them securely, they are shown exactly once.
    #[serde(default)]
    pub recovery_codes: Option<Vec<String>>,
}

fn default_true() -> bool {
    true
}

/// Result of `challenge_factor` — TOTP or WebAuthn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MfaChallengeResult {
    /// WebAuthn challenge (carries `request_options_json`).
    WebAuthn(WebAuthnChallengeResult),
    /// TOTP challenge.
    Totp(TotpChallengeResult),
}

/// `POST /auth/v1/factors/:id/challenge` response for a TOTP factor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TotpChallengeResult {
    /// Challenge id to pass to `verify_challenge`.
    pub challenge_id: String,
}

/// `POST /auth/v1/factors/:id/challenge` response for a WebAuthn factor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebAuthnChallengeResult {
    /// Challenge id to pass to `verify_challenge`.
    pub challenge_id: String,
    /// Serialised `PublicKeyCredentialRequestOptions` JSON for
    /// `navigator.credentials.get()`.
    pub request_options_json: String,
}

// ---------------------------------------------------------------------------
// Query / data
// ---------------------------------------------------------------------------

/// Non-row result for writes: `{ ok: true, tag: "..." }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecTag {
    /// Always `true` on success.
    pub ok: bool,
    /// Engine command tag, e.g. `"INSERT 0 1"`.
    pub tag: String,
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

/// A storage bucket descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bucket {
    /// Bucket id (ULID).
    pub id: String,
    /// Bucket name.
    pub name: String,
    /// Whether objects are publicly downloadable without a JWT.
    pub public: bool,
    /// Per-object byte size cap, if any.
    #[serde(default)]
    pub file_size_limit: Option<i64>,
    /// Allowed MIME types (empty = any).
    #[serde(default)]
    pub allowed_mime_types: Vec<String>,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
    /// RFC 3339 last-updated timestamp.
    pub updated_at: String,
}

/// A storage object descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageObject {
    /// Object id (ULID).
    pub id: String,
    /// Owning bucket id.
    pub bucket_id: String,
    /// Object path within the bucket.
    pub path: String,
    /// Size in bytes.
    pub size: i64,
    /// MIME type, if recorded.
    #[serde(default)]
    pub mime_type: Option<String>,
    /// Arbitrary user metadata.
    #[serde(default)]
    pub metadata: Option<Value>,
    /// Owner user id, if any.
    #[serde(default)]
    pub owner: Option<String>,
    /// Entity tag (content hash).
    pub etag: String,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
    /// RFC 3339 last-updated timestamp.
    pub updated_at: String,
}

/// A signed object URL minted by `create_signed_url`.
///
/// [`signed_url`](Self::signed_url) is server-relative;
/// [`absolute_url`](Self::absolute_url) is the full URL with the base prepended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedUrl {
    /// Server-relative signed URL.
    pub signed_url: String,
    /// RFC 3339 expiry timestamp.
    pub expires_at: String,
    /// Full URL (base + `signed_url`).
    pub absolute_url: String,
}

impl SignedUrl {
    /// Build a [`SignedUrl`] from the server's JSON body and the client base
    /// URL. Accepts both camelCase (`signedUrl`/`expiresAt`) and snake_case
    /// field spellings the server has used across versions.
    pub(crate) fn from_value(d: &Value, base_url: &str) -> Self {
        let rel = d
            .get("signedUrl")
            .or_else(|| d.get("signed_url"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let expires_at = d
            .get("expiresAt")
            .or_else(|| d.get("expires_at"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let absolute_url = format!("{base_url}{rel}");
        SignedUrl {
            signed_url: rel,
            expires_at,
            absolute_url,
        }
    }
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

/// Proxied response from an HTTP-handler function (`ANY /fn/v1/:name`).
#[derive(Debug, Clone)]
pub struct FunctionInvokeResult {
    /// HTTP status returned by the function.
    pub status: u16,
    /// Response headers.
    pub headers: std::collections::HashMap<String, String>,
    /// Parsed JSON body when the content type was JSON, else the raw text as a
    /// JSON string value.
    pub data: Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_url_camelcase() {
        let v: Value =
            serde_json::from_str(r#"{"signedUrl":"/s/x","expiresAt":"2030-01-01T00:00:00Z"}"#)
                .unwrap();
        let s = SignedUrl::from_value(&v, "http://h");
        assert_eq!(s.signed_url, "/s/x");
        assert_eq!(s.absolute_url, "http://h/s/x");
        assert_eq!(s.expires_at, "2030-01-01T00:00:00Z");
    }

    #[test]
    fn signed_url_snakecase() {
        let v: Value =
            serde_json::from_str(r#"{"signed_url":"/s/y","expires_at":"x"}"#).unwrap();
        let s = SignedUrl::from_value(&v, "http://h");
        assert_eq!(s.signed_url, "/s/y");
    }

    #[test]
    fn mfa_enroll_untagged_picks_totp() {
        let r: MfaEnrollResult = serde_json::from_str(
            r#"{"factor_id":"f","factor_type":"totp","secret_b32":"AAA","otpauth_uri":"otpauth://x"}"#,
        )
        .unwrap();
        assert!(matches!(r, MfaEnrollResult::Totp(_)));
    }

    #[test]
    fn mfa_enroll_untagged_picks_webauthn() {
        let r: MfaEnrollResult = serde_json::from_str(
            r#"{"factor_id":"f","factor_type":"webauthn","challenge_id":"c","creation_options_json":"{}"}"#,
        )
        .unwrap();
        assert!(matches!(r, MfaEnrollResult::WebAuthn(_)));
    }
}
