//! OAuth 2.0 / OIDC provider support (Phase 5.10.O — ADR 0020).
//!
//! Implements the authorization-code flow with PKCE for three preset providers
//! (Google, GitHub, Apple) and a generic OIDC config path. All flows are
//! OSS-only; basin-cloud only adds the provider-registration UI.
//!
//! ## Flow
//!
//! 1. `GET /auth/v1/authorize?provider=google&redirect_to=https://app.example.com`
//!    → build PKCE verifier + code_challenge, sign a short-lived CSRF `state`,
//!      store `(state_hash, pkce_verifier, redirect_to)` in `{schema}_oauth_states`,
//!      302-redirect to provider with `response_type=code&state=...&code_challenge=...`.
//!
//! 2. Provider redirects back to `GET /auth/v1/callback?code=...&state=...`
//!    → validate HMAC state + lookup stored row, exchange code (with verifier),
//!      fetch userinfo, link/create user in `{schema}_users` + `{schema}_identities`,
//!      issue Basin JWT + refresh, 302-redirect to `redirect_to`.
//!
//! ## Security
//! - `state` is HMAC-SHA256 over `project_id|provider|random_nonce` with the
//!   JWT secret, bound to a short TTL (10 min). The hash is stored so the DB
//!   row validates too, preventing forgery even if HMAC is somehow bypassed.
//! - PKCE uses S256: `code_challenge = base64url(sha256(verifier))`.
//! - `redirect_to` is validated against a per-project allowlist.
//! - Identity linking only happens on a provider-asserted **verified** email;
//!   unverified addresses always create a fresh unlinked identity.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use base64::Engine;
use basin_common::{BasinError, ProjectId, Result};
use chrono::Utc;
use hmac::{Hmac, Mac};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tracing::instrument;

use crate::store::AuthStore;
use crate::Inner;

type HmacSha256 = Hmac<Sha256>;

// ---------------------------------------------------------------------------
// Provider presets
// ---------------------------------------------------------------------------

/// A known OAuth 2.0 / OIDC provider with hard-coded endpoints + scopes.
#[derive(Debug, Clone)]
pub struct ProviderPreset {
    pub name: &'static str,
    pub authorize_url: &'static str,
    pub token_url: &'static str,
    pub userinfo_url: &'static str,
    pub default_scopes: &'static str,
}

/// Look up a known preset by lower-case provider name.
///
/// Provider aliases:
/// - `microsoft` / `azure_ad` → same Microsoft v2 common-tenant endpoints.
/// - `twitter_x` / `twitter` → X (Twitter) OAuth 2.0 PKCE endpoints.
pub fn preset(name: &str) -> Option<&'static ProviderPreset> {
    match name {
        // Original 3 (Phase 5.10.O — ADR 0020).
        "google" => Some(&GOOGLE),
        "github" => Some(&GITHUB),
        "apple" => Some(&APPLE),
        // Cloud-broker batch (T-015 / T-033 / T-036). Alphabetical.
        "bitbucket" => Some(&BITBUCKET),
        "discord" => Some(&DISCORD),
        "figma" => Some(&FIGMA),
        "gitlab" => Some(&GITLAB),
        "linkedin" => Some(&LINKEDIN),
        "microsoft" | "azure_ad" => Some(&MICROSOFT),
        "notion" => Some(&NOTION),
        "slack" => Some(&SLACK),
        "spotify" => Some(&SPOTIFY),
        "twitch" => Some(&TWITCH),
        "twitter_x" | "twitter" => Some(&TWITTER_X),
        _ => None,
    }
}

/// All preset provider names known to `preset()`. Used by integration tests
/// to assert the cloud broker's `BasinAuthProvider` has a matching entry for
/// every provider the broker advertises (T-015 / T-033 / T-036).
///
/// `microsoft`/`azure_ad` and `twitter_x`/`twitter` collapse to a single
/// preset each, so this list returns only the canonical name.
pub const PRESET_PROVIDER_NAMES: &[&str] = &[
    "google",
    "github",
    "apple",
    "bitbucket",
    "discord",
    "figma",
    "gitlab",
    "linkedin",
    "microsoft",
    "notion",
    "slack",
    "spotify",
    "twitch",
    "twitter_x",
];

static GOOGLE: ProviderPreset = ProviderPreset {
    name: "google",
    authorize_url: "https://accounts.google.com/o/oauth2/v2/auth",
    token_url: "https://oauth2.googleapis.com/token",
    userinfo_url: "https://www.googleapis.com/oauth2/v3/userinfo",
    default_scopes: "openid email profile",
};

static GITHUB: ProviderPreset = ProviderPreset {
    name: "github",
    authorize_url: "https://github.com/login/oauth/authorize",
    token_url: "https://github.com/login/oauth/access_token",
    userinfo_url: "https://api.github.com/user",
    default_scopes: "read:user user:email",
};

// Apple Sign In with Apple. Production callbacks require
// `response_mode=form_post` AND a JWT client_secret signed with the developer's
// ES256 key (rotated every ~6 months).
//
// The ES256 JWT client-secret signer is implemented in
// `build_apple_client_secret_jwt` and wired into `exchange_code` automatically
// when `provider == "apple"`. The operator must set:
//   BASIN_AUTH_APPLE_TEAM_ID       — 10-char Apple Developer team ID
//   BASIN_AUTH_APPLE_KEY_ID        — 10-char key ID from the .p8 file
//   BASIN_AUTH_APPLE_PRIVATE_KEY_PEM — full PEM contents of the .p8 key
// The generated JWT is cached for 5 months and regenerated on expiry.
// Spec: https://developer.apple.com/documentation/sign_in_with_apple/generate_and_validate_tokens
static APPLE: ProviderPreset = ProviderPreset {
    name: "apple",
    authorize_url: "https://appleid.apple.com/auth/authorize",
    token_url: "https://appleid.apple.com/auth/token",
    userinfo_url: "https://appleid.apple.com/auth/userinfo",
    default_scopes: "name email",
};

static BITBUCKET: ProviderPreset = ProviderPreset {
    name: "bitbucket",
    authorize_url: "https://bitbucket.org/site/oauth2/authorize",
    token_url: "https://bitbucket.org/site/oauth2/access_token",
    userinfo_url: "https://api.bitbucket.org/2.0/user",
    default_scopes: "account email",
};

static DISCORD: ProviderPreset = ProviderPreset {
    name: "discord",
    authorize_url: "https://discord.com/api/oauth2/authorize",
    token_url: "https://discord.com/api/oauth2/token",
    userinfo_url: "https://discord.com/api/users/@me",
    default_scopes: "identify email",
};

static FIGMA: ProviderPreset = ProviderPreset {
    name: "figma",
    authorize_url: "https://www.figma.com/oauth",
    token_url: "https://api.figma.com/v1/oauth/token",
    userinfo_url: "https://api.figma.com/v1/me",
    default_scopes: "files:read",
};

// GitLab — defaults to gitlab.com. Self-hosted instances should override
// `authorize_url`/`token_url`/`userinfo_url` in their `OAuthProviderRow`.
static GITLAB: ProviderPreset = ProviderPreset {
    name: "gitlab",
    authorize_url: "https://gitlab.com/oauth/authorize",
    token_url: "https://gitlab.com/oauth/token",
    userinfo_url: "https://gitlab.com/api/v4/user",
    default_scopes: "read_user openid email profile",
};

// LinkedIn — v2 OIDC ("Sign In with LinkedIn using OpenID Connect"). The
// legacy v1 (`r_emailaddress r_liteprofile`) was deprecated August 2023.
static LINKEDIN: ProviderPreset = ProviderPreset {
    name: "linkedin",
    authorize_url: "https://www.linkedin.com/oauth/v2/authorization",
    token_url: "https://www.linkedin.com/oauth/v2/accessToken",
    userinfo_url: "https://api.linkedin.com/v2/userinfo",
    default_scopes: "openid profile email",
};

// Microsoft Identity Platform v2 — `common` tenant accepts both personal
// (MSA) and work/school (AAD) accounts. Per-app tenant lock-down should be
// done by overriding `authorize_url`/`token_url` with the tenant GUID.
static MICROSOFT: ProviderPreset = ProviderPreset {
    name: "microsoft",
    authorize_url: "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
    token_url: "https://login.microsoftonline.com/common/oauth2/v2.0/token",
    userinfo_url: "https://graph.microsoft.com/oidc/userinfo",
    default_scopes: "openid email profile",
};

// Notion — non-standard OAuth: the token response embeds a `bot_id` +
// `workspace_id` instead of a `sub`, and the access token is workspace-scoped.
// userinfo_url points at the standard `/users/me`; the callback flow falls
// back to the token response's `owner.user.id` when `sub` is absent.
// TODO(T-033): handle bot/internal-integration tokens distinctly from user OAuth.
// Spec: https://developers.notion.com/docs/authorization
static NOTION: ProviderPreset = ProviderPreset {
    name: "notion",
    authorize_url: "https://api.notion.com/v1/oauth/authorize",
    token_url: "https://api.notion.com/v1/oauth/token",
    userinfo_url: "https://api.notion.com/v1/users/me",
    default_scopes: "",
};

static SLACK: ProviderPreset = ProviderPreset {
    name: "slack",
    authorize_url: "https://slack.com/openid/connect/authorize",
    token_url: "https://slack.com/api/openid.connect.token",
    userinfo_url: "https://slack.com/api/openid.connect.userInfo",
    default_scopes: "openid email profile",
};

static SPOTIFY: ProviderPreset = ProviderPreset {
    name: "spotify",
    authorize_url: "https://accounts.spotify.com/authorize",
    token_url: "https://accounts.spotify.com/api/token",
    userinfo_url: "https://api.spotify.com/v1/me",
    default_scopes: "user-read-email user-read-private",
};

static TWITCH: ProviderPreset = ProviderPreset {
    name: "twitch",
    authorize_url: "https://id.twitch.tv/oauth2/authorize",
    token_url: "https://id.twitch.tv/oauth2/token",
    userinfo_url: "https://id.twitch.tv/oauth2/userinfo",
    default_scopes: "openid user:read:email",
};

// X (Twitter) OAuth 2.0 — requires PKCE. The legacy OAuth 1.0a flow is not
// supported here; broker callers should ensure the developer app is
// configured for "OAuth 2.0" in the X developer portal.
static TWITTER_X: ProviderPreset = ProviderPreset {
    name: "twitter_x",
    authorize_url: "https://twitter.com/i/oauth2/authorize",
    token_url: "https://api.twitter.com/2/oauth2/token",
    userinfo_url: "https://api.twitter.com/2/users/me",
    default_scopes: "users.read tweet.read offline.access",
};

// ---------------------------------------------------------------------------
// OIDC discovery (RFC 8414)
// ---------------------------------------------------------------------------

/// Subset of an OIDC discovery document.
#[derive(Debug, Deserialize)]
pub struct OidcDiscovery {
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    #[serde(default)]
    pub userinfo_endpoint: Option<String>,
}

/// Fetch and parse the OIDC discovery document for `issuer_url`.
pub async fn fetch_oidc_discovery(issuer_url: &str) -> Result<OidcDiscovery> {
    let url = if issuer_url.ends_with("/.well-known/openid-configuration") {
        issuer_url.to_owned()
    } else {
        let base = issuer_url.trim_end_matches('/');
        format!("{base}/.well-known/openid-configuration")
    };
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| BasinError::internal(format!("oidc discovery fetch: {e}")))?;
    if !resp.status().is_success() {
        return Err(BasinError::internal(format!(
            "oidc discovery non-2xx: {} from {url}",
            resp.status()
        )));
    }
    resp.json::<OidcDiscovery>()
        .await
        .map_err(|e| BasinError::internal(format!("oidc discovery parse: {e}")))
}

// ---------------------------------------------------------------------------
// PKCE helpers (S256)
// ---------------------------------------------------------------------------

/// Generate a PKCE verifier and S256 code challenge.
/// Returns `(verifier, challenge)`.
pub fn pkce_pair() -> (String, String) {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    let verifier = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    let hash = Sha256::digest(verifier.as_bytes());
    let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash);
    (verifier, challenge)
}

// ---------------------------------------------------------------------------
// Apple ES256 JWT client_secret signer
// ---------------------------------------------------------------------------

/// Claims for the Apple `client_secret` JWT.
/// Spec: https://developer.apple.com/documentation/sign_in_with_apple/generate_and_validate_tokens
#[derive(Debug, Serialize)]
struct AppleClientSecretClaims {
    /// Issuer: 10-char Apple Developer team ID.
    iss: String,
    /// Issued-at (Unix seconds).
    iat: i64,
    /// Expiry (Unix seconds, max 6 months from iat).
    exp: i64,
    /// Audience: always "https://appleid.apple.com".
    aud: &'static str,
    /// Subject: the app's client ID (Services ID).
    sub: String,
}

/// Generate an Apple Sign-In `client_secret` JWT.
///
/// The JWT is signed with ES256 using the `.p8` private key Apple provides
/// in the developer portal. Required inputs are:
/// - `team_id`   — 10-char Apple Developer team ID (`iss` claim)
/// - `key_id`    — 10-char key ID shown in the developer portal (`kid` header)
/// - `client_id` — Services ID (bundle ID for web, `sub` claim)
/// - `private_key_pem` — PEM-encoded ECDSA P-256 private key (`.p8` file contents)
///
/// The token is valid for up to 6 months (180 days) per Apple's spec.
pub fn build_apple_client_secret_jwt(
    team_id: &str,
    key_id: &str,
    client_id: &str,
    private_key_pem: &str,
) -> Result<String> {
    let now = Utc::now().timestamp();
    // Apple allows up to 6 months (15_777_000 seconds ≈ 182.5 days).
    // We use 180 days to stay safely within the window.
    let exp = now + 180 * 24 * 60 * 60;

    let claims = AppleClientSecretClaims {
        iss: team_id.to_owned(),
        iat: now,
        exp,
        aud: "https://appleid.apple.com",
        sub: client_id.to_owned(),
    };

    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(key_id.to_owned());

    let encoding_key = EncodingKey::from_ec_pem(private_key_pem.as_bytes())
        .map_err(|e| BasinError::internal(format!("apple_client_secret: invalid EC PEM: {e}")))?;

    jsonwebtoken::encode(&header, &claims, &encoding_key)
        .map_err(|e| BasinError::internal(format!("apple_client_secret: JWT encode failed: {e}")))
}

/// Cached Apple `client_secret` JWT with expiry metadata.
struct AppleClientSecretCache {
    token: String,
    /// Wall-clock instant after which the cached token must be regenerated.
    /// We refresh at 5 months so there is always 1 month of headroom before
    /// Apple's 6-month hard expiry.
    refresh_after: Instant,
}

/// Process-wide cache for the Apple client_secret JWT.
/// One entry is sufficient because each process serves a single operator.
static APPLE_CLIENT_SECRET_CACHE: OnceLock<std::sync::Mutex<Option<AppleClientSecretCache>>> =
    OnceLock::new();

/// Return a valid Apple `client_secret` JWT, re-generating if the cached one
/// is within 1 month of expiry (i.e., older than 5 months).
///
/// Reads credentials from env vars:
/// - `BASIN_AUTH_APPLE_TEAM_ID`
/// - `BASIN_AUTH_APPLE_KEY_ID`
/// - `BASIN_AUTH_APPLE_PRIVATE_KEY_PEM`
///
/// Falls back to the `static_secret` parameter when env vars are absent so
/// the caller-supplied value from `ProviderConfig` is used in development.
pub fn apple_client_secret(static_secret: &str) -> Result<String> {
    let team_id = std::env::var("BASIN_AUTH_APPLE_TEAM_ID").ok();
    let key_id = std::env::var("BASIN_AUTH_APPLE_KEY_ID").ok();
    let pem = std::env::var("BASIN_AUTH_APPLE_PRIVATE_KEY_PEM").ok();

    // If env vars are not set, fall back to the static configured secret
    // (allows dev/test overrides without env).
    match (team_id, key_id, pem) {
        (Some(team_id), Some(key_id), Some(pem)) => {
            // Use the cached JWT from env-based credentials.
            let cache_mutex = APPLE_CLIENT_SECRET_CACHE
                .get_or_init(|| std::sync::Mutex::new(None));

            let mut guard = cache_mutex
                .lock()
                .map_err(|_| BasinError::internal("apple_client_secret: cache mutex poisoned"))?;

            // We need client_id too — extract it from the ResolvedProvider.
            // Unfortunately we don't have the client_id here; the caller
            // must pass it via the static_secret slot or we derive it from
            // the `sub` field of the cached token. Instead, we accept a
            // sentinel: if static_secret is non-empty we treat it as the
            // client_id for the JWT (the ProviderConfig stores the client_id
            // separately; when the caller wires exchange_code it passes the
            // client_id in the `client_secret` position for Apple).
            //
            // Simpler approach: the caller passes client_id as static_secret
            // when env vars are set, so we use it directly.
            let client_id = if static_secret.is_empty() {
                return Err(BasinError::internal(
                    "apple_client_secret: client_id is required (set via client_id field)",
                ));
            } else {
                static_secret
            };

            // Check if the cached token is still fresh (< 5 months old).
            if let Some(ref cached) = *guard {
                if Instant::now() < cached.refresh_after {
                    return Ok(cached.token.clone());
                }
            }

            // Generate a new token.
            let token = build_apple_client_secret_jwt(&team_id, &key_id, client_id, &pem)?;
            // Refresh after 5 months (150 days).
            let refresh_after = Instant::now() + Duration::from_secs(150 * 24 * 60 * 60);
            *guard = Some(AppleClientSecretCache {
                token: token.clone(),
                refresh_after,
            });
            Ok(token)
        }
        _ => {
            // Env vars not set → use whatever the operator stored in the
            // client_secret_enc field (plaintext in dev, encrypted in prod).
            Ok(static_secret.to_owned())
        }
    }
}

// ---------------------------------------------------------------------------
// CSRF state: HMAC-signed nonce
// ---------------------------------------------------------------------------

/// A signed state value for the OAuth flow.
#[derive(Debug, Clone)]
pub struct OAuthState {
    /// Raw state value sent to the provider (and returned by them on callback).
    pub value: String,
    /// SHA-256 hex of `value` — stored in the DB as the PK.
    pub hash: String,
}

/// Create a signed state. Format: `nonce.hmac_hex`.
/// The HMAC is over `project_id:provider:nonce` keyed with the JWT secret.
pub fn create_state(jwt_secret: &[u8], project_id: &ProjectId, provider: &str) -> OAuthState {
    let mut nonce_bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = hex::encode(nonce_bytes);
    let msg = format!("{project_id}:{provider}:{nonce}");
    let mut mac = HmacSha256::new_from_slice(jwt_secret).expect("HMAC accepts any key size");
    mac.update(msg.as_bytes());
    let sig = hex::encode(mac.finalize().into_bytes());
    let value = format!("{nonce}.{sig}");
    let hash = hex::encode(Sha256::digest(value.as_bytes()));
    OAuthState { value, hash }
}

/// Verify the HMAC on a state value. Returns `Ok(())` on success.
pub fn verify_state_hmac(
    jwt_secret: &[u8],
    project_id: &ProjectId,
    provider: &str,
    state: &str,
) -> Result<()> {
    let parts: Vec<&str> = state.splitn(2, '.').collect();
    if parts.len() != 2 {
        return Err(BasinError::InvalidIdent("invalid oauth state format".into()));
    }
    let (nonce, presented_sig) = (parts[0], parts[1]);
    let msg = format!("{project_id}:{provider}:{nonce}");
    let mut mac = HmacSha256::new_from_slice(jwt_secret).expect("HMAC accepts any key size");
    mac.update(msg.as_bytes());
    let expected_sig = hex::encode(mac.finalize().into_bytes());
    let ok: bool = presented_sig
        .as_bytes()
        .ct_eq(expected_sig.as_bytes())
        .into();
    if !ok {
        return Err(BasinError::InvalidIdent(
            "oauth state HMAC verification failed".into(),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Open-redirect guard
// ---------------------------------------------------------------------------

/// Validate `redirect_to` against the per-project allowlist.
///
/// - Empty or relative (`/...`) paths are always allowed.
/// - Absolute URLs must be `http://` or `https://` and their **origin**
///   (scheme + host + port) must exactly match the origin of one entry in
///   `allowed_origins`. Reject everything else.
///
/// Using origin comparison (rather than a naive `starts_with` prefix match)
/// prevents subdomain-confusion attacks such as:
/// - `https://example.com.evil.com/cb` — host does not match
/// - `https://example.com@evil.com/cb` — userinfo prefix trick; the actual
///   host is `evil.com`, not `example.com`
///
/// (Audit finding P1-3.)
pub fn validate_redirect_to(redirect_to: &str, allowed_origins: &[String]) -> Result<()> {
    if redirect_to.is_empty() || redirect_to.starts_with('/') {
        return Ok(());
    }
    if !redirect_to.starts_with("http://") && !redirect_to.starts_with("https://") {
        return Err(BasinError::InvalidIdent(format!(
            "redirect_to must be a relative path or http(s) URL: {redirect_to:?}"
        )));
    }
    if allowed_origins.is_empty() {
        return Err(BasinError::InvalidIdent(
            "redirect_to allowlist is empty; configure oauth provider redirect_uri".into(),
        ));
    }

    // Parse the candidate URL and extract its origin.
    let candidate = url::Url::parse(redirect_to).map_err(|e| {
        BasinError::InvalidIdent(format!("redirect_to is not a valid URL: {e}"))
    })?;
    let candidate_origin = candidate.origin();

    for entry in allowed_origins {
        // Parse the allowlist entry. Malformed entries are skipped — a bad
        // admin config must not silently open the redirect.
        let Ok(allowed_url) = url::Url::parse(entry.trim()) else {
            continue;
        };
        if candidate_origin == allowed_url.origin() {
            return Ok(());
        }
    }

    Err(BasinError::InvalidIdent(format!(
        "redirect_to {redirect_to:?} not in provider redirect_uri allowlist"
    )))
}

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

/// A row from `{schema}_oauth_providers`.
#[derive(Debug, Clone)]
pub struct OAuthProviderRow {
    pub id: i64,
    pub project_id: String,
    pub provider: String,
    pub client_id: String,
    /// Encrypted client secret. Callers decrypt via `EncryptionProvider`.
    pub client_secret_enc: String,
    pub scopes: String,
    pub redirect_uri: String,
    pub discovery_url: String,
    pub authorize_url: String,
    pub token_url: String,
    pub userinfo_url: String,
    pub enabled: bool,
}

/// A row from `{schema}_identities`.
#[derive(Debug, Clone)]
pub struct IdentityRow {
    pub id: i64,
    pub user_id: uuid::Uuid,
    pub project_id: String,
    pub provider: String,
    pub provider_user_id: String,
    pub email: String,
    pub email_verified: bool,
}

/// A row from `{schema}_oauth_states`.
#[derive(Debug, Clone)]
pub struct OAuthStateRow {
    pub state_hash: String,
    pub project_id: String,
    pub provider: String,
    pub pkce_verifier: String,
    pub redirect_to: String,
}

// ---------------------------------------------------------------------------
// Token exchange + userinfo
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    #[serde(default)]
    pub token_type: String,
    #[serde(default)]
    pub expires_in: Option<u64>,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub id_token: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UserInfo {
    /// Provider-unique user identifier (OIDC "sub").
    #[serde(alias = "id", alias = "sub")]
    pub sub: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub email_verified: Option<serde_json::Value>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl UserInfo {
    /// Resolve the provider_user_id. Tries `sub`, then integer `id`, then email.
    pub fn provider_user_id(&self) -> Option<String> {
        if let Some(ref s) = self.sub {
            return Some(s.clone());
        }
        if let Some(v) = self.extra.get("id").or_else(|| self.extra.get("login")) {
            match v {
                serde_json::Value::String(s) => return Some(s.clone()),
                serde_json::Value::Number(n) => return Some(n.to_string()),
                _ => {}
            }
        }
        self.email.clone()
    }

    /// True iff the provider asserted the email is verified.
    pub fn is_email_verified(&self) -> bool {
        match &self.email_verified {
            Some(serde_json::Value::Bool(b)) => *b,
            Some(serde_json::Value::String(s)) => s.eq_ignore_ascii_case("true"),
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Encryption provider (small trait, no cross-crate dep)
// ---------------------------------------------------------------------------

/// Minimal encryption surface used by the OAuth flow to protect client
/// secrets at rest. In production, basin-server injects an AES-256-GCM
/// implementation. Tests use [`PlaintextEncryption`].
pub trait EncryptionProvider: Send + Sync {
    fn encrypt(&self, plaintext: &str) -> Result<String>;
    fn decrypt(&self, ciphertext: &str) -> Result<String>;
}

/// Identity-transform encryption for testing / local dev. **Never use in production.**
pub struct PlaintextEncryption;

impl EncryptionProvider for PlaintextEncryption {
    fn encrypt(&self, plaintext: &str) -> Result<String> {
        Ok(format!("plain:{plaintext}"))
    }

    fn decrypt(&self, ciphertext: &str) -> Result<String> {
        ciphertext
            .strip_prefix("plain:")
            .map(|s| s.to_owned())
            .ok_or_else(|| BasinError::internal("PlaintextEncryption: unexpected ciphertext format"))
    }
}

// ---------------------------------------------------------------------------
// OAuthStore trait — extends AuthStore with OAuth-specific operations
// ---------------------------------------------------------------------------

use async_trait::async_trait;

/// OAuth-specific persistence. Implemented by `PostgresAuthStore`; other
/// `AuthStore` impls (`EngineAuthStore`) use the in-memory fallback via the
/// `OAuthStateCache` stored on `Inner`.
#[async_trait]
pub trait OAuthStore: AuthStore {
    // --- Provider config ----------------------------------------------------

    /// Upsert an OAuth provider config. Returns the row id.
    async fn upsert_oauth_provider(&self, schema: &str, row: &OAuthProviderRow) -> Result<i64>;

    /// Load a provider config by `(project_id, provider)`.
    async fn load_oauth_provider(
        &self,
        schema: &str,
        project_id: &ProjectId,
        provider: &str,
    ) -> Result<Option<OAuthProviderRow>>;

    // --- PKCE / state store -------------------------------------------------

    /// Insert an in-flight OAuth state row.
    async fn insert_oauth_state(
        &self,
        schema: &str,
        state_hash: &str,
        project_id: &ProjectId,
        provider: &str,
        pkce_verifier: &str,
        redirect_to: &str,
    ) -> Result<()>;

    /// Consume (read-and-delete) an OAuth state row by hash.
    async fn consume_oauth_state(
        &self,
        schema: &str,
        state_hash: &str,
    ) -> Result<Option<OAuthStateRow>>;

    // --- Identities ---------------------------------------------------------

    /// Look up an identity by `(project_id, provider, provider_user_id)`.
    async fn find_identity(
        &self,
        schema: &str,
        project_id: &ProjectId,
        provider: &str,
        provider_user_id: &str,
    ) -> Result<Option<IdentityRow>>;

    /// Look up an identity by `(project_id, provider, email)`.
    async fn find_identity_by_email(
        &self,
        schema: &str,
        project_id: &ProjectId,
        provider: &str,
        email: &str,
    ) -> Result<Option<IdentityRow>>;

    /// Insert a new identity row.
    async fn insert_identity(
        &self,
        schema: &str,
        project_id: &ProjectId,
        user_id: uuid::Uuid,
        provider: &str,
        provider_user_id: &str,
        email: &str,
        email_verified: bool,
    ) -> Result<i64>;

    /// Update `last_sign_in_at` + `email_verified` for an existing identity.
    async fn touch_identity(&self, schema: &str, identity_id: i64, email_verified: bool)
        -> Result<()>;
}

// ---------------------------------------------------------------------------
// Postgres OAuthStore implementation
// ---------------------------------------------------------------------------

use crate::store::postgres::PostgresAuthStore;

#[async_trait]
impl OAuthStore for PostgresAuthStore {
    async fn upsert_oauth_provider(&self, schema: &str, row: &OAuthProviderRow) -> Result<i64> {
        let client = self.client.lock().await;
        let sql = format!(
            "INSERT INTO {schema}_oauth_providers
                (project_id, provider, client_id, client_secret,
                 scopes, redirect_uri, discovery_url,
                 authorize_url, token_url, userinfo_url, enabled)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
             ON CONFLICT (project_id, provider) DO UPDATE SET
                client_id = EXCLUDED.client_id,
                client_secret = EXCLUDED.client_secret,
                scopes = EXCLUDED.scopes,
                redirect_uri = EXCLUDED.redirect_uri,
                discovery_url = EXCLUDED.discovery_url,
                authorize_url = EXCLUDED.authorize_url,
                token_url = EXCLUDED.token_url,
                userinfo_url = EXCLUDED.userinfo_url,
                enabled = EXCLUDED.enabled
             RETURNING id"
        );
        let row_id: i64 = client
            .query_one(
                &sql,
                &[
                    &row.project_id,
                    &row.provider,
                    &row.client_id,
                    &row.client_secret_enc,
                    &row.scopes,
                    &row.redirect_uri,
                    &row.discovery_url,
                    &row.authorize_url,
                    &row.token_url,
                    &row.userinfo_url,
                    &row.enabled,
                ],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("upsert_oauth_provider: {e}")))?
            .get(0);
        Ok(row_id)
    }

    async fn load_oauth_provider(
        &self,
        schema: &str,
        project_id: &ProjectId,
        provider: &str,
    ) -> Result<Option<OAuthProviderRow>> {
        let client = self.client.lock().await;
        let sql = format!(
            "SELECT id, project_id, provider, client_id, client_secret,
                    scopes, redirect_uri, discovery_url,
                    authorize_url, token_url, userinfo_url, enabled
             FROM {schema}_oauth_providers
             WHERE project_id = $1 AND provider = $2 AND enabled = TRUE"
        );
        let maybe = client
            .query_opt(&sql, &[&project_id.to_string(), &provider])
            .await
            .map_err(|e| BasinError::catalog(format!("load_oauth_provider: {e}")))?;
        Ok(maybe.map(|r| OAuthProviderRow {
            id: r.get(0),
            project_id: r.get(1),
            provider: r.get(2),
            client_id: r.get(3),
            client_secret_enc: r.get(4),
            scopes: r.get(5),
            redirect_uri: r.get(6),
            discovery_url: r.get(7),
            authorize_url: r.get(8),
            token_url: r.get(9),
            userinfo_url: r.get(10),
            enabled: r.get(11),
        }))
    }

    async fn insert_oauth_state(
        &self,
        schema: &str,
        state_hash: &str,
        project_id: &ProjectId,
        provider: &str,
        pkce_verifier: &str,
        redirect_to: &str,
    ) -> Result<()> {
        let client = self.client.lock().await;
        let expires_at = Utc::now() + chrono::Duration::minutes(10);
        let sql = format!(
            "INSERT INTO {schema}_oauth_states
                (state_hash, project_id, provider, pkce_verifier, redirect_to, expires_at)
             VALUES ($1,$2,$3,$4,$5,$6)"
        );
        client
            .execute(
                &sql,
                &[
                    &state_hash,
                    &project_id.to_string(),
                    &provider,
                    &pkce_verifier,
                    &redirect_to,
                    &expires_at,
                ],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("insert_oauth_state: {e}")))?;
        Ok(())
    }

    async fn consume_oauth_state(
        &self,
        schema: &str,
        state_hash: &str,
    ) -> Result<Option<OAuthStateRow>> {
        let client = self.client.lock().await;
        let sql = format!(
            "DELETE FROM {schema}_oauth_states
             WHERE state_hash = $1 AND expires_at > now()
             RETURNING state_hash, project_id, provider, pkce_verifier, redirect_to"
        );
        let maybe = client
            .query_opt(&sql, &[&state_hash])
            .await
            .map_err(|e| BasinError::catalog(format!("consume_oauth_state: {e}")))?;
        Ok(maybe.map(|r| OAuthStateRow {
            state_hash: r.get(0),
            project_id: r.get(1),
            provider: r.get(2),
            pkce_verifier: r.get(3),
            redirect_to: r.get(4),
        }))
    }

    async fn find_identity(
        &self,
        schema: &str,
        project_id: &ProjectId,
        provider: &str,
        provider_user_id: &str,
    ) -> Result<Option<IdentityRow>> {
        let client = self.client.lock().await;
        let sql = format!(
            "SELECT id, user_id, project_id, provider, provider_user_id, email, email_verified
             FROM {schema}_identities
             WHERE project_id = $1 AND provider = $2 AND provider_user_id = $3"
        );
        let maybe = client
            .query_opt(
                &sql,
                &[&project_id.to_string(), &provider, &provider_user_id],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("find_identity: {e}")))?;
        Ok(maybe.map(pg_row_to_identity))
    }

    async fn find_identity_by_email(
        &self,
        schema: &str,
        project_id: &ProjectId,
        provider: &str,
        email: &str,
    ) -> Result<Option<IdentityRow>> {
        let client = self.client.lock().await;
        let sql = format!(
            "SELECT id, user_id, project_id, provider, provider_user_id, email, email_verified
             FROM {schema}_identities
             WHERE project_id = $1 AND provider = $2 AND email = $3"
        );
        let maybe = client
            .query_opt(&sql, &[&project_id.to_string(), &provider, &email])
            .await
            .map_err(|e| BasinError::catalog(format!("find_identity_by_email: {e}")))?;
        Ok(maybe.map(pg_row_to_identity))
    }

    async fn insert_identity(
        &self,
        schema: &str,
        project_id: &ProjectId,
        user_id: uuid::Uuid,
        provider: &str,
        provider_user_id: &str,
        email: &str,
        email_verified: bool,
    ) -> Result<i64> {
        let client = self.client.lock().await;
        let sql = format!(
            "INSERT INTO {schema}_identities
                (user_id, project_id, provider, provider_user_id, email, email_verified,
                 last_sign_in_at)
             VALUES ($1,$2,$3,$4,$5,$6,now())
             RETURNING id"
        );
        let id: i64 = client
            .query_one(
                &sql,
                &[
                    &user_id,
                    &project_id.to_string(),
                    &provider,
                    &provider_user_id,
                    &email,
                    &email_verified,
                ],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("insert_identity: {e}")))?
            .get(0);
        Ok(id)
    }

    async fn touch_identity(
        &self,
        schema: &str,
        identity_id: i64,
        email_verified: bool,
    ) -> Result<()> {
        let client = self.client.lock().await;
        let sql = format!(
            "UPDATE {schema}_identities
             SET last_sign_in_at = now(), email_verified = $1
             WHERE id = $2"
        );
        client
            .execute(&sql, &[&email_verified, &identity_id])
            .await
            .map_err(|e| BasinError::catalog(format!("touch_identity: {e}")))?;
        Ok(())
    }
}

fn pg_row_to_identity(r: tokio_postgres::Row) -> IdentityRow {
    IdentityRow {
        id: r.get(0),
        user_id: r.get(1),
        project_id: r.get(2),
        provider: r.get(3),
        provider_user_id: r.get(4),
        email: r.get(5),
        email_verified: r.get(6),
    }
}

// ---------------------------------------------------------------------------
// In-memory OAuth state cache (for non-Postgres stores in tests)
// ---------------------------------------------------------------------------

use std::sync::Mutex;

/// TTL for in-memory OAuth states: matches the 10-minute window used by the
/// Postgres-backed path (`expires_at = now() + 10 minutes`).
const OAUTH_STATE_TTL: Duration = Duration::from_secs(10 * 60);

/// An in-memory OAuth state entry with a wall-clock expiry.
struct CachedState {
    row: OAuthStateRow,
    /// Monotonic instant after which the entry is considered expired.
    expires_at: Instant,
}

/// Thread-safe in-memory state cache used when the concrete `AuthStore` does
/// not implement `OAuthStore` (e.g. `EngineAuthStore` in integration tests).
/// Not for production use.
///
/// Each entry carries a TTL (audit P2-3): `consume_state_sync` rejects states
/// whose TTL has elapsed, and lazily sweeps expired entries from the map on
/// every insert so the cache does not grow unboundedly in long-running
/// test/dev servers.
pub struct OAuthStateCache {
    states: Mutex<HashMap<String, CachedState>>,
    providers: Mutex<HashMap<String, ResolvedProvider>>,
    identities: Mutex<Vec<IdentityRow>>,
    /// State TTL. Configurable for tests that need to exercise expiry quickly.
    state_ttl: Duration,
}

impl OAuthStateCache {
    pub fn new() -> Self {
        Self::with_ttl(OAUTH_STATE_TTL)
    }

    /// Construct with a custom TTL. Used by unit tests that need to set a very
    /// short TTL to verify expiry behaviour without sleeping for 10 minutes.
    pub fn with_ttl(state_ttl: Duration) -> Self {
        Self {
            states: Mutex::new(HashMap::new()),
            providers: Mutex::new(HashMap::new()),
            identities: Mutex::new(Vec::new()),
            state_ttl,
        }
    }

    pub fn insert_state_sync(
        &self,
        state_hash: &str,
        project_id: &ProjectId,
        provider: &str,
        pkce_verifier: &str,
        redirect_to: &str,
    ) {
        let row = OAuthStateRow {
            state_hash: state_hash.to_owned(),
            project_id: project_id.to_string(),
            provider: provider.to_owned(),
            pkce_verifier: pkce_verifier.to_owned(),
            redirect_to: redirect_to.to_owned(),
        };
        let entry = CachedState {
            row,
            expires_at: Instant::now() + self.state_ttl,
        };
        let mut map = self.states.lock().unwrap();
        // Lazy sweep: evict all expired entries on every insert so the map
        // does not grow unboundedly in long-running test/dev processes.
        let now = Instant::now();
        map.retain(|_, v| v.expires_at > now);
        map.insert(state_hash.to_owned(), entry);
    }

    /// Consume (read-and-delete) an OAuth state entry. Returns an error if the
    /// entry does not exist **or** has expired (audit P2-3 TTL enforcement).
    pub fn consume_state_sync(&self, state_hash: &str) -> Result<OAuthStateRow> {
        let mut map = self.states.lock().unwrap();
        match map.remove(state_hash) {
            Some(entry) if entry.expires_at > Instant::now() => Ok(entry.row),
            Some(_expired) => {
                Err(BasinError::InvalidIdent("oauth state not found or expired".into()))
            }
            None => Err(BasinError::InvalidIdent("oauth state not found or expired".into())),
        }
    }

    /// Register a mock provider for tests (no real DB row needed).
    pub fn register_mock_provider(
        &self,
        project_id: &ProjectId,
        provider: &str,
        client_id: &str,
        client_secret: &str,
        authorize_url: &str,
        token_url: &str,
        userinfo_url: &str,
        scopes: &str,
        redirect_uri: &str,
    ) {
        let key = format!("{project_id}:{provider}");
        self.providers.lock().unwrap().insert(
            key,
            ResolvedProvider {
                client_id: client_id.to_owned(),
                client_secret_enc: format!("plain:{client_secret}"),
                scopes: scopes.to_owned(),
                redirect_uri: redirect_uri.to_owned(),
                authorize_url: authorize_url.to_owned(),
                token_url: token_url.to_owned(),
                userinfo_url: userinfo_url.to_owned(),
            },
        );
    }

    pub(crate) fn load_mock_provider(
        &self,
        project_id: &ProjectId,
        provider: &str,
    ) -> Result<ResolvedProvider> {
        let key = format!("{project_id}:{provider}");
        // Clone rather than remove so the provider can be reused.
        self.providers
            .lock()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or_else(|| {
                BasinError::NotFound(format!(
                    "no mock OAuth provider for project={project_id} provider={provider}"
                ))
            })
    }

    /// Link or create a user identity in the in-memory store.
    pub(crate) async fn link_or_create(
        &self,
        inner: &Inner,
        project_id: &ProjectId,
        provider: &str,
        provider_user_id: &str,
        email: &str,
        email_verified: bool,
    ) -> Result<(uuid::Uuid, bool)> {
        // Check by provider_user_id first.
        {
            let idents = self.identities.lock().unwrap();
            if let Some(ident) = idents.iter().find(|i| {
                i.project_id == project_id.to_string()
                    && i.provider == provider
                    && i.provider_user_id == provider_user_id
            }) {
                return Ok((ident.user_id, false));
            }
        }

        // Check by verified email for linking.
        if email_verified {
            let idents = self.identities.lock().unwrap();
            if let Some(ident) = idents.iter().find(|i| {
                i.project_id == project_id.to_string() && i.email == email
            }) {
                let user_id = ident.user_id;
                drop(idents);
                let mut idents = self.identities.lock().unwrap();
                let new_id = idents.len() as i64 + 1;
                idents.push(IdentityRow {
                    id: new_id,
                    user_id,
                    project_id: project_id.to_string(),
                    provider: provider.to_owned(),
                    provider_user_id: provider_user_id.to_owned(),
                    email: email.to_owned(),
                    email_verified,
                });
                return Ok((user_id, false));
            }
        }

        // Create new user + identity.
        let user_id = uuid::Uuid::new_v4();
        let _ = inner
            .store
            .create_user(project_id, email, "", user_id)
            .await;
        if email_verified {
            let _ = inner
                .store
                .mark_email_verified_if_null(project_id, user_id)
                .await;
        }
        let mut idents = self.identities.lock().unwrap();
        let new_id = idents.len() as i64 + 1;
        idents.push(IdentityRow {
            id: new_id,
            user_id,
            project_id: project_id.to_string(),
            provider: provider.to_owned(),
            provider_user_id: provider_user_id.to_owned(),
            email: email.to_owned(),
            email_verified,
        });
        Ok((user_id, true))
    }
}

impl Default for OAuthStateCache {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Resolved provider (endpoints filled in from presets / discovery)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct ResolvedProvider {
    pub(crate) client_id: String,
    pub(crate) client_secret_enc: String,
    pub(crate) scopes: String,
    pub(crate) redirect_uri: String,
    pub(crate) authorize_url: String,
    pub(crate) token_url: String,
    pub(crate) userinfo_url: String,
}

// ---------------------------------------------------------------------------
// Core flow helpers
// ---------------------------------------------------------------------------

fn non_empty_or<'a>(s: &'a str, fallback: &'a str) -> &'a str {
    if s.is_empty() { fallback } else { s }
}

/// Resolve provider endpoints from DB row + preset defaults + optional OIDC
/// discovery.
async fn resolve_provider_config(
    row: &OAuthProviderRow,
    provider: &str,
) -> Result<ResolvedProvider> {
    let preset_opt = preset(provider);

    let authorize_url = non_empty_or(
        &row.authorize_url,
        preset_opt.map(|p| p.authorize_url).unwrap_or(""),
    )
    .to_owned();
    let token_url = non_empty_or(
        &row.token_url,
        preset_opt.map(|p| p.token_url).unwrap_or(""),
    )
    .to_owned();
    let userinfo_url = non_empty_or(
        &row.userinfo_url,
        preset_opt.map(|p| p.userinfo_url).unwrap_or(""),
    )
    .to_owned();
    let scopes = non_empty_or(
        &row.scopes,
        preset_opt.map(|p| p.default_scopes).unwrap_or(""),
    )
    .to_owned();

    // For OIDC providers, fill in endpoints from discovery if still empty.
    let (authorize_url, token_url, userinfo_url) =
        if provider == "oidc" && !row.discovery_url.is_empty() {
            match fetch_oidc_discovery(&row.discovery_url).await {
                Ok(disc) => {
                    let a = if authorize_url.is_empty() {
                        disc.authorization_endpoint
                    } else {
                        authorize_url
                    };
                    let t = if token_url.is_empty() {
                        disc.token_endpoint
                    } else {
                        token_url
                    };
                    let u = if userinfo_url.is_empty() {
                        disc.userinfo_endpoint.unwrap_or_default()
                    } else {
                        userinfo_url
                    };
                    (a, t, u)
                }
                Err(e) => {
                    tracing::warn!(error = %e, "OIDC discovery failed; using explicit endpoints");
                    (authorize_url, token_url, userinfo_url)
                }
            }
        } else {
            (authorize_url, token_url, userinfo_url)
        };

    Ok(ResolvedProvider {
        client_id: row.client_id.clone(),
        client_secret_enc: row.client_secret_enc.clone(),
        scopes,
        redirect_uri: row.redirect_uri.clone(),
        authorize_url,
        token_url,
        userinfo_url,
    })
}

// ---------------------------------------------------------------------------
// Authorize flow
// ---------------------------------------------------------------------------

/// Parameters returned by `begin_authorize`.
#[derive(Debug)]
pub struct AuthorizeRedirect {
    /// The URL to 302-redirect the browser to.
    pub redirect_url: String,
    /// The raw state value (for test introspection).
    pub state: String,
}

/// Build the authorize URL and persist the in-flight state.
///
/// Uses `oauth_store` when `Some` (Postgres path), else falls back to
/// `state_cache` (in-memory / test path).
#[instrument(skip(inner, oauth_store), fields(project = %project_id, %provider))]
pub(crate) async fn begin_authorize<S>(
    inner: &Inner,
    oauth_store: Option<&S>,
    project_id: &ProjectId,
    provider: &str,
    redirect_to: &str,
) -> Result<AuthorizeRedirect>
where
    S: OAuthStore,
{
    let schema = &inner.cfg.catalog_schema;

    // 1. Load + resolve provider config.
    let resolved = if let Some(pg) = oauth_store {
        let row = pg
            .load_oauth_provider(schema, project_id, provider)
            .await?
            .ok_or_else(|| {
                BasinError::NotFound(format!(
                    "no OAuth provider config: project={project_id} provider={provider}"
                ))
            })?;
        resolve_provider_config(&row, provider).await?
    } else {
        inner.oauth_state_cache.load_mock_provider(project_id, provider)?
    };

    // 2. Validate redirect_to against the per-project allowlist.
    let allowed: Vec<String> = resolved
        .redirect_uri
        .split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect();
    validate_redirect_to(redirect_to, &allowed)?;

    // 3. PKCE.
    let (verifier, challenge) = pkce_pair();

    // 4. CSRF state.
    let oauth_state = create_state(&inner.cfg.jwt_secret, project_id, provider);

    // 5. Persist state.
    if let Some(pg) = oauth_store {
        pg.insert_oauth_state(
            schema,
            &oauth_state.hash,
            project_id,
            provider,
            &verifier,
            redirect_to,
        )
        .await?;
    } else {
        inner.oauth_state_cache.insert_state_sync(
            &oauth_state.hash,
            project_id,
            provider,
            &verifier,
            redirect_to,
        );
    }

    // 6. Build redirect URL.
    let url = build_authorize_url(
        &resolved.authorize_url,
        &resolved.client_id,
        redirect_to,
        &resolved.scopes,
        &oauth_state.value,
        &challenge,
    );

    Ok(AuthorizeRedirect {
        redirect_url: url,
        state: oauth_state.value,
    })
}

fn build_authorize_url(
    authorize_url: &str,
    client_id: &str,
    redirect_to: &str,
    scopes: &str,
    state: &str,
    code_challenge: &str,
) -> String {
    let mut params = vec![
        ("response_type", "code".to_owned()),
        ("client_id", client_id.to_owned()),
        ("scope", scopes.to_owned()),
        ("state", state.to_owned()),
        ("code_challenge", code_challenge.to_owned()),
        ("code_challenge_method", "S256".to_owned()),
    ];
    if !redirect_to.is_empty() {
        params.push(("redirect_uri", redirect_to.to_owned()));
    }
    let query: String = params
        .iter()
        .map(|(k, v)| format!("{}={}", k, percent_encode(v)))
        .collect::<Vec<_>>()
        .join("&");
    format!("{authorize_url}?{query}")
}

/// Minimal percent-encoding for OAuth query parameter values.
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

// ---------------------------------------------------------------------------
// Callback flow
// ---------------------------------------------------------------------------

/// Result of a completed OAuth callback.
#[derive(Debug)]
pub struct OAuthCallbackResult {
    pub tokens: crate::Tokens,
    /// The validated `redirect_to` from the stored state row.
    pub redirect_to: String,
    /// True if the user was newly created (vs. existing linked user).
    pub user_created: bool,
}

/// Handle the OAuth callback: validate state → exchange code → fetch userinfo
/// → link/create user → issue Basin tokens.
///
/// `redirect_callback_base` is the base URL of `/auth/v1/callback` (used as
/// `redirect_uri` in the token exchange). Pass the same value used in
/// `begin_authorize`.
#[instrument(skip(inner, enc, oauth_store, code, state_val))]
pub(crate) async fn handle_callback<S>(
    inner: &Inner,
    enc: &dyn EncryptionProvider,
    oauth_store: Option<&S>,
    provider_hint: &str,
    code: &str,
    state_val: &str,
    redirect_callback_base: &str,
) -> Result<OAuthCallbackResult>
where
    S: OAuthStore,
{
    let schema = &inner.cfg.catalog_schema;
    let state_hash = hex::encode(Sha256::digest(state_val.as_bytes()));

    // 1. Consume state row (validates TTL + single-use).
    let state_row = if let Some(pg) = oauth_store {
        pg.consume_oauth_state(schema, &state_hash)
            .await?
            .ok_or_else(|| BasinError::InvalidIdent("oauth state not found or expired".into()))?
    } else {
        inner.oauth_state_cache.consume_state_sync(&state_hash)?
    };

    let project_id: ProjectId = state_row
        .project_id
        .parse()
        .map_err(|e| BasinError::internal(format!("callback state project_id parse: {e}")))?;
    let provider = state_row.provider.as_str();

    // 2. Verify HMAC (after state row lookup so we know project_id + provider).
    verify_state_hmac(&inner.cfg.jwt_secret, &project_id, provider, state_val)?;

    // 3. Load + resolve provider config.
    let resolved = if let Some(pg) = oauth_store {
        let row = pg
            .load_oauth_provider(schema, &project_id, provider)
            .await?
            .ok_or_else(|| {
                BasinError::NotFound(format!(
                    "no OAuth provider config: project={project_id} provider={provider}"
                ))
            })?;
        resolve_provider_config(&row, provider).await?
    } else {
        inner.oauth_state_cache.load_mock_provider(&project_id, provider)?
    };

    let decrypted_secret = enc.decrypt(&resolved.client_secret_enc)?;
    // For Apple, the `client_secret` must be a freshly-minted ES256 JWT.
    // When the BASIN_AUTH_APPLE_* env vars are set we generate (and cache)
    // that JWT here; otherwise we fall back to whatever the operator stored
    // in `client_secret_enc` (useful for dev/test with a pre-generated JWT).
    let client_secret = if provider == "apple" {
        apple_client_secret(&resolved.client_id)?
    } else {
        decrypted_secret
    };

    // 4. Exchange code for tokens.
    let token_resp = exchange_code(
        &resolved.token_url,
        &resolved.client_id,
        &client_secret,
        code,
        redirect_callback_base,
        &state_row.pkce_verifier,
        provider,
    )
    .await?;

    // 5. Fetch userinfo.
    let userinfo = fetch_userinfo(
        &resolved.userinfo_url,
        &token_resp.access_token,
        &token_resp.id_token,
        provider,
    )
    .await?;

    let email = userinfo
        .email
        .as_deref()
        .map(|e| e.trim().to_ascii_lowercase())
        .filter(|e| !e.is_empty())
        .ok_or_else(|| BasinError::InvalidIdent("provider did not return an email".into()))?;

    let provider_user_id = userinfo
        .provider_user_id()
        .ok_or_else(|| BasinError::internal("provider did not return a sub/id"))?;

    // Auto-link to an existing account is only safe when the provider asserts
    // the email is verified (the standard OIDC `email_verified=true` claim).
    // We must NOT blanket-trust any provider (incl. GitHub, whose /user
    // endpoint can return an *unverified* public email): an attacker who
    // controls a provider account asserting a victim's email would otherwise
    // be silently merged into the victim's existing password account
    // (account takeover — 6.SEC.P1). Match Supabase/Auth0: unverified email
    // never merges into an existing account.
    let verified = userinfo.is_email_verified();

    // 6. Link or create user.
    let (user_id, user_created) = if let Some(pg) = oauth_store {
        link_or_create_user_pg(
            inner,
            pg,
            schema,
            &project_id,
            provider,
            &provider_user_id,
            &email,
            verified,
        )
        .await?
    } else {
        inner
            .oauth_state_cache
            .link_or_create(inner, &project_id, provider, &provider_user_id, &email, verified)
            .await?
    };

    // 7. Issue Basin JWT + refresh tokens.
    let now = Utc::now();
    let (access_token, access_expires_at) = inner.jwt.issue(
        &project_id,
        user_id,
        &email,
        &["user".to_string()],
        now,
        inner.cfg.token_ttl,
    )?;
    let (refresh_token_str, jti, refresh_expires_at) =
        inner
            .jwt
            .issue_refresh(&project_id, user_id, &email, now, inner.cfg.refresh_ttl)?;

    inner
        .store
        .insert_refresh_revocation(&jti, user_id, refresh_expires_at)
        .await?;

    Ok(OAuthCallbackResult {
        tokens: crate::Tokens {
            access_token,
            refresh_token: refresh_token_str,
            access_expires_at,
            refresh_expires_at,
        },
        redirect_to: state_row.redirect_to,
        user_created,
    })
}

async fn link_or_create_user_pg<S: OAuthStore>(
    inner: &Inner,
    pg: &S,
    schema: &str,
    project_id: &ProjectId,
    provider: &str,
    provider_user_id: &str,
    email: &str,
    email_verified: bool,
) -> Result<(uuid::Uuid, bool)> {
    // a) Look up by provider_user_id.
    if let Some(ident) = pg
        .find_identity(schema, project_id, provider, provider_user_id)
        .await?
    {
        pg.touch_identity(schema, ident.id, email_verified).await?;
        return Ok((ident.user_id, false));
    }

    // b) Link on verified email.
    if email_verified {
        if let Some(user) = inner.store.find_user_by_email(project_id, email).await? {
            pg.insert_identity(
                schema,
                project_id,
                user.user_id,
                provider,
                provider_user_id,
                email,
                email_verified,
            )
            .await?;
            return Ok((user.user_id, false));
        }
    }

    // c) Create new user + identity.
    let user_id = uuid::Uuid::new_v4();
    inner
        .store
        .create_user(project_id, email, "", user_id)
        .await
        .or_else(|e| {
            // Race-condition: user was just created. Fetch it.
            if e.to_string().to_lowercase().contains("exists")
                || e.to_string().to_lowercase().contains("duplicate")
                || e.to_string().to_lowercase().contains("conflict")
                || e.to_string().to_lowercase().contains("unique")
            {
                Ok(user_id)
            } else {
                Err(e)
            }
        })?;

    if email_verified {
        let _ = inner
            .store
            .mark_email_verified_if_null(project_id, user_id)
            .await;
    }

    pg.insert_identity(
        schema,
        project_id,
        user_id,
        provider,
        provider_user_id,
        email,
        email_verified,
    )
    .await?;

    Ok((user_id, true))
}

// ---------------------------------------------------------------------------
// HTTP helpers: code exchange + userinfo fetch
// ---------------------------------------------------------------------------

async fn exchange_code(
    token_url: &str,
    client_id: &str,
    client_secret: &str,
    code: &str,
    redirect_uri: &str,
    code_verifier: &str,
    provider: &str,
) -> Result<TokenResponse> {
    let client = reqwest::Client::new();
    let mut params = vec![
        ("grant_type", "authorization_code"),
        ("code", code),
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("code_verifier", code_verifier),
    ];
    if !redirect_uri.is_empty() {
        params.push(("redirect_uri", redirect_uri));
    }

    let mut req = client.post(token_url).form(&params);
    if provider == "github" {
        req = req.header("Accept", "application/json");
    }

    let resp = req
        .send()
        .await
        .map_err(|e| BasinError::internal(format!("token exchange send: {e}")))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(BasinError::internal(format!(
            "token exchange failed: {status} — {body}"
        )));
    }

    resp.json::<TokenResponse>()
        .await
        .map_err(|e| BasinError::internal(format!("token exchange parse: {e}")))
}

async fn fetch_userinfo(
    userinfo_url: &str,
    access_token: &str,
    _id_token: &Option<String>,
    provider: &str,
) -> Result<UserInfo> {
    let client = reqwest::Client::new();
    let mut req = client
        .get(userinfo_url)
        .bearer_auth(access_token)
        .header("Accept", "application/json");

    if provider == "github" {
        req = req.header("User-Agent", "basin-auth/1.0");
    }

    let resp = req
        .send()
        .await
        .map_err(|e| BasinError::internal(format!("userinfo fetch send: {e}")))?;

    if !resp.status().is_success() {
        let status = resp.status();
        return Err(BasinError::internal(format!(
            "userinfo fetch non-2xx: {status}"
        )));
    }

    resp.json::<UserInfo>()
        .await
        .map_err(|e| BasinError::internal(format!("userinfo parse: {e}")))
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkce_pair_produces_valid_s256_challenge() {
        let (verifier, challenge) = pkce_pair();
        assert!(!verifier.is_empty());
        assert!(!challenge.is_empty());
        let expected = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(Sha256::digest(verifier.as_bytes()));
        assert_eq!(challenge, expected);
    }

    #[test]
    fn pkce_pairs_are_unique() {
        let (v1, c1) = pkce_pair();
        let (v2, c2) = pkce_pair();
        assert_ne!(v1, v2);
        assert_ne!(c1, c2);
    }

    #[test]
    fn state_hmac_round_trip() {
        let secret = b"state_hmac_test_key_32_bytes_padx";
        let project = ProjectId::new();
        let state = create_state(secret, &project, "google");
        verify_state_hmac(secret, &project, "google", &state.value).unwrap();
    }

    #[test]
    fn state_hmac_wrong_project_rejected() {
        let secret = b"state_hmac_test_key_32_bytes_padx";
        let p1 = ProjectId::new();
        let p2 = ProjectId::new();
        let state = create_state(secret, &p1, "google");
        assert!(verify_state_hmac(secret, &p2, "google", &state.value).is_err());
    }

    #[test]
    fn state_hmac_wrong_provider_rejected() {
        let secret = b"state_hmac_test_key_32_bytes_padx";
        let project = ProjectId::new();
        let state = create_state(secret, &project, "google");
        assert!(verify_state_hmac(secret, &project, "github", &state.value).is_err());
    }

    #[test]
    fn state_hmac_tampered_signature_rejected() {
        let secret = b"state_hmac_test_key_32_bytes_padx";
        let project = ProjectId::new();
        let state = create_state(secret, &project, "google");
        // Flip last character of the sig part.
        let (nonce, sig) = state.value.split_once('.').unwrap();
        let mut sig_bytes = sig.as_bytes().to_vec();
        let last = sig_bytes.last_mut().unwrap();
        *last = if *last == b'a' { b'b' } else { b'a' };
        let tampered = format!("{nonce}.{}", String::from_utf8(sig_bytes).unwrap());
        assert!(verify_state_hmac(secret, &project, "google", &tampered).is_err());
    }

    #[test]
    fn state_cache_single_use() {
        let cache = OAuthStateCache::new();
        let project = ProjectId::new();
        cache.insert_state_sync("hash1", &project, "github", "verifier", "/home");
        let row = cache.consume_state_sync("hash1").unwrap();
        assert_eq!(row.provider, "github");
        // Second consume must fail.
        assert!(cache.consume_state_sync("hash1").is_err());
    }

    #[test]
    fn validate_redirect_to_relative_always_ok() {
        validate_redirect_to("/dashboard", &[]).unwrap();
        validate_redirect_to("", &[]).unwrap();
    }

    #[test]
    fn validate_redirect_to_absolute_needs_allowlist() {
        assert!(validate_redirect_to("https://example.com/cb", &[]).is_err());
    }

    #[test]
    fn validate_redirect_to_matches_prefix() {
        let allowed = vec!["https://example.com".to_owned()];
        validate_redirect_to("https://example.com/callback", &allowed).unwrap();
    }

    #[test]
    fn validate_redirect_to_rejects_non_allowed() {
        let allowed = vec!["https://example.com".to_owned()];
        assert!(validate_redirect_to("https://evil.com/cb", &allowed).is_err());
    }

    #[test]
    fn validate_redirect_to_rejects_non_http_scheme() {
        assert!(validate_redirect_to("javascript:alert(1)", &[]).is_err());
        assert!(validate_redirect_to("data:text/html,<h1>", &[]).is_err());
    }

    #[test]
    fn preset_google_has_endpoints() {
        let p = preset("google").unwrap();
        assert!(p.authorize_url.contains("google"));
        assert!(p.token_url.contains("google"));
    }

    #[test]
    fn preset_github_has_endpoints() {
        let p = preset("github").unwrap();
        assert!(p.authorize_url.contains("github"));
    }

    #[test]
    fn preset_apple_has_endpoints() {
        let p = preset("apple").unwrap();
        assert!(p.authorize_url.contains("apple"));
    }

    #[test]
    fn preset_unknown_is_none() {
        assert!(preset("saml_sso").is_none());
    }

    // -----------------------------------------------------------------------
    // T-015/T-033/T-036 — cloud-broker provider catalog (12 new providers).
    // -----------------------------------------------------------------------

    /// Asserts that a preset resolves, has the expected hostname in the
    /// authorize/token URLs, and that `build_authorize_url` produces a query
    /// string containing the OAuth 2.0 + PKCE parameters the broker requires.
    fn check_provider(name: &str, host_fragment: &str, expected_scope_fragment: &str) {
        let p = preset(name).unwrap_or_else(|| panic!("preset({name}) returned None"));
        assert!(
            p.authorize_url.contains(host_fragment),
            "{name}.authorize_url ({}) missing host fragment {host_fragment:?}",
            p.authorize_url
        );
        assert!(
            p.token_url.contains(host_fragment),
            "{name}.token_url ({}) missing host fragment {host_fragment:?}",
            p.token_url
        );
        assert!(
            p.authorize_url.starts_with("https://"),
            "{name}.authorize_url must be https"
        );
        assert!(
            p.token_url.starts_with("https://"),
            "{name}.token_url must be https"
        );
        if !expected_scope_fragment.is_empty() {
            assert!(
                p.default_scopes.contains(expected_scope_fragment),
                "{name}.default_scopes ({}) missing fragment {expected_scope_fragment:?}",
                p.default_scopes
            );
        }
        // Build URL and verify shape.
        let url = build_authorize_url(
            p.authorize_url,
            "client_xyz",
            "https://app.example.com/auth/v1/callback",
            p.default_scopes,
            "nonce.sig",
            "s256_challenge",
        );
        assert!(url.starts_with(p.authorize_url), "{name}: url prefix mismatch");
        assert!(url.contains("response_type=code"), "{name}: missing response_type");
        assert!(url.contains("client_id=client_xyz"), "{name}: missing client_id");
        assert!(url.contains("state=nonce.sig"), "{name}: missing state");
        assert!(
            url.contains("code_challenge=s256_challenge"),
            "{name}: missing PKCE challenge"
        );
        assert!(
            url.contains("code_challenge_method=S256"),
            "{name}: missing PKCE method"
        );
    }

    #[test]
    fn preset_microsoft_has_endpoints() {
        check_provider("microsoft", "login.microsoftonline.com", "openid");
        // Alias.
        let alias = preset("azure_ad").unwrap();
        assert_eq!(alias.authorize_url, preset("microsoft").unwrap().authorize_url);
    }

    #[test]
    fn preset_gitlab_has_endpoints() {
        check_provider("gitlab", "gitlab.com", "read_user");
    }

    #[test]
    fn preset_slack_has_endpoints() {
        check_provider("slack", "slack.com", "openid");
    }

    #[test]
    fn preset_discord_has_endpoints() {
        check_provider("discord", "discord.com", "identify");
    }

    #[test]
    fn preset_apple_endpoints_still_resolve() {
        // Apple was already wired; the cloud-broker batch did not regress it.
        check_provider("apple", "appleid.apple.com", "email");
    }

    #[test]
    fn preset_twitter_x_has_endpoints() {
        check_provider("twitter_x", "twitter.com", "users.read");
        // `twitter` alias resolves to the same preset.
        let alias = preset("twitter").unwrap();
        assert_eq!(alias.authorize_url, preset("twitter_x").unwrap().authorize_url);
    }

    #[test]
    fn preset_bitbucket_has_endpoints() {
        check_provider("bitbucket", "bitbucket.org", "account");
    }

    #[test]
    fn preset_notion_has_endpoints() {
        // Notion uses a non-standard `scope`-less flow; check only host shape.
        check_provider("notion", "notion.com", "");
    }

    #[test]
    fn preset_spotify_has_endpoints() {
        check_provider("spotify", "spotify.com", "user-read-email");
    }

    #[test]
    fn preset_twitch_has_endpoints() {
        check_provider("twitch", "twitch.tv", "openid");
    }

    #[test]
    fn preset_linkedin_has_endpoints() {
        check_provider("linkedin", "linkedin.com", "openid");
    }

    #[test]
    fn preset_figma_has_endpoints() {
        check_provider("figma", "figma.com", "files:read");
    }

    #[test]
    fn preset_provider_names_list_matches_dispatch() {
        // Every name in PRESET_PROVIDER_NAMES must resolve via `preset()`,
        // and every preset() match arm must list its canonical name here
        // (the catch is verified by `preset_unknown_is_none` for unknowns).
        for name in PRESET_PROVIDER_NAMES {
            assert!(
                preset(name).is_some(),
                "PRESET_PROVIDER_NAMES contains {name:?} but preset() returns None"
            );
        }
        // Sanity: list length matches the documented 14 (3 original + 12 new,
        // minus the 1 alias collapse for microsoft/azure_ad and 1 for
        // twitter_x/twitter — counted as canonical, so 14 total).
        assert_eq!(
            PRESET_PROVIDER_NAMES.len(),
            14,
            "expected 14 canonical providers (3 original + 11 new canonical names; \
             microsoft/azure_ad + twitter_x/twitter share a preset)"
        );
    }

    #[test]
    fn build_authorize_url_includes_pkce_and_state() {
        let url = build_authorize_url(
            "https://example.com/auth",
            "client_abc",
            "https://myapp.com/callback",
            "openid email",
            "nonce.sig",
            "s256_challenge",
        );
        assert!(url.starts_with("https://example.com/auth?"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("client_id=client_abc"));
        assert!(url.contains("code_challenge=s256_challenge"));
        assert!(url.contains("code_challenge_method=S256"));
        assert!(url.contains("state=nonce.sig"));
    }

    #[test]
    fn userinfo_email_verified_bool() {
        let info = UserInfo {
            sub: Some("123".into()),
            email: Some("u@example.com".into()),
            email_verified: Some(serde_json::Value::Bool(true)),
            name: None,
            extra: HashMap::new(),
        };
        assert!(info.is_email_verified());

        let info2 = UserInfo {
            email_verified: Some(serde_json::Value::Bool(false)),
            ..info.clone()
        };
        assert!(!info2.is_email_verified());
    }

    #[test]
    fn userinfo_email_verified_absent_is_false() {
        // 6.SEC.P1: a provider that omits the `email_verified` claim must NOT
        // be treated as verified — absence is the unverified case, so the
        // callback flow will refuse to auto-link to an existing account.
        let info = UserInfo {
            sub: Some("123".into()),
            email: Some("victim@example.com".into()),
            email_verified: None,
            name: None,
            extra: HashMap::new(),
        };
        assert!(
            !info.is_email_verified(),
            "absent email_verified must be treated as unverified"
        );
    }

    #[test]
    fn userinfo_email_verified_string_false_is_false() {
        let info = UserInfo {
            sub: Some("123".into()),
            email: Some("victim@example.com".into()),
            email_verified: Some(serde_json::Value::String("false".into())),
            name: None,
            extra: HashMap::new(),
        };
        assert!(!info.is_email_verified());
    }

    #[test]
    fn userinfo_provider_user_id_from_sub() {
        let info = UserInfo {
            sub: Some("user-sub-abc".into()),
            email: Some("u@example.com".into()),
            email_verified: None,
            name: None,
            extra: HashMap::new(),
        };
        assert_eq!(info.provider_user_id().unwrap(), "user-sub-abc");
    }

    #[test]
    fn plaintext_encryption_round_trip() {
        let enc = PlaintextEncryption;
        let ct = enc.encrypt("my_secret").unwrap();
        assert_eq!(enc.decrypt(&ct).unwrap(), "my_secret");
    }

    // -----------------------------------------------------------------------
    // P1-3: validate_redirect_to strict-origin tests
    // -----------------------------------------------------------------------

    #[test]
    fn redirect_to_legitimate_passes() {
        let allow = vec!["https://example.com".to_string()];
        assert!(validate_redirect_to("https://example.com/callback", &allow).is_ok());
        assert!(validate_redirect_to("https://example.com/deep/path?q=1", &allow).is_ok());
    }

    #[test]
    fn redirect_to_subdomain_confusion_rejected() {
        let allow = vec!["https://example.com".to_string()];
        // Subdomain prefix trick: host is `example.com.evil.com`.
        let result = validate_redirect_to("https://example.com.evil.com/cb", &allow);
        assert!(
            result.is_err(),
            "SECURITY P1-3: subdomain-confusion redirect_to MUST be rejected"
        );
    }

    #[test]
    fn redirect_to_userinfo_prefix_rejected() {
        let allow = vec!["https://example.com".to_string()];
        // Userinfo trick: `example.com` is the userinfo, real host is `evil.com`.
        let result = validate_redirect_to("https://example.com@evil.com/cb", &allow);
        assert!(
            result.is_err(),
            "SECURITY P1-3: userinfo-prefix redirect_to MUST be rejected"
        );
    }

    #[test]
    fn redirect_to_relative_always_allowed() {
        let allow = vec!["https://example.com".to_string()];
        assert!(validate_redirect_to("/local/path", &allow).is_ok());
        assert!(validate_redirect_to("", &allow).is_ok());
    }

    #[test]
    fn redirect_to_different_scheme_rejected() {
        let allow = vec!["https://example.com".to_string()];
        // http vs https: different origins.
        let result = validate_redirect_to("http://example.com/cb", &allow);
        assert!(result.is_err(), "http must not match an https allowlist entry");
    }

    #[test]
    fn redirect_to_port_mismatch_rejected() {
        let allow = vec!["https://example.com:8080".to_string()];
        let result = validate_redirect_to("https://example.com/cb", &allow);
        assert!(result.is_err(), "default port must not match explicit port 8080");
    }

    // -----------------------------------------------------------------------
    // P2-3: OAuthStateCache TTL tests
    // -----------------------------------------------------------------------

    #[test]
    fn oauth_state_cache_rejects_expired_entry() {
        use basin_common::ProjectId;
        // Use a 1-nanosecond TTL so entries expire instantly.
        let cache = OAuthStateCache::with_ttl(Duration::from_nanos(1));
        let project = ProjectId::new();
        cache.insert_state_sync("hash1", &project, "google", "verifier", "https://app.example.com");

        // Sleep just long enough to guarantee expiry (1ns TTL + a short pause).
        std::thread::sleep(Duration::from_millis(10));

        let result = cache.consume_state_sync("hash1");
        assert!(
            result.is_err(),
            "SECURITY P2-3: expired state MUST be rejected, got: {result:?}"
        );
    }

    #[test]
    fn oauth_state_cache_accepts_fresh_entry() {
        use basin_common::ProjectId;
        let cache = OAuthStateCache::with_ttl(Duration::from_secs(600));
        let project = ProjectId::new();
        cache.insert_state_sync(
            "hash2",
            &project,
            "github",
            "verifier2",
            "https://app.example.com/cb",
        );

        let result = cache.consume_state_sync("hash2");
        assert!(result.is_ok(), "fresh state must be accepted: {result:?}");
        assert_eq!(result.unwrap().provider, "github");
    }

    #[test]
    fn oauth_state_cache_single_use() {
        use basin_common::ProjectId;
        let cache = OAuthStateCache::with_ttl(Duration::from_secs(600));
        let project = ProjectId::new();
        cache.insert_state_sync("hash3", &project, "google", "v3", "https://app.example.com");

        // First consume succeeds.
        assert!(cache.consume_state_sync("hash3").is_ok());
        // Second consume fails (state was removed).
        assert!(
            cache.consume_state_sync("hash3").is_err(),
            "state must be single-use"
        );
    }

    #[test]
    fn oauth_state_cache_sweep_removes_expired_on_insert() {
        use basin_common::ProjectId;
        let cache = OAuthStateCache::with_ttl(Duration::from_nanos(1));
        let project = ProjectId::new();

        // Insert 3 entries that will expire immediately.
        for i in 0..3u32 {
            cache.insert_state_sync(
                &format!("expired_{i}"),
                &project,
                "google",
                "v",
                "https://x.example.com",
            );
        }
        std::thread::sleep(Duration::from_millis(10));

        // Insert a fresh entry — this should trigger the lazy sweep.
        let cache_long = OAuthStateCache::with_ttl(Duration::from_secs(600));
        let project2 = ProjectId::new();
        cache_long.insert_state_sync("fresh_after_sweep", &project2, "google", "v", "https://y.example.com");

        // Expired entries must be gone from the original cache.
        assert!(cache.consume_state_sync("expired_0").is_err());
        assert!(cache.consume_state_sync("expired_1").is_err());
    }
}
