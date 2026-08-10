//! Integration tests for Phase 5.10.O — OAuth providers.
//!
//! Tests the full authorize→callback→JWT flow using a mock provider
//! (in-memory `OAuthStateCache` + a minimal axum HTTP server). No external
//! network or real OAuth provider required.
//!
//! Test matrix:
//! - `pkce_pair_s256_is_correct`              — verifier→S256 challenge math.
//! - `state_hmac_round_trip`                  — signed state verifies.
//! - `state_hmac_tampered_rejected`           — tampered state fails.
//! - `csrf_state_wrong_provider_rejected`     — cross-provider state rejected.
//! - `open_redirect_*`                        — redirect_to validation.
//! - `plaintext_enc_round_trip`               — PlaintextEncryption self-check.
//! - `provider_presets_have_endpoints`        — google/github/apple presets.
//! - `provider_preset_unknown_is_none`        — unknown preset = None.
//! - `state_cache_single_use`                 — state is consumed once.
//! - `authorize_callback_issues_jwt`          — full flow → valid Basin JWT.
//! - `callback_csrf_state_rejected`           — tampered state → error.
//! - `callback_state_single_use`              — replayed state → error.
//! - `verified_email_links_existing_user`     — second sign-in reuses user.
//! - `open_redirect_rejected_at_authorize`    — evil redirect_to → error.

use std::sync::Arc;
use std::time::Duration;

use basin_auth::config::{AuthConfig, SmtpConfig, SmtpTls};
use basin_auth::email::StubMailer;
use basin_auth::oauth::{
    create_state, validate_redirect_to, EncryptionProvider, OAuthStateCache, PlaintextEncryption,
};
use basin_auth::AuthService;
use basin_common::ProjectId;

// ---------------------------------------------------------------------------
// Configuration helper
// ---------------------------------------------------------------------------

fn base_cfg(schema: &str) -> AuthConfig {
    AuthConfig {
        jwt_secret: vec![7u8; 32],
        token_ttl: Duration::from_secs(3600),
        refresh_ttl: Duration::from_secs(86_400),
        catalog_dsn: None,
        catalog_schema: schema.to_owned(),
        smtp: SmtpConfig {
            host: "smtp.invalid".into(),
            port: 587,
            username: "u".into(),
            password: "p".into(),
            from_email: "noreply@example.com".into(),
            from_name: None,
            tls: SmtpTls::StartTls,
        },
        bcrypt_cost: 4,
        password_min_len: 10,
        rate_limit_per_ip_per_min: 1000,
        email_enabled: false,
        pgwire_public_host: "127.0.0.1:5433".into(),
    }
}

// ---------------------------------------------------------------------------
// Minimal mock OAuth HTTP server
// ---------------------------------------------------------------------------

/// An axum-backed mock OAuth provider. Answers:
/// - `POST /token`    → `{"access_token":..., "token_type":"bearer"}`
/// - `GET  /userinfo` → `{"sub":..., "email":..., "email_verified":...}`
struct MockProvider {
    pub base_url: String,
    _server: tokio::task::JoinHandle<()>,
}

impl MockProvider {
    async fn start(
        access_token: &str,
        user_sub: &str,
        user_email: &str,
        email_verified: bool,
    ) -> Self {
        use axum::{routing::get, routing::post, Json, Router};
        use serde_json::json;

        let at = access_token.to_owned();
        let sub = user_sub.to_owned();
        let em = user_email.to_owned();

        let app = Router::new()
            .route(
                "/token",
                post({
                    let at2 = at.clone();
                    move || {
                        let at3 = at2.clone();
                        async move { Json(json!({ "access_token": at3, "token_type": "bearer" })) }
                    }
                }),
            )
            .route(
                "/userinfo",
                get({
                    let s2 = sub.clone();
                    let e2 = em.clone();
                    move || {
                        let s3 = s2.clone();
                        let e3 = e2.clone();
                        async move {
                            Json(
                                json!({ "sub": s3, "email": e3, "email_verified": email_verified }),
                            )
                        }
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://127.0.0.1:{}", addr.port());
        let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        Self {
            base_url,
            _server: handle,
        }
    }
}

// ---------------------------------------------------------------------------
// AuthService helper (skip when Postgres unavailable)
// ---------------------------------------------------------------------------

async fn try_make_auth_service() -> Option<AuthService> {
    use ulid::Ulid;
    let schema = format!("oauth_test_{}", Ulid::new().to_string().to_lowercase());
    let cfg = base_cfg(&schema);
    let mailer = Arc::new(StubMailer::new(cfg.smtp.from_email.clone()));
    match tokio::time::timeout(
        Duration::from_secs(2),
        AuthService::connect_with_mailer(cfg, mailer),
    )
    .await
    {
        Ok(Ok(svc)) => Some(svc),
        Ok(Err(e)) => {
            eprintln!("basin auth unavailable, skipping: {e}");
            None
        }
        Err(_) => {
            eprintln!("basin auth connect timed out, skipping");
            None
        }
    }
}

/// Type alias to avoid repeating the long turbofish in every test.
type MaybePg<'a> = Option<&'a basin_auth::store::postgres::PostgresAuthStore>;

/// Returns `None` with the right type for the oauth store parameter.
#[inline]
fn no_pg() -> MaybePg<'static> {
    None
}

// ---------------------------------------------------------------------------
// Unit tests — always run, no DB, no network
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pkce_pair_s256_is_correct() {
    use base64::Engine;
    use sha2::{Digest, Sha256};
    let (verifier, challenge) = basin_auth::oauth::pkce_pair();
    let expected = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(Sha256::digest(verifier.as_bytes()));
    assert_eq!(challenge, expected);
}

#[tokio::test]
async fn state_hmac_round_trip() {
    let secret = b"test_secret_32bytes_paddddddddddd";
    let project = ProjectId::new();
    let state = create_state(secret, &project, "google");
    basin_auth::oauth::verify_state_hmac(secret, &project, "google", &state.value).unwrap();
}

#[tokio::test]
async fn state_hmac_tampered_rejected() {
    let secret = b"test_secret_32bytes_paddddddddddd";
    let project = ProjectId::new();
    let state = create_state(secret, &project, "github");
    let tampered = format!("{}x", state.value);
    assert!(basin_auth::oauth::verify_state_hmac(secret, &project, "github", &tampered).is_err());
}

#[tokio::test]
async fn csrf_state_wrong_provider_rejected() {
    let secret = b"test_secret_32bytes_paddddddddddd";
    let project = ProjectId::new();
    let state = create_state(secret, &project, "google");
    assert!(
        basin_auth::oauth::verify_state_hmac(secret, &project, "github", &state.value).is_err()
    );
}

#[test]
fn open_redirect_relative_allowed() {
    validate_redirect_to("/dashboard", &[]).unwrap();
    validate_redirect_to("", &[]).unwrap();
}

#[test]
fn open_redirect_absolute_rejected_without_allowlist() {
    assert!(validate_redirect_to("https://evil.com/steal", &[]).is_err());
}

#[test]
fn open_redirect_absolute_matches_allowlist() {
    let allowed = vec!["https://myapp.com".to_owned()];
    validate_redirect_to("https://myapp.com/callback", &allowed).unwrap();
}

#[test]
fn open_redirect_javascript_scheme_rejected() {
    assert!(validate_redirect_to("javascript:alert(1)", &[]).is_err());
}

#[test]
fn plaintext_enc_round_trip() {
    let enc = PlaintextEncryption;
    let ct = enc.encrypt("supersecret").unwrap();
    assert_eq!(enc.decrypt(&ct).unwrap(), "supersecret");
}

#[test]
fn provider_presets_have_endpoints() {
    for name in &["google", "github", "apple"] {
        let p =
            basin_auth::oauth::preset(name).unwrap_or_else(|| panic!("preset {name} must exist"));
        assert!(!p.authorize_url.is_empty());
        assert!(!p.token_url.is_empty());
        assert!(!p.userinfo_url.is_empty());
    }
}

#[test]
fn provider_preset_unknown_is_none() {
    assert!(basin_auth::oauth::preset("saml").is_none());
}

#[test]
fn state_cache_single_use() {
    let cache = OAuthStateCache::new();
    let project = ProjectId::new();
    cache.insert_state_sync("myhash", &project, "github", "verifier42", "/home");
    let row = cache.consume_state_sync("myhash").unwrap();
    assert_eq!(row.provider, "github");
    // Second consume must fail (single-use).
    assert!(cache.consume_state_sync("myhash").is_err());
}

// ---------------------------------------------------------------------------
// Integration tests — full authorize→callback→JWT
// ---------------------------------------------------------------------------

/// Full flow: authorize → PKCE redirect URL → callback → valid Basin JWT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authorize_callback_issues_jwt() {
    let Some(svc) = try_make_auth_service().await else {
        return;
    };

    let mock = MockProvider::start("tok-abc", "sub-42", "alice@example.com", true).await;
    let project = ProjectId::new();
    let provider = "mockp";
    let enc = PlaintextEncryption;

    svc.register_mock_oauth_provider(
        &project,
        provider,
        "cid",
        "csecret",
        &format!("{}/authorize", mock.base_url),
        &format!("{}/token", mock.base_url),
        &format!("{}/userinfo", mock.base_url),
        "openid email",
        "https://myapp.com",
    );

    let auth = svc
        .begin_oauth_authorize(no_pg(), &project, provider, "https://myapp.com/cb")
        .await
        .expect("begin_oauth_authorize must succeed");

    assert!(
        auth.redirect_url.contains("code_challenge="),
        "must have PKCE challenge"
    );
    assert!(
        auth.redirect_url.contains("code_challenge_method=S256"),
        "must use S256"
    );
    assert!(
        auth.redirect_url.contains("state="),
        "must have state param"
    );

    // Re-register for callback leg.
    svc.register_mock_oauth_provider(
        &project,
        provider,
        "cid",
        "csecret",
        &format!("{}/authorize", mock.base_url),
        &format!("{}/token", mock.base_url),
        &format!("{}/userinfo", mock.base_url),
        "openid email",
        "https://myapp.com",
    );

    let cb = svc
        .handle_oauth_callback(
            &enc,
            no_pg(),
            provider,
            "code-from-provider",
            &auth.state,
            "https://myapp.com/cb",
        )
        .await
        .expect("handle_oauth_callback must succeed");

    let claims = svc
        .verify_jwt(&cb.tokens.access_token)
        .expect("JWT must be valid");
    assert_eq!(claims.project_id, project);
    assert_eq!(claims.email, "alice@example.com");
    assert!(!cb.tokens.refresh_token.is_empty());
}

/// Tampered CSRF state must be rejected at callback time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callback_csrf_state_rejected() {
    let Some(svc) = try_make_auth_service().await else {
        return;
    };

    let project = ProjectId::new();
    let enc = PlaintextEncryption;

    svc.register_mock_oauth_provider(
        &project,
        "mockp2",
        "cid",
        "csec",
        "https://example.com/auth",
        "https://example.com/token",
        "https://example.com/userinfo",
        "openid",
        "",
    );

    let err = svc
        .handle_oauth_callback(
            &enc,
            no_pg(),
            "mockp2",
            "some-code",
            "tampered_nonce.deadbeef",
            "",
        )
        .await
        .unwrap_err();

    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("not found")
            || msg.contains("expired")
            || msg.contains("invalid")
            || msg.contains("hmac"),
        "tampered CSRF state must be rejected, got: {msg}"
    );
}

/// A state that was already consumed must be rejected (single-use).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callback_state_single_use() {
    let Some(svc) = try_make_auth_service().await else {
        return;
    };

    let mock = MockProvider::start("tok", "sub1", "bob@example.com", true).await;
    let project = ProjectId::new();
    let provider = "mock_su";
    let enc = PlaintextEncryption;

    let reg = || {
        svc.register_mock_oauth_provider(
            &project,
            provider,
            "cid",
            "csec",
            &format!("{}/authorize", mock.base_url),
            &format!("{}/token", mock.base_url),
            &format!("{}/userinfo", mock.base_url),
            "openid email",
            "",
        );
    };

    reg();
    let auth = svc
        .begin_oauth_authorize(no_pg(), &project, provider, "")
        .await
        .unwrap();

    // First callback succeeds.
    reg();
    svc.handle_oauth_callback(&enc, no_pg(), provider, "code1", &auth.state, "")
        .await
        .unwrap();

    // Second callback with same state must fail.
    reg();
    let err = svc
        .handle_oauth_callback(&enc, no_pg(), provider, "code1", &auth.state, "")
        .await
        .unwrap_err();

    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("not found") || msg.contains("expired") || msg.contains("invalid"),
        "replayed state must be rejected, got: {msg}"
    );
}

/// A second OAuth sign-in with the same verified email reuses the existing user.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_email_links_existing_user() {
    let Some(svc) = try_make_auth_service().await else {
        return;
    };

    let mock = MockProvider::start("tok-xyz", "sub99", "carol@example.com", true).await;
    let project = ProjectId::new();
    let provider = "mock_link";
    let enc = PlaintextEncryption;

    let reg = || {
        svc.register_mock_oauth_provider(
            &project,
            provider,
            "cid",
            "csec",
            &format!("{}/authorize", mock.base_url),
            &format!("{}/token", mock.base_url),
            &format!("{}/userinfo", mock.base_url),
            "openid email",
            "",
        );
    };

    // First sign-in.
    reg();
    let a1 = svc
        .begin_oauth_authorize(no_pg(), &project, provider, "")
        .await
        .unwrap();
    reg();
    let r1 = svc
        .handle_oauth_callback(&enc, no_pg(), provider, "code-a", &a1.state, "")
        .await
        .unwrap();
    let uid1 = svc.verify_jwt(&r1.tokens.access_token).unwrap().user_id;

    // Second sign-in (same verified email → same user).
    reg();
    let a2 = svc
        .begin_oauth_authorize(no_pg(), &project, provider, "")
        .await
        .unwrap();
    reg();
    let r2 = svc
        .handle_oauth_callback(&enc, no_pg(), provider, "code-b", &a2.state, "")
        .await
        .unwrap();
    let uid2 = svc.verify_jwt(&r2.tokens.access_token).unwrap().user_id;

    assert_eq!(
        uid1, uid2,
        "second sign-in with same verified email must reuse the same user"
    );
}

/// 6.SEC.P1 — account-takeover via unverified provider email.
///
/// A callback whose email matches an existing account but whose
/// `email_verified=false` must NOT be auto-linked into that account. It must
/// resolve to a DISTINCT user. (Here "existing account" is an existing OAuth
/// identity created by a verified first sign-in; the second sign-in uses a
/// different provider `sub` + the same email but `email_verified=false`.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unverified_email_does_not_link_to_existing_account() {
    let Some(svc) = try_make_auth_service().await else {
        return;
    };

    let project = ProjectId::new();
    let provider = "mock_unverified";
    let enc = PlaintextEncryption;
    let email = "victim@example.com";

    // 1. Verified first sign-in (sub-trusted) establishes the account.
    let mock_v = MockProvider::start("tok-v", "sub-trusted", email, true).await;
    let reg_v = || {
        svc.register_mock_oauth_provider(
            &project,
            provider,
            "cid",
            "csec",
            &format!("{}/authorize", mock_v.base_url),
            &format!("{}/token", mock_v.base_url),
            &format!("{}/userinfo", mock_v.base_url),
            "openid email",
            "",
        );
    };
    reg_v();
    let a1 = svc
        .begin_oauth_authorize(no_pg(), &project, provider, "")
        .await
        .unwrap();
    reg_v();
    let r1 = svc
        .handle_oauth_callback(&enc, no_pg(), provider, "code-v", &a1.state, "")
        .await
        .unwrap();
    let uid_verified = svc.verify_jwt(&r1.tokens.access_token).unwrap().user_id;

    // 2. Attacker sign-in: SAME email, DIFFERENT sub, email_verified=false.
    let mock_u = MockProvider::start("tok-u", "sub-attacker", email, false).await;
    let reg_u = || {
        svc.register_mock_oauth_provider(
            &project,
            provider,
            "cid",
            "csec",
            &format!("{}/authorize", mock_u.base_url),
            &format!("{}/token", mock_u.base_url),
            &format!("{}/userinfo", mock_u.base_url),
            "openid email",
            "",
        );
    };
    reg_u();
    let a2 = svc
        .begin_oauth_authorize(no_pg(), &project, provider, "")
        .await
        .unwrap();
    reg_u();
    let r2 = svc
        .handle_oauth_callback(&enc, no_pg(), provider, "code-u", &a2.state, "")
        .await
        .unwrap();
    let uid_unverified = svc.verify_jwt(&r2.tokens.access_token).unwrap().user_id;

    assert_ne!(
        uid_verified, uid_unverified,
        "unverified provider email must NOT auto-link into the existing verified account (account takeover)"
    );
}

/// 6.SEC.P1 — the legitimate flow: a returning user identified by a
/// pre-existing `(provider, provider_user_id)` identity logs into the SAME
/// account. The match is keyed by `sub`, independent of `email_verified` on
/// the *return* leg.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn existing_identity_by_sub_logs_into_same_account() {
    let Some(svc) = try_make_auth_service().await else {
        return;
    };

    let mock = MockProvider::start("tok-sub", "sub-stable", "dave@example.com", true).await;
    let project = ProjectId::new();
    let provider = "mock_sub_stable";
    let enc = PlaintextEncryption;

    let reg = || {
        svc.register_mock_oauth_provider(
            &project,
            provider,
            "cid",
            "csec",
            &format!("{}/authorize", mock.base_url),
            &format!("{}/token", mock.base_url),
            &format!("{}/userinfo", mock.base_url),
            "openid email",
            "",
        );
    };

    reg();
    let a1 = svc
        .begin_oauth_authorize(no_pg(), &project, provider, "")
        .await
        .unwrap();
    reg();
    let r1 = svc
        .handle_oauth_callback(&enc, no_pg(), provider, "c1", &a1.state, "")
        .await
        .unwrap();
    let uid1 = svc.verify_jwt(&r1.tokens.access_token).unwrap().user_id;

    reg();
    let a2 = svc
        .begin_oauth_authorize(no_pg(), &project, provider, "")
        .await
        .unwrap();
    reg();
    let r2 = svc
        .handle_oauth_callback(&enc, no_pg(), provider, "c2", &a2.state, "")
        .await
        .unwrap();
    let uid2 = svc.verify_jwt(&r2.tokens.access_token).unwrap().user_id;

    assert_eq!(
        uid1, uid2,
        "returning user matched by (provider, sub) must reuse the same account"
    );
}

/// 6.SEC.P1 — provider `sub` collision: the SAME `sub` value asserted by two
/// DIFFERENT providers must resolve to two DISTINCT identities/accounts. The
/// identity key is `(provider, provider_user_id)`, never the bare `sub`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_sub_across_providers_resolves_distinct_identities() {
    let Some(svc) = try_make_auth_service().await else {
        return;
    };

    let project = ProjectId::new();
    let enc = PlaintextEncryption;
    let shared_sub = "12345";

    // Provider A: unverified email so we never collapse via the email-link path.
    let mock_a = MockProvider::start("tok-a", shared_sub, "a@provider-a.example", false).await;
    let prov_a = "mock_prov_a";
    let reg_a = || {
        svc.register_mock_oauth_provider(
            &project,
            prov_a,
            "cid",
            "csec",
            &format!("{}/authorize", mock_a.base_url),
            &format!("{}/token", mock_a.base_url),
            &format!("{}/userinfo", mock_a.base_url),
            "openid email",
            "",
        );
    };
    reg_a();
    let aa = svc
        .begin_oauth_authorize(no_pg(), &project, prov_a, "")
        .await
        .unwrap();
    reg_a();
    let ra = svc
        .handle_oauth_callback(&enc, no_pg(), prov_a, "ca", &aa.state, "")
        .await
        .unwrap();
    let uid_a = svc.verify_jwt(&ra.tokens.access_token).unwrap().user_id;

    // Provider B: SAME sub, different provider + different email.
    let mock_b = MockProvider::start("tok-b", shared_sub, "b@provider-b.example", false).await;
    let prov_b = "mock_prov_b";
    let reg_b = || {
        svc.register_mock_oauth_provider(
            &project,
            prov_b,
            "cid",
            "csec",
            &format!("{}/authorize", mock_b.base_url),
            &format!("{}/token", mock_b.base_url),
            &format!("{}/userinfo", mock_b.base_url),
            "openid email",
            "",
        );
    };
    reg_b();
    let ab = svc
        .begin_oauth_authorize(no_pg(), &project, prov_b, "")
        .await
        .unwrap();
    reg_b();
    let rb = svc
        .handle_oauth_callback(&enc, no_pg(), prov_b, "cb", &ab.state, "")
        .await
        .unwrap();
    let uid_b = svc.verify_jwt(&rb.tokens.access_token).unwrap().user_id;

    assert_ne!(
        uid_a, uid_b,
        "the same sub from two different providers must NOT collapse into one identity"
    );
}

/// Absolute `redirect_to` not in the allowlist must be rejected at authorize time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_redirect_rejected_at_authorize() {
    let Some(svc) = try_make_auth_service().await else {
        return;
    };

    let project = ProjectId::new();
    let provider = "mock_redir";

    // Allowlist = "https://myapp.com" only.
    svc.register_mock_oauth_provider(
        &project,
        provider,
        "cid",
        "csec",
        "https://example.com/auth",
        "https://example.com/token",
        "https://example.com/userinfo",
        "openid",
        "https://myapp.com",
    );

    let err = svc
        .begin_oauth_authorize(no_pg(), &project, provider, "https://evil.com/steal")
        .await
        .unwrap_err();

    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("allowlist") || msg.contains("not in") || msg.contains("invalid"),
        "open redirect to evil.com must be rejected, got: {msg}"
    );
}
