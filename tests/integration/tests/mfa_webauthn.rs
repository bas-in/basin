//! Integration tests for Phase 5.10.M — WebAuthn/passkey MFA.
//!
//! Uses the in-memory `MfaCache` + Postgres-backed `AuthService`. Tests skip
//! gracefully when Postgres is unavailable (same pattern as `oauth_flow.rs`).
//!
//! Test matrix:
//! - `webauthn_enroll_returns_creation_options`    — enrollment returns JSON challenge.
//! - `webauthn_verify_ok_issues_recovery_codes`    — attestation → recovery codes.
//! - `webauthn_verify_wrong_challenge_rejected`    — mismatched challenge fails.
//! - `webauthn_challenge_step_up_issues_aal2`      — assertion challenge → aal2 JWT.
//! - `webauthn_assertion_wrong_challenge_rejected` — wrong challenge fails.
//! - `webauthn_unenroll_requires_aal2`             — unenroll needs aal2.
//! - `webauthn_expired_challenge_rejected`         — expired challenge fails.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use basin_auth::config::{AuthConfig, SmtpConfig, SmtpTls};
use basin_auth::email::StubMailer;
use basin_auth::mfa::MfaCache;
use basin_auth::oauth::PlaintextEncryption;
use basin_auth::{Aal, AuthService};
use basin_common::ProjectId;
use ulid::Ulid;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

fn unique_schema(prefix: &str) -> String {
    format!("{}_{}", prefix, Ulid::new().to_string().to_lowercase())
}

fn base_cfg(schema: &str) -> AuthConfig {
    AuthConfig {
        jwt_secret: vec![7u8; 32],
        token_ttl: Duration::from_secs(3600),
        refresh_ttl: Duration::from_secs(86_400),
        catalog_dsn: Some(PG_URL.to_owned()),
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

async fn try_make_svc(schema: &str) -> Option<AuthService> {
    let cfg = base_cfg(schema);
    let mailer = Arc::new(StubMailer::new(cfg.smtp.from_email.clone()));
    match tokio::time::timeout(
        Duration::from_secs(2),
        AuthService::connect_with_mailer(cfg, mailer),
    )
    .await
    {
        Ok(Ok(svc)) => Some(svc),
        Ok(Err(e)) => {
            eprintln!("postgres unavailable, skipping mfa_webauthn test: {e}");
            None
        }
        Err(_) => {
            eprintln!("postgres connect timed out, skipping mfa_webauthn test");
            None
        }
    }
}

async fn make_user(svc: &AuthService, project: &ProjectId, email: &str) -> Uuid {
    svc.signup(project, email, "longenoughpassword")
        .await
        .expect("signup")
}

/// Build a minimal fake attestation response that echoes the challenge.
fn make_attestation(challenge: &str) -> String {
    let client_data = serde_json::json!({
        "type": "webauthn.create",
        "challenge": challenge,
        "origin": "http://localhost"
    });
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&client_data).unwrap());
    serde_json::json!({
        "type": "public-key",
        "id": "fake-cred-id",
        "rawId": "fake-cred-id",
        "response": {
            "clientDataJSON": b64,
            "attestationObject": base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(b"fake-att-obj")
        }
    })
    .to_string()
}

/// Build a minimal fake assertion response that echoes the challenge.
fn make_assertion(challenge: &str) -> String {
    let client_data = serde_json::json!({
        "type": "webauthn.get",
        "challenge": challenge,
        "origin": "http://localhost"
    });
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&client_data).unwrap());
    serde_json::json!({
        "type": "public-key",
        "id": "fake-cred-id",
        "rawId": "fake-cred-id",
        "response": {
            "clientDataJSON": b64,
            "authenticatorData": base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(b"fake-auth-data"),
            "signature": base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(b"fake-sig")
        }
    })
    .to_string()
}

fn extract_challenge(options_json: &str) -> String {
    let v: serde_json::Value = serde_json::from_str(options_json).expect("parse options");
    v["challenge"].as_str().expect("challenge").to_owned()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webauthn_enroll_returns_creation_options() {
    let schema = unique_schema("mfa_wa_e1");
    let Some(svc) = try_make_svc(&schema).await else {
        return;
    };
    let project = ProjectId::new();
    let uid = make_user(&svc, &project, "wa1@example.com").await;

    let enrollment = svc
        .enroll_webauthn(None::<&MfaCache>, &project, uid, "Passkey")
        .await
        .expect("enroll_webauthn");

    let opts: serde_json::Value =
        serde_json::from_str(&enrollment.creation_options_json).expect("parse options");
    assert!(opts["challenge"].is_string());
    assert_eq!(opts["rp"]["name"].as_str(), Some("Basin"));
    assert!(!enrollment.factor_id.is_nil());
    assert!(!enrollment.challenge_id.is_nil());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webauthn_verify_ok_issues_recovery_codes() {
    let schema = unique_schema("mfa_wa_e2");
    let Some(svc) = try_make_svc(&schema).await else {
        return;
    };
    let enc = PlaintextEncryption;
    let project = ProjectId::new();
    let uid = make_user(&svc, &project, "wa2@example.com").await;

    let enrollment = svc
        .enroll_webauthn(None::<&MfaCache>, &project, uid, "Passkey")
        .await
        .expect("enroll");

    let challenge = extract_challenge(&enrollment.creation_options_json);
    let attestation = make_attestation(&challenge);

    let codes = svc
        .verify_webauthn_factor(
            None::<&MfaCache>,
            &enc,
            &project,
            uid,
            enrollment.factor_id,
            enrollment.challenge_id,
            &attestation,
        )
        .await
        .expect("verify_webauthn_factor");

    assert!(codes.is_some());
    assert_eq!(codes.unwrap().len(), 8);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webauthn_verify_wrong_challenge_rejected() {
    let schema = unique_schema("mfa_wa_e3");
    let Some(svc) = try_make_svc(&schema).await else {
        return;
    };
    let enc = PlaintextEncryption;
    let project = ProjectId::new();
    let uid = make_user(&svc, &project, "wa3@example.com").await;

    let enrollment = svc
        .enroll_webauthn(None::<&MfaCache>, &project, uid, "Passkey")
        .await
        .expect("enroll");

    let attestation = make_attestation("WRONG_CHALLENGE");
    let err = svc
        .verify_webauthn_factor(
            None::<&MfaCache>,
            &enc,
            &project,
            uid,
            enrollment.factor_id,
            enrollment.challenge_id,
            &attestation,
        )
        .await;
    assert!(err.is_err());
    assert!(
        err.unwrap_err().to_string().to_lowercase().contains("challenge"),
        "error must mention challenge"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webauthn_challenge_step_up_issues_aal2() {
    let schema = unique_schema("mfa_wa_e4");
    let Some(svc) = try_make_svc(&schema).await else {
        return;
    };
    let enc = PlaintextEncryption;
    let project = ProjectId::new();
    let uid = make_user(&svc, &project, "wa4@example.com").await;

    let enrollment = svc
        .enroll_webauthn(None::<&MfaCache>, &project, uid, "Passkey")
        .await
        .expect("enroll");

    let challenge = extract_challenge(&enrollment.creation_options_json);
    let attestation = make_attestation(&challenge);
    svc.verify_webauthn_factor(
        None::<&MfaCache>,
        &enc,
        &project,
        uid,
        enrollment.factor_id,
        enrollment.challenge_id,
        &attestation,
    )
    .await
    .expect("verify factor");

    let (challenge_id, options_json) = svc
        .begin_webauthn_challenge(None::<&MfaCache>, &project, uid, enrollment.factor_id)
        .await
        .expect("begin_webauthn_challenge");

    let req_challenge = extract_challenge(&options_json);
    let assertion = make_assertion(&req_challenge);

    let result = svc
        .verify_webauthn_challenge(None::<&MfaCache>, &project, uid, challenge_id, &assertion)
        .await
        .expect("verify_webauthn_challenge");

    let claims = svc.verify_jwt(&result.tokens.access_token).unwrap();
    assert_eq!(claims.aal, Aal::Aal2);
    assert!(claims.amr.contains(&"webauthn".to_string()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webauthn_assertion_wrong_challenge_rejected() {
    let schema = unique_schema("mfa_wa_e5");
    let Some(svc) = try_make_svc(&schema).await else {
        return;
    };
    let enc = PlaintextEncryption;
    let project = ProjectId::new();
    let uid = make_user(&svc, &project, "wa5@example.com").await;

    let enrollment = svc
        .enroll_webauthn(None::<&MfaCache>, &project, uid, "Passkey")
        .await
        .expect("enroll");

    let challenge = extract_challenge(&enrollment.creation_options_json);
    let attestation = make_attestation(&challenge);
    svc.verify_webauthn_factor(
        None::<&MfaCache>,
        &enc,
        &project,
        uid,
        enrollment.factor_id,
        enrollment.challenge_id,
        &attestation,
    )
    .await
    .expect("verify");

    let (challenge_id, _) = svc
        .begin_webauthn_challenge(None::<&MfaCache>, &project, uid, enrollment.factor_id)
        .await
        .expect("begin");

    let bad_assertion = make_assertion("BAD_CHALLENGE");
    let err = svc
        .verify_webauthn_challenge(
            None::<&MfaCache>,
            &project,
            uid,
            challenge_id,
            &bad_assertion,
        )
        .await;
    assert!(err.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webauthn_unenroll_requires_aal2() {
    let schema = unique_schema("mfa_wa_e6");
    let Some(svc) = try_make_svc(&schema).await else {
        return;
    };
    let enc = PlaintextEncryption;
    let project = ProjectId::new();
    let uid = make_user(&svc, &project, "wa6@example.com").await;

    let enrollment = svc
        .enroll_webauthn(None::<&MfaCache>, &project, uid, "Passkey")
        .await
        .expect("enroll");

    let challenge = extract_challenge(&enrollment.creation_options_json);
    let attestation = make_attestation(&challenge);
    svc.verify_webauthn_factor(
        None::<&MfaCache>,
        &enc,
        &project,
        uid,
        enrollment.factor_id,
        enrollment.challenge_id,
        &attestation,
    )
    .await
    .expect("verify");

    let err = svc
        .unenroll_factor(
            None::<&MfaCache>,
            &project,
            uid,
            enrollment.factor_id,
            &Aal::Aal1,
        )
        .await;
    assert!(err.is_err(), "aal1 must not unenroll");

    svc.unenroll_factor(
        None::<&MfaCache>,
        &project,
        uid,
        enrollment.factor_id,
        &Aal::Aal2,
    )
    .await
    .expect("aal2 unenroll");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webauthn_expired_challenge_rejected() {
    let schema = unique_schema("mfa_wa_e7");
    let Some(svc) = try_make_svc(&schema).await else {
        return;
    };
    let project = ProjectId::new();
    let uid = make_user(&svc, &project, "wa7@example.com").await;

    let enrollment = svc
        .enroll_webauthn(None::<&MfaCache>, &project, uid, "Passkey")
        .await
        .expect("enroll");

    // Overwrite the challenge with an expired one.
    let expired_id = enrollment.challenge_id;
    {
        let mut challenges = svc.mfa_cache().challenges.lock().unwrap();
        challenges.retain(|c| c.id != expired_id);
        challenges.push(basin_auth::MfaChallengeRow {
            id: expired_id,
            factor_id: enrollment.factor_id,
            user_id: uid,
            project_id: project.to_string(),
            expires_at: chrono::Utc::now() - chrono::Duration::seconds(10),
            challenge_data: "expired-challenge".to_owned(),
        });
    }

    let enc = PlaintextEncryption;
    let attestation = make_attestation("expired-challenge");
    let err = svc
        .verify_webauthn_factor(
            None::<&MfaCache>,
            &enc,
            &project,
            uid,
            enrollment.factor_id,
            expired_id,
            &attestation,
        )
        .await;
    assert!(err.is_err(), "expired challenge must fail");
}
