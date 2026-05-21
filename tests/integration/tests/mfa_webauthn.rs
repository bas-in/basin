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

const RP_ID: &str = "localhost";
const ORIGIN: &str = "http://localhost";

fn b64url(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn sha256(data: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().into()
}

/// A software P-256 authenticator that produces real FIDO2 attestation +
/// assertion blobs. Mirrors the in-prod verifier in `basin-auth::mfa`.
struct SoftAuthenticator {
    signing_key: p256::ecdsa::SigningKey,
    credential_id: Vec<u8>,
}

impl SoftAuthenticator {
    fn new() -> Self {
        Self {
            signing_key: p256::ecdsa::SigningKey::random(&mut rand_core::OsRng),
            credential_id: b"soft-cred-int-01".to_vec(),
        }
    }

    fn cose_key(&self) -> Vec<u8> {
        use ciborium::value::{Integer, Value};
        let vk = p256::ecdsa::VerifyingKey::from(&self.signing_key);
        let point = vk.to_encoded_point(false);
        let x = point.x().unwrap().to_vec();
        let y = point.y().unwrap().to_vec();
        let map = Value::Map(vec![
            (Value::Integer(Integer::from(1)), Value::Integer(Integer::from(2))),
            (Value::Integer(Integer::from(3)), Value::Integer(Integer::from(-7))),
            (Value::Integer(Integer::from(-1)), Value::Integer(Integer::from(1))),
            (Value::Integer(Integer::from(-2)), Value::Bytes(x)),
            (Value::Integer(Integer::from(-3)), Value::Bytes(y)),
        ]);
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&map, &mut buf).unwrap();
        buf
    }

    fn auth_data(&self, sign_count: u32, include_cred: bool) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&sha256(RP_ID.as_bytes()));
        let mut flags = 0x01u8; // UP
        if include_cred {
            flags |= 0x40; // AT
        }
        out.push(flags);
        out.extend_from_slice(&sign_count.to_be_bytes());
        if include_cred {
            out.extend_from_slice(&[0u8; 16]); // aaguid
            out.extend_from_slice(&(self.credential_id.len() as u16).to_be_bytes());
            out.extend_from_slice(&self.credential_id);
            out.extend_from_slice(&self.cose_key());
        }
        out
    }

    fn client_data(&self, ty: &str, challenge: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "type": ty, "challenge": challenge, "origin": ORIGIN,
        }))
        .unwrap()
    }

    fn attestation(&self, challenge: &str, sign_count: u32) -> String {
        use ciborium::value::Value;
        let client_data = self.client_data("webauthn.create", challenge);
        let att_obj = Value::Map(vec![
            (Value::Text("fmt".into()), Value::Text("none".into())),
            (Value::Text("attStmt".into()), Value::Map(vec![])),
            (Value::Text("authData".into()), Value::Bytes(self.auth_data(sign_count, true))),
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

    let auth = SoftAuthenticator::new();
    let challenge = extract_challenge(&enrollment.creation_options_json);
    let attestation = auth.attestation(&challenge, 1);

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

    let auth = SoftAuthenticator::new();
    let attestation = auth.attestation("WRONG_CHALLENGE", 1);
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

    let auth = SoftAuthenticator::new();
    let challenge = extract_challenge(&enrollment.creation_options_json);
    let attestation = auth.attestation(&challenge, 1);
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
    // Counter must strictly advance past the registration value (1).
    let assertion = auth.assertion(&req_challenge, 2);

    let result = svc
        .verify_webauthn_challenge(None::<&MfaCache>, &enc, &project, uid, challenge_id, &assertion)
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

    let auth = SoftAuthenticator::new();
    let challenge = extract_challenge(&enrollment.creation_options_json);
    let attestation = auth.attestation(&challenge, 1);
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

    // Assertion over the wrong challenge: clientDataJSON binding fails.
    let bad_assertion = auth.assertion("BAD_CHALLENGE", 2);
    let err = svc
        .verify_webauthn_challenge(
            None::<&MfaCache>,
            &enc,
            &project,
            uid,
            challenge_id,
            &bad_assertion,
        )
        .await;
    assert!(err.is_err());
}

/// 6.SEC.P0.2: a sign-counter regression (cloned authenticator replaying an
/// old counter) is rejected even though the signature is valid.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webauthn_counter_regression_rejected() {
    let schema = unique_schema("mfa_wa_e8");
    let Some(svc) = try_make_svc(&schema).await else {
        return;
    };
    let enc = PlaintextEncryption;
    let project = ProjectId::new();
    let uid = make_user(&svc, &project, "wa8@example.com").await;

    let auth = SoftAuthenticator::new();
    let enrollment = svc
        .enroll_webauthn(None::<&MfaCache>, &project, uid, "Passkey")
        .await
        .expect("enroll");
    let challenge = extract_challenge(&enrollment.creation_options_json);
    // Register at counter 10.
    svc.verify_webauthn_factor(
        None::<&MfaCache>,
        &enc,
        &project,
        uid,
        enrollment.factor_id,
        enrollment.challenge_id,
        &auth.attestation(&challenge, 10),
    )
    .await
    .expect("verify");

    // First assertion advances to 11 → accepted.
    let (cid1, opts1) = svc
        .begin_webauthn_challenge(None::<&MfaCache>, &project, uid, enrollment.factor_id)
        .await
        .expect("begin1");
    svc.verify_webauthn_challenge(
        None::<&MfaCache>,
        &enc,
        &project,
        uid,
        cid1,
        &auth.assertion(&extract_challenge(&opts1), 11),
    )
    .await
    .expect("assertion at counter 11 accepted");

    // Second assertion REPLAYS counter 11 (no advance) → must be rejected.
    let (cid2, opts2) = svc
        .begin_webauthn_challenge(None::<&MfaCache>, &project, uid, enrollment.factor_id)
        .await
        .expect("begin2");
    let err = svc
        .verify_webauthn_challenge(
            None::<&MfaCache>,
            &enc,
            &project,
            uid,
            cid2,
            &auth.assertion(&extract_challenge(&opts2), 11),
        )
        .await;
    assert!(
        err.is_err(),
        "non-advancing sign-counter must be rejected as a clone"
    );
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

    let auth = SoftAuthenticator::new();
    let challenge = extract_challenge(&enrollment.creation_options_json);
    let attestation = auth.attestation(&challenge, 1);
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
    let auth = SoftAuthenticator::new();
    let attestation = auth.attestation("expired-challenge", 1);
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
