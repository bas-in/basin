//! Provider-registration unit tests for the cloud-broker OAuth catalog
//! (T-015 / T-033 / T-036). Lives as an external integration test (not
//! `#[cfg(test)] mod tests`) so the public surface an out-of-tree consumer
//! sees — `preset()` and `PRESET_PROVIDER_NAMES`, which a control plane's
//! provider-registration UI drives — is exercised through the published API
//! rather than from inside the crate.
//!
//! Also contains tests for the Apple ES256 JWT `client_secret` signer
//! (closes #54 P0 Apple OAuth).
//!
//! Pure offline: no DB, no network. The flow-level smoke for these
//! providers lives in `tests/integration/tests/oauth_providers.rs`.

use basin_auth::oauth::{build_apple_client_secret_jwt, preset, PRESET_PROVIDER_NAMES};

/// Sanity: the canonical list and the dispatch arm must agree.
#[test]
fn preset_dispatch_covers_every_listed_name() {
    for name in PRESET_PROVIDER_NAMES {
        let p = preset(name).unwrap_or_else(|| {
            panic!(
                "PRESET_PROVIDER_NAMES advertises {name:?} but preset() \
                 returns None — cloud broker would return `invalid_provider`"
            )
        });
        assert_eq!(
            p.name, *name,
            "preset({name}).name = {:?}, expected {name:?}",
            p.name
        );
    }
}

/// Expect at least the 14 canonical providers we documented:
/// google, github, apple (original) +
/// bitbucket, discord, figma, gitlab, linkedin, microsoft, notion, slack,
/// spotify, twitch, twitter_x (cloud-broker batch).
#[test]
fn preset_list_contains_all_documented_providers() {
    let expected: &[&str] = &[
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
    for name in expected {
        assert!(
            PRESET_PROVIDER_NAMES.contains(name),
            "PRESET_PROVIDER_NAMES is missing canonical provider {name:?}"
        );
    }
    assert!(
        PRESET_PROVIDER_NAMES.len() >= expected.len(),
        "PRESET_PROVIDER_NAMES shorter than expected catalog"
    );
}

/// Each new provider's endpoint URLs must point at the documented host.
/// Locks the canonical preset so a future bulk-rename doesn't silently
/// rewrite a customer-facing OAuth endpoint.
#[test]
fn new_provider_endpoints_lock_to_documented_hosts() {
    let cases: &[(&str, &str, &str)] = &[
        // (provider, authorize_host_fragment, default_scope_fragment)
        ("microsoft", "login.microsoftonline.com", "openid"),
        ("gitlab", "gitlab.com", "read_user"),
        ("slack", "slack.com", "openid"),
        ("discord", "discord.com", "identify"),
        ("apple", "appleid.apple.com", "email"),
        ("twitter_x", "twitter.com", "users.read"),
        ("bitbucket", "bitbucket.org", "account"),
        ("notion", "notion.com", ""),
        ("spotify", "spotify.com", "user-read-email"),
        ("twitch", "twitch.tv", "openid"),
        ("linkedin", "linkedin.com", "openid"),
        ("figma", "figma.com", "files:read"),
    ];
    for (name, host, scope_frag) in cases {
        let p = preset(name).unwrap_or_else(|| panic!("preset({name}) missing"));
        assert!(
            p.authorize_url.contains(host),
            "{name}.authorize_url ({}) missing host fragment {host:?}",
            p.authorize_url
        );
        assert!(
            p.token_url.starts_with("https://"),
            "{name}.token_url must be https"
        );
        if !scope_frag.is_empty() {
            assert!(
                p.default_scopes.contains(scope_frag),
                "{name}.default_scopes ({}) missing fragment {scope_frag:?}",
                p.default_scopes
            );
        }
    }
}

/// `microsoft` / `azure_ad` must share a preset; same for
/// `twitter_x` / `twitter`. Aliases let the cloud broker accept either
/// spelling without minting two divergent provider rows.
#[test]
fn provider_aliases_share_a_preset() {
    let microsoft = preset("microsoft").unwrap();
    let azure_ad = preset("azure_ad").unwrap();
    assert_eq!(microsoft.authorize_url, azure_ad.authorize_url);
    assert_eq!(microsoft.token_url, azure_ad.token_url);
    assert_eq!(microsoft.userinfo_url, azure_ad.userinfo_url);
    assert_eq!(microsoft.default_scopes, azure_ad.default_scopes);

    let twitter_x = preset("twitter_x").unwrap();
    let twitter = preset("twitter").unwrap();
    assert_eq!(twitter_x.authorize_url, twitter.authorize_url);
    assert_eq!(twitter_x.token_url, twitter.token_url);
    assert_eq!(twitter_x.userinfo_url, twitter.userinfo_url);
    assert_eq!(twitter_x.default_scopes, twitter.default_scopes);
}

/// Unknown names must return None so the broker emits `invalid_provider`
/// rather than fabricating an unusable authorize URL.
#[test]
fn unknown_provider_returns_none() {
    assert!(preset("").is_none());
    assert!(preset("saml_sso").is_none());
    assert!(preset("oauth").is_none());
    assert!(preset("not_a_provider_xyz").is_none());
}

// ---------------------------------------------------------------------------
// Apple ES256 JWT client_secret signer (closes #54 P0 Apple OAuth).
// ---------------------------------------------------------------------------
//
// Test key generated with:
//   openssl ecparam -name prime256v1 -genkey -noout | \
//     openssl pkcs8 -topk8 -nocrypt
// This is a throwaway key; it never touches Apple infrastructure.

/// PKCS#8 PEM private key for tests. NOT a real Apple key.
const TEST_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgt3YioOdxYEeQfuGm\n\
g0Na1/YqjxTk+rSbRHUgq4X0VE2hRANCAATjCOwhIRlBUUEqX+ee7Kwf1yODhieW\n\
FHyWx4yDM8/ijRHnIApB88tEPvVmhqhuB7CpMdfiFF+aRaVh1B9VXQ9e\n\
-----END PRIVATE KEY-----\n";

/// Corresponding public key (SubjectPublicKeyInfo PEM) for verification.
const TEST_PUBLIC_KEY_PEM: &str = "-----BEGIN PUBLIC KEY-----\n\
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE4wjsISEZQVFBKl/nnuysH9cjg4Yn\n\
lhR8lseMgzPP4o0R5yAKQfPLRD71ZoaobgewqTHX4hRfmkWlYdQfVV0PXg==\n\
-----END PUBLIC KEY-----\n";

/// `build_apple_client_secret_jwt` produces a JWT that:
/// 1. Has the correct `kid` header.
/// 2. Has `alg: ES256`.
/// 3. Verifies under the corresponding public key.
/// 4. Contains the expected `iss`, `sub`, `aud`, `iat`, `exp` claims.
#[test]
fn apple_jwt_verifies_under_public_key_and_claims_are_correct() {
    use jsonwebtoken::{Algorithm, DecodingKey, Header, Validation};
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct Claims {
        iss: String,
        sub: String,
        aud: String,
        iat: i64,
        exp: i64,
    }

    let team_id = "ABCD123456";
    let key_id = "KEYID12345";
    let client_id = "com.example.app";

    let jwt = build_apple_client_secret_jwt(team_id, key_id, client_id, TEST_PRIVATE_KEY_PEM)
        .expect("build_apple_client_secret_jwt should succeed");

    // Decode and verify the header separately to check kid + alg.
    let header: Header =
        jsonwebtoken::decode_header(&jwt).expect("JWT header should be decodable");
    assert_eq!(header.alg, Algorithm::ES256, "alg must be ES256");
    assert_eq!(
        header.kid.as_deref(),
        Some(key_id),
        "kid header must match key_id"
    );

    // Verify signature + decode claims using the public key.
    let decoding_key =
        DecodingKey::from_ec_pem(TEST_PUBLIC_KEY_PEM.as_bytes()).expect("public key should parse");
    let mut validation = Validation::new(Algorithm::ES256);
    // Apple `aud` is a single string scalar; jsonwebtoken's `set_audience`
    // accepts it just fine and the JSON deserialize side stays String too.
    validation.set_audience(&["https://appleid.apple.com"]);

    let token_data = jsonwebtoken::decode::<Claims>(&jwt, &decoding_key, &validation)
        .expect("JWT signature should verify under the test public key");

    let claims = token_data.claims;
    assert_eq!(claims.iss, team_id, "iss must equal team_id");
    assert_eq!(claims.sub, client_id, "sub must equal client_id");
    assert_eq!(
        claims.aud, "https://appleid.apple.com",
        "aud must be Apple's token endpoint"
    );

    // iat should be in the past (or at most a few seconds ago).
    let now = chrono::Utc::now().timestamp();
    assert!(
        claims.iat <= now + 2,
        "iat ({}) should be <= now ({})",
        claims.iat,
        now
    );
    // exp should be ~180 days in the future.
    let expected_exp_min = now + 179 * 24 * 60 * 60;
    let expected_exp_max = now + 181 * 24 * 60 * 60;
    assert!(
        claims.exp >= expected_exp_min && claims.exp <= expected_exp_max,
        "exp ({}) should be ~180 days from now ({now})",
        claims.exp
    );
}

/// `build_apple_client_secret_jwt` returns an error given a garbage PEM.
#[test]
fn apple_jwt_rejects_invalid_pem() {
    let result =
        build_apple_client_secret_jwt("TEAM123456", "KEY1234567", "com.example.app", "not-a-pem");
    assert!(result.is_err(), "invalid PEM must return Err");
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("invalid EC PEM"),
        "error should mention invalid EC PEM, got: {msg}"
    );
}

/// Two successive calls with the same inputs should produce structurally valid
/// JWTs. They will differ in `iat` only by the sub-second resolution of the
/// clock, but both must verify under the same public key.
#[test]
fn apple_jwt_two_calls_both_verify() {
    use jsonwebtoken::{Algorithm, DecodingKey, Validation};
    use serde::Deserialize;
    #[derive(Deserialize)]
    struct Claims {
        iss: String,
        sub: String,
    }

    let decode = |jwt: &str| {
        let dk = DecodingKey::from_ec_pem(TEST_PUBLIC_KEY_PEM.as_bytes()).unwrap();
        let mut v = Validation::new(Algorithm::ES256);
        v.set_audience(&["https://appleid.apple.com"]);
        jsonwebtoken::decode::<Claims>(jwt, &dk, &v).expect("both JWTs should verify")
    };

    let j1 = build_apple_client_secret_jwt(
        "TEAMXXXXXX",
        "KEYXXXXXXX",
        "com.example.myapp",
        TEST_PRIVATE_KEY_PEM,
    )
    .unwrap();
    let j2 = build_apple_client_secret_jwt(
        "TEAMXXXXXX",
        "KEYXXXXXXX",
        "com.example.myapp",
        TEST_PRIVATE_KEY_PEM,
    )
    .unwrap();

    let c1 = decode(&j1).claims;
    let c2 = decode(&j2).claims;
    assert_eq!(c1.iss, "TEAMXXXXXX");
    assert_eq!(c1.sub, "com.example.myapp");
    assert_eq!(c2.iss, c1.iss);
    assert_eq!(c2.sub, c1.sub);
}
