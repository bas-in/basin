//! Integration smoke test for the OAuth provider catalog
//! (T-015 / T-033 / T-036 — cloud-broker `BasinAuthProvider` gate).
//!
//! Pure offline test: iterates every preset name advertised by
//! `basin_auth::oauth::PRESET_PROVIDER_NAMES`, registers a mock provider
//! row for each in an in-memory `OAuthStateCache`, and asserts every
//! provider resolves to a well-formed authorize URL (correct host,
//! https scheme, non-empty token endpoint).
//!
//! No real HTTP, no DB. Catches the regression where the cloud broker
//! advertises a provider whose name is not wired into `preset()` and the
//! server returns `invalid_provider` at /auth/v1/authorize.

use basin_auth::oauth::{preset, OAuthStateCache, PRESET_PROVIDER_NAMES};
use basin_common::ProjectId;

/// The single source of truth for "the broker can dispatch to every
/// preset". For each canonical name in [`PRESET_PROVIDER_NAMES`]:
///
/// 1. `preset(name)` must resolve.
/// 2. The endpoints must be HTTPS (cloud broker refuses plaintext).
/// 3. `register_mock_provider` must accept the preset endpoints and
///    store them in the in-memory cache (mirrors what
///    `BasinAuthProvider::register_dynamic` does on the cloud side).
#[test]
fn every_preset_provider_can_register_in_cache() {
    let cache = OAuthStateCache::new();
    let project = ProjectId::new();
    let redirect_uri = "https://app.example.com/auth/v1/callback";

    assert!(
        PRESET_PROVIDER_NAMES.len() >= 14,
        "PRESET_PROVIDER_NAMES must list at least the 14 canonical providers \
         (3 original + 11 new); the cloud broker rejects names absent from \
         this list with `invalid_provider`."
    );

    for name in PRESET_PROVIDER_NAMES {
        let p = preset(name).unwrap_or_else(|| {
            panic!("PRESET_PROVIDER_NAMES contains {name:?} but preset() returns None")
        });

        // Endpoint shape — broker invariant: never advertise an http://
        // OAuth endpoint, never accept an empty token URL.
        assert!(
            p.authorize_url.starts_with("https://"),
            "{name}: authorize_url must be https, got {:?}",
            p.authorize_url
        );
        assert!(
            p.token_url.starts_with("https://"),
            "{name}: token_url must be https, got {:?}",
            p.token_url
        );
        assert!(
            p.userinfo_url.starts_with("https://"),
            "{name}: userinfo_url must be https, got {:?}",
            p.userinfo_url
        );
        assert!(
            !p.authorize_url.is_empty()
                && !p.token_url.is_empty()
                && !p.userinfo_url.is_empty(),
            "{name}: endpoints must all be non-empty"
        );

        // Register a mock provider row using the preset defaults — this
        // is exactly what the cloud broker does at `/v1/oauth/providers`
        // dynamic-registration time.
        cache.register_mock_provider(
            &project,
            name,
            "client_id_xyz",
            "client_secret_xyz",
            p.authorize_url,
            p.token_url,
            p.userinfo_url,
            p.default_scopes,
            redirect_uri,
        );
    }
}

/// Per-provider host expectations. Locks the canonical preset endpoints so
/// a typo in a future bulk-rename PR (e.g. `figma.com` → `figma.io`) gets
/// caught here before the cloud broker bakes the wrong URL into a customer
/// project's `oauth_providers` row.
#[test]
fn preset_host_fragments_match_documented_providers() {
    let cases: &[(&str, &str)] = &[
        // google authorize is on accounts.google.com but token is on
        // oauth2.googleapis.com — use the common substring so both pass.
        ("google", "google"),
        ("github", "github.com"),
        ("apple", "appleid.apple.com"),
        ("bitbucket", "bitbucket.org"),
        ("discord", "discord.com"),
        ("figma", "figma.com"),
        ("gitlab", "gitlab.com"),
        ("linkedin", "linkedin.com"),
        ("microsoft", "login.microsoftonline.com"),
        ("notion", "notion.com"),
        ("slack", "slack.com"),
        ("spotify", "spotify.com"),
        ("twitch", "twitch.tv"),
        ("twitter_x", "twitter.com"),
    ];
    for (name, host) in cases {
        let p = preset(name).unwrap_or_else(|| panic!("preset({name}) missing"));
        assert!(
            p.authorize_url.contains(host),
            "{name}.authorize_url ({}) missing host {host:?}",
            p.authorize_url
        );
        assert!(
            p.token_url.contains(host),
            "{name}.token_url ({}) missing host {host:?}",
            p.token_url
        );
    }
}

/// The alias names `azure_ad` and `twitter` must resolve to the same preset
/// as their canonical counterparts (`microsoft`, `twitter_x`). The cloud
/// broker accepts either spelling and shouldn't return `invalid_provider`
/// for the alias.
#[test]
fn provider_aliases_resolve_to_same_endpoints() {
    let microsoft = preset("microsoft").expect("microsoft preset missing");
    let azure_ad = preset("azure_ad").expect("azure_ad alias missing");
    assert_eq!(microsoft.authorize_url, azure_ad.authorize_url);
    assert_eq!(microsoft.token_url, azure_ad.token_url);
    assert_eq!(microsoft.userinfo_url, azure_ad.userinfo_url);

    let twitter_x = preset("twitter_x").expect("twitter_x preset missing");
    let twitter = preset("twitter").expect("twitter alias missing");
    assert_eq!(twitter_x.authorize_url, twitter.authorize_url);
    assert_eq!(twitter_x.token_url, twitter.token_url);
    assert_eq!(twitter_x.userinfo_url, twitter.userinfo_url);
}

/// Unknown provider names must return None so the broker can surface
/// `invalid_provider` rather than silently building a malformed URL.
#[test]
fn unknown_provider_returns_none() {
    assert!(preset("definitely_not_a_real_provider").is_none());
    assert!(preset("").is_none());
    assert!(preset("saml_sso").is_none());
}
