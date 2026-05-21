//! Provider-registration unit tests for the cloud-broker OAuth catalog
//! (T-015 / T-033 / T-036). Lives as an external integration test (not
//! `#[cfg(test)] mod tests`) so the public surface that `basin-cloud`'s
//! `BasinAuthProvider` consumes — `preset()` and `PRESET_PROVIDER_NAMES`
//! — is exercised through the published API.
//!
//! Pure offline: no DB, no network. The flow-level smoke for these
//! providers lives in `tests/integration/tests/oauth_providers.rs`.

use basin_auth::oauth::{preset, PRESET_PROVIDER_NAMES};

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
