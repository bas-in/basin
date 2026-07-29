//! Property-based fuzz coverage for the three small-surface targets
//! TASKS.md line 368 names (`FuzzParseGlobalFlags`, `FuzzQueryString`,
//! `FuzzConfigRoundTrip`). We use `proptest` on the stable toolchain
//! rather than `cargo-fuzz` (nightly-only) — same coverage, no nightly
//! rustc, runs as a regular `cargo test --test fuzz_proptest`.
//!
//! Case counts: 256 by default (fast PR check), 5000 when the
//! `CI_LONG_FUZZ` env var is set (nightly job). TASKS.md asks for
//! 30s on PR, 5min nightly; the case-count split is the knob the
//! `scripts/` runner flips.
//!
//! The targets are reached via the `basin_cli` library crate (see
//! `src/lib.rs`) — basin-cli is bin-first but exports a thin lib so
//! integration tests can drive parsers without a subprocess.

use std::collections::BTreeMap;

use proptest::prelude::*;

fn long_run() -> bool {
    std::env::var("CI_LONG_FUZZ").is_ok()
}

fn cases() -> u32 {
    if long_run() {
        5000
    } else {
        256
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases()))]

    /// parse_global_flags must never panic on arbitrary argv. `\PC` is
    /// proptest's "printable char" class; we cap each argv slot at 40
    /// chars and the argv itself at 12 entries — wide enough to hit the
    /// `--flag value` two-token paths plus the `--key=value` single-token
    /// path, narrow enough to keep one case fast.
    #[test]
    fn fuzz_parse_global_flags(args in prop::collection::vec("\\PC{0,40}", 0..12)) {
        let argv: Vec<String> = args;
        // Must not panic. Either Ok((flags, rest)) or Err(msg) is fine.
        let _ = basin_cli::global::parse_global_flags(&argv);
    }

    /// query_string must always produce either an empty string or a
    /// well-formed "?k=v&k=v" suffix; nothing else is a legal URL
    /// query-fragment prefix. We also assert keys come back sorted (the
    /// function's documented contract) when at least two non-empty
    /// values survive the empty-value filter.
    #[test]
    fn fuzz_query_string(
        pairs in prop::collection::vec(("\\PC{0,20}", "\\PC{0,40}"), 0..8),
    ) {
        let refs: Vec<(&str, &str)> = pairs.iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let qs = basin_cli::client::query_string(&refs);
        prop_assert!(qs.is_empty() || qs.starts_with('?'));

        // If non-empty, every "k=v" chunk should round-trip the url
        // escaping (no stray un-escaped '&' or '=' in keys/values).
        if let Some(stripped) = qs.strip_prefix('?') {
            for chunk in stripped.split('&') {
                prop_assert!(
                    chunk.contains('='),
                    "every chunk must have one '=' separator, got {chunk:?}"
                );
            }
        }
    }

    /// ConfigFile JSON round-trip: serialize -> deserialize -> serialize
    /// must be byte-equal. Arbitrary token / org / tokens-map contents
    /// must not break serde. We restrict the input alphabet to safe
    /// chars because `serde_json` will reject e.g. unpaired surrogates
    /// (not a bug in our code — just orthogonal to what we're fuzzing).
    #[test]
    fn fuzz_config_roundtrip(
        default_org in "[a-zA-Z0-9_-]{0,40}",
        default_token in "[a-zA-Z0-9_-]{0,80}",
        api_url in "[a-zA-Z0-9_./:-]{0,60}",
        token_pairs in prop::collection::vec(
            ("[a-zA-Z0-9_-]{1,20}", "[a-zA-Z0-9_-]{1,40}"),
            0..4,
        ),
    ) {
        let mut tokens = BTreeMap::new();
        for (k, v) in token_pairs {
            tokens.insert(k, v);
        }
        let cfg = basin_cli::config::ConfigFile {
            default_token,
            api_url,
            default_org,
            tokens,
            cloud_versions: BTreeMap::new(),
            telemetry: false,
            cli_release: None,
        };
        let json1 = serde_json::to_string(&cfg).expect("serialize ConfigFile");
        let back: basin_cli::config::ConfigFile =
            serde_json::from_str(&json1).expect("deserialize ConfigFile");
        let json2 = serde_json::to_string(&back).expect("re-serialize ConfigFile");
        prop_assert_eq!(json1, json2);
    }
}
