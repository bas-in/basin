//! Unit tests for the standalone allowlist / rate-limit primitives.
//!
//! These complement the higher-level [`HttpClient`] tests by exercising the
//! gate types in isolation. Useful when reviewing the safety story.

use basin_common::ProjectId;
use basin_net::{AllowList, GuardConfig, HttpError, RateLimit};

#[tokio::test]
async fn allowlist_check_returns_host_denied_variant() {
    let allow = AllowList::new();
    let project = ProjectId::new();
    let err = allow
        .check(&project, "https://attacker.example.com/")
        .await
        .unwrap_err();
    assert!(matches!(err, HttpError::HostDenied { .. }), "got {err:?}");
}

#[tokio::test]
async fn allowlist_list_is_sorted() {
    let allow = AllowList::new();
    let project = ProjectId::new();
    allow.allow(&project, "zeta.example").await;
    allow.allow(&project, "alpha.example").await;
    allow.allow(&project, "mu.example").await;
    let v = allow.list(&project).await;
    assert_eq!(v, vec!["alpha.example", "mu.example", "zeta.example"]);
}

#[tokio::test]
async fn allowlist_deny_removes_host() {
    let allow = AllowList::new();
    let project = ProjectId::new();
    allow.allow(&project, "example.com").await;
    assert!(allow.deny(&project, "example.com").await);
    let err = allow
        .check(&project, "https://example.com/")
        .await
        .unwrap_err();
    assert!(matches!(err, HttpError::HostDenied { .. }), "got {err:?}");
}

#[test]
fn rate_limit_constants_match_doc() {
    // Our crate-level doc and the spec both say "10 req/s sustained, burst 30".
    assert_eq!(RateLimit::SUSTAINED_PER_SEC, 10);
    assert_eq!(RateLimit::BURST, 30);
}

#[test]
fn guard_config_default_reads_env() {
    // Without env vars set, the constants must hold. We deliberately don't
    // mutate the process env — that races with parallel test threads.
    let cfg = GuardConfig::from_env();
    // The default body cap is 10 MiB; allow override but assert sensible
    // bounds either way.
    assert!(cfg.max_body_bytes >= 1024);
    assert!(cfg.timeout.as_secs() >= 1);
}
