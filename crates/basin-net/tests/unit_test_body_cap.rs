//! Unit test: outgoing body cap rejects oversized POST bodies.

use basin_common::TenantId;
use basin_net::{AllowList, GuardConfig, HttpClient, RateLimit};

#[tokio::test]
async fn oversized_post_body_rejected() {
    // Tighten the cap so we don't have to allocate 10 MiB in the test.
    let cfg = GuardConfig {
        max_body_bytes: 1024,
        timeout: std::time::Duration::from_secs(5),
    };
    let client = HttpClient::with_config(cfg, AllowList::new(), RateLimit::new());
    let tenant = TenantId::new();
    client.allow_host(&tenant, "127.0.0.1").await;

    let body = vec![b'x'; 4096];
    let err = client
        .http_post(&tenant, "http://127.0.0.1:1/", body, "application/octet-stream")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("body exceeds cap"), "got {msg}");
}

#[tokio::test]
async fn at_cap_post_body_passes_the_gate() {
    // Body exactly at the cap is allowed; the dispatch then fails with a
    // transport error, which is fine. We only assert the body-cap path
    // didn't fire.
    let cfg = GuardConfig {
        max_body_bytes: 64,
        timeout: std::time::Duration::from_secs(5),
    };
    let client = HttpClient::with_config(cfg, AllowList::new(), RateLimit::new());
    let tenant = TenantId::new();
    client.allow_host(&tenant, "127.0.0.1").await;

    let body = vec![b'y'; 64];
    let err = client
        .http_post(&tenant, "http://127.0.0.1:1/", body, "application/octet-stream")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(!msg.contains("body exceeds cap"), "tripped at cap: {msg}");
}

#[test]
fn default_cap_matches_documented_value() {
    // Doc-comment promise: 10 MiB. The viability bar that says "POST with
    // body > 10 MiB rejected" depends on this.
    assert_eq!(GuardConfig::DEFAULT_MAX_BODY_BYTES, 10 * 1024 * 1024);
}
