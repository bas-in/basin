//! Unit test: per-tenant URL allowlist gates outbound requests.
//!
//! The allowlist is the single most-load-bearing safety check in
//! `basin-net`. Default DENY-ALL prevents SSRF to AWS metadata
//! (`169.254.169.254`) or any other internal endpoint. We assert here that
//! a fresh tenant cannot reach a host until the host is explicitly opted
//! in, and that the opt-in is per-tenant (A's allowlist does not unlock B).

use basin_common::TenantId;
use basin_net::{AllowList, GuardConfig, HttpClient, RateLimit};

fn fresh_client() -> HttpClient {
    HttpClient::with_config(GuardConfig::default(), AllowList::new(), RateLimit::new())
}

#[tokio::test]
async fn unknown_host_is_denied() {
    let client = fresh_client();
    let tenant = TenantId::new();
    let err = client
        .http_get(&tenant, "http://203.0.113.5/admin")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("host not on allowlist"), "got {msg}");
}

#[tokio::test]
async fn allowlisted_host_passes_the_gate() {
    // We're not actually doing IO here — the test asserts the *gate*
    // accepts an allowlisted host. The check passes; the dispatch then
    // fails with a transport error against an unbound port, which is the
    // expected next step. We assert the failure surface is *not* the
    // allowlist one.
    let client = fresh_client();
    let tenant = TenantId::new();
    client.allow_host(&tenant, "127.0.0.1").await;
    let err = client
        .http_get(&tenant, "http://127.0.0.1:1/never-listens")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        !msg.contains("host not on allowlist"),
        "allowlist gate fired for an opted-in host: {msg}"
    );
}

#[tokio::test]
async fn allowlist_is_per_tenant() {
    let client = fresh_client();
    let alice = TenantId::new();
    let bob = TenantId::new();
    client.allow_host(&alice, "example.com").await;

    // Alice's host is unknown to Bob.
    let err = client
        .http_get(&bob, "http://example.com/")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("host not on allowlist"),
        "Bob got past the allowlist: {msg}"
    );
}

#[tokio::test]
async fn host_check_lowercases() {
    // RFC 3986 says host comparison is case-insensitive. The client's
    // lookup must lowercase before comparing.
    let allow = AllowList::new();
    let tenant = TenantId::new();
    allow.allow(&tenant, "EXAMPLE.com").await;
    allow.check(&tenant, "http://example.COM/path").await.unwrap();
    allow
        .check(&tenant, "http://EXAMPLE.com/path")
        .await
        .unwrap();
}

#[tokio::test]
async fn invalid_url_surfaces_invalid_url() {
    let client = fresh_client();
    let tenant = TenantId::new();
    let err = client.http_get(&tenant, "not-a-url").await.unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("invalid url"), "got {msg}");
}
