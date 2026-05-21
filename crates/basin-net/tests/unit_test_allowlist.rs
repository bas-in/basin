//! Unit test: per-project URL allowlist gates outbound requests.
//!
//! The allowlist is the single most-load-bearing safety check in
//! `basin-net`. Default DENY-ALL prevents SSRF to AWS metadata
//! (`169.254.169.254`) or any other internal endpoint. We assert here that
//! a fresh project cannot reach a host until the host is explicitly opted
//! in, and that the opt-in is per-project (A's allowlist does not unlock B).

use basin_common::ProjectId;
use basin_net::{AllowList, GuardConfig, HttpClient, RateLimit};

fn fresh_client() -> HttpClient {
    HttpClient::with_config(GuardConfig::default(), AllowList::new(), RateLimit::new())
}

#[tokio::test]
async fn unknown_host_is_denied() {
    // Use a hostname rather than an IP literal here: since the SSRF
    // hardening (#53) any non-globally-routable literal would short-
    // circuit on the IP-class denylist before this assertion ever ran.
    // The point of *this* test is to verify the *allowlist* gate, so
    // give it a domain whose only possible failure mode is allowlist.
    let client = fresh_client();
    let project = ProjectId::new();
    let err = client
        .http_get(&project, "http://attacker.example/admin")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("host not on allowlist"), "got {msg}");
}

#[tokio::test]
async fn allowlisted_host_passes_the_gate() {
    // We're not actually doing IO here — the test asserts the *gate*
    // accepts an allowlisted host. The check passes; the dispatch then
    // fails with a transport / DNS error against an unbound port, which
    // is the expected next step. We assert the failure surface is *not*
    // the allowlist one.
    //
    // We deliberately use a routable-looking public hostname here rather
    // than `127.0.0.1`. Since the SSRF hardening (#53), the IP-class
    // denylist refuses loopback even when allowlisted — that property
    // is itself tested in `unit_test_ssrf.rs::ssrf_ip_literal_*`.
    let client = fresh_client();
    let project = ProjectId::new();
    client.allow_host(&project, "example.invalid").await;
    let err = client
        .http_get(&project, "http://example.invalid:1/never-listens")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        !msg.contains("host not on allowlist"),
        "allowlist gate fired for an opted-in host: {msg}"
    );
    assert!(
        !msg.contains("ip literal in denied class"),
        "IP-class denylist fired for a hostname: {msg}"
    );
}

#[tokio::test]
async fn allowlist_is_per_project() {
    let client = fresh_client();
    let alice = ProjectId::new();
    let bob = ProjectId::new();
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
    let project = ProjectId::new();
    allow.allow(&project, "EXAMPLE.com").await;
    allow
        .check(&project, "http://example.COM/path")
        .await
        .unwrap();
    allow
        .check(&project, "http://EXAMPLE.com/path")
        .await
        .unwrap();
}

#[tokio::test]
async fn invalid_url_surfaces_invalid_url() {
    let client = fresh_client();
    let project = ProjectId::new();
    let err = client.http_get(&project, "not-a-url").await.unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("invalid url"), "got {msg}");
}

#[tokio::test]
async fn allowlisting_a_private_ip_literal_does_not_let_it_through() {
    // The point of the SSRF hardening (issue #53). Operator allowlists a
    // private IP — perhaps because they have a legitimate on-cluster
    // peer with the same literal — and the IP-class denylist still
    // refuses. The allowlist is a positive opt-in for *names*; the IP
    // denylist is a negative absolute on *addresses*.
    let client = fresh_client();
    let project = ProjectId::new();
    client.allow_host(&project, "127.0.0.1").await;
    let err = client
        .http_get(&project, "http://127.0.0.1:1/whatever")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("ip literal in denied class"),
        "expected IP-class rejection, got {msg}"
    );
}
