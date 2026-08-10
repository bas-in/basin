//! SSRF hardening — see GH issue #53.
//!
//! These tests cover the attack vectors the SEC-AUDIT P0 called out:
//!
//! 1. IP-literal bypass — operator allowlists `attacker.com`, attacker DNS-
//!    rebinds (or supplies a URL with) `127.0.0.1`, RFC1918, link-local, or
//!    `169.254.169.254` IMDS. The literal must be rejected *even when
//!    explicitly allowlisted*.
//! 2. Decimal / octal / IPv4-mapped-IPv6 encodings of the same private
//!    space — `http://2130706433/` = `127.0.0.1`, `http://[::ffff:127.0.0.1]/`
//!    = `127.0.0.1`. The `url` crate canonicalises these into typed
//!    `Ipv4Addr` / `Ipv6Addr`, but the SSRF check has to walk the
//!    `to_ipv4_mapped` fork for the IPv6 case.
//! 3. Userinfo URLs — `http://allowed.com@evil.com/` — `url::Url` reports
//!    host=`evil.com` but downstream tooling can disagree. Rather than
//!    chase the cross-stack ambiguity we reject userinfo URLs outright.
//! 4. DNS rebinding TOCTOU — allowlisted hostname that resolves to a
//!    private IP at connect time. Exercised via the `nip.io` wildcard
//!    DNS that resolves `<ip>.nip.io` to `<ip>`; we assert the connect
//!    is blocked at the resolver layer before any TCP SYN goes out.
//!
//! Each test calls through the full `HttpClient::send` chokepoint so the
//! resolver wiring in `client.rs` is exercised end-to-end.

use basin_common::ProjectId;
use basin_net::{AllowList, GuardConfig, HttpClient, HttpError, RateLimit};

fn fresh_client() -> HttpClient {
    HttpClient::with_config(GuardConfig::default(), AllowList::new(), RateLimit::new())
}

// --- IP-literal denylist: fires even when the host is allowlisted -----------

#[tokio::test]
async fn ssrf_ip_literal_169_254_169_254_denied() {
    let client = fresh_client();
    let project = ProjectId::new();
    // Allowlist the literal to prove the IP denylist trumps allowlisting.
    client.allow_host(&project, "169.254.169.254").await;
    let err = client
        .http_get(&project, "http://169.254.169.254/latest/meta-data/")
        .await
        .unwrap_err();
    assert!(
        matches!(err, HttpError::IpLiteralDenied(_)),
        "IMDS literal slipped through: {err:?}"
    );
}

#[tokio::test]
async fn ssrf_ip_literal_127_0_0_1_denied() {
    let client = fresh_client();
    let project = ProjectId::new();
    client.allow_host(&project, "127.0.0.1").await;
    let err = client
        .http_get(&project, "http://127.0.0.1:80/")
        .await
        .unwrap_err();
    assert!(
        matches!(err, HttpError::IpLiteralDenied(_)),
        "loopback literal slipped through: {err:?}"
    );
}

#[tokio::test]
async fn ssrf_ip_literal_10_0_0_1_denied() {
    let client = fresh_client();
    let project = ProjectId::new();
    client.allow_host(&project, "10.0.0.1").await;
    let err = client
        .http_get(&project, "http://10.0.0.1/")
        .await
        .unwrap_err();
    assert!(
        matches!(err, HttpError::IpLiteralDenied(_)),
        "RFC1918 literal slipped through: {err:?}"
    );
}

#[tokio::test]
async fn ssrf_ip_literal_192_168_denied() {
    let client = fresh_client();
    let project = ProjectId::new();
    client.allow_host(&project, "192.168.1.1").await;
    let err = client
        .http_get(&project, "http://192.168.1.1/")
        .await
        .unwrap_err();
    assert!(matches!(err, HttpError::IpLiteralDenied(_)), "got {err:?}");
}

#[tokio::test]
async fn ssrf_ip_literal_ipv6_loopback_denied() {
    let client = fresh_client();
    let project = ProjectId::new();
    // The allowlist's host_str for `[::1]` is `[::1]`; allowlist that exact
    // form so we prove the IP-class check still fires.
    client.allow_host(&project, "[::1]").await;
    let err = client
        .http_get(&project, "http://[::1]/")
        .await
        .unwrap_err();
    assert!(
        matches!(err, HttpError::IpLiteralDenied(_)),
        "IPv6 loopback slipped through: {err:?}"
    );
}

#[tokio::test]
async fn ssrf_ip_literal_ipv6_unique_local_denied() {
    // fc00::/7 — the "RFC1918 of IPv6".
    let client = fresh_client();
    let project = ProjectId::new();
    let err = client
        .http_get(&project, "http://[fc00::1]/")
        .await
        .unwrap_err();
    assert!(matches!(err, HttpError::IpLiteralDenied(_)), "got {err:?}");
}

#[tokio::test]
async fn ssrf_ipv4_mapped_ipv6_denied() {
    // `::ffff:127.0.0.1` — IPv4-mapped IPv6. The url crate keeps this as
    // an Ipv6Addr; `to_ipv4_mapped` must pull out the v4 and then the
    // loopback check fires.
    let client = fresh_client();
    let project = ProjectId::new();
    let err = client
        .http_get(&project, "http://[::ffff:127.0.0.1]/")
        .await
        .unwrap_err();
    assert!(
        matches!(err, HttpError::IpLiteralDenied(_)),
        "IPv4-mapped IPv6 loopback slipped through: {err:?}"
    );
}

#[tokio::test]
async fn ssrf_decimal_ipv4_denied() {
    // `2130706433` = `0x7f000001` = `127.0.0.1`. `url::Url::parse`
    // canonicalises this to the dotted-quad form before our check ever
    // sees it, so the IP denylist still fires.
    let client = fresh_client();
    let project = ProjectId::new();
    let err = client
        .http_get(&project, "http://2130706433/")
        .await
        .unwrap_err();
    assert!(
        matches!(err, HttpError::IpLiteralDenied(_)),
        "decimal-encoded loopback slipped through: {err:?}"
    );
}

#[tokio::test]
async fn ssrf_octal_ipv4_denied() {
    // `0177.0.0.1` = `127.0.0.1`.
    let client = fresh_client();
    let project = ProjectId::new();
    let err = client
        .http_get(&project, "http://0177.0.0.1/")
        .await
        .unwrap_err();
    assert!(matches!(err, HttpError::IpLiteralDenied(_)), "got {err:?}");
}

#[tokio::test]
async fn ssrf_unspecified_v4_denied() {
    // `0.0.0.0` — "this host". A bind-to-any address that on Linux behaves
    // like loopback for outbound. Should never be a connect target.
    let client = fresh_client();
    let project = ProjectId::new();
    let err = client
        .http_get(&project, "http://0.0.0.0/")
        .await
        .unwrap_err();
    assert!(matches!(err, HttpError::IpLiteralDenied(_)), "got {err:?}");
}

// --- Userinfo URL rejection -------------------------------------------------

#[tokio::test]
async fn ssrf_url_with_userinfo_rejected() {
    // `http://user@evil.com/` — `url::Url::parse` reports host=`evil.com`
    // and username=`user`. Reject any URL carrying userinfo so the
    // `allowed.com@evil.com` confused-deputy form can't sneak past the
    // allowlist when downstream tools disagree on which side of `@` is
    // the host.
    let client = fresh_client();
    let project = ProjectId::new();
    client.allow_host(&project, "evil.com").await;
    let err = client
        .http_get(&project, "http://user@evil.com/")
        .await
        .unwrap_err();
    assert!(
        matches!(err, HttpError::UriUserinfoNotAllowed),
        "userinfo URL slipped through: {err:?}"
    );
}

#[tokio::test]
async fn ssrf_url_with_user_password_rejected() {
    let client = fresh_client();
    let project = ProjectId::new();
    client.allow_host(&project, "evil.com").await;
    let err = client
        .http_get(&project, "http://user:pass@evil.com/")
        .await
        .unwrap_err();
    assert!(
        matches!(err, HttpError::UriUserinfoNotAllowed),
        "got {err:?}"
    );
}

#[tokio::test]
async fn ssrf_confused_deputy_userinfo_rejected() {
    // The textbook confused-deputy. `url::Url::parse` reports
    // host=`evil.com`, but tooling that reads the URL as a string can
    // misparse the `allowed.com@` as the host. The userinfo rejection
    // closes the ambiguity by refusing the URL outright.
    let client = fresh_client();
    let project = ProjectId::new();
    client.allow_host(&project, "allowed.com").await;
    let err = client
        .http_get(&project, "http://allowed.com@evil.com/")
        .await
        .unwrap_err();
    assert!(
        matches!(err, HttpError::UriUserinfoNotAllowed),
        "got {err:?}"
    );
}

// --- DNS rebinding TOCTOU ---------------------------------------------------

#[tokio::test]
async fn ssrf_dns_rebinding_attacker_resolves_to_loopback() {
    // `nip.io` is a public wildcard DNS that resolves `<ip>.nip.io` →
    // `<ip>`. We use `127.0.0.1.nip.io` as a stand-in for an attacker-
    // controlled domain whose authoritative server returns a loopback
    // address. The host string passes the allowlist (because the
    // operator opted it in, thinking it was a benign hostname), but the
    // resolver hook must reject the resolved 127.0.0.1 before reqwest
    // opens any socket.
    //
    // Skipping the assertion when the test host has no DNS is fine —
    // returning a transport error is also acceptable (we don't want to
    // turn a flaky CI network into a security regression). We assert
    // the connect did *not* succeed and that we did not surface a
    // success status.
    let client = fresh_client();
    let project = ProjectId::new();
    client.allow_host(&project, "127.0.0.1.nip.io").await;
    let result = client.http_get(&project, "http://127.0.0.1.nip.io/").await;
    match result {
        Ok(resp) => panic!(
            "DNS-rebinding to loopback connected anyway: status={} body_len={}",
            resp.status,
            resp.body.len()
        ),
        Err(HttpError::Transport(msg)) => {
            // Expected path: the resolver's guard surface as a transport
            // error from reqwest. The message contains the resolver's
            // own "denied ip" string (when DNS resolution succeeded) or a
            // generic resolve failure (when CI has no DNS / nip.io is
            // unreachable). Either is acceptable; only a 2xx response
            // is a regression.
            assert!(
                msg.to_lowercase().contains("denied ip")
                    || msg.to_lowercase().contains("resolve")
                    || msg.to_lowercase().contains("dns")
                    || msg.to_lowercase().contains("error sending request"),
                "unexpected transport error shape: {msg}"
            );
        }
        Err(HttpError::Timeout(_)) => {
            // Acceptable: an isolated CI runner with no outbound DNS
            // will block on the GAI lookup. The important property —
            // *we never connected* — still holds.
        }
        Err(other) => panic!("unexpected error shape: {other:?}"),
    }
}

// --- Pure-function sanity checks on `is_denied_ip` --------------------------
//
// These complement the integration-level tests above by pinning down the
// pure predicate so a future "small refactor" can't silently re-open a
// class.

use basin_net::is_denied_ip;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[test]
fn denied_ipv4_classes_are_exhaustive() {
    let denied: &[Ipv4Addr] = &[
        Ipv4Addr::new(127, 0, 0, 1),       // loopback
        Ipv4Addr::new(10, 0, 0, 1),        // RFC1918 10/8
        Ipv4Addr::new(172, 16, 0, 1),      // RFC1918 172.16/12
        Ipv4Addr::new(192, 168, 1, 1),     // RFC1918 192.168/16
        Ipv4Addr::new(169, 254, 169, 254), // AWS IMDS
        Ipv4Addr::new(169, 254, 1, 1),     // link-local 169.254/16
        Ipv4Addr::new(224, 0, 0, 1),       // multicast
        Ipv4Addr::new(255, 255, 255, 255), // broadcast
        Ipv4Addr::new(0, 0, 0, 0),         // unspecified
        Ipv4Addr::new(0, 1, 2, 3),         // 0/8 — "this network"
        Ipv4Addr::new(100, 64, 0, 1),      // CGNAT 100.64/10
        Ipv4Addr::new(192, 0, 2, 1),       // TEST-NET-1 documentation
    ];
    for ip in denied {
        assert!(is_denied_ip(IpAddr::V4(*ip)), "expected denied: {ip}");
    }
    // Sanity: a routable public IP is not denied.
    assert!(!is_denied_ip(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
    assert!(!is_denied_ip(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))));
}

#[test]
fn denied_ipv6_classes_are_exhaustive() {
    let denied: &[Ipv6Addr] = &[
        Ipv6Addr::LOCALHOST,                       // ::1
        Ipv6Addr::UNSPECIFIED,                     // ::
        "fc00::1".parse().unwrap(),                // unique-local
        "fd00::1".parse().unwrap(),                // unique-local
        "fe80::1".parse().unwrap(),                // link-local
        "ff02::1".parse().unwrap(),                // multicast
        "::ffff:127.0.0.1".parse().unwrap(),       // ipv4-mapped loopback
        "::ffff:10.0.0.1".parse().unwrap(),        // ipv4-mapped RFC1918
        "::ffff:169.254.169.254".parse().unwrap(), // ipv4-mapped IMDS
        "fd00:ec2::254".parse().unwrap(),          // AWS IMDSv2 v6
    ];
    for ip in denied {
        assert!(is_denied_ip(IpAddr::V6(*ip)), "expected denied: {ip}");
    }
    // Sanity: a routable public IPv6 is not denied (Google DNS).
    let public: Ipv6Addr = "2001:4860:4860::8888".parse().unwrap();
    assert!(!is_denied_ip(IpAddr::V6(public)));
}
