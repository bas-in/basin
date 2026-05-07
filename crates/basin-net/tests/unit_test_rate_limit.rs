//! Unit test: per-tenant rate limiter caps outbound requests.
//!
//! Default budget is 10 req/s sustained, burst 30. We submit 50 immediate
//! attempts against the same tenant and assert at most `BURST + small slack`
//! pass the gate — the rest must surface `RateLimited`.

use basin_common::TenantId;
use basin_net::{AllowList, GuardConfig, HttpClient, RateLimit};

#[tokio::test]
async fn burst_is_capped_to_governor_budget() {
    let client = HttpClient::with_config(GuardConfig::default(), AllowList::new(), RateLimit::new());
    let tenant = TenantId::new();
    client.allow_host(&tenant, "127.0.0.1").await;

    // The rate-limit gate is hit *before* the actual reqwest dispatch. By
    // pointing at a port nothing is listening on we still drive the gate
    // exactly once per call, and the post-gate dispatch surfaces a
    // transport error rather than a rate-limited error. Counting "got past
    // the rate gate" = (any error other than RateLimited).
    let mut allowed = 0usize;
    let mut rate_limited = 0usize;
    for _ in 0..50 {
        let r = client.http_get(&tenant, "http://127.0.0.1:1/").await;
        match r {
            Err(e) => {
                let msg = format!("{e}");
                if msg.contains("rate limited") {
                    rate_limited += 1;
                } else {
                    allowed += 1;
                }
            }
            Ok(_) => allowed += 1,
        }
    }
    // 30 burst tokens + at most a few sustained tokens during the test
    // wall-clock. Accept up to 35 to keep this test stable on slow CI.
    assert!(
        allowed <= 35,
        "rate limit failed to fire enough; allowed={allowed} rate_limited={rate_limited}"
    );
    assert!(
        rate_limited >= 15,
        "expected at least 15 RateLimited; got {rate_limited} (allowed={allowed})"
    );
}

#[tokio::test]
async fn rate_limit_is_per_tenant() {
    let client = HttpClient::with_config(GuardConfig::default(), AllowList::new(), RateLimit::new());
    let alice = TenantId::new();
    let bob = TenantId::new();
    client.allow_host(&alice, "127.0.0.1").await;
    client.allow_host(&bob, "127.0.0.1").await;

    // Burn Alice's burst budget.
    for _ in 0..40 {
        let _ = client.http_get(&alice, "http://127.0.0.1:1/").await;
    }
    // Bob's first call must not be rate-limited (his bucket is full).
    let r = client.http_get(&bob, "http://127.0.0.1:1/").await;
    let msg = match r {
        Ok(_) => "ok".into(),
        Err(e) => format!("{e}"),
    };
    assert!(
        !msg.contains("rate limited"),
        "Bob got rate-limited by Alice's traffic: {msg}"
    );
}
