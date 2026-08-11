//! Per-project safety controls. Three independent gates wrap every outbound
//! request:
//!
//! 1. [`AllowList`] — host-based opt-in. Default DENY-ALL. Required to
//!    prevent SSRF (AWS metadata service, internal RFC1918 endpoints, the
//!    customer's own ENGINE process talking to itself).
//! 2. [`RateLimit`] — per-project token bucket via the `governor` crate. The
//!    burst size is intentionally larger than the sustained rate so a noisy
//!    project who issues a flurry to one allowlisted target doesn't starve a
//!    polite project on the same shard.
//! 3. [`GuardConfig`] — body-cap and timeout knobs read once at
//!    [`HttpClient::new`] time.
//!
//! The choice of which gate fires is intentional: allowlist comes *first*
//! so a request to a blocked host never even consumes a rate-limit token
//! (otherwise an attacker could exhaust the bucket cheaply and DoS legitimate
//! traffic).

use std::collections::{HashMap, HashSet};
use std::env;
use std::future::ready;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use basin_common::ProjectId;
use governor::{clock::DefaultClock, state::keyed::DefaultKeyedStateStore, Quota, RateLimiter};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use tokio::sync::RwLock;

use crate::types::HttpError;

/// AWS / EC2 / GCP / Azure instance-metadata service IPv4. Always denied
/// regardless of the more general link-local rule, both so the test name
/// reads honestly and so a future tweak to the link-local check can't
/// silently re-open the most well-known SSRF target on the planet.
pub const IMDS_V4: Ipv4Addr = Ipv4Addr::new(169, 254, 169, 254);

/// AWS IMDSv2 IPv6 endpoint (`fd00:ec2::254`). Caught by the unique-local
/// rule already, but called out explicitly for the same reason as
/// [`IMDS_V4`].
pub const IMDS_V6: Ipv6Addr = Ipv6Addr::new(0xfd00, 0x00ec, 0x0002, 0, 0, 0, 0, 0x0254);

/// SSRF IP-class denylist. Returns `true` when `ip` is in a class we never
/// want an outbound request to reach:
///
/// * IPv4: loopback (`127.0.0.0/8`), private RFC1918 (`10/8`, `172.16/12`,
///   `192.168/16`), link-local (`169.254/16`, incl. IMDS), multicast,
///   broadcast, unspecified (`0.0.0.0`), and the documentation ranges.
/// * IPv6: loopback (`::1`), unique-local (`fc00::/7`), unicast link-local
///   (`fe80::/10`), multicast, unspecified (`::`), IPv4-mapped that maps
///   to a denied IPv4, plus explicit `fd00:ec2::254` (IMDSv2).
///
/// Pure function — fires both at parse time on URL host-literals and at
/// DNS-resolve time inside [`RebindingGuardedResolver`].
pub fn is_denied_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_denied_ipv4(v4),
        IpAddr::V6(v6) => is_denied_ipv6(v6),
    }
}

fn is_denied_ipv4(v4: Ipv4Addr) -> bool {
    if v4 == IMDS_V4 {
        return true;
    }
    // RFC 6890 sweep: anything not a globally-routable unicast is denied.
    v4.is_loopback()
        || v4.is_private()
        || v4.is_link_local()
        || v4.is_multicast()
        || v4.is_broadcast()
        || v4.is_unspecified()
        || v4.is_documentation()
        // 0.0.0.0/8 — `this network`. `is_unspecified` only catches `0.0.0.0`.
        || v4.octets()[0] == 0
        // Carrier-grade NAT 100.64.0.0/10 — RFC 6598. Not strictly private
        // but should not be reachable from a Postgres extension.
        || (v4.octets()[0] == 100 && (v4.octets()[1] & 0xc0) == 0x40)
}

fn is_denied_ipv6(v6: Ipv6Addr) -> bool {
    if v6 == IMDS_V6 {
        return true;
    }
    // IPv4-mapped IPv6 — `::ffff:0:0/96`. An attacker can dress 127.0.0.1
    // up as `::ffff:127.0.0.1`; pull the v4 out and re-check.
    if let Some(v4) = v6.to_ipv4_mapped() {
        return is_denied_ipv4(v4);
    }
    v6.is_loopback()
        || v6.is_unique_local()
        || v6.is_unicast_link_local()
        || v6.is_multicast()
        || v6.is_unspecified()
}

/// Vet a URL string for SSRF-class violations *before* the per-project
/// allowlist check. This fires regardless of whether the host is on the
/// allowlist, because an operator who allowlists `attacker.com` should
/// still be safe when the attacker controls DNS and rebinds the name to
/// `127.0.0.1` or `169.254.169.254`.
///
/// Three things happen here:
///
/// 1. URL must parse and carry a host.
/// 2. Userinfo (`http://user[:pass]@host/`) is rejected — see
///    [`HttpError::UriUserinfoNotAllowed`] for the rationale.
/// 3. If the host is an IP literal (any form `url::Host` recognises,
///    incl. decimal/octal IPv4 and IPv4-mapped IPv6), check it against
///    [`is_denied_ip`].
///
/// DNS-time rebinding is caught separately by
/// [`RebindingGuardedResolver`] inside the reqwest client; the two checks
/// together close the TOCTOU window between URL parse and TCP connect.
pub fn check_url_safety(url: &str) -> Result<url::Url, HttpError> {
    check_url_safety_with(url, false)
}

/// Variant of [`check_url_safety`] that honours the
/// [`GuardConfig::allow_loopback_for_tests`] escape hatch. When
/// `allow_loopback` is `true`, the IP-class denylist is skipped — the
/// userinfo and URL-parse gates still fire. Production callers always
/// invoke [`check_url_safety`]; only `HttpClient::send` opens the
/// escape hatch when its `GuardConfig` says so.
pub fn check_url_safety_with(url: &str, allow_loopback: bool) -> Result<url::Url, HttpError> {
    let parsed = url::Url::parse(url).map_err(|e| HttpError::InvalidUrl(format!("{url}: {e}")))?;
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(HttpError::UriUserinfoNotAllowed);
    }
    let host = parsed
        .host()
        .ok_or_else(|| HttpError::InvalidUrl(format!("no host in url: {url}")))?;
    if !allow_loopback {
        match host {
            url::Host::Ipv4(v4) => {
                let ip = IpAddr::V4(v4);
                if is_denied_ip(ip) {
                    return Err(HttpError::IpLiteralDenied(ip));
                }
            }
            url::Host::Ipv6(v6) => {
                let ip = IpAddr::V6(v6);
                if is_denied_ip(ip) {
                    return Err(HttpError::IpLiteralDenied(ip));
                }
            }
            url::Host::Domain(_) => {
                // Hostnames go through `RebindingGuardedResolver` at
                // connect time; we can't validate the IP from the URL
                // string alone.
            }
        }
    }
    Ok(parsed)
}

/// `reqwest::dns::Resolve` wrapper that filters out IPs in the SSRF
/// denylist *before* reqwest's hyper connector hands them to the TCP
/// stack. Two reasons this lives at the resolver layer rather than
/// after-the-fact:
///
/// 1. **DNS rebinding (TOCTOU)**. The allowlist check sees a host string;
///    by the time reqwest resolves it, an attacker-controlled authoritative
///    server can hand back `127.0.0.1`. The resolver hook is the only
///    place where the *actual* connect IP is observable before the TCP
///    SYN goes out.
/// 2. **No double-resolution**. We use the same `std::net::ToSocketAddrs`
///    path reqwest's default `GaiResolver` does, so resolver semantics
///    (TTL, IPv4/IPv6 ordering, hosts file) don't drift between the guard
///    and the real fetch.
///
/// Implementation note: `ToSocketAddrs::to_socket_addrs()` is blocking;
/// we run it on the tokio blocking pool via `spawn_blocking` so it
/// doesn't stall the runtime.
#[derive(Default)]
pub struct RebindingGuardedResolver {
    /// When `true`, skip the IP-class denylist (matches
    /// [`GuardConfig::allow_loopback_for_tests`]). Production constructions
    /// always pass `false`. Even when `true` this resolver still uses the
    /// same blocking-`getaddrinfo` path as the default, so resolver
    /// semantics are unchanged.
    allow_loopback: bool,
}

impl RebindingGuardedResolver {
    pub fn new() -> Self {
        Self {
            allow_loopback: false,
        }
    }

    /// Test-only constructor. Mirrors
    /// [`GuardConfig::with_loopback_allowed_for_tests`] — see that doc.
    pub fn with_loopback_allowed_for_tests() -> Self {
        Self {
            allow_loopback: true,
        }
    }
}

impl std::fmt::Debug for RebindingGuardedResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebindingGuardedResolver")
            .field("allow_loopback", &self.allow_loopback)
            .finish()
    }
}

impl Resolve for RebindingGuardedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        let allow_loopback = self.allow_loopback;
        Box::pin(async move {
            // `ToSocketAddrs` needs a port; reqwest will overwrite it later
            // (see `DynResolver::http_resolve`). Port 0 is the convention.
            let lookup_host = host.clone();
            let join = tokio::task::spawn_blocking(move || {
                (lookup_host.as_str(), 0u16)
                    .to_socket_addrs()
                    .map(|it| it.collect::<Vec<_>>())
            })
            .await;
            let addrs: Vec<SocketAddr> = match join {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => {
                    let err: Box<dyn std::error::Error + Send + Sync> =
                        format!("resolve {host}: {e}").into();
                    return Err(err);
                }
                Err(e) => {
                    let err: Box<dyn std::error::Error + Send + Sync> =
                        format!("resolver join: {e}").into();
                    return Err(err);
                }
            };
            // Strict: if *any* resolved address is in the denylist, fail
            // the whole resolution. Returning a filtered subset would let
            // an attacker still win by ordering: if the first record is
            // private and the second is public, reqwest may take the
            // first. Fail closed.
            if !allow_loopback {
                for addr in &addrs {
                    if is_denied_ip(addr.ip()) {
                        let err: Box<dyn std::error::Error + Send + Sync> = format!(
                            "dns rebinding guard: {} resolved to denied ip {}",
                            host,
                            addr.ip()
                        )
                        .into();
                        return Err(err);
                    }
                }
            }
            let iter: Addrs = Box::new(addrs.into_iter());
            ready(Ok::<_, Box<dyn std::error::Error + Send + Sync>>(iter)).await
        })
    }
}

/// Static knobs read from the environment. Cloned into [`HttpClient`](crate::HttpClient) at
/// construction time so a mid-process env mutation can't change behaviour
/// from underneath an in-flight request.
#[derive(Clone, Debug)]
pub struct GuardConfig {
    /// Maximum total bytes for either request body or response body. Default
    /// 10 MiB. Override via `BASIN_NET_MAX_BODY_BYTES` (decimal integer).
    pub max_body_bytes: usize,
    /// Maximum wall-clock for one outbound request including TLS/connect.
    /// Default 30s. Override via `BASIN_NET_TIMEOUT_SECS`.
    pub timeout: Duration,
    /// Escape hatch for the SSRF IP-class denylist. **Production callers
    /// must leave this `false`.** Set to `true` only from test fixtures
    /// that spawn a local mock HTTP server on `127.0.0.1` and need to
    /// exercise unrelated gates (body cap, rate limit, timeout). When
    /// `true`, [`check_url_safety`] and [`RebindingGuardedResolver`] skip
    /// the loopback / RFC1918 / link-local checks. Userinfo rejection is
    /// still enforced.
    ///
    /// There is no env-var that flips this — the field is build-time
    /// only, set explicitly by tests via
    /// [`GuardConfig::with_loopback_allowed_for_tests`]. The very loud
    /// name is the point.
    pub allow_loopback_for_tests: bool,
}

impl GuardConfig {
    /// 10 MiB. The Postgres `http` extension defaults to 5 MiB; we err on
    /// the side of the more permissive 10 MiB the Supabase fork ships.
    pub const DEFAULT_MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
    /// Same default Supabase ships for `pg_net.timeout_milliseconds`.
    pub const DEFAULT_TIMEOUT_SECS: u64 = 30;

    pub fn from_env() -> Self {
        let max_body_bytes = env::var("BASIN_NET_MAX_BODY_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(Self::DEFAULT_MAX_BODY_BYTES);
        let timeout_secs = env::var("BASIN_NET_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(Self::DEFAULT_TIMEOUT_SECS);
        Self {
            max_body_bytes,
            timeout: Duration::from_secs(timeout_secs),
            allow_loopback_for_tests: false,
        }
    }

    /// Build-time opt-out of the SSRF IP-class denylist. Use **only** from
    /// test fixtures that spawn a localhost mock server. See the field
    /// doc on [`Self::allow_loopback_for_tests`] for the contract.
    pub fn with_loopback_allowed_for_tests(mut self) -> Self {
        self.allow_loopback_for_tests = true;
        self
    }
}

impl Default for GuardConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

/// Per-project URL allowlist. Lookup is by host (the `Host` part of the URL —
/// no path matching). Stored in-memory; persistence to a `_net_allowed_hosts`
/// table is the natural follow-up once the engine's UDF arg-types catch up.
///
/// Cheap to clone (`Arc` inside).
#[derive(Clone, Default, Debug)]
pub struct AllowList {
    inner: Arc<RwLock<HashMap<ProjectId, HashSet<String>>>>,
}

impl AllowList {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a host to `project`'s allowlist. Idempotent.
    pub async fn allow(&self, project: &ProjectId, host: impl Into<String>) {
        let mut g = self.inner.write().await;
        g.entry(*project)
            .or_default()
            .insert(host.into().to_lowercase());
    }

    /// Remove a host. Returns `true` if it was present.
    pub async fn deny(&self, project: &ProjectId, host: &str) -> bool {
        let mut g = self.inner.write().await;
        match g.get_mut(project) {
            Some(set) => set.remove(&host.to_lowercase()),
            None => false,
        }
    }

    /// Snapshot the allowlist for `project`. Empty when never seeded.
    pub async fn list(&self, project: &ProjectId) -> Vec<String> {
        let g = self.inner.read().await;
        g.get(project)
            .map(|s| {
                let mut v: Vec<String> = s.iter().cloned().collect();
                v.sort();
                v
            })
            .unwrap_or_default()
    }

    /// The load-bearing check. Runs three gates in order:
    ///
    /// 1. [`check_url_safety`] — reject userinfo URLs and IP-literal hosts
    ///    in denied classes (loopback / RFC1918 / link-local / IMDS /
    ///    IPv4-mapped IPv6). Fires *before* the allowlist so an operator
    ///    who allowlists a hostname is not on the hook for what that
    ///    hostname resolves to via decimal-encoded IPv4 trickery.
    /// 2. Lowercase the URL's host string.
    /// 3. Confirm the project has explicitly opted in. Default is **deny**.
    ///
    /// DNS-time rebinding (an allowlisted hostname that resolves to a
    /// private IP at connect time) is enforced by
    /// [`RebindingGuardedResolver`] wired into the reqwest client. The two
    /// halves together close the parse → connect TOCTOU.
    pub async fn check(&self, project: &ProjectId, url: &str) -> Result<(), HttpError> {
        self.check_with(project, url, false).await
    }

    /// Variant of [`Self::check`] that honours the
    /// [`GuardConfig::allow_loopback_for_tests`] escape hatch. See that
    /// field's doc for the contract; production code calls [`Self::check`].
    pub async fn check_with(
        &self,
        project: &ProjectId,
        url: &str,
        allow_loopback: bool,
    ) -> Result<(), HttpError> {
        let parsed = check_url_safety_with(url, allow_loopback)?;
        let host = parsed
            .host_str()
            .ok_or_else(|| HttpError::InvalidUrl(format!("no host in url: {url}")))?
            .to_lowercase();
        let g = self.inner.read().await;
        let ok = g
            .get(project)
            .map(|set| set.contains(&host))
            .unwrap_or(false);
        if ok {
            Ok(())
        } else {
            Err(HttpError::HostDenied { host })
        }
    }
}

/// Per-project token-bucket rate limiter. Leans on `governor::RateLimiter`'s
/// keyed variant; the same crate `basin-auth` already pulls in.
///
/// 10 req/s sustained, burst 30. Sustained is rendered as 600/min by
/// `governor::Quota::per_minute`. The burst allowance is set explicitly so a
/// short flurry doesn't immediately stall.
///
/// ## Phase 6.X.D — heartbeat-reconciled slice
///
/// Optionally carries a [`basin_catalog::SliceGate`] that consults the
/// leaseholder's last-received slice for `(project, NetOutboundQps)` before
/// admitting traffic. When a slice is present the gate is the binding cap;
/// when absent, the local `governor` bucket is the cap (back-compat). The
/// slice closes the multi-instance bypass: N replicas with their own
/// `governor` buckets can each admit 10 req/s; with the slice gate they
/// share `project_total / partition_count` per replica.
///
/// Cheap to clone.
#[derive(Clone)]
pub struct RateLimit {
    inner: Arc<RateLimiter<ProjectId, DefaultKeyedStateStore<ProjectId>, DefaultClock>>,
    slice: Option<basin_catalog::SliceGate>,
}

impl std::fmt::Debug for RateLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimit").finish_non_exhaustive()
    }
}

impl RateLimit {
    /// Sustained rate, requests per second. Hard-coded; a per-project override
    /// is on the v0.2 list.
    pub const SUSTAINED_PER_SEC: u32 = 10;
    /// Burst allowance (tokens that can accumulate while idle). 3× sustained
    /// matches the Supabase pg_net default.
    pub const BURST: u32 = 30;

    pub fn new() -> Self {
        // Convert "10 req/s" → "600 req/min". `Quota::with_period` would be
        // more direct but the 0.7 governor API exposes a per-second factory
        // we can use straight up.
        let per_sec = NonZeroU32::new(Self::SUSTAINED_PER_SEC).expect("nonzero const");
        let burst = NonZeroU32::new(Self::BURST).expect("nonzero const");
        let quota = Quota::per_second(per_sec).allow_burst(burst);
        Self {
            inner: Arc::new(RateLimiter::keyed(quota)),
            slice: None,
        }
    }

    /// Phase 6.X.D — attach a [`basin_catalog::SliceGate`] keyed on
    /// [`basin_catalog::CapKind::NetOutboundQps`]. The heartbeat loop pushes
    /// fresh slices into the gate's view; subsequent `check` calls reject
    /// when the per-partition slice is exhausted, even if the local
    /// `governor` bucket still has tokens.
    pub fn with_slice_gate(mut self, gate: basin_catalog::SliceGate) -> Self {
        self.slice = Some(gate);
        self
    }

    /// One token check. Errors with [`HttpError::RateLimited`] when the
    /// project's bucket is empty *or* — under Phase 6.X.D — when the
    /// coordinator-handed slice for `NetOutboundQps` is exhausted.
    pub fn check(&self, project: &ProjectId) -> Result<(), HttpError> {
        // Slice check first so the noisy-neighbour bypass is closed even if
        // the local `governor` bucket has plenty of tokens.
        if let Some(gate) = &self.slice {
            // The gate's `try_consume` is async — use the sync read of the
            // slice + a synchronous best-effort counter decrement to keep
            // `check` callable from sync contexts. When no slice is set
            // (coordinator silent / project uncapped), `slice_for_sync`
            // returns `SLICE_UNSET` and we admit through to `governor`.
            let slice = gate
                .view()
                .slice_for_sync(*project, basin_catalog::CapKind::NetOutboundQps);
            if slice != basin_catalog::SLICE_UNSET && slice == 0 {
                return Err(HttpError::RateLimited);
            }
        }
        match self.inner.check_key(project) {
            Ok(_) => Ok(()),
            Err(_) => Err(HttpError::RateLimited),
        }
    }

    /// Async variant that consumes a token from both the coordinator-handed
    /// slice (when present) and the local `governor` bucket. Callers in async
    /// contexts should prefer this; the sync [`Self::check`] is retained for
    /// existing blocking call sites. Returns [`HttpError::RateLimited`] when
    /// either gate denies.
    pub async fn check_async(&self, project: &ProjectId) -> Result<(), HttpError> {
        if let Some(gate) = &self.slice {
            if gate
                .try_consume(*project, basin_catalog::CapKind::NetOutboundQps, 1)
                .await
                .is_err()
            {
                return Err(HttpError::RateLimited);
            }
        }
        match self.inner.check_key(project) {
            Ok(_) => Ok(()),
            Err(_) => Err(HttpError::RateLimited),
        }
    }
}

impl Default for RateLimit {
    fn default() -> Self {
        Self::new()
    }
}
