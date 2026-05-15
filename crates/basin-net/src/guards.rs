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
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use basin_common::ProjectId;
use governor::{clock::DefaultClock, state::keyed::DefaultKeyedStateStore, Quota, RateLimiter};
use tokio::sync::RwLock;

use crate::types::HttpError;

/// Static knobs read from the environment. Cloned into [`HttpClient`] at
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
        }
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

    /// The load-bearing check. Splits `url` for its host part, lowercases
    /// it, and confirms `project` has explicitly opted in. The default
    /// behaviour is **deny** so a freshly-provisioned project cannot
    /// accidentally exfiltrate to anywhere.
    pub async fn check(&self, project: &ProjectId, url: &str) -> Result<(), HttpError> {
        let parsed =
            url::Url::parse(url).map_err(|e| HttpError::InvalidUrl(format!("{url}: {e}")))?;
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
/// Cheap to clone.
#[derive(Clone)]
pub struct RateLimit {
    inner: Arc<RateLimiter<ProjectId, DefaultKeyedStateStore<ProjectId>, DefaultClock>>,
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
        }
    }

    /// One token check. Errors with [`HttpError::RateLimited`] when the
    /// project's bucket is empty.
    pub fn check(&self, project: &ProjectId) -> Result<(), HttpError> {
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
