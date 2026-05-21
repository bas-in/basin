//! Per-project pgwire rate limiter.
//!
//! Mirrors `basin_net::guards::RateLimit` (which throttles outbound HTTP
//! from `basin-net`), keeping the token-bucket implementation uniform
//! across both the *inbound* pgwire protocol and the *outbound* HTTP
//! protocol. The two limiters are separate instances with separate
//! quotas so a project burning their HTTP budget doesn't starve their
//! pgwire budget and vice versa.
//!
//! # Quota
//!
//! v0.1 hard-codes 100 statements/sec sustained, burst 300 (3× sustained,
//! same shape as the basin-net default). A per-project override and a
//! catalog-driven config are deferred to v0.2; the env var
//! `BASIN_PGWIRE_RATE_LIMIT_QPS` lets the operator dial the global rate
//! at startup, with `0` (the default) disabling the limiter entirely so
//! existing demos and benches keep their unbounded throughput.
//!
//! # When the bucket is empty
//!
//! [`PgRateLimit::check`] returns `Err(())`; the protocol layer maps this
//! to Postgres SQLSTATE `53400` (`configuration_limit_exceeded`) — the
//! same code Postgres itself raises for connection / statement quota
//! breaches. Drivers map it to a specific exception class so app code
//! can retry-with-backoff distinct from a parse / permission error.

use std::num::NonZeroU32;
use std::sync::Arc;

use basin_common::ProjectId;
use governor::{clock::DefaultClock, state::keyed::DefaultKeyedStateStore, Quota, RateLimiter};

/// Sustained statements/sec when the limiter is enabled. Matches the
/// 10:30 ratio basin-net uses for outbound HTTP, scaled 10× because
/// pgwire is inherently chattier than per-project HTTP egress.
pub const DEFAULT_SUSTAINED_QPS: u32 = 100;

/// Burst factor (peak allowance / sustained). Same 3× ratio as basin-net.
pub const BURST_FACTOR: u32 = 3;

/// Per-project pgwire rate limiter. Cheap to clone (Arc inside).
///
/// ## Phase 6.X.D — heartbeat-reconciled slice (ADR 0023)
///
/// Optionally carries a [`basin_catalog::SliceGate`] keyed on
/// [`basin_catalog::CapKind::PgQps`]. When a slice is present each `check`
/// also consumes one token from the coordinator-handed slice, closing the
/// multi-instance bypass: N replicas with their own `governor` buckets used
/// to admit `N × sustained_qps` total; with the slice they share
/// `project_total / partition_count` per replica.
#[derive(Clone)]
pub struct PgRateLimit {
    inner: Arc<RateLimiter<ProjectId, DefaultKeyedStateStore<ProjectId>, DefaultClock>>,
    sustained_qps: u32,
    slice: Option<basin_catalog::SliceGate>,
}

impl std::fmt::Debug for PgRateLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgRateLimit")
            .field("sustained_qps", &self.sustained_qps)
            .finish_non_exhaustive()
    }
}

impl PgRateLimit {
    /// Build a limiter with the default quota (100 qps sustained, 300
    /// burst).
    pub fn new() -> Self {
        Self::with_qps(DEFAULT_SUSTAINED_QPS)
    }

    /// Build a limiter with a custom sustained-qps cap. Burst is always
    /// `qps × BURST_FACTOR`. Panics if `qps` is zero — callers that want
    /// "off" should not construct a limiter at all (the protocol-side
    /// option is `None`).
    pub fn with_qps(qps: u32) -> Self {
        let per_sec = NonZeroU32::new(qps).expect("PgRateLimit::with_qps requires qps > 0");
        let burst_n = qps.saturating_mul(BURST_FACTOR).max(1);
        let burst = NonZeroU32::new(burst_n).expect("nonzero burst");
        let quota = Quota::per_second(per_sec).allow_burst(burst);
        Self {
            inner: Arc::new(RateLimiter::keyed(quota)),
            sustained_qps: qps,
            slice: None,
        }
    }

    /// Phase 6.X.D — attach a [`basin_catalog::SliceGate`] so `check_async`
    /// consults the leaseholder's last-received `PgQps` slice. The local
    /// `governor` bucket remains as a secondary cap (back-compat); the
    /// slice is the binding one when a coordinator total is configured.
    pub fn with_slice_gate(mut self, gate: basin_catalog::SliceGate) -> Self {
        self.slice = Some(gate);
        self
    }

    /// Sustained quota in statements per second. Surfaced for telemetry
    /// / startup logs so the operator can confirm the env var was parsed
    /// correctly.
    pub fn sustained_qps(&self) -> u32 {
        self.sustained_qps
    }

    /// One token check. Returns `Err(())` if the project has burned their
    /// budget for the current window. The bare `()` keeps this module
    /// dependency-free of pgwire types; the call site does the
    /// SQLSTATE-53400 mapping.
    ///
    /// Sync variant — does **not** consume from the slice gate (it can't,
    /// without awaiting). Use [`Self::check_async`] from async sites to get
    /// the slice enforcement; this sync entrypoint preserves the existing
    /// pgwire call site shape and falls through to `governor` only.
    pub fn check(&self, project: &ProjectId) -> Result<(), ()> {
        if let Some(gate) = &self.slice {
            let slice = gate
                .view()
                .slice_for_sync(*project, basin_catalog::CapKind::PgQps);
            if slice != basin_catalog::SLICE_UNSET && slice == 0 {
                return Err(());
            }
        }
        self.inner.check_key(project).map(|_| ()).map_err(|_| ())
    }

    /// Async variant of [`Self::check`] that consumes one token from the
    /// coordinator-handed slice (Phase 6.X.D) as well as the local
    /// `governor` bucket. Either side denying fails the check.
    pub async fn check_async(&self, project: &ProjectId) -> Result<(), ()> {
        if let Some(gate) = &self.slice {
            if gate
                .try_consume(*project, basin_catalog::CapKind::PgQps, 1)
                .await
                .is_err()
            {
                return Err(());
            }
        }
        self.inner.check_key(project).map(|_| ()).map_err(|_| ())
    }
}

impl Default for PgRateLimit {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse `BASIN_PGWIRE_RATE_LIMIT_QPS` into an `Option<PgRateLimit>`.
/// Empty / `0` → `None` (limiter disabled). Anything else → enabled at
/// that rate. A non-numeric value is a hard startup error so a typo
/// doesn't silently disable the limiter.
pub fn from_env_qps(raw: Option<&str>) -> Result<Option<PgRateLimit>, String> {
    let s = match raw {
        Some(s) if !s.trim().is_empty() => s.trim(),
        _ => return Ok(None),
    };
    let qps: u32 = s
        .parse()
        .map_err(|e| format!("BASIN_PGWIRE_RATE_LIMIT_QPS={s:?} is not a u32: {e}"))?;
    if qps == 0 {
        return Ok(None);
    }
    Ok(Some(PgRateLimit::with_qps(qps)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_passes_within_burst() {
        let rl = PgRateLimit::with_qps(10);
        let t = ProjectId::new();
        // At 10 qps sustained × 3 burst = 30 free tokens before throttle.
        // Hit it 20 times immediately — must all succeed.
        for i in 0..20 {
            rl.check(&t)
                .unwrap_or_else(|_| panic!("rejected at iter {i}"));
        }
    }

    #[test]
    fn check_throttles_after_burst_exhausted() {
        let rl = PgRateLimit::with_qps(1);
        let t = ProjectId::new();
        // 1 qps × 3 burst = 3 tokens. Drain them.
        for _ in 0..3 {
            rl.check(&t).unwrap();
        }
        // Next call within the same second must reject.
        let mut throttled = false;
        for _ in 0..10 {
            if rl.check(&t).is_err() {
                throttled = true;
                break;
            }
        }
        assert!(throttled, "limiter never throttled despite drained burst");
    }

    #[test]
    fn separate_projects_have_separate_buckets() {
        let rl = PgRateLimit::with_qps(1);
        let a = ProjectId::new();
        let b = ProjectId::new();
        // Drain project A's bucket.
        for _ in 0..3 {
            rl.check(&a).unwrap();
        }
        // Project A is now (almost certainly) throttled — but project B
        // must still pass on the very first call. The limiter is keyed
        // per-ProjectId; one project burning their quota cannot starve
        // another.
        rl.check(&b)
            .expect("project B starved by project A's burst");
    }

    #[test]
    fn sustained_qps_round_trips() {
        let rl = PgRateLimit::with_qps(250);
        assert_eq!(rl.sustained_qps(), 250);
    }

    #[test]
    fn default_uses_documented_constant() {
        let rl = PgRateLimit::default();
        assert_eq!(rl.sustained_qps(), DEFAULT_SUSTAINED_QPS);
    }

    #[test]
    fn from_env_unset_returns_none() {
        assert!(from_env_qps(None).unwrap().is_none());
    }

    #[test]
    fn from_env_empty_returns_none() {
        assert!(from_env_qps(Some("")).unwrap().is_none());
        assert!(from_env_qps(Some("   ")).unwrap().is_none());
    }

    #[test]
    fn from_env_zero_returns_none() {
        assert!(from_env_qps(Some("0")).unwrap().is_none());
    }

    #[test]
    fn from_env_valid_returns_limiter() {
        let rl = from_env_qps(Some("42")).unwrap().unwrap();
        assert_eq!(rl.sustained_qps(), 42);
    }

    #[test]
    fn from_env_non_numeric_errors() {
        let err = from_env_qps(Some("notanumber")).unwrap_err();
        assert!(err.contains("not a u32"), "got {err}");
    }

    #[test]
    fn from_env_trims_whitespace() {
        let rl = from_env_qps(Some("  17  ")).unwrap().unwrap();
        assert_eq!(rl.sustained_qps(), 17);
    }
}
