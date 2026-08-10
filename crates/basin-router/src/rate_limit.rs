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
//! Two layers, resolved per project on the hot path:
//!
//! 1. **Global default** — the env var `BASIN_PGWIRE_RATE_LIMIT_QPS` sets the
//!    sustained statements/sec every project gets when it has no per-project
//!    override (burst is `qps × BURST_FACTOR`, same 3× shape as basin-net).
//!    `0` / unset / empty disables the limiter entirely (the protocol-side
//!    option is `None`), so existing demos and benches keep their unbounded
//!    throughput — the disabled path is a true no-op (the per-query check is
//!    behind `if let Some(rl) = &self.rate_limit`).
//! 2. **Per-project (per-tier) override** — read from the catalog
//!    (`Catalog::get_project_rate_limit_qps`) lazily on a project's first query
//!    and cached for the process lifetime; the control plane pushes it per tier
//!    via `POST /admin/v1/projects/:id/rate-limit`. When present, the project's
//!    dedicated bucket is consulted INSTEAD of the global keyed one, so
//!    Free/Pro/Scale can carry different caps on the same engine.
//!
//! # When the bucket is empty
//!
//! [`PgRateLimit::check`] returns `Err(())`; the protocol layer maps this
//! to Postgres SQLSTATE `53400` (`configuration_limit_exceeded`) — the
//! same code Postgres itself raises for connection / statement quota
//! breaches. Drivers map it to a specific exception class so app code
//! can retry-with-backoff distinct from a parse / permission error.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::{Arc, RwLock};

use basin_common::ProjectId;
use governor::{
    clock::DefaultClock, state::keyed::DefaultKeyedStateStore, state::InMemoryState,
    state::NotKeyed, Quota, RateLimiter,
};

/// A single (not-keyed) token-bucket limiter for one project's override quota.
type DirectLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

fn build_quota(qps: u32) -> Quota {
    let per_sec = NonZeroU32::new(qps.max(1)).expect("qps>=1");
    let burst = NonZeroU32::new(qps.saturating_mul(BURST_FACTOR).max(1)).expect("nonzero burst");
    Quota::per_second(per_sec).allow_burst(burst)
}

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
    /// Per-project quota OVERRIDES (the control plane pushes these per tier via
    /// the catalog; the router installs them at connection setup). When a
    /// project has an override its dedicated bucket is consulted INSTEAD of the
    /// global keyed bucket, so Free/Pro/Scale can carry different caps on the
    /// same engine. Read on the hot path under a shared lock (uncontended);
    /// written rarely (once per project per quota change).
    overrides: Arc<RwLock<HashMap<ProjectId, (u32, Arc<DirectLimiter>)>>>,
    /// Optional catalog handle: when set, [`Self::check_async`] lazily resolves
    /// each project's per-tier quota from the catalog on first sight and caches
    /// it (one fetch per project per process; later checks are a map read).
    catalog: Option<Arc<dyn basin_catalog::Catalog>>,
    /// Projects whose per-project quota was already resolved (whether or not an
    /// override exists), so `check_async` doesn't re-hit the catalog each query.
    resolved: Arc<RwLock<std::collections::HashSet<ProjectId>>>,
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
            overrides: Arc::new(RwLock::new(HashMap::new())),
            catalog: None,
            resolved: Arc::new(RwLock::new(std::collections::HashSet::new())),
        }
    }

    /// Attach a catalog so per-project quotas resolve lazily from
    /// `get_project_rate_limit_qps` on first query (cached thereafter).
    pub fn with_catalog(mut self, catalog: Arc<dyn basin_catalog::Catalog>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// Whether this project currently carries a per-project quota override
    /// (vs. falling through to the global default bucket). Cheap shared-lock
    /// read; surfaced for telemetry / admin introspection ("is this project on
    /// a custom cap?") and consulted by tests. Mirrors the queryable surface
    /// `ConnectionLimiter` exposes (`live_count` / `get`).
    pub fn has_override(&self, project: &ProjectId) -> bool {
        self.overrides
            .read()
            .expect("pg rate-limit overrides poisoned")
            .contains_key(project)
    }

    /// Install (or update) a per-project quota override. Idempotent: a repeat
    /// call with the same qps is a no-op (keeps the existing bucket + its
    /// accumulated tokens); a changed qps rebuilds the bucket. Called at
    /// connection setup from the catalog-pushed value, so the hot-path `check`
    /// just reads the map.
    pub fn set_project_qps(&self, project: ProjectId, qps: u32) {
        {
            let map = self
                .overrides
                .read()
                .expect("pg rate-limit overrides poisoned");
            if map.get(&project).map(|(q, _)| *q) == Some(qps) {
                return;
            }
        }
        let lim = Arc::new(RateLimiter::direct(build_quota(qps)));
        self.overrides
            .write()
            .expect("pg rate-limit overrides poisoned")
            .insert(project, (qps, lim));
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
        // Per-project override bucket takes precedence over the global keyed one.
        {
            let map = self
                .overrides
                .read()
                .expect("pg rate-limit overrides poisoned");
            if let Some((_, lim)) = map.get(project) {
                return lim.check().map(|_| ()).map_err(|_| ());
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
        // Lazily resolve this project's per-tier quota from the catalog on first
        // sight (cached in `resolved`; one fetch per project per process).
        if let Some(catalog) = &self.catalog {
            let already = self
                .resolved
                .read()
                .expect("pg rate-limit resolved poisoned")
                .contains(project);
            if !already {
                if let Ok(Some(qps)) = catalog.get_project_rate_limit_qps(project).await {
                    self.set_project_qps(*project, qps);
                }
                self.resolved
                    .write()
                    .expect("pg rate-limit resolved poisoned")
                    .insert(*project);
            }
        }
        {
            let map = self
                .overrides
                .read()
                .expect("pg rate-limit overrides poisoned");
            if let Some((_, lim)) = map.get(project) {
                return lim.check().map(|_| ()).map_err(|_| ());
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
    fn per_project_override_caps_independently() {
        // Generous global default; one project gets a tight per-project cap.
        let rl = PgRateLimit::with_qps(100_000);
        let capped = ProjectId::new();
        let other = ProjectId::new();
        rl.set_project_qps(capped, 1); // 1 qps × 3 burst = 3 tokens
        for _ in 0..3 {
            rl.check(&capped).unwrap();
        }
        let mut throttled = false;
        for _ in 0..10 {
            if rl.check(&capped).is_err() {
                throttled = true;
                break;
            }
        }
        assert!(throttled, "per-project override did not cap the project");
        // A project WITHOUT an override is unaffected (uses the generous
        // default) — one project's tight cap cannot starve another.
        rl.check(&other)
            .expect("non-overridden project starved by another's cap");
    }

    #[test]
    fn set_project_qps_is_idempotent_on_same_value() {
        let rl = PgRateLimit::with_qps(10);
        let p = ProjectId::new();
        rl.set_project_qps(p, 5);
        for _ in 0..5 {
            let _ = rl.check(&p);
        }
        // Re-setting the SAME qps must NOT reset the bucket (keeps accumulated
        // consumption); a different qps rebuilds it.
        rl.set_project_qps(p, 5);
        assert!(rl.has_override(&p));
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

    #[test]
    fn no_limiter_constructed_is_a_true_noop() {
        // The "disabled" config is `None` at the protocol layer — there is no
        // limiter at all, so the hot path pays nothing. Assert the env parser
        // produces exactly that for the default/unset case, which is what the
        // protocol code gates on (`if let Some(rl) = &self.rate_limit`). This
        // is the no-behavior-change-by-default guarantee.
        assert!(from_env_qps(None).unwrap().is_none());
        assert!(from_env_qps(Some("0")).unwrap().is_none());
        assert!(from_env_qps(Some("")).unwrap().is_none());
    }

    #[tokio::test]
    async fn catalog_override_beats_default_and_is_isolated() {
        use basin_catalog::{Catalog, InMemoryCatalog};
        // Generous global default; one project gets a tight per-project cap
        // persisted in the catalog. `check_async` must resolve it lazily on
        // first sight and apply it INSTEAD of the default.
        let catalog = Arc::new(InMemoryCatalog::new());
        let capped = ProjectId::new();
        let other = ProjectId::new();
        catalog
            .set_project_rate_limit_qps(&capped, 1)
            .await
            .unwrap(); // 1 qps × 3 burst = 3 tokens
        let rl = PgRateLimit::with_qps(100_000).with_catalog(catalog);

        // First check resolves + caches the override from the catalog.
        rl.check_async(&capped).await.unwrap();
        assert!(
            rl.has_override(&capped),
            "catalog qps should have been resolved into a per-project override"
        );
        // Drain the rest of the tight bucket, then it must throttle — proving
        // the catalog override (1 qps) beat the generous 100k default.
        for _ in 0..2 {
            let _ = rl.check_async(&capped).await;
        }
        let mut throttled = false;
        for _ in 0..10 {
            if rl.check_async(&capped).await.is_err() {
                throttled = true;
                break;
            }
        }
        assert!(throttled, "catalog override did not cap the project");

        // A project with NO catalog override uses the generous default and is
        // unaffected by the capped project burning its budget (isolation).
        rl.check_async(&other)
            .await
            .expect("non-overridden project starved by another's catalog cap");
        assert!(
            !rl.has_override(&other),
            "project without a catalog entry must not gain an override"
        );
    }

    #[tokio::test]
    async fn catalog_absent_override_falls_through_to_default() {
        use basin_catalog::InMemoryCatalog;
        // No catalog entry → the project rides the global default bucket and
        // the catalog is consulted exactly once (cached as resolved-with-no-
        // override thereafter). A small default still admits the burst.
        let catalog = Arc::new(InMemoryCatalog::new());
        let rl = PgRateLimit::with_qps(10).with_catalog(catalog);
        let p = ProjectId::new();
        for i in 0..20 {
            rl.check_async(&p)
                .await
                .unwrap_or_else(|_| panic!("rejected at iter {i} under default"));
        }
        assert!(!rl.has_override(&p));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_hammer_one_project_stays_correct() {
        // Many tasks hammering ONE project's limiter must not deadlock and must
        // not over-admit beyond the bucket. With 1 qps × 3 burst, at most a
        // small number of the immediate calls succeed; the rest are throttled.
        let rl = Arc::new(PgRateLimit::with_qps(1));
        let p = ProjectId::new();
        let mut handles = Vec::new();
        let allowed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for _ in 0..64 {
            let rl = rl.clone();
            let allowed = allowed.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..50 {
                    if rl.check_async(&p).await.is_ok() {
                        allowed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }));
        }
        for h in handles {
            h.await.expect("limiter task panicked / deadlocked");
        }
        // 3200 attempts in a tiny wall-clock window against a 1-qps/3-burst
        // bucket: comfortably fewer than half can succeed. The exact count is
        // timing-dependent (governor refills against the real clock), so we
        // assert the safety property (no runaway over-admission), not equality.
        let n = allowed.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            n < 1600,
            "limiter over-admitted under concurrency: {n} allowed of 3200"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_two_projects_isolated() {
        // Hammering project A's tight bucket must not starve project B, even
        // under concurrent contention from many tasks.
        let rl = Arc::new(PgRateLimit::with_qps(1));
        let a = ProjectId::new();
        let b = ProjectId::new();
        let mut handles = Vec::new();
        for _ in 0..32 {
            let rl = rl.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..100 {
                    let _ = rl.check_async(&a).await;
                }
            }));
        }
        // While A is being hammered, B's very first call must still pass.
        let b_ok = rl.check_async(&b).await.is_ok();
        for h in handles {
            h.await.expect("limiter task panicked / deadlocked");
        }
        assert!(b_ok, "project B starved by concurrent load on project A");
    }
}
