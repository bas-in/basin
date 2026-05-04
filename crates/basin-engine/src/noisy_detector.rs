//! Per-tenant noisy-tenant detector.
//!
//! ## What this is
//!
//! A cooperative throttle hint: every time a tenant runs an RPC-ish unit of
//! work (currently: one [`TenantSession::execute`](crate::TenantSession::execute)),
//! we bump a tiny per-tenant rate estimator. If the rate stays above a
//! threshold for "long enough", we tag the tenant *noisy*. The engine reads
//! that bit when constructing the next session and downshifts DataFusion's
//! `target_partitions` from `num_cpus` to `1` for that tenant — its bulk
//! scans stop fanning out parallel range reads at full strength, which is
//! what was previously saturating the shared object-store concurrency budget.
//!
//! This is intentionally a hint, not a hard cap. The fair-share scheduler
//! over in `basin-storage::scheduler` is the real fairness mechanism; this
//! module just lets a heavy tenant self-cap so the scheduler has less work
//! to do.
//!
//! ## Cost
//!
//! [`NoisyState`] is sized to ~32 bytes on a 64-bit target (one `u64`
//! timestamp, one `u64` last-quiet-since timestamp, one `f32` rate, one
//! `bool` is_noisy + padding). A `HashMap<TenantId, NoisyState>` entry adds
//! the usual hashbrown bucket overhead (~16 bytes amortised), so per-tenant
//! footprint is ~48-64 bytes including the map slot. Lazy-allocated on the
//! first `record_query` call for a given tenant.
//!
//! ## Concurrency
//!
//! One `RwLock<HashMap<...>>` for the whole detector. `record_query` and
//! `is_noisy` both take the read lock for the lookup; only first-time
//! insertion takes the write lock. Lock is never held across `await` — this
//! file is sync top-to-bottom. Update of a tenant's `NoisyState` happens
//! while holding the read lock plus a per-entry `Mutex`, so two queries from
//! the same tenant don't lose updates.

use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use std::time::Instant;

use basin_common::TenantId;

/// Sustained rate (queries / sec) above which a tenant is considered noisy.
/// Picked to be well above any plausible OLTP workload (a few QPS) but
/// reachable by a single bulk-scan worker on this PoC (~10-30 scans/sec on a
/// quiet local filesystem). The bar in `scaling_noisy_neighbor` runs four
/// parallel full-scan tasks against a 5M-row table; sustained rate there is
/// roughly 6-12 scans/sec/tenant on the test machine, which puts the noisy
/// tenant comfortably above this floor while leaving the quiet tenant (200
/// point queries over ~1s) below it.
pub(crate) const NOISY_RATE_THRESHOLD_QPS: f32 = 8.0;

/// How long a tenant must sustain the threshold before the noisy bit flips
/// on. We require the rate to clear the bar at the moment of `is_noisy`
/// inspection, AND the rate-EWMA to have been integrating for at least this
/// many seconds — otherwise a single burst (e.g. a cold-start refresh) would
/// classify a tenant as noisy.
const NOISY_ARM_SECS: f32 = 2.0;

/// Once flipped on, the noisy bit only flips back off after the rate has
/// stayed below the threshold for at least this many seconds. Picked at 5s
/// per the design brief — long enough that a bulk job winding down doesn't
/// re-toggle on each lull, short enough that a tenant that genuinely went
/// quiet recovers its parallel read budget within "feels-instant" time.
const NOISY_DISARM_SECS: f32 = 5.0;

/// EWMA half-life for the per-tenant rate estimate. With a 1s half-life the
/// rate tracks the last ~2 seconds of traffic, which matches the "sustained
/// over the last 2s" wording of the design.
const RATE_HALF_LIFE_SECS: f32 = 1.0;

/// Per-tenant state. Sized to ~32 bytes on a 64-bit target.
///
/// Fields are pub(crate) only so the unit tests in this file can poke at
/// them; nothing outside this module should mutate them directly.
#[derive(Debug)]
struct NoisyState {
    /// Engine-clock millis at the most recent `record_query`. Used to decay
    /// `rate` between updates.
    last_update_ms: u64,
    /// Engine-clock millis at the most recent transition into the
    /// "below-threshold" regime. `is_noisy` flips off only after the tenant
    /// has been below threshold for `NOISY_DISARM_SECS` since this point.
    last_quiet_since_ms: u64,
    /// Engine-clock millis at the *first* `record_query` we ever saw for
    /// this tenant. Used to enforce the "must have been integrating for
    /// NOISY_ARM_SECS before going noisy" guard.
    first_seen_ms: u64,
    /// EWMA-decayed rate in queries-per-second.
    rate_qps: f32,
    /// Latched noisy bit. Read by `is_noisy`.
    is_noisy: bool,
}

impl NoisyState {
    fn new(now_ms: u64) -> Self {
        Self {
            last_update_ms: now_ms,
            last_quiet_since_ms: now_ms,
            first_seen_ms: now_ms,
            rate_qps: 0.0,
            is_noisy: false,
        }
    }

    /// Decay `rate_qps` by the elapsed time since `last_update_ms` and add
    /// one event-worth of mass. Then refresh `is_noisy` per the threshold +
    /// arm/disarm hysteresis.
    fn record(&mut self, now_ms: u64) {
        let dt_secs = ((now_ms.saturating_sub(self.last_update_ms)) as f32) / 1000.0;
        // Exponential decay: each `RATE_HALF_LIFE_SECS` halves the running
        // rate. `(0.5_f32).powf(dt / hl)` is exact to float precision for
        // the values we care about.
        let decay = (0.5_f32).powf(dt_secs / RATE_HALF_LIFE_SECS);
        // The "+ 1.0 / hl" term is the discrete-event impulse: one query
        // contributes `1 / half_life` to the rate so an event train at rate
        // `r` per second steady-states near `r` (within an order-of-1
        // constant we don't need exactly here — the threshold is calibrated
        // against the actual estimator output below).
        self.rate_qps = self.rate_qps * decay + 1.0 / RATE_HALF_LIFE_SECS;
        self.last_update_ms = now_ms;
        self.refresh_noisy(now_ms);
    }

    /// Re-evaluate `is_noisy` against the current `rate_qps`. Idempotent;
    /// safe to call from `is_noisy()` paths that haven't recorded a new
    /// query but want a fresh decision (e.g. a tenant that went quiet a
    /// while ago should be reported non-noisy without first issuing a
    /// query).
    fn refresh_noisy(&mut self, now_ms: u64) {
        // Decay the stored rate to "now" without folding in a new event.
        // Without this, `is_noisy` calls between record_query calls would
        // see a stale-high rate.
        let dt_secs = ((now_ms.saturating_sub(self.last_update_ms)) as f32) / 1000.0;
        let decayed = self.rate_qps * (0.5_f32).powf(dt_secs / RATE_HALF_LIFE_SECS);

        if decayed >= NOISY_RATE_THRESHOLD_QPS {
            // Only arm if we've been integrating for at least the arm window.
            // Without this guard a single fast burst at the start of a
            // session could trip the bit before `rate_qps` had time to
            // converge to a meaningful estimate.
            let integrated_secs =
                ((now_ms.saturating_sub(self.first_seen_ms)) as f32) / 1000.0;
            if integrated_secs >= NOISY_ARM_SECS {
                self.is_noisy = true;
            }
            // Reset the disarm clock as long as we're above threshold.
            self.last_quiet_since_ms = now_ms;
        } else {
            // Below threshold. If we've been below for long enough, flip
            // the bit back off. We do NOT update `last_quiet_since_ms`
            // here because we want the disarm window to count from the
            // *first* moment we dropped below, not the most recent
            // is_noisy poll.
            let quiet_secs =
                ((now_ms.saturating_sub(self.last_quiet_since_ms)) as f32) / 1000.0;
            if self.is_noisy && quiet_secs >= NOISY_DISARM_SECS {
                self.is_noisy = false;
            }
        }
    }
}

/// Per-tenant noisy-tenant detector. Cheap to construct; lazy-allocates one
/// `NoisyState` per tenant on first `record_query`.
pub(crate) struct NoisyDetector {
    /// `RwLock` because the steady-state path (record_query for a tenant
    /// that's already in the map) only needs the read lock plus per-entry
    /// `Mutex` to serialise two concurrent queries from the same tenant.
    /// First-time insertion takes the write lock; that's at most O(active
    /// tenants) across the lifetime of the engine.
    states: RwLock<HashMap<TenantId, Mutex<NoisyState>>>,
    /// Reference clock. We use `Instant` rather than `SystemTime` so wall
    /// clock skew can't affect detection.
    started: Instant,
}

impl NoisyDetector {
    pub(crate) fn new() -> Self {
        Self {
            states: RwLock::new(HashMap::new()),
            started: Instant::now(),
        }
    }

    fn now_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    /// Bump `tenant`'s rate by one event. O(1) on the steady-state path
    /// (HashMap probe + per-entry Mutex). Lazy-allocates a `NoisyState` on
    /// first call for a tenant.
    pub(crate) fn record_query(&self, tenant: &TenantId) {
        let now = self.now_ms();
        // Fast path: read lock + per-entry mutex.
        {
            let map = self
                .states
                .read()
                .expect("noisy detector states lock poisoned");
            if let Some(state) = map.get(tenant) {
                let mut s = state
                    .lock()
                    .expect("noisy detector tenant state poisoned");
                s.record(now);
                return;
            }
        }
        // Slow path: take the write lock and insert. Re-check on the way
        // in in case another caller raced us.
        let mut map = self
            .states
            .write()
            .expect("noisy detector states lock poisoned");
        let entry = map
            .entry(*tenant)
            .or_insert_with(|| Mutex::new(NoisyState::new(now)));
        let mut s = entry.lock().expect("noisy detector tenant state poisoned");
        s.record(now);
    }

    /// O(1) noisy-bit lookup. Returns `false` for tenants we've never seen
    /// (the lazy-allocate happens on `record_query`, not here).
    pub(crate) fn is_noisy(&self, tenant: &TenantId) -> bool {
        let now = self.now_ms();
        let map = self
            .states
            .read()
            .expect("noisy detector states lock poisoned");
        let Some(state) = map.get(tenant) else {
            return false;
        };
        let mut s = state.lock().expect("noisy detector tenant state poisoned");
        // Refresh the noisy bit against `now` even if no query was just
        // recorded — otherwise a tenant that went quiet a while ago would
        // stay tagged noisy until its next record_query, defeating the
        // 5-second disarm window.
        s.refresh_noisy(now);
        s.is_noisy
    }

    /// Test-only: drop all per-tenant state. Used by the unit tests below
    /// to keep cases independent.
    #[cfg(test)]
    fn clear(&self) {
        self.states
            .write()
            .expect("noisy detector states lock poisoned")
            .clear();
    }
}

impl Default for NoisyDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive `record_query` against a fixed reference time so we can test
    /// the EWMA + arm/disarm transitions without sleeping.
    ///
    /// We can't easily mock `Instant` without a virtual clock crate, so the
    /// tests below simulate time by calling the inner `record` /
    /// `refresh_noisy` directly with synthetic timestamps. The high-level
    /// `record_query` / `is_noisy` flow is covered by the engine-level
    /// integration test in `lib.rs::tests::noisy_detector_integration`.
    fn ms(secs: f32) -> u64 {
        (secs * 1000.0) as u64
    }

    #[test]
    fn quiet_tenant_never_noisy() {
        let mut s = NoisyState::new(0);
        // 1 query/sec for 30s — well below the 8 qps bar.
        for i in 1..=30 {
            s.record(ms(i as f32));
        }
        assert!(!s.is_noisy, "rate={:.2}", s.rate_qps);
    }

    #[test]
    fn sustained_noisy_arms_after_window() {
        let mut s = NoisyState::new(0);
        // 20 qps for 5 seconds. Should be classified noisy by the end.
        let mut t_ms: u64 = 0;
        for _ in 0..(20 * 5) {
            t_ms += 50; // 20 qps
            s.record(t_ms);
        }
        assert!(
            s.is_noisy,
            "rate={:.2} should be > {}",
            s.rate_qps, NOISY_RATE_THRESHOLD_QPS
        );
    }

    #[test]
    fn brief_burst_does_not_arm() {
        // A burst of 5 queries within the first 100ms shouldn't classify
        // as noisy because we haven't been integrating for the arm window
        // yet.
        let mut s = NoisyState::new(0);
        for i in 0..5 {
            s.record(20 * (i + 1));
        }
        assert!(!s.is_noisy, "rate={:.2}", s.rate_qps);
    }

    #[test]
    fn noisy_tenant_disarms_after_quiet_window() {
        let mut s = NoisyState::new(0);
        // Arm: 20 qps for 5 seconds.
        let mut t_ms: u64 = 0;
        for _ in 0..(20 * 5) {
            t_ms += 50;
            s.record(t_ms);
        }
        assert!(s.is_noisy);

        // Now go quiet. A single refresh_noisy call after the disarm
        // window must clear the bit.
        let later = t_ms + ms(NOISY_DISARM_SECS + 1.0);
        s.refresh_noisy(later);
        assert!(
            !s.is_noisy,
            "expected disarm after {}s of quiet, rate={:.2}",
            NOISY_DISARM_SECS, s.rate_qps
        );
    }

    #[test]
    fn noisy_tenant_stays_noisy_within_quiet_window() {
        let mut s = NoisyState::new(0);
        let mut t_ms: u64 = 0;
        for _ in 0..(20 * 5) {
            t_ms += 50;
            s.record(t_ms);
        }
        assert!(s.is_noisy);

        // Look 1 second later — well within the 5s disarm window.
        s.refresh_noisy(t_ms + 1_000);
        assert!(
            s.is_noisy,
            "expected to remain noisy for at least {}s after going quiet",
            NOISY_DISARM_SECS
        );
    }

    #[test]
    fn detector_returns_false_for_unseen_tenant() {
        let det = NoisyDetector::new();
        let t = TenantId::new();
        assert!(!det.is_noisy(&t));
    }

    #[test]
    fn detector_per_tenant_isolation() {
        // Two tenants: one noisy, one quiet. The quiet one's `is_noisy`
        // must stay false even when the noisy one trips its bit.
        let det = NoisyDetector::new();
        let noisy = TenantId::new();
        let quiet = TenantId::new();

        // Manually inject a noisy state for `noisy` so we don't depend on
        // wall-clock pacing inside this test.
        {
            let mut map = det.states.write().unwrap();
            let mut st = NoisyState::new(0);
            // Fake an arm: rate well over threshold, integrated long enough.
            st.first_seen_ms = 0;
            st.last_update_ms = 10_000;
            st.last_quiet_since_ms = 10_000;
            st.rate_qps = 100.0;
            st.is_noisy = true;
            map.insert(noisy, Mutex::new(st));
        }

        // Hack: temporarily make `now_ms` agree with our injected state by
        // never letting the disarm window trigger. We do that by calling
        // `record_query` on `noisy` so its `last_quiet_since_ms` updates
        // to a recent point, and by reading immediately.
        det.record_query(&noisy);
        assert!(det.is_noisy(&noisy), "noisy tenant should report noisy");
        assert!(
            !det.is_noisy(&quiet),
            "quiet tenant must not be tagged noisy"
        );

        det.clear();
    }
}
