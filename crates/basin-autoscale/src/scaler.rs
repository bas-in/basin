//! Core autoscale decision logic.
//!
//! `Scaler` is a pure-function evaluator: given a project ID and a
//! [`TenantSnapshot`] it returns a [`ScaleDecision`]. All mutable state
//! (consecutive hot/cool tick counts, cooldown timestamps, current shard
//! counts) lives in `Scaler` and is updated via `evaluate`.
//!
//! # Decision algorithm
//!
//! Each tick `evaluate` is called once per active project:
//!
//! 1. Compute `cpu_pct = snapshot.cpu_ms / cpu_budget_ms_per_tick * 100`.
//! 2. If `cpu_pct >= threshold_cpu_pct`:
//!    - Increment the project's hot-tick counter.
//!    - Reset the cool-tick counter.
//!    - If hot-ticks `>= hysteresis_window` AND current shards `< max_shards`
//!      AND cooldown has elapsed → emit `SplitShard`; reset hot-ticks;
//!      record split timestamp.
//! 3. Else (cool tick):
//!    - Increment the project's cool-tick counter.
//!    - Reset the hot-tick counter.
//!    - If cool-ticks `>= hysteresis_window` AND current shards `> min_shards`
//!      AND cooldown has elapsed → emit `MergeShards`; reset cool-ticks.
//! 4. All other cases → `Hold`.
//!
//! A project with `cpu_ms == 0` (completely idle) is always `Hold` unless
//! its shard count exceeds `min_shards` AND the merge hysteresis window has
//! filled.

use std::collections::HashMap;
use std::time::Duration;

use basin_common::ProjectId;
use chrono::{DateTime, Utc};

/// Per-project usage snapshot passed to [`Scaler::evaluate`].
///
/// Mirrors `basin_catalog::usage::TenantCounters` field-for-field so the
/// service binary can populate this from the catalog query without pulling
/// `basin-catalog` into the policy library itself (which would create a
/// heavyweight dependency on the full catalog crate just for a struct).
#[derive(Clone, Debug, Default)]
pub struct TenantSnapshot {
    /// Project this snapshot belongs to.
    pub project_id: ProjectId,
    /// Hour-truncated period start (UTC). Used for logging; not load-bearing
    /// for the decision algorithm.
    pub period_start: DateTime<Utc>,
    /// Cumulative bytes written (monotonic).
    pub storage_bytes: u64,
    /// Total object-store operations (monotonic).
    pub ops_count: u64,
    /// 1.0 if active this period, 0.0 otherwise.
    pub active_hours: f64,
    /// Cumulative CPU in milliseconds (monotonic). The primary signal for
    /// the autoscale decision.
    pub cpu_ms: u64,
}

/// Configuration for [`Scaler`].
#[derive(Clone, Debug)]
pub struct ScalerConfig {
    /// CPU budget per tick in milliseconds. Used to normalise `cpu_ms` into a
    /// percentage. Typically `interval_secs * 1000 * num_vcpu_budget`.
    /// Default: 60_000 ms (60 s at 1 vCPU).
    pub cpu_budget_ms_per_tick: u64,

    /// Percentage of the CPU budget above which a project is considered "hot".
    /// Range 1–100. Default: 80.
    pub threshold_cpu_pct: u8,

    /// Percentage below which a project is considered "cool" and eligible for
    /// merge. Must be ≤ `threshold_cpu_pct`. Default: 30.
    pub merge_threshold_cpu_pct: u8,

    /// Number of consecutive hot (or cool) ticks required before a split (or
    /// merge) decision is emitted. Default: 3.
    pub hysteresis_window: u32,

    /// Minimum number of shards per tenant. Split is never proposed below
    /// this value; merge is held at this floor. Default: 1.
    pub min_shards: u32,

    /// Maximum number of shards per tenant. Split is capped here. Default: 16.
    pub max_shards: u32,

    /// How long after a split before another split (or a merge) may be
    /// proposed for the same project. Prevents rapid flapping. Default: 300 s.
    pub cooldown: Duration,
}

impl Default for ScalerConfig {
    fn default() -> Self {
        Self {
            cpu_budget_ms_per_tick: 60_000,
            threshold_cpu_pct: 80,
            merge_threshold_cpu_pct: 30,
            hysteresis_window: 3,
            min_shards: 1,
            max_shards: 16,
            cooldown: Duration::from_secs(300),
        }
    }
}

/// Decision returned by [`Scaler::evaluate`] for a single project tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScaleDecision {
    /// No action required this tick.
    Hold,

    /// The project has been hot for `hysteresis_window` ticks; request a
    /// hash-bucket split via `basin-cli admin split <project_id>`.
    SplitShard {
        project_id: ProjectId,
        /// Human-readable explanation for the ops log.
        reason: String,
    },

    /// The project has been cool for `hysteresis_window` ticks and the
    /// cooldown has elapsed; request a rebalance/merge via
    /// `basin-cli admin rebalance <project_id>`.
    MergeShards {
        project_id: ProjectId,
        /// Human-readable explanation for the ops log.
        reason: String,
    },
}

/// Per-project tracking state managed inside `Scaler`.
#[derive(Debug, Default)]
struct ProjectState {
    /// Number of consecutive ticks where cpu was above the hot threshold.
    hot_ticks: u32,
    /// Number of consecutive ticks where cpu was below the merge threshold.
    cool_ticks: u32,
    /// Current number of shards for this project (starts at `min_shards`,
    /// incremented on each `SplitShard` decision, decremented on `MergeShards`).
    shard_count: u32,
    /// Timestamp of the last split or merge decision. Used to enforce the
    /// cooldown window.
    last_action: Option<DateTime<Utc>>,
}

/// Pure-function autoscale controller. Holds per-project tick state.
///
/// Thread safety: wrap in a `Mutex` / `RwLock` if used from multiple tasks.
#[derive(Debug)]
pub struct Scaler {
    config: ScalerConfig,
    state: HashMap<ProjectId, ProjectState>,
}

impl Scaler {
    /// Create a new `Scaler` with the given configuration.
    pub fn new(config: ScalerConfig) -> Self {
        Self {
            config,
            state: HashMap::new(),
        }
    }

    /// Create a `Scaler` with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(ScalerConfig::default())
    }

    /// Evaluate one tick for a project given the current `snapshot`.
    ///
    /// Updates internal tick counters and cooldown timestamps for the project
    /// and returns the scale decision for this tick.
    pub fn evaluate(&mut self, snapshot: &TenantSnapshot) -> ScaleDecision {
        let project_id = snapshot.project_id;
        let cfg = &self.config;

        // Initialise state for new projects at min_shards.
        let state = self.state.entry(project_id).or_insert_with(|| ProjectState {
            shard_count: cfg.min_shards,
            ..Default::default()
        });

        // Normalise cpu_ms to a percentage of the per-tick budget.
        let cpu_pct = if cfg.cpu_budget_ms_per_tick == 0 {
            0u64
        } else {
            snapshot.cpu_ms.saturating_mul(100) / cfg.cpu_budget_ms_per_tick
        };

        let now = Utc::now();
        let cooldown_elapsed = state.last_action.map_or(true, |t| {
            let elapsed = (now - t)
                .to_std()
                .unwrap_or(Duration::ZERO);
            elapsed >= cfg.cooldown
        });

        if cpu_pct >= u64::from(cfg.threshold_cpu_pct) {
            // Hot tick.
            state.hot_ticks += 1;
            state.cool_ticks = 0;

            if state.hot_ticks >= cfg.hysteresis_window
                && state.shard_count < cfg.max_shards
                && cooldown_elapsed
            {
                state.hot_ticks = 0;
                state.last_action = Some(now);
                state.shard_count += 1;
                return ScaleDecision::SplitShard {
                    project_id,
                    reason: format!(
                        "cpu_ms={} exceeded {}% of budget for {} consecutive ticks \
                         (shard_count now {})",
                        snapshot.cpu_ms,
                        cfg.threshold_cpu_pct,
                        cfg.hysteresis_window,
                        state.shard_count,
                    ),
                };
            }
        } else if cpu_pct <= u64::from(cfg.merge_threshold_cpu_pct) {
            // Cool tick.
            state.cool_ticks += 1;
            state.hot_ticks = 0;

            if state.cool_ticks >= cfg.hysteresis_window
                && state.shard_count > cfg.min_shards
                && cooldown_elapsed
            {
                state.cool_ticks = 0;
                state.last_action = Some(now);
                state.shard_count -= 1;
                return ScaleDecision::MergeShards {
                    project_id,
                    reason: format!(
                        "cpu_ms={} stayed below {}% of budget for {} consecutive ticks \
                         (shard_count now {})",
                        snapshot.cpu_ms,
                        cfg.merge_threshold_cpu_pct,
                        cfg.hysteresis_window,
                        state.shard_count,
                    ),
                };
            }
        } else {
            // Between thresholds: reset both counters.
            state.hot_ticks = 0;
            state.cool_ticks = 0;
        }

        ScaleDecision::Hold
    }

    /// Return the current tracked shard count for `project_id`, or `None` if
    /// the project has never been evaluated.
    pub fn shard_count(&self, project_id: &ProjectId) -> Option<u32> {
        self.state.get(project_id).map(|s| s.shard_count)
    }

    /// Override the shard count for a project. Useful when the daemon starts
    /// and reads the actual current shard count from the catalog before
    /// beginning tick evaluation.
    pub fn set_shard_count(&mut self, project_id: ProjectId, count: u32) {
        self.state
            .entry(project_id)
            .or_default()
            .shard_count = count;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::ProjectId;
    use chrono::Utc;

    fn snapshot(project_id: ProjectId, cpu_ms: u64) -> TenantSnapshot {
        TenantSnapshot {
            project_id,
            period_start: Utc::now(),
            storage_bytes: 0,
            ops_count: 1,
            active_hours: 1.0,
            cpu_ms,
        }
    }

    fn cold_snapshot(project_id: ProjectId) -> TenantSnapshot {
        snapshot(project_id, 0)
    }

    fn hot_snapshot(project_id: ProjectId, config: &ScalerConfig) -> TenantSnapshot {
        // 90% of budget — well above the default 80% threshold.
        let cpu_ms = config.cpu_budget_ms_per_tick * 90 / 100;
        snapshot(project_id, cpu_ms)
    }

    fn cool_snapshot(project_id: ProjectId, config: &ScalerConfig) -> TenantSnapshot {
        // 10% of budget — well below the default 30% merge threshold.
        let cpu_ms = config.cpu_budget_ms_per_tick * 10 / 100;
        snapshot(project_id, cpu_ms)
    }

    // T1: A completely idle project with one shard is always Hold.
    #[test]
    fn cold_project_returns_hold() {
        let mut scaler = Scaler::with_defaults();
        let p = ProjectId::new();
        for _ in 0..10 {
            let d = scaler.evaluate(&cold_snapshot(p));
            assert_eq!(d, ScaleDecision::Hold, "cold project should always Hold");
        }
    }

    // T2: A hot project triggers SplitShard after hysteresis_window ticks.
    #[test]
    fn hot_project_splits_after_hysteresis_window() {
        let config = ScalerConfig {
            hysteresis_window: 3,
            cooldown: Duration::ZERO, // no cooldown in tests
            ..Default::default()
        };
        let mut scaler = Scaler::new(config.clone());
        let p = ProjectId::new();

        // First (window - 1) ticks should Hold.
        for i in 0..(config.hysteresis_window - 1) {
            let d = scaler.evaluate(&hot_snapshot(p, &config));
            assert_eq!(
                d,
                ScaleDecision::Hold,
                "tick {} should still be Hold (window not reached)",
                i + 1
            );
        }

        // Tick at index `hysteresis_window` should emit SplitShard.
        let d = scaler.evaluate(&hot_snapshot(p, &config));
        assert!(
            matches!(d, ScaleDecision::SplitShard { project_id, .. } if project_id == p),
            "expected SplitShard on tick {}, got {:?}",
            config.hysteresis_window,
            d,
        );

        // Shard count should have incremented.
        assert_eq!(scaler.shard_count(&p), Some(2));
    }

    // T3: A project that was hot then cools down eventually merges.
    #[test]
    fn hot_then_cool_merges_after_cooldown() {
        let config = ScalerConfig {
            hysteresis_window: 3,
            cooldown: Duration::ZERO, // no cooldown delay in tests
            min_shards: 1,
            max_shards: 16,
            ..Default::default()
        };
        let mut scaler = Scaler::new(config.clone());
        let p = ProjectId::new();

        // Force the project to 2 shards.
        scaler.set_shard_count(p, 2);
        // Seed a past last_action so cooldown is treated as elapsed.
        if let Some(s) = scaler.state.get_mut(&p) {
            s.last_action = Some(Utc::now() - chrono::Duration::seconds(600));
        }

        // Cool ticks — should merge after hysteresis_window ticks.
        for i in 0..(config.hysteresis_window - 1) {
            let d = scaler.evaluate(&cool_snapshot(p, &config));
            assert_eq!(
                d,
                ScaleDecision::Hold,
                "cool tick {} should still be Hold",
                i + 1
            );
        }

        let d = scaler.evaluate(&cool_snapshot(p, &config));
        assert!(
            matches!(d, ScaleDecision::MergeShards { project_id, .. } if project_id == p),
            "expected MergeShards after {} cool ticks, got {:?}",
            config.hysteresis_window,
            d,
        );

        // Shard count should have decremented back to 1.
        assert_eq!(scaler.shard_count(&p), Some(1));
    }

    // T4: A hot project at max_shards holds, not splits.
    #[test]
    fn hot_project_at_max_shards_holds() {
        let config = ScalerConfig {
            max_shards: 2,
            hysteresis_window: 1,
            cooldown: Duration::ZERO,
            ..Default::default()
        };
        let mut scaler = Scaler::new(config.clone());
        let p = ProjectId::new();
        scaler.set_shard_count(p, 2); // already at max

        let d = scaler.evaluate(&hot_snapshot(p, &config));
        assert_eq!(
            d,
            ScaleDecision::Hold,
            "project at max_shards should Hold even when hot"
        );
    }

    // T5: A cool project at min_shards holds, not merges.
    #[test]
    fn cool_project_at_min_shards_holds() {
        let config = ScalerConfig {
            min_shards: 1,
            hysteresis_window: 1,
            cooldown: Duration::ZERO,
            ..Default::default()
        };
        let mut scaler = Scaler::new(config.clone());
        let p = ProjectId::new();
        // Default shard_count starts at min_shards (1).

        let d = scaler.evaluate(&cool_snapshot(p, &config));
        assert_eq!(
            d,
            ScaleDecision::Hold,
            "project at min_shards should Hold even when cool"
        );
    }

    // T6: Cooldown blocks a second split immediately after the first.
    #[test]
    fn cooldown_blocks_immediate_second_split() {
        let config = ScalerConfig {
            hysteresis_window: 1,
            cooldown: Duration::from_secs(300), // 5 min cooldown
            ..Default::default()
        };
        let mut scaler = Scaler::new(config.clone());
        let p = ProjectId::new();

        // First hot tick — should split (window=1, no prior action).
        let d1 = scaler.evaluate(&hot_snapshot(p, &config));
        assert!(
            matches!(d1, ScaleDecision::SplitShard { .. }),
            "first hot tick should split, got {:?}",
            d1
        );

        // Immediately try again — cooldown should block it.
        let d2 = scaler.evaluate(&hot_snapshot(p, &config));
        assert_eq!(
            d2,
            ScaleDecision::Hold,
            "second hot tick within cooldown window should Hold"
        );
    }

    // T7: Interrupting a hot run resets the counter.
    #[test]
    fn hot_run_interrupted_resets_counter() {
        let config = ScalerConfig {
            hysteresis_window: 3,
            cooldown: Duration::ZERO,
            ..Default::default()
        };
        let mut scaler = Scaler::new(config.clone());
        let p = ProjectId::new();

        // 2 hot ticks (just under window).
        scaler.evaluate(&hot_snapshot(p, &config));
        scaler.evaluate(&hot_snapshot(p, &config));

        // One cool tick interrupts the run.
        let d = scaler.evaluate(&cool_snapshot(p, &config));
        assert_eq!(d, ScaleDecision::Hold);

        // 2 more hot ticks — still not enough (counter was reset).
        scaler.evaluate(&hot_snapshot(p, &config));
        let d2 = scaler.evaluate(&hot_snapshot(p, &config));
        assert_eq!(
            d2,
            ScaleDecision::Hold,
            "counter should have reset; only 2 new hot ticks, not enough"
        );
    }

    // T8: Multiple isolated projects don't interfere with each other.
    #[test]
    fn projects_are_isolated() {
        let config = ScalerConfig {
            hysteresis_window: 2,
            cooldown: Duration::ZERO,
            ..Default::default()
        };
        let mut scaler = Scaler::new(config.clone());
        let hot = ProjectId::new();
        let cold = ProjectId::new();

        // Run hot project to window.
        scaler.evaluate(&hot_snapshot(hot, &config));
        let d = scaler.evaluate(&hot_snapshot(hot, &config));
        assert!(
            matches!(d, ScaleDecision::SplitShard { project_id, .. } if project_id == hot),
            "hot project should split"
        );

        // Cold project should not have been touched.
        let d2 = scaler.evaluate(&cold_snapshot(cold));
        assert_eq!(d2, ScaleDecision::Hold, "cold project should be unaffected");
    }
}
