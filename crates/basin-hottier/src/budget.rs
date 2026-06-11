//! Memory budget configuration and flush policy for the HTAP hot tier.
//!
//! # Constants
//!
//! Default values are exposed as module-level constants so tests and
//! documentation can reference the canonical numbers without duplicating the
//! literals.
//!
//! # Configuration
//!
//! [`MemTableConfig`] is the single source of truth for all budget numbers.
//! Construct it with [`MemTableConfig::from_env`] at engine startup to honour
//! the `BASIN_MEMTABLE_*` environment variable overrides, or call
//! `MemTableConfig::default()` in tests.
//!
//! # Flush policy
//!
//! [`FlushPolicy`] selects the scheduling algorithm used when global memory
//! pressure requires evicting a project's memtable. `LargestFirst` is the
//! default and matches ADR 0016 §"Memory budgets". `Lru` is a stub reserved
//! for a configurable fallback; it is not yet wired.

/// Default project-level hard cap in bytes (256 MiB).
///
/// Blocks new writes on the per-project semaphore when exceeded.
/// Configurable at project level via `ALTER PROJECT … SET basin.memtable_hard_cap`.
pub const DEFAULT_PROJECT_HARD_CAP_BYTES: u64 = 256 * 1024 * 1024;

/// Default project-level soft cap in bytes (192 MiB = 75 % of hard cap).
///
/// Triggers a background flush of the project's largest single-table memtable
/// (Phase 5.14.C4). Writes continue while the flush is in flight.
pub const DEFAULT_PROJECT_SOFT_CAP_BYTES: u64 = 192 * 1024 * 1024;

/// Default per-table soft cap in bytes (16 MiB).
///
/// Triggers a per-table flush independently of the project-level cap.
pub const DEFAULT_TABLE_SOFT_CAP_BYTES: u64 = 16 * 1024 * 1024;

/// Default age-based flush threshold in seconds (60 s).
///
/// A memtable whose oldest entry is older than this triggers a flush even
/// when neither the project nor the table soft cap has been reached.
pub const DEFAULT_MEMTABLE_MAX_AGE_SECS: u64 = 60;

/// Default clean-retention window in seconds (5 min) — S4 age-based residency.
///
/// Rows that have been flushed (acked clean) stay readable in memory until
/// they are this old, then become eviction candidates for the clean-budget
/// sweep. `0` = retain nothing: flushed rows are dropped immediately after
/// the ack, byte-for-byte the pre-S4 drain-at-flush behavior (the kill
/// switch). Override with `BASIN_HOTTIER_RETAIN_SECS`.
pub const DEFAULT_RETAIN_SECS: u64 = 300;

/// Default per-project cap on clean (flushed-and-retained) bytes (32 MiB).
///
/// Clean bytes also keep counting against the project hard/soft caps; this is
/// an additional, lower bound on how much memory mere read-acceleration may
/// hold per project. Override with `BASIN_HOTTIER_RETAIN_CAP`.
pub const DEFAULT_PROJECT_CLEAN_CAP_BYTES: u64 = 32 * 1024 * 1024;

/// Minimum allowed hard cap (1 MiB). Enforced by the `ALTER PROJECT` parser.
pub const MIN_HARD_CAP_BYTES: u64 = 1024 * 1024;

/// Maximum allowed hard cap (64 GiB). Enforced by the `ALTER PROJECT` parser.
pub const MAX_HARD_CAP_BYTES: u64 = 64 * 1024 * 1024 * 1024;

// ── FlushPolicy ───────────────────────────────────────────────────────────────

/// Scheduling algorithm for the flush task when global memory pressure rises.
///
/// `LargestFirst` is the default and matches ADR 0016. `Lru` is reserved for
/// future use but is **not implemented**: the flush scheduler requires a
/// `last_write_at` timestamp per project which is not yet threaded through the
/// `(project_id, bytes_allocated)` pairs accepted by
/// [`GlobalPressureScheduler::pick_flush_candidates`].
///
/// Attempting to run the scheduler with `FlushPolicy::Lru` will panic. Use
/// `largest_first` or `oldest_first` instead. Setting
/// `BASIN_MEMTABLE_FLUSH_POLICY=lru` logs a warning and falls back to
/// `largest_first`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FlushPolicy {
    /// Flush the project (or table) with the most bytes allocated first.
    /// Maximises memory reclaimed per flush I/O round-trip.
    #[default]
    LargestFirst,
    /// **Not implemented.** Reserved for a future LRU-by-last-write-at policy.
    ///
    /// The flush scheduler does not yet carry `last_write_at` timestamps, so
    /// this variant cannot be dispatched. Constructing a
    /// [`GlobalPressureScheduler`] with this policy and then calling
    /// [`GlobalPressureScheduler::pick_flush_candidates`] will panic.
    ///
    /// Use [`FlushPolicy::LargestFirst`] until this variant is wired.
    Lru,
}

// ── MemTableConfig ────────────────────────────────────────────────────────────

/// Process-wide memtable memory budget configuration.
///
/// One `MemTableConfig` is created at engine startup and shared across the
/// process. Per-project overrides (set via `ALTER PROJECT`) are applied by
/// passing a modified config to [`crate::registry::MemTableRegistry::new_with_config`]
/// for that project's sub-registry, or by storing the override in the
/// [`crate::registry::ProjectMemState`] (Phase 5.14.C2+ wiring).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemTableConfig {
    /// Project-level hard cap in bytes. Writes block on the per-project
    /// semaphore when `bytes_allocated` would exceed this.
    pub project_hard_cap_bytes: u64,
    /// Project-level soft cap in bytes. Crossing this threshold enqueues a
    /// background flush request (dequeued by the Phase 5.14.C4 flush task).
    pub project_soft_cap_bytes: u64,
    /// Per-table soft cap in bytes. Crossing this triggers a per-table flush
    /// independently of the project-level caps.
    pub table_soft_cap_bytes: u64,
    /// Age-based flush threshold in seconds. A memtable entry older than this
    /// triggers a flush even when size caps have not been reached.
    pub memtable_max_age_secs: u64,
    /// Flush scheduling algorithm used by the global-pressure flush path.
    pub flush_policy: FlushPolicy,
    /// S4 age-based residency: how long flushed (clean) rows stay readable in
    /// memory before the clean-budget sweep evicts them. `0` = retain nothing
    /// — the flush ack degenerates to today's drain-at-flush behavior (kill
    /// switch). Clean bytes keep counting against the project hard/soft caps
    /// regardless of this value.
    pub retain_secs: u64,
    /// S4 age-based residency: per-project cap on clean (flushed-and-retained)
    /// bytes. Exceeding it triggers eviction of the oldest clean entries down
    /// to the cap, independently of `retain_secs`.
    pub project_clean_cap_bytes: u64,
}

impl Default for MemTableConfig {
    fn default() -> Self {
        Self {
            project_hard_cap_bytes: DEFAULT_PROJECT_HARD_CAP_BYTES,
            project_soft_cap_bytes: DEFAULT_PROJECT_SOFT_CAP_BYTES,
            table_soft_cap_bytes: DEFAULT_TABLE_SOFT_CAP_BYTES,
            memtable_max_age_secs: DEFAULT_MEMTABLE_MAX_AGE_SECS,
            flush_policy: FlushPolicy::LargestFirst,
            retain_secs: DEFAULT_RETAIN_SECS,
            project_clean_cap_bytes: DEFAULT_PROJECT_CLEAN_CAP_BYTES,
        }
    }
}

impl MemTableConfig {
    /// Read budget overrides from well-known `BASIN_MEMTABLE_*` environment
    /// variables, falling back to compiled-in defaults for any variable that
    /// is absent or unparseable.
    ///
    /// | Variable | Default | Description |
    /// |---|---|---|
    /// | `BASIN_MEMTABLE_HARD_CAP` | 268435456 | Project hard cap, bytes |
    /// | `BASIN_MEMTABLE_SOFT_CAP` | 201326592 | Project soft cap, bytes |
    /// | `BASIN_MEMTABLE_TABLE_CAP` | 16777216 | Per-table soft cap, bytes |
    /// | `BASIN_MEMTABLE_MAX_AGE_SECS` | 60 | Age flush threshold, seconds |
    /// | `BASIN_MEMTABLE_FLUSH_POLICY` | `largest_first` | `largest_first` or `lru` |
    /// | `BASIN_HOTTIER_RETAIN_SECS` | 300 | Clean-retention window, seconds (0 = retain nothing) |
    /// | `BASIN_HOTTIER_RETAIN_CAP` | 33554432 | Per-project clean-bytes cap, bytes |
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

        if let Some(v) = env_u64("BASIN_MEMTABLE_HARD_CAP") {
            cfg.project_hard_cap_bytes = v;
        }
        if let Some(v) = env_u64("BASIN_MEMTABLE_SOFT_CAP") {
            cfg.project_soft_cap_bytes = v;
        }
        if let Some(v) = env_u64("BASIN_MEMTABLE_TABLE_CAP") {
            cfg.table_soft_cap_bytes = v;
        }
        if let Some(v) = env_u64("BASIN_MEMTABLE_MAX_AGE_SECS") {
            cfg.memtable_max_age_secs = v;
        }
        if let Some(v) = env_u64("BASIN_HOTTIER_RETAIN_SECS") {
            cfg.retain_secs = v;
        }
        if let Some(v) = env_u64("BASIN_HOTTIER_RETAIN_CAP") {
            cfg.project_clean_cap_bytes = v;
        }
        if let Ok(s) = std::env::var("BASIN_MEMTABLE_FLUSH_POLICY") {
            match s.to_ascii_lowercase().as_str() {
                "largest_first" | "largestfirst" => {
                    cfg.flush_policy = FlushPolicy::LargestFirst;
                }
                "lru" => {
                    // FlushPolicy::Lru is not implemented (no last_write_at in
                    // the scheduler interface). Reject loudly and keep the
                    // default so the process starts safely.
                    tracing::warn!(
                        "BASIN_MEMTABLE_FLUSH_POLICY=lru is not yet implemented \
                         (flush_policy=lru not supported; use largest_first). \
                         Falling back to largest_first."
                    );
                }
                other => {
                    tracing::warn!(
                        "BASIN_MEMTABLE_FLUSH_POLICY={other:?} not recognised; \
                         using default (largest_first)"
                    );
                }
            }
        }

        cfg
    }

    /// Validate that `bytes` is a legal project hard cap value: between
    /// [`MIN_HARD_CAP_BYTES`] (1 MiB) and [`MAX_HARD_CAP_BYTES`] (64 GiB)
    /// inclusive.
    ///
    /// Returns `Err` with a human-readable message if the value is out of
    /// range. Used by the `ALTER PROJECT` DDL handler.
    pub fn validate_hard_cap(bytes: u64) -> Result<(), String> {
        if bytes < MIN_HARD_CAP_BYTES {
            return Err(format!(
                "memtable_hard_cap {bytes} is below minimum (1 MiB = {MIN_HARD_CAP_BYTES})"
            ));
        }
        if bytes > MAX_HARD_CAP_BYTES {
            return Err(format!(
                "memtable_hard_cap {bytes} exceeds maximum (64 GiB = {MAX_HARD_CAP_BYTES})"
            ));
        }
        Ok(())
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn env_u64(key: &str) -> Option<u64> {
    let raw = std::env::var(key).ok()?;
    match raw.trim().parse::<u64>() {
        Ok(v) => Some(v),
        Err(_) => {
            tracing::warn!("{key}={raw:?} is not a valid u64; using default");
            None
        }
    }
}

// ── GlobalPressureScheduler ───────────────────────────────────────────────────

/// Default global process memory threshold for engaging largest-first flush
/// scheduling (4 GiB).
///
/// When the sum of all project `bytes_allocated` counters exceeds this value,
/// [`GlobalPressureScheduler::pick_flush_candidates`] activates and returns
/// projects in largest-first order so the flush task maximises memory reclaimed
/// per I/O round-trip.
pub const DEFAULT_GLOBAL_PRESSURE_THRESHOLD_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Stateless largest-first global flush scheduler.
///
/// # Purpose
///
/// When total process memory pressure crosses a configurable threshold the
/// scheduler ranks all active projects by their current `bytes_allocated` and
/// returns them largest-first so the caller (Phase 5.14.C4 flush task) can
/// reclaim memory efficiently.
///
/// # Design
///
/// The scheduler is stateless — it has no mutable fields. All inputs are
/// passed at call time, which makes it trivially testable without touching the
/// async runtime, the registry, or the flush channel.
///
/// # Semaphore integration
///
/// Each project's `ProjectMemState.hard_cap_sem` tracks the available headroom
/// below the hard cap. When a flush succeeds the flush task calls
/// `MemTableRegistry::release_bytes` which calls `Semaphore::add_permits`;
/// this unblocks any writer awaiting the semaphore in the hard-cap path.
///
/// The scheduler itself does **not** touch the semaphore — it only decides
/// *which* projects to schedule. Semaphore bookkeeping lives in
/// [`crate::registry::MemTableRegistry::release_bytes`].
#[derive(Debug, Clone)]
pub struct GlobalPressureScheduler {
    /// Total bytes across all projects that triggers largest-first scheduling.
    pub global_pressure_threshold_bytes: u64,
    /// Scheduling policy (largest-first by default).
    pub policy: FlushPolicy,
}

impl Default for GlobalPressureScheduler {
    fn default() -> Self {
        Self {
            global_pressure_threshold_bytes: DEFAULT_GLOBAL_PRESSURE_THRESHOLD_BYTES,
            policy: FlushPolicy::LargestFirst,
        }
    }
}

impl GlobalPressureScheduler {
    /// Construct a scheduler from a [`MemTableConfig`].
    ///
    /// Uses `config.flush_policy` and `DEFAULT_GLOBAL_PRESSURE_THRESHOLD_BYTES`.
    pub fn from_config(config: &MemTableConfig) -> Self {
        Self {
            global_pressure_threshold_bytes: DEFAULT_GLOBAL_PRESSURE_THRESHOLD_BYTES,
            policy: config.flush_policy,
        }
    }

    /// Decide which projects should be flushed next given the current per-project
    /// byte totals.
    ///
    /// # Arguments
    ///
    /// - `project_bytes`: slice of `(project_id, bytes_allocated)` pairs. The
    ///   caller should supply one entry per active project. The total sum of
    ///   `bytes_allocated` is used to determine whether global pressure applies.
    /// - `limit`: maximum number of candidates to return. Pass `usize::MAX` to
    ///   return all projects above the threshold.
    ///
    /// # Returns
    ///
    /// - `None` when total bytes <= `global_pressure_threshold_bytes` — global
    ///   pressure is not active; the caller should rely on per-project soft-cap
    ///   triggers instead.
    /// - `Some(Vec<ProjectId>)` (largest-first by default) when global pressure
    ///   is active. The vec is sorted descending by bytes allocated; ties are
    ///   broken arbitrarily (not guaranteed to be stable across calls).
    ///
    /// # Panics
    ///
    /// Panics if `self.policy == FlushPolicy::Lru`. That variant requires
    /// per-project `last_write_at` timestamps which this interface does not
    /// carry. Use [`FlushPolicy::LargestFirst`] until LRU is wired.
    ///
    /// # Complexity
    ///
    /// O(n log n) on the number of active projects. At 10 000 active projects
    /// this is < 1 ms on modern hardware.
    pub fn pick_flush_candidates<P: Copy>(
        &self,
        project_bytes: &[(P, u64)],
        limit: usize,
    ) -> Option<Vec<P>> {
        let total: u64 = project_bytes.iter().map(|(_, b)| b).sum();
        if total <= self.global_pressure_threshold_bytes {
            return None;
        }

        let mut sorted: Vec<(P, u64)> = project_bytes.to_vec();
        match self.policy {
            FlushPolicy::LargestFirst => {
                // Largest bytes_allocated first — maximises reclaimed bytes per flush.
                sorted.sort_unstable_by(|a, b| b.1.cmp(&a.1));
            }
            FlushPolicy::Lru => {
                // FlushPolicy::Lru requires per-project last_write_at timestamps
                // which are not carried in the (project_id, bytes_allocated) slice.
                // Reaching this arm is a configuration bug — the from_env() parser
                // already warns and falls back to LargestFirst, so this path should
                // be unreachable in production.
                panic!(
                    "FlushPolicy::Lru is not implemented: \
                     pick_flush_candidates requires last_write_at timestamps that \
                     are not present in the project_bytes slice. \
                     Use FlushPolicy::LargestFirst instead."
                );
            }
        }

        let candidates: Vec<P> = sorted.into_iter().take(limit).map(|(p, _)| p).collect();
        Some(candidates)
    }

    /// Returns `true` when the sum of `project_bytes` exceeds the global
    /// pressure threshold. A cheap O(n) predicate for callers that only need
    /// to know whether to invoke the scheduler.
    pub fn is_under_pressure(&self, project_bytes: &[(impl Copy, u64)]) -> bool {
        let total: u64 = project_bytes.iter().map(|(_, b)| b).sum();
        total > self.global_pressure_threshold_bytes
    }
}

// ── parse helpers (used by ALTER PROJECT DDL) ─────────────────────────────────

/// Parse a human-readable byte size string into a `u64`.
///
/// Accepted forms (case-insensitive):
/// - Bare integer: `"268435456"` → 268 435 456
/// - `NMB` / `N MB`: `"256MB"` → 268 435 456
/// - `NGB` / `N GB`: `"1GB"` → 1 073 741 824
/// - `NKB` / `N KB`: `"512KB"` → 524 288
///
/// Returns `None` when the string cannot be parsed.
pub fn parse_byte_size(s: &str) -> Option<u64> {
    let s = s.trim();
    // Try bare integer first.
    if let Ok(n) = s.parse::<u64>() {
        return Some(n);
    }
    let upper = s.to_ascii_uppercase();
    // Strip optional whitespace between number and suffix.
    let (num_part, suffix) = if let Some(pos) = upper.find(|c: char| c.is_alphabetic()) {
        let (n, suf) = upper.split_at(pos);
        (n.trim(), suf.trim())
    } else {
        return None;
    };
    let base: u64 = num_part.trim().parse().ok()?;
    match suffix {
        "KB" | "KIB" => Some(base * 1024),
        "MB" | "MIB" => Some(base * 1024 * 1024),
        "GB" | "GIB" => Some(base * 1024 * 1024 * 1024),
        "TB" | "TIB" => Some(base * 1024 * 1024 * 1024 * 1024),
        _ => None,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── GlobalPressureScheduler tests ─────────────────────────────────────────

    #[test]
    fn scheduler_returns_none_below_threshold() {
        let sched = GlobalPressureScheduler::default();
        // 3 GiB total — below 4 GiB threshold.
        let projects = vec![
            (1u64, 1 * 1024 * 1024 * 1024u64),
            (2u64, 1 * 1024 * 1024 * 1024),
            (3u64, 1 * 1024 * 1024 * 1024),
        ];
        assert!(sched.pick_flush_candidates(&projects, usize::MAX).is_none());
    }

    #[test]
    fn scheduler_returns_largest_first_above_threshold() {
        let sched = GlobalPressureScheduler::default();
        // 5 GiB total — above 4 GiB threshold.
        let projects = vec![
            (1u64, 1 * 1024 * 1024 * 1024u64), // 1 GiB
            (2u64, 2 * 1024 * 1024 * 1024),    // 2 GiB (largest)
            (3u64, 2 * 1024 * 1024 * 1024),    // 2 GiB
        ];
        let candidates = sched
            .pick_flush_candidates(&projects, usize::MAX)
            .expect("must return candidates under pressure");
        assert_eq!(candidates.len(), 3);
        // First element must be one of the 2 GiB projects (2 or 3).
        assert!(
            candidates[0] == 2 || candidates[0] == 3,
            "first candidate must be one of the largest projects"
        );
        // Last element must be the 1 GiB project.
        assert_eq!(candidates[2], 1, "smallest project must come last");
    }

    #[test]
    fn scheduler_respects_limit() {
        let sched = GlobalPressureScheduler::default();
        // 5 GiB total — above threshold.
        let projects: Vec<(u64, u64)> = (0..10)
            .map(|i| (i, (i + 1) * 512 * 1024 * 1024))
            .collect();
        let candidates = sched
            .pick_flush_candidates(&projects, 3)
            .expect("must return candidates under pressure");
        assert_eq!(candidates.len(), 3, "limit must be respected");
    }

    #[test]
    fn scheduler_is_under_pressure_matches_pick() {
        let sched = GlobalPressureScheduler::default();
        // Exactly at threshold — not under pressure.
        let at_threshold = vec![(1u64, DEFAULT_GLOBAL_PRESSURE_THRESHOLD_BYTES)];
        assert!(!sched.is_under_pressure(&at_threshold));
        assert!(sched.pick_flush_candidates(&at_threshold, usize::MAX).is_none());

        // One byte over — under pressure.
        let over_threshold = vec![(1u64, DEFAULT_GLOBAL_PRESSURE_THRESHOLD_BYTES + 1)];
        assert!(sched.is_under_pressure(&over_threshold));
        assert!(sched
            .pick_flush_candidates(&over_threshold, usize::MAX)
            .is_some());
    }

    #[test]
    fn scheduler_from_config_uses_flush_policy() {
        let cfg = MemTableConfig {
            flush_policy: FlushPolicy::Lru,
            ..MemTableConfig::default()
        };
        let sched = GlobalPressureScheduler::from_config(&cfg);
        assert_eq!(sched.policy, FlushPolicy::Lru);
    }

    /// `FlushPolicy::Lru` panics when `pick_flush_candidates` is called because
    /// the scheduler interface carries no `last_write_at` timestamps (#54 P1).
    #[test]
    #[should_panic(expected = "FlushPolicy::Lru is not implemented")]
    fn scheduler_lru_pick_panics_not_silent() {
        // Build a scheduler explicitly configured with Lru so we can verify the
        // panic — this bypasses from_env which already rejects "lru".
        let sched = GlobalPressureScheduler {
            global_pressure_threshold_bytes: 0, // threshold=0 ensures pressure is always active
            policy: FlushPolicy::Lru,
        };
        // 1 byte is enough to exceed the 0-byte threshold and reach the match arm.
        let projects = vec![(1u64, 1u64)];
        // This must panic, not silently fall through to LargestFirst.
        let _ = sched.pick_flush_candidates(&projects, usize::MAX);
    }

    /// `from_env` with `BASIN_MEMTABLE_FLUSH_POLICY=lru` must warn and fall
    /// back to `LargestFirst`, never silently install `Lru` (#54 P1).
    #[test]
    fn from_env_lru_falls_back_to_largest_first() {
        // Temporarily set the env var for this test only.
        std::env::set_var("BASIN_MEMTABLE_FLUSH_POLICY", "lru");
        let cfg = MemTableConfig::from_env();
        std::env::remove_var("BASIN_MEMTABLE_FLUSH_POLICY");
        assert_eq!(
            cfg.flush_policy,
            FlushPolicy::LargestFirst,
            "from_env must not install FlushPolicy::Lru — it should warn and fall back"
        );
    }

    #[test]
    fn scheduler_empty_project_list_returns_none() {
        let sched = GlobalPressureScheduler::default();
        let empty: Vec<(u64, u64)> = vec![];
        assert!(sched.pick_flush_candidates(&empty, usize::MAX).is_none());
    }

    // ── MemTableConfig tests ──────────────────────────────────────────────────

    #[test]
    fn default_values_match_adr_0016() {
        let cfg = MemTableConfig::default();
        assert_eq!(cfg.project_hard_cap_bytes, 256 * 1024 * 1024);
        assert_eq!(cfg.project_soft_cap_bytes, 192 * 1024 * 1024);
        assert_eq!(cfg.table_soft_cap_bytes, 16 * 1024 * 1024);
        assert_eq!(cfg.memtable_max_age_secs, 60);
        assert_eq!(cfg.flush_policy, FlushPolicy::LargestFirst);
        assert_eq!(cfg.retain_secs, 300);
        assert_eq!(cfg.project_clean_cap_bytes, 32 * 1024 * 1024);
    }

    #[test]
    fn from_env_reads_retention_overrides() {
        std::env::set_var("BASIN_HOTTIER_RETAIN_SECS", "0");
        std::env::set_var("BASIN_HOTTIER_RETAIN_CAP", "1048576");
        let cfg = MemTableConfig::from_env();
        std::env::remove_var("BASIN_HOTTIER_RETAIN_SECS");
        std::env::remove_var("BASIN_HOTTIER_RETAIN_CAP");
        assert_eq!(cfg.retain_secs, 0, "retain_secs=0 is the kill switch");
        assert_eq!(cfg.project_clean_cap_bytes, 1024 * 1024);
    }

    #[test]
    fn validate_hard_cap_rejects_too_small() {
        assert!(MemTableConfig::validate_hard_cap(0).is_err());
        assert!(MemTableConfig::validate_hard_cap(1024 * 1024 - 1).is_err());
    }

    #[test]
    fn validate_hard_cap_accepts_boundaries() {
        assert!(MemTableConfig::validate_hard_cap(MIN_HARD_CAP_BYTES).is_ok());
        assert!(MemTableConfig::validate_hard_cap(MAX_HARD_CAP_BYTES).is_ok());
    }

    #[test]
    fn validate_hard_cap_rejects_too_large() {
        assert!(MemTableConfig::validate_hard_cap(MAX_HARD_CAP_BYTES + 1).is_err());
    }

    #[test]
    fn parse_byte_size_bare_integer() {
        assert_eq!(parse_byte_size("268435456"), Some(268_435_456));
    }

    #[test]
    fn parse_byte_size_mb() {
        assert_eq!(parse_byte_size("256MB"), Some(256 * 1024 * 1024));
        assert_eq!(parse_byte_size("256 MB"), Some(256 * 1024 * 1024));
        assert_eq!(parse_byte_size("256mb"), Some(256 * 1024 * 1024));
    }

    #[test]
    fn parse_byte_size_gb() {
        assert_eq!(parse_byte_size("1GB"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_byte_size("2 GB"), Some(2 * 1024 * 1024 * 1024));
    }

    #[test]
    fn parse_byte_size_kb() {
        assert_eq!(parse_byte_size("512KB"), Some(512 * 1024));
    }

    #[test]
    fn parse_byte_size_invalid() {
        assert_eq!(parse_byte_size("abc"), None);
        assert_eq!(parse_byte_size(""), None);
    }
}
