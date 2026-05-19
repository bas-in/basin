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

/// Minimum allowed hard cap (1 MiB). Enforced by the `ALTER PROJECT` parser.
pub const MIN_HARD_CAP_BYTES: u64 = 1024 * 1024;

/// Maximum allowed hard cap (64 GiB). Enforced by the `ALTER PROJECT` parser.
pub const MAX_HARD_CAP_BYTES: u64 = 64 * 1024 * 1024 * 1024;

// ── FlushPolicy ───────────────────────────────────────────────────────────────

/// Scheduling algorithm for the flush task when global memory pressure rises.
///
/// `LargestFirst` is the default and matches ADR 0016. `Lru` is a stub
/// reserved for future use as a configurable alternative; it is not yet wired
/// into the flush executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FlushPolicy {
    /// Flush the project (or table) with the most bytes allocated first.
    /// Maximises memory reclaimed per flush I/O round-trip.
    #[default]
    LargestFirst,
    /// Flush the project (or table) whose last flush was the longest ago.
    /// Stub — not yet wired. Penalises write-light projects; documented
    /// as non-default per ADR 0016.
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
}

impl Default for MemTableConfig {
    fn default() -> Self {
        Self {
            project_hard_cap_bytes: DEFAULT_PROJECT_HARD_CAP_BYTES,
            project_soft_cap_bytes: DEFAULT_PROJECT_SOFT_CAP_BYTES,
            table_soft_cap_bytes: DEFAULT_TABLE_SOFT_CAP_BYTES,
            memtable_max_age_secs: DEFAULT_MEMTABLE_MAX_AGE_SECS,
            flush_policy: FlushPolicy::LargestFirst,
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
        if let Ok(s) = std::env::var("BASIN_MEMTABLE_FLUSH_POLICY") {
            match s.to_ascii_lowercase().as_str() {
                "largest_first" | "largestfirst" => {
                    cfg.flush_policy = FlushPolicy::LargestFirst;
                }
                "lru" => {
                    cfg.flush_policy = FlushPolicy::Lru;
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

    #[test]
    fn default_values_match_adr_0016() {
        let cfg = MemTableConfig::default();
        assert_eq!(cfg.project_hard_cap_bytes, 256 * 1024 * 1024);
        assert_eq!(cfg.project_soft_cap_bytes, 192 * 1024 * 1024);
        assert_eq!(cfg.table_soft_cap_bytes, 16 * 1024 * 1024);
        assert_eq!(cfg.memtable_max_age_secs, 60);
        assert_eq!(cfg.flush_policy, FlushPolicy::LargestFirst);
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
