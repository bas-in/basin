//! Process-level region identity.
//!
//! Multi-region Basin (ADR 0009) runs one independent deployment per region:
//! a per-region Raft WAL, per-region shard owners, and a per-region object
//! store that S3 cross-region replication (CRR) keeps in sync at the bucket
//! layer. Every Basin process therefore belongs to exactly one region, named
//! by the `BASIN_REGION` environment variable.
//!
//! ADR 0009's scaffolding promised a "stable accessor (`local_region()`)" for
//! any layer that needs to make a regional decision, but never actually
//! shipped one — `BASIN_REGION` was only read ad hoc in `basin-server` for a
//! startup log line. This module is that accessor. It lives in `basin-common`
//! (not `basin-server`) because the *engine* and the *router* — not just the
//! process entrypoint — now branch on it: the engine refuses a write to a
//! project homed in another region (`BasinError::WrongRegion`), and the router
//! renders that refusal. A single source of truth keeps the comparison
//! `home_region == local_region()` consistent across crates.
//!
//! ## Default
//!
//! Unset / empty `BASIN_REGION` resolves to [`DEFAULT_REGION`] (`"local"`),
//! exactly the ADR 0009 default. A single-region deployment that never sets
//! the variable therefore reports `"local"` everywhere, and a legacy project
//! whose `home_region` is `None` is treated as "homed in the local region"
//! (see [`is_home_region`]) — so nothing rejects until an operator opts into
//! multi-region by setting distinct `BASIN_REGION` values per deployment AND
//! pinning projects to specific regions.

use std::sync::OnceLock;

/// The region name a Basin process reports when `BASIN_REGION` is unset or
/// empty. Matches the ADR 0009 scaffolding default.
pub const DEFAULT_REGION: &str = "local";

/// The environment variable naming this process's region.
pub const REGION_ENV: &str = "BASIN_REGION";

/// Process-wide cache. `BASIN_REGION` is read exactly once, the first time
/// [`local_region`] is called, and is immutable for the life of the process —
/// the region a deployment runs in does not change without a restart, and
/// caching keeps the accessor allocation-free on the hot write-gate path.
static LOCAL_REGION: OnceLock<String> = OnceLock::new();

/// This process's region name.
///
/// Reads `BASIN_REGION` once (trimmed); an unset or empty value resolves to
/// [`DEFAULT_REGION`]. The value is cached for the life of the process, so
/// later mutations of the environment do not change it — region identity is
/// fixed at deployment, not per-call.
///
/// Returns `&'static str` so callers can compare and embed it in error
/// messages without cloning.
pub fn local_region() -> &'static str {
    LOCAL_REGION
        .get_or_init(|| {
            std::env::var(REGION_ENV)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| DEFAULT_REGION.to_string())
        })
        .as_str()
}

/// True when `home_region` names the region this process runs in.
///
/// Legacy-tolerant: a `None` home (a project catalogued before per-project
/// homing existed, or one never pinned) is treated as **homed locally** — it
/// can be written and read from any region, preserving single-region
/// behaviour. Once a project is pinned (`Some(region)`), only the matching
/// region is its home.
///
/// The comparison is exact-string (case-sensitive), matching how `BASIN_REGION`
/// and the catalogued `home_region` are both stored verbatim; operators must
/// use identical spellings (`"us-east-1"`, not `"US-East-1"`) across a
/// deployment, the same discipline ADR 0009 assumes for the env var.
pub fn is_home_region(home_region: Option<&str>) -> bool {
    match home_region {
        None => true,
        Some(home) => home == local_region(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_home_is_always_local() {
        // A legacy / unpinned project (home_region == None) is home everywhere.
        assert!(is_home_region(None));
    }

    #[test]
    fn matching_home_is_local() {
        // Whatever this process resolves to, a project pinned to the same
        // string is at home. Independent of the env value so the test is
        // stable regardless of how the harness sets BASIN_REGION.
        let here = local_region();
        assert!(is_home_region(Some(here)));
    }

    #[test]
    fn different_home_is_not_local() {
        // A region name guaranteed distinct from the local one.
        let foreign = format!("{}__definitely_not_here", local_region());
        assert!(!is_home_region(Some(&foreign)));
    }

    #[test]
    fn local_region_is_stable_across_calls() {
        // OnceLock cache: two reads return the identical pointer.
        let a = local_region();
        let b = local_region();
        assert_eq!(a, b);
        assert!(std::ptr::eq(a.as_ptr(), b.as_ptr()));
    }

    #[test]
    fn default_is_local_string() {
        assert_eq!(DEFAULT_REGION, "local");
    }
}
