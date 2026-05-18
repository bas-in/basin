//! Project -> shard endpoint mapping (consistent-ish hashing + whale pins).
//!
//! The router holds a `ShardMap` whenever it's running in compute-sharded
//! mode. For every authenticated `ProjectId` we compute a stable hash and
//! pick `endpoints[hash % endpoints.len()]`. The result is the address of
//! the shard-owner pgwire listener that owns that project's in-memory
//! state; the router proxies queries to it for the lifetime of the
//! connection.
//!
//! v0.1 deliberately uses plain modular hashing (rather than Karger-style
//! consistent hashing rings) because the shard count is fixed at startup
//! and resharding on the fly is out of scope for this milestone — the
//! point of the test is that *re-connections from the same project land
//! on the same shard*, not that we can add shards live.
//!
//! ## Whale pinning (Phase 5.5)
//!
//! Some projects are 100× the size of the median. Letting the
//! consistent-hash bucketize them is fine *until* it lands two whales on
//! the same shard. Pinning lets the operator say "this whale always goes
//! to endpoint #2" — typically a node provisioned with bigger compute,
//! still sharing the same object-store bucket.
//!
//! Source for pins in v0.1 is `BASIN_PROJECT_PINS`, a comma-separated
//! list of `project_ulid:endpoint_index` pairs. Empty/unset = no pins.
//! v0.2 will move pins into the catalog so they survive cluster restart
//! and can be edited at runtime.
//!
//! The hash function is `std::collections::hash_map::DefaultHasher` fed the
//! `ProjectId`'s ULID byte representation. That gives us a stable hash for
//! the lifetime of the process; what matters for the routing-stability
//! test is that two calls with the same `ProjectId` in the same process
//! produce the same shard. (DefaultHasher is randomly seeded per-process,
//! so cross-process the placement may differ — fine, since each router
//! constructs its own `ShardMap` from the same endpoint list and uses it
//! consistently for that process's lifetime.)

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use basin_common::ProjectId;

/// Maps `ProjectId -> shard endpoint`. Cheap to clone.
#[derive(Clone, Debug)]
pub struct ShardMap {
    endpoints: Vec<String>,
    pins: HashMap<ProjectId, usize>,
}

impl ShardMap {
    /// Build a `ShardMap` over the given endpoints. The endpoint vector
    /// must be non-empty; the router guards on this before constructing.
    pub fn new(endpoints: Vec<String>) -> Self {
        assert!(
            !endpoints.is_empty(),
            "ShardMap requires at least one endpoint",
        );
        Self {
            endpoints,
            pins: HashMap::new(),
        }
    }

    /// Build a `ShardMap` over the given endpoints with a set of whale
    /// pins. Each pin's endpoint index must be `< endpoints.len()`; an
    /// out-of-range pin is rejected at construction so the operator
    /// finds out at startup, not on the whale's first connection.
    pub fn with_pins(
        endpoints: Vec<String>,
        pins: HashMap<ProjectId, usize>,
    ) -> Result<Self, String> {
        if endpoints.is_empty() {
            return Err("ShardMap requires at least one endpoint".to_string());
        }
        for (project, idx) in &pins {
            if *idx >= endpoints.len() {
                return Err(format!(
                    "pin for project {project} points at endpoint index {idx} \
                     but only {} endpoints are configured",
                    endpoints.len()
                ));
            }
        }
        Ok(Self { endpoints, pins })
    }

    /// Pick the endpoint that owns `project`. Stable for the lifetime of
    /// this `ShardMap`. If the project is pinned, the pinned endpoint
    /// wins; otherwise consistent-hash modulo endpoint count.
    pub fn shard_for(&self, project: &ProjectId) -> &str {
        let idx = self.shard_index(project);
        &self.endpoints[idx]
    }

    /// Endpoint slice, in declaration order. Tests use this to match a
    /// reported shard counter back to the endpoint it came from.
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    /// 0-based index of the shard that owns `project`. Pinned projects
    /// short-circuit; everyone else hashes their ULID. Pulled out of
    /// `shard_for` so tests can sanity-check the bucketing without
    /// having to round-trip a `&str` to an index lookup.
    pub fn shard_index(&self, project: &ProjectId) -> usize {
        if let Some(idx) = self.pins.get(project) {
            return *idx;
        }
        let mut h = std::collections::hash_map::DefaultHasher::new();
        project.as_ulid().0.hash(&mut h);
        let v = h.finish();
        (v as usize) % self.endpoints.len()
    }

    /// True if this project has an explicit pin (vs falling through to
    /// the consistent-hash path). Useful for telemetry / log correlation.
    pub fn is_pinned(&self, project: &ProjectId) -> bool {
        self.pins.contains_key(project)
    }

    /// Number of distinct pinned projects. Surfaced for startup logs so
    /// the operator can confirm the env var was parsed correctly.
    pub fn pin_count(&self) -> usize {
        self.pins.len()
    }
}

/// Parse the `BASIN_PROJECT_PINS` env-var format into a `(ProjectId, usize)`
/// map. Format: `ulid:idx,ulid:idx`. Whitespace around commas / colons is
/// trimmed. An empty string returns an empty map (legitimately "no pins").
///
/// Errors are returned as `String` so the caller can decide whether a
/// malformed pin is a hard startup failure (current behavior) or a
/// `tracing::warn!` + degrade-to-no-pins.
pub fn parse_pins_env(s: &str) -> Result<HashMap<ProjectId, usize>, String> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(HashMap::new());
    }
    let mut out = HashMap::new();
    for entry in s.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (project_str, idx_str) = entry
            .split_once(':')
            .ok_or_else(|| format!("pin entry {entry:?} missing ':' separator"))?;
        let project = ProjectId::from_str(project_str.trim()).map_err(|e| {
            format!("pin entry {entry:?}: invalid project ulid {project_str:?}: {e}")
        })?;
        let idx: usize = idx_str
            .trim()
            .parse()
            .map_err(|e| format!("pin entry {entry:?}: invalid endpoint index {idx_str:?}: {e}"))?;
        out.insert(project, idx);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoints(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("127.0.0.1:{}", 5500 + i)).collect()
    }

    #[test]
    fn shard_for_is_stable_within_process() {
        let map = ShardMap::new(endpoints(4));
        let t = ProjectId::new();
        let first = map.shard_for(&t).to_owned();
        for _ in 0..16 {
            assert_eq!(map.shard_for(&t), first);
        }
    }

    #[test]
    fn shard_for_distributes_across_endpoints() {
        // Hash a hundred random projects into 4 shards and assert no shard
        // got more than 60% of them. With 100 random ULIDs and 4 buckets
        // the expected fraction is 25%; 60% is wide enough that the test
        // is not flaky but tight enough that a totally degenerate hash
        // (everything to one shard) fails it.
        let map = ShardMap::new(endpoints(4));
        let mut counts = vec![0usize; 4];
        for _ in 0..100 {
            let t = ProjectId::new();
            counts[map.shard_index(&t)] += 1;
        }
        let max = *counts.iter().max().unwrap();
        assert!(max < 60, "shard distribution degenerate: {counts:?}");
    }

    #[test]
    fn endpoints_round_trips() {
        let e = endpoints(3);
        let map = ShardMap::new(e.clone());
        assert_eq!(map.endpoints(), &e[..]);
    }

    #[test]
    #[should_panic(expected = "at least one endpoint")]
    fn empty_endpoints_rejected() {
        ShardMap::new(Vec::new());
    }

    #[test]
    fn pinned_project_lands_on_pinned_endpoint() {
        let whale = ProjectId::new();
        let mut pins = HashMap::new();
        pins.insert(whale, 2);
        let map = ShardMap::with_pins(endpoints(4), pins).unwrap();
        assert_eq!(map.shard_index(&whale), 2);
        assert_eq!(map.shard_for(&whale), "127.0.0.1:5502");
        assert!(map.is_pinned(&whale));
        assert_eq!(map.pin_count(), 1);
    }

    #[test]
    fn pinned_project_overrides_what_hash_would_pick() {
        // Construct a project, see which shard the hash would assign,
        // then pin them somewhere else and assert the pin wins.
        let map_unpinned = ShardMap::new(endpoints(4));
        let t = ProjectId::new();
        let hashed = map_unpinned.shard_index(&t);
        let pin_target = (hashed + 1) % 4;
        let mut pins = HashMap::new();
        pins.insert(t, pin_target);
        let map = ShardMap::with_pins(endpoints(4), pins).unwrap();
        assert_eq!(map.shard_index(&t), pin_target);
        assert_ne!(map.shard_index(&t), hashed, "pin must override hash");
    }

    #[test]
    fn unpinned_projects_still_use_hash_path() {
        let whale = ProjectId::new();
        let other = ProjectId::new();
        let mut pins = HashMap::new();
        pins.insert(whale, 0);
        let map = ShardMap::with_pins(endpoints(4), pins).unwrap();

        assert!(map.is_pinned(&whale));
        assert!(!map.is_pinned(&other));

        // The unpinned project's index must match what the bare hash gives
        // — pinning one project must not perturb anyone else.
        let map_no_pins = ShardMap::new(endpoints(4));
        assert_eq!(map.shard_index(&other), map_no_pins.shard_index(&other));
    }

    #[test]
    fn out_of_range_pin_rejected_at_construction() {
        let t = ProjectId::new();
        let mut pins = HashMap::new();
        pins.insert(t, 99);
        let err = ShardMap::with_pins(endpoints(4), pins).unwrap_err();
        assert!(err.contains("99") && err.contains("4"), "got {err}");
    }

    #[test]
    fn empty_endpoints_with_pins_rejected() {
        let err = ShardMap::with_pins(Vec::new(), HashMap::new()).unwrap_err();
        assert!(err.contains("at least one"), "got {err}");
    }

    #[test]
    fn parse_pins_env_empty_is_ok() {
        assert!(parse_pins_env("").unwrap().is_empty());
        assert!(parse_pins_env("   ").unwrap().is_empty());
    }

    #[test]
    fn parse_pins_env_single_entry() {
        let t = ProjectId::new();
        let s = format!("{}:3", t.as_ulid());
        let pins = parse_pins_env(&s).unwrap();
        assert_eq!(pins.len(), 1);
        assert_eq!(pins.get(&t), Some(&3));
    }

    #[test]
    fn parse_pins_env_multiple_entries_with_whitespace() {
        let a = ProjectId::new();
        let b = ProjectId::new();
        let s = format!(" {} : 1 , {} : 2 ", a.as_ulid(), b.as_ulid());
        let pins = parse_pins_env(&s).unwrap();
        assert_eq!(pins.len(), 2);
        assert_eq!(pins.get(&a), Some(&1));
        assert_eq!(pins.get(&b), Some(&2));
    }

    #[test]
    fn parse_pins_env_skips_empty_segments() {
        let t = ProjectId::new();
        let s = format!(",,{}:0,,", t.as_ulid());
        let pins = parse_pins_env(&s).unwrap();
        assert_eq!(pins.len(), 1);
        assert_eq!(pins.get(&t), Some(&0));
    }

    #[test]
    fn parse_pins_env_rejects_missing_colon() {
        let err = parse_pins_env("nocolon").unwrap_err();
        assert!(err.contains("missing ':'"), "got {err}");
    }

    #[test]
    fn parse_pins_env_rejects_bad_ulid() {
        let err = parse_pins_env("notaulid:0").unwrap_err();
        assert!(err.contains("invalid project ulid"), "got {err}");
    }

    #[test]
    fn parse_pins_env_rejects_bad_index() {
        let t = ProjectId::new();
        let err = parse_pins_env(&format!("{}:notanumber", t.as_ulid())).unwrap_err();
        assert!(err.contains("invalid endpoint index"), "got {err}");
    }
}
