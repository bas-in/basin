//! Project -> shard endpoint mapping.
//!
//! The router holds a [`ShardMap`] whenever it's running in compute-sharded
//! mode. The map exposes two routing modes, selected per call:
//!
//! 1. **Legacy hash** (`shard_for(project)`): consistent-ish hashing of
//!    `ProjectId` to one of the configured endpoints, plus whale pins. This
//!    is the pre-ADR-0023 byte-equivalent path; still the default when no
//!    [`LeaseRegistry`] is wired in, and still the only path callers that
//!    only have a `ProjectId` use.
//!
//! 2. **Partition-aware lease lookup** ([`LeaseAwareShardMap::owner_for`]):
//!    consults a [`basin_catalog::LeaseRegistry`] for the current owner of
//!    `(project, partition)`. This is the Phase 6.X.B path (ADR 0023) that
//!    lets a project's partitions distribute across replicas. Behaviour for
//!    the 1-partition default project is *byte-equivalent* to `shard_for`
//!    when no replica has yet acquired the lease — the local replica tries
//!    to acquire (v1 rule: any replica can become owner if free) and from
//!    then on every router sees the lease through the catalog.
//!
//! ## Cache
//!
//! Lease lookups are cached for [`DEFAULT_LEASE_CACHE_TTL`] (overridable
//! via `BASIN_ROUTER_LEASE_CACHE_TTL_MS`, default 5000). On miss, the
//! router does one indexed lookup against the catalog's lease table; if
//! no live lease exists, the local replica tries to `acquire` it. The
//! result is cached. Stale cache entries are safe — at worst the router
//! routes one round trip to a peer that has to reject the proxy and the
//! cache re-fills; correctness is preserved by the WAL epoch fence
//! (Phase 6.X.A).
//!
//! ## Whale pinning (Phase 5.5)
//!
//! Pins apply at the `shard_for` (legacy) layer; the partition-aware lease
//! layer naturally distributes a partitioned project across replicas, so
//! whale pinning is the small-N path while partition DDL is the proper
//! horizontal scale-out path (`ALTER PROJECT … SET partitions = N`).
//!
//! Source for pins in v0.1 is `BASIN_PROJECT_PINS`, a comma-separated
//! list of `project_ulid:endpoint_index` pairs. Empty/unset = no pins.
//!
//! The hash function is `std::collections::hash_map::DefaultHasher` fed the
//! `ProjectId`'s ULID byte representation (plus the partition id for the
//! partition-aware bootstrap path). DefaultHasher is randomly seeded per
//! process, so cross-process placement may differ. Within a process it is
//! stable, which is all the routing-stability test requires.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::LeaseRegistry;
use basin_common::ProjectId;
use tokio::sync::Mutex;

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

/// Default TTL for the router's lease-lookup cache. ADR 0023 fixes the
/// lease itself at 15 s (5 s renew); the router caches the lookup at a
/// strict subset of that so a stolen lease is observed within one
/// heartbeat window.
pub const DEFAULT_LEASE_CACHE_TTL: Duration = Duration::from_millis(5_000);

/// Lease-aware router. Wraps a [`ShardMap`] (for the no-lease back-compat
/// path) plus an optional [`LeaseRegistry`] (for partition-aware routing).
///
/// When `registry` is `None`, [`Self::owner_for`] is byte-equivalent to
/// [`ShardMap::shard_for`] regardless of partition — this is the
/// 1-partition default project path that preserves existing behaviour
/// without any lease-table dependency.
///
/// When `registry` is `Some`, [`Self::owner_for`] consults the lease
/// table. Hits are cached for `cache_ttl`; misses ask the registry and,
/// if no live lease exists, the local replica tries to acquire it
/// (v1 "any replica can become owner if free" rule from ADR 0023).
#[derive(Clone)]
pub struct LeaseAwareShardMap {
    inner: ShardMap,
    registry: Option<Arc<dyn LeaseRegistry>>,
    /// Identity this replica uses when acquiring leases on cache miss.
    /// Empty / not-applicable in no-registry mode. Operators wire this to
    /// the local pgwire endpoint string (`"host:port"`) so the holder
    /// identity round-trips through the lease table back to a routable
    /// endpoint at every other replica's router.
    replica_id: String,
    cache: Arc<Mutex<HashMap<(ProjectId, String), CachedOwner>>>,
    cache_ttl: Duration,
    /// Lease TTL used when this replica tries to acquire on a miss.
    /// Mirrors [`basin_catalog::DEFAULT_LEASE_TTL_SECS`]; configurable per
    /// construction so a test can shorten it without touching the global
    /// default.
    acquire_ttl: Duration,
}

#[derive(Clone, Copy, Debug)]
struct CachedOwner {
    endpoint_idx: usize,
    cached_at: Instant,
}

impl LeaseAwareShardMap {
    /// Construct a lease-aware map. `registry = None` is the back-compat
    /// path: `owner_for` collapses to the underlying `shard_for(project)`
    /// for every partition.
    pub fn new(
        inner: ShardMap,
        registry: Option<Arc<dyn LeaseRegistry>>,
        replica_id: impl Into<String>,
        cache_ttl: Duration,
        acquire_ttl: Duration,
    ) -> Self {
        Self {
            inner,
            registry,
            replica_id: replica_id.into(),
            cache: Arc::new(Mutex::new(HashMap::new())),
            cache_ttl,
            acquire_ttl,
        }
    }

    /// Construct a lease-aware map with the ADR 0023 defaults: 5 s cache
    /// TTL and 15 s lease acquire TTL.
    pub fn with_defaults(
        inner: ShardMap,
        registry: Option<Arc<dyn LeaseRegistry>>,
        replica_id: impl Into<String>,
    ) -> Self {
        Self::new(
            inner,
            registry,
            replica_id,
            DEFAULT_LEASE_CACHE_TTL,
            Duration::from_secs(basin_catalog::DEFAULT_LEASE_TTL_SECS),
        )
    }

    /// Underlying [`ShardMap`]. Exposed so callers that still only have a
    /// `ProjectId` can fall through to the legacy `shard_for` directly.
    pub fn inner(&self) -> &ShardMap {
        &self.inner
    }

    /// Resolve the owning endpoint for `(project, partition_id)`.
    ///
    /// Lookup order:
    /// 1. **No-registry mode** (`registry == None`): legacy
    ///    [`ShardMap::shard_for`]. Byte-equivalent to pre-6.X.B.
    /// 2. **Registry mode, cache hit, fresh**: return cached endpoint.
    /// 3. **Registry mode, cache miss / stale**:
    ///    - Ask the registry for the current owner.
    ///    - If a live lease exists, map `holder` → endpoint index and cache.
    ///    - If no live lease, the local replica tries to acquire. On
    ///      success the local replica becomes the owner; on lose-race
    ///      re-poll the registry for the new owner.
    ///
    /// Returns the owning endpoint string (cloned). On any catalog error
    /// the call degrades to the legacy hash so the data path stays open;
    /// the WAL epoch fence (Phase 6.X.A) catches a stale routing decision
    /// at append time.
    pub async fn owner_for(&self, project: &ProjectId, partition_id: &str) -> String {
        // Path 1: no registry → legacy hash. Used by the default 1-partition
        // back-compat configuration and by every test that doesn't wire a
        // catalog into the router.
        let Some(registry) = self.registry.clone() else {
            return self.inner.shard_for(project).to_owned();
        };

        // Path 2: cache hit + fresh.
        let key = (*project, partition_id.to_owned());
        {
            let cache = self.cache.lock().await;
            if let Some(entry) = cache.get(&key) {
                if entry.cached_at.elapsed() < self.cache_ttl {
                    if let Some(ep) = self.inner.endpoints().get(entry.endpoint_idx) {
                        return ep.clone();
                    }
                }
            }
        }

        // Path 3: cache miss / stale → consult the registry.
        let endpoint_idx = self
            .resolve_via_registry(&*registry, project, partition_id)
            .await;
        {
            let mut cache = self.cache.lock().await;
            cache.insert(
                key,
                CachedOwner {
                    endpoint_idx,
                    cached_at: Instant::now(),
                },
            );
        }
        self.inner.endpoints()[endpoint_idx].clone()
    }

    /// Inner registry-resolution path. Returns the endpoint index. On any
    /// error returns the legacy hash so the caller stays available.
    async fn resolve_via_registry(
        &self,
        registry: &dyn LeaseRegistry,
        project: &ProjectId,
        partition_id: &str,
    ) -> usize {
        // Step A: is there a live owner already?
        if let Ok(Some((holder, _epoch))) = registry.owner_of(project, partition_id).await {
            if let Some(idx) = self.endpoint_index_for(&holder) {
                return idx;
            }
            // Holder identity doesn't map to a known endpoint (e.g. peer
            // we don't know about). Fall through to the legacy hash so we
            // still produce a routable answer.
            return self.hash_index(project, partition_id);
        }

        // Step B: no live lease. v1 rule (ADR 0023): any replica can try
        // to acquire. The local replica picks itself (via `replica_id`);
        // successful acquire makes us the owner. On lose-race we re-poll
        // and follow whoever did win.
        if !self.replica_id.is_empty() {
            match registry
                .acquire(project, partition_id, &self.replica_id, self.acquire_ttl)
                .await
            {
                Ok(Some(_lease)) => {
                    // We're the owner now. Map our replica_id → endpoint.
                    if let Some(idx) = self.endpoint_index_for(&self.replica_id) {
                        return idx;
                    }
                    // Self replica_id isn't a known endpoint string.
                    // Honour the lease but route via hash so the answer
                    // is still routable to some endpoint.
                    return self.hash_index(project, partition_id);
                }
                Ok(None) => {
                    // Lost the race — someone else just acquired. One
                    // more owner_of probe; if still nothing (rare TOCTOU
                    // gap), degrade to hash.
                    if let Ok(Some((holder, _))) =
                        registry.owner_of(project, partition_id).await
                    {
                        if let Some(idx) = self.endpoint_index_for(&holder) {
                            return idx;
                        }
                    }
                }
                Err(_e) => {
                    // Catalog error. Degrade to hash; WAL epoch fence
                    // catches a stale decision at append time.
                }
            }
        }

        self.hash_index(project, partition_id)
    }

    /// Hash `(project, partition_id)` to an endpoint index. Used as the
    /// bootstrap fallback before any lease is granted.
    fn hash_index(&self, project: &ProjectId, partition_id: &str) -> usize {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        project.as_ulid().0.hash(&mut h);
        partition_id.hash(&mut h);
        (h.finish() as usize) % self.inner.endpoints().len()
    }

    /// Find the endpoint index whose string matches `holder`. Replicas are
    /// identified by their pgwire endpoint string (`"host:port"`) so the
    /// holder identity round-trips through the lease table.
    fn endpoint_index_for(&self, holder: &str) -> Option<usize> {
        self.inner.endpoints().iter().position(|e| e == holder)
    }

    /// Test-only: drop the cache so a subsequent `owner_for` is forced to
    /// re-consult the registry.
    #[doc(hidden)]
    pub async fn clear_cache_for_test(&self) {
        self.cache.lock().await.clear();
    }
}

impl std::fmt::Debug for LeaseAwareShardMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseAwareShardMap")
            .field("inner", &self.inner)
            .field("registry", &self.registry.is_some())
            .field("replica_id", &self.replica_id)
            .field("cache_ttl", &self.cache_ttl)
            .field("acquire_ttl", &self.acquire_ttl)
            .finish()
    }
}

/// Parse the `BASIN_ROUTER_LEASE_CACHE_TTL_MS` env-var into a [`Duration`].
/// `None` / empty / unset returns [`DEFAULT_LEASE_CACHE_TTL`]; a bad value
/// is returned as `Err` so the operator finds out at startup, not on the
/// first cache miss.
pub fn parse_lease_cache_ttl_env(s: Option<&str>) -> Result<Duration, String> {
    let raw = s.map(|s| s.trim()).unwrap_or("");
    if raw.is_empty() {
        return Ok(DEFAULT_LEASE_CACHE_TTL);
    }
    let ms: u64 = raw
        .parse()
        .map_err(|e| format!("BASIN_ROUTER_LEASE_CACHE_TTL_MS={raw:?}: {e}"))?;
    Ok(Duration::from_millis(ms))
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

    // ── Phase 6.X.B — LeaseAwareShardMap tests ─────────────────────────────

    use basin_catalog::InMemoryCatalog;

    /// No-registry construction collapses to the legacy `shard_for` for
    /// every partition id (the 1-partition default project byte-equivalent
    /// back-compat path).
    #[tokio::test]
    async fn no_registry_collapses_to_legacy_shard_for() {
        let inner = ShardMap::new(endpoints(4));
        let lease_map =
            LeaseAwareShardMap::with_defaults(inner.clone(), None, "127.0.0.1:5500");
        let project = ProjectId::new();
        let expected = inner.shard_for(&project).to_owned();
        // Default partition.
        assert_eq!(lease_map.owner_for(&project, "_default").await, expected);
        // Any other partition id also routes to the same endpoint (no
        // partition awareness when no registry is configured).
        assert_eq!(
            lease_map.owner_for(&project, "anything-else").await,
            expected
        );
    }

    /// With a registry but no lease yet, the first call from the local
    /// replica acquires it; the lease table reflects the holder identity.
    #[tokio::test]
    async fn first_call_acquires_lease_for_local_replica() {
        let eps = endpoints(3);
        let inner = ShardMap::new(eps.clone());
        let cat: Arc<InMemoryCatalog> = Arc::new(InMemoryCatalog::new());
        let registry: Arc<dyn LeaseRegistry> = cat.clone();
        // Self replica identity == endpoint[1].
        let lease_map =
            LeaseAwareShardMap::with_defaults(inner, Some(registry.clone()), eps[1].clone());
        let project = ProjectId::new();
        let owner = lease_map.owner_for(&project, "_default").await;
        // Our replica acquired the lease, so we should route to ourselves.
        assert_eq!(owner, eps[1]);
        // Lease table now records us as the holder.
        let live = registry.owner_of(&project, "_default").await.unwrap();
        assert_eq!(live.unwrap().0, eps[1]);
    }

    /// When the lease is already held by a peer, the router returns that
    /// peer's endpoint (not the local replica's).
    #[tokio::test]
    async fn pre_held_lease_routes_to_existing_holder() {
        let eps = endpoints(3);
        let inner = ShardMap::new(eps.clone());
        let cat: Arc<InMemoryCatalog> = Arc::new(InMemoryCatalog::new());
        let registry: Arc<dyn LeaseRegistry> = cat.clone();
        let project = ProjectId::new();
        // Peer at endpoint[2] grabs the lease first.
        registry
            .acquire(&project, "_default", &eps[2], Duration::from_secs(30))
            .await
            .unwrap();
        // We are endpoint[0]. owner_for should report endpoint[2].
        let lease_map =
            LeaseAwareShardMap::with_defaults(inner, Some(registry), eps[0].clone());
        assert_eq!(lease_map.owner_for(&project, "_default").await, eps[2]);
    }

    /// Two partitions of the same project go through independent
    /// resolutions (different cache keys, independent lease rows).
    #[tokio::test]
    async fn partitions_are_independently_keyed() {
        let eps = endpoints(4);
        let inner = ShardMap::new(eps.clone());
        let cat: Arc<InMemoryCatalog> = Arc::new(InMemoryCatalog::new());
        let registry: Arc<dyn LeaseRegistry> = cat.clone();
        let lease_map =
            LeaseAwareShardMap::with_defaults(inner, Some(registry.clone()), eps[1].clone());
        let project = ProjectId::new();
        let o0 = lease_map.owner_for(&project, "0").await;
        let o1 = lease_map.owner_for(&project, "1").await;
        // Both acquired by us (no peer contending).
        assert_eq!(o0, eps[1]);
        assert_eq!(o1, eps[1]);
        // But the lease table has two distinct rows, one per partition.
        assert!(registry.owner_of(&project, "0").await.unwrap().is_some());
        assert!(registry.owner_of(&project, "1").await.unwrap().is_some());
    }

    /// Multi-partition + multi-replica hot-tenant pinning verification:
    /// 4 partitions, 4 replicas, each replica preempts a different
    /// partition. After resolution, all 4 partitions land on 4 different
    /// endpoints. This is the "whales partition across replicas"
    /// acceptance criterion from the task brief.
    #[tokio::test]
    async fn multi_partition_distributes_across_replicas() {
        let eps = endpoints(4);
        let cat: Arc<InMemoryCatalog> = Arc::new(InMemoryCatalog::new());
        let registry: Arc<dyn LeaseRegistry> = cat.clone();
        let project = ProjectId::new();

        // Each replica acquires a different partition first.
        for (i, ep) in eps.iter().enumerate() {
            registry
                .acquire(&project, &i.to_string(), ep, Duration::from_secs(30))
                .await
                .unwrap();
        }
        // A router on replica[0] then asks `owner_for` for each partition;
        // it should get the correct endpoint for each.
        let lease_map = LeaseAwareShardMap::with_defaults(
            ShardMap::new(eps.clone()),
            Some(registry),
            eps[0].clone(),
        );
        let mut seen = std::collections::HashSet::new();
        for i in 0..4 {
            let owner = lease_map.owner_for(&project, &i.to_string()).await;
            seen.insert(owner);
        }
        assert_eq!(
            seen.len(),
            4,
            "expected 4 distinct shard owners for 4 partitions, got {seen:?}"
        );
    }

    /// Cache hit avoids a registry round-trip: after the first lookup,
    /// the lease is dropped out of the registry but the cached answer
    /// still surfaces.
    #[tokio::test]
    async fn cache_hit_survives_lease_removal() {
        let eps = endpoints(3);
        let inner = ShardMap::new(eps.clone());
        let cat: Arc<InMemoryCatalog> = Arc::new(InMemoryCatalog::new());
        let registry: Arc<dyn LeaseRegistry> = cat.clone();
        let project = ProjectId::new();
        // Long cache TTL so the hit doesn't expire mid-test.
        let lease_map = LeaseAwareShardMap::new(
            inner,
            Some(registry.clone()),
            eps[2].clone(),
            Duration::from_secs(60),
            Duration::from_secs(30),
        );
        let first = lease_map.owner_for(&project, "_default").await;
        assert_eq!(first, eps[2]);
        // Wipe the lease table; the cache should still answer.
        registry
            .release(&project, "_default", &eps[2])
            .await
            .unwrap();
        let cached = lease_map.owner_for(&project, "_default").await;
        assert_eq!(cached, first, "cache should answer without re-fetch");
    }

    #[test]
    fn parse_lease_cache_ttl_env_defaults() {
        assert_eq!(parse_lease_cache_ttl_env(None).unwrap(), DEFAULT_LEASE_CACHE_TTL);
        assert_eq!(parse_lease_cache_ttl_env(Some("")).unwrap(), DEFAULT_LEASE_CACHE_TTL);
        assert_eq!(
            parse_lease_cache_ttl_env(Some("  ")).unwrap(),
            DEFAULT_LEASE_CACHE_TTL
        );
    }

    #[test]
    fn parse_lease_cache_ttl_env_accepts_explicit() {
        assert_eq!(
            parse_lease_cache_ttl_env(Some("250")).unwrap(),
            Duration::from_millis(250)
        );
        assert_eq!(
            parse_lease_cache_ttl_env(Some("10000")).unwrap(),
            Duration::from_millis(10_000)
        );
    }

    #[test]
    fn parse_lease_cache_ttl_env_rejects_garbage() {
        assert!(parse_lease_cache_ttl_env(Some("nope")).is_err());
        assert!(parse_lease_cache_ttl_env(Some("-5")).is_err());
    }
}
