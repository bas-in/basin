//! Tenant -> shard endpoint mapping (consistent-ish hashing).
//!
//! The router holds a `ShardMap` whenever it's running in compute-sharded
//! mode. For every authenticated `TenantId` we compute a stable hash and
//! pick `endpoints[hash % endpoints.len()]`. The result is the address of
//! the shard-owner pgwire listener that owns that tenant's in-memory
//! state; the router proxies queries to it for the lifetime of the
//! connection.
//!
//! v0.1 deliberately uses plain modular hashing (rather than Karger-style
//! consistent hashing rings) because the shard count is fixed at startup
//! and resharding on the fly is out of scope for this milestone — the
//! point of the test is that *re-connections from the same tenant land
//! on the same shard*, not that we can add shards live.
//!
//! The hash function is `std::collections::hash_map::DefaultHasher` fed the
//! `TenantId`'s ULID byte representation. That gives us a stable hash for
//! the lifetime of the process; what matters for the routing-stability
//! test is that two calls with the same `TenantId` in the same process
//! produce the same shard. (DefaultHasher is randomly seeded per-process,
//! so cross-process the placement may differ — fine, since each router
//! constructs its own `ShardMap` from the same endpoint list and uses it
//! consistently for that process's lifetime.)

use std::hash::{Hash, Hasher};

use basin_common::TenantId;

/// Maps `TenantId -> shard endpoint`. Cheap to clone (just `Vec<String>`).
#[derive(Clone, Debug)]
pub struct ShardMap {
    endpoints: Vec<String>,
}

impl ShardMap {
    /// Build a `ShardMap` over the given endpoints. The endpoint vector
    /// must be non-empty; the router guards on this before constructing.
    pub fn new(endpoints: Vec<String>) -> Self {
        assert!(
            !endpoints.is_empty(),
            "ShardMap requires at least one endpoint",
        );
        Self { endpoints }
    }

    /// Pick the endpoint that owns `tenant`. Stable for the lifetime of
    /// this `ShardMap`.
    pub fn shard_for(&self, tenant: &TenantId) -> &str {
        let idx = self.shard_index(tenant);
        &self.endpoints[idx]
    }

    /// Endpoint slice, in declaration order. Tests use this to match a
    /// reported shard counter back to the endpoint it came from.
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    /// 0-based index of the shard that owns `tenant`. Pulled out of
    /// `shard_for` so tests can sanity-check the bucketing without
    /// having to round-trip a `&str` to an index lookup.
    pub fn shard_index(&self, tenant: &TenantId) -> usize {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        // Hash the ULID bytes directly. `Hash for TenantId` is derived,
        // but we go through the bytes so the hash domain doesn't change
        // if the derive is later replaced.
        tenant.as_ulid().0.hash(&mut h);
        let v = h.finish();
        (v as usize) % self.endpoints.len()
    }
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
        let t = TenantId::new();
        let first = map.shard_for(&t).to_owned();
        for _ in 0..16 {
            assert_eq!(map.shard_for(&t), first);
        }
    }

    #[test]
    fn shard_for_distributes_across_endpoints() {
        // Hash a hundred random tenants into 4 shards and assert no shard
        // got more than 60% of them. With 100 random ULIDs and 4 buckets
        // the expected fraction is 25%; 60% is wide enough that the test
        // is not flaky but tight enough that a totally degenerate hash
        // (everything to one shard) fails it.
        let map = ShardMap::new(endpoints(4));
        let mut counts = vec![0usize; 4];
        for _ in 0..100 {
            let t = TenantId::new();
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
}
