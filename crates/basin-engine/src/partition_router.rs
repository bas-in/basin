//! Deterministic owner resolution for transparent per-partition write
//! forwarding (#28 multi-node bulk ingest).
//!
//! N Basin engine nodes share one project's object-store catalog + lease
//! registry + Tigris data. A single bulk COPY hits whichever node the client
//! connected to, but the write fan-out (`executor::write_batch_fanout`) spreads
//! a table's chunks across P partitions for P independent compaction lanes.
//! With multiple nodes we want those lanes spread across nodes too — so each
//! partition has ONE deterministic owner node, and a batch destined for a
//! partition this node doesn't own is forwarded to the owner, transparently to
//! the client.
//!
//! # Why a fixed hash (not `DefaultHasher`)
//!
//! Owner resolution must agree across processes: node A and node B must both
//! compute the *same* owner for `(project, partition)`, or a partition could be
//! written by two nodes at once (the lease CAS would catch it, but we'd thrash
//! and forward in circles). `std::collections::hash_map::DefaultHasher` is
//! randomly seeded per process (see `basin-router/src/sharding.rs`, which uses
//! it precisely because its single-process routing only needs in-process
//! stability) — so it is unusable here. This module uses **FNV-1a**, a fixed,
//! seedless, well-distributed hash, so the index is identical in every process
//! for all time.
//!
//! # Peer list + self-identity
//!
//! [`PartitionRouter`] is built from `BASIN_SHARD_PEERS` (comma-separated REST
//! base URLs, e.g. `http://m1.vm.app.internal:5434,http://m2...:5434`,
//! INCLUDING this node) plus this node's id (`BASIN_REPLICA_ID`, which on the
//! dev deploy is set to this node's own peer URL). The desired owner of a
//! partition is `peers[ fnv1a(project, partition_id) % peers.len() ]`.
//!
//! **Back-compat is load-bearing:** unset / single-entry `BASIN_SHARD_PEERS`
//! makes [`PartitionRouter::desired_owner`] always return self, so
//! `write_batch_fanout` takes the existing local path byte-for-byte. The router
//! is only consulted when a multi-peer list is configured.

use basin_common::ProjectId;

/// Environment variable holding the ordered peer list for partition-write
/// forwarding: comma-separated REST base URLs (host:port or full
/// `http://host:port`), INCLUDING this node. Unset / single-entry → all writes
/// stay local (single-node behaviour, byte-identical to before this feature).
pub const SHARD_PEERS_ENV: &str = "BASIN_SHARD_PEERS";

/// A resolved peer: an entry from [`SHARD_PEERS_ENV`]. The string is the peer's
/// REST base URL exactly as configured; the partition-forward client appends
/// the receive path. `is_self` is decided once at resolution time against this
/// node's id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerRef {
    /// The peer's REST base URL (as configured in [`SHARD_PEERS_ENV`]).
    pub base_url: String,
    /// True when this peer is the local node.
    pub is_self: bool,
}

/// FNV-1a over the project ULID bytes + a separator + the partition id bytes.
/// Fixed offset basis / prime (the canonical 64-bit FNV constants); identical
/// across every process and architecture, so two nodes resolve the same owner.
fn fnv1a_owner_hash(project: &ProjectId, partition_id: &str) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut h = FNV_OFFSET;
    let mut mix = |byte: u8| {
        h ^= byte as u64;
        h = h.wrapping_mul(FNV_PRIME);
    };
    for b in project.as_ulid().0.to_be_bytes() {
        mix(b);
    }
    // Separator so (proj=AB, part=C) and (proj=A, part=BC) can't collide.
    mix(0xff);
    for b in partition_id.as_bytes() {
        mix(*b);
    }
    h
}

/// Resolves the deterministic owner node of a `(project, partition)` from a
/// shared, ordered peer list. Cheap to clone (a `Vec<String>` + a `usize`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionRouter {
    /// Ordered peer base URLs, exactly as configured. Always non-empty for a
    /// multi-peer router; an empty/single list collapses to local-only.
    peers: Vec<String>,
    /// Index of this node within `peers`, if it appears there. When `None`
    /// (self not in the list — a misconfiguration) we still forward to the
    /// resolved owner, but `is_self` is never true so nothing is written
    /// locally; resolution still works because owner is purely positional.
    self_index: Option<usize>,
}

impl PartitionRouter {
    /// Build from an explicit peer list + this node's id. Used by tests and by
    /// [`Self::from_env`]. `self_id` is matched against the peer entries to find
    /// `self_index`; an entry equal to `self_id` (after trimming) is "self".
    pub fn new(peers: Vec<String>, self_id: &str) -> Self {
        let self_id = self_id.trim();
        let self_index = peers.iter().position(|p| p == self_id);
        Self { peers, self_index }
    }

    /// Parse a raw comma-separated peer list. Empty entries are skipped. An
    /// empty/single-entry result yields a local-only router (`desired_owner`
    /// always returns self).
    pub fn parse(raw: &str, self_id: &str) -> Self {
        let peers: Vec<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        Self::new(peers, self_id)
    }

    /// Build from the process environment ([`SHARD_PEERS_ENV`] + `self_id`,
    /// which the caller derives from `Shard::replica_id`). Unset → local-only.
    pub fn from_env(self_id: &str) -> Self {
        let raw = std::env::var(SHARD_PEERS_ENV).unwrap_or_default();
        Self::parse(&raw, self_id)
    }

    /// True when forwarding is effectively disabled: 0 or 1 configured peers.
    /// In this mode every partition is owned by self and `write_batch_fanout`
    /// takes the original local path. This is the single-node default.
    pub fn is_local_only(&self) -> bool {
        self.peers.len() <= 1
    }

    /// Number of configured peers (for startup logging / tests).
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// True when this node's id matched an entry in the peer list (i.e. the
    /// SELF-ID INVARIANT holds: `BASIN_REPLICA_ID` is one of `BASIN_SHARD_PEERS`).
    /// When false on a multi-peer router this node never claims any partition as
    /// self — it forwards EVERY write and owns nothing (a misconfiguration the
    /// server warns about at startup).
    pub fn self_is_peer(&self) -> bool {
        self.self_index.is_some()
    }

    /// The deterministic owner node of `(project, partition_id)`.
    ///
    /// Local-only routers (0/1 peers) always return self (`base_url` = the lone
    /// peer or empty, `is_self = true`). Multi-peer routers index the peer list
    /// by the fixed FNV-1a hash modulo the peer count, so every node agrees on
    /// the owner.
    pub fn desired_owner(&self, project: &ProjectId, partition_id: &str) -> PeerRef {
        if self.is_local_only() {
            return PeerRef {
                base_url: self.peers.first().cloned().unwrap_or_default(),
                is_self: true,
            };
        }
        let idx = (fnv1a_owner_hash(project, partition_id) % self.peers.len() as u64) as usize;
        PeerRef {
            base_url: self.peers[idx].clone(),
            is_self: self.self_index == Some(idx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proj_from_u128(v: u128) -> ProjectId {
        ProjectId::from_ulid(ulid::Ulid(v))
    }

    #[test]
    fn unset_or_single_peer_is_always_self() {
        // Unset → local-only, every partition owned by self.
        let r = PartitionRouter::parse("", "node-a");
        assert!(r.is_local_only());
        let p = proj_from_u128(42);
        for part in ["_default", "s1", "s2", "s3"] {
            let o = r.desired_owner(&p, part);
            assert!(o.is_self, "{part}: local-only must be self");
        }

        // Single peer → still local-only.
        let r = PartitionRouter::parse("http://a:5434", "http://a:5434");
        assert!(r.is_local_only());
        assert!(r.desired_owner(&p, "s1").is_self);
    }

    #[test]
    fn desired_owner_is_deterministic_and_cross_process_stable() {
        // Two independently-built routers (modelling two processes) with the
        // SAME peer list must resolve the SAME owner for every input — this is
        // the property `DefaultHasher` would break.
        let peers = vec![
            "http://a:5434".to_string(),
            "http://b:5434".to_string(),
            "http://c:5434".to_string(),
        ];
        let ra = PartitionRouter::new(peers.clone(), "http://a:5434");
        let rb = PartitionRouter::new(peers.clone(), "http://b:5434");
        let p = proj_from_u128(0xdead_beef);
        for part in ["_default", "s1", "s2", "s3", "s4", "s5"] {
            let oa = ra.desired_owner(&p, part);
            let ob = rb.desired_owner(&p, part);
            assert_eq!(
                oa.base_url, ob.base_url,
                "{part}: both processes must agree on owner base_url"
            );
            // Exactly one of them sees it as self (the owner node).
            assert_eq!(oa.is_self, oa.base_url == "http://a:5434");
            assert_eq!(ob.is_self, ob.base_url == "http://b:5434");
        }

        // And stable across re-resolution within a process.
        assert_eq!(
            ra.desired_owner(&p, "s2").base_url,
            ra.desired_owner(&p, "s2").base_url
        );
    }

    #[test]
    fn fixed_hash_value_is_pinned() {
        // Pin a concrete FNV-1a value so a future refactor that swaps the hash
        // (re-introducing a randomized one) fails loudly: cross-node agreement
        // depends on this exact arithmetic.
        let p = proj_from_u128(1);
        let h = fnv1a_owner_hash(&p, "_default");
        // Recompute the canonical FNV-1a by hand-rolled reference to assert the
        // constants are the standard ones (offset basis / prime).
        let expected = {
            const FNV_OFFSET: u64 = 0xcbf29ce484222325;
            const FNV_PRIME: u64 = 0x100000001b3;
            let mut hh = FNV_OFFSET;
            for b in p.as_ulid().0.to_be_bytes() {
                hh ^= b as u64;
                hh = hh.wrapping_mul(FNV_PRIME);
            }
            hh ^= 0xff;
            hh = hh.wrapping_mul(FNV_PRIME);
            for b in b"_default" {
                hh ^= *b as u64;
                hh = hh.wrapping_mul(FNV_PRIME);
            }
            hh
        };
        assert_eq!(h, expected);
    }

    #[test]
    fn distributes_partitions_roughly_evenly() {
        // Over many distinct projects, the 4 default stripe partitions should
        // land across all 3 peers (no peer starves). We count owner picks for
        // partition "s1" across 600 projects.
        let peers = vec![
            "http://a:5434".to_string(),
            "http://b:5434".to_string(),
            "http://c:5434".to_string(),
        ];
        let r = PartitionRouter::new(peers, "http://a:5434");
        let mut counts = [0usize; 3];
        for i in 0..600u128 {
            let p = proj_from_u128(i.wrapping_mul(0x9e3779b97f4a7c15));
            let owner = r.desired_owner(&p, "s1");
            let idx = match owner.base_url.as_str() {
                "http://a:5434" => 0,
                "http://b:5434" => 1,
                "http://c:5434" => 2,
                other => panic!("unexpected owner {other}"),
            };
            counts[idx] += 1;
        }
        // Each peer should get a non-trivial share (well above zero; perfect
        // balance is 200 each — allow a wide band, we only assert no starvation).
        for (i, c) in counts.iter().enumerate() {
            assert!(*c > 100, "peer {i} starved: only {c}/600 partitions");
        }
    }

    #[test]
    fn self_not_in_list_never_claims_self() {
        // A misconfigured node whose id isn't in the peer list resolves owners
        // positionally but never sees any partition as self → it forwards
        // everything (no accidental local write that would split-brain).
        let peers = vec!["http://a:5434".to_string(), "http://b:5434".to_string()];
        let r = PartitionRouter::new(peers, "http://stranger:5434");
        let p = proj_from_u128(7);
        for part in ["_default", "s1", "s2", "s3"] {
            assert!(!r.desired_owner(&p, part).is_self);
        }
    }
}
