//! `basin-placement` — `(project_id, partition_key) → shard_owner_node` map.
//!
//! Phase 3 work. Stateless service backed by a strongly-consistent store
//! (etcd, FoundationDB, or Postgres advisory locks). Consistent hashing with
//! virtual nodes; per-project overrides for big customers; fast failover.
//!
//! v0.2 adds the [`rebalance`] submodule — trait + types for moving whole
//! shards between machines. See `basin/docs/scaling/shard-rebalance.md`
//! for the full design.

#![forbid(unsafe_code)]

pub mod rebalance;

pub use rebalance::{
    LoadMap, LocalRebalancer, MachineId, MachineLoad, MovePlan, MoveReason, MoveReport, Rebalancer,
    ShardId, ShardLoad,
};
