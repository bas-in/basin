//! Whole-shard rebalance between machines (v0.2 manual, v0.3 automatic).
//!
//! See `docs/scaling/shard-rebalance.md` for the full design. This file
//! holds the trait + struct stubs; the implementation lands in v0.3.
//!
//! ## Overview
//!
//! When one shard *machine* is hot and another is cold, we move whole
//! shards between them. The object-store bucket is shared substrate (no
//! data copy); the move is purely a "who owns the in-memory state" decision
//! plus a WAL tail replay on the target. Steps:
//!
//! 1. Flush + Parquet-drain on `M_src` (no in-flight tail in RAM only).
//! 2. Bring up `M_tgt` (cold engine, same bucket + catalog).
//! 3. Atomic placement flip in the catalog (the linearisation point).
//! 4. `M_tgt` lazy-loads each `(tenant, partition)` on first access via
//!    the existing WAL replay path.
//! 5. `M_src` reports zero resident partitions; Fly machine destroy.
//!
//! Unavailability window for the moved set: 2–10 seconds. Other
//! partitions on `M_src` are unaffected.
//!
//! ## Status
//!
//! Trait + types only. The concrete `LocalRebalancer` impl (v0.2,
//! single-machine no-op) and `RemoteRebalancer` impl (v0.3, real cluster
//! coordination) land in their respective milestone PRs. Calling any
//! method on the placeholder impl today returns
//! `BasinError::FeatureNotSupported`.

use std::collections::HashMap;

use async_trait::async_trait;
use basin_common::{BasinError, Result, TenantId};

/// Opaque shard identifier. In v0.2 this is `(TenantId, PartitionKey)`
/// flattened to a single string for placement-table rows; v0.3 promotes
/// to a typed ULID minted by `basin-placement` at first shard activation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ShardId(pub String);

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Machine identifier in the placement layer. On Fly this is the Fly
/// machine id (`148ed3a4...`); elsewhere the operator's preferred
/// host name.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MachineId(pub String);

impl std::fmt::Display for MachineId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Per-machine load snapshot fed to [`Rebalancer::plan_moves`].
#[derive(Clone, Debug, Default)]
pub struct MachineLoad {
    pub write_qps: f64,
    pub read_qps: f64,
    pub mem_bytes: u64,
    pub disk_bytes: u64,
    pub write_lag_ms: u64,
    pub resident_shards: u64,
}

/// Cluster-wide load map. `Rebalancer` impls inspect this and produce a
/// (possibly empty) move plan.
#[derive(Clone, Debug, Default)]
pub struct LoadMap {
    pub by_machine: HashMap<MachineId, MachineLoad>,
    /// Per-shard QPS, used to pick which shards to move off a hot machine.
    pub by_shard: HashMap<ShardId, ShardLoad>,
}

/// Per-shard load snapshot.
#[derive(Clone, Debug, Default)]
pub struct ShardLoad {
    pub tenant: Option<TenantId>,
    pub write_qps: f64,
    pub read_qps: f64,
    pub bytes_resident: u64,
}

/// Why a move is being proposed. Used for logs + ops dashboards; not
/// load-bearing on the protocol itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MoveReason {
    /// Source machine is above its sustained write-QPS budget.
    WriteQpsHot,
    /// Source machine's resident bytes exceed the disk budget.
    StorageHot,
    /// Source machine's memory pressure is above threshold.
    MemoryHot,
    /// Source machine is being decommissioned (Fly drain, version upgrade).
    DrainAndShutdown,
    /// Operator triggered via `basin-cli admin rebalance`.
    Manual,
}

/// One scheduled shard move.
#[derive(Clone, Debug)]
pub struct MovePlan {
    pub shard_id: ShardId,
    pub src: MachineId,
    pub tgt: MachineId,
    pub reason: MoveReason,
}

/// Outcome of a single [`Rebalancer::execute_move`].
#[derive(Clone, Debug, Default)]
pub struct MoveReport {
    /// Time spent in step 1 (flush + Parquet drain on M_src).
    pub flush_duration_ms: u64,
    /// Time spent in step 3 (atomic catalog placement flip).
    pub catalog_flip_duration_ms: u64,
    /// Time spent in step 4 (WAL replay on M_tgt up to high-water).
    pub catchup_duration_ms: u64,
    /// Total window in which the shard was unavailable to clients.
    pub unavailability_window_ms: u64,
}

/// Plan + execute shard moves between machines.
///
/// Implementations:
///   - `LocalRebalancer` (v0.2, single-machine — `plan_moves` always
///     returns the empty plan; `execute_move` returns
///     `FeatureNotSupported`),
///   - `RemoteRebalancer` (v0.3, real cluster coordination via the
///     placement catalog table).
#[async_trait]
pub trait Rebalancer: Send + Sync {
    /// Inspect the per-machine load map and produce a (possibly empty)
    /// move plan. Implementations are free to be conservative — a
    /// "no moves" plan is always a valid answer.
    async fn plan_moves(&self, load: &LoadMap) -> Result<Vec<MovePlan>>;

    /// Execute one move atomically. The implementation:
    ///   - flushes `src`,
    ///   - brings up `tgt` (if not already serving),
    ///   - flips the placement row in the catalog,
    ///   - waits for `tgt` to report the moved shards resident,
    ///   - tears down `src` if it's now empty.
    async fn execute_move(&self, plan: &MovePlan) -> Result<MoveReport>;
}

/// Single-machine placeholder. `plan_moves` always returns empty;
/// `execute_move` returns `FeatureNotSupported`. Wire this in v0.2
/// deployments where only one machine exists.
#[derive(Default)]
pub struct LocalRebalancer;

impl LocalRebalancer {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Rebalancer for LocalRebalancer {
    async fn plan_moves(&self, _load: &LoadMap) -> Result<Vec<MovePlan>> {
        // Single machine: nowhere to move shards to. Empty plan is
        // semantically the right answer, not an error.
        Ok(Vec::new())
    }

    async fn execute_move(&self, _plan: &MovePlan) -> Result<MoveReport> {
        Err(BasinError::internal(
            "Rebalancer::execute_move not implemented in v0.2; \
             see docs/scaling/shard-rebalance.md for the v0.3 protocol",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_id_displays() {
        let s = ShardId("tenant-foo/bucket=0007".into());
        assert_eq!(s.to_string(), "tenant-foo/bucket=0007");
    }

    #[test]
    fn machine_id_displays() {
        let m = MachineId("148ed3a4abcd1234".into());
        assert_eq!(m.to_string(), "148ed3a4abcd1234");
    }

    #[tokio::test]
    async fn local_rebalancer_returns_empty_plan() {
        let r = LocalRebalancer::new();
        let load = LoadMap::default();
        let plan = r.plan_moves(&load).await.unwrap();
        assert!(plan.is_empty());
    }

    #[tokio::test]
    async fn local_rebalancer_rejects_execute_move() {
        let r = LocalRebalancer::new();
        let plan = MovePlan {
            shard_id: ShardId("test".into()),
            src: MachineId("a".into()),
            tgt: MachineId("b".into()),
            reason: MoveReason::Manual,
        };
        assert!(r.execute_move(&plan).await.is_err());
    }
}
