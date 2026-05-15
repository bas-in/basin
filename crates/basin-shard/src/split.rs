//! Hash-bucket shard split protocol (v0.2 — skeleton).
//!
//! See `docs/scaling/shard-rebalance.md` for the full design. This file
//! holds the trait + struct stubs; the implementation lands in v0.2.
//!
//! ## Overview
//!
//! A "split" doubles a table's hash bucket count (e.g. `N → 2N`). It is
//! driven through five phases against an atomic-commit catalog so writes
//! and reads in flight see a single linearisation point:
//!
//! ```text
//! phase 0  pre-split      catalog adds new buckets, no traffic
//! phase 1  dual-write     writes land in both old + new buckets
//! phase 2  catchup        repartition old Parquet under new layout,
//!                          drain old WAL tail through new buckets
//! phase 3  cutover        atomic catalog epoch bump
//! phase 4  drop old       reclaim old bucket's storage
//! ```
//!
//! Writers carry `partition_epoch` on every catalog commit; a stale
//! epoch returns `CommitConflict`, the writer reloads the spec, and
//! retries against the new layout. There is no 2PC across machines.
//!
//! ## Status
//!
//! Trait + types only. The concrete `LocalShardSplitter` impl is
//! deferred to the v0.2 milestone PR. Calling any method on a placeholder
//! impl today returns `BasinError::FeatureNotSupported`.

use std::sync::Arc;

use async_trait::async_trait;
use basin_catalog::PartitionSpec;
use basin_common::{BasinError, Result, TableName, ProjectId};
use basin_wal::Lsn;

/// Catalog epoch for a table's partition spec. Bumped on every successful
/// `cutover`. Writers carry this on every commit; mismatches return
/// `CommitConflict`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch(pub u64);

impl Epoch {
    pub const ZERO: Epoch = Epoch(0);
    pub fn next(self) -> Epoch {
        Epoch(self.0 + 1)
    }
}

impl std::fmt::Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Outcome of [`ShardSplitter::catchup`].
#[derive(Clone, Debug, Default)]
pub struct CatchupReport {
    pub files_repartitioned: u64,
    pub bytes_rewritten: u64,
    pub tail_entries_drained: u64,
}

/// All state a split run carries between phases. Built by
/// [`ShardSplitter::prepare_split`]; threaded through every subsequent
/// call so the implementation never has to look up the same row twice.
#[derive(Clone, Debug)]
pub struct SplitPlan {
    pub project: ProjectId,
    pub table: TableName,
    pub old_spec: PartitionSpec,
    pub new_spec: PartitionSpec,
    /// Catalog epoch the split is targeting. Equal to `live_epoch + 1`
    /// at `prepare_split` time; promoted to live on `cutover`.
    pub proposed_epoch: Epoch,
    /// Set by [`ShardSplitter::open_dual_write`] — the highest LSN that
    /// existed in the old WAL when dual-write opened. `catchup` is
    /// complete once every entry `lsn ≤ dual_write_open_lsn` is also
    /// represented under the new layout.
    pub dual_write_open_lsn: Option<Lsn>,
}

/// Drive a hash-bucket split for a single `(project, table)`.
///
/// Implementations are responsible for:
///   - catalog round-trips (proposed spec rows, dual-write flag, atomic
///     cutover, epoch bump),
///   - storage-side re-partitioning of existing Parquet files,
///   - draining the in-flight WAL tail under the new layout,
///   - tearing down the old bucket's resident state.
///
/// The trait is object-safe so `LocalShardSplitter` (single machine,
/// v0.2) and `RemoteShardSplitter` (cluster-wide coordinated, v0.3) can
/// be swapped behind an `Arc<dyn ShardSplitter>`.
#[async_trait]
pub trait ShardSplitter: Send + Sync {
    /// Phase 0 — register the proposed new spec alongside the live one.
    /// Returns a [`SplitPlan`] that all subsequent phases use.
    async fn prepare_split(
        &self,
        project: &ProjectId,
        table: &TableName,
        new_spec: PartitionSpec,
    ) -> Result<SplitPlan>;

    /// Phase 1 — open the dual-write window. From this point until
    /// [`Self::cutover`], every write lands in BOTH the old and new
    /// bucket layouts. Records `dual_write_open_lsn` into the plan.
    async fn open_dual_write(&self, plan: &mut SplitPlan) -> Result<()>;

    /// Phase 2 — repartition existing Parquet files under the new layout
    /// and drain the old WAL tail through the new buckets. Idempotent;
    /// safe to call multiple times to make incremental progress.
    async fn catchup(&self, plan: &SplitPlan) -> Result<CatchupReport>;

    /// Phase 3 — atomically promote the proposed spec to live, bumping
    /// the partition epoch and clearing the dual-write flag. Optimistic
    /// concurrency: returns `CommitConflict` if a parallel split landed
    /// first.
    async fn cutover(&self, plan: &SplitPlan) -> Result<Epoch>;

    /// Phase 4 — drop the old layout's residual resources (now-empty
    /// partition state, stale Parquet pointers, etc.). Best-effort.
    async fn drop_old(&self, plan: &SplitPlan) -> Result<()>;
}

/// Single-machine `ShardSplitter` implementation. Runs the five-phase
/// protocol against the local `Shard` + `Catalog` + `Wal` triple.
///
/// Stub today; the real impl lands in the v0.2 milestone PR.
pub struct LocalShardSplitter {
    #[allow(dead_code)]
    pub(crate) shard: crate::Shard,
    #[allow(dead_code)]
    pub(crate) catalog: Arc<dyn basin_catalog::Catalog>,
    #[allow(dead_code)]
    pub(crate) wal: Arc<dyn basin_wal::Wal>,
}

impl LocalShardSplitter {
    /// Wire a splitter against a live shard. The caller owns the lifetime
    /// of the triple; this struct does not spawn background tasks.
    pub fn new(
        shard: crate::Shard,
        catalog: Arc<dyn basin_catalog::Catalog>,
        wal: Arc<dyn basin_wal::Wal>,
    ) -> Self {
        Self {
            shard,
            catalog,
            wal,
        }
    }
}

#[async_trait]
impl ShardSplitter for LocalShardSplitter {
    async fn prepare_split(
        &self,
        _project: &ProjectId,
        _table: &TableName,
        _new_spec: PartitionSpec,
    ) -> Result<SplitPlan> {
        Err(not_yet_implemented("prepare_split"))
    }

    async fn open_dual_write(&self, _plan: &mut SplitPlan) -> Result<()> {
        Err(not_yet_implemented("open_dual_write"))
    }

    async fn catchup(&self, _plan: &SplitPlan) -> Result<CatchupReport> {
        Err(not_yet_implemented("catchup"))
    }

    async fn cutover(&self, _plan: &SplitPlan) -> Result<Epoch> {
        Err(not_yet_implemented("cutover"))
    }

    async fn drop_old(&self, _plan: &SplitPlan) -> Result<()> {
        Err(not_yet_implemented("drop_old"))
    }
}

fn not_yet_implemented(phase: &str) -> BasinError {
    BasinError::internal(format!(
        "ShardSplitter::{phase} not implemented in v0.1; \
         see docs/scaling/shard-rebalance.md for the v0.2 protocol",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_orders_and_advances() {
        assert!(Epoch::ZERO < Epoch::ZERO.next());
        assert_eq!(Epoch(3).next(), Epoch(4));
    }

    #[test]
    fn catchup_report_default_is_zero() {
        let r = CatchupReport::default();
        assert_eq!(r.files_repartitioned, 0);
        assert_eq!(r.bytes_rewritten, 0);
        assert_eq!(r.tail_entries_drained, 0);
    }
}
