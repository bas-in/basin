//! Follower-replica skeleton (v0.2 design — see
//! `basin/docs/scaling/read-replicas.md` for the full architecture).
//!
//! ## Why this file is a skeleton
//!
//! Read scale on Basin = follower replicas that tail the WAL produced by
//! a shard's leader, rebuild the same `PartitionState` the leader holds,
//! and serve reads. The single-writer model (see `in_process.rs`) stays
//! intact on the leader; followers are a strict read-only mirror.
//!
//! This module ships the *surface area* — the trait and struct stubs the
//! router and placement layer will program against — without the
//! implementations, so the rest of the v0.2 work (WAL streaming, lease-
//! based leader election in basin-placement, router-side load balancing)
//! can be staged against a stable API.
//!
//! ## Surface
//!
//! - [`ReplicaRole`]: per-shard discriminator. Today every shard is a
//!   [`ReplicaRole::Leader`] by default; v0.2 adds [`ReplicaRole::Follower`].
//! - [`ShardFollower`]: object-safe trait the router consults on each
//!   eligible-for-read decision. Implementations live next to the
//!   in-process leader (`InProcessShard`) so they share the same
//!   `PartitionState` map shape.
//! - [`FollowerShard`]: handle wrapping a follower implementation. Same
//!   shape as the existing [`crate::Shard`] handle, intentionally —
//!   the router can hold either via an enum without bifurcating call
//!   sites.
//! - [`FollowerConfig`]: knobs (lag threshold, tail poll interval,
//!   catchup batch size). Defaults sized for the v0.2 single-region
//!   target (≤ 5 s eventual freshness).
//! - [`LagTier`]: the read-tier enum the router maps client intent to.
//!   `Strong` → leader only; `ReadYourWrites` → follower or leader that
//!   satisfies the client's last-seen LSN; `Bounded(ms)` and `Eventual`
//!   → follower preferred.
//! - [`FollowerStats`]: per-follower telemetry mirroring [`crate::ShardStats`].
//!
//! ## What's *not* here yet
//!
//! - The actual `apply_wal_record` decode path that mirrors
//!   `in_process::decode_payload` into the follower's tail.
//! - The `WalTailer` background task that drives `Wal::tail` (a streaming
//!   variant of `Wal::read_from`, not yet on the trait).
//! - The leader-election lease in `basin-placement`.
//! - Failover promotion (follower drops its read-only flag, takes
//!   ownership of `Wal::append` / `Wal::truncate`).
//!
//! Each is called out in the design doc with its phasing.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use basin_common::{PartitionKey, Result, TableName, ProjectId};
use basin_wal::{Lsn, WalEntry};

/// Per-shard role. Set when [`crate::Shard::new`] (leader) or
/// [`FollowerShard::new`] (follower) constructs the handle. The router
/// uses this to refuse writes against a follower handle at the call site
/// rather than letting them propagate to a confusing `ShardImpl` error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicaRole {
    /// Today's behaviour. Owns writes, owns the WAL append + truncate,
    /// owns catalog commits.
    Leader,
    /// Read-only mirror. Consumes the leader's WAL via [`ShardFollower::apply_wal_record`].
    Follower,
}

/// Read-freshness tier the router maps client intent to.
///
/// The router resolves a tier from one of three places, in this order:
/// 1. An explicit client header / pgwire parameter (`x-basin-read-tier`).
/// 2. A `lastSeenLsn` echo (basin-js sets this automatically) → upgrades
///    to [`LagTier::ReadYourWrites`].
/// 3. The SQL statement kind: writes → [`LagTier::Strong`], reads →
///    [`LagTier::Eventual`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LagTier {
    /// Leader only. SELECTs inside an active write transaction land here.
    Strong,
    /// Follower or leader, whichever satisfies the client's last-seen
    /// LSN. Carried in the `Lsn` payload.
    ReadYourWrites(Lsn),
    /// Follower preferred; must be within `Duration` of leader's
    /// high-water. Falls back to leader if no follower qualifies.
    Bounded(Duration),
    /// Follower preferred; "eventually consistent". Default for
    /// stateless GETs and free-standing SELECTs.
    Eventual,
}

impl Default for LagTier {
    fn default() -> Self {
        Self::Eventual
    }
}

/// Knobs for [`FollowerShard::new`].
#[derive(Clone, Debug)]
pub struct FollowerConfig {
    /// Maximum acceptable lag for the default [`LagTier::Eventual`] tier.
    /// A follower with `leader_high_water - current_lsn > eventual_threshold`
    /// is removed from the round-robin set until it catches up.
    pub eventual_threshold: Duration,
    /// How often the tailer polls the WAL for new entries. Lower bounds
    /// the follower lag floor on the poll-on-object-store path; the
    /// push-channel path (v0.2.1) makes this irrelevant.
    pub tail_poll_interval: Duration,
    /// Maximum number of WAL records to apply in one tick. Caps the
    /// memory footprint of a follower catching up after a restart.
    pub catchup_batch_size: usize,
    /// Cooldown after a follower errors mid-apply, before the tailer
    /// retries. Errors here are treated as transient; permanent errors
    /// (decode failure on a known-good payload format) are escalated
    /// to placement, which demotes the follower.
    pub apply_error_cooldown: Duration,
}

impl Default for FollowerConfig {
    fn default() -> Self {
        Self {
            eventual_threshold: Duration::from_secs(5),
            tail_poll_interval: Duration::from_millis(200),
            catchup_batch_size: 1024,
            apply_error_cooldown: Duration::from_secs(1),
        }
    }
}

/// Snapshot of a follower's progress. Implementations push these out via
/// tracing for the dashboard, mirroring [`crate::ShardStats`].
#[derive(Clone, Debug)]
pub struct FollowerStats {
    /// Highest LSN this follower has applied.
    pub current_lsn: Lsn,
    /// Last observed leader high-water (from the most recent tail batch).
    pub leader_high_water: Lsn,
    /// Estimated `(leader_high_water - current_lsn)` in WAL records.
    pub records_behind: u64,
    /// Number of WAL records this follower has applied since boot.
    pub records_applied: u64,
    /// Number of times the tailer hit an apply error and retried.
    pub apply_errors: u64,
}

impl Default for FollowerStats {
    fn default() -> Self {
        Self {
            current_lsn: Lsn::ZERO,
            leader_high_water: Lsn::ZERO,
            records_behind: 0,
            records_applied: 0,
            apply_errors: 0,
        }
    }
}

/// Object-safe trait the router consults on each eligible-for-read
/// decision and the tailer drives on every WAL record. Designed so the
/// router can hold `Arc<dyn ShardFollower>` without bifurcating call
/// sites across replica roles.
#[async_trait]
pub trait ShardFollower: Send + Sync {
    /// Apply one WAL record to the follower's state. Returns the new
    /// `current_lsn` after apply (always strictly greater than the
    /// previous one, monotonic per `(project, partition)`).
    ///
    /// Idempotent on already-applied records (returns the current LSN
    /// unchanged) — this keeps tail-restart cheap.
    async fn apply_wal_record(&self, entry: &WalEntry) -> Result<Lsn>;

    /// Highest LSN this follower has applied. Stable across calls from
    /// the same task once they observe a given record.
    async fn current_lsn(&self, project: &ProjectId, partition: &PartitionKey) -> Result<Lsn>;

    /// Predicate the router consults on each read. True iff
    /// `(leader_high_water - current_lsn) <= threshold`.
    async fn is_caught_up(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        threshold: Duration,
    ) -> Result<bool>;

    /// True iff `current_lsn >= target_lsn` — the read-your-writes
    /// predicate. Cheaper than [`Self::is_caught_up`] because no clock
    /// arithmetic is involved.
    async fn has_applied(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        target_lsn: Lsn,
    ) -> Result<bool>;

    /// Read-only data path. Same surface as [`crate::ProjectHandle::read`]
    /// — the storage view is identical (Parquet base + in-memory tail);
    /// the only difference is that the tail is reconstructed from the
    /// WAL stream rather than maintained by direct writes.
    async fn read(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        table: &TableName,
        opts: basin_storage::ReadOptions,
    ) -> Result<Vec<RecordBatch>>;

    /// Snapshot of this follower's stats. Same intent as
    /// [`crate::Shard::stats`].
    async fn stats(&self, project: &ProjectId, partition: &PartitionKey) -> Result<FollowerStats>;
}

/// Handle to a follower replica's shard map. Cheap to clone (`Arc`
/// inside). Mirrors [`crate::Shard`] so router-side code can hold either
/// behind a small enum without further plumbing.
///
/// v0.2 ships the real backing implementation. v0.1 callers should not
/// construct one — the constructor signature is fixed for forward-
/// compatibility but the body is `todo!()` until the WAL streaming
/// surface lands on the [`basin_wal::Wal`] trait.
#[derive(Clone)]
pub struct FollowerShard {
    #[allow(dead_code)]
    inner: Arc<dyn ShardFollower>,
    role: ReplicaRole,
}

impl FollowerShard {
    /// Construct a follower handle. The caller supplies the storage +
    /// catalog handles (shared with the leader — same object store) and
    /// the WAL handle the follower will tail. Stubbed in v0.1; lands in
    /// v0.2 alongside `Wal::tail`.
    ///
    /// # Panics
    ///
    /// Always, in v0.1. The signature is stable; the body is the
    /// integration point for the v0.2 WAL streaming work.
    pub fn new(
        _storage: basin_storage::Storage,
        _catalog: Arc<dyn basin_catalog::Catalog>,
        _wal: Arc<dyn basin_wal::Wal>,
        _cfg: FollowerConfig,
    ) -> Self {
        // v0.2: build an `InProcessFollower` (sibling of `InProcessShard`)
        // that owns the per-partition `Arc<RwLock<PartitionState>>` map
        // and spawns one tailer task per resident partition.
        unimplemented!(
            "FollowerShard requires Wal::tail; lands in v0.2. See \
             basin/docs/scaling/read-replicas.md section 2.2.",
        )
    }

    /// Role discriminator. Always [`ReplicaRole::Follower`] for handles
    /// produced by this type; the leader-side [`crate::Shard`] handle is
    /// implicitly [`ReplicaRole::Leader`].
    pub fn role(&self) -> ReplicaRole {
        self.role
    }

    /// Pass-through to [`ShardFollower::is_caught_up`].
    pub async fn is_caught_up(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        threshold: Duration,
    ) -> Result<bool> {
        self.inner.is_caught_up(project, partition, threshold).await
    }

    /// Pass-through to [`ShardFollower::has_applied`].
    pub async fn has_applied(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        target_lsn: Lsn,
    ) -> Result<bool> {
        self.inner.has_applied(project, partition, target_lsn).await
    }

    /// Pass-through to [`ShardFollower::current_lsn`].
    pub async fn current_lsn(&self, project: &ProjectId, partition: &PartitionKey) -> Result<Lsn> {
        self.inner.current_lsn(project, partition).await
    }

    /// Pass-through to [`ShardFollower::read`].
    pub async fn read(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        table: &TableName,
        opts: basin_storage::ReadOptions,
    ) -> Result<Vec<RecordBatch>> {
        self.inner.read(project, partition, table, opts).await
    }

    /// Pass-through to [`ShardFollower::stats`].
    pub async fn stats(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
    ) -> Result<FollowerStats> {
        self.inner.stats(project, partition).await
    }
}

impl std::fmt::Debug for FollowerShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FollowerShard")
            .field("role", &self.role)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_tier_default_is_eventual() {
        assert_eq!(LagTier::default(), LagTier::Eventual);
    }

    #[test]
    fn lag_tier_read_your_writes_carries_lsn() {
        let t = LagTier::ReadYourWrites(Lsn(42));
        match t {
            LagTier::ReadYourWrites(l) => assert_eq!(l, Lsn(42)),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn lag_tier_bounded_carries_duration() {
        let t = LagTier::Bounded(Duration::from_millis(500));
        match t {
            LagTier::Bounded(d) => assert_eq!(d, Duration::from_millis(500)),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn follower_config_defaults_are_v02_sized() {
        let c = FollowerConfig::default();
        assert_eq!(c.eventual_threshold, Duration::from_secs(5));
        assert_eq!(c.tail_poll_interval, Duration::from_millis(200));
        assert_eq!(c.catchup_batch_size, 1024);
        assert_eq!(c.apply_error_cooldown, Duration::from_secs(1));
    }

    #[test]
    fn follower_stats_default_is_zero() {
        let s = FollowerStats::default();
        assert_eq!(s.current_lsn, Lsn::ZERO);
        assert_eq!(s.leader_high_water, Lsn::ZERO);
        assert_eq!(s.records_behind, 0);
        assert_eq!(s.records_applied, 0);
        assert_eq!(s.apply_errors, 0);
    }

    #[test]
    fn replica_role_distinct() {
        assert_ne!(ReplicaRole::Leader, ReplicaRole::Follower);
    }
}

/// Promotion path: turn a [`FollowerShard`] into a leader [`crate::Shard`].
///
/// Called by basin-placement when the leader machine's lease expires
/// and this follower is the highest-LSN candidate. Stubbed in v0.1;
/// lands in v0.2 alongside the lease-based election.
///
/// The contract: after `promote` returns, the resulting [`crate::Shard`]
/// holds the *same* `Arc<RwLock<PartitionState>>` map the follower was
/// building, plus exclusive write access to the WAL (`append` +
/// `truncate`). Existing follower tailers on the same WAL are notified
/// via the placement layer's `generation` bump.
#[allow(dead_code)]
pub fn promote(_follower: FollowerShard) -> Result<crate::Shard> {
    unimplemented!(
        "Follower promotion lands in v0.2 with the placement lease. See \
         basin/docs/scaling/read-replicas.md section 4.3.",
    )
}
