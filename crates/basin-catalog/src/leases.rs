//! Partition-lease primitive — the foundation of the ADR 0023 scale-out
//! architecture (Phase 6.X.A).
//!
//! A *lease* is the durable record of which replica currently owns a
//! `(project, partition)`. Ownership used to be an implicit hash on
//! `ProjectId` (`ShardMap::shard_for`); ADR 0023 converts it into a lease in
//! the catalog Postgres so replicas become stateless and a hot tenant can
//! distribute across replicas by partitioning. This module ships the
//! primitive; partition-aware routing (6.X.B), handoff (6.X.C), and budget
//! reconciliation (6.X.D) build on top.
//!
//! ## The lease lifecycle
//!
//! ```text
//!   acquire  → first-grant (INSERT … ON CONFLICT DO NOTHING) OR
//!              steal-on-expiry (UPDATE … WHERE holder=$old AND expires_at < now)
//!   renew    → extend expires_at while still the holder at the known epoch
//!   release  → drop the row (best-effort; the TTL also reclaims it)
//!   owner_of → who holds it right now (NULL if expired)
//! ```
//!
//! ## Epoch — the fencing token
//!
//! Every (re)grant bumps a monotonic `epoch`. A WAL append carries the
//! holder's epoch; if a network partition produces two replicas that both
//! think they hold the lease, the *loser* (lower epoch) is rejected at the
//! WAL append site (`basin-wal`). The epoch is the only thing that makes the
//! Postgres-CAS design safe without Raft: correctness is preserved even
//! during the brief dual-leaseholder window.
//!
//! ## Keying / cost
//!
//! Same shape rule as every other catalog state module: one row per
//! `(project, partition)`, primary-keyed on the pair. Per-project cost stays
//! `O(partitions)`, not `O(replicas)`; there is no per-replica heavy
//! resource. The InMemory backend keys a single shared `HashMap` by the same
//! pair; the Postgres backend keys the `partition_leases` table by the same
//! pair.

use async_trait::async_trait;
use basin_common::ProjectId;
use chrono::{DateTime, Utc};

/// Default lease time-to-live. A lease that is not renewed within this window
/// is reclaimable by any replica. Matches ADR 0023's 15 s default.
pub const DEFAULT_LEASE_TTL_SECS: u64 = 15;

/// Default heartbeat / renew cadence. Renewing every 5 s against a 15 s TTL
/// gives two missed renewals of slack before a lease is stolen.
pub const DEFAULT_LEASE_RENEW_SECS: u64 = 5;

/// A granted lease. Carries the fencing `epoch` the holder must present on
/// every WAL append, plus the grant/expiry bookkeeping.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub project: ProjectId,
    pub partition_id: String,
    pub holder: String,
    /// Monotonic fencing token. Strictly increases on every (re)grant of this
    /// `(project, partition)` — a first-grant starts at 1, each steal-on-expiry
    /// adds 1. Never resets, never decreases, even across holder changes.
    pub epoch: i64,
    pub granted_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// The lease ownership registry. Implemented by both catalog backends
/// (`InMemoryCatalog`, `PostgresCatalog`) so the same call sites work in
/// tests and production.
///
/// Concurrency contract: every method is atomic with respect to concurrent
/// callers contending on the same `(project, partition)`. The InMemory
/// backend serialises on a per-pair guard; the Postgres backend uses a
/// single CAS statement (or a `SELECT … FOR UPDATE` transaction) so two
/// replicas racing to acquire never both win.
#[async_trait]
pub trait LeaseRegistry: Send + Sync {
    /// Acquire (or steal-on-expiry) the lease for `(project, partition)` on
    /// behalf of `holder`, valid for `ttl`.
    ///
    /// Returns `Some(Lease)` with the freshly-granted epoch if the caller now
    /// holds the lease. Returns `None` if a *different, non-expired* holder
    /// owns it (the caller lost the race / must wait for expiry).
    ///
    /// Idempotent for the current holder: re-acquiring a lease you already
    /// hold renews it and bumps the epoch (treated as a fresh grant).
    ///
    /// Implementation: `INSERT … ON CONFLICT DO NOTHING` for the first grant;
    /// `UPDATE … WHERE holder = $existing AND expires_at < now()` (or holder
    /// matches the caller) for steal-on-expiry / self-reacquire.
    async fn acquire(
        &self,
        project: &ProjectId,
        partition_id: &str,
        holder: &str,
        ttl: std::time::Duration,
    ) -> basin_common::Result<Option<Lease>>;

    /// Extend the lease's expiry by `ttl`, but only if `holder` still owns it
    /// at exactly `epoch`. Returns `true` on success, `false` if the lease was
    /// lost (stolen, expired-and-regranted, or never held). On `false` the
    /// caller must drop its partition state — it is no longer the leaseholder.
    ///
    /// Renew does **not** bump the epoch: it is the steady-state heartbeat,
    /// not a (re)grant.
    async fn renew(
        &self,
        project: &ProjectId,
        partition_id: &str,
        holder: &str,
        epoch: i64,
        ttl: std::time::Duration,
    ) -> basin_common::Result<bool>;

    /// Voluntarily release the lease if `holder` owns it. Best-effort: a
    /// concurrent steal that already replaced the holder makes this a no-op.
    /// Returns `true` if a row owned by `holder` was removed.
    async fn release(
        &self,
        project: &ProjectId,
        partition_id: &str,
        holder: &str,
    ) -> basin_common::Result<bool>;

    /// Current owner of `(project, partition)`, or `None` if no live lease
    /// exists (never granted, or expired). Returns `(holder, epoch)`.
    /// An expired lease is reported as `None` even though the row may still
    /// exist — the row is reclaimable, so it has no live owner.
    async fn owner_of(
        &self,
        project: &ProjectId,
        partition_id: &str,
    ) -> basin_common::Result<Option<(String, i64)>>;
}
