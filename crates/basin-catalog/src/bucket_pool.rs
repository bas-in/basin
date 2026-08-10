//! Bounded multi-bucket storage-pool catalog records (engine task #36, Stage 1).
//!
//! Today every project's objects live under a per-project key prefix in a
//! single bucket. Stage 1 of the multi-bucket pool lays the FOUNDATION for
//! spreading projects across a *bounded* set of pooled buckets, each isolated
//! by the existing per-project prefix. This module defines the two catalog
//! records the pool persists; the routing/assignment logic lives in
//! `basin-storage` and is gated behind `BASIN_BUCKET_POOL` (default OFF).
//!
//! Two record shapes:
//!
//! - [`BucketRegistryEntry`] — one per pooled bucket: `bucket_id → endpoint,
//!   region, credentials-ref`. Credentials are **referenced** by name (an
//!   env/secret key the deployment resolves), never inlined. The registry is a
//!   single global JSON list (not per-project) so any node can read the full
//!   pool topology.
//! - [`BucketAssignment`] — one per project: `project_id → bucket_id (+ tier)`.
//!   Decided once on first write when the flag is ON, then STABLE: persisted,
//!   re-read on restart, never recomputed.
//!
//! See `docs/multi-bucket-pool-design.md`.

use serde::{Deserialize, Serialize};

/// Storage tier a project's bucket assignment sits in. Stage 1 only ever uses
/// [`BucketTier::Pooled`]; [`BucketTier::Dedicated`] is reserved for the
/// (deferred) dedicated-promotion path so the persisted record shape doesn't
/// have to change when promotion lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BucketTier {
    /// Shared pooled bucket — many projects, isolated by key prefix.
    #[default]
    Pooled,
    /// Dedicated bucket for a single heavy project (deferred to a later stage).
    Dedicated,
}

/// One entry in the bucket registry: where a pooled bucket lives and how to
/// authenticate to it. Credentials are **referenced** (`credentials_ref` names
/// an env/secret key the deployment resolves to real keys), never inlined into
/// the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketRegistryEntry {
    /// Stable identifier for this bucket within the pool. Used as the key in
    /// [`BucketAssignment::bucket_id`].
    pub bucket_id: String,
    /// The provider bucket name objects are written to.
    pub bucket_name: String,
    /// S3-compatible endpoint URL (e.g. the Tigris endpoint).
    pub endpoint: String,
    /// Region literal (often `"auto"` for Tigris).
    pub region: String,
    /// **Reference** to the credentials for this bucket — an env/secret key
    /// name the deployment resolves. NEVER the secret itself. `None` means
    /// "use the process-default credentials" (the same `AWS_*` the single-bucket
    /// path uses today).
    pub credentials_ref: Option<String>,
    /// Stage-1 load signal: number of projects assigned to this bucket. Cheap
    /// to read (it travels with the registry) and bumped under the pool's
    /// grow-lock on each new assignment. Best-effort — used only to spread new
    /// assignments and decide pool growth, never for correctness. The richer
    /// sustained-PUT-rate signal from the design doc would augment this later.
    #[serde(default)]
    pub assigned_count: u64,
}

/// A project's stable assignment to a pooled bucket. Persisted once on first
/// write (when the pool flag is ON) and re-read thereafter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketAssignment {
    /// The registry bucket this project's objects live in. For a striped
    /// assignment this is `stripe[0]` (the primary), so a striping-unaware
    /// reader still resolves a valid bucket.
    pub bucket_id: String,
    /// Tier of the assignment. Stage 1 always [`BucketTier::Pooled`].
    #[serde(default)]
    pub tier: BucketTier,
    /// #36 (Stage 2a) — the project's ORDERED, FROZEN stripe set of pooled
    /// bucket ids. A single project's partitions are spread across these
    /// buckets by `stripe[fnv1a(partition_id) % stripe.len()]`, raising the
    /// project's aggregate write bandwidth beyond a single bucket's cap.
    ///
    /// Back-compat + no-op contract:
    /// - **Empty** (the default — every Stage-1 assignment persisted before
    ///   this field existed) means "no striping": treat the project as a
    ///   single-bucket assignment on [`Self::bucket_id`]. Deserialising an old
    ///   record yields an empty vec, so the behaviour is unchanged.
    /// - A **single-element** stripe (`[bucket_id]`) is also single-bucket —
    ///   byte-identical routing to today.
    ///
    /// The order is significant and is NEVER reordered or shrunk while files
    /// exist (the `% k` map would otherwise remap an existing partition's
    /// already-written files). Growing the set is a (deferred) migration.
    #[serde(default)]
    pub stripe: Vec<String>,
}

impl BucketAssignment {
    /// The effective ordered stripe set: the explicit [`Self::stripe`] when
    /// non-empty, else the single [`Self::bucket_id`] (the Stage-1 / legacy
    /// shape). Never empty for a well-formed assignment, so callers can index
    /// `% len()` safely.
    pub fn effective_stripe(&self) -> Vec<String> {
        if self.stripe.is_empty() {
            vec![self.bucket_id.clone()]
        } else {
            self.stripe.clone()
        }
    }
}

/// The phase of an in-flight online-consolidation migration (#37). The phase
/// is the resume point of the crash-safe state machine: on restart the engine
/// reads the [`MigrationIntent`] and re-enters at exactly this phase. Each
/// phase's work is idempotent, so re-running a partially-completed phase
/// converges to the same result.
///
/// Ordering is the forward progression `Copy → Verify → Cutover → Drain →
/// Delete`. The numeric discriminants are stable (persisted) — never reorder
/// or renumber.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationPhase {
    /// Server-side copy every live object of the project from `A/<prefix>` to
    /// `B/<prefix>`. Writes still go to A (assignment unchanged). Idempotent:
    /// re-copy is a no-op overwrite.
    Copy,
    /// Confirm every catalog-referenced live object exists in B with matching
    /// size. Re-copy any mismatch. Only a fully-verified copy proceeds.
    Verify,
    /// The linearization point: atomically flip the assignment record
    /// `project → B` (single catalog write). After this, reads/writes resolve
    /// to B.
    Cutover,
    /// Copy any objects written to A between Copy and Cutover, verify again,
    /// then delete A's now-orphaned project objects.
    Drain,
    /// Delete the source bucket from the registry iff it holds no live project
    /// (provably empty). The LAST step; idempotent (delete-if-exists).
    Delete,
}

impl MigrationPhase {
    /// The phase that follows `self` in the forward progression, or `None` when
    /// `self` is the terminal [`MigrationPhase::Delete`] (migration complete).
    pub fn next(self) -> Option<MigrationPhase> {
        match self {
            MigrationPhase::Copy => Some(MigrationPhase::Verify),
            MigrationPhase::Verify => Some(MigrationPhase::Cutover),
            MigrationPhase::Cutover => Some(MigrationPhase::Drain),
            MigrationPhase::Drain => Some(MigrationPhase::Delete),
            MigrationPhase::Delete => None,
        }
    }
}

/// A persisted intent record for an online-consolidation migration of one
/// project's objects from a sparse/cold source bucket (`from`) into a target
/// pooled bucket (`to`). Stored in the catalog (no external DB), one per
/// in-flight migration, keyed by `project`. Its presence means "a migration is
/// in progress or crashed mid-flight"; its absence means "no migration".
///
/// The record is the single source of truth for resuming after a crash: read
/// it, resume from [`Self::phase`]. It is deleted only after the terminal
/// [`MigrationPhase::Delete`] completes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationIntent {
    /// The project whose objects are being migrated.
    pub project: ProjectId,
    /// Source bucket id (the sparse/cold bucket being reclaimed).
    pub from: String,
    /// Target bucket id (a pooled bucket with headroom).
    pub to: String,
    /// Resume point: the phase to (re-)enter on restart.
    pub phase: MigrationPhase,
}

use basin_common::ProjectId;

/// FNV-1a (64-bit) over a partition id — the SAME hash basin-engine's
/// `partition_router` uses for partition→node ownership, reused here for the
/// partition→bucket stripe map so there is one deterministic hashing story.
/// Pure + stable across processes and restarts (no random seed).
pub fn fnv1a_partition_hash(partition_id: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET;
    for b in partition_id.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(PRIME);
    }
    h
}

/// The stripe slot (index into the ordered stripe set) for `partition_id`
/// given a stripe width `k`. Deterministic + stable. `k == 0` is treated as
/// `k == 1` (defensive; a well-formed stripe is never empty).
pub fn stripe_slot(partition_id: &str, k: usize) -> usize {
    let k = k.max(1);
    (fnv1a_partition_hash(partition_id) % k as u64) as usize
}

/// The bucket registry as persisted: a flat list of entries. Stored as a
/// single global catalog object so the whole pool topology round-trips in one
/// read.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketRegistry {
    pub buckets: Vec<BucketRegistryEntry>,
}

impl BucketRegistry {
    /// Find a registry entry by its `bucket_id`.
    pub fn get(&self, bucket_id: &str) -> Option<&BucketRegistryEntry> {
        self.buckets.iter().find(|b| b.bucket_id == bucket_id)
    }

    /// Increment the assigned-project count for `bucket_id` (the Stage-1 load
    /// signal). No-op if the bucket is unknown.
    pub fn bump_assigned(&mut self, bucket_id: &str) {
        if let Some(b) = self.buckets.iter_mut().find(|b| b.bucket_id == bucket_id) {
            b.assigned_count = b.assigned_count.saturating_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_stripe_defaults_to_single_bucket() {
        // A legacy / Stage-1 assignment (empty stripe) is a width-1 stripe on
        // bucket_id — the no-op shape.
        let a = BucketAssignment {
            bucket_id: "pool-0000".into(),
            tier: BucketTier::Pooled,
            stripe: Vec::new(),
        };
        assert_eq!(a.effective_stripe(), vec!["pool-0000".to_string()]);

        // An explicit stripe is returned verbatim, in order.
        let b = BucketAssignment {
            bucket_id: "pool-0000".into(),
            tier: BucketTier::Pooled,
            stripe: vec!["pool-0000".into(), "pool-0001".into(), "pool-0002".into()],
        };
        assert_eq!(
            b.effective_stripe(),
            vec![
                "pool-0000".to_string(),
                "pool-0001".to_string(),
                "pool-0002".to_string()
            ]
        );
    }

    #[test]
    fn legacy_assignment_deserialises_to_empty_stripe() {
        // A record serialised before the `stripe` field existed must
        // round-trip to an empty stripe (back-compat / no-op).
        let json = r#"{"bucket_id":"pool-0007","tier":"pooled"}"#;
        let a: BucketAssignment = serde_json::from_str(json).unwrap();
        assert_eq!(a.bucket_id, "pool-0007");
        assert!(a.stripe.is_empty());
        assert_eq!(a.effective_stripe(), vec!["pool-0007".to_string()]);
    }

    #[test]
    fn stripe_slot_is_deterministic_and_in_range() {
        // Same partition + width → same slot, every call (stable across
        // restarts because the hash has no random seed).
        for pid in ["", "p0", "region:us/2026", "year=2026/month=06/day=26"] {
            for k in 1..=8usize {
                let s1 = stripe_slot(pid, k);
                let s2 = stripe_slot(pid, k);
                assert_eq!(s1, s2, "slot must be deterministic for {pid:?} k={k}");
                assert!(s1 < k, "slot {s1} out of range for k={k}");
            }
        }
        // k == 0 is treated as 1 (defensive).
        assert_eq!(stripe_slot("anything", 0), 0);
    }

    #[test]
    fn stripe_slot_distributes_partitions_across_buckets() {
        // Across many partitions the slots cover the whole width — i.e. the
        // map actually spreads load (not a constant). 256 partitions across 4
        // buckets must touch all 4.
        let k = 4;
        let mut seen = [false; 4];
        for i in 0..256 {
            let pid = format!("partition-{i}");
            seen[stripe_slot(&pid, k)] = true;
        }
        assert!(
            seen.iter().all(|&b| b),
            "stripe map must spread across all buckets"
        );
    }
}
