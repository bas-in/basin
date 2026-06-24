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
    /// The registry bucket this project's objects live in.
    pub bucket_id: String,
    /// Tier of the assignment. Stage 1 always [`BucketTier::Pooled`].
    #[serde(default)]
    pub tier: BucketTier,
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
