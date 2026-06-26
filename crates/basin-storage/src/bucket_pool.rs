//! Bounded multi-bucket storage pool — routing + assignment (#36, Stage 1).
//!
//! FLAG-GATED, default OFF. This is the foundation only: many projects share a
//! bounded set of pooled buckets, isolated by the existing per-project key
//! prefix. Consolidation/migration (#37), dedicated-tier promotion, and
//! multi-provider pools are explicitly OUT of scope for Stage 1.
//!
//! ## The single chokepoint
//!
//! [`crate::Storage::project_object_store`] is the one place a project is
//! mapped to an `ObjectStore` handle. With the flag OFF, [`BucketPool`] is
//! inert and that function returns exactly today's single shared store — a
//! provable no-op. With the flag ON, the write path first warms a project's
//! stable assignment (`ensure_assignment`, async, catalog-backed) and
//! `project_object_store` then returns the assigned pooled bucket's store from
//! a per-process cache.
//!
//! ## Why a per-process cache + async warm
//!
//! `project_object_store` is synchronous and on the hot read/write path;
//! catalog reads are async. So, exactly like the per-project storage-config
//! cache, the assignment is resolved once on the async write path and cached;
//! the sync routing call only ever reads the cache. A cache miss with the flag
//! ON means routing falls back to the default store for that single call (the
//! warm runs on the next write), never an error.
//!
//! ## Load signal (Stage 1)
//!
//! Assignment picks the least-loaded pooled bucket where "load" is the count
//! of projects assigned to each bucket (cheap, derived from the persisted
//! assignments). The richer sustained-PUT-rate / bytes-per-second signal noted
//! in the design doc would plug in at [`BucketPool::choose_bucket`] — see the
//! comment there.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use basin_catalog::bucket_pool::stripe_slot;
use basin_catalog::{BucketAssignment, BucketRegistry, BucketRegistryEntry, BucketTier, Catalog};
use basin_common::{ProjectId, Result};
use object_store::ObjectStore;

/// Env flag that turns the pool ON. Default OFF (single-bucket, today's
/// behaviour). Any value other than the off-set (`0`, `false`, `off`, empty,
/// unset) enables it.
pub const BUCKET_POOL_ENV: &str = "BASIN_BUCKET_POOL";
/// Hard ceiling on the number of pooled buckets the pool will register.
pub const BUCKET_POOL_MAX_ENV: &str = "BASIN_BUCKET_POOL_MAX";
/// Occupancy watermark: when every existing pooled bucket holds at least this
/// many assigned projects, the pool registers a new bucket (up to the max).
pub const BUCKET_POOL_WATERMARK_ENV: &str = "BASIN_BUCKET_POOL_WATERMARK";
/// Per-project stripe width: how many pooled buckets a single project's
/// partitions are spread across (#36 Stage 2a). Default 1 = single-bucket
/// (byte-identical to Stage 1). Clamped to the pool ceiling.
pub const BUCKET_POOL_STRIPE_ENV: &str = "BASIN_BUCKET_POOL_STRIPE";

/// Default pool ceiling when `BASIN_BUCKET_POOL_MAX` is unset/invalid.
const DEFAULT_POOL_MAX: usize = 8;
/// Default occupancy watermark when `BASIN_BUCKET_POOL_WATERMARK` is unset.
const DEFAULT_WATERMARK: usize = 64;
/// Default per-project stripe width when `BASIN_BUCKET_POOL_STRIPE` is unset:
/// 1 = no striping, single-bucket-per-project (Stage 1 behaviour).
const DEFAULT_STRIPE: usize = 1;

/// Resolves a registry entry into a ready-to-use [`ObjectStore`] handle. The
/// engine registers a real-S3 resolver (reading the per-bucket
/// `credentials_ref` from the environment, like the single-bucket path);
/// tests register one that maps `bucket_id` straight to an in-memory store.
///
/// Credentials are resolved HERE, from the reference in the registry entry —
/// they are never persisted into the catalog.
pub trait BucketResolver: Send + Sync {
    /// Build (or return a cached) object store for `entry`. Called at most
    /// once per `bucket_id` per process (the result is cached by
    /// [`BucketPool`]).
    fn resolve(&self, entry: &BucketRegistryEntry) -> Result<Arc<dyn ObjectStore>>;
}

/// Parsed, immutable pool configuration read once from the environment.
#[derive(Debug, Clone, Copy)]
pub struct PoolConfig {
    pub enabled: bool,
    pub max_buckets: usize,
    pub watermark: usize,
    /// Per-project stripe width (#36 Stage 2a). `1` = single-bucket
    /// (Stage-1 behaviour, the default). Always clamped to `1..=max_buckets`
    /// at assignment time.
    pub stripe: usize,
}

impl PoolConfig {
    /// Read the pool config from the process environment. `enabled` is OFF
    /// unless `BASIN_BUCKET_POOL` is set to a truthy value.
    pub fn from_env() -> Self {
        let enabled = std::env::var(BUCKET_POOL_ENV)
            .ok()
            .map(|v| {
                let t = v.trim().to_ascii_lowercase();
                !(t.is_empty() || t == "0" || t == "false" || t == "off" || t == "no")
            })
            .unwrap_or(false);
        let max_buckets = std::env::var(BUCKET_POOL_MAX_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_POOL_MAX);
        let watermark = std::env::var(BUCKET_POOL_WATERMARK_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_WATERMARK);
        let stripe = std::env::var(BUCKET_POOL_STRIPE_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_STRIPE);
        Self {
            enabled,
            max_buckets,
            watermark,
            stripe,
        }
    }
}

/// The per-process bucket pool. Holds the config, the assignment cache, the
/// resolved-store cache, and the resolver. Inert (and never consulted) when
/// `config.enabled` is false.
pub struct BucketPool {
    config: PoolConfig,
    /// Per-process cache of `project → ORDERED stripe of assigned bucket
    /// stores`. Populated by [`ensure_assignment`]; read by the sync routing
    /// path. A missing entry means "no cached assignment" — routing then
    /// falls back to the default store for that one call. The vec is never
    /// empty for a cached project (it always holds at least the primary
    /// bucket), and its order matches the persisted `BucketAssignment::stripe`
    /// so `stripe_slot(partition_id, k)` indexes it deterministically.
    assignment_cache: RwLock<HashMap<ProjectId, Vec<Arc<dyn ObjectStore>>>>,
    /// Per-process cache of `bucket_id → resolved store`, so each bucket's
    /// handle is built at most once.
    store_cache: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
    /// Serialises assignment + registry-growth so two first-writes for two
    /// different projects don't both grow the pool past the watermark
    /// simultaneously. Cheap async mutex; only taken on the rare first-write.
    grow_lock: tokio::sync::Mutex<()>,
    resolver: Arc<dyn BucketResolver>,
}

impl BucketPool {
    /// Build a pool from env config and a resolver.
    pub fn new(config: PoolConfig, resolver: Arc<dyn BucketResolver>) -> Self {
        Self {
            config,
            assignment_cache: RwLock::new(HashMap::new()),
            store_cache: RwLock::new(HashMap::new()),
            grow_lock: tokio::sync::Mutex::new(()),
            resolver,
        }
    }

    /// Whether the pool is enabled. When false the pool is a no-op and the
    /// caller must use the single shared store.
    pub fn enabled(&self) -> bool {
        self.config.enabled
    }

    /// Sync routing: the project's PRIMARY assigned-bucket store if a stripe
    /// has been warmed into the cache, else `None` (caller falls back to the
    /// default store). The primary is `stripe[0]`, so a single-bucket (`k==1`)
    /// project routes byte-identically to Stage 1. Never blocks, never errors
    /// — safe on the hot path.
    pub fn routed_store(&self, project: &ProjectId) -> Option<Arc<dyn ObjectStore>> {
        if !self.config.enabled {
            return None;
        }
        self.assignment_cache
            .read()
            .expect("assignment_cache poisoned")
            .get(project)
            .and_then(|stripe| stripe.first().cloned())
    }

    /// Sync routing: the FULL ordered stripe of stores for `project` if warmed,
    /// else `None`. Used by the union table read (LIST every bucket in the
    /// stripe). For a `k==1` project this is a one-element vec — the single
    /// LIST of today. Never blocks, never errors.
    pub fn routed_stores(&self, project: &ProjectId) -> Option<Vec<Arc<dyn ObjectStore>>> {
        if !self.config.enabled {
            return None;
        }
        self.assignment_cache
            .read()
            .expect("assignment_cache poisoned")
            .get(project)
            .cloned()
    }

    /// Sync routing: the store a given partition's DATA files PUT/read from,
    /// `stripe[stripe_slot(partition_id, k)]`. Deterministic + stable across
    /// restarts (the stripe order is persisted, the slot is a pure FNV-1a
    /// hash). Returns `None` when the project's stripe is not warmed (caller
    /// falls back to the default store for that one call). For a `k==1`
    /// project every partition resolves to the single primary bucket — the
    /// byte-identical Stage-1 behaviour.
    pub fn store_for_partition(
        &self,
        project: &ProjectId,
        partition_id: &str,
    ) -> Option<Arc<dyn ObjectStore>> {
        if !self.config.enabled {
            return None;
        }
        let cache = self.assignment_cache.read().expect("assignment_cache poisoned");
        let stripe = cache.get(project)?;
        if stripe.is_empty() {
            return None;
        }
        let slot = stripe_slot(partition_id, stripe.len());
        stripe.get(slot).cloned()
    }

    /// Drop any cached assignment + routed store for `project` (used by tests /
    /// simulated restarts so the next `ensure_assignment` re-reads the catalog).
    pub fn invalidate(&self, project: &ProjectId) {
        self.assignment_cache
            .write()
            .expect("assignment_cache poisoned")
            .remove(project);
    }

    /// Drop the ENTIRE per-process cache (assignment + resolved stores). Used
    /// to simulate a fresh process that must re-read every assignment from the
    /// catalog.
    pub fn invalidate_all(&self) {
        self.assignment_cache
            .write()
            .expect("assignment_cache poisoned")
            .clear();
        self.store_cache
            .write()
            .expect("store_cache poisoned")
            .clear();
    }

    /// Resolve (and cache) the [`ObjectStore`] for `bucket_id`, building it via
    /// the resolver from the registry entry on first use.
    fn store_for_bucket(
        &self,
        bucket_id: &str,
        registry: &BucketRegistry,
    ) -> Result<Arc<dyn ObjectStore>> {
        if let Some(s) = self
            .store_cache
            .read()
            .expect("store_cache poisoned")
            .get(bucket_id)
            .cloned()
        {
            return Ok(s);
        }
        let entry = registry.get(bucket_id).ok_or_else(|| {
            basin_common::BasinError::storage(format!(
                "bucket pool: assignment references unknown bucket_id {bucket_id}"
            ))
        })?;
        let store = self.resolver.resolve(entry)?;
        self.store_cache
            .write()
            .expect("store_cache poisoned")
            .insert(bucket_id.to_string(), store.clone());
        Ok(store)
    }

    /// Pick ONE bucket for a project against the working registry, mutating it
    /// in place (registering a fresh bucket when warranted, then bumping the
    /// chosen bucket's `assigned_count`). Factored out of the stripe chooser so
    /// each stripe slot reuses the identical Stage-1 least-loaded logic.
    ///
    /// `exclude` holds bucket ids already chosen for THIS project's stripe so a
    /// stripe never lands two slots on the same bucket (which would silently
    /// halve the striping width). When every bucket is excluded (stripe wider
    /// than the pool) the chooser falls back to allowing reuse — graceful
    /// degrade, never fail the write.
    ///
    /// NOTE (future): the richer load signal from the design doc (sustained PUT
    /// rate / bytes per second) would replace `assigned_count` here.
    fn choose_one_bucket(&self, registry: &mut BucketRegistry, exclude: &[String]) -> String {
        // No pooled buckets at all → register the first one. The bootstrap
        // bucket carries empty endpoint/credentials, which the engine's
        // resolver maps to the process-default store (the same single bucket
        // used today) — so enabling the flag on an existing single-bucket
        // deployment keeps every project on the original bucket.
        if registry.buckets.is_empty() {
            let id = "pool-0000".to_string();
            registry.buckets.push(BucketRegistryEntry {
                bucket_id: id.clone(),
                bucket_name: id.clone(),
                endpoint: String::new(),
                region: String::new(),
                credentials_ref: None,
                assigned_count: 0,
            });
            registry.bump_assigned(&id);
            return id;
        }

        // Candidate set = buckets not already in this project's stripe. If that
        // empties (stripe ≥ pool size), allow reuse.
        let has_unexcluded = registry
            .buckets
            .iter()
            .any(|b| !exclude.contains(&b.bucket_id));

        let min_load = registry
            .buckets
            .iter()
            .filter(|b| !has_unexcluded || !exclude.contains(&b.bucket_id))
            .map(|b| b.assigned_count)
            .min()
            .unwrap_or(0);

        // Grow only when EVERY candidate bucket is at/above the watermark and we
        // have headroom under the ceiling.
        if min_load >= self.config.watermark as u64
            && registry.buckets.len() < self.config.max_buckets
        {
            let id = format!("pool-{:04}", registry.buckets.len());
            registry.buckets.push(BucketRegistryEntry {
                bucket_id: id.clone(),
                bucket_name: id.clone(),
                endpoint: String::new(),
                region: String::new(),
                credentials_ref: None,
                assigned_count: 0,
            });
            registry.bump_assigned(&id);
            return id;
        }

        // Otherwise pack into the least-loaded candidate bucket. Ties broken by
        // bucket_id for determinism.
        let chosen = registry
            .buckets
            .iter()
            .filter(|b| !has_unexcluded || !exclude.contains(&b.bucket_id))
            .min_by(|a, b| {
                a.assigned_count
                    .cmp(&b.assigned_count)
                    .then_with(|| a.bucket_id.cmp(&b.bucket_id))
            })
            .map(|b| b.bucket_id.clone())
            .expect("candidate set non-empty");
        registry.bump_assigned(&chosen);
        chosen
    }

    /// Choose an ORDERED stripe set of distinct pooled buckets for a project
    /// that has no assignment yet. Width = `min(config.stripe, max_buckets)`,
    /// at least 1. Each slot reuses the Stage-1 least-loaded chooser, excluding
    /// buckets already in the stripe so slots land on distinct buckets where
    /// the pool is wide enough.
    ///
    /// Returns `(stripe, grown registry)`; the caller persists the grown
    /// registry (with every chosen bucket's `assigned_count` bumped) before
    /// persisting the assignment.
    fn choose_stripe(&self, registry: &BucketRegistry) -> (Vec<String>, BucketRegistry) {
        let mut registry = registry.clone();
        let width = self.config.stripe.clamp(1, self.config.max_buckets.max(1));
        let mut stripe: Vec<String> = Vec::with_capacity(width);
        for _ in 0..width {
            let id = self.choose_one_bucket(&mut registry, &stripe);
            stripe.push(id);
        }
        (stripe, registry)
    }

    /// Resolve an ordered stripe of bucket ids into an ordered stripe of
    /// stores, building each via the resolver (cached per bucket).
    fn stores_for_stripe(
        &self,
        stripe: &[String],
        registry: &BucketRegistry,
    ) -> Result<Vec<Arc<dyn ObjectStore>>> {
        stripe
            .iter()
            .map(|id| self.store_for_bucket(id, registry))
            .collect()
    }

    /// Ensure `project` has a stable bucket assignment (a possibly-striped
    /// ordered bucket set) and that its routed stripe of stores is cached for
    /// the sync path. Catalog-backed; idempotent.
    ///
    /// - Flag OFF → returns immediately, cache untouched (no-op).
    /// - Already cached → returns immediately.
    /// - Persisted assignment exists → cache its stripe of stores, done (the
    ///   stable, re-read-on-restart path; an old Stage-1 single-bucket record
    ///   yields a width-1 stripe via `effective_stripe`).
    /// - No assignment → choose a stripe (growing the registry if warranted),
    ///   persist via the catalog's create-if-absent CAS (first writer wins),
    ///   then cache the WINNER's stripe of stores.
    pub async fn ensure_assignment(
        &self,
        project: &ProjectId,
        catalog: &dyn Catalog,
    ) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }
        if self
            .assignment_cache
            .read()
            .expect("assignment_cache poisoned")
            .contains_key(project)
        {
            return Ok(());
        }

        // Fast path: a durable assignment already exists (e.g. after restart).
        if let Some(existing) = catalog.get_bucket_assignment(project).await? {
            let registry = catalog.get_bucket_registry().await?;
            let stripe = self.stores_for_stripe(&existing.effective_stripe(), &registry)?;
            self.assignment_cache
                .write()
                .expect("assignment_cache poisoned")
                .insert(*project, stripe);
            return Ok(());
        }

        // Slow path: first write for this project — choose + persist. Serialise
        // so concurrent first-writes don't race the registry growth.
        let _guard = self.grow_lock.lock().await;

        // Re-check under the lock (another task may have just assigned).
        if let Some(existing) = catalog.get_bucket_assignment(project).await? {
            let registry = catalog.get_bucket_registry().await?;
            let stripe = self.stores_for_stripe(&existing.effective_stripe(), &registry)?;
            self.assignment_cache
                .write()
                .expect("assignment_cache poisoned")
                .insert(*project, stripe);
            return Ok(());
        }

        let registry = catalog.get_bucket_registry().await?;
        let (chosen_stripe, grown) = self.choose_stripe(&registry);

        let proposed = BucketAssignment {
            bucket_id: chosen_stripe[0].clone(),
            tier: BucketTier::Pooled,
            stripe: chosen_stripe.clone(),
        };
        // CAS: first writer wins; we cache whatever became durable.
        let winner = catalog.assign_bucket_if_absent(project, &proposed).await?;

        // Persist the (count-bumped, possibly grown) registry only when WE won
        // the assignment to the stripe we chose — otherwise the durable counts
        // belong to whoever won. `grown` already references the chosen stripe's
        // buckets (choose_stripe bumped each); if we lost we skip the write and
        // just route to the winner's stripe.
        let we_won = winner.stripe == proposed.stripe && winner.bucket_id == proposed.bucket_id;
        if we_won {
            catalog.put_bucket_registry(&grown).await?;
        }

        let registry_for_store = if we_won {
            grown
        } else {
            // Lost the race to a different stripe; re-read so the store lookups
            // see the winner's registry entries.
            catalog.get_bucket_registry().await?
        };
        let stripe = self.stores_for_stripe(&winner.effective_stripe(), &registry_for_store)?;
        self.assignment_cache
            .write()
            .expect("assignment_cache poisoned")
            .insert(*project, stripe);
        Ok(())
    }
}
