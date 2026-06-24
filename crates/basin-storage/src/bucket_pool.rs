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

/// Default pool ceiling when `BASIN_BUCKET_POOL_MAX` is unset/invalid.
const DEFAULT_POOL_MAX: usize = 8;
/// Default occupancy watermark when `BASIN_BUCKET_POOL_WATERMARK` is unset.
const DEFAULT_WATERMARK: usize = 64;

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
        Self {
            enabled,
            max_buckets,
            watermark,
        }
    }
}

/// The per-process bucket pool. Holds the config, the assignment cache, the
/// resolved-store cache, and the resolver. Inert (and never consulted) when
/// `config.enabled` is false.
pub struct BucketPool {
    config: PoolConfig,
    /// Per-process cache of `project → assigned bucket store`. Populated by
    /// [`ensure_assignment`]; read by the sync routing path. `None` would mean
    /// "no cached assignment" — routing then falls back to the default store
    /// for that one call.
    assignment_cache: RwLock<HashMap<ProjectId, Arc<dyn ObjectStore>>>,
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

    /// Sync routing: the assigned-bucket store for `project` if one has been
    /// warmed into the cache, else `None` (caller falls back to the default
    /// store). Never blocks, never errors — safe on the hot path.
    pub fn routed_store(&self, project: &ProjectId) -> Option<Arc<dyn ObjectStore>> {
        if !self.config.enabled {
            return None;
        }
        self.assignment_cache
            .read()
            .expect("assignment_cache poisoned")
            .get(project)
            .cloned()
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

    /// Choose a bucket for a project that has no assignment yet.
    ///
    /// Stage 1 load signal = `assigned_count` per bucket (carried in the
    /// registry). The registry is grown lazily: if every existing pooled
    /// bucket is at/above the occupancy watermark and we're below the pool
    /// ceiling, register a fresh bucket; otherwise pick the least-loaded
    /// existing bucket (graceful degrade at the ceiling — never fail the
    /// write).
    ///
    /// Returns `(bucket_id, grown registry)`; the caller persists the grown
    /// registry before persisting the assignment. The returned registry has
    /// the chosen bucket's `assigned_count` already incremented.
    ///
    /// NOTE (future): the richer load signal from the design doc (sustained PUT
    /// rate / bytes per second) would replace `assigned_count` here, sourced
    /// from the per-bucket aggregation of the existing inflight/metrics
    /// counters.
    fn choose_bucket(&self, registry: &BucketRegistry) -> (String, BucketRegistry) {
        let mut registry = registry.clone();

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
            return (id, registry);
        }

        let min_load = registry
            .buckets
            .iter()
            .map(|b| b.assigned_count)
            .min()
            .unwrap_or(0);

        // Grow only when EVERY bucket is at/above the watermark and we have
        // headroom under the ceiling.
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
            return (id, registry);
        }

        // Otherwise pack into the least-loaded existing bucket. Ties broken by
        // bucket_id for determinism.
        let chosen = registry
            .buckets
            .iter()
            .min_by(|a, b| {
                a.assigned_count
                    .cmp(&b.assigned_count)
                    .then_with(|| a.bucket_id.cmp(&b.bucket_id))
            })
            .map(|b| b.bucket_id.clone())
            .expect("registry non-empty");
        registry.bump_assigned(&chosen);
        (chosen, registry)
    }

    /// Ensure `project` has a stable bucket assignment and that its routed
    /// store is cached for the sync path. Catalog-backed; idempotent.
    ///
    /// - Flag OFF → returns immediately, cache untouched (no-op).
    /// - Already cached → returns immediately.
    /// - Persisted assignment exists → cache its store, done (the stable,
    ///   re-read-on-restart path).
    /// - No assignment → choose a bucket (growing the registry if warranted),
    ///   persist via the catalog's create-if-absent CAS (first writer wins),
    ///   then cache the WINNER's store.
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
            let store = self.store_for_bucket(&existing.bucket_id, &registry)?;
            self.assignment_cache
                .write()
                .expect("assignment_cache poisoned")
                .insert(*project, store);
            return Ok(());
        }

        // Slow path: first write for this project — choose + persist. Serialise
        // so concurrent first-writes don't race the registry growth.
        let _guard = self.grow_lock.lock().await;

        // Re-check under the lock (another task may have just assigned).
        if let Some(existing) = catalog.get_bucket_assignment(project).await? {
            let registry = catalog.get_bucket_registry().await?;
            let store = self.store_for_bucket(&existing.bucket_id, &registry)?;
            self.assignment_cache
                .write()
                .expect("assignment_cache poisoned")
                .insert(*project, store);
            return Ok(());
        }

        let registry = catalog.get_bucket_registry().await?;
        let (bucket_id, grown) = self.choose_bucket(&registry);

        let proposed = BucketAssignment {
            bucket_id: bucket_id.clone(),
            tier: BucketTier::Pooled,
        };
        // CAS: first writer wins; we cache whatever became durable.
        let winner = catalog.assign_bucket_if_absent(project, &proposed).await?;

        // Persist the (count-bumped, possibly grown) registry only when WE won
        // the assignment to the bucket we chose — otherwise the durable count
        // belongs to whoever won. `grown` already references `winner.bucket_id`
        // when we won (choose_bucket bumped it); if we lost we skip the write
        // and just route to the winner's bucket.
        if winner.bucket_id == bucket_id {
            catalog.put_bucket_registry(&grown).await?;
        }

        let registry_for_store = if winner.bucket_id == bucket_id {
            grown
        } else {
            // Lost the race to a different bucket; re-read so the store lookup
            // sees the winner's registry entry.
            catalog.get_bucket_registry().await?
        };
        let store = self.store_for_bucket(&winner.bucket_id, &registry_for_store)?;
        self.assignment_cache
            .write()
            .expect("assignment_cache poisoned")
            .insert(*project, store);
        Ok(())
    }
}
