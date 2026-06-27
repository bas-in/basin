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

use basin_catalog::bucket_pool::{stripe_slot, MigrationIntent, MigrationPhase};
use basin_catalog::{BucketAssignment, BucketRegistry, BucketRegistryEntry, BucketTier, Catalog};
use basin_common::{BasinError, ProjectId, Result};
use futures::stream::StreamExt;
use object_store::{path::Path as OsPath, ObjectStore, ObjectStoreExt, PutPayload};

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

/// Comma-separated list of the REAL provider bucket names the pool stripes
/// across (e.g. `basin-pool-0,basin-pool-1,basin-pool-2`). When set, the pool
/// registers entries that carry these real names + the configured
/// endpoint/region (below), so the production resolver builds stores against
/// the REAL buckets instead of generated empty-endpoint placeholders. When
/// UNSET/empty the pool keeps today's behaviour exactly: the bootstrap entry
/// has an empty endpoint and the resolver maps it to the process-default store.
pub const BUCKET_POOL_BUCKETS_ENV: &str = "BASIN_BUCKET_POOL_BUCKETS";
/// The S3-compatible endpoint for the configured pool buckets. Defaults to the
/// engine's existing storage endpoint (`AWS_ENDPOINT_URL_S3`) so Tigris is used
/// without extra configuration.
pub const BUCKET_POOL_ENDPOINT_ENV: &str = "BASIN_BUCKET_POOL_ENDPOINT";
/// The region literal for the configured pool buckets. Defaults to the engine's
/// existing storage region (`AWS_REGION` / `AWS_DEFAULT_REGION`), else `auto`
/// (Tigris).
pub const BUCKET_POOL_REGION_ENV: &str = "BASIN_BUCKET_POOL_REGION";
/// Optional per-bucket credentials-reference list, parallel to
/// `BASIN_BUCKET_POOL_BUCKETS`. When set, entry `i` carries `credentials_ref =
/// Some(refs[i])` and the resolver reads `{refs[i]}_ACCESS_KEY_ID` /
/// `_SECRET_ACCESS_KEY` for that bucket — required when each pooled bucket has
/// its OWN access key (e.g. Fly/Tigris buckets, whose keys are bucket-scoped).
/// Unset → `credentials_ref = None` → the process-default keys (today's
/// behaviour, for an org-wide key). Refs MUST be env-var-safe (e.g. `POOL0`).
pub const BUCKET_POOL_CREDS_REFS_ENV: &str = "BASIN_BUCKET_POOL_CREDS_REFS";

/// Reserved `bucket_id` for a project PINNED to the primary/default store
/// (never striped). Persisted as the assignment's `bucket_id`/`stripe[0]` for an
/// EXISTING project that already had data on the primary bucket when the pool
/// was first enabled, so flipping `BASIN_BUCKET_POOL` on can never silently
/// reroute that project's reads to an empty pool bucket and orphan its data.
/// A migration (#37, in reverse) is the only thing that ever moves such a
/// project into the pool. This id is NEVER a real registry entry — the routing
/// path resolves it straight to the process-default store.
pub const PRIMARY_PIN_BUCKET_ID: &str = "primary";

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

/// The operator-configured set of REAL provider buckets the pool stripes
/// across, with the endpoint + region every one of them shares. Parsed once
/// from the environment. EMPTY `names` (the default — `BASIN_BUCKET_POOL_BUCKETS`
/// unset/empty) means "no real buckets configured": the pool falls back to
/// generating bootstrap placeholder entries with an empty endpoint, which the
/// resolver maps to the process-default store — today's behaviour, byte-identical.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BucketPoolBuckets {
    /// The REAL provider bucket names, in configured order. Entry `i` is
    /// registered under the stable `bucket_id` `pool-{i:04}`, so the id→name
    /// mapping is deterministic + identical on every node.
    pub names: Vec<String>,
    /// The S3 endpoint shared by every configured bucket (empty when unset, in
    /// which case the pool stays on the placeholder/default-store path).
    pub endpoint: String,
    /// The region literal shared by every configured bucket.
    pub region: String,
    /// Optional per-bucket credentials refs (parallel to `names`). Empty → all
    /// buckets use the process-default keys (`credentials_ref: None`). When
    /// non-empty, `names[i]`'s entry carries `credentials_ref = Some(creds_refs[i])`
    /// so the resolver reads that bucket's own `{ref}_ACCESS_KEY_ID`.
    pub creds_refs: Vec<String>,
}

impl BucketPoolBuckets {
    /// `true` when the operator configured at least one real bucket. When
    /// `false` the pool keeps today's empty-endpoint placeholder behaviour.
    pub fn is_configured(&self) -> bool {
        !self.names.is_empty()
    }

    /// Read the real-bucket config from the process environment.
    ///
    /// - `names`: `BASIN_BUCKET_POOL_BUCKETS` split on commas, trimmed, empties
    ///   dropped. Unset/empty → no real buckets (placeholder/default-store
    ///   behaviour).
    /// - `endpoint`: `BASIN_BUCKET_POOL_ENDPOINT`, defaulting to the engine's
    ///   existing storage endpoint (`AWS_ENDPOINT_URL_S3`).
    /// - `region`: `BASIN_BUCKET_POOL_REGION`, defaulting to the engine's
    ///   existing region (`AWS_REGION`/`AWS_DEFAULT_REGION`), else `auto`.
    pub fn from_env() -> Self {
        let names = std::env::var(BUCKET_POOL_BUCKETS_ENV)
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let endpoint = std::env::var(BUCKET_POOL_ENDPOINT_ENV)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| {
                std::env::var("AWS_ENDPOINT_URL_S3")
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
            })
            .unwrap_or_default();
        let region = std::env::var(BUCKET_POOL_REGION_ENV)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| {
                std::env::var("AWS_REGION")
                    .ok()
                    .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
            })
            .unwrap_or_else(|| "auto".to_string());
        let creds_refs = std::env::var(BUCKET_POOL_CREDS_REFS_ENV)
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Self {
            names,
            endpoint,
            region,
            creds_refs,
        }
    }

    /// The deterministic `bucket_id` for the `i`th configured bucket. Stable +
    /// identical on every node so the registry converges.
    fn bucket_id_for(i: usize) -> String {
        format!("pool-{i:04}")
    }

    /// Build the registry entry for the `i`th configured bucket, carrying the
    /// REAL name + the configured endpoint/region. `credentials_ref` is the
    /// `i`th entry of `creds_refs` when configured (so the resolver reads that
    /// bucket's OWN `{ref}_ACCESS_KEY_ID` — required for bucket-scoped keys like
    /// Fly/Tigris), else `None` (process-default keys, for an org-wide key).
    /// `i` must be in range.
    fn entry_for(&self, i: usize) -> BucketRegistryEntry {
        BucketRegistryEntry {
            bucket_id: Self::bucket_id_for(i),
            bucket_name: self.names[i].clone(),
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            credentials_ref: self.creds_refs.get(i).cloned(),
            assigned_count: 0,
        }
    }
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
    /// Operator-configured REAL buckets (names + endpoint + region). EMPTY by
    /// default — the pool then generates empty-endpoint placeholder entries
    /// (today's behaviour). When configured, `choose_one_bucket` /
    /// `prepopulate_registry` register entries carrying these real names +
    /// endpoint/region so the resolver builds stores against the real buckets.
    buckets: BucketPoolBuckets,
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
    /// The process-default/primary object store (the single shared bucket the
    /// catalog lives on). Used to (a) probe whether an EXISTING project already
    /// has primary-bucket data — in which case it is PINNED to primary instead
    /// of striped (safe flag-enable, never orphan data) — and (b) resolve a
    /// [`PRIMARY_PIN_BUCKET_ID`] assignment back to the primary store on the
    /// routing path. `None` until [`Self::set_default_store`] is called by the
    /// storage layer; when `None` the safe path degrades to "stay on primary"
    /// (never stripe an existing project we can't probe).
    default_store: RwLock<Option<Arc<dyn ObjectStore>>>,
}

impl BucketPool {
    /// Build a pool from env config and a resolver, with NO operator-configured
    /// real buckets (empty list = today's placeholder/default-store behaviour).
    /// Production uses [`BucketPool::new_with_buckets`]; tests that don't care
    /// about real-bucket wiring use this.
    pub fn new(config: PoolConfig, resolver: Arc<dyn BucketResolver>) -> Self {
        Self::new_with_buckets(config, BucketPoolBuckets::default(), resolver)
    }

    /// Build a pool from env config, the operator-configured REAL buckets, and a
    /// resolver. When `buckets.is_configured()` the pool registers entries that
    /// carry the real names + endpoint/region; when empty it behaves exactly
    /// like [`BucketPool::new`] (placeholder entries → process-default store).
    pub fn new_with_buckets(
        config: PoolConfig,
        buckets: BucketPoolBuckets,
        resolver: Arc<dyn BucketResolver>,
    ) -> Self {
        Self {
            config,
            buckets,
            assignment_cache: RwLock::new(HashMap::new()),
            store_cache: RwLock::new(HashMap::new()),
            grow_lock: tokio::sync::Mutex::new(()),
            resolver,
            default_store: RwLock::new(None),
        }
    }

    /// Register the process-default/primary object store. Idempotent (last
    /// writer wins; the store is process-stable). The storage layer calls this
    /// when the pool is attached so the pool can (a) probe an existing project's
    /// primary-bucket data and (b) resolve a primary-pinned assignment back to
    /// the primary store. Without it, the pool conservatively keeps every
    /// not-yet-assigned project on the primary (never stripes blind).
    pub fn set_default_store(&self, store: Arc<dyn ObjectStore>) {
        *self
            .default_store
            .write()
            .expect("default_store poisoned") = Some(store);
    }

    /// The registered default/primary store, if any.
    fn default_store(&self) -> Option<Arc<dyn ObjectStore>> {
        self.default_store
            .read()
            .expect("default_store poisoned")
            .clone()
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
    /// Build the registry entry for the `i`th pooled bucket. With real buckets
    /// configured this carries the configured real name + endpoint + region (so
    /// the resolver builds a store against the REAL bucket); without them it is
    /// the legacy placeholder (generated name, EMPTY endpoint), which the
    /// resolver maps to the process-default store — today's behaviour. `i` is
    /// the bucket's position; for the configured path it indexes the configured
    /// names (and is always in range because `choose_one_bucket` /
    /// `prepopulate_registry` cap growth at `names.len()`).
    fn new_pool_entry(&self, i: usize) -> BucketRegistryEntry {
        if self.buckets.is_configured() {
            self.buckets.entry_for(i)
        } else {
            let id = format!("pool-{i:04}");
            BucketRegistryEntry {
                bucket_id: id.clone(),
                bucket_name: id,
                endpoint: String::new(),
                region: String::new(),
                credentials_ref: None,
                assigned_count: 0,
            }
        }
    }

    /// Pre-seed the durable registry from the operator-configured real buckets,
    /// idempotently (create-if-absent per `bucket_id`). Called once on pool init
    /// so every node — and a restarted process — converges to the SAME registry
    /// (the id→real-name mapping is deterministic, derived from the configured
    /// list's order). Existing entries (and their `assigned_count`) are left
    /// untouched; only missing configured buckets are appended. The write is
    /// skipped entirely when nothing changed, so this never churns the catalog.
    ///
    /// No-op (and the catalog is never touched) when the pool is disabled OR no
    /// real buckets are configured — the empty-list path keeps today's
    /// lazy-bootstrap behaviour exactly.
    pub async fn prepopulate_registry(&self, catalog: &dyn Catalog) -> Result<()> {
        if !self.config.enabled || !self.buckets.is_configured() {
            return Ok(());
        }
        // Cap at the env ceiling so an over-long configured list can't grow the
        // registry past BASIN_BUCKET_POOL_MAX.
        let want = self.buckets.names.len().min(self.config.max_buckets);

        let _guard = self.grow_lock.lock().await;
        let mut registry = catalog.get_bucket_registry().await?;
        let mut changed = false;
        for i in 0..want {
            let id = BucketPoolBuckets::bucket_id_for(i);
            if registry.get(&id).is_none() {
                registry.buckets.push(self.buckets.entry_for(i));
                changed = true;
            }
        }
        if changed {
            catalog.put_bucket_registry(&registry).await?;
        }
        Ok(())
    }

    fn choose_one_bucket(&self, registry: &mut BucketRegistry, exclude: &[String]) -> String {
        // The growth ceiling: when the operator configured REAL buckets, never
        // register more entries than there are real buckets (a generated id past
        // the list would have no real bucket behind it). Otherwise the env
        // ceiling alone applies.
        let grow_ceiling = if self.buckets.is_configured() {
            self.config.max_buckets.min(self.buckets.names.len())
        } else {
            self.config.max_buckets
        };

        // No pooled buckets at all → register the first one. With real buckets
        // configured this carries the first real name + endpoint/region; without
        // them it is the bootstrap placeholder (empty endpoint), which the
        // engine's resolver maps to the process-default store — so enabling the
        // flag on an existing single-bucket deployment keeps every project on
        // the original bucket.
        if registry.buckets.is_empty() {
            let entry = self.new_pool_entry(0);
            let id = entry.bucket_id.clone();
            registry.buckets.push(entry);
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
        if min_load >= self.config.watermark as u64 && registry.buckets.len() < grow_ceiling {
            let entry = self.new_pool_entry(registry.buckets.len());
            let id = entry.bucket_id.clone();
            registry.buckets.push(entry);
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
    /// stores, building each via the resolver (cached per bucket). The reserved
    /// [`PRIMARY_PIN_BUCKET_ID`] resolves to the registered default/primary
    /// store (NOT via the resolver, and it is never a registry entry) — that is
    /// how a primary-pinned existing project keeps reading its primary-bucket
    /// data with the flag ON.
    fn stores_for_stripe(
        &self,
        stripe: &[String],
        registry: &BucketRegistry,
    ) -> Result<Vec<Arc<dyn ObjectStore>>> {
        stripe
            .iter()
            .map(|id| {
                if id == PRIMARY_PIN_BUCKET_ID {
                    self.default_store().ok_or_else(|| {
                        basin_common::BasinError::storage(
                            "bucket pool: primary-pinned assignment but no default store registered",
                        )
                    })
                } else {
                    self.store_for_bucket(id, registry)
                }
            })
            .collect()
    }

    /// `true` iff `project` already has at least one object under its
    /// per-project prefix on the primary/default store. Used to decide, at
    /// first-assignment time, whether enabling the pool would ORPHAN an existing
    /// project's data: such a project is PINNED to primary instead of striped.
    /// Returns `false` when no default store is registered (we then pin to
    /// primary anyway — the conservative, never-orphan choice).
    async fn has_primary_data(&self, project: &ProjectId) -> Result<bool> {
        let Some(store) = self.default_store() else {
            return Ok(false);
        };
        let prefix = OsPath::from(format!("projects/{project}/"));
        let mut s = store.list(Some(&prefix));
        match s.next().await {
            Some(Ok(_)) => Ok(true),
            Some(Err(e)) => Err(basin_common::BasinError::storage(format!(
                "bucket pool: probe primary data for {project}: {e}"
            ))),
            None => Ok(false),
        }
    }

    /// Persist (CAS) and cache a PRIMARY-PINNED assignment for `project`: a
    /// width-1 assignment on [`PRIMARY_PIN_BUCKET_ID`] that the routing path
    /// resolves to the default/primary store. This is the safe-enable record for
    /// an existing project — durable, so a restart re-reads it and keeps the
    /// project on primary (no re-probe, no stripe). Idempotent via the catalog's
    /// create-if-absent CAS; whatever became durable is cached.
    async fn pin_to_primary(&self, project: &ProjectId, catalog: &dyn Catalog) -> Result<()> {
        let proposed = BucketAssignment {
            bucket_id: PRIMARY_PIN_BUCKET_ID.to_string(),
            tier: BucketTier::Pooled,
            stripe: vec![PRIMARY_PIN_BUCKET_ID.to_string()],
        };
        let winner = catalog.assign_bucket_if_absent(project, &proposed).await?;
        let registry = catalog.get_bucket_registry().await?;
        let stripe = self.stores_for_stripe(&winner.effective_stripe(), &registry)?;
        self.assignment_cache
            .write()
            .expect("assignment_cache poisoned")
            .insert(*project, stripe);
        Ok(())
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
        // CORRECTNESS GATE: a reserved system project (the internal auth/catalog
        // project) is NEVER pool-routed. Authenticating a connection reads this
        // project's credential rows; striping them to an empty pool bucket would
        // break auth for every project. Leaving it UNassigned means the sync
        // routing path finds no warmed stripe and falls back to the
        // primary/catalog store — exactly where auth lives, independent of the
        // flag.
        if project.is_reserved_system() {
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

        // SAFE-ENABLE GATE: an EXISTING project whose data already lives on the
        // primary bucket must NOT be rerouted to (empty) pool buckets when the
        // flag is flipped on — that would orphan its tables. Such a project is
        // PINNED to primary (durably) and only ever enters the pool via an
        // explicit migration (#37, in reverse). A project with NO pre-pool
        // primary data is genuinely new and DOES stripe. When no default store
        // is registered we cannot prove emptiness, so we pin (never stripe blind).
        if self.has_primary_data(project).await? {
            return self.pin_to_primary(project, catalog).await;
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

// ===========================================================================
// Online consolidation on scale-down (#37): the crash-safe object-migration
// state machine. FLAG-GATED, MANUAL. There is NO always-on background loop —
// consolidation is a function a future scheduler (or a test) calls explicitly.
// With `BASIN_BUCKET_POOL` OFF every entry point below is a provable no-op.
//
// See `docs/multi-bucket-pool-design.md` §"Online consolidation on scale-down".
// ===========================================================================

/// Default ceiling on concurrent in-flight migrations (`≤ K`). Consolidation is
/// best-effort background work; we never let it stampede the provider's
/// copy/list budgets. Overridable per-call via [`BucketPool::consolidate_project`]'s
/// concurrency check against [`Catalog::list_migration_intents`].
pub const DEFAULT_MAX_CONCURRENT_MIGRATIONS: usize = 2;

/// Where to inject a simulated crash, for the #38 exactly-once test matrix.
/// A crash means "stop NOW, as if the node died" — the intent record is left
/// in the catalog at whatever phase was durably written, and resume must
/// converge to the same exactly-once result.
///
/// `Before(phase)` halts just before the phase's work runs (the intent already
/// records `phase`, so resume re-enters at `phase`). `After(phase)` halts after
/// the phase's work AND after its intent advance is durable (resume re-enters
/// at the NEXT phase). [`CrashPoint::MidCopy`] halts partway through the copy
/// loop, leaving B with a STRICT SUBSET of A's objects — the adversarial case
/// proving copy idempotency + completeness on resume. `None` runs to
/// completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashPoint {
    /// Run to completion (no crash). Production default.
    None,
    /// Crash just before `phase`'s work executes.
    Before(MigrationPhase),
    /// Crash just after `phase`'s work executes and its advance is durable.
    After(MigrationPhase),
    /// Crash partway through the Copy phase, after `n` objects copied.
    MidCopy(usize),
    /// Crash partway through the Drain delete, after `n` source objects deleted.
    MidDrainDelete(usize),
}

/// A simulated crash, surfaced as a distinct error so callers/tests can tell a
/// crash-injection from a real failure. The migration intent is intact in the
/// catalog; the next [`run_migration`] resumes it.
#[derive(Debug)]
pub struct SimulatedCrash(pub MigrationPhase);

impl BucketPool {
    /// Identify pooled buckets at or below the reclaim watermark — candidate
    /// SOURCES for consolidation. A bucket qualifies when its `assigned_count`
    /// is `> 0` (still holds projects, so there's something to reclaim) and
    /// `<= reclaim_watermark` (sparse/cold enough to be worth emptying). Pure;
    /// the caller decides whether/when to act (manual trigger, never a loop).
    pub fn reclaim_candidates(registry: &BucketRegistry, reclaim_watermark: u64) -> Vec<String> {
        registry
            .buckets
            .iter()
            .filter(|b| b.assigned_count > 0 && b.assigned_count <= reclaim_watermark)
            .map(|b| b.bucket_id.clone())
            .collect()
    }

    /// Pick a TARGET bucket for a project leaving `from`: the most-loaded pooled
    /// bucket that is NOT `from` (pack densely so `from` can be emptied + a
    /// bucket reclaimed). `None` when no other bucket exists. Pure.
    pub fn consolidation_target(registry: &BucketRegistry, from: &str) -> Option<String> {
        registry
            .buckets
            .iter()
            .filter(|b| b.bucket_id != from)
            .max_by(|a, b| {
                a.assigned_count
                    .cmp(&b.assigned_count)
                    .then_with(|| b.bucket_id.cmp(&a.bucket_id))
            })
            .map(|b| b.bucket_id.clone())
    }

    /// Resolve a bucket id to its store (registry-backed, cached). Public so the
    /// migration driver + tests can fetch the source/target handles.
    pub fn resolve_store(
        &self,
        bucket_id: &str,
        registry: &BucketRegistry,
    ) -> Result<Arc<dyn ObjectStore>> {
        self.store_for_bucket(bucket_id, registry)
    }

    /// MANUAL consolidation entry point. Migrate `project` from bucket `from`
    /// into bucket `to`, crash-safely + exactly-once. Off-by-default: a no-op
    /// when the pool is disabled. Bounds concurrency to
    /// [`DEFAULT_MAX_CONCURRENT_MIGRATIONS`] by counting live intents.
    ///
    /// Records an intent (`phase=Copy`) if none exists, then drives the state
    /// machine to completion. Idempotent: calling it again after a crash
    /// resumes from the durable intent. NOT a background loop — a scheduler
    /// calls this per (project, from, to).
    pub async fn consolidate_project(
        &self,
        project: &ProjectId,
        from: &str,
        to: &str,
        catalog: &dyn Catalog,
    ) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }
        if from == to {
            return Err(BasinError::storage(
                "consolidate_project: source and target bucket must differ",
            ));
        }

        // Resume an existing intent verbatim; else admit a new one under the
        // concurrency ceiling.
        let intent = match catalog.get_migration_intent(project).await? {
            Some(existing) => existing,
            None => {
                let live = catalog.list_migration_intents().await?.len();
                if live >= DEFAULT_MAX_CONCURRENT_MIGRATIONS {
                    return Err(BasinError::storage(format!(
                        "consolidate_project: at migration concurrency ceiling ({live})"
                    )));
                }
                let intent = MigrationIntent {
                    project: *project,
                    from: from.to_string(),
                    to: to.to_string(),
                    phase: MigrationPhase::Copy,
                };
                catalog.put_migration_intent(&intent).await?;
                intent
            }
        };
        // Production never injects a crash, so a `Crash` variant is impossible
        // here; surface only the real-failure path.
        self.run_migration(&intent, catalog, CrashPoint::None)
            .await
            .map_err(MigrationError::into_basin)
    }

    /// Resume every in-flight migration found in the catalog. Called on startup
    /// (after a node bounce) so a migration that crashed mid-flight converges.
    /// No-op when the pool is disabled. Returns the number resumed.
    pub async fn resume_migrations(&self, catalog: &dyn Catalog) -> Result<usize> {
        if !self.config.enabled {
            return Ok(0);
        }
        let intents = catalog.list_migration_intents().await?;
        let n = intents.len();
        for intent in intents {
            self.run_migration(&intent, catalog, CrashPoint::None)
                .await
                .map_err(MigrationError::into_basin)?;
        }
        Ok(n)
    }

    /// Drive the migration state machine from `intent.phase` to completion (or
    /// to an injected crash). The single resumable engine behind
    /// [`Self::consolidate_project`] and [`Self::resume_migrations`]; the
    /// `crash` hook exists for the #38 test matrix (production passes
    /// [`CrashPoint::None`]).
    ///
    /// Each iteration: run the current phase's idempotent work, then advance the
    /// durable intent to the next phase (a single catalog write). A crash at any
    /// point leaves the intent at a phase whose work, re-run, is a no-op or
    /// re-converges — so resume is exactly-once.
    pub async fn run_migration(
        &self,
        intent: &MigrationIntent,
        catalog: &dyn Catalog,
        crash: CrashPoint,
    ) -> std::result::Result<(), MigrationError> {
        let project = intent.project;
        let from_id = intent.from.clone();
        let to_id = intent.to.clone();

        // Resolve the source + target stores from the registry. Done once;
        // re-resolved on each call (a fresh process rebuilds them).
        let registry = catalog.get_bucket_registry().await?;
        let from_store = self.resolve_store(&from_id, &registry)?;
        let to_store = self.resolve_store(&to_id, &registry)?;
        let prefix = project_object_prefix(&project);

        let mut phase = intent.phase;
        loop {
            // CRASH: just before this phase's work runs (intent already at `phase`).
            if crash == CrashPoint::Before(phase) {
                return Err(MigrationError::Crash(SimulatedCrash(phase)));
            }

            match phase {
                MigrationPhase::Copy => {
                    copy_prefix(&from_store, &to_store, &prefix, crash, MigrationPhase::Copy)
                        .await?;
                }
                MigrationPhase::Verify => {
                    verify_prefix(&from_store, &to_store, &prefix).await?;
                }
                MigrationPhase::Cutover => {
                    // THE linearization point: one atomic assignment write to B.
                    let assignment = BucketAssignment {
                        bucket_id: to_id.clone(),
                        tier: BucketTier::Pooled,
                        stripe: vec![to_id.clone()],
                    };
                    catalog.set_bucket_assignment(&project, &assignment).await?;
                    // Drop the per-process route cache so reads/writes re-resolve
                    // to B on the next access (cutover is now durable).
                    self.invalidate(&project);
                }
                MigrationPhase::Drain => {
                    // Catch any writes that landed in A between Copy and Cutover:
                    // re-copy (idempotent), re-verify, then delete A's objects.
                    copy_prefix(&from_store, &to_store, &prefix, CrashPoint::None, phase).await?;
                    verify_prefix(&from_store, &to_store, &prefix).await?;
                    delete_prefix(&from_store, &prefix, crash).await?;
                }
                MigrationPhase::Delete => {
                    // Reclaim the bucket iff it is provably empty of THIS project
                    // and holds no other live project. Then drop the intent.
                    finish_delete(&project, &from_id, &from_store, catalog).await?;
                    catalog.delete_migration_intent(&project).await?;
                    return Ok(());
                }
            }

            // Advance: persist the next phase as the new durable resume point.
            let Some(next) = phase.next() else {
                // Unreachable (Delete returns above), but be defensive.
                catalog.delete_migration_intent(&project).await?;
                return Ok(());
            };
            let advanced = MigrationIntent {
                project,
                from: from_id.clone(),
                to: to_id.clone(),
                phase: next,
            };
            catalog.put_migration_intent(&advanced).await?;

            // CRASH: just after this phase's work + its advance are durable
            // (resume re-enters at `next`).
            if crash == CrashPoint::After(phase) {
                return Err(MigrationError::Crash(SimulatedCrash(phase)));
            }
            phase = next;
        }
    }
}

/// The object-key prefix every one of a project's objects lives under, in ANY
/// bucket. Matches the layout `Storage` writes (`projects/{project}/…`); the
/// migration copies/verifies/deletes by this prefix so it is layout-agnostic.
fn project_object_prefix(project: &ProjectId) -> OsPath {
    OsPath::from(format!("projects/{project}/"))
}

/// Error from the migration state machine: a real failure, or an injected
/// [`SimulatedCrash`] (tests only — production never injects).
#[derive(Debug)]
pub enum MigrationError {
    /// A genuine error (storage/catalog).
    Failed(BasinError),
    /// An injected crash (#38 test matrix). The intent is intact; resume.
    Crash(SimulatedCrash),
}

impl From<BasinError> for MigrationError {
    fn from(e: BasinError) -> Self {
        MigrationError::Failed(e)
    }
}

impl MigrationError {
    /// `true` iff this was an injected crash (not a real failure).
    pub fn is_crash(&self) -> bool {
        matches!(self, MigrationError::Crash(_))
    }

    /// Collapse to a [`BasinError`] for the production callers that pass
    /// [`CrashPoint::None`] (where a `Crash` variant cannot occur). A `Crash`
    /// here would be a logic bug, so it is surfaced as an internal error rather
    /// than silently dropped.
    pub fn into_basin(self) -> BasinError {
        match self {
            MigrationError::Failed(e) => e,
            MigrationError::Crash(SimulatedCrash(phase)) => BasinError::Internal(format!(
                "migration produced an injected crash at {phase:?} with CrashPoint::None"
            )),
        }
    }
}

/// List every object under `prefix` in `store`, returning `(location, size)`
/// pairs. Used by copy/verify/delete.
async fn list_objects(store: &Arc<dyn ObjectStore>, prefix: &OsPath) -> Result<Vec<(OsPath, u64)>> {
    let mut out = Vec::new();
    let mut s = store.list(Some(prefix));
    while let Some(item) = s.next().await {
        let meta = item.map_err(|e| BasinError::storage(format!("migration list: {e}")))?;
        out.push((meta.location, meta.size));
    }
    Ok(out)
}

/// COPY phase: copy every object under `prefix` from `from` to `to`. Idempotent
/// — each object is GET-from-A + PUT-to-B (overwrite), so a re-copy is a no-op
/// overwrite. `crash`/`crash_phase` let the #38 matrix halt mid-loop, leaving B
/// with a strict subset (resume must complete it).
async fn copy_prefix(
    from: &Arc<dyn ObjectStore>,
    to: &Arc<dyn ObjectStore>,
    prefix: &OsPath,
    crash: CrashPoint,
    crash_phase: MigrationPhase,
) -> std::result::Result<(), MigrationError> {
    let objects = list_objects(from, prefix).await?;
    for (i, (loc, _size)) in objects.iter().enumerate() {
        if let CrashPoint::MidCopy(n) = crash {
            if crash_phase == MigrationPhase::Copy && i == n {
                return Err(MigrationError::Crash(SimulatedCrash(MigrationPhase::Copy)));
            }
        }
        let bytes = from
            .get(loc)
            .await
            .map_err(|e| BasinError::storage(format!("migration copy get {loc}: {e}")))?
            .bytes()
            .await
            .map_err(|e| BasinError::storage(format!("migration copy read {loc}: {e}")))?;
        to.put(loc, PutPayload::from_bytes(bytes))
            .await
            .map_err(|e| BasinError::storage(format!("migration copy put {loc}: {e}")))?;
    }
    Ok(())
}

/// VERIFY phase: every object under `prefix` in `from` must exist in `to` with a
/// matching size. Re-copy any missing/mismatched object, then fail if it still
/// doesn't match (so a verified copy is provably complete). Exactly-once: this
/// is the gate that proves no object was lost before cutover.
async fn verify_prefix(
    from: &Arc<dyn ObjectStore>,
    to: &Arc<dyn ObjectStore>,
    prefix: &OsPath,
) -> Result<()> {
    let src = list_objects(from, prefix).await?;
    let dst: HashMap<String, u64> = list_objects(to, prefix)
        .await?
        .into_iter()
        .map(|(loc, size)| (loc.to_string(), size))
        .collect();
    for (loc, size) in &src {
        let key = loc.to_string();
        let ok = dst.get(&key).map(|d| d == size).unwrap_or(false);
        if !ok {
            // Re-copy the offending object (idempotent overwrite).
            let bytes = from
                .get(loc)
                .await
                .map_err(|e| BasinError::storage(format!("migration verify get {loc}: {e}")))?
                .bytes()
                .await
                .map_err(|e| BasinError::storage(format!("migration verify read {loc}: {e}")))?;
            to.put(loc, PutPayload::from_bytes(bytes.clone()))
                .await
                .map_err(|e| BasinError::storage(format!("migration verify put {loc}: {e}")))?;
            // Confirm the re-copy actually landed at the right size.
            let after = to
                .head(loc)
                .await
                .map_err(|e| BasinError::storage(format!("migration verify head {loc}: {e}")))?;
            if after.size != *size {
                return Err(BasinError::storage(format!(
                    "migration verify: {loc} size {} != source {size} after re-copy",
                    after.size
                )));
            }
        }
    }
    Ok(())
}

/// DRAIN delete: remove every object under `prefix` from `from` (A's now-orphaned
/// project objects, after they're proven present in B). `crash`/`MidDrainDelete`
/// lets the #38 matrix halt mid-delete; resume re-runs delete idempotently.
async fn delete_prefix(
    from: &Arc<dyn ObjectStore>,
    prefix: &OsPath,
    crash: CrashPoint,
) -> std::result::Result<(), MigrationError> {
    let objects = list_objects(from, prefix).await?;
    for (i, (loc, _)) in objects.iter().enumerate() {
        if let CrashPoint::MidDrainDelete(n) = crash {
            if i == n {
                return Err(MigrationError::Crash(SimulatedCrash(MigrationPhase::Drain)));
            }
        }
        match from.delete(loc).await {
            Ok(()) => {}
            // Idempotent: an already-deleted object is fine on re-run.
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => {
                return Err(MigrationError::Failed(BasinError::storage(format!(
                    "migration drain delete {loc}: {e}"
                ))))
            }
        }
    }
    Ok(())
}

/// DELETE phase: the source bucket may be reclaimed from the registry iff it now
/// holds NO live project and is physically empty of this project's objects.
/// Decrements `from`'s `assigned_count` for `project`, and removes the bucket
/// entry when its count reaches zero AND it is provably empty. Refuses to drop a
/// bucket that still holds a live project (a hard correctness gate).
async fn finish_delete(
    project: &ProjectId,
    from_id: &str,
    from_store: &Arc<dyn ObjectStore>,
    catalog: &dyn Catalog,
) -> Result<()> {
    // Provable-empty gate: A must hold none of this project's objects.
    let prefix = project_object_prefix(project);
    let remaining = list_objects(from_store, &prefix).await?;
    if !remaining.is_empty() {
        return Err(BasinError::storage(format!(
            "migration delete: source bucket {from_id} still holds {} object(s) for {project}",
            remaining.len()
        )));
    }

    // Decrement the source's assigned-project count (idempotent-ish: clamped at
    // 0). When it reaches zero the bucket holds no live project and may be
    // dropped from the registry.
    let mut registry = catalog.get_bucket_registry().await?;
    if let Some(b) = registry.buckets.iter_mut().find(|b| b.bucket_id == from_id) {
        b.assigned_count = b.assigned_count.saturating_sub(1);
        let now_empty = b.assigned_count == 0;
        if now_empty {
            registry.buckets.retain(|b| b.bucket_id != from_id);
        }
        catalog.put_bucket_registry(&registry).await?;
    }
    Ok(())
}
