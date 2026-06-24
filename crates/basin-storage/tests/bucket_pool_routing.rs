//! Stage 1 multi-bucket storage pool (#36): routing + assignment coverage.
//!
//! The load-bearing safety property is the **no-op proof**: with
//! `BASIN_BUCKET_POOL` OFF, routing yields the identical default bucket +
//! per-project prefix as today, for several projects. The remaining tests
//! exercise the flag-ON behaviour against in-memory stores: spread across
//! pooled buckets, stability across a simulated restart (re-read from catalog,
//! not recomputed), the pool ceiling + dense packing, and a write→read
//! round-trip through the assigned bucket.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::bucket_pool::BucketRegistryEntry;
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{PartitionKey, ProjectId, Result, TableName};
use basin_storage::bucket_pool::{BucketPool, BucketResolver, PoolConfig};
use basin_storage::{Storage, StorageConfig};
use futures::stream::StreamExt;
use object_store::memory::InMemory;
use object_store::ObjectStore;

/// Resolver that hands out one distinct `InMemory` store per `bucket_id`, so
/// we can inspect exactly which bucket a project's objects landed in.
#[derive(Default)]
struct InMemoryResolver {
    stores: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl InMemoryResolver {
    fn store_for(&self, bucket_id: &str) -> Arc<dyn ObjectStore> {
        self.stores
            .lock()
            .unwrap()
            .entry(bucket_id.to_string())
            .or_insert_with(|| Arc::new(InMemory::new()))
            .clone()
    }
}

impl BucketResolver for InMemoryResolver {
    fn resolve(&self, entry: &BucketRegistryEntry) -> Result<Arc<dyn ObjectStore>> {
        Ok(self.store_for(&entry.bucket_id))
    }
}

fn small_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ids: Int64Array = (0..16).collect();
    RecordBatch::try_new(schema, vec![Arc::new(ids)]).unwrap()
}

fn build_storage_with_default(
    default_store: Arc<dyn ObjectStore>,
) -> (Storage, Arc<InMemoryCatalog>) {
    let cat = Arc::new(InMemoryCatalog::new());
    let s = Storage::new(StorageConfig {
        object_store: default_store,
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    s.attach_catalog(cat.clone() as Arc<dyn Catalog>);
    (s, cat)
}

async fn list_keys(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    let mut out: Vec<String> = store
        .list(None)
        .map(|r| r.unwrap().location.to_string())
        .collect()
        .await;
    out.sort();
    out
}

/// Strip the random `{ulid}.vortex` filename, leaving the deterministic key
/// prefix (`projects/{p}/tables/{t}/data/{part}/yyyy/mm/dd/`). The filename is
/// a per-write ULID, so two byte-identical routings still differ in the last
/// segment; the prefix is the part the routing/layout actually decides.
fn key_prefixes(keys: &[String]) -> Vec<String> {
    let mut out: Vec<String> = keys
        .iter()
        .map(|k| match k.rfind('/') {
            Some(i) => k[..=i].to_string(),
            None => k.clone(),
        })
        .collect();
    out.sort();
    out
}

/// THE no-op proof: with the flag OFF, every project's objects land in the
/// single default store under `projects/{project}/…` — byte-for-byte today's
/// behaviour — and nothing else is created. Proven both with NO pool attached
/// and with a pool attached but OFF (so the attach itself is inert).
#[tokio::test]
async fn flag_off_is_a_noop_identical_to_today() {
    // (a) No pool attached at all.
    let default_a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (storage_a, _cat_a) = build_storage_with_default(default_a.clone());

    // (b) Pool ATTACHED but flag OFF — must be indistinguishable from (a).
    let default_b: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (storage_b, _cat_b) = build_storage_with_default(default_b.clone());
    let resolver = Arc::new(InMemoryResolver::default());
    let pool_off = Arc::new(BucketPool::new(
        PoolConfig {
            enabled: false,
            max_buckets: 8,
            watermark: 1,
        },
        resolver.clone(),
    ));
    storage_b.attach_bucket_pool(pool_off);

    let table = TableName::new("t").unwrap();
    let part = PartitionKey::default_key();

    let mut projects = Vec::new();
    for _ in 0..4 {
        let p = ProjectId::new();
        projects.push(p);
        storage_a.write_batch(&p, &table, &part, &small_batch()).await.unwrap();
        storage_b.write_batch(&p, &table, &part, &small_batch()).await.unwrap();
    }

    // Both default stores hold the same set of keys; the per-bucket resolver
    // was NEVER consulted (no pooled buckets created).
    let keys_a = list_keys(&default_a).await;
    let keys_b = list_keys(&default_b).await;
    // Key PREFIXES (everything but the per-write random ULID filename) must be
    // identical: same bucket, same per-project layout.
    assert_eq!(
        key_prefixes(&keys_a),
        key_prefixes(&keys_b),
        "flag-OFF routing must match no-pool routing"
    );
    assert!(!keys_a.is_empty(), "writes must land in the default store");
    for p in &projects {
        let prefix = format!("projects/{p}/");
        assert!(
            keys_a.iter().any(|k| k.starts_with(&prefix)),
            "project {p} objects must live under its prefix in the default store"
        );
    }
    assert!(
        resolver.stores.lock().unwrap().is_empty(),
        "flag OFF must never resolve a pooled bucket"
    );
}

/// Flag ON: assignment spreads projects across pooled buckets, is stable
/// across a simulated restart (re-read, not recomputed), and a project's reads
/// see exactly what its writes wrote (round-trip through the assigned bucket).
#[tokio::test]
async fn flag_on_spreads_is_stable_and_round_trips() {
    let default_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (storage, cat) = build_storage_with_default(default_store.clone());
    let resolver = Arc::new(InMemoryResolver::default());
    // watermark=1 with max=4: each new project past the first finds every
    // existing bucket at/above watermark, so a fresh bucket is registered —
    // spreading projects one-per-bucket until the ceiling.
    let pool = Arc::new(BucketPool::new(
        PoolConfig {
            enabled: true,
            max_buckets: 4,
            watermark: 1,
        },
        resolver.clone(),
    ));
    storage.attach_bucket_pool(pool.clone());

    let table = TableName::new("t").unwrap();
    let part = PartitionKey::default_key();

    let mut projects = Vec::new();
    for _ in 0..4 {
        let p = ProjectId::new();
        storage.write_batch(&p, &table, &part, &small_batch()).await.unwrap();
        projects.push(p);
    }

    // Each project got a distinct bucket (spread), all under its prefix.
    let mut assigned: Vec<String> = Vec::new();
    for p in &projects {
        let a = cat.get_bucket_assignment(p).await.unwrap().expect("assigned");
        assigned.push(a.bucket_id.clone());
        let store = resolver.store_for(&a.bucket_id);
        let keys = list_keys(&store).await;
        let prefix = format!("projects/{p}/");
        assert!(
            keys.iter().any(|k| k.starts_with(&prefix)),
            "project {p} objects must live in its assigned bucket {}",
            a.bucket_id
        );
    }
    let distinct: std::collections::HashSet<_> = assigned.iter().collect();
    assert_eq!(distinct.len(), 4, "four projects should spread to four buckets");

    // The default store must hold NOTHING — every write was routed to a pooled
    // bucket, not the single shared store.
    assert!(
        list_keys(&default_store).await.is_empty(),
        "flag ON must route writes off the default store"
    );

    // Round-trip: reads see exactly what writes wrote, through the assigned
    // bucket.
    let stream = storage
        .read(&projects[0], &table, basin_storage::ReadOptions::default())
        .await
        .unwrap();
    let batches: Vec<_> = stream.collect().await;
    let total: usize = batches.iter().map(|b| b.as_ref().unwrap().num_rows()).sum();
    assert_eq!(total, 16, "round-trip through assigned bucket must read back rows");

    // Simulated restart: drop the per-process cache; the SAME catalog must
    // re-yield the SAME assignments (re-read, not recomputed).
    let before = assigned.clone();
    pool.invalidate_all();
    for (i, p) in projects.iter().enumerate() {
        pool.ensure_assignment(p, cat.as_ref()).await.unwrap();
        let a = cat.get_bucket_assignment(p).await.unwrap().unwrap();
        assert_eq!(
            a.bucket_id, before[i],
            "assignment must be stable across restart (re-read, not recomputed)"
        );
    }
}

/// Flag ON: growth stops at `BASIN_BUCKET_POOL_MAX`; past the ceiling, new
/// projects pack into the least-full pooled bucket (graceful degrade, no
/// error).
#[tokio::test]
async fn flag_on_stops_growing_at_ceiling_then_packs() {
    let default_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (storage, cat) = build_storage_with_default(default_store);
    let resolver = Arc::new(InMemoryResolver::default());
    let pool = Arc::new(BucketPool::new(
        PoolConfig {
            enabled: true,
            max_buckets: 2,
            watermark: 1,
        },
        resolver.clone(),
    ));
    storage.attach_bucket_pool(pool.clone());

    let table = TableName::new("t").unwrap();
    let part = PartitionKey::default_key();

    // Assign 6 projects with a 2-bucket ceiling. The registry must never grow
    // beyond 2 buckets; the extra projects pack into the least-full bucket.
    let mut per_bucket: HashMap<String, usize> = HashMap::new();
    for _ in 0..6 {
        let p = ProjectId::new();
        storage.write_batch(&p, &table, &part, &small_batch()).await.unwrap();
        let a = cat.get_bucket_assignment(&p).await.unwrap().unwrap();
        *per_bucket.entry(a.bucket_id).or_default() += 1;
    }

    let registry = cat.get_bucket_registry().await.unwrap();
    assert_eq!(
        registry.buckets.len(),
        2,
        "pool must stop growing at BASIN_BUCKET_POOL_MAX"
    );
    assert_eq!(per_bucket.values().sum::<usize>(), 6);
    // Dense, balanced packing: 6 across 2 buckets → 3 each (least-full pick).
    for (_id, n) in &per_bucket {
        assert_eq!(*n, 3, "projects past the ceiling pack into the least-full bucket");
    }
}
