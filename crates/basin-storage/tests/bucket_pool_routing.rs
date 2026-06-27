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
use basin_storage::bucket_pool::{BucketPool, BucketPoolBuckets, BucketResolver, PoolConfig};
use basin_storage::{Storage, StorageConfig};
use futures::stream::StreamExt;
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt};

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
            stripe: 1,
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
            stripe: 1,
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
            stripe: 1,
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

// ===========================================================================
// #36 Stage 2a — single-project partition→bucket STRIPING (throughput lever).
// ===========================================================================

/// Build a pool with an explicit stripe width and attach it to a fresh
/// Storage over a fresh default store. Returns (storage, catalog, pool,
/// resolver) so a test can inspect every pooled bucket directly.
fn build_striped(
    stripe: usize,
    max_buckets: usize,
) -> (Storage, Arc<InMemoryCatalog>, Arc<BucketPool>, Arc<InMemoryResolver>) {
    let default_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (storage, cat) = build_storage_with_default(default_store);
    let resolver = Arc::new(InMemoryResolver::default());
    let pool = Arc::new(BucketPool::new(
        PoolConfig {
            enabled: true,
            max_buckets,
            watermark: 1,
            stripe,
        },
        resolver.clone(),
    ));
    storage.attach_bucket_pool(pool.clone());
    (storage, cat, pool, resolver)
}

/// A striped project's partitions distribute across its N pooled buckets, the
/// distribution is deterministic, and it is STABLE across a simulated restart
/// (re-read from the catalog, not recomputed).
#[tokio::test]
async fn striped_partitions_distribute_and_are_stable() {
    let (_storage, cat, pool, resolver) = build_striped(4, 8);
    let project = ProjectId::new();

    // Warm the assignment: a width-4 stripe of distinct buckets is allocated +
    // persisted on first ensure.
    pool.ensure_assignment(&project, cat.as_ref()).await.unwrap();
    let a = cat.get_bucket_assignment(&project).await.unwrap().unwrap();
    assert_eq!(a.stripe.len(), 4, "stripe width must equal config.stripe");
    let distinct: std::collections::HashSet<_> = a.stripe.iter().collect();
    assert_eq!(distinct.len(), 4, "stripe buckets must be distinct");
    assert_eq!(a.bucket_id, a.stripe[0], "primary bucket_id == stripe[0]");

    // Map 64 partitions → buckets via the public routing primitive and assert
    // coverage of all 4 buckets (the spread is real, not a constant).
    let mut covered: std::collections::HashSet<*const ()> = std::collections::HashSet::new();
    for i in 0..64 {
        let pid = format!("partition-{i}");
        let store = pool.store_for_partition(&project, &pid).expect("warmed stripe");
        covered.insert(Arc::as_ptr(&store) as *const ());
    }
    assert_eq!(
        covered.len(),
        4,
        "64 partitions must spread across all 4 stripe buckets"
    );

    // Stability across restart: drop the per-process cache, re-warm from the
    // SAME catalog, and assert every partition resolves to its slot-derived
    // bucket store (deterministic + stable, re-read not recomputed).
    pool.invalidate_all();
    pool.ensure_assignment(&project, cat.as_ref()).await.unwrap();
    for i in 0..64 {
        let pid = format!("partition-{i}");
        let store = pool.store_for_partition(&project, &pid).expect("re-warmed stripe");
        let bucket_id = &a.stripe[basin_catalog::bucket_pool::stripe_slot(&pid, a.stripe.len())];
        let expected = resolver.store_for(bucket_id);
        assert_eq!(
            Arc::as_ptr(&store) as *const (),
            Arc::as_ptr(&expected) as *const (),
            "partition {pid} must re-resolve its stable bucket after restart"
        );
    }
}

/// Write partition P then read partition P resolves the SAME bucket and the
/// data round-trips exactly. Also asserts the file physically landed in the
/// slot-derived bucket (not a sibling stripe bucket).
#[tokio::test]
async fn striped_write_then_read_same_partition_round_trips() {
    let (storage, cat, pool, resolver) = build_striped(4, 8);
    let project = ProjectId::new();
    pool.ensure_assignment(&project, cat.as_ref()).await.unwrap();
    let a = cat.get_bucket_assignment(&project).await.unwrap().unwrap();

    for i in 0..8 {
        let part = PartitionKey::new(format!("p{i}")).unwrap();
        let pid = part.as_str().to_string();

        // WRITE through the partition-routed store seam.
        let store_w = storage.partition_object_store(&project, &pid);
        let key = object_store::path::Path::from(format!(
            "projects/{project}/tables/t/data/{pid}/2026/06/26/file-{i}.vortex"
        ));
        store_w
            .put(&key, object_store::PutPayload::from_static(b"hello-stripe"))
            .await
            .unwrap();

        // READ through the seam: same partition → same bucket → exact bytes.
        let store_r = storage.partition_object_store(&project, &pid);
        let got = store_r.get(&key).await.unwrap().bytes().await.unwrap();
        assert_eq!(&got[..], b"hello-stripe", "partition {pid} round-trip");

        // Physical placement: the file is in the slot-derived bucket only.
        let slot = basin_catalog::bucket_pool::stripe_slot(&pid, a.stripe.len());
        let expected_bucket = &a.stripe[slot];
        let keys = list_keys(&resolver.store_for(expected_bucket)).await;
        assert!(
            keys.iter().any(|k| k == key.as_ref()),
            "partition {pid} file must live in its slot bucket {expected_bucket}"
        );
        for (j, other) in a.stripe.iter().enumerate() {
            if j == slot {
                continue;
            }
            let other_keys = list_keys(&resolver.store_for(other)).await;
            assert!(
                !other_keys.iter().any(|k| k == key.as_ref()),
                "partition {pid} must NOT appear in sibling bucket {other}"
            );
        }
    }
}

/// The unioned table read aggregates files across ALL stripe buckets: the
/// exact path set and count match what was written, with no file missed or
/// double-counted.
#[tokio::test]
async fn striped_union_read_aggregates_all_buckets() {
    let (storage, cat, pool, resolver) = build_striped(4, 8);
    let project = ProjectId::new();
    pool.ensure_assignment(&project, cat.as_ref()).await.unwrap();
    let a = cat.get_bucket_assignment(&project).await.unwrap().unwrap();

    let mut written: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for i in 0..40 {
        let pid = format!("part-{i}");
        let store = storage.partition_object_store(&project, &pid);
        let key = object_store::path::Path::from(format!(
            "projects/{project}/tables/t/data/{pid}/2026/06/26/f{i}.vortex"
        ));
        store
            .put(&key, object_store::PutPayload::from_static(b"x"))
            .await
            .unwrap();
        written.insert(key.to_string());
    }

    // Union read = LIST every stripe-bucket store and concat (what the striped
    // reader will do via routed_stores). The union must equal exactly the
    // written set, with no file appearing twice.
    let stores = pool.routed_stores(&project).expect("warmed stripe");
    assert_eq!(stores.len(), 4, "routed_stores must return the full stripe");
    let mut union: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for s in &stores {
        for k in list_keys(s).await {
            assert!(
                union.insert(k.clone()),
                "no file may appear in two stripe buckets (would double-count): {k}"
            );
        }
    }
    assert_eq!(union, written, "union read must equal exactly the written set");
    assert_eq!(union.len(), 40, "union read count must be exact");

    // No single bucket holds the whole table (proves the spread is real).
    for b in &a.stripe {
        let n = list_keys(&resolver.store_for(b)).await.len();
        assert!(n < 40, "no single bucket should hold the whole table");
    }
}

/// Exactly-once: the per-project create-if-absent CAS yields ONE stable striped
/// assignment under concurrent first-writers from two independent catalog
/// handles over the SAME store (two "nodes"). The loser adopts the winner's
/// stripe verbatim — no split-brain, both nodes route identically.
#[tokio::test]
async fn striped_assignment_cas_is_exactly_once_across_nodes() {
    use basin_catalog::ObjectStoreCatalog;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let node_a: Arc<dyn Catalog> = Arc::new(ObjectStoreCatalog::new(store.clone()));
    let node_b: Arc<dyn Catalog> = Arc::new(ObjectStoreCatalog::new(store.clone()));

    let resolver = Arc::new(InMemoryResolver::default());
    let pool_a = BucketPool::new(
        PoolConfig { enabled: true, max_buckets: 8, watermark: 1, stripe: 3 },
        resolver.clone(),
    );
    let pool_b = BucketPool::new(
        PoolConfig { enabled: true, max_buckets: 8, watermark: 1, stripe: 3 },
        resolver.clone(),
    );

    let project = ProjectId::new();
    let (ra, rb) = tokio::join!(
        pool_a.ensure_assignment(&project, node_a.as_ref()),
        pool_b.ensure_assignment(&project, node_b.as_ref()),
    );
    ra.unwrap();
    rb.unwrap();

    // Exactly one durable assignment, a valid width-3 distinct stripe.
    let durable = node_a.get_bucket_assignment(&project).await.unwrap().unwrap();
    assert_eq!(durable.stripe.len(), 3, "durable stripe width");
    let distinct: std::collections::HashSet<_> = durable.stripe.iter().collect();
    assert_eq!(distinct.len(), 3, "durable stripe buckets distinct");

    // Both nodes route a given partition to the SAME bucket (the durable
    // winner's stripe), regardless of who won the CAS.
    for i in 0..16 {
        let pid = format!("p{i}");
        let sa = pool_a.store_for_partition(&project, &pid).unwrap();
        let sb = pool_b.store_for_partition(&project, &pid).unwrap();
        assert_eq!(
            Arc::as_ptr(&sa) as *const (),
            Arc::as_ptr(&sb) as *const (),
            "both nodes must route partition {pid} to the same stripe bucket"
        );
    }
}

// ===========================================================================
// #36 Stage 2a — END-TO-END wiring of the seam into the real write/read entry
// points (`Storage::write_batch` / `Storage::read` / `list_data_files`). These
// exercise the production hot path, not just the routing primitives.
// ===========================================================================

/// Multi-row batch over several partitions so the writer's real PUT path is
/// driven across distinct partitions (hence distinct stripe buckets).
fn batch_rows(n: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ids: Int64Array = (0..n).collect();
    RecordBatch::try_new(schema, vec![Arc::new(ids)]).unwrap()
}

/// THE end-to-end striping proof on the real `Storage::write_batch` /
/// `Storage::read` path: writes across many partitions stripe their data
/// files across the project's pooled buckets, NOTHING lands on the single
/// default store, the per-partition placement matches the slot map, and the
/// unioned `Storage::read` reads back EXACTLY the rows written across all
/// buckets.
#[tokio::test]
async fn wired_write_then_read_round_trips_across_stripe() {
    let default_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cat = Arc::new(InMemoryCatalog::new());
    let storage = Storage::new(StorageConfig {
        object_store: default_store.clone(),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    storage.attach_catalog(cat.clone() as Arc<dyn Catalog>);
    let resolver = Arc::new(InMemoryResolver::default());
    let pool = Arc::new(BucketPool::new(
        PoolConfig { enabled: true, max_buckets: 8, watermark: 1, stripe: 4 },
        resolver.clone(),
    ));
    storage.attach_bucket_pool(pool.clone());

    let project = ProjectId::new();
    let table = TableName::new("t").unwrap();

    // Warm so we can read the durable stripe + assert physical placement.
    pool.ensure_assignment(&project, cat.as_ref()).await.unwrap();
    let a = cat.get_bucket_assignment(&project).await.unwrap().unwrap();
    assert_eq!(a.stripe.len(), 4);

    let mut total_written = 0i64;
    let mut per_bucket_files: HashMap<String, usize> = HashMap::new();
    for i in 0..24 {
        let part = PartitionKey::new(format!("p{i}")).unwrap();
        let rows = 4 + (i % 3);
        storage
            .write_batch(&project, &table, &part, &batch_rows(rows))
            .await
            .unwrap();
        total_written += rows;
        // The data file must be in this partition's slot bucket — and ONLY it.
        let slot = basin_catalog::bucket_pool::stripe_slot(part.as_str(), a.stripe.len());
        let expected_bucket = &a.stripe[slot];
        let prefix = format!("projects/{project}/tables/t/data/{}/", part.as_str());
        let keys = list_keys(&resolver.store_for(expected_bucket)).await;
        let here = keys.iter().filter(|k| k.starts_with(&prefix)).count();
        assert_eq!(here, 1, "partition {} file must be in slot bucket {expected_bucket}", part.as_str());
        *per_bucket_files.entry(expected_bucket.clone()).or_default() += 1;
        for (j, other) in a.stripe.iter().enumerate() {
            if j == slot {
                continue;
            }
            let ok = list_keys(&resolver.store_for(other)).await;
            assert!(
                !ok.iter().any(|k| k.starts_with(&prefix)),
                "partition {} must not appear in sibling bucket {other}", part.as_str()
            );
        }
    }

    // The single default store holds NOTHING — every data file was striped.
    assert!(
        list_keys(&default_store).await.is_empty(),
        "striped writes must never touch the single default store"
    );
    // The spread is real: more than one bucket holds data files.
    assert!(per_bucket_files.len() > 1, "data files must spread across >1 bucket");

    // Union READ through the real `Storage::read` entry point returns EXACTLY
    // the rows written, aggregated across all stripe buckets.
    let stream = storage
        .read(&project, &table, basin_storage::ReadOptions::default())
        .await
        .unwrap();
    let batches: Vec<_> = stream.collect().await;
    let total_read: i64 = batches
        .iter()
        .map(|b| b.as_ref().unwrap().num_rows() as i64)
        .sum();
    assert_eq!(
        total_read, total_written,
        "unioned read must return exactly the striped rows across all buckets"
    );

    // `list_data_files` (the union LIST) sees every file across the stripe.
    let listed = storage.list_data_files(&project, &table).await.unwrap();
    assert_eq!(listed.len(), 24, "union LIST must find every striped data file");
}

/// Cold-catalog restart: drop the per-process cache, re-warm from the SAME
/// catalog, and confirm reads still resolve the SAME buckets the writes landed
/// on (the partition→bucket map is stable, so a fresh process reads where the
/// old one wrote).
#[tokio::test]
async fn wired_read_after_restart_resolves_same_buckets() {
    let default_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cat = Arc::new(InMemoryCatalog::new());
    let storage = Storage::new(StorageConfig {
        object_store: default_store,
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    storage.attach_catalog(cat.clone() as Arc<dyn Catalog>);
    let resolver = Arc::new(InMemoryResolver::default());
    let pool = Arc::new(BucketPool::new(
        PoolConfig { enabled: true, max_buckets: 8, watermark: 1, stripe: 3 },
        resolver.clone(),
    ));
    storage.attach_bucket_pool(pool.clone());

    let project = ProjectId::new();
    let table = TableName::new("t").unwrap();
    let mut total = 0i64;
    for i in 0..18 {
        let part = PartitionKey::new(format!("p{i}")).unwrap();
        storage.write_batch(&project, &table, &part, &batch_rows(5)).await.unwrap();
        total += 5;
    }

    // Simulated restart: forget every in-process assignment + resolved store.
    pool.invalidate_all();

    // A fresh read re-warms from the catalog and unions across the SAME stripe.
    let stream = storage
        .read(&project, &table, basin_storage::ReadOptions::default())
        .await
        .unwrap();
    let batches: Vec<_> = stream.collect().await;
    let read_total: i64 = batches
        .iter()
        .map(|b| b.as_ref().unwrap().num_rows() as i64)
        .sum();
    assert_eq!(
        read_total, total,
        "after a cold-catalog restart the read must still find every striped row"
    );
}

/// Flag-OFF data-path no-op on the REAL write/read entry points: every data
/// file lands on the single default store, the read returns every row, and the
/// per-bucket resolver is NEVER consulted (the striping seam is fully inert).
#[tokio::test]
async fn wired_flag_off_data_path_is_a_noop() {
    let default_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (storage, _cat) = build_storage_with_default(default_store.clone());
    let resolver = Arc::new(InMemoryResolver::default());
    let pool_off = Arc::new(BucketPool::new(
        PoolConfig { enabled: false, max_buckets: 8, watermark: 1, stripe: 4 },
        resolver.clone(),
    ));
    storage.attach_bucket_pool(pool_off);

    let project = ProjectId::new();
    let table = TableName::new("t").unwrap();
    let mut total = 0i64;
    for i in 0..12 {
        let part = PartitionKey::new(format!("p{i}")).unwrap();
        storage.write_batch(&project, &table, &part, &batch_rows(6)).await.unwrap();
        total += 6;
    }

    // Everything is on the single default store; no pooled bucket was created.
    let keys = list_keys(&default_store).await;
    assert_eq!(keys.len(), 12, "flag OFF: all 12 data files on the single store");
    assert!(
        resolver.stores.lock().unwrap().is_empty(),
        "flag OFF must never resolve a pooled bucket (seam fully inert)"
    );

    // Read returns every row from the single store.
    let stream = storage
        .read(&project, &table, basin_storage::ReadOptions::default())
        .await
        .unwrap();
    let batches: Vec<_> = stream.collect().await;
    let read_total: i64 = batches
        .iter()
        .map(|b| b.as_ref().unwrap().num_rows() as i64)
        .sum();
    assert_eq!(read_total, total, "flag OFF: read returns every row from the single store");
}

/// The catalog-stays-on-primary / data-stripes invariant, proven with a real
/// `ObjectStoreCatalog` whose objects live on the PRIMARY store while striped
/// data files land on the pooled buckets. After striped writes: the primary
/// store holds catalog objects (`_catalog/…`) and NO `tables/.../data/` files;
/// the pooled stripe buckets hold the data files and NO catalog objects.
#[tokio::test]
async fn wired_catalog_stays_on_primary_data_stripes() {
    use basin_catalog::ObjectStoreCatalog;
    // The primary store backs BOTH the catalog and is the pool's bootstrap
    // bucket fallback (empty-endpoint entry). We instead force a real stripe of
    // distinct in-memory buckets via the resolver, and keep the catalog on this
    // primary store explicitly — exactly the design invariant
    // (object_store_catalog stays on the catalog/primary store; data stripes).
    let primary: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog_store = primary.clone();
    let cat = Arc::new(ObjectStoreCatalog::new(catalog_store.clone()));
    let storage = Storage::new(StorageConfig {
        object_store: primary.clone(),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    storage.attach_catalog(cat.clone() as Arc<dyn Catalog>);
    let resolver = Arc::new(InMemoryResolver::default());
    let pool = Arc::new(BucketPool::new(
        PoolConfig { enabled: true, max_buckets: 8, watermark: 1, stripe: 4 },
        resolver.clone(),
    ));
    storage.attach_bucket_pool(pool.clone());

    let project = ProjectId::new();
    let table = TableName::new("t").unwrap();
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

    // A real catalog object on the PRIMARY store: the table manifest + HEAD.
    cat.create_namespace(&project).await.unwrap();
    cat.create_table(&project, &table, &schema).await.unwrap();

    // Warm the (striped) assignment — this also persists the bucket registry +
    // assignment to the catalog/primary store, another catalog-on-primary fact.
    pool.ensure_assignment(&project, cat.as_ref()).await.unwrap();

    // Write the data through the real writer (which stripes the data file).
    for i in 0..16 {
        let part = PartitionKey::new(format!("p{i}")).unwrap();
        storage.write_batch(&project, &table, &part, &batch_rows(4)).await.unwrap();
    }

    // PRIMARY store: holds catalog objects, holds NO striped data file.
    let primary_keys = list_keys(&primary).await;
    assert!(
        !primary_keys.is_empty(),
        "primary store must hold catalog objects (manifest/HEAD/registry)"
    );
    assert!(
        !primary_keys.iter().any(|k| k.contains("/tables/t/data/")),
        "primary store must hold NO striped data file, got {primary_keys:?}"
    );

    // POOLED stripe buckets: hold the data files, hold NO catalog object.
    let a = cat.get_bucket_assignment(&project).await.unwrap().unwrap();
    let mut data_files_on_stripe = 0usize;
    for b in &a.stripe {
        let keys = list_keys(&resolver.store_for(b)).await;
        for k in &keys {
            assert!(
                k.contains("/tables/t/data/"),
                "stripe bucket {b} must hold only data files, found {k}"
            );
            assert!(
                !k.contains("/parts/") && !k.contains("_catalog") && !k.ends_with("HEAD"),
                "stripe bucket {b} must hold NO catalog object, found {k}"
            );
            data_files_on_stripe += 1;
        }
    }
    assert_eq!(data_files_on_stripe, 16, "all 16 data files must be on the stripe buckets");
}

/// Striped DROP TABLE: `delete_table_prefix` must free the table's data files
/// across EVERY stripe bucket (they don't live on the primary), so a later
/// re-create of the same name starts empty. Proves the maintenance-path
/// stripe fan-out.
#[tokio::test]
async fn wired_drop_table_frees_striped_data_across_buckets() {
    let default_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cat = Arc::new(InMemoryCatalog::new());
    let storage = Storage::new(StorageConfig {
        object_store: default_store,
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    storage.attach_catalog(cat.clone() as Arc<dyn Catalog>);
    let resolver = Arc::new(InMemoryResolver::default());
    let pool = Arc::new(BucketPool::new(
        PoolConfig { enabled: true, max_buckets: 8, watermark: 1, stripe: 4 },
        resolver.clone(),
    ));
    storage.attach_bucket_pool(pool.clone());

    let project = ProjectId::new();
    let table = TableName::new("t").unwrap();
    pool.ensure_assignment(&project, cat.as_ref()).await.unwrap();
    let a = cat.get_bucket_assignment(&project).await.unwrap().unwrap();

    for i in 0..20 {
        let part = PartitionKey::new(format!("p{i}")).unwrap();
        storage.write_batch(&project, &table, &part, &batch_rows(3)).await.unwrap();
    }
    // Pre-drop: data files exist across the stripe.
    let pre: usize = {
        let mut n = 0;
        for b in &a.stripe {
            n += list_keys(&resolver.store_for(b)).await.len();
        }
        n
    };
    assert_eq!(pre, 20, "all 20 data files present before drop");

    // DROP TABLE.
    let freed = storage.delete_table_prefix(&project, &table).await.unwrap();
    assert_eq!(freed, 20, "drop must free every striped data file");

    // Post-drop: nothing left on any stripe bucket under the table prefix.
    for b in &a.stripe {
        let keys = list_keys(&resolver.store_for(b)).await;
        let table_prefix = format!("projects/{project}/tables/t/");
        assert!(
            !keys.iter().any(|k| k.starts_with(&table_prefix)),
            "stripe bucket {b} must hold no table data after drop, got {keys:?}"
        );
    }
}

/// Stripe width == 1 is byte-identical single-bucket routing: every partition
/// of a project resolves to the SAME single store, and that store is the
/// primary routed_store (the no-op proof for the striping layer specifically).
#[tokio::test]
async fn stripe_width_one_is_single_bucket() {
    let (_storage, cat, pool, _resolver) = build_striped(1, 8);
    let project = ProjectId::new();
    pool.ensure_assignment(&project, cat.as_ref()).await.unwrap();
    let a = cat.get_bucket_assignment(&project).await.unwrap().unwrap();
    assert_eq!(a.stripe.len(), 1, "width-1 stripe");

    let s0 = pool.store_for_partition(&project, "part-A").unwrap();
    let mut ptrs = std::collections::HashSet::new();
    for i in 0..32 {
        let pid = format!("part-{i}");
        let s = pool.store_for_partition(&project, &pid).unwrap();
        ptrs.insert(Arc::as_ptr(&s) as *const ());
    }
    assert_eq!(ptrs.len(), 1, "width-1 must route every partition to one store");

    let primary = pool.routed_store(&project).unwrap();
    assert_eq!(
        Arc::as_ptr(&primary) as *const (),
        Arc::as_ptr(&s0) as *const (),
        "width-1: primary routed store == every partition's store"
    );
}

// ===========================================================================
// #36 — REAL-BUCKET WIRING: operator-configured pooled buckets carry their
// real provider name + endpoint + region into the registry, so the production
// resolver builds stores against the REAL buckets (not generated empty-endpoint
// placeholders). These tests use a resolver keyed by the REAL bucket_name +
// endpoint to prove the configured values actually reach the resolver.
// ===========================================================================

/// Resolver keyed by the REAL `bucket_name` (not `bucket_id`), and that records
/// the (bucket_name, endpoint, region) of every entry it resolved — so a test
/// can assert the registered entries carried the configured endpoint/region.
#[derive(Default)]
struct RealNameResolver {
    stores: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
    resolved: Mutex<Vec<(String, String, String)>>,
}

impl RealNameResolver {
    fn store_for_name(&self, bucket_name: &str) -> Arc<dyn ObjectStore> {
        self.stores
            .lock()
            .unwrap()
            .entry(bucket_name.to_string())
            .or_insert_with(|| Arc::new(InMemory::new()))
            .clone()
    }
}

impl BucketResolver for RealNameResolver {
    fn resolve(&self, entry: &BucketRegistryEntry) -> Result<Arc<dyn ObjectStore>> {
        self.resolved.lock().unwrap().push((
            entry.bucket_name.clone(),
            entry.endpoint.clone(),
            entry.region.clone(),
        ));
        Ok(self.store_for_name(&entry.bucket_name))
    }
}

const POOL_ENDPOINT: &str = "https://t3.storage.dev";
const POOL_REGION: &str = "auto";

fn configured_buckets() -> BucketPoolBuckets {
    BucketPoolBuckets {
        names: vec![
            "basin-pool-0".into(),
            "basin-pool-1".into(),
            "basin-pool-2".into(),
            "basin-pool-3".into(),
        ],
        endpoint: POOL_ENDPOINT.into(),
        region: POOL_REGION.into(),
        creds_refs: Vec::new(),
    }
}

/// With real buckets configured, `prepopulate_registry` seeds the registry with
/// those real names + the configured endpoint/region, `choose_stripe` picks
/// among them, and every resolved entry carries the configured endpoint/region
/// (not empty) keyed by the REAL bucket name — proving the resolver would build
/// a store against the real bucket.
#[tokio::test]
async fn configured_real_buckets_prepopulate_and_carry_endpoint_region() {
    let cat = Arc::new(InMemoryCatalog::new());
    let resolver = Arc::new(RealNameResolver::default());
    let pool = Arc::new(BucketPool::new_with_buckets(
        PoolConfig { enabled: true, max_buckets: 8, watermark: 1, stripe: 3 },
        configured_buckets(),
        resolver.clone(),
    ));

    // Pre-seed: the registry now holds exactly the 4 configured buckets, in
    // order, each carrying the REAL name + configured endpoint/region.
    pool.prepopulate_registry(cat.as_ref()).await.unwrap();
    let registry = cat.get_bucket_registry().await.unwrap();
    assert_eq!(registry.buckets.len(), 4, "all configured buckets pre-seeded");
    for (i, b) in registry.buckets.iter().enumerate() {
        assert_eq!(b.bucket_id, format!("pool-{i:04}"));
        assert_eq!(b.bucket_name, format!("basin-pool-{i}"), "real provider name");
        assert_eq!(b.endpoint, POOL_ENDPOINT, "configured endpoint, not empty");
        assert_eq!(b.region, POOL_REGION, "configured region, not empty");
        assert!(b.credentials_ref.is_none(), "creds via process-default keys");
    }

    // Pre-seed again: idempotent — same registry, no duplicate entries.
    pool.prepopulate_registry(cat.as_ref()).await.unwrap();
    let registry2 = cat.get_bucket_registry().await.unwrap();
    assert_eq!(registry2.buckets, registry.buckets, "prepopulate is idempotent");

    // A striped project picks among the configured buckets; resolving its
    // stripe builds stores keyed by the REAL bucket names with the configured
    // endpoint/region.
    let project = ProjectId::new();
    pool.ensure_assignment(&project, cat.as_ref()).await.unwrap();
    let a = cat.get_bucket_assignment(&project).await.unwrap().unwrap();
    assert_eq!(a.stripe.len(), 3, "stripe width");
    for id in &a.stripe {
        let i: usize = id.strip_prefix("pool-").unwrap().parse().unwrap();
        assert!(i < 4, "stripe bucket {id} is one of the configured buckets");
    }
    // Force store resolution for every stripe bucket.
    let _ = pool.routed_stores(&project).expect("warmed stripe");
    let resolved = resolver.resolved.lock().unwrap();
    assert!(!resolved.is_empty(), "resolver consulted for the real buckets");
    for (name, endpoint, region) in resolved.iter() {
        assert!(name.starts_with("basin-pool-"), "resolved by REAL name: {name}");
        assert_eq!(endpoint, POOL_ENDPOINT, "resolver saw configured endpoint");
        assert_eq!(region, POOL_REGION, "resolver saw configured region");
    }
}

/// Cross-node convergence: two independent pools over the SAME object-store
/// catalog, each pre-seeding from the SAME configured list, converge on an
/// identical registry (same id→real-name mapping), and both resolve a given
/// project's partitions to the same real bucket.
#[tokio::test]
async fn configured_real_buckets_converge_across_nodes() {
    use basin_catalog::ObjectStoreCatalog;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let node_a: Arc<dyn Catalog> = Arc::new(ObjectStoreCatalog::new(store.clone()));
    let node_b: Arc<dyn Catalog> = Arc::new(ObjectStoreCatalog::new(store.clone()));
    let resolver = Arc::new(RealNameResolver::default());
    let pool_a = BucketPool::new_with_buckets(
        PoolConfig { enabled: true, max_buckets: 8, watermark: 1, stripe: 3 },
        configured_buckets(),
        resolver.clone(),
    );
    let pool_b = BucketPool::new_with_buckets(
        PoolConfig { enabled: true, max_buckets: 8, watermark: 1, stripe: 3 },
        configured_buckets(),
        resolver.clone(),
    );

    // Both nodes pre-seed (idempotent create-if-absent) → identical registry.
    pool_a.prepopulate_registry(node_a.as_ref()).await.unwrap();
    pool_b.prepopulate_registry(node_b.as_ref()).await.unwrap();
    let reg_a = node_a.get_bucket_registry().await.unwrap();
    let reg_b = node_b.get_bucket_registry().await.unwrap();
    assert_eq!(reg_a.buckets, reg_b.buckets, "registries converge across nodes");
    assert_eq!(reg_a.buckets.len(), 4);

    // Both nodes route a given partition of the same project to the same real
    // bucket store.
    let project = ProjectId::new();
    let (ra, rb) = tokio::join!(
        pool_a.ensure_assignment(&project, node_a.as_ref()),
        pool_b.ensure_assignment(&project, node_b.as_ref()),
    );
    ra.unwrap();
    rb.unwrap();
    for i in 0..16 {
        let pid = format!("p{i}");
        let sa = pool_a.store_for_partition(&project, &pid).unwrap();
        let sb = pool_b.store_for_partition(&project, &pid).unwrap();
        assert_eq!(
            Arc::as_ptr(&sa) as *const (),
            Arc::as_ptr(&sb) as *const (),
            "both nodes route partition {pid} to the same real bucket"
        );
    }
}

/// The growth ceiling is the configured list length: a stripe wider than the
/// configured bucket count never registers a bucket without a real name behind
/// it (no generated placeholder past the list).
#[tokio::test]
async fn configured_real_buckets_cap_growth_at_list_length() {
    let cat = Arc::new(InMemoryCatalog::new());
    let resolver = Arc::new(RealNameResolver::default());
    // Only 2 real buckets, but stripe=4 requested.
    let pool = Arc::new(BucketPool::new_with_buckets(
        PoolConfig { enabled: true, max_buckets: 8, watermark: 1, stripe: 4 },
        BucketPoolBuckets {
            names: vec!["basin-pool-0".into(), "basin-pool-1".into()],
            endpoint: POOL_ENDPOINT.into(),
            region: POOL_REGION.into(),
            creds_refs: Vec::new(),
        },
        resolver.clone(),
    ));
    let project = ProjectId::new();
    pool.ensure_assignment(&project, cat.as_ref()).await.unwrap();
    let registry = cat.get_bucket_registry().await.unwrap();
    assert_eq!(registry.buckets.len(), 2, "never grow past the configured list");
    for b in &registry.buckets {
        assert!(b.bucket_name.starts_with("basin-pool-"), "only real buckets registered");
        assert_eq!(b.endpoint, POOL_ENDPOINT);
    }
}

/// OFF / empty-list no-op proof for the real-bucket wiring: with NO configured
/// buckets, `prepopulate_registry` never touches the catalog and the registry
/// stays empty (lazy bootstrap on first write, as today). With the flag OFF and
/// buckets configured, `prepopulate_registry` is still a no-op (the resolver is
/// never consulted).
#[tokio::test]
async fn no_real_buckets_or_flag_off_is_a_noop() {
    // (a) Flag ON but empty list → prepopulate is a no-op, registry untouched.
    let cat = Arc::new(InMemoryCatalog::new());
    let resolver = Arc::new(RealNameResolver::default());
    let pool = Arc::new(BucketPool::new_with_buckets(
        PoolConfig { enabled: true, max_buckets: 8, watermark: 1, stripe: 1 },
        BucketPoolBuckets::default(),
        resolver.clone(),
    ));
    pool.prepopulate_registry(cat.as_ref()).await.unwrap();
    assert!(
        cat.get_bucket_registry().await.unwrap().buckets.is_empty(),
        "empty list: prepopulate must not seed the registry"
    );

    // (b) Flag OFF but a real list configured → prepopulate still a no-op.
    let cat_off = Arc::new(InMemoryCatalog::new());
    let resolver_off = Arc::new(RealNameResolver::default());
    let pool_off = Arc::new(BucketPool::new_with_buckets(
        PoolConfig { enabled: false, max_buckets: 8, watermark: 1, stripe: 1 },
        configured_buckets(),
        resolver_off.clone(),
    ));
    pool_off.prepopulate_registry(cat_off.as_ref()).await.unwrap();
    assert!(
        cat_off.get_bucket_registry().await.unwrap().buckets.is_empty(),
        "flag OFF: prepopulate must not seed the registry"
    );
    assert!(
        resolver_off.resolved.lock().unwrap().is_empty(),
        "flag OFF: resolver never consulted"
    );
}

/// Empty configured list keeps the legacy bootstrap entry shape EXACTLY: the
/// first registered bucket is `pool-0000` with an EMPTY endpoint/region (which
/// the production resolver maps to the process-default store) — byte-identical
/// to today.
#[tokio::test]
async fn empty_list_keeps_legacy_bootstrap_entry_shape() {
    let cat = Arc::new(InMemoryCatalog::new());
    let resolver = Arc::new(RealNameResolver::default());
    let pool = Arc::new(BucketPool::new_with_buckets(
        PoolConfig { enabled: true, max_buckets: 8, watermark: 1, stripe: 1 },
        BucketPoolBuckets::default(),
        resolver.clone(),
    ));
    let project = ProjectId::new();
    pool.ensure_assignment(&project, cat.as_ref()).await.unwrap();
    let registry = cat.get_bucket_registry().await.unwrap();
    assert_eq!(registry.buckets.len(), 1, "lazy bootstrap registers one entry");
    let b = &registry.buckets[0];
    assert_eq!(b.bucket_id, "pool-0000");
    assert_eq!(b.bucket_name, "pool-0000", "legacy generated name");
    assert!(b.endpoint.is_empty(), "legacy bootstrap entry has EMPTY endpoint");
    assert!(b.region.is_empty(), "legacy bootstrap entry has EMPTY region");
}
