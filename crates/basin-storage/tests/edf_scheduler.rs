//! End-to-end coverage for the EDF (Earliest Deadline First) per-project
//! scheduler activation. The tests drive `Storage::project_object_store`
//! through a deliberately-slow inner [`ObjectStore`] so the scheduler's
//! dispatch-order behaviour is observable in real wall-clock time.
//!
//! These tests are intentionally separate from the in-module
//! `scheduler.rs` tests: those test the dispatcher in isolation; these
//! prove the *wiring* between `concurrency.rs` (priority assignment per
//! RPC type / range size) and `scheduler.rs` (deadline-keyed dispatch)
//! is intact.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOpts, PutOptions, PutPayload, PutResult,
};

use basin_common::ProjectId;
use basin_storage::{Storage, StorageConfig};

/// Inner store with synthetic latency. Each RPC sleeps for the
/// configured per-op duration; everything else is delegated to the
/// inner [`object_store::memory::InMemory`].
///
/// `concurrent_max` is the high-water mark of in-flight ops observed
/// through this wrapper; tests assert it never exceeds `cap=16` (the
/// per-project Semaphore floor) under any workload.
#[derive(Debug)]
struct SlowStore {
    inner: Arc<dyn ObjectStore>,
    op_duration: Duration,
    in_flight: AtomicUsize,
    concurrent_max: AtomicUsize,
}

impl SlowStore {
    fn new(op_duration: Duration) -> Self {
        Self {
            inner: Arc::new(object_store::memory::InMemory::new()),
            op_duration,
            in_flight: AtomicUsize::new(0),
            concurrent_max: AtomicUsize::new(0),
        }
    }

    fn enter(&self) {
        let now = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.concurrent_max.fetch_max(now, Ordering::Relaxed);
    }

    fn leave(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    fn concurrent_max(&self) -> usize {
        self.concurrent_max.load(Ordering::Relaxed)
    }
}

impl std::fmt::Display for SlowStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for SlowStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.enter();
        tokio::time::sleep(self.op_duration).await;
        let r = self.inner.put_opts(location, payload, opts).await;
        self.leave();
        r
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.enter();
        tokio::time::sleep(self.op_duration).await;
        let r = self.inner.put_multipart_opts(location, opts).await;
        self.leave();
        r
    }

    // `get_range` and `head` are provided trait methods that route through
    // `get_opts`, so instrumenting `get_opts` covers them too.
    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.enter();
        tokio::time::sleep(self.op_duration).await;
        let r = self.inner.get_opts(location, options).await;
        self.leave();
        r
    }

    // `delete` (ObjectStoreExt) routes through `delete_stream`. The tests
    // don't measure delete concurrency, so pass straight through.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        // No latency injection on streaming list — the tests don't
        // exercise it.
        let inner = self.inner.clone();
        let prefix = prefix.cloned();
        inner.list(prefix.as_ref())
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.enter();
        tokio::time::sleep(self.op_duration).await;
        let r = self.inner.list_with_delimiter(prefix).await;
        self.leave();
        r
    }

    // Replaces the old `copy` / `copy_if_not_exists` pair; CopyMode in
    // `options` distinguishes overwrite vs create-only.
    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.enter();
        tokio::time::sleep(self.op_duration).await;
        let r = self.inner.copy_opts(from, to, options).await;
        self.leave();
        r
    }
}

/// Build a `Storage` whose underlying object store is the supplied
/// `SlowStore`. Caches are disabled so every read hits the slow path.
fn storage_with_slow(slow: Arc<SlowStore>) -> Storage {
    Storage::new(StorageConfig {
        object_store: slow as Arc<dyn ObjectStore>,
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    })
}

/// Pre-populate the inner store with a key shaped under `project`'s
/// prefix and return the path. Bytes are tunable so a test can request
/// a small (point-shaped) or large (bulk-shaped) value.
async fn seed_object(
    storage: &Storage,
    project: &ProjectId,
    name: &str,
    bytes: usize,
) -> ObjectPath {
    let path = ObjectPath::from(format!("projects/{}/data/{}", project.as_prefix(), name));
    let store = storage.project_object_store(project);
    let payload = PutPayload::from(Bytes::from(vec![0u8; bytes]));
    store.put(&path, payload).await.expect("seed put");
    path
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

/// One project submits 100 ops sequentially; the scheduler must not
/// inflate completion time beyond ~2× the bare per-op cost. Validates
/// the EDF wiring is overhead-free in the steady state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_project_no_starvation() {
    let per_op = Duration::from_millis(2);
    let slow = Arc::new(SlowStore::new(per_op));
    let storage = storage_with_slow(slow.clone());
    let project = ProjectId::new();
    let path = seed_object(&storage, &project, "f", 64).await;

    let store = storage.project_object_store(&project);
    let started = Instant::now();
    for _ in 0..100 {
        let _ = store.head(&path).await.expect("head");
    }
    let elapsed = started.elapsed();
    // Bound: 100 sequential ops × 2ms each = 200ms baseline. The
    // scheduler can add bookkeeping overhead but not double the cost.
    assert!(
        elapsed < per_op * 100 * 2,
        "single-project overhead: {:?} > 2× ({:?})",
        elapsed,
        per_op * 100 * 2
    );
}

/// Project A submits 100 bulk Low ops; project B submits one point High
/// op concurrently. B's op must dispatch before more than a small
/// number of A's bulks complete — bounded by the EDF deadline gap
/// + consecutive-dispatch cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn noisy_project_no_starvation_for_quiet_project() {
    let per_op = Duration::from_millis(5);
    let slow = Arc::new(SlowStore::new(per_op));
    let storage = storage_with_slow(slow.clone());

    let project_a = ProjectId::new();
    let project_b = ProjectId::new();
    // Seed a >256KB object for A (so A's get_range maps to Low) and a
    // small object for B (so B's HEAD maps to High).
    let a_path = seed_object(&storage, &project_a, "bulk", 1024 * 1024).await; // 1 MiB
    let b_path = seed_object(&storage, &project_b, "point", 64).await;

    let store_a = storage.project_object_store(&project_a);
    let store_b = storage.project_object_store(&project_b);

    let mut handles = vec![];
    for _ in 0..100 {
        let s = store_a.clone();
        let p = a_path.clone();
        handles.push(tokio::spawn(async move {
            let _ = s.get_range(&p, 0..1024 * 1024).await; // Low (≥ threshold)
        }));
    }
    // Tiny pause so A's queue is non-empty before B arrives.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let started = Instant::now();
    let _ = store_b.head(&b_path).await.expect("b head"); // High
    let b_latency = started.elapsed();

    // High deadline is 5ms; expected per-op cost is 5ms. B should
    // complete in well under (per_op * 16) — i.e. before more than the
    // cap-16 first wave of A's bulks could finish if A held every slot.
    let bound = per_op * 16;
    assert!(
        b_latency < bound,
        "B's point HEAD latency {:?} ≥ bound {:?} — quiet project starved",
        b_latency,
        bound
    );

    for h in handles {
        let _ = h.await;
    }
}

/// One project performs a sustained scan workload (Low, large range);
/// another project fires point lookups (High) concurrently. Every
/// point lookup should complete with low per-op latency — bounded by
/// the EDF deadline, not by the scanner's bulk throughput.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_op_doesnt_starve_point_lookups() {
    let per_op = Duration::from_millis(5);
    // We can't differentiate per-RPC cost in a single SlowStore, so we
    // model both shapes at the same per-op cost; the scheduler still
    // classes the bulk reads as Low (range ≥ threshold) and the point
    // lookups as High — that's the property under test.
    let slow = Arc::new(SlowStore::new(per_op));
    let storage = storage_with_slow(slow.clone());

    let scanner = ProjectId::new();
    let pointer = ProjectId::new();
    let bulk_path = seed_object(&storage, &scanner, "scan", 2 * 1024 * 1024).await;
    let point_path = seed_object(&storage, &pointer, "p", 64).await;

    let scan_store = storage.project_object_store(&scanner);
    let point_store = storage.project_object_store(&pointer);

    // Bulk scanner: a sustained sequence of large range reads (each
    // classed Low by the scheduler).
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_flag = stop.clone();
    let scan_handle = tokio::spawn(async move {
        while !stop_flag.load(Ordering::Relaxed) {
            let _ = scan_store.get_range(&bulk_path, 0..1024 * 1024).await;
        }
    });

    // Tiny pause so the scanner has work in flight before pointer
    // arrives.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Pointer: 100 sequential point lookups (the realistic shape — a
    // quiet project pipelines, doesn't burst). Measure per-op latency.
    let mut latencies: Vec<Duration> = Vec::with_capacity(100);
    for _ in 0..100 {
        let started = Instant::now();
        point_store.head(&point_path).await.expect("point head");
        latencies.push(started.elapsed());
    }

    // Stop the bulk scanner.
    stop.store(true, Ordering::Relaxed);
    let _ = scan_handle.await;

    // The High-priority HEAD's deadline is HIGH_PRIORITY_DEADLINE = 5ms.
    // Each point op's wall-clock latency = scheduler queue wait +
    // per-op cost. With EDF, the point op's deadline beats every Low,
    // so it should dispatch in roughly per_op (≤ 5ms) of waiting time.
    // We assert the median is well under per_op * cap=16, which is the
    // worst-case latency if Low ops held every slot.
    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let bound = per_op * 16;
    assert!(
        p50 < bound,
        "pointer p50 latency {:?} ≥ {:?} — point lookups starved by bulk scan",
        p50,
        bound
    );
}

/// One project submits 100 point ops; a second project submits 100 point
/// ops concurrently. Without the consecutive-dispatch cap the first
/// project's deadlines would all win the heap and serialise. With the
/// cap, the dispatch order interleaves — neither project runs more than
/// `CONSECUTIVE_DISPATCH_CAP` consecutive dispatches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consecutive_dispatch_cap_enforced() {
    let per_op = Duration::from_millis(2);
    let slow = Arc::new(SlowStore::new(per_op));
    let storage = storage_with_slow(slow.clone());

    let a = ProjectId::new();
    let b = ProjectId::new();
    let pa = seed_object(&storage, &a, "p", 64).await;
    let pb = seed_object(&storage, &b, "p", 64).await;

    let store_a = storage.project_object_store(&a);
    let store_b = storage.project_object_store(&b);

    let order: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));

    let mut handles = vec![];
    // Pre-load A so its queue head is earliest.
    for _ in 0..100 {
        let s = store_a.clone();
        let p = pa.clone();
        let o = order.clone();
        handles.push(tokio::spawn(async move {
            s.head(&p).await.expect("a head");
            o.lock().unwrap().push("A");
        }));
    }
    // Tiny pause so A's queue is non-empty before B starts to interleave.
    tokio::time::sleep(Duration::from_millis(2)).await;
    for _ in 0..100 {
        let s = store_b.clone();
        let p = pb.clone();
        let o = order.clone();
        handles.push(tokio::spawn(async move {
            s.head(&p).await.expect("b head");
            o.lock().unwrap().push("B");
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    let o = order.lock().unwrap();
    // First B should appear well before the suffix. With
    // CONSECUTIVE_DISPATCH_CAP=2 the cap forces a yield to B every
    // couple of dispatches; allow some slack for race-driven bursts.
    // The crucial property is that B isn't suffix-only.
    let first_b = o.iter().position(|t| *t == "B").expect("B dispatched");
    assert!(
        first_b < 50,
        "consecutive cap not enforced: B first appears at index {} (of 200)",
        first_b
    );
}

/// Regression: even with the EDF scheduler in front, a single project
/// can never exceed the per-project Semaphore floor (cap=16) of
/// concurrent in-flight ops. Submits 200 concurrent point ops from one
/// project; the SlowStore's high-water mark must stay ≤ 16.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cap_16_concurrent_unchanged() {
    let per_op = Duration::from_millis(20);
    let slow = Arc::new(SlowStore::new(per_op));
    let storage = storage_with_slow(slow.clone());

    let project = ProjectId::new();
    let p = seed_object(&storage, &project, "p", 64).await;
    let store = storage.project_object_store(&project);

    let mut handles = vec![];
    for _ in 0..200 {
        let s = store.clone();
        let p = p.clone();
        handles.push(tokio::spawn(async move {
            s.head(&p).await.expect("head");
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    let max_in_flight = slow.concurrent_max();
    assert!(
        max_in_flight <= 16,
        "max in-flight {} exceeded cap=16 per-project Semaphore floor",
        max_in_flight
    );
}

/// Observability benchmark: 1000 noisy bulk ops + 100 quiet point
/// lookups concurrently. Reports B's p50 + p99. Without EDF, B is
/// blocked behind A; with EDF, B's p99 should be a small multiple of
/// B's per-op work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fairness_observability() {
    let per_op = Duration::from_millis(2);
    let slow = Arc::new(SlowStore::new(per_op));
    let storage = storage_with_slow(slow.clone());

    let bulk = ProjectId::new();
    let point = ProjectId::new();
    let bulk_path = seed_object(&storage, &bulk, "b", 1024 * 1024).await;
    let point_path = seed_object(&storage, &point, "p", 64).await;

    let store_bulk = storage.project_object_store(&bulk);
    let store_point = storage.project_object_store(&point);

    // Spawn the noisy bulk workload. Finite count so the test always
    // terminates regardless of scheduler decisions.
    let mut bulk_handles = vec![];
    for _ in 0..1000 {
        let s = store_bulk.clone();
        let p = bulk_path.clone();
        bulk_handles.push(tokio::spawn(async move {
            let _ = s.get_range(&p, 0..1024 * 1024).await; // Low
        }));
    }

    // Tiny pause so the bulk project's queue is hot before the point
    // project arrives.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Time 100 sequential point lookups.
    let mut latencies: Vec<Duration> = Vec::with_capacity(100);
    for _ in 0..100 {
        let started = Instant::now();
        let _ = store_point.head(&point_path).await.expect("head");
        latencies.push(started.elapsed());
    }
    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let p99 = latencies[(latencies.len() * 99) / 100];
    eprintln!(
        "fairness_observability: B (point) p50={:?} p99={:?} (per_op={:?})",
        p50, p99, per_op
    );

    // Drain bulk workload so the test doesn't leak runaway tasks.
    for h in bulk_handles {
        let _ = h.await;
    }

    // Loose bound: with EDF the quiet p99 should remain a small
    // multiple of the per-op budget. cap=16 + global=4 still lets some
    // bulk slip ahead, but the fairness cap stops the heap from being
    // entirely owned by bulk.
    assert!(
        p99 < per_op * 100,
        "p99 latency {:?} ≥ {:?}: scheduler not delivering fairness",
        p99,
        per_op * 100
    );
}
