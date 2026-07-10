//! Per-project concurrency limiting on top of an [`ObjectStore`].
//!
//! Two-level gating, both layers required:
//!
//! 1. A per-project [`tokio::sync::Semaphore`] (default cap 16) — the
//!    *liveness floor*. Sized so the Parquet reader's range fan-out
//!    × concurrent queries per project never exceeds it (smaller caps
//!    deadlock; see ADR 0008). It still serves as the "no single project
//!    can use more than N concurrent RPCs" guarantee.
//! 2. A global EDF [`Scheduler`] (default budget 4) across all projects.
//!    Each request gets a deadline based on its priority class
//!    (point-shaped vs bulk-shaped) and the dispatcher pulls the
//!    earliest-deadline first within the global budget. Point reads
//!    (HEAD / GET-opts / small range / LIST) carry a 5ms deadline;
//!    bulk ops (PUT / multipart / large range) carry a 1s deadline so
//!    they can't crowd out point lookups. A `CONSECUTIVE_DISPATCH_CAP`
//!    inside the scheduler prevents one project from flooding the heap
//!    with deadline=now requests and starving everyone else.
//!
//! Per-RPC ordering: acquire the per-project semaphore FIRST (cheap when
//! we're under the floor), THEN the scheduler permit. Holding the
//! per-project permit while waiting on the scheduler is fine: all
//! projects do the same dance, so there's no priority-inversion path
//! between them. Within one project, the per-project floor serializes us
//! into the scheduler one request at a time per concurrent caller — the
//! scheduler then re-fairs across projects.
//!
//! The wrapper is at the granularity of a *single* underlying object_store
//! RPC. Holding a permit across many fan-out RPCs would funnel a Parquet
//! scan through one slot and tank latency; we acquire per RPC, the
//! natural granularity for fairness.
//!
//! Deadlock note: a permit (either layer) is only ever held across the
//! duration of one `await` on the inner store. We never hold a permit
//! across an await that re-enters `Storage`, so there is no risk of a
//! permit-holder waiting on a permit-holder.

use std::sync::Arc;

use async_trait::async_trait;
use basin_common::{ProjectCounters, ProjectId};
use futures::stream::BoxStream;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult,
};
use tokio::sync::Semaphore;

use crate::scheduler::{Priority, Scheduler};

tokio::task_local! {
    /// Marks the current async task's object-store RPCs as BACKGROUND
    /// (compaction / merge). When set, full-object GETs are demoted from High to
    /// Low priority so compaction reads never PREEMPT live ingest writes (also
    /// Low) on the shared per-project EDF scheduler — the fix for the
    /// compaction-vs-ingest priority INVERSION that decayed ingest r/s as a
    /// table grows (#78: a full-object GET defaulted to High, so merge's
    /// data-file body reads out-ranked ingest PUTs). Propagates through the
    /// merge read/write chain because that path never `tokio::spawn`s.
    static BACKGROUND_IO: bool;
}

/// Run `f` with its object-store RPCs marked background (see [`BACKGROUND_IO`]).
/// Wrap compaction / merge work in this so its I/O yields to live ingest and
/// ingest throughput stays flat as a table grows.
pub async fn with_background_io<F>(f: F) -> F::Output
where
    F: std::future::Future,
{
    BACKGROUND_IO.scope(true, f).await
}

#[inline]
fn is_background_io() -> bool {
    BACKGROUND_IO.try_with(|b| *b).unwrap_or(false)
}

/// Process-wide cap on concurrent BACKGROUND (compaction / merge) object-store
/// I/O. Demoting merge reads to Low (see [`BACKGROUND_IO`]) removes the
/// priority INVERSION, but merge and live-ingest PUTs then share the Low pool
/// as equals — so as a table grows and merge volume accumulates, merge still
/// nibbles a fixed slice of ingest's bandwidth, stepping ingest r/s down
/// (~0.73 flatness at 200M, measured). This semaphore bounds how many merge
/// I/Os can be in flight at once, so merge can never consume more than N of any
/// project's scheduler budget and live ingest keeps the rest — ingest r/s stays
/// FLAT as the table grows (#78). Merge still makes steady progress at N-way
/// concurrency; the sealed-file exclusion already bounds merge WORK per tick.
/// Tunable via `BASIN_STORAGE_BG_CONCURRENCY` (default 6) so the flat-vs-keep-up
/// balance can be dialled in from config without a rebuild.
fn bg_io_semaphore() -> &'static Semaphore {
    static SEM: std::sync::OnceLock<Semaphore> = std::sync::OnceLock::new();
    SEM.get_or_init(|| {
        let n = std::env::var("BASIN_STORAGE_BG_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(6);
        Semaphore::new(n)
    })
}

/// Wraps an [`ObjectStore`] so every RPC it forwards is gated on
/// (a) the per-project liveness-floor semaphore, then (b) the cross-project
/// EDF scheduler. Both fields are cheap-to-clone `Arc`s shared with
/// every other [`ProjectScopedStore`] for the same project — so concurrent
/// reads from the engine layer and from inside `basin-storage` itself
/// contend on the *same* permit pool and the *same* scheduler heap.
///
/// Optionally carries a per-project [`ProjectCounters`] handle. When
/// present, every forwarded RPC bumps the corresponding Class-A
/// (state-changing: PUT, multipart-complete, COPY, DELETE) or Class-B
/// (read: GET, HEAD, LIST) op counter, feeding the basin-cloud billing
/// meter per the `2026-05-21-billing-meter-gap.md` audit. Counter bumps
/// happen *after* the inner RPC succeeds so failed/rejected ops aren't
/// billed (errors go through `record_error` on the counters at higher
/// layers).
#[derive(Debug)]
pub(crate) struct ProjectScopedStore {
    inner: Arc<dyn ObjectStore>,
    sem: Arc<Semaphore>,
    scheduler: Scheduler,
    project: ProjectId,
    /// `None` when no `ProjectCounterRegistry` is attached to `Storage`
    /// (legacy test paths) — the bump is then a no-op via `if let Some(..)`.
    counters: Option<Arc<ProjectCounters>>,
}

impl ProjectScopedStore {
    pub(crate) fn new(
        inner: Arc<dyn ObjectStore>,
        sem: Arc<Semaphore>,
        scheduler: Scheduler,
        project: ProjectId,
        counters: Option<Arc<ProjectCounters>>,
    ) -> Self {
        Self {
            inner,
            sem,
            scheduler,
            project,
            counters,
        }
    }

    /// Bump the Class-A op counter by one if a registry is attached.
    fn bump_class_a(&self) {
        if let Some(c) = &self.counters {
            c.record_class_a_op();
        }
    }

    /// Bump the Class-B op counter by one if a registry is attached.
    fn bump_class_b(&self) {
        if let Some(c) = &self.counters {
            c.record_class_b_op();
        }
    }

    /// Bump the Class-B op counter by `n` (used for streaming LIST where
    /// one logical call can yield many continuation requests; we charge
    /// at least one even if `n == 0` for the "issued the LIST" intent).
    fn bump_class_b_n(&self, n: u64) {
        if let Some(c) = &self.counters {
            c.record_class_b_ops(n.max(1));
        }
    }
}

impl std::fmt::Display for ProjectScopedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProjectScopedStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ProjectScopedStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        // #78: cap concurrent BACKGROUND (merge) output writes too, so merge's
        // PUTs can't crowd live ingest PUTs out of the shared Low pool. Live
        // ingest / tail-drain PUTs run outside the background scope and are
        // never capped here.
        let _bg = if is_background_io() {
            Some(
                bg_io_semaphore()
                    .acquire()
                    .await
                    .expect("bg io semaphore not closed"),
            )
        } else {
            None
        };
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        // PUT is bulk-shaped: bytes-on-the-wire scale with the payload,
        // and a small write is rare in our writer path (we batch into
        // Parquet files). Schedule as Low so it never starves point
        // reads.
        let _slot = self.scheduler.acquire(self.project, Priority::Low).await;
        let res = self.inner.put_opts(location, payload, opts).await;
        // Class-A: state-changing. Only bill successful RPCs — failed
        // PUTs aren't billable in the Tigris/S3 cost model.
        if res.is_ok() {
            self.bump_class_a();
        }
        res
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        // The outer call (the one that sets up the multipart session) takes
        // a permit; the per-part calls go through the returned trait object
        // which is owned by the caller. Wrapping each part's I/O would
        // require interposing on `MultipartUpload`, which we punt on:
        // the writer path uses `put` for our Parquet sizes, and the
        // multipart path is only reachable through the engine's analytical
        // plumbing where we don't yet drive uploads at this scale.
        //
        // Billing note: the multipart *completion* is the Class-A op in
        // the Tigris/S3 model. Initiating the session bumps Class-A here
        // because we don't currently interpose on the returned
        // `MultipartUpload` — under-counting per-part PUTs is the safer
        // (less surprising-bill) default until the interposer lands.
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        let _slot = self.scheduler.acquire(self.project, Priority::Low).await;
        let res = self.inner.put_multipart_opts(location, opts).await;
        if res.is_ok() {
            self.bump_class_a();
        }
        res
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // #78: cap concurrent BACKGROUND (merge) reads so they can't crowd live
        // ingest out of the shared Low pool. Acquired first and held for the
        // whole RPC. No-op for foreground reads.
        let _bg = if is_background_io() {
            Some(
                bg_io_semaphore()
                    .acquire()
                    .await
                    .expect("bg io semaphore not closed"),
            )
        } else {
            None
        };
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        // Full-object GET is bulk-shaped on data files (Parquet bodies
        // are MB-shaped) and sub-MB on footers / sidecars. Without a
        // size hint, default to High so footers / metadata fetches stay
        // snappy; the engine's range-read path is the bulk channel.
        //
        // #78: BACKGROUND (compaction / merge) reads are demoted to Low so their
        // MB-shaped data-file body GETs never PREEMPT live ingest writes (also
        // Low) — foreground reads keep High so query footers stay snappy. This
        // removes the priority inversion where compaction I/O out-ranked live
        // writes, decaying ingest r/s as a table grew.
        let read_prio = if is_background_io() {
            Priority::Low
        } else {
            Priority::High
        };
        let _slot = self.scheduler.acquire(self.project, read_prio).await;
        let is_head = options.head;
        let res = self.inner.get_opts(location, options).await;
        // Class-B: GET / HEAD. `GetOptions { head: true, .. }` is the
        // HEAD-only path (no body); still one Class-B op.
        if res.is_ok() {
            let _ = is_head; // both HEAD and GET are Class-B, single bump.
            self.bump_class_b();
        }
        res
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        // We need per-item billing for DELETE (Class-A), so wrap the
        // inner stream and bump on every successful item yielded. The
        // semaphore/scheduler gating still happens inside the inner
        // store's per-item delete path.
        use futures::stream::StreamExt;
        let inner_stream = self.inner.delete_stream(locations);
        let counters = self.counters.clone();
        inner_stream
            .inspect(move |res| {
                if res.is_ok() {
                    if let Some(c) = &counters {
                        c.record_class_a_op();
                    }
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        // We collect the inner stream into a Vec under a single
        // permit + scheduler slot, then return a stream over that Vec.
        // Every caller in `basin-storage` and the engine listings
        // consumer drives the stream to exhaustion before moving on.
        //
        // LIST is metadata-shaped → High priority by default.
        //
        // Billing: one Class-B op per `list()` call, regardless of how
        // many objects are returned (matches the Tigris/S3 wire model
        // where a single LIST request returns up to 1000 keys; we don't
        // currently observe continuation-token pagination at this layer,
        // so a single bump per logical call is the right under-bound).
        use futures::stream::StreamExt;
        let sem = self.sem.clone();
        let scheduler = self.scheduler.clone();
        let project = self.project;
        let inner = self.inner.clone();
        let prefix = prefix.cloned();
        let counters = self.counters.clone();
        futures::stream::once(async move {
            let _floor = sem.acquire_owned().await.expect("semaphore not closed");
            let _slot = scheduler.acquire(project, Priority::High).await;
            let items = inner.list(prefix.as_ref()).collect::<Vec<_>>().await;
            if let Some(c) = &counters {
                c.record_class_b_op();
            }
            items
        })
        .flat_map(futures::stream::iter)
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        let _slot = self.scheduler.acquire(self.project, Priority::High).await;
        let res = self.inner.list_with_delimiter(prefix).await;
        if res.is_ok() {
            // Charge the same single Class-B op as `list()`; the
            // delimiter variant is also a single LIST RPC at the wire.
            self.bump_class_b_n(1);
        }
        res
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        // COPY is server-side; from the client's perspective it's a
        // single small request, so High. CopyMode::Overwrite = old `copy`,
        // CopyMode::Create = old `copy_if_not_exists`.
        let _slot = self.scheduler.acquire(self.project, Priority::High).await;
        let res = self.inner.copy_opts(from, to, options).await;
        // Class-A: COPY is state-changing.
        if res.is_ok() {
            self.bump_class_a();
        }
        res
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for Class-A / Class-B op metering.
    //!
    //! We exercise the wrapper directly against an `InMemory` object store
    //! (no `Storage` machinery needed) so the test stays focused on the
    //! counter-bump invariants and isn't sensitive to compaction or
    //! catalog state. Cross-project isolation is asserted by running the
    //! same operations against two wrappers with distinct project ids
    //! sharing the same registry, and confirming each project's snapshot
    //! reflects only its own ops.

    use super::*;
    use basin_common::ProjectCounterRegistry;
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use object_store::PutPayload;

    fn make_store(
        project: ProjectId,
        registry: &Arc<ProjectCounterRegistry>,
        inner: Arc<dyn ObjectStore>,
    ) -> ProjectScopedStore {
        let sem = Arc::new(Semaphore::new(16));
        let scheduler = Scheduler::new(crate::DEFAULT_GLOBAL_BUDGET);
        let counters = Some(registry.for_project(&project));
        ProjectScopedStore::new(inner, sem, scheduler, project, counters)
    }

    #[tokio::test]
    async fn put_get_list_delete_increment_class_a_b_counters() {
        let registry = Arc::new(ProjectCounterRegistry::new());
        let project = ProjectId::new();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = make_store(project, &registry, inner);

        // 1 PUT — Class-A == 1.
        let path = ObjectPath::from("a/b/c");
        store
            .put_opts(&path, PutPayload::from_static(b"hello"), Default::default())
            .await
            .expect("put_opts");
        let s = registry.snapshot(&project).unwrap();
        assert_eq!(s.class_a_ops_total, 1, "PUT should bump Class-A");
        assert_eq!(s.class_b_ops_total, 0, "PUT must not bump Class-B");

        // 1 GET — Class-B == 1.
        let got = store
            .get_opts(&path, Default::default())
            .await
            .expect("get_opts");
        let _ = got.bytes().await.unwrap();
        let s = registry.snapshot(&project).unwrap();
        assert_eq!(s.class_a_ops_total, 1);
        assert_eq!(s.class_b_ops_total, 1, "GET should bump Class-B");

        // 1 HEAD via get_opts(head=true) — Class-B == 2.
        let head_opts = object_store::GetOptions {
            head: true,
            ..Default::default()
        };
        store
            .get_opts(&path, head_opts)
            .await
            .expect("get_opts(head=true)");
        let s = registry.snapshot(&project).unwrap();
        assert_eq!(s.class_b_ops_total, 2, "HEAD-as-GET should also bump Class-B");

        // 1 LIST — Class-B == 3.
        let listed: Vec<_> = store.list(None).collect().await;
        assert!(!listed.is_empty());
        let s = registry.snapshot(&project).unwrap();
        assert_eq!(
            s.class_b_ops_total, 3,
            "LIST stream consumption should bump Class-B once"
        );

        // 1 list_with_delimiter — Class-B == 4.
        let _ = store
            .list_with_delimiter(None)
            .await
            .expect("list_with_delimiter");
        let s = registry.snapshot(&project).unwrap();
        assert_eq!(s.class_b_ops_total, 4);

        // 1 COPY — Class-A == 2.
        let path_copy = ObjectPath::from("a/b/c2");
        store
            .copy_opts(&path, &path_copy, Default::default())
            .await
            .expect("copy_opts");
        let s = registry.snapshot(&project).unwrap();
        assert_eq!(s.class_a_ops_total, 2, "COPY should bump Class-A");

        // DELETE 2 items via delete_stream — Class-A == 4.
        let to_delete =
            futures::stream::iter(vec![Ok(path.clone()), Ok(path_copy.clone())]).boxed();
        let results: Vec<_> = store.delete_stream(to_delete).collect().await;
        assert_eq!(results.len(), 2);
        for r in &results {
            assert!(r.is_ok(), "delete failed: {r:?}");
        }
        let s = registry.snapshot(&project).unwrap();
        assert_eq!(
            s.class_a_ops_total, 4,
            "two DELETEs via delete_stream should bump Class-A by 2"
        );
        assert_eq!(s.class_b_ops_total, 4, "DELETEs must not bump Class-B");
    }

    #[tokio::test]
    async fn cross_project_isolation_class_a_b() {
        // Same shared registry + same shared inner store, but two distinct
        // project ids. Project A does writes only; project B does reads only.
        // After the workload each project's snapshot must reflect only its
        // own ops — no cross-project bleed.
        let registry = Arc::new(ProjectCounterRegistry::new());
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let project_a = ProjectId::new();
        let project_b = ProjectId::new();
        let store_a = make_store(project_a, &registry, inner.clone());
        let store_b = make_store(project_b, &registry, inner.clone());

        // Project A: 3 PUTs.
        for i in 0..3 {
            let key = ObjectPath::from(format!("a/{i}"));
            store_a
                .put_opts(&key, PutPayload::from_static(b"x"), Default::default())
                .await
                .expect("put_opts (project A)");
        }

        // Project B: 2 GETs against a key project A wrote (so the read
        // succeeds and the Class-B bump fires), plus one LIST.
        let key_a0 = ObjectPath::from("a/0");
        for _ in 0..2 {
            let got = store_b
                .get_opts(&key_a0, Default::default())
                .await
                .expect("get_opts (project B)");
            let _ = got.bytes().await.unwrap();
        }
        let _listed: Vec<_> = store_b.list(None).collect().await;

        let snap_a = registry.snapshot(&project_a).expect("snapshot A");
        let snap_b = registry.snapshot(&project_b).expect("snapshot B");

        assert_eq!(snap_a.class_a_ops_total, 3, "project A wrote 3");
        assert_eq!(snap_a.class_b_ops_total, 0, "project A did no reads");
        assert_eq!(snap_b.class_a_ops_total, 0, "project B did no writes");
        assert_eq!(
            snap_b.class_b_ops_total, 3,
            "project B did 2 GET + 1 LIST = 3 Class-B ops"
        );
    }

    #[tokio::test]
    async fn no_registry_attached_is_a_noop() {
        // Confirm the legacy un-instrumented path stays compatible: when
        // counters is None, no panics, no extra alloc, all ops succeed.
        let project = ProjectId::new();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sem = Arc::new(Semaphore::new(16));
        let scheduler = Scheduler::new(crate::DEFAULT_GLOBAL_BUDGET);
        let store = ProjectScopedStore::new(inner, sem, scheduler, project, None);

        let path = ObjectPath::from("x");
        store
            .put_opts(&path, PutPayload::from_static(b"y"), Default::default())
            .await
            .expect("put_opts (no registry)");
        let got = store
            .get_opts(&path, Default::default())
            .await
            .expect("get_opts (no registry)");
        let _ = got.bytes().await.unwrap();
    }

    // ── #78: compaction/merge reads must NOT preempt live ingest ──────────
    //
    // The priority INVERSION we fixed: a full-object GET defaults to
    // Priority::High (footers / point reads want to stay snappy), but a
    // compaction *merge* streams MB-shaped data-file bodies through the same
    // GET path — so merge reads used to out-rank live ingest PUTs
    // (Priority::Low) on the shared per-project EDF scheduler. As a table
    // grew, more merge GETs starved ingest and write r/s decayed with table
    // size. `with_background_io` demotes merge reads to Low so they share a
    // lane with ingest PUTs instead of preempting them.

    #[tokio::test]
    async fn background_io_flag_scopes_and_resets() {
        assert!(!is_background_io(), "default must be foreground");
        with_background_io(async {
            assert!(is_background_io(), "inside scope must be background");
            // Propagates across an await point (same task, no spawn) — this
            // is exactly why the merge read/write chain (spawn-free) inherits
            // the flag.
            tokio::task::yield_now().await;
            assert!(is_background_io(), "still background after await");
        })
        .await;
        assert!(!is_background_io(), "must reset to foreground after scope");
    }

    /// An `ObjectStore` decorator that blocks every GET on a semaphore the
    /// test controls, so we can hold GETs in-flight and observe how the
    /// scheduler placed them (which priority pool) — deterministically, with
    /// no wall-clock timing. `entered` counts GETs that got PAST the scheduler
    /// and reached this store (i.e. were dispatched, not left queued).
    #[derive(Debug)]
    struct GateStore {
        inner: Arc<dyn ObjectStore>,
        gate: Arc<Semaphore>,
        entered: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl std::fmt::Display for GateStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "GateStore({})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for GateStore {
        async fn put_opts(
            &self,
            l: &ObjectPath,
            p: PutPayload,
            o: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &ObjectPath,
            o: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(
            &self,
            l: &ObjectPath,
            o: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.entered
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Block until the test releases a permit; consume it so each
            // release admits exactly one GET.
            self.gate.acquire().await.expect("gate closed").forget();
            self.inner.get_opts(l, o).await
        }
        fn delete_stream(
            &self,
            locs: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locs)
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            o: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, o).await
        }
    }

    #[tokio::test]
    async fn background_reads_take_low_lane_not_high() {
        // Budget 16 → High pool = 4, Low pool = 12 (see split_budget).
        // Saturate High with 4 foreground GETs; a 5th foreground GET must
        // QUEUE on High (there is no High-borrows-Low rule). A BACKGROUND
        // GET, though, is demoted to Low and dispatches on the reserved Low
        // pool — proving merge reads get their own lane and never queue
        // behind / preempt foreground reads (and, being Low like ingest PUTs,
        // never preempt ingest either). Before the fix the background GET
        // would have been High and contended in the same pool as the
        // foreground reads.
        let project = ProjectId::new();
        let gate = Arc::new(Semaphore::new(0));
        let entered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let inner: Arc<dyn ObjectStore> = Arc::new(GateStore {
            inner: Arc::new(InMemory::new()),
            gate: gate.clone(),
            entered: entered.clone(),
        });
        let sem = Arc::new(Semaphore::new(1024)); // floor wide open; scheduler is the gate
        let scheduler = Scheduler::new(16);
        let store = Arc::new(ProjectScopedStore::new(
            inner,
            sem,
            scheduler.clone(),
            project,
            None,
        ));

        // Seed the object the GETs will read (PUT is not gated).
        let path = ObjectPath::from("obj");
        store
            .put_opts(&path, PutPayload::from_static(b"v"), Default::default())
            .await
            .expect("seed put");

        // 5 foreground GETs (High) + 1 background GET (demoted to Low).
        let mut handles = Vec::new();
        for _ in 0..5 {
            let s = store.clone();
            let p = path.clone();
            handles.push(tokio::spawn(async move {
                let _ = s.get_opts(&p, Default::default()).await;
            }));
        }
        {
            let s = store.clone();
            let p = path.clone();
            handles.push(tokio::spawn(async move {
                let _ = with_background_io(s.get_opts(&p, Default::default())).await;
            }));
        }

        // Drive the runtime until steady state: 5 GETs dispatched (4 High +
        // 1 Low) and reached the gate, with exactly 1 foreground GET left
        // queued on High. High is hard-capped at 4, so the only way a 5th GET
        // reaches the store is the BACKGROUND one dispatching on Low.
        let mut settled = false;
        let mut last = scheduler.project_stats(&project);
        for _ in 0..100_000 {
            tokio::task::yield_now().await;
            last = scheduler.project_stats(&project);
            let seen = entered.load(std::sync::atomic::Ordering::SeqCst);
            if last.in_flight == 5 && last.queue_depth_high == 1 && seen == 5 {
                settled = true;
                break;
            }
        }
        assert!(
            settled,
            "scheduler never reached steady state; last = {last:?}, entered = {}",
            entered.load(std::sync::atomic::Ordering::SeqCst),
        );

        // The crux: with High capped at 4, a 5th GET reaching the store proves
        // the background read dispatched on the Low pool.
        assert_eq!(
            last.queue_depth_low, 0,
            "background read must NOT be queued — it owns the Low lane",
        );
        assert_eq!(
            last.queue_depth_high, 1,
            "exactly one foreground read stays queued behind saturated High",
        );

        // Release everything and let the tasks finish cleanly.
        gate.add_permits(6);
        for h in handles {
            let _ = h.await;
        }
    }
}
