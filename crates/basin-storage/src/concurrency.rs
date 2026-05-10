//! Per-tenant concurrency limiting on top of an [`ObjectStore`].
//!
//! Two-level gating, both layers required:
//!
//! 1. A per-tenant [`tokio::sync::Semaphore`] (default cap 16) — the
//!    *liveness floor*. Sized so the Parquet reader's range fan-out
//!    × concurrent queries per tenant never exceeds it (smaller caps
//!    deadlock; see ADR 0008). It still serves as the "no single tenant
//!    can use more than N concurrent RPCs" guarantee.
//! 2. A global EDF [`Scheduler`] (default budget 4) across all tenants.
//!    Each request gets a deadline based on its priority class
//!    (point-shaped vs bulk-shaped) and the dispatcher pulls the
//!    earliest-deadline first within the global budget. Point reads
//!    (HEAD / GET-opts / small range / LIST) carry a 5ms deadline;
//!    bulk ops (PUT / multipart / large range) carry a 1s deadline so
//!    they can't crowd out point lookups. A `CONSECUTIVE_DISPATCH_CAP`
//!    inside the scheduler prevents one tenant from flooding the heap
//!    with deadline=now requests and starving everyone else.
//!
//! Per-RPC ordering: acquire the per-tenant semaphore FIRST (cheap when
//! we're under the floor), THEN the scheduler permit. Holding the
//! per-tenant permit while waiting on the scheduler is fine: all
//! tenants do the same dance, so there's no priority-inversion path
//! between them. Within one tenant, the per-tenant floor serializes us
//! into the scheduler one request at a time per concurrent caller — the
//! scheduler then re-fairs across tenants.
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
use basin_common::TenantId;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult,
};
use tokio::sync::Semaphore;

use crate::scheduler::{Priority, Scheduler, PRIORITY_RANGE_BYTES_THRESHOLD};

/// Wraps an [`ObjectStore`] so every RPC it forwards is gated on
/// (a) the per-tenant liveness-floor semaphore, then (b) the cross-tenant
/// EDF scheduler. Both fields are cheap-to-clone `Arc`s shared with
/// every other [`TenantScopedStore`] for the same tenant — so concurrent
/// reads from the engine layer and from inside `basin-storage` itself
/// contend on the *same* permit pool and the *same* scheduler heap.
#[derive(Debug)]
pub(crate) struct TenantScopedStore {
    inner: Arc<dyn ObjectStore>,
    sem: Arc<Semaphore>,
    scheduler: Scheduler,
    tenant: TenantId,
}

impl TenantScopedStore {
    pub(crate) fn new(
        inner: Arc<dyn ObjectStore>,
        sem: Arc<Semaphore>,
        scheduler: Scheduler,
        tenant: TenantId,
    ) -> Self {
        Self {
            inner,
            sem,
            scheduler,
            tenant,
        }
    }
}

impl std::fmt::Display for TenantScopedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TenantScopedStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for TenantScopedStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        // PUT is bulk-shaped: bytes-on-the-wire scale with the payload,
        // and a small write is rare in our writer path (we batch into
        // Parquet files). Schedule as Low so it never starves point
        // reads.
        let _slot = self.scheduler.acquire(self.tenant, Priority::Low).await;
        self.inner.put_opts(location, payload, opts).await
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
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        let _slot = self.scheduler.acquire(self.tenant, Priority::Low).await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        // Full-object GET is bulk-shaped on data files (Parquet bodies
        // are MB-shaped) and sub-MB on footers / sidecars. Without a
        // size hint, default to High so footers / metadata fetches stay
        // snappy; the engine's range-read path is the bulk channel.
        let _slot = self.scheduler.acquire(self.tenant, Priority::High).await;
        self.inner.get_opts(location, options).await
    }

    async fn get_range(
        &self,
        location: &ObjectPath,
        range: std::ops::Range<usize>,
    ) -> object_store::Result<Bytes> {
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        // Range size determines priority: small range = point-shaped
        // (footer / dictionary page / single page), large range =
        // bulk-shaped (full row group). The threshold is the same one
        // `concurrency.rs` heuristics flip on (PRIORITY_RANGE_BYTES_THRESHOLD).
        let priority = if range.end.saturating_sub(range.start) >= PRIORITY_RANGE_BYTES_THRESHOLD {
            Priority::Low
        } else {
            Priority::High
        };
        let _slot = self.scheduler.acquire(self.tenant, priority).await;
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &ObjectPath) -> object_store::Result<ObjectMeta> {
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        // HEAD is canonical point-shape: 0 bytes, single round-trip.
        let _slot = self.scheduler.acquire(self.tenant, Priority::High).await;
        self.inner.head(location).await
    }

    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        let _slot = self.scheduler.acquire(self.tenant, Priority::High).await;
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        // We collect the inner stream into a Vec under a single
        // permit + scheduler slot, then return a stream over that Vec.
        // Every caller in `basin-storage` and the engine listings
        // consumer drives the stream to exhaustion before moving on.
        //
        // LIST is metadata-shaped → High priority by default.
        use futures::stream::StreamExt;
        let sem = self.sem.clone();
        let scheduler = self.scheduler.clone();
        let tenant = self.tenant;
        let inner_stream = self.inner.list(prefix);
        futures::stream::once(async move {
            let _floor = sem.acquire_owned().await.expect("semaphore not closed");
            let _slot = scheduler.acquire(tenant, Priority::High).await;
            inner_stream.collect::<Vec<_>>().await
        })
        .flat_map(|items| futures::stream::iter(items))
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        let _slot = self.scheduler.acquire(self.tenant, Priority::High).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        // COPY is server-side; from the client's perspective it's a
        // single small request, so High.
        let _slot = self.scheduler.acquire(self.tenant, Priority::High).await;
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        let _slot = self.scheduler.acquire(self.tenant, Priority::High).await;
        self.inner.copy_if_not_exists(from, to).await
    }
}
