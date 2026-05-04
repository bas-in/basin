//! Per-tenant concurrency limiting on top of an [`ObjectStore`].
//!
//! Two-level gating, both layers required (v0.2):
//!
//! 1. A per-tenant [`tokio::sync::Semaphore`] (default cap 16) — the v0.1
//!    *liveness floor*. Sized so the Parquet reader's range fan-out
//!    × concurrent queries per tenant never exceeds it (smaller caps
//!    deadlock; see ADR 0008). It still serves as the "no single tenant
//!    can use more than N concurrent RPCs" guarantee.
//! 2. A global [`Scheduler`] with a fair-share round-robin dispatcher
//!    (default budget 16) across all tenants. This is the v0.2 fix for
//!    the noisy-neighbor benchmark on bounded backends (single-process
//!    MinIO at ~8–12 server-side concurrent reads). On real S3 it's a
//!    no-op for fairness — its job is to ensure that when the
//!    server-side gets queue-bound, quiet tenants still get their fair
//!    turn.
//!
//! Per-RPC ordering: acquire the per-tenant permit FIRST (which costs
//! ~nothing once we're under the floor), then the scheduler permit.
//! Holding the per-tenant permit while waiting on the scheduler is fine:
//! all tenants do the same dance, so there's no priority-inversion path
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
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult,
};
use tokio::sync::Semaphore;

/// Wraps an [`ObjectStore`] so every RPC it forwards is gated on the
/// per-tenant liveness-floor semaphore. The semaphore is supplied by
/// [`Storage`] and is shared between every [`TenantScopedStore`] for the
/// same tenant — so concurrent reads from the engine layer and from
/// inside `basin-storage` itself contend on the *same* permit pool.
///
/// The fair-share scheduler that lived here in v0.2/v0.3 was reverted:
/// the EDF dispatcher had a deadlock that surfaced even on LocalFS and
/// we couldn't trust it. The architectural fairness primitive is just
/// the per-tenant Semaphore for now; ADR 0008 documents the v0.3 path.
#[derive(Debug)]
pub(crate) struct TenantScopedStore {
    inner: Arc<dyn ObjectStore>,
    sem: Arc<Semaphore>,
}

impl TenantScopedStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, sem: Arc<Semaphore>) -> Self {
        Self { inner, sem }
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
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        self.inner.get_opts(location, options).await
    }

    async fn get_range(
        &self,
        location: &ObjectPath,
        range: std::ops::Range<usize>,
    ) -> object_store::Result<Bytes> {
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &ObjectPath) -> object_store::Result<ObjectMeta> {
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        self.inner.head(location).await
    }

    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        // We collect the inner stream into a Vec under a single
        // permit, then return a stream over that Vec. Every caller in
        // `basin-storage` and the engine listings consumer drives the
        // stream to exhaustion before moving on (see
        // `reader::list_data_files`, `reader::read`'s prelude, and
        // `vector_index::vector_search`), so the materialised-to-Vec
        // semantics match observed usage. This also gives us exactly
        // the right fairness: the entire LIST (including pagination)
        // consumes one permit, just like a point read or range read.
        //
        // Memory: list responses are O(files-per-table). At v0.1 scale
        // (max millions of files per table) this is bounded by the
        // workload; if compaction / partitioning ever shrinks the
        // listing too aggressively we'd revisit, but for v0.2 the
        // simple shape is preferred over a per-page permit dance.
        //
        // LIST is metadata-shaped → High priority by default. A
        // single LIST often unblocks many subsequent reads, so we
        // want quiet tenants' LISTs to skip noisy tenants' bulk PUTs.
        use futures::stream::StreamExt;
        let sem = self.sem.clone();
        let inner_stream = self.inner.list(prefix);
        futures::stream::once(async move {
            let _floor = sem.acquire_owned().await.expect("semaphore not closed");
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
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        let _floor = self.sem.acquire().await.expect("semaphore not closed");
        self.inner.copy_if_not_exists(from, to).await
    }
}
