//! #36 (Stage 2a, Scheme C) — a striping-aware [`ObjectStore`] for the
//! DataFusion scan path.
//!
//! DataFusion registers exactly ONE object store per URL authority
//! (`basin://engine/`) and turns every `DataFileRef.path` into a
//! `basin://engine/<path>` object URL it resolves against that one store
//! (`basin-engine::session`). With partition→bucket striping ON, a data file's
//! bucket is a pure function of the partition encoded in its key (Scheme C in
//! `docs/multi-bucket-throughput-striping.md`). This wrapper is the single
//! switch point: it re-derives the partition from each operation's path and
//! delegates to that partition's stripe-bucket store, and it unions LISTs
//! across the whole stripe.
//!
//! ## No-op contract
//!
//! The engine only registers this wrapper when the pool is enabled AND the
//! project's warmed stripe is wider than one bucket
//! ([`Storage::should_stripe_scan`]). For every other case (pool absent, flag
//! OFF, width-1, unwarmed, or a BYO project) the engine registers the plain
//! [`Storage::project_object_store`] exactly as before — this type is never
//! constructed, so the scan path is byte-identical to today. Even if it WERE
//! constructed for a width-1 project, `store_for_data_file` collapses to the
//! single project store and `partition_stores_for_listing` yields one store,
//! so behaviour is still identical; the gate is purely to avoid the per-op
//! key parse when striping buys nothing.
//!
//! ## Why route per-op rather than register N stores
//!
//! Registering `basin://b0/`, `basin://b1/` … would touch the planner's URL
//! construction broadly (the rejected Scheme B). Routing inside one registered
//! store keeps the planner, `DataFileRef`, and the catalog untouched — the
//! file ref carries no bucket; the bucket is recovered at I/O time.

use std::sync::Arc;

use async_trait::async_trait;
use basin_common::ProjectId;
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult,
};

use crate::Storage;

/// Routes each object-store operation to the stripe bucket that owns the
/// operation's path, re-deriving the partition from the key (Scheme C). LISTs
/// union across the project's whole stripe. Holds a cheap `Storage` clone
/// (`Arc<Inner>`) plus the project id; every routing decision reuses the
/// crate's existing `store_for_data_file` / `partition_stores_for_listing`
/// helpers, so the per-project semaphore + scheduler + billing wrapping is
/// identical to the non-striped path.
pub(crate) struct StripeRouterStore {
    storage: Storage,
    project: ProjectId,
}

impl StripeRouterStore {
    pub(crate) fn new(storage: Storage, project: ProjectId) -> Self {
        Self { storage, project }
    }

    /// The (project-scoped) store that owns `location`'s partition.
    fn store_for(&self, location: &ObjectPath) -> Arc<dyn ObjectStore> {
        crate::reader::store_for_data_file(&self.storage, &self.project, location)
    }
}

impl std::fmt::Debug for StripeRouterStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StripeRouterStore(project={})", self.project)
    }
}

impl std::fmt::Display for StripeRouterStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StripeRouterStore(project={})", self.project)
    }
}

#[async_trait]
impl ObjectStore for StripeRouterStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.store_for(location).put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.store_for(location).put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // Covers GET, ranged GET, and HEAD: the provided `get_range`,
        // `get_ranges`, and `head` trait methods all route through `get_opts`,
        // so re-deriving the partition here is sufficient for every read shape
        // DataFusion drives.
        self.store_for(location).get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        // Route each delete to its partition's stripe bucket. Scans never
        // delete; this exists for trait completeness and correctness if a
        // maintenance path ever drives deletes through a registered store.
        let storage = self.storage.clone();
        let project = self.project;
        locations
            .flat_map(move |loc| {
                let storage = storage.clone();
                match loc {
                    Ok(loc) => {
                        let store =
                            crate::reader::store_for_data_file(&storage, &project, &loc);
                        // Delegate to the resolved store's own delete_stream
                        // with this single path (keeps per-store gating).
                        let single =
                            futures::stream::once(async move { Ok(loc) }).boxed();
                        store.delete_stream(single)
                    }
                    Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        // Union the LIST across the project's whole stripe. A partition lives
        // in exactly one bucket so the per-bucket subtrees are disjoint — no
        // object is double-listed or missed. Width-1 = a single store == the
        // plain LIST.
        let stores = self.storage.partition_stores_for_listing(&self.project);
        let prefix = prefix.cloned();
        futures::stream::iter(stores)
            .flat_map(move |store| store.list(prefix.as_ref()))
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        // Merge the per-bucket delimiter listings. DataFusion's listing-table
        // discovery uses `list` (above), not this; we provide a correct union
        // for completeness. Common prefixes and objects are concatenated; the
        // disjoint-subtree property keeps them unique.
        let stores = self.storage.partition_stores_for_listing(&self.project);
        let mut common_prefixes = Vec::new();
        let mut objects = Vec::new();
        for store in stores {
            let part = store.list_with_delimiter(prefix).await?;
            common_prefixes.extend(part.common_prefixes);
            objects.extend(part.objects);
        }
        Ok(ListResult {
            common_prefixes,
            objects,
        })
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        // Route by destination. A cross-bucket copy (from/to in different
        // stripes) is not something the scan path drives; scans never copy, so
        // routing by `to` is the safe, simple choice.
        self.store_for(to).copy_opts(from, to, options).await
    }
}
