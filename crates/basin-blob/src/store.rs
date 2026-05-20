//! `BlobStore` — storage-ops API for `basin-blob`.
//!
//! This is the primary entry point for all bucket and object operations.
//! It owns:
//!
//! - An in-process catalog of bucket and object rows (a thin `BlobCatalog`
//!   trait so tests can use the in-memory implementation and production can
//!   swap in a Postgres-backed one without changing call sites).
//! - An `Arc<dyn ObjectStore>` for the actual bytes (supplied by the caller
//!   — `BlobStore` never constructs one itself to avoid reaching into
//!   `basin-engine`).
//!
//! # Operations (5.17.A surface)
//!
//! | Method | Description |
//! |---|---|
//! | [`BlobStore::create_bucket`] | Insert a bucket row. |
//! | [`BlobStore::get_bucket`] | Look up a bucket by name. |
//! | [`BlobStore::put_object`] | Write bytes to `object_store` + upsert row. |
//! | [`BlobStore::get_object`] | Read bytes from `object_store` + return row. |
//! | [`BlobStore::delete_object`] | Delete bytes from `object_store` + remove row. |

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::{path::Path as ObjectPath, ObjectStore, ObjectStoreExt, PutPayload};
use tokio::sync::RwLock;

use basin_common::{ProjectCounterRegistry, ProjectId};

use crate::{
    error::{BlobError, Result},
    model::{Bucket, BucketId, Object},
    paths,
    rls::{CallerCtx, ObjectPolicy, ObjectPolicyCommand, ObjectRlsStore},
};

// ---------------------------------------------------------------------------
// BlobCatalog — thin trait so the row store is swappable
// ---------------------------------------------------------------------------

/// Minimal catalog trait that `BlobStore` depends on.
///
/// Production implementations back this with a Postgres table; the
/// [`InMemoryBlobCatalog`] backs unit tests.  Only the methods needed by
/// the 5.17.A surface are required here; additional queries (list, policy
/// enforcement) can be added in later phases without breaking the trait.
#[async_trait]
pub trait BlobCatalog: Send + Sync + 'static {
    // --- Bucket operations ---

    /// Insert a new bucket row.  Returns an error if a bucket with the same
    /// name already exists in this project.
    async fn insert_bucket(&self, project: &ProjectId, bucket: Bucket) -> Result<()>;

    /// Retrieve a bucket by name.
    async fn get_bucket_by_name(
        &self,
        project: &ProjectId,
        name: &str,
    ) -> Result<Option<Bucket>>;

    // --- Object operations ---

    /// Upsert an object row (insert or replace by `bucket_id + path`).
    async fn upsert_object(&self, project: &ProjectId, object: Object) -> Result<()>;

    /// Retrieve an object row by bucket + path.
    async fn get_object_by_path(
        &self,
        project: &ProjectId,
        bucket_id: BucketId,
        path: &str,
    ) -> Result<Option<Object>>;

    /// Remove an object row by bucket + path.  Returns `true` if a row was
    /// deleted, `false` if no matching row existed.
    async fn delete_object_by_path(
        &self,
        project: &ProjectId,
        bucket_id: BucketId,
        path: &str,
    ) -> Result<bool>;

    /// List all object rows for a bucket, optionally filtered to those whose
    /// `path` starts with `prefix`.  Returns an empty `Vec` when the bucket is
    /// empty or no paths match the prefix.
    async fn list_objects_by_prefix(
        &self,
        project: &ProjectId,
        bucket_id: BucketId,
        prefix: Option<&str>,
    ) -> Result<Vec<Object>>;
}

// ---------------------------------------------------------------------------
// InMemoryBlobCatalog — test / single-node implementation
// ---------------------------------------------------------------------------

/// A simple in-memory implementation of [`BlobCatalog`] backed by `RwLock`-
/// protected `HashMap`s.  Suitable for unit tests and single-process
/// development.  Not suitable for multi-node deployments.
#[derive(Debug, Default)]
pub struct InMemoryBlobCatalog {
    /// `(project, bucket_name)` → Bucket
    buckets: RwLock<HashMap<(String, String), Bucket>>,
    /// `(project, bucket_id, object_path)` → Object
    objects: RwLock<HashMap<(String, String, String), Object>>,
}

impl InMemoryBlobCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wrap in an `Arc` for use with [`BlobStore::new`].
    pub fn arc() -> Arc<Self> {
        Arc::new(Self::new())
    }
}

#[async_trait]
impl BlobCatalog for InMemoryBlobCatalog {
    async fn insert_bucket(&self, project: &ProjectId, bucket: Bucket) -> Result<()> {
        let key = (project.to_string(), bucket.name.clone());
        let mut guard = self.buckets.write().await;
        if guard.contains_key(&key) {
            return Err(BlobError::BucketAlreadyExists(bucket.name));
        }
        guard.insert(key, bucket);
        Ok(())
    }

    async fn get_bucket_by_name(
        &self,
        project: &ProjectId,
        name: &str,
    ) -> Result<Option<Bucket>> {
        let key = (project.to_string(), name.to_string());
        Ok(self.buckets.read().await.get(&key).cloned())
    }

    async fn upsert_object(&self, project: &ProjectId, object: Object) -> Result<()> {
        let key = (
            project.to_string(),
            object.bucket_id.to_string(),
            object.path.clone(),
        );
        self.objects.write().await.insert(key, object);
        Ok(())
    }

    async fn get_object_by_path(
        &self,
        project: &ProjectId,
        bucket_id: BucketId,
        path: &str,
    ) -> Result<Option<Object>> {
        let key = (
            project.to_string(),
            bucket_id.to_string(),
            path.to_string(),
        );
        Ok(self.objects.read().await.get(&key).cloned())
    }

    async fn delete_object_by_path(
        &self,
        project: &ProjectId,
        bucket_id: BucketId,
        path: &str,
    ) -> Result<bool> {
        let key = (
            project.to_string(),
            bucket_id.to_string(),
            path.to_string(),
        );
        Ok(self.objects.write().await.remove(&key).is_some())
    }

    async fn list_objects_by_prefix(
        &self,
        project: &ProjectId,
        bucket_id: BucketId,
        prefix: Option<&str>,
    ) -> Result<Vec<Object>> {
        let project_str = project.to_string();
        let bucket_id_str = bucket_id.to_string();
        let guard = self.objects.read().await;
        let mut results: Vec<Object> = guard
            .iter()
            .filter(|((p, b, path), _)| {
                p == &project_str
                    && b == &bucket_id_str
                    && prefix.map_or(true, |pfx| path.starts_with(pfx))
            })
            .map(|(_, obj)| obj.clone())
            .collect();
        // Stable order for reproducible tests.
        results.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// BlobStore
// ---------------------------------------------------------------------------

/// The primary object-storage API.
///
/// `C` is a [`BlobCatalog`] implementation (usually [`InMemoryBlobCatalog`]
/// in tests, a Postgres-backed catalog in production).
///
/// The underlying `object_store` is provided at construction time.  No root
/// prefix is set here; callers that need a prefix should wrap their store
/// with `object_store::PrefixStore` before passing it in.
pub struct BlobStore<C> {
    catalog: Arc<C>,
    object_store: Arc<dyn ObjectStore>,
    /// Optional root prefix prepended to every object key.
    root: Option<ObjectPath>,
    /// Optional per-project byte counter registry.  When present, `put_object`
    /// increments `bytes_written_total` by the object size and `delete_object`
    /// decrements it (saturating) by the previously stored size.
    counters: Option<Arc<ProjectCounterRegistry>>,
    /// RLS policy store for `storage.objects` (Phase 5.17.C).
    ///
    /// Holds per-project policies in memory.  The caller (usually `basin-rest`)
    /// registers policies via `set_rls_enabled` / `add_object_policy` after
    /// parsing `CREATE POLICY … ON storage.objects` DDL statements.
    pub rls: Arc<ObjectRlsStore>,
}

impl<C: BlobCatalog> BlobStore<C> {
    /// Create a new `BlobStore`.
    ///
    /// - `catalog` — row catalog (bucket + object metadata).
    /// - `object_store` — byte store for blob payloads.
    /// - `root` — optional path prefix inside the object store (e.g.
    ///   `"warehouse"` for a shared bucket with other tenants).
    pub fn new(
        catalog: Arc<C>,
        object_store: Arc<dyn ObjectStore>,
        root: Option<ObjectPath>,
    ) -> Self {
        Self {
            catalog,
            object_store,
            root,
            counters: None,
            rls: Arc::new(ObjectRlsStore::new()),
        }
    }

    /// Attach a [`ProjectCounterRegistry`] so that `put_object` / `delete_object`
    /// bump the per-project byte counter.  Returns `Self` for chaining.
    pub fn with_counters(mut self, registry: Arc<ProjectCounterRegistry>) -> Self {
        self.counters = Some(registry);
        self
    }

    // -----------------------------------------------------------------------
    // Bucket operations
    // -----------------------------------------------------------------------

    /// Create a new bucket in the project.
    ///
    /// Returns [`BlobError::BucketAlreadyExists`] if a bucket with the same
    /// name already exists in this project.
    pub async fn create_bucket(&self, project: &ProjectId, bucket: Bucket) -> Result<Bucket> {
        self.catalog.insert_bucket(project, bucket.clone()).await?;
        tracing::debug!(
            project = %project,
            bucket = %bucket.name,
            "bucket created"
        );
        Ok(bucket)
    }

    /// Retrieve a bucket by name.
    ///
    /// Returns [`BlobError::BucketNotFound`] if no bucket with this name
    /// exists in the project.
    pub async fn get_bucket(&self, project: &ProjectId, name: &str) -> Result<Bucket> {
        self.catalog
            .get_bucket_by_name(project, name)
            .await?
            .ok_or_else(|| BlobError::BucketNotFound(name.to_string()))
    }

    // -----------------------------------------------------------------------
    // Object operations
    // -----------------------------------------------------------------------

    /// Write `data` bytes to the object store and upsert the object row in the
    /// catalog.
    ///
    /// Enforces bucket policy:
    /// - `file_size_limit` — rejects uploads that exceed the configured limit.
    /// - `allowed_mime_types` — rejects uploads with a disallowed MIME type
    ///   (skip if `mime_type` is `None` and the list is non-empty — callers
    ///   should sniff the content type before calling).
    ///
    /// The `etag` is the hex-encoded content hash supplied by the caller.  If
    /// the object already exists at this path it is overwritten.
    ///
    /// When a [`ProjectCounterRegistry`] is attached (via [`BlobStore::with_counters`]),
    /// the project's `bytes_written_total` counter is incremented by the object size
    /// after a successful write.
    pub async fn put_object(
        &self,
        project: &ProjectId,
        bucket_name: &str,
        object_path: &str,
        data: Bytes,
        mime_type: Option<String>,
        metadata: serde_json::Value,
        owner: Option<uuid::Uuid>,
        etag: impl Into<String>,
    ) -> Result<Object> {
        let bucket = self.get_bucket(project, bucket_name).await?;

        let size = data.len() as u64;

        // Enforce file_size_limit.
        if !bucket.allows_size(size) {
            return Err(BlobError::PayloadTooLarge {
                size,
                limit: bucket.file_size_limit.unwrap_or(0),
            });
        }

        // Enforce allowed_mime_types.
        if let Some(ref mt) = mime_type {
            if !bucket.allows_mime_type(mt) {
                return Err(BlobError::MimeTypeNotAllowed(mt.clone()));
            }
        }

        // Build the object-store key and write the bytes.
        let key = paths::blob_key(self.root.as_ref(), project, bucket_name, object_path)?;
        self.object_store
            .put(&key, PutPayload::from_bytes(data.clone()))
            .await
            .map_err(BlobError::ObjectStore)?;

        // Upsert the catalog row.
        let object = Object::new(
            bucket.id,
            object_path,
            size,
            mime_type,
            metadata,
            owner,
            etag,
        );
        self.catalog.upsert_object(project, object.clone()).await?;

        // Bump the per-project byte counter (fire-and-forget; never fails).
        if let Some(ref registry) = self.counters {
            registry.for_project(project).record_bytes_written(size);
        }

        tracing::debug!(
            project = %project,
            bucket = %bucket_name,
            path = %object_path,
            size,
            "object stored"
        );
        Ok(object)
    }

    /// Read the bytes for an object and return the catalog row.
    ///
    /// Returns [`BlobError::ObjectNotFound`] if either the catalog row or the
    /// bytes are absent.
    pub async fn get_object(
        &self,
        project: &ProjectId,
        bucket_name: &str,
        object_path: &str,
    ) -> Result<(Object, Bytes)> {
        let bucket = self.get_bucket(project, bucket_name).await?;

        let row = self
            .catalog
            .get_object_by_path(project, bucket.id, object_path)
            .await?
            .ok_or_else(|| BlobError::ObjectNotFound {
                bucket: bucket_name.to_string(),
                path: object_path.to_string(),
            })?;

        let key = paths::blob_key(self.root.as_ref(), project, bucket_name, object_path)?;
        let data: Bytes = self
            .object_store
            .get(&key)
            .await
            .map_err(BlobError::ObjectStore)?
            .bytes()
            .await
            .map_err(BlobError::ObjectStore)?;

        Ok((row, data))
    }

    /// Delete an object's bytes from the object store and remove the catalog row.
    ///
    /// Returns `Ok(())` whether or not the object existed (idempotent).
    ///
    /// When a [`ProjectCounterRegistry`] is attached, the project's
    /// `bytes_written_total` counter is decremented (saturating) by the size of
    /// the deleted object.  If the object did not exist, the counter is unchanged.
    pub async fn delete_object(
        &self,
        project: &ProjectId,
        bucket_name: &str,
        object_path: &str,
    ) -> Result<()> {
        let bucket = self.get_bucket(project, bucket_name).await?;

        // Fetch the catalog row before deletion so we know the stored size for
        // the counter decrement.  Only needed when a counter registry is attached.
        let prior_size: Option<u64> = if self.counters.is_some() {
            self.catalog
                .get_object_by_path(project, bucket.id, object_path)
                .await?
                .map(|o| o.size)
        } else {
            None
        };

        let key = paths::blob_key(self.root.as_ref(), project, bucket_name, object_path)?;

        // Delete bytes — ignore NotFound from the object store (idempotent).
        match self.object_store.delete(&key).await {
            Ok(()) => {}
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(BlobError::ObjectStore(e)),
        }

        // Remove catalog row.
        self.catalog
            .delete_object_by_path(project, bucket.id, object_path)
            .await?;

        // Decrement the per-project byte counter by the size of the deleted
        // object (saturating — never goes below 0).
        if let (Some(ref registry), Some(size)) = (&self.counters, prior_size) {
            registry.for_project(project).record_bytes_deleted(size);
        }

        tracing::debug!(
            project = %project,
            bucket = %bucket_name,
            path = %object_path,
            "object deleted"
        );
        Ok(())
    }

    /// List objects in a bucket, optionally filtered by `prefix`.
    ///
    /// Returns all matching object catalog rows (not bytes).  An empty `prefix`
    /// or `None` returns every object in the bucket.
    pub async fn list_objects(
        &self,
        project: &ProjectId,
        bucket_name: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<Object>> {
        let bucket = self.get_bucket(project, bucket_name).await?;
        self.catalog
            .list_objects_by_prefix(project, bucket.id, prefix)
            .await
    }

    /// Return the underlying object-store key for an object path.
    ///
    /// Useful for constructing signed URLs (5.17.D) without going through the
    /// full put/get path.
    pub fn object_key(
        &self,
        project: &ProjectId,
        bucket_name: &str,
        object_path: &str,
    ) -> Result<ObjectPath> {
        paths::blob_key(self.root.as_ref(), project, bucket_name, object_path)
    }

    // -----------------------------------------------------------------------
    // RLS management (Phase 5.17.C)
    // -----------------------------------------------------------------------

    /// Enable or disable RLS on `storage.objects` for `project`.
    ///
    /// When enabled, subsequent list / get / delete calls that receive a
    /// [`CallerCtx`] will enforce the registered policies.
    pub fn set_objects_rls_enabled(&self, project: &ProjectId, enabled: bool) {
        self.rls.set_enabled(project, "objects", enabled);
    }

    /// Register a policy on `storage.objects` for `project`.
    ///
    /// Returns `true` on success, `false` if a policy with the same name
    /// already exists (mirrors Postgres `ERROR:  policy already exists`).
    pub fn add_object_policy(&self, project: &ProjectId, policy: ObjectPolicy) -> bool {
        self.rls.create_policy(project, "objects", policy)
    }

    /// Remove a policy from `storage.objects` by name.
    ///
    /// Returns `true` if the policy existed and was removed.
    pub fn drop_object_policy(&self, project: &ProjectId, name: &str) -> bool {
        self.rls.drop_policy(project, "objects", name)
    }

    /// Return the applicable RLS policies for `command` in `project` for a
    /// caller in `role`.
    ///
    /// Returns `(rls_enabled, applicable_policies)`.  If `rls_enabled` is
    /// `false` the caller should skip the policy check entirely.
    pub fn objects_rls_applicable(
        &self,
        project: &ProjectId,
        command: ObjectPolicyCommand,
        role: &str,
    ) -> (bool, Vec<ObjectPolicy>) {
        self.rls.applicable(project, "objects", command, role)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::ProjectCounterRegistry;
    use object_store::memory::InMemory;

    fn make_store() -> BlobStore<InMemoryBlobCatalog> {
        let catalog = InMemoryBlobCatalog::arc();
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        BlobStore::new(catalog, obj_store, None)
    }

    // -----------------------------------------------------------------------
    // Bucket round-trip
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn create_and_get_bucket() {
        let store = make_store();
        let project = ProjectId::new();

        let bucket = Bucket::new("avatars");
        let created = store.create_bucket(&project, bucket).await.unwrap();
        assert_eq!(created.name, "avatars");

        let fetched = store.get_bucket(&project, "avatars").await.unwrap();
        assert_eq!(fetched.id, created.id);
        assert!(!fetched.public);
    }

    #[tokio::test]
    async fn create_bucket_duplicate_name_errors() {
        let store = make_store();
        let project = ProjectId::new();

        store
            .create_bucket(&project, Bucket::new("docs"))
            .await
            .unwrap();

        let err = store
            .create_bucket(&project, Bucket::new("docs"))
            .await
            .unwrap_err();
        assert!(matches!(err, BlobError::BucketAlreadyExists(_)));
    }

    #[tokio::test]
    async fn get_bucket_not_found() {
        let store = make_store();
        let project = ProjectId::new();
        let err = store
            .get_bucket(&project, "nonexistent")
            .await
            .unwrap_err();
        assert!(matches!(err, BlobError::BucketNotFound(_)));
    }

    #[tokio::test]
    async fn buckets_are_project_scoped() {
        let store = make_store();
        let p1 = ProjectId::new();
        let p2 = ProjectId::new();

        store
            .create_bucket(&p1, Bucket::new("shared-name"))
            .await
            .unwrap();

        // Same name in a different project must succeed.
        store
            .create_bucket(&p2, Bucket::new("shared-name"))
            .await
            .unwrap();

        // p2 cannot see p1's bucket ID.
        let b1 = store.get_bucket(&p1, "shared-name").await.unwrap();
        let b2 = store.get_bucket(&p2, "shared-name").await.unwrap();
        assert_ne!(b1.id, b2.id);
    }

    // -----------------------------------------------------------------------
    // Object round-trip
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn put_and_get_object_round_trip() {
        let store = make_store();
        let project = ProjectId::new();

        store
            .create_bucket(&project, Bucket::new("blobs"))
            .await
            .unwrap();

        let payload = Bytes::from_static(b"hello, basin-blob!");
        let obj = store
            .put_object(
                &project,
                "blobs",
                "greet/hello.txt",
                payload.clone(),
                Some("text/plain".to_string()),
                serde_json::Value::Null,
                None,
                "abc123",
            )
            .await
            .unwrap();

        assert_eq!(obj.size, payload.len() as u64);
        assert_eq!(obj.etag, "abc123");
        assert_eq!(obj.path, "greet/hello.txt");

        let (fetched_obj, fetched_bytes) = store
            .get_object(&project, "blobs", "greet/hello.txt")
            .await
            .unwrap();

        assert_eq!(fetched_obj.id, obj.id);
        assert_eq!(fetched_bytes, payload);
    }

    #[tokio::test]
    async fn get_object_not_found() {
        let store = make_store();
        let project = ProjectId::new();

        store
            .create_bucket(&project, Bucket::new("b"))
            .await
            .unwrap();

        let err = store
            .get_object(&project, "b", "missing.bin")
            .await
            .unwrap_err();
        assert!(matches!(err, BlobError::ObjectNotFound { .. }));
    }

    #[tokio::test]
    async fn delete_object_removes_bytes_and_row() {
        let store = make_store();
        let project = ProjectId::new();

        store
            .create_bucket(&project, Bucket::new("tmp"))
            .await
            .unwrap();

        let payload = Bytes::from_static(b"temporary");
        store
            .put_object(
                &project,
                "tmp",
                "file.bin",
                payload,
                None,
                serde_json::Value::Null,
                None,
                "",
            )
            .await
            .unwrap();

        store
            .delete_object(&project, "tmp", "file.bin")
            .await
            .unwrap();

        // Row should be gone.
        let err = store
            .get_object(&project, "tmp", "file.bin")
            .await
            .unwrap_err();
        assert!(matches!(err, BlobError::ObjectNotFound { .. }));
    }

    #[tokio::test]
    async fn delete_object_is_idempotent() {
        let store = make_store();
        let project = ProjectId::new();

        store
            .create_bucket(&project, Bucket::new("b"))
            .await
            .unwrap();

        // Deleting a non-existent object should not error.
        store
            .delete_object(&project, "b", "ghost.bin")
            .await
            .unwrap();
    }

    // -----------------------------------------------------------------------
    // Policy enforcement
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn put_object_rejects_oversized_payload() {
        let store = make_store();
        let project = ProjectId::new();

        let mut bucket = Bucket::new("capped");
        bucket.file_size_limit = Some(10);
        store.create_bucket(&project, bucket).await.unwrap();

        let big = Bytes::from(vec![0u8; 11]);
        let err = store
            .put_object(
                &project,
                "capped",
                "large.bin",
                big,
                None,
                serde_json::Value::Null,
                None,
                "",
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BlobError::PayloadTooLarge { .. }));
    }

    #[tokio::test]
    async fn put_object_rejects_disallowed_mime_type() {
        let store = make_store();
        let project = ProjectId::new();

        let mut bucket = Bucket::new("images");
        bucket.allowed_mime_types = vec!["image/png".to_string()];
        store.create_bucket(&project, bucket).await.unwrap();

        let err = store
            .put_object(
                &project,
                "images",
                "doc.pdf",
                Bytes::from_static(b"%PDF"),
                Some("application/pdf".to_string()),
                serde_json::Value::Null,
                None,
                "",
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BlobError::MimeTypeNotAllowed(_)));
    }

    #[tokio::test]
    async fn put_object_rejects_path_traversal() {
        let store = make_store();
        let project = ProjectId::new();

        store
            .create_bucket(&project, Bucket::new("b"))
            .await
            .unwrap();

        let err = store
            .put_object(
                &project,
                "b",
                "../secret",
                Bytes::from_static(b"data"),
                None,
                serde_json::Value::Null,
                None,
                "",
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BlobError::InvalidPath(_)));
    }

    // -----------------------------------------------------------------------
    // Object-key helper
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn object_key_matches_blob_key_helper() {
        let store = make_store();
        let project = ProjectId::new();
        let key = store.object_key(&project, "avatars", "a.png").unwrap();
        assert!(key.as_ref().contains("storage/avatars/a.png"));
    }

    // -----------------------------------------------------------------------
    // Per-project byte counter (Phase 5.17.E)
    // -----------------------------------------------------------------------

    fn make_store_with_counters(
        registry: Arc<ProjectCounterRegistry>,
    ) -> BlobStore<InMemoryBlobCatalog> {
        let catalog = InMemoryBlobCatalog::arc();
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        BlobStore::new(catalog, obj_store, None).with_counters(registry)
    }

    #[tokio::test]
    async fn put_object_increments_byte_counter() {
        let registry = Arc::new(ProjectCounterRegistry::new());
        let store = make_store_with_counters(registry.clone());
        let project = ProjectId::new();

        store
            .create_bucket(&project, Bucket::new("b"))
            .await
            .unwrap();

        let payload = Bytes::from(vec![0u8; 512]);
        store
            .put_object(
                &project,
                "b",
                "file.bin",
                payload,
                None,
                serde_json::Value::Null,
                None,
                "etag",
            )
            .await
            .unwrap();

        let snap = registry.snapshot(&project).unwrap();
        assert_eq!(snap.bytes_written_total, 512);
    }

    #[tokio::test]
    async fn delete_object_decrements_byte_counter() {
        let registry = Arc::new(ProjectCounterRegistry::new());
        let store = make_store_with_counters(registry.clone());
        let project = ProjectId::new();

        store
            .create_bucket(&project, Bucket::new("b"))
            .await
            .unwrap();

        let payload = Bytes::from(vec![1u8; 256]);
        store
            .put_object(
                &project,
                "b",
                "file.bin",
                payload,
                None,
                serde_json::Value::Null,
                None,
                "etag",
            )
            .await
            .unwrap();

        assert_eq!(registry.snapshot(&project).unwrap().bytes_written_total, 256);

        store
            .delete_object(&project, "b", "file.bin")
            .await
            .unwrap();

        // Counter decremented back to 0 after delete.
        assert_eq!(registry.snapshot(&project).unwrap().bytes_written_total, 0);
    }

    #[tokio::test]
    async fn counter_is_project_isolated() {
        let registry = Arc::new(ProjectCounterRegistry::new());
        let store = make_store_with_counters(registry.clone());

        let p1 = ProjectId::new();
        let p2 = ProjectId::new();

        store
            .create_bucket(&p1, Bucket::new("b"))
            .await
            .unwrap();
        store
            .create_bucket(&p2, Bucket::new("b"))
            .await
            .unwrap();

        store
            .put_object(
                &p1,
                "b",
                "a.bin",
                Bytes::from(vec![0u8; 100]),
                None,
                serde_json::Value::Null,
                None,
                "e1",
            )
            .await
            .unwrap();

        store
            .put_object(
                &p2,
                "b",
                "a.bin",
                Bytes::from(vec![0u8; 200]),
                None,
                serde_json::Value::Null,
                None,
                "e2",
            )
            .await
            .unwrap();

        // Each project sees only its own bytes.
        assert_eq!(registry.snapshot(&p1).unwrap().bytes_written_total, 100);
        assert_eq!(registry.snapshot(&p2).unwrap().bytes_written_total, 200);

        // Deleting p1's object does not affect p2.
        store.delete_object(&p1, "b", "a.bin").await.unwrap();
        assert_eq!(registry.snapshot(&p1).unwrap().bytes_written_total, 0);
        assert_eq!(registry.snapshot(&p2).unwrap().bytes_written_total, 200);
    }

    #[tokio::test]
    async fn delete_nonexistent_object_does_not_change_counter() {
        let registry = Arc::new(ProjectCounterRegistry::new());
        let store = make_store_with_counters(registry.clone());
        let project = ProjectId::new();

        store
            .create_bucket(&project, Bucket::new("b"))
            .await
            .unwrap();

        // Delete something that was never stored — counter should remain absent.
        store
            .delete_object(&project, "b", "ghost.bin")
            .await
            .unwrap();

        // No activity recorded — snapshot is None.
        assert!(registry.snapshot(&project).is_none());
    }
}
