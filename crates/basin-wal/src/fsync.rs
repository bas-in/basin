//! `FsyncOnPut` — an [`ObjectStore`] delegate that makes selected PUTs
//! power-loss durable on the local filesystem backend.
//!
//! ## Why this exists
//!
//! [`object_store::local::LocalFileSystem`]'s `put_opts` stages the payload
//! into a temp file and renames it into place — it never calls `fsync`. A
//! "successful PUT" on the local backend therefore survives a process crash
//! (the rename is visible to a re-opened process) but **not** an OS crash or
//! power loss (the page cache may still hold both the data and the rename).
//! S3-style backends don't have this gap: a 200 on PUT is the durability
//! contract.
//!
//! ## How it works
//!
//! The WAL flusher marks a PUT that a synchronous-commit waiter depends on
//! by inserting the zero-sized [`DurablePut`] marker into
//! [`PutOptions::extensions`]. Extensions are ignored by every backend the
//! `object_store` crate ships, so unmarked stores pass them through
//! harmlessly. When the WAL's store is wrapped in [`FsyncOnPut`] (see
//! `basin-server`'s `build_wal_object_store`), a marked PUT is followed by:
//!
//! 1. `sync_data` on the renamed destination file, and
//! 2. `sync_all` on its parent directory (so the rename itself is durable).
//!
//! Unmarked PUTs delegate straight through — the async WAL path keeps its
//! exact pre-existing cost profile (no fsync, no extra syscalls).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutOptions, PutPayload, PutResult};

/// Zero-sized [`PutOptions::extensions`] marker: "a synchronous-commit
/// waiter depends on this PUT — make it power-loss durable if the backend
/// needs help". Inserted by the WAL flusher for segments that carry at least
/// one durable-append waiter; read by [`FsyncOnPut`]. Backends that are
/// already durable on PUT (S3/Tigris) run unwrapped and simply never see it.
#[derive(Clone, Copy, Debug)]
pub struct DurablePut;

/// [`ObjectStore`] wrapper for the **local filesystem** backend that fsyncs
/// the destination file and its parent directory after every PUT marked with
/// [`DurablePut`]. All other operations (and unmarked PUTs) delegate
/// untouched.
#[derive(Debug)]
pub struct FsyncOnPut {
    inner: Arc<LocalFileSystem>,
    /// Count of fsync-completed PUTs, for tests and telemetry. One marked
    /// PUT == one increment (file `sync_data` + parent-dir `sync_all`).
    fsyncs: AtomicU64,
}

impl FsyncOnPut {
    pub fn new(inner: Arc<LocalFileSystem>) -> Self {
        Self {
            inner,
            fsyncs: AtomicU64::new(0),
        }
    }

    /// Number of marked PUTs that have completed their fsync pass.
    pub fn fsync_count(&self) -> u64 {
        self.fsyncs.load(Ordering::SeqCst)
    }

    /// fsync the renamed destination file and its parent directory. Runs on
    /// the blocking pool — `sync_data`/`sync_all` are real disk barriers and
    /// must not stall the async runtime.
    async fn fsync_location(&self, location: &ObjectPath) -> object_store::Result<()> {
        let path = self.inner.path_to_filesystem(location)?;
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            std::fs::File::open(&path)?.sync_data()?;
            if let Some(parent) = path.parent() {
                // Directory fsync makes the rename itself durable. Opening a
                // directory read-only for fsync is the standard unix idiom.
                std::fs::File::open(parent)?.sync_all()?;
            }
            Ok(())
        })
        .await
        .map_err(|e| object_store::Error::Generic {
            store: "FsyncOnPut",
            source: Box::new(e),
        })?
        .map_err(|e| object_store::Error::Generic {
            store: "FsyncOnPut",
            source: Box::new(e),
        })
    }
}

impl std::fmt::Display for FsyncOnPut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FsyncOnPut({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FsyncOnPut {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let durable = opts.extensions.get::<DurablePut>().is_some();
        let result = self.inner.put_opts(location, payload, opts).await?;
        if durable {
            self.fsync_location(location).await?;
            self.fsyncs.fetch_add(1, Ordering::SeqCst);
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: object_store::PutMultipartOpts,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use object_store::ObjectStoreExt;
    use tempfile::TempDir;

    fn wrapper_in(dir: &TempDir) -> FsyncOnPut {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        FsyncOnPut::new(Arc::new(fs))
    }

    /// Unmarked PUTs (the async WAL path) must delegate straight through:
    /// no fsync pass, counter stays zero, content readable.
    #[tokio::test]
    async fn unmarked_put_skips_fsync() {
        let dir = TempDir::new().unwrap();
        let store = wrapper_in(&dir);
        let path = ObjectPath::from("wal/a/b/plain.seg");

        store
            .put(&path, PutPayload::from_bytes(Bytes::from_static(b"plain")))
            .await
            .unwrap();
        assert_eq!(store.fsync_count(), 0, "unmarked PUT must never fsync");

        let body = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&body[..], b"plain");
    }

    /// A PUT carrying the `DurablePut` extension marker must run the fsync
    /// pass exactly once (file + parent dir) and leave the content intact.
    /// A unit test cannot observe the kernel barrier itself; it asserts the
    /// code path executed via the wrapper's counter.
    #[tokio::test]
    async fn marked_put_fsyncs_once() {
        let dir = TempDir::new().unwrap();
        let store = wrapper_in(&dir);
        let path = ObjectPath::from("wal/a/b/durable.seg");

        let mut opts = PutOptions::default();
        opts.extensions.insert(DurablePut);
        store
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"durable")),
                opts,
            )
            .await
            .unwrap();
        assert_eq!(store.fsync_count(), 1, "marked PUT must fsync exactly once");

        let body = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&body[..], b"durable");

        // A second unmarked PUT leaves the counter alone.
        store
            .put(
                &ObjectPath::from("wal/a/b/plain2.seg"),
                PutPayload::from_bytes(Bytes::from_static(b"x")),
            )
            .await
            .unwrap();
        assert_eq!(store.fsync_count(), 1);
    }
}
