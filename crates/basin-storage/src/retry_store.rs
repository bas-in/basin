//! Retry transient object-store READ failures instead of failing the query.
//!
//! `get_bytes_with_retry` (reader.rs) already retries the GETs that Basin's own
//! read path issues. But the Vortex/DataFusion scan opens files through the
//! `ObjectStore` handle directly (`ObjectStoreReadAt`), so it never sees that
//! retry — and a single transient body error anywhere in a large scan kills the
//! whole query:
//!
//! ```text
//! ERROR: Failed to read Vortex file: .../01KXD2X8MJ....vortex:
//!        Generic S3 error: HTTP error: request or response body error
//! ```
//!
//! Tigris hands out those mid-stream body errors routinely under load, and a
//! 1B-row scan touches thousands of files, so "fails a scan every so often" is
//! really "cannot reliably scan a large table at all". This decorator sits at
//! the bottom of the store stack so EVERY reader — the scan, the merge, the
//! footer probe, the disk cache's own fills — inherits the retry.
//!
//! Reads are materialised into a buffer before being handed back, because a
//! body error surfaces halfway through consuming the stream: you cannot retry
//! what you have already yielded. The read path buffers anyway (`read_one`
//! collects to `Bytes`), and the ranged reads a Vortex scan issues are small,
//! so this costs nothing it was not already paying.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
};

/// Max attempts for a transient read failure. Small: the S3 client already does
/// its own HTTP-level retries underneath, so this only catches what it gave up
/// on (a body that died after the response headers were accepted).
const MAX_READ_ATTEMPTS: u32 = 4;

/// Is this a transient read failure worth another GET?
///
/// `Generic` is what a dead/truncated body surfaces as once the S3 client has
/// exhausted its own HTTP retry budget. `NotFound` is deliberately NOT retried
/// here: the read path has its own bounded NotFound retry with a different
/// rationale (a just-committed file racing the object store's listing), and
/// silently retrying it here would double that budget.
fn is_transient(e: &object_store::Error) -> bool {
    matches!(e, object_store::Error::Generic { .. })
}

async fn backoff(attempt: u32) {
    // 50ms, 100ms, 200ms — bounded, and short: a scan holds its slot while it waits.
    let ms = 50u64 << attempt.min(2);
    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
}

/// Wraps an [`ObjectStore`], retrying transient failures on the read path.
#[derive(Debug)]
pub(crate) struct RetryingStore {
    inner: Arc<dyn ObjectStore>,
}

impl RetryingStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl std::fmt::Display for RetryingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RetryingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for RetryingStore {
    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let mut attempt: u32 = 0;
        loop {
            // Retry the GET *and* the body materialisation. The failure we are
            // actually chasing happens in the second one: the response headers
            // are fine, then the body dies mid-stream.
            let res = match self.inner.get_opts(location, options.clone()).await {
                Ok(r) => r,
                Err(e) if attempt < MAX_READ_ATTEMPTS && is_transient(&e) => {
                    backoff(attempt).await;
                    attempt += 1;
                    continue;
                }
                Err(e) => return Err(e),
            };

            let meta = res.meta.clone();
            let range = res.range.clone();
            let attributes = res.attributes.clone();
            match res.bytes().await {
                Ok(payload) => {
                    // The payload is materialised, so hand it back as a
                    // single-chunk stream: `GetResultPayload` has no buffer
                    // variant, and re-streaming what we already hold cannot
                    // fail again.
                    let stream = futures::stream::once(async move { Ok(payload) }).boxed();
                    return Ok(GetResult {
                        payload: GetResultPayload::Stream(stream),
                        meta,
                        range,
                        attributes,
                    });
                }
                Err(e) if attempt < MAX_READ_ATTEMPTS && is_transient(&e) => {
                    tracing::debug!(
                        target: "basin_storage",
                        path = %location,
                        attempt,
                        error = %e,
                        "object-store read body failed; re-issuing the GET",
                    );
                    backoff(attempt).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

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
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// An inner store whose response BODY dies the first `fail_times` reads —
    /// exactly the Tigris failure that was killing whole-table scans.
    #[derive(Debug)]
    struct FlakyBody {
        inner: Arc<dyn ObjectStore>,
        fail_times: u32,
        seen: AtomicU32,
    }

    impl std::fmt::Display for FlakyBody {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FlakyBody")
        }
    }

    #[async_trait]
    impl ObjectStore for FlakyBody {
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let res = self.inner.get_opts(location, options).await?;
            let n = self.seen.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_times {
                // Headers fine, body dies mid-stream.
                let meta = res.meta.clone();
                let range = res.range.clone();
                let attributes = res.attributes.clone();
                let stream = futures::stream::once(async {
                    Err(object_store::Error::Generic {
                        store: "test",
                        source: "request or response body error".into(),
                    })
                })
                .boxed();
                return Ok(GetResult {
                    payload: GetResultPayload::Stream(stream),
                    meta,
                    range,
                    attributes,
                });
            }
            Ok(res)
        }

        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
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
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    async fn seed(fail_times: u32) -> (Arc<dyn ObjectStore>, ObjectPath) {
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = ObjectPath::from("t/data.vortex");
        mem.put_opts(
            &path,
            PutPayload::from_static(b"the-rows"),
            PutOptions::default(),
        )
        .await
        .unwrap();
        let flaky: Arc<dyn ObjectStore> = Arc::new(FlakyBody {
            inner: mem,
            fail_times,
            seen: AtomicU32::new(0),
        });
        (flaky, path)
    }

    /// A body error mid-scan must be retried, not surfaced. Without the retry
    /// this is the "Failed to read Vortex file: ... request or response body
    /// error" that killed an entire 1B-row table scan.
    #[tokio::test]
    async fn transient_body_error_is_retried_not_surfaced() {
        let (flaky, path) = seed(2).await;
        let store = RetryingStore::new(flaky);
        let got = store
            .get_opts(&path, GetOptions::default())
            .await
            .expect("must retry past the body error");
        let bytes = got.bytes().await.unwrap();
        assert_eq!(&bytes[..], b"the-rows");
    }

    /// The retry is BOUNDED — a permanently broken body still fails, loudly,
    /// rather than spinning forever.
    #[tokio::test]
    async fn a_permanently_broken_body_still_fails() {
        let (flaky, path) = seed(u32::MAX).await;
        let store = RetryingStore::new(flaky);
        let res = match store.get_opts(&path, GetOptions::default()).await {
            Ok(r) => r.bytes().await.map(|_| ()),
            Err(e) => Err(e),
        };
        assert!(res.is_err(), "a body that never recovers must surface an error");
    }
}
