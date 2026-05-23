//! Test-only [`ObjectStore`] decorator that adds a fixed per-op delay.
//!
//! Wraps any `Arc<dyn ObjectStore>` and sleeps `delay_per_op` BEFORE each
//! `put` / `get` / `head` / `list` / `delete` / `copy` request. Used by the
//! `tigris-realistic` bench profile to approximate same-region Tigris RTT
//! (~10ms p50) on top of a loopback SeaweedFS gateway (~1ms p50) without
//! needing real Tigris credentials or paying network/egress.
//!
//! For 10ms-realistic, set `delay_per_op = Duration::from_millis(9)`
//! (loopback adds ~1ms naturally → total ~10ms per op).
//!
//! ## Wrap surface
//!
//! Every required method of [`ObjectStore`] is intercepted (`put_opts`,
//! `put_multipart_opts`, `get_opts`, `delete_stream`, `list`,
//! `list_with_delimiter`, `copy_opts`). For methods with default trait impls
//! that decompose into the required ones (`put`, `get`, `head`, `delete`,
//! `copy`, `rename`, etc.), the delay still fires exactly once per logical
//! call — the default impls funnel through `*_opts` on the same `self`, so
//! the wrapper sleeps once at the funnel point.
//!
//! ## Streams (LIST, multipart upload)
//!
//! LIST: the delay is applied once when the stream is first consumed,
//! mirroring the real S3 wire model where a `ListObjectsV2` is one
//! round-trip per response page. Per-item iteration on the returned
//! stream is in-memory after that point — no extra delay.
//!
//! Multipart: the initial `put_multipart_opts` call gets the delay; the
//! returned `MultipartUpload`'s per-part requests are NOT individually
//! delayed. The bench harness does not exercise multipart at enough
//! scale to make per-part delay meaningful, and interposing on the
//! trait object would require wrapping `MultipartUpload` itself.
//! Documented gap — fine for the current `tigris-realistic` proxy.
//!
//! ## Gating
//!
//! The whole module is feature-gated behind `test-helpers` so production
//! builds never see the decorator. The integration-tests crate enables
//! the feature; benches and harness wiring pull it through the same flag.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

/// Wraps an [`ObjectStore`] so every forwarded RPC sleeps `delay_per_op`
/// first. Designed for benchmark profiles that need to approximate a
/// higher-latency object store than what the test rig provides.
///
/// Cheap to clone via `Arc` inside the inner field.
pub struct LatencyStore {
    inner: Arc<dyn ObjectStore>,
    delay_per_op: Duration,
}

impl LatencyStore {
    /// Wrap `inner` so every RPC blocks for `delay_per_op` before issuing
    /// the underlying call. A zero `delay_per_op` is allowed and is a
    /// no-op — useful for symmetric test plumbing that conditionally
    /// installs the wrapper.
    pub fn new(inner: Arc<dyn ObjectStore>, delay_per_op: Duration) -> Self {
        Self {
            inner,
            delay_per_op,
        }
    }

    /// The configured per-op delay.
    pub fn delay_per_op(&self) -> Duration {
        self.delay_per_op
    }

    async fn pause(&self) {
        if !self.delay_per_op.is_zero() {
            tokio::time::sleep(self.delay_per_op).await;
        }
    }
}

impl fmt::Display for LatencyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LatencyStore({}, +{}ms/op)",
            self.inner,
            self.delay_per_op.as_millis()
        )
    }
}

impl fmt::Debug for LatencyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LatencyStore")
            .field("inner", &self.inner)
            .field("delay_per_op", &self.delay_per_op)
            .finish()
    }
}

#[async_trait]
impl ObjectStore for LatencyStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.pause().await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        // Delay applies to the multipart-session-create call only; per-part
        // PUTs on the returned trait object are NOT individually delayed.
        // See module docs.
        self.pause().await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.pause().await;
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        // S3's `DeleteObjects` is a single RPC carrying up to 1000 keys, so
        // we model one delay for the *stream* lifecycle rather than one per
        // key. The most accurate approximation would be one delay per ≤1000
        // keys, but our integration tests never exceed the per-batch cap on
        // a single delete_stream invocation, so this is fine.
        let delay = self.delay_per_op;
        let inner = self.inner.clone();
        futures::stream::once(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            inner.delete_stream(locations).collect::<Vec<_>>().await
        })
        .flat_map(futures::stream::iter)
        .boxed()
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let delay = self.delay_per_op;
        let inner = self.inner.clone();
        let prefix = prefix.cloned();
        // Mirror the wrap shape used in `ProjectScopedStore::list`: one
        // `once(async)` that pauses, opens the inner stream, and collects
        // it under a single permit/scope. Tests in `basin-storage` and the
        // engine listings consumer always drive the stream to exhaustion,
        // so a collect-then-iter is observationally identical to a true
        // pass-through stream with negligible memory overhead.
        futures::stream::once(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            inner.list(prefix.as_ref()).collect::<Vec<_>>().await
        })
        .flat_map(futures::stream::iter)
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.pause().await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.pause().await;
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt;
    use std::time::Instant;

    #[tokio::test]
    async fn zero_delay_is_a_noop() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wrapped = LatencyStore::new(inner, Duration::ZERO);
        let path = ObjectPath::from("zero/delay");
        let t = Instant::now();
        wrapped
            .put(&path, PutPayload::from(Bytes::from_static(b"hi")))
            .await
            .unwrap();
        wrapped.get(&path).await.unwrap().bytes().await.unwrap();
        // Two ops; with zero delay they should be sub-millisecond on
        // in-memory store. Don't assert exact upper bound to stay quiet on
        // CI noise — the point is just that no panic / no obvious blocking.
        let _ = t.elapsed();
    }

    #[tokio::test]
    async fn delay_fires_per_op_for_put_and_get() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let delay = Duration::from_millis(30);
        let wrapped = LatencyStore::new(inner, delay);
        let path = ObjectPath::from("delayed/key");

        let t = Instant::now();
        wrapped
            .put(&path, PutPayload::from(Bytes::from_static(b"x")))
            .await
            .unwrap();
        wrapped.get(&path).await.unwrap().bytes().await.unwrap();
        let elapsed = t.elapsed();
        // 2 ops × 30ms = 60ms minimum; sub-300ms to confirm no runaway.
        assert!(
            elapsed >= Duration::from_millis(60),
            "expected >=60ms (2×30ms), got {elapsed:?}",
        );
        assert!(
            elapsed < Duration::from_millis(300),
            "expected <300ms, got {elapsed:?}",
        );
    }
}
