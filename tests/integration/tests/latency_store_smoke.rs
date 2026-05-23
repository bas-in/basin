//! Smoke test: `basin_storage::LatencyStore` actually injects the
//! configured per-op delay.
//!
//! Wraps an in-memory `ObjectStore` in `LatencyStore` with a 50 ms delay
//! and confirms that a PUT followed by a GET takes ≥ 100 ms (one delay
//! per op) but < 400 ms (no runaway / no per-byte multiplier). This is
//! the smallest exec-time check that catches the regression where the
//! decorator silently drops the sleep — for example, if a future trait
//! refactor adds a new required method and a contributor forgets to
//! wrap it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

#[tokio::test]
async fn latency_store_adds_per_op_delay() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let wrapped = basin_storage::LatencyStore::new(inner, Duration::from_millis(50));

    let path = Path::from("smoke/key");
    let t0 = Instant::now();
    wrapped
        .put(&path, PutPayload::from(Bytes::from_static(b"bar")))
        .await
        .expect("put");
    wrapped
        .get(&path)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    let elapsed = t0.elapsed();

    assert!(
        elapsed >= Duration::from_millis(100),
        "expected >=100ms (50ms x 2 ops), got {elapsed:?}",
    );
    // Loose upper bound — InMemory is microsecond-fast and the only
    // wall-clock contribution is the two 50 ms sleeps. 400 ms is wide
    // enough to absorb CI scheduler jitter without hiding a per-byte
    // bug.
    assert!(
        elapsed < Duration::from_millis(400),
        "expected <400ms (no extra overhead), got {elapsed:?}",
    );
}
