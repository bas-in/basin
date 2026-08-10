//! Data-store fsync wiring (durability bug 3).
//!
//! `LocalFileSystem`'s PUT is write-temp + rename with NO fsync, so a power
//! loss can vanish a freshly-flushed *data* file that the fsync-durable catalog
//! already references — after the covering WAL was truncated. The fix wraps the
//! local DATA store in [`basin_wal::FsyncOnPutAll`], which fsyncs every PUT and
//! every multipart completion (unlike the WAL-only [`basin_wal::FsyncOnPut`],
//! which only fsyncs PUTs carrying the WAL flusher's `DurablePut` marker).
//!
//! An actual power-loss is untestable in-process; these tests assert the
//! construction/wiring contract: the wrapper is an `ObjectStore`, fsyncs
//! unconditionally on the data-path PUT shape, and round-trips content. The
//! server's `build_storage_object_store` selects this wrapper for the `local`
//! backend when `BASIN_DATA_FSYNC` is not opted out (default ON).

use std::sync::Arc;

use basin_wal::FsyncOnPutAll;
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use tempfile::TempDir;

fn data_store(dir: &TempDir) -> FsyncOnPutAll {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    FsyncOnPutAll::new(Arc::new(fs))
}

/// The data-store wrapper fsyncs EVERY plain PUT — the exact shape the storage
/// writer issues (`store.put(key, payload)` for Parquet/Vortex files) — with no
/// `DurablePut` marker. This is the property `FsyncOnPut` (the WAL wrapper)
/// lacks: wrapping the data store in `FsyncOnPut` would fsync nothing.
#[tokio::test]
async fn data_put_is_fsynced_without_marker() {
    let dir = TempDir::new().unwrap();
    let store = data_store(&dir);
    let path = ObjectPath::from("projects/p/data/orders/00000.parquet");

    // The storage writer's exact call shape: a plain `put` with a payload.
    store
        .put(
            &path,
            PutPayload::from_bytes(Bytes::from_static(b"coldfile")),
        )
        .await
        .unwrap();

    assert_eq!(
        store.fsync_count(),
        1,
        "a data-path PUT must be fsynced (file + parent dir) with no marker",
    );

    // Content survives a reopen — the rename + fsync made it durable.
    let reopened = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let body = reopened.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(&body[..], b"coldfile");
}

/// The wrapper is usable behind `Arc<dyn ObjectStore>` exactly as
/// `build_storage_object_store` returns it, and still fsyncs through the trait
/// object. This pins the wiring contract the server relies on.
#[tokio::test]
async fn wrapper_is_object_store_trait_object() {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let wrapped = Arc::new(FsyncOnPutAll::new(Arc::new(fs)));
    let dyn_store: Arc<dyn ObjectStore> = wrapped.clone();

    let path = ObjectPath::from("projects/p/data/t/sidecar.bin");
    dyn_store
        .put(&path, PutPayload::from_bytes(Bytes::from_static(b"x")))
        .await
        .unwrap();

    assert_eq!(
        wrapped.fsync_count(),
        1,
        "PUTs routed through the dyn ObjectStore must still fsync",
    );
}
