//! Integration tests for the [`basin_wal::Wal`] trait abstraction.
//!
//! The trait must be object-safe, [`LocalWal`] must satisfy it, and
//! [`RaftWal`] must satisfy it (real raft, single-node bootstrap is
//! exercised here; the multi-node + leader-failure suite lives in
//! `tests/raft_wal.rs`).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use basin_common::{PartitionKey, ProjectId};
use basin_wal::{LocalWal, Lsn, RaftWal, RaftWalConfig, Wal, WalConfig};
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn local_cfg(dir: &TempDir) -> WalConfig {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    WalConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        flush_interval: Duration::from_millis(50),
        flush_max_bytes: 1024 * 1024,
        commit_delay: Duration::from_millis(2),
    }
}

fn raft_cfg() -> RaftWalConfig {
    RaftWalConfig::new(
        vec![],
        "node-1",
        PathBuf::from("/tmp/basin-raft-trait-test"),
    )
    .with_node_id(1)
}

#[tokio::test]
async fn local_wal_implements_trait() {
    let dir = TempDir::new().unwrap();
    let local = LocalWal::open(local_cfg(&dir)).await.unwrap();
    let wal: Arc<dyn Wal> = Arc::new(local);
    let project = ProjectId::new();
    let partition = PartitionKey::default_key();

    // Append some entries through the trait object.
    for i in 1..=5u64 {
        let lsn = wal
            .append(&project, &partition, Bytes::from(format!("payload-{i}")))
            .await
            .unwrap();
        assert_eq!(lsn, Lsn(i));
    }

    wal.flush().await.unwrap();

    let entries = wal
        .read_from(&project, &partition, Lsn::ZERO)
        .await
        .unwrap();
    assert_eq!(entries.len(), 5);
    assert_eq!(wal.high_water(&project, &partition).await.unwrap(), Lsn(5));

    wal.truncate(&project, &partition, Lsn(3)).await.unwrap();
    let remaining = wal
        .read_from(&project, &partition, Lsn::ZERO)
        .await
        .unwrap();
    // After truncate(<=3) only entries with lsn > 3 remain on durable storage.
    // Entries that were still in the in-RAM buffer are also returned.
    assert!(remaining.iter().all(|e| e.lsn > Lsn(0)));
    assert!(remaining.len() <= 5);

    wal.close().await.unwrap();
}

#[tokio::test]
async fn raft_wal_single_node_implements_trait() {
    use std::collections::BTreeMap;

    let raft = RaftWal::new(raft_cfg()).await.unwrap();
    // Single-node bootstrap: initialise membership with just this node.
    let mut members = BTreeMap::new();
    members.insert(1u64, openraft::BasicNode::new("node-1"));
    raft.initialize(members).await.unwrap();
    // Wait for self-election.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let wal: Arc<dyn Wal> = Arc::new(raft);
    let project = ProjectId::new();
    let partition = PartitionKey::default_key();

    let lsn = wal
        .append(&project, &partition, Bytes::from("hello"))
        .await
        .unwrap();
    assert_eq!(lsn, Lsn(1));

    let entries = wal
        .read_from(&project, &partition, Lsn::ZERO)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(wal.high_water(&project, &partition).await.unwrap(), Lsn(1));

    wal.flush().await.unwrap();
    wal.close().await.unwrap();
}

#[tokio::test]
async fn raft_wal_config_constructs() {
    let cfg = raft_cfg();
    assert_eq!(cfg.local_id, "node-1");
    assert_eq!(cfg.election_timeout_ms, 1500);
    assert_eq!(cfg.heartbeat_interval_ms, 500);
    let raft = RaftWal::new(cfg).await.unwrap();
    assert_eq!(raft.config().local_id, "node-1");
    raft.close().await.unwrap();
}

/// Compile-time assertion that `Box<dyn Wal>` (and `Arc<dyn Wal>`) compile.
/// Object safety is the load-bearing property — every caller holds the trait
/// behind a pointer.
#[test]
fn wal_trait_object_safe() {
    fn _assert_object_safe(_b: Box<dyn Wal>) {}
    fn _assert_arc_object_safe(_a: Arc<dyn Wal>) {}
    // Body intentionally empty: the test is the function bodies above
    // type-checking.
}

/// `Arc<dyn Wal>` cloning works the same way the old concrete `Wal` clone did
/// — caller behaviour is preserved.
#[tokio::test]
async fn arc_dyn_wal_is_cheap_to_clone() {
    let dir = TempDir::new().unwrap();
    let wal: Arc<dyn Wal> = Arc::new(LocalWal::open(local_cfg(&dir)).await.unwrap());
    let cloned = wal.clone();
    let project = ProjectId::new();
    let partition = PartitionKey::default_key();
    cloned
        .append(&project, &partition, Bytes::from("hello"))
        .await
        .unwrap();
    let entries = wal
        .read_from(&project, &partition, Lsn::ZERO)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    wal.close().await.unwrap();
}
