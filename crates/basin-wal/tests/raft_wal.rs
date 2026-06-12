//! Multi-node integration tests for [`basin_wal::RaftWal`].
//!
//! Every test brings up a cluster inside one tokio runtime, sharing one
//! [`SimCluster`] across the nodes so the in-process `RaftNetwork` can
//! dispatch RPCs by direct method call. Cross-process gRPC `RaftNetwork`
//! is a follow-up (see `crates/basin-wal/RAFT.md`); these tests pin
//! consensus correctness without locking in a wire protocol.
//!
//! Storage is the disk-backed store (multi-node commit 2): every node gets
//! its own subdirectory under one per-test `TempDir` (the dir is returned to
//! the test so it outlives the cluster — concurrent tests and re-runs never
//! see each other's persisted log/vote files).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_common::{BasinError, PartitionKey, ProjectId};
use basin_wal::{
    BasinRaftItem, BasinRaftRequest, DurabilityBackend, Lsn, RaftDurability, RaftWal, RaftWalConfig,
    SimCluster, Wal, WalMode,
};
use bytes::Bytes;
use openraft::BasicNode;
use tempfile::TempDir;

const ELECTION_MS: u32 = 300;
const HEARTBEAT_MS: u32 = 100;

fn cfg(node_id: u64, cluster: Arc<SimCluster>, dir: &TempDir) -> RaftWalConfig {
    let mut cfg = RaftWalConfig::new(
        vec![],
        format!("node-{node_id}"),
        dir.path().join(format!("node-{node_id}")),
    )
    .with_node_id(node_id)
    .with_cluster(cluster);
    cfg.election_timeout_ms = ELECTION_MS;
    cfg.heartbeat_interval_ms = HEARTBEAT_MS;
    cfg
}

/// Build a cluster of `n` nodes, bootstrap node 1 as leader, add the rest as
/// learners, then promote them to voters. The returned `TempDir` holds every
/// node's raft storage and must stay alive for the cluster's lifetime.
async fn spin_up_cluster(n: u64) -> (Arc<SimCluster>, Vec<Arc<RaftWal>>, TempDir) {
    let dir = TempDir::new().unwrap();
    let cluster = SimCluster::new();
    let mut nodes = Vec::new();
    for id in 1..=n {
        let wal = RaftWal::new(cfg(id, cluster.clone(), &dir)).await.unwrap();
        nodes.push(Arc::new(wal));
    }

    // Bootstrap with node 1 only, then add learners + change_membership.
    // This sequence works for any cluster size; single-node case skips the
    // learner add and goes straight to a one-member quorum.
    let mut initial = BTreeMap::new();
    initial.insert(1u64, BasicNode::new("node-1"));
    nodes[0].initialize(initial).await.unwrap();

    // Wait for self-election before adding learners.
    wait_for_leader(&nodes[0], Duration::from_secs(2)).await;

    if n > 1 {
        for id in 2..=n {
            nodes[0]
                .add_learner(id, BasicNode::new(format!("node-{id}")))
                .await
                .unwrap();
        }
        let voters: BTreeSet<u64> = (1..=n).collect();
        nodes[0].change_membership(voters).await.unwrap();
    }

    (cluster, nodes, dir)
}

async fn wait_for_leader(wal: &RaftWal, timeout: Duration) -> u64 {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(leader) = wal.current_leader().await {
            return leader;
        }
        if Instant::now() > deadline {
            panic!("no leader elected within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_high_water(
    wal: &dyn Wal,
    project: &ProjectId,
    partition: &PartitionKey,
    target: Lsn,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let hw = wal.high_water(project, partition).await.unwrap();
        if hw >= target {
            return;
        }
        if Instant::now() > deadline {
            panic!("high_water never reached {target}; last seen {hw}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_initializes() {
    let (_cluster, nodes, _dir) = spin_up_cluster(1).await;
    let wal = nodes[0].clone();
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    let lsn = wal
        .append(&project, &part, Bytes::from("hello"))
        .await
        .unwrap();
    assert_eq!(lsn, Lsn(1));
    assert_eq!(wal.high_water(&project, &part).await.unwrap(), Lsn(1));
    let entries = wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
    assert_eq!(entries.len(), 1);
    wal.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_appends() {
    let (_cluster, nodes, _dir) = spin_up_cluster(3).await;
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    let leader_id = wait_for_leader(&nodes[0], Duration::from_secs(2)).await;
    let leader = &nodes[(leader_id - 1) as usize];

    // Latency signal: time the first 10 appends.
    let start = Instant::now();
    for i in 1..=10u64 {
        let lsn = leader
            .append(&project, &part, Bytes::from(format!("payload-{i}")))
            .await
            .unwrap();
        assert_eq!(lsn, Lsn(i));
    }
    let elapsed = start.elapsed();
    eprintln!(
        "3-node append: 10 entries in {elapsed:?} ({:?}/op)",
        elapsed / 10
    );

    // Wait for replication to all nodes and verify every node's state
    // machine sees the full log.
    for node in &nodes {
        wait_for_high_water(
            node.as_ref(),
            &project,
            &part,
            Lsn(10),
            Duration::from_secs(2),
        )
        .await;
        let entries = node.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(entries.len(), 10);
    }

    for n in &nodes {
        n.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn five_node_cluster_appends() {
    let (_cluster, nodes, _dir) = spin_up_cluster(5).await;
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    let leader_id = wait_for_leader(&nodes[0], Duration::from_secs(2)).await;
    let leader = &nodes[(leader_id - 1) as usize];

    for i in 1..=10u64 {
        let lsn = leader
            .append(&project, &part, Bytes::from(format!("payload-{i}")))
            .await
            .unwrap();
        assert_eq!(lsn, Lsn(i));
    }

    for node in &nodes {
        wait_for_high_water(
            node.as_ref(),
            &project,
            &part,
            Lsn(10),
            Duration::from_secs(3),
        )
        .await;
    }

    for n in &nodes {
        n.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failure_triggers_election() {
    let (cluster, nodes, _dir) = spin_up_cluster(3).await;
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    let leader_id = wait_for_leader(&nodes[0], Duration::from_secs(2)).await;

    // Append one entry through the original leader to confirm steady state.
    let leader = &nodes[(leader_id - 1) as usize];
    leader
        .append(&project, &part, Bytes::from("pre-failure"))
        .await
        .unwrap();
    for node in &nodes {
        wait_for_high_water(
            node.as_ref(),
            &project,
            &part,
            Lsn(1),
            Duration::from_secs(2),
        )
        .await;
    }

    // Take leader down.
    cluster.take_down(leader_id).await;

    // Wait for one of the remaining nodes to win an election. Be generous:
    // the simulation network's RPC drop only happens *to* the downed node,
    // not from it (raft just times out heartbeats), and 0.9's election
    // timing has some warmup quirks. 30× the configured min is plenty.
    let deadline = Instant::now() + Duration::from_millis((ELECTION_MS as u64) * 30);
    let mut new_leader_id = None;
    while Instant::now() < deadline {
        for node in &nodes {
            if node.config().node_id == leader_id {
                continue;
            }
            if let Some(leader) = node.current_leader().await {
                if leader != leader_id {
                    new_leader_id = Some(leader);
                    break;
                }
            }
        }
        if new_leader_id.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let new_leader_id = new_leader_id.expect("no new leader elected after failure");
    assert_ne!(new_leader_id, leader_id);

    let new_leader = &nodes[(new_leader_id - 1) as usize];
    let lsn = new_leader
        .append(&project, &part, Bytes::from("post-failure"))
        .await
        .unwrap();
    assert_eq!(lsn, Lsn(2));

    for n in &nodes {
        n.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quorum_commit_required() {
    let (cluster, nodes, _dir) = spin_up_cluster(3).await;
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    let leader_id = wait_for_leader(&nodes[0], Duration::from_secs(2)).await;
    let leader = &nodes[(leader_id - 1) as usize];

    // Take down the two non-leader nodes. Survivor has no quorum; appends
    // should not commit.
    let mut downed = Vec::new();
    for node in &nodes {
        let id = node.config().node_id;
        if id != leader_id {
            cluster.take_down(id).await;
            downed.push(id);
        }
    }

    let append_fut = leader.append(&project, &part, Bytes::from("quorumless"));
    // Give the append a short window. With no quorum it must not return.
    let res = tokio::time::timeout(Duration::from_millis(500), append_fut).await;
    assert!(
        res.is_err(),
        "append unexpectedly committed without quorum: {res:?}"
    );

    // Bring one peer back; the append (or a fresh one) should commit.
    cluster.bring_up(downed[0]).await;
    // The previous append future was dropped; issue a fresh one.
    let lsn = tokio::time::timeout(
        Duration::from_secs(3),
        leader.append(&project, &part, Bytes::from("now-quorum")),
    )
    .await
    .expect("append timed out even with quorum")
    .unwrap();
    assert!(lsn >= Lsn(1));

    // Bring the rest back so shutdown is clean.
    for id in &downed[1..] {
        cluster.bring_up(*id).await;
    }
    for n in &nodes {
        n.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn log_replicated_to_all_nodes() {
    let (_cluster, nodes, _dir) = spin_up_cluster(3).await;
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    let leader_id = wait_for_leader(&nodes[0], Duration::from_secs(2)).await;
    let leader = &nodes[(leader_id - 1) as usize];

    for i in 1..=100u64 {
        leader
            .append(&project, &part, Bytes::from(format!("p{i}")))
            .await
            .unwrap();
    }
    for node in &nodes {
        wait_for_high_water(
            node.as_ref(),
            &project,
            &part,
            Lsn(100),
            Duration::from_secs(5),
        )
        .await;
        let entries = node.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(entries.len(), 100);
        for (i, e) in entries.iter().enumerate() {
            assert_eq!(e.lsn, Lsn((i + 1) as u64));
        }
    }
    for n in &nodes {
        n.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "snapshot truncation requires snapshot worker timing tuning; v0.2 follow-up"]
async fn truncate_with_snapshot() {
    let (_cluster, nodes, _dir) = spin_up_cluster(3).await;
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    let leader_id = wait_for_leader(&nodes[0], Duration::from_secs(2)).await;
    let leader = &nodes[(leader_id - 1) as usize];

    for i in 1..=1000u64 {
        leader
            .append(&project, &part, Bytes::from(format!("p{i}")))
            .await
            .unwrap();
    }
    for node in &nodes {
        wait_for_high_water(
            node.as_ref(),
            &project,
            &part,
            Lsn(1000),
            Duration::from_secs(10),
        )
        .await;
    }

    leader.truncate(&project, &part, Lsn(500)).await.unwrap();
    let remaining = leader.read_from(&project, &part, Lsn::ZERO).await.unwrap();
    for e in &remaining {
        assert!(
            e.lsn > Lsn(500),
            "entry {} should have been truncated",
            e.lsn
        );
    }
    assert_eq!(remaining.len(), 500);

    for n in &nodes {
        n.close().await.unwrap();
    }
}

/// Disk persistence end-to-end (multi-node commit 2): a single-node cluster
/// appends, shuts down, and a NEW `RaftWal` over the SAME `data_dir`
/// recovers the raft log + vote from disk — once the node re-elects itself
/// the protocol re-applies the recovered log and the state machine serves
/// every pre-restart entry with its original LSN.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_restart_recovers_log_from_disk() {
    let dir = TempDir::new().unwrap();
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    // First incarnation: bootstrap, append, shut down cleanly.
    {
        let cluster = SimCluster::new();
        let wal = RaftWal::new(cfg(1, cluster, &dir)).await.unwrap();
        let mut initial = BTreeMap::new();
        initial.insert(1u64, BasicNode::new("node-1"));
        wal.initialize(initial).await.unwrap();
        wait_for_leader(&wal, Duration::from_secs(2)).await;
        for i in 1..=7u64 {
            let lsn = wal
                .append(&project, &part, Bytes::from(format!("persisted-{i}")))
                .await
                .unwrap();
            assert_eq!(lsn, Lsn(i));
        }
        wal.close().await.unwrap();
    }

    // Second incarnation: fresh SimCluster (a "rebooted process"), same
    // data_dir. No initialize() — the membership lives in the recovered
    // log; the node elects itself from its persisted vote + log.
    let cluster = SimCluster::new();
    let wal = RaftWal::new(cfg(1, cluster, &dir)).await.unwrap();
    wait_for_leader(&wal, Duration::from_secs(5)).await;
    wait_for_high_water(&wal, &project, &part, Lsn(7), Duration::from_secs(5)).await;

    let entries = wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
    assert_eq!(entries.len(), 7, "all pre-restart entries recovered");
    for (i, e) in entries.iter().enumerate() {
        assert_eq!(e.lsn, Lsn((i + 1) as u64));
        assert_eq!(
            &e.payload[..],
            format!("persisted-{}", i + 1).as_bytes(),
            "payload byte-identical after restart"
        );
    }

    // The recovered node keeps accepting appends with continuous LSNs.
    let lsn = wal
        .append(&project, &part, Bytes::from("post-restart"))
        .await
        .unwrap();
    assert_eq!(lsn, Lsn(8), "LSN sequence continues across restart");

    wal.close().await.unwrap();
}

// ===========================================================================
// Multi-node commit 3 — manifest-anchored snapshots, log bounded by flush
// ===========================================================================

/// Count `raft.log` segment files across a node's data dir (used to assert the
/// log shrinks after a watermark purge — the on-disk size proxy).
fn raft_log_bytes(dir: &TempDir, node_id: u64) -> u64 {
    let p = dir.path().join(format!("node-{node_id}")).join("raft.log");
    std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0)
}

/// A flush watermark recorded on the leader purges the raft log up to the
/// watermark's log index, so the on-disk log stays bounded by the un-flushed
/// window — and the manifest pointer (durable LSN + catalog snapshot id) is
/// captured in a snapshot and survives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_watermark_purges_log_and_bounds_it() {
    let (_cluster, nodes, dir) = spin_up_cluster(1).await;
    let wal = nodes[0].clone();
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    // Write enough entries that the log file is non-trivial.
    for i in 1..=200u64 {
        wal.append(&project, &part, Bytes::from(format!("e{i}")))
            .await
            .unwrap();
    }
    wait_for_high_water(wal.as_ref(), &project, &part, Lsn(200), Duration::from_secs(10)).await;
    let bytes_before = raft_log_bytes(&dir, 1);
    assert!(bytes_before > 0, "log file should hold entries");

    // The compactor flushed rows through LSN 150 to object storage and
    // recorded catalog snapshot "cat-1": record the watermark, which snapshots
    // + purges the raft log up to the applied index.
    let purged = wal
        .record_flush_watermark(&project, &part, Lsn(150), "cat-1")
        .await
        .unwrap();
    assert!(purged.is_some(), "watermark advanced the purge floor");

    // The manifest pointer now certifies durability through 150.
    assert_eq!(
        wal.durable_watermark(&project, &part).await,
        Lsn(150),
        "manifest pointer records the flushed watermark",
    );

    // The on-disk raft log was compacted: it is strictly smaller now.
    let bytes_after = raft_log_bytes(&dir, 1);
    assert!(
        bytes_after < bytes_before,
        "watermark purge must shrink the raft log ({bytes_before} -> {bytes_after})",
    );

    // A second, non-advancing watermark is a no-op (idempotent).
    let purged2 = wal
        .record_flush_watermark(&project, &part, Lsn(150), "cat-1")
        .await
        .unwrap();
    assert!(purged2.is_none(), "non-advancing watermark does not re-purge");

    wal.close().await.unwrap();
}

/// Snapshot build → install round-trip across the live cluster: a follower
/// that falls behind the purge floor MUST catch up via raft snapshot install,
/// and that snapshot carries the leader's manifest pointer — so the follower's
/// recorded durable watermark ends up matching the leader's.
///
/// The manifest pointer is leader-local until a follower needs a snapshot
/// (full follower-catchup orchestration is the network-layer seam — see
/// APPLY.md). This test drives the one path that IS in scope here: a lagging
/// follower installing a snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_follower_gets_manifest_via_snapshot_install() {
    let (cluster, nodes, _dir) = spin_up_cluster(3).await;
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    let leader_id = wait_for_leader(&nodes[0], Duration::from_secs(2)).await;
    let leader = nodes[(leader_id - 1) as usize].clone();

    // Pick a follower to fall behind, and take it down so it misses the writes
    // that get purged away.
    let follower = nodes
        .iter()
        .find(|n| n.config().node_id != leader_id)
        .cloned()
        .unwrap();
    let follower_id = follower.config().node_id;
    cluster.take_down(follower_id).await;

    // The leader + remaining follower form a quorum; keep writing.
    for i in 1..=120u64 {
        leader
            .append(&project, &part, Bytes::from(format!("e{i}")))
            .await
            .unwrap();
    }

    // Flush watermark on the leader: snapshot + purge. The purged entries are
    // exactly the ones the downed follower is missing, so when it comes back
    // it cannot be caught up by log replication — it must install the snapshot
    // (which carries the manifest pointer).
    leader
        .record_flush_watermark(&project, &part, Lsn(100), "cat-7")
        .await
        .unwrap();
    assert_eq!(leader.durable_watermark(&project, &part).await, Lsn(100));

    // Bring the follower back: it installs the leader's snapshot.
    cluster.bring_up(follower_id).await;

    // The follower converges on the leader's durable watermark — proof the
    // manifest pointer rode the snapshot install.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if follower.durable_watermark(&project, &part).await == Lsn(100) {
            break;
        }
        if Instant::now() > deadline {
            panic!(
                "follower {follower_id} never received the manifest pointer via snapshot install"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for n in &nodes {
        n.close().await.unwrap();
    }
}

// ===========================================================================
// Multi-node commit 4 — batched proposals into the durability seam
// ===========================================================================

/// `WalMode` parses strictly: `local` (default) and `raft` are valid; anything
/// else is a hard error so a typo never silently downgrades durability.
#[test]
fn wal_mode_parses_strictly() {
    assert_eq!(WalMode::parse("").unwrap(), WalMode::Local);
    assert_eq!(WalMode::parse("local").unwrap(), WalMode::Local);
    assert_eq!(WalMode::parse("LOCAL").unwrap(), WalMode::Local);
    assert_eq!(WalMode::parse("raft").unwrap(), WalMode::Raft);
    assert_eq!(WalMode::parse(" Raft ").unwrap(), WalMode::Raft);
    assert!(WalMode::parse("banana").is_err());
    assert_eq!(WalMode::default(), WalMode::Local);
}

/// A batched proposal commits as one consensus round and advances `durable_lsn`
/// for every item, in batch order, with monotonic per-partition LSNs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batched_proposal_commits_on_quorum() {
    let (_cluster, nodes, _dir) = spin_up_cluster(3).await;
    let project = ProjectId::new();
    let part_a = PartitionKey::default_key();

    let leader_id = wait_for_leader(&nodes[0], Duration::from_secs(2)).await;
    let leader = nodes[(leader_id - 1) as usize].clone();
    let backend = RaftDurability::new(leader.clone());
    assert_eq!(backend.mode(), WalMode::Raft);

    // One proposal carrying five appends to the same partition.
    let batch: Vec<BasinRaftItem> = (1..=5)
        .map(|i| BasinRaftItem {
            project,
            partition: part_a.clone(),
            payload: format!("b{i}").into_bytes(),
        })
        .collect();
    let lsns = backend.commit_batch(batch).await.unwrap();
    assert_eq!(lsns, vec![Lsn(1), Lsn(2), Lsn(3), Lsn(4), Lsn(5)], "monotonic per-partition LSNs in batch order");

    // durable_lsn (quorum-committed) covers the whole batch on every node.
    for node in &nodes {
        wait_for_high_water(node.as_ref(), &project, &part_a, Lsn(5), Duration::from_secs(10)).await;
    }

    // A mixed-partition batch keeps each partition's LSN sequence independent.
    let part_b = PartitionKey::new("partition-b").unwrap();
    let mixed = vec![
        BasinRaftItem { project, partition: part_a.clone(), payload: b"a6".to_vec() },
        BasinRaftItem { project, partition: part_b.clone(), payload: b"b1".to_vec() },
        BasinRaftItem { project, partition: part_a.clone(), payload: b"a7".to_vec() },
    ];
    let lsns = backend.commit_batch(mixed).await.unwrap();
    assert_eq!(lsns, vec![Lsn(6), Lsn(1), Lsn(7)], "per-partition LSNs assigned in item order");

    // Empty batch is a no-op.
    assert!(backend.commit_batch(Vec::new()).await.unwrap().is_empty());

    for n in &nodes {
        n.close().await.unwrap();
    }
}

/// No quorum ⇒ the write fails with the typed retryable `RaftNoQuorum` error
/// rather than silently dropping or hanging forever. Triggered against a node
/// that has no leader (never initialised), where `client_write` returns
/// `ForwardToLeader` immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_quorum_fails_typed() {
    let dir = TempDir::new().unwrap();
    let cluster = SimCluster::new();
    // A single node that is NOT initialised — it has no leader, so a write
    // cannot reach quorum and openraft returns ForwardToLeader(None).
    let wal = Arc::new(RaftWal::new(cfg(1, cluster, &dir)).await.unwrap());
    let backend = RaftDurability::new(wal.clone());

    let project = ProjectId::new();
    let part = PartitionKey::default_key();
    let item = BasinRaftItem { project, partition: part, payload: b"x".to_vec() };

    let err = tokio::time::timeout(
        Duration::from_secs(3),
        backend.commit_batch(vec![item]),
    )
    .await
    .expect("write should fail fast, not hang, with no leader")
    .expect_err("write without quorum must error");

    assert!(
        matches!(err, BasinError::RaftNoQuorum(_)),
        "no-quorum write must surface the typed retryable error, got {err:?}",
    );

    wal.close().await.unwrap();
}

/// Single-node raft (quorum of 1) is a real end-to-end durability backend:
/// write → quorum (self) commit → durable, readable, LSN-stamped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_raft_write_to_durable() {
    let (_cluster, nodes, _dir) = spin_up_cluster(1).await;
    let wal = nodes[0].clone();
    let backend = RaftDurability::new(wal.clone());

    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    // A quorum of one: each proposal commits the moment it is fsync'd locally.
    let lsns = backend
        .commit_batch(vec![
            BasinRaftItem { project, partition: part.clone(), payload: b"one".to_vec() },
            BasinRaftItem { project, partition: part.clone(), payload: b"two".to_vec() },
        ])
        .await
        .unwrap();
    assert_eq!(lsns, vec![Lsn(1), Lsn(2)]);

    let entries = wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(&entries[0].payload[..], b"one");
    assert_eq!(&entries[1].payload[..], b"two");
    assert_eq!(wal.high_water(&project, &part).await.unwrap(), Lsn(2));

    wal.close().await.unwrap();
}

/// The `BasinRaftRequest::single` / `propose_batch` surface the network agent
/// (commit 5) and the engine build against stays stable: a single proposal is
/// a batch of one, and the response exposes the one LSN.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_proposal_is_batch_of_one() {
    let (_cluster, nodes, _dir) = spin_up_cluster(1).await;
    let wal = nodes[0].clone();
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    let lsns = wal
        .propose_batch(BasinRaftRequest::single(project, part.clone(), b"solo".to_vec()))
        .await
        .unwrap();
    assert_eq!(lsns, vec![Lsn(1)]);

    wal.close().await.unwrap();
}
