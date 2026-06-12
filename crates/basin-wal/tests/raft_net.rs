//! Cross-process raft transport integration tests (multi-node commit 5).
//!
//! These run a real 3-node cluster over tonic on localhost EPHEMERAL ports
//! (`127.0.0.1:0`), exchange the OS-assigned addresses, and drive consensus
//! through the actual gRPC wire — no `SimCluster` short-circuit. They cover:
//!
//!   * leader election + log replication over the wire,
//!   * killing a node (dropping its server) → cluster keeps committing 2/3,
//!     then restarting it → it catches up,
//!   * a peer that is bound but never answers → vote/append timeout maps to
//!     `Unreachable` and the cluster still makes progress,
//!   * large entry batch round-trip fidelity (payload bytes identical after a
//!     full serde_json → gRPC → serde_json round-trip).
//!
//! The whole file is gated on the `raft-net` feature so the default test run
//! (which has no `protoc`) is unaffected:
//!   cargo test -p basin-wal --features raft-net --test raft_net
#![cfg(feature = "raft-net")]

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_common::{PartitionKey, ProjectId};
use basin_wal::{
    serve_raft_on_listener, RaftTransportService, RaftWal, RaftWalConfig, StaticPeers,
    TonicNetworkConfig, TonicNetworkFactory, Wal,
};
use bytes::Bytes;
use openraft::BasicNode;
use tempfile::TempDir;
use tokio::net::TcpListener;

/// A live node: the wal, its server task handle (abort to "kill"), the disk
/// store dir (kept so a killed node can reopen from the same path), its bound
/// address, and the shared peer registry (to rebuild the network factory on
/// restart).
struct Node {
    id: u64,
    wal: Arc<RaftWal>,
    server: tokio::task::JoinHandle<()>,
    addr: SocketAddr,
    dir: TempDir,
    peers: Arc<StaticPeers>,
}

impl Node {
    /// Fully kill the node: shut its raft instance down (so it stops
    /// campaigning) and abort its server task. The disk state under `dir`
    /// survives, so [`restart_node`] can bring it back from the same path —
    /// modelling a process crash + restart.
    async fn kill(&self) {
        let _ = self.wal.close().await; // raft.shutdown()
        self.server.abort();
    }

}

/// Reopen a previously-killed node from its on-disk state at the same address
/// and rejoin it to the cluster, replacing the dead `Node` in `nodes`.
async fn restart_node(nodes: &mut [Node], id: u64) {
    let idx = nodes.iter().position(|n| n.id == id).unwrap();
    let addr = nodes[idx].addr;
    let peers = nodes[idx].peers.clone();
    let data_dir = nodes[idx].dir.path().to_path_buf();

    let base = RaftWalConfig::new(vec![], format!("node-{id}"), data_dir).with_node_id(id);
    let cfg = RaftWalConfig {
        election_timeout_ms: 300,
        heartbeat_interval_ms: 100,
        ..base
    };
    // Reopen storage from the same dir — membership + log + vote recover.
    let wal = Arc::new(
        RaftWal::new_with_network(cfg, factory(peers))
            .await
            .unwrap(),
    );
    let svc = RaftTransportService::new(wal.raft().clone());
    let listener = bind_retry(addr).await;
    let (_bound, server) = serve_raft_on_listener(svc, listener).unwrap();

    nodes[idx].wal = wal;
    nodes[idx].server = server;
}

// A single project id per test run. `ProjectId::new()` mints a fresh ULID,
// so callers MUST reuse the value (don't call this twice and expect equality).
fn project() -> ProjectId {
    ProjectId::new()
}

fn partition(s: &str) -> PartitionKey {
    PartitionKey::new(s).unwrap()
}

/// Build a `TonicNetworkFactory` over a shared static peer registry with
/// snappy timeouts/backoff suitable for tests.
fn factory(peers: Arc<StaticPeers>) -> TonicNetworkFactory<StaticPeers> {
    let cfg = TonicNetworkConfig {
        rpc_timeout: Duration::from_millis(800),
        connect_timeout: Duration::from_millis(400),
        backoff_min: Duration::from_millis(50),
        backoff_max: Duration::from_millis(400),
    };
    TonicNetworkFactory::with_shared(peers, cfg)
}

/// Bring up a `count`-node cluster over real tonic. Returns the nodes with
/// node 1 already designated leader candidate (it calls `initialize`).
async fn spin_up_cluster(count: u64) -> Vec<Node> {
    // 1. Bind a listener per node UP FRONT so we know each OS-assigned port
    //    before building the cluster. We keep the listeners and serve on the
    //    very same sockets later — no drop-then-rebind race.
    let mut listeners: BTreeMap<u64, TcpListener> = BTreeMap::new();
    let mut addrs: BTreeMap<u64, SocketAddr> = BTreeMap::new();
    for id in 1..=count {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.insert(id, l.local_addr().unwrap());
        listeners.insert(id, l);
    }
    let mut peers = StaticPeers::new();
    for (id, addr) in &addrs {
        peers.insert(*id, format!("127.0.0.1:{}", addr.port()));
    }
    let peers = Arc::new(peers);

    // 2. Construct each node (lazy-connect factory) + serve on its listener.
    let mut nodes = Vec::new();
    for id in 1..=count {
        let dir = TempDir::new().unwrap();
        let addr = addrs[&id];
        let base = RaftWalConfig::new(vec![], format!("node-{id}"), dir.path().to_path_buf())
            .with_node_id(id);
        // Snappy election timing for tests.
        let cfg = RaftWalConfig {
            election_timeout_ms: 300,
            heartbeat_interval_ms: 100,
            ..base
        };

        let wal = Arc::new(
            RaftWal::new_with_network(cfg, factory(peers.clone()))
                .await
                .unwrap(),
        );
        let svc = RaftTransportService::new(wal.raft().clone());
        let listener = listeners.remove(&id).unwrap();
        let (bound, server) = serve_raft_on_listener(svc, listener).unwrap();
        assert_eq!(bound, addr, "served on the reserved socket");

        nodes.push(Node {
            id,
            wal,
            server,
            addr,
            dir,
            peers: peers.clone(),
        });
    }

    nodes
}

/// Bind `addr` with a short retry to tolerate the brief window after a killed
/// server's socket is released (TIME_WAIT / async abort lag).
async fn bind_retry(addr: SocketAddr) -> TcpListener {
    for _ in 0..40 {
        match TcpListener::bind(addr).await {
            Ok(l) => return l,
            Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
    TcpListener::bind(addr).await.expect("rebind killed node addr")
}

fn basic_node(n: &Node) -> BasicNode {
    BasicNode::new(format!("127.0.0.1:{}", n.addr.port()))
}

/// Poll until `f` returns true or `deadline` elapses.
async fn wait_until<F, Fut>(deadline: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < deadline {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Initialize node 1 as the cluster, add the rest as learners, then promote
/// to voters. Returns once a leader is elected.
async fn bootstrap(nodes: &[Node]) {
    let leader = &nodes[0];

    // Initialize the cluster with ONLY the bootstrap node as a voter; the
    // rest join as learners and are then promoted. (Initializing with all
    // members up front would make add_learner a contradiction.)
    let mut seed = BTreeMap::new();
    seed.insert(leader.id, basic_node(leader));
    leader.wal.initialize(seed).await.unwrap();

    // Wait for the single-node cluster to self-elect before adding members —
    // add_learner / change_membership must run on the leader.
    let elected = wait_until(Duration::from_secs(10), || async {
        leader.wal.current_leader().await == Some(leader.id)
    })
    .await;
    assert!(elected, "bootstrap node did not self-elect");

    for n in &nodes[1..] {
        leader.wal.add_learner(n.id, basic_node(n)).await.unwrap();
    }
    let voters: BTreeSet<u64> = nodes.iter().map(|n| n.id).collect();
    leader.wal.change_membership(voters).await.unwrap();

    let ok = wait_until(Duration::from_secs(10), || async {
        leader.wal.current_leader().await.is_some()
    })
    .await;
    assert!(ok, "no leader elected within deadline");
}

/// Find the current leader node (by id), if any.
async fn leader_of(nodes: &[Node]) -> Option<u64> {
    for n in nodes {
        if let Some(l) = n.wal.current_leader().await {
            return Some(l);
        }
    }
    None
}

fn node_by_id(nodes: &[Node], id: u64) -> &Node {
    nodes.iter().find(|n| n.id == id).unwrap()
}

/// Append `payload` through whichever node is currently leader, tolerating
/// leadership moves (a partitioned-but-campaigning peer can shift the lead).
/// Retries within `deadline`; panics if no commit lands. Returns the
/// committing leader's id so the caller can read its high-water mark.
async fn commit_anywhere(
    nodes: &[Node],
    proj: &ProjectId,
    part: &PartitionKey,
    payload: Bytes,
    deadline: Duration,
) -> u64 {
    let start = Instant::now();
    loop {
        // Find a node that currently believes it is leader.
        let mut leader_id = None;
        for n in nodes {
            if n.wal.current_leader().await == Some(n.id) {
                leader_id = Some(n.id);
                break;
            }
        }
        if let Some(id) = leader_id {
            let leader = node_by_id(nodes, id);
            if leader.wal.append(proj, part, payload.clone()).await.is_ok() {
                return id;
            }
        }
        assert!(
            start.elapsed() < deadline,
            "no leader committed within {deadline:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_elects_leader_and_replicates() {
    let nodes = spin_up_cluster(3).await;
    bootstrap(&nodes).await;

    let leader_id = leader_of(&nodes).await.expect("leader");
    let leader = node_by_id(&nodes, leader_id);

    // Append 5 entries through the leader; each is quorum-committed.
    let proj = project();
    let part = partition("p");
    let mut lsns = Vec::new();
    for i in 0..5u8 {
        let lsn = leader
            .wal
            .append(&proj, &part, Bytes::from(vec![i; 16]))
            .await
            .unwrap();
        lsns.push(lsn);
    }
    assert_eq!(lsns.len(), 5);

    // Every node converges to the leader's high-water mark (replication over
    // the wire). Followers serve reads from their replicated copy.
    let hw_leader = leader.wal.high_water(&proj, &part).await.unwrap();
    for n in &nodes {
        let ok = wait_until(Duration::from_secs(5), || async {
            n.wal.high_water(&proj, &part).await.unwrap() == hw_leader
        })
        .await;
        assert!(ok, "node {} did not catch up to hw {:?}", n.id, hw_leader);
    }

    for n in &nodes {
        n.wal.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_survives_node_kill_and_catches_up_on_restart() {
    let mut nodes = spin_up_cluster(3).await;
    bootstrap(&nodes).await;

    let proj = project();
    let part = partition("p");

    // Commit some entries while all 3 are up.
    for i in 0..3u8 {
        commit_anywhere(&nodes, &proj, &part, Bytes::from(vec![i; 8]), Duration::from_secs(10)).await;
    }

    // Kill a FOLLOWER (not the current leader): shut its raft down + drop its
    // server, modelling a process crash. 2/3 quorum remains.
    let leader_id = leader_of(&nodes).await.unwrap();
    let victim_id = nodes.iter().map(|n| n.id).find(|id| *id != leader_id).unwrap();
    node_by_id(&nodes, victim_id).kill().await;

    // Cluster keeps committing with the 2 survivors. Commit through whoever
    // leads (the survivors re-elect among themselves if needed).
    let mut last_leader = leader_id;
    for i in 10..15u8 {
        last_leader = commit_anywhere(
            &nodes,
            &proj,
            &part,
            Bytes::from(vec![i; 8]),
            Duration::from_secs(10),
        )
        .await;
    }
    let hw_after = node_by_id(&nodes, last_leader)
        .wal
        .high_water(&proj, &part)
        .await
        .unwrap();

    // Restart the victim from its on-disk state at the same address. It
    // rejoins and replays the entries the cluster committed while it was down.
    restart_node(&mut nodes, victim_id).await;

    let ok = wait_until(Duration::from_secs(15), || async {
        node_by_id(&nodes, victim_id)
            .wal
            .high_water(&proj, &part)
            .await
            .unwrap()
            == hw_after
    })
    .await;
    assert!(ok, "restarted node did not catch up to {hw_after:?}");

    for n in &nodes {
        let _ = n.wal.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bound_but_unresponsive_peer_times_out_and_cluster_progresses() {
    // 3 voters; then one peer's server is replaced with a black-hole listener
    // that accepts TCP but never answers gRPC. append/vote RPCs to it must
    // hit the request timeout → Unreachable, and the remaining 2/3 keep
    // committing.
    let mut nodes = spin_up_cluster(3).await;
    bootstrap(&nodes).await;

    let proj = project();
    let part = partition("p");

    let leader_id = leader_of(&nodes).await.unwrap();
    let victim_id = nodes.iter().map(|n| n.id).find(|id| *id != leader_id).unwrap();

    // Fully stop the victim (raft + server) so it neither answers nor
    // campaigns, then plant a BLACK HOLE at its address: a listener that
    // accepts TCP but never speaks gRPC, so the two live nodes' RPCs to it
    // hang until the client-side request timeout fires → Unreachable.
    node_by_id(&nodes, victim_id).kill().await;
    let victim_addr = node_by_id(&nodes, victim_id).addr;
    let blackhole = tokio::spawn(async move {
        let l = bind_retry(victim_addr).await;
        let mut held = Vec::new();
        loop {
            match l.accept().await {
                // Hold each accepted socket open and never respond.
                Ok((sock, _)) => held.push(sock),
                Err(_) => break,
            }
        }
    });
    // Park the blackhole handle in the victim slot so it isn't dropped.
    {
        let idx = nodes.iter().position(|n| n.id == victim_id).unwrap();
        nodes[idx].server = blackhole;
    }

    // The two live nodes keep committing despite every RPC to the black hole
    // timing out. `commit_anywhere` routes to whichever survivor leads; each
    // commit must land within a budget that comfortably exceeds the 800 ms
    // per-RPC timeout (so a wedged peer cannot stall the quorum).
    for i in 0..6u8 {
        commit_anywhere(
            &nodes,
            &proj,
            &part,
            Bytes::from(vec![i; 8]),
            Duration::from_secs(12),
        )
        .await;
    }

    for n in &nodes {
        let _ = n.wal.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_entry_batch_round_trips_with_byte_fidelity() {
    let nodes = spin_up_cluster(3).await;
    bootstrap(&nodes).await;

    let proj = project();
    let part = partition("big");
    let leader_id = leader_of(&nodes).await.unwrap();
    let leader = node_by_id(&nodes, leader_id);

    // A batch of large, distinct payloads. These cross the wire as
    // serde_json-encoded openraft entries inside AppendEntries RPCs.
    let mut expected: Vec<Vec<u8>> = Vec::new();
    for i in 0..40u32 {
        // ~64 KiB each, deterministic but distinct content.
        let mut buf = vec![0u8; 64 * 1024];
        for (j, b) in buf.iter_mut().enumerate() {
            *b = ((i as usize).wrapping_mul(31).wrapping_add(j)) as u8;
        }
        leader
            .wal
            .append(&proj, &part, Bytes::from(buf.clone()))
            .await
            .unwrap();
        expected.push(buf);
    }

    // Read back from a FOLLOWER to prove the bytes survived the full
    // wire round-trip (leader → AppendEntries → follower state machine).
    let follower = nodes.iter().find(|n| n.id != leader_id).unwrap();
    let hw = leader.wal.high_water(&proj, &part).await.unwrap();
    let ok = wait_until(Duration::from_secs(10), || async {
        follower.wal.high_water(&proj, &part).await.unwrap() == hw
    })
    .await;
    assert!(ok, "follower did not replicate the large batch");

    let entries = follower
        .wal
        .read_from(&proj, &part, basin_wal::Lsn(0))
        .await
        .unwrap();
    assert_eq!(entries.len(), expected.len(), "entry count mismatch");
    for (e, exp) in entries.iter().zip(expected.iter()) {
        assert_eq!(e.payload.as_ref(), exp.as_slice(), "payload byte mismatch");
    }

    for n in &nodes {
        n.wal.close().await.unwrap();
    }
}
