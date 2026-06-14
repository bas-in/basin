//! Mutual-TLS raft transport integration test (multi-node hardening).
//!
//! A 3-node localhost cluster over tonic where EVERY hop runs mutual TLS:
//! each node presents a leaf cert signed by a throwaway in-test cluster CA and
//! requires its peers to do the same. The cluster must elect a leader and
//! replicate over the encrypted+authenticated wire exactly as the plaintext
//! transport does — proving [`basin_wal::RaftTlsConfig`] removes the
//! "plaintext only" caveat without changing consensus behaviour.
//!
//! The CA + leaf certs are minted in-process with `rcgen`; no key material is
//! committed. Leaf certs carry the SNI domain (`basin-raft`) as a SAN and
//! `127.0.0.1` as an IP SAN so the localhost dial verifies.
//!
//! Gated on `raft-net` (needs `protoc` + tonic's `tls` feature):
//!   cargo test -p basin-wal --features raft-net --test raft_net_tls
#![cfg(feature = "raft-net")]

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_common::{PartitionKey, ProjectId};
use basin_wal::{
    serve_raft_on_listener_with_tls, RaftTlsConfig, RaftTransportService, RaftWal, RaftWalConfig,
    StaticPeers, TonicNetworkConfig, TonicNetworkFactory, Wal,
};
use bytes::Bytes;
use openraft::BasicNode;
use tempfile::TempDir;
use tokio::net::TcpListener;

const TLS_DOMAIN: &str = "basin-raft";

/// A throwaway cluster CA plus a helper to mint node leaf certs signed by it.
struct TestCa {
    ca_pem: String,
    ca: rcgen::Certificate,
    key: rcgen::KeyPair,
}

impl TestCa {
    fn new() -> Self {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "basin-raft-test-ca");
        let key = rcgen::KeyPair::generate().unwrap();
        let ca = params.self_signed(&key).unwrap();
        Self {
            ca_pem: ca.pem(),
            ca,
            key,
        }
    }

    /// Mint a leaf cert+key (PEM) carrying the `basin-raft` DNS SAN and the
    /// `127.0.0.1` IP SAN so a localhost dial verifies both SNI and address.
    fn leaf(&self) -> (String, String) {
        let mut params = rcgen::CertificateParams::new(vec![TLS_DOMAIN.to_string()]).unwrap();
        params
            .subject_alt_names
            .push(rcgen::SanType::IpAddress(std::net::IpAddr::V4(
                std::net::Ipv4Addr::LOCALHOST,
            )));
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, TLS_DOMAIN);
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let leaf = params.signed_by(&leaf_key, &self.ca, &self.key).unwrap();
        (leaf.pem(), leaf_key.serialize_pem())
    }

    /// A [`RaftTlsConfig`] for one node: a fresh leaf signed by this CA.
    fn node_tls(&self) -> RaftTlsConfig {
        let (cert, key) = self.leaf();
        RaftTlsConfig::from_pem(cert, key, self.ca_pem.clone(), TLS_DOMAIN)
    }
}

struct Node {
    id: u64,
    wal: Arc<RaftWal>,
    #[allow(dead_code)]
    server: tokio::task::JoinHandle<()>,
    addr: SocketAddr,
    #[allow(dead_code)]
    dir: TempDir,
}

fn partition(s: &str) -> PartitionKey {
    PartitionKey::new(s).unwrap()
}

fn factory(peers: Arc<StaticPeers>, tls: RaftTlsConfig) -> TonicNetworkFactory<StaticPeers> {
    let cfg = TonicNetworkConfig {
        rpc_timeout: Duration::from_millis(1500),
        connect_timeout: Duration::from_millis(800),
        backoff_min: Duration::from_millis(50),
        backoff_max: Duration::from_millis(400),
    };
    TonicNetworkFactory::with_shared(peers, cfg).with_tls(tls)
}

/// Bring up a `count`-node TLS cluster. Peers are registered as bare
/// `127.0.0.1:port` (the factory upgrades to `https://` because TLS is on).
async fn spin_up_tls_cluster(count: u64, ca: &TestCa) -> Vec<Node> {
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

    let mut nodes = Vec::new();
    for id in 1..=count {
        let dir = TempDir::new().unwrap();
        let addr = addrs[&id];
        let base = RaftWalConfig::new(vec![], format!("node-{id}"), dir.path().to_path_buf())
            .with_node_id(id);
        let cfg = RaftWalConfig {
            election_timeout_ms: 400,
            heartbeat_interval_ms: 120,
            ..base
        };
        let node_tls = ca.node_tls();
        let wal = Arc::new(
            RaftWal::new_with_network(cfg, factory(peers.clone(), node_tls.clone()))
                .await
                .unwrap(),
        );
        let svc = RaftTransportService::new(wal.raft().clone());
        let listener = listeners.remove(&id).unwrap();
        let (bound, server) =
            serve_raft_on_listener_with_tls(svc, listener, Some(node_tls)).unwrap();
        assert_eq!(bound, addr);
        nodes.push(Node {
            id,
            wal,
            server,
            addr,
            dir,
        });
    }
    nodes
}

fn basic_node(n: &Node) -> BasicNode {
    BasicNode::new(format!("127.0.0.1:{}", n.addr.port()))
}

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

async fn bootstrap(nodes: &[Node]) {
    let leader = &nodes[0];
    let mut seed = BTreeMap::new();
    seed.insert(leader.id, basic_node(leader));
    leader.wal.initialize(seed).await.unwrap();

    let elected = wait_until(Duration::from_secs(10), || async {
        leader.wal.current_leader().await == Some(leader.id)
    })
    .await;
    assert!(elected, "bootstrap node did not self-elect over TLS");

    for n in &nodes[1..] {
        leader.wal.add_learner(n.id, basic_node(n)).await.unwrap();
    }
    let voters: BTreeSet<u64> = nodes.iter().map(|n| n.id).collect();
    leader.wal.change_membership(voters).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_tls_cluster_elects_and_replicates() {
    let ca = TestCa::new();
    let nodes = spin_up_tls_cluster(3, &ca).await;
    bootstrap(&nodes).await;

    let ok = wait_until(Duration::from_secs(10), || async {
        nodes[0].wal.current_leader().await.is_some()
    })
    .await;
    assert!(ok, "no leader over TLS");
    let leader_id = nodes[0].wal.current_leader().await.unwrap();
    let leader = nodes.iter().find(|n| n.id == leader_id).unwrap();

    let proj = ProjectId::new();
    let part = partition("p");
    for i in 0..6u8 {
        leader
            .wal
            .append(&proj, &part, Bytes::from(vec![i; 32]))
            .await
            .unwrap();
    }

    // Every node converges to the leader's high-water mark over the TLS wire.
    let hw = leader.wal.high_water(&proj, &part).await.unwrap();
    for n in &nodes {
        let ok = wait_until(Duration::from_secs(8), || async {
            n.wal.high_water(&proj, &part).await.unwrap() == hw
        })
        .await;
        assert!(ok, "node {} did not replicate over TLS", n.id);
    }

    // Read back from a follower to prove the replicated bytes survived the
    // encrypted round-trip.
    let follower = nodes.iter().find(|n| n.id != leader.id).unwrap();
    let entries = follower
        .wal
        .read_from(&proj, &part, basin_wal::Lsn(0))
        .await
        .unwrap();
    assert_eq!(entries.len(), 6);

    for n in &nodes {
        let _ = n.wal.close().await;
    }
}
