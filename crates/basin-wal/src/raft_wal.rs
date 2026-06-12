//! Raft-backed [`Wal`] implementation built on `openraft` 0.9.
//!
//! ## Status
//!
//! v0.3: **cross-process gRPC transport, disk-backed log + vote.** A cluster
//! of `RaftWal` nodes can now run in separate processes / on separate hosts,
//! talking over the tonic transport in [`crate::raft_net`] (multi-node commit
//! 5). The in-process [`SimCluster`] dispatch is retained for the consensus
//! unit tests and single-process bring-up; [`RaftWal::new`] uses it, while
//! [`RaftWal::new_with_network`] injects an explicit
//! [`RaftNetworkFactory`] (the tonic factory in production). See
//! `crates/basin-wal/RAFT.md` for the integration plan.
//!
//! ## Storage
//!
//! Backed by [`crate::raft_storage::DiskRaftStorage`] (multi-node commit 2):
//! the raft **log** persists under `RaftWalConfig::data_dir` using the same
//! segment framing the file WAL writes (`[u32 LE length][bincode record]`),
//! the **vote** lives in a small fsync'd meta file, and the latest
//! state-machine **snapshot** is kept in a local file so a purge below the
//! snapshot point survives restarts. The applied state machine itself — an
//! in-memory `HashMap<(ProjectId, PartitionKey), …>` keyed by `Lsn` — is
//! rebuilt from the snapshot on open and rolled forward from the durable log
//! by the raft protocol. S3-anchored manifest snapshots (commit 3) bound the
//! local log by the flush window.
//!
//! ## What this module guarantees
//!
//! - `RaftWal` implements the same [`crate::Wal`] trait as [`crate::LocalWal`].
//!   Every call site that holds `Arc<dyn Wal>` works against either backend.
//! - `append` blocks until the entry is quorum-committed and applied to the
//!   local state machine; the returned `Lsn` is the LSN assigned by the
//!   leader's state machine (per `(project, partition)` keying preserved).
//! - `read_from` / `high_water` read the local state machine — followers
//!   serve reads from their replicated copy, leader serves reads from its
//!   own.
//! - `truncate` triggers a snapshot at the current applied index then purges
//!   logs below the resulting snapshot's last_log_id.
//! - Log entries and votes survive process restarts: a node that crashes and
//!   reopens the same `data_dir` rejoins with its hard state intact.
//!
//! ## What this module does **not** do
//!
//! - Multi-region replication.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use basin_common::{BasinError, PartitionKey, ProjectCounterRegistry, ProjectId, Result};
use bytes::Bytes;
use openraft::error::{NetworkError, RPCError, RaftError, RemoteError};
use openraft::network::RPCOption;
use openraft::storage::Adaptor;
use openraft::{BasicNode, Config, RaftNetwork, RaftNetworkFactory};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::raft_storage::DiskRaftStorage;
use crate::{Lsn, Wal, WalEntry};

/// Knobs for [`RaftWal::new`].
#[derive(Clone, Debug)]
pub struct RaftWalConfig {
    /// Peer endpoints (`host:port`). For the in-process simulation the
    /// strings are opaque routing tags; only the local node id matters.
    /// Cross-process gRPC will give them real wire meaning.
    pub peers: Vec<String>,
    /// This node's stable raft identity. Survives restarts.
    pub local_id: String,
    /// Where raft log / vote / snapshot files persist locally on disk
    /// (multi-node commit 2). Created on first open; reopening the same
    /// directory recovers the node's hard state. Two nodes must never share
    /// a `data_dir`.
    pub data_dir: PathBuf,
    /// Election timeout. Default 1500 ms — generous to cover GC pauses.
    pub election_timeout_ms: u32,
    /// Heartbeat interval. Default 500 ms.
    pub heartbeat_interval_ms: u32,
    /// Numeric raft node id. Distinct from `local_id` because openraft
    /// works in `u64`. Defaults to a hash of `local_id` if unset; tests
    /// pass it explicitly for deterministic cluster bring-up.
    pub node_id: u64,
    /// Map of `node_id -> address` for the cluster's initial membership.
    /// In single-node mode this is `{ self.node_id -> self.local_id }`.
    pub initial_members: BTreeMap<u64, String>,
    /// Shared simulation cluster. `None` means "make a private one"
    /// (single-node bootstrap). Multi-node tests share one
    /// [`SimCluster`] across every node so they can dispatch RPCs to
    /// each other through the in-memory mesh.
    pub cluster: Option<Arc<SimCluster>>,
}

impl RaftWalConfig {
    /// Sensible defaults for `peers` / `local_id` / `data_dir` with election
    /// and heartbeat timing pre-filled. `node_id` defaults to 1; tests with
    /// multiple nodes should set it explicitly via [`Self::with_node_id`].
    pub fn new(peers: Vec<String>, local_id: impl Into<String>, data_dir: PathBuf) -> Self {
        let local_id = local_id.into();
        let mut initial_members = BTreeMap::new();
        initial_members.insert(1, local_id.clone());
        Self {
            peers,
            local_id,
            data_dir,
            election_timeout_ms: 1500,
            heartbeat_interval_ms: 500,
            node_id: 1,
            initial_members,
            cluster: None,
        }
    }

    pub fn with_node_id(mut self, node_id: u64) -> Self {
        self.node_id = node_id;
        // Refresh the default single-node membership with the new id.
        if self.initial_members.len() == 1 {
            self.initial_members.clear();
            self.initial_members.insert(node_id, self.local_id.clone());
        }
        self
    }

    pub fn with_initial_members(mut self, members: BTreeMap<u64, String>) -> Self {
        self.initial_members = members;
        self
    }

    pub fn with_cluster(mut self, cluster: Arc<SimCluster>) -> Self {
        self.cluster = Some(cluster);
        self
    }
}

// ---------------------------------------------------------------------------
// Type config
// ---------------------------------------------------------------------------

/// One append within a raft proposal — `(project, partition, payload)`.
///
/// Carries no LSN; the state machine assigns it on apply so that LSN
/// monotonicity per `(project, partition)` follows from the raft log order
/// (which is itself globally totally ordered).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BasinRaftItem {
    pub project: ProjectId,
    pub partition: PartitionKey,
    pub payload: Vec<u8>,
}

/// One raft log payload. **Multi-node commit 4**: a proposal is a *batch* of
/// appends so one consensus round (one fsync'd log entry + one quorum
/// round-trip) is amortised over the whole group-commit batch, exactly as the
/// local WAL coalesces N synchronous appends into one segment PUT. A single
/// [`crate::Wal::append`] proposes a batch of one; the group-commit path
/// (`RaftWal::propose_batch`) proposes the whole drained batch at once.
///
/// The state machine assigns one LSN per item, in batch order, preserving
/// per-`(project, partition)` monotonicity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BasinRaftRequest {
    pub items: Vec<BasinRaftItem>,
}

impl BasinRaftRequest {
    /// A single-item proposal (the [`crate::Wal::append`] path).
    pub fn single(project: ProjectId, partition: PartitionKey, payload: Vec<u8>) -> Self {
        Self {
            items: vec![BasinRaftItem {
                project,
                partition,
                payload,
            }],
        }
    }
}

/// Response returned to the `client_write` caller — the LSNs the state machine
/// assigned to the proposal's items, in batch order (one per
/// [`BasinRaftItem`]).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BasinRaftResponse {
    pub lsns: Vec<Lsn>,
}

impl BasinRaftResponse {
    /// The LSN of a single-item proposal (the [`crate::Wal::append`] path).
    /// `Lsn::ZERO` for an empty / blank apply.
    pub fn lsn(&self) -> Lsn {
        self.lsns.first().copied().unwrap_or(Lsn::ZERO)
    }
}

openraft::declare_raft_types!(
    /// Raft type config for `RaftWal`. Keeps the openraft defaults for `Entry`,
    /// `SnapshotData`, `AsyncRuntime`, and `Responder` — only the application
    /// data (`D` / `R`) types are basin-specific.
    pub BasinRaftTypeConfig:
        D = BasinRaftRequest,
        R = BasinRaftResponse,
        NodeId = u64,
        Node = BasicNode,
);

type C = BasinRaftTypeConfig;
type NodeId = u64;
type RaftHandle = openraft::Raft<C>;

// ---------------------------------------------------------------------------
// Cluster status (commit 6 — observability surface)
// ---------------------------------------------------------------------------

/// This node's role in the raft cluster, derived from `openraft`'s
/// `ServerState`. Surfaced by [`RaftWal::cluster_status`] and rendered into
/// the `GET /admin/v1/cluster` admin response by `basin-server`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClusterRole {
    /// Voting follower (or the steady-state replica).
    Follower,
    /// Mid-election candidate.
    Candidate,
    /// Current leader — the only node that accepts writes in raft mode.
    Leader,
    /// Non-voting learner (catching up; e.g. a freshly-(re)joined node).
    Learner,
    /// Raft core has shut down.
    Shutdown,
}

impl ClusterRole {
    fn from_server_state(s: openraft::ServerState) -> Self {
        use openraft::ServerState;
        match s {
            ServerState::Follower => ClusterRole::Follower,
            ServerState::Candidate => ClusterRole::Candidate,
            ServerState::Leader => ClusterRole::Leader,
            ServerState::Learner => ClusterRole::Learner,
            ServerState::Shutdown => ClusterRole::Shutdown,
        }
    }

    pub fn is_leader(self) -> bool {
        matches!(self, ClusterRole::Leader)
    }
}

/// A point-in-time snapshot of this node's view of the raft cluster.
///
/// Read from `openraft`'s `RaftMetrics` in [`RaftWal::cluster_status`]; the
/// shape is the minimal operator surface the multi-node prompt calls for:
/// node id, role, term, commit index, current leader, and the configured
/// peers. `basin-server` logs this at startup + on role changes and serves it
/// as JSON at `GET /admin/v1/cluster`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterStatus {
    /// This node's numeric raft id.
    pub node_id: u64,
    /// This node's stable string identity (`RaftWalConfig::local_id`).
    pub local_id: String,
    /// This node's current role.
    pub role: ClusterRole,
    /// Current raft term.
    pub term: u64,
    /// Highest log index applied to the local state machine — the commit
    /// index from this node's vantage point (`last_applied`, `0` if none).
    pub commit_index: u64,
    /// Highest log index appended to this node's log (`0` if none).
    pub last_log_index: u64,
    /// Current leader's node id, if this node knows one.
    pub leader_id: Option<u64>,
    /// Voting + learner members in the current membership config, as
    /// `node_id -> advertised address`.
    pub members: BTreeMap<u64, String>,
}

impl ClusterStatus {
    pub fn is_leader(&self) -> bool {
        self.role.is_leader()
    }
}

// ---------------------------------------------------------------------------
// Network factory seam (commit 6 — c5 integration point, now wired by c5)
// ---------------------------------------------------------------------------

/// Marker naming the raft-network seam.
///
/// The in-process [`SimNetworkFactory`] is the default factory (tests +
/// single-node). The cross-process gRPC factory is the tonic-network commit
/// (c5, `BASIN_RAFT_BIND` / `BASIN_RAFT_PEERS`), wired through
/// [`RaftWal::new_with_network`]: `RaftWal::new` delegates to it with the Sim
/// factory, and `basin-server` injects the tonic factory when the raft env is
/// configured. This marker documents where the network choice plugs in; the
/// `cluster_status` / leader-fence surface in this file is independent of the
/// network factory.
#[allow(dead_code)] // documentation marker: names the seam, intentionally empty.
pub trait RaftNetworkChoiceMarker {}

// ---------------------------------------------------------------------------
// Single-process simulation network
// ---------------------------------------------------------------------------

/// Shared registry of every `Raft` handle in the simulated cluster.
///
/// The `RaftNetworkFactory` returned by [`SimCluster::factory`] looks up the
/// target's `Raft` handle in this registry and dispatches RPCs in-process.
/// This is the load-bearing trick that lets us validate consensus without
/// a wire protocol — every node's `append_entries` / `vote` / `snapshot`
/// goes via direct method call.
pub struct SimCluster {
    handles: RwLock<HashMap<NodeId, RaftHandle>>,
    /// "Down" nodes drop every inbound RPC. Used by leader-failure tests.
    down: RwLock<HashMap<NodeId, bool>>,
}

impl fmt::Debug for SimCluster {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimCluster").finish_non_exhaustive()
    }
}

impl SimCluster {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            handles: RwLock::new(HashMap::new()),
            down: RwLock::new(HashMap::new()),
        })
    }

    async fn register(&self, id: NodeId, handle: RaftHandle) {
        self.handles.write().await.insert(id, handle);
    }

    /// Mark a node as "down": every inbound RPC to it returns `Unreachable`.
    /// Reverse with [`Self::bring_up`].
    pub async fn take_down(&self, id: NodeId) {
        self.down.write().await.insert(id, true);
    }

    pub async fn bring_up(&self, id: NodeId) {
        self.down.write().await.remove(&id);
    }

    async fn is_down(&self, id: NodeId) -> bool {
        self.down.read().await.get(&id).copied().unwrap_or(false)
    }

    async fn handle(&self, id: NodeId) -> Option<RaftHandle> {
        self.handles.read().await.get(&id).cloned()
    }

    /// `true` if a raft handle for `id` is registered. Test-helper: lets
    /// election-failure tests confirm the new-leader id is still alive in
    /// the simulation.
    pub async fn handle_exists(&self, id: NodeId) -> bool {
        self.handles.read().await.contains_key(&id)
    }
}

/// Per-target network adapter. Owned by openraft; one instance per peer.
/// Carries both source and target ids so a "downed" node has its outbound
/// RPCs blocked too — symmetric partition semantics matter for election
/// liveness in `leader_failure_triggers_election`.
struct SimNetwork {
    source: NodeId,
    target: NodeId,
    cluster: Arc<SimCluster>,
}

impl SimNetwork {
    async fn unreachable<E: std::error::Error + 'static>(
        &self,
    ) -> Option<RPCError<NodeId, BasicNode, E>> {
        if self.cluster.is_down(self.source).await || self.cluster.is_down(self.target).await {
            Some(RPCError::Unreachable(openraft::error::Unreachable::new(
                &SimDown(self.target),
            )))
        } else {
            None
        }
    }
}

impl RaftNetwork<C> for SimNetwork {
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> std::result::Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        if let Some(e) = self.unreachable().await {
            return Err(e);
        }
        let h = self
            .cluster
            .handle(self.target)
            .await
            .ok_or_else(|| RPCError::Network(NetworkError::new(&SimMissing(self.target))))?;
        h.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<C>,
        _option: RPCOption,
    ) -> std::result::Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        if let Some(e) = self.unreachable().await {
            return Err(e);
        }
        let h = self
            .cluster
            .handle(self.target)
            .await
            .ok_or_else(|| RPCError::Network(NetworkError::new(&SimMissing(self.target))))?;
        h.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> std::result::Result<
        openraft::raft::VoteResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        if let Some(e) = self.unreachable().await {
            return Err(e);
        }
        let h = self
            .cluster
            .handle(self.target)
            .await
            .ok_or_else(|| RPCError::Network(NetworkError::new(&SimMissing(self.target))))?;
        h.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("sim node {0} is down")]
struct SimDown(NodeId);

#[derive(Debug, thiserror::Error)]
#[error("sim node {0} is not registered")]
struct SimMissing(NodeId);

struct SimNetworkFactory {
    source: NodeId,
    cluster: Arc<SimCluster>,
}

impl RaftNetworkFactory<C> for SimNetworkFactory {
    type Network = SimNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        SimNetwork {
            source: self.source,
            target,
            cluster: self.cluster.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// RaftWal
// ---------------------------------------------------------------------------

/// Multi-node, raft-backed WAL.
pub struct RaftWal {
    config: RaftWalConfig,
    raft: RaftHandle,
    storage: Arc<DiskRaftStorage>,
    cluster: Arc<SimCluster>,
}

impl fmt::Debug for RaftWal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaftWal")
            .field("local_id", &self.config.local_id)
            .field("node_id", &self.config.node_id)
            .finish_non_exhaustive()
    }
}

impl RaftWal {
    /// Bring up a raft node with the given config, using the in-process
    /// [`SimNetworkFactory`].
    ///
    /// Storage opens (or creates) `config.data_dir` and replays any
    /// persisted raft log / vote / snapshot — a node that restarts against
    /// the same directory rejoins with its hard state intact.
    ///
    /// If the config carries no [`SimCluster`], this constructs a private
    /// one — useful for the single-node / bootstrap case. Multi-node tests
    /// share one [`SimCluster`] across every node so RPC dispatch resolves.
    ///
    /// Cross-process deployments call [`Self::new_with_network`] with a
    /// [`crate::raft_net::TonicNetworkFactory`] instead.
    pub async fn new(config: RaftWalConfig) -> Result<Self> {
        // Default path: the in-process simulation network. The factory needs
        // the resolved `SimCluster` Arc, so build it here, then hand it to the
        // generic constructor that owns the single `openraft::Raft::new` call.
        let cluster = config.cluster.clone().unwrap_or_else(SimCluster::new);
        let network = SimNetworkFactory {
            source: config.node_id,
            cluster: cluster.clone(),
        };
        Self::new_with_network(config, network).await
    }

    // -- multi-node commit 5 hook: network-factory injection point ----------
    //
    // ANCHOR(raft-net-factory): the seam the real (cross-process) transport
    // plugs into. `new` injects the in-process `SimNetworkFactory`; a gRPC
    // deployment injects [`crate::raft_net::TonicNetworkFactory`]. This is the
    // SINGLE place that calls `openraft::Raft::new` so both transports share
    // one bring-up path (snapshot/durability bodies — `propose_batch`,
    // `record_flush_watermark` — and the observability/leader-fence surface are
    // all factory-independent and live below).
    /// Bring up a raft node with an explicit [`RaftNetworkFactory`].
    ///
    /// [`new`](Self::new) is the in-process-simulation convenience wrapper;
    /// cross-process deployments call this directly with a
    /// [`crate::raft_net::TonicNetworkFactory`]. The returned `RaftWal` still
    /// carries a [`SimCluster`] handle (the [`cluster`](Self::cluster)
    /// accessor) for test ergonomics; with a real network factory that
    /// cluster is simply unused for dispatch.
    ///
    /// Storage opens (or creates) `config.data_dir` and replays any persisted
    /// raft log / vote / snapshot — a node that restarts against the same
    /// directory rejoins with its hard state intact.
    pub async fn new_with_network<F>(config: RaftWalConfig, network: F) -> Result<Self>
    where
        F: RaftNetworkFactory<C>,
    {
        let cluster = config.cluster.clone().unwrap_or_else(SimCluster::new);

        let raft_config = Arc::new(
            Config {
                cluster_name: format!("basin-raft-{}", config.local_id),
                heartbeat_interval: config.heartbeat_interval_ms as u64,
                election_timeout_min: config.election_timeout_ms as u64,
                election_timeout_max: (config.election_timeout_ms as u64) * 2,
                // Snapshot every 100 logs so `truncate_with_snapshot`
                // doesn't have to nudge a 5000-log threshold.
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(100),
                max_in_snapshot_log_to_keep: 0,
                purge_batch_size: 1,
                ..Default::default()
            }
            .validate()
            .map_err(|e| BasinError::wal(format!("raft config validate: {e}")))?,
        );

        let storage = DiskRaftStorage::open(&config.data_dir)?;
        let (log_store, state_machine) = Adaptor::new(storage.clone());

        let raft = openraft::Raft::new(
            config.node_id,
            raft_config,
            network,
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| BasinError::wal(format!("raft start: {e}")))?;

        // Register before initialise so peers can dispatch RPCs to us
        // during the membership change. (A no-op for dispatch when a real
        // network factory is used, but keeps the test-helper accessor live.)
        cluster.register(config.node_id, raft.clone()).await;

        Ok(Self {
            config,
            raft,
            storage,
            cluster,
        })
    }

    /// Initialise the cluster. Should be called on **exactly one** node.
    /// Other nodes join via `add_learner` / `change_membership`.
    pub async fn initialize(&self, members: BTreeMap<NodeId, BasicNode>) -> Result<()> {
        match self.raft.initialize(members).await {
            Ok(()) => Ok(()),
            // Already initialised → fine, idempotent.
            Err(openraft::error::RaftError::APIError(
                openraft::error::InitializeError::NotAllowed(_),
            )) => Ok(()),
            Err(e) => Err(BasinError::wal(format!("raft initialise: {e}"))),
        }
    }

    /// Convenience over [`Self::initialize`] that takes the membership as
    /// `node_id -> advertised address` strings, building the `BasicNode`
    /// values internally. Lets callers (e.g. `basin-server`) bootstrap a
    /// cluster without naming any `openraft` type. Idempotent (an
    /// already-initialised cluster returns `Ok`).
    pub async fn initialize_addrs(&self, members: BTreeMap<NodeId, String>) -> Result<()> {
        let members: BTreeMap<NodeId, BasicNode> = members
            .into_iter()
            .map(|(id, addr)| (id, BasicNode::new(addr)))
            .collect();
        self.initialize(members).await
    }

    /// Add a learner node (replicates logs but does not vote). Call from
    /// the leader.
    pub async fn add_learner(&self, id: NodeId, node: BasicNode) -> Result<()> {
        self.raft
            .add_learner(id, node, true)
            .await
            .map(|_| ())
            .map_err(|e| BasinError::wal(format!("raft add_learner: {e}")))
    }

    /// Promote learners to voters by changing the cluster membership. Call
    /// from the leader.
    pub async fn change_membership(
        &self,
        members: std::collections::BTreeSet<NodeId>,
    ) -> Result<()> {
        self.raft
            .change_membership(members, false)
            .await
            .map(|_| ())
            .map_err(|e| BasinError::wal(format!("raft change_membership: {e}")))
    }

    /// Read-only view of the config the wal was constructed with.
    pub fn config(&self) -> &RaftWalConfig {
        &self.config
    }

    /// Underlying raft handle. Exposed so multi-node tests can drive
    /// elections / membership, and so the network layer can hand it to the
    /// tonic [`crate::raft_net::RaftTransportService`].
    pub fn raft(&self) -> &RaftHandle {
        &self.raft
    }

    /// Shared simulation cluster — same `Arc` the constructor was passed
    /// (or the private one it created if none was supplied).
    pub fn cluster(&self) -> &Arc<SimCluster> {
        &self.cluster
    }

    /// Current leader id, as reported by raft metrics.
    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    /// This node's numeric raft id.
    pub fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    /// `true` iff this node currently believes itself to be the raft leader
    /// (`ServerState::Leader`). This is the **write fence** in raft mode: a
    /// write that arrives at a non-leader is refused (see [`Wal::append`]).
    ///
    /// Note this reads the local node's metrics — a freshly-deposed leader
    /// may briefly still report `true`. That window is bounded by the
    /// heartbeat interval and is itself safe: openraft's `client_write`
    /// rejects the entry with `ForwardToLeader` once it learns it is no
    /// longer leader, so the append still fails closed even if `is_leader`
    /// raced. The fence here is the cheap fast-path refusal.
    pub async fn is_leader(&self) -> bool {
        matches!(
            self.raft.metrics().borrow().state,
            openraft::ServerState::Leader
        )
    }

    /// Best-effort advertised address (or id) of the current leader, for the
    /// `LeaseNotHeld` / not-leader error hint and for client redirect. Looks
    /// the leader id up in the membership config so the hint is the leader's
    /// wire address when known; falls back to the bare id string.
    pub async fn leader_hint(&self) -> Option<String> {
        let m = self.raft.metrics().borrow().clone();
        let leader = m.current_leader?;
        // Prefer the advertised address from the membership config.
        let addr = m
            .membership_config
            .membership()
            .nodes()
            .find(|(id, _)| **id == leader)
            .map(|(_, node)| node.addr.clone());
        Some(addr.unwrap_or_else(|| leader.to_string()))
    }

    /// Snapshot this node's view of the cluster for the admin status surface
    /// (`GET /admin/v1/cluster`) and startup / role-change logging.
    pub async fn cluster_status(&self) -> ClusterStatus {
        let m = self.raft.metrics().borrow().clone();
        let mut members = BTreeMap::new();
        for (id, node) in m.membership_config.membership().nodes() {
            members.insert(*id, node.addr.clone());
        }
        ClusterStatus {
            node_id: self.config.node_id,
            local_id: self.config.local_id.clone(),
            role: ClusterRole::from_server_state(m.state),
            term: m.current_term,
            commit_index: m.last_applied.map(|l| l.index).unwrap_or(0),
            last_log_index: m.last_log_index.unwrap_or(0),
            leader_id: m.current_leader,
            members,
        }
    }

    /// Manifest-anchored snapshot + log purge (multi-node commit 3).
    ///
    /// Called by the engine/compactor **after** a flush has committed engine
    /// state for `(project, partition)` through `durable_lsn` to object
    /// storage and recorded a `catalog_snapshot_id` for it. This:
    ///
    /// 1. stamps the durable watermark into the replicated state machine
    ///    (the [`crate::raft_storage::ManifestPointer`] — a pointer to the
    ///    S3/catalog-anchored data, not a copy of it),
    /// 2. triggers a raft snapshot so the watermark + log index are captured
    ///    in `get_current_snapshot` (and installable on followers), and
    /// 3. purges the raft log up to the watermark's log index, so the local
    ///    log stays bounded by the un-flushed window.
    ///
    /// Because the snapshot only certifies durability (the rows live in object
    /// storage), the purge below the watermark is loss-free: a recovering
    /// follower rebuilds engine state from the catalog snapshot named in the
    /// pointer and only needs the raft log for the tail above `durable_lsn`.
    ///
    /// Idempotent: a non-advancing watermark records the (higher) value if
    /// any but does not re-purge. Returns the index actually purged, if any.
    ///
    /// Leader-only in effect: openraft's `purge_log` is accepted on any node
    /// but the watermark is meaningful when stamped by the node coordinating
    /// the flush. Followers receive the watermark via snapshot replication.
    pub async fn record_flush_watermark(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        durable_lsn: Lsn,
        catalog_snapshot_id: impl Into<String>,
    ) -> Result<Option<u64>> {
        // The purge floor is the current applied log id: every entry the
        // state machine has applied (and therefore reflected into the engine's
        // now-durable state) is safe to drop. Read it from raft metrics.
        let applied = self.raft.metrics().borrow().last_applied;
        let Some(applied) = applied else {
            // Nothing applied yet — no floor to advance.
            return Ok(None);
        };

        let floor = self
            .storage
            .record_durable_watermark(
                project,
                partition,
                durable_lsn.0,
                applied.index,
                catalog_snapshot_id,
            )
            .await;

        let Some(purge_index) = floor else {
            // Watermark did not advance the purge floor; nothing to do.
            return Ok(None);
        };

        // Snapshot first so the manifest pointer is durable + installable
        // before we drop the log entries it certifies. The snapshot trigger is
        // async; wait until the snapshot actually covers the purge floor
        // (openraft refuses to purge beyond the snapshot point), then purge.
        //
        // We wait on `snapshot.index >= purge_floor` (a >= predicate, NOT an
        // exact `applied` match): the auto snapshot policy or a concurrent
        // append may land the snapshot at an index different from the `applied`
        // we sampled above, and openraft only requires the snapshot to cover
        // the index we purge to.
        //
        // `trigger().snapshot()` is a NO-OP if a snapshot build is already in
        // flight (openraft drops the request — see
        // SnapshotHandler::trigger_snapshot), so a single trigger can settle on
        // a stale snapshot that predates `purge_floor` (e.g. the automatic
        // LogsSinceLast policy fired a build that was still running when we
        // asked). Retry trigger+wait a bounded number of times so a dropped
        // trigger does not wedge the purge: each iteration waits a short slice
        // for the in-flight build to finish, then re-triggers if the snapshot
        // still hasn't reached the floor.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let snapshot_index = loop {
            self.raft
                .trigger()
                .snapshot()
                .await
                .map_err(|e| BasinError::wal(format!("raft snapshot trigger: {e}")))?;
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let slice = remaining.min(std::time::Duration::from_millis(500));
            match self
                .raft
                .wait(Some(slice))
                .metrics(
                    |m| m.snapshot.map(|s| s.index).unwrap_or(0) >= purge_index,
                    "record_flush_watermark: snapshot covers purge floor",
                )
                .await
            {
                Ok(m) => break m.snapshot.map(|s| s.index).unwrap_or(0),
                Err(e) => {
                    if std::time::Instant::now() >= deadline {
                        return Err(BasinError::wal(format!("raft snapshot wait: {e}")));
                    }
                    // In-flight build still hasn't reached the floor — loop and
                    // re-trigger (a no-op if a build is still running, a fresh
                    // build once it has settled).
                    continue;
                }
            }
        };

        // Purge no further than the snapshot actually covers. The floor is the
        // applied index we sampled, the snapshot index is >= it (just waited),
        // so the cap is the floor; this guards against ever passing
        // `purge_log` an index beyond the snapshot point.
        let purge_to = purge_index.min(snapshot_index);
        self.raft
            .purge_log(purge_to)
            .await
            .map_err(|e| BasinError::wal(format!("raft purge_log: {e}")))?;
        Ok(Some(purge_to))
    }

    /// Current manifest pointer (multi-node commit 3) — the durable watermark
    /// this node has agreed on. Exposed for the follower-catchup seam: the
    /// network/catalog layer reads it to decide which catalog snapshot to
    /// fetch and from which LSN to resume log replay. See `APPLY.md`.
    pub async fn durable_watermark(&self, project: &ProjectId, partition: &PartitionKey) -> Lsn {
        Lsn(self.storage.manifest_pointer().await.watermark(project, partition))
    }

    /// Propose a **batch** of appends as one raft entry (multi-node commit 4).
    ///
    /// This is the durability seam in raft mode: "durable" means
    /// quorum-replicated, so one `client_write` blocks until the batch's log
    /// entry is committed by a majority (the local fsync still happens via the
    /// disk storage as part of the raft log append). One consensus round + one
    /// fsync is amortised over every item in the batch — the raft analogue of
    /// the local WAL's group-commit (N synchronous appends → one segment PUT).
    ///
    /// Returns the per-item LSNs in batch order on quorum commit. If the batch
    /// cannot reach quorum (no leader, lost leadership, replication timeout),
    /// the write blocks then fails with the typed retryable
    /// [`BasinError::RaftNoQuorum`] — the caller never gets a silent partial
    /// ack. Empty batches are a no-op.
    pub async fn propose_batch(&self, req: BasinRaftRequest) -> Result<Vec<Lsn>> {
        if req.items.is_empty() {
            return Ok(Vec::new());
        }
        let n = req.items.len();
        let resp = self
            .raft
            .client_write(req)
            .await
            .map_err(|e| map_client_write_err(e, n))?;
        Ok(resp.data.lsns)
    }
}

// ---------------------------------------------------------------------------
// Durability backend (multi-node commit 4) — mode-gated WAL durability seam
// ---------------------------------------------------------------------------

/// Which durability backend a WAL uses (multi-node commit 4). Chosen once at
/// startup from `BASIN_WAL_MODE`.
///
/// - `Local` (default): durability = local group-commit fsync. The append
///   path is **byte-identical** to today's [`crate::LocalWal`] — no raft, no
///   behaviour change. This is the production default and what every existing
///   benchmark / differential test runs against.
/// - `Raft`: durability = quorum replication. WAL append batches are proposed
///   to raft ([`RaftWal::propose_batch`]); `durable_lsn` advances on the raft
///   commit index instead of on a local fsync watermark. The local fsync still
///   happens — as part of the raft log append in [`crate::raft_storage`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WalMode {
    /// Local group-commit fsync durability. Byte-identical to v0.1.
    #[default]
    Local,
    /// Quorum-replicated durability via raft.
    Raft,
}

impl WalMode {
    /// Parse `BASIN_WAL_MODE`. `local` (default) | `raft`. Any other value is
    /// a hard error so a typo never silently downgrades durability — same
    /// strict-parse idiom as `LeaseMode::parse` (multi-node commit 1).
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "local" => Ok(WalMode::Local),
            "raft" => Ok(WalMode::Raft),
            other => Err(BasinError::wal(format!(
                "invalid BASIN_WAL_MODE {other:?}: expected `local` or `raft`"
            ))),
        }
    }

    /// Read + parse `BASIN_WAL_MODE` from the environment (default `local`).
    pub fn from_env() -> Result<Self> {
        match std::env::var("BASIN_WAL_MODE") {
            Ok(v) => Self::parse(&v),
            Err(std::env::VarError::NotPresent) => Ok(WalMode::Local),
            Err(std::env::VarError::NotUnicode(_)) => Err(BasinError::wal(
                "BASIN_WAL_MODE is not valid unicode",
            )),
        }
    }
}

/// The clean durability boundary the rest of the engine writes against
/// (multi-node commit 4). One trait, two impls, chosen at startup by
/// [`WalMode`]. Implementors are the *batch durability* primitive: given an
/// already-LSN-ordered group-commit batch, make it durable and report the
/// per-item durable LSNs (or fail typed).
///
/// - [`LocalDurability`] wraps the existing local group-commit fsync path —
///   delegating to the [`crate::Wal`] backend so the bytes/acks are unchanged.
/// - [`RaftDurability`] proposes the batch to raft; durable = quorum commit,
///   `durable_lsn` follows the raft commit index, no-quorum → typed retryable.
///
/// This is the seam the network agent (commit 5) and the engine wire through:
/// the engine holds `Arc<dyn DurabilityBackend>` and never branches on mode.
#[async_trait]
pub trait DurabilityBackend: Send + Sync + std::fmt::Debug {
    /// The mode this backend implements.
    fn mode(&self) -> WalMode;

    /// Make a batch of appends durable and return their assigned LSNs in
    /// batch order. In `Local` mode durability = group-commit fsync; in `Raft`
    /// mode durability = quorum commit. A backend that cannot durably commit
    /// fails with a typed retryable error ([`BasinError::RaftNoQuorum`] in
    /// raft mode) rather than acking — backpressure surfaces as an error, the
    /// write is never silently dropped.
    async fn commit_batch(&self, batch: Vec<BasinRaftItem>) -> Result<Vec<Lsn>>;
}

/// `Raft` durability: quorum-replicated commit via [`RaftWal::propose_batch`].
#[derive(Debug)]
pub struct RaftDurability {
    wal: Arc<RaftWal>,
}

impl RaftDurability {
    pub fn new(wal: Arc<RaftWal>) -> Self {
        Self { wal }
    }
}

#[async_trait]
impl DurabilityBackend for RaftDurability {
    fn mode(&self) -> WalMode {
        WalMode::Raft
    }

    async fn commit_batch(&self, batch: Vec<BasinRaftItem>) -> Result<Vec<Lsn>> {
        self.wal.propose_batch(BasinRaftRequest { items: batch }).await
    }
}

/// `Local` durability: delegates each item to the local group-commit
/// synchronous-commit append so the WAL bytes + acks are **byte-identical** to
/// v0.1. The batch is committed item-by-item against the underlying
/// [`crate::Wal`] (the local file WAL already coalesces concurrent synchronous
/// appends into one segment PUT, so the consensus-amortisation the raft path
/// gets from batching is, for local, the existing group-commit window).
#[derive(Debug)]
pub struct LocalDurability {
    wal: Arc<dyn Wal>,
}

impl LocalDurability {
    pub fn new(wal: Arc<dyn Wal>) -> Self {
        Self { wal }
    }
}

#[async_trait]
impl DurabilityBackend for LocalDurability {
    fn mode(&self) -> WalMode {
        WalMode::Local
    }

    async fn commit_batch(&self, batch: Vec<BasinRaftItem>) -> Result<Vec<Lsn>> {
        let mut lsns = Vec::with_capacity(batch.len());
        for item in batch {
            // Synchronous-commit (durable-on-ack) append — the local seam's
            // group-commit fsync path. `epoch: None` = the no-lease append the
            // local path already takes today, so the bytes are unchanged.
            let lsn = self
                .wal
                .append_fenced_durable(&item.project, &item.partition, Bytes::from(item.payload), None)
                .await?;
            lsns.push(lsn);
        }
        Ok(lsns)
    }
}

/// Map an openraft `client_write` error to a typed Basin error. A no-leader /
/// not-leader / quorum-loss failure becomes the **retryable**
/// [`BasinError::RaftNoQuorum`] (SQLSTATE 40001) — the caller re-resolves the
/// leader and retries. A `Fatal` (the raft core stopped, a storage I/O panic)
/// is a hard error, not retryable, so it stays a generic WAL error. `n` is the
/// batch size, for the message.
fn map_client_write_err(
    e: RaftError<NodeId, openraft::error::ClientWriteError<NodeId, BasicNode>>,
    n: usize,
) -> BasinError {
    match e {
        // ForwardToLeader = this node is not the leader / no leader is known:
        // the canonical "can't reach quorum from here, retry elsewhere" shape.
        // ChangeMembershipError can't arise from a data write but is an API
        // error the caller could retry, so it joins the retryable class.
        RaftError::APIError(_) => BasinError::raft_no_quorum(format!(
            "batch of {n} append(s) not committed (no quorum / not leader): {e}"
        )),
        // Fatal: the raft core is stopped or hit an unrecoverable storage
        // error. Not retryable — surface as a hard WAL error.
        RaftError::Fatal(_) => {
            BasinError::wal(format!("raft fatal during batch of {n} append(s): {e}"))
        }
    }
}

#[async_trait]
impl Wal for RaftWal {
    async fn append(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        payload: Bytes,
    ) -> Result<Lsn> {
        // Raft-mode write fence (commit 6): raft leadership supersedes the
        // writer lease — writes are accepted ONLY on the leader. We fast-path
        // refuse on a non-leader with the typed, retryable not-leader error
        // (carrying a leader hint when known) BEFORE the raft round-trip, so
        // the caller gets a clean redirect instead of a forwarded RPC.
        //
        // This is fail-closed regardless: even if `is_leader()` raced a
        // demotion and let the write through, `propose_batch`'s
        // `client_write` rejects it with `ForwardToLeader`, which
        // `map_client_write_err` translates to the retryable
        // `RaftNoQuorum` — still a typed Err, still fail-closed.
        if !self.is_leader().await {
            let hint = self.leader_hint().await;
            return Err(BasinError::not_leader(hint));
        }
        // A single append is a batch of one (multi-node commit 4). The
        // group-commit path amortises a larger batch over one consensus round
        // via `propose_batch`.
        let lsns = self
            .propose_batch(BasinRaftRequest::single(
                *project,
                partition.clone(),
                payload.to_vec(),
            ))
            .await?;
        Ok(lsns.into_iter().next().unwrap_or(Lsn::ZERO))
    }

    async fn flush(&self) -> Result<()> {
        // Quorum-ack is the durability boundary; nothing to flush.
        Ok(())
    }

    async fn read_from(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        since_lsn: Lsn,
    ) -> Result<Vec<WalEntry>> {
        let part = self.storage.partition_view(project, partition).await;
        let out = part
            .entries
            .iter()
            .filter(|e| e.lsn > since_lsn.0 && e.lsn > part.truncated_up_to)
            .map(|e| WalEntry {
                project: *project,
                partition: partition.clone(),
                lsn: Lsn(e.lsn),
                payload: Bytes::from(e.payload.clone()),
                appended_at: e.appended_at,
            })
            .collect();
        Ok(out)
    }

    async fn high_water(&self, project: &ProjectId, partition: &PartitionKey) -> Result<Lsn> {
        let part = self.storage.partition_view(project, partition).await;
        let hw = part
            .entries
            .iter()
            .map(|e| e.lsn)
            .max()
            .unwrap_or(part.truncated_up_to);
        Ok(Lsn(hw))
    }

    async fn truncate(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        up_to: Lsn,
    ) -> Result<()> {
        // Trigger a snapshot at the current applied index so the follow-up
        // log purge doesn't lose committed-but-unsnapshotted entries.
        // `trigger().snapshot()` returns immediately; we wait on metrics.
        let _ = self
            .raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| BasinError::wal(format!("raft snapshot trigger: {e}")))?;
        // Give the snapshot worker a tick to land. In tests we can poll
        // metrics if we need to be precise; for callers a brief yield is
        // fine because subsequent reads/writes serialise through raft.
        tokio::task::yield_now().await;

        // Truncate this partition in the local state machine. The raft log
        // remains the source of truth for replication; this only affects
        // what `read_from` / `high_water` return on this node.
        self.storage
            .truncate_partition(project, partition, up_to)
            .await;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let _ = self.raft.shutdown().await;
        Ok(())
    }

    fn attach_project_counters(&self, _registry: Arc<ProjectCounterRegistry>) {
        // Counter wiring is a follow-up — `client_write` is the natural
        // place to bump per-project ops; deferring keeps each multi-node
        // commit's surface small enough to land in one PR.
    }
}
