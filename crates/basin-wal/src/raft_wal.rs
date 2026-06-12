//! Raft-backed [`Wal`] implementation built on `openraft` 0.9.
//!
//! ## Status
//!
//! v0.2: **single-process simulation, disk-backed log + vote.** A cluster of
//! `RaftWal` nodes runs inside one tokio runtime; inter-node RPCs
//! short-circuit through a shared handle map ([`SimCluster`]) instead of
//! going over the wire. This is enough to validate consensus correctness —
//! leader election, log replication, quorum commit, snapshot transfer —
//! without locking in a wire protocol. Cross-process gRPC `RaftNetwork` is a
//! follow-up; see `crates/basin-wal/RAFT.md` for the integration plan.
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
//! by the raft protocol. S3-anchored manifest snapshots are the follow-up
//! that bounds the local log by the flush window.
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
//! - Cross-process networking. The default [`SimNetworkFactory`] dispatches
//!   directly via in-memory `Raft` handles. A real gRPC factory plugs in
//!   behind the same trait when the network commit lands.
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

/// One raft log payload — an append for `(project, partition, payload)`.
///
/// Carries no LSN; the state machine assigns it on apply so that LSN
/// monotonicity per `(project, partition)` follows from the raft log order
/// (which is itself globally totally ordered).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BasinRaftRequest {
    pub project: ProjectId,
    pub partition: PartitionKey,
    pub payload: Vec<u8>,
}

/// Response returned to the `client_write` caller — the LSN the state
/// machine assigned to this entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BasinRaftResponse {
    pub lsn: Lsn,
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
    /// Bring up a raft node with the given config.
    ///
    /// Storage opens (or creates) `config.data_dir` and replays any
    /// persisted raft log / vote / snapshot — a node that restarts against
    /// the same directory rejoins with its hard state intact.
    ///
    /// If the config carries no [`SimCluster`], this constructs a private
    /// one — useful for the single-node / bootstrap case. Multi-node tests
    /// share one [`SimCluster`] across every node so RPC dispatch resolves.
    ///
    /// On success the node is registered with the cluster's handle map and
    /// the raft membership is initialised (single-node clusters bootstrap
    /// immediately; multi-node clusters wait for the caller to bootstrap
    /// one node and add the rest).
    pub async fn new(config: RaftWalConfig) -> Result<Self> {
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

        let network = SimNetworkFactory {
            source: config.node_id,
            cluster: cluster.clone(),
        };

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
        // during the membership change.
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

    /// Underlying raft handle. Test-only — production code uses the [`Wal`]
    /// trait. Exposed so multi-node tests can drive elections / membership.
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
}

#[async_trait]
impl Wal for RaftWal {
    async fn append(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        payload: Bytes,
    ) -> Result<Lsn> {
        let req = BasinRaftRequest {
            project: *project,
            partition: partition.clone(),
            payload: payload.to_vec(),
        };
        let resp = self
            .raft
            .client_write(req)
            .await
            .map_err(|e| BasinError::wal(format!("raft client_write: {e}")))?;
        Ok(resp.data.lsn)
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
