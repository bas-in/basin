//! Client side of the raft transport: the openraft
//! [`RaftNetworkFactory`] / [`RaftNetwork`] impl over tonic channels.
//!
//! ## Connection management
//!
//! * **Lazy connect.** `new_client` builds a [`TonicNetwork`] but opens no
//!   socket — per the `RaftNetworkFactory` contract. The channel is dialed on
//!   the first RPC (and re-dialed after a drop).
//! * **Per-peer channel reuse.** Channels live in a shared
//!   `node_id → CachedChannel` map on the factory, so every `TonicNetwork`
//!   for the same target reuses one HTTP/2 connection (tonic multiplexes
//!   concurrent RPCs over it). A successful dial is cached; a transport
//!   failure evicts the entry so the next call re-dials a fresh address from
//!   the [`PeerRegistry`] (handles a peer that moved).
//! * **Reconnect with backoff.** openraft drives the backoff: on an
//!   `Unreachable` error it calls [`RaftNetwork::backoff`] and waits before
//!   retrying. We surface connect / timeout failures as `Unreachable` so this
//!   kicks in, and expose a tunable backoff schedule.
//! * **Request timeouts.** Each RPC is wrapped in `tokio::time::timeout`
//!   bounded by `RPCOption::hard_ttl` (openraft's own deadline) clamped to a
//!   configured ceiling (`BASIN_RAFT_RPC_TIMEOUT_MS`). A timeout maps to
//!   `Unreachable` (peer may be wedged → back off), matching the
//!   "bound-but-never-responds" liveness expectation.
//!
//! ## Error mapping (liveness-critical)
//!
//! openraft uses the error variant to decide whether a peer is a live
//! participant:
//!
//!   * connect refused / DNS fail / channel dial error → **Unreachable**
//!     (back off, keep counting toward election timeout).
//!   * request timeout → **Unreachable**.
//!   * unknown node id (registry miss) → **Unreachable** (membership may be
//!     ahead of discovery).
//!   * peer answered but the bytes were malformed / decode failed →
//!     **NetworkError** (retry immediately; transport glitch, not a dead
//!     peer).
//!   * peer answered with a `raft_error` envelope → **RemoteError** carrying
//!     the deserialised `RaftError` (the peer is alive and rejected us at the
//!     protocol layer).
//!
//! Conflating any of these would corrupt openraft's liveness decisions (e.g.
//! mapping a protocol rejection to Unreachable would make a healthy peer look
//! dead and churn elections), so the mapping is explicit and tested.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{NetworkError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{Backoff, RPCOption};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
use std::sync::Mutex;
use tonic::transport::{Channel, Endpoint};

use super::codec;
use super::peers::PeerRegistry;
use super::proto::raft_transport_client::RaftTransportClient;
use super::proto::{raft_rpc_response, RaftRpcRequest};
use super::tls::RaftTlsConfig;
use crate::raft_wal::BasinRaftTypeConfig;

type C = BasinRaftTypeConfig;
type NodeId = u64;

/// Tunables for the tonic transport. All env-overridable so ops can adjust
/// without a rebuild.
#[derive(Clone, Debug)]
pub struct TonicNetworkConfig {
    /// Per-RPC ceiling, clamping openraft's own `hard_ttl`. A request that
    /// exceeds this is treated as `Unreachable`. Default 5 s.
    pub rpc_timeout: Duration,
    /// TCP connect timeout for a fresh dial. Default 2 s.
    pub connect_timeout: Duration,
    /// First backoff interval handed to openraft on `Unreachable`. Default
    /// 200 ms.
    pub backoff_min: Duration,
    /// Backoff ceiling. The schedule doubles from `backoff_min` up to this.
    /// Default 3 s.
    pub backoff_max: Duration,
}

impl Default for TonicNetworkConfig {
    fn default() -> Self {
        Self {
            rpc_timeout: Duration::from_millis(5_000),
            connect_timeout: Duration::from_millis(2_000),
            backoff_min: Duration::from_millis(200),
            backoff_max: Duration::from_millis(3_000),
        }
    }
}

impl TonicNetworkConfig {
    /// Override defaults from env:
    /// `BASIN_RAFT_RPC_TIMEOUT_MS`, `BASIN_RAFT_CONNECT_TIMEOUT_MS`,
    /// `BASIN_RAFT_BACKOFF_MIN_MS`, `BASIN_RAFT_BACKOFF_MAX_MS`.
    /// Malformed values are ignored (fall back to default) with a warn — a
    /// bad timeout knob should not stop a node from joining.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Some(ms) = env_ms("BASIN_RAFT_RPC_TIMEOUT_MS") {
            cfg.rpc_timeout = ms;
        }
        if let Some(ms) = env_ms("BASIN_RAFT_CONNECT_TIMEOUT_MS") {
            cfg.connect_timeout = ms;
        }
        if let Some(ms) = env_ms("BASIN_RAFT_BACKOFF_MIN_MS") {
            cfg.backoff_min = ms;
        }
        if let Some(ms) = env_ms("BASIN_RAFT_BACKOFF_MAX_MS") {
            cfg.backoff_max = ms;
        }
        cfg
    }
}

fn env_ms(key: &str) -> Option<Duration> {
    match std::env::var(key) {
        Ok(v) => match v.trim().parse::<u64>() {
            Ok(ms) => Some(Duration::from_millis(ms)),
            Err(_) => {
                tracing::warn!(key, value = %v, "ignoring malformed raft timeout env var");
                None
            }
        },
        Err(_) => None,
    }
}

/// A connected channel plus the URI it was dialed against, so a stale entry
/// can be re-resolved after eviction.
#[derive(Clone)]
struct CachedChannel {
    uri: String,
    channel: Channel,
}

/// Factory: owns the peer registry, the per-peer channel cache, the transport
/// config, and (optionally) the mTLS material. Cloned (cheap — all `Arc`s /
/// PEM bytes) into each `TonicNetwork`.
#[derive(Clone)]
pub struct TonicNetworkFactory<P: PeerRegistry> {
    peers: Arc<P>,
    config: TonicNetworkConfig,
    channels: Arc<Mutex<HashMap<NodeId, CachedChannel>>>,
    /// mTLS material for outbound channels. `None` ⇒ plaintext (private-net).
    tls: Option<RaftTlsConfig>,
}

impl<P: PeerRegistry> TonicNetworkFactory<P> {
    pub fn new(peers: P, config: TonicNetworkConfig) -> Self {
        Self {
            peers: Arc::new(peers),
            config,
            channels: Arc::new(Mutex::new(HashMap::new())),
            tls: None,
        }
    }

    /// Build from a shared registry (the cloud passes its discovery `Arc`).
    pub fn with_shared(peers: Arc<P>, config: TonicNetworkConfig) -> Self {
        Self {
            peers,
            config,
            channels: Arc::new(Mutex::new(HashMap::new())),
            tls: None,
        }
    }

    /// Enable mutual TLS on every outbound channel this factory dials. The
    /// client presents its node identity and verifies each peer against the
    /// cluster CA + domain (see [`RaftTlsConfig`]). Returns `self` for chaining.
    pub fn with_tls(mut self, tls: RaftTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }
}

impl<P: PeerRegistry> RaftNetworkFactory<C> for TonicNetworkFactory<P> {
    type Network = TonicNetwork<P>;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        // No socket opened here — lazy connect on first RPC.
        TonicNetwork {
            target,
            peers: self.peers.clone(),
            config: self.config.clone(),
            channels: self.channels.clone(),
            tls: self.tls.clone(),
        }
    }
}

/// Per-target network adapter. Owned by openraft; one per peer.
pub struct TonicNetwork<P: PeerRegistry> {
    target: NodeId,
    peers: Arc<P>,
    config: TonicNetworkConfig,
    channels: Arc<Mutex<HashMap<NodeId, CachedChannel>>>,
    tls: Option<RaftTlsConfig>,
}

impl<P: PeerRegistry> TonicNetwork<P> {
    /// Resolve + (re)dial the target, reusing a cached channel when the
    /// resolved URI is unchanged. Returns an `Unreachable`-shaped error on
    /// any dial / resolve failure.
    async fn channel<E: std::error::Error + 'static>(
        &self,
    ) -> Result<Channel, RPCError<NodeId, BasicNode, E>> {
        let resolved = self
            .peers
            .resolve(self.target)
            .ok_or_else(|| unreachable(&PeerUnknown(self.target)))?;
        // With mTLS on, a registry that returns a bare `http://` (the
        // `normalize_uri` default) must be dialed over `https://` — the scheme
        // is what makes tonic run the TLS handshake. An explicit `https://`
        // from the operator is left untouched.
        let uri = if self.tls.is_some() {
            upgrade_to_https(&resolved)
        } else {
            resolved
        };

        let mut guard = self.channels.lock().expect("channel cache mutex poisoned");
        if let Some(cached) = guard.get(&self.target) {
            if cached.uri == uri {
                return Ok(cached.channel.clone());
            }
            // Address changed (peer moved) — drop the stale channel.
            guard.remove(&self.target);
        }

        let mut endpoint = Endpoint::from_shared(uri.clone())
            .map_err(|e| unreachable(&DialError(self.target, e.to_string())))?
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.rpc_timeout)
            // Detect a dead peer (killed process / dropped link) promptly so a
            // cached lazy channel re-dials instead of buffering RPCs against a
            // half-open connection. Without keepalive a peer that died and
            // restarted at the same address can leave the leader's cached
            // channel wedged: RPCs neither succeed nor fail fast, the follower
            // never hears a heartbeat, times out, and campaigns with a rising
            // term it can never win — a rejoining node fails to catch up.
            // HTTP/2 keepalive PINGs (bounded by the RPC timeout) surface the
            // break as a transport error, which `drive_rpc` maps to Unreachable
            // + evicts, so the next attempt re-dials the restarted peer.
            .tcp_keepalive(Some(self.config.connect_timeout))
            .http2_keep_alive_interval(self.config.connect_timeout)
            .keep_alive_timeout(self.config.rpc_timeout)
            .keep_alive_while_idle(true);

        // Apply mTLS to the endpoint when configured: present our node identity
        // and verify the peer against the cluster CA + domain. A bad TLS config
        // is a dial setup error → Unreachable (back off), never silent
        // plaintext.
        if let Some(tls) = &self.tls {
            endpoint = endpoint
                .tls_config(tls.client_tls())
                .map_err(|e| unreachable(&DialError(self.target, e.to_string())))?;
        }

        // `connect_lazy` returns immediately; the first RPC drives the dial,
        // and a dial failure surfaces there as a tonic transport error which
        // we map to Unreachable. This keeps `new_client`/`channel` cheap and
        // matches openraft's "don't block on connect" contract.
        let channel = endpoint.connect_lazy();
        guard.insert(
            self.target,
            CachedChannel {
                uri,
                channel: channel.clone(),
            },
        );
        Ok(channel)
    }

    /// Evict the cached channel after a transport failure so the next call
    /// re-resolves and re-dials.
    fn evict(&self) {
        self.channels
            .lock()
            .expect("channel cache mutex poisoned")
            .remove(&self.target);
    }

    /// Effective per-RPC deadline: the smaller of openraft's `hard_ttl` and
    /// our configured ceiling.
    fn deadline(&self, option: &RPCOption) -> Duration {
        option.hard_ttl().min(self.config.rpc_timeout)
    }
}

/// Shared RPC driver: encode request → unary call (with timeout) → decode the
/// envelope into `Ok(resp)` or `Err(RemoteError)`, mapping transport faults to
/// Unreachable/NetworkError.
///
/// `Resp` is the openraft response type; `E` is the remote error payload type
/// (`()` for append/vote, `InstallSnapshotError` for snapshot).
async fn drive_rpc<P, Req, Resp, E, Fut>(
    net: &TonicNetwork<P>,
    option: &RPCOption,
    req: &Req,
    call: impl FnOnce(RaftTransportClient<Channel>, RaftRpcRequest) -> Fut,
) -> Result<Resp, RPCError<NodeId, BasicNode, RaftError<NodeId, E>>>
where
    P: PeerRegistry,
    Req: serde::Serialize,
    Resp: serde::de::DeserializeOwned,
    E: serde::Serialize + serde::de::DeserializeOwned + std::error::Error + 'static,
    Fut: std::future::Future<
        Output = Result<tonic::Response<super::proto::RaftRpcResponse>, tonic::Status>,
    >,
{
    // Encode request payload (a local serialisation bug, not a network fault).
    let payload = codec::encode(req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
    let wire = RaftRpcRequest { payload };

    let channel = net.channel().await?;
    let client = RaftTransportClient::new(channel);

    let deadline = net.deadline(option);
    let result = tokio::time::timeout(deadline, call(client, wire)).await;

    let response = match result {
        // Timed out: peer may be wedged → Unreachable so openraft backs off.
        Err(_elapsed) => {
            net.evict();
            return Err(unreachable(&RpcTimeout(net.target, deadline)));
        }
        Ok(Ok(resp)) => resp.into_inner(),
        Ok(Err(status)) => {
            // tonic Status: distinguish transport-ish from app-ish.
            net.evict();
            return Err(map_status(net.target, status));
        }
    };

    match response.result {
        Some(raft_rpc_response::Result::Ok(bytes)) => {
            // Peer answered with a protocol response — decode it.
            codec::decode::<Resp>(&bytes).map_err(|e| RPCError::Network(NetworkError::new(&e)))
        }
        Some(raft_rpc_response::Result::RaftError(bytes)) => {
            // Peer is alive but its raft instance returned an error.
            let raft_err: RaftError<NodeId, E> = codec::decode_raft_error(&bytes)
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
            Err(RPCError::RemoteError(RemoteError::new(
                net.target, raft_err,
            )))
        }
        None => Err(RPCError::Network(NetworkError::new(&EmptyEnvelope(
            net.target,
        )))),
    }
}

/// Map a tonic `Status` into the right openraft error variant.
///
/// `Unavailable` / `DeadlineExceeded` / `Cancelled` are transport-level: the
/// peer is unreachable or wedged → `Unreachable` (back off). `invalid_argument`
/// means the peer rejected our bytes — a transport/encoding glitch, retryable
/// immediately → `NetworkError`. Anything else is treated as `Unreachable`
/// conservatively (a peer returning an odd status is more likely broken than
/// healthy-and-rejecting).
fn map_status<E>(
    _target: NodeId,
    status: tonic::Status,
) -> RPCError<NodeId, BasicNode, RaftError<NodeId, E>>
where
    E: std::error::Error,
{
    use tonic::Code;
    match status.code() {
        // Peer rejected our bytes — a transport/encoding glitch, not a dead
        // peer. Retry immediately.
        Code::InvalidArgument => RPCError::Network(NetworkError::new(&StatusWrap(status))),
        // Everything else (Unavailable, DeadlineExceeded, Cancelled, Unknown,
        // Aborted, …) means the peer is unreachable or wedged from our point
        // of view → back off. Conservative: an odd status from a peer is more
        // likely "broken" than "healthy and deliberately rejecting".
        _ => unreachable(&StatusWrap(status)),
    }
}

/// Construct an `Unreachable` RPCError. The `Unreachable` variant does not
/// carry the remote-error payload type, so the caller's `E` is left free for
/// inference at the use site (append/vote: `RaftError<NID>`; snapshot:
/// `RaftError<NID, InstallSnapshotError>`).
fn unreachable<E>(e: &(impl std::error::Error + 'static)) -> RPCError<NodeId, BasicNode, E>
where
    E: std::error::Error,
{
    RPCError::Unreachable(Unreachable::new(e))
}

impl<P: PeerRegistry> RaftNetwork<C> for TonicNetwork<P> {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        drive_rpc(self, &option, &rpc, |mut client, wire| async move {
            client.append_entries(wire).await
        })
        .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        drive_rpc(self, &option, &rpc, |mut client, wire| async move {
            client.vote(wire).await
        })
        .await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<C>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        drive_rpc(self, &option, &rpc, |mut client, wire| async move {
            client.install_snapshot(wire).await
        })
        .await
    }

    fn backoff(&self) -> Backoff {
        // openraft calls this when it has decided this peer is unreachable and
        // is about to back off before retrying. Drop the cached channel so the
        // retry re-dials from scratch: a peer that died and restarted at the
        // same address leaves a `connect_lazy` channel whose internal
        // reconnect can stay wedged against the old (now-closed) connection
        // state — RPCs on it neither complete nor surface a transport error we
        // would otherwise evict on, so the follower never hears a heartbeat,
        // times out, and campaigns forever without catching up. Evicting here
        // forces a fresh dial on the next replication attempt, which the
        // restarted peer answers, so it rejoins and catches up.
        self.evict();

        // Exponential schedule from backoff_min, doubling up to backoff_max,
        // then constant. openraft drops the iterator on the first success.
        let min = self.config.backoff_min;
        let max = self.config.backoff_max;
        let mut cur = min;
        Backoff::new(std::iter::from_fn(move || {
            let this = cur;
            cur = (cur * 2).min(max);
            Some(this)
        }))
    }
}

// ---------------------------------------------------------------------------
// Transport error types (all map to Unreachable/NetworkError, never carried
// as raft protocol state).
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
#[error("raft peer {0} has no resolvable address")]
struct PeerUnknown(NodeId);

#[derive(Debug, thiserror::Error)]
#[error("raft peer {0} dial setup failed: {1}")]
struct DialError(NodeId, String);

#[derive(Debug, thiserror::Error)]
#[error("raft rpc to peer {0} timed out after {1:?}")]
struct RpcTimeout(NodeId, Duration);

#[derive(Debug, thiserror::Error)]
#[error("raft rpc to peer transport status: {0}")]
struct StatusWrap(tonic::Status);

#[derive(Debug, thiserror::Error)]
#[error("raft peer {0} returned an empty response envelope")]
struct EmptyEnvelope(NodeId);

/// Rewrite a `http://…` URI to `https://…` so tonic runs the TLS handshake.
/// A URI that already names `https://` (operator opted in explicitly) is
/// returned unchanged; anything else is prefixed defensively.
fn upgrade_to_https(uri: &str) -> String {
    if let Some(rest) = uri.strip_prefix("http://") {
        format!("https://{rest}")
    } else if uri.starts_with("https://") {
        uri.to_string()
    } else {
        format!("https://{uri}")
    }
}

#[cfg(test)]
mod tests {
    use super::upgrade_to_https;

    #[test]
    fn upgrades_http_scheme() {
        assert_eq!(
            upgrade_to_https("http://10.0.0.1:6010"),
            "https://10.0.0.1:6010"
        );
    }

    #[test]
    fn leaves_https_untouched() {
        assert_eq!(upgrade_to_https("https://node:6010"), "https://node:6010");
    }

    #[test]
    fn prefixes_bare_authority() {
        assert_eq!(upgrade_to_https("node:6010"), "https://node:6010");
    }
}
