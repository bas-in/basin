//! Cross-process raft transport over tonic/gRPC (multi-node commit 5).
//!
//! This module replaces the in-process [`crate::raft_wal::SimCluster`]
//! dispatch with a real wire protocol so `RaftWal` nodes can run in separate
//! processes / on separate hosts. It is feature-gated behind `raft-net`; the
//! default `basin-wal` build does not pull in tonic/prost and `build.rs`
//! skips codegen.
//!
//! ## Shape
//!
//! * [`proto`] — generated tonic stubs from `proto/raft.proto`.
//! * [`codec`] — serde_json (de)serialisation of openraft messages into the
//!   opaque `bytes payload` the proto carries, plus the error envelope.
//! * [`peers`] — node-id → address registry. [`PeerRegistry`] is the seam the
//!   cloud's service discovery plugs into later; [`StaticPeers`] is the v1
//!   env-driven (`BASIN_RAFT_PEERS`) implementation.
//! * [`server`] — the tonic [`RaftTransport`](proto::raft_transport_server)
//!   service hosting `append_entries` / `vote` / `install_snapshot`,
//!   dispatching into a local [`RaftHandle`].
//! * [`client`] — [`TonicNetworkFactory`] / `TonicNetwork`: the openraft
//!   `RaftNetworkFactory` / `RaftNetwork` impl over per-peer tonic channels
//!   with lazy connect, reconnect-with-backoff, channel reuse, request
//!   timeouts, and typed error mapping into openraft's RPCError semantics.
//!
//! ## Security posture (honest)
//!
//! v1 ships **plaintext gRPC, no authentication**. The cluster network is
//! assumed private (a single VPC / private mesh) and the raft port is NOT
//! exposed publicly. The openraft `Vote` carried in every RPC fences stale
//! leaders at the protocol layer, but it provides NO confidentiality and NO
//! peer authentication: anyone who can reach `BASIN_RAFT_BIND` can speak raft
//! to this node. **mTLS is the required production follow-up** — tonic's
//! `tls_config` on both `Server` and `Channel` is the seam, and the
//! [`PeerRegistry`] already returns full URIs so a future `https://` scheme +
//! client cert wiring is additive. Do not run this transport across an
//! untrusted network until mTLS lands.

#[allow(clippy::all)]
pub(crate) mod proto {
    //! Generated tonic stubs (`package basin.raft.v1`).
    tonic::include_proto!("basin.raft.v1");
}

pub mod client;
pub mod codec;
pub mod peers;
pub mod server;

pub use client::{TonicNetworkFactory, TonicNetworkConfig};
pub use peers::{PeerRegistry, StaticPeers};
pub use server::{raft_bind_addr_from_env, serve_raft, serve_raft_on_listener, RaftTransportService};
