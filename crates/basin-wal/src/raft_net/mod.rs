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
//! ## Security posture
//!
//! The transport defaults to **plaintext gRPC** for a private cluster network
//! (a single VPC / private mesh, e.g. fly.io 6PN) where the raft port is NOT
//! exposed publicly. The openraft `Vote` carried in every RPC fences stale
//! leaders at the protocol layer.
//!
//! For deployments that cannot assume a trusted network, **mutual TLS** is
//! opt-in via [`tls::RaftTlsConfig`] (`BASIN_RAFT_TLS_CERT/KEY/CA`): both ends
//! present a certificate signed by the cluster CA, giving confidentiality plus
//! peer authentication — a node that cannot prove cluster membership cannot
//! speak raft. The [`PeerRegistry`] returns full URIs; with TLS on, a bare
//! `http://` is upgraded to `https://` so tonic runs the handshake. Use mTLS
//! whenever the raft port is reachable from anything outside the trusted
//! cluster network.

#[allow(clippy::all)]
pub(crate) mod proto {
    //! Generated tonic stubs (`package basin.raft.v1`).
    tonic::include_proto!("basin.raft.v1");
}

pub mod client;
pub mod codec;
pub mod peers;
pub mod server;
pub mod tls;

pub use client::{TonicNetworkFactory, TonicNetworkConfig};
pub use peers::{PeerRegistry, StaticPeers};
pub use server::{
    raft_bind_addr_from_env, serve_raft, serve_raft_on_listener,
    serve_raft_on_listener_with_tls, serve_raft_with_tls, RaftTransportService,
};
pub use tls::{RaftTlsConfig, DEFAULT_RAFT_TLS_DOMAIN};
