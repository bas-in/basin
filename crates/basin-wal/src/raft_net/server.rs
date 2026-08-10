//! Server side of the raft transport: a tonic service hosting
//! `append_entries` / `vote` / `install_snapshot`, dispatching each inbound
//! RPC into the local openraft [`RaftHandle`].
//!
//! ## Dispatch & error packaging
//!
//! An inbound RPC decodes the openraft request from the opaque payload, calls
//! the matching method on the local `Raft` handle, and packages the result
//! into the [`proto::RaftRpcResponse`] envelope:
//!
//!   * `Ok(resp)`  → `result = ok(serde_json(resp))`. The RPC *succeeded* at
//!     the transport layer even if the protocol response is a rejection
//!     (higher term, vote-not-granted). That is correct: the peer answered.
//!   * `Err(raft_err)` → `result = raft_error(serde_json(raft_err))`. The
//!     local raft instance returned a `RaftError` (Fatal / APIError). The
//!     client re-inflates this into a `RemoteError`, NOT a network error.
//!
//! A decode failure of the *request* payload is a malformed-wire fault: it
//! returns a tonic `Status::invalid_argument`, which the client maps to
//! `NetworkError` (retry), never `RemoteError`.
//!
//! The gRPC call itself always returns `Ok(Response)` in the protocol-error
//! case — protocol errors travel inside the envelope, not as tonic statuses.
//! Only genuine transport faults (bad request bytes, encode failure of the
//! response) become tonic `Status`es.

use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::{BasinError, Result};
use tonic::{Request, Response, Status};

use super::codec;
use super::proto::raft_transport_server::{RaftTransport, RaftTransportServer};
use super::proto::{raft_rpc_response, RaftRpcRequest, RaftRpcResponse};
use super::tls::RaftTlsConfig;
use crate::raft_wal::BasinRaftTypeConfig;

type C = BasinRaftTypeConfig;
type RaftHandle = openraft::Raft<C>;

/// tonic service wrapping a local [`RaftHandle`]. One per node.
#[derive(Clone)]
pub struct RaftTransportService {
    raft: RaftHandle,
}

impl RaftTransportService {
    pub fn new(raft: RaftHandle) -> Self {
        Self { raft }
    }

    /// Build the tonic server stub for this service, ready to add to a
    /// `tonic::transport::Server` router (or serve directly via
    /// [`serve_raft`]).
    pub fn into_server(self) -> RaftTransportServer<Self> {
        RaftTransportServer::new(self)
    }
}

/// Wrap a successful openraft response into the envelope.
fn ok_envelope<T: serde::Serialize>(resp: &T) -> std::result::Result<RaftRpcResponse, Status> {
    let bytes =
        codec::encode(resp).map_err(|e| Status::internal(format!("encode raft response: {e}")))?;
    Ok(RaftRpcResponse {
        result: Some(raft_rpc_response::Result::Ok(bytes)),
    })
}

/// Wrap a local raft error into the envelope's `raft_error` arm.
fn err_envelope<E: serde::Serialize>(
    err: &openraft::error::RaftError<u64, E>,
) -> std::result::Result<RaftRpcResponse, Status> {
    let bytes = codec::encode_raft_error(err)
        .map_err(|e| Status::internal(format!("encode raft error: {e}")))?;
    Ok(RaftRpcResponse {
        result: Some(raft_rpc_response::Result::RaftError(bytes)),
    })
}

#[tonic::async_trait]
impl RaftTransport for RaftTransportService {
    async fn append_entries(
        &self,
        request: Request<RaftRpcRequest>,
    ) -> std::result::Result<Response<RaftRpcResponse>, Status> {
        let rpc = codec::decode(&request.into_inner().payload)
            .map_err(|e| Status::invalid_argument(format!("decode append_entries: {e}")))?;
        let env = match self.raft.append_entries(rpc).await {
            Ok(resp) => ok_envelope(&resp)?,
            Err(e) => err_envelope(&e)?,
        };
        Ok(Response::new(env))
    }

    async fn vote(
        &self,
        request: Request<RaftRpcRequest>,
    ) -> std::result::Result<Response<RaftRpcResponse>, Status> {
        let rpc = codec::decode(&request.into_inner().payload)
            .map_err(|e| Status::invalid_argument(format!("decode vote: {e}")))?;
        let env = match self.raft.vote(rpc).await {
            Ok(resp) => ok_envelope(&resp)?,
            Err(e) => err_envelope(&e)?,
        };
        Ok(Response::new(env))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftRpcRequest>,
    ) -> std::result::Result<Response<RaftRpcResponse>, Status> {
        let rpc = codec::decode(&request.into_inner().payload)
            .map_err(|e| Status::invalid_argument(format!("decode install_snapshot: {e}")))?;
        // `install_snapshot` returns RaftError<NID, InstallSnapshotError>.
        let env = match self.raft.install_snapshot(rpc).await {
            Ok(resp) => ok_envelope(&resp)?,
            Err(e) => err_envelope(&e)?,
        };
        Ok(Response::new(env))
    }
}

/// Bind a raft transport server on `addr` and serve until the process exits.
///
/// Spawns the tonic server on the current runtime and returns the bound
/// [`SocketAddr`] (resolved from `:0` ephemeral binds so the caller can
/// publish it to peers) together with a [`tokio::task::JoinHandle`]. The
/// handle is detached-friendly: dropping it does not stop the server, but
/// aborting it (used by the kill-a-node tests) tears the listener down.
///
/// Bind address precedence: explicit `addr` arg. For the env-driven path use
/// [`raft_bind_addr_from_env`] to resolve `BASIN_RAFT_BIND` first.
pub async fn serve_raft(
    service: RaftTransportService,
    addr: SocketAddr,
) -> Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    serve_raft_with_tls(service, addr, None).await
}

/// As [`serve_raft`], but applies mutual TLS when `tls` is `Some` (presents the
/// node identity and requires a CA-signed client cert; see [`RaftTlsConfig`]).
/// `None` ⇒ plaintext (the private-network default).
pub async fn serve_raft_with_tls(
    service: RaftTransportService,
    addr: SocketAddr,
    tls: Option<RaftTlsConfig>,
) -> Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    // Bind explicitly first so we can recover the OS-assigned port when the
    // caller passed `:0`.
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| BasinError::wal(format!("raft bind {addr}: {e}")))?;
    serve_raft_on_listener_with_tls(service, listener, tls)
}

/// As [`serve_raft`], but serves on an already-bound [`TcpListener`]. Tests
/// use this to bind a `:0` ephemeral port, learn its address, exchange it with
/// peers, and THEN start serving on the very same socket — eliminating the
/// drop-then-rebind race a bind-by-address API would have.
pub fn serve_raft_on_listener(
    service: RaftTransportService,
    listener: tokio::net::TcpListener,
) -> Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    serve_raft_on_listener_with_tls(service, listener, None)
}

/// As [`serve_raft_on_listener`], but applies mutual TLS when `tls` is `Some`.
/// `None` ⇒ plaintext.
pub fn serve_raft_on_listener_with_tls(
    service: RaftTransportService,
    listener: tokio::net::TcpListener,
    tls: Option<RaftTlsConfig>,
) -> Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let local_addr = listener
        .local_addr()
        .map_err(|e| BasinError::wal(format!("raft local_addr: {e}")))?;

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let server = service.into_server();

    // Build the tonic server, applying server-side mTLS up front so a config
    // error fails before the task is spawned (rather than logging a warn from
    // inside the detached task).
    let mut builder = tonic::transport::Server::builder();
    let secure = tls.is_some();
    if let Some(tls) = tls {
        builder = builder
            .tls_config(tls.server_tls())
            .map_err(|e| BasinError::wal(format!("raft transport server TLS config: {e}")))?;
    }

    let join = tokio::spawn(async move {
        if let Err(e) = builder
            .add_service(server)
            .serve_with_incoming(incoming)
            .await
        {
            tracing::warn!(error = %e, "raft transport server exited");
        }
    });

    if secure {
        tracing::info!(addr = %local_addr, "raft transport listening (mutual TLS)");
    } else {
        tracing::info!(addr = %local_addr, "raft transport listening (plaintext; private network assumed)");
    }
    Ok((local_addr, join))
}

/// Resolve the raft bind address from `BASIN_RAFT_BIND` (e.g.
/// `0.0.0.0:7001`). Returns `None` if unset (caller decides the default /
/// single-node behaviour); errors on a malformed value.
pub fn raft_bind_addr_from_env() -> Result<Option<SocketAddr>> {
    match std::env::var("BASIN_RAFT_BIND") {
        Ok(v) => {
            let trimmed = v.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            let addr = trimmed
                .parse::<SocketAddr>()
                .map_err(|e| BasinError::wal(format!("BASIN_RAFT_BIND '{trimmed}': {e}")))?;
            Ok(Some(addr))
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(BasinError::wal("BASIN_RAFT_BIND is not valid UTF-8"))
        }
    }
}

/// Convenience used by `Arc<RaftTransportService>`-shaped callers.
impl RaftTransportService {
    pub fn shared(raft: RaftHandle) -> Arc<Self> {
        Arc::new(Self::new(raft))
    }
}
