//! `basin-router` — pgwire v3 front-end for the Basin PoC.
//!
//! Stands up a TCP listener that speaks the Postgres simple-query protocol,
//! authenticates each connection to a single `TenantId`, and runs SQL against
//! that tenant's `basin_engine::TenantSession` for the lifetime of the
//! connection.
//!
//! ## What's implemented
//!
//! - Startup + cleartext-password authentication. The password is **accepted
//!   unconditionally** — any non-empty password is fine. This is documented
//!   here intentionally because nothing about it is appropriate for
//!   production. Production deployments will replace `BasinStartupHandler`
//!   with one that delegates to a real auth source.
//! - Username -> `TenantId` resolution via the pluggable [`TenantResolver`]
//!   trait. The default [`StaticTenantResolver`] is a `HashMap` lookup.
//! - **Simple query protocol** (what `psql` uses for `SELECT 1` before
//!   switching to prepared statements).
//! - **Extended query protocol** v1: `Parse`/`Bind`/`Describe`/`Execute`/
//!   `Close`/`Sync` against the engine's prepared-statement API. Unblocks
//!   `tokio_postgres::query`, `asyncpg`, JDBC, and every popular ORM that
//!   defaults to extended protocol. Both parameter binding and result rows
//!   use Postgres text format; v2 will add binary.
//! - Arrow -> Postgres text-format encoding for a small set of types: int8,
//!   text, bool, float8, and timestamp (rendered RFC3339, UTC). Anything
//!   else falls through to TEXT with a debug-formatted body.
//!
//! ## Out of scope
//!
//! - Transactions, `COPY`, binary format codes.
//! - In-band tenant switching. The connection's tenant is fixed at startup;
//!   any SQL trying to change it (e.g. `SET tenant TO ...`) routes through
//!   the engine, which will reject it.
//!
//! ## Public API
//!
//! - [`ServerConfig`]
//! - [`TenantResolver`], [`StaticTenantResolver`]
//! - [`run`], [`run_with_shutdown`], [`run_until_bound`]
//! - [`RunningServer`]

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::{BasinError, Result};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Mutex};

mod error;
mod protocol;
mod resolver;
mod types;

pub use resolver::{JwtTenantResolver, StackedTenantResolver, StaticTenantResolver, TenantResolver};

use crate::protocol::{
    BasinExtendedQueryHandler, BasinHandlers, BasinSimpleQueryHandlerSlot, BasinStartupHandler,
    EngineSessionFactory, PooledSessionFactory, SessionFactory,
};

/// Configuration for the pgwire server.
///
/// `pool` is optional. When `Some`, the per-connection session is acquired from
/// the pool (and returned to it on disconnect via `PooledSession::Drop`).
/// When `None`, the legacy `Engine::open_session` path runs unchanged so
/// deployments without a pool stay byte-for-byte identical.
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub engine: basin_engine::Engine,
    pub tenant_resolver: Arc<dyn TenantResolver>,
    pub pool: Option<Arc<basin_pool::SessionPool>>,
}

/// Bind, listen, accept until the process is killed.
pub async fn run(cfg: ServerConfig) -> Result<()> {
    let (_tx, rx) = oneshot::channel();
    // Hold tx so the receiver never fires. We drop tx at the end of the
    // function (i.e. never until the listener loop exits on its own).
    run_with_shutdown(cfg, rx).await
}

/// Bind, listen, accept until either the process is killed or `shutdown`
/// fires. A fired shutdown stops accepting new connections; in-flight
/// connections proceed to completion as their tasks finish.
pub async fn run_with_shutdown(cfg: ServerConfig, shutdown: oneshot::Receiver<()>) -> Result<()> {
    let listener = TcpListener::bind(cfg.bind_addr).await.map_err(|e| {
        BasinError::Internal(format!("bind {} failed: {e}", cfg.bind_addr))
    })?;
    accept_loop(listener, cfg.engine, cfg.tenant_resolver, cfg.pool, shutdown).await
}

/// Bind synchronously (so the caller can read `local_addr`), then spawn the
/// accept loop on a background task. Useful for integration tests that need
/// a `0.0.0.0:0` ephemeral port.
pub async fn run_until_bound(cfg: ServerConfig) -> Result<RunningServer> {
    let listener = TcpListener::bind(cfg.bind_addr).await.map_err(|e| {
        BasinError::Internal(format!("bind {} failed: {e}", cfg.bind_addr))
    })?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| BasinError::Internal(format!("local_addr: {e}")))?;
    let (tx, rx) = oneshot::channel();
    let engine = cfg.engine;
    let resolver = cfg.tenant_resolver;
    let pool = cfg.pool;
    let join =
        tokio::spawn(async move { accept_loop(listener, engine, resolver, pool, rx).await });
    Ok(RunningServer {
        local_addr,
        shutdown: tx,
        join,
    })
}

/// Handle returned by [`run_until_bound`]. Drop the `shutdown` sender, or send
/// `()` through it, to tell the accept loop to exit.
pub struct RunningServer {
    pub local_addr: SocketAddr,
    pub shutdown: oneshot::Sender<()>,
    pub join: tokio::task::JoinHandle<Result<()>>,
}

async fn accept_loop(
    listener: TcpListener,
    engine: basin_engine::Engine,
    resolver: Arc<dyn TenantResolver>,
    pool: Option<Arc<basin_pool::SessionPool>>,
    mut shutdown: oneshot::Receiver<()>,
) -> Result<()> {
    // The session factory is selected once per `accept_loop`. We avoid making
    // `handle_connection` generic on two factory types by branching here and
    // letting each arm parameterise its own task. Both factories produce the
    // same engine `Session`, so the rest of the per-connection plumbing is
    // shared.
    if let Some(pool) = pool {
        let factory = Arc::new(PooledSessionFactory::new(pool));
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("router shutdown signaled");
                    return Ok(());
                }
                res = listener.accept() => {
                    let (sock, peer) = match res {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::warn!(error = %e, "accept failed");
                            continue;
                        }
                    };
                    let factory = factory.clone();
                    let resolver = resolver.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(sock, peer, factory, resolver).await {
                            tracing::warn!(error = %e, %peer, "connection ended with error");
                        }
                    });
                }
            }
        }
    } else {
        let factory = Arc::new(EngineSessionFactory(engine));
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("router shutdown signaled");
                    return Ok(());
                }
                res = listener.accept() => {
                    let (sock, peer) = match res {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::warn!(error = %e, "accept failed");
                            continue;
                        }
                    };
                    let factory = factory.clone();
                    let resolver = resolver.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(sock, peer, factory, resolver).await {
                            tracing::warn!(error = %e, %peer, "connection ended with error");
                        }
                    });
                }
            }
        }
    }
}

#[tracing::instrument(skip_all, fields(peer = %peer))]
async fn handle_connection<F>(
    sock: tokio::net::TcpStream,
    peer: SocketAddr,
    factory: Arc<F>,
    resolver: Arc<dyn TenantResolver>,
) -> Result<()>
where
    F: SessionFactory + 'static,
{
    let slot = Arc::new(Mutex::new(None::<Arc<F::Session>>));
    let handlers = BasinHandlers {
        startup: Arc::new(BasinStartupHandler::new(
            factory,
            resolver,
            slot.clone(),
        )),
        simple: Arc::new(BasinSimpleQueryHandlerSlot { slot: slot.clone() }),
        extended: Arc::new(BasinExtendedQueryHandler::new(slot)),
    };
    pgwire::tokio::process_socket(sock, None, handlers)
        .await
        .map_err(|e| BasinError::Internal(format!("pgwire: {e}")))
}
