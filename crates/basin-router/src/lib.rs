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

mod copy;
mod error;
mod protocol;
mod rate_limit;
mod remote_shard;
mod resolver;
mod sharding;
pub mod test_cluster;
mod tls;
mod types;

pub use rate_limit::{from_env_qps, PgRateLimit, BURST_FACTOR, DEFAULT_SUSTAINED_QPS};
pub use resolver::{
    ApiKeyTenantResolver, JwtTenantResolver, StackedTenantResolver, StaticTenantResolver,
    TenantCredentialsResolver, TenantResolver,
};
pub use sharding::{parse_pins_env, ShardMap};
pub use tls::{build_acceptor, TlsConfig};

use crate::protocol::{
    BasinExtendedQueryHandler, BasinHandlers, BasinSimpleQueryHandlerSlot, BasinStartupHandler,
    EngineSessionFactory, PooledSessionFactory, SessionFactory,
};
use crate::remote_shard::RemoteShardSessionFactory;

/// Configuration for the pgwire server.
///
/// `pool` is optional. When `Some`, the per-connection session is acquired from
/// the pool (and returned to it on disconnect via `PooledSession::Drop`).
/// When `None`, the legacy `Engine::open_session` path runs unchanged so
/// deployments without a pool stay byte-for-byte identical.
///
/// `shard_endpoints` is optional. When `Some(vec)`, the router runs in
/// compute-sharded mode: every authenticated connection is mapped via
/// stable hashing of `TenantId` to one of the supplied endpoints, and
/// pgwire traffic is forwarded to the upstream basin-router listening at
/// that endpoint. The local `engine` and `pool` are unused in this mode
/// but must still be supplied (the field is part of the trait surface).
/// When `None`, behaviour is byte-identical to single-process Basin.
///
/// `tls` is optional. When `Some`, the listener answers `'S'` to a Postgres
/// `SSLRequest` and wraps the socket with the supplied acceptor before the
/// regular pgwire startup; `None` answers `'N'` and stays plaintext, which
/// matches pre-TLS behaviour byte-for-byte. See [`tls`] module docs.
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub engine: basin_engine::Engine,
    pub tenant_resolver: Arc<dyn TenantResolver>,
    pub pool: Option<Arc<basin_pool::SessionPool>>,
    pub shard_endpoints: Option<Vec<String>>,
    pub tls: Option<Arc<TlsConfig>>,
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
    let listener = TcpListener::bind(cfg.bind_addr)
        .await
        .map_err(|e| BasinError::Internal(format!("bind {} failed: {e}", cfg.bind_addr)))?;
    accept_loop(
        listener,
        cfg.engine,
        cfg.tenant_resolver,
        cfg.pool,
        cfg.shard_endpoints,
        cfg.tls,
        shutdown,
    )
    .await
}

/// Bind synchronously (so the caller can read `local_addr`), then spawn the
/// accept loop on a background task. Useful for integration tests that need
/// a `0.0.0.0:0` ephemeral port.
pub async fn run_until_bound(cfg: ServerConfig) -> Result<RunningServer> {
    let listener = TcpListener::bind(cfg.bind_addr)
        .await
        .map_err(|e| BasinError::Internal(format!("bind {} failed: {e}", cfg.bind_addr)))?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| BasinError::Internal(format!("local_addr: {e}")))?;
    let (tx, rx) = oneshot::channel();
    let engine = cfg.engine;
    let resolver = cfg.tenant_resolver;
    let pool = cfg.pool;
    let shard_endpoints = cfg.shard_endpoints;
    let tls = cfg.tls;
    let join = tokio::spawn(async move {
        accept_loop(listener, engine, resolver, pool, shard_endpoints, tls, rx).await
    });
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
    shard_endpoints: Option<Vec<String>>,
    tls: Option<Arc<TlsConfig>>,
    mut shutdown: oneshot::Receiver<()>,
) -> Result<()> {
    // Build the TlsAcceptor once at startup so every connection shares one
    // rustls ServerConfig (cheap clone on accept). Failure here is a hard
    // startup error so a bad cert is loud, not silently per-connection.
    let tls_acceptor = match tls.as_deref() {
        Some(cfg) => {
            let acceptor = build_acceptor(cfg)?;
            tracing::info!("pgwire TLS enabled");
            Some(Arc::new(acceptor))
        }
        None => None,
    };
    // Per-tenant pgwire rate limiter (Phase 6). Read once at startup so
    // the env var doesn't need to be re-parsed on every connection.
    // `0` / unset / empty = disabled. A typo (non-numeric) is a hard
    // startup error so the operator finds out immediately, not on the
    // first burst that should have been throttled.
    let rate_limit =
        match from_env_qps(std::env::var("BASIN_PGWIRE_RATE_LIMIT_QPS").ok().as_deref()) {
            Ok(rl) => rl.map(Arc::new),
            Err(e) => return Err(BasinError::Internal(e)),
        };
    if let Some(rl) = &rate_limit {
        tracing::info!(
            sustained_qps = rl.sustained_qps(),
            "pgwire rate limit enabled"
        );
    }
    // The session factory is selected once per `accept_loop`. We avoid making
    // `handle_connection` generic on two factory types by branching here and
    // letting each arm parameterise its own task. All factories produce a
    // pgwire-compatible `Session`, so the rest of the per-connection
    // plumbing is shared.
    //
    // Order of preference: shard mode > pool > local engine. Shard mode is
    // the explicit opt-in for compute sharding; if it's set, the engine and
    // pool are bypassed entirely.
    if let Some(endpoints) = shard_endpoints {
        if endpoints.is_empty() {
            return Err(BasinError::Internal(
                "shard_endpoints supplied but empty; need at least one endpoint".into(),
            ));
        }
        // Whale pinning (Phase 5.5): operator can override the consistent
        // hash for specific tenants via `BASIN_TENANT_PINS=ulid:idx,...`.
        // Parse failure is a hard startup error so a typo doesn't silently
        // route a whale onto the wrong (overloaded) shard.
        let pins = match std::env::var("BASIN_TENANT_PINS") {
            Ok(s) => parse_pins_env(&s)
                .map_err(|e| BasinError::Internal(format!("BASIN_TENANT_PINS: {e}")))?,
            Err(_) => Default::default(),
        };
        let map = Arc::new(ShardMap::with_pins(endpoints, pins).map_err(BasinError::Internal)?);
        tracing::info!(
            shards = map.endpoints().len(),
            pinned_tenants = map.pin_count(),
            "router running in compute-sharded mode"
        );
        let factory = Arc::new(RemoteShardSessionFactory::new(map));
        return run_accept_loop(
            listener,
            factory,
            resolver,
            rate_limit,
            tls_acceptor,
            &mut shutdown,
        )
        .await;
    }

    if let Some(pool) = pool {
        let factory = Arc::new(PooledSessionFactory::new(pool));
        return run_accept_loop(
            listener,
            factory,
            resolver,
            rate_limit,
            tls_acceptor,
            &mut shutdown,
        )
        .await;
    }

    let factory = Arc::new(EngineSessionFactory::new(engine));
    run_accept_loop(
        listener,
        factory,
        resolver,
        rate_limit,
        tls_acceptor,
        &mut shutdown,
    )
    .await
}

/// Inner accept loop, parameterised on a single concrete factory type.
/// Pulled out to share the loop body between the engine / pool / remote-shard
/// arms in `accept_loop` without making `handle_connection` generic over a
/// trait object.
async fn run_accept_loop<F>(
    listener: TcpListener,
    factory: Arc<F>,
    resolver: Arc<dyn TenantResolver>,
    rate_limit: Option<Arc<PgRateLimit>>,
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
    shutdown: &mut oneshot::Receiver<()>,
) -> Result<()>
where
    F: SessionFactory + 'static,
{
    loop {
        tokio::select! {
            _ = &mut *shutdown => {
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
                let rate_limit = rate_limit.clone();
                let tls_acceptor = tls_acceptor.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(sock, peer, factory, resolver, rate_limit, tls_acceptor).await {
                        tracing::warn!(error = %e, %peer, "connection ended with error");
                    }
                });
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
    rate_limit: Option<Arc<PgRateLimit>>,
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
) -> Result<()>
where
    F: SessionFactory + 'static,
{
    let slot = Arc::new(Mutex::new(None::<Arc<F::Session>>));
    let simple = Arc::new(BasinSimpleQueryHandlerSlot::new(
        slot.clone(),
        rate_limit.clone(),
    ));
    let copy_state = simple.copy_state.clone();
    let handlers = BasinHandlers {
        startup: Arc::new(BasinStartupHandler::new(factory, resolver, slot.clone())),
        simple,
        extended: Arc::new(BasinExtendedQueryHandler::new(slot, rate_limit, copy_state)),
    };
    // pgwire 0.28 owns the SSLRequest peek + 'S'/'N' response + TLS wrap when
    // a TlsAcceptor is supplied; `None` keeps the pre-TLS plaintext path.
    pgwire::tokio::process_socket(sock, tls_acceptor, handlers)
        .await
        .map_err(|e| BasinError::Internal(format!("pgwire: {e}")))
}
