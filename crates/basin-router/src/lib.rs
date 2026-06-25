//! `basin-router` — pgwire v3 front-end for the Basin PoC.
//!
//! Stands up a TCP listener that speaks the Postgres simple-query protocol,
//! authenticates each connection to a single `ProjectId`, and runs SQL against
//! that project's `basin_engine::ProjectSession` for the lifetime of the
//! connection.
//!
//! ## What's implemented
//!
//! - Startup + cleartext-password authentication. The password is **accepted
//!   unconditionally** — any non-empty password is fine. This is documented
//!   here intentionally because nothing about it is appropriate for
//!   production. Production deployments will replace `BasinStartupHandler`
//!   with one that delegates to a real auth source.
//! - Username -> `ProjectId` resolution via the pluggable [`ProjectResolver`]
//!   trait. The default [`StaticProjectResolver`] is a `HashMap` lookup.
//! - **Simple query protocol** (what `psql` uses for `SELECT 1` before
//!   switching to prepared statements).
//! - **Extended query protocol**: `Parse`/`Bind`/`Describe`/`Execute`/
//!   `Close`/`Sync` against the engine's prepared-statement API. Unblocks
//!   `tokio_postgres::query`, `asyncpg`, JDBC, and every popular ORM that
//!   defaults to extended protocol. Per-column result format codes from the
//!   `Bind` message are respected: binary is emitted when the client requests
//!   format code 1, text otherwise.
//! - Arrow -> Postgres wire encoding for the common scalar types: int8, text,
//!   bool, float8, timestamp, date, interval, bytea, JSONB, UUID, arrays, and
//!   NUMERIC (both text and binary). Anything else falls through to TEXT with
//!   a debug-formatted body.
//!
//! ## Out of scope
//!
//! - Transactions (auto-commit only in v1).
//! - In-band project switching. The connection's project is fixed at startup;
//!   any SQL trying to change it (e.g. `SET project TO ...`) routes through
//!   the engine, which will reject it.
//!
//! ## Public API
//!
//! - [`ServerConfig`]
//! - [`ProjectResolver`], [`StaticProjectResolver`]
//! - [`run`], [`run_with_shutdown`], [`run_until_bound`]
//! - [`RunningServer`]

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_common::{BasinError, Result};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Mutex};

/// Per-connection activity clock.
///
/// Every time the pgwire handlers process a frontend message (startup,
/// simple/extended query, COPY data) they call [`Activity::touch`]. The
/// idle watchdog in [`handle_connection`] reads [`Activity::idle`] to decide
/// whether a connection has gone silent for longer than the idle ceiling.
///
/// This is the application-level backstop for the connection-ceiling leak:
/// TCP keepalive tears down a *directly* dead socket, but when an L4 proxy
/// sits in front of the engine (Fly's edge) the proxy↔engine TCP hop stays
/// healthy even after the real client vanishes, so keepalive never fires and
/// the blocked `process_socket` read would pin the project's connection slot
/// forever. The watchdog forces the session task to exit, which drops the
/// `ConnectionGuard` and releases the slot. The ceiling is generous so a
/// long-idle but *healthy* pooled connection is never killed.
#[derive(Clone, Debug)]
pub(crate) struct Activity {
    /// Millis since the shared `start` baseline of the most recent touch.
    last: Arc<AtomicU64>,
    start: Instant,
}

impl Activity {
    fn new() -> Self {
        Self {
            last: Arc::new(AtomicU64::new(0)),
            start: Instant::now(),
        }
    }

    /// Record activity at "now". Cheap (one relaxed atomic store).
    pub(crate) fn touch(&self) {
        let elapsed = self.start.elapsed().as_millis() as u64;
        self.last.store(elapsed, Ordering::Relaxed);
    }

    /// How long since the last [`touch`](Self::touch).
    fn idle(&self) -> Duration {
        let last = self.last.load(Ordering::Relaxed);
        let now = self.start.elapsed().as_millis() as u64;
        Duration::from_millis(now.saturating_sub(last))
    }
}

/// Idle ceiling for an authenticated pgwire connection, in seconds.
///
/// A connection that processes no frontend message for this long is assumed
/// dead (its client gone behind a proxy that keeps the engine-side TCP open)
/// and is torn down so its per-project connection slot is released. The
/// default is deliberately generous — 30 minutes — so a healthy connection
/// pooler that parks idle connections between bursts is never reaped; only a
/// genuinely abandoned socket trips it. Override with
/// `BASIN_PGWIRE_IDLE_TIMEOUT_SECS`; `0` disables the watchdog entirely
/// (keepalive remains the only backstop).
fn idle_timeout_from_env() -> Option<Duration> {
    let secs = std::env::var("BASIN_PGWIRE_IDLE_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1800);
    if secs == 0 {
        None
    } else {
        Some(Duration::from_secs(secs))
    }
}

mod connection_limit;
mod copy;
mod error;
mod protocol;
mod rate_limit;
mod remote_shard;
/// ADR 0028 Phase 5 — Postgres logical-replication command path
/// (IDENTIFY_SYSTEM / CREATE_REPLICATION_SLOT / START_REPLICATION). Flagged
/// off by default (`BASIN_PGWIRE_REPLICATION`).
pub mod replication;
mod resolver;
mod sharding;
pub mod test_cluster;
mod tls;
mod types;

pub use connection_limit::{
    CatalogConnectionLimitProvider, ConnectionGuard, ConnectionLimitProvider, ConnectionLimiter,
    NoConnectionLimit,
};
/// Multi-region v0.2: edge helper that turns a [`basin_common::BasinError::ForwardReplay`]
/// control signal into the value of the Fly `fly-replay` response header.
pub use error::fly_replay_directive;
pub use protocol::{COMMIT_CONFLICT_EXHAUSTED_COUNT, COMMIT_CONFLICT_RETRY_COUNT};
pub use rate_limit::{from_env_qps, PgRateLimit, BURST_FACTOR, DEFAULT_SUSTAINED_QPS};
pub use resolver::{
    ApiKeyProjectResolver, JwtProjectResolver, ProjectCredentialsResolver, ProjectResolver,
    StackedProjectResolver, StaticProjectResolver,
};
pub use sharding::{
    parse_lease_cache_ttl_env, parse_pins_env, LeaseAwareShardMap, ShardMap,
    DEFAULT_LEASE_CACHE_TTL,
};
pub use tls::{build_acceptor, TlsConfig};

use crate::protocol::{
    BasinExtendedQueryHandler, BasinHandlers, BasinSimpleQueryHandlerSlot, BasinStartupHandler,
    EngineSessionFactory, PooledSessionFactory, SessionFactory,
};
use crate::remote_shard::RemoteShardSessionFactory;

// ---------------------------------------------------------------------------
// Test-accessible helpers for the numeric binary codec tests.
//
// These thin wrappers expose `pub(crate)` functions from `types.rs` to the
// integration tests in `tests/numeric_binary_codec.rs` without adding them
// to the main public API.
// ---------------------------------------------------------------------------

/// Exposed for `tests/numeric_binary_codec.rs` only.
/// Delegates to `types::encode_batches_with_formats`.
#[doc(hidden)]
pub fn encode_batches_with_formats_for_test(
    schema: &std::sync::Arc<arrow_schema::Schema>,
    batches: &[arrow_array::RecordBatch],
    format_codes: &[i16],
) -> basin_common::Result<Vec<pgwire::messages::data::DataRow>> {
    crate::types::encode_batches_with_formats(schema, batches, format_codes)
}

/// Exposed for `tests/numeric_binary_codec.rs` only.
/// Delegates to `types::encode_batches` (text-only path).
#[doc(hidden)]
pub fn encode_batches_text_for_test(
    schema: &std::sync::Arc<arrow_schema::Schema>,
    batches: &[arrow_array::RecordBatch],
) -> Vec<pgwire::messages::data::DataRow> {
    crate::types::encode_batches(schema, batches)
}

/// Configuration for the pgwire server.
///
/// `pool` is optional. When `Some`, the per-connection session is acquired from
/// the pool (and returned to it on disconnect via `PooledSession::Drop`).
/// When `None`, the legacy `Engine::open_session` path runs unchanged so
/// deployments without a pool stay byte-for-byte identical.
///
/// `shard_endpoints` is optional. When `Some(vec)`, the router runs in
/// compute-sharded mode: every authenticated connection is mapped via
/// stable hashing of `ProjectId` to one of the supplied endpoints, and
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
    pub project_resolver: Arc<dyn ProjectResolver>,
    pub pool: Option<Arc<basin_pool::SessionPool>>,
    pub shard_endpoints: Option<Vec<String>>,
    pub tls: Option<Arc<TlsConfig>>,
    /// Per-project connection ceiling enforcement.
    ///
    /// `None` → unlimited (equivalent to `Some(Arc::new(ConnectionLimiter::unlimited()))`).
    /// Pass a `ConnectionLimiter` wired to a [`ConnectionLimitProvider`] that
    /// reads the project's `max_connections` from the control plane to enable
    /// enforcement. The limit is resolved dynamically on each new connection
    /// (not cached lifetime-wide), so a plan upgrade propagates to new
    /// connections without a restart.
    pub connection_limiter: Option<Arc<ConnectionLimiter>>,
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
        cfg.project_resolver,
        cfg.pool,
        cfg.shard_endpoints,
        cfg.tls,
        cfg.connection_limiter,
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
    let resolver = cfg.project_resolver;
    let pool = cfg.pool;
    let shard_endpoints = cfg.shard_endpoints;
    let tls = cfg.tls;
    let connection_limiter = cfg.connection_limiter;
    let join = tokio::spawn(async move {
        accept_loop(
            listener,
            engine,
            resolver,
            pool,
            shard_endpoints,
            tls,
            connection_limiter,
            rx,
        )
        .await
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
    resolver: Arc<dyn ProjectResolver>,
    pool: Option<Arc<basin_pool::SessionPool>>,
    shard_endpoints: Option<Vec<String>>,
    tls: Option<Arc<TlsConfig>>,
    connection_limiter: Option<Arc<ConnectionLimiter>>,
    mut shutdown: oneshot::Receiver<()>,
) -> Result<()> {
    // Resolve the idle ceiling once at startup from the environment
    // (`BASIN_PGWIRE_IDLE_TIMEOUT_SECS`, default 30 min, `0` disables) so it is
    // not re-parsed on every accept.
    let idle_ceiling = idle_timeout_from_env();
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
    // Per-project pgwire rate limiter (Phase 6). Read once at startup so
    // the env var doesn't need to be re-parsed on every connection.
    // `0` / unset / empty = disabled. A typo (non-numeric) is a hard
    // startup error so the operator finds out immediately, not on the
    // first burst that should have been throttled.
    let rate_limit =
        match from_env_qps(std::env::var("BASIN_PGWIRE_RATE_LIMIT_QPS").ok().as_deref()) {
            // Attach the catalog so the limiter resolves per-project (per-tier)
            // quotas lazily from `get_project_rate_limit_qps` — the global env
            // qps becomes the default; cloud-pushed per-project values override.
            Ok(rl) => rl.map(|l| Arc::new(l.with_catalog(engine.config().catalog.clone()))),
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
        // hash for specific projects via `BASIN_PROJECT_PINS=ulid:idx,...`.
        // Parse failure is a hard startup error so a typo doesn't silently
        // route a whale onto the wrong (overloaded) shard.
        let pins = match std::env::var("BASIN_PROJECT_PINS") {
            Ok(s) => parse_pins_env(&s)
                .map_err(|e| BasinError::Internal(format!("BASIN_PROJECT_PINS: {e}")))?,
            Err(_) => Default::default(),
        };
        let map = Arc::new(ShardMap::with_pins(endpoints, pins).map_err(BasinError::Internal)?);
        tracing::info!(
            shards = map.endpoints().len(),
            pinned_projects = map.pin_count(),
            "router running in compute-sharded mode"
        );
        let factory = Arc::new(RemoteShardSessionFactory::new(map));
        return run_accept_loop(
            listener,
            factory,
            resolver,
            rate_limit,
            tls_acceptor,
            connection_limiter,
            idle_ceiling,
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
            connection_limiter,
            idle_ceiling,
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
        connection_limiter,
        idle_ceiling,
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
    resolver: Arc<dyn ProjectResolver>,
    rate_limit: Option<Arc<PgRateLimit>>,
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
    connection_limiter: Option<Arc<ConnectionLimiter>>,
    idle_ceiling: Option<Duration>,
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
                // Harden long-lived pgwire connections: TCP keepalive stops an
                // L4 proxy in front of the engine (e.g. Fly's edge) from reaping
                // a connection that's quiet between statements, and TCP_NODELAY
                // removes Nagle latency on the small request/response messages a
                // SQL session exchanges. Best-effort — a socket that rejects an
                // option is still usable, so failures only log at debug.
                tune_pgwire_socket(&sock, peer);
                let factory = factory.clone();
                let resolver = resolver.clone();
                let rate_limit = rate_limit.clone();
                let tls_acceptor = tls_acceptor.clone();
                let connection_limiter = connection_limiter.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(sock, peer, factory, resolver, rate_limit, tls_acceptor, connection_limiter, idle_ceiling).await {
                        tracing::warn!(error = %e, %peer, "connection ended with error");
                    }
                });
            }
        }
    }
}

/// Apply TCP_NODELAY + SO_KEEPALIVE (with a 30s idle / 10s interval probe
/// schedule) to a freshly-accepted pgwire socket. Keepalive is what prevents an
/// L4 proxy ahead of the engine (Fly's edge, a cloud LB) from silently dropping
/// a SQL connection that goes quiet between statements; NODELAY trims Nagle
/// latency on the small messages pgwire exchanges. All best-effort: a platform
/// that rejects an option leaves the socket usable, so we log at debug and move
/// on rather than fail the connection.
fn tune_pgwire_socket(sock: &tokio::net::TcpStream, peer: SocketAddr) {
    if let Err(e) = sock.set_nodelay(true) {
        tracing::debug!(error = %e, %peer, "set_nodelay failed");
    }
    let sref = socket2::SockRef::from(sock);
    // 30s idle before the first probe, 10s between probes, give up after 6
    // unanswered probes. `with_retries` pins the probe count instead of
    // inheriting the host's `tcp_keepalive_probes` (often 9), so a *directly*
    // dead socket is torn down deterministically (~90s) on any platform —
    // unblocking the session read and dropping its ConnectionGuard. (The proxy-
    // held-socket case, where keepalive can't help, is covered by the
    // application idle watchdog in `handle_connection`.)
    let ka = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(30))
        .with_interval(std::time::Duration::from_secs(10))
        .with_retries(6);
    if let Err(e) = sref.set_tcp_keepalive(&ka) {
        tracing::debug!(error = %e, %peer, "set_tcp_keepalive failed");
    }
}

#[tracing::instrument(skip_all, fields(peer = %peer))]
async fn handle_connection<F>(
    sock: tokio::net::TcpStream,
    peer: SocketAddr,
    factory: Arc<F>,
    resolver: Arc<dyn ProjectResolver>,
    rate_limit: Option<Arc<PgRateLimit>>,
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
    connection_limiter: Option<Arc<ConnectionLimiter>>,
    idle_ceiling: Option<Duration>,
) -> Result<()>
where
    F: SessionFactory + 'static,
{
    let activity = Activity::new();
    activity.touch();
    let slot = Arc::new(Mutex::new(None::<Arc<F::Session>>));
    let simple = Arc::new(BasinSimpleQueryHandlerSlot::new(
        slot.clone(),
        rate_limit.clone(),
        activity.clone(),
    ));
    let copy_state = simple.copy_state.clone();
    let handlers = BasinHandlers {
        startup: Arc::new(BasinStartupHandler::new(
            factory,
            resolver,
            slot.clone(),
            connection_limiter,
            activity.clone(),
        )),
        simple,
        extended: Arc::new(BasinExtendedQueryHandler::new(
            slot,
            rate_limit,
            copy_state,
            activity.clone(),
        )),
    };
    // pgwire 0.28 owns the SSLRequest peek + 'S'/'N' response + TLS wrap when
    // a TlsAcceptor is supplied; `None` keeps the pre-TLS plaintext path.
    //
    // Wrap the session in an idle watchdog so a silently-dead client (whose
    // engine-side TCP a proxy keeps open, defeating keepalive) cannot pin the
    // project's connection-ceiling slot forever. When the watchdog wins the
    // select, `process_socket` is dropped, which drops the handlers — and with
    // them the `ConnectionGuard` — releasing the slot. The ceiling is generous
    // (default 30 min) so a healthy long-idle pooled connection survives.
    let serve = pgwire::tokio::process_socket(sock, tls_acceptor, handlers);
    match idle_ceiling {
        None => serve
            .await
            .map_err(|e| BasinError::Internal(format!("pgwire: {e}"))),
        Some(ceiling) => {
            tokio::pin!(serve);
            // Probe at a fraction of the ceiling so detection lag is bounded
            // (a connection is reaped within one tick of crossing the ceiling)
            // without spinning a hot timer.
            let tick = (ceiling / 4).max(Duration::from_secs(1));
            loop {
                tokio::select! {
                    res = &mut serve => {
                        return res.map_err(|e| BasinError::Internal(format!("pgwire: {e}")));
                    }
                    _ = tokio::time::sleep(tick) => {
                        if activity.idle() >= ceiling {
                            tracing::info!(
                                %peer,
                                idle_secs = activity.idle().as_secs(),
                                "pgwire connection idle past ceiling; reaping to release slot",
                            );
                            // Dropping `serve` here (loop exit) drops the
                            // handlers and the ConnectionGuard with them.
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}
