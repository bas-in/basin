//! `basin-server` — single-process Basin proof-of-concept.
//!
//! Wires `basin-storage` + `basin-catalog` + `basin-engine` + `basin-router`
//! into one TCP listener that speaks pgwire. Configuration is environment-
//! driven so the PoC can be exercised by:
//!
//! ```text
//! BASIN_DATA_DIR=/tmp/basin BASIN_BIND=127.0.0.1:5433 \
//! BASIN_TENANTS=alice=01HABCD..,bob=01HEFGH.. \
//! cargo run -p basin-server
//! ```
//!
//! `BASIN_TENANTS` is a comma-separated list of `user=tenant_id_ulid` pairs.
//! For convenience, an entry of the form `user=*` allocates a fresh tenant id
//! at startup and prints it to stderr. This is PoC-grade only — production
//! tenant provisioning lives in the (not-yet-built) control plane.
//!
//! Catalog backend is selected via `BASIN_CATALOG`:
//!
//! ```text
//! BASIN_CATALOG=memory                                  # default; volatile
//! BASIN_CATALOG=postgres://pc@127.0.0.1:5432/postgres   # durable, persists across restarts
//! BASIN_CATALOG_SCHEMA=basin_catalog                    # optional, default = basin_catalog
//! ```
//!
//! ## WAL + shard owner
//!
//! Two env vars gate the new WAL-acked write path:
//!
//! ```text
//! BASIN_SHARD_ENABLED=1     # default 0; when 1, INSERTs route through basin-shard
//! BASIN_WAL_DIR=/tmp/wal    # default ${BASIN_DATA_DIR}/wal
//! ```
//!
//! When `BASIN_SHARD_ENABLED=1`, the server opens a `basin-wal::Wal` rooted at
//! `BASIN_WAL_DIR`, constructs a `basin-shard::Shard` over it, and hands a clone
//! into `EngineConfig::shard`. The shard's background eviction + compaction
//! loops are spawned and shut down cleanly on Ctrl-C. With the flag unset the
//! engine falls back to its legacy synchronous Parquet write path so existing
//! demos remain reproducible.
//!
//! ## Pool, auth, REST
//!
//! Three additional env vars layer the optional pieces of the Basin stack on
//! top of the pgwire baseline:
//!
//! ```text
//! BASIN_POOL_ENABLED=1                     # default 0; route pgwire sessions through basin-pool
//! BASIN_AUTH_ENABLED=1                     # default 0; start basin-auth (requires BASIN_AUTH_* env)
//! BASIN_REST_ENABLED=1                     # default 0; start basin-rest (REQUIRES BASIN_AUTH_ENABLED=1)
//! BASIN_REST_BIND=127.0.0.1:5434           # rest server bind addr; default 127.0.0.1:5434
//! ```
//!
//! ## Analytical (DuckDB) path
//!
//! ```text
//! BASIN_ANALYTICAL_ENABLED=1               # default 0; build basin-analytical and let basin-engine route
//! ```
//!
//! When set, the server constructs a `basin_analytical::AnalyticalEngine`
//! over the same `Storage` + `Catalog` the OLTP path uses and attaches it to
//! the engine via `Engine::with_analytical`. The engine's planner heuristic
//! (`crates/basin-engine/src/analytical_route.rs`) then forwards aggregate
//! and GROUP BY queries — and any query carrying a `/*+ analytical */`
//! hint — to DuckDB, falling back to DataFusion on any execution error.
//! Local-FS only in v0.1; S3/HTTPFS lands with the analytical engine's v0.2.
//!
//! `BASIN_REST_ENABLED=1` *requires* `BASIN_AUTH_ENABLED=1` per ADR 0006 — a
//! REST stack without auth is the largest data-leak class we know how to
//! ship, so the binary refuses to start in that combination.
//!
//! Default behaviour with none of these set is unchanged: a single pgwire
//! listener with `StaticTenantResolver`, no pool, no REST, and no auth side
//! channel.
//!
//! ## Pgwire username convention with auth
//!
//! When `BASIN_AUTH_ENABLED=1`, the pgwire `user` parameter doubles as a
//! bearer-token slot: clients can connect with `user=<jwt>` (optionally
//! `user=Bearer <jwt>`) and the server resolves the embedded `tenant_id`
//! claim through `JwtTenantResolver`. Static `BASIN_TENANTS` mappings such
//! as `alice=*` keep working as a fallback: the binary stacks the JWT
//! resolver in front of the static one, so dev clients and JWT clients
//! coexist on the same listener.

// mimalloc as the global allocator. Allocator-heavy hot paths in basin-server
// (Parquet decode in basin-storage::reader, Arrow batch construction in
// basin-engine, JSON/pgwire response encoding in basin-router) all benefit
// measurably from a faster malloc; mimalloc is a one-line drop-in with no
// other code changes required.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use basin_common::{
    telemetry::{init, LogFormat},
    BasinError, TenantId,
};
use basin_router::{
    ApiKeyTenantResolver, JwtTenantResolver, ServerConfig, StaticTenantResolver, TenantResolver,
    TlsConfig,
};
use object_store::local::LocalFileSystem;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = init(tracing::Level::INFO, LogFormat::Pretty);

    let cfg = Cfg::from_env()?;
    tracing::info!(
        bind = %cfg.bind,
        data_dir = %cfg.data_dir.display(),
        tenants = cfg.tenants.len(),
        shard_enabled = cfg.shard_enabled,
        pool_enabled = cfg.pool_enabled,
        auth_enabled = cfg.auth_enabled,
        rest_enabled = cfg.rest_enabled,
        analytical_enabled = cfg.analytical_enabled,
        "starting basin-server"
    );

    std::fs::create_dir_all(&cfg.data_dir)
        .with_context(|| format!("create data dir {}", cfg.data_dir.display()))?;
    let fs = LocalFileSystem::new_with_prefix(&cfg.data_dir)
        .with_context(|| format!("LocalFileSystem at {}", cfg.data_dir.display()))?;

    // Production cache defaults: disk + page cache ON unless explicitly
    // disabled. Knobs (in priority order, highest wins):
    //
    //   BASIN_DISK_CACHE_ROOT       (path; default <XDG_CACHE_HOME or ~/.cache>/basin/disk-cache)
    //   BASIN_DISK_CACHE_MAX_BYTES  (u64; default 10 GiB — see StorageConfig::DEFAULT_DISK_CACHE_BYTES)
    //   BASIN_PAGE_CACHE_MAX_BYTES  (u64; default 1 GiB — see StorageConfig::DEFAULT_PAGE_CACHE_BYTES)
    //
    // Setting `BASIN_DISK_CACHE_MAX_BYTES=0` disables the disk cache;
    // `BASIN_PAGE_CACHE_MAX_BYTES=0` disables the page cache.
    let disk_cache_root: PathBuf = std::env::var("BASIN_DISK_CACHE_ROOT")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let base = std::env::var("XDG_CACHE_HOME")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .map(PathBuf::from)
                .or_else(|| {
                    std::env::var("HOME")
                        .ok()
                        .map(|h| PathBuf::from(h).join(".cache"))
                })
                .unwrap_or_else(|| PathBuf::from("/tmp"));
            base.join("basin").join("disk-cache")
        });
    let disk_cache_max_bytes: u64 = std::env::var("BASIN_DISK_CACHE_MAX_BYTES")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(basin_storage::StorageConfig::DEFAULT_DISK_CACHE_BYTES);
    let page_cache_max_bytes: u64 = std::env::var("BASIN_PAGE_CACHE_MAX_BYTES")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(basin_storage::StorageConfig::DEFAULT_PAGE_CACHE_BYTES);

    let disk_cache = if disk_cache_max_bytes == 0 {
        None
    } else {
        if let Err(e) = std::fs::create_dir_all(&disk_cache_root) {
            tracing::warn!(
                target = "basin_server",
                error = %e,
                path = %disk_cache_root.display(),
                "disk_cache: cannot create root; cache will be disabled",
            );
            None
        } else {
            Some(basin_storage::DiskCacheConfig::new(
                disk_cache_root.clone(),
                disk_cache_max_bytes,
            ))
        }
    };
    let page_cache = if page_cache_max_bytes == 0 {
        None
    } else {
        Some(basin_storage::PageCacheConfig::new(page_cache_max_bytes))
    };
    tracing::info!(
        disk_cache_enabled = disk_cache.is_some(),
        disk_cache_root = %disk_cache_root.display(),
        disk_cache_max_bytes,
        page_cache_enabled = page_cache.is_some(),
        page_cache_max_bytes,
        "storage cache configuration",
    );

    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache,
        page_cache,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = match &cfg.catalog {
        CatalogBackend::Memory => {
            tracing::info!("catalog backend: in-memory (volatile)");
            Arc::new(basin_catalog::InMemoryCatalog::new())
        }
        CatalogBackend::Postgres { url, schema } => {
            tracing::info!(%schema, "catalog backend: postgres (durable)");
            let cat = basin_catalog::PostgresCatalog::connect_with_schema(url, schema)
                .await
                .with_context(|| format!("connect postgres catalog at {url}"))?;
            Arc::new(cat)
        }
    };

    // Optional WAL + shard owner. Constructed when BASIN_SHARD_ENABLED=1 so we
    // can ship the wedge-deepening change incrementally without breaking demos
    // that don't have a writable WAL directory available.
    let mut shard_handles: Option<(
        basin_shard::Shard,
        basin_shard::ShardBackgroundHandle,
        Arc<dyn basin_wal::Wal>,
    )> = None;
    let shard_for_engine: Option<basin_shard::Shard> = if cfg.shard_enabled {
        std::fs::create_dir_all(&cfg.wal_dir)
            .with_context(|| format!("create WAL dir {}", cfg.wal_dir.display()))?;
        let wal_fs = LocalFileSystem::new_with_prefix(&cfg.wal_dir)
            .with_context(|| format!("WAL LocalFileSystem at {}", cfg.wal_dir.display()))?;
        let wal: Arc<dyn basin_wal::Wal> = Arc::new(
            basin_wal::LocalWal::open(basin_wal::WalConfig {
                object_store: Arc::new(wal_fs),
                root_prefix: None,
                flush_interval: std::time::Duration::from_millis(200),
                flush_max_bytes: 1024 * 1024,
            })
            .await
            .context("open WAL")?,
        );
        let shard = basin_shard::Shard::new(basin_shard::ShardConfig::new(
            storage.clone(),
            catalog.clone(),
            wal.clone(),
        ));
        let bg = shard.spawn_background();
        tracing::info!(
            wal_dir = %cfg.wal_dir.display(),
            "shard owner enabled; INSERTs will route through WAL + compactor"
        );
        let to_engine = shard.clone();
        shard_handles = Some((shard, bg, wal));
        Some(to_engine)
    } else {
        None
    };

    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: shard_for_engine,
    });

    // Optional analytical (DuckDB) path. Wired in via `with_analytical` so
    // the basic `EngineConfig` literal stays byte-stable across other
    // crates (basin-router, basin-rest, basin-pool) that build engines
    // without analytical support.
    let engine = if cfg.analytical_enabled {
        let analytical_cfg = basin_analytical::AnalyticalConfig {
            storage: storage.clone(),
            catalog: catalog.clone(),
            local_fs_root: Some(cfg.data_dir.clone()),
        };
        let analytical = basin_analytical::AnalyticalEngine::new(analytical_cfg)
            .context("build basin-analytical engine")?;
        tracing::info!(
            data_dir = %cfg.data_dir.display(),
            "analytical (DuckDB) path enabled; aggregate / GROUP BY queries route to DuckDB"
        );
        engine.with_analytical(analytical)
    } else {
        engine
    };

    // Build the static resolver from `BASIN_TENANTS`. We then ALWAYS
    // auto-inject `basin_auth -> INTERNAL_AUTH_TENANT_ID` so basin-auth can
    // authenticate as itself over the loopback pgwire catalog path — even
    // when auth is off, the entry is harmless (basin-auth only uses it when
    // it's actually starting up). The reserved entry is the seed that
    // unblocks the chicken-and-egg in the auth-on case: basin-auth needs the
    // listener up, the listener needs a resolver, the resolver needs an
    // identity for basin-auth.
    let mut static_resolver = StaticTenantResolver::default();
    for (user, tenant) in cfg.tenants {
        tracing::info!(%user, %tenant, "tenant registered");
        static_resolver = static_resolver.with_entry(user, tenant);
    }
    let internal_auth_tenant: TenantId = basin_auth::INTERNAL_AUTH_TENANT_ID
        .parse()
        .map_err(|e| anyhow!("INTERNAL_AUTH_TENANT_ID is not a valid tenant id: {e}"))?;
    static_resolver =
        static_resolver.with_entry(basin_auth::INTERNAL_AUTH_USERNAME, internal_auth_tenant);
    tracing::info!(
        user = basin_auth::INTERNAL_AUTH_USERNAME,
        tenant = %internal_auth_tenant,
        "reserved system tenant auto-registered for basin-auth loopback"
    );

    // Resolver stack with a lazy auth slot. The listener has to come up
    // BEFORE basin-auth's `connect()` runs (basin-auth's catalog now lives
    // inside engine's own pgwire), so the resolver `ServerConfig` takes can't
    // yet hold the `AuthService`. Instead we build a `DeferredAuthResolver`:
    // a static-first resolver with a `OnceLock<Arc<AuthService>>` for JWT +
    // API-key paths. Until the cell is filled, the resolver is just
    // static. After basin-auth boots, we `set()` the cell and JWT clients
    // start working — same final behaviour as the pre-loopback wiring.
    let auth_slot: Arc<OnceLock<Arc<basin_auth::AuthService>>> = Arc::new(OnceLock::new());
    let tenant_resolver: Arc<dyn TenantResolver> = Arc::new(DeferredAuthResolver {
        static_resolver,
        auth_slot: auth_slot.clone(),
    });
    if cfg.auth_enabled {
        tracing::info!(
            "pgwire resolver: JWT + API-key (deferred until basin-auth is up) + static (always)"
        );
    } else {
        tracing::info!("pgwire resolver: static (auth disabled)");
    }

    // --- optional connection pool ------------------------------------------
    //
    // Built before the router so we can hand the pool into `ServerConfig`.
    // Independent of basin-auth — the pool only depends on the engine.
    let mut eviction_handle: Option<basin_pool::EvictionHandle> = None;
    let pool: Option<Arc<basin_pool::SessionPool>> = if cfg.pool_enabled {
        let p = Arc::new(basin_pool::SessionPool::new(
            engine.clone(),
            basin_pool::PoolConfig::default(),
        ));
        eviction_handle = Some(p.spawn_eviction());
        tracing::info!("basin-pool enabled; pgwire sessions route through pool");
        Some(p)
    } else {
        None
    };

    // Optional TLS. Both env vars must be set together — half-configured TLS
    // is the kind of footgun that ships a "secure" server with the cert path
    // typo'd to plaintext. Hard error at startup.
    let tls = load_tls_from_env()?;
    if tls.is_some() {
        tracing::info!("pgwire TLS configured from BASIN_TLS_CERT_PATH/BASIN_TLS_KEY_PATH");
    }

    // Spawn the pgwire router BEFORE basin-auth starts. basin-auth's catalog
    // now lives inside basin engine's own pgwire (loopback DSN by default),
    // so the listener must be `accept()`-able by the time `AuthService::
    // connect()` runs.
    let (router_tx, router_rx) = tokio::sync::oneshot::channel();
    let server_cfg = ServerConfig {
        bind_addr: cfg.bind,
        engine: engine.clone(),
        tenant_resolver,
        pool,
        shard_endpoints: None,
        tls,
    };
    let router_bind = cfg.bind;

    let router_join =
        tokio::spawn(async move { basin_router::run_with_shutdown(server_cfg, router_rx).await });

    // Wait until the pgwire listener is accept()-able. A short TCP probe loop
    // is sufficient and avoids a deeper coupling with `basin-router::run`. 5s
    // upper bound — anything slower means the bind itself failed and we want
    // to surface that, not hang here.
    wait_for_pgwire_accept(router_bind)
        .await
        .context("pgwire listener never became accept-able")?;
    tracing::info!(bind = %router_bind, "pgwire listener is accept-ready");

    // --- optional auth service (now that pgwire is up) ---------------------
    //
    // basin-auth's default catalog DSN is the loopback `postgres://
    // basin_auth:basin_auth@127.0.0.1:5433/basin?sslmode=disable` — engine's
    // own pgwire. The `basin_auth` username resolves through the
    // auto-injected static-tenant entry above. Operators can still override
    // by setting `BASIN_AUTH_CATALOG_DSN` to an external Postgres.
    let auth_service: Option<Arc<basin_auth::AuthService>> = if cfg.auth_enabled {
        let auth_cfg = basin_auth::AuthConfig::from_env()
            .context("BASIN_AUTH_ENABLED=1 but AuthConfig::from_env failed")?;
        let svc = basin_auth::AuthService::connect(auth_cfg)
            .await
            .context("basin-auth connect failed")?;
        tracing::info!("basin-auth enabled");
        let arced = Arc::new(svc);
        // Activate JWT + API-key paths now that AuthService is ready. `set`
        // is idempotent — the slot is owned by us and only filled here.
        let _ = auth_slot.set(arced.clone());
        Some(arced)
    } else {
        None
    };

    // --- optional REST listener --------------------------------------------
    //
    // Per ADR 0006: REST requires AUTH. We refuse to bring up the HTTP
    // listener without an `AuthService` — that combination is the largest
    // data-leak class in this stack.
    let mut rest_handle: Option<basin_rest::RunningRest> = None;
    if cfg.rest_enabled {
        let auth = auth_service.clone().ok_or_else(|| {
            anyhow!("BASIN_REST_ENABLED=1 requires BASIN_AUTH_ENABLED=1 (per ADR 0006)")
        })?;
        let rest_cfg = basin_rest::RestConfig::new(cfg.rest_bind, engine.clone(), auth);
        let svc = basin_rest::RestService::new(rest_cfg);
        let running = svc
            .run_until_bound()
            .await
            .map_err(|e| anyhow!("basin-rest bind failed: {e}"))?;
        tracing::info!(bind = %running.local_addr, "basin-rest listening");
        rest_handle = Some(running);
    }

    // Wait for Ctrl-C, then signal the router to stop.
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
    let _ = router_tx.send(());

    // Tear down REST first so requests in flight don't survive past the
    // router; then the eviction loop; then router; then shard / WAL. The
    // shard / WAL ordering is unchanged from before this PR.
    if let Some(rest) = rest_handle.take() {
        tracing::info!("stopping basin-rest");
        let _ = rest.shutdown.send(());
        // Awaiting the join tells us the accept loop actually exited.
        let _ = rest.join.await;
    }
    if let Some(h) = eviction_handle.take() {
        tracing::info!("stopping basin-pool eviction loop");
        h.shutdown().await;
    }

    let router_result = router_join.await.map_err(|e| anyhow!("router join: {e}"))?;

    if let Some((_, bg, wal)) = shard_handles.take() {
        tracing::info!("draining shard background loops");
        bg.shutdown().await;
        tracing::info!("closing WAL");
        if let Err(e) = wal.close().await {
            tracing::warn!(error = %e, "WAL close failed");
        }
    }

    // `auth_service` is dropped at the end of `main` — its `Arc` is the only
    // lifeline, so nothing to do explicitly.
    drop(auth_service);

    router_result.map_err(|e| anyhow!("router exited: {e}"))?;
    Ok(())
}

struct Cfg {
    bind: SocketAddr,
    data_dir: PathBuf,
    wal_dir: PathBuf,
    shard_enabled: bool,
    pool_enabled: bool,
    auth_enabled: bool,
    rest_enabled: bool,
    analytical_enabled: bool,
    rest_bind: SocketAddr,
    tenants: Vec<(String, TenantId)>,
    catalog: CatalogBackend,
}

enum CatalogBackend {
    Memory,
    /// `url` is passed verbatim to `tokio_postgres::connect`. NoTls only;
    /// production deployments need to wrap the connector in rustls/native-tls.
    Postgres {
        url: String,
        schema: String,
    },
}

impl Cfg {
    fn from_env() -> Result<Self> {
        let bind: SocketAddr = std::env::var("BASIN_BIND")
            .unwrap_or_else(|_| "127.0.0.1:5433".to_string())
            .parse()
            .context("BASIN_BIND must be host:port")?;
        let data_dir: PathBuf = std::env::var("BASIN_DATA_DIR")
            .unwrap_or_else(|_| "./.basin-data".to_string())
            .into();
        let wal_dir: PathBuf = std::env::var("BASIN_WAL_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| data_dir.join("wal"));
        let shard_enabled = bool_env("BASIN_SHARD_ENABLED");
        let pool_enabled = bool_env("BASIN_POOL_ENABLED");
        let auth_enabled = bool_env("BASIN_AUTH_ENABLED");
        let rest_enabled = bool_env("BASIN_REST_ENABLED");
        let analytical_enabled = bool_env("BASIN_ANALYTICAL_ENABLED");
        let rest_bind: SocketAddr = std::env::var("BASIN_REST_BIND")
            .unwrap_or_else(|_| "127.0.0.1:5434".to_string())
            .parse()
            .context("BASIN_REST_BIND must be host:port")?;
        let raw = std::env::var("BASIN_TENANTS").unwrap_or_else(|_| "alice=*".to_string());
        let mut tenants = Vec::new();
        for entry in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (user, tid) = entry
                .split_once('=')
                .ok_or_else(|| anyhow!("bad BASIN_TENANTS entry: {entry:?} (want user=tid)"))?;
            let tenant = if tid == "*" {
                let t = TenantId::new();
                eprintln!("provisioned tenant {user} -> {t}");
                t
            } else {
                tid.parse()
                    .map_err(|e| anyhow!("bad tenant id {tid:?} for user {user:?}: {e}"))?
            };
            tenants.push((user.to_owned(), tenant));
        }
        if tenants.is_empty() {
            return Err(anyhow!("BASIN_TENANTS produced no entries"));
        }
        let catalog = parse_catalog_env()?;
        Ok(Self {
            bind,
            data_dir,
            wal_dir,
            shard_enabled,
            pool_enabled,
            auth_enabled,
            rest_enabled,
            analytical_enabled,
            rest_bind,
            tenants,
            catalog,
        })
    }
}

/// Resolver that fronts a `StaticTenantResolver` with a lazily-populated
/// `AuthService`-backed JWT + API-key path. While the slot is empty (during
/// startup, before `basin_auth::AuthService::connect()` completes), the
/// resolver behaves as if only the static map exists — exactly what we need
/// for basin-auth's own loopback connect, which authenticates as the
/// reserved `basin_auth` user. After the slot is filled, JWT and API-key
/// clients on the same listener get the full stack.
///
/// Static-first resolver with a lazily-populated `AuthService` slot for the
/// JWT + API-key paths. Before basin-auth boots, the auth slot is empty and
/// the resolver behaves as static-only; once `connect()` succeeds the slot
/// is `set()` and JWT clients start working.
struct DeferredAuthResolver {
    static_resolver: StaticTenantResolver,
    auth_slot: Arc<OnceLock<Arc<basin_auth::AuthService>>>,
}

#[async_trait]
impl TenantResolver for DeferredAuthResolver {
    async fn resolve(&self, username: &str) -> basin_common::Result<TenantId> {
        if let Some(auth) = self.auth_slot.get().cloned() {
            let jwt = JwtTenantResolver::new(auth.clone());
            if let Ok(t) = jwt.resolve(username).await {
                return Ok(t);
            }
            let api_key = ApiKeyTenantResolver::new(auth);
            if let Ok(t) = api_key.resolve(username).await {
                return Ok(t);
            }
        }
        self.static_resolver.resolve(username).await
    }

    async fn resolve_credentials(
        &self,
        username: &str,
        password: &str,
    ) -> basin_common::Result<TenantId> {
        if let Some(auth) = self.auth_slot.get().cloned() {
            let jwt = JwtTenantResolver::new(auth.clone());
            if let Ok(t) = jwt.resolve_credentials(username, password).await {
                return Ok(t);
            }
            let api_key = ApiKeyTenantResolver::new(auth);
            if let Ok(t) = api_key.resolve_credentials(username, password).await {
                return Ok(t);
            }
        }
        self.static_resolver
            .resolve_credentials(username, password)
            .await
    }
}

/// Polls `127.0.0.1:<port>` until `connect()` succeeds, with 100ms backoff
/// and a 5s deadline. Used to gate basin-auth's startup on the pgwire
/// listener actually being ready to serve. `bind` may be an `INADDR_ANY`
/// address (e.g. `0.0.0.0:5433`); we probe `127.0.0.1:<port>` because the
/// loopback DSN basin-auth uses always targets `127.0.0.1`.
async fn wait_for_pgwire_accept(bind: SocketAddr) -> Result<()> {
    let port = bind.port();
    let target: SocketAddr = format!("127.0.0.1:{port}")
        .parse()
        .context("build loopback probe address")?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match tokio::time::timeout(
            Duration::from_millis(200),
            tokio::net::TcpStream::connect(target),
        )
        .await
        {
            Ok(Ok(_)) => return Ok(()),
            _ => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(anyhow!(
                        "pgwire listener at {target} not accept-able after 5s"
                    ));
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

fn bool_env(name: &str) -> bool {
    matches!(
        std::env::var(name).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

/// Reads `BASIN_TLS_CERT_PATH` + `BASIN_TLS_KEY_PATH` and loads the PEM bytes.
/// Both unset = no TLS; both set = TLS on; exactly one set = hard error.
fn load_tls_from_env() -> Result<Option<Arc<TlsConfig>>> {
    let cert_path = std::env::var("BASIN_TLS_CERT_PATH")
        .ok()
        .filter(|s| !s.is_empty());
    let key_path = std::env::var("BASIN_TLS_KEY_PATH")
        .ok()
        .filter(|s| !s.is_empty());
    match (cert_path, key_path) {
        (None, None) => Ok(None),
        (Some(cert), Some(key)) => {
            let cert_pem = std::fs::read(&cert)
                .with_context(|| format!("read BASIN_TLS_CERT_PATH at {cert}"))?;
            let key_pem =
                std::fs::read(&key).with_context(|| format!("read BASIN_TLS_KEY_PATH at {key}"))?;
            Ok(Some(Arc::new(TlsConfig { cert_pem, key_pem })))
        }
        _ => Err(anyhow!(
            "TLS half-configured: set both BASIN_TLS_CERT_PATH and BASIN_TLS_KEY_PATH, or neither"
        )),
    }
}

fn parse_catalog_env() -> Result<CatalogBackend> {
    let raw = std::env::var("BASIN_CATALOG").unwrap_or_else(|_| "memory".to_string());
    if raw == "memory" {
        return Ok(CatalogBackend::Memory);
    }
    // `tokio_postgres::connect` accepts both `postgres://...` URL form and
    // libpq keyword form (`host=... user=...`). We accept either as the
    // postgres backend marker.
    if raw.starts_with("postgres://") || raw.starts_with("postgresql://") || raw.contains('=') {
        let schema =
            std::env::var("BASIN_CATALOG_SCHEMA").unwrap_or_else(|_| "basin_catalog".to_string());
        return Ok(CatalogBackend::Postgres { url: raw, schema });
    }
    Err(anyhow!(
        "BASIN_CATALOG must be 'memory' or a postgres connection string, got {raw:?}"
    ))
}
