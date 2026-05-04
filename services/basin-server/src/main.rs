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

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use basin_common::{TenantId, telemetry::{init, LogFormat}};
use basin_router::{
    JwtTenantResolver, ServerConfig, StackedTenantResolver, StaticTenantResolver, TenantResolver,
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
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
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
    let mut shard_handles: Option<(basin_shard::Shard, basin_shard::ShardBackgroundHandle, basin_wal::Wal)> = None;
    let shard_for_engine: Option<basin_shard::Shard> = if cfg.shard_enabled {
        std::fs::create_dir_all(&cfg.wal_dir)
            .with_context(|| format!("create WAL dir {}", cfg.wal_dir.display()))?;
        let wal_fs = LocalFileSystem::new_with_prefix(&cfg.wal_dir)
            .with_context(|| format!("WAL LocalFileSystem at {}", cfg.wal_dir.display()))?;
        let wal = basin_wal::Wal::open(basin_wal::WalConfig {
            object_store: Arc::new(wal_fs),
            root_prefix: None,
            flush_interval: std::time::Duration::from_millis(200),
            flush_max_bytes: 1024 * 1024,
        })
        .await
        .context("open WAL")?;
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

    let mut static_resolver = StaticTenantResolver::default();
    for (user, tenant) in cfg.tenants {
        tracing::info!(%user, %tenant, "tenant registered");
        static_resolver = static_resolver.with_entry(user, tenant);
    }

    // --- optional auth service ---------------------------------------------
    //
    // Constructed before REST so the REST gate can require it. Built before
    // the pool / router too because the pgwire resolver stack depends on it
    // when `BASIN_AUTH_ENABLED=1`.
    let auth_service: Option<Arc<basin_auth::AuthService>> = if cfg.auth_enabled {
        let auth_cfg = basin_auth::AuthConfig::from_env()
            .context("BASIN_AUTH_ENABLED=1 but AuthConfig::from_env failed")?;
        let svc = basin_auth::AuthService::connect(auth_cfg)
            .await
            .context("basin-auth connect failed")?;
        tracing::info!("basin-auth enabled");
        Some(Arc::new(svc))
    } else {
        None
    };

    // Pgwire resolver: when auth is on, prefer JWT and fall back to the
    // static map so existing `alice=*`-style demos keep working alongside
    // JWT clients on the same listener. With auth off the static resolver
    // is the only path — byte-identical to pre-auth behaviour.
    let tenant_resolver: Arc<dyn TenantResolver> = match auth_service.as_ref() {
        Some(auth) => {
            tracing::info!("pgwire resolver: JWT (primary) + static (fallback)");
            Arc::new(StackedTenantResolver::new(vec![
                Arc::new(JwtTenantResolver::new(auth.clone())),
                Arc::new(static_resolver),
            ]))
        }
        None => Arc::new(static_resolver),
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

    // --- optional connection pool ------------------------------------------
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

    // Run the router until Ctrl-C, then shut down the shard's background loops
    // and close the WAL. Order matters: stop accepting writes (router exit),
    // then drain compactions (shutdown), then close the WAL — that way no
    // segment is mid-flush when the file handles drop.
    let (router_tx, router_rx) = tokio::sync::oneshot::channel();
    let server_cfg = ServerConfig {
        bind_addr: cfg.bind,
        engine,
        tenant_resolver,
        pool,
    };

    let router_join = tokio::spawn(async move {
        basin_router::run_with_shutdown(server_cfg, router_rx).await
    });

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
    Postgres { url: String, schema: String },
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

fn bool_env(name: &str) -> bool {
    matches!(
        std::env::var(name).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

fn parse_catalog_env() -> Result<CatalogBackend> {
    let raw = std::env::var("BASIN_CATALOG").unwrap_or_else(|_| "memory".to_string());
    if raw == "memory" {
        return Ok(CatalogBackend::Memory);
    }
    // `tokio_postgres::connect` accepts both `postgres://...` URL form and
    // libpq keyword form (`host=... user=...`). We accept either as the
    // postgres backend marker.
    if raw.starts_with("postgres://")
        || raw.starts_with("postgresql://")
        || raw.contains('=')
    {
        let schema = std::env::var("BASIN_CATALOG_SCHEMA")
            .unwrap_or_else(|_| "basin_catalog".to_string());
        return Ok(CatalogBackend::Postgres { url: raw, schema });
    }
    Err(anyhow!(
        "BASIN_CATALOG must be 'memory' or a postgres connection string, got {raw:?}"
    ))
}
