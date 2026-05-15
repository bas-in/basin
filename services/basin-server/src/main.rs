//! `basin-server` — single-process Basin proof-of-concept.
//!
//! Wires `basin-storage` + `basin-catalog` + `basin-engine` + `basin-router`
//! into one TCP listener that speaks pgwire. Configuration is environment-
//! driven so the PoC can be exercised by:
//!
//! ```text
//! BASIN_DATA_DIR=/tmp/basin BASIN_BIND=127.0.0.1:5433 \
//! BASIN_PROJECTS=alice=01HABCD..,bob=01HEFGH.. \
//! cargo run -p basin-server
//! ```
//!
//! `BASIN_PROJECTS` is a comma-separated list of `user=project_id_ulid` pairs.
//! For convenience, an entry of the form `user=*` allocates a fresh project id
//! at startup and prints it to stderr. This is PoC-grade only — production
//! project provisioning lives in the (not-yet-built) control plane.
//!
//! Catalog backend is selected via `BASIN_CATALOG`:
//!
//! ```text
//! BASIN_CATALOG=memory                                  # default; volatile
//! BASIN_CATALOG=postgres://pc@127.0.0.1:5432/postgres   # durable, persists across restarts
//! BASIN_CATALOG_SCHEMA=basin_catalog                    # optional, default = basin_catalog
//! ```
//!
//! Object storage is selected via `BASIN_STORAGE_BACKEND`:
//!
//! ```text
//! BASIN_STORAGE_BACKEND=local                           # default; uses BASIN_DATA_DIR
//! BASIN_STORAGE_BACKEND=s3|tigris                       # S3-compatible object store
//! BASIN_STORAGE_ROOT_PREFIX=warehouse                   # optional bucket sub-prefix
//! ```
//!
//! S3-compatible backends use `BASIN_STORAGE_*` / AWS-compatible env vars parsed
//! by `basin_storage::backends::s3_compatible::S3LikeConfig`.
//!
//! ## WAL + shard owner
//!
//! Two env vars gate the new WAL-acked write path:
//!
//! ```text
//! BASIN_SHARD_ENABLED=1     # default 0; when 1, INSERTs route through basin-shard
//! BASIN_WAL_DIR=/tmp/wal    # default ${BASIN_DATA_DIR}/wal
//! BASIN_WAL_BACKEND=local   # optional; unset mirrors BASIN_STORAGE_BACKEND
//! BASIN_WAL_ROOT_PREFIX=wal-archive # optional; defaults to BASIN_STORAGE_ROOT_PREFIX
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
//! ## Auth storage backend
//!
//! When `BASIN_AUTH_ENABLED=1`, auth tables are stored in-process by default:
//! the server builds an `EngineAuthStore` that runs SQL directly against the
//! in-process `basin_engine::Engine` under the reserved
//! `INTERNAL_AUTH_PROJECT_ID`. This eliminates the loopback TCP round-trip and
//! the chicken-and-egg startup ordering problem it caused.
//!
//! Operators who need auth stored in an external Postgres (Neon, RDS, etc.)
//! can still override via `BASIN_AUTH_CATALOG_DSN`:
//!
//! ```text
//! BASIN_AUTH_CATALOG_DSN=postgres://user:pass@host/dbname
//! ```
//!
//! When that env var is set, the server falls back to `AuthService::connect`
//! (outbound TCP, the old path) instead of `EngineAuthStore`.
//!
//! `BASIN_REST_ENABLED=1` *requires* `BASIN_AUTH_ENABLED=1` per ADR 0006 — a
//! REST stack without auth is the largest data-leak class we know how to
//! ship, so the binary refuses to start in that combination.
//!
//! Default behaviour with none of these set is unchanged: a single pgwire
//! listener with `StaticProjectResolver`, no pool, no REST, and no auth side
//! channel.
//!
//! ## Pgwire username convention with auth
//!
//! When `BASIN_AUTH_ENABLED=1`, the pgwire `user` parameter doubles as a
//! bearer-token slot: clients can connect with `user=<jwt>` (optionally
//! `user=Bearer <jwt>`) and the server resolves the embedded `project_id`
//! claim through `JwtProjectResolver`. Static `BASIN_PROJECTS` mappings such
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

mod engine_auth_store;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use basin_common::{
    telemetry::{init, LogFormat},
    ProjectId,
};
use basin_router::{
    ApiKeyProjectResolver, JwtProjectResolver, ServerConfig, StackedProjectResolver,
    StaticProjectResolver, ProjectCredentialsResolver, ProjectResolver, TlsConfig,
};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = init(tracing::Level::INFO, LogFormat::Pretty);

    let cfg = Cfg::from_env()?;
    tracing::info!(
        bind = %cfg.bind,
        data_dir = %cfg.data_dir.display(),
        projects = cfg.projects.len(),
        shard_enabled = cfg.shard_enabled,
        pool_enabled = cfg.pool_enabled,
        auth_enabled = cfg.auth_enabled,
        rest_enabled = cfg.rest_enabled,
        "starting basin-server"
    );

    std::fs::create_dir_all(&cfg.data_dir)
        .with_context(|| format!("create data dir {}", cfg.data_dir.display()))?;
    let object_store = build_storage_object_store(&cfg)?;

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
        object_store,
        root_prefix: cfg.storage_root_prefix.clone(),
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
        let wal_store = build_wal_object_store(&cfg)?;
        let wal: Arc<dyn basin_wal::Wal> = Arc::new(
            basin_wal::LocalWal::open(basin_wal::WalConfig {
                object_store: wal_store,
                root_prefix: cfg.wal_root_prefix.clone(),
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

    // Build the static resolver from `BASIN_PROJECTS`.
    let mut static_resolver = StaticProjectResolver::default();
    for (user, project) in cfg.projects {
        tracing::info!(%user, %project, "project registered");
        static_resolver = static_resolver.with_entry(user, project);
    }

    // --- optional auth service (built before the router) --------------------
    //
    // With `EngineAuthStore`, basin-auth no longer needs an outbound TCP
    // connection back to our own pgwire listener. We can build the full
    // AuthService in-process and hand the live resolvers directly to
    // `StackedProjectResolver` before the pgwire router even binds.
    //
    // Fallback: when `BASIN_AUTH_CATALOG_DSN` is explicitly set, operators
    // want auth stored in an external Postgres. In that case we fall back to
    // `AuthService::connect` (the old outbound-TCP path). The pgwire listener
    // is started before the connect in that case to keep the loopback option
    // available, but for most deployments the in-process path is preferred.
    let auth_service: Option<Arc<basin_auth::AuthService>> = if cfg.auth_enabled {
        let auth_cfg = basin_auth::AuthConfig::from_env()
            .context("BASIN_AUTH_ENABLED=1 but AuthConfig::from_env failed")?;

        let has_external_dsn = auth_cfg.catalog_dsn.is_some();

        let svc = if has_external_dsn {
            // External Postgres path: connect outbound. The listener need not
            // be up first because we're hitting a real PG, not ourselves.
            tracing::info!("basin-auth: using external BASIN_AUTH_CATALOG_DSN (outbound Postgres)");
            basin_auth::AuthService::connect(auth_cfg)
                .await
                .context("basin-auth connect (external DSN) failed")?
        } else {
            // In-process path: build EngineAuthStore backed by the engine we
            // just constructed — no loopback TCP, no OnceLock, no race.
            tracing::info!("basin-auth: using in-process EngineAuthStore (no loopback TCP)");
            let internal_auth_project: ProjectId = basin_auth::INTERNAL_AUTH_PROJECT_ID
                .parse()
                .map_err(|e| anyhow!("INTERNAL_AUTH_PROJECT_ID is not a valid project id: {e}"))?;
            let auth_store = Arc::new(engine_auth_store::EngineAuthStore::new(
                Arc::new(engine.clone()),
                auth_cfg.catalog_schema.clone(),
                internal_auth_project,
            ));
            let mailer: Arc<dyn basin_auth::Mailer> =
                Arc::new(basin_auth::SmtpMailer::from_config(&auth_cfg.smtp)?);
            basin_auth::AuthService::with_store(auth_cfg, auth_store, mailer)
                .await
                .context("basin-auth with_store (EngineAuthStore) failed")?
        };

        tracing::info!("basin-auth enabled");
        Some(Arc::new(svc))
    } else {
        None
    };

    // Migrate legacy pgwire credentials to the new self-routing format.
    // This is safe to run on every startup — `list_legacy_credentials`
    // returns an empty list once all rows have been rotated, so subsequent
    // runs are instant no-ops. Failures warn but do not abort startup:
    // old-format credentials remain valid during the transition window.
    if let Some(ref auth) = auth_service {
        match auth.migrate_legacy_credentials().await {
            Ok(n) if n > 0 => tracing::info!(
                count = n,
                "migrated legacy pgwire credentials to new self-routing format"
            ),
            Ok(_) => {}
            Err(e) => tracing::warn!(
                error = %e,
                "legacy credential migration failed — will retry on next startup"
            ),
        }
    }

    // Build the resolver stack. When auth is enabled we have a live
    // AuthService right now (no deferred slot needed), so we wire up the
    // full JWT + API-key + credentials + static stack immediately.
    let project_resolver: Arc<dyn ProjectResolver> = if let Some(ref auth) = auth_service {
        tracing::info!("pgwire resolver: credentials + JWT + API-key + static");
        Arc::new(StackedProjectResolver::new(vec![
            Arc::new(ProjectCredentialsResolver::new(auth.clone())),
            Arc::new(JwtProjectResolver::new(auth.clone())),
            Arc::new(ApiKeyProjectResolver::new(auth.clone())),
            Arc::new(static_resolver),
        ]))
    } else {
        tracing::info!("pgwire resolver: static (auth disabled)");
        Arc::new(static_resolver)
    };

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

    // Spawn the pgwire router. The resolver is fully wired with live auth
    // resolvers (when auth is enabled) so JWT / API-key / credential clients
    // work from the very first connection — no deferred slot needed.
    let server_cfg = ServerConfig {
        bind_addr: cfg.bind,
        engine: engine.clone(),
        project_resolver,
        pool,
        shard_endpoints: None,
        tls,
        // Pass None for now — unlimited connections. Wire a
        // ConnectionLimiter here to enforce per-project max_connections
        // limits sourced from the control plane.
        connection_limiter: None,
    };
    let router = basin_router::run_until_bound(server_cfg)
        .await
        .context("basin-router bind failed")?;
    tracing::info!(bind = %router.local_addr, "pgwire listener is accept-ready");

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
    let _ = router.shutdown.send(());

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

    let router_result = router.join.await.map_err(|e| anyhow!("router join: {e}"))?;

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
    rest_bind: SocketAddr,
    projects: Vec<(String, ProjectId)>,
    catalog: CatalogBackend,
    storage_root_prefix: Option<ObjectPath>,
    wal_root_prefix: Option<ObjectPath>,
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
        let rest_bind: SocketAddr = std::env::var("BASIN_REST_BIND")
            .unwrap_or_else(|_| "127.0.0.1:5434".to_string())
            .parse()
            .context("BASIN_REST_BIND must be host:port")?;
        let raw = std::env::var("BASIN_PROJECTS").unwrap_or_else(|_| "alice=*".to_string());
        let mut projects = Vec::new();
        for entry in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (user, tid) = entry
                .split_once('=')
                .ok_or_else(|| anyhow!("bad BASIN_PROJECTS entry: {entry:?} (want user=tid)"))?;
            let project = if tid == "*" {
                let t = ProjectId::new();
                eprintln!("provisioned project {user} -> {t}");
                t
            } else {
                tid.parse()
                    .map_err(|e| anyhow!("bad project id {tid:?} for user {user:?}: {e}"))?
            };
            projects.push((user.to_owned(), project));
        }
        if projects.is_empty() {
            return Err(anyhow!("BASIN_PROJECTS produced no entries"));
        }
        let catalog = parse_catalog_env()?;
        let storage_root_prefix = parse_object_prefix_env("BASIN_STORAGE_ROOT_PREFIX")?;
        let wal_root_prefix = parse_object_prefix_env("BASIN_WAL_ROOT_PREFIX")?
            .or_else(|| storage_root_prefix.clone());
        Ok(Self {
            bind,
            data_dir,
            wal_dir,
            shard_enabled,
            pool_enabled,
            auth_enabled,
            rest_enabled,
            rest_bind,
            projects,
            catalog,
            storage_root_prefix,
            wal_root_prefix,
        })
    }
}

fn bool_env(name: &str) -> bool {
    matches!(
        std::env::var(name).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

/// Loads TLS config from env. Priority:
///   1. `BASIN_TLS_CERT_PEM` + `BASIN_TLS_KEY_PEM`  — raw PEM content in env vars (Fly secrets)
///   2. `BASIN_TLS_CERT_PATH` + `BASIN_TLS_KEY_PATH` — file paths (original behaviour)
///   3. Neither set → no TLS.
/// Exactly one of a pair being set is a hard error.
fn load_tls_from_env() -> Result<Option<Arc<TlsConfig>>> {
    // Priority 1: inline PEM content from env vars.
    let cert_pem_env = std::env::var("BASIN_TLS_CERT_PEM")
        .ok()
        .filter(|s| !s.is_empty());
    let key_pem_env = std::env::var("BASIN_TLS_KEY_PEM")
        .ok()
        .filter(|s| !s.is_empty());
    match (cert_pem_env, key_pem_env) {
        (Some(cert), Some(key)) => {
            tracing::info!("pgwire TLS configured from BASIN_TLS_CERT_PEM/BASIN_TLS_KEY_PEM");
            return Ok(Some(Arc::new(TlsConfig {
                cert_pem: cert.into_bytes(),
                key_pem: key.into_bytes(),
            })));
        }
        (None, None) => {}
        _ => {
            return Err(anyhow!(
                "TLS half-configured: set both BASIN_TLS_CERT_PEM and BASIN_TLS_KEY_PEM, or neither"
            ));
        }
    }

    // Priority 2: file paths.
    let cert_path = std::env::var("BASIN_TLS_CERT_PATH")
        .ok()
        .filter(|s| !s.is_empty());
    let key_path = std::env::var("BASIN_TLS_KEY_PATH")
        .ok()
        .filter(|s| !s.is_empty());
    match (cert_path, key_path) {
        (None, None) => Ok(None),
        (Some(cert), Some(key)) => {
            tracing::info!("pgwire TLS configured from BASIN_TLS_CERT_PATH/BASIN_TLS_KEY_PATH");
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

fn build_storage_object_store(cfg: &Cfg) -> Result<Arc<dyn ObjectStore>> {
    let backend = std::env::var("BASIN_STORAGE_BACKEND")
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "local".to_string());

    match backend.as_str() {
        "local" => {
            let fs = LocalFileSystem::new_with_prefix(&cfg.data_dir)
                .with_context(|| format!("LocalFileSystem at {}", cfg.data_dir.display()))?;
            tracing::info!(
                backend = "local",
                data_dir = %cfg.data_dir.display(),
                root_prefix = ?cfg.storage_root_prefix,
                "storage object-store backend configured",
            );
            Ok(Arc::new(fs))
        }
        "s3" | "tigris" => {
            let s3_cfg = basin_storage::backends::s3_compatible::S3LikeConfig::from_env()
                .map_err(|e| anyhow!(e))
                .context("load S3-compatible storage backend config")?;
            let provider = format!("{:?}", s3_cfg.provider);
            let bucket = s3_cfg.bucket.clone();
            let endpoint = s3_cfg.endpoint.clone();
            let region = s3_cfg.region.clone();
            let store = s3_cfg
                .build_object_store()
                .map_err(|e| anyhow!(e))
                .context("build S3-compatible storage object store")?;
            tracing::info!(
                backend,
                provider,
                bucket,
                endpoint = endpoint.as_deref().unwrap_or("<aws-default>"),
                region,
                root_prefix = ?cfg.storage_root_prefix,
                "storage object-store backend configured",
            );
            Ok(store)
        }
        other => Err(anyhow!(
            "BASIN_STORAGE_BACKEND must be 'local', 's3', or 'tigris', got {other:?}"
        )),
    }
}

fn build_wal_object_store(cfg: &Cfg) -> Result<Arc<dyn ObjectStore>> {
    let backend = std::env::var("BASIN_WAL_BACKEND")
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::env::var("BASIN_STORAGE_BACKEND")
                .ok()
                .map(|s| s.trim().to_ascii_lowercase())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "local".to_string());

    match backend.as_str() {
        "local" => {
            std::fs::create_dir_all(&cfg.wal_dir)
                .with_context(|| format!("create WAL dir {}", cfg.wal_dir.display()))?;
            let fs = LocalFileSystem::new_with_prefix(&cfg.wal_dir)
                .with_context(|| format!("WAL LocalFileSystem at {}", cfg.wal_dir.display()))?;
            tracing::info!(
                backend = "local",
                wal_dir = %cfg.wal_dir.display(),
                root_prefix = ?cfg.wal_root_prefix,
                "WAL object-store backend configured",
            );
            Ok(Arc::new(fs))
        }
        "s3" | "tigris" => {
            let s3_cfg = basin_storage::backends::s3_compatible::S3LikeConfig::from_env()
                .map_err(|e| anyhow!(e))
                .context(
                    "load S3-compatible WAL backend config; BASIN_WAL_BACKEND uses BASIN_STORAGE_* credentials",
                )?;
            let provider = format!("{:?}", s3_cfg.provider);
            let bucket = s3_cfg.bucket.clone();
            let endpoint = s3_cfg.endpoint.clone();
            let region = s3_cfg.region.clone();
            let store = s3_cfg
                .build_object_store()
                .map_err(|e| anyhow!(e))
                .context("build S3-compatible WAL object store")?;
            tracing::info!(
                backend,
                provider,
                bucket,
                endpoint = endpoint.as_deref().unwrap_or("<aws-default>"),
                region,
                root_prefix = ?cfg.wal_root_prefix,
                "WAL object-store backend configured",
            );
            Ok(store)
        }
        other => Err(anyhow!(
            "BASIN_WAL_BACKEND must be 'local', 's3', or 'tigris', got {other:?}"
        )),
    }
}

fn parse_object_prefix_env(name: &str) -> Result<Option<ObjectPath>> {
    let Some(raw) = std::env::var(name).ok() else {
        return Ok(None);
    };
    let trimmed = raw.trim().trim_matches('/');
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.contains("..") {
        return Err(anyhow!("{name} must not contain '..' path segments"));
    }
    Ok(Some(ObjectPath::from(trimmed)))
}
