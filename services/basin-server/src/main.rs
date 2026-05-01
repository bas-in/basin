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

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use basin_common::{TenantId, telemetry::{init, LogFormat}};
use basin_router::{ServerConfig, StaticTenantResolver};
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
        storage,
        catalog,
        shard: shard_for_engine,
    });

    let mut resolver = StaticTenantResolver::default();
    for (user, tenant) in cfg.tenants {
        tracing::info!(%user, %tenant, "tenant registered");
        resolver = resolver.with_entry(user, tenant);
    }

    // Run the router until Ctrl-C, then shut down the shard's background loops
    // and close the WAL. Order matters: stop accepting writes (router exit),
    // then drain compactions (shutdown), then close the WAL — that way no
    // segment is mid-flush when the file handles drop.
    let (router_tx, router_rx) = tokio::sync::oneshot::channel();
    let server_cfg = ServerConfig {
        bind_addr: cfg.bind,
        engine,
        tenant_resolver: Arc::new(resolver),
    };

    let router_join = tokio::spawn(async move {
        basin_router::run_with_shutdown(server_cfg, router_rx).await
    });

    // Wait for Ctrl-C, then signal the router to stop.
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
    let _ = router_tx.send(());
    let router_result = router_join.await.map_err(|e| anyhow!("router join: {e}"))?;

    if let Some((_, bg, wal)) = shard_handles.take() {
        tracing::info!("draining shard background loops");
        bg.shutdown().await;
        tracing::info!("closing WAL");
        if let Err(e) = wal.close().await {
            tracing::warn!(error = %e, "WAL close failed");
        }
    }

    router_result.map_err(|e| anyhow!("router exited: {e}"))?;
    Ok(())
}

struct Cfg {
    bind: SocketAddr,
    data_dir: PathBuf,
    wal_dir: PathBuf,
    shard_enabled: bool,
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
        let shard_enabled = matches!(
            std::env::var("BASIN_SHARD_ENABLED").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE")
        );
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
        Ok(Self { bind, data_dir, wal_dir, shard_enabled, tenants, catalog })
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
