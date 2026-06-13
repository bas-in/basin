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
//! ## WAL durability mode (multi-node raft, commit 6)
//!
//! ```text
//! BASIN_WAL_MODE=local       # default; single-node file-backed WAL (LocalWal)
//! BASIN_WAL_MODE=raft        # replicated WAL via openraft (RaftWal)
//! ```
//!
//! `BASIN_WAL_MODE` selects the [`basin_wal::Wal`] backend handed to the
//! shard. `local` (the default) is **byte-identical** to today — a
//! `LocalWal` over the configured object store. `raft` opens a
//! [`basin_wal::RaftWal`] under `${BASIN_WAL_DIR}/raft`, starts the raft
//! service, joins-or-bootstraps the cluster, and makes the WAL durability
//! boundary a **quorum ack** instead of a local fsync.
//!
//! Raft mode requires the cluster identity + topology env surface:
//!
//! ```text
//! BASIN_NODE_ID=1                  # REQUIRED in raft mode; this node's numeric raft id (u64, >0)
//! BASIN_RAFT_BIND=127.0.0.1:6010   # REQUIRED in raft mode; this node's raft RPC listen addr
//! BASIN_RAFT_PEERS=1@127.0.0.1:6010,2@10.0.0.2:6010,3@10.0.0.3:6010
//!                                  # REQUIRED in raft mode; id@host:port for every voter incl. self
//! BASIN_RAFT_BOOTSTRAP=1           # optional; set on exactly ONE node to initialize the cluster
//!                                  # when its raft log is empty. Other nodes await leader contact.
//! ```
//!
//! Config validation (commit 6): `BASIN_WAL_MODE=raft` without
//! `BASIN_SHARD_ENABLED=1`, without `BASIN_RAFT_BIND`, or without
//! `BASIN_RAFT_PEERS` is a **startup error** — same refuse-to-start idiom as
//! `BASIN_LEASE_MODE=required` / `BASIN_REST_ENABLED=1`. `BASIN_NODE_ID` must
//! appear in `BASIN_RAFT_PEERS` with a `host:port` that matches
//! `BASIN_RAFT_BIND`. `local` mode ignores the raft env entirely.
//!
//! ### Precedence: raft leadership vs. writer lease
//!
//! In `raft` mode, **raft leadership is the write fence and it supersedes the
//! writer lease**:
//!
//! - Writes are accepted only on the raft **leader**. A write that arrives at
//!   a follower / candidate / learner is refused before it reaches the raft
//!   log with the typed, retryable `LeaseNotHeld` error carrying a leader
//!   hint (`BasinError::not_leader`, SQLSTATE 40001) — the router re-resolves
//!   and retries against the leader. (Reads are never refused.)
//! - The lease fence (`BASIN_LEASE_MODE=required`) and the raft fence are not
//!   stacked: in raft mode the raft leader IS the single writer, so the lease
//!   registry is redundant. If both `BASIN_WAL_MODE=raft` and
//!   `BASIN_LEASE_MODE=required` are set, raft wins and the server logs that
//!   the lease fence is subsumed by raft leadership (the shard still carries
//!   the lease registry, but a follower is fenced by the raft check first, so
//!   the lease never gates a write the raft fence would have allowed). The
//!   epoch-fenced WAL append remains correct underneath either fence.
//! - In `local` mode nothing changes: `BASIN_LEASE_MODE` is the only write
//!   fence, exactly as before.
//!
//! ## Writer leases (multi-node phase 1, ADR 0023)
//!
//! ```text
//! BASIN_LEASE_MODE=off       # default; current single-replica behaviour
//! BASIN_LEASE_MODE=required  # writer leases ENFORCED (requires BASIN_SHARD_ENABLED=1)
//! BASIN_REPLICA_ID=node-a    # optional stable holder id; default host:pid:salt
//! ```
//!
//! With `BASIN_LEASE_MODE=required` the catalog doubles as the
//! `LeaseRegistry` (both backends implement it: the in-memory catalog for
//! dev, Postgres CAS rows for production) and the shard acquires the writer
//! lease for every `(project, partition)` it touches. The shard's background
//! loop — the same one that runs eviction + compaction — heartbeats every
//! `BASIN_LEASE_RENEW_SECS` (default 5 s) against a `BASIN_LEASE_TTL_SECS`
//! TTL (default 15 s). On lease loss the partition's in-memory state is
//! dropped and writes fail with the typed `LeaseNotHeld` error (SQLSTATE
//! 40001, retryable) until the lease is re-acquired; reads continue
//! throughout. WAL appends stay epoch-fenced exactly as in Phase 6.X.A, so a
//! dual-leaseholder window during a network partition still fails closed at
//! the WAL. `required` without `BASIN_SHARD_ENABLED=1` is a startup error:
//! the legacy synchronous write path has no lease seam to enforce.
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
//!
//! ## Feature flags (ADR 0018)
//!
//! Optional subsystems are Cargo-feature-gated so a self-hoster can build a
//! minimal pgwire-only binary:
//!
//! ```text
//! cargo build -p basin-server                               # default (auth+rest+webhooks)
//! cargo build -p basin-server --no-default-features         # minimal pgwire-only
//! cargo build -p basin-server --all-features                # kitchen-sink
//! ```
//!
//! Gates live only at registration boundaries in this file; library crates
//! remain feature-clean.

// mimalloc as the global allocator. Allocator-heavy hot paths in basin-server
// (Parquet decode in basin-storage::reader, Arrow batch construction in
// basin-engine, JSON/pgwire response encoding in basin-router) all benefit
// measurably from a faster malloc; mimalloc is a one-line drop-in with no
// other code changes required.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// ADR 0018: engine_auth_store is only compiled when the `auth` feature is on.
#[cfg(feature = "auth")]
mod engine_auth_store;

// Feature 1 (5.11.W6): catalog-backed FunctionInvoker for LANGUAGE javascript.
// Compiled only when `wasm-fn` feature is on (pulls in basin-fn / wasmtime).
#[cfg(feature = "wasm-fn")]
mod fn_runtime;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use basin_common::{
    telemetry::{init, LogFormat},
    ProjectId,
};
// Base resolver types always needed (pgwire core).
use basin_router::{
    CatalogConnectionLimitProvider, ConnectionLimiter, ProjectResolver, ServerConfig,
    StaticProjectResolver, TlsConfig,
};
// Auth-gated resolver types — only available when `auth` (and by extension
// `basin-auth`) is compiled in.
#[cfg(feature = "auth")]
use basin_router::{
    ApiKeyProjectResolver, JwtProjectResolver, ProjectCredentialsResolver, StackedProjectResolver,
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
        lease_mode = ?cfg.lease_mode,
        wal_mode = ?cfg.wal_mode,
        region = basin_common::local_region(),
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
    // Both catalog backends also implement `LeaseRegistry` (ADR 0023 Phase
    // 6.X.A: in-memory per-pair guard for dev, Postgres CAS rows for
    // production), so the same concrete `Arc` is kept under both trait
    // objects. The registry handle is only consumed when
    // `BASIN_LEASE_MODE=required` wires it into the shard below.
    let (catalog, lease_registry): (
        Arc<dyn basin_catalog::Catalog>,
        Arc<dyn basin_catalog::LeaseRegistry>,
    ) = match &cfg.catalog {
        CatalogBackend::Memory => {
            tracing::info!("catalog backend: in-memory (volatile)");
            let cat = Arc::new(basin_catalog::InMemoryCatalog::new());
            (cat.clone(), cat)
        }
        CatalogBackend::Postgres { url, schema } => {
            tracing::info!(%schema, "catalog backend: postgres (durable)");
            let cat = basin_catalog::PostgresCatalog::connect_with_schema(url, schema)
                .await
                .with_context(|| format!("connect postgres catalog at {url}"))?;
            let cat = Arc::new(cat);
            (cat.clone(), cat)
        }
    };

    // Optional WAL + shard owner. Constructed when BASIN_SHARD_ENABLED=1 so we
    // can ship the wedge-deepening change incrementally without breaking demos
    // that don't have a writable WAL directory available.
    //
    // A handle to the raft WAL (when `BASIN_WAL_MODE=raft`) is kept alongside
    // the shard handles so the admin status surface + startup logging can read
    // cluster status, and so shutdown can close the raft node cleanly.
    let mut shard_handles: Option<(
        basin_shard::Shard,
        basin_shard::ShardBackgroundHandle,
        Arc<dyn basin_wal::Wal>,
    )> = None;
    // Raft handle for the admin status surface. `None` in local mode.
    let mut raft_wal: Option<Arc<basin_wal::RaftWal>> = None;
    let shard_for_engine: Option<basin_shard::Shard> = if cfg.shard_enabled {
        // Select the WAL durability backend (commit 6). `local` is the
        // unchanged file-backed WAL; `raft` is the replicated WAL.
        let wal: Arc<dyn basin_wal::Wal> = match cfg.wal_mode {
            WalMode::Local => {
                let wal_store = build_wal_object_store(&cfg)?;
                Arc::new(
                    basin_wal::LocalWal::open(basin_wal::WalConfig {
                        object_store: wal_store,
                        root_prefix: cfg.wal_root_prefix.clone(),
                        flush_interval: std::time::Duration::from_millis(200),
                        flush_max_bytes: 1024 * 1024,
                        // Group-commit window for `SET basin.synchronous_commit = on`
                        // appends. Default 2 ms (`BASIN_WAL_COMMIT_DELAY_MS`).
                        commit_delay: basin_wal::WalConfig::default_commit_delay(),
                    })
                    .await
                    .context("open WAL")?,
                )
            }
            WalMode::Raft => {
                let raft = build_raft_wal(&cfg).await?;
                let arc = Arc::new(raft);
                // Stash a typed handle for the admin status surface + logging
                // before we erase it behind `dyn Wal`.
                raft_wal = Some(arc.clone());
                arc
            }
        };

        let mut shard_cfg =
            basin_shard::ShardConfig::new(storage.clone(), catalog.clone(), wal.clone());
        // Multi-node phase 1 (ADR 0023): with BASIN_LEASE_MODE=required the
        // catalog's lease registry is wired into the shard and single-writer
        // becomes enforced — writes only proceed under a held writer lease
        // (typed `LeaseNotHeld` refusal otherwise; reads continue). The
        // renewal heartbeat rides the shard background loop spawned below,
        // same as eviction + compaction. `off` (the default) changes nothing.
        //
        // PRECEDENCE (commit 6): in raft mode the raft leader IS the single
        // writer (see `RaftWal`'s leader fence), so the lease fence is
        // redundant. We still allow both knobs to be set — the raft fence
        // refuses a follower's write before the lease is ever consulted — but
        // we log that the lease is subsumed by raft leadership so operators
        // aren't surprised that the lease registry is effectively a no-op.
        if cfg.lease_mode == basin_shard::LeaseMode::Required {
            if cfg.wal_mode == WalMode::Raft {
                tracing::info!(
                    "lease mode: required — but BASIN_WAL_MODE=raft is set; \
                     raft leadership supersedes the writer lease as the write \
                     fence (a non-leader write is refused before the lease is \
                     consulted). The lease registry is wired but redundant."
                );
            }
            let replica_id = cfg
                .replica_id
                .clone()
                .unwrap_or_else(|| shard_cfg.replica_id.clone());
            shard_cfg = shard_cfg
                .with_lease_registry(lease_registry.clone(), replica_id)
                .with_lease_mode(basin_shard::LeaseMode::Required);
            tracing::info!(
                replica_id = %shard_cfg.replica_id,
                lease_ttl_secs = shard_cfg.lease_ttl.as_secs(),
                lease_renew_secs = shard_cfg.lease_renew_interval.as_secs(),
                "lease mode: required — writer leases enforced via catalog registry"
            );
        }
        let shard = basin_shard::Shard::new(shard_cfg);
        let bg = shard.spawn_background();
        tracing::info!(
            wal_dir = %cfg.wal_dir.display(),
            wal_mode = ?cfg.wal_mode,
            "shard owner enabled; INSERTs will route through WAL + compactor"
        );
        let to_engine = shard.clone();
        shard_handles = Some((shard, bg, wal));
        Some(to_engine)
    } else {
        None
    };

    // Raft cluster startup status (commit 6 observability). Logged once the
    // node has had a moment to (self-)elect; non-fatal if it never converges
    // here (the background raft loop keeps trying).
    if let Some(ref raft) = raft_wal {
        log_initial_cluster_status(raft).await;
    }

    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: shard_for_engine,
    });

    // Feature 1 (5.11.W6): install the catalog-backed FunctionInvoker so that
    // LANGUAGE javascript (and LANGUAGE wasm component-model) functions stored
    // in the catalog are actually executed when /fn/v1/:name is called.
    // Requires the `wasm-fn` feature which pulls in basin-fn / wasmtime.
    #[cfg(feature = "wasm-fn")]
    {
        fn_runtime::install_catalog_invoker(catalog.clone());
    }

    // --- optional realtime sink (Phase 5.11.R1, ADR 0018) -------------------
    //
    // Attach *before* the first connection is accepted so no events slip
    // through the window between engine construction and sink registration.
    // The sink is a no-op until at least one SSE/WebSocket subscriber calls
    // `RealtimeSink::registry().subscribe(key)` (R2/R3).
    #[cfg(feature = "realtime")]
    let realtime_sink = {
        let sink = basin_realtime::RealtimeSink::new();
        engine.attach_post_commit_sink(std::sync::Arc::new(sink.clone()));
        tracing::info!("basin-realtime post-commit sink attached");
        sink
    };

    // --- durable CDC ring sink (ADR 0028 Phase 1) ---------------------------
    //
    // Attached as a SECOND post-commit sink alongside realtime, over the same
    // capture seam (`dispatch_post_commit`). This requires NO executor change:
    // the post-commit hook already fires for every committed mutation,
    // including the HTAP hot-tier UPDATE/DELETE fast paths that bypass the WAL.
    // The writer batches events to a durable per-project object-store ring; the
    // co-mounted SSE route (GET /v1/cdc/:project/stream) replays from the ring
    // then live-tails. The time-window flusher and retention GC run as
    // background tasks for the process lifetime.
    #[cfg(feature = "realtime")]
    let cdc_sink = {
        let sink = basin_cdc::CdcRingWriter::new(engine.config().storage.clone());
        engine.attach_post_commit_sink(std::sync::Arc::new(sink.clone()));
        // Background tasks run for the process lifetime. Dropping a tokio
        // JoinHandle detaches (does not cancel) the task, so the flusher keeps
        // flushing and the GC keeps sweeping; we deliberately do not retain the
        // handles. (`spawn_retention_gc` returns a guard whose `abort()` is the
        // only way to stop the sweep — we want it to run forever here.)
        let _flusher = sink.spawn_flusher();
        std::mem::forget(basin_cdc::spawn_retention_gc(sink.clone()));
        std::mem::forget(_flusher);
        // >>> ADR 0028 Phase 2 CDC-WEBHOOK SUPERVISOR (anchored, flagged) >>>
        // Process-wide webhook push supervisor: discovers projects with a CDC
        // ring and runs one isolated per-endpoint delivery worker per
        // registered webhook (catalog-backed subscriptions + cursor). Runs for
        // the process lifetime; detached like the flusher / GC above.
        std::mem::forget(basin_cdc::spawn_webhook_supervisor(
            sink.clone(),
            catalog.clone(),
            basin_cdc::WebhookConfig::from_env(),
        ));
        tracing::info!(
            "basin-cdc post-commit sink attached (durable ring + retention GC + webhook push)"
        );
        // <<< ADR 0028 Phase 2 CDC-WEBHOOK SUPERVISOR (anchored, flagged) <<<
        sink
    };

    // Build the static resolver from `BASIN_PROJECTS`.
    //
    // SECURITY: `StaticProjectResolver` accepts ANY password (its
    // `ProjectResolver` impl falls through to the trait's default
    // `resolve_credentials`, which drops the password slot). The previous
    // default `BASIN_PROJECTS=alice=*` therefore made the static map an
    // open relay for `user=alice` when combined with a public pgwire
    // listener. We now default `BASIN_PROJECTS` to empty (see config
    // parse), and only mount the static resolver into the stack below
    // when at least one user mapping was explicitly provided.
    let static_resolver_has_entries = !cfg.projects.is_empty();
    let mut static_resolver = StaticProjectResolver::default();
    for (user, project) in cfg.projects {
        tracing::info!(%user, %project, "project registered");
        static_resolver = static_resolver.with_entry(user, project);
    }

    // --- optional auth service (ADR 0018: `auth` feature) -------------------
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
    //
    // When compiled without the `auth` feature (minimal build), this entire
    // block is elided — no basin-auth dependency at link time.
    #[cfg(feature = "auth")]
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
    // In the minimal build (no `auth` feature), auth_service is a typed
    // placeholder so downstream code that uses `if let Some(ref auth) = auth_service`
    // compiles away cleanly.
    #[cfg(not(feature = "auth"))]
    let auth_service: Option<std::convert::Infallible> = None;

    // Migrate legacy pgwire credentials to the new self-routing format.
    // This is safe to run on every startup — `list_legacy_credentials`
    // returns an empty list once all rows have been rotated, so subsequent
    // runs are instant no-ops. Failures warn but do not abort startup:
    // old-format credentials remain valid during the transition window.
    #[cfg(feature = "auth")]
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
    // In the minimal build (no `auth` feature), only the static resolver is used.
    //
    // SECURITY: the static resolver is only appended when an operator
    // explicitly populated `BASIN_PROJECTS`. Empty map => no static fallback
    // (otherwise an attacker who lands on `user=<any-static-mapping>` would
    // bypass credential / JWT / API-key auth via the trait's default
    // `resolve_credentials` that drops the password slot).
    #[cfg(feature = "auth")]
    let project_resolver: Arc<dyn ProjectResolver> = if let Some(ref auth) = auth_service {
        let mut stack: Vec<Arc<dyn ProjectResolver>> = vec![
            Arc::new(ProjectCredentialsResolver::new(auth.clone())),
            Arc::new(JwtProjectResolver::new(auth.clone())),
            Arc::new(ApiKeyProjectResolver::new(auth.clone())),
        ];
        if static_resolver_has_entries {
            tracing::info!("pgwire resolver: credentials + JWT + API-key + static");
            stack.push(Arc::new(static_resolver));
        } else {
            tracing::info!("pgwire resolver: credentials + JWT + API-key (static disabled)");
        }
        Arc::new(StackedProjectResolver::new(stack))
    } else {
        tracing::info!("pgwire resolver: static (auth disabled)");
        Arc::new(static_resolver)
    };
    #[cfg(not(feature = "auth"))]
    let project_resolver: Arc<dyn ProjectResolver> = {
        tracing::info!("pgwire resolver: static (auth feature not compiled)");
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
    // Per-project pgwire connection ceiling enforcement (issue #28b).
    // CatalogConnectionLimitProvider reads the ceiling on every new connect;
    // the admin route (POST /admin/v1/projects/:id/max-connections) writes it.
    // Fail-closed: a project with no stored ceiling gets 25 (the Free tier).
    let connection_limiter = {
        let provider = CatalogConnectionLimitProvider::new(catalog.clone());
        Some(Arc::new(ConnectionLimiter::new(Arc::new(provider))))
    };
    let server_cfg = ServerConfig {
        bind_addr: cfg.bind,
        engine: engine.clone(),
        project_resolver,
        pool,
        shard_endpoints: None,
        tls,
        connection_limiter,
    };
    let router = basin_router::run_until_bound(server_cfg)
        .await
        .context("basin-router bind failed")?;
    tracing::info!(bind = %router.local_addr, "pgwire listener is accept-ready");

    // --- optional REST listener (ADR 0018: `rest` feature) ------------------
    //
    // Per ADR 0006: REST requires AUTH. We refuse to bring up the HTTP
    // listener without an `AuthService` — that combination is the largest
    // data-leak class in this stack.
    //
    // The `rest` feature implies `auth` in Cargo.toml, so `auth_service` is
    // always `Option<Arc<AuthService>>` (never Infallible) when `rest` is on.
    #[cfg(feature = "rest")]
    let mut rest_handle: Option<basin_rest::RunningRest> = None;
    #[cfg(not(feature = "rest"))]
    let mut rest_handle: Option<std::convert::Infallible> = None;
    #[cfg(feature = "rest")]
    if cfg.rest_enabled {
        let auth = auth_service.clone().ok_or_else(|| {
            anyhow!("BASIN_REST_ENABLED=1 requires BASIN_AUTH_ENABLED=1 (per ADR 0006)")
        })?;
        let rest_cfg = basin_rest::RestConfig::new(cfg.rest_bind, engine.clone(), auth.clone());
        let mut svc = basin_rest::RestService::new(rest_cfg);
        // fn-persist: wire the live catalog into RestService so handler
        // function deploys (LANGUAGE wasm/javascript) survive server restarts.
        // Shares the same Arc<dyn Catalog> the engine + fn_runtime already use;
        // no extra connection needed.
        svc.with_fn_catalog(catalog.clone());
        // Multi-node raft (commit 6): hand the raft WAL handle to the REST
        // service so `GET /admin/v1/cluster` can serve live cluster status.
        //
        // INTEGRATION CONTRACT (TODO — finalise against basin-rest's admin
        // route idiom; the fn-persist mirror shows the `with_*` builder +
        // `Inner` field pattern this mirrors). The cheap, self-contained
        // surface that does NOT depend on the c5 wire commit:
        //   - `RestService::with_cluster_status(handle: Arc<dyn ClusterStatusProvider>)`
        //     where `ClusterStatusProvider::status() -> ClusterStatus` is a
        //     1-method trait `basin-wal::RaftWal` implements (it already has
        //     `cluster_status()`).
        //   - `admin_routes::cluster_status` renders the `ClusterStatus` JSON
        //     at `GET /admin/v1/cluster`, mirroring `admin_functions`' shape.
        // Until that builder lands in basin-rest, the status surface is the
        // startup + role-change LOG (load-bearing observability), and this
        // block is a documented seam. When wiring it, gate on
        // `if let Some(ref raft) = raft_wal { svc.with_cluster_status(raft.clone()); }`.
        let _ = &raft_wal; // keep the handle live for the seam above + shutdown.
        // Feature 2: co-mount realtime SSE + WS on the REST port when both
        // features are compiled in and an auth service is available. The
        // standalone BASIN_REALTIME_BIND / BASIN_REALTIME_WS_BIND ports are
        // still started below for clients that prefer dedicated ports.
        #[cfg(feature = "realtime")]
        svc.attach_realtime(basin_rest::RealtimeCoMount {
            registry: realtime_sink.registry().clone(),
            auth: auth.clone(),
            replay_rings: Some(realtime_sink.replay_rings().clone()),
        });
        // ADR 0028 Phase 1: co-mount the durable CDC SSE stream on the REST
        // port. Shares the engine Storage (durable replay) + the sink's live
        // registry (fast-path tail).
        #[cfg(feature = "realtime")]
        svc.attach_cdc(basin_rest::CdcCoMount {
            storage: engine.config().storage.clone(),
            live: cdc_sink.live().clone(),
            auth: auth.clone(),
        });
        let running = svc
            .run_until_bound()
            .await
            .map_err(|e| anyhow!("basin-rest bind failed: {e}"))?;
        tracing::info!(bind = %running.local_addr, "basin-rest listening (realtime co-mounted)");
        rest_handle = Some(running);
    }

    // --- optional realtime SSE listener (Phase 5.11.R2, ADR 0018) ------------
    //
    // Requires auth (same invariant as basin-rest per ADR 0006). Binds on
    // BASIN_REALTIME_BIND (default 127.0.0.1:5435) and serves
    // GET /realtime/v1/sse/:project/:table.
    //
    // The `sse_serve` function is self-contained inside `basin-realtime`:
    // `basin-server` never imports `axum` directly.
    #[cfg(feature = "realtime")]
    let _realtime_handle: Option<tokio::task::JoinHandle<()>> =
        if let Some(ref auth) = auth_service {
            let realtime_bind: std::net::SocketAddr = std::env::var("BASIN_REALTIME_BIND")
                .unwrap_or_else(|_| "127.0.0.1:5435".to_string())
                .parse()
                .context("BASIN_REALTIME_BIND must be host:port")?;
            let handle = basin_realtime::sse_serve(
                realtime_bind,
                realtime_sink.registry().clone(),
                auth.clone(),
            )
            .await
            .with_context(|| format!("basin-realtime bind {realtime_bind} failed"))?;
            tracing::info!(bind = %realtime_bind, "basin-realtime SSE listener is accept-ready");
            Some(handle)
        } else {
            tracing::info!(
                "basin-realtime: SSE listener not started (requires BASIN_AUTH_ENABLED=1)"
            );
            None
        };
    #[cfg(not(feature = "realtime"))]
    let _realtime_handle: Option<tokio::task::JoinHandle<()>> = None;

    // --- optional realtime WebSocket listener (Phase 5.11.R3, ADR 0018) ----
    //
    // Mounts the multiplexed WS handler at `GET /realtime/v1/ws/:project` on
    // BASIN_REALTIME_WS_BIND (default 127.0.0.1:5436).  Auth is identical to
    // R2's SSE listener — same JWT, same cross-project isolation invariant.
    //
    // `ws_serve` is self-contained inside `basin-realtime`; `basin-server`
    // never imports `axum` or `tokio-tungstenite` directly.
    #[cfg(feature = "realtime")]
    let _realtime_ws_handle: Option<tokio::task::JoinHandle<()>> =
        if let Some(ref auth) = auth_service {
            let ws_bind: std::net::SocketAddr = std::env::var("BASIN_REALTIME_WS_BIND")
                .unwrap_or_else(|_| "127.0.0.1:5436".to_string())
                .parse()
                .context("BASIN_REALTIME_WS_BIND must be host:port")?;
            let handle = basin_realtime::ws_serve(
                ws_bind,
                realtime_sink.registry().clone(),
                auth.clone(),
            )
            .await
            .with_context(|| format!("basin-realtime WS bind {ws_bind} failed"))?;
            tracing::info!(bind = %ws_bind, "basin-realtime WS listener is accept-ready");
            Some(handle)
        } else {
            tracing::info!(
                "basin-realtime: WS listener not started (requires BASIN_AUTH_ENABLED=1)"
            );
            None
        };
    #[cfg(not(feature = "realtime"))]
    let _realtime_ws_handle: Option<tokio::task::JoinHandle<()>> = None;

    // Wait for Ctrl-C, then signal the router to stop.
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
    let _ = router.shutdown.send(());

    // Tear down REST first so requests in flight don't survive past the
    // router; then the eviction loop; then router; then shard / WAL. The
    // shard / WAL ordering is unchanged from before this PR.
    #[cfg(feature = "rest")]
    if let Some(rest) = rest_handle.take() {
        tracing::info!("stopping basin-rest");
        let _ = rest.shutdown.send(());
        // Awaiting the join tells us the accept loop actually exited.
        let _ = rest.join.await;
    }
    #[cfg(not(feature = "rest"))]
    drop(rest_handle);

    if let Some(h) = eviction_handle.take() {
        tracing::info!("stopping basin-pool eviction loop");
        h.shutdown().await;
    }

    let router_result = router.join.await.map_err(|e| anyhow!("router join: {e}"))?;

    if let Some((shard, bg, wal)) = shard_handles.take() {
        tracing::info!("draining shard background loops");
        bg.shutdown().await;

        // DURABILITY (graceful-shutdown drain): before we close the WAL, force
        // every durability path that is NOT covered by the WAL to land.
        //
        // 1. Hot-tier UPDATE/DELETE overlay (the auto-commit point fast paths)
        //    is registry-only — it is never WAL-logged — so a graceful restart
        //    would otherwise lose the last reconcile-window of acked point
        //    mutations. `drain_overlays_for_shutdown` materializes every
        //    pending overlay into cold Parquet + commits the catalog. (A
        //    non-graceful crash window remains; see the method's doc-comment.)
        //
        // 2. The shard's in-memory INSERT tail IS covered by the WAL, but
        //    draining it here too means a graceful restart starts from a clean,
        //    fully-compacted state (the catalog commit is durable; the WAL is
        //    then truncated through the drained LSN by `flush_to_parquet`).
        //
        // Order matters: overlays first (they read the cold base the tail flush
        // is about to extend; a foreground reconcile would otherwise have to
        // re-run), then the tail flush, then `wal.close()`.
        tracing::info!("draining hot-tier overlays to durable storage");
        if let Err(e) = engine.drain_overlays_for_shutdown().await {
            tracing::warn!(error = %e, "shutdown overlay drain failed");
        }
        tracing::info!("flushing shard tail to Parquet");
        if let Err(e) = shard.flush_to_parquet().await {
            tracing::warn!(error = %e, "shutdown shard tail flush failed");
        }

        tracing::info!("closing WAL");
        if let Err(e) = wal.close().await {
            tracing::warn!(error = %e, "WAL close failed");
        }
    }
    // Drop the raft handle after the shard/WAL teardown (its `Arc` is the same
    // node `wal.close()` already shut down; the explicit drop documents the
    // lifetime so the handle is not held past shutdown).
    drop(raft_wal);

    // `auth_service` is dropped at the end of `main` — its `Arc` is the only
    // lifeline, so nothing to do explicitly.
    drop(auth_service); // no-op in minimal build (Option<Infallible>)

    router_result.map_err(|e| anyhow!("router exited: {e}"))?;
    Ok(())
}

/// Multi-node raft (commit 6): WAL durability backend selector
/// (`BASIN_WAL_MODE`). `Local` is the unchanged single-node file-backed WAL;
/// `Raft` is the replicated WAL whose durability boundary is a quorum ack.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WalMode {
    Local,
    Raft,
}

impl WalMode {
    fn from_env() -> Result<Self> {
        match std::env::var("BASIN_WAL_MODE")
            .ok()
            .map(|s| s.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("") | Some("local") => Ok(WalMode::Local),
            Some("raft") => Ok(WalMode::Raft),
            Some(other) => Err(anyhow!(
                "BASIN_WAL_MODE must be 'local' or 'raft', got {other:?}"
            )),
        }
    }
}

/// Parsed `BASIN_RAFT_PEERS` entry: `id@host:port`.
#[derive(Clone, Debug)]
struct RaftPeer {
    id: u64,
    addr: String,
}

/// Raft cluster topology parsed from the env surface, populated only in raft
/// mode. Mirrors the knobs documented in the module header.
#[derive(Clone, Debug)]
struct RaftCfg {
    node_id: u64,
    bind: String,
    peers: Vec<RaftPeer>,
    bootstrap: bool,
}

/// Build the [`basin_wal::RaftWal`] for `BASIN_WAL_MODE=raft`.
///
/// Opens the raft storage under `${BASIN_WAL_DIR}/raft`, constructs the node
/// with the parsed topology, and joins-or-bootstraps the cluster:
///   - if `BASIN_RAFT_BOOTSTRAP=1` AND the raft log is empty, this node
///     `initialize`s the cluster with the full peer set, then awaits its own
///     election;
///   - otherwise the node awaits leader contact (a peer will replicate the
///     membership + log to it).
///
/// INTEGRATION CONTRACT (TODO — finalise against the concurrent commits):
///   - **c5 (tonic network, `/tmp/basin_raft_c5`)**: the wire `RaftNetwork`
///     factory. Until c5 lands, `RaftWal::new` uses its built-in (Sim)
///     factory; this function passes the parsed `bind` / `peers` through
///     `RaftWalConfig` so that when c5's selector lands, the only change here
///     is choosing the tonic factory. The `peers`' `host:port` strings are
///     already the wire addresses c5 needs.
///   - **c34 (DurabilityBackend, `/tmp/basin_raft_c34`)**: the shard's
///     `mark_range_durable` routes through `DurabilityBackend::{Local,Raft}`.
///     This function only supplies the raft `Arc<dyn Wal>`; the shard selects
///     the backend internally from the WAL impl type (or a `BASIN_WAL_MODE`
///     read of its own — c34's APPLY.md is authoritative). `durable_lsn`
///     advancing on quorum is c34's contract; raft's `append` already returns
///     only after quorum commit, which is the signal c34 consumes.
async fn build_raft_wal(cfg: &Cfg) -> Result<basin_wal::RaftWal> {
    let rcfg = cfg
        .raft
        .as_ref()
        .ok_or_else(|| anyhow!("BASIN_WAL_MODE=raft but raft config missing (internal error)"))?;

    // Raft state lives under a dedicated subdir so it never collides with the
    // file-WAL segments (which the local-mode WAL writes under BASIN_WAL_DIR).
    let raft_dir = cfg.wal_dir.join("raft");
    std::fs::create_dir_all(&raft_dir)
        .with_context(|| format!("create raft dir {}", raft_dir.display()))?;

    // initial_members: every peer (voters). The node's own id must be present.
    let mut initial_members: BTreeMap<u64, String> = BTreeMap::new();
    for p in &rcfg.peers {
        initial_members.insert(p.id, p.addr.clone());
    }
    let peer_addrs: Vec<String> = rcfg.peers.iter().map(|p| p.addr.clone()).collect();

    let wal_cfg = basin_wal::RaftWalConfig::new(peer_addrs, rcfg.bind.clone(), raft_dir)
        .with_node_id(rcfg.node_id)
        .with_initial_members(initial_members.clone());

    tracing::info!(
        node_id = rcfg.node_id,
        bind = %rcfg.bind,
        peers = rcfg.peers.len(),
        bootstrap = rcfg.bootstrap,
        "raft WAL: opening node"
    );

    // Network selection (multi-node commit 5). With the `raft-net` feature the
    // node talks to peers over the tonic gRPC transport: build the wal with the
    // `TonicNetworkFactory` over the static peer registry, then start the
    // transport server on the bind addr so peers can reach this node. Without
    // the feature (or in a single-process build) the in-process Sim factory is
    // used, so raft mode stays observable + leader-fenced without the wire.
    #[cfg(feature = "raft-net")]
    let wal = {
        let peer_spec = rcfg
            .peers
            .iter()
            .map(|p| format!("{}@{}", p.id, p.addr))
            .collect::<Vec<_>>()
            .join(",");
        let peers = std::sync::Arc::new(
            basin_wal::StaticPeers::parse(&peer_spec)
                .map_err(|e| anyhow!("parse BASIN_RAFT_PEERS for transport: {e}"))?,
        );
        let factory = basin_wal::TonicNetworkFactory::with_shared(
            peers,
            basin_wal::TonicNetworkConfig::from_env(),
        );
        let wal = basin_wal::RaftWal::new_with_network(wal_cfg, factory)
            .await
            .map_err(|e| anyhow!("open RaftWal (tonic transport): {e}"))?;

        let bind_addr: std::net::SocketAddr = rcfg
            .bind
            .parse()
            .with_context(|| format!("BASIN_RAFT_BIND '{}' is not a socket addr", rcfg.bind))?;
        let svc = basin_wal::RaftTransportService::new(wal.raft().clone());
        // The server task runs for the lifetime of the process; the join handle
        // is intentionally detached (the OS reclaims the socket on exit, and
        // graceful shutdown drops the RaftWal which stops the raft core). The
        // bound addr is logged for operator confirmation.
        let (bound, _server) = basin_wal::serve_raft(svc, bind_addr)
            .await
            .map_err(|e| anyhow!("start raft transport on {}: {e}", rcfg.bind))?;
        tracing::info!(bind = %bound, "raft transport server listening (tonic/gRPC)");
        wal
    };
    #[cfg(not(feature = "raft-net"))]
    let wal = basin_wal::RaftWal::new(wal_cfg)
        .await
        .map_err(|e| anyhow!("open RaftWal: {e}"))?;

    // Join-or-bootstrap. Bootstrap only when explicitly designated AND the
    // log is empty (so a restart of the bootstrap node does NOT re-initialize
    // — `initialize` is idempotent via the NotAllowed branch in RaftWal, but
    // we still gate on the log being empty to keep intent clear and to avoid a
    // spurious membership churn). `last_log_index == None` ⇒ empty log.
    let status = wal.cluster_status().await;
    let log_empty = status.last_log_index == 0 && status.commit_index == 0;
    if rcfg.bootstrap && log_empty {
        tracing::info!(
            node_id = rcfg.node_id,
            members = initial_members.len(),
            "raft WAL: bootstrapping cluster (BASIN_RAFT_BOOTSTRAP=1, empty log)"
        );
        // `initialize_addrs` builds the openraft `BasicNode` map internally,
        // so basin-server never depends on openraft directly.
        wal.initialize_addrs(initial_members.clone())
            .await
            .map_err(|e| anyhow!("raft initialize: {e}"))?;
    } else if rcfg.bootstrap {
        tracing::info!(
            node_id = rcfg.node_id,
            "raft WAL: BASIN_RAFT_BOOTSTRAP=1 but log is non-empty; skipping initialize (recovering)"
        );
    } else {
        tracing::info!(
            node_id = rcfg.node_id,
            "raft WAL: awaiting leader contact (not the bootstrap node)"
        );
    }

    Ok(wal)
}

/// Log this node's view of the cluster shortly after startup. Best-effort: we
/// poll cluster status for a brief window so the first log line reflects a
/// (likely) converged state instead of the pre-election transient. Never
/// fatal — the raft background loop keeps the node live regardless.
async fn log_initial_cluster_status(raft: &basin_wal::RaftWal) {
    use std::time::{Duration, Instant};
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = raft.cluster_status().await;
        if status.leader_id.is_some() || Instant::now() > deadline {
            tracing::info!(
                node_id = status.node_id,
                local_id = %status.local_id,
                role = ?status.role,
                term = status.term,
                commit_index = status.commit_index,
                last_log_index = status.last_log_index,
                leader_id = ?status.leader_id,
                members = status.members.len(),
                "raft cluster status"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

struct Cfg {
    bind: SocketAddr,
    data_dir: PathBuf,
    wal_dir: PathBuf,
    shard_enabled: bool,
    pool_enabled: bool,
    /// Multi-node phase 1 (ADR 0023): `BASIN_LEASE_MODE`. `Required` wires
    /// the catalog's `LeaseRegistry` into the shard so single-writer is
    /// enforced (writes refused without a held lease); `Off` (default) is
    /// the unchanged single-replica behaviour.
    lease_mode: basin_shard::LeaseMode,
    /// `BASIN_REPLICA_ID` — stable lease holder id for this process. `None`
    /// falls back to basin-shard's per-process default (`host:pid:salt`).
    replica_id: Option<String>,
    /// Multi-node raft (commit 6): `BASIN_WAL_MODE`. `Raft` replaces the
    /// file-backed WAL with the replicated WAL; `Local` (default) is
    /// unchanged. See [`WalMode`] + the module header for precedence rules.
    wal_mode: WalMode,
    /// Raft cluster topology — `Some` only in raft mode. Parsed from
    /// `BASIN_NODE_ID` / `BASIN_RAFT_BIND` / `BASIN_RAFT_PEERS` /
    /// `BASIN_RAFT_BOOTSTRAP`.
    raft: Option<RaftCfg>,
    // ADR 0018: auth/rest fields compiled away in minimal build.
    #[cfg(feature = "auth")]
    auth_enabled: bool,
    #[cfg(feature = "rest")]
    rest_enabled: bool,
    #[cfg(feature = "rest")]
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
        // BASIN_LEASE_MODE (multi-node phase 1, ADR 0023). Strict parse: a
        // typo'd enforcement knob must not silently mean "off".
        let lease_mode = basin_shard::LeaseMode::from_env().map_err(|e| anyhow!(e))?;
        if lease_mode == basin_shard::LeaseMode::Required && !shard_enabled {
            // Same refuse-to-start idiom as BASIN_REST_ENABLED without auth:
            // the legacy synchronous Parquet write path has no lease seam,
            // so "required" enforcement would be silently meaningless.
            return Err(anyhow!(
                "BASIN_LEASE_MODE=required requires BASIN_SHARD_ENABLED=1 \
                 (writer leases fence the WAL write path through basin-shard)"
            ));
        }
        let replica_id = std::env::var("BASIN_REPLICA_ID")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        // BASIN_WAL_MODE (multi-node raft, commit 6). Strict parse + config
        // validation mirroring the lease-mode / rest-requires-auth idiom.
        let wal_mode = WalMode::from_env()?;
        let raft = if wal_mode == WalMode::Raft {
            if !shard_enabled {
                return Err(anyhow!(
                    "BASIN_WAL_MODE=raft requires BASIN_SHARD_ENABLED=1 \
                     (the replicated WAL is the shard's durability backend)"
                ));
            }
            Some(parse_raft_env()?)
        } else {
            None
        };

        #[cfg(feature = "auth")]
        let auth_enabled = bool_env("BASIN_AUTH_ENABLED");
        #[cfg(feature = "rest")]
        let rest_enabled = bool_env("BASIN_REST_ENABLED");
        #[cfg(feature = "rest")]
        let rest_bind: SocketAddr = std::env::var("BASIN_REST_BIND")
            .unwrap_or_else(|_| "127.0.0.1:5434".to_string())
            .parse()
            .context("BASIN_REST_BIND must be host:port")?;
        // BASIN_PROJECTS — static `user=project` map for the dev / bootstrap
        // resolver. Default is **empty**: a previous default of `"alice=*"`
        // silently provisioned a `user=alice` mapping that the static
        // resolver would then accept with ANY password (the trait's
        // default `resolve_credentials` impl drops the password slot).
        // Combined with a public pgwire listener, that silently turned any
        // basin-server into an open relay for `user=alice`. Empty default
        // closes the hole; operators (and dev scripts like
        // `basin-cloud/scripts/local-engine.sh`) that need the static map
        // still set `BASIN_PROJECTS` explicitly.
        let raw = std::env::var("BASIN_PROJECTS").unwrap_or_default();
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
            tracing::info!(
                "BASIN_PROJECTS not set — static resolver disabled (cred/JWT/API-key only)"
            );
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
            lease_mode,
            replica_id,
            wal_mode,
            raft,
            #[cfg(feature = "auth")]
            auth_enabled,
            #[cfg(feature = "rest")]
            rest_enabled,
            #[cfg(feature = "rest")]
            rest_bind,
            projects,
            catalog,
            storage_root_prefix,
            wal_root_prefix,
        })
    }
}

/// Parse the raft env surface (`BASIN_WAL_MODE=raft` only). Hard errors on
/// every misconfiguration so a half-wired raft node refuses to start.
fn parse_raft_env() -> Result<RaftCfg> {
    let node_id: u64 = std::env::var("BASIN_NODE_ID")
        .map_err(|_| anyhow!("BASIN_WAL_MODE=raft requires BASIN_NODE_ID (this node's u64 id)"))?
        .trim()
        .parse()
        .context("BASIN_NODE_ID must be a positive u64")?;
    if node_id == 0 {
        return Err(anyhow!("BASIN_NODE_ID must be > 0"));
    }

    let bind = std::env::var("BASIN_RAFT_BIND")
        .map_err(|_| anyhow!("BASIN_WAL_MODE=raft requires BASIN_RAFT_BIND (host:port)"))?
        .trim()
        .to_string();
    if bind.is_empty() {
        return Err(anyhow!("BASIN_RAFT_BIND must not be empty"));
    }

    let peers_raw = std::env::var("BASIN_RAFT_PEERS").map_err(|_| {
        anyhow!("BASIN_WAL_MODE=raft requires BASIN_RAFT_PEERS (id@host:port,...)")
    })?;
    let mut peers = Vec::new();
    for entry in peers_raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (id_str, addr) = entry.split_once('@').ok_or_else(|| {
            anyhow!("bad BASIN_RAFT_PEERS entry {entry:?} (want id@host:port)")
        })?;
        let id: u64 = id_str
            .trim()
            .parse()
            .with_context(|| format!("bad peer id in BASIN_RAFT_PEERS entry {entry:?}"))?;
        let addr = addr.trim().to_string();
        if id == 0 || addr.is_empty() {
            return Err(anyhow!("bad BASIN_RAFT_PEERS entry {entry:?}"));
        }
        peers.push(RaftPeer { id, addr });
    }
    if peers.is_empty() {
        return Err(anyhow!("BASIN_RAFT_PEERS must list at least this node"));
    }
    // This node's id must appear in the peer set, and its advertised address
    // must match BASIN_RAFT_BIND so the cluster's membership view is coherent.
    match peers.iter().find(|p| p.id == node_id) {
        None => {
            return Err(anyhow!(
                "BASIN_NODE_ID={node_id} is not present in BASIN_RAFT_PEERS"
            ));
        }
        Some(self_peer) if self_peer.addr != bind => {
            return Err(anyhow!(
                "BASIN_NODE_ID={node_id}'s address in BASIN_RAFT_PEERS ({}) \
                 does not match BASIN_RAFT_BIND ({bind})",
                self_peer.addr
            ));
        }
        Some(_) => {}
    }
    // Reject duplicate ids — a typo'd peer list must not silently collapse two
    // logical nodes into one.
    let mut seen = std::collections::HashSet::new();
    for p in &peers {
        if !seen.insert(p.id) {
            return Err(anyhow!("duplicate node id {} in BASIN_RAFT_PEERS", p.id));
        }
    }

    let bootstrap = bool_env("BASIN_RAFT_BOOTSTRAP");

    Ok(RaftCfg {
        node_id,
        bind,
        peers,
        bootstrap,
    })
}

fn bool_env(name: &str) -> bool {
    matches!(
        std::env::var(name).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

/// Like [`bool_env`] but defaults to `true` when the var is unset/empty. Used
/// for durability knobs whose safe default is ON (e.g. `BASIN_DATA_FSYNC`):
/// an explicit `0` / `false` / `off` opts out; anything else (including unset)
/// stays on.
fn bool_env_default_on(name: &str) -> bool {
    match std::env::var(name) {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        ),
        Err(_) => true,
    }
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
            // DURABILITY (data-store fsync): `LocalFileSystem`'s PUT is
            // write-temp + rename with NO fsync, so a power loss can vanish a
            // data file the fsync-durable catalog already references — after
            // the covering WAL was truncated. `FsyncOnPutAll` fsyncs every PUT
            // and every multipart completion (the WAL-only `FsyncOnPut` would
            // be a no-op here: the data writer never sets the `DurablePut`
            // marker it gates on). Default ON — durability is the default
            // story; data flushes are background work so the fsync barrier is
            // off the request latency path. Opt out with `BASIN_DATA_FSYNC=0`.
            let data_fsync = bool_env_default_on("BASIN_DATA_FSYNC");
            tracing::info!(
                backend = "local",
                data_dir = %cfg.data_dir.display(),
                root_prefix = ?cfg.storage_root_prefix,
                data_fsync,
                "storage object-store backend configured",
            );
            if data_fsync {
                Ok(Arc::new(basin_wal::FsyncOnPutAll::new(Arc::new(fs))))
            } else {
                Ok(Arc::new(fs))
            }
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
                fsync_on_durable_put = true,
                "WAL object-store backend configured",
            );
            // LocalFileSystem's PUT is write + rename with NO fsync, so a
            // bare local store cannot honor `synchronous_commit = on` across
            // a power loss. The FsyncOnPut wrapper fsyncs exactly the
            // segment PUTs the WAL marks as carrying a synchronous-commit
            // waiter (DurablePut extension); all other PUTs pass through
            // untouched. The S3/Tigris backends below are durable on PUT
            // and stay unwrapped.
            Ok(Arc::new(basin_wal::FsyncOnPut::new(Arc::new(fs))))
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
