//! Helpers for spinning up an N-shard Basin cluster against a single shared
//! storage backend.
//!
//! Two pieces:
//!
//! 1. [`SpawnedShard`] — one in-process basin-router listening on an
//!    ephemeral port, fronted by its own `basin-engine::Engine` whose
//!    `EngineConfig` shares the same `Storage` + catalog with all other
//!    shards in the cluster. Multiple shards over one `Storage`/`Catalog`
//!    instance is fine for v0.1: projects are partitioned by hash, so two
//!    different shards never read or write the same project prefix.
//! 2. [`SpawnedRouter`] — a separate basin-router bound to its own
//!    ephemeral port, configured in shard mode against the spawned shards'
//!    endpoints. Pgwire clients connect here.
//!
//! [`start_n_shards`] composes the two, returning the router's local
//! address plus the per-shard addresses (so tests can verify which shard
//! a request reached).
//!
//! ## Lifetime / cleanup
//!
//! The returned [`Cluster`] owns every spawned task and the underlying
//! storage tempdir. Dropping it stops all listeners and reclaims disk.
//!
//! This module is `pub` (not test-only) because integration tests live in
//! a separate crate and there is no good way to share `#[cfg(test)]`
//! helpers across crate boundaries. Production binaries don't import it.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use basin_common::{ProjectId, Result};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::sync::oneshot;

use crate::resolver::{ProjectResolver, StaticProjectResolver};
use crate::{run_until_bound, RunningServer, ServerConfig};

/// A single in-process Basin shard owner. Listens on an ephemeral port and
/// uses the supplied storage/catalog (shared with the other shards in the
/// cluster).
///
/// Each shard wraps the cluster-wide [`DynamicProjectResolver`] in a
/// `CountingResolver` so the test can read back, per-shard:
///
/// - total connection count
/// - per-project connection count
///
/// That gives the routing-stability test enough signal to verify (a) that
/// the same project always lands on the same shard, and (b) the load
/// distribution across shards.
pub struct SpawnedShard {
    pub addr: SocketAddr,
    /// Counter of authentications this shard has handled. Incremented every
    /// time the wrapped resolver is consulted — i.e. once per accepted
    /// pgwire connection.
    pub stats: Arc<ShardStats>,
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl SpawnedShard {
    /// Stop the listener and wait for it to drain.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(j) = self.join.take() {
            let _ = j.await;
        }
    }
}

impl Drop for SpawnedShard {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        // We deliberately don't await the join here — tests may drop the
        // cluster from a sync context. The tasks observe shutdown and exit
        // on their own; the tokio runtime cleans them up at suite tear-down.
    }
}

/// Front-of-the-cluster basin-router. Pgwire clients connect to
/// `addr`; queries fan out to the spawned shards behind it.
pub struct SpawnedRouter {
    pub addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl SpawnedRouter {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(j) = self.join.take() {
            let _ = j.await;
        }
    }
}

impl Drop for SpawnedRouter {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Bundle returned by [`start_n_shards`]. Hold onto it for the duration of
/// the test; dropping it tears the cluster down.
pub struct Cluster {
    pub router: SpawnedRouter,
    pub shards: Vec<SpawnedShard>,
    /// Endpoints in the same order as `shards`. Convenience for tests that
    /// want to map a shard index back to a string and vice versa.
    pub shard_endpoints: Vec<String>,
    /// Project resolver shared across the cluster. Tests insert
    /// `(username, ProjectId)` pairs here so each shard (and the router)
    /// resolves the same name to the same project.
    pub resolver: Arc<DynamicProjectResolver>,
    _data_dir: TempDir,
}

impl Cluster {
    pub fn router_addr(&self) -> SocketAddr {
        self.router.addr
    }

    pub fn shard_endpoint(&self, idx: usize) -> &str {
        &self.shard_endpoints[idx]
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }
}

/// Project resolver whose backing map can be mutated after construction.
/// Tests register usernames lazily as they connect new clients, so the
/// router and every shard need to agree on the username -> project mapping
/// without rebuilding their resolvers.
#[derive(Default)]
pub struct DynamicProjectResolver {
    inner: tokio::sync::RwLock<HashMap<String, ProjectId>>,
}

impl DynamicProjectResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn insert(&self, user: impl Into<String>, project: ProjectId) {
        self.inner.write().await.insert(user.into(), project);
    }
}

#[async_trait::async_trait]
impl ProjectResolver for DynamicProjectResolver {
    async fn resolve(&self, username: &str) -> Result<ProjectId> {
        self.inner
            .read()
            .await
            .get(username)
            .copied()
            .ok_or_else(|| {
                basin_common::BasinError::not_found(format!(
                    "no project mapped for user {username:?}"
                ))
            })
    }
}

/// Per-shard statistics. Incremented atomically in `CountingResolver`
/// every time a connection authenticates against the shard.
#[derive(Default)]
pub struct ShardStats {
    /// Total connections this shard has authenticated.
    pub total: AtomicU64,
    /// Connections per project. Bounded by the number of distinct projects
    /// the test connects under, so it doesn't grow without limit.
    pub per_project: tokio::sync::Mutex<HashMap<ProjectId, u64>>,
}

impl ShardStats {
    pub fn snapshot_total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    pub async fn snapshot_per_project(&self) -> HashMap<ProjectId, u64> {
        self.per_project.lock().await.clone()
    }
}

/// Wraps an inner [`ProjectResolver`] and bumps a [`ShardStats`] counter
/// every time the inner resolver succeeds. Used by [`start_n_shards`] to
/// give each shard its own counter; the cluster-wide
/// [`DynamicProjectResolver`] holds the actual `username -> ProjectId`
/// mappings.
struct CountingResolver {
    inner: Arc<dyn ProjectResolver>,
    stats: Arc<ShardStats>,
}

#[async_trait::async_trait]
impl ProjectResolver for CountingResolver {
    async fn resolve(&self, username: &str) -> Result<ProjectId> {
        let t = self.inner.resolve(username).await?;
        self.stats.total.fetch_add(1, Ordering::Relaxed);
        let mut g = self.stats.per_project.lock().await;
        *g.entry(t).or_insert(0) += 1;
        Ok(t)
    }
}

/// Spin up an N-shard Basin cluster on localhost.
///
/// The shards share a single `LocalFileSystem`-backed storage tempdir and a
/// single in-memory catalog (`InMemoryCatalog::new()`). Each shard owns its
/// own `Engine` and pgwire listener; the front router runs in shard mode
/// against the resulting endpoint list.
///
/// Project -> shard mapping is performed by the `ShardMap` baked into the
/// router's accept loop. Tests register usernames into `cluster.resolver`
/// before connecting; both the router and every shard consult the same
/// resolver, so resolution is consistent across processes.
pub async fn start_n_shards(n: usize) -> Cluster {
    let data_dir = TempDir::new().expect("tempdir for shared storage");
    let fs =
        LocalFileSystem::new_with_prefix(data_dir.path()).expect("local fs at storage tempdir");
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    start_n_shards_with_storage(n, storage, data_dir).await
}

/// Variant of [`start_n_shards`] that lets the caller supply a pre-built
/// [`basin_storage::Storage`] and the [`TempDir`] backing it. Tests that
/// need to point shards at an alternative object store (e.g. real S3 / Tigris)
/// build the `Storage` themselves and feed it in here. The returned
/// `Cluster` keeps the tempdir alive for the duration of the test.
///
/// The supplied `data_dir` is used purely as a lifetime anchor — the
/// caller decides whether anything actually lives under it. For S3
/// callers, pass an empty `TempDir` and let the bucket-side `CleanupOnDrop`
/// take care of teardown.
pub async fn start_n_shards_with_storage(
    n: usize,
    storage: basin_storage::Storage,
    data_dir: TempDir,
) -> Cluster {
    assert!(n >= 1, "start_n_shards requires N >= 1");

    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(basin_catalog::InMemoryCatalog::new());

    let resolver = Arc::new(DynamicProjectResolver::new());

    let mut shards: Vec<SpawnedShard> = Vec::with_capacity(n);
    let mut shard_endpoints: Vec<String> = Vec::with_capacity(n);
    for _ in 0..n {
        let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
            storage: storage.clone(),
            catalog: catalog.clone(),
            shard: None,
        });
        let stats = Arc::new(ShardStats::default());
        let counting: Arc<dyn ProjectResolver> = Arc::new(CountingResolver {
            inner: resolver.clone() as Arc<dyn ProjectResolver>,
            stats: stats.clone(),
        });
        let running: RunningServer = run_until_bound(ServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            engine,
            project_resolver: counting,
            pool: None,
            shard_endpoints: None,
            tls: None,
            connection_limiter: None,
        })
        .await
        .expect("spawn shard listener");
        let addr = running.local_addr;
        shard_endpoints.push(format!("{}:{}", addr.ip(), addr.port()));
        shards.push(SpawnedShard {
            addr,
            stats,
            shutdown: Some(running.shutdown),
            join: Some(running.join),
        });
    }

    // Front router: a fresh Engine that's only there to satisfy the config
    // type — accept_loop bypasses it once shard_endpoints is set.
    let router_engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: None,
    });
    let resolver_dyn: Arc<dyn ProjectResolver> = resolver.clone();
    let router_running = run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine: router_engine,
        project_resolver: resolver_dyn,
        pool: None,
        shard_endpoints: Some(shard_endpoints.clone()),
        tls: None,
        connection_limiter: None,
    })
    .await
    .expect("spawn front router");

    let router = SpawnedRouter {
        addr: router_running.local_addr,
        shutdown: Some(router_running.shutdown),
        join: Some(router_running.join),
    };

    Cluster {
        router,
        shards,
        shard_endpoints,
        resolver,
        _data_dir: data_dir,
    }
}

/// Convenience helper used by [`StaticProjectResolver`]-only callers that
/// just need a one-shot map of usernames -> projects. The integration test
/// uses [`DynamicProjectResolver`] directly because projects are added one
/// at a time, but having both forms documented saves a step for static
/// callers.
#[allow(dead_code)]
pub fn static_resolver_with(
    entries: impl IntoIterator<Item = (String, ProjectId)>,
) -> Arc<StaticProjectResolver> {
    let mut map = HashMap::new();
    for (k, v) in entries {
        map.insert(k, v);
    }
    Arc::new(StaticProjectResolver::new(map))
}
