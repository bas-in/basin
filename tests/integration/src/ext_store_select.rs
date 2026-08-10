//! Env-driven store selection for the `ext_bench_*` and `compare_postgres_ext_*`
//! card families.
//!
//! ## Environment variable
//!
//! `BASIN_BENCH_STORE=local` (default) — build a `LocalFileSystem`-backed engine,
//!   write artifacts to `benchmark/data/<name>.json`.
//! `BASIN_BENCH_STORE=s3` — build an S3-backed engine using the config from
//!   `testing/basin-test.seaweedfs.toml` (same lookup order as `BasinTestConfig::load()`),
//!   write artifacts to `benchmark/data_seaweedfs/<name>.json`.
//!
//! When `local` is active, both functions (`build_ext_engine`, `ext_artifact_dir`)
//! are byte-identical to the original hard-coded behaviour; the default behaviour
//! is fully backward-compatible.
//!
//! ## Usage (from a test file that loads compare_postgres_common.rs)
//!
//! ```rust,ignore
//! use basin_integration_tests::ext_store_select::{build_ext_engine, ext_artifact_dir};
//!
//! let mut instance = build_ext_engine().await;
//! let artifact_dir = ext_artifact_dir();  // PathBuf
//! let path = artifact_dir.join("ext_bench_foo.json");
//! ```

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
use tempfile::TempDir;

use crate::cache_defaults;

/// Opaque engine instance returned by `build_ext_engine`.
/// Mirrors the fields of `BasinInstance` in `compare_postgres_common.rs`.
pub struct ExtBasinInstance {
    pub engine: Engine,
    pub project: ProjectId,
    pub bg: Option<basin_shard::ShardBackgroundHandle>,
    pub wal: Arc<dyn basin_wal::Wal>,
    /// Retained so callers can call `flush_to_parquet()` / `flush_wal()`.
    pub shard: basin_shard::Shard,
    /// Temp dirs — kept alive for the duration of the run.  `None` for S3
    /// builds (no local temp dir needed for storage; WAL still uses one).
    pub _storage_dir: Option<TempDir>,
    pub _wal_dir: TempDir,
}

/// Which store the current run is using.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchStore {
    Local,
    S3,
}

/// Read `BASIN_BENCH_STORE` and return which store mode is active.
/// Defaults to `BenchStore::Local` when the env var is absent or empty.
pub fn bench_store() -> BenchStore {
    match std::env::var("BASIN_BENCH_STORE")
        .unwrap_or_default()
        .to_lowercase()
        .as_str()
    {
        "s3" | "seaweedfs" => BenchStore::S3,
        _ => BenchStore::Local,
    }
}

/// Return the directory where ext-bench and compare-postgres-ext artifacts
/// should be written.
///
/// * `BASIN_BENCH_STORE=local` (default) → `benchmark/data`
/// * `BASIN_BENCH_STORE=s3`              → `benchmark/data_seaweedfs`
pub fn ext_artifact_dir() -> PathBuf {
    let subdir = match bench_store() {
        BenchStore::Local => "benchmark/data",
        BenchStore::S3 => "benchmark/data_seaweedfs",
    };
    // Resolve relative to the workspace root via CARGO_MANIFEST_DIR.
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = manifest
        .parent()
        .and_then(std::path::Path::parent)
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    let dir = root.join(subdir);
    let _ = std::fs::create_dir_all(&dir);
    dir
}

/// Build a Basin engine instance whose backing object store is selected by
/// `BASIN_BENCH_STORE`.
///
/// * `local` (default) — `LocalFileSystem` under a fresh tempdir, identical to
///   the existing `build_basin_engine()` in `compare_postgres_common.rs`.
/// * `s3` — uses the `[s3]` section from `testing/basin-test.seaweedfs.toml` (or
///   `BASIN_TEST_CONFIG`); panics with a clear message when no config is found
///   so the runner can gate on seaweed availability.
pub async fn build_ext_engine() -> ExtBasinInstance {
    match bench_store() {
        BenchStore::Local => build_ext_engine_local().await,
        BenchStore::S3 => build_ext_engine_s3().await,
    }
}

async fn build_ext_engine_local() -> ExtBasinInstance {
    use object_store::local::LocalFileSystem;

    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();

    let fs = LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: cache_defaults::default_test_disk_cache(),
        page_cache: cache_defaults::default_test_page_cache(),
    });

    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
    let wal: Arc<dyn basin_wal::Wal> = Arc::new(
        basin_wal::LocalWal::open(basin_wal::WalConfig {
            object_store: Arc::new(wal_fs),
            root_prefix: None,
            flush_interval: Duration::from_millis(200),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );

    let shard = basin_shard::Shard::new(basin_shard::ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let bg = shard.spawn_background();
    let shard_for_flush = shard.clone();

    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard),
    });
    let project = ProjectId::new();

    ExtBasinInstance {
        engine,
        project,
        bg: Some(bg),
        wal,
        shard: shard_for_flush,
        _storage_dir: Some(storage_dir),
        _wal_dir: wal_dir,
    }
}

async fn build_ext_engine_s3() -> ExtBasinInstance {
    use crate::test_config::BasinTestConfig;

    let cfg = BasinTestConfig::load().expect("load .basin-test.toml for BASIN_BENCH_STORE=s3");
    let object_store = cfg
        .build_object_store("ext_bench_s3")
        .expect("build s3 object store")
        .unwrap_or_else(|| {
            panic!(
                "BASIN_BENCH_STORE=s3 but no [s3] section in test config. \
                 Set BASIN_TEST_CONFIG=./testing/basin-test.seaweedfs.toml or add an [s3] section."
            )
        });

    let wal_dir = TempDir::new().unwrap();
    let run_prefix = object_store::path::Path::from(format!(
        "ext-bench/{}",
        ulid::Ulid::new().to_string().to_lowercase()
    ));

    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store,
        root_prefix: Some(run_prefix),
        disk_cache: cache_defaults::default_test_disk_cache(),
        page_cache: cache_defaults::default_test_page_cache(),
    });

    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal_fs = object_store::local::LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
    let wal: Arc<dyn basin_wal::Wal> = Arc::new(
        basin_wal::LocalWal::open(basin_wal::WalConfig {
            object_store: Arc::new(wal_fs),
            root_prefix: None,
            flush_interval: Duration::from_millis(200),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );

    let shard = basin_shard::Shard::new(basin_shard::ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let bg = shard.spawn_background();
    let shard_for_flush = shard.clone();

    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard),
    });
    let project = ProjectId::new();

    ExtBasinInstance {
        engine,
        project,
        bg: Some(bg),
        wal,
        shard: shard_for_flush,
        _storage_dir: None,
        _wal_dir: wal_dir,
    }
}
