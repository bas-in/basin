//! [`BenchHarness`] — builds an engine + storage + catalog from a
//! [`BenchConfig`] and exposes the helpers the workload primitives consume.
//!
//! The harness owns:
//! - one [`basin_storage::Storage`] wired with caches from
//!   [`crate::config::EngineFlags`];
//! - one [`basin_catalog::InMemoryCatalog`] (the LocalFS profile) or a
//!   stub that points at the same Postgres catalog for the cloud profiles
//!   (deferred — single-instance profile uses InMemoryCatalog because the
//!   cloud catalog wiring is gated and outside this harness's blast radius);
//! - one [`basin_engine::Engine`];
//! - N projects with M tables each, provisioned at construction time.
//!
//! A profile shape is then a pure function over `(self, BenchConfig)` —
//! see [`crate::workload`].
//!
//! Construction is async because catalog provisioning is async.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_engine::{Engine, EngineConfig};
use basin_storage::{DiskCacheConfig, PageCacheConfig, Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use crate::config::{BenchConfig, StorageBackend};

/// Built environment for one bench run.
///
/// All fields are public so workload helpers can reach in without going
/// through getters. The harness is `Clone` (Arc inside `Engine` and
/// `Storage` makes that cheap) so workloads can spawn it across tokio tasks.
#[derive(Clone)]
pub struct BenchHarness {
    pub engine: Engine,
    pub storage: Storage,
    pub catalog: Arc<dyn Catalog>,
    pub projects: Vec<ProjectId>,
    pub table: TableName,
    pub schema: Arc<Schema>,
    /// Keep tempdirs alive until the harness drops.
    _tempdirs: Arc<Vec<TempDir>>,
}

/// Standard `(id BIGINT, payload TEXT)` schema used by point-query / scan
/// shapes. Adding new shapes that need their own schema should expose a
/// dedicated `BenchHarness::with_schema(...)` constructor; do not mutate
/// this one — the dashboard cards key by `(profile, shape)` so a schema
/// change silently invalidates published numbers.
pub fn default_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

/// Build a batch of `len` rows starting at `start`. Payload is a fixed-width
/// string so size-scaling tests are predictable.
pub fn build_batch(schema: &Arc<Schema>, start: i64, len: usize) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let payloads: Vec<String> = (0..len)
        .map(|i| format!("payload-{:040}", start + i as i64))
        .collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(payload_arr)])
        .expect("batch shape matches schema")
}

impl BenchHarness {
    /// Build the harness. Provisions [`BenchConfig::workload`]`.project_count`
    /// projects, each with one `events` table of `workload.table_rows` rows.
    ///
    /// Non-LocalFS backends are currently rejected — wiring AWS / Tigris /
    /// SeaweedFS requires env vars + credentials that this crate intentionally
    /// doesn't read. Sibling helpers will land in a follow-up; LocalFS is
    /// enough to lock in every dashboard JSON the legacy `data/` directory
    /// holds today.
    pub async fn build(cfg: &BenchConfig) -> Result<Self> {
        let mut tempdirs = Vec::new();

        let object_store = match &cfg.backend {
            StorageBackend::LocalFs => {
                let dir = TempDir::new()?;
                let fs = LocalFileSystem::new_with_prefix(dir.path())?;
                tempdirs.push(dir);
                Arc::new(fs) as Arc<dyn object_store::ObjectStore>
            }
            other => {
                anyhow::bail!(
                    "BenchHarness: backend {:?} not yet wired in this crate; \
                     LocalFS-only for now. The legacy s3_* integration tests \
                     remain the path to exercise real S3/Tigris/SeaweedFS.",
                    other
                )
            }
        };

        let disk_cache = {
            let dir = TempDir::new()?;
            let path = dir.path().to_path_buf();
            tempdirs.push(dir);
            Some(DiskCacheConfig::new(path, cfg.engine.disk_cache_bytes))
        };
        let page_cache = Some(PageCacheConfig::new(cfg.engine.page_cache_bytes));

        let storage = Storage::new(StorageConfig {
            object_store,
            root_prefix: None,
            disk_cache,
            page_cache,
        });

        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        let engine = Engine::new(EngineConfig {
            storage: storage.clone(),
            catalog: catalog.clone(),
            shard: None,
        });

        let schema = default_schema();
        let table = TableName::new("events").expect("events is valid identifier");
        let part = PartitionKey::default_key();

        let mut projects = Vec::with_capacity(cfg.workload.project_count);
        for _ in 0..cfg.workload.project_count {
            let project = ProjectId::new();
            catalog.create_namespace(&project).await?;
            catalog.create_table(&project, &table, &schema).await?;

            // Seed rows. Write in 100k batches so the test doesn't sit on one
            // giant Arrow allocation for multi-million-row tables.
            const BATCH: u64 = 100_000;
            let mut written: u64 = 0;
            while written < cfg.workload.table_rows {
                let take = (cfg.workload.table_rows - written).min(BATCH) as usize;
                let batch = build_batch(&schema, written as i64, take);
                let df = storage.write_batch(&project, &table, &part, &batch).await?;
                let meta = catalog.load_table(&project, &table).await?;
                catalog
                    .append_data_files(
                        &project,
                        &table,
                        meta.current_snapshot,
                        vec![DataFileRef {
                            path: df.path.as_ref().to_string(),
                            size_bytes: df.size_bytes,
                            row_count: df.row_count,
                            column_stats: df.column_stats.clone(),
                            bloom_filters: ::std::collections::BTreeMap::new(),
                            hll_sketches: ::std::collections::BTreeMap::new(),
                            tdigest_sketches: ::std::collections::BTreeMap::new(),
                        }],
                    )
                    .await?;
                written += take as u64;
            }
            projects.push(project);
        }

        Ok(Self {
            engine,
            storage,
            catalog,
            projects,
            table,
            schema,
            _tempdirs: Arc::new(tempdirs),
        })
    }

    /// Per-shape output directory. Resolves to
    /// `<repo_root>/benchmark/<backend.data_slug()>/` unless the config
    /// overrides it.
    pub fn output_dir(&self, cfg: &BenchConfig) -> PathBuf {
        if let Some(p) = &cfg.output_dir {
            return p.clone();
        }
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        manifest
            .parent()
            .and_then(std::path::Path::parent)
            .map(|p| p.join("benchmark").join(cfg.backend.data_slug()))
            .unwrap_or_else(|| PathBuf::from(format!("benchmark/{}", cfg.backend.data_slug())))
    }
}
