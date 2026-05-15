//! Per-project continuous-aggregate registry, backed by [`basin_catalog`].
//!
//! [`CvStore`] is the primary write surface for the v0.1 Rust API. It
//! registers a CV by:
//!
//! 1. Running the source SQL once against `project`'s engine session to
//!    learn the result schema.
//! 2. Creating a regular Basin table under the CV's name with that
//!    schema (so future SELECTs go through the engine's standard
//!    Parquet read path).
//! 3. Stamping a [`basin_catalog::CvDef`] onto the new table via
//!    [`basin_catalog::Catalog::set_continuous_aggregate`].
//! 4. Writing the materialised result of step 1 as the table's first
//!    Parquet file (one-shot bootstrap; subsequent refreshes go through
//!    [`crate::CvRefresher::tick`]).
//!
//! All reads and writes go through the catalog and the engine, so:
//!
//! - Project isolation is the same as for user SQL.
//! - RLS on the source table is honoured at refresh time (the same
//!   session that runs ad-hoc queries runs the refresh SQL).
//! - Persistence is handled by the catalog and storage layers; no
//!   separate durability story for CV bookkeeping.

use std::sync::Arc;

use arrow_array::RecordBatch;
use basin_catalog::{CvDef, DataFileRef, SnapshotId, TableMetadata};
use basin_common::{BasinError, PartitionKey, Result, TableName, ProjectId};
use basin_engine::{Engine, ExecResult};
use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::types::{CvRefreshState, CvSpec};

/// Errors specific to CV registration. Refresh-time errors flow back as
/// [`crate::CvRefreshOutcome::Failed`] instead.
#[derive(Debug, Error)]
pub enum RegisterError {
    #[error("CV {0:?} already exists")]
    Duplicate(String),
    #[error("CV {0:?} not found")]
    NotFound(String),
    #[error("CV name {0:?} is invalid: {1}")]
    InvalidName(String, String),
    #[error("CV source query produced no rows; cannot infer materialised schema")]
    EmptyBootstrap,
    #[error("engine error: {0}")]
    Engine(#[from] BasinError),
}

/// Per-project CV registry. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct CvStore {
    inner: Arc<CvStoreInner>,
}

struct CvStoreInner {
    engine: Engine,
}

impl CvStore {
    pub fn new(engine: Engine) -> Self {
        Self {
            inner: Arc::new(CvStoreInner { engine }),
        }
    }

    /// Reference to the engine the store writes through.
    pub fn engine(&self) -> &Engine {
        &self.inner.engine
    }

    /// Register a new CV under `project`. Errors if a table or CV with the
    /// same name already exists, or if the source SQL fails to plan / run.
    ///
    /// Note: v0.1 runs the source query at registration time to learn the
    /// CV's schema. The result of that run is also written as the CV's
    /// first materialisation, so a SELECT immediately after registration
    /// returns the bootstrap rows without waiting for the first refresh
    /// tick. v0.2 will let the caller declare the schema up front (so
    /// registration doesn't block on a potentially-large scan) and defer
    /// materialisation to the first tick.
    pub async fn register_cv(
        &self,
        project: &ProjectId,
        spec: CvSpec,
    ) -> std::result::Result<(), RegisterError> {
        self.register_cv_at(project, spec, Utc::now()).await
    }

    /// Like [`Self::register_cv`] but accepts an explicit `now` instant —
    /// used by tests driving a [`crate::TestClock`] so the registration's
    /// `last_refreshed_at` aligns with the same logical clock the
    /// refresher's tick reads.
    pub async fn register_cv_at(
        &self,
        project: &ProjectId,
        spec: CvSpec,
        now: DateTime<Utc>,
    ) -> std::result::Result<(), RegisterError> {
        let name = TableName::new(spec.name.clone())
            .map_err(|e| RegisterError::InvalidName(spec.name.clone(), format!("{e}")))?;

        // Reject if a table with the same name already exists. The catalog's
        // `create_table` itself returns a Catalog error on conflict, but we
        // want a typed `Duplicate` error at this layer.
        let existing = self
            .inner
            .engine
            .config()
            .catalog
            .list_tables(project)
            .await?;
        if existing.iter().any(|t| t.as_str() == name.as_str()) {
            return Err(RegisterError::Duplicate(spec.name.clone()));
        }

        // 1. Run the source query in a session bound to `project` to learn
        //    the result schema and bootstrap rows.
        let sess = self.inner.engine.open_session(*project).await?;
        let res = sess.execute(&spec.query_sql).await?;
        let (schema, batches) = match res {
            ExecResult::Rows { schema, batches } => (schema, batches),
            ExecResult::Empty { tag } => {
                return Err(RegisterError::Engine(BasinError::internal(format!(
                    "CV source query returned non-rows ({tag})"
                ))));
            }
        };
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            // We could still create an empty CV here; but without any rows
            // the schema's nullability inference may be too narrow for v0.2's
            // incremental refresh. Keep the conservative error in v0.1.
            return Err(RegisterError::EmptyBootstrap);
        }

        // 2. Create the CV's catalog table.
        let catalog = self.inner.engine.config().catalog.clone();
        let _meta = catalog.create_table(project, &name, schema.as_ref()).await?;

        // 3. Write the bootstrap result as the CV's first Parquet file.
        let merged = concat_batches(&schema, &batches)?;
        let storage = self.inner.engine.config().storage.clone();
        let part = PartitionKey::default_key();
        let written = storage.write_batch(project, &name, &part, &merged).await?;

        // 4. Append the file to the catalog.
        let after = catalog
            .append_data_files(
                project,
                &name,
                SnapshotId::GENESIS,
                vec![DataFileRef {
                    path: written.path.as_ref().to_string(),
                    size_bytes: written.size_bytes,
                    row_count: written.row_count,
                    column_stats: written.column_stats.clone(),
                }],
            )
            .await?;
        let _ = after;

        // 5. Stamp the CV definition on the catalog row.
        let def = CvDef {
            source_table: spec.source_table.clone(),
            query_sql: spec.query_sql.clone(),
            refresh_interval_secs: spec.refresh_interval_secs,
            last_refreshed_at_unix_ms: Some(now.timestamp_millis()),
            last_bucket_max_unix_ms: spec.last_bucket_max.map(|t| t.timestamp_millis()),
        };
        catalog
            .set_continuous_aggregate(project, &name, Some(def))
            .await?;

        Ok(())
    }

    /// Read every registered CV under `project`. Order is by `name` for
    /// stable test assertions.
    pub async fn list_cvs(&self, project: &ProjectId) -> Result<Vec<CvSpec>> {
        let catalog = self.inner.engine.config().catalog.clone();
        let names = catalog.list_tables(project).await?;
        let mut out = Vec::new();
        for name in names {
            let meta = catalog.load_table(project, &name).await?;
            if let Some(def) = meta.continuous_aggregate.clone() {
                out.push(spec_from_meta(&meta, def));
            }
        }
        // `list_tables` already returns names sorted, so `out` is sorted by
        // name as well — no extra sort needed.
        Ok(out)
    }

    /// Look up a single CV by name.
    pub async fn get_cv(&self, project: &ProjectId, name: &str) -> Result<Option<CvSpec>> {
        let catalog = self.inner.engine.config().catalog.clone();
        let table = match TableName::new(name.to_string()) {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };
        let meta = match catalog.load_table(project, &table).await {
            Ok(m) => m,
            Err(BasinError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        Ok(meta
            .continuous_aggregate
            .clone()
            .map(|d| spec_from_meta(&meta, d)))
    }

    /// Update the bookkeeping fields after a successful refresh. The
    /// immutable parts of the CV definition (`source_table`, `query_sql`,
    /// `refresh_interval_secs`) are left alone.
    pub async fn update_refresh_state(
        &self,
        project: &ProjectId,
        name: &str,
        state: CvRefreshState,
    ) -> Result<()> {
        let catalog = self.inner.engine.config().catalog.clone();
        let table = TableName::new(name.to_string())
            .map_err(|e| BasinError::internal(format!("update_refresh_state: bad name: {e}")))?;
        let meta = catalog.load_table(project, &table).await?;
        let mut def = meta
            .continuous_aggregate
            .clone()
            .ok_or_else(|| BasinError::not_found(format!("CV {name} on {project}")))?;
        def.last_refreshed_at_unix_ms = Some(state.last_refreshed_at.timestamp_millis());
        def.last_bucket_max_unix_ms = state.last_bucket_max.map(|t| t.timestamp_millis());
        catalog
            .set_continuous_aggregate(project, &table, Some(def))
            .await?;
        let _ = state.last_row_count;
        Ok(())
    }
}

fn spec_from_meta(meta: &TableMetadata, def: CvDef) -> CvSpec {
    CvSpec {
        name: meta.table.as_str().to_string(),
        source_table: def.source_table,
        query_sql: def.query_sql,
        refresh_interval_secs: def.refresh_interval_secs,
        last_refreshed_at: def
            .last_refreshed_at_unix_ms
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        last_bucket_max: def
            .last_bucket_max_unix_ms
            .and_then(DateTime::<Utc>::from_timestamp_millis),
    }
}

/// Merge a slice of `RecordBatch`es with a shared schema into one
/// contiguous batch. Used by the bootstrap path so the CV's first
/// materialisation is one Parquet file rather than one per query batch.
pub(crate) fn concat_batches(
    schema: &Arc<arrow_schema::Schema>,
    batches: &[RecordBatch],
) -> Result<RecordBatch> {
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    let refs: Vec<&RecordBatch> = batches.iter().collect();
    arrow_select::concat::concat_batches(schema, refs)
        .map_err(|e| BasinError::internal(format!("concat_batches: {e}")))
}
