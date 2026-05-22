//! Phase 5.29.C — virtual `timescaledb_information.chunks` table provider.
//!
//! Serves a synchronous Arrow snapshot of the hypertable chunk registry for
//! the calling project.  DataFusion can filter / project the result on top;
//! we do not implement predicate pushdown (v0.1 — the chunk list is tiny).
//!
//! ## Schema
//!
//! | column          | type    | notes                         |
//! |-----------------|---------|-------------------------------|
//! | hypertable_name | Utf8    | base table name               |
//! | chunk_name      | Utf8    | internal chunk identifier     |
//! | chunk_schema    | Utf8    | always `"public"` in v0.1     |
//! | range_start     | Utf8    | ISO-8601 UTC                  |
//! | range_end       | Utf8    | ISO-8601 UTC (exclusive)      |
//! | is_compressed   | Boolean |                               |

use std::any::Any;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use basin_common::ProjectId;
use datafusion::arrow::datatypes::SchemaRef as DfSchemaRef;
use datafusion::catalog::{SchemaProvider, Session};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::hypertable::HypertableRegistry;

// ─── schema ───────────────────────────────────────────────────────────────────

fn chunks_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("hypertable_name", DataType::Utf8, false),
        Field::new("chunk_name", DataType::Utf8, false),
        Field::new("chunk_schema", DataType::Utf8, false),
        Field::new("range_start", DataType::Utf8, false),
        Field::new("range_end", DataType::Utf8, false),
        Field::new("is_compressed", DataType::Boolean, false),
    ]))
}

// ─── provider ────────────────────────────────────────────────────────────────

/// `TableProvider` for `timescaledb_information.chunks` filtered to the
/// calling project.
#[derive(Debug)]
pub(crate) struct ChunksProvider {
    registry: Arc<HypertableRegistry>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl ChunksProvider {
    pub(crate) fn new(registry: Arc<HypertableRegistry>, project: ProjectId) -> Self {
        Self {
            registry,
            project,
            schema: chunks_schema(),
        }
    }

    /// Build the record batch by querying the registry.
    async fn build_batch(
        &self,
    ) -> DfResult<datafusion::arrow::record_batch::RecordBatch> {
        let rows = self.registry.snapshot_all_chunks(&self.project).await;

        let mut ht_names: Vec<String> = Vec::with_capacity(rows.len());
        let mut chunk_names: Vec<String> = Vec::with_capacity(rows.len());
        let mut chunk_schemas: Vec<String> = Vec::with_capacity(rows.len());
        let mut range_starts: Vec<String> = Vec::with_capacity(rows.len());
        let mut range_ends: Vec<String> = Vec::with_capacity(rows.len());
        let mut is_compressed: Vec<bool> = Vec::with_capacity(rows.len());

        for (ht_name, chunk) in &rows {
            ht_names.push(ht_name.clone());
            chunk_names.push(chunk.chunk_name.clone());
            chunk_schemas.push("public".to_string());
            range_starts.push(chunk.range_start.format("%Y-%m-%d %H:%M:%S+00").to_string());
            range_ends.push(chunk.range_end.format("%Y-%m-%d %H:%M:%S+00").to_string());
            is_compressed.push(chunk.is_compressed);
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(ht_names)),
            Arc::new(StringArray::from(chunk_names)),
            Arc::new(StringArray::from(chunk_schemas)),
            Arc::new(StringArray::from(range_starts)),
            Arc::new(StringArray::from(range_ends)),
            Arc::new(BooleanArray::from(is_compressed)),
        ];

        datafusion::arrow::record_batch::RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

#[async_trait]
impl TableProvider for ChunksProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let batch = self.build_batch().await?;
        let batches = vec![vec![batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &batches,
            self.schema.clone(),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

// ─── schema provider ─────────────────────────────────────────────────────────

/// Schema provider for `timescaledb_information` that exposes the `chunks`
/// virtual table.
pub(crate) struct TimescaleInfoSchema {
    registry: Arc<HypertableRegistry>,
    project: ProjectId,
}

impl TimescaleInfoSchema {
    pub(crate) fn new(registry: Arc<HypertableRegistry>, project: ProjectId) -> Self {
        Self { registry, project }
    }
}

impl std::fmt::Debug for TimescaleInfoSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TimescaleInfoSchema({})", self.project)
    }
}

#[async_trait]
impl SchemaProvider for TimescaleInfoSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec!["chunks".to_string()]
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        if name.eq_ignore_ascii_case("chunks") {
            Ok(Some(Arc::new(ChunksProvider::new(
                self.registry.clone(),
                self.project,
            ))))
        } else {
            Ok(None)
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        name.eq_ignore_ascii_case("chunks")
    }
}
