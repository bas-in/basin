//! Inv-W5 / W9 — custom [`TableProvider`] + [`ExecutionPlan`] for JSONB `@>`
//! queries that prune at row-group granularity via the per-`(key, value)`
//! posting list (in [`basin_storage::index::jsonb_posting::JsonbPostingRegistry`]).
//!
//! # Why this module exists
//!
//! The existing [`crate::gin_rowgroup_scan`] drives `ReadOptions.row_group_selection`
//! from the bloom-based GIN row-group registry.  For workloads where the
//! searched term is in every row-group the bloom answers "maybe present"
//! everywhere and no row-group is pruned.  This module performs the same
//! row-group-allowlist bridging but is driven by the *posting list*
//! computed by `JsonbPostingRegistry::probe`, which AND-merges per-atom
//! posting lists for the needle's `(key, value)` pairs — yielding the
//! precise row-group set a `@>` query may need to read.
//!
//! # Correctness contract
//!
//! The row-group allowlist is a **conservative superset**: the posting
//! list reports exactly the row-groups whose stored value had at least one
//! matching `(key, value)` pair; the AND-merge across needle atoms at file
//! granularity removes files that lack any needle atom.  The full
//! `jsonb_contains` UDF re-evaluates every emitted row, so per-row false
//! positives (e.g. a row that has both atoms in different sub-objects) are
//! filtered out.  No false negatives are possible.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use basin_catalog::TableFileFormat;
use basin_common::{ProjectId, TableName};
use basin_storage::{ReadOptions, Storage};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use futures::StreamExt;
use object_store::path::Path as ObjectPath;

/// A [`datafusion::catalog::TableProvider`] that reads only the row-groups
/// listed in `row_group_selection` via Basin's native storage reader.
pub(crate) struct JsonbPostingPrunedTable {
    schema: SchemaRef,
    storage: Storage,
    project: ProjectId,
    table: TableName,
    #[allow(dead_code)]
    file_format: TableFileFormat,
    candidate_paths: Vec<String>,
    row_group_selection: HashMap<String, Vec<u32>>,
}

impl JsonbPostingPrunedTable {
    pub(crate) fn new(
        schema: SchemaRef,
        storage: Storage,
        project: ProjectId,
        table: TableName,
        file_format: TableFileFormat,
        candidate_paths: Vec<String>,
        row_group_selection: HashMap<String, Vec<u32>>,
    ) -> Self {
        Self {
            schema,
            storage,
            project,
            table,
            file_format,
            candidate_paths,
            row_group_selection,
        }
    }
}

impl fmt::Debug for JsonbPostingPrunedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonbPostingPrunedTable")
            .field("table", &self.table)
            .field("n_files", &self.candidate_paths.len())
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for JsonbPostingPrunedTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let proj_names: Option<Vec<String>> = projection.map(|idxs| {
            idxs.iter()
                .map(|&i| self.schema.field(i).name().clone())
                .collect()
        });

        let paths: Vec<ObjectPath> = self
            .candidate_paths
            .iter()
            .map(|p| ObjectPath::from(p.as_str()))
            .collect();

        let opts = ReadOptions {
            projection: proj_names,
            limit,
            row_group_selection: Some(self.row_group_selection.clone()),
            ..ReadOptions::default()
        };

        let output_schema: SchemaRef = match &projection {
            Some(idxs) => Arc::new(
                self.schema
                    .project(idxs)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
            ),
            None => self.schema.clone(),
        };

        Ok(Arc::new(JsonbPostingScanExec::new(
            output_schema,
            self.storage.clone(),
            self.project,
            self.table.clone(),
            self.schema.clone(),
            paths,
            opts,
        )))
    }
}

#[derive(Debug)]
pub(crate) struct JsonbPostingScanExec {
    output_schema: SchemaRef,
    storage: Storage,
    project: ProjectId,
    table: TableName,
    catalog_schema: SchemaRef,
    paths: Vec<ObjectPath>,
    opts: ReadOptions,
    props: Arc<PlanProperties>,
}

impl JsonbPostingScanExec {
    fn new(
        output_schema: SchemaRef,
        storage: Storage,
        project: ProjectId,
        table: TableName,
        catalog_schema: SchemaRef,
        paths: Vec<ObjectPath>,
        opts: ReadOptions,
    ) -> Self {
        let eq = EquivalenceProperties::new(output_schema.clone());
        let props = Arc::new(PlanProperties::new(
            eq,
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            output_schema,
            storage,
            project,
            table,
            catalog_schema,
            paths,
            opts,
            props,
        }
    }
}

impl DisplayAs for JsonbPostingScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "JsonbPostingScanExec: table={} n_files={}",
            self.table,
            self.paths.len()
        )
    }
}

impl ExecutionPlan for JsonbPostingScanExec {
    fn name(&self) -> &str {
        "JsonbPostingScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let storage = self.storage.clone();
        let project = self.project;
        let paths = self.paths.clone();
        let opts = self.opts.clone();
        let catalog_schema = Some(self.catalog_schema.clone());
        let output_schema = self.output_schema.clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel::<DFResult<arrow_array::RecordBatch>>(64);
        tokio::spawn(async move {
            let result = storage
                .read_paths_with_schema(&project, paths, opts, catalog_schema)
                .await;
            match result {
                Err(e) => {
                    let _ = tx.send(Err(DataFusionError::External(Box::new(e)))).await;
                }
                Ok(mut inner) => {
                    while let Some(item) = inner.next().await {
                        let df_item = item.map_err(|e| DataFusionError::External(Box::new(e)));
                        if tx.send(df_item).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });

        let stream = futures::stream::poll_fn(move |cx| rx.poll_recv(cx));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }
}
