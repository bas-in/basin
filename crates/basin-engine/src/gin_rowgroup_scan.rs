//! C2 — custom [`TableProvider`] + [`ExecutionPlan`] for JSONB `@>` queries
//! with row-group-granular GIN prune.
//!
//! # Why this module exists
//!
//! Basin's `@>` scan path goes through DataFusion's `ListingTable`, which uses
//! DataFusion's own Parquet reader.  That reader has no knowledge of Basin's
//! `ReadOptions.row_group_selection`; even when `apply_gin_pruning_for_query`
//! computes a per-file row-group allowlist from the `GinRowGroupRegistry`, it
//! has nowhere to deliver that allowlist — it can only swap the registered
//! `ListingTable` to a narrower *file* set, not a narrower *row-group* set.
//!
//! This module provides the missing bridge:
//!
//! * [`GinRowGroupPrunedTable`] — a [`TableProvider`] that holds the per-file
//!   row-group allowlist computed at planning time and, when DataFusion calls
//!   its `scan()`, drives Basin's native storage reader with
//!   `ReadOptions { row_group_selection: Some(map), .. }`.  Only the listed
//!   row-groups are opened; the rest are skipped at the Parquet reader level.
//!
//! * [`GinRowGroupScanExec`] — the leaf [`ExecutionPlan`] that drives
//!   `storage.read_paths_with_schema` lazily inside `execute()`, adapting its
//!   async `BoxStream` into a DataFusion `SendableRecordBatchStream` via
//!   `RecordBatchStreamAdapter`.
//!
//! # Correctness contract
//!
//! The row-group allowlist is a **conservative superset**: `rowgroup_prune_for_containment`
//! returns only row-groups whose bloom reports every needle term as
//! possibly-present.  The full `jsonb_contains` UDF still re-evaluates every
//! emitted row; false positives from the bloom are silently filtered out.  No
//! false negatives are possible.
//!
//! Files absent from the allowlist map (i.e., files that were not summarised
//! by the registry because they pre-date the indexing wave) are still read in
//! full — `ReadOptions::row_group_selection` leaves those files untouched.
//!
//! # Vortex tables
//!
//! Vortex files have no concept of "row group", so `ReadOptions::row_group_selection`
//! is silently ignored for them (see `basin-storage/src/reader.rs`).  This
//! provider is therefore safe for mixed Parquet+Vortex table configurations —
//! the Vortex files are simply read in full.

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
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use futures::StreamExt;
use object_store::path::Path as ObjectPath;

// ── GinRowGroupPrunedTable ─────────────────────────────────────────────────────

/// A [`datafusion::catalog::TableProvider`] that reads only the row-groups
/// listed in `row_group_selection` via Basin's native storage reader.
///
/// Registered by `apply_gin_pruning_for_query` (session.rs) when the
/// `GinRowGroupRegistry` yields a `PerFile` prune map for the current `@>`
/// query.  DataFusion replaces its internal `ListingTable` scan with this
/// provider for the duration of the query and restores the original table
/// on the next `refresh_table` call.
pub(crate) struct GinRowGroupPrunedTable {
    /// Arrow schema (DataFusion-typed, converted by `schema_ws_to_df`).
    schema: SchemaRef,
    /// Basin native storage handle — used in `scan()` to drive
    /// `read_paths_with_schema`.
    storage: Storage,
    project: ProjectId,
    table: TableName,
    file_format: TableFileFormat,
    /// Ordered list of candidate file paths (bucket-relative keys as returned
    /// by `DataFileRef::path`).  Must be a subset of the catalog's live files.
    candidate_paths: Vec<String>,
    /// Per-file row-group allowlist.  Keys are the same path strings as
    /// `candidate_paths`; values are ascending sorted row-group indices.
    /// A file absent from this map is read in full (conservative fallback).
    row_group_selection: HashMap<String, Vec<u32>>,
}

impl GinRowGroupPrunedTable {
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

impl fmt::Debug for GinRowGroupPrunedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GinRowGroupPrunedTable")
            .field("table", &self.table)
            .field("n_files", &self.candidate_paths.len())
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for GinRowGroupPrunedTable {
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
        // Convert the index-based projection to column name projection for
        // `ReadOptions`.  `None` means "all columns".
        let proj_names: Option<Vec<String>> = projection.map(|idxs| {
            idxs.iter()
                .map(|&i| self.schema.field(i).name().clone())
                .collect()
        });

        // Build object-store paths from the catalog-relative path strings.
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

        // Resolve the projected output schema.
        let output_schema: SchemaRef = match &projection {
            Some(idxs) => Arc::new(
                self.schema
                    .project(idxs)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
            ),
            None => self.schema.clone(),
        };

        Ok(Arc::new(GinRowGroupScanExec::new(
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

// ── GinRowGroupScanExec ────────────────────────────────────────────────────────

/// A leaf [`ExecutionPlan`] that drives `Storage::read_paths_with_schema` with
/// a `row_group_selection` allowlist so DataFusion sees only the surviving
/// row-groups of each candidate file.
///
/// Produces a single output partition.  DataFusion's query optimizer may insert
/// a repartition node above this if the plan requires multiple partitions.
#[derive(Debug)]
pub(crate) struct GinRowGroupScanExec {
    /// Arrow schema of the output batches (post-projection).
    output_schema: SchemaRef,
    storage: Storage,
    project: ProjectId,
    table: TableName,
    /// Full table schema (pre-projection) passed to `read_paths_with_schema`
    /// as `catalog_schema`.
    catalog_schema: SchemaRef,
    paths: Vec<ObjectPath>,
    opts: ReadOptions,
    props: Arc<PlanProperties>,
}

impl GinRowGroupScanExec {
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

impl DisplayAs for GinRowGroupScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GinRowGroupScanExec: table={} n_files={}",
            self.table,
            self.paths.len()
        )
    }
}

impl ExecutionPlan for GinRowGroupScanExec {
    fn name(&self) -> &str {
        "GinRowGroupScanExec"
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
        // Separate clone moved into the producer task for per-batch schema
        // normalization; the original stays for the stream adapter below.
        let normalize_schema = self.output_schema.clone();

        // Lazily drive `read_paths_with_schema` inside the stream.
        //
        // Pattern: `async_stream::stream!` macro or equivalent; here we use
        // the simpler `tokio::sync::mpsc` + spawn approach to bridge the
        // async future into a DataFusion stream.
        //
        // We spawn a tokio task that calls `read_paths_with_schema`, consumes
        // its stream, and sends batches over an `mpsc` channel.  The returned
        // DataFusion stream receives them on the other end.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<DFResult<arrow_array::RecordBatch>>(64);
        tokio::spawn(async move {
            let result = storage
                .read_paths_with_schema(&project, paths, opts, catalog_schema)
                .await;
            match result {
                Err(e) => {
                    let _ = tx
                        .send(Err(DataFusionError::External(Box::new(e))))
                        .await;
                }
                Ok(mut inner) => {
                    while let Some(item) = inner.next().await {
                        // Normalize each produced batch to the declared output
                        // schema before handing it to DataFusion.  The cold
                        // Vortex decode narrows JSONB `LargeBinary` → `Binary`
                        // (and the text/view families likewise), but
                        // `output_schema` carries the catalog-declared types.
                        // `RecordBatchStreamAdapter` asserts the produced batch
                        // schema matches the declared schema, so without this
                        // coercion an aggregate/`SELECT` over a JSONB column
                        // routed through this provider panics on a
                        // `Binary` vs `LargeBinary` mismatch.  This is the same
                        // read-boundary normalization applied elsewhere (hot
                        // overlay merge, fast-select cold scan).
                        let df_item = item
                            .map(|b| {
                                crate::hot_tombstone::normalize_batch_to_schema(
                                    b,
                                    normalize_schema.as_ref(),
                                )
                            })
                            .map_err(|e| DataFusionError::External(Box::new(e)));
                        if tx.send(df_item).await.is_err() {
                            // Receiver dropped — consumer gave up (LIMIT reached, etc.)
                            break;
                        }
                    }
                }
            }
        });

        let stream = futures::stream::poll_fn(move |cx| rx.poll_recv(cx));
        Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, stream)))
    }
}
