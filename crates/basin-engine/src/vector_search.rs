//! Engine-side fast path for vector search.
//!
//! Walks the storage HNSW segments via `Storage::vector_search`, then reads
//! the matching rows back from the source Parquet files. The returned
//! `RecordBatch`es carry a final `_distance` column with the metric value
//! reported by HNSW.
//!
//! This is the manual-route entry point. The default SQL execution path
//! still goes through DataFusion + the brute-force distance UDF, which is
//! slower but correct without an index. The smoke test exercises both and
//! cross-checks the top-k.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use basin_common::{BasinError, Result, TableName};
use basin_storage::VectorHit;
use basin_vector::Distance;
use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::TenantSession;

impl TenantSession {
    /// Approximate nearest-neighbour search using the HNSW sidecar indexes.
    ///
    /// Returns the matched rows as workspace-Arrow `RecordBatch`es, with a
    /// trailing `_distance` Float64 column carrying the HNSW-reported
    /// distance for each matched row. The ordering is global top-k by
    /// distance ascending.
    pub async fn vector_search(
        &self,
        table: &TableName,
        column: &str,
        query: Vec<f32>,
        k: usize,
        distance: Distance,
    ) -> Result<Vec<RecordBatch>> {
        let storage = &self.engine.config().storage;
        let hits: Vec<VectorHit> = storage
            .vector_search(&self.tenant, table, column, &query, k, distance)
            .await?;
        if hits.is_empty() {
            return Ok(Vec::new());
        }

        // Group hits by the source data-file path so we read each Parquet
        // file at most once.
        let mut by_file: HashMap<ObjectPath, Vec<&VectorHit>> = HashMap::new();
        for h in &hits {
            by_file.entry(h.file_path.clone()).or_default().push(h);
        }

        let store = storage.object_store_handle();
        let mut output_rows: Vec<(f32, RecordBatch)> = Vec::with_capacity(hits.len());

        for (path, group) in by_file {
            let batch = read_rows_at_positions(&store, &path, &group).await?;
            // Pair each output row with its distance so we can re-sort
            // globally after collecting from every file.
            for (i, h) in group.iter().enumerate() {
                let single = batch.slice(i, 1);
                output_rows.push((h.distance, single));
            }
        }

        // Sort global top-k by distance ascending.
        output_rows.sort_by(|a, b| {
            a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)
        });
        output_rows.truncate(k);

        // Concatenate single-row batches and append the `_distance` column.
        if output_rows.is_empty() {
            return Ok(Vec::new());
        }
        let table_schema = output_rows[0].1.schema();
        let mut out_schema_fields: Vec<Arc<Field>> = table_schema.fields().iter().cloned().collect();
        out_schema_fields.push(Arc::new(Field::new("_distance", DataType::Float64, false)));
        let out_schema = Arc::new(Schema::new(out_schema_fields));

        // Combine each single-row batch into one big batch by transposing
        // columns. Since RecordBatch is per-row the simplest stable thing is
        // to walk row-by-row building parallel column builders. Avoid arrow's
        // `concat_batches` here: we want to emit a single batch with the
        // appended `_distance` column without round-tripping through
        // arrow-select.
        let mut distances: Vec<f64> = Vec::with_capacity(output_rows.len());
        let mut row_batches: Vec<RecordBatch> = Vec::with_capacity(output_rows.len());
        for (d, b) in output_rows {
            distances.push(d as f64);
            row_batches.push(b);
        }
        let combined = arrow_select::concat::concat_batches(&table_schema, &row_batches)
            .map_err(|e| BasinError::internal(format!("concat row batches: {e}")))?;
        let mut cols: Vec<ArrayRef> = combined.columns().to_vec();
        cols.push(Arc::new(Float64Array::from(distances)) as ArrayRef);
        let final_batch = RecordBatch::try_new(out_schema, cols)
            .map_err(|e| BasinError::internal(format!("rebuild output batch: {e}")))?;
        Ok(vec![final_batch])
    }
}

/// Read the Parquet file at `path` and project only the rows at the
/// requested positions. The returned batch is in the same order as the
/// `hits` slice, so the caller can pair `_distance` values back up.
async fn read_rows_at_positions(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    hits: &[&VectorHit],
) -> Result<RecordBatch> {
    let bytes: Bytes = store
        .get(path)
        .await
        .map_err(|e| BasinError::storage(format!("get {path}: {e}")))?
        .bytes()
        .await
        .map_err(|e| BasinError::storage(format!("read {path}: {e}")))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| BasinError::storage(format!("open {path}: {e}")))?;
    let reader = builder
        .build()
        .map_err(|e| BasinError::storage(format!("build {path}: {e}")))?;

    // Walk the file batches and pluck out rows by absolute position.
    let mut wanted: Vec<(usize, usize)> = hits
        .iter()
        .enumerate()
        .map(|(i, h)| (h.row_id as usize, i))
        .collect();
    wanted.sort_by_key(|(pos, _)| *pos);

    // Collect everything into a single batch first; the per-file row count is
    // bounded by what the writer chose (default 65_536). For PoC scale this
    // is fine — the planner-level path will replace this with a row-group
    // selective reader.
    let mut all: Vec<RecordBatch> = Vec::new();
    let mut schema_opt = None;
    for r in reader {
        let b = r.map_err(|e| BasinError::storage(format!("read {path}: {e}")))?;
        if schema_opt.is_none() {
            schema_opt = Some(b.schema());
        }
        all.push(b);
    }
    let Some(schema) = schema_opt else {
        return Err(BasinError::storage(format!("empty parquet at {path}")));
    };
    let combined = arrow_select::concat::concat_batches(&schema, &all)
        .map_err(|e| BasinError::storage(format!("concat {path}: {e}")))?;

    // Project rows in the original `hits` order. We do that by building a
    // result in `hits` order using `RecordBatch::slice(..)` per row.
    let mut row_batches: Vec<RecordBatch> = Vec::with_capacity(hits.len());
    for h in hits {
        let pos = h.row_id as usize;
        if pos >= combined.num_rows() {
            return Err(BasinError::storage(format!(
                "hit row_id {pos} >= {} rows in {path}",
                combined.num_rows()
            )));
        }
        row_batches.push(combined.slice(pos, 1));
    }
    let out = arrow_select::concat::concat_batches(&schema, &row_batches)
        .map_err(|e| BasinError::storage(format!("hit concat {path}: {e}")))?;
    Ok(out)
}

