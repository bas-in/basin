//! Engine-side fast path for vector search.
//!
//! Walks the storage HNSW segments via `Storage::vector_search`, then reads
//! the matching rows back from the source data files. The returned
//! `RecordBatch`es carry a final `_distance` column with the metric value
//! reported by HNSW.
//!
//! This is the manual-route entry point. The default SQL execution path
//! still goes through DataFusion + the brute-force distance UDF, which is
//! slower but correct without an index. The smoke test exercises both and
//! cross-checks the top-k.
//!
//! Format-agnostic by construction: the row-fetch step resolves each HNSW
//! hit back to its *data* file via the ULID stem the index segment and the
//! data file share, then reads that file through the storage reader (which
//! dispatches Parquet vs. Vortex internally). It never assumes a Parquet
//! footer / row groups / column stats, so it works identically whether the
//! table's data files are `.parquet` or `.vortex`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use basin_common::{BasinError, ProjectId, Result, TableName};
use basin_storage::{Storage, VectorHit};
use basin_vector::Distance;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;

use crate::ProjectSession;

/// Data-file suffixes the storage layer can produce. The HNSW sidecar shares
/// a ULID stem with the data file it indexes, but `Storage::vector_search`
/// can only reattach the data-file path when it recognises the data file's
/// suffix; for any suffix it doesn't (e.g. Vortex), the hit comes back
/// carrying the index-segment path instead. We re-resolve the real data
/// file here from the shared ULID stem so the fast path is format-agnostic.
const DATA_FILE_SUFFIXES: [&str; 2] = [".parquet", ".vortex"];

impl ProjectSession {
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
            .vector_search(&self.project, table, column, &query, k, distance)
            .await?;
        if hits.is_empty() {
            return Ok(Vec::new());
        }

        // Build a ULID-stem -> data-file path map covering *every* data-file
        // format the table uses. `Storage::list_data_files` returns both
        // `.parquet` and `.vortex` files; keying on the shared ULID stem lets
        // us reattach a hit to its real data file regardless of which path
        // `Storage::vector_search` was able to surface (it falls back to the
        // index-segment path for any data-file suffix it can't itself map,
        // e.g. Vortex). For Parquet the hit already carries the right path,
        // so this lookup is an identity and behaviour is unchanged.
        let data_files = storage.list_data_files(&self.project, table).await?;
        let mut by_stem: HashMap<String, ObjectPath> = HashMap::with_capacity(data_files.len());
        for df in &data_files {
            if let Some(stem) = path_stem(df.path.as_ref()) {
                by_stem.insert(stem, df.path.clone());
            }
        }

        // Group hits by the *resolved* source data-file path so we read each
        // file at most once.
        let mut by_file: HashMap<ObjectPath, Vec<&VectorHit>> = HashMap::new();
        for h in &hits {
            let resolved = resolve_data_file(&h.file_path, &by_stem);
            by_file.entry(resolved).or_default().push(h);
        }

        let mut output_rows: Vec<(f32, RecordBatch)> = Vec::with_capacity(hits.len());

        for (path, group) in by_file {
            let batch = read_rows_at_positions(storage, &self.project, &path, &group).await?;
            // Pair each output row with its distance so we can re-sort
            // globally after collecting from every file.
            for (i, h) in group.iter().enumerate() {
                let single = batch.slice(i, 1);
                output_rows.push((h.distance, single));
            }
        }

        // Sort global top-k by distance ascending.
        output_rows.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        output_rows.truncate(k);

        // Concatenate single-row batches and append the `_distance` column.
        if output_rows.is_empty() {
            return Ok(Vec::new());
        }
        let table_schema = output_rows[0].1.schema();
        let mut out_schema_fields: Vec<Arc<Field>> =
            table_schema.fields().iter().cloned().collect();
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

/// Strip the trailing path segment + a recognised data-file suffix, yielding
/// the bare ULID stem the data file and its HNSW sidecar share. Returns
/// `None` for paths that aren't a recognised data file.
fn path_stem(path: &str) -> Option<String> {
    let last = path.rsplit('/').next()?;
    for suffix in DATA_FILE_SUFFIXES {
        if let Some(stem) = last.strip_suffix(suffix) {
            return Some(stem.to_string());
        }
    }
    None
}

/// Re-resolve the data-file path for one hit. `Storage::vector_search`
/// returns the data-file path when it can map the hit's index segment back
/// to it (Parquet), but falls back to the index-segment path itself for
/// data-file formats it can't map (Vortex). Both the index segment
/// (`<ulid>.hnsw`) and the data file (`<ulid>.parquet` / `<ulid>.vortex`)
/// share the same ULID stem, so we look the real data file up by stem.
///
/// If the hit's path is already a known data file, the lookup returns it
/// unchanged (Parquet path: behaviour identical to before). If the stem
/// can't be resolved at all we keep the original path so the subsequent
/// read surfaces a precise error rather than silently dropping the hit.
fn resolve_data_file(hit_path: &ObjectPath, by_stem: &HashMap<String, ObjectPath>) -> ObjectPath {
    let raw = hit_path.as_ref();
    let last = raw.rsplit('/').next().unwrap_or(raw);
    // Try every known suffix (data file *or* index segment) so we recover
    // the ULID stem whether storage handed us a data path or a `.hnsw` path.
    let stem = DATA_FILE_SUFFIXES
        .iter()
        .chain(std::iter::once(&".hnsw"))
        .find_map(|suffix| last.strip_suffix(suffix));
    if let Some(stem) = stem {
        if let Some(p) = by_stem.get(stem) {
            return p.clone();
        }
    }
    hit_path.clone()
}

/// Read the data file at `path` (Parquet *or* Vortex — the storage reader
/// dispatches on the suffix) and project only the rows at the requested
/// positions. The returned batch is in the same order as the `hits` slice,
/// so the caller can pair `_distance` values back up.
///
/// The HNSW `row_id` is the row's absolute position within the data file as
/// the writer laid it out; the storage reader yields batches in that same
/// order for both formats, so concatenating then slicing by position is
/// format-agnostic.
async fn read_rows_at_positions(
    storage: &Storage,
    project: &ProjectId,
    path: &ObjectPath,
    hits: &[&VectorHit],
) -> Result<RecordBatch> {
    let mut stream = storage.read_file(project, path).await?;
    let mut all: Vec<RecordBatch> = Vec::new();
    let mut schema_opt = None;
    while let Some(r) = stream.next().await {
        let b = r.map_err(|e| BasinError::storage(format!("read {path}: {e}")))?;
        if schema_opt.is_none() {
            schema_opt = Some(b.schema());
        }
        all.push(b);
    }
    let Some(schema) = schema_opt else {
        return Err(BasinError::storage(format!("empty data file at {path}")));
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
