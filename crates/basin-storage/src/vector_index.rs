//! HNSW index sidecar to Parquet data files.
//!
//! Layout: every Parquet write that contains a `FixedSizeList<Float32>`
//! column may produce a sibling `.hnsw` segment under
//! `{project_prefix}/index/{column}.hnsw/{ulid}.hnsw`. The directory shape
//! mirrors the data-file-per-write convention so a future compactor can
//! coalesce both sides as one operation.
//!
//! Read path: [`vector_search`] lists every segment under the column's
//! directory, opens each, runs `search(query, k)`, then merges the per-
//! segment top-k by distance to produce the global top-k.

use arrow_array::{Array, FixedSizeListArray, Float32Array, RecordBatch};
use arrow_schema::DataType;
use basin_common::{BasinError, ProjectId, Result, TableName};
use basin_vector::{Distance, HnswIndex, HnswIndexBuilder};
use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStoreExt, PutPayload};
use ulid::Ulid;

use crate::Storage;

/// Scan a `RecordBatch` for `FixedSizeList<Float32>` columns. For each one,
/// build an HNSW segment from the rows and write it as a sibling object.
///
/// `data_ulid` is the same ULID embedded in the data file's name. We reuse
/// it for the index-segment file name so a search hit can be mapped back to
/// the source Parquet file by string match — the engine layer needs that
/// correspondence to fetch the matching rows.
///
/// Distance defaults to L2 since that is the most common starting metric for
/// embeddings; once we add per-table options to the catalog this becomes a
/// lookup. Until then, callers wanting cosine/dot can supply the metric via
/// `Storage::vector_search` regardless of which metric the segment was built
/// with — the index encodes it in its own header.
pub(crate) async fn build_indexes_for_batch(
    storage: &Storage,
    project: &ProjectId,
    table: &TableName,
    batch: &RecordBatch,
    data_ulid: Ulid,
) -> Result<()> {
    let schema = batch.schema();
    // Pick up declared HNSW index build parameters (today: `ef_construction`)
    // so a `CREATE INDEX … USING hnsw WITH (ef_construction = N)` declaration
    // is consumed by the sidecar build. Best-effort: a missing catalog or no
    // matching index leaves the library defaults in place.
    let indexes = storage
        .catalog_table_indexes(project, table)
        .await
        .unwrap_or_default();
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let DataType::FixedSizeList(child, n) = field.data_type() else {
            continue;
        };
        if *child.data_type() != DataType::Float32 {
            continue;
        }
        let dim = *n as usize;
        // Find an HNSW index covering this column and decode its
        // `ef_construction` build param (if any).
        let ef_construction = indexes
            .iter()
            .filter(|idx| idx.access_method == "hnsw" && idx.columns.iter().any(|c| c == field.name()))
            .find_map(|idx| idx.opclass.as_deref().and_then(decode_ef_construction));
        let array = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| {
                BasinError::storage(format!(
                    "column {} typed FixedSizeList but downcast failed",
                    field.name()
                ))
            })?;
        let values = array
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| {
                BasinError::storage(format!(
                    "FixedSizeList child of column {} not Float32Array",
                    field.name()
                ))
            })?;
        let mut builder = HnswIndexBuilder::new(Distance::L2, dim)
            .with_ef_construction(ef_construction);
        for row_idx in 0..array.len() {
            if array.is_null(row_idx) {
                continue;
            }
            let mut v = Vec::with_capacity(dim);
            let base = row_idx * dim;
            let mut had_null = false;
            for k in 0..dim {
                let idx = base + k;
                if values.is_null(idx) {
                    had_null = true;
                    break;
                }
                v.push(values.value(idx));
            }
            if had_null {
                continue;
            }
            // Row id is the row's position within this Parquet file. The
            // engine's `vector_search` translates these back to the matching
            // logical rows by reading the Parquet body.
            builder.insert(row_idx as u64, v)?;
        }
        let index = builder.build();
        let key = index_segment_key(
            storage.root_prefix(),
            project,
            table,
            field.name(),
            data_ulid,
        );
        let mut buf: Vec<u8> = Vec::new();
        index.write_to(&mut buf)?;
        storage
            .project_store(project)
            .put(&key, PutPayload::from_bytes(Bytes::from(buf)))
            .await
            .map_err(|e| BasinError::storage(format!("put hnsw {key}: {e}")))?;
    }
    Ok(())
}

/// One match from a vector search.
#[derive(Clone, Debug)]
pub struct VectorHit {
    pub file_path: ObjectPath,
    pub row_id: u64,
    pub distance: f32,
}

/// Search the HNSW segments for `(project, table, column)`, merging the per-
/// segment top-k into a global top-k by distance.
///
/// `distance` is the metric the caller wants applied. The index file's own
/// header records the metric it was built with; we currently only build L2
/// segments (see `build_indexes_for_batch`). For now `distance` is checked
/// against the segment header; mismatches are an error so the caller doesn't
/// silently get a different metric than they asked for.
pub(crate) async fn vector_search(
    storage: &Storage,
    project: &ProjectId,
    table: &TableName,
    column: &str,
    query: &[f32],
    k: usize,
    distance: Distance,
) -> Result<Vec<VectorHit>> {
    let prefix = column_index_prefix(storage.root_prefix(), project, table, column);
    let store = storage.project_store(project);

    // Build a map from data-file ULID to data-file path so we can return the
    // matching Parquet path with each hit. The two are linked by the shared
    // ULID embedded in both filenames.
    let mut data_paths: std::collections::HashMap<String, ObjectPath> =
        std::collections::HashMap::new();
    for df in crate::reader::list_data_files(storage, project, table).await? {
        if let Some(stem) = df
            .path
            .as_ref()
            .rsplit('/')
            .next()
            .and_then(|f| f.strip_suffix(".parquet"))
        {
            data_paths.insert(stem.to_string(), df.path.clone());
        }
    }

    // List the segments first, then fan out per-segment loads in parallel.
    // With many small segments (one per Parquet write before compaction)
    // sequential `get` per segment dominates wall-clock; the substrate is
    // an object store, where parallelism is the cheapest knob we have.
    let mut paths: Vec<ObjectPath> = Vec::new();
    let mut stream = store.list(Some(&prefix));
    while let Some(meta) = stream.next().await {
        let meta = meta.map_err(|e| BasinError::storage(format!("list hnsw: {e}")))?;
        if meta.location.as_ref().ends_with(".hnsw") {
            paths.push(meta.location);
        }
    }

    // Per-segment cache: parsed `HnswIndex`es are immutable on disk (each
    // is written once under a fresh ULID), so once cached we can reuse
    // without invalidation. Same invariant as the parquet metadata cache.
    let segment_cache = storage.hnsw_segment_cache().clone();
    let segment_results: Vec<Result<(ObjectPath, std::sync::Arc<HnswIndex>)>> =
        futures::stream::iter(paths)
            .map(|loc| {
                let store = store.clone();
                let cache = segment_cache.clone();
                async move {
                    if let Some(cached) = cache.get(&loc) {
                        return Ok::<_, BasinError>((loc, cached));
                    }
                    let bytes = store
                        .get(&loc)
                        .await
                        .map_err(|e| BasinError::storage(format!("get hnsw {loc}: {e}")))?
                        .bytes()
                        .await
                        .map_err(|e| BasinError::storage(format!("read hnsw {loc}: {e}")))?;
                    let idx = std::sync::Arc::new(HnswIndex::read_from(bytes.as_ref())?);
                    cache.insert(loc.clone(), idx.clone());
                    Ok::<_, BasinError>((loc, idx))
                }
            })
            .buffer_unordered(16)
            .collect()
            .await;

    let mut hits: Vec<VectorHit> = Vec::new();
    for r in segment_results {
        let (loc, index) = r?;
        if index.distance() != distance {
            return Err(BasinError::storage(format!(
                "segment {loc} has distance {:?}, requested {:?}",
                index.distance(),
                distance
            )));
        }
        let data_path =
            data_path_for_index_segment(&loc, &data_paths).unwrap_or_else(|| loc.clone());
        for r in index.search(query, k)? {
            hits.push(VectorHit {
                file_path: data_path.clone(),
                row_id: r.id,
                distance: r.distance,
            });
        }
    }

    hits.sort_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    hits.truncate(k);
    Ok(hits)
}

/// `{root?}/projects/{t}/tables/{tbl}/index/{col}.hnsw/`. Matches the layout
/// the ADR pins; the trailing `.hnsw/` is kept on the directory so listing
/// for one column never picks up another column's segments.
fn column_index_prefix(
    root: Option<&ObjectPath>,
    project: &ProjectId,
    table: &TableName,
    column: &str,
) -> ObjectPath {
    let mut p = root.cloned().unwrap_or_else(|| ObjectPath::from(""));
    p = p.child(crate::paths::PROJECTS_SEGMENT);
    p = p.child(project.as_prefix());
    p = p.child("tables");
    p = p.child(table.as_str());
    p = p.child("index");
    p.child(format!("{column}.hnsw"))
}

fn index_segment_key(
    root: Option<&ObjectPath>,
    project: &ProjectId,
    table: &TableName,
    column: &str,
    file_id: Ulid,
) -> ObjectPath {
    column_index_prefix(root, project, table, column).child(format!("{file_id}.hnsw"))
}

/// Public helper: given a data file path under the standard
/// `projects/{t}/tables/{tbl}/data/.../{ulid}.parquet` layout, return the
/// HNSW sidecar path that *would* exist for `column` if that column had
/// embeddings in this file. The engine uses this for best-effort cleanup
/// after a copy-on-write rewrite drops the parent data file — stale
/// sidecars are not a correctness bug (search merges across all
/// segments) but they waste object-store quota over time.
///
/// Returns `None` if the data path can't be parsed (e.g. a bare
/// non-project path), so the caller can skip cleanup without an error.
pub fn vector_index_segment_key_for_data_file(
    root: Option<&ObjectPath>,
    project: &ProjectId,
    table: &TableName,
    column: &str,
    data_file_path: &str,
) -> Option<ObjectPath> {
    let last = data_file_path.rsplit('/').next()?;
    let stem = last.strip_suffix(".parquet")?;
    let ulid: Ulid = stem.parse().ok()?;
    Some(index_segment_key(root, project, table, column, ulid))
}

/// Map an index-segment path back to the source data-file path. Both files
/// are named with the same ULID stem, so we look up the data file whose name
/// matches the index segment's ULID stem.
fn data_path_for_index_segment(
    idx_path: &ObjectPath,
    data_paths: &std::collections::HashMap<String, ObjectPath>,
) -> Option<ObjectPath> {
    let last = idx_path.as_ref().rsplit('/').next()?;
    let stem = last.strip_suffix(".hnsw")?;
    data_paths.get(stem).cloned()
}

/// Decode the `ef_construction` build parameter out of a persisted catalog
/// opclass string. The engine encodes HNSW params as
/// `<opclass>[;m=<n>][;ef_construction=<n>]` (optionally prefixed `ivfflat:`);
/// here we only need the `ef_construction` token. Returns `None` when the
/// param is absent or unparseable (keep the library default).
fn decode_ef_construction(raw: &str) -> Option<usize> {
    let raw = raw.strip_prefix("ivfflat:").unwrap_or(raw);
    for kv in raw.split(';') {
        if let Some((k, v)) = kv.split_once('=') {
            if k.trim() == "ef_construction" {
                return v.trim().parse().ok();
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::types::Float32Type;
    use arrow_array::{FixedSizeListArray, Int64Array};
    use arrow_schema::{DataType, Field, Schema};
    use basin_common::{PartitionKey, ProjectId, TableName};
    use basin_vector::Distance;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use super::*;
    use crate::{Storage, StorageConfig};

    #[test]
    fn decode_ef_construction_variants() {
        assert_eq!(decode_ef_construction("vector_l2_ops"), None);
        assert_eq!(
            decode_ef_construction("vector_l2_ops;m=16;ef_construction=64"),
            Some(64)
        );
        assert_eq!(
            decode_ef_construction("ivfflat:vector_ip_ops;ef_construction=40"),
            Some(40)
        );
        assert_eq!(decode_ef_construction("vector_l2_ops;m=16"), None);
    }

    fn embedding_schema(dim: i32) -> Arc<Schema> {
        // The Arrow `FixedSizeListArray::from_iter_primitive` builder creates
        // a child field with `nullable=true` regardless of the values, so the
        // schema we declare here has to match it; otherwise RecordBatch::new
        // rejects the column with "column types must match schema types".
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
                false,
            ),
        ]))
    }

    fn build_batch(start: i64, len: usize, dim: usize) -> RecordBatch {
        let ids: Int64Array = (start..start + len as i64).collect();
        let rows: Vec<Option<Vec<Option<f32>>>> = (0..len)
            .map(|i| {
                Some(
                    (0..dim)
                        .map(|d| Some(((start + i as i64) as f32) * 0.001 + d as f32 * 0.01))
                        .collect(),
                )
            })
            .collect();
        let emb = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(rows, dim as i32);
        RecordBatch::try_new(
            embedding_schema(dim as i32),
            vec![Arc::new(ids), Arc::new(emb)],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn write_then_search_finds_self() {
        let dir = TempDir::new().unwrap();
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let project = ProjectId::new();
        let table = TableName::new("docs").unwrap();
        let part = PartitionKey::default_key();
        let dim = 8usize;

        let batch = build_batch(0, 100, dim);
        storage
            .write_batch(&project, &table, &part, &batch)
            .await
            .unwrap();

        // Query exactly the embedding for row 42; it must appear in the top-5.
        let query: Vec<f32> = (0..dim)
            .map(|d| (42_f32) * 0.001 + d as f32 * 0.01)
            .collect();
        let hits = storage
            .vector_search(&project, &table, "embedding", &query, 5, Distance::L2)
            .await
            .unwrap();
        assert!(!hits.is_empty(), "vector_search returned empty");
        assert!(
            hits.iter().any(|h| h.row_id == 42),
            "top-5 missed self: {:?}",
            hits.iter().map(|h| h.row_id).collect::<Vec<_>>()
        );
    }
}
