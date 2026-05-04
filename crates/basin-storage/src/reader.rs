//! Parquet reader with projection + predicate pushdown.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use basin_common::{BasinError, Result, TableName, TenantId};
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, RowFilter};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;

use crate::data_file::{ColumnStats, DataFile};
use crate::metadata_cache::{CachedParquetMeta, ParquetMetaCache};
use crate::paths::table_data_prefix;
use crate::predicate::{self, Predicate, ScalarValue};
use crate::{ReadOptions, Storage};

pub(crate) async fn list_data_files(
    storage: &Storage,
    tenant: &TenantId,
    table: &TableName,
) -> Result<Vec<DataFile>> {
    let prefix = table_data_prefix(storage.root_prefix(), tenant, table);
    let store = storage.tenant_store(tenant);
    let mut files = Vec::new();
    let mut stream = store.list(Some(&prefix));
    while let Some(meta) = stream.next().await {
        let meta = meta.map_err(|e| BasinError::storage(format!("list: {e}")))?;
        if !meta.location.as_ref().ends_with(".parquet") {
            continue;
        }
        files.push(DataFile {
            path: meta.location,
            size_bytes: meta.size as u64,
            row_count: 0,
            column_stats: BTreeMap::new(),
        });
    }
    Ok(files)
}

/// Same as [`list_data_files`] but also pulls the Parquet footer for each
/// file and populates `row_count` plus `column_stats` (the latter mirrors
/// the writer's `extract_column_stats` shape so a freshly-listed file
/// looks identical to a freshly-written one). Uses the metadata cache so
/// repeated listings are footer-fetch-free.
pub(crate) async fn list_data_files_with_stats(
    storage: &Storage,
    tenant: &TenantId,
    table: &TableName,
) -> Result<Vec<DataFile>> {
    let mut files = list_data_files(storage, tenant, table).await?;
    let store = storage.tenant_store(tenant);
    let cache = storage.parquet_meta_cache().clone();

    // Fan out per-file footer reads with bounded concurrency. Each cache
    // hit is a no-op; cache misses do one short range GET each.
    let work: Vec<_> = files
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let store = store.clone();
            let cache = cache.clone();
            let path = f.path.clone();
            async move {
                let meta = if let Some(cached) = cache.get(&path) {
                    cached.meta
                } else {
                    let head = store
                        .head(&path)
                        .await
                        .map_err(|e| BasinError::storage(format!("head {path}: {e}")))?;
                    let size = head.size as u64;
                    let mut reader = ParquetObjectReader::new(store.clone(), head);
                    let arrow_meta =
                        ArrowReaderMetadata::load_async(&mut reader, ArrowReaderOptions::default())
                            .await
                            .map_err(|e| {
                                BasinError::storage(format!("open parquet {path}: {e}"))
                            })?;
                    let m = arrow_meta.metadata().clone();
                    cache.insert(
                        path.clone(),
                        CachedParquetMeta {
                            meta: m.clone(),
                            size,
                        },
                    );
                    m
                };
                Result::<_>::Ok((i, decode_file_stats(&meta)))
            }
        })
        .collect();

    let resolved: Vec<Result<(usize, (u64, BTreeMap<String, ColumnStats>))>> =
        futures::stream::iter(work).buffer_unordered(8).collect().await;

    for r in resolved {
        let (i, (rows, stats)) = r?;
        files[i].row_count = rows;
        files[i].column_stats = stats;
    }
    Ok(files)
}

/// Extract per-file stats from a parsed Parquet footer. Mirrors the
/// writer's aggregation rule so a listed file's stats are byte-equivalent
/// to a freshly-written one's.
fn decode_file_stats(
    meta: &parquet::file::metadata::ParquetMetaData,
) -> (u64, BTreeMap<String, ColumnStats>) {
    let mut out: BTreeMap<String, ColumnStats> = BTreeMap::new();
    let mut total_rows: u64 = 0;
    for rg in meta.row_groups() {
        total_rows += rg.num_rows() as u64;
        for col in rg.columns() {
            let name = col.column_descr().name().to_string();
            let entry = out.entry(name).or_default();
            if let Some(stats) = col.statistics() {
                if let Some(n) = stats.null_count_opt() {
                    entry.null_count = Some(entry.null_count.unwrap_or(0) + n);
                }
                merge_typed_stats(entry, stats);
            }
        }
    }
    (total_rows, out)
}

/// Mirror of `writer::merge_typed_stats`: uses typed comparisons for
/// primitive numerics so the merged bytes round-trip back to the actual
/// min/max. Lexicographic merge is correct for byte-array (Utf8) stats.
fn merge_typed_stats(
    entry: &mut ColumnStats,
    stats: &parquet::file::statistics::Statistics,
) {
    use parquet::file::statistics::Statistics as ParquetStats;
    match stats {
        ParquetStats::Int64(s) => {
            if let Some(min) = s.min_opt() {
                let cur = entry.min_bytes.as_deref().and_then(decode_le_i64);
                if !matches!(cur, Some(prev) if prev <= *min) {
                    entry.min_bytes = Some(min.to_le_bytes().to_vec());
                }
            }
            if let Some(max) = s.max_opt() {
                let cur = entry.max_bytes.as_deref().and_then(decode_le_i64);
                if !matches!(cur, Some(prev) if prev >= *max) {
                    entry.max_bytes = Some(max.to_le_bytes().to_vec());
                }
            }
        }
        ParquetStats::Double(s) => {
            if let Some(min) = s.min_opt() {
                let cur = entry.min_bytes.as_deref().and_then(decode_le_f64);
                if !matches!(cur, Some(prev) if prev <= *min) {
                    entry.min_bytes = Some(min.to_le_bytes().to_vec());
                }
            }
            if let Some(max) = s.max_opt() {
                let cur = entry.max_bytes.as_deref().and_then(decode_le_f64);
                if !matches!(cur, Some(prev) if prev >= *max) {
                    entry.max_bytes = Some(max.to_le_bytes().to_vec());
                }
            }
        }
        ParquetStats::Boolean(s) => {
            if let Some(min) = s.min_opt() {
                let prev_min = entry
                    .min_bytes
                    .as_deref()
                    .and_then(|b| b.first().copied())
                    .map(|b| b != 0);
                if !matches!(prev_min, Some(prev) if prev <= *min) {
                    entry.min_bytes = Some(vec![if *min { 1u8 } else { 0u8 }]);
                }
            }
            if let Some(max) = s.max_opt() {
                let prev_max = entry
                    .max_bytes
                    .as_deref()
                    .and_then(|b| b.first().copied())
                    .map(|b| b != 0);
                if !matches!(prev_max, Some(prev) if prev >= *max) {
                    entry.max_bytes = Some(vec![if *max { 1u8 } else { 0u8 }]);
                }
            }
        }
        ParquetStats::ByteArray(_) | ParquetStats::FixedLenByteArray(_) => {
            if let Some(min) = stats.min_bytes_opt() {
                let v = min.to_vec();
                if entry.min_bytes.as_deref().map(|p| p > v.as_slice()).unwrap_or(true) {
                    entry.min_bytes = Some(v);
                }
            }
            if let Some(max) = stats.max_bytes_opt() {
                let v = max.to_vec();
                if entry.max_bytes.as_deref().map(|p| p < v.as_slice()).unwrap_or(true) {
                    entry.max_bytes = Some(v);
                }
            }
        }
        _ => {
            if let Some(min) = stats.min_bytes_opt() {
                if entry.min_bytes.is_none() {
                    entry.min_bytes = Some(min.to_vec());
                }
            }
            if let Some(max) = stats.max_bytes_opt() {
                if entry.max_bytes.is_none() {
                    entry.max_bytes = Some(max.to_vec());
                }
            }
        }
    }
}

fn decode_le_i64(b: &[u8]) -> Option<i64> {
    if b.len() != 8 {
        return None;
    }
    let mut a = [0u8; 8];
    a.copy_from_slice(b);
    Some(i64::from_le_bytes(a))
}

fn decode_le_f64(b: &[u8]) -> Option<f64> {
    if b.len() != 8 {
        return None;
    }
    let mut a = [0u8; 8];
    a.copy_from_slice(b);
    Some(f64::from_le_bytes(a))
}

pub(crate) async fn read(
    storage: &Storage,
    tenant: &TenantId,
    table: &TableName,
    opts: ReadOptions,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let prefix = table_data_prefix(storage.root_prefix(), tenant, table);
    let store = storage.tenant_store(tenant);
    let tenant_id_string = tenant.as_prefix();

    let mut paths: Vec<ObjectPath> = Vec::new();
    {
        let mut s = store.list(Some(&prefix));
        while let Some(meta) = s.next().await {
            let meta = meta.map_err(|e| BasinError::storage(format!("list: {e}")))?;
            if !meta.location.as_ref().ends_with(".parquet") {
                continue;
            }
            // Belt-and-braces: never read a file whose key isn't under the
            // tenant prefix. If this ever fired we'd want a P0; we treat it
            // as `IsolationViolation`, not `Storage`.
            let expected = format!("tenants/{tenant_id_string}/");
            if !meta.location.as_ref().contains(&expected) {
                return Err(BasinError::isolation(format!(
                    "listed object {} does not contain {}",
                    meta.location, expected
                )));
            }
            paths.push(meta.location);
        }
    }

    let opts = Arc::new(opts);
    let store_for_stream = store.clone();
    let cache = storage.parquet_meta_cache().clone();
    let stream = futures::stream::iter(paths)
        .map(move |p| {
            let store = store_for_stream.clone();
            let opts = opts.clone();
            let cache = cache.clone();
            async move { read_one(store, p, opts, cache).await }
        })
        .buffered(4)
        .map(|res: Result<BoxStream<'static, Result<RecordBatch>>>| match res {
            Ok(s) => s,
            Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
        })
        .flatten();

    Ok(stream.boxed())
}

/// Public entry for reading a single data file's contents. Mirrors what
/// the table-wide reader does per-file (footer cache hit, projection on,
/// row-group pruning by stats), but skipped projection + zero filters
/// because the UPDATE/DELETE rewrite path needs every row of the chosen
/// file. Tenant-prefix enforcement is the caller's responsibility — this
/// function trusts the path it's handed.
pub(crate) async fn read_file(
    storage: &Storage,
    tenant: &TenantId,
    path: &ObjectPath,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let opts = Arc::new(ReadOptions::default());
    let store = storage.tenant_store(tenant);
    let cache = storage.parquet_meta_cache().clone();
    read_one(store, path.clone(), opts, cache).await
}

async fn read_one(
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    opts: Arc<ReadOptions>,
    meta_cache: Arc<ParquetMetaCache>,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    // Cache lookup. On hit we skip BOTH the footer fetch and the HEAD
    // round-trip — we have the file size in cache, so we can synthesise
    // an `ObjectMeta` (the `ParquetObjectReader` only reads `location`
    // and `size` off it; `last_modified`/`e_tag` are unused for our
    // immutable-data-files invariant).
    //
    // On miss we do the full HEAD + footer-fetch path and populate the
    // cache for next time.
    let builder = if let Some(cached) = meta_cache.get(&path) {
        let synthetic = object_store::ObjectMeta {
            location: path.clone(),
            last_modified: chrono::Utc::now(),
            size: cached.size as usize,
            e_tag: None,
            version: None,
        };
        let reader = ParquetObjectReader::new(store, synthetic);
        let arrow_meta = ArrowReaderMetadata::try_new(cached.meta, ArrowReaderOptions::default())
            .map_err(|e| BasinError::storage(format!("rehydrate parquet meta {path}: {e}")))?;
        ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_meta)
    } else {
        let head = store
            .head(&path)
            .await
            .map_err(|e| BasinError::storage(format!("head {path}: {e}")))?;
        let size = head.size as u64;
        let mut reader = ParquetObjectReader::new(store, head);
        let arrow_meta = ArrowReaderMetadata::load_async(&mut reader, ArrowReaderOptions::default())
            .await
            .map_err(|e| BasinError::storage(format!("open parquet {path}: {e}")))?;
        // Insert the parsed metadata into the cache for next time. Cloning
        // is cheap — `ArrowReaderMetadata` carries an `Arc<ParquetMetaData>`
        // internally, so we extract that arc and share it.
        meta_cache.insert(
            path.clone(),
            CachedParquetMeta {
                meta: arrow_meta.metadata().clone(),
                size,
            },
        );
        ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_meta)
    };

    let arrow_schema: SchemaRef = builder.schema().clone();
    let parquet_schema = builder.metadata().file_metadata().schema_descr_ptr();

    // Projection.
    let projection_mask = match &opts.projection {
        Some(cols) => {
            let mut idxs = Vec::with_capacity(cols.len());
            for c in cols {
                let i = arrow_schema
                    .index_of(c)
                    .map_err(|_| BasinError::storage(format!("unknown column {c}")))?;
                idxs.push(i);
            }
            // Use roots() with column indexes — this works for flat schemas;
            // for nested schemas we'd descend, but Phase 1 is flat-only.
            ProjectionMask::roots(&parquet_schema, idxs)
        }
        None => ProjectionMask::all(),
    };

    // Row-group pruning by stats.
    let kept: Vec<usize> = {
        let row_groups = builder.metadata().row_groups();
        (0..row_groups.len())
            .filter(|i| !row_group_pruned(&row_groups[*i], &arrow_schema, &opts.filters))
            .collect()
    };

    let mut builder = builder
        .with_projection(projection_mask.clone())
        .with_row_groups(kept);

    // Per-row filtering as a fallback. Pushed in via RowFilter so the parquet
    // reader can also use it for page index pruning where available.
    if !opts.filters.is_empty() {
        let predicates = build_row_filter(
            &opts.filters,
            &arrow_schema,
            &parquet_schema,
        )?;
        builder = builder.with_row_filter(predicates);
    }

    let stream = builder
        .build()
        .map_err(|e| BasinError::storage(format!("parquet build {path}: {e}")))?;

    let mapped = stream.map(|res| res.map_err(|e| BasinError::storage(format!("parquet read: {e}"))));
    Ok(mapped.boxed())
}

fn build_row_filter(
    filters: &[Predicate],
    arrow_schema: &SchemaRef,
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
) -> Result<RowFilter> {
    let mut predicates: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> =
        Vec::with_capacity(filters.len());

    for filter in filters {
        let col = filter.column().to_string();
        let col_idx = arrow_schema
            .index_of(&col)
            .map_err(|_| BasinError::storage(format!("unknown column {col}")))?;
        let mask = ProjectionMask::roots(parquet_schema, [col_idx]);
        let f = filter.clone();
        let pred = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
            predicate::evaluate(&batch, &f)
                .map_err(|e| arrow_schema::ArrowError::ExternalError(Box::new(e)))
        });
        predicates.push(Box::new(pred));
    }

    // Combined RowFilter ANDs predicates.
    Ok(RowFilter::new(predicates))
}

/// Decide if an entire row group can be pruned given the conjunction of
/// filters.
fn row_group_pruned(
    rg: &RowGroupMetaData,
    arrow_schema: &SchemaRef,
    filters: &[Predicate],
) -> bool {
    if filters.is_empty() {
        return false;
    }
    for f in filters {
        if predicate_excludes_group(rg, arrow_schema, f) {
            return true;
        }
    }
    false
}

fn predicate_excludes_group(
    rg: &RowGroupMetaData,
    arrow_schema: &SchemaRef,
    filter: &Predicate,
) -> bool {
    let col = filter.column();
    let Ok(idx) = arrow_schema.index_of(col) else {
        return false;
    };
    let Some(col_meta) = rg.columns().get(idx) else {
        return false;
    };
    let Some(stats) = col_meta.statistics() else {
        return false;
    };

    let Some(value) = filter_value(filter) else {
        return false;
    };

    match (filter, &value, stats) {
        (Predicate::Eq(_, _), ScalarValue::Int64(v), Statistics::Int64(s)) => {
            let min = s.min_opt().copied();
            let max = s.max_opt().copied();
            match (min, max) {
                (Some(min), Some(max)) => *v < min || *v > max,
                _ => false,
            }
        }
        (Predicate::Lt(_, _), ScalarValue::Int64(v), Statistics::Int64(s)) => {
            // exclude if every value >= v
            s.min_opt().copied().is_some_and(|min| min >= *v)
        }
        (Predicate::Gt(_, _), ScalarValue::Int64(v), Statistics::Int64(s)) => {
            s.max_opt().copied().is_some_and(|max| max <= *v)
        }
        (Predicate::Eq(_, _), ScalarValue::Utf8(v), Statistics::ByteArray(s)) => {
            let bytes = v.as_bytes();
            let min = s.min_opt().map(|b| b.data());
            let max = s.max_opt().map(|b| b.data());
            match (min, max) {
                (Some(min), Some(max)) => bytes < min || bytes > max,
                _ => false,
            }
        }
        _ => false,
    }
}

fn filter_value(filter: &Predicate) -> Option<ScalarValue> {
    match filter {
        Predicate::Eq(_, v) | Predicate::Gt(_, v) | Predicate::Lt(_, v) => Some(v.clone()),
    }
}

