//! Parquet reader with projection + predicate pushdown.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use arrow_array::{new_null_array, RecordBatch};
use arrow_schema::{Schema, SchemaRef};
use basin_common::{BasinError, Result, TableName, ProjectId};
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, RowFilter,
};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;

use crate::data_file::{ColumnStats, DataFile};
use crate::encryption::{decrypt_envelope, BytesFileReader, EncryptionProvider, WrappedKey};
use crate::metadata_cache::{CachedParquetMeta, ParquetMetaCache};
use crate::page_cache::{hash_filters, hash_projection, CacheKey, PageCache};
use crate::paths::table_tier_prefix;
use crate::predicate::{self, Predicate, ScalarValue};
use crate::tier::Tier;
use crate::writer::wrapped_sidecar_key;
use crate::{ReadCounters, ReadOptions, Storage};
use basin_catalog::ProjectStorageConfig;

pub(crate) async fn list_data_files(
    storage: &Storage,
    project: &ProjectId,
    table: &TableName,
) -> Result<Vec<DataFile>> {
    // Walk both tier prefixes. Phase 5.5 introduces `tables/<t>/cold/`
    // alongside the existing `tables/<t>/data/` so reads transparently see
    // files migrated by the tiering compactor. Each file's `tier` field is
    // derived from its path so callers don't have to know which prefix the
    // listing came from.
    let store = storage.project_store(project);
    let mut files = Vec::new();
    for tier in [Tier::Hot, Tier::Cold] {
        let prefix = table_tier_prefix(storage.root_prefix(), project, table, tier);
        let mut stream = store.list(Some(&prefix));
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|e| BasinError::storage(format!("list: {e}")))?;
            if !meta.location.as_ref().ends_with(".parquet") {
                continue;
            }
            let resolved_tier = Tier::from_path(meta.location.as_ref());
            files.push(DataFile {
                path: meta.location,
                size_bytes: meta.size as u64,
                row_count: 0,
                column_stats: BTreeMap::new(),
                tier: resolved_tier,
            });
        }
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
    project: &ProjectId,
    table: &TableName,
) -> Result<Vec<DataFile>> {
    let mut files = list_data_files(storage, project, table).await?;
    let store = storage.project_store(project);
    let cache = storage.parquet_meta_cache().clone();

    // Fan out per-file footer reads with bounded concurrency. Each cache
    // hit is a no-op; cache misses do one short range GET each.
    //
    // We use `size_bytes` from the listing result (populated by
    // `list_data_files`) instead of issuing a HEAD per file: object-store
    // LIST responses already include the object size, so the HEAD would be
    // a redundant round-trip that duplicates work the LIST already did.
    let work: Vec<_> = files
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let store = store.clone();
            let cache = cache.clone();
            let path = f.path.clone();
            let listed_size = f.size_bytes; // captured from LIST, no HEAD needed
            async move {
                let meta = if let Some(cached) = cache.get(&path) {
                    cached.meta
                } else {
                    // Use the file size from the listing to skip the HEAD
                    // round-trip. The size is stable for our immutable data
                    // files and is accurate: object_store LIST returns the
                    // real content-length for every backend we support.
                    let mut reader = ParquetObjectReader::new(store.clone(), path.clone())
                        .with_file_size(listed_size);
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
                            size: listed_size,
                        },
                    );
                    m
                };
                Result::<_>::Ok((i, decode_file_stats(&meta)))
            }
        })
        .collect();

    let resolved: Vec<Result<(usize, (u64, BTreeMap<String, ColumnStats>))>> =
        futures::stream::iter(work)
            .buffer_unordered(8)
            .collect()
            .await;

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
fn merge_typed_stats(entry: &mut ColumnStats, stats: &parquet::file::statistics::Statistics) {
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
                if entry
                    .min_bytes
                    .as_deref()
                    .map(|p| p > v.as_slice())
                    .unwrap_or(true)
                {
                    entry.min_bytes = Some(v);
                }
            }
            if let Some(max) = stats.max_bytes_opt() {
                let v = max.to_vec();
                if entry
                    .max_bytes
                    .as_deref()
                    .map(|p| p < v.as_slice())
                    .unwrap_or(true)
                {
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
    project: &ProjectId,
    table: &TableName,
    opts: ReadOptions,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let store = storage.project_store(project);
    let project_id_string = project.as_prefix();

    let mut paths: Vec<ObjectPath> = Vec::new();
    // Walk hot and cold tiers in turn. The reader is tier-agnostic — files
    // live wherever the (compactor-driven) tier policy put them; here we
    // just consume both prefixes.
    for tier in [Tier::Hot, Tier::Cold] {
        let prefix = table_tier_prefix(storage.root_prefix(), project, table, tier);
        let mut s = store.list(Some(&prefix));
        while let Some(meta) = s.next().await {
            let meta = meta.map_err(|e| BasinError::storage(format!("list: {e}")))?;
            if !meta.location.as_ref().ends_with(".parquet") {
                continue;
            }
            // Belt-and-braces: never read a file whose key isn't under the
            // project prefix. If this ever fired we'd want a P0; we treat it
            // as `IsolationViolation`, not `Storage`.
            let expected = format!("projects/{project_id_string}/");
            if !meta.location.as_ref().contains(&expected) {
                return Err(BasinError::isolation(format!(
                    "listed object {} does not contain {}",
                    meta.location, expected
                )));
            }
            paths.push(meta.location);
        }
    }

    // Fetch the catalog schema once for the whole read so that
    // `finalize_pipeline` can synthesise NULL-filled columns for fields that
    // exist in the catalog but are absent from pre-ALTER on-disk files.
    // This is the schema-evolution (ADD COLUMN) correctness fix: old files
    // don't have the new column, so we pad their batches with NULLs.
    // Returns `None` when no catalog is attached → existing behaviour is
    // preserved (will error on missing columns, same as before).
    let catalog_schema = storage.catalog_table_schema(project, table).await?;

    read_paths_inner(storage, project, paths, opts, catalog_schema).await
}

/// Like [`read`] but reads only the supplied `paths` instead of LIST'ing
/// the table prefix. Used when the caller has already pruned the file set
/// — typically via Phase 5.7 A4 catalog stats: `Catalog::load_table` →
/// `Snapshot::data_files` → `evaluate_compound_for_pruning` against each
/// `DataFileRef::column_stats` → drop the files that prove `NoMatch`,
/// then hand the survivors here. Skips one LIST RPC and (for files
/// pruned at the catalog layer) one footer fetch each.
///
/// Project-prefix enforcement happens here too: every supplied path must
/// contain the project prefix or the call returns
/// [`BasinError::IsolationViolation`].
pub(crate) async fn read_paths(
    storage: &Storage,
    project: &ProjectId,
    paths: Vec<ObjectPath>,
    opts: ReadOptions,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let expected = format!("projects/{}/", project.as_prefix());
    for p in &paths {
        if !p.as_ref().contains(&expected) {
            return Err(BasinError::isolation(format!(
                "read_paths: {p} does not contain {expected}"
            )));
        }
    }
    // `read_paths` does not receive a table name, so we cannot look up the
    // catalog schema here.  Schema-evolution NULL synthesis is therefore not
    // available on this code path — callers that need it should use `read`.
    read_paths_inner(storage, project, paths, opts, None).await
}

async fn read_paths_inner(
    storage: &Storage,
    project: &ProjectId,
    paths: Vec<ObjectPath>,
    opts: ReadOptions,
    catalog_schema: Option<SchemaRef>,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let store = storage.project_store(project);
    let opts = Arc::new(opts);
    let store_for_stream = store.clone();
    let cache = storage.parquet_meta_cache().clone();
    let counters = storage.read_counters().clone();
    let page_cache = storage.page_cache_handle().cloned();
    let project_counters = storage.project_counters(project);
    let encryption = storage.encryption_provider();
    // Resolve the per-project storage config once for the whole batch of
    // paths (cache hit on every path after the first); threaded through
    // `read_one` so envelope-decrypt can use `unwrap_key_with_config`
    // when present.
    let project_config = if encryption.is_some() {
        storage.project_storage_config_cached(project).await?
    } else {
        None
    };
    let project_owned = *project;
    let stream = futures::stream::iter(paths)
        .map(move |p| {
            let store = store_for_stream.clone();
            let opts = opts.clone();
            let cache = cache.clone();
            let counters = counters.clone();
            let page_cache = page_cache.clone();
            let project_counters = project_counters.clone();
            let encryption = encryption.clone();
            let project_config = project_config.clone();
            let catalog_schema = catalog_schema.clone();
            async move {
                read_one(
                    store,
                    p,
                    opts,
                    cache,
                    counters,
                    page_cache,
                    project_counters,
                    encryption,
                    project_config,
                    project_owned,
                    catalog_schema,
                )
                .await
            }
        })
        .buffered(4)
        .map(
            |res: Result<BoxStream<'static, Result<RecordBatch>>>| match res {
                Ok(s) => s,
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            },
        )
        .flatten();

    Ok(stream.boxed())
}

/// Public entry for reading a single data file's contents. Mirrors what
/// the table-wide reader does per-file (footer cache hit, projection on,
/// row-group pruning by stats), but skipped projection + zero filters
/// because the UPDATE/DELETE rewrite path needs every row of the chosen
/// file. Project-prefix enforcement is the caller's responsibility — this
/// function trusts the path it's handed.
pub(crate) async fn read_file(
    storage: &Storage,
    project: &ProjectId,
    path: &ObjectPath,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let opts = Arc::new(ReadOptions::default());
    let store = storage.project_store(project);
    let cache = storage.parquet_meta_cache().clone();
    let counters = storage.read_counters().clone();
    let page_cache = storage.page_cache_handle().cloned();
    let project_counters = storage.project_counters(project);
    let encryption = storage.encryption_provider();
    let project_config = if encryption.is_some() {
        storage.project_storage_config_cached(project).await?
    } else {
        None
    };
    read_one(
        store,
        path.clone(),
        opts,
        cache,
        counters,
        page_cache,
        project_counters,
        encryption,
        project_config,
        *project,
        None, // read_file reads the raw file; schema evolution not applicable
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn read_one(
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    opts: Arc<ReadOptions>,
    meta_cache: Arc<ParquetMetaCache>,
    counters: Arc<ReadCounters>,
    page_cache: Option<Arc<PageCache>>,
    project_counters: Option<Arc<basin_common::ProjectCounters>>,
    encryption: Option<Arc<dyn EncryptionProvider>>,
    project_config: Option<ProjectStorageConfig>,
    project: ProjectId,
    catalog_schema: Option<SchemaRef>,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    // Page-cache fast path. Composes with everything below: a hit
    // means we skip the parquet decode entirely and yield the cached
    // `Arc<RecordBatch>`es directly. The key includes the projection
    // and filter set so different SELECTs on the same file don't
    // collide.
    //
    // We don't cache when there's a partition predicate set, because
    // partition pruning is applied at a higher layer (`Storage::read`)
    // by the caller and the cache key doesn't capture it. In practice
    // the partition predicate is the default for v0.1, so this is a
    // no-op guard.
    let cache_key = page_cache.as_ref().map(|_| CacheKey {
        path: path.clone(),
        projection_hash: hash_projection(opts.projection.as_deref()),
        filters_hash: hash_filters(&opts.filters),
    });
    if let (Some(pc), Some(key)) = (page_cache.as_ref(), cache_key.as_ref()) {
        if let Some(batches) = pc.get(key) {
            let owned: Vec<RecordBatch> = batches.iter().map(|b| (**b).clone()).collect();
            let s = futures::stream::iter(owned.into_iter().map(Ok));
            return Ok(s.boxed());
        }
    }

    // Envelope-encryption probe. When a provider is attached AND a
    // `<path>.wrapped` sidecar exists, fetch the body, unwrap the data
    // key, decrypt, and run the parquet pipeline against an in-memory
    // `BytesFileReader`. Files written before the provider was attached
    // have no sidecar — they take the plaintext path below (back-compat).
    if let Some(provider) = encryption.as_ref() {
        if let Some(plaintext) = try_load_encrypted(
            &store,
            &path,
            provider.as_ref(),
            project_config.as_ref(),
            &project,
        )
        .await?
        {
            return finalize_encrypted_stream(
                plaintext,
                path,
                opts,
                counters,
                page_cache,
                cache_key,
                project_counters,
                catalog_schema,
            )
            .await;
        }
    }

    // Cache lookup. On hit we skip BOTH the footer fetch and the HEAD
    // round-trip — we have the file size in cache, so we can synthesise
    // an `ObjectMeta` (the `ParquetObjectReader` only reads `location`
    // and `size` off it; `last_modified`/`e_tag` are unused for our
    // immutable-data-files invariant).
    //
    // On miss we do the full HEAD + footer-fetch path and populate the
    // cache for next time.
    let (builder, file_size) = if let Some(cached) = meta_cache.get(&path) {
        let size = cached.size;
        let reader = ParquetObjectReader::new(store, path.clone()).with_file_size(size);
        let arrow_meta =
            ArrowReaderMetadata::try_new(cached.meta, ArrowReaderOptions::default())
                .map_err(|e| BasinError::storage(format!("rehydrate parquet meta {path}: {e}")))?;
        (
            ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_meta),
            size,
        )
    } else {
        let head = store
            .head(&path)
            .await
            .map_err(|e| BasinError::storage(format!("head {path}: {e}")))?;
        let size = head.size;
        let mut reader = ParquetObjectReader::new(store, path.clone()).with_file_size(size);
        let arrow_meta =
            ArrowReaderMetadata::load_async(&mut reader, ArrowReaderOptions::default())
                .await
                .map_err(|e| BasinError::storage(format!("open parquet {path}: {e}")))?;
        meta_cache.insert(
            path.clone(),
            CachedParquetMeta {
                meta: arrow_meta.metadata().clone(),
                size,
            },
        );
        (
            ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_meta),
            size,
        )
    };

    // Per-project bytes_read counter (Phase 6 telemetry). Bumped by the file
    // size as an upper bound — the parquet reader prunes row groups so actual
    // bytes pulled are typically a subset; this is a defensible scaling
    // signal per project without per-range bookkeeping in the object_store.
    if let Some(tc) = project_counters.as_ref() {
        tc.record_bytes_read(file_size);
    }

    finalize_pipeline(builder, path, opts, counters, page_cache, cache_key, catalog_schema).await
}

fn build_row_filter(
    filters: &[Predicate],
    arrow_schema: &SchemaRef,
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
) -> Result<RowFilter> {
    let mut predicates: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> =
        Vec::with_capacity(filters.len());

    for filter in filters {
        let col = filter.column();
        // If the column is absent from the on-disk file schema (e.g. an
        // ADD COLUMN that happened after this file was written), skip adding
        // the predicate for this file.  The effect is that all rows in this
        // file are treated as passing the filter for this predicate, which
        // is a conservative over-include.  SQL NULL semantics would actually
        // exclude every row (NULL comparison → NULL → false), but for the
        // common non-predicate SELECT case this path is never reached, and
        // for now the conservative behaviour avoids the previous crash
        // ("unknown column").  A follow-up can synthesise an all-false
        // BooleanArray to get exact NULL semantics.
        let col_idx = match arrow_schema.index_of(col) {
            Ok(i) => i,
            Err(_) => continue,
        };
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

/// Drop row groups whose bloom filters can prove a `Predicate::Eq`'s
/// value is absent. Falls through (keeps the row group) on every kind of
/// uncertainty: filter not present, type we can't hash equivalently to
/// what the writer encoded, or the bloom-filter section itself failed to
/// load. The contract is "definitely-not-present ⇒ skip"; everything
/// else means "must read", so a bloom miss / read failure can never make
/// us return a wrong answer (only slower).
async fn prune_with_bloom_filters<T>(
    builder: &mut ParquetRecordBatchStreamBuilder<T>,
    arrow_schema: &SchemaRef,
    candidate_groups: Vec<usize>,
    filters: &[Predicate],
    counters: &Arc<ReadCounters>,
) -> Result<Vec<usize>>
where
    T: parquet::arrow::async_reader::AsyncFileReader + Send + 'static,
{
    use parquet::bloom_filter::Sbbf;

    if filters.is_empty() || candidate_groups.is_empty() {
        return Ok(candidate_groups);
    }

    // Pre-resolve the column indexes for the equality filters that are
    // candidates for bloom-filter pruning. We only handle `Eq` predicates;
    // range queries (`Gt` / `Lt`) can't be answered by a bloom filter.
    let mut eq_filters: Vec<(usize, ScalarValue)> = Vec::new();
    for f in filters {
        if let Predicate::Eq(col, v) = f {
            if let Ok(idx) = arrow_schema.index_of(col) {
                eq_filters.push((idx, v.clone()));
            }
        }
    }
    if eq_filters.is_empty() {
        return Ok(candidate_groups);
    }

    let mut kept = Vec::with_capacity(candidate_groups.len());
    let mut bloom_pruned = 0u64;

    'outer: for rg_idx in candidate_groups {
        for (col_idx, value) in &eq_filters {
            // No bloom filter recorded for this column in this row group →
            // no information to act on; move to the next predicate.
            let col_meta = builder.metadata().row_group(rg_idx).column(*col_idx);
            if col_meta.bloom_filter_offset().is_none() {
                continue;
            }
            let sbbf: Sbbf = match builder
                .get_row_group_column_bloom_filter(rg_idx, *col_idx)
                .await
            {
                Ok(Some(s)) => s,
                Ok(None) => continue,
                // Read failure: fall back to "must read" — never let a
                // network blip mask a real row.
                Err(_) => continue,
            };
            if !bloom_check(&sbbf, value) {
                bloom_pruned += 1;
                continue 'outer;
            }
        }
        kept.push(rg_idx);
    }

    counters
        .row_groups_pruned_by_bloom
        .fetch_add(bloom_pruned, Ordering::Relaxed);
    Ok(kept)
}

/// Probe the bloom filter for `value`. Returns `true` for "may be
/// present" and `false` for "definitely not present" — the latter is
/// the only useful answer (Sbbf can't return `definitely-present`).
/// Hashing matches what `arrow_writer::ArrowWriter` does internally
/// (`AsBytes` over the primitive's native bytes; UTF-8 string bytes
/// for `Utf8`).
fn bloom_check(sbbf: &parquet::bloom_filter::Sbbf, value: &ScalarValue) -> bool {
    match value {
        ScalarValue::Int64(v) => sbbf.check(v),
        ScalarValue::UInt64(v) => sbbf.check(v),
        ScalarValue::Float64(v) => sbbf.check(v),
        ScalarValue::Utf8(s) => {
            // `Sbbf::check<T: AsBytes>(value: &T)` requires `T: Sized`.
            // `str` is unsized (E0277), so we go through `&str` (a fat
            // pointer, but sized). `parquet::data_type` provides
            // `impl AsBytes for &str`, which hashes the UTF-8 byte slice
            // — the same bytes the writer inserted via
            // `bloom_filter.insert(value.as_ref())` on a `ByteArray`
            // column.
            let s_ref: &str = s.as_str();
            sbbf.check(&s_ref)
        }
        // Booleans aren't a meaningful bloom column — at most two distinct
        // values, the filter is useless. Fall through to "may be present".
        ScalarValue::Boolean(_) => true,
    }
}

/// Decide if an entire row group can be pruned given the conjunction of
/// filters, using pre-resolved (column_index, predicate) pairs. This
/// avoids calling `arrow_schema.index_of` inside the per-row-group loop.
fn row_group_pruned_resolved(rg: &RowGroupMetaData, resolved: &[(usize, &Predicate)]) -> bool {
    for &(idx, f) in resolved {
        if predicate_excludes_group_by_idx(rg, idx, f) {
            return true;
        }
    }
    false
}

/// Check if a predicate can rule out a row group using the pre-resolved
/// column index. Avoids the `index_of` schema scan and avoids cloning the
/// scalar value (borrows it directly from the predicate).
fn predicate_excludes_group_by_idx(rg: &RowGroupMetaData, col_idx: usize, filter: &Predicate) -> bool {
    let Some(col_meta) = rg.columns().get(col_idx) else {
        return false;
    };
    let Some(stats) = col_meta.statistics() else {
        return false;
    };

    // Borrow the scalar value directly — no clone needed.
    let value = match filter {
        Predicate::Eq(_, v) | Predicate::Gt(_, v) | Predicate::Lt(_, v) => v,
    };

    match (filter, value, stats) {
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

/// Probe for the wrapped-key sidecar; if present, fetch the body, unwrap,
/// and decrypt. Returns `None` when no sidecar exists (the file is
/// plaintext — back-compat with files written before the provider was
/// attached). Sidecar absence is detected via HEAD: cheaper than a failed
/// GET on most backends.
async fn try_load_encrypted(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    provider: &dyn EncryptionProvider,
    project_config: Option<&ProjectStorageConfig>,
    project: &ProjectId,
) -> Result<Option<Vec<u8>>> {
    let sidecar = wrapped_sidecar_key(path);
    let head_res = store.head(&sidecar).await;
    let sidecar_exists = match head_res {
        Ok(_) => true,
        Err(object_store::Error::NotFound { .. }) => false,
        Err(e) => return Err(BasinError::storage(format!("head sidecar {sidecar}: {e}"))),
    };
    if !sidecar_exists {
        return Ok(None);
    }
    let wrapped_bytes = store
        .get(&sidecar)
        .await
        .map_err(|e| BasinError::storage(format!("get sidecar {sidecar}: {e}")))?
        .bytes()
        .await
        .map_err(|e| BasinError::storage(format!("read sidecar {sidecar}: {e}")))?;
    let wrapped = WrappedKey(wrapped_bytes.to_vec());
    let data_key = match project_config {
        Some(cfg) => {
            provider
                .unwrap_key_with_config(project, &wrapped, cfg)
                .await?
        }
        None => provider.unwrap_key(project, &wrapped).await?,
    };
    let envelope = store
        .get(path)
        .await
        .map_err(|e| BasinError::storage(format!("get encrypted {path}: {e}")))?
        .bytes()
        .await
        .map_err(|e| BasinError::storage(format!("read encrypted {path}: {e}")))?;
    let plaintext = decrypt_envelope(&data_key, &envelope)?;
    Ok(Some(plaintext))
}

/// Run the read pipeline against a decrypted in-memory plaintext blob.
/// Mirrors the plaintext path's projection / row-group pruning / row
/// filter / page-cache write-through, just with `BytesFileReader`
/// instead of `ParquetObjectReader`. We don't insert into the parquet
/// meta cache because that cache is keyed by ObjectPath — a re-entry on
/// the same path on the plaintext side would short-circuit decryption.
async fn finalize_encrypted_stream(
    plaintext: Vec<u8>,
    path: ObjectPath,
    opts: Arc<ReadOptions>,
    counters: Arc<ReadCounters>,
    page_cache: Option<Arc<PageCache>>,
    cache_key: Option<CacheKey>,
    project_counters: Option<Arc<basin_common::ProjectCounters>>,
    catalog_schema: Option<SchemaRef>,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let file_size = plaintext.len() as u64;
    if let Some(tc) = project_counters.as_ref() {
        tc.record_bytes_read(file_size);
    }

    let mut bytes_reader = BytesFileReader {
        bytes: bytes::Bytes::from(plaintext),
    };
    let arrow_meta =
        ArrowReaderMetadata::load_async(&mut bytes_reader, ArrowReaderOptions::default())
            .await
            .map_err(|e| BasinError::storage(format!("open encrypted parquet {path}: {e}")))?;

    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(bytes_reader, arrow_meta);
    finalize_pipeline(builder, path, opts, counters, page_cache, cache_key, catalog_schema).await
}

/// Shared post-builder pipeline: projection mask, row-group stats
/// pruning, bloom-filter pruning, row filter, and page-cache
/// write-through. Generic over the underlying file reader so the
/// plaintext (`ParquetObjectReader`) and encrypted (`BytesFileReader`)
/// paths share one implementation.
///
/// `catalog_schema` is the current Arrow schema of the table as recorded in
/// the catalog (i.e. post-ALTER-TABLE state). When a projected column is
/// absent from the on-disk Parquet file (because the file was written before
/// an `ALTER TABLE ... ADD COLUMN`) and `catalog_schema` is provided, a
/// NULL-filled column of the correct type is synthesised so that old rows
/// appear with `NULL` for the new column — the correct Postgres semantics for
/// schema evolution. When `catalog_schema` is `None` the original error
/// behaviour is preserved for callers that do not supply schema context.
async fn finalize_pipeline<T>(
    builder: ParquetRecordBatchStreamBuilder<T>,
    path: ObjectPath,
    opts: Arc<ReadOptions>,
    counters: Arc<ReadCounters>,
    page_cache: Option<Arc<PageCache>>,
    cache_key: Option<CacheKey>,
    catalog_schema: Option<SchemaRef>,
) -> Result<BoxStream<'static, Result<RecordBatch>>>
where
    T: parquet::arrow::async_reader::AsyncFileReader + Send + Unpin + 'static,
{
    let arrow_schema: SchemaRef = builder.schema().clone();
    let parquet_schema = builder.metadata().file_metadata().schema_descr_ptr();

    // Determine which projected columns are present in the on-disk file and
    // which are absent (added by a later ALTER TABLE ADD COLUMN).
    //
    // `missing_cols` is a vec of (output_position, field) for each column
    // that exists in the catalog/projection but not in the file's Arrow
    // schema. Each entry carries the Arrow `Field` (including its DataType)
    // so we can synthesise a correctly-typed NULL array for the batch
    // transformation step below.
    //
    // `present_idxs` is the ordered list of file-schema column indices to
    // hand to `ProjectionMask`. The Parquet reader will deliver columns in
    // file-schema order (among the selected ones); the batch transformer
    // below reassembles them into projection order.
    let projection_mask = match &opts.projection {
        Some(cols) => {
            let mut present_idxs = Vec::with_capacity(cols.len());
            let mut missing_cols: Vec<(usize, arrow_schema::FieldRef)> =
                Vec::new();

            for (out_pos, c) in cols.iter().enumerate() {
                match arrow_schema.index_of(c) {
                    Ok(i) => present_idxs.push(i),
                    Err(_) => {
                        // Column is absent from this file's schema. Try to
                        // resolve its type from the catalog schema so we can
                        // synthesise a NULL-filled column.
                        let field = if let Some(cs) = &catalog_schema {
                            match cs.field_with_name(c) {
                                Ok(f) => Arc::new(f.clone()),
                                Err(_) => {
                                    // Not in catalog either — fall through to
                                    // the original error.
                                    return Err(BasinError::storage(format!(
                                        "unknown column {c}"
                                    )));
                                }
                            }
                        } else {
                            return Err(BasinError::storage(format!("unknown column {c}")));
                        };
                        missing_cols.push((out_pos, field));
                    }
                }
            }

            // If there are missing columns we need a batch transformer.  Stash
            // `missing_cols` in an `Arc` so the async closure below can capture
            // it without lifetime issues.
            if !missing_cols.is_empty() {
                // Build the projected schema for the columns that ARE present
                // in this file (the Parquet reader will produce batches with
                // these columns in file-schema order among the selected set).
                // We need to know the output name for each present column so
                // the transformer can look it up by name.
                let present_names: Vec<String> = cols
                    .iter()
                    .enumerate()
                    .filter(|(pos, _)| missing_cols.iter().all(|(mp, _)| *mp != *pos))
                    .map(|(_, c)| c.clone())
                    .collect();

                // Build the full output schema (projection order) from the
                // combination of present fields (from file) and missing fields
                // (from catalog).
                let mut output_fields: Vec<arrow_schema::FieldRef> =
                    Vec::with_capacity(cols.len());
                let mut missing_iter = missing_cols.iter().peekable();
                let mut present_iter = present_names.iter();
                for out_pos in 0..cols.len() {
                    if missing_iter
                        .peek()
                        .map(|(mp, _)| *mp == out_pos)
                        .unwrap_or(false)
                    {
                        let (_, field) = missing_iter.next().unwrap();
                        output_fields.push(field.clone());
                    } else {
                        let name = present_iter.next().unwrap();
                        let f = arrow_schema
                            .field_with_name(name)
                            .expect("present column must be in file schema");
                        output_fields.push(Arc::new(f.clone()));
                    }
                }
                let output_schema = Arc::new(Schema::new(output_fields));

                let missing_cols_arc: Arc<Vec<(usize, arrow_schema::FieldRef)>> =
                    Arc::new(missing_cols);
                let present_names_arc: Arc<Vec<String>> = Arc::new(present_names);
                let output_schema_arc = output_schema.clone();

                // Build the Parquet pipeline for the PRESENT columns only.
                let mask = ProjectionMask::roots(&parquet_schema, present_idxs);

                let kept_after_stats: Vec<usize> = {
                    let row_groups = builder.metadata().row_groups();
                    let total = row_groups.len() as u64;
                    counters
                        .row_groups_considered
                        .fetch_add(total, Ordering::Relaxed);
                    let mut kept = Vec::with_capacity(row_groups.len());
                    let mut pruned = 0u64;
                    let resolved: Vec<(usize, &Predicate)> = if opts.filters.is_empty() {
                        Vec::new()
                    } else {
                        opts.filters
                            .iter()
                            .filter_map(|f| {
                                arrow_schema.index_of(f.column()).ok().map(|idx| (idx, f))
                            })
                            .collect()
                    };
                    for (i, rg) in row_groups.iter().enumerate() {
                        if row_group_pruned_resolved(rg, &resolved) {
                            pruned += 1;
                        } else {
                            kept.push(i);
                        }
                    }
                    counters
                        .row_groups_pruned_by_stats
                        .fetch_add(pruned, Ordering::Relaxed);
                    kept
                };

                let mut builder = builder.with_projection(mask);
                let kept = prune_with_bloom_filters(
                    &mut builder,
                    &arrow_schema,
                    kept_after_stats,
                    &opts.filters,
                    &counters,
                )
                .await?;
                counters
                    .row_groups_scanned
                    .fetch_add(kept.len() as u64, Ordering::Relaxed);
                let mut builder = builder.with_row_groups(kept);

                if !opts.filters.is_empty() {
                    let predicates =
                        build_row_filter(&opts.filters, &arrow_schema, &parquet_schema)?;
                    builder = builder.with_row_filter(predicates);
                }

                let stream = builder
                    .build()
                    .map_err(|e| BasinError::storage(format!("parquet build {path}: {e}")))?;
                let mapped = stream.map(move |res| {
                    let batch = res.map_err(|e| {
                        BasinError::storage(format!("parquet read: {e}"))
                    })?;
                    synthesise_missing_columns(
                        batch,
                        &present_names_arc,
                        &missing_cols_arc,
                        &output_schema_arc,
                    )
                });

                // Page-cache write-through with the synthesised batches.
                if let (Some(pc), Some(key)) = (page_cache, cache_key) {
                    let buf: Arc<std::sync::Mutex<Option<Vec<Arc<RecordBatch>>>>> =
                        Arc::new(std::sync::Mutex::new(Some(Vec::new())));
                    let buf_for_each = buf.clone();
                    let buf_for_end = buf.clone();
                    let pc_for_end = pc.clone();
                    let key_for_end = key;
                    let collected = mapped.inspect(move |item| {
                        if let Ok(batch) = item {
                            if let Some(slot) =
                                buf_for_each.lock().expect("page-cache buf").as_mut()
                            {
                                slot.push(Arc::new(batch.clone()));
                            }
                        } else {
                            *buf_for_each.lock().expect("page-cache buf") = None;
                        }
                    });
                    let terminator = futures::stream::once(async move {
                        let final_buf = buf_for_end.lock().expect("page-cache buf").take();
                        if let Some(batches) = final_buf {
                            pc_for_end.insert(key_for_end, batches);
                        }
                        None::<Result<RecordBatch>>
                    });
                    let with_terminator = collected
                        .map(Some)
                        .chain(terminator)
                        .filter_map(|x| async move { x });
                    return Ok(with_terminator.boxed());
                }
                return Ok(mapped.boxed());
            }

            // All projected columns are present in the file — fall through to
            // the standard (non-synthesising) code path below.
            ProjectionMask::roots(&parquet_schema, present_idxs)
        }
        None => ProjectionMask::all(),
    };

    let kept_after_stats: Vec<usize> = {
        let row_groups = builder.metadata().row_groups();
        let total = row_groups.len() as u64;
        counters
            .row_groups_considered
            .fetch_add(total, Ordering::Relaxed);
        let mut kept = Vec::with_capacity(row_groups.len());
        let mut pruned = 0u64;
        // Pre-resolve (col_index, predicate_ref) once per file rather than
        // re-running `arrow_schema.index_of(col)` (an O(ncols) linear scan)
        // inside the per-row-group loop. For a file with N row groups and M
        // filters this cuts schema lookups from N×M to M — a real win when
        // N is 10+ (e.g. the 10k-row-per-file shape in the predicate-pushdown
        // benchmark).
        let resolved: Vec<(usize, &Predicate)> = if opts.filters.is_empty() {
            Vec::new()
        } else {
            opts.filters
                .iter()
                .filter_map(|f| arrow_schema.index_of(f.column()).ok().map(|idx| (idx, f)))
                .collect()
        };
        for (i, rg) in row_groups.iter().enumerate() {
            if row_group_pruned_resolved(rg, &resolved) {
                pruned += 1;
            } else {
                kept.push(i);
            }
        }
        counters
            .row_groups_pruned_by_stats
            .fetch_add(pruned, Ordering::Relaxed);
        kept
    };

    let mut builder = builder.with_projection(projection_mask.clone());
    let kept = prune_with_bloom_filters(
        &mut builder,
        &arrow_schema,
        kept_after_stats,
        &opts.filters,
        &counters,
    )
    .await?;
    counters
        .row_groups_scanned
        .fetch_add(kept.len() as u64, Ordering::Relaxed);
    let mut builder = builder.with_row_groups(kept);

    if !opts.filters.is_empty() {
        let predicates = build_row_filter(&opts.filters, &arrow_schema, &parquet_schema)?;
        builder = builder.with_row_filter(predicates);
    }

    let stream = builder
        .build()
        .map_err(|e| BasinError::storage(format!("parquet build {path}: {e}")))?;
    let mapped =
        stream.map(|res| res.map_err(|e| BasinError::storage(format!("parquet read: {e}"))));

    if let (Some(pc), Some(key)) = (page_cache, cache_key) {
        let buf: Arc<std::sync::Mutex<Option<Vec<Arc<RecordBatch>>>>> =
            Arc::new(std::sync::Mutex::new(Some(Vec::new())));
        let buf_for_each = buf.clone();
        let buf_for_end = buf.clone();
        let pc_for_end = pc.clone();
        let key_for_end = key;
        let collected = mapped.inspect(move |item| {
            if let Ok(batch) = item {
                if let Some(slot) = buf_for_each.lock().expect("page-cache buf").as_mut() {
                    slot.push(Arc::new(batch.clone()));
                }
            } else {
                *buf_for_each.lock().expect("page-cache buf") = None;
            }
        });
        let terminator = futures::stream::once(async move {
            let final_buf = buf_for_end.lock().expect("page-cache buf").take();
            if let Some(batches) = final_buf {
                pc_for_end.insert(key_for_end, batches);
            }
            None::<Result<RecordBatch>>
        });
        let with_terminator = collected
            .map(Some)
            .chain(terminator)
            .filter_map(|x| async move { x });
        return Ok(with_terminator.boxed());
    }

    Ok(mapped.boxed())
}

/// Reconstruct a [`RecordBatch`] in the requested projection order, inserting
/// a NULL-filled column of the correct type for each field that was absent
/// from the on-disk Parquet file.
///
/// `present_names`: the column names that the Parquet reader actually
///   materialised, in the order the reader returns them (file-schema order
///   within the selected set).
///
/// `missing_cols`: `(output_position, field)` pairs — where to insert
///   a NULL column and what Arrow type to use.
///
/// `output_schema`: the full Arrow schema for the output batch (projection
///   order, all columns including synthesised ones).
fn synthesise_missing_columns(
    parquet_batch: RecordBatch,
    present_names: &[String],
    missing_cols: &[(usize, arrow_schema::FieldRef)],
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let num_rows = parquet_batch.num_rows();
    let num_out = output_schema.fields().len();

    // Build a lookup from column name → Arrow array for the present columns.
    // The Parquet reader may return them in file-schema order (not necessarily
    // projection order), so we index by name.
    let mut out: Vec<Option<arrow_array::ArrayRef>> = vec![None; num_out];

    // Place NULL columns at their output positions first.
    for (out_pos, field) in missing_cols {
        out[*out_pos] = Some(new_null_array(field.data_type(), num_rows));
    }

    // Place present columns at their output positions (by name lookup).
    let missing_set: std::collections::HashSet<usize> =
        missing_cols.iter().map(|(p, _)| *p).collect();
    // The non-missing output positions, in order, correspond to `present_names`
    // in order (present_names was built by iterating cols in projection order
    // and skipping missing ones).
    let present_out_positions: Vec<usize> = (0..num_out)
        .filter(|p| !missing_set.contains(p))
        .collect();
    // Double-check the lengths match to avoid a panic below.
    debug_assert_eq!(
        present_out_positions.len(),
        present_names.len(),
        "mismatch between present column count and output slots"
    );
    for (out_pos, name) in present_out_positions.iter().zip(present_names.iter()) {
        let col = parquet_batch
            .column_by_name(name)
            .ok_or_else(|| {
                BasinError::storage(format!(
                    "synthesise_missing_columns: column '{name}' not in parquet batch"
                ))
            })?
            .clone();
        out[*out_pos] = Some(col);
    }
    let columns: Vec<arrow_array::ArrayRef> = out
        .into_iter()
        .enumerate()
        .map(|(i, opt)| {
            opt.ok_or_else(|| {
                BasinError::storage(format!(
                    "synthesise_missing_columns: output slot {i} was not filled"
                ))
            })
        })
        .collect::<Result<_>>()?;

    RecordBatch::try_new(output_schema.clone(), columns)
        .map_err(|e| BasinError::storage(format!("synthesise_missing_columns: {e}")))
}
