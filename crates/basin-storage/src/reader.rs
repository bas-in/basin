//! Parquet reader with projection + predicate pushdown.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
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
use crate::page_cache::{hash_filters, hash_projection, CacheKey, PageCache};
use crate::paths::table_tier_prefix;
use crate::predicate::{self, Predicate, ScalarValue};
use crate::tier::Tier;
use crate::{ReadCounters, ReadOptions, Storage};

pub(crate) async fn list_data_files(
    storage: &Storage,
    tenant: &TenantId,
    table: &TableName,
) -> Result<Vec<DataFile>> {
    // Walk both tier prefixes. Phase 5.5 introduces `tables/<t>/cold/`
    // alongside the existing `tables/<t>/data/` so reads transparently see
    // files migrated by the tiering compactor. Each file's `tier` field is
    // derived from its path so callers don't have to know which prefix the
    // listing came from.
    let store = storage.tenant_store(tenant);
    let mut files = Vec::new();
    for tier in [Tier::Hot, Tier::Cold] {
        let prefix = table_tier_prefix(storage.root_prefix(), tenant, table, tier);
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
    let store = storage.tenant_store(tenant);
    let tenant_id_string = tenant.as_prefix();

    let mut paths: Vec<ObjectPath> = Vec::new();
    // Walk hot and cold tiers in turn. The reader is tier-agnostic — files
    // live wherever the (compactor-driven) tier policy put them; here we
    // just consume both prefixes.
    for tier in [Tier::Hot, Tier::Cold] {
        let prefix = table_tier_prefix(storage.root_prefix(), tenant, table, tier);
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

    read_paths_inner(storage, tenant, paths, opts).await
}

/// Like [`read`] but reads only the supplied `paths` instead of LIST'ing
/// the table prefix. Used when the caller has already pruned the file set
/// — typically via Phase 5.7 A4 catalog stats: `Catalog::load_table` →
/// `Snapshot::data_files` → `evaluate_compound_for_pruning` against each
/// `DataFileRef::column_stats` → drop the files that prove `NoMatch`,
/// then hand the survivors here. Skips one LIST RPC and (for files
/// pruned at the catalog layer) one footer fetch each.
///
/// Tenant-prefix enforcement happens here too: every supplied path must
/// contain the tenant prefix or the call returns
/// [`BasinError::IsolationViolation`].
pub(crate) async fn read_paths(
    storage: &Storage,
    tenant: &TenantId,
    paths: Vec<ObjectPath>,
    opts: ReadOptions,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let expected = format!("tenants/{}/", tenant.as_prefix());
    for p in &paths {
        if !p.as_ref().contains(&expected) {
            return Err(BasinError::isolation(format!(
                "read_paths: {p} does not contain {expected}"
            )));
        }
    }
    read_paths_inner(storage, tenant, paths, opts).await
}

async fn read_paths_inner(
    storage: &Storage,
    tenant: &TenantId,
    paths: Vec<ObjectPath>,
    opts: ReadOptions,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let store = storage.tenant_store(tenant);
    let opts = Arc::new(opts);
    let store_for_stream = store.clone();
    let cache = storage.parquet_meta_cache().clone();
    let counters = storage.read_counters().clone();
    let page_cache = storage.page_cache_handle().cloned();
    let tenant_counters = storage.tenant_counters(tenant);
    let stream = futures::stream::iter(paths)
        .map(move |p| {
            let store = store_for_stream.clone();
            let opts = opts.clone();
            let cache = cache.clone();
            let counters = counters.clone();
            let page_cache = page_cache.clone();
            let tenant_counters = tenant_counters.clone();
            async move {
                read_one(store, p, opts, cache, counters, page_cache, tenant_counters).await
            }
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
    let counters = storage.read_counters().clone();
    let page_cache = storage.page_cache_handle().cloned();
    let tenant_counters = storage.tenant_counters(tenant);
    read_one(
        store,
        path.clone(),
        opts,
        cache,
        counters,
        page_cache,
        tenant_counters,
    )
    .await
}

async fn read_one(
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    opts: Arc<ReadOptions>,
    meta_cache: Arc<ParquetMetaCache>,
    counters: Arc<ReadCounters>,
    page_cache: Option<Arc<PageCache>>,
    tenant_counters: Option<Arc<basin_common::TenantCounters>>,
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
            // Hit: yield the cached batches as a stream. Each entry
            // is an `Arc<RecordBatch>`; we clone the Arc and unwrap
            // it into an owned `RecordBatch` because the public stream
            // contract is `Result<RecordBatch>`. The clone is cheap
            // (Arrow buffers are themselves Arc'd).
            let owned: Vec<RecordBatch> =
                batches.iter().map(|b| (**b).clone()).collect();
            let s = futures::stream::iter(owned.into_iter().map(Ok));
            return Ok(s.boxed());
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
        let synthetic = object_store::ObjectMeta {
            location: path.clone(),
            last_modified: chrono::Utc::now(),
            size: size as usize,
            e_tag: None,
            version: None,
        };
        let reader = ParquetObjectReader::new(store, synthetic);
        let arrow_meta = ArrowReaderMetadata::try_new(cached.meta, ArrowReaderOptions::default())
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
        (
            ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_meta),
            size,
        )
    };

    // Per-tenant bytes_read counter (Phase 6 telemetry). Bumped by the file
    // size as an upper bound — the parquet reader prunes row groups so actual
    // bytes pulled are typically a subset; this is a defensible scaling
    // signal per tenant without per-range bookkeeping in the object_store.
    if let Some(tc) = tenant_counters.as_ref() {
        tc.record_bytes_read(file_size);
    }

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

    // Row-group pruning by stats. Tracks per-group `considered` and
    // counts the stats-driven prunes; bloom-filter pruning is layered on
    // afterwards because it needs an extra (async) byte-range read of the
    // bloom-filter section in the Parquet footer.
    let kept_after_stats: Vec<usize> = {
        let row_groups = builder.metadata().row_groups();
        let total = row_groups.len() as u64;
        counters
            .row_groups_considered
            .fetch_add(total, Ordering::Relaxed);
        let mut kept = Vec::with_capacity(row_groups.len());
        let mut pruned = 0u64;
        for (i, rg) in row_groups.iter().enumerate() {
            if row_group_pruned(rg, &arrow_schema, &opts.filters) {
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

    // Bloom-filter pruning. For every `Eq` predicate on a column whose
    // bloom filter is present in the Parquet footer, ask the filter
    // whether the value is `definitely-not-present` and drop the row
    // group if so. We probe filters per-column lazily and short-circuit
    // on the first proof of absence — so a single `Eq` filter against a
    // bloomed column requires at most one byte-range fetch per row
    // group, only for groups that survived stats pruning.
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

    // Page-cache write-through. We tee every successful batch into a
    // `Mutex<Option<Vec<...>>>` and, when the underlying stream ends
    // *successfully*, swap that buffer into the cache. On any error
    // we drop the buffer so we don't cache partial reads.
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
                // Error path: drop the buffer so we don't cache a
                // partial result. Subsequent items are no-ops.
                *buf_for_each.lock().expect("page-cache buf") = None;
            }
        });
        // `chain` a single-element terminator stream that, when
        // polled, performs the final insert. It yields nothing
        // visible to the caller (filtered out by `flat_map` below).
        let terminator = futures::stream::once(async move {
            let final_buf = buf_for_end
                .lock()
                .expect("page-cache buf")
                .take();
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

