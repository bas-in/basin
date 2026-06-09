//! Parquet reader with projection + predicate pushdown.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use arrow_array::{new_null_array, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};
use basin_common::{BasinError, ProjectId, Result, TableName};
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, RowFilter, RowSelection,
    RowSelector,
};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::{PageIndexPolicy, RowGroupMetaData};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::statistics::Statistics;

use crate::data_file::{ColumnStats, DataFile};
use crate::encryption::{decrypt_envelope, BytesFileReader, EncryptionProvider, WrappedKey};
use crate::metadata_cache::{CachedParquetMeta, ParquetMetaCache};
use crate::page_cache::{hash_filters, hash_projection, CacheKey, PageCache};
use crate::paths::table_tier_prefix;
use crate::predicate::{
    self, evaluate_compound_for_pruning, CompoundPredicate, Predicate, PruneOutcome, ScalarValue,
};
use crate::tier::Tier;
use crate::vortex_footer_cache::VortexFooterCache;
use crate::writer::wrapped_sidecar_key;
use crate::{ReadCounters, ReadOptions, Storage};
use basin_catalog::ProjectStorageConfig;

/// Default upper bound on the number of files
/// [`list_data_files_with_stats`] will accept before returning
/// [`BasinError::QueryCostExceeded`]. Override at process start with
/// `BASIN_STORAGE_MAX_LISTED_FILES` (positive `usize`); any unset /
/// unparseable / zero value keeps this default.
///
/// This is a SAFETY NET, not a performance target. The real fix at
/// >100k files/table is on the caller: prefix-filter, paginate via
/// [`list_data_files_stream`], or list per-partition. We hit the cap and
/// raise a typed error rather than OOM the worker walking the full list.
pub(crate) const DEFAULT_MAX_LISTED_FILES: usize = 50_000;

/// Conservative compressed-→-decoded expansion factor used to estimate
/// whether a `.vortex` file's full unfiltered decode would fit one page-cache
/// shard's byte budget before we attempt the shared unfiltered-decode reuse
/// path (see the fresh-key point-read fast path in [`read_one`]). We multiply
/// the on-disk (compressed) blob size by this factor and compare against
/// `PageCache::per_shard_budget()`. Deliberately generous: over-estimating
/// only declines the optimisation for a borderline file (it falls back to the
/// existing pushdown decode), whereas under-estimating could let an oversized
/// decode thrash a shard. Vortex's lightweight encodings rarely exceed ~8× on
/// Basin-shaped data; the conservative bound keeps the gate honest.
const VORTEX_UNFILTERED_DECODE_EXPANSION: u64 = 8;

/// Resolve [`DEFAULT_MAX_LISTED_FILES`], honoring
/// `BASIN_STORAGE_MAX_LISTED_FILES` when present and parseable to a
/// positive `usize`.
pub(crate) fn resolve_max_listed_files() -> usize {
    if let Ok(v) = std::env::var("BASIN_STORAGE_MAX_LISTED_FILES") {
        if let Ok(n) = v.parse::<usize>() {
            if n > 0 {
                return n;
            }
        }
    }
    DEFAULT_MAX_LISTED_FILES
}

/// Streaming listing: yields each [`DataFile`] (path + size + tier; no
/// footer stats) as it comes off the object-store LIST. Walks both
/// `data/` and `cold/` prefixes in sequence — readers that LIMIT can
/// short-circuit and avoid materialising the full set in RAM, which is
/// the scalability fix at >100k files/table. The existing `Vec`-returning
/// [`list_data_files`] is a thin wrapper that drains this stream.
///
/// Filter rules match [`list_data_files`]: `.parquet` and `.vortex`
/// suffixes only — sidecar `.wrapped` files and other auxiliary objects
/// are skipped silently.
pub(crate) fn list_data_files_stream<'a>(
    storage: &'a Storage,
    project: &'a ProjectId,
    table: &'a TableName,
) -> BoxStream<'a, Result<DataFile>> {
    let store = storage.project_store(project);
    let root_prefix = storage.root_prefix().cloned();
    let project = project.clone();
    let table = table.clone();

    // We can't borrow `storage` across the async stream because callers
    // expect a `'static`-ish stream; clone the cheap handles up front and
    // build the per-tier listing inside `try_unfold`. We use
    // `stream::iter` over the two tiers and `flat_map` so the entire
    // listing is consumed lazily.
    let store_for_stream = store.clone();
    stream::iter([Tier::Hot, Tier::Cold])
        .flat_map(move |tier| {
            let prefix = table_tier_prefix(root_prefix.as_ref(), &project, &table, tier);
            let store = store_for_stream.clone();
            let listing = store.list(Some(&prefix));
            listing.filter_map(|res| async move {
                match res {
                    Err(e) => Some(Err(BasinError::storage(format!("list: {e}")))),
                    Ok(meta) => {
                        // Filter to recognised data-file extensions; skip
                        // sidecars and other auxiliary objects.
                        if !(meta.location.as_ref().ends_with(".parquet")
                            || meta.location.as_ref().ends_with(".vortex"))
                        {
                            return None;
                        }
                        let resolved_tier = Tier::from_path(meta.location.as_ref());
                        Some(Ok(DataFile {
                            path: meta.location,
                            size_bytes: meta.size as u64,
                            row_count: 0,
                            column_stats: BTreeMap::new(),
                            bloom_filters: BTreeMap::new(),
                            hll_sketches: BTreeMap::new(),
                            tdigest_sketches: BTreeMap::new(),
                            tier: resolved_tier,
                        }))
                    }
                }
            })
        })
        .boxed()
}

pub(crate) async fn list_data_files(
    storage: &Storage,
    project: &ProjectId,
    table: &TableName,
) -> Result<Vec<DataFile>> {
    // Backward-compat thin wrapper: drains the streaming variant into a
    // `Vec`. Callers that need to short-circuit on LIMIT should use
    // [`list_data_files_stream`] directly so they don't pay for the cold
    // tail of the listing.
    let mut stream = list_data_files_stream(storage, project, table);
    let mut files = Vec::new();
    while let Some(item) = stream.next().await {
        files.push(item?);
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
    // Safety net: bail out before per-file footer fan-out OOMs the worker
    // on a table that has somehow accumulated >MAX listed files. This is
    // NOT a substitute for the caller pruning the file set up front
    // (catalog-stats prune, prefix filtering, per-partition listing); it
    // exists so a runaway tenant can't burst the per-tenant memory
    // budget. Surfaces as SQLSTATE 54000 (`program_limit_exceeded`).
    let max_listed = resolve_max_listed_files();
    if files.len() > max_listed {
        return Err(BasinError::QueryCostExceeded(format!(
            "list_data_files_with_stats: table {table} has {n} files; \
             configured cap is {max_listed} (BASIN_STORAGE_MAX_LISTED_FILES). \
             Use catalog-stats pruning, prefix filtering, or per-partition listing.",
            n = files.len(),
        )));
    }
    let store = storage.project_store(project);
    let cache = storage.parquet_meta_cache().clone();

    // Fan out per-file footer reads with bounded concurrency. Each cache
    // hit is a no-op; cache misses do one short range GET each.
    //
    // We use `size_bytes` from the listing result (populated by
    // `list_data_files`) instead of issuing a HEAD per file: object-store
    // LIST responses already include the object size, so the HEAD would be
    // a redundant round-trip that duplicates work the LIST already did.
    // Only `.parquet` files carry a Parquet footer to fetch. `.vortex`
    // files keep the listing defaults (row_count 0, empty column_stats):
    // Vortex stats-pruning is intentionally a no-op (the writer records no
    // per-file Parquet-shaped stats for Vortex), and every caller
    // (constraint / FK / UNIQUE / UPDATE / DELETE / TRUNCATE) reads the
    // file contents to enforce — they need the path, not the stats — so
    // empty stats only disables an optimisation, never correctness.
    let work: Vec<_> = files
        .iter()
        .enumerate()
        .filter(|(_, f)| f.path.as_ref().ends_with(".parquet"))
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

    // Vortex files carry no Parquet footer, but DELETE/UPDATE row math and
    // the constraint/FK/UNIQUE scans rely on `row_count` (e.g. exec_delete
    // does `deleted += f.row_count` and `removed = f.row_count - kept`), and
    // the catalog file-pruning path needs `column_stats`. Both come from
    // the Vortex footer (no data decode) off a single GET of the blob.
    // `column_stats` is type-gated (i64/f64/null-count only) in the same
    // byte contract as Parquet so `evaluate_compound_for_pruning` treats
    // them identically; the Vortex⇆Parquet differential harness is the
    // correctness gate. The GET is best-effort: a failure (e.g. an
    // envelope-encrypted blob, which this listing path does not decrypt —
    // the same limitation the Parquet footer read here has) leaves the
    // listing defaults (row_count 0, empty stats), no worse than before.
    let vortex_idxs: Vec<usize> = files
        .iter()
        .enumerate()
        .filter(|(_, f)| f.path.as_ref().ends_with(".vortex"))
        .map(|(i, _)| i)
        .collect();
    if !vortex_idxs.is_empty() {
        type VortexStat = (usize, Option<u64>, BTreeMap<String, ColumnStats>);
        let vwork: Vec<_> = vortex_idxs
            .into_iter()
            .map(|i| {
                let store = store.clone();
                let path = files[i].path.clone();
                async move {
                    let bytes = match store.get(&path).await {
                        Ok(obj) => obj.bytes().await.ok(),
                        Err(_) => None,
                    };
                    // One footer open returns BOTH row_count and stats.
                    let (rc, stats) = match bytes {
                        Some(b) => match crate::vortex_format::footer_meta(b).await {
                            Ok((n, s)) => (Some(n), s),
                            Err(_) => (None, BTreeMap::new()),
                        },
                        None => (None, BTreeMap::new()),
                    };
                    (i, rc, stats)
                }
            })
            .collect();
        let vresolved: Vec<VortexStat> = futures::stream::iter(vwork)
            .buffer_unordered(8)
            .collect()
            .await;
        for (i, rc, stats) in vresolved {
            if let Some(rows) = rc {
                files[i].row_count = rows;
            }
            if !stats.is_empty() {
                files[i].column_stats = stats;
            }
        }
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
    // Fetch the catalog schema once for the whole read so that
    // `finalize_pipeline` can synthesise NULL-filled columns for fields that
    // exist in the catalog but are absent from pre-ALTER on-disk files.
    // This is the schema-evolution (ADD COLUMN) correctness fix: old files
    // don't have the new column, so we pad their batches with NULLs.
    // Returns `None` when no catalog is attached → existing behaviour is
    // preserved (will error on missing columns, same as before).
    let catalog_schema = storage.catalog_table_schema(project, table).await?;

    // File-level catalog-stats pruning. When the request carries any
    // predicate AND we know the table's Arrow schema (catalog attached), we
    // pull per-file min/max/null-count stats and skip any file whose stats
    // prove the predicate cannot match. This mirrors the engine-path prune
    // in `basin-engine::fast_select`, but lifts it into the storage layer
    // so the wide set of callers that drive `Storage::read` directly
    // (integration tests, `Storage::read` from sources without their own
    // catalog walk) get the same pruning win.
    //
    // For Vortex this is decisive: opening a Vortex file is far heavier
    // than a Parquet footer fetch, and the cost we save here is a full
    // file open per pruned file. For Parquet it saves the per-file footer
    // fetch (the row-group-level prune inside `read_one` still fires for
    // surviving files).
    //
    // Falls back to the legacy inline-LIST path when filters are empty or
    // the catalog schema is unavailable — both branches preserve the
    // pre-prune behaviour exactly.
    let prune_with_stats = !opts.filters.is_empty() && catalog_schema.is_some();
    let paths: Vec<ObjectPath> = if prune_with_stats {
        let schema = catalog_schema
            .as_ref()
            .expect("checked is_some above")
            .as_ref()
            .clone();
        let cp = filters_to_compound(&opts.filters);
        let files = list_data_files_with_stats(storage, project, table).await?;
        files
            .into_iter()
            .filter(|f| {
                // Files whose stats are empty (no footer parsed, e.g.
                // envelope-encrypted Parquet or unsupported Vortex types)
                // skip pruning — `evaluate_compound_for_pruning` will see
                // no per-column entry and return Mixed, which keeps the
                // file. This is the same conservative rule the engine
                // path follows.
                !matches!(
                    evaluate_compound_for_pruning(&cp, &f.column_stats, &schema, f.row_count),
                    PruneOutcome::NoMatch
                )
            })
            .map(|f| f.path)
            .collect()
    } else {
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
                // Data files are `.parquet` or (opt-in) `.vortex`. List both;
                // skip everything else (e.g. `.wrapped` encryption sidecars).
                // Filtering to `.parquet` only made every Vortex data file
                // invisible to the constraint / FK / UNIQUE / PK and
                // UPDATE/DELETE row-matching scans (list-then-read), so a dup
                // INSERT was accepted and post-INSERT UPDATEs matched zero rows.
                if !(meta.location.as_ref().ends_with(".parquet")
                    || meta.location.as_ref().ends_with(".vortex"))
                {
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
        paths
    };

    read_paths_inner(storage, project, paths, opts, catalog_schema).await
}

/// Build a single AND-of-atoms [`CompoundPredicate`] from the read-path's
/// flat filter list. Mirrors the shape that `basin-engine::fast_select`
/// builds for catalog-stats pruning so the pruning decision is identical
/// across both call paths.
fn filters_to_compound(filters: &[Predicate]) -> CompoundPredicate {
    let mut atoms: Vec<CompoundPredicate> = filters
        .iter()
        .cloned()
        .map(CompoundPredicate::Atom)
        .collect();
    if atoms.len() == 1 {
        atoms.pop().expect("len==1")
    } else {
        CompoundPredicate::And(atoms)
    }
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

/// Like [`read_paths`] but accepts an explicit `catalog_schema` so the
/// Vortex filter-pushdown optimisation can fire even on the paths-only
/// read path. Callers that already hold the table schema (e.g.
/// `fast_select`) should prefer this over `read_paths` to avoid the Arrow
/// post-filter pass when all predicates were type-safe-pushed.
pub(crate) async fn read_paths_with_schema(
    storage: &Storage,
    project: &ProjectId,
    paths: Vec<ObjectPath>,
    opts: ReadOptions,
    catalog_schema: Option<SchemaRef>,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let expected = format!("projects/{}/", project.as_prefix());
    for p in &paths {
        if !p.as_ref().contains(&expected) {
            return Err(BasinError::isolation(format!(
                "read_paths_with_schema: {p} does not contain {expected}"
            )));
        }
    }
    read_paths_inner(storage, project, paths, opts, catalog_schema).await
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
    let vortex_cache = storage.vortex_footer_cache_handle().clone();
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
    let limit = opts.limit;
    let stream = futures::stream::iter(paths)
        .map(move |p| {
            let store = store_for_stream.clone();
            let opts = opts.clone();
            let cache = cache.clone();
            let counters = counters.clone();
            let page_cache = page_cache.clone();
            let vortex_cache = vortex_cache.clone();
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
                    vortex_cache,
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

    Ok(apply_limit_to_stream(stream.boxed(), limit))
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
    read_file_with_options(storage, project, path, ReadOptions::default()).await
}

/// Same as [`read_file`] but accepts a [`ReadOptions`] so callers can push
/// down a column projection (the PK / UNIQUE constraint scans need only one
/// or a few columns and benefit enormously from skipping JSONB / TEXT
/// payload decode). All other options (filters, partition, limit,
/// row_group_selection) are honoured exactly like the table-wide path.
pub(crate) async fn read_file_with_options(
    storage: &Storage,
    project: &ProjectId,
    path: &ObjectPath,
    opts: ReadOptions,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let opts = Arc::new(opts);
    let store = storage.project_store(project);
    let cache = storage.parquet_meta_cache().clone();
    let counters = storage.read_counters().clone();
    let page_cache = storage.page_cache_handle().cloned();
    let vortex_cache = storage.vortex_footer_cache_handle().clone();
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
        vortex_cache,
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
    vortex_cache: Arc<VortexFooterCache>,
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
    // Page-cache key: file path + projection + filters. The parquet
    // reader pushes filters all the way down (with_row_filter +
    // with_row_selection), so the cached `Vec<RecordBatch>` is
    // post-filter; sharing entries across queries with different WHEREs
    // would return wrong rows. See `CacheKey` docs for the constraint.
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

    // Opt-in Vortex format branch. Object keys ending in `.vortex` were
    // written by the Vortex codec (`crate::vortex_format::encode`); keys
    // ending in `.parquet` fall through untouched so the Parquet pipeline
    // below is byte-identical to before this branch existed. Vortex is
    // strictly opt-in per table (`WITH (basin.file_format='vortex')`); the
    // default format is unchanged.
    //
    // Vortex decode is self-describing, symmetric with Parquet's footer:
    // when the table-aware path supplies the catalog schema we pass it
    // through (authoritative for projection / exact-type fidelity); when a
    // schema-less caller (`read_file` / `read_paths`: continuous-view
    // refresh, cron-job state, system tables) passes `None`, the decoder
    // recovers the Arrow schema from the Vortex file's own DType. A Vortex
    // file is therefore decodable from nothing but its bytes, exactly like
    // a Parquet file — so Vortex is safe on every read path, not just the
    // table-aware one.
    //
    // NOTE: `ReadOptions::row_group_selection` is a PARQUET-ONLY pruning
    // hint. Vortex has no concept of "row group" (its layout is a column-
    // chunk tree pruned natively via zone maps + the pushed-down filter
    // we hand `decode_with_cache`). The engine's index_probe builds
    // selection maps using parquet row-group ordinals from the
    // `TrigramRowGroupRegistry` / per-file GIN summary — those ordinals
    // are meaningless on a Vortex file. We therefore intentionally
    // ignore `row_group_selection` for Vortex files; the Vortex scan
    // still gets the projection + predicate pushdown above, which is
    // its native equivalent. Wiring index-probe → Vortex chunk prune is
    // separate, larger work (it needs a `ChunkProbeRegistry` keyed on
    // Vortex layout offsets, not parquet row-group ids).
    if path.as_ref().ends_with(".vortex") {
        // ADR 0024 — UUID-as-Decimal256 storage encoding. The catalog
        // schema reports UUID columns as `FixedSizeBinary(16)`, but on
        // disk (and via Vortex's executor) they are `Decimal256(39, 0)`.
        // We must hand Vortex the *physical* schema so
        // `execute_record_batch` doesn't try to cast Decimal256→FSB(16)
        // (Vortex 0.70 has no FSB encoder and the cast fails). The
        // post-decode inverse in `vortex_project_and_filter` will
        // re-stamp the BASIN_TYPE marker and reinterpret the buffer
        // back to FSB(16) so the engine never sees the disguise.
        // TODO(adr-0024): drop when vortex grows native FSB(N) support.
        let schema = catalog_schema.as_ref().map(|s| {
            // First UUID FSB(16) → Decimal256, then POINT FSB(21) →
            // LargeBinary. The two translations key on disjoint
            // physical types so order is incidental.
            let s = catalog_schema_uuid_to_decimal256(s.as_ref());
            let s = catalog_schema_point_to_large_binary(&s);
            Arc::new(s)
        });

        // ── Pre-GET unfiltered-decode cache short-circuit ────────────────────
        //
        // The unfiltered-decode reuse below caches a file's raw (read-projected,
        // no-filter) batches under a key of `(path, read_proj, EMPTY filters)`.
        // That key is computable WITHOUT the file bytes.  When the entry is
        // already warm we can answer the read by Arrow-filtering the cached
        // batches and never issue the object-store GET (which on localfs still
        // copies the whole compressed blob into RAM, and on cloud is a network
        // RTT) — the dominant per-query cost once the decode is cached.
        //
        // Gated identically to the post-GET `unfiltered_eligible` path EXCEPT
        // for the size check (the entry's mere presence proves it passed the
        // size gate at insert time) and the schema-inference fallback (we only
        // short-circuit when the authoritative catalog schema is present, so we
        // never need to infer a schema from bytes we haven't fetched).  When
        // any gate fails we fall straight through to the GET, byte-for-byte
        // unchanged.
        {
            let unfiltered_cache_disabled =
                std::env::var("BASIN_UNFILTERED_DECODE_CACHE_DISABLE").as_deref() == Ok("1");
            if !opts.filters.is_empty()
                && !unfiltered_cache_disabled
                && catalog_schema.is_some()
            {
                if let Some(pc) = page_cache.as_ref() {
                    let read_proj = vortex_read_projection(opts.as_ref());
                    let unfiltered_key = CacheKey {
                        path: path.clone(),
                        projection_hash: hash_projection(read_proj.as_deref()),
                        filters_hash: hash_filters(&[]),
                    };
                    if let Some(cached) = pc.get(&unfiltered_key) {
                        let raw: Vec<RecordBatch> =
                            cached.iter().map(|b| (**b).clone()).collect();
                        // Early per-call LIMIT cut inside the filter pass so the
                        // projection + restamp work touches at most `limit` rows
                        // (the caller re-applies the same cap; this is purely a
                        // work-saving early cut).
                        let batches = vortex_project_and_filter_limited(
                            raw,
                            opts.as_ref(),
                            catalog_schema.as_ref(),
                            true,
                            opts.limit,
                        )?;
                        let stream = futures::stream::iter(batches.into_iter().map(Ok));
                        return Ok(stream.boxed());
                    }
                }
            }
        }

        // Two byte-acquisition paths must feed the Vortex decoder:
        //   (a) envelope-encrypted: a `<path>.wrapped` sidecar exists, so we
        //       unwrap + AES-GCM decrypt to recover the plaintext blob;
        //   (b) plaintext: no sidecar (file written before a provider was
        //       attached, or no provider configured) — GET the raw bytes.
        // Either way we end up with the fully decrypted/plaintext Vortex
        // blob and route it through the same decoder.
        // Fetch bytes as `bytes::Bytes` to avoid a redundant Vec copy:
        // object_store returns `Bytes` natively; the encrypted path returns
        // `Vec<u8>` which converts zero-copy via `Bytes::from(vec)`.
        let bytes: bytes::Bytes = match encryption.as_ref() {
            Some(provider) => match try_load_encrypted(
                &store,
                &path,
                provider.as_ref(),
                project_config.as_ref(),
                &project,
            )
            .await?
            {
                Some(plaintext) => bytes::Bytes::from(plaintext),
                None => store
                    .get(&path)
                    .await
                    .map_err(|e| BasinError::storage(format!("get vortex {path}: {e}")))?
                    .bytes()
                    .await
                    .map_err(|e| BasinError::storage(format!("read vortex {path}: {e}")))?,
            },
            None => store
                .get(&path)
                .await
                .map_err(|e| BasinError::storage(format!("get vortex {path}: {e}")))?
                .bytes()
                .await
                .map_err(|e| BasinError::storage(format!("read vortex {path}: {e}")))?,
        };

        if let Some(tc) = project_counters.as_ref() {
            tc.record_bytes_read(bytes.len() as u64);
        }

        // Push projection + filter into the Vortex scan: read only the
        // needed columns and let Vortex's native zone maps prune chunks the
        // predicate excludes (the analogue of Parquet's ProjectionMask +
        // row-group stats pruning). When every predicate was type-safe and
        // the scan did not fall back to a full decode, skip the Arrow
        // post-filter pass entirely (projection + missing-column synthesis
        // still run). The smoke gate proves correctness: a wrong skip
        // would produce results that differ from Parquet.
        //
        // `bytes.len()` is captured before moving `bytes` into the decoder.
        let size_bytes = bytes.len() as u64;
        let read_proj = vortex_read_projection(opts.as_ref());

        // Filter pushdown drives Vortex's native zone-map chunk pruning. The
        // type-safety gate in `vortex_filter_expr` needs the column's exact
        // Arrow type, which normally comes from the catalog schema. But the
        // schema-less read paths (`read_file_with_options`: the UPDATE
        // pre-image / constraint-probe scans) pass `catalog_schema = None`,
        // so WITHOUT a schema nothing is pushed — every surviving chunk then
        // decodes in full and the predicate is applied only by the Arrow
        // post-filter, defeating the prune. A Vortex file is self-describing,
        // so we recover the physical Arrow schema from the file's own footer
        // (reusing the footer cache — parse-free on a warm file) purely to
        // type-check the pushdown. This is a pure optimisation: the inferred
        // schema is used ONLY to decide what is safe to push, and we keep the
        // Arrow post-filter pass ACTIVE whenever it drove the push (see
        // `apply_filter` below), so the final row set is byte-identical to
        // the no-pushdown path. Kill switch: `BASIN_VORTEX_SELECTIVE_DECODE_
        // DISABLE=1` reverts to the prior (no inferred-schema push) behaviour.
        let selective_decode_disabled =
            std::env::var("BASIN_VORTEX_SELECTIVE_DECODE_DISABLE").as_deref() == Ok("1");
        let inferred_schema: Option<Arc<Schema>> = if catalog_schema.is_none()
            && !opts.filters.is_empty()
            && !selective_decode_disabled
        {
            crate::vortex_format::infer_arrow_schema(
                &bytes,
                Some(&vortex_cache),
                Some(&path),
                size_bytes,
            )
            .ok()
            .map(Arc::new)
        } else {
            None
        };
        let filter_schema: Option<&Schema> = match catalog_schema.as_deref() {
            Some(s) => Some(s),
            None => inferred_schema.as_deref(),
        };
        // ── Unfiltered-decode reuse (fresh-key point-read fast path) ──
        //
        // A point read pushes a PK-equality filter into the scan, so the
        // post-filter `Vec<RecordBatch>` we'd cache under the normal
        // `cache_key` is keyed on `filters_hash(<that key value>)`. Every
        // DIFFERENT key value is therefore a cold miss: the file's
        // surviving chunks are re-fetched + re-decoded per key. At 1M rows
        // that's ~5–15 ms per file per fresh key.
        //
        // Instead, when filters are present and the file's decoded size is
        // expected to fit one shard's byte budget, we decode the file ONCE
        // *without* a filter and cache those raw (pre-project/-filter)
        // batches under a single shared key — `filters_hash(EMPTY)` plus
        // the SAME read-projection every point lookup of this file uses. The
        // first fresh key pays the decode; every later fresh key is then an
        // Arrow filter over in-memory batches (sub-ms) via the SAME
        // `vortex_project_and_filter` path that already enforces the
        // authoritative predicate + NULL semantics, so the result is
        // byte-identical to the pushdown path.
        //
        // Gates (all must hold, else fall straight through to the existing
        // pushdown decode below, byte-for-byte unchanged):
        //   (a) filters present                — point-lookup-ish; a full
        //       analytical scan has no filter and must not start decoding
        //       unfiltered;
        //   (b) decoded size fits one shard's budget — `size_bytes` (the
        //       compressed Vortex blob) × a conservative expansion factor
        //       must be ≤ `per_shard_budget()`; a big file bypasses so large
        //       scans never materialise the whole unfiltered decode;
        //   (c) the page cache exists (nowhere to share the decode otherwise);
        //   (d) the kill switch is unset.
        //
        // Eviction correctness rides on the existing invariants: the key
        // carries `path`, and `.vortex` files are immutable — compaction /
        // rewrite writes a NEW path and calls `invalidate_path` on the old
        // one, dropping every variant (including this unfiltered entry).
        let unfiltered_cache_disabled =
            std::env::var("BASIN_UNFILTERED_DECODE_CACHE_DISABLE").as_deref() == Ok("1");
        let unfiltered_eligible = !opts.filters.is_empty()
            && !unfiltered_cache_disabled
            && page_cache.is_some()
            && size_bytes.saturating_mul(VORTEX_UNFILTERED_DECODE_EXPANSION)
                <= page_cache
                    .as_ref()
                    .map(|pc| pc.per_shard_budget())
                    .unwrap_or(0);

        if unfiltered_eligible {
            // Shared key: same path + same READ projection (`read_proj`,
            // which already folds the filter columns in) + EMPTY filter set.
            // Disjoint from the normal post-filter key (which hashes
            // `opts.projection` and the real filters), so the two never
            // collide.
            let unfiltered_key = CacheKey {
                path: path.clone(),
                projection_hash: hash_projection(read_proj.as_deref()),
                filters_hash: hash_filters(&[]),
            };
            let pc = page_cache.as_ref().expect("eligibility implies page cache");

            // Raw unfiltered decode batches (read-projection columns, no
            // predicate applied). On hit we reuse the cached Arc; on miss we
            // decode once with NO filter, cache, then proceed.
            let raw: Vec<RecordBatch> = if let Some(cached) = pc.get(&unfiltered_key) {
                cached.iter().map(|b| (**b).clone()).collect()
            } else {
                let (decoded, _) = crate::vortex_format::decode_with_cache(
                    bytes,
                    schema,
                    read_proj.as_deref(),
                    None,
                    Some(&vortex_cache),
                    &path,
                    size_bytes,
                )
                .await?;
                if pc.has_capacity() {
                    let cached: Vec<Arc<RecordBatch>> =
                        decoded.iter().cloned().map(Arc::new).collect();
                    pc.insert(unfiltered_key, cached);
                }
                decoded
            };

            // Apply the authoritative predicate + projection in Arrow over
            // the (shared) unfiltered batches. `apply_filter = true` always:
            // the cached batches are unfiltered, so the WHERE must run here.
            // The early per-call LIMIT cut (mirrors the pushdown path below)
            // is folded INTO the filter pass so projection + restamp touch at
            // most `limit` rows; this raw-batch path never write-throughs the
            // post-filter result, so an early cut is sound.
            let batches = vortex_project_and_filter_limited(
                raw,
                opts.as_ref(),
                catalog_schema.as_ref(),
                true,
                opts.limit,
            )?;

            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            return Ok(stream.boxed());
        }

        let (push_filter, all_filters_pushed) =
            vortex_filter_expr(&opts.filters, filter_schema);
        let (batches, decode_used_filter) = crate::vortex_format::decode_with_cache(
            bytes,
            schema,
            read_proj.as_deref(),
            push_filter,
            Some(&vortex_cache),
            &path,
            size_bytes,
        )
        .await?;
        // Skip the Arrow re-filter only when Vortex handled ALL predicates
        // natively (all_filters_pushed) AND the scan actually ran with
        // pushdown (decode_used_filter, i.e. did not fall back) AND we had the
        // authoritative catalog schema. When the push was driven by the
        // *inferred* file schema (no catalog), keep the post-filter active for
        // defence in depth — it is cheap on the few surviving rows and removes
        // any doubt about inferred-type vs catalog-type semantics, leaving this
        // change a pure prune optimisation with an identical final row set.
        let apply_filter =
            !(all_filters_pushed && decode_used_filter && catalog_schema.is_some());
        let batches =
            vortex_project_and_filter(batches, opts.as_ref(), catalog_schema.as_ref(), apply_filter)?;

        // Page-cache write-through, keyed by the same (path, projection)
        // cache key the Parquet path uses, so a repeat of the identical
        // read is served from cache and stays consistent. Skip the
        // insert when the cache is at its byte budget — the entry would
        // be evicted on insert anyway, so the per-batch Arc::new is
        // pure overhead.
        // Cache the FULL (untruncated) result so a later query with a
        // larger or absent LIMIT can still be served from cache correctly.
        if let (Some(pc), Some(key)) = (page_cache.as_ref(), cache_key) {
            if pc.has_capacity() {
                let cached: Vec<Arc<RecordBatch>> = batches.iter().cloned().map(Arc::new).collect();
                pc.insert(key, cached);
            }
        }

        // Per-file LIMIT gate: when a row cap is set, trim the batch list
        // so this file contributes at most `lim` post-filter rows to the
        // output stream.  `apply_limit_to_stream` in `read_paths_inner`
        // still handles the cross-file cap, but by cutting early here we
        // let that combinator close the stream sooner — cancelling decode
        // futures for files that are still in-flight in the `buffered(4)`
        // pipeline.  The page-cache write-through above stores the full
        // batch list, so truncation only affects the stream handed back to
        // the caller, not the cache entry.
        let batches = if let Some(lim) = opts.limit {
            let mut kept = Vec::with_capacity(batches.len());
            let mut seen = 0usize;
            for b in batches {
                if seen >= lim {
                    break;
                }
                let take = (lim - seen).min(b.num_rows());
                seen += take;
                if take == b.num_rows() {
                    kept.push(b);
                } else {
                    kept.push(b.slice(0, take));
                }
            }
            kept
        } else {
            batches
        };

        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        return Ok(stream.boxed());
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
    //
    // Page index (column min/max per data page) is loaded so that
    // `finalize_pipeline` can build a `RowSelection` and skip data pages
    // within a kept row group whose stats prove no predicate can match.
    // This is the sub-row-group pruning tier that narrows the IN-list
    // bounding-range from "one row group (~65k rows)" to "the few pages
    // that actually hold the matching IDs".
    let page_index_opts =
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let (builder, file_size) = if let Some(cached) = meta_cache.get(&path) {
        let size = cached.size;
        let reader = ParquetObjectReader::new(store, path.clone()).with_file_size(size);
        let arrow_meta =
            ArrowReaderMetadata::try_new(cached.meta, page_index_opts)
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
            ArrowReaderMetadata::load_async(
                &mut reader,
                ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional),
            )
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

    finalize_pipeline(
        builder,
        path,
        opts,
        counters,
        page_cache,
        cache_key,
        catalog_schema,
    )
    .await
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

        // `StartsWith` (case-sensitive LIKE prefix) gets a direct fast path
        // that avoids the predicate::evaluate dispatch and explicitly handles
        // Utf8, LargeUtf8, and Utf8View — parquet-rs may decode strings as
        // any of the three depending on Arrow version and writer config.
        // Case-insensitive (ILIKE) is left to the generic path below.
        if let Predicate::StartsWith {
            prefix,
            case_insensitive: false,
            ..
        } = filter
        {
            let prefix_str = prefix.clone();
            let pred = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
                use arrow_array::cast::AsArray;
                use arrow_array::BooleanArray;
                let col = batch.column(0);
                Ok(match col.data_type() {
                    arrow_schema::DataType::Utf8 => {
                        let arr = col.as_string::<i32>();
                        BooleanArray::from_iter(
                            arr.iter()
                                .map(|v| v.map(|s| s.starts_with(prefix_str.as_str()))),
                        )
                    }
                    arrow_schema::DataType::LargeUtf8 => {
                        let arr = col.as_string::<i64>();
                        BooleanArray::from_iter(
                            arr.iter()
                                .map(|v| v.map(|s| s.starts_with(prefix_str.as_str()))),
                        )
                    }
                    arrow_schema::DataType::Utf8View => {
                        let arr = col.as_string_view();
                        BooleanArray::from_iter(
                            arr.iter()
                                .map(|v| v.map(|s| s.starts_with(prefix_str.as_str()))),
                        )
                    }
                    _ => {
                        // Non-string column: conservative all-false mask so
                        // no spurious rows pass through to the result set.
                        BooleanArray::from(vec![false; batch.num_rows()])
                    }
                })
            });
            predicates.push(Box::new(pred));
            continue;
        }

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
fn predicate_excludes_group_by_idx(
    rg: &RowGroupMetaData,
    col_idx: usize,
    filter: &Predicate,
) -> bool {
    let Some(col_meta) = rg.columns().get(col_idx) else {
        return false;
    };
    let Some(stats) = col_meta.statistics() else {
        return false;
    };

    // Borrow the scalar value directly — no clone needed.
    let value = match filter {
        Predicate::Eq(_, v) | Predicate::Gt(_, v) | Predicate::Lt(_, v) => v,
        // StartsWith uses a separate pruning rule (lex-bounded by prefix /
        // prefix_end). Apply it inline and short-circuit before the generic
        // (predicate, scalar, stats) match below.
        Predicate::StartsWith {
            prefix,
            case_insensitive,
            ..
        } => return prune_starts_with_row_group(stats, prefix, *case_insensitive),
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

/// Rule out a row group whose ByteArray min/max can prove no value starts
/// with `prefix`. Mirrors `predicate::prune_starts_with` but operates on
/// Parquet's `Statistics::ByteArray` (the engine-level pruning path uses
/// raw `min_bytes` / `max_bytes` from `ColumnStats`).
///
/// Returns `true` for "this row group can be skipped". `false` is always
/// safe (caller will read the file and let the per-row filter decide).
fn prune_starts_with_row_group(
    stats: &Statistics,
    prefix: &str,
    case_insensitive: bool,
) -> bool {
    // ASCII-fold pruning would need both min and max folded plus careful
    // handling of code points where lowercasing changes byte length.
    // Conservatively bail — the per-row filter still gets the win.
    if case_insensitive || prefix.is_empty() {
        return false;
    }
    let Statistics::ByteArray(s) = stats else {
        return false;
    };
    let pbytes = prefix.as_bytes();
    let min = s.min_opt().map(|b| b.data());
    let max = s.max_opt().map(|b| b.data());

    if let Some(max_b) = max {
        if max_b < pbytes {
            return true;
        }
    }
    let last = *pbytes.last().expect("non-empty prefix");
    if last != 0xFF {
        let mut pend = pbytes.to_vec();
        let n = pend.len();
        pend[n - 1] = last + 1;
        if let Some(min_b) = min {
            if min_b >= pend.as_slice() {
                return true;
            }
        }
    }
    false
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
        ArrowReaderMetadata::load_async(
            &mut bytes_reader,
            ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional),
        )
        .await
        .map_err(|e| BasinError::storage(format!("open encrypted parquet {path}: {e}")))?;

    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(bytes_reader, arrow_meta);
    finalize_pipeline(
        builder,
        path,
        opts,
        counters,
        page_cache,
        cache_key,
        catalog_schema,
    )
    .await
}

/// Build a Parquet [`RowSelection`] from per-page min/max stats (the column
/// index) for the supplied list of surviving row groups.
///
/// For each kept row group we inspect the column index of a **single
/// representative column** (the first predicate column that has a column
/// index entry). For each data page in that row group we apply the same
/// `Gt` / `Lt` / `Eq` logic used at row-group level: if the page's min/max
/// stats prove no row can satisfy ANY of the supplied predicates we skip that
/// page; otherwise we keep it. The result is a contiguous `RowSelection`
/// spanning all kept row groups.
///
/// Predicate shapes handled at page granularity:
/// * `Gt(col, v)` — skip page if `page_max <= v` (entire page is ≤ v, so
///   no row satisfies `> v`).
/// * `Lt(col, v)` — skip page if `page_min >= v` (entire page is ≥ v, so
///   no row satisfies `< v`).
/// * `Eq(col, v)` — skip page if `v < page_min || v > page_max`.
///
/// Predicate shapes NOT handled (`StartsWith`, `In`, multi-column conjuncts
/// where the representative column isn't covered) fall through to keep-all
/// pages, which is conservative and always correct — the per-row `RowFilter`
/// re-checks every materialized row.
///
/// Returns `None` when:
/// * `filters` is empty, or
/// * no predicate column has a column index, or
/// * the metadata has no column index (file written without page index).
///
/// In all `None` cases the caller should not call `with_row_selection` and
/// the read proceeds as a full row-group scan (existing behaviour).
fn build_page_row_selection(
    metadata: &parquet::file::metadata::ParquetMetaData,
    kept_row_groups: &[usize],
    filters: &[Predicate],
    arrow_schema: &SchemaRef,
) -> Option<(RowSelection, u64)> {
    if filters.is_empty() || kept_row_groups.is_empty() {
        return None;
    }

    // Require both column index and offset index to be present.
    let col_idx_all = metadata.column_index()?;
    let off_idx_all = metadata.offset_index()?;

    // Resolve (parquet-column-index, Predicate) pairs for range predicates only.
    // We handle Gt / Lt / Eq; StartsWith falls through (keep all pages).
    // We pick the FIRST predicate that has a column index in the file to drive
    // page pruning. Using one column is conservative: if the column index for
    // that column says "page can be skipped" we skip it; we never skip a page
    // that might match another predicate. More predicates could be intersected
    // (AND semantics) for a tighter prune, but one is enough for the IN-list
    // range atoms (Gt(min-1) AND Lt(max+1) on the same pk column).
    //
    // For each filter find its parquet column position in the arrow schema.
    let range_filters: Vec<(usize, &Predicate)> = filters
        .iter()
        .filter(|f| matches!(f, Predicate::Gt(..) | Predicate::Lt(..) | Predicate::Eq(..)))
        .filter_map(|f| arrow_schema.index_of(f.column()).ok().map(|idx| (idx, f)))
        .collect();

    if range_filters.is_empty() {
        return None;
    }

    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut total_selected: u64 = 0;
    let mut any_page_pruned = false;

    for &rg_idx in kept_row_groups {
        // Get the offset index for this row group — it tells us the
        // first_row_index of each data page per column.
        let off_rg = match off_idx_all.get(rg_idx) {
            Some(v) => v,
            None => {
                // No offset index for this row group — keep all rows.
                let n_rows = metadata.row_group(rg_idx).num_rows() as usize;
                total_selected += n_rows as u64;
                selectors.push(RowSelector::select(n_rows));
                continue;
            }
        };
        let col_rg = match col_idx_all.get(rg_idx) {
            Some(v) => v,
            None => {
                let n_rows = metadata.row_group(rg_idx).num_rows() as usize;
                total_selected += n_rows as u64;
                selectors.push(RowSelector::select(n_rows));
                continue;
            }
        };

        // Collect ALL (page_locations, col_index, predicate) triples that have
        // usable page stats. A page is excluded when ANY predicate in this list
        // proves no row can satisfy it (AND-conjunction semantics: excluding on
        // one predicate is sufficient because all must hold simultaneously).
        type PageTuple<'a> = (
            &'a [parquet::file::page_index::offset_index::PageLocation],
            &'a ColumnIndexMetaData,
            &'a Predicate,
        );
        let mut active: Vec<PageTuple<'_>> = Vec::with_capacity(range_filters.len());
        for &(col_idx, pred) in &range_filters {
            let off_col = match off_rg.get(col_idx) {
                Some(o) => o,
                None => continue,
            };
            let col_col = match col_rg.get(col_idx) {
                Some(c) => c,
                None => continue,
            };
            if matches!(col_col, ColumnIndexMetaData::NONE) {
                continue;
            }
            active.push((off_col.page_locations(), col_col, pred));
        }

        let rg_num_rows = metadata.row_group(rg_idx).num_rows() as usize;

        if active.is_empty() {
            // No usable page index for any predicate column in this row group.
            total_selected += rg_num_rows as u64;
            selectors.push(RowSelector::select(rg_num_rows));
            continue;
        }

        // Use the page locations from the first active predicate's column for
        // determining page boundaries (row count per page). All columns share the
        // same page boundaries (same number of rows per data page) within a row
        // group, so any column's offset index gives the correct row spans.
        let page_locs = active[0].0;
        let n_pages = page_locs.len();

        if n_pages == 0 {
            total_selected += rg_num_rows as u64;
            selectors.push(RowSelector::select(rg_num_rows));
            continue;
        }

        // Walk pages: compute each page's row span from first_row_index values.
        for page_i in 0..n_pages {
            let first_row = page_locs[page_i].first_row_index as usize;
            let next_first_row = if page_i + 1 < n_pages {
                page_locs[page_i + 1].first_row_index as usize
            } else {
                rg_num_rows
            };
            let page_rows = next_first_row - first_row;
            if page_rows == 0 {
                continue;
            }

            // A page is skipped when ANY predicate in the AND-conjunction can prove
            // no row in this page could satisfy it. We check each (col_index, pred)
            // pair; skipping is also safe for all-null pages (NULLs never match
            // Eq/Gt/Lt against non-null literals).
            let should_skip = active.iter().any(|(_, col_index, pred)| {
                if col_index.is_null_page(page_i) {
                    // Null-only page: no value can satisfy Eq/Gt/Lt against a
                    // non-null literal. Safe to skip.
                    true
                } else {
                    page_excluded_by_predicate(col_index, page_i, pred)
                }
            });

            if should_skip {
                any_page_pruned = true;
                selectors.push(RowSelector::skip(page_rows));
            } else {
                total_selected += page_rows as u64;
                selectors.push(RowSelector::select(page_rows));
            }
        }
    }

    if !any_page_pruned {
        // No page was actually pruned — don't install a selection that
        // would add overhead for zero benefit.
        return None;
    }

    let selection = RowSelection::from(selectors);
    Some((selection, total_selected))
}

/// Decide whether a single data page can be skipped for a given predicate,
/// using the page-level column index min/max. Returns `true` (skip) only
/// when the stats **prove** no row in the page can satisfy the predicate.
/// Returns `false` (keep) on any uncertainty (missing stats, type mismatch,
/// or overlap).
///
/// Correctness guarantee: a `true` result is only returned when the page's
/// min > predicate-max or page's max < predicate-min for range predicates, or
/// the literal is entirely outside [page_min, page_max] for Eq. Any `false`
/// return is safe — it only means we decode the page unnecessarily, but never
/// lose matching rows.
fn page_excluded_by_predicate(
    col_index: &ColumnIndexMetaData,
    page_i: usize,
    pred: &Predicate,
) -> bool {
    match (pred, col_index) {
        // --- Int64 column vs Int64 scalar ---
        (Predicate::Gt(_, ScalarValue::Int64(v)), ColumnIndexMetaData::INT64(idx)) => {
            // Skip if page_max <= v (no value in page is > v).
            idx.max_values().get(page_i).map_or(false, |max| *max <= *v)
        }
        (Predicate::Lt(_, ScalarValue::Int64(v)), ColumnIndexMetaData::INT64(idx)) => {
            // Skip if page_min >= v (no value in page is < v).
            idx.min_values().get(page_i).map_or(false, |min| *min >= *v)
        }
        (Predicate::Eq(_, ScalarValue::Int64(v)), ColumnIndexMetaData::INT64(idx)) => {
            let min = idx.min_values().get(page_i).copied();
            let max = idx.max_values().get(page_i).copied();
            match (min, max) {
                (Some(mn), Some(mx)) => *v < mn || *v > mx,
                _ => false,
            }
        }
        // --- Int32 column vs Int64 scalar (widen to i64) ---
        (Predicate::Gt(_, ScalarValue::Int64(v)), ColumnIndexMetaData::INT32(idx)) => {
            idx.max_values().get(page_i).map_or(false, |max| (*max as i64) <= *v)
        }
        (Predicate::Lt(_, ScalarValue::Int64(v)), ColumnIndexMetaData::INT32(idx)) => {
            idx.min_values().get(page_i).map_or(false, |min| (*min as i64) >= *v)
        }
        (Predicate::Eq(_, ScalarValue::Int64(v)), ColumnIndexMetaData::INT32(idx)) => {
            let min = idx.min_values().get(page_i).map(|x| *x as i64);
            let max = idx.max_values().get(page_i).map(|x| *x as i64);
            match (min, max) {
                (Some(mn), Some(mx)) => *v < mn || *v > mx,
                _ => false,
            }
        }
        // --- Float64 column vs Float64 scalar ---
        (Predicate::Gt(_, ScalarValue::Float64(v)), ColumnIndexMetaData::DOUBLE(idx)) => {
            idx.max_values().get(page_i).map_or(false, |max| *max <= *v)
        }
        (Predicate::Lt(_, ScalarValue::Float64(v)), ColumnIndexMetaData::DOUBLE(idx)) => {
            idx.min_values().get(page_i).map_or(false, |min| *min >= *v)
        }
        (Predicate::Eq(_, ScalarValue::Float64(v)), ColumnIndexMetaData::DOUBLE(idx)) => {
            let min = idx.min_values().get(page_i).copied();
            let max = idx.max_values().get(page_i).copied();
            match (min, max) {
                (Some(mn), Some(mx)) => *v < mn || *v > mx,
                _ => false,
            }
        }
        // --- Utf8 / ByteArray column vs Utf8 scalar (byte-lex order) ---
        (Predicate::Gt(_, ScalarValue::Utf8(v)), ColumnIndexMetaData::BYTE_ARRAY(idx)) => {
            let vb = v.as_bytes();
            idx.max_value(page_i).map_or(false, |max| max <= vb)
        }
        (Predicate::Lt(_, ScalarValue::Utf8(v)), ColumnIndexMetaData::BYTE_ARRAY(idx)) => {
            let vb = v.as_bytes();
            idx.min_value(page_i).map_or(false, |min| min >= vb)
        }
        (Predicate::Eq(_, ScalarValue::Utf8(v)), ColumnIndexMetaData::BYTE_ARRAY(idx)) => {
            let vb = v.as_bytes();
            let min = idx.min_value(page_i);
            let max = idx.max_value(page_i);
            match (min, max) {
                (Some(mn), Some(mx)) => vb < mn || vb > mx,
                _ => false,
            }
        }
        // All other type/predicate combinations: conservative keep.
        _ => false,
    }
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
            let mut missing_cols: Vec<(usize, arrow_schema::FieldRef)> = Vec::new();

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
                                    return Err(BasinError::storage(format!("unknown column {c}")));
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
                let mut output_fields: Vec<arrow_schema::FieldRef> = Vec::with_capacity(cols.len());
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
                    // Index-probe row-group allowlist for this file (if any).
                    // None or missing-file = scan every row-group (legacy
                    // behaviour). Files in the map honour only the listed ids.
                    let rg_allow = rowgroup_allow_for_file(opts.as_ref(), &path);
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
                        // Index allowlist applied first; the predicate-stats
                        // prune below is still authoritative for the rest.
                        if let Some(allow) = rg_allow.as_ref() {
                            if !allow.contains(&(i as u32)) {
                                pruned += 1;
                                continue;
                            }
                        }
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

                // Build a page-level RowSelection for the surviving row groups
                // using the column index (per-page min/max stats). This narrows
                // decoding from "all ~65k rows in the kept row group" to only
                // the data pages whose stats don't rule out a predicate match.
                let page_selection = build_page_row_selection(
                    builder.metadata(),
                    &kept,
                    &opts.filters,
                    &arrow_schema,
                );
                if let Some((_, rows_sel)) = &page_selection {
                    counters
                        .rows_selected_by_page_index
                        .fetch_add(*rows_sel, Ordering::Relaxed);
                }

                let mut builder = builder.with_row_groups(kept);
                if let Some((sel, _)) = page_selection {
                    builder = builder.with_row_selection(sel);
                }

                if !opts.filters.is_empty() {
                    let predicates =
                        build_row_filter(&opts.filters, &arrow_schema, &parquet_schema)?;
                    builder = builder.with_row_filter(predicates);
                }

                // LIMIT pushdown into the Parquet builder. parquet-rs
                // `with_limit` does BOTH inter-row-group skip (stops decoding
                // entire row groups once N rows are produced) AND
                // intra-row-group early-exit (stops mid-batch). The
                // `apply_limit_to_stream` wrapper at the call site is kept as
                // a belt-and-braces guard (no-op when the builder exits first).
                if let Some(lim) = opts.limit {
                    builder = builder.with_limit(lim);
                }

                let stream = builder
                    .build()
                    .map_err(|e| BasinError::storage(format!("parquet build {path}: {e}")))?;
                let catalog_schema_for_stamp = catalog_schema.clone();
                let mapped = stream.map(move |res| {
                    let batch =
                        res.map_err(|e| BasinError::storage(format!("parquet read: {e}")))?;
                    let synth = synthesise_missing_columns(
                        batch,
                        &present_names_arc,
                        &missing_cols_arc,
                        &output_schema_arc,
                    )?;
                    // Re-stamp BASIN_TYPE (and any other) field metadata from
                    // the catalog schema — Parquet's `ArrowWriter` round-trip
                    // does not re-apply the `ARROW:schema` blob's field
                    // metadata to the per-batch schema the reader emits.
                    restamp_field_metadata_from_catalog(synth, catalog_schema_for_stamp.as_ref())
                });

                // Page-cache write-through with the synthesised batches.
                //
                // LRU-budget-aware: skip the per-batch clone entirely
                // when the cache is already at its byte budget. The
                // entry would be evicted on insert anyway, so the clone
                // is pure overhead — and on cache-full workloads we've
                // measured the layer (c) read path at ~2× of layer (b)
                // because of this clone alone.
                // Page-cache write-through is keyed on (file, projection,
                // filters) but NOT on LIMIT. Caching a partial (LIMIT-bounded)
                // result would later satisfy unlimited / larger-N callers from
                // a too-short entry, corrupting query results. Skip the
                // write-through whenever a LIMIT is in effect.
                if let (Some(pc), Some(key), true) =
                    (page_cache, cache_key, opts.limit.is_none())
                {
                    if !pc.has_capacity() {
                        return Ok(mapped.boxed());
                    }
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
        // Index-probe row-group allowlist for this file (if any). None or
        // missing-file means scan every row-group (legacy behaviour).
        let rg_allow = rowgroup_allow_for_file(opts.as_ref(), &path);
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
            // The index allowlist is a structural pre-filter (a superset of
            // matching row-groups, per the GIN row-group prune contract); we
            // intersect with it before the stats-based prune.
            if let Some(allow) = rg_allow.as_ref() {
                if !allow.contains(&(i as u32)) {
                    pruned += 1;
                    continue;
                }
            }
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

    // Build a page-level RowSelection for the surviving row groups using the
    // column index (per-page min/max stats). This narrows decoding within each
    // kept row group from all rows to only the data pages whose stats don't
    // rule out a predicate match. Critical for IN-list queries: the
    // bounding-range atoms (Gt(min-1), Lt(max+1)) synthesised by the engine
    // target only a narrow band of pages in an otherwise large row group.
    let page_selection = build_page_row_selection(
        builder.metadata(),
        &kept,
        &opts.filters,
        &arrow_schema,
    );
    if let Some((_, rows_sel)) = &page_selection {
        counters
            .rows_selected_by_page_index
            .fetch_add(*rows_sel, Ordering::Relaxed);
    }

    let mut builder = builder.with_row_groups(kept);
    if let Some((sel, _)) = page_selection {
        builder = builder.with_row_selection(sel);
    }

    if !opts.filters.is_empty() {
        let predicates = build_row_filter(&opts.filters, &arrow_schema, &parquet_schema)?;
        builder = builder.with_row_filter(predicates);
    }

    // LIMIT pushdown into the Parquet builder (see notes on the
    // synth-batch site above): inter- and intra-row-group early-exit.
    if let Some(lim) = opts.limit {
        builder = builder.with_limit(lim);
    }

    let stream = builder
        .build()
        .map_err(|e| BasinError::storage(format!("parquet build {path}: {e}")))?;
    let catalog_schema_for_stamp = catalog_schema.clone();
    // Projection-order reassembly: `ProjectionMask::roots` selects the
    // requested columns but the Parquet reader delivers them in *file-schema*
    // order, not in the caller's requested `opts.projection` order. Callers
    // that build an output schema in projection order (e.g.
    // `TombstoneColdScanExec`, which augments the projection with the PK
    // column and declares its exec schema in that order) then see a batch
    // whose physical column layout disagrees with the declared schema —
    // surfacing downstream as a DataFusion "expected <T> but found <U> at
    // column index N" error. Reorder each batch by the requested projection
    // names so the emitted column order always matches what the caller asked
    // for. `None` projection (read all columns) is unaffected. The
    // missing-column synth path above already reorders, so this only covers
    // the all-present fall-through. The reorder is a metadata + Arc-pointer
    // shuffle (no data motion) and is a no-op when the file order already
    // matches the projection order.
    let projection_order: Option<Arc<Vec<String>>> =
        opts.projection.as_ref().map(|p| Arc::new(p.clone()));
    let mapped = stream.map(move |res| {
        let batch = res.map_err(|e| BasinError::storage(format!("parquet read: {e}")))?;
        let batch = match &projection_order {
            Some(names) => reorder_batch_to_projection(batch, names)?,
            None => batch,
        };
        // Re-stamp BASIN_TYPE field metadata from the catalog schema —
        // Parquet's `ArrowWriter` round-trip does not re-apply the
        // `ARROW:schema` blob's field metadata to the per-batch schema the
        // reader emits, so semantic types (MONEY, INET, …) would otherwise
        // be downgraded to their physical Arrow type post-storage.
        restamp_field_metadata_from_catalog(batch, catalog_schema_for_stamp.as_ref())
    });

    // Page-cache write-through is keyed on (file, projection, filters)
    // but NOT on LIMIT. Caching a partial (LIMIT-bounded) result would
    // later satisfy unlimited / larger-N callers from a too-short
    // entry, corrupting query results. Skip the write-through whenever
    // a LIMIT is in effect.
    if let (Some(pc), Some(key), true) = (page_cache, cache_key, opts.limit.is_none()) {
        // LRU-budget-aware (see notes on the synth-batch site above):
        // skip write-through entirely when the cache has no capacity
        // for a new entry. Avoids paying the per-batch clone cost on
        // queries that would just churn the LRU.
        if !pc.has_capacity() {
            return Ok(mapped.boxed());
        }
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
/// Post-decode projection + predicate application for Vortex data files.
///
/// The Parquet pipeline pushes `opts.filters` into a `RowFilter` and
/// `opts.projection` into a `ProjectionMask`; the Vortex codec has no
/// equivalent pushdown yet, so we reproduce the *result* here: filter the
/// decoded batches with the same `predicate::evaluate` the Parquet
/// `RowFilter` uses, then subset/reorder columns by name. Filtering runs
/// before projection so predicate columns are still present (matching the
/// Parquet ordering). A projected column absent from the file is
/// synthesised as a typed NULL column from the catalog schema (mirrors
/// `synthesise_missing_columns` for the post-`ALTER TABLE ADD COLUMN`
/// case). Correctness-critical: every predicate-driven read (SELECT …
/// WHERE, and the constraint/UNIQUE/FK/RLS/UPDATE/DELETE row-matching
/// scans) delegates the predicate to the storage layer just as it does
/// for Parquet.
///
/// `apply_filter`: when `false` the Arrow filter pass is skipped entirely
/// (caller has confirmed all predicates were pushed into Vortex natively
/// and the scan did not fall back). The projection + missing-column
/// synthesis always runs.
fn vortex_project_and_filter(
    batches: Vec<RecordBatch>,
    opts: &ReadOptions,
    catalog_schema: Option<&SchemaRef>,
    apply_filter: bool,
) -> Result<Vec<RecordBatch>> {
    vortex_project_and_filter_limited(batches, opts, catalog_schema, apply_filter, None)
}

/// Same as [`vortex_project_and_filter`] but with an optional early per-call
/// row cap (`early_limit`).  When `Some(lim)`, the running post-filter row
/// total is truncated to `lim` BEFORE the per-column projection + metadata
/// restamp passes, so those passes touch at most `lim` rows rather than every
/// surviving row of the file.
///
/// This is the keyset-pagination win: each cold file is ASC-sorted on the
/// cluster column, so the first `lim` post-filter rows are exactly that file's
/// top-`lim` contribution.  The caller applies the identical truncation
/// afterwards, so this is a pure work-saving early cut, not a semantic change.
///
/// IMPORTANT: this MUST NOT be used on a code path that write-throughs the
/// post-filter result into the page cache (the normal pushdown decode does),
/// because a truncated entry would be served — wrongly — to a later query with
/// a larger or absent LIMIT.  Only the unfiltered-decode-reuse paths (which
/// cache the RAW pre-filter batches, never this truncated output) pass a
/// non-`None` `early_limit`.
fn vortex_project_and_filter_limited(
    batches: Vec<RecordBatch>,
    opts: &ReadOptions,
    catalog_schema: Option<&SchemaRef>,
    apply_filter: bool,
    early_limit: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    let limit = early_limit;
    let mut seen = 0usize;
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        if let Some(lim) = limit {
            if seen >= lim {
                break;
            }
        }
        // 1) Filter (predicate columns referenced by name; pre-projection).
        let filtered = if !apply_filter || opts.filters.is_empty() {
            batch
        } else {
            let mut mask: Option<arrow_array::BooleanArray> = None;
            for f in &opts.filters {
                let m = predicate::evaluate(&batch, f)?;
                mask = Some(match mask {
                    None => m,
                    Some(prev) => arrow::compute::and(&prev, &m)
                        .map_err(|e| BasinError::storage(format!("vortex filter AND: {e}")))?,
                });
            }
            match mask {
                Some(m) => arrow::compute::filter_record_batch(&batch, &m)
                    .map_err(|e| BasinError::storage(format!("vortex filter: {e}")))?,
                None => batch,
            }
        };

        // Early per-call LIMIT cut: slice the post-filter batch to the rows
        // still needed before any per-column projection / restamp work.
        let filtered = if let Some(lim) = limit {
            let remaining = lim - seen;
            let take = remaining.min(filtered.num_rows());
            seen += take;
            if take == filtered.num_rows() {
                filtered
            } else {
                filtered.slice(0, take)
            }
        } else {
            filtered
        };

        // 2) Projection (subset + reorder by name; missing → typed NULL).
        let projected = match &opts.projection {
            None => filtered,
            Some(cols) => {
                let in_schema = filtered.schema();
                let mut fields: Vec<arrow_schema::FieldRef> = Vec::with_capacity(cols.len());
                let mut arrays: Vec<arrow_array::ArrayRef> = Vec::with_capacity(cols.len());
                for name in cols {
                    if let Some((idx, _)) = in_schema.column_with_name(name) {
                        fields.push(in_schema.field(idx).clone().into());
                        arrays.push(filtered.column(idx).clone());
                    } else {
                        let field = catalog_schema
                            .and_then(|s| s.column_with_name(name).map(|(i, _)| s.field(i).clone()))
                            .ok_or_else(|| {
                                BasinError::storage(format!(
                                    "vortex projection: column '{name}' absent from file \
                                     and from the catalog schema"
                                ))
                            })?;
                        arrays.push(new_null_array(field.data_type(), filtered.num_rows()));
                        fields.push(field.into());
                    }
                }
                let pschema = Arc::new(Schema::new(
                    fields
                        .iter()
                        .map(|f| f.as_ref().clone())
                        .collect::<Vec<_>>(),
                ));
                RecordBatch::try_new(pschema, arrays)
                    .map_err(|e| BasinError::storage(format!("vortex projection assemble: {e}")))?
            }
        };
        // Re-stamp BASIN_TYPE field metadata from the catalog schema —
        // Vortex's `DType::to_arrow_schema` drops field metadata wholesale
        // on decode, so semantic types (MONEY, INET, …) would otherwise be
        // downgraded to their physical Arrow type post-storage.
        let restamped = restamp_field_metadata_from_catalog(projected, catalog_schema)?;
        // ADR 0024 — UUID-as-Decimal256 read-side inverse. The write path
        // reinterprets `FixedSizeBinary(16) + BASIN_TYPE=UUID` columns as
        // `Decimal256(39, 0)` before handing to Vortex; restore the UUID
        // layout here so basin-engine, planner, pgwire, and REST keep
        // seeing UUIDs above the storage trait. Restamping above is the
        // load-bearing pre-condition: without `BASIN_TYPE=UUID` on the
        // post-decode schema we cannot distinguish a UUID-disguised
        // Decimal256 from a genuine wide-precision NUMERIC column.
        // TODO(adr-0024): drop when vortex grows native FixedSizeBinary(N).
        let restored = decimal256_to_uuid_fsb(restamped);
        // PG-Wave-α — POINT-as-LargeBinary read-side inverse. The write
        // path reinterprets `FixedSizeBinary(21) + BASIN_TYPE=POINT`
        // columns as `LargeBinary` for Vortex; restore the FSB(21)
        // layout here so downstream layers keep seeing POINT. Same
        // metadata pre-condition applies (the restamp above retags the
        // column with `BASIN_TYPE=POINT`).
        let restored = large_binary_to_point_fsb(restored);
        out.push(restored);
    }
    Ok(out)
}

/// Translate Basin's storage `Predicate`s into a single Vortex filter
/// `Expression` to push into the scan (drives native zone-map chunk
/// pruning).
///
/// **Type-safety is mandatory, not best-effort.** Vortex *panics* (in a
/// spawned scan task — uncatchable by the `decode` `Result` fallback) when
/// a pushed comparison mixes DTypes, e.g. an `i64` literal against an
/// `INT4`/`i32` column ("Cannot compare different DTypes i32 and i64").
/// So a predicate is pushed ONLY when the catalog schema proves the
/// column's Arrow type *exactly* equals the scalar's natural type
/// (Int64/Float64/Boolean/UInt64). Anything else — width mismatch,
/// strings (Vortex `Utf8View` nuance), timestamps, or no catalog schema —
/// is NOT pushed; `vortex_project_and_filter` still applies it
/// post-decode, so correctness is unaffected and only the zone-prune
/// optimisation is skipped for that predicate. Returns `None` when nothing
/// is safely pushable.
///
/// Also returns `all_pushed: bool` — true iff every predicate in `filters`
/// was type-safe-pushed (or `filters` is empty). When `all_pushed` is true
/// AND `decode` reports it actually ran with pushdown (no fallback), the
/// caller can skip the Arrow post-filter pass entirely.
fn vortex_filter_expr(
    filters: &[Predicate],
    catalog_schema: Option<&Schema>,
) -> (Option<vortex_array::expr::Expression>, bool) {
    use arrow_schema::DataType;
    use vortex_array::expr::{and_collect, col, eq, gt, like, lit, lt, Expression};

    if filters.is_empty() {
        return (None, true);
    }
    let Some(schema) = catalog_schema else {
        return (None, false);
    };
    let type_safe_lit = |c: &str, v: &ScalarValue| -> Option<Expression> {
        let dt = schema.field_with_name(c).ok()?.data_type();
        match (v, dt) {
            (ScalarValue::Int64(i), DataType::Int64) => Some(lit(*i)),
            (ScalarValue::Float64(f), DataType::Float64) => Some(lit(*f)),
            (ScalarValue::Boolean(b), DataType::Boolean) => Some(lit(*b)),
            (ScalarValue::UInt64(u), DataType::UInt64) => Some(lit(*u)),
            // Width/type mismatch, strings, timestamps, decimals, etc. —
            // do NOT push (would risk a Vortex dtype-compare panic).
            _ => None,
        }
    };

    let mut all_pushed = true;
    let terms: Vec<Expression> = filters
        .iter()
        .filter_map(|p| match p {
            Predicate::Eq(c, v) => {
                let e = type_safe_lit(c, v).map(|l| eq(col(c.as_str()), l));
                if e.is_none() {
                    all_pushed = false;
                }
                e
            }
            Predicate::Gt(c, v) => {
                let e = type_safe_lit(c, v).map(|l| gt(col(c.as_str()), l));
                if e.is_none() {
                    all_pushed = false;
                }
                e
            }
            Predicate::Lt(c, v) => {
                let e = type_safe_lit(c, v).map(|l| lt(col(c.as_str()), l));
                if e.is_none() {
                    all_pushed = false;
                }
                e
            }
            // Prefix match → Vortex `LIKE 'escaped_prefix%'`. The kernel
            // requires both child and pattern to be UTF8 dtype, and its
            // `stat_falsification` does min/max zone-map pruning for the
            // `LIKE 'prefix%'` shape (when not negated, not ILIKE) — so
            // pushing this is a real prune win, not just a post-decode
            // cost shift. ILIKE is left to the post-decode re-evaluation
            // (Vortex's stat-falsification doesn't handle case-insensitive).
            //
            // Type-safety mirrors the scalar arms above: only push when
            // the catalog schema proves the column is Arrow `Utf8`.
            // `LargeUtf8` / `Utf8View` are NOT pushed (avoid the dtype
            // mismatch panic in the spawned scan task).
            Predicate::StartsWith {
                column,
                prefix,
                case_insensitive,
            } => {
                let dt_utf8 = schema
                    .field_with_name(column)
                    .ok()
                    .map(|f| matches!(f.data_type(), DataType::Utf8))
                    .unwrap_or(false);
                if *case_insensitive || !dt_utf8 {
                    all_pushed = false;
                    None
                } else {
                    // Escape LIKE metacharacters in the literal prefix
                    // (`\`, `%`, `_`) so a pattern like `100%off` matches
                    // literally up to the appended `%`.
                    let mut escaped = String::with_capacity(prefix.len() + 1);
                    for ch in prefix.chars() {
                        if ch == '\\' || ch == '%' || ch == '_' {
                            escaped.push('\\');
                        }
                        escaped.push(ch);
                    }
                    escaped.push('%');
                    Some(like(col(column.as_str()), lit(escaped)))
                }
            }
        })
        .collect();

    (and_collect(terms.into_iter()), all_pushed)
}

/// The set of columns the Vortex scan must materialise: the requested
/// projection plus any columns referenced only by the (post-decode)
/// filter, in a stable order (projection order first). Returning `None`
/// means "read every column" (no projection requested). Filter columns are
/// included so `vortex_project_and_filter` can still re-verify the
/// predicate authoritatively before dropping them.
fn vortex_read_projection(opts: &ReadOptions) -> Option<Vec<String>> {
    let proj = opts.projection.as_ref()?;
    let mut out: Vec<String> = Vec::with_capacity(proj.len() + opts.filters.len());
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for c in proj {
        if seen.insert(c.as_str()) {
            out.push(c.clone());
        }
    }
    for f in &opts.filters {
        let c = f.column();
        if seen.insert(c) {
            out.push(c.to_string());
        }
    }
    Some(out)
}

/// Re-stamp each output field's metadata from the catalog schema (by name)
/// so logical-type markers like `BASIN_TYPE` survive the storage round-trip.
///
/// Both the Parquet `ArrowWriter` and the Vortex codec drop field-level
/// Arrow metadata on the way back in (Parquet's `ARROW:schema` blob is not
/// re-applied to the per-batch schema the reader emits; Vortex's
/// `DType::to_arrow_schema` strips field metadata wholesale). The engine
/// emits BASIN_TYPE field metadata at DDL time so semantic types (MONEY,
/// INET, CIDR, MACADDR, MACADDR8, BIT(n), VARBIT(n), JSONB, UUID, …)
/// can be reconstructed from the Arrow schema alone; losing those markers
/// downgrades reads to the physical Arrow type and breaks parity with
/// Postgres for the storage→pgwire/REST/realtime round-trip.
///
/// Pure metadata rewrap — column arrays are reused unchanged. When
/// `catalog_schema` is `None` (schema-less callers: `read_file`,
/// continuous-view refresh, cron-job state, system tables) the input
/// batch is returned untouched.
fn restamp_field_metadata_from_catalog(
    batch: RecordBatch,
    catalog_schema: Option<&SchemaRef>,
) -> Result<RecordBatch> {
    let Some(catalog) = catalog_schema else {
        return Ok(batch);
    };
    let in_schema = batch.schema();
    // Cheap probe: bail out when no field in the batch has a catalog
    // counterpart that would change its metadata. Avoids cloning the
    // schema for the common-case batch that is already correctly stamped
    // (e.g. write-then-read in the same process before any encode/decode).
    let needs_rewrap = in_schema.fields().iter().any(|f| {
        catalog
            .field_with_name(f.name())
            .ok()
            .map(|cf| cf.metadata() != f.metadata())
            .unwrap_or(false)
    });
    if !needs_rewrap {
        return Ok(batch);
    }
    let new_fields: Vec<arrow_schema::FieldRef> = in_schema
        .fields()
        .iter()
        .map(|f| match catalog.field_with_name(f.name()) {
            Ok(cf) if cf.metadata() != f.metadata() => {
                // Keep the physical type from the file (it may differ
                // legitimately — e.g. timezone normalisation) but adopt the
                // catalog's metadata so BASIN_TYPE survives.
                Arc::new(
                    Field::new(f.name(), f.data_type().clone(), f.is_nullable())
                        .with_metadata(cf.metadata().clone()),
                )
            }
            _ => f.clone(),
        })
        .collect();
    let new_schema = Arc::new(Schema::new_with_metadata(
        new_fields,
        in_schema.metadata().clone(),
    ));
    // Reuse the column arrays — no data motion. This is a metadata-only
    // rewrap (field names, types and column arrays are unchanged — only the
    // per-field metadata map is adopted from the catalog), so it cannot fail
    // under normal invariants. We surface any failure as a clean BasinError
    // rather than a non-string panic so a future schema-shape drift (e.g. a
    // type divergence that slips past the metadata-only contract) reports as
    // an error the caller can handle instead of aborting the worker thread.
    let cols = batch.columns().to_vec();
    RecordBatch::try_new(new_schema, cols).map_err(|e| {
        BasinError::storage(format!(
            "restamp_field_metadata_from_catalog: metadata-only rewrap failed: {e}"
        ))
    })
}

/// Field-metadata key the engine plants on UUID columns. Mirrors
/// `basin_engine::types::BASIN_TYPE_KEY` (load-bearing duplicate — the
/// storage layer must not depend on basin-engine).
const BASIN_TYPE_KEY: &str = "BASIN_TYPE";
/// `BASIN_TYPE` value for UUID columns. Mirrors
/// `basin_engine::types::BASIN_TYPE_UUID`.
const BASIN_TYPE_UUID: &str = "UUID";
/// PG-Wave-α: `BASIN_TYPE` value for POINT columns. Mirrors
/// `basin_engine::types::BASIN_TYPE_POINT`. Vortex stores POINT as
/// `LargeBinary`; restamping uses this marker to rebuild FSB(21).
const BASIN_TYPE_POINT: &str = "POINT";

/// ADR 0024 — translate the catalog schema so UUID columns claim
/// `Decimal256(39, 0)` instead of `FixedSizeBinary(16)`. Vortex 0.70 has
/// no FSB encoder, so the on-disk physical representation chosen by the
/// write path is Decimal256; if we hand the catalog schema (which says
/// FSB) to Vortex's `execute_record_batch`, it tries to cast Decimal256
/// → FSB and fails ("Conversion to Arrow type FixedSizeBinary(16) is not
/// supported"). The post-decode inverse in `decimal256_to_uuid_fsb`
/// rebuilds the FSB layout for callers above the storage trait, so the
/// disguise is invisible to basin-engine.
///
/// Field metadata is preserved verbatim (BASIN_TYPE=UUID + any others) so
/// the post-decode inverse can identify the disguised column.
///
/// TODO(adr-0024): drop when Vortex grows native `FixedSizeBinary(N)`
/// encoding and basin-engine pins the new release.
fn catalog_schema_uuid_to_decimal256(schema: &Schema) -> Schema {
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            let is_uuid_fsb = matches!(
                f.data_type(),
                arrow_schema::DataType::FixedSizeBinary(16)
            ) && f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str())
                == Some(BASIN_TYPE_UUID);
            if is_uuid_fsb {
                Field::new(f.name(), arrow_schema::DataType::Decimal256(39, 0), f.is_nullable())
                    .with_metadata(f.metadata().clone())
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Schema::new_with_metadata(new_fields, schema.metadata().clone())
}

/// ADR 0024 — read-side inverse of `writer::uuid_fsb_to_decimal256`. Walks
/// `batch`'s schema; for every column with Arrow type `Decimal256(39, 0)`
/// AND field metadata `BASIN_TYPE=UUID`, strips the 16-byte leading zero
/// padding the writer added and rebuilds `FixedSizeBinary(16)` so the
/// rest of Basin keeps seeing UUIDs.
///
/// Returns the input batch unchanged when no column needs translation.
///
/// TODO(adr-0024): drop when Vortex grows native `FixedSizeBinary(N)`
/// encoding and basin-engine pins the new release.
fn decimal256_to_uuid_fsb(batch: RecordBatch) -> RecordBatch {
    use arrow_array::{Array, Decimal256Array, FixedSizeBinaryArray};

    let schema = batch.schema();
    let needs_xlate = schema.fields().iter().any(|f| {
        matches!(f.data_type(), arrow_schema::DataType::Decimal256(39, 0))
            && f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_UUID)
    });
    if !needs_xlate {
        return batch;
    }

    let mut new_fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
    let mut new_cols: Vec<arrow_array::ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (i, f) in schema.fields().iter().enumerate() {
        let is_uuid = matches!(f.data_type(), arrow_schema::DataType::Decimal256(39, 0))
            && f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_UUID);
        if is_uuid {
            let src = batch
                .column(i)
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .expect("UUID-disguised column must be Decimal256Array");
            let len = src.len();
            // Build via the per-row sparse-iterator helper: it accepts
            // `Option<[u8; 16]>` and constructs the FixedSizeBinaryArray
            // with the right null mask. The writer left-padded the 16-byte
            // UUID with 16 zero bytes to form a 32-byte non-negative
            // `i256`; the inverse is `to_be_bytes()[16..32]`.
            let rows = (0..len).map(|r| {
                if src.is_null(r) {
                    None
                } else {
                    let full = src.value(r).to_be_bytes();
                    let mut buf = [0u8; 16];
                    buf.copy_from_slice(&full[16..32]);
                    Some(buf)
                }
            });
            let arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(rows, 16)
                .expect("FixedSizeBinary(16) construction cannot fail");
            let new_field =
                Field::new(f.name(), arrow_schema::DataType::FixedSizeBinary(16), f.is_nullable())
                    .with_metadata(f.metadata().clone());
            new_fields.push(new_field);
            new_cols.push(Arc::new(arr));
        } else {
            new_fields.push(f.as_ref().clone());
            new_cols.push(batch.column(i).clone());
        }
    }
    let new_schema = Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));
    RecordBatch::try_new(new_schema, new_cols)
        .expect("decimal256_to_uuid_fsb: schema swap cannot fail")
}

/// PG-Wave-α — catalog-schema reinterpretation: POINT FSB(21) →
/// LargeBinary so the Vortex executor sees the physical type it can
/// actually decode. The post-decode inverse `large_binary_to_point_fsb`
/// rebuilds FSB(21) before handing the batch above the storage trait.
fn catalog_schema_point_to_large_binary(schema: &Schema) -> Schema {
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            let is_point_fsb = matches!(
                f.data_type(),
                arrow_schema::DataType::FixedSizeBinary(n)
                    if *n == basin_geo::POINT_WKB_LEN as i32
            ) && f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str())
                == Some(BASIN_TYPE_POINT);
            if is_point_fsb {
                Field::new(f.name(), arrow_schema::DataType::LargeBinary, f.is_nullable())
                    .with_metadata(f.metadata().clone())
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Schema::new_with_metadata(new_fields, schema.metadata().clone())
}

/// PG-Wave-α — read-side inverse of `writer::point_fsb_to_large_binary`.
/// For every column with Arrow type `LargeBinary` + `BASIN_TYPE=POINT`,
/// rebuild the `FixedSizeBinary(21)` layout so the rest of basin keeps
/// seeing the canonical POINT physical type.
fn large_binary_to_point_fsb(batch: RecordBatch) -> RecordBatch {
    use arrow_array::{Array, BinaryArray, BinaryViewArray, FixedSizeBinaryArray, LargeBinaryArray};

    let schema = batch.schema();
    // Vortex 0.71 surfaces the on-disk LargeBinary column as either
    // LargeBinary, Binary, or BinaryView depending on layout. Accept
    // all three so the inverse fires regardless of the codec's choice.
    fn is_byte_array(dt: &arrow_schema::DataType) -> bool {
        matches!(
            dt,
            arrow_schema::DataType::LargeBinary
                | arrow_schema::DataType::Binary
                | arrow_schema::DataType::BinaryView
        )
    }
    let needs_xlate = schema.fields().iter().any(|f| {
        is_byte_array(f.data_type())
            && f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_POINT)
    });
    if !needs_xlate {
        return batch;
    }
    let mut new_fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
    let mut new_cols: Vec<arrow_array::ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (i, f) in schema.fields().iter().enumerate() {
        let is_point = is_byte_array(f.data_type())
            && f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_POINT);
        if is_point {
            let col = batch.column(i);
            let len = col.len();
            let rows: Vec<Option<[u8; basin_geo::POINT_WKB_LEN]>> = match col.data_type() {
                arrow_schema::DataType::LargeBinary => {
                    let a = col
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                        .expect("LargeBinaryArray for POINT");
                    (0..len)
                        .map(|r| {
                            if a.is_null(r) {
                                None
                            } else {
                                let v = a.value(r);
                                if v.len() == basin_geo::POINT_WKB_LEN {
                                    let mut buf = [0u8; basin_geo::POINT_WKB_LEN];
                                    buf.copy_from_slice(v);
                                    Some(buf)
                                } else {
                                    None
                                }
                            }
                        })
                        .collect()
                }
                arrow_schema::DataType::Binary => {
                    let a = col
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .expect("BinaryArray for POINT");
                    (0..len)
                        .map(|r| {
                            if a.is_null(r) {
                                None
                            } else {
                                let v = a.value(r);
                                if v.len() == basin_geo::POINT_WKB_LEN {
                                    let mut buf = [0u8; basin_geo::POINT_WKB_LEN];
                                    buf.copy_from_slice(v);
                                    Some(buf)
                                } else {
                                    None
                                }
                            }
                        })
                        .collect()
                }
                arrow_schema::DataType::BinaryView => {
                    let a = col
                        .as_any()
                        .downcast_ref::<BinaryViewArray>()
                        .expect("BinaryViewArray for POINT");
                    (0..len)
                        .map(|r| {
                            if a.is_null(r) {
                                None
                            } else {
                                let v = a.value(r);
                                if v.len() == basin_geo::POINT_WKB_LEN {
                                    let mut buf = [0u8; basin_geo::POINT_WKB_LEN];
                                    buf.copy_from_slice(v);
                                    Some(buf)
                                } else {
                                    None
                                }
                            }
                        })
                        .collect()
                }
                _ => unreachable!("checked by is_byte_array above"),
            };
            let arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                rows.into_iter(),
                basin_geo::POINT_WKB_LEN as i32,
            )
            .expect("FixedSizeBinary(POINT_WKB_LEN) construction cannot fail");
            let new_field = Field::new(
                f.name(),
                arrow_schema::DataType::FixedSizeBinary(basin_geo::POINT_WKB_LEN as i32),
                f.is_nullable(),
            )
            .with_metadata(f.metadata().clone());
            new_fields.push(new_field);
            new_cols.push(Arc::new(arr));
        } else {
            new_fields.push(f.as_ref().clone());
            new_cols.push(batch.column(i).clone());
        }
    }
    let new_schema = Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));
    RecordBatch::try_new(new_schema, new_cols)
        .expect("large_binary_to_point_fsb: schema swap cannot fail")
}

/// Reassemble `batch`'s columns into the order named by `projection`.
///
/// The Parquet reader's `ProjectionMask` selects the requested columns but
/// emits them in *file-schema* order; callers that requested a specific
/// projection order (and declared an output schema in that order) need the
/// batch reordered to match. When the batch's column order already equals the
/// projection order this returns the batch unchanged (no allocation). Columns
/// named in `projection` but absent from the batch are left to the caller's
/// missing-column synthesis path — here we only reorder columns that are
/// present, and fall back to the original batch if any name is missing (the
/// non-projected / all-columns read path passes `projection=None` and never
/// reaches this function).
fn reorder_batch_to_projection(batch: RecordBatch, projection: &[String]) -> Result<RecordBatch> {
    let schema = batch.schema();
    // Fast path: already in projection order (and same arity).
    if schema.fields().len() == projection.len()
        && schema
            .fields()
            .iter()
            .zip(projection.iter())
            .all(|(f, name)| f.name() == name)
    {
        return Ok(batch);
    }
    // Build the new column order by name. If any projected name is missing from
    // the batch, bail out unchanged — that case is handled by the
    // missing-column synthesis path, not here.
    let mut indices: Vec<usize> = Vec::with_capacity(projection.len());
    for name in projection {
        match schema.index_of(name) {
            Ok(i) => indices.push(i),
            Err(_) => return Ok(batch),
        }
    }
    batch
        .project(&indices)
        .map_err(|e| BasinError::storage(format!("reorder_batch_to_projection: {e}")))
}

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
    let present_out_positions: Vec<usize> =
        (0..num_out).filter(|p| !missing_set.contains(p)).collect();
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

/// Look up the surviving row-group ids for `path` in the read-options
/// allowlist. Returns `None` when no allowlist is set OR when the file is
/// not summarised (W's API contract: a missing file = "Unknown" = read all
/// row-groups). Returns `Some(&allow)` when the caller's prune decision
/// applies to this file. Membership is checked with a `HashSet`-style probe
/// by the caller (small surviving lists are typical: a Vec scan is fine).
fn rowgroup_allow_for_file<'a>(
    opts: &'a ReadOptions,
    path: &ObjectPath,
) -> Option<&'a Vec<u32>> {
    let map = opts.row_group_selection.as_ref()?;
    map.get(path.as_ref())
}

/// Wrap a record-batch stream with a post-filter row limit. When `limit` is
/// `None` the stream is returned unchanged (byte-identical to the
/// pre-limit-pushdown path). When `Some(n)`, the wrapper counts emitted
/// rows (which are already post-predicate, post-projection — i.e. only the
/// rows that pass `ReadOptions::filters`), slices the batch crossing the
/// boundary so exactly `n` rows are returned, and stops pulling further
/// batches from the upstream. Matches PG btree-scan LIMIT semantics: the
/// limit applies AFTER the filter, so a query like `WHERE col = X LIMIT 100`
/// returns 100 *matches* (not 100 candidate rows).
fn apply_limit_to_stream(
    stream: BoxStream<'static, Result<RecordBatch>>,
    limit: Option<usize>,
) -> BoxStream<'static, Result<RecordBatch>> {
    let Some(mut remaining) = limit else {
        return stream;
    };
    if remaining == 0 {
        // A LIMIT 0 query yields the schema with zero rows. Drop the upstream
        // entirely — DataFusion's planner will already have short-circuited
        // most of these, but be defensive.
        return futures::stream::empty().boxed();
    }
    // `take_while` would let through batches that overflow `remaining`. Use
    // `scan` so we can SLICE the boundary batch to exactly `remaining` rows
    // and terminate. The state carries the remaining-budget across yields;
    // once exhausted we emit None and the wrapper completes.
    let limited = stream
        .scan((), move |_, item| {
            let res = match item {
                Err(e) => Some(Some(Err(e))),
                Ok(batch) => {
                    if remaining == 0 {
                        // Already satisfied; ignore any trailing batches the
                        // upstream emits (e.g. page-cache terminator).
                        Some(None)
                    } else {
                        let n = batch.num_rows();
                        if n <= remaining {
                            remaining -= n;
                            Some(Some(Ok(batch)))
                        } else {
                            let take = remaining;
                            remaining = 0;
                            Some(Some(Ok(batch.slice(0, take))))
                        }
                    }
                }
            };
            async move { res }
        })
        // After we've emitted the boundary-slice batch, every subsequent
        // poll returns `Some(None)`. `take_while` on Option-Some lets us
        // terminate the stream once `remaining == 0`.
        .take_while(|opt| {
            let cont = opt.is_some();
            async move { cont }
        })
        .filter_map(|opt| async move { opt });
    limited.boxed()
}

#[cfg(test)]
mod tests {
    //! Reader-side coverage for the opt-in Vortex format branch in
    //! [`read_one`]. Mirrors the existing encrypted round-trip pattern used
    //! across the storage crate (attach an `EncryptionProvider`, write an
    //! envelope-encrypted body + `.wrapped` sidecar via the public writer,
    //! then read it back through `read_one`) — only here the on-disk format
    //! is Vortex (`.vortex` key) instead of Parquet.

    use super::*;

    use std::sync::Arc;

    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use basin_common::{PartitionKey, ProjectId, Result as BasinResult, TableName};
    use futures::StreamExt;
    use object_store::memory::InMemory;

    use crate::encryption::{EncryptionProvider, WrappedKey};
    use crate::writer::{FileFormat, WriteOptions};
    use crate::{ReadOptions, Storage, StorageConfig};

    /// Minimal in-process provider: a fixed 32-byte data key, round-tripped
    /// through an opaque sidecar. Mirrors the no-config (`wrap_key` /
    /// `unwrap_key`) leg of the integration suite's `RecordingProvider`,
    /// trimmed to exactly what the reader's encrypted path needs.
    struct StaticProvider;

    #[async_trait]
    impl EncryptionProvider for StaticProvider {
        async fn wrap_key(&self, _project: &ProjectId) -> BasinResult<(Vec<u8>, WrappedKey)> {
            let key = vec![0x42u8; 32];
            Ok((key.clone(), WrappedKey(key)))
        }

        async fn unwrap_key(
            &self,
            _project: &ProjectId,
            wrapped: &WrappedKey,
        ) -> BasinResult<Vec<u8>> {
            Ok(wrapped.0.clone())
        }
    }

    fn sample_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("note", DataType::Utf8, true),
            Field::new("score", DataType::Float64, false),
        ]))
    }

    fn sample_batch() -> RecordBatch {
        let id = Int64Array::from(vec![10, 20, 30, 40]);
        let note = StringArray::from(vec![Some("a"), None, Some("c"), Some("d")]);
        let score = Float64Array::from(vec![1.5, -2.25, 3.0, 0.0]);
        RecordBatch::try_new(
            sample_schema(),
            vec![Arc::new(id), Arc::new(note), Arc::new(score)],
        )
        .expect("build sample batch")
    }

    /// End-to-end: write an envelope-encrypted Vortex file via the public
    /// writer, then read it back through `read_one`. The decrypted plaintext
    /// must flow into `vortex_format::decode` and the RecordBatch must round
    /// -trip. This is the encrypted-byte-acquisition path for `.vortex`.
    #[tokio::test]
    async fn encrypted_vortex_round_trips_through_read_one() {
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(InMemory::new()),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        storage.attach_encryption_provider(Arc::new(StaticProvider));

        let project = ProjectId::new();
        let table = TableName::new("vortex_enc").unwrap();
        let part = PartitionKey::default_key();
        let schema = sample_schema();
        let original = sample_batch();

        // Public writer with the opt-in Vortex format. An encryption
        // provider is attached, so this produces a `.vortex` body that is
        // AES-GCM enveloped plus a `<key>.wrapped` sidecar.
        let df = storage
            .write_batch_with_options(
                &project,
                &table,
                &part,
                &original,
                &WriteOptions {
                    file_format: FileFormat::Vortex,
                    ..WriteOptions::default()
                },
            )
            .await
            .expect("write encrypted vortex");
        assert!(
            df.path.as_ref().ends_with(".vortex"),
            "writer must produce a .vortex key, got {}",
            df.path
        );

        // Read it back through `read_one` directly. No catalog is attached,
        // so we thread the table schema in via `catalog_schema` exactly as
        // the table-aware reader would — that is the `Arc<Schema>` the
        // Vortex decoder needs (the Parquet path resolves the same schema
        // from the catalog at this site).
        let project_config = storage
            .project_storage_config_cached(&project)
            .await
            .unwrap();
        let stream = read_one(
            storage.project_store(&project),
            df.path.clone(),
            Arc::new(ReadOptions::default()),
            storage.parquet_meta_cache().clone(),
            storage.read_counters().clone(),
            storage.page_cache_handle().cloned(),
            storage.vortex_footer_cache_handle().clone(),
            storage.project_counters(&project),
            storage.encryption_provider(),
            project_config,
            project,
            Some(schema.clone()),
        )
        .await
        .expect("read_one encrypted vortex");

        let batches: Vec<_> = stream.collect().await;
        let decoded: Vec<RecordBatch> = batches
            .into_iter()
            .map(|b| b.expect("batch decode"))
            .collect();

        let total_rows: usize = decoded.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            total_rows,
            original.num_rows(),
            "row count must round-trip through encrypted vortex"
        );
        assert_eq!(decoded.len(), 1, "single-chunk fixture -> one batch");

        let got = &decoded[0];
        assert_eq!(
            got.num_columns(),
            original.num_columns(),
            "column count must round-trip"
        );

        let g_id = got
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id Int64");
        let o_id = original
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(g_id, o_id, "Int64 column must round-trip");

        let g_note = got
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("note Utf8");
        let o_note = original
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            g_note, o_note,
            "Utf8 column (incl. nulls) must round-trip through encryption"
        );

        let g_score = got
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("score Float64");
        let o_score = original
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(g_score, o_score, "Float64 column must round-trip");
    }

    // -----------------------------------------------------------------------
    // Unfiltered-decode reuse (fresh-key point-read fast path)
    // -----------------------------------------------------------------------

    /// Storage with the page cache ENABLED at an explicit byte budget. The
    /// unfiltered-decode reuse path only fires when a page cache is present;
    /// these tests need that handle plus its hit/miss counters.
    fn paged_storage(max_bytes: u64) -> Storage {
        Storage::new(StorageConfig {
            object_store: Arc::new(InMemory::new()),
            root_prefix: None,
            disk_cache: None,
            page_cache: Some(crate::PageCacheConfig::new(max_bytes)),
        })
    }

    /// Schema with an Int64 PK plus a nullable Utf8 payload, so the parity
    /// test can exercise NULL semantics through the Arrow post-filter.
    fn pk_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("note", DataType::Utf8, true),
        ]))
    }

    /// `len` rows: id = start..start+len, note NULL on every 3rd row.
    fn pk_batch(start: i64, len: usize) -> RecordBatch {
        let id: Int64Array = (start..start + len as i64).collect();
        let note: StringArray = (0..len)
            .map(|i| {
                if i % 3 == 0 {
                    None
                } else {
                    Some(format!("n{}", start + i as i64))
                }
            })
            .collect();
        RecordBatch::try_new(pk_schema(), vec![Arc::new(id), Arc::new(note)]).unwrap()
    }

    /// Write `batch` as a `.vortex` file under one (project, table), forcing
    /// `row_block_size` so a small batch still produces multiple Vortex
    /// chunks (the multi-chunk parity surface). Returns the file path.
    async fn write_vortex(
        storage: &Storage,
        project: &ProjectId,
        table: &TableName,
        batch: &RecordBatch,
        row_block_size: Option<u32>,
    ) -> ObjectPath {
        let part = PartitionKey::default_key();
        let df = storage
            .write_batch_with_options(
                project,
                table,
                &part,
                batch,
                &WriteOptions {
                    file_format: FileFormat::Vortex,
                    row_block_size,
                    ..WriteOptions::default()
                },
            )
            .await
            .expect("write vortex");
        assert!(df.path.as_ref().ends_with(".vortex"));
        df.path
    }

    /// Read one `.vortex` file through `read_one` with an `Eq(id, v)` filter,
    /// returning the concatenated decoded rows' `id` column values.
    async fn read_eq_ids(
        storage: &Storage,
        project: &ProjectId,
        path: &ObjectPath,
        schema: Arc<Schema>,
        eq: i64,
    ) -> Vec<i64> {
        let project_config = storage
            .project_storage_config_cached(project)
            .await
            .unwrap();
        let opts = ReadOptions {
            filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(eq))],
            ..ReadOptions::default()
        };
        let stream = read_one(
            storage.project_store(project),
            path.clone(),
            Arc::new(opts),
            storage.parquet_meta_cache().clone(),
            storage.read_counters().clone(),
            storage.page_cache_handle().cloned(),
            storage.vortex_footer_cache_handle().clone(),
            storage.project_counters(project),
            storage.encryption_provider(),
            project_config,
            project.clone(),
            Some(schema),
        )
        .await
        .expect("read_one");
        let batches: Vec<RecordBatch> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|b| b.expect("batch"))
            .collect();
        let mut out = Vec::new();
        for b in &batches {
            let ids = b
                .column(b.schema().index_of("id").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            out.extend(ids.values().iter().copied());
        }
        out
    }

    /// The unfiltered key a small no-projection point read shares.
    fn unfiltered_key_for(path: &ObjectPath) -> CacheKey {
        CacheKey {
            path: path.clone(),
            projection_hash: hash_projection(None),
            filters_hash: hash_filters(&[]),
        }
    }

    /// All behaviours of the unfiltered-decode reuse path in ONE test. The
    /// kill switch (`BASIN_UNFILTERED_DECODE_CACHE_DISABLE`) is a
    /// process-global env var, so parallel `#[test]` execution would race —
    /// we sequence the cases here, exactly as
    /// `list_data_files_with_stats_cap_resolver_and_safety_net` does for its
    /// env knob.
    ///
    /// Covers:
    ///  1. fresh-key-after-warm: two distinct `Eq` keys on the same file
    ///     return correct disjoint rows, and the SECOND fresh key is served
    ///     from the shared unfiltered cache entry (page-cache hit counter
    ///     advances without a re-decode);
    ///  2. size cap respected: a tiny-budget cache bypasses the path and
    ///     never populates the shared key, yet stays correct;
    ///  3. kill switch reverts to the pushdown path (shared key never made);
    ///  4. parity: unfiltered+Arrow-filter == pushdown across present /
    ///     NULL-payload / absent keys on a multi-chunk file with NULLs.
    #[tokio::test]
    async fn unfiltered_decode_reuse_behaviours() {
        let key_env = "BASIN_UNFILTERED_DECODE_CACHE_DISABLE";
        let prior = std::env::var(key_env).ok();
        let project = ProjectId::new();
        let schema = pk_schema();

        // ───────────── 1. fresh-key-after-warm hits unfiltered cache ────────
        std::env::remove_var(key_env);
        let storage = paged_storage(64 * 1024 * 1024);
        let table = TableName::new("pts").unwrap();
        // 12 rows / row_block_size 4 -> 3 Vortex chunks.
        let path = write_vortex(&storage, &project, &table, &pk_batch(0, 12), Some(4)).await;

        let ids0 = read_eq_ids(&storage, &project, &path, schema.clone(), 5).await;
        assert_eq!(ids0, vec![5], "Eq(id,5) returns exactly that row");
        let after_first = storage.page_cache().unwrap().counters();

        let ids1 = read_eq_ids(&storage, &project, &path, schema.clone(), 8).await;
        assert_eq!(ids1, vec![8], "Eq(id,8) returns exactly that row");
        let after_second = storage.page_cache().unwrap().counters();
        assert!(
            after_second.hits > after_first.hits,
            "second fresh key must hit the shared unfiltered entry (hits {} -> {})",
            after_first.hits,
            after_second.hits,
        );

        let ids_none = read_eq_ids(&storage, &project, &path, schema.clone(), 9999).await;
        assert!(ids_none.is_empty(), "absent key returns no rows");

        // ───────────── 2. size cap bypasses (tiny budget) ──────────────────
        let storage_small = Storage::new(StorageConfig {
            object_store: Arc::new(InMemory::new()),
            root_prefix: None,
            disk_cache: None,
            // 16-byte budget -> per_shard_budget is tiny; any real file's
            // `size_bytes * EXPANSION` exceeds it, so the gate trips.
            page_cache: Some(crate::PageCacheConfig::new(16)),
        });
        let table_s = TableName::new("big").unwrap();
        let path_s =
            write_vortex(&storage_small, &project, &table_s, &pk_batch(0, 12), Some(4)).await;
        let ids_s = read_eq_ids(&storage_small, &project, &path_s, schema.clone(), 7).await;
        assert_eq!(ids_s, vec![7], "result still correct on the bypass path");
        assert!(
            storage_small
                .page_cache()
                .unwrap()
                .get(&unfiltered_key_for(&path_s))
                .is_none(),
            "size-capped file must NOT populate the shared unfiltered entry"
        );

        // ───────────── 3. kill switch reverts to pushdown ──────────────────
        std::env::set_var(key_env, "1");
        let storage_ks = paged_storage(64 * 1024 * 1024);
        let table_k = TableName::new("ks").unwrap();
        let path_k =
            write_vortex(&storage_ks, &project, &table_k, &pk_batch(0, 12), Some(4)).await;
        let ids_k = read_eq_ids(&storage_ks, &project, &path_k, schema.clone(), 6).await;
        assert_eq!(ids_k, vec![6], "result correct with kill switch on");
        assert!(
            storage_ks
                .page_cache()
                .unwrap()
                .get(&unfiltered_key_for(&path_k))
                .is_none(),
            "kill switch must suppress the shared unfiltered entry"
        );

        // ───────────── 4. parity: unfiltered+Arrow == pushdown ─────────────
        // Two independent stores so cache state can't bleed between paths.
        let batch = pk_batch(100, 12);
        let storage_unf = paged_storage(64 * 1024 * 1024);
        let storage_push = paged_storage(64 * 1024 * 1024);
        let table_p = TableName::new("par").unwrap();

        std::env::remove_var(key_env);
        let path_unf =
            write_vortex(&storage_unf, &project, &table_p, &batch, Some(4)).await;
        // The pushdown-path file is written ONCE under the kill switch; the
        // env var only affects the READ path, so writing it here is fine.
        std::env::set_var(key_env, "1");
        let path_push =
            write_vortex(&storage_push, &project, &table_p, &batch, Some(4)).await;
        std::env::remove_var(key_env);

        // Keys: present-with-note, present-NULL-note (id%3==0 → 100/103/106/109),
        // and absent.
        for eq in [100i64, 103, 106, 109, 105, 99999] {
            std::env::remove_var(key_env);
            let cold = read_eq_ids(&storage_unf, &project, &path_unf, schema.clone(), eq).await;
            let warm = read_eq_ids(&storage_unf, &project, &path_unf, schema.clone(), eq).await;

            std::env::set_var(key_env, "1");
            let pushed =
                read_eq_ids(&storage_push, &project, &path_push, schema.clone(), eq).await;
            std::env::remove_var(key_env);

            assert_eq!(cold, warm, "cold vs warm disagree for Eq(id,{eq})");
            assert_eq!(cold, pushed, "unfiltered vs pushdown disagree for Eq(id,{eq})");
            let expected: Vec<i64> = if (100..112).contains(&eq) {
                vec![eq]
            } else {
                vec![]
            };
            assert_eq!(cold, expected, "wrong rows for Eq(id,{eq})");
        }

        // Restore the env var to whatever it was before the test.
        match prior {
            Some(v) => std::env::set_var(key_env, v),
            None => std::env::remove_var(key_env),
        }
    }

    // -----------------------------------------------------------------------
    // list_data_files streaming + cap tests
    // -----------------------------------------------------------------------

    /// Helper: build a tiny Storage with an `InMemory` object store and
    /// no encryption / no catalog. Pairs with `write_small` below.
    fn fresh_storage() -> Storage {
        Storage::new(StorageConfig {
            object_store: Arc::new(InMemory::new()),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        })
    }

    fn small_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn small_batch(start: i64, len: usize) -> RecordBatch {
        let ids: Int64Array = (start..start + len as i64).collect();
        RecordBatch::try_new(small_schema(), vec![Arc::new(ids)]).unwrap()
    }

    /// Write N small files into one (project, table). Returns the project
    /// and table so the caller can list them back.
    async fn write_n_files(storage: &Storage, n: usize) -> (ProjectId, TableName) {
        let project = ProjectId::new();
        let table = TableName::new("scl").unwrap();
        let part = PartitionKey::default_key();
        for i in 0..n {
            storage
                .write_batch(&project, &table, &part, &small_batch(i as i64 * 10, 3))
                .await
                .expect("write_batch");
        }
        (project, table)
    }

    /// The streaming variant must yield the same set of files (as a set, not
    /// necessarily the same order) as the Vec wrapper. The wrapper is now
    /// implemented in terms of the stream, so this also guards against any
    /// future drift.
    #[tokio::test]
    async fn list_data_files_stream_matches_vec() {
        let storage = fresh_storage();
        let (project, table) = write_n_files(&storage, 5).await;

        let vec_paths: Vec<String> = list_data_files(&storage, &project, &table)
            .await
            .unwrap()
            .into_iter()
            .map(|f| f.path.as_ref().to_string())
            .collect();

        let mut stream_paths: Vec<String> = list_data_files_stream(&storage, &project, &table)
            .map(|r| r.unwrap().path.as_ref().to_string())
            .collect()
            .await;

        let mut sorted_vec = vec_paths.clone();
        sorted_vec.sort();
        stream_paths.sort();
        assert_eq!(
            sorted_vec, stream_paths,
            "stream and Vec must enumerate the same files",
        );
        assert_eq!(vec_paths.len(), 5);
    }

    /// A streaming consumer that takes only `LIMIT` items must not be forced
    /// to walk the entire listing. We can't directly observe "did the
    /// remaining LIST fetches fire" with `InMemory`, but we can at least
    /// confirm the API supports early termination (the underlying object
    /// store's stream is dropped when the consumer drops, which is the
    /// short-circuit the cliff fix relies on).
    #[tokio::test]
    async fn list_data_files_stream_supports_early_termination() {
        let storage = fresh_storage();
        let (project, table) = write_n_files(&storage, 5).await;

        let first_two: Vec<DataFile> = list_data_files_stream(&storage, &project, &table)
            .take(2)
            .map(|r| r.unwrap())
            .collect()
            .await;
        assert_eq!(first_two.len(), 2, "take(2) must yield exactly 2");
    }

    /// Combined coverage of the safety-net cap on `list_data_files_with_stats`:
    ///
    /// - Default cap is 50_000 (matches the audit recommendation).
    /// - `BASIN_STORAGE_MAX_LISTED_FILES` overrides on positive parses.
    /// - Zero / unparseable values fall back to default.
    /// - When the listing exceeds the cap, the call returns the typed
    ///   `BasinError::QueryCostExceeded` rather than walking the footers.
    /// - When the cap is raised above the listing, the call succeeds.
    ///
    /// Merged into a single test (rather than split) because the env var
    /// is process-global and parallel `#[test]` execution would race.
    #[tokio::test]
    async fn list_data_files_with_stats_cap_resolver_and_safety_net() {
        let key = "BASIN_STORAGE_MAX_LISTED_FILES";
        let prior = std::env::var(key).ok();

        // --- resolver: default ---
        std::env::remove_var(key);
        assert_eq!(resolve_max_listed_files(), 50_000);
        assert_eq!(DEFAULT_MAX_LISTED_FILES, 50_000);

        // --- resolver: positive override ---
        std::env::set_var(key, "7");
        assert_eq!(resolve_max_listed_files(), 7);

        // --- resolver: zero falls back ---
        std::env::set_var(key, "0");
        assert_eq!(resolve_max_listed_files(), DEFAULT_MAX_LISTED_FILES);

        // --- resolver: garbage falls back ---
        std::env::set_var(key, "garbage");
        assert_eq!(resolve_max_listed_files(), DEFAULT_MAX_LISTED_FILES);

        // --- safety-net: cap exceeded → typed error ---
        let storage = fresh_storage();
        let (project, table) = write_n_files(&storage, 4).await;
        std::env::set_var(key, "2");
        let err = list_data_files_with_stats(&storage, &project, &table)
            .await
            .expect_err("cap-exceeded must return Err");
        assert!(
            matches!(err, BasinError::QueryCostExceeded(_)),
            "cap-exceeded must surface as QueryCostExceeded, got {err:?}",
        );

        // --- safety-net: under cap → success, listing complete ---
        std::env::set_var(key, "100");
        let ok = list_data_files_with_stats(&storage, &project, &table)
            .await
            .expect("under-cap call must succeed");
        assert_eq!(ok.len(), 4);

        // Restore.
        match prior {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
    }
}
