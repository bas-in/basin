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
use crate::page_cache::{hash_filters, hash_projection, CacheKey, PageCache, RowGatedGet};
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

/// Compressed-→-decoded expansion factor used as the a-priori pre-check for
/// the shared unfiltered-decode reuse path: we multiply the on-disk
/// (compressed) blob size by this factor and require it to fit one page-cache
/// shard's budget (`PageCache::per_shard_budget()`) before speculatively
/// decoding the file unfiltered.
///
/// The factor is deliberately MODEST (not the old 8×). Real expansion ratios
/// measured on Basin data run the gamut — a narrow all-Int64 table decodes
/// ~180× its tiny compressed footprint (still kilobytes, trivially cacheable),
/// while a payload-heavy JSONB events stripe barely compresses (~1.3×:
/// ~2.7 MB compressed → ~3.4 MB decoded, which fits a 4 MiB shard). The old 8×
/// over-estimated the events case (2.7 MB × 8 = 21.8 MB ≫ 4 MiB) and wrongly
/// excluded a file that comfortably fits, forcing a full re-decode on every
/// query. A smaller factor admits these big-but-cacheable files; the
/// AUTHORITATIVE fits-in-a-shard guard is then re-checked against the REAL
/// decoded size right before insert (see the store-after-decode logic in
/// [`read_one`]), so a rare under-estimate never thrashes a shard — it simply
/// declines to cache (the decode already in hand still answers the query).
/// A file that fails even this modest a-priori gate is genuinely large and
/// keeps the pushdown decode path (Vortex native chunk pruning), so big
/// analytical scans are unaffected.
const VORTEX_UNFILTERED_DECODE_EXPANSION: u64 = 2;

/// Serve-side row ceiling for answering a FILTERED read from the shared
/// unfiltered-decode cache entry. Override with
/// `BASIN_UNFILTERED_SERVE_MAX_ROWS` (positive integer); any unset /
/// unparseable / zero value keeps this default.
///
/// Serving a selective read from the unfiltered cache means vectorized-
/// filtering EVERY cached row per query. That only beats the alternative
/// when the alternative is a full re-decode — it loses badly to the
/// zone-map-pruned selective decode on large files (measured: a 1M-row
/// LocalFS point read costs ~23 ms via the cached-then-filter path vs
/// ~0.8 ms via GET + pruned decode of the 1–2 surviving chunks). The
/// admission/populate side is NOT gated by this — a big unfiltered entry
/// is still cached for later full scans (its home turf); this ceiling
/// only decides whether a *filtered* read is allowed to be answered from
/// it. 65 536 rows ≈ one row group / decode chunk: filtering that many
/// in-memory rows costs ~1 ms worst case (the measured 1M-row filter is
/// ~20 ms, so 1M/16 ≈ 65k ≈ 1.3 ms), the same order as the pruned
/// selective path it displaces — the break-even point.
pub(crate) const DEFAULT_UNFILTERED_SERVE_MAX_ROWS: u64 = 65_536;

/// Resolve [`DEFAULT_UNFILTERED_SERVE_MAX_ROWS`], honoring
/// `BASIN_UNFILTERED_SERVE_MAX_ROWS` when present and parseable to a
/// positive `u64`.
fn resolve_unfiltered_serve_max_rows() -> u64 {
    if let Ok(v) = std::env::var("BASIN_UNFILTERED_SERVE_MAX_ROWS") {
        if let Ok(n) = v.parse::<u64>() {
            if n > 0 {
                return n;
            }
        }
    }
    DEFAULT_UNFILTERED_SERVE_MAX_ROWS
}

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
    // Catalog-driven fast path: when a catalog is attached it already holds
    // the authoritative live file set (path + size + row_count +
    // column_stats), so we discover the table's files with ZERO object-store
    // LIST RPCs. On a warm, RAM-resident table this collapses the per-query
    // 2× LIST round-trip (hot `data/` + `cold/` prefixes) that dominated the
    // scan/write latency floor on high-RTT object stores. Both the
    // catalog and the LIST enumerate the same physical files (the catalog is
    // updated transactionally at write-commit / compaction), so this is a
    // strict round-trip elimination, not a visibility change.
    //
    // Falls back to the object-store LIST when no catalog is attached or the
    // table is not catalog-known (schema-less callers, integration tests).
    if let Some(files) = storage.catalog_live_data_files(project, table).await {
        return Ok(files);
    }

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
    // exists so a runaway project can't burst the per-project memory
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
    let stats_cache = storage.data_file_stats_cache().clone();

    // LEVER 1 / A4: catalog-persisted file stats. The catalog already records
    // `(row_count, column_stats)` per file at write-commit time (A4), in the
    // same byte shape this function would otherwise extract from the footer.
    // Seed those in up front so a file the catalog already knows about NEVER
    // pays the footer GET — the dominant cold-path round-trip on an S3
    // backend (~10 ms RTT/file). Files the catalog has no stats for (written
    // pre-A4, or whose `column_stats` is empty) are simply absent from this
    // map and fall through to the footer path below — a strict optimisation,
    // never a correctness regression. We also warm the in-RAM stats cache so
    // a same-process repeat listing of a pre-A4 file (footer-fetched once)
    // still benefits, and a catalog-known file needs no catalog round-trip on
    // the warm path either.
    let catalog_stats = storage.catalog_file_stats(project, table).await;

    // Stats-cache fast path. `(row_count, column_stats)` is what every caller
    // of this function actually consumes; once we've extracted it for a file
    // (write-once, immutable) we never need to touch the file's bytes again.
    // A warm hit therefore turns the per-query footer GET+parse into an
    // in-RAM lookup for BOTH Parquet and Vortex — collapsing the dominant
    // per-query cost on a many-file table. Misses fall through to the
    // format-specific footer paths below and populate the cache.
    let mut needs_stats: Vec<usize> = Vec::with_capacity(files.len());
    for (i, f) in files.iter_mut().enumerate() {
        if let Some(cached) = stats_cache.get(&f.path, f.size_bytes) {
            let (rows, stats) = cached.as_ref();
            f.row_count = *rows;
            f.column_stats = stats.clone();
        } else if let Some((rows, stats)) = catalog_stats.get(f.path.as_ref()) {
            // Catalog already carries this file's stats — skip the footer GET.
            f.row_count = *rows;
            f.column_stats = stats.clone();
            stats_cache.insert(
                f.path.clone(),
                f.size_bytes,
                Arc::new((*rows, stats.clone())),
            );
        } else {
            needs_stats.push(i);
        }
    }

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
    let needs: std::collections::HashSet<usize> = needs_stats.iter().copied().collect();
    let work: Vec<_> = files
        .iter()
        .enumerate()
        .filter(|(i, f)| needs.contains(i) && f.path.as_ref().ends_with(".parquet"))
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

    // 32 concurrent GETs: footer reads are IO-bound remote requests;
    // higher concurrency hides per-request RTT with no added CPU cost.
    let resolved: Vec<Result<(usize, (u64, BTreeMap<String, ColumnStats>))>> =
        futures::stream::iter(work)
            .buffer_unordered(32)
            .collect()
            .await;

    for r in resolved {
        let (i, (rows, stats)) = r?;
        files[i].row_count = rows;
        files[i].column_stats = stats;
        stats_cache.insert(
            files[i].path.clone(),
            files[i].size_bytes,
            Arc::new((files[i].row_count, files[i].column_stats.clone())),
        );
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
        .filter(|(i, f)| needs.contains(i) && f.path.as_ref().ends_with(".vortex"))
        .map(|(i, _)| i)
        .collect();
    if !vortex_idxs.is_empty() {
        type VortexStat = (usize, Option<u64>, BTreeMap<String, ColumnStats>);
        let vwork: Vec<_> = vortex_idxs
            .into_iter()
            .map(|i| {
                let store = store.clone();
                let path = files[i].path.clone();
                // Size from the LIST result — no HEAD; feeds the tail reader so
                // it skips the implicit `size()` round-trip too.
                let listed_size = files[i].size_bytes;
                async move {
                    // LEVER 2: read the Vortex footer/stats via TAIL RANGE GETs
                    // rather than a full-file GET. Vortex footers live at the
                    // tail (postscript + EOF + layout/dtype/file-statistics
                    // segments), so `footer_meta_from_store` opens the file over
                    // an object-store-backed `VortexReadAt` that fetches only
                    // ~64 KiB from the end plus the small bounded footer
                    // segments — never the data. Same `(row_count, stats)` byte
                    // contract as the old full-file `footer_meta`; the
                    // Vortex⇆Parquet differential harness gates correctness.
                    // Best-effort: any error leaves the listing defaults, no
                    // worse than the previous full-GET path.
                    let (rc, stats) = match crate::vortex_format::footer_meta_from_store(
                        store,
                        &path,
                        listed_size,
                    )
                    .await
                    {
                        Ok((n, s)) => (Some(n), s),
                        Err(_) => (None, BTreeMap::new()),
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
            // Only cache a SUCCESSFUL footer read (we got a row_count). A
            // best-effort failure left the listing defaults, which we must
            // not memoise — a later query (e.g. once an encryption provider
            // is attached, or transient GET error clears) should retry.
            if rc.is_some() {
                stats_cache.insert(
                    files[i].path.clone(),
                    files[i].size_bytes,
                    Arc::new((files[i].row_count, files[i].column_stats.clone())),
                );
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
    // Stats-pruning is enabled whenever the request carries any predicate.
    // When the catalog is attached we prune against the authoritative
    // catalog schema; when it is NOT (schema-less callers: integration
    // tests, sources that drive `Storage::read` without a catalog walk) we
    // synthesise a pruning schema from the predicates' own scalar types
    // (`filters_to_prune_schema`). Both feed the same
    // `evaluate_compound_for_pruning`, which only ever DROPS a file it can
    // PROVE is `NoMatch` from per-file min/max — a wrong/absent type maps
    // to the `_ => Mixed` arm and keeps the file, so the synthesised schema
    // can never produce a false `NoMatch`. This lifts the per-file
    // open+decode cost off every point query on a many-file table even
    // without a catalog (previously the no-catalog branch opened ALL files).
    let prune_with_stats = !opts.filters.is_empty();
    let paths: Vec<ObjectPath> = if prune_with_stats {
        let schema = match catalog_schema.as_ref() {
            Some(s) => s.as_ref().clone(),
            None => filters_to_prune_schema(&opts.filters),
        };
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
    } else if let Some(files) = storage.catalog_live_data_files(project, table).await {
        // Catalog-driven fast path (no predicate): the catalog already knows
        // the live file set, so a warm unfiltered scan makes ZERO LIST RPCs.
        // The catalog never records files outside the project prefix; the
        // suffix filter already ran inside `catalog_live_data_files`.
        files.into_iter().map(|f| f.path).collect()
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

/// Synthesise a minimal Arrow schema for stats-pruning from the predicate
/// list alone, used when no catalog schema is attached. Each predicated
/// column gets one field whose `DataType` is inferred from the predicate's
/// own `ScalarValue` (the type the comparison literal carries).
///
/// This is sound because `prune_atom` only acts on `(DataType, ScalarValue)`
/// pairs it recognises and falls through to `Mixed` (keep the file) for any
/// other combination, and the stats bytes are decoded with the SAME type
/// that selects the arm — so a column whose physical type differs from the
/// inferred one yields at worst a `Mixed` (no prune), never a false
/// `NoMatch`. `StartsWith` carries no scalar; it prunes on the column's
/// byte-lexicographic stats, so we type it as `Utf8`.
fn filters_to_prune_schema(filters: &[Predicate]) -> Schema {
    use arrow_schema::DataType;
    let mut fields: Vec<Field> = Vec::new();
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for f in filters {
        let col = f.column();
        if !seen.insert(col) {
            continue;
        }
        let dt = match f {
            Predicate::Eq(_, v) | Predicate::Gt(_, v) | Predicate::Lt(_, v) => match v {
                ScalarValue::Int64(_) => DataType::Int64,
                ScalarValue::Float64(_) => DataType::Float64,
                ScalarValue::Boolean(_) => DataType::Boolean,
                ScalarValue::Utf8(_) => DataType::Utf8,
                // No stats-pruning arm for UInt64 today (writer/footer record
                // no UInt64-typed stats); keep the file by typing it as a
                // value `prune_atom` won't match.
                ScalarValue::UInt64(_) => DataType::UInt64,
            },
            Predicate::StartsWith { .. } => DataType::Utf8,
            // InInt64 targets an Int64-family column; type it Int64 so the
            // schema is coherent (the membership atom itself prunes to Mixed).
            Predicate::InInt64(..) => DataType::Int64,
        };
        // Nullable=true is conservative: it never enables an unsafe prune
        // (null-count from stats is what `prune_atom` actually consults).
        fields.push(Field::new(col, dt, true));
    }
    Schema::new(fields)
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
    // Page-cache key: file path + projection + filters + representation
    // stamp. The parquet reader pushes filters all the way down
    // (with_row_filter + with_row_selection), so the cached
    // `Vec<RecordBatch>` is post-filter; sharing entries across queries
    // with different WHEREs would return wrong rows. The stamp keys
    // catalog-aware reads (which cache the post-restamp canonical
    // representation: BASIN_TYPE metadata present, ADR 0024 UUID/POINT
    // columns restored to FixedSizeBinary) apart from schema-less reads
    // (which cache the raw physical decode) — the generic hit below
    // serves entries VERBATIM, so the two representations must never
    // share an entry. See `CacheKey` docs for both constraints.
    let cache_key = page_cache.as_ref().map(|_| CacheKey {
        path: path.clone(),
        projection_hash: hash_projection(opts.projection.as_deref()),
        filters_hash: hash_filters(&opts.filters),
        stamped: catalog_schema.is_some(),
    });
    if let (Some(pc), Some(key)) = (page_cache.as_ref(), cache_key.as_ref()) {
        if let Some(batches) = pc.get(key) {
            // Cache hit: no object-store GET, no decode. Counts as a
            // cache-served file (NOT `files_opened`), and contributes 0 to
            // `rows_decoded` — the batches were materialized on the cold read
            // that filled this entry, not here. This is what lets the
            // repeated-key point SELECT gate assert files_opened == 0.
            counters
                .files_served_from_cache
                .fetch_add(1, Ordering::Relaxed);
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
        //
        // SERVE-SIDE ROW GATE: answering a filtered read from this entry means
        // vectorized-filtering EVERY cached row, which only wins when the
        // alternative is a full re-decode. On a large file the alternative is
        // the zone-map-PRUNED selective decode (1–2 surviving chunks, ~sub-ms),
        // which beats filtering a 1M-row cached decode (~20 ms) by >20×. So a
        // filtered read is served from the entry only when its total row count
        // is at most `BASIN_UNFILTERED_SERVE_MAX_ROWS` (default 65 536 ≈ one
        // chunk's worth, ~1 ms filter worst case). The decision is O(1): the
        // row count was computed once at insert and the gated lookup is a
        // single non-promoting peek + integer compare — no decode, no batch
        // clone, no extra locks on the decline path. Unfiltered reads are
        // untouched (they never enter this block) and keep being served from
        // the entry at ANY size via the generic page-cache hit above.
        let unfiltered_serve_max_rows = resolve_unfiltered_serve_max_rows();
        // Set when the shared unfiltered entry EXISTS but exceeds the serve
        // row ceiling. The post-GET reuse branch is then skipped wholesale —
        // its gated lookup would just decline again (and its miss arm must NOT
        // re-decode + re-insert an entry that is already cached) — so the read
        // takes the pushdown decode with native zone-map chunk pruning.
        let mut unfiltered_serve_declined = false;
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
                        // This block is gated on `catalog_schema.is_some()`,
                        // so the entry probed is the post-restamp one.
                        stamped: true,
                    };
                    match pc.get_if_rows_le(&unfiltered_key, unfiltered_serve_max_rows) {
                        RowGatedGet::Serve(cached) => {
                            // Pre-GET cache hit: the unfiltered decode is already
                            // warm, so we answer this fresh-key point read by
                            // Arrow-filtering the cached batches and NEVER issue the
                            // GET. Cache-served (not `files_opened`); `rows_decoded`
                            // stays 0 because no new Arrow materialization happens —
                            // the rows were decoded on the cold read that filled the
                            // entry. This is the path that makes the repeated /
                            // second in-list point read provably 0 cold work.
                            counters
                                .files_served_from_cache
                                .fetch_add(1, Ordering::Relaxed);
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
                        RowGatedGet::Decline => {
                            unfiltered_serve_declined = true;
                        }
                        RowGatedGet::Absent => {}
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

        // Cold read: we issued the GET and now hold the file bytes. Count the
        // file opened + the GET bytes ONCE here, before any decode-reuse
        // branch below (a post-GET unfiltered-reuse hit still paid this GET).
        // `files_opened` is the per-query "cold files touched" signal the
        // scale-invariant gates pin to O(1): whole-file column_stats min/max
        // pruning in `read()` should have already dropped every file whose PK
        // zone-map excludes the lookup key, so a point read reaches this line
        // for at most the one (or boundary-straddling two) surviving file(s).
        counters.files_opened.fetch_add(1, Ordering::Relaxed);
        counters
            .bytes_fetched
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
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
        // Compute filter pushability once (reused for the pushdown path below).
        // When every predicate pushes into Vortex natively, the pushdown decode
        // gets chunk-level zone-map pruning — so for a LARGE pushable-filter
        // file we must NOT divert to the unfiltered whole-file decode (it would
        // throw that pruning away). When the filter is NOT pushable (e.g. a
        // Utf8 `status = '…'` predicate), the pushdown path decodes the whole
        // file anyway, so the unfiltered reuse path is strictly better whenever
        // the decode can be cached.
        let (push_filter, all_filters_pushed) =
            vortex_filter_expr(&opts.filters, filter_schema);
        // A-priori expansion estimate of the unfiltered decode.
        let est_decode = size_bytes.saturating_mul(VORTEX_UNFILTERED_DECODE_EXPANSION);
        let per_shard = page_cache
            .as_ref()
            .map(|pc| pc.per_shard_budget())
            .unwrap_or(0);
        let total = page_cache
            .as_ref()
            .map(|pc| pc.total_budget())
            .unwrap_or(0);
        // Eligibility budget:
        //   * pushable filter → must fit one shard a priori (modest factor);
        //     a larger file keeps pushdown + native chunk pruning.
        //   * non-pushable filter → pushdown gains nothing, so admit up to the
        //     whole-cache upper bound and let the post-decode real-size check
        //     decide whether to actually cache (a payload-heavy events stripe
        //     whose compressed size mis-predicts its true ~1.3× decode lands
        //     here and IS cached because the real decode fits one shard).
        let est_ceiling = if all_filters_pushed { per_shard } else { total };
        // `!unfiltered_serve_declined`: the pre-GET probe already found the
        // shared entry present-but-over-the-serve-row-ceiling, so don't pay a
        // second lookup that would decline again — go straight to the pruned
        // pushdown decode below.
        let unfiltered_eligible = !opts.filters.is_empty()
            && !unfiltered_cache_disabled
            && !unfiltered_serve_declined
            && page_cache.is_some()
            && est_decode <= est_ceiling;

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
                // Match the representation of this read's own cold path:
                // a catalog-aware probe must only serve the post-restamp
                // entry, a schema-less probe only the raw one (the shared
                // `finish` tail restamps under the SAME catalog_schema, so
                // each class reproduces exactly its cold-path output).
                stamped: catalog_schema.is_some(),
            };
            let pc = page_cache.as_ref().expect("eligibility implies page cache");

            // Shared tail for both served arms below: apply the authoritative
            // predicate + projection in Arrow over the (shared) unfiltered
            // batches. `apply_filter = true` always: the cached batches are
            // unfiltered, so the WHERE must run here. The early per-call LIMIT
            // cut (mirrors the pushdown path below) is folded INTO the filter
            // pass so projection + restamp touch at most `limit` rows; this
            // raw-batch path never write-throughs the post-filter result, so
            // an early cut is sound.
            let finish =
                |raw: Vec<RecordBatch>| -> Result<BoxStream<'static, Result<RecordBatch>>> {
                    let batches = vortex_project_and_filter_limited(
                        raw,
                        opts.as_ref(),
                        catalog_schema.as_ref(),
                        true,
                        opts.limit,
                    )?;
                    Ok(futures::stream::iter(batches.into_iter().map(Ok)).boxed())
                };

            // Raw unfiltered decode batches (read-projection columns, no
            // predicate applied). A FILTERED read is a serve-only consumer of
            // this entry: Serve (present and under the row ceiling) → filter
            // the cached Arc. Absent or Decline → fall out of this branch to
            // the zone-map-pruned pushdown decode below. Filtered reads must
            // NEVER populate this entry by doing the whole-file unfiltered
            // decode as a side effect: measured on a 1M-row file that decode
            // costs ~20ms per query, and an entry over the shard budget is
            // never admitted, so every query repeats it — collapsing point-
            // read throughput ~30x vs the pruned path (~0.8ms). The entry is
            // populated by actual unfiltered scans (a no-projection full
            // scan's per-filter cache key with no filters IS this key), where
            // the whole-file decode genuinely is the read.
            match pc.get_if_rows_le(&unfiltered_key, unfiltered_serve_max_rows) {
                RowGatedGet::Serve(cached) => {
                    return finish(cached.iter().map(|b| (**b).clone()).collect());
                }
                RowGatedGet::Absent | RowGatedGet::Decline => {
                    // Take the pruned pushdown decode below.
                }
            }
        }

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
        // Rows materialized into Arrow from this cold file, BEFORE the Arrow
        // project/filter pass below. When a filter pushed natively, Vortex's
        // zone-map chunk pruning already dropped non-matching chunks, so this
        // is the rows in the SURVIVING chunks (≤ one chunk for a point read on
        // a PK whose values are chunk-local) — exactly the decode work the
        // gate bounds. When nothing pushed it is the whole file; the gate's
        // per-file chunk bound still holds because each seeded file is one
        // chunk.
        let decoded_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        counters
            .rows_decoded
            .fetch_add(decoded_rows, Ordering::Relaxed);
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
    // Cold Parquet read: footer fetched (or rehydrated from meta cache) and a
    // record-batch stream about to be built. Count the file opened once.
    // `bytes_fetched` uses `file_size` as an upper bound (the parquet reader
    // issues ranged GETs for only the surviving row groups + pages, so the real
    // wire bytes are typically a strict subset — same caveat as the per-project
    // counter above). `rows_decoded` for the Parquet path is bumped inside
    // `finalize_pipeline`, where the record-batch stream is wrapped, because
    // the stream is lazy and rows materialize as it is polled.
    counters.files_opened.fetch_add(1, Ordering::Relaxed);
    counters
        .bytes_fetched
        .fetch_add(file_size, Ordering::Relaxed);
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
        // IN-list membership cannot be excluded from a single [min,max] row
        // group (overlap with the key range does not imply a hit, and vice
        // versa). Never exclude — the engine pushes a separate range predicate
        // for row-group pruning; this atom keeps every overlapping group.
        Predicate::InInt64(..) => return false,
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

/// Build a Parquet [`RowSelection`] for a row-tier GIN allowlist.
///
/// `row_offsets` is the ascending, deduplicated list of ABSOLUTE row indices
/// (file-relative, as the GIN row tier stored them) that may match. `kept` is
/// the list of surviving row-group ordinals (after the row-group / stats prune)
/// in ascending order — the exact set passed to `with_row_groups`. The returned
/// `RowSelection` is expressed in the SAME coordinate space parquet-rs applies
/// it: the concatenation of the kept row groups' rows, in `kept` order. A row
/// whose absolute index is NOT in `row_offsets` (or lies in a row group that is
/// being kept) is skipped; rows outside the kept row groups are implicitly
/// dropped by `with_row_groups` and never enter this selection.
///
/// Correctness: the selection is a SUPERSET filter. `row_offsets` is itself a
/// superset of true matches (the GIN row tier is raw-bytes containment), and
/// this routine only ever turns offsets in `row_offsets` into `select` runs —
/// it never selects a row absent from `row_offsets`, and never skips a row
/// present in it (within a kept row group). The per-row `RowFilter` re-checks
/// every surviving row, so any false positive is filtered out and no true
/// match is ever dropped.
///
/// Returns `None` when nothing can be narrowed (empty `kept`, or every kept row
/// is selected anyway) so the caller skips installing a no-op selection.
fn build_row_tier_selection(
    metadata: &parquet::file::metadata::ParquetMetaData,
    kept_row_groups: &[usize],
    row_offsets: &[u64],
) -> Option<(RowSelection, u64)> {
    if kept_row_groups.is_empty() {
        return None;
    }
    // Absolute row offsets of each row group's first row (cumulative sum over
    // ALL row groups, since the GIN offsets are file-absolute).
    let all_rgs = metadata.row_groups();
    let mut rg_start: Vec<u64> = Vec::with_capacity(all_rgs.len());
    let mut acc = 0u64;
    for rg in all_rgs {
        rg_start.push(acc);
        acc += rg.num_rows() as u64;
    }

    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut total_selected: u64 = 0;
    let mut any_skipped = false;
    // `row_offsets` is ascending; walk it with a cursor so each kept row group
    // is matched in O(group_rows + offsets_in_group).
    let mut oi = 0usize;
    for &rg_idx in kept_row_groups {
        let start = rg_start[rg_idx];
        let n_rows = all_rgs[rg_idx].num_rows() as u64;
        let end = start + n_rows;
        // Advance the cursor to the first offset >= start (skip offsets that
        // fall before this kept row group — they belong to pruned groups and
        // are not part of the selection coordinate space).
        while oi < row_offsets.len() && row_offsets[oi] < start {
            oi += 1;
        }
        // Walk the rows of this row group, emitting select/skip runs.
        let mut pos = start;
        while pos < end {
            if oi < row_offsets.len() && row_offsets[oi] < end {
                let next = row_offsets[oi];
                if next > pos {
                    // Skip the gap [pos, next).
                    let gap = (next - pos) as usize;
                    selectors.push(RowSelector::skip(gap));
                    any_skipped = true;
                    pos = next;
                }
                // Coalesce a contiguous run of selected offsets.
                let run_start = pos;
                while oi < row_offsets.len()
                    && row_offsets[oi] < end
                    && row_offsets[oi] == pos
                {
                    pos += 1;
                    oi += 1;
                }
                let run = (pos - run_start) as usize;
                if run > 0 {
                    selectors.push(RowSelector::select(run));
                    total_selected += run as u64;
                }
            } else {
                // No more offsets in this row group — skip the remainder.
                let gap = (end - pos) as usize;
                selectors.push(RowSelector::skip(gap));
                any_skipped = true;
                pos = end;
            }
        }
    }

    if !any_skipped {
        // Every kept row was selected — installing a selection is pure
        // overhead. Behave like the page-selection path's no-op guard.
        return None;
    }
    Some((RowSelection::from(selectors), total_selected))
}

/// Look up the row-tier offset allowlist for `path` in the read options.
/// Returns `None` when no row selection is set or the file is absent (a file
/// without a row-tier entry decodes every surviving row — the safe default).
fn row_tier_offsets_for_file<'a>(
    opts: &'a ReadOptions,
    path: &ObjectPath,
) -> Option<&'a Vec<u64>> {
    opts.row_selection.as_ref()?.get(path.as_ref())
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

                // Row-tier GIN selection (if any) for this file, expressed in
                // the kept-row-group coordinate space, then intersected with
                // the page-stats selection so both pruning sources compose.
                let row_tier_selection = row_tier_offsets_for_file(opts.as_ref(), &path)
                    .and_then(|offs| build_row_tier_selection(builder.metadata(), &kept, offs));
                if let Some((_, rows_sel)) = &row_tier_selection {
                    counters
                        .rows_selected_by_page_index
                        .fetch_add(*rows_sel, Ordering::Relaxed);
                }
                let combined_selection = match (page_selection, row_tier_selection) {
                    (Some((p, _)), Some((r, _))) => Some(p.intersection(&r)),
                    (Some((p, _)), None) => Some(p),
                    (None, Some((r, _))) => Some(r),
                    (None, None) => None,
                };

                let mut builder = builder.with_row_groups(kept);
                if let Some(sel) = combined_selection {
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
                let counters_for_rows = counters.clone();
                let mapped = stream.map(move |res| {
                    let batch =
                        res.map_err(|e| BasinError::storage(format!("parquet read: {e}")))?;
                    // Rows yielded by the parquet stream — post row-group / page
                    // / row-filter pushdown, pre-engine-filter. This is the
                    // Parquet analogue of the Vortex `rows_decoded` bump: the
                    // volume actually materialized off disk for this file.
                    counters_for_rows
                        .rows_decoded
                        .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
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

    // Row-tier GIN selection (if any) for this file, intersected with the
    // page-stats selection (same kept-row-group coordinate space).
    let row_tier_selection = row_tier_offsets_for_file(opts.as_ref(), &path)
        .and_then(|offs| build_row_tier_selection(builder.metadata(), &kept, offs));
    if let Some((_, rows_sel)) = &row_tier_selection {
        counters
            .rows_selected_by_page_index
            .fetch_add(*rows_sel, Ordering::Relaxed);
    }
    let combined_selection = match (page_selection, row_tier_selection) {
        (Some((p, _)), Some((r, _))) => Some(p.intersection(&r)),
        (Some((p, _)), None) => Some(p),
        (None, Some((r, _))) => Some(r),
        (None, None) => None,
    };

    let mut builder = builder.with_row_groups(kept);
    if let Some(sel) = combined_selection {
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
    let counters_for_rows = counters.clone();
    let mapped = stream.map(move |res| {
        let batch = res.map_err(|e| BasinError::storage(format!("parquet read: {e}")))?;
        // Rows yielded by the parquet stream — post row-group / page / row
        // filter pushdown, pre-engine-filter. Parquet analogue of the Vortex
        // `rows_decoded` bump.
        counters_for_rows
            .rows_decoded
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
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

/// Process-level kill switch for the sorted-key skip. `BASIN_SORTED_SKIP_DISABLE=1`
/// (or `true`) reverts every `InInt64`-on-sort-column read to the plain
/// vectorized-filter path.
fn sorted_skip_disabled() -> bool {
    std::env::var("BASIN_SORTED_SKIP_DISABLE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Per-call plan for the sorted-key skip. Holds the index of the single
/// `InInt64` filter that targets `opts.sorted_by`, plus the (sorted, dedup)
/// key vector. `None` means the optimisation does not apply and the caller
/// uses the plain filter path.
struct SortedSkipPlan<'a> {
    /// The sort column name (== `opts.sorted_by`).
    column: &'a str,
    /// ASC-sorted, deduplicated IN-list keys.
    keys: &'a [i64],
    /// Indices into `opts.filters` of every predicate OTHER than the chosen
    /// `InInt64` — these residual filters are re-applied as a mask on the
    /// (small) taken batch so the result is exact, not a superset.
    residual: Vec<usize>,
}

/// Decide whether the sorted-key skip applies for this read. Eligible iff:
///   * the kill switch is OFF,
///   * `opts.sorted_by` names a column,
///   * exactly one `InInt64` filter targets that column (more than one IN on
///     the sort column is not the bench shape — fall back),
///   * its key vector is non-empty.
/// Returns the plan (borrowing into `opts`) or `None`.
fn sorted_skip_plan(opts: &ReadOptions) -> Option<SortedSkipPlan<'_>> {
    if sorted_skip_disabled() {
        return None;
    }
    let sort_col = opts.sorted_by.as_deref()?;
    let mut chosen: Option<(usize, &[i64])> = None;
    for (i, f) in opts.filters.iter().enumerate() {
        if let Predicate::InInt64(c, keys) = f {
            if c == sort_col {
                if chosen.is_some() {
                    // Two IN-lists on the sort column — unusual; bail to the
                    // plain path rather than guess an intersection.
                    return None;
                }
                chosen = Some((i, keys.as_slice()));
            }
        }
    }
    let (idx, keys) = chosen?;
    if keys.is_empty() {
        return None;
    }
    let residual = (0..opts.filters.len()).filter(|&j| j != idx).collect();
    Some(SortedSkipPlan {
        column: sort_col,
        keys,
        residual,
    })
}

/// Execute the sorted-key skip on one decode chunk: binary-search the sort
/// column for the IN-list keys, `take` only the matching rows, then re-apply
/// any residual (non-IN) filters as a mask on the taken batch.
///
/// Falls back to evaluating the `InInt64` predicate as a plain mask on the
/// full batch when the sort column is absent from the batch or is not a
/// widenable integer (e.g. an unexpected type) — correctness is preserved
/// either way.
fn apply_sorted_skip(
    batch: &RecordBatch,
    opts: &ReadOptions,
    plan: &SortedSkipPlan<'_>,
) -> Result<RecordBatch> {
    // Locate the sort column in this batch. If it was projected away or
    // renamed, fall back to a full-batch mask over all filters.
    let taken = match batch.column_by_name(plan.column) {
        Some(col) => match predicate::sorted_in_int64_indices(col, plan.keys) {
            Some(indices) => {
                let idx_arr = arrow_array::UInt32Array::from(indices);
                arrow::compute::take_record_batch(batch, &idx_arr)
                    .map_err(|e| BasinError::storage(format!("sorted-skip take: {e}")))?
            }
            // Column present but not widenable — fall back to the IN mask.
            None => {
                let m = predicate::evaluate(batch, &reconstruct_in_pred(plan))?;
                arrow::compute::filter_record_batch(batch, &m)
                    .map_err(|e| BasinError::storage(format!("sorted-skip in-mask: {e}")))?
            }
        },
        None => {
            let m = predicate::evaluate(batch, &reconstruct_in_pred(plan))?;
            arrow::compute::filter_record_batch(batch, &m)
                .map_err(|e| BasinError::storage(format!("sorted-skip in-mask: {e}")))?
        }
    };

    // Re-apply residual (non-IN) filters on the (small) taken batch so the
    // result is exact. This is cheap: the taken batch has at most `keys.len()`
    // rows, not the chunk's full row count.
    if plan.residual.is_empty() {
        return Ok(taken);
    }
    let mut mask: Option<arrow_array::BooleanArray> = None;
    for &j in &plan.residual {
        let m = predicate::evaluate(&taken, &opts.filters[j])?;
        mask = Some(match mask {
            None => m,
            Some(prev) => arrow::compute::and(&prev, &m)
                .map_err(|e| BasinError::storage(format!("sorted-skip residual AND: {e}")))?,
        });
    }
    match mask {
        Some(m) => arrow::compute::filter_record_batch(&taken, &m)
            .map_err(|e| BasinError::storage(format!("sorted-skip residual: {e}"))),
        None => Ok(taken),
    }
}

/// Rebuild the `InInt64` predicate from a plan for the mask-fallback path.
fn reconstruct_in_pred(plan: &SortedSkipPlan<'_>) -> Predicate {
    Predicate::InInt64(plan.column.to_string(), plan.keys.to_vec())
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

    // Sorted-key skip eligibility (computed once for the whole call): when the
    // filter set carries exactly one `InInt64` on the column named by
    // `opts.sorted_by` (the file's physical ASC sort column), we can serve that
    // predicate by binary-searching each decode chunk and `take`-ing ONLY the
    // matching rows — O(k log n) index probes + materialization of the matches,
    // instead of an O(n) Arrow filter over every wide column. Any *other*
    // residual filters are still applied (as a mask) on the small taken batch.
    // The whole optimisation is reverted by `BASIN_SORTED_SKIP_DISABLE=1`.
    let sorted_skip = if apply_filter { sorted_skip_plan(opts) } else { None };

    for batch in batches {
        if let Some(lim) = limit {
            if seen >= lim {
                break;
            }
        }
        // 1) Filter (predicate columns referenced by name; pre-projection).
        let filtered = if !apply_filter || opts.filters.is_empty() {
            batch
        } else if let Some(plan) = &sorted_skip {
            // ── Sorted-key skip take path ────────────────────────────────────
            // Binary-search this (internally ASC-sorted) chunk for the IN-list
            // keys, take only the matching rows, then apply any residual
            // (non-IN) filters on the much smaller taken batch.
            apply_sorted_skip(&batch, opts, plan)?
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
            // Utf8 string equality → push a Utf8 literal so Vortex's native
            // zone-map (min/max) chunk pruning + selective decode engages.
            // This is the dominant path for promoted-JSONB shadow-column
            // lookups (`__promoted$col$key = '…'`, ADR 0027): without the
            // push the whole Utf8 column decodes per query and the predicate
            // runs only in the Arrow post-filter — a full-column scan at 1M
            // rows. The `like`/StartsWith arm below already pushes Utf8
            // literals safely, so an `eq` Utf8 literal is equally sound; the
            // decode path is fault-tolerant (any dtype-mismatch scan error
            // falls back to a plain full decode, see `decode_with_cache`) and
            // the Arrow post-filter stays active when the push was inferred,
            // so the final row set is byte-identical either way.
            //
            // Only `Utf8` is pushed — `LargeUtf8` / `Utf8View` are left to the
            // post-filter (same conservatism as the StartsWith arm) to avoid a
            // dtype-mismatch in the spawned scan task.
            (ScalarValue::Utf8(s), DataType::Utf8) => Some(lit(s.clone())),
            // Width/type mismatch, other string widths, timestamps, decimals,
            // etc. — do NOT push (would risk a Vortex dtype-compare panic).
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
            // IN-list membership is NOT pushed into the Vortex scan expression
            // (no native multi-literal membership zone-prune that we trust
            // across dtypes). It is evaluated Arrow-side — either via the
            // sorted-key skip take path or the vectorized membership mask in
            // `vortex_project_and_filter_limited`. Mark not-all-pushed so the
            // caller keeps the post-decode filter active.
            Predicate::InInt64(_, _) => {
                all_pushed = false;
                None
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
    use crate::predicate::ScalarValue;
    use crate::writer::{FileFormat, WriteOptions};
    use crate::{ReadOptions, Storage, StorageConfig};

    #[test]
    fn prune_schema_infers_types_from_predicate_scalars() {
        use arrow_schema::DataType;
        let filters = vec![
            Predicate::Eq("id".into(), ScalarValue::Int64(7)),
            Predicate::Gt("score".into(), ScalarValue::Float64(1.5)),
            Predicate::Eq("flag".into(), ScalarValue::Boolean(true)),
            Predicate::Eq("name".into(), ScalarValue::Utf8("x".into())),
            Predicate::StartsWith {
                column: "code".into(),
                prefix: "ab".into(),
                case_insensitive: false,
            },
            // Duplicate column: first type wins, no dup field.
            Predicate::Lt("id".into(), ScalarValue::Int64(99)),
        ];
        let schema = filters_to_prune_schema(&filters);
        let by_name = |n: &str| {
            schema
                .fields()
                .iter()
                .find(|f| f.name() == n)
                .map(|f| f.data_type().clone())
        };
        assert_eq!(by_name("id"), Some(DataType::Int64));
        assert_eq!(by_name("score"), Some(DataType::Float64));
        assert_eq!(by_name("flag"), Some(DataType::Boolean));
        assert_eq!(by_name("name"), Some(DataType::Utf8));
        assert_eq!(by_name("code"), Some(DataType::Utf8));
        // `id` appears once despite two predicates.
        assert_eq!(
            schema.fields().iter().filter(|f| f.name() == "id").count(),
            1
        );
    }

    // ── Sorted-key skip (Predicate::InInt64 + ReadOptions.sorted_by) ────────

    /// Build a multi-chunk batch list whose `id` column is globally ASC-sorted
    /// across chunks AND internally sorted within each chunk (the file shape:
    /// each decode chunk is a sorted slice of the file's sorted PK column).
    fn sorted_chunks() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("payload", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let mk = |ids: Vec<i64>| {
            let id = Int64Array::from(ids.clone());
            let pay = StringArray::from(
                ids.iter().map(|v| Some(format!("p{v}"))).collect::<Vec<_>>(),
            );
            let sc = Float64Array::from(ids.iter().map(|v| Some(*v as f64)).collect::<Vec<_>>());
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(id), Arc::new(pay), Arc::new(sc)],
            )
            .unwrap()
        };
        vec![
            mk(vec![1, 4, 7, 10]),   // chunk 0
            mk(vec![13, 16, 19, 22]), // chunk 1
            mk(vec![25, 28, 31, 34]), // chunk 2
        ]
    }

    fn read_ids(batches: &[RecordBatch]) -> Vec<i64> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;
        let mut out = Vec::new();
        for b in batches {
            let c = b.column_by_name("id").unwrap().as_primitive::<Int64Type>();
            for i in 0..c.len() {
                out.push(c.value(i));
            }
        }
        out
    }

    #[test]
    fn sorted_skip_exact_rows_across_multi_chunk() {
        // Keys spanning chunk boundaries: first-of-chunk (1, 13, 25),
        // last-of-chunk (10, 34), interior (16), and an absent key (99).
        let mut keys = vec![1i64, 10, 13, 16, 25, 34, 99];
        keys.sort_unstable();
        keys.dedup();
        let opts = ReadOptions {
            filters: vec![Predicate::InInt64("id".into(), keys)],
            sorted_by: Some("id".into()),
            ..ReadOptions::default()
        };
        let out =
            vortex_project_and_filter_limited(sorted_chunks(), &opts, None, true, None).unwrap();
        // Absent 99 excluded; everything else present, in ASC order.
        assert_eq!(read_ids(&out), vec![1, 10, 13, 16, 25, 34]);
    }

    #[test]
    fn sorted_skip_equivalence_vs_plain_filter() {
        // Same keys, with and without the sorted_by hint must yield identical
        // rows (kill-switch path is the same as the no-hint path).
        let mut keys = vec![4i64, 7, 19, 22, 28, 31];
        keys.sort_unstable();
        keys.dedup();
        let skip_opts = ReadOptions {
            filters: vec![Predicate::InInt64("id".into(), keys.clone())],
            sorted_by: Some("id".into()),
            ..ReadOptions::default()
        };
        let plain_opts = ReadOptions {
            filters: vec![Predicate::InInt64("id".into(), keys)],
            sorted_by: None, // no hint → plain vectorized membership mask
            ..ReadOptions::default()
        };
        let a = vortex_project_and_filter_limited(sorted_chunks(), &skip_opts, None, true, None)
            .unwrap();
        let b = vortex_project_and_filter_limited(sorted_chunks(), &plain_opts, None, true, None)
            .unwrap();
        assert_eq!(read_ids(&a), read_ids(&b));
        assert_eq!(read_ids(&a), vec![4, 7, 19, 22, 28, 31]);
    }

    #[test]
    fn sorted_skip_residual_filter_applied_exact() {
        // IN-list on the sort column PLUS a residual `score > 15` filter. The
        // skip path takes the IN rows then re-applies the residual on the
        // small taken batch — result must be exact, not a superset.
        let mut keys = vec![1i64, 16, 19, 34];
        keys.sort_unstable();
        keys.dedup();
        let opts = ReadOptions {
            filters: vec![
                Predicate::InInt64("id".into(), keys),
                Predicate::Gt("score".into(), ScalarValue::Float64(15.0)),
            ],
            sorted_by: Some("id".into()),
            ..ReadOptions::default()
        };
        let out =
            vortex_project_and_filter_limited(sorted_chunks(), &opts, None, true, None).unwrap();
        // score == id; keep IN-keys with score>15 → 16, 19, 34 (1 dropped).
        assert_eq!(read_ids(&out), vec![16, 19, 34]);
    }

    #[test]
    fn sorted_skip_no_hint_uses_plain_path() {
        // Without sorted_by, the InInt64 still filters correctly via the
        // vectorized membership mask (no take path).
        let mut keys = vec![7i64, 25];
        keys.sort_unstable();
        keys.dedup();
        let opts = ReadOptions {
            filters: vec![Predicate::InInt64("id".into(), keys)],
            sorted_by: None,
            ..ReadOptions::default()
        };
        let out =
            vortex_project_and_filter_limited(sorted_chunks(), &opts, None, true, None).unwrap();
        assert_eq!(read_ids(&out), vec![7, 25]);
    }

    #[test]
    fn sorted_skip_kill_switch_reverts_to_plain() {
        // With the kill switch set, sorted_skip_plan returns None even with the
        // hint present; the result is unchanged (correctness preserved).
        std::env::set_var("BASIN_SORTED_SKIP_DISABLE", "1");
        let mut keys = vec![10i64, 13, 28];
        keys.sort_unstable();
        keys.dedup();
        let opts = ReadOptions {
            filters: vec![Predicate::InInt64("id".into(), keys)],
            sorted_by: Some("id".into()),
            ..ReadOptions::default()
        };
        assert!(sorted_skip_plan(&opts).is_none(), "kill switch disables plan");
        let out =
            vortex_project_and_filter_limited(sorted_chunks(), &opts, None, true, None).unwrap();
        std::env::remove_var("BASIN_SORTED_SKIP_DISABLE");
        assert_eq!(read_ids(&out), vec![10, 13, 28]);
    }

    #[test]
    fn sorted_skip_plan_requires_hint_on_in_column() {
        // Hint on a DIFFERENT column than the IN-list → no plan (the IN is not
        // on the sorted column, so the take path is unsound).
        let opts = ReadOptions {
            filters: vec![Predicate::InInt64("id".into(), vec![1, 2, 3])],
            sorted_by: Some("other".into()),
            ..ReadOptions::default()
        };
        assert!(sorted_skip_plan(&opts).is_none());
    }

    /// End-to-end: with NO catalog attached, a point predicate must still
    /// prune the file set down via the synthesised prune schema and return
    /// exactly the matching row. Guards the no-catalog stats-prune path that
    /// fixes the s3_scaling point-query regression.
    #[tokio::test]
    async fn no_catalog_point_query_prunes_and_returns_one_row() {
        let store = Arc::new(InMemory::new());
        let storage = Storage::new(StorageConfig {
            object_store: store,
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let project = ProjectId::new();
        let table = TableName::new("events").unwrap();
        let part = PartitionKey::default_key();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        // 5 single-row-range files: ids [0..10), [10..20), ... [40..50).
        for b in 0..5i64 {
            let ids: Int64Array = (b * 10..b * 10 + 10).collect();
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids)]).unwrap();
            storage
                .write_batch(&project, &table, &part, &batch)
                .await
                .unwrap();
        }

        // Point query for id=27 → only the [20..30) file can match.
        let opts = ReadOptions {
            filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(27))],
            ..Default::default()
        };
        let mut stream = storage.read(&project, &table, opts).await.unwrap();
        let mut hits = 0usize;
        while let Some(b) = stream.next().await {
            hits += b.unwrap().num_rows();
        }
        assert_eq!(hits, 1, "exactly one row matches id=27");

        // Run twice more to exercise the warm stats-cache path.
        for _ in 0..2 {
            let opts = ReadOptions {
                filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(27))],
                ..Default::default()
            };
            let mut stream = storage.read(&project, &table, opts).await.unwrap();
            let mut hits = 0usize;
            while let Some(b) = stream.next().await {
                hits += b.unwrap().num_rows();
            }
            assert_eq!(hits, 1, "warm path still returns exactly one row");
        }

        // A predicate that matches nothing must return zero rows (and prune
        // every file).
        let opts = ReadOptions {
            filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(999))],
            ..Default::default()
        };
        let mut stream = storage.read(&project, &table, opts).await.unwrap();
        let mut hits = 0usize;
        while let Some(b) = stream.next().await {
            hits += b.unwrap().num_rows();
        }
        assert_eq!(hits, 0, "no file matches id=999");
    }

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

    /// Read one `.vortex` file through `read_one` with the given options,
    /// returning the concatenated decoded rows' `id` column values.
    async fn read_ids_with_opts(
        storage: &Storage,
        project: &ProjectId,
        path: &ObjectPath,
        schema: Arc<Schema>,
        opts: ReadOptions,
    ) -> Vec<i64> {
        let project_config = storage
            .project_storage_config_cached(project)
            .await
            .unwrap();
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

    /// Read one `.vortex` file through `read_one` with an `Eq(id, v)` filter,
    /// returning the concatenated decoded rows' `id` column values.
    async fn read_eq_ids(
        storage: &Storage,
        project: &ProjectId,
        path: &ObjectPath,
        schema: Arc<Schema>,
        eq: i64,
    ) -> Vec<i64> {
        let opts = ReadOptions {
            filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(eq))],
            ..ReadOptions::default()
        };
        read_ids_with_opts(storage, project, path, schema, opts).await
    }

    /// Read one `.vortex` file through `read_one` with NO filters (a full
    /// scan), returning the concatenated `id` column values.
    async fn read_unfiltered_ids(
        storage: &Storage,
        project: &ProjectId,
        path: &ObjectPath,
        schema: Arc<Schema>,
    ) -> Vec<i64> {
        read_ids_with_opts(storage, project, path, schema, ReadOptions::default()).await
    }

    /// The unfiltered key a small no-projection point read shares. All
    /// reads in these tests are catalog-aware (`read_one` is handed
    /// `Some(schema)`), so the shared entry is the stamped one.
    fn unfiltered_key_for(path: &ObjectPath) -> CacheKey {
        CacheKey {
            path: path.clone(),
            projection_hash: hash_projection(None),
            filters_hash: hash_filters(&[]),
            stamped: true,
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

        // Filtered reads are serve-only consumers of the shared unfiltered
        // entry: an unfiltered scan populates it, after which fresh-key Eq
        // reads on this small (12-row) file are served from it.
        let all = read_unfiltered_ids(&storage, &project, &path, schema.clone()).await;
        assert_eq!(all.len(), 12, "unfiltered scan returns every row");

        let ids0 = read_eq_ids(&storage, &project, &path, schema.clone(), 5).await;
        assert_eq!(ids0, vec![5], "Eq(id,5) returns exactly that row");
        let after_first = storage.page_cache().unwrap().counters();

        let ids1 = read_eq_ids(&storage, &project, &path, schema.clone(), 8).await;
        assert_eq!(ids1, vec![8], "Eq(id,8) returns exactly that row");
        let after_second = storage.page_cache().unwrap().counters();
        assert!(
            after_second.hits > after_first.hits,
            "fresh key after an unfiltered warm must hit the shared entry (hits {} -> {})",
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

    /// `BASIN_UNFILTERED_SERVE_MAX_ROWS` resolver: positive integers
    /// override, everything else keeps the 65 536 default.
    ///
    /// Parallel-test safety: this env var is read by every concurrent
    /// `read_one` call, so the values set here are chosen to be
    /// behaviour-preserving for the other tests in this file whichever one
    /// is momentarily visible — the tiny (≤12-row) files stay servable
    /// (12 ≤ 30 000 and ≤ 65 536) and the 70 000-row file in
    /// `unfiltered_serve_row_threshold_behaviours` stays non-servable
    /// (70 000 > 30 000 and > 65 536). Never set a value below 12 or in
    /// 65 537..=70 000 here.
    #[test]
    fn unfiltered_serve_max_rows_resolver() {
        let key = "BASIN_UNFILTERED_SERVE_MAX_ROWS";
        let prior = std::env::var(key).ok();

        std::env::remove_var(key);
        assert_eq!(
            resolve_unfiltered_serve_max_rows(),
            DEFAULT_UNFILTERED_SERVE_MAX_ROWS
        );
        std::env::set_var(key, "30000");
        assert_eq!(resolve_unfiltered_serve_max_rows(), 30_000);
        std::env::set_var(key, "0");
        assert_eq!(
            resolve_unfiltered_serve_max_rows(),
            DEFAULT_UNFILTERED_SERVE_MAX_ROWS
        );
        std::env::set_var(key, "not-a-number");
        assert_eq!(
            resolve_unfiltered_serve_max_rows(),
            DEFAULT_UNFILTERED_SERVE_MAX_ROWS
        );

        match prior {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
    }

    /// Serve-side row ceiling on the unfiltered-decode reuse path
    /// (`BASIN_UNFILTERED_SERVE_MAX_ROWS`, default 65 536). Serving a
    /// SELECTIVE read from the shared unfiltered entry means vectorized-
    /// filtering every cached row per query, which loses badly to the
    /// zone-map-pruned selective decode once the entry is large — so the
    /// reader only serves filtered reads from entries at or under the
    /// ceiling. No env mutation needed: the "large" file simply exceeds the
    /// default ceiling by row count.
    ///
    /// Covers:
    ///  1. LARGE cached file (70 000 rows > 65 536): a fresh-key filtered
    ///     read is NOT served from the shared entry (page-cache hit counter
    ///     frozen, `files_served_from_cache` still) and takes the pruned
    ///     pushdown path — one GET (`files_opened` +1) decoding at most one
    ///     chunk — while the populate side keeps caching the entry;
    ///  2. SMALL cached file: a fresh-key filtered read keeps being served
    ///     from the entry (hits advance, ZERO GETs);
    ///  3. an UNFILTERED read on the large file is served from cache at any
    ///     size (the row gate applies only to filtered serves);
    ///  4. parity: filtered-via-cache (small warm file) equals
    ///     filtered-via-pruned (page-cache-less storage) across present /
    ///     NULL-note / absent keys.
    #[tokio::test]
    async fn unfiltered_serve_row_threshold_behaviours() {
        let project = ProjectId::new();
        let schema = pk_schema();

        // ───────── 1. large cached file: filtered serve declines ───────────
        // 70 000 rows > DEFAULT_UNFILTERED_SERVE_MAX_ROWS. A 1 GiB budget
        // (per-shard 16 MiB at the default 64 shards) guarantees the ~2 MB
        // decoded entry passes BOTH admission gates, so the cold read really
        // does populate it — this test gates SERVING, not admission.
        const LARGE_ROWS: usize = 70_000;
        const BLOCK: u32 = 8192;
        assert!((LARGE_ROWS as u64) > DEFAULT_UNFILTERED_SERVE_MAX_ROWS);
        let storage = paged_storage(1024 * 1024 * 1024);
        let table = TableName::new("rowgate").unwrap();
        let path = write_vortex(
            &storage,
            &project,
            &table,
            &pk_batch(0, LARGE_ROWS),
            Some(BLOCK),
        )
        .await;

        // Cold filtered read takes the pruned path and must NOT populate the
        // shared unfiltered entry (filtered reads are serve-only consumers —
        // populating would mean a whole-file decode per query until the entry
        // admits). An unfiltered scan is what populates it.
        let ids_cold = read_eq_ids(&storage, &project, &path, schema.clone(), 5).await;
        assert_eq!(ids_cold, vec![5], "cold read returns the right row");
        let pc = storage.page_cache().unwrap();
        assert!(
            pc.get(&unfiltered_key_for(&path)).is_none(),
            "a filtered read must not populate the shared unfiltered entry"
        );
        let all = read_unfiltered_ids(&storage, &project, &path, schema.clone()).await;
        assert_eq!(all.len(), LARGE_ROWS, "unfiltered scan returns every row");
        assert!(
            pc.get(&unfiltered_key_for(&path)).is_some(),
            "the unfiltered scan populates the shared entry"
        );

        let pc_before = pc.counters();
        let rc_before = storage.read_counters().snapshot();
        let ids_warm = read_eq_ids(&storage, &project, &path, schema.clone(), 60_001).await;
        let pc_after = pc.counters();
        let d = storage.read_counters().snapshot().delta(&rc_before);
        assert_eq!(ids_warm, vec![60_001], "pruned path returns the right row");
        assert_eq!(
            pc_after.hits, pc_before.hits,
            "filtered read on a {LARGE_ROWS}-row cached entry must NOT serve from it"
        );
        assert_eq!(
            d.files_served_from_cache, 0,
            "declined serve is not cache-served"
        );
        assert_eq!(d.files_opened, 1, "pruned path pays the GET exactly once");
        assert!(
            d.rows_decoded > 0 && d.rows_decoded <= u64::from(BLOCK),
            "zone-map pruning must decode at most one {BLOCK}-row chunk, decoded {}",
            d.rows_decoded
        );

        // ───────── 3. unfiltered read serves from cache at ANY size ────────
        // (Done on the same large file/storage while it's warm.) A no-filter
        // full scan is the entry's home turf: it shares the cache key of the
        // raw entry and is served whole, no GET, regardless of row count.
        let pc_before = pc.counters();
        let rc_before = storage.read_counters().snapshot();
        let all = read_unfiltered_ids(&storage, &project, &path, schema.clone()).await;
        let pc_after = pc.counters();
        let d = storage.read_counters().snapshot().delta(&rc_before);
        assert_eq!(all.len(), LARGE_ROWS, "full scan returns every row");
        assert_eq!(all.first(), Some(&0i64));
        assert_eq!(all.last(), Some(&(LARGE_ROWS as i64 - 1)));
        assert!(
            pc_after.hits > pc_before.hits,
            "unfiltered read must be served from cache regardless of entry size"
        );
        assert_eq!(d.files_opened, 0, "no GET on an unfiltered cache hit");
        assert_eq!(d.files_served_from_cache, 1, "counted as cache-served");

        // ───────── 2. small cached file: filtered serve still fires ────────
        let storage_small = paged_storage(64 * 1024 * 1024);
        let table_small = TableName::new("rowgate_small").unwrap();
        let path_small = write_vortex(
            &storage_small,
            &project,
            &table_small,
            &pk_batch(0, 12),
            Some(4),
        )
        .await;
        let ids_cold = read_eq_ids(&storage_small, &project, &path_small, schema.clone(), 5).await;
        assert_eq!(ids_cold, vec![5]);
        // Populate the shared entry the serve-only way: an unfiltered scan.
        let all = read_unfiltered_ids(&storage_small, &project, &path_small, schema.clone()).await;
        assert_eq!(all.len(), 12);

        let pc_small = storage_small.page_cache().unwrap();
        let pc_before = pc_small.counters();
        let rc_before = storage_small.read_counters().snapshot();
        let ids_warm = read_eq_ids(&storage_small, &project, &path_small, schema.clone(), 8).await;
        let pc_after = pc_small.counters();
        let d = storage_small.read_counters().snapshot().delta(&rc_before);
        assert_eq!(ids_warm, vec![8], "cache-served read returns the right row");
        assert!(
            pc_after.hits > pc_before.hits,
            "small entry must keep serving fresh-key filtered reads"
        );
        assert_eq!(
            d.files_opened, 0,
            "pre-GET short-circuit: zero object-store GETs on a small warm file"
        );
        assert_eq!(d.files_served_from_cache, 1, "counted as cache-served");

        // ───────── 4. parity: filtered-via-cache == filtered-via-pruned ────
        // Same content written to a paged storage (warm: serves from the
        // shared entry) and a page-cache-LESS storage (always the pruned
        // pushdown path). Keys cover present-with-note, present-NULL-note
        // (id % 3 == 0), and absent.
        let batch = pk_batch(300, 12);
        let storage_cache = paged_storage(64 * 1024 * 1024);
        let storage_pruned = fresh_storage(); // page_cache: None → pushdown only
        let table_p = TableName::new("rowgate_par").unwrap();
        let path_cache = write_vortex(&storage_cache, &project, &table_p, &batch, Some(4)).await;
        let path_pruned = write_vortex(&storage_pruned, &project, &table_p, &batch, Some(4)).await;

        // Warm the shared entry on the paged storage (serve-only policy:
        // only an unfiltered scan populates it).
        let warmup = read_unfiltered_ids(&storage_cache, &project, &path_cache, schema.clone()).await;
        assert_eq!(warmup.len(), 12);

        let keys = [300i64, 303, 306, 309, 305, 99_999];
        let hits_before = storage_cache.page_cache().unwrap().counters().hits;
        for eq in keys {
            let via_cache =
                read_eq_ids(&storage_cache, &project, &path_cache, schema.clone(), eq).await;
            let via_pruned =
                read_eq_ids(&storage_pruned, &project, &path_pruned, schema.clone(), eq).await;
            assert_eq!(
                via_cache, via_pruned,
                "cache-served vs pruned disagree for Eq(id,{eq})"
            );
            let expected: Vec<i64> = if (300..312).contains(&eq) {
                vec![eq]
            } else {
                vec![]
            };
            assert_eq!(via_cache, expected, "wrong rows for Eq(id,{eq})");
        }
        let hits_after = storage_cache.page_cache().unwrap().counters().hits;
        assert!(
            hits_after >= hits_before + keys.len() as u64,
            "every parity read on the warm small file must be a cache serve ({hits_before} -> {hits_after})"
        );
    }

    // -----------------------------------------------------------------------
    // Semantic-typed columns (ADR 0024 UUID, PG-Wave-α POINT) × page cache:
    // the cache stores the POST-restamp canonical representation for
    // catalog-aware reads, and the `stamped` key bit keeps schema-less raw
    // entries from ever being served verbatim to a catalog-aware read.
    // -----------------------------------------------------------------------

    /// `BASIN_TYPE` marker map for a semantic column.
    fn semantic_metadata(v: &str) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::from([(BASIN_TYPE_KEY.to_string(), v.to_string())])
    }

    /// Catalog schema with an Int64 PK, a UUID column (FSB(16) +
    /// BASIN_TYPE=UUID — physically Decimal256(39,0) inside a `.vortex`
    /// file per ADR 0024) and a POINT column (FSB(21) + BASIN_TYPE=POINT —
    /// physically LargeBinary inside Vortex).
    fn semantic_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("u", DataType::FixedSizeBinary(16), true)
                .with_metadata(semantic_metadata(BASIN_TYPE_UUID)),
            Field::new(
                "p",
                DataType::FixedSizeBinary(basin_geo::POINT_WKB_LEN as i32),
                true,
            )
            .with_metadata(semantic_metadata(BASIN_TYPE_POINT)),
        ]))
    }

    /// `len` rows of deterministic semantic data. UUID bytes have the high
    /// bit SET (the Decimal256 disguise must round-trip the full 128-bit
    /// space, not just non-negative i128 patterns); both semantic columns
    /// carry NULLs so the disguise inverses are exercised with null masks.
    fn semantic_batch(start: i64, len: usize) -> RecordBatch {
        use arrow_array::FixedSizeBinaryArray;
        let id: Int64Array = (start..start + len as i64).collect();
        let uuids = (0..len).map(|i| {
            if i % 5 == 4 {
                None
            } else {
                let mut b = [0u8; 16];
                b[0] = 0x80 | (i as u8); // high bit set on purpose
                b[15] = (start as u8).wrapping_add(i as u8);
                Some(b)
            }
        });
        let u = FixedSizeBinaryArray::try_from_sparse_iter_with_size(uuids, 16).unwrap();
        let points = (0..len).map(|i| {
            if i % 7 == 6 {
                None
            } else {
                let mut b = [0u8; basin_geo::POINT_WKB_LEN];
                b[0] = 1; // WKB little-endian marker byte; payload opaque here
                b[basin_geo::POINT_WKB_LEN - 1] = i as u8;
                Some(b)
            }
        });
        let p = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            points,
            basin_geo::POINT_WKB_LEN as i32,
        )
        .unwrap();
        RecordBatch::try_new(
            semantic_schema(),
            vec![Arc::new(id), Arc::new(u), Arc::new(p)],
        )
        .unwrap()
    }

    /// `read_one` with explicit `catalog_schema` (Some = table-aware read,
    /// None = schema-less `read_file`-shaped read), returning all batches.
    async fn read_semantic_batches(
        storage: &Storage,
        project: &ProjectId,
        path: &ObjectPath,
        schema: Option<Arc<Schema>>,
        opts: ReadOptions,
    ) -> Vec<RecordBatch> {
        let project_config = storage
            .project_storage_config_cached(project)
            .await
            .unwrap();
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
            *project,
            schema,
        )
        .await
        .expect("read_one");
        stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|b| b.expect("batch"))
            .collect()
    }

    /// Concatenate every batch (served paths may emit one batch per cached
    /// chunk, the pruned path one per surviving chunk — chunking must not
    /// affect the comparison).
    fn concat_all(batches: &[RecordBatch]) -> RecordBatch {
        assert!(!batches.is_empty(), "expected at least one batch");
        let schema = batches[0].schema();
        arrow::compute::concat_batches(&schema, batches).expect("concat")
    }

    /// Every catalog column served must carry the catalog's EXACT Arrow
    /// `DataType` and field metadata — i.e. UUID is `FixedSizeBinary(16)` +
    /// `BASIN_TYPE=UUID` (never the on-disk Decimal256 disguise) and POINT
    /// is `FixedSizeBinary(21)` + `BASIN_TYPE=POINT` (never LargeBinary).
    fn assert_canonical(batch: &RecordBatch, catalog: &Schema) {
        let s = batch.schema();
        for cf in catalog.fields() {
            let f = s
                .field_with_name(cf.name())
                .expect("served batch must contain every catalog column");
            assert_eq!(
                f.data_type(),
                cf.data_type(),
                "served Arrow type for column '{}' must equal the catalog's",
                cf.name()
            );
            assert_eq!(
                f.metadata(),
                cf.metadata(),
                "served field metadata for column '{}' must equal the catalog's",
                cf.name()
            );
        }
    }

    /// Paged storage over a SHARED object store, so a cache-less reference
    /// storage can read the very same file.
    fn paged_storage_over(
        store: Arc<object_store::memory::InMemory>,
        max_bytes: u64,
    ) -> Storage {
        Storage::new(StorageConfig {
            object_store: store,
            root_prefix: None,
            disk_cache: None,
            page_cache: Some(crate::PageCacheConfig::new(max_bytes)),
        })
    }

    fn cacheless_storage_over(store: Arc<object_store::memory::InMemory>) -> Storage {
        Storage::new(StorageConfig {
            object_store: store,
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        })
    }

    /// UUID/POINT through the page cache, both orders:
    ///
    ///  1. unfiltered scan (populates the shared post-restamp entry) →
    ///     filtered read served from that entry must be byte/type-identical
    ///     to the same filtered read on a page-cache-LESS storage over the
    ///     same file (pruned pushdown path), with every column's DataType
    ///     and metadata equal to the catalog schema's;
    ///  2. reverse order: filtered read first (cold pushdown populates the
    ///     per-filter key), then the identical read again — a verbatim
    ///     generic cache hit (zero GETs) that must still be canonical and
    ///     identical to both the cold result and the pruned reference —
    ///     then the unfiltered scan, also canonical.
    #[tokio::test]
    async fn semantic_cache_serves_canonical_representation_both_orders() {
        use arrow_array::FixedSizeBinaryArray;

        let project = ProjectId::new();
        let schema = semantic_schema();
        let original = semantic_batch(0, 12);

        let shared = Arc::new(InMemory::new());
        let storage = paged_storage_over(shared.clone(), 64 * 1024 * 1024);
        let storage_pruned = cacheless_storage_over(shared.clone());
        let table = TableName::new("sem").unwrap();
        let path = write_vortex(&storage, &project, &table, &original, Some(4)).await;

        // ── 1. unfiltered populate → filtered serve ─────────────────────────
        let unf = concat_all(
            &read_semantic_batches(
                &storage,
                &project,
                &path,
                Some(schema.clone()),
                ReadOptions::default(),
            )
            .await,
        );
        assert_eq!(unf.num_rows(), 12);
        assert_canonical(&unf, &schema);
        // Value-level ground truth: the semantic columns round-trip the
        // original bytes (incl. NULL masks) through the on-disk disguise.
        let got_u = unf
            .column_by_name("u")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("u served as FixedSizeBinary(16)");
        let want_u = original
            .column_by_name("u")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(got_u, want_u, "UUID bytes must round-trip");
        let got_p = unf
            .column_by_name("p")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("p served as FixedSizeBinary(21)");
        let want_p = original
            .column_by_name("p")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(got_p, want_p, "POINT bytes must round-trip");
        assert!(
            storage
                .page_cache()
                .unwrap()
                .get(&unfiltered_key_for(&path))
                .is_some(),
            "unfiltered catalog-aware scan populates the stamped shared entry"
        );

        let eq_opts = || ReadOptions {
            filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(7))],
            ..ReadOptions::default()
        };
        let rc_before = storage.read_counters().snapshot();
        let served = concat_all(
            &read_semantic_batches(&storage, &project, &path, Some(schema.clone()), eq_opts())
                .await,
        );
        let d = storage.read_counters().snapshot().delta(&rc_before);
        assert_eq!(
            d.files_opened, 0,
            "warm filtered read must be served from the shared entry (no GET)"
        );
        let pruned = concat_all(
            &read_semantic_batches(
                &storage_pruned,
                &project,
                &path,
                Some(schema.clone()),
                eq_opts(),
            )
            .await,
        );
        assert_eq!(served.num_rows(), 1);
        assert_canonical(&served, &schema);
        assert_eq!(
            served, pruned,
            "cache-served filtered read must be byte-identical to the pruned read"
        );

        // ── 2. reverse order: filtered cold → filtered warm → unfiltered ───
        // Fresh page cache over the SAME file.
        let storage2 = paged_storage_over(shared.clone(), 64 * 1024 * 1024);
        let cold = concat_all(
            &read_semantic_batches(&storage2, &project, &path, Some(schema.clone()), eq_opts())
                .await,
        );
        assert_canonical(&cold, &schema);
        let rc_before = storage2.read_counters().snapshot();
        let warm = concat_all(
            &read_semantic_batches(&storage2, &project, &path, Some(schema.clone()), eq_opts())
                .await,
        );
        let d = storage2.read_counters().snapshot().delta(&rc_before);
        assert_eq!(
            d.files_opened, 0,
            "repeat of the identical filtered read is a verbatim generic hit"
        );
        assert_canonical(&warm, &schema);
        assert_eq!(warm, cold, "verbatim hit must equal the cold result");
        assert_eq!(warm, pruned, "…and the pruned reference");

        let unf2 = concat_all(
            &read_semantic_batches(
                &storage2,
                &project,
                &path,
                Some(schema.clone()),
                ReadOptions::default(),
            )
            .await,
        );
        assert_canonical(&unf2, &schema);
        assert_eq!(unf2, unf, "unfiltered scan after filtered reads is unchanged");
    }

    /// The filter/projection/restamp tail (`vortex_project_and_filter`) is
    /// IDEMPOTENT for semantic types: running it over already-canonical
    /// batches — exactly what the row-gated serve paths do to a cached
    /// post-restamp entry — is a byte-identical no-op, and running it over
    /// the raw decode shape (UUID as metadata-less Decimal256(39,0), POINT
    /// as metadata-less LargeBinary — Vortex drops field metadata wholesale)
    /// restores the canonical representation.
    #[test]
    fn restamp_tail_is_idempotent_for_uuid_and_point() {
        use arrow_array::{Array, Decimal256Array, FixedSizeBinaryArray};
        use arrow_buffer::i256;

        let schema = semantic_schema();
        let canonical = semantic_batch(0, 8);

        // Raw decode shape, mirroring `writer::uuid_fsb_to_decimal256` /
        // `writer::point_fsb_to_large_binary` plus Vortex's metadata drop.
        let u_src = canonical
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let mut u_vals: Vec<Option<i256>> = Vec::with_capacity(u_src.len());
        for r in 0..u_src.len() {
            if u_src.is_null(r) {
                u_vals.push(None);
            } else {
                let mut buf = [0u8; 32];
                buf[16..32].copy_from_slice(u_src.value(r));
                u_vals.push(Some(i256::from_be_bytes(buf)));
            }
        }
        let u_raw = Decimal256Array::from(u_vals)
            .with_precision_and_scale(39, 0)
            .unwrap();
        let p_src = canonical
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let mut p_builder = arrow_array::builder::LargeBinaryBuilder::new();
        for r in 0..p_src.len() {
            if p_src.is_null(r) {
                p_builder.append_null();
            } else {
                p_builder.append_value(p_src.value(r));
            }
        }
        let raw = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("u", DataType::Decimal256(39, 0), true),
                Field::new("p", DataType::LargeBinary, true),
            ])),
            vec![
                canonical.column(0).clone(),
                Arc::new(u_raw),
                Arc::new(p_builder.finish()),
            ],
        )
        .unwrap();

        let opts = ReadOptions::default();
        let once =
            vortex_project_and_filter_limited(vec![raw], &opts, Some(&schema), true, None)
                .unwrap();
        assert_eq!(once.len(), 1);
        assert_eq!(
            once[0], canonical,
            "the tail must restore the canonical representation from the raw decode"
        );

        let twice =
            vortex_project_and_filter_limited(once.clone(), &opts, Some(&schema), true, None)
                .unwrap();
        assert_eq!(
            twice[0], canonical,
            "re-running the tail over canonical batches must be a byte-identical no-op"
        );
    }

    /// Regression for the representation collision the `CacheKey::stamped`
    /// bit fixes: a schema-less unfiltered read (the `read_file`
    /// UPDATE/DELETE-rewrite shape, `catalog_schema = None`) caches the RAW
    /// physical decode (UUID still Decimal256-disguised, no BASIN_TYPE
    /// metadata) under the same `(path, projection=ALL, filters=∅)` triple a
    /// catalog-aware full scan uses. The two caller classes must NOT share
    /// the entry: each must keep seeing exactly its own cold-path
    /// representation, in both warm-up orders.
    #[tokio::test]
    async fn schema_less_and_catalog_reads_use_distinct_cache_entries() {
        let project = ProjectId::new();
        let schema = semantic_schema();
        let batch = semantic_batch(40, 12);

        let shared = Arc::new(InMemory::new());
        let storage = paged_storage_over(shared.clone(), 64 * 1024 * 1024);
        let storage_ref = cacheless_storage_over(shared.clone());
        let table = TableName::new("semless").unwrap();
        let path = write_vortex(&storage, &project, &table, &batch, Some(4)).await;

        // 1) Schema-less unfiltered read populates ONLY the raw entry.
        let raw = read_semantic_batches(&storage, &project, &path, None, ReadOptions::default())
            .await;
        assert_eq!(raw.iter().map(RecordBatch::num_rows).sum::<usize>(), 12);
        let pc = storage.page_cache().unwrap();
        let raw_key = CacheKey {
            path: path.clone(),
            projection_hash: hash_projection(None),
            filters_hash: hash_filters(&[]),
            stamped: false,
        };
        assert!(
            pc.get(&raw_key).is_some(),
            "schema-less unfiltered read populates the raw (unstamped) entry"
        );
        assert!(
            pc.get(&unfiltered_key_for(&path)).is_none(),
            "…and never the catalog-stamped entry"
        );

        // 2) A catalog-aware full scan of the same file must NOT be served
        //    the raw entry verbatim: every column must carry the catalog's
        //    exact DataType + metadata, identical to a cache-less storage.
        let canon = concat_all(
            &read_semantic_batches(
                &storage,
                &project,
                &path,
                Some(schema.clone()),
                ReadOptions::default(),
            )
            .await,
        );
        assert_canonical(&canon, &schema);
        let reference = concat_all(
            &read_semantic_batches(
                &storage_ref,
                &project,
                &path,
                Some(schema.clone()),
                ReadOptions::default(),
            )
            .await,
        );
        assert_eq!(
            canon, reference,
            "catalog-aware scan with a warm raw entry must equal the cache-less read"
        );
        assert!(
            pc.get(&unfiltered_key_for(&path)).is_some(),
            "the catalog-aware scan caches its own stamped entry"
        );

        // 3) Reverse direction: with the stamped entry warm, a schema-less
        //    read must keep returning its own cold-path (raw)
        //    representation, not the canonical one.
        let raw_again = concat_all(
            &read_semantic_batches(&storage, &project, &path, None, ReadOptions::default())
                .await,
        );
        let raw_reference = concat_all(
            &read_semantic_batches(&storage_ref, &project, &path, None, ReadOptions::default())
                .await,
        );
        assert_eq!(
            raw_again, raw_reference,
            "schema-less reads must not change representation when a stamped entry is warm"
        );
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

    /// LEVER 2 cold-stats test: a recording store that captures the `range`
    /// of every `get_opts`, so we can prove the Vortex footer/stats path reads
    /// only the TAIL (bounded/suffix ranges) and never the whole file.
    #[derive(Debug)]
    struct RecordingStore {
        inner: Arc<dyn ObjectStore>,
        // (was_ranged, bytes_fetched) per GET.
        gets: std::sync::Mutex<Vec<(bool, u64)>>,
    }

    impl std::fmt::Display for RecordingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RecordingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for RecordingStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: object_store::PutMultipartOpts,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let was_ranged = options.range.is_some();
            let res = self.inner.get_opts(location, options).await?;
            let n = (res.range.end - res.range.start) as u64;
            self.gets.lock().unwrap().push((was_ranged, n));
            Ok(res)
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }
        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// The Vortex cold-stats path (`list_data_files_with_stats`) must read the
    /// footer via TAIL range GETs, never a whole-file GET. We write a Vortex
    /// file big enough that a full-file fetch would be unmistakable, list it
    /// through a recording store, and assert: (a) at least one GET happened
    /// (cold — nothing cached), (b) NO unranged (full-file) GET was issued,
    /// and (c) the total bytes fetched are far below the file size.
    #[tokio::test]
    async fn vortex_cold_stats_reads_tail_not_full_file() {
        let inner = Arc::new(InMemory::new());
        // First, write the Vortex file through a plain store so the WRITE PUTs
        // don't pollute the GET recording.
        let writer_storage = Storage::new(StorageConfig {
            object_store: inner.clone(),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let project = ProjectId::new();
        let table = TableName::new("events").unwrap();
        let part = PartitionKey::default_key();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        // ~40k rows with a HIGH-ENTROPY payload → a multi-hundred-KiB Vortex
        // stripe whose data dwarfs the ~64 KiB tail footer read. The payload is
        // a per-row pseudo-random hex blob so Vortex's encoders cannot collapse
        // it (a low-entropy payload compresses below the 256 KiB floor the
        // assertion below relies on).
        let ids: Int64Array = (0..40_000i64).collect();
        let pays = StringArray::from(
            (0..40_000u64)
                .map(|i| {
                    // splitmix64-style scramble → 64 distinct hex chars/row.
                    let mut s = String::with_capacity(64);
                    let mut z = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
                    for _ in 0..4 {
                        z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
                        let mut x = z;
                        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                        x ^= x >> 31;
                        s.push_str(&format!("{x:016x}"));
                    }
                    Some(s)
                })
                .collect::<Vec<_>>(),
        );
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(pays)]).unwrap();
        let written = writer_storage
            .write_batch(&project, &table, &part, &batch)
            .await
            .unwrap();
        assert!(
            written.path.as_ref().ends_with(".vortex"),
            "default write must be Vortex for this lever to apply"
        );
        let file_size = written.size_bytes;
        assert!(
            file_size > 256 * 1024,
            "test file ({file_size} B) must be much larger than the ~64 KiB tail \
             footer read for the assertion to be meaningful"
        );

        // Now drive a COLD listing through the recording store (fresh Storage
        // so the stats cache is empty).
        let recording = Arc::new(RecordingStore {
            inner: inner.clone(),
            gets: std::sync::Mutex::new(Vec::new()),
        });
        let storage = Storage::new(StorageConfig {
            object_store: recording.clone(),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });

        let files = list_data_files_with_stats(&storage, &project, &table)
            .await
            .expect("cold listing must succeed");
        assert_eq!(files.len(), 1, "exactly one data file");
        // Stats must actually be populated (proves the tail read parsed the
        // footer, not just that it skipped the fetch).
        assert!(files[0].row_count > 0, "row_count from footer");
        assert!(
            files[0].column_stats.contains_key("id"),
            "id column stats decoded from the tail footer"
        );

        let gets = recording.gets.lock().unwrap().clone();
        assert!(!gets.is_empty(), "cold path must issue at least one GET");
        // (b) No full-file GET: every GET must be ranged.
        assert!(
            gets.iter().all(|(ranged, _)| *ranged),
            "Vortex cold-stats path issued an UNRANGED full-file GET: {gets:?}"
        );
        // (c) Total tail bytes far below the file size (no full-file read).
        let total: u64 = gets.iter().map(|(_, n)| *n).sum();
        assert!(
            total < file_size,
            "tail reads ({total} B) must be smaller than the file ({file_size} B)"
        );
    }
}
