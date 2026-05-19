//! Parquet reader with projection + predicate pushdown.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use arrow_array::{new_null_array, RecordBatch};
use arrow_schema::{Schema, SchemaRef};
use basin_common::{BasinError, ProjectId, Result, TableName};
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
use crate::vortex_footer_cache::VortexFooterCache;
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
            let resolved_tier = Tier::from_path(meta.location.as_ref());
            files.push(DataFile {
                path: meta.location,
                size_bytes: meta.size as u64,
                row_count: 0,
                column_stats: BTreeMap::new(),
                bloom_filters: BTreeMap::new(),
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
    if path.as_ref().ends_with(".vortex") {
        let schema = catalog_schema.clone();

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
        let (push_filter, all_filters_pushed) =
            vortex_filter_expr(&opts.filters, catalog_schema.as_deref());
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
        // pushdown (decode_used_filter, i.e. did not fall back).
        let apply_filter = !(all_filters_pushed && decode_used_filter);
        let batches =
            vortex_project_and_filter(batches, opts.as_ref(), catalog_schema.as_ref(), apply_filter)?;

        // Page-cache write-through, keyed by the same (path, projection,
        // filters) cache key the Parquet path uses, so a repeat of the
        // identical read is served from cache and stays consistent.
        if let (Some(pc), Some(key)) = (page_cache.as_ref(), cache_key) {
            let cached: Vec<Arc<RecordBatch>> = batches.iter().cloned().map(Arc::new).collect();
            pc.insert(key, cached);
        }
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
                    let batch =
                        res.map_err(|e| BasinError::storage(format!("parquet read: {e}")))?;
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
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
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
        out.push(projected);
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
    use vortex_array::expr::{and_collect, col, eq, gt, lit, lt, Expression};

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
}
