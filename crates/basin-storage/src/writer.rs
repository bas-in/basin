//! Parquet writer.
//!
//! Atomicity. Object stores generally do not give us cheap multi-key atomic
//! commits; what they do give us is single-object PUT atomicity. We rely on
//! that: a Parquet file appears in full or not at all. To avoid colliding
//! with concurrent writers, the file name embeds a ULID. Visibility across
//! many files is the catalog's problem, not ours — see the basin-catalog
//! Iceberg snapshot commit path.

use std::collections::BTreeMap;

use arrow_array::{Array, RecordBatch};
use basin_common::{BasinError, PartitionKey, ProjectId, Result, TableName};
use bytes::Bytes;
use chrono::Utc;
use object_store::{ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use ulid::Ulid;

use crate::data_file::{ColumnStats, DataFile};
use crate::paths::data_file_key;
use crate::Storage;

/// Suffix appended to a data-file key to derive its envelope-encryption
/// sidecar (the wrapped data key). Single source of truth so the reader
/// can probe for the same suffix on GET.
pub(crate) const WRAPPED_SIDECAR_SUFFIX: &str = ".wrapped";

/// AES-GCM nonce size: 96 bits, the only size the spec recommends for
/// random nonces. Prefixed to the ciphertext on disk so the reader can
/// recover it without an additional sidecar.
pub(crate) const AES_GCM_NONCE_LEN: usize = 12;

/// Build the sidecar key for an envelope-encrypted data file. The format
/// is `<data-file-path>.wrapped`; project-prefix enforcement is implicit
/// because the input key already lives under the project prefix.
pub(crate) fn wrapped_sidecar_key(data_key: &object_store::path::Path) -> object_store::path::Path {
    object_store::path::Path::from(format!("{}{}", data_key.as_ref(), WRAPPED_SIDECAR_SUFFIX))
}

/// AES-GCM encrypt `plaintext` with `data_key`. The on-disk layout is
/// `nonce(12) || ciphertext_with_tag` so a reader needs only the file
/// body and the wrapped key sidecar to round-trip. A fresh random nonce
/// per file is safe with a fresh per-file data key — nonce reuse risk is
/// confined to within-key, and we never reuse the data key.
pub(crate) fn encrypt_envelope(data_key: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
    use aes_gcm::aead::Aead;
    use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
    use rand::RngCore;

    if data_key.len() != 32 {
        return Err(BasinError::storage(format!(
            "envelope encrypt: data key is {} bytes, expected 32",
            data_key.len()
        )));
    }
    let key = aes_gcm::Key::<Aes256Gcm>::from_slice(data_key);
    let cipher = Aes256Gcm::new(key);
    let mut nonce_bytes = [0u8; AES_GCM_NONCE_LEN];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ct = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| BasinError::storage(format!("aes-gcm encrypt: {e}")))?;
    let mut out = Vec::with_capacity(AES_GCM_NONCE_LEN + ct.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ct);
    Ok(out)
}

/// Row groups of 65_536 rows. Big enough that per-group metadata overhead is
/// small relative to the data, small enough that statistics pruning still
/// drops most of a file on selective queries. Production tuning will move to
/// per-table catalog options once the catalog grows them.
const DEFAULT_MAX_ROW_GROUP_SIZE: usize = 65_536;

/// Default expected number of distinct values per row group for a bloom-
/// filtered column. The bitset size (and therefore the false-positive rate)
/// is computed from `(ndv, fpp)`. 1024 is a sensible starting value: for
/// row-group sizes around 65k, most enum-shaped or moderately-cardinal
/// columns sit comfortably below this; if the column is near-unique the
/// filter stays useful at the configured FPP, just larger.
const DEFAULT_BLOOM_NDV: u64 = 1024;
/// Default false-positive probability target. 1% is the typical industry
/// default; turning it tighter has diminishing returns vs. the bitset size
/// it costs in the footer.
const DEFAULT_BLOOM_FPP: f64 = 0.01;

/// On-disk storage format for a table's data files. `Parquet` (the default)
/// preserves the legacy behaviour byte-for-byte and Iceberg / Athena / Spark
/// read-compat; `Vortex` is the opt-in columnar format (#161). A table is
/// single-format (mixed Parquet+Vortex within one table is a deferred
/// feature), so the read path selects one format per table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FileFormat {
    Parquet,
    #[default]
    Vortex,
}

impl FileFormat {
    /// Object-key file extension (no leading dot).
    pub fn extension(&self) -> &'static str {
        match self {
            FileFormat::Parquet => "parquet",
            FileFormat::Vortex => "vortex",
        }
    }
}

/// Knobs for [`write_batch_with_options`]. All defaults preserve the legacy
/// behaviour exactly, so callers that don't care about bloom filters can
/// keep using [`write_batch`] without churn.
#[derive(Clone, Debug, Default)]
pub struct WriteOptions {
    /// On-disk format for this write. Default [`FileFormat::Parquet`] keeps
    /// the legacy path byte-for-byte.
    pub file_format: FileFormat,
    /// Columns that should get a native Parquet bloom filter section. Empty
    /// (the default) is the pre-bloom behaviour: no filter is written, the
    /// reader's pruning is driven by min/max stats alone.
    pub bloom_filter_columns: Vec<String>,
    /// Override [`DEFAULT_MAX_ROW_GROUP_SIZE`]. `None` keeps the default.
    /// Tests use this to force multiple row groups out of small batches so
    /// the bloom-filter pruning path is observable.
    pub max_row_group_size: Option<usize>,
    /// Phase 5.7 B2: physically sort the batch by these columns before
    /// flushing to Parquet. Combined with A3 bloom filters and A4 catalog
    /// stats, point queries on the cluster columns prune to one file in
    /// the common case. Empty (the default) preserves the pre-B2 write
    /// path exactly. Unknown column names are silently ignored — the
    /// engine validates the column set against the schema before
    /// constructing `WriteOptions`, so a stray name here means
    /// schema-evolved-out and isn't worth a hard error on the hot path.
    pub cluster_columns: Vec<String>,
    /// Per-table chunk / row-group size in rows (from `basin.row_block_size`).
    ///
    /// For Vortex tables this is forwarded to
    /// `WriteStrategyBuilder::with_row_block_size`. For Parquet tables it
    /// maps to `WriterProperties::max_row_group_size` (same knob as
    /// `max_row_group_size`, but sourced from the `WITH` clause at CREATE
    /// TABLE time). `None` keeps the writer's built-in default for each
    /// format.
    pub row_block_size: Option<u32>,
    /// Phase 5.14.A2 — columns for which the writer should compute a
    /// fastbloom filter and store in `DataFile::bloom_filters`. Typically
    /// the table's `global_sort_order` columns (from `basin.sort_by`).
    /// Empty (the default) preserves the pre-5.14 write path exactly.
    /// Only `Int64` and `Utf8` columns are bloomed; all others are silently
    /// skipped.
    pub bloom_columns: Vec<String>,
}

pub(crate) async fn write_batch(
    storage: &Storage,
    project: &ProjectId,
    table: &TableName,
    partition: &PartitionKey,
    batch: &RecordBatch,
) -> Result<DataFile> {
    write_batch_with_options(
        storage,
        project,
        table,
        partition,
        batch,
        &WriteOptions::default(),
    )
    .await
}

pub(crate) async fn write_batch_with_options(
    storage: &Storage,
    project: &ProjectId,
    table: &TableName,
    partition: &PartitionKey,
    batch: &RecordBatch,
    opts: &WriteOptions,
) -> Result<DataFile> {
    let data_ulid = Ulid::new();
    let key = data_file_key(
        storage.root_prefix(),
        project,
        table,
        partition,
        Utc::now(),
        data_ulid,
    );
    // `data_file_key` hardcodes the `.parquet` extension. For a Vortex
    // table swap the trailing extension (the filename is `{ulid}.vortex`;
    // ULIDs contain no `.`, so the suffix swap is unambiguous). Tables are
    // single-format, so this is a clean per-file rename, not a mixed set.
    let key = match opts.file_format {
        FileFormat::Parquet => key,
        FileFormat::Vortex => {
            let s = key.to_string();
            let swapped = s
                .strip_suffix(".parquet")
                .map(|base| format!("{base}.{}", FileFormat::Vortex.extension()))
                .unwrap_or(s);
            object_store::path::Path::from(swapped)
        }
    };

    // Phase 5.7 B2: physically sort the batch by `cluster_columns` before
    // flushing so related rows live in the same row group. Skipped when
    // the cluster spec is empty — the common case. The sorted batch is
    // local; we don't mutate the caller's input.
    let sorted_batch_owned;
    let batch_to_write = if opts.cluster_columns.is_empty() {
        batch
    } else {
        sorted_batch_owned = sort_batch_by_cluster_cols(batch, &opts.cluster_columns)?;
        &sorted_batch_owned
    };

    let bytes = match opts.file_format {
        FileFormat::Parquet => encode_parquet(batch_to_write, opts)?,
        FileFormat::Vortex => {
            crate::vortex_format::encode(batch_to_write, opts.row_block_size).await?
        }
    };
    let row_count = batch_to_write.num_rows() as u64;

    // Stats are extracted from the *plaintext* bytes regardless of whether
    // envelope encryption is on — the catalog's per-file ColumnStats need
    // to round-trip the typed min/max, and we don't want to re-decrypt at
    // commit time. For Vortex, min/max stat extraction is a later
    // increment: an empty map means "no per-file pruning info", which the
    // catalog treats conservatively (scan the file) — correct, just less
    // pruning. Parquet behaviour is unchanged.
    let column_stats = match opts.file_format {
        FileFormat::Parquet => extract_column_stats(&bytes, batch_to_write)?,
        // Vortex stats from the in-memory batch (same source Parquet uses)
        // — NOT by re-opening the encoded blob, which doubled insert
        // latency. Same byte contract / type gate; differential-harness
        // gated.
        FileFormat::Vortex => crate::vortex_format::column_stats_from_batch(batch_to_write),
    };

    // Phase 5.14.A2: compute per-column fastbloom filters for columns named
    // in WriteOptions::bloom_columns. Only Int64 and Utf8 are supported in
    // phase 1; other types are silently skipped (no entry in bloom_filters).
    let bloom_filters =
        compute_bloom_filters(batch_to_write, &opts.bloom_columns, row_count);

    // Phase 5.14.B2: compute per-column HLL and t-digest sketches for all
    // eligible columns in the batch. HLL covers cardinality-meaningful types
    // (int, string, bytes, date, timestamp); t-digest covers numeric types
    // (int, float, decimal). Both are keyed by column name.
    let (hll_sketches, tdigest_sketches) = compute_sketches(batch_to_write);

    // Envelope-encrypt the body if a provider is attached. The on-disk
    // layout in that case is `nonce(12) || ciphertext_with_tag` and a
    // `<key>.wrapped` sidecar carrying the wrapped data key. With no
    // provider attached, the file is the plaintext Parquet bytes —
    // byte-for-byte the legacy path.
    //
    // When a catalog is also attached and the project has a
    // [`ProjectStorageConfig`] persisted, we route through
    // `wrap_key_with_config` so the provider can resolve a per-project
    // CMK; otherwise we fall back to plain `wrap_key`. The cache means
    // the catalog round-trip happens at most once per project per
    // process (until invalidated by `set_project_storage_config`).
    let (body_to_put, wrapped_for_sidecar) = match storage.encryption_provider() {
        Some(provider) => {
            let cfg = storage.project_storage_config_cached(project).await?;
            let (data_key, wrapped) = match cfg {
                Some(cfg) => provider.wrap_key_with_config(project, &cfg).await?,
                None => provider.wrap_key(project).await?,
            };
            let envelope = encrypt_envelope(&data_key, &bytes)?;
            (envelope, Some(wrapped))
        }
        None => (bytes, None),
    };
    let size = body_to_put.len() as u64;

    storage
        .project_store(project)
        .put(&key, PutPayload::from_bytes(Bytes::from(body_to_put)))
        .await
        .map_err(|e| BasinError::storage(format!("put {key}: {e}")))?;

    if let Some(wrapped) = wrapped_for_sidecar {
        let sidecar_key = wrapped_sidecar_key(&key);
        storage
            .project_store(project)
            .put(&sidecar_key, PutPayload::from_bytes(Bytes::from(wrapped.0)))
            .await
            .map_err(|e| BasinError::storage(format!("put sidecar {sidecar_key}: {e}")))?;
    }

    // Per-project byte counter (Phase 6 telemetry). No-op when no registry is
    // attached. Bumped after a successful PUT — failed writes don't count.
    if let Some(tc) = storage.project_counters(project) {
        tc.record_bytes_written(size);
    }

    // Build and persist HNSW sidecars for any FixedSizeList<Float32> columns
    // in the batch. One sidecar per Parquet write, mirroring the data-file
    // pattern; merging across writes is deferred to the future compactor.
    crate::vector_index::build_indexes_for_batch(
        storage,
        project,
        table,
        batch_to_write,
        data_ulid,
    )
    .await?;

    Ok(DataFile {
        path: key,
        size_bytes: size,
        row_count,
        column_stats,
        // New writes always land in the hot tier. The compactor migrates files
        // to cold later via `Storage::migrate_to_cold`.
        tier: crate::tier::Tier::Hot,
        bloom_filters,
        hll_sketches,
        tdigest_sketches,
    })
}

/// Phase 5.7 B2: physically reorder `batch` so rows with equal-or-close
/// values on `cluster_columns` end up adjacent (and therefore in the same
/// Parquet row group). Unknown column names are silently ignored — the
/// engine's catalog-validated set is the canonical source; a stray here
/// just means schema-evolved-out and skipping it is the right back-compat.
fn sort_batch_by_cluster_cols(
    batch: &RecordBatch,
    cluster_columns: &[String],
) -> Result<RecordBatch> {
    use arrow::compute::{lexsort_to_indices, take, SortColumn};
    let schema = batch.schema();
    let sort_cols: Vec<SortColumn> = cluster_columns
        .iter()
        .filter_map(|name| {
            schema.index_of(name.as_str()).ok().map(|idx| SortColumn {
                values: batch.column(idx).clone(),
                options: None,
            })
        })
        .collect();
    if sort_cols.is_empty() {
        // Every column was unknown — nothing to sort by. Same shape as
        // empty `cluster_columns`: fall through with a clone so the
        // caller can still treat the result uniformly.
        return Ok(batch.clone());
    }
    let indices = lexsort_to_indices(&sort_cols, None)
        .map_err(|e| BasinError::storage(format!("lexsort cluster columns: {e}")))?;
    let columns = batch
        .columns()
        .iter()
        .map(|c| {
            take(c.as_ref(), &indices, None)
                .map_err(|e| BasinError::storage(format!("take cluster-sorted column: {e}")))
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|e| BasinError::storage(format!("rebuild cluster-sorted batch: {e}")))
}

fn encode_parquet(batch: &RecordBatch, opts: &WriteOptions) -> Result<Vec<u8>> {
    // ZSTD level 1: ~3x faster writes than ZSTD-3 with only a few percent
    // worse compression on log-shaped data. Audit-log retention still beats
    // CSV by an order of magnitude (the wedge claim), and the synchronous
    // write path (no WAL yet) doesn't crater. Once basin-wal lands and the
    // background compactor exists, the long-tail Parquet files can be
    // re-encoded at ZSTD-3 or ZSTD-9 for archival storage.
    //
    // Priority: `row_block_size` (from WITH clause) > `max_row_group_size`
    // (from ALTER TABLE) > the built-in default.
    let max_row_group_size = opts
        .row_block_size
        .map(|v| v as usize)
        .or(opts.max_row_group_size)
        .unwrap_or(DEFAULT_MAX_ROW_GROUP_SIZE);
    let mut builder = WriterProperties::builder()
        .set_max_row_group_size(max_row_group_size)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(1).expect("ZSTD level 1 is valid"),
        ));

    // Bloom filters are configured per-column. We only enable them on
    // columns the caller asked for so the default (empty list) is
    // byte-equivalent to the pre-bloom Parquet output. Each column gets the
    // same default `(NDV, FPP)` knobs; per-column overrides are a future
    // catalog hook.
    for col_name in &opts.bloom_filter_columns {
        let col = ColumnPath::from(col_name.as_str());
        builder = builder
            .set_column_bloom_filter_enabled(col.clone(), true)
            .set_column_bloom_filter_ndv(col.clone(), DEFAULT_BLOOM_NDV)
            .set_column_bloom_filter_fpp(col, DEFAULT_BLOOM_FPP);
    }

    let props = builder.build();

    let mut buf: Vec<u8> = Vec::with_capacity(batch.get_array_memory_size());
    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .map_err(|e| BasinError::storage(format!("parquet writer init: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| BasinError::storage(format!("parquet write: {e}")))?;
        writer
            .close()
            .map_err(|e| BasinError::storage(format!("parquet close: {e}")))?;
    }
    Ok(buf)
}

fn extract_column_stats(
    parquet_bytes: &[u8],
    batch: &RecordBatch,
) -> Result<BTreeMap<String, ColumnStats>> {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let cursor = Bytes::copy_from_slice(parquet_bytes);
    let reader = SerializedFileReader::new(cursor)
        .map_err(|e| BasinError::storage(format!("parquet reader: {e}")))?;
    let meta = reader.metadata();

    let mut out: BTreeMap<String, ColumnStats> = BTreeMap::new();
    let schema = batch.schema();
    for f in schema.fields() {
        out.insert(f.name().clone(), ColumnStats::default());
    }

    // Aggregate across row groups using typed comparisons. The naive
    // lexicographic merge from earlier was wrong for primitive types whose
    // byte representation isn't order-preserving (e.g. little-endian
    // integers, or any negative number). The pruning consumer relies on
    // these bytes round-tripping back to the original min/max.
    for rg in meta.row_groups() {
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
    Ok(out)
}

/// Merge a row-group's stats into the file-level entry, using a
/// type-appropriate comparison so the running `min` is the actual smallest
/// value across row groups (same for `max`). The bytes we store are the
/// raw Parquet PLAIN encoding, which the pruning helper decodes per
/// `DataType` to recover the typed value.
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
                let bytes = vec![if *min { 1u8 } else { 0u8 }];
                let prev_min = entry
                    .min_bytes
                    .as_deref()
                    .and_then(|b| b.first().copied())
                    .map(|b| b != 0);
                if !matches!(prev_min, Some(prev) if prev <= *min) {
                    entry.min_bytes = Some(bytes);
                }
            }
            if let Some(max) = s.max_opt() {
                let bytes = vec![if *max { 1u8 } else { 0u8 }];
                let prev_max = entry
                    .max_bytes
                    .as_deref()
                    .and_then(|b| b.first().copied())
                    .map(|b| b != 0);
                if !matches!(prev_max, Some(prev) if prev >= *max) {
                    entry.max_bytes = Some(bytes);
                }
            }
        }
        // Utf8 / ByteArray: lexicographic comparison is the same comparison
        // SQL uses for strings, so byte-wise merge is correct.
        ParquetStats::ByteArray(_) | ParquetStats::FixedLenByteArray(_) => {
            if let Some(min_bytes) = stats.min_bytes_opt() {
                let v = min_bytes.to_vec();
                let keep = match entry.min_bytes.as_deref() {
                    Some(prev) => prev > v.as_slice(),
                    None => true,
                };
                if keep {
                    entry.min_bytes = Some(v);
                }
            }
            if let Some(max_bytes) = stats.max_bytes_opt() {
                let v = max_bytes.to_vec();
                let keep = match entry.max_bytes.as_deref() {
                    Some(prev) => prev < v.as_slice(),
                    None => true,
                };
                if keep {
                    entry.max_bytes = Some(v);
                }
            }
        }
        // Other primitive types fall through with raw bytes; pruning will
        // see them as Mixed (it only decodes the types we model in
        // ScalarValue).
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

/// Phase 5.14.B2: compute per-column HLL and t-digest sketches.
///
/// **HLL** (cardinality): computed for `Int8`, `Int16`, `Int32`, `Int64`,
/// `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Utf8`, `LargeUtf8`, `Binary`,
/// `LargeBinary`, `Date32`, `Date64`, `Timestamp(*, _)`. Skipped for
/// `Float*`, `Boolean`, `Null`, and any complex types — APPROX_COUNT_DISTINCT
/// is not meaningful there.
///
/// **t-digest** (quantiles): computed for `Int*`, `UInt*`, `Float32`,
/// `Float64`, `Decimal128`. Skipped for string/bytes/bool/null/date/timestamp
/// — quantile queries on those types use min/max, not t-digest.
///
/// Returns `(hll_sketches, tdigest_sketches)` keyed by column name.
/// Columns that are all-null still produce a (zero-filled) sketch entry —
/// the downstream merging path needs a consistent schema.
fn compute_sketches(
    batch: &RecordBatch,
) -> (BTreeMap<String, Vec<u8>>, BTreeMap<String, Vec<u8>>) {
    use arrow_array::cast::AsArray;
    use arrow_array::Array;
    use arrow_schema::DataType;
    use basin_sketch::hll::Hll;
    use basin_sketch::tdigest::TDigest;

    let mut hlls: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let mut tdigests: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let schema = batch.schema();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let col_name = field.name().clone();
        let col = batch.column(col_idx);
        let dtype = col.data_type();

        // ── HLL (cardinality) ────────────────────────────────────────────
        let hll_eligible = matches!(
            dtype,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::Date32
                | DataType::Date64
        ) || matches!(dtype, DataType::Timestamp(_, _));

        if hll_eligible {
            let mut hll = Hll::new();
            match dtype {
                DataType::Int8 => {
                    let arr = col.as_primitive::<arrow_array::types::Int8Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            hll.insert(&arr.value(i).to_le_bytes());
                        }
                    }
                }
                DataType::Int16 => {
                    let arr = col.as_primitive::<arrow_array::types::Int16Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            hll.insert(&arr.value(i).to_le_bytes());
                        }
                    }
                }
                DataType::Int32 => {
                    let arr = col.as_primitive::<arrow_array::types::Int32Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            hll.insert(&arr.value(i).to_le_bytes());
                        }
                    }
                }
                DataType::Int64 => {
                    let arr = col.as_primitive::<arrow_array::types::Int64Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            hll.insert(&arr.value(i).to_le_bytes());
                        }
                    }
                }
                DataType::UInt8 => {
                    let arr = col.as_primitive::<arrow_array::types::UInt8Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            hll.insert(&arr.value(i).to_le_bytes());
                        }
                    }
                }
                DataType::UInt16 => {
                    let arr = col.as_primitive::<arrow_array::types::UInt16Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            hll.insert(&arr.value(i).to_le_bytes());
                        }
                    }
                }
                DataType::UInt32 => {
                    let arr = col.as_primitive::<arrow_array::types::UInt32Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            hll.insert(&arr.value(i).to_le_bytes());
                        }
                    }
                }
                DataType::UInt64 => {
                    let arr = col.as_primitive::<arrow_array::types::UInt64Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            hll.insert(&arr.value(i).to_le_bytes());
                        }
                    }
                }
                DataType::Date32 => {
                    let arr = col.as_primitive::<arrow_array::types::Date32Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            hll.insert(&arr.value(i).to_le_bytes());
                        }
                    }
                }
                DataType::Date64 => {
                    let arr = col.as_primitive::<arrow_array::types::Date64Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            hll.insert(&arr.value(i).to_le_bytes());
                        }
                    }
                }
                DataType::Timestamp(unit, _) => {
                    use arrow_schema::TimeUnit;
                    match unit {
                        TimeUnit::Second => {
                            let arr = col.as_primitive::<arrow_array::types::TimestampSecondType>();
                            for i in 0..arr.len() {
                                if arr.is_valid(i) {
                                    hll.insert(&arr.value(i).to_le_bytes());
                                }
                            }
                        }
                        TimeUnit::Millisecond => {
                            let arr = col
                                .as_primitive::<arrow_array::types::TimestampMillisecondType>();
                            for i in 0..arr.len() {
                                if arr.is_valid(i) {
                                    hll.insert(&arr.value(i).to_le_bytes());
                                }
                            }
                        }
                        TimeUnit::Microsecond => {
                            let arr = col
                                .as_primitive::<arrow_array::types::TimestampMicrosecondType>();
                            for i in 0..arr.len() {
                                if arr.is_valid(i) {
                                    hll.insert(&arr.value(i).to_le_bytes());
                                }
                            }
                        }
                        TimeUnit::Nanosecond => {
                            let arr = col
                                .as_primitive::<arrow_array::types::TimestampNanosecondType>();
                            for i in 0..arr.len() {
                                if arr.is_valid(i) {
                                    hll.insert(&arr.value(i).to_le_bytes());
                                }
                            }
                        }
                    }
                }
                DataType::Utf8 => {
                    let arr = col.as_string::<i32>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            hll.insert(arr.value(i).as_bytes());
                        }
                    }
                }
                DataType::LargeUtf8 => {
                    let arr = col.as_string::<i64>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            hll.insert(arr.value(i).as_bytes());
                        }
                    }
                }
                DataType::Binary => {
                    let arr = col.as_binary::<i32>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            hll.insert(arr.value(i));
                        }
                    }
                }
                DataType::LargeBinary => {
                    let arr = col.as_binary::<i64>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            hll.insert(arr.value(i));
                        }
                    }
                }
                _ => {} // already gated by hll_eligible above
            }
            hlls.insert(col_name.clone(), hll.to_bytes());
        }

        // ── t-digest (quantiles) ─────────────────────────────────────────
        let tdigest_eligible = matches!(
            dtype,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
        ) || matches!(dtype, DataType::Decimal128(_, _));

        if tdigest_eligible {
            let mut td = TDigest::new();
            match dtype {
                DataType::Int8 => {
                    let arr = col.as_primitive::<arrow_array::types::Int8Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            td.add(arr.value(i) as f64);
                        }
                    }
                }
                DataType::Int16 => {
                    let arr = col.as_primitive::<arrow_array::types::Int16Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            td.add(arr.value(i) as f64);
                        }
                    }
                }
                DataType::Int32 => {
                    let arr = col.as_primitive::<arrow_array::types::Int32Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            td.add(arr.value(i) as f64);
                        }
                    }
                }
                DataType::Int64 => {
                    let arr = col.as_primitive::<arrow_array::types::Int64Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            td.add(arr.value(i) as f64);
                        }
                    }
                }
                DataType::UInt8 => {
                    let arr = col.as_primitive::<arrow_array::types::UInt8Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            td.add(arr.value(i) as f64);
                        }
                    }
                }
                DataType::UInt16 => {
                    let arr = col.as_primitive::<arrow_array::types::UInt16Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            td.add(arr.value(i) as f64);
                        }
                    }
                }
                DataType::UInt32 => {
                    let arr = col.as_primitive::<arrow_array::types::UInt32Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            td.add(arr.value(i) as f64);
                        }
                    }
                }
                DataType::UInt64 => {
                    let arr = col.as_primitive::<arrow_array::types::UInt64Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            td.add(arr.value(i) as f64);
                        }
                    }
                }
                DataType::Float32 => {
                    let arr = col.as_primitive::<arrow_array::types::Float32Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            let v = arr.value(i) as f64;
                            if v.is_finite() {
                                td.add(v);
                            }
                        }
                    }
                }
                DataType::Float64 => {
                    let arr = col.as_primitive::<arrow_array::types::Float64Type>();
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            let v = arr.value(i);
                            if v.is_finite() {
                                td.add(v);
                            }
                        }
                    }
                }
                DataType::Decimal128(_, scale) => {
                    let scale = *scale;
                    let arr = col.as_primitive::<arrow_array::types::Decimal128Type>();
                    let divisor = 10f64.powi(scale as i32);
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            td.add(arr.value(i) as f64 / divisor);
                        }
                    }
                }
                _ => {} // already gated by tdigest_eligible above
            }
            td.compress();
            tdigests.insert(col_name, td.to_bytes());
        }
    }

    (hlls, tdigests)
}

/// Phase 5.14.A2: compute fastbloom filters for the specified columns.
///
/// For each column in `bloom_cols` that exists in `batch` and has a supported
/// Arrow type (`Int64` or `Utf8`), we:
///   1. Build a `BloomFilter` sized for `n` items at 1% FPP.
///   2. Insert every non-null value (serialised to bytes as documented on
///      `DataFile::bloom_filters`).
///   3. Serialise to our wire format:
///        `[num_hashes: u32 LE (4 bytes)] || [u64 words, each 8 bytes LE]`
///
/// The serialisation is stable across restarts because fastbloom's
/// `DefaultHasher` is seeded deterministically (seed = 0 by default) and
/// the u64 words are native-endian on write. We force LE on wire by
/// iterating words and calling `to_le_bytes()`.
fn compute_bloom_filters(
    batch: &RecordBatch,
    bloom_cols: &[String],
    n: u64,
) -> BTreeMap<String, Vec<u8>> {
    use arrow_array::cast::AsArray;
    use arrow_array::Array;
    use arrow_schema::DataType;
    use fastbloom::BloomFilter;

    let mut out: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    if bloom_cols.is_empty() {
        return out;
    }
    // Clamp n to at least 1 to avoid a zero-size bloom.
    let n_items = (n as usize).max(1);
    let schema = batch.schema();

    for col_name in bloom_cols {
        let Some(col_idx) = schema.index_of(col_name.as_str()).ok() else {
            continue;
        };
        let col = batch.column(col_idx);
        // Seed 0 is deterministic so bloom_from_bytes can reconstruct the
        // same hasher at probe time. The DefaultHasher::default() path uses
        // getrandom for its seed, which would make every deserialized probe
        // use a different hash function from the one used at insert time,
        // producing near-100% false-negative rates.
        let mut filter =
            BloomFilter::with_false_pos(DEFAULT_BLOOM_FPP).seed(&0u128).expected_items(n_items);

        match col.data_type() {
            DataType::Int64 => {
                let arr = col.as_primitive::<arrow_array::types::Int64Type>();
                for i in 0..arr.len() {
                    if arr.is_valid(i) {
                        let bytes = arr.value(i).to_le_bytes();
                        filter.insert(bytes.as_ref());
                    }
                }
            }
            DataType::Utf8 => {
                let arr = col.as_string::<i32>();
                for i in 0..arr.len() {
                    if arr.is_valid(i) {
                        filter.insert(arr.value(i).as_bytes());
                    }
                }
            }
            // Phase 1: only Int64 and Utf8 are bloomed; skip all other types.
            _ => continue,
        }

        out.insert(col_name.clone(), bloom_to_bytes(&filter));
    }
    out
}

/// Serialise a `BloomFilter` to the wire format used by `DataFile::bloom_filters`:
///   `[num_hashes: u32 LE (4 bytes)] || [u64 words, each 8 bytes LE]`
pub fn bloom_to_bytes(filter: &fastbloom::BloomFilter) -> Vec<u8> {
    let num_hashes = filter.num_hashes();
    let words = filter.as_slice();
    let mut buf = Vec::with_capacity(4 + words.len() * 8);
    buf.extend_from_slice(&num_hashes.to_le_bytes());
    for &w in words {
        buf.extend_from_slice(&w.to_le_bytes());
    }
    buf
}

/// Deserialise bytes written by `bloom_to_bytes` back into a `BloomFilter`.
/// Returns `None` if the bytes are malformed (wrong length, zero words).
pub fn bloom_from_bytes(bytes: &[u8]) -> Option<fastbloom::BloomFilter> {
    if bytes.len() < 4 {
        return None;
    }
    let num_hashes = u32::from_le_bytes(bytes[..4].try_into().ok()?);
    let rest = &bytes[4..];
    if rest.len() % 8 != 0 || rest.is_empty() {
        return None;
    }
    let words: Vec<u64> = rest
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    // Seed 0 matches the deterministic seed used at write time in
    // `compute_bloom_filters`. Without a matching seed the hasher diverges
    // from the one used during insert, making contains() unreliable.
    Some(
        fastbloom::BloomFilter::from_vec(words).seed(&0u128).hashes(num_hashes),
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(ids: &[i64], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let id_arr: Int64Array = ids.iter().copied().collect();
        let name_arr: StringArray = names.iter().map(|s| Some(*s)).collect();
        RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(name_arr)]).unwrap()
    }

    fn ids(b: &RecordBatch) -> Vec<i64> {
        b.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn sort_orders_by_single_cluster_column() {
        let b = batch(&[3, 1, 2], &["c", "a", "b"]);
        let sorted = sort_batch_by_cluster_cols(&b, &["id".into()]).unwrap();
        assert_eq!(ids(&sorted), vec![1, 2, 3]);
    }

    #[test]
    fn sort_is_lexicographic_across_columns() {
        // (id, name) cluster: equal ids tie-break on name.
        let b = batch(&[2, 1, 2, 1], &["b", "z", "a", "a"]);
        let sorted = sort_batch_by_cluster_cols(&b, &["id".into(), "name".into()]).unwrap();
        // Expect rows reordered to (1,"a"), (1,"z"), (2,"a"), (2,"b").
        let names: Vec<String> = sorted
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap().to_string())
            .collect();
        assert_eq!(ids(&sorted), vec![1, 1, 2, 2]);
        assert_eq!(names, vec!["a", "z", "a", "b"]);
    }

    #[test]
    fn sort_with_unknown_column_falls_through() {
        // A column name that's not in the schema is silently skipped; if
        // every name is unknown, the batch passes through unchanged.
        let b = batch(&[3, 1, 2], &["c", "a", "b"]);
        let sorted = sort_batch_by_cluster_cols(&b, &["nonexistent".into()]).unwrap();
        assert_eq!(ids(&sorted), vec![3, 1, 2]);
    }

    #[test]
    fn sort_empty_batch_is_a_noop() {
        let b = batch(&[], &[]);
        let sorted = sort_batch_by_cluster_cols(&b, &["id".into()]).unwrap();
        assert_eq!(sorted.num_rows(), 0);
    }

    /// Phase 5.14.B2 gate: write 10k rows with mixed dtypes; assert that
    /// hll_sketches and tdigest_sketches are non-empty for eligible columns
    /// and absent for skipped dtypes (Float and Bool).
    #[test]
    fn sketches_present_for_eligible_absent_for_skipped() {
        use basin_sketch::hll::Hll;
        use basin_sketch::tdigest::TDigest;

        const N: usize = 10_000;

        // Build a batch with: Int64 (eligible both), Float64 (tdigest only),
        // Utf8 (HLL only), Boolean (neither).
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("flag", DataType::Boolean, false),
        ]));
        let id_arr: Int64Array = (0i64..N as i64).collect();
        let score_arr: Float64Array = (0..N).map(|i| (i as f64) * 0.1).collect();
        let label_arr: StringArray = (0..N).map(|i| Some(format!("label_{}", i % 500))).collect();
        let flag_arr: BooleanArray = (0..N).map(|i| Some(i % 2 == 0)).collect();

        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_arr),
                Arc::new(score_arr),
                Arc::new(label_arr),
                Arc::new(flag_arr),
            ],
        )
        .unwrap();

        let (hlls, tdigests) = compute_sketches(&b);

        // Int64: both HLL and t-digest
        assert!(hlls.contains_key("id"), "id must have HLL sketch");
        assert!(tdigests.contains_key("id"), "id must have t-digest sketch");

        // Float64: t-digest only, no HLL
        assert!(!hlls.contains_key("score"), "score (Float64) must NOT have HLL sketch");
        assert!(tdigests.contains_key("score"), "score (Float64) must have t-digest sketch");

        // Utf8: HLL only, no t-digest
        assert!(hlls.contains_key("label"), "label must have HLL sketch");
        assert!(!tdigests.contains_key("label"), "label (Utf8) must NOT have t-digest sketch");

        // Boolean: neither
        assert!(!hlls.contains_key("flag"), "flag (Boolean) must NOT have HLL sketch");
        assert!(!tdigests.contains_key("flag"), "flag (Boolean) must NOT have t-digest sketch");

        // Validate byte lengths are non-zero and round-trip correctly.
        let hll_bytes = hlls.get("id").unwrap();
        assert!(!hll_bytes.is_empty(), "HLL bytes must be non-empty");
        Hll::from_bytes(hll_bytes).expect("HLL bytes must deserialise");

        let td_bytes = tdigests.get("id").unwrap();
        assert!(!td_bytes.is_empty(), "t-digest bytes must be non-empty");
        TDigest::from_bytes(td_bytes).expect("t-digest bytes must deserialise");
    }

    /// Phase 5.14.B2 differential gate: HLL count_distinct ±5% of actual;
    /// t-digest bytes round-trip cleanly and median estimate is ±5% of true.
    #[test]
    fn sketches_round_trip_and_accuracy() {
        use basin_sketch::hll::Hll;
        use basin_sketch::tdigest::TDigest;

        const N: usize = 10_000;
        // 2 500 distinct string labels out of 10 000 rows.
        const N_DISTINCT_LABELS: usize = 2_500;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let id_arr: Int64Array = (0i64..N as i64).collect();
        let label_arr: StringArray = (0..N)
            .map(|i| Some(format!("lbl_{}", i % N_DISTINCT_LABELS)))
            .collect();

        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(id_arr), Arc::new(label_arr)],
        )
        .unwrap();

        let (hlls, tdigests) = compute_sketches(&b);

        // ── HLL accuracy: id has N distinct values ──────────────────────
        let hll_bytes = hlls.get("id").unwrap();
        let hll = Hll::from_bytes(hll_bytes).expect("HLL round-trip");
        let est = hll.cardinality() as f64;
        let err = (est - N as f64).abs() / N as f64;
        assert!(
            err <= 0.05,
            "HLL(id): estimate={est} actual={N} err={:.2}%",
            err * 100.0
        );

        // ── HLL accuracy: label has N_DISTINCT_LABELS distinct values ───
        let hll_bytes = hlls.get("label").unwrap();
        let hll = Hll::from_bytes(hll_bytes).expect("HLL label round-trip");
        let est = hll.cardinality() as f64;
        let err = (est - N_DISTINCT_LABELS as f64).abs() / N_DISTINCT_LABELS as f64;
        assert!(
            err <= 0.05,
            "HLL(label): estimate={est} actual={N_DISTINCT_LABELS} err={:.2}%",
            err * 100.0
        );

        // ── t-digest round-trip and median accuracy ──────────────────────
        // id is 0..10000; true median is 4999.5
        let td_bytes = tdigests.get("id").unwrap();
        let mut td = TDigest::from_bytes(td_bytes).expect("t-digest round-trip");
        let median = td.quantile(0.5);
        let err = (median - 4999.5).abs() / N as f64;
        assert!(
            err <= 0.05,
            "t-digest(id) median={median:.2} true=4999.5 err={:.2}%",
            err * 100.0
        );
    }
}
