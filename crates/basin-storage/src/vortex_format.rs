//! Vortex columnar storage codec (opt-in `WITH (basin.file_format='vortex')`).
//! Mirrors the proven benchmark/vortex_compare write/scan path; BtrBlocks
//! cascade (zstd strings + pco numerics).
//!
//! Phase 0/1: this module is self-contained — it only converts between Arrow
//! `RecordBatch`es and an in-memory Vortex byte blob. The writer/reader/session
//! wiring that selects this codec is a later phase.

use std::sync::{Arc, OnceLock};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use basin_common::{BasinError, Result};
use futures::StreamExt;
use vortex_array::arrow::{ArrowArrayExecutor, FromArrowArray};
use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_array::{ArrayRef, VortexSessionExecute};
use vortex_btrblocks::schemes::{float, integer, string};
use vortex_btrblocks::{BtrBlocksCompressorBuilder, SchemeExt};
use vortex_buffer::ByteBufferMut;

use crate::writer::EncodingMode;
use vortex_file::{
    register_default_encodings, OpenOptionsSessionExt, WriteOptionsSessionExt, WriteStrategyBuilder,
};
use vortex_io::session::{RuntimeSession, RuntimeSessionExt};
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;

/// Process-wide Vortex session. Built once (encoding registration is not free)
/// and reused for every encode/decode call. Mirrors
/// `benchmark/vortex_compare::build_vortex_session()` verbatim.
fn session() -> &'static VortexSession {
    static SESSION: OnceLock<VortexSession> = OnceLock::new();
    SESSION.get_or_init(|| {
        let session = VortexSession::empty()
            .with::<ArraySession>()
            .with::<LayoutSession>()
            .with::<ScalarFnSession>()
            .with::<RuntimeSession>()
            .with_tokio();
        register_default_encodings(&session);
        session
    })
}

/// Larger row block for [`EncodingMode::Fast`] when the caller hasn't pinned
/// one via `basin.row_block_size`. The default is 8192; a bigger block means
/// fewer chunk boundaries / zone maps per column, which cuts the per-chunk
/// compressor-dispatch and stats-accumulation overhead on a big bulk batch.
/// Stays well under a typical 65k+ bulk batch so multi-chunk files still form.
const FAST_DEFAULT_ROW_BLOCK_SIZE: usize = 32_768;

/// Encode with the legacy aggressive cascade ([`EncodingMode::Best`]). Kept as
/// a thin wrapper so existing call sites and tests that don't care about the
/// 2-pass selector stay byte-identical to the pre-#92 path.
pub(crate) async fn encode(batch: &RecordBatch, row_block_size: Option<u32>) -> Result<Vec<u8>> {
    encode_with_mode(batch, row_block_size, EncodingMode::Best).await
}

/// Encode one Arrow `RecordBatch` into a self-describing Vortex byte blob.
///
/// `mode` selects the BtrBlocks cascade (#92):
/// * [`EncodingMode::Best`] — the full `with_compact` cascade (zstd strings +
///   pco numerics, every scheme, ~1%-sample per-column ratio search). Smallest
///   files; byte-identical to the pre-#92 write path.
/// * [`EncodingMode::Fast`] — drops the expensive *search* schemes
///   (dictionary / FSST / RLE / Pco), so each column takes a single cheap
///   structure-preserving encoding (constant / FoR / bit-packing / ALP /
///   decimal / temporal) with a Zstd fallback for strings — skipping the
///   per-column sampling search that dominates bulk-INSERT encode time. When
///   the caller hasn't pinned `row_block_size`, Fast also widens the default
///   block to [`FAST_DEFAULT_ROW_BLOCK_SIZE`].
///
/// `row_block_size` – when `Some(n)`, forwards `n` to
/// `WriteStrategyBuilder::with_row_block_size` to control Vortex chunk
/// granularity. `None` keeps the per-mode default (8192 for Best, the wider
/// [`FAST_DEFAULT_ROW_BLOCK_SIZE`] for Fast).
pub(crate) async fn encode_with_mode(
    batch: &RecordBatch,
    row_block_size: Option<u32>,
    mode: EncodingMode,
) -> Result<Vec<u8>> {
    // Arrow RecordBatch -> Vortex struct array. The top-level struct must be
    // NON-nullable: vortex 0.70's `FileStatsAccumulator` rejects nullable
    // top-level structs ("Use Validity::NonNullable"). Per-COLUMN nullability
    // is independent — it is taken from each Arrow field's `is_nullable()`
    // inside `from_arrow`, so nullable columns still round-trip their nulls.
    let varr: ArrayRef = FromArrowArray::from_arrow(batch, false)
        .map_err(|e| BasinError::storage(format!("vortex: from_arrow encode: {e}")))?;

    let compressor = btrblocks_builder_for(mode);
    let mut builder = WriteStrategyBuilder::default().with_btrblocks_builder(compressor);
    match row_block_size {
        Some(sz) => builder = builder.with_row_block_size(sz as usize),
        None if mode == EncodingMode::Fast => {
            builder = builder.with_row_block_size(FAST_DEFAULT_ROW_BLOCK_SIZE)
        }
        None => {} // Best mode keeps the Vortex default (8192).
    }
    let strategy = builder.build();

    let mut buf = ByteBufferMut::empty();
    session()
        .write_options()
        .with_strategy(strategy)
        .write(&mut buf, varr.to_array_stream())
        .await
        .map_err(|e| BasinError::storage(format!("vortex: write: {e}")))?;

    Ok(buf.as_slice().to_vec())
}

/// Build the BtrBlocks compressor for the given [`EncodingMode`].
///
/// * `Best`: `default().with_compact()` — every scheme, the proven aggressive
///   strategy (zstd strings + pco numerics). Each candidate scheme estimates
///   its ratio by compressing a ~1% sample of the column, then the best wins;
///   that per-column search is exactly what makes Best slow.
/// * `Fast`: starts from the same `with_compact` set, then *excludes* the
///   schemes whose `matches` / ratio estimate is expensive — the integer and
///   string dictionaries, FSST, the integer/float run-length encoders, and the
///   Pco integer/float codecs. What remains is cheap structure-preserving
///   encodings (constant, FoR, ZigZag, bit-packing, sparse, ALP/ALP-RD,
///   decimal, temporal) plus the Zstd string fallback, so every Basin column
///   type still has at least one applicable scheme and round-trips losslessly
///   — including UUID-as-Decimal256 (DecimalScheme is retained).
fn btrblocks_builder_for(mode: EncodingMode) -> BtrBlocksCompressorBuilder {
    let builder = BtrBlocksCompressorBuilder::default().with_compact();
    match mode {
        EncodingMode::Best => builder,
        EncodingMode::Fast => builder.exclude_schemes([
            integer::IntDictScheme.id(),
            integer::IntRLEScheme.id(),
            integer::RunEndScheme.id(),
            integer::PcoScheme.id(),
            float::FloatDictScheme.id(),
            float::FloatRLEScheme.id(),
            float::PcoScheme.id(),
            string::StringDictScheme.id(),
            string::FSSTScheme.id(),
        ]),
    }
}


/// Per-column min/max/null-count computed directly from the **in-memory
/// Arrow batch** being written — the same source Parquet's
/// `extract_column_stats(&bytes, batch)` uses. This is the WRITE path:
/// it must NOT re-open the encoded Vortex blob (doing so doubled insert
/// latency — the blob open/parse is as expensive as the encode itself).
/// Same `ColumnStats` byte contract + type gate as [`column_stats`]
/// (i64/f64 8-byte LE + null-count), so catalog file-pruning treats
/// Vortex and Parquet identically; correctness gated by the
/// Vortex⇆Parquet differential harness.
pub(crate) fn column_stats_from_batch(
    batch: &RecordBatch,
) -> std::collections::BTreeMap<String, crate::data_file::ColumnStats> {
    use arrow_array::{
        Array, Float64Array, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow_schema::TimeUnit;

    let mut out = std::collections::BTreeMap::new();
    let schema = batch.schema();
    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        let null_count = Some(col.null_count() as u64);
        // sum_bytes uses the SAME 8-byte-LE i64/f64 contract as min/max
        // (the fast_aggregate decoder reads it identically). Type-gated to
        // exact Int64/Float64 — anything else stays None so the
        // metadata-only SUM path correctly bails to a full scan.
        let (min_bytes, max_bytes, sum_bytes) = match field.data_type() {
            DataType::Int64 => {
                let a = col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64 column");
                (
                    arrow::compute::min(a).map(|v| v.to_le_bytes().to_vec()),
                    arrow::compute::max(a).map(|v| v.to_le_bytes().to_vec()),
                    arrow::compute::sum(a).map(|v| v.to_le_bytes().to_vec()),
                )
            }
            DataType::Float64 => {
                let a = col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("Float64 column");
                (
                    arrow::compute::min(a).map(|v| v.to_le_bytes().to_vec()),
                    arrow::compute::max(a).map(|v| v.to_le_bytes().to_vec()),
                    arrow::compute::sum(a).map(|v| v.to_le_bytes().to_vec()),
                )
            }
            // Timestamp columns: emit min/max as native-unit i64-LE, matching
            // the Parquet TIMESTAMPTZ stat contract (int64 in the column's
            // unit) so Vortex and Parquet tables behave identically for
            // catalog stats pruning AND the tiering sweep's cold-age
            // classification (which decodes max_bytes as i64). Without this,
            // Vortex (the default format) had no timestamp stats, so
            // age-based tiering moved 0 files — the gap behind
            // viability_tiered_storage's #[ignore]. sum_bytes stays None
            // (SUM over a timestamp is not a metadata-aggregate shape, and
            // fast_aggregate type-gates to Int64/Float64 so it ignores these).
            DataType::Timestamp(unit, _) => {
                let (mn, mx): (Option<i64>, Option<i64>) = match unit {
                    TimeUnit::Second => {
                        let a = col
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .expect("TimestampSecond column");
                        (arrow::compute::min(a), arrow::compute::max(a))
                    }
                    TimeUnit::Millisecond => {
                        let a = col
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .expect("TimestampMillisecond column");
                        (arrow::compute::min(a), arrow::compute::max(a))
                    }
                    TimeUnit::Microsecond => {
                        let a = col
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .expect("TimestampMicrosecond column");
                        (arrow::compute::min(a), arrow::compute::max(a))
                    }
                    TimeUnit::Nanosecond => {
                        let a = col
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .expect("TimestampNanosecond column");
                        (arrow::compute::min(a), arrow::compute::max(a))
                    }
                };
                (
                    mn.map(|v| v.to_le_bytes().to_vec()),
                    mx.map(|v| v.to_le_bytes().to_vec()),
                    None,
                )
            }
            _ => (None, None, None),
        };
        out.insert(
            field.name().clone(),
            crate::data_file::ColumnStats {
                null_count,
                min_bytes,
                max_bytes,
                sum_bytes,
            },
        );
    }
    out
}

/// Single-`open_buffer` footer read returning BOTH the logical row count
/// and the per-column min/max/null-count, **footer only** (no data
/// decode). The list path (`list_data_files_with_stats`) needs both; one
/// open instead of two halves the per-file footer cost when listing many
/// Vortex files. (The write path uses [`column_stats_from_batch`] — it
/// must NOT re-open the encoded blob.)
///
/// Stats use Basin's `ColumnStats` byte contract so the catalog
/// file-pruning path (`evaluate_compound_for_pruning` / `file_outcome`)
/// treats Vortex and Parquet identically, and are **type-gated for
/// correctness** exactly like the filter-pushdown: `min`/`max` bytes are
/// emitted ONLY for columns whose Vortex dtype is precisely `i64`/`f64`
/// (8-byte little-endian, matching `reader::decode_le_i64`/`decode_le_f64`);
/// any other type leaves min/max `None` (no prune participation) rather
/// than risk an encoding mismatch that could wrongly prune a matching
/// file. The Vortex⇆Parquet differential harness is the correctness gate.
pub(crate) async fn footer_meta(
    bytes: bytes::Bytes,
) -> Result<(
    u64,
    std::collections::BTreeMap<String, crate::data_file::ColumnStats>,
)> {
    use vortex_array::dtype::{DType, PType};
    use vortex_array::expr::stats::Stat;

    let mut out = std::collections::BTreeMap::new();
    let vf = session()
        .open_options()
        .open_buffer(bytes)
        .map_err(|e| BasinError::storage(format!("vortex: open_buffer (footer): {e}")))?;
    let rc = vf.row_count();

    // Top-level struct field names, in field order (the same order the
    // file statistics are recorded in).
    let names: Vec<String> = match vf.dtype() {
        DType::Struct(sd, _) => sd.names().iter().map(|n| n.to_string()).collect(),
        _ => return Ok((rc, out)),
    };
    let Some(fs) = vf.file_stats() else {
        return Ok((rc, out));
    };

    for (idx, name) in names.iter().enumerate() {
        let (ss, col_dt) = fs.get(idx);

        let null_count = Stat::NullCount
            .dtype(col_dt)
            .and_then(|nc_dt| ss.get_as::<u64>(Stat::NullCount, &nc_dt))
            .and_then(|p| p.as_exact());

        // Only exact i64 / f64 columns get min/max/sum, in the 8-byte LE
        // contract `reader::decode_le_i64` / `decode_le_f64` (and the
        // fast_aggregate SUM decoder) expect. If the Vortex footer's Sum
        // stat isn't representable as the column's i64/f64 (e.g. a wider
        // accumulator), `get_as` yields None → sum_bytes None → the
        // metadata SUM path bails to a correct full scan; the differential
        // harness gates this.
        let (min_bytes, max_bytes, sum_bytes) = match col_dt {
            DType::Primitive(PType::I64, _) => {
                let mn = ss
                    .get_as::<i64>(Stat::Min, col_dt)
                    .and_then(|p| p.as_exact())
                    .map(|v| v.to_le_bytes().to_vec());
                let mx = ss
                    .get_as::<i64>(Stat::Max, col_dt)
                    .and_then(|p| p.as_exact())
                    .map(|v| v.to_le_bytes().to_vec());
                let sm = ss
                    .get_as::<i64>(Stat::Sum, col_dt)
                    .and_then(|p| p.as_exact())
                    .map(|v| v.to_le_bytes().to_vec());
                (mn, mx, sm)
            }
            DType::Primitive(PType::F64, _) => {
                let mn = ss
                    .get_as::<f64>(Stat::Min, col_dt)
                    .and_then(|p| p.as_exact())
                    .map(|v| v.to_le_bytes().to_vec());
                let mx = ss
                    .get_as::<f64>(Stat::Max, col_dt)
                    .and_then(|p| p.as_exact())
                    .map(|v| v.to_le_bytes().to_vec());
                let sm = ss
                    .get_as::<f64>(Stat::Sum, col_dt)
                    .and_then(|p| p.as_exact())
                    .map(|v| v.to_le_bytes().to_vec());
                (mn, mx, sm)
            }
            _ => (None, None, None),
        };

        if null_count.is_some()
            || min_bytes.is_some()
            || max_bytes.is_some()
            || sum_bytes.is_some()
        {
            out.insert(
                name.clone(),
                crate::data_file::ColumnStats {
                    null_count,
                    min_bytes,
                    max_bytes,
                    sum_bytes,
                },
            );
        }
    }
    Ok((rc, out))
}

/// Decode a Vortex byte blob back to `RecordBatch`es. One `RecordBatch` is
/// produced per Vortex chunk in the file.
///
/// `schema` is **optional and self-describing**, mirroring Parquet's footer:
/// * `Some(s)` — the table-aware read path supplies the authoritative
///   catalog schema; it drives projection and exact-type fidelity (e.g.
///   timestamp timezone, dictionary types).
/// * `None` — Basin's schema-less internal read paths (continuous-view
///   refresh, cron-job state, system tables). The Arrow schema is recovered
///   from the Vortex file's own `DType`, so a Vortex file is decodable with
///   nothing but its bytes, exactly like a Parquet file.
/// `projection` (column names, in order) and `filter` are **pushed into the
/// Vortex scan** — projection reads only those columns, and a pushed filter
/// drives Vortex's native zone-map chunk pruning (skip chunks the predicate
/// excludes) instead of decoding the whole file. Both are a pure
/// optimisation: the engine still re-applies the authoritative filter +
/// projection post-decode (see `reader::vortex_project_and_filter`), and any
/// pushdown error transparently falls back to a full decode, so results are
/// always correct.
///
/// Returns `(batches, filter_was_pushed)` where `filter_was_pushed` is `true`
/// iff a filter was supplied AND `decode_inner` succeeded with pushdown active
/// (i.e. did not fall back to a full decode). Callers use this to decide
/// whether the Arrow post-filter pass can be skipped.
pub(crate) async fn decode(
    bytes: bytes::Bytes,
    schema: Option<Arc<Schema>>,
    projection: Option<&[String]>,
    filter: Option<vortex_array::expr::Expression>,
) -> Result<(Vec<RecordBatch>, bool)> {
    // Pushdown is best-effort: a literal/column dtype mismatch or any other
    // scan error must never change results. Try with pushdown; on failure
    // retry once with a plain full decode (the path the engine post-filters
    // anyway). Only retry when pushdown was actually requested.
    // `bytes::Bytes` is cheap to clone (reference count bump only) so the
    // fallback path can reuse the same backing allocation.
    let has_filter = filter.is_some();
    let pushed = projection.is_some() || has_filter;
    match decode_inner(bytes.clone(), schema.clone(), projection, filter, None, None, 0).await {
        Ok(b) => Ok((b, has_filter)),
        Err(_) if pushed => decode_inner(bytes, schema, None, None, None, None, 0)
            .await
            .map(|b| (b, false)),
        Err(e) => Err(e),
    }
}

/// Recover the Arrow schema a Vortex file describes itself with, normalised
/// to Basin-canonical types (the same normalisation [`decode_inner`] applies
/// to its inferred full schema — `Utf8View`/`BinaryView` → `Utf8`/`Binary`).
///
/// A Vortex file is self-describing: its footer carries the DType, so the
/// schema is recoverable from the bytes alone, with no catalog. The footer
/// cache is reused (and populated on miss) exactly as [`decode_with_cache`]
/// does, so on a warm file this is a parse-free probe.
///
/// Callers that read a `.vortex` file *without* an authoritative catalog
/// schema (e.g. the UPDATE pre-image / `read_file_with_options` path) use
/// this to recover the physical column types so a point-read `Eq` predicate
/// can be type-safe-pushed into the scan — which is what activates Vortex's
/// native zone-map chunk pruning + selective (mask-applied) decode. Without
/// it, no filter is pushed, every surviving chunk decodes in full, and the
/// predicate is only applied by the Arrow post-filter pass.
pub(crate) fn infer_arrow_schema(
    bytes: &bytes::Bytes,
    cache: Option<&crate::vortex_footer_cache::VortexFooterCache>,
    path: Option<&object_store::path::Path>,
    size_bytes: u64,
) -> Result<Schema> {
    let cached_footer = match (cache, path) {
        (Some(c), Some(p)) => c.get(p, size_bytes),
        _ => None,
    };

    let vf = if let Some(footer) = cached_footer {
        session()
            .open_options()
            .with_footer(footer)
            .open_buffer(bytes.clone())
            .map_err(|e| {
                BasinError::storage(format!("vortex: open_buffer (cached footer): {e}"))
            })?
    } else {
        let vf = session()
            .open_options()
            .open_buffer(bytes.clone())
            .map_err(|e| BasinError::storage(format!("vortex: open_buffer: {e}")))?;
        if let (Some(c), Some(p)) = (cache, path) {
            c.insert(p.clone(), size_bytes, vf.footer().clone());
        }
        vf
    };

    let inferred = vf.dtype().to_arrow_schema().map_err(|e| {
        BasinError::storage(format!("vortex: infer arrow schema from file dtype: {e}"))
    })?;
    Ok(normalize_view_types_schema(&inferred))
}

/// Like [`decode`] but threads a [`VortexFooterCache`] through so repeated
/// queries to the same physical file skip the per-file flatbuffer footer parse.
///
/// `path` + `size_bytes` form the cache key; see [`VortexFooterCache`] for the
/// key-safety argument. Both the cache lookup and the cache populate are
/// transparent to callers — results are identical to [`decode`].
pub(crate) async fn decode_with_cache(
    bytes: bytes::Bytes,
    schema: Option<Arc<Schema>>,
    projection: Option<&[String]>,
    filter: Option<vortex_array::expr::Expression>,
    cache: Option<&crate::vortex_footer_cache::VortexFooterCache>,
    path: &object_store::path::Path,
    size_bytes: u64,
) -> Result<(Vec<RecordBatch>, bool)> {
    let has_filter = filter.is_some();
    let pushed = projection.is_some() || has_filter;
    match decode_inner(
        bytes.clone(),
        schema.clone(),
        projection,
        filter,
        cache,
        Some(path),
        size_bytes,
    )
    .await
    {
        Ok(b) => Ok((b, has_filter)),
        Err(_) if pushed => decode_inner(bytes, schema, None, None, cache, Some(path), size_bytes)
            .await
            .map(|b| (b, false)),
        Err(e) => Err(e),
    }
}

async fn decode_inner(
    bytes: bytes::Bytes,
    schema: Option<Arc<Schema>>,
    projection: Option<&[String]>,
    filter: Option<vortex_array::expr::Expression>,
    cache: Option<&crate::vortex_footer_cache::VortexFooterCache>,
    path: Option<&object_store::path::Path>,
    size_bytes: u64,
) -> Result<Vec<RecordBatch>> {
    use vortex_array::expr::{root, select};

    // Footer cache fast path: if a cache and path are provided, probe the
    // cache before the (synchronous) flatbuffer footer parse. On hit, pass
    // the cached Footer into `open_options().with_footer()` so
    // `open_buffer` skips `block_on(read_footer(...))` entirely. On miss,
    // parse the footer as normal then populate the cache for next time.
    let cached_footer = match (cache, path) {
        (Some(c), Some(p)) => c.get(p, size_bytes),
        _ => None,
    };

    let vf = if let Some(footer) = cached_footer {
        session()
            .open_options()
            .with_footer(footer)
            .open_buffer(bytes)
            .map_err(|e| BasinError::storage(format!("vortex: open_buffer (cached footer): {e}")))?
    } else {
        let vf = session()
            .open_options()
            .open_buffer(bytes)
            .map_err(|e| BasinError::storage(format!("vortex: open_buffer: {e}")))?;
        // Populate the cache for the next query on this file.
        if let (Some(c), Some(p)) = (cache, path) {
            c.insert(p.clone(), size_bytes, vf.footer().clone());
        }
        vf
    };

    // Inferred (self-describing) full schema, normalised so Vortex's
    // `Utf8View`/`BinaryView` become Basin-canonical `Utf8`/`Binary` (the
    // engine downcasts to `StringArray` everywhere).
    let infer_full = || -> Result<Schema> {
        let inferred = vf.dtype().to_arrow_schema().map_err(|e| {
            BasinError::storage(format!("vortex: infer arrow schema from file dtype: {e}"))
        })?;
        Ok(normalize_view_types_schema(&inferred))
    };

    let mut sb = vf
        .scan()
        .map_err(|e| BasinError::storage(format!("vortex: scan: {e}")))?;
    // Effective projection. When the caller asks for an explicit projection we
    // honour it. When it does NOT (full-row read), the physical Vortex file may
    // carry MORE columns than the logical/catalog `schema` — e.g. JSONB
    // shadow-column promotion (ADR 0027) appends `__promoted$…` fields to the
    // file that are absent from the catalog schema. Decoding the full physical
    // struct (8 fields) against a 6-field target schema fails inside
    // `execute_record_batch` ("StructArray has N fields, but target Arrow type
    // has M fields"). Derive a name-projection from the target schema so the
    // decoded struct is exactly the logical columns, in catalog order.
    let derived_proj: Option<Vec<String>> = match (projection, &schema) {
        (None, Some(s)) => {
            // Names the physical file actually carries (avoid projecting a
            // logical column that pre-dates this file's write).
            let phys = vf.dtype().to_arrow_schema().ok();
            let phys_has = |n: &str| {
                phys.as_ref()
                    .map(|p| p.field_with_name(n).is_ok())
                    .unwrap_or(true)
            };
            let phys_field_count = phys.as_ref().map(|p| p.fields().len());
            // Narrow whenever the physical shape differs from the logical one
            // in EITHER direction: extra physical columns (promoted shadow
            // fields) AND missing physical columns (file written before an
            // ALTER ADD COLUMN) both fail execute_record_batch's field-count
            // check. Decode the name-intersection in catalog order; the read
            // layers above null-fill logical columns the file pre-dates.
            if phys_field_count.map(|c| c != s.fields().len()).unwrap_or(false) {
                let names: Vec<String> = s
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .filter(|n| phys_has(n))
                    .collect();
                if names.is_empty() {
                    None
                } else {
                    Some(names)
                }
            } else {
                None
            }
        }
        _ => None,
    };
    // An explicit projection may name logical columns this file pre-dates
    // (ALTER ADD COLUMN): hand Vortex only the columns the file physically
    // carries — the reader's projection assembler null-fills the rest from
    // the catalog schema.
    let projection_phys: Option<Vec<String>> = projection.map(|cols| {
        let phys = vf.dtype().to_arrow_schema().ok();
        cols.iter()
            .filter(|n| {
                phys.as_ref()
                    .map(|p| p.field_with_name(n).is_ok())
                    .unwrap_or(true)
            })
            .cloned()
            .collect()
    });
    // The conversion target must mirror the EFFECTIVE projection handed to
    // the scan builder (post physical-intersection), not the caller's raw
    // request — the produced struct has exactly the intersected columns.
    let effective_proj: Option<&Vec<String>> =
        projection_phys.as_ref().or(derived_proj.as_ref());
    if let Some(cols) = effective_proj {
        if !cols.is_empty() {
            let names: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
            sb = sb.with_projection(select(names, root()));
        }
    }
    if let Some(f) = filter {
        sb = sb.with_some_filter(Some(f));
    }

    // Target Arrow schema for `into_record_batch_with_schema`. With a
    // projection the returned struct is exactly the projected columns in
    // order, so the schema must be that subset: prefer the authoritative
    // catalog `schema` (keeps exact types — e.g. timestamp tz), else the
    // projected dtype the scan reports.
    let arrow_schema: Arc<Schema> = match (effective_proj, &schema) {
        (Some(cols), Some(s)) => {
            let mut fields = Vec::with_capacity(cols.len());
            let mut all_present = true;
            for name in cols {
                match s.field_with_name(name) {
                    Ok(f) => fields.push(f.clone()),
                    Err(_) => {
                        all_present = false;
                        break;
                    }
                }
            }
            if all_present {
                Arc::new(Schema::new(fields))
            } else {
                Arc::new(normalize_view_types_schema(&sb.dtype().map_err(|e| {
                    BasinError::storage(format!("vortex: projected dtype: {e}"))
                })?.to_arrow_schema().map_err(|e| {
                    BasinError::storage(format!("vortex: projected to_arrow_schema: {e}"))
                })?))
            }
        }
        (Some(_), None) => Arc::new(normalize_view_types_schema(
            &sb.dtype()
                .map_err(|e| BasinError::storage(format!("vortex: projected dtype: {e}")))?
                .to_arrow_schema()
                .map_err(|e| {
                    BasinError::storage(format!("vortex: projected to_arrow_schema: {e}"))
                })?,
        )),
        (None, Some(s)) => s.clone(),
        (None, None) => Arc::new(infer_full()?),
    };

    let stream = sb
        .into_array_stream()
        .map_err(|e| BasinError::storage(format!("vortex: into_array_stream: {e}")))?;
    futures::pin_mut!(stream);

    // One ExecutionCtx per decode call, reused across all chunks.  Creating a
    // new ctx per chunk (the old `into_record_batch_with_schema` path) was
    // building a LEGACY_SESSION ctx on every iteration — this hoists that cost
    // to once per file and threads the warm ctx through every chunk.
    let mut ctx = session().create_execution_ctx();

    let mut batches = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk: ArrayRef =
            chunk.map_err(|e| BasinError::storage(format!("vortex: scan chunk: {e}")))?;
        let rb = chunk
            .execute_record_batch(arrow_schema.as_ref(), &mut ctx)
            .map_err(|e| {
                BasinError::storage(format!("vortex: execute_record_batch: {e}"))
            })?;
        batches.push(rb);
    }

    Ok(batches)
}

/// Rewrite a single Arrow [`DataType`], replacing the zero-copy *view*
/// string/binary variants with Basin's canonical non-view equivalents and
/// recursing through nested containers. Everything else is returned
/// unchanged. Keeps Vortex's self-describing schema type-compatible with the
/// Parquet read path the rest of the engine assumes.
fn normalize_view_dtype(dt: &DataType) -> DataType {
    match dt {
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        DataType::List(f) => DataType::List(normalize_view_field(f).into()),
        DataType::LargeList(f) => DataType::LargeList(normalize_view_field(f).into()),
        DataType::FixedSizeList(f, n) => {
            DataType::FixedSizeList(normalize_view_field(f).into(), *n)
        }
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(|f| normalize_view_field(f)).collect())
        }
        DataType::Map(f, sorted) => DataType::Map(normalize_view_field(f).into(), *sorted),
        DataType::Dictionary(k, v) => {
            DataType::Dictionary(k.clone(), Box::new(normalize_view_dtype(v)))
        }
        other => other.clone(),
    }
}

fn normalize_view_field(f: &Field) -> Field {
    Field::new(
        f.name(),
        normalize_view_dtype(f.data_type()),
        f.is_nullable(),
    )
    .with_metadata(f.metadata().clone())
}

/// Normalise every field of an inferred Vortex schema (see
/// [`normalize_view_dtype`]).
fn normalize_view_types_schema(schema: &Schema) -> Schema {
    Schema::new(
        schema
            .fields()
            .iter()
            .map(|f| normalize_view_field(f))
            .collect::<Vec<_>>(),
    )
    .with_metadata(schema.metadata().clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{
        Array, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array,
        StringArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, TimeUnit};

    /// Build a representative Basin-shaped RecordBatch covering the risky types:
    /// Int64, Utf8 (also the JSONB representation), Float64, Boolean (with
    /// nulls), Timestamp(Microsecond, UTC), and FixedSizeList<Float32, 4> (a
    /// vector column). Nulls appear in `flag` and `note`.
    fn sample_batch() -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("note", DataType::Utf8, true),
            Field::new("score", DataType::Float64, false),
            Field::new("flag", DataType::Boolean, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new(
                "embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 4),
                false,
            ),
        ]));

        let id = Int64Array::from(vec![1, 2, 3, 4]);
        let note = StringArray::from(vec![
            Some("{\"k\":1}"),
            None,
            Some("plain text"),
            Some("{\"k\":2}"),
        ]);
        let score = Float64Array::from(vec![1.5, 2.25, -3.75, 0.0]);
        let flag = BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]);
        let ts = TimestampMicrosecondArray::from(vec![
            1_700_000_000_000_000,
            1_700_000_001_000_000,
            1_700_000_002_000_000,
            1_700_000_003_000_000,
        ])
        .with_timezone("UTC");

        let values = Float32Array::from(vec![
            0.1, 0.2, 0.3, 0.4, // row 0
            1.1, 1.2, 1.3, 1.4, // row 1
            2.1, 2.2, 2.3, 2.4, // row 2
            3.1, 3.2, 3.3, 3.4, // row 3
        ]);
        let embedding = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            4,
            Arc::new(values),
            None,
        );

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id),
                Arc::new(note),
                Arc::new(score),
                Arc::new(flag),
                Arc::new(ts),
                Arc::new(embedding),
            ],
        )
        .expect("build sample batch");

        (schema, batch)
    }

    #[tokio::test]
    async fn round_trip_basin_shaped_schema() {
        let (schema, original) = sample_batch();

        let bytes = encode(&original, None).await.expect("encode");
        assert!(!bytes.is_empty(), "encoded blob must be non-empty");

        let (decoded, _) = decode(bytes::Bytes::from(bytes), Some(schema.clone()), None, None)
            .await
            .expect("decode");

        // All rows come back; merge chunks (this fixture is a single chunk).
        let total_rows: usize = decoded.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, original.num_rows(), "row count must match");
        assert_eq!(decoded.len(), 1, "single-chunk fixture -> one batch");

        let got = &decoded[0];

        // Schema fidelity: same column count + same field names in order.
        assert_eq!(
            got.num_columns(),
            original.num_columns(),
            "column count must match"
        );
        let orig_schema = original.schema();
        let got_schema = got.schema();
        let orig_names: Vec<&str> = orig_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let got_names: Vec<&str> = got_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(got_names, orig_names, "field names must match in order");

        // Element-wise value equality, per column, with explicit downcasts so a
        // type-level regression is impossible to hide.
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
        assert_eq!(g_note, o_note, "Utf8 column (incl. nulls) must round-trip");

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

        let g_flag = got
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("flag Boolean");
        let o_flag = original
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(
            g_flag, o_flag,
            "Boolean column (incl. nulls) must round-trip"
        );

        let g_ts = got
            .column(4)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("ts Timestamp(us, UTC)");
        let o_ts = original
            .column(4)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            g_ts.values(),
            o_ts.values(),
            "Timestamp(Microsecond) values must round-trip"
        );
        // Timezone metadata must survive (Basin stores UTC-tagged timestamps).
        assert_eq!(
            got.schema().field(4).data_type(),
            original.schema().field(4).data_type(),
            "Timestamp timezone must round-trip"
        );

        let g_emb = got
            .column(5)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("embedding FixedSizeList<Float32,4>");
        let o_emb = original
            .column(5)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(
            g_emb.len(),
            o_emb.len(),
            "FixedSizeList row count must round-trip"
        );
        let g_vals = g_emb
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("embedding child Float32");
        let o_vals = o_emb
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(
            g_vals, o_vals,
            "FixedSizeList<Float32,4> values must round-trip (vector column)"
        );
    }

    /// #92 — `EncodingMode::Fast` must round-trip identically to `Best`
    /// (lossless), produce valid Vortex output, and not be slower than the
    /// full cascade on a large bulk batch. Uses a 100k-row batch shaped like a
    /// bulk INSERT: an Int64 id, a moderately-cardinal Utf8 label (the kind of
    /// column the dropped dictionary/FSST search is expensive on), and a
    /// Float64 measure.
    #[tokio::test]
    async fn fast_mode_round_trips_and_is_not_slower() {
        use std::time::Instant;

        const N: usize = 100_000;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("measure", DataType::Float64, false),
        ]));
        let id = Int64Array::from((0i64..N as i64).collect::<Vec<_>>());
        // 500 distinct labels → dictionary/FSST would normally win the search;
        // Fast skips that search but must still round-trip exactly.
        let label = StringArray::from(
            (0..N).map(|i| format!("label_{}", i % 500)).collect::<Vec<_>>(),
        );
        let measure = Float64Array::from((0..N).map(|i| (i as f64) * 0.25).collect::<Vec<_>>());
        let original = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id), Arc::new(label), Arc::new(measure)],
        )
        .expect("build bulk batch");

        // Encode both modes. Time them; warm the codec first (session init +
        // encoding registration is one-time and would otherwise be charged to
        // whichever mode runs first).
        let _warm = encode_with_mode(&original, None, EncodingMode::Best)
            .await
            .expect("warm encode");

        let t_best = Instant::now();
        let best_bytes = encode_with_mode(&original, None, EncodingMode::Best)
            .await
            .expect("encode best");
        let best_elapsed = t_best.elapsed();

        let t_fast = Instant::now();
        let fast_bytes = encode_with_mode(&original, None, EncodingMode::Fast)
            .await
            .expect("encode fast");
        let fast_elapsed = t_fast.elapsed();

        assert!(!fast_bytes.is_empty(), "Fast mode must produce a non-empty blob");
        assert!(!best_bytes.is_empty(), "Best mode must produce a non-empty blob");

        // Fast must skip the expensive per-column search, so it should not be
        // materially slower than Best. Allow a 25% slack for scheduler / cache
        // noise on a loaded CI box — the assertion is "Fast is not a
        // regression", not a tight micro-benchmark.
        assert!(
            fast_elapsed <= best_elapsed.mul_f64(1.25),
            "Fast encode ({fast_elapsed:?}) should not be slower than Best ({best_elapsed:?})"
        );

        // Round-trip: Fast-encoded bytes decode back to the exact input.
        let (decoded, _) =
            decode(bytes::Bytes::from(fast_bytes), Some(schema.clone()), None, None)
                .await
                .expect("decode fast blob");
        let total_rows: usize = decoded.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, N, "Fast round-trip must preserve every row");

        // Concatenate the decoded chunks (Fast widens row blocks → may be
        // multi-chunk) and compare column-for-column with the original.
        let decoded_refs: Vec<&RecordBatch> = decoded.iter().collect();
        let merged = arrow::compute::concat_batches(&schema, decoded_refs)
            .expect("concat decoded chunks");

        let g_id = merged
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id Int64");
        let o_id = original
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(g_id, o_id, "Int64 column must round-trip under Fast mode");

        let g_label = merged
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("label Utf8");
        let o_label = original
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(g_label, o_label, "Utf8 column must round-trip under Fast mode");

        let g_measure = merged
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("measure Float64");
        let o_measure = original
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            g_measure, o_measure,
            "Float64 column must round-trip under Fast mode"
        );
    }

    /// Self-describing decode: with `schema = None` the Arrow schema is
    /// recovered from the Vortex file's own DType (Parquet-footer-symmetric).
    /// This is the path Basin's schema-less internal readers
    /// (continuous-view refresh, cron-job state, system tables) take. The
    /// simple types those paths use must round-trip with no external schema.
    #[tokio::test]
    async fn self_describing_decode_without_schema() {
        let (_schema, original) = sample_batch();
        let bytes = encode(&original, None).await.expect("encode");

        let (decoded, _) = decode(bytes::Bytes::from(bytes), None, None, None)
            .await
            .expect("schema-less decode");

        let total_rows: usize = decoded.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, original.num_rows(), "row count must match");
        let got = &decoded[0];

        // Field names recovered from the file's DType, in order.
        let got_schema = got.schema();
        let got_names: Vec<&str> = got_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            got_names,
            vec!["id", "note", "score", "flag", "ts", "embedding"],
            "field names must be recovered from the file DType"
        );

        // The plain types Basin's internal read paths actually use must be
        // value-identical with no external schema. (Timestamp-tz and
        // FixedSizeList are exactly why the table-aware path passes the
        // authoritative catalog schema; they are asserted in the
        // schema-driven test, not here.)
        let g_id = got
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id Int64 inferred");
        let o_id = original
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(g_id, o_id, "Int64 must round-trip self-describing");

        // Vortex's DType→Arrow inference yields `Utf8View`; the decoder
        // normalises it back to Basin's canonical `Utf8` so a self-described
        // Vortex file presents the exact Arrow types a Parquet file would
        // (the engine downcasts to `StringArray` everywhere). Value + null
        // identity, no external schema.
        let g_note = got
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("note normalised to Utf8 (not Utf8View)");
        let o_note = original
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            g_note, o_note,
            "Utf8 incl. nulls must round-trip self-describing (view-normalised)"
        );

        let g_score = got
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("score Float64 inferred");
        let o_score = original
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(g_score, o_score, "Float64 must round-trip self-describing");

        let g_flag = got
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("flag Boolean inferred");
        let o_flag = original
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(
            g_flag, o_flag,
            "Boolean incl. nulls must round-trip self-describing"
        );
    }

    // ---- Selective-decode / pushed-filter equivalence ----------------------
    //
    // These prove that reading a Vortex file with an `Eq` filter pushed into
    // the scan (which drives zone-map chunk pruning + Vortex's mask-applied
    // selective decode) yields the EXACT row set the unfiltered decode would
    // yield after an Arrow post-filter. The pushed path is a pure
    // optimisation; the assertion is row-set equality, never just row count.

    /// A wide multi-chunk fixture: `n` rows of (id Int64 PK, name Utf8 nullable,
    /// val Float64), encoded with a tiny `row_block_size` so the file is split
    /// into several Vortex chunks. `null_ids` get a NULL name to exercise the
    /// NULL-in-filter-column case.
    fn multi_chunk_batch(n: i64, null_ids: &[i64]) -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("val", DataType::Float64, false),
        ]));
        let ids: Vec<i64> = (0..n).collect();
        let names: Vec<Option<String>> = (0..n)
            .map(|i| {
                if null_ids.contains(&i) {
                    None
                } else {
                    Some(format!("row-{i}"))
                }
            })
            .collect();
        let vals: Vec<f64> = (0..n).map(|i| i as f64 * 1.5).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(vals)),
            ],
        )
        .expect("build multi-chunk batch");
        (schema, batch)
    }

    /// Collect all `id` values present across a set of decoded batches, in
    /// order — the canonical "row set" projection used for equivalence checks.
    fn ids_of(batches: &[RecordBatch]) -> Vec<i64> {
        let mut out = Vec::new();
        for b in batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id Int64");
            for i in 0..col.len() {
                out.push(col.value(i));
            }
        }
        out
    }

    /// Decode WITHOUT any pushdown, then apply an `id == target` filter in
    /// Arrow — the reference row set the pushed-filter decode must match.
    async fn reference_eq_ids(bytes: &bytes::Bytes, schema: &Arc<Schema>, target: i64) -> Vec<i64> {
        let (batches, _) = decode(bytes.clone(), Some(schema.clone()), None, None)
            .await
            .expect("reference full decode");
        let mut out = Vec::new();
        for b in &batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id Int64");
            for i in 0..col.len() {
                if !col.is_null(i) && col.value(i) == target {
                    out.push(col.value(i));
                }
            }
        }
        out
    }

    /// Drive the pushed-filter decode for `id == target` and return the
    /// resulting `id` row set.
    async fn pushed_eq_ids(bytes: &bytes::Bytes, schema: &Arc<Schema>, target: i64) -> Vec<i64> {
        use vortex_array::expr::{col, eq, lit};
        let filter = eq(col("id"), lit(target));
        let (batches, used_filter) = decode(bytes.clone(), Some(schema.clone()), None, Some(filter))
            .await
            .expect("pushed-filter decode");
        assert!(used_filter, "decode must report the filter was used (no fallback)");
        ids_of(&batches)
    }

    /// Match in the FIRST chunk vs match in the LAST chunk: both must return
    /// exactly the one matching row, identical to the Arrow-filtered reference.
    #[tokio::test]
    async fn pushed_eq_first_and_last_chunk_match() {
        let (schema, batch) = multi_chunk_batch(5000, &[]);
        // row_block_size = 512 -> ~10 chunks.
        let bytes = bytes::Bytes::from(encode(&batch, Some(512)).await.expect("encode"));

        for target in [3i64, 4999i64] {
            let mut got = pushed_eq_ids(&bytes, &schema, target).await;
            let mut want = reference_eq_ids(&bytes, &schema, target).await;
            got.sort_unstable();
            want.sort_unstable();
            assert_eq!(want, vec![target], "reference must isolate exactly the target");
            assert_eq!(got, want, "pushed Eq row set must equal Arrow-filtered reference (target={target})");
        }
    }

    /// No matching row anywhere: pushed decode must yield an empty row set,
    /// matching the empty Arrow-filtered reference.
    #[tokio::test]
    async fn pushed_eq_no_match() {
        let (schema, batch) = multi_chunk_batch(2000, &[]);
        let bytes = bytes::Bytes::from(encode(&batch, Some(256)).await.expect("encode"));

        let got = pushed_eq_ids(&bytes, &schema, 999_999).await;
        let want = reference_eq_ids(&bytes, &schema, 999_999).await;
        assert!(want.is_empty(), "reference must be empty for an absent key");
        assert_eq!(got, want, "pushed Eq must yield the same empty row set");
    }

    /// NULL values in a non-filter column must not perturb an Eq filter on the
    /// PK: the matching row (whose name is NULL) is still returned exactly.
    #[tokio::test]
    async fn pushed_eq_with_nulls_in_payload() {
        // Make the target row's name NULL, plus some other NULLs.
        let target = 1234i64;
        let (schema, batch) = multi_chunk_batch(3000, &[0, 1, target, 2999]);
        let bytes = bytes::Bytes::from(encode(&batch, Some(300)).await.expect("encode"));

        let got = pushed_eq_ids(&bytes, &schema, target).await;
        let want = reference_eq_ids(&bytes, &schema, target).await;
        assert_eq!(want, vec![target], "reference must isolate the (NULL-name) target");
        assert_eq!(got, want, "pushed Eq must match exactly even when payload column is NULL");
    }

    /// Multiple matching rows for the same key: the pushed decode must return
    /// every match, in the same set as the Arrow-filtered reference.
    #[tokio::test]
    async fn pushed_eq_multiple_matches() {
        // Build a batch where id repeats: id = i % 100, so key 7 appears 30x.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Float64, false),
        ]));
        let n = 3000i64;
        let ids: Vec<i64> = (0..n).map(|i| i % 100).collect();
        let vals: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let expected_count = ids.iter().filter(|&&v| v == 7).count();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(ids)), Arc::new(Float64Array::from(vals))],
        )
        .expect("build repeated-key batch");
        let bytes = bytes::Bytes::from(encode(&batch, Some(256)).await.expect("encode"));

        let mut got = pushed_eq_ids(&bytes, &schema, 7).await;
        let mut want = reference_eq_ids(&bytes, &schema, 7).await;
        got.sort_unstable();
        want.sort_unstable();
        assert_eq!(want.len(), expected_count, "reference must find every duplicate");
        assert_eq!(got, want, "pushed Eq must return every matching row");
    }

    /// String PK Eq: Basin does NOT push string predicates into Vortex (the
    /// dtype-compare panic risk), so the scan runs unfiltered and the engine
    /// post-filters. Reading with no pushed filter and applying the predicate
    /// in Arrow must still isolate exactly the matching row — this guards the
    /// non-pushed branch the selective-decode optimisation deliberately leaves
    /// to the Arrow post-filter.
    #[tokio::test]
    async fn string_pk_eq_post_filter_equivalence() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("sk", DataType::Utf8, false),
            Field::new("val", DataType::Float64, false),
        ]));
        let n = 2000i64;
        let keys: Vec<String> = (0..n).map(|i| format!("k-{i:06}")).collect();
        let vals: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Float64Array::from(vals)),
            ],
        )
        .expect("build string-pk batch");
        let bytes = bytes::Bytes::from(encode(&batch, Some(256)).await.expect("encode"));

        // No filter pushed (mirrors `vortex_filter_expr` declining strings);
        // full decode then Arrow-filter on the string key.
        let (batches, _) = decode(bytes, Some(schema.clone()), None, None)
            .await
            .expect("string-pk full decode");
        let target = "k-001234";
        let mut matched: Vec<String> = Vec::new();
        for b in &batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("sk Utf8");
            for i in 0..col.len() {
                if !col.is_null(i) && col.value(i) == target {
                    matched.push(col.value(i).to_string());
                }
            }
        }
        assert_eq!(matched, vec![target.to_string()], "string-PK Eq must isolate exactly one row via Arrow post-filter");
    }

    /// `infer_arrow_schema` recovers the physical column types from the file
    /// footer alone (no catalog) — this is what lets the schema-less read path
    /// type-check an Int64 PK Eq and push it. Names + canonical types must
    /// match the original schema (view-normalised).
    #[tokio::test]
    async fn infer_arrow_schema_recovers_types() {
        let (schema, batch) = multi_chunk_batch(64, &[]);
        let bytes = bytes::Bytes::from(encode(&batch, Some(16)).await.expect("encode"));

        let inferred = infer_arrow_schema(&bytes, None, None, 0).expect("infer schema");
        let names: Vec<&str> = inferred.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name", "val"], "names recovered from footer");
        assert_eq!(
            inferred.field_with_name("id").unwrap().data_type(),
            &DataType::Int64,
            "Int64 PK type must be recoverable for pushdown type-check"
        );
        assert_eq!(
            inferred.field_with_name("name").unwrap().data_type(),
            &DataType::Utf8,
            "Utf8View must be normalised to canonical Utf8"
        );
        // Sanity: the recovered schema agrees with the original (modulo
        // metadata, which Vortex drops).
        assert_eq!(inferred.fields().len(), schema.fields().len());
    }
}
