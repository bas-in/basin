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
use vortex_array::arrow::FromArrowArray;
use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_array::{ArrayRef, ToCanonical};
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_buffer::ByteBufferMut;
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

/// Encode one Arrow `RecordBatch` into a self-describing Vortex byte blob using
/// the aggressive BtrBlocks cascade (zstd for strings/binary, pco for numerics).
pub(crate) async fn encode(batch: &RecordBatch) -> Result<Vec<u8>> {
    // Arrow RecordBatch -> Vortex struct array. The top-level struct must be
    // NON-nullable: vortex 0.70's `FileStatsAccumulator` rejects nullable
    // top-level structs ("Use Validity::NonNullable"). Per-COLUMN nullability
    // is independent — it is taken from each Arrow field's `is_nullable()`
    // inside `from_arrow`, so nullable columns still round-trip their nulls.
    let varr: ArrayRef = FromArrowArray::from_arrow(batch, false)
        .map_err(|e| BasinError::storage(format!("vortex: from_arrow encode: {e}")))?;

    // BtrBlocks with `with_compact` = the proven aggressive strategy.
    let compressor = BtrBlocksCompressorBuilder::default().with_compact();
    let strategy = WriteStrategyBuilder::default()
        .with_btrblocks_builder(compressor)
        .build();

    let mut buf = ByteBufferMut::empty();
    session()
        .write_options()
        .with_strategy(strategy)
        .write(&mut buf, varr.to_array_stream())
        .await
        .map_err(|e| BasinError::storage(format!("vortex: write: {e}")))?;

    Ok(buf.as_slice().to_vec())
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
    use arrow_array::{Array, Float64Array, Int64Array};

    let mut out = std::collections::BTreeMap::new();
    let schema = batch.schema();
    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        let null_count = Some(col.null_count() as u64);
        let (min_bytes, max_bytes) = match field.data_type() {
            DataType::Int64 => {
                let a = col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64 column");
                (
                    arrow::compute::min(a).map(|v| v.to_le_bytes().to_vec()),
                    arrow::compute::max(a).map(|v| v.to_le_bytes().to_vec()),
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
                )
            }
            _ => (None, None),
        };
        out.insert(
            field.name().clone(),
            crate::data_file::ColumnStats {
                null_count,
                min_bytes,
                max_bytes,
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
    bytes: &[u8],
) -> Result<(
    u64,
    std::collections::BTreeMap<String, crate::data_file::ColumnStats>,
)> {
    use vortex_array::dtype::{DType, PType};
    use vortex_array::expr::stats::Stat;

    let mut out = std::collections::BTreeMap::new();
    let vf = session()
        .open_options()
        .open_buffer(bytes.to_vec())
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

        // Only exact i64 / f64 columns get min/max, in the 8-byte LE
        // contract `reader::decode_le_i64` / `decode_le_f64` expect.
        let (min_bytes, max_bytes) = match col_dt {
            DType::Primitive(PType::I64, _) => {
                let mn = ss
                    .get_as::<i64>(Stat::Min, col_dt)
                    .and_then(|p| p.as_exact())
                    .map(|v| v.to_le_bytes().to_vec());
                let mx = ss
                    .get_as::<i64>(Stat::Max, col_dt)
                    .and_then(|p| p.as_exact())
                    .map(|v| v.to_le_bytes().to_vec());
                (mn, mx)
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
                (mn, mx)
            }
            _ => (None, None),
        };

        if null_count.is_some() || min_bytes.is_some() || max_bytes.is_some() {
            out.insert(
                name.clone(),
                crate::data_file::ColumnStats {
                    null_count,
                    min_bytes,
                    max_bytes,
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
pub(crate) async fn decode(
    bytes: &[u8],
    schema: Option<Arc<Schema>>,
    projection: Option<&[String]>,
    filter: Option<vortex_array::expr::Expression>,
) -> Result<Vec<RecordBatch>> {
    // Pushdown is best-effort: a literal/column dtype mismatch or any other
    // scan error must never change results. Try with pushdown; on failure
    // retry once with a plain full decode (the path the engine post-filters
    // anyway). Only retry when pushdown was actually requested.
    let pushed = projection.is_some() || filter.is_some();
    match decode_inner(bytes, schema.clone(), projection, filter).await {
        Ok(b) => Ok(b),
        Err(_) if pushed => decode_inner(bytes, schema, None, None).await,
        Err(e) => Err(e),
    }
}

async fn decode_inner(
    bytes: &[u8],
    schema: Option<Arc<Schema>>,
    projection: Option<&[String]>,
    filter: Option<vortex_array::expr::Expression>,
) -> Result<Vec<RecordBatch>> {
    use vortex_array::expr::{root, select};

    let vf = session()
        .open_options()
        .open_buffer(bytes.to_vec())
        .map_err(|e| BasinError::storage(format!("vortex: open_buffer: {e}")))?;

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
    if let Some(cols) = projection {
        let names: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        sb = sb.with_projection(select(names, root()));
    }
    if let Some(f) = filter {
        sb = sb.with_some_filter(Some(f));
    }

    // Target Arrow schema for `into_record_batch_with_schema`. With a
    // projection the returned struct is exactly the projected columns in
    // order, so the schema must be that subset: prefer the authoritative
    // catalog `schema` (keeps exact types — e.g. timestamp tz), else the
    // projected dtype the scan reports.
    let arrow_schema: Arc<Schema> = match (projection, &schema) {
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

    let mut batches = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk: ArrayRef =
            chunk.map_err(|e| BasinError::storage(format!("vortex: scan chunk: {e}")))?;
        let struct_array = chunk.to_struct();
        let rb = struct_array
            .into_record_batch_with_schema(arrow_schema.as_ref())
            .map_err(|e| {
                BasinError::storage(format!("vortex: into_record_batch_with_schema: {e}"))
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

        let bytes = encode(&original).await.expect("encode");
        assert!(!bytes.is_empty(), "encoded blob must be non-empty");

        let decoded = decode(&bytes, Some(schema.clone()), None, None)
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

    /// Self-describing decode: with `schema = None` the Arrow schema is
    /// recovered from the Vortex file's own DType (Parquet-footer-symmetric).
    /// This is the path Basin's schema-less internal readers
    /// (continuous-view refresh, cron-job state, system tables) take. The
    /// simple types those paths use must round-trip with no external schema.
    #[tokio::test]
    async fn self_describing_decode_without_schema() {
        let (_schema, original) = sample_batch();
        let bytes = encode(&original).await.expect("encode");

        let decoded = decode(&bytes, None, None, None)
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
}
