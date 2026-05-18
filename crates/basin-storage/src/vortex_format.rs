//! Vortex columnar storage codec (opt-in `WITH (basin.file_format='vortex')`).
//! Mirrors the proven benchmark/vortex_compare write/scan path; BtrBlocks
//! cascade (zstd strings + pco numerics).
//!
//! Phase 0/1: this module is self-contained — it only converts between Arrow
//! `RecordBatch`es and an in-memory Vortex byte blob. The writer/reader/session
//! wiring that selects this codec is a later phase.

use std::sync::{Arc, OnceLock};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use basin_common::{BasinError, Result};
use futures::StreamExt;
use vortex_array::arrow::FromArrowArray;
use vortex_array::{ArrayRef, ToCanonical};
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_buffer::ByteBufferMut;
use vortex_file::{
    OpenOptionsSessionExt, WriteOptionsSessionExt, WriteStrategyBuilder,
    register_default_encodings,
};
use vortex_io::session::{RuntimeSession, RuntimeSessionExt};
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;
use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;

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

/// Decode a Vortex byte blob back to `RecordBatch`es with the given Arrow
/// schema (schema-driven, like the Parquet read path). One `RecordBatch` is
/// produced per Vortex chunk in the file.
pub(crate) async fn decode(bytes: &[u8], schema: Arc<Schema>) -> Result<Vec<RecordBatch>> {
    let vf = session()
        .open_options()
        .open_buffer(bytes.to_vec())
        .map_err(|e| BasinError::storage(format!("vortex: open_buffer: {e}")))?;

    let stream = vf
        .scan()
        .map_err(|e| BasinError::storage(format!("vortex: scan: {e}")))?
        .into_array_stream()
        .map_err(|e| BasinError::storage(format!("vortex: into_array_stream: {e}")))?;
    futures::pin_mut!(stream);

    let mut batches = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk: ArrayRef =
            chunk.map_err(|e| BasinError::storage(format!("vortex: scan chunk: {e}")))?;
        // Each scanned chunk is a struct-typed Vortex array. Canonicalize to a
        // `StructArray` then project onto the caller's Arrow schema (this is
        // exactly what vortex's own `into_record_batch_with_schema` does
        // internally via the legacy session).
        let struct_array = chunk.to_struct();
        let rb = struct_array
            .into_record_batch_with_schema(schema.as_ref())
            .map_err(|e| {
                BasinError::storage(format!("vortex: into_record_batch_with_schema: {e}"))
            })?;
        batches.push(rb);
    }

    Ok(batches)
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
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    4,
                ),
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

        let decoded = decode(&bytes, schema.clone()).await.expect("decode");

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
        assert_eq!(g_flag, o_flag, "Boolean column (incl. nulls) must round-trip");

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
}
