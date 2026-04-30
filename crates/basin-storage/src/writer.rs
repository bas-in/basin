//! Parquet writer.
//!
//! Atomicity. Object stores generally do not give us cheap multi-key atomic
//! commits; what they do give us is single-object PUT atomicity. We rely on
//! that: a Parquet file appears in full or not at all. To avoid colliding
//! with concurrent writers, the file name embeds a ULID. Visibility across
//! many files is the catalog's problem, not ours — see the basin-catalog
//! Iceberg snapshot commit path.

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use basin_common::{BasinError, PartitionKey, Result, TableName, TenantId};
use bytes::Bytes;
use chrono::Utc;
use object_store::PutPayload;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use ulid::Ulid;

use crate::data_file::{ColumnStats, DataFile};
use crate::paths::data_file_key;
use crate::Storage;

/// Row groups of 65_536 rows. Big enough that per-group metadata overhead is
/// small relative to the data, small enough that statistics pruning still
/// drops most of a file on selective queries. Production tuning will move to
/// per-table catalog options once the catalog grows them.
const DEFAULT_MAX_ROW_GROUP_SIZE: usize = 65_536;

pub(crate) async fn write_batch(
    storage: &Storage,
    tenant: &TenantId,
    table: &TableName,
    partition: &PartitionKey,
    batch: &RecordBatch,
) -> Result<DataFile> {
    let data_ulid = Ulid::new();
    let key = data_file_key(
        storage.root_prefix(),
        tenant,
        table,
        partition,
        Utc::now(),
        data_ulid,
    );

    let bytes = encode_parquet(batch)?;
    let size = bytes.len() as u64;
    let row_count = batch.num_rows() as u64;

    storage
        .object_store()
        .put(&key, PutPayload::from_bytes(Bytes::from(bytes.clone())))
        .await
        .map_err(|e| BasinError::storage(format!("put {key}: {e}")))?;

    // Build and persist HNSW sidecars for any FixedSizeList<Float32> columns
    // in the batch. One sidecar per Parquet write, mirroring the data-file
    // pattern; merging across writes is deferred to the future compactor.
    crate::vector_index::build_indexes_for_batch(storage, tenant, table, batch, data_ulid).await?;

    let column_stats = extract_column_stats(&bytes, batch)?;

    Ok(DataFile {
        path: key,
        size_bytes: size,
        row_count,
        column_stats,
    })
}

fn encode_parquet(batch: &RecordBatch) -> Result<Vec<u8>> {
    // ZSTD level 1: ~3x faster writes than ZSTD-3 with only a few percent
    // worse compression on log-shaped data. Audit-log retention still beats
    // CSV by an order of magnitude (the wedge claim), and the synchronous
    // write path (no WAL yet) doesn't crater. Once basin-wal lands and the
    // background compactor exists, the long-tail Parquet files can be
    // re-encoded at ZSTD-3 or ZSTD-9 for archival storage.
    let props = WriterProperties::builder()
        .set_max_row_group_size(DEFAULT_MAX_ROW_GROUP_SIZE)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(1).expect("ZSTD level 1 is valid"),
        ))
        .build();

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

    for rg in meta.row_groups() {
        for col in rg.columns() {
            let name = col.column_descr().name().to_string();
            let entry = out.entry(name).or_default();
            if let Some(stats) = col.statistics() {
                let nulls = stats.null_count_opt();
                if let Some(n) = nulls {
                    entry.null_count = Some(entry.null_count.unwrap_or(0) + n);
                }
                if let Some(min) = stats.min_bytes_opt() {
                    let min_vec = min.to_vec();
                    entry.min_bytes = Some(match entry.min_bytes.take() {
                        Some(prev) if prev <= min_vec => prev,
                        _ => min_vec,
                    });
                }
                if let Some(max) = stats.max_bytes_opt() {
                    let max_vec = max.to_vec();
                    entry.max_bytes = Some(match entry.max_bytes.take() {
                        Some(prev) if prev >= max_vec => prev,
                        _ => max_vec,
                    });
                }
            }
        }
    }
    Ok(out)
}

