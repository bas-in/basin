//! Bulk-insert workload — writes `cfg.workload.table_rows` rows in 10k-row
//! batches via `Storage::write_batch` (the same write path the integration
//! `s3_viability_*` tests exercise).
//!
//! Why not `INSERT … VALUES` via the engine? The engine's pgwire path
//! single-row-inserts to `MemTable` (HTAP) or rejects bulk DML; the column
//! store's bulk write happens via `Storage::write_batch`. We measure what
//! the dashboard's "ingest throughput" card is meant to surface.

use std::time::Instant;

use anyhow::Result;
use basin_catalog::DataFileRef;
use basin_common::PartitionKey;

use crate::config::BenchConfig;
use crate::harness::{build_batch, BenchHarness};
use crate::sample::{LatencySamples, SampleReport};

pub struct InsertOutcome {
    pub samples: SampleReport,
    pub wall_secs: f64,
    pub rows_written: u64,
}

/// Append `cfg.workload.table_rows` additional rows to project[project_idx]'s
/// `events` table, batched 10k at a time. Each batch's wall-clock is one
/// histogram sample so the bar can assert on per-batch p99.
pub async fn bulk_insert(
    h: &BenchHarness,
    cfg: &BenchConfig,
    project_idx: usize,
) -> Result<InsertOutcome> {
    const BATCH: u64 = 10_000;
    let project = h
        .projects
        .get(project_idx)
        .copied()
        .ok_or_else(|| anyhow::anyhow!("project index out of range"))?;
    let part = PartitionKey::default_key();

    let samples = LatencySamples::new();
    let started = Instant::now();
    let mut written: u64 = 0;
    let start_offset = cfg.workload.table_rows as i64; // append after seeded rows
    while written < cfg.workload.table_rows {
        let take = (cfg.workload.table_rows - written).min(BATCH) as usize;
        let batch = build_batch(&h.schema, start_offset + written as i64, take);
        let t0 = Instant::now();
        let df = h
            .storage
            .write_batch(&project, &h.table, &part, &batch)
            .await?;
        let meta = h.catalog.load_table(&project, &h.table).await?;
        h.catalog
            .append_data_files(
                &project,
                &h.table,
                meta.current_snapshot,
                vec![DataFileRef {
                    path: df.path.as_ref().to_string(),
                    size_bytes: df.size_bytes,
                    row_count: df.row_count,
                    column_stats: df.column_stats.clone(),
                    bloom_filters: ::std::collections::BTreeMap::new(),
                    hll_sketches: ::std::collections::BTreeMap::new(),
                    tdigest_sketches: ::std::collections::BTreeMap::new(),
                }],
            )
            .await?;
        samples.record(t0.elapsed());
        written += take as u64;
    }
    Ok(InsertOutcome {
        samples: samples.report(),
        wall_secs: started.elapsed().as_secs_f64(),
        rows_written: written,
    })
}
