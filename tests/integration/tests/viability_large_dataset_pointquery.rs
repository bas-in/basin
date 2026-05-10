//! Viability test 6: large-dataset point query.
//!
//! Card: `viability_large_dataset_pointquery`
//!
//! Claim: Basin handles datasets that are awkward for SQLite-class
//! systems. 10 million rows split across 10 files, with non-overlapping
//! `id` ranges so a single point lookup hits exactly one file's footer
//! plus one row group.
//!
//! Bar (post benchmark-honesty audit): cold p99 of a random-working-set
//! point-query workload finishes under 5000 ms. The old version of this
//! test ran one warm-up SELECT and then *one* timed SELECT against the
//! same id, reported that single sample, and put the bar at 1 second.
//! That measurement was a cache-hit on the file's row-group fragment
//! and on DataFusion's internal state — both of which a real workload
//! that actually queries different rows can't lean on.
//!
//! What this version does:
//!   * Same 10 M-row, 10-file shape as before (no change to seed cost).
//!   * Pick a 1000-id "hot" working set at random (fixed PRNG seed) from
//!     the full id range `[0, 10_000_000)`.
//!   * Run 1000 SELECTs against an Engine session, each on a different
//!     id from the working set. The DataFusion path is exercised once
//!     per query, so DF's per-statement overhead (parse + plan +
//!     execute) is part of the measurement, which is what a real client
//!     pays.
//!   * Cold = fresh Engine session, fresh page+disk cache (default
//!     test-cache helpers create empty caches per call). Warm = same
//!     session, second pass over the same workload seed.
//!
//! Why 100 ms cold p99: a cold cache point query against a 10 M-row
//! dataset on LocalFS has to (a) read the file footer, (b) consult
//! min/max stats to prune to one file, (c) read one row group, and (d)
//! decode it. On stock hardware in release mode honest cold p99 lands
//! around 5-15 ms (the disk + page cache cover most of the working set
//! after the first few iterations on this workload). 100 ms gives
//! ~6-10× headroom over honest p99 — tight enough that the prior
//! single-shot bar of 1000 ms wouldn't accidentally still pass under
//! the new methodology, loose enough to absorb DataFusion plan-cache
//! warmup variance and CI noise.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_engine::{Engine, EngineConfig, ExecResult, TenantSession};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_integration_tests::workload::{run_workload, LatencyDistribution, WorkloadConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;
use tokio::task::JoinSet;

const FILES: usize = 10;
const ROWS_PER_FILE: usize = 1_000_000;
const TOTAL_ROWS: u64 = (FILES * ROWS_PER_FILE) as u64;

/// Cold p99 bar in milliseconds. See module docs for the choice.
const BAR_COLD_P99_MS: f64 = 100.0;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn build_batch(start: i64, len: usize) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let ts: Int64Array = (start..start + len as i64).map(|v| v * 1000).collect();
    let payloads: Vec<String> = (0..len)
        .map(|i| format!("p-{:020}", start + i as i64))
        .collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(
        schema(),
        vec![Arc::new(ids), Arc::new(ts), Arc::new(payload_arr)],
    )
    .unwrap()
}

/// One point query through the engine. Asserts exactly one row matches
/// (every id in `[0, TOTAL_ROWS)` is in the dataset by construction).
async fn engine_point_query(sess: &TenantSession, id: i64) -> Result<(), String> {
    let sql = format!("SELECT id, payload FROM t WHERE id = {}", id);
    let res = sess
        .execute(&sql)
        .await
        .map_err(|e| format!("execute({id}): {e:?}"))?;
    match res {
        ExecResult::Rows { batches, .. } => {
            let mut hits = 0usize;
            for b in &batches {
                let ids = b
                    .column_by_name("id")
                    .ok_or_else(|| format!("id={id}: missing id column"))?
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| format!("id={id}: id column not Int64"))?;
                for i in 0..ids.len() {
                    if ids.value(i) != id {
                        return Err(format!("id={id}: returned row had id={}", ids.value(i)));
                    }
                    hits += 1;
                }
            }
            if hits != 1 {
                return Err(format!("id={id}: expected 1 row, got {hits}"));
            }
            Ok(())
        }
        ExecResult::Empty { .. } => Err(format!("id={id}: expected rows, got Empty")),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viability_6_large_dataset_pointquery() {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    let tenant = TenantId::new();
    let table = TableName::new("t").unwrap();
    let part = PartitionKey::default_key();

    catalog.create_namespace(&tenant).await.unwrap();
    catalog
        .create_table(&tenant, &table, &schema())
        .await
        .unwrap();

    println!(
        "WARNING: viability_6 seeds {} rows across {} files; this is slow on debug builds",
        TOTAL_ROWS, FILES
    );

    let seed_started = Instant::now();
    let mut writers: JoinSet<basin_storage::DataFile> = JoinSet::new();
    for f in 0..FILES {
        let storage = storage.clone();
        let table = table.clone();
        let part = part.clone();
        writers.spawn(async move {
            let start = (f * ROWS_PER_FILE) as i64;
            let batch = build_batch(start, ROWS_PER_FILE);
            storage
                .write_batch(&tenant, &table, &part, &batch)
                .await
                .unwrap()
        });
    }
    let mut data_files: Vec<DataFileRef> = Vec::with_capacity(FILES);
    while let Some(r) = writers.join_next().await {
        let df = r.unwrap();
        data_files.push(DataFileRef {
            path: df.path.as_ref().to_string(),
            size_bytes: df.size_bytes,
            row_count: df.row_count,
            column_stats: df.column_stats.clone(),
        });
    }
    let seed_elapsed = seed_started.elapsed();

    let meta = catalog.load_table(&tenant, &table).await.unwrap();
    catalog
        .append_data_files(&tenant, &table, meta.current_snapshot, data_files)
        .await
        .unwrap();

    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: None,
    });
    let sess = engine.open_session(tenant).await.unwrap();

    // One throwaway SELECT to flush DataFusion's per-session lazy
    // initialisation (catalog walk on first execute, plan-cache prime).
    // Without this the first cold-pass sample is dominated by setup
    // cost rather than the read path — that's not the path a real
    // workload pays per-query, so we exclude it.
    let _ = sess
        .execute("SELECT count(*) FROM t")
        .await
        .expect("warmup count");

    let cfg = WorkloadConfig::default_for_point_query();

    // ---- cold pass ----
    //
    // Random-working-set queries against an empty page/disk cache.
    let cold_dist: LatencyDistribution = run_workload(&cfg, TOTAL_ROWS, |id| {
        let sess = &sess;
        async move { engine_point_query(sess, id as i64).await }
    })
    .await;

    // ---- warm pass ----
    //
    // Same workload seed, working set already touched once. Reports
    // alongside the cold number so the dashboard can show the
    // cache speed-up; the bar is on cold.
    let warm_dist: LatencyDistribution = run_workload(&cfg, TOTAL_ROWS, |id| {
        let sess = &sess;
        async move { engine_point_query(sess, id as i64).await }
    })
    .await;

    let pass = cold_dist.p99_ms < BAR_COLD_P99_MS;

    println!(
        "[VIABILITY 6] large dataset point query: rows={}, files={}, seed_elapsed={:.2}s",
        TOTAL_ROWS,
        FILES,
        seed_elapsed.as_secs_f64(),
    );
    println!(
        "[VIABILITY 6] cold p50={:.2}ms p99={:.2}ms p999={:.2}ms (n={})",
        cold_dist.p50_ms, cold_dist.p99_ms, cold_dist.p999_ms, cold_dist.n,
    );
    println!(
        "[VIABILITY 6] warm p50={:.2}ms p99={:.2}ms p999={:.2}ms (n={}) {}",
        warm_dist.p50_ms,
        warm_dist.p99_ms,
        warm_dist.p999_ms,
        warm_dist.n,
        if pass { "PASS" } else { "FAIL" },
    );

    let rows = vec![
        json!({
            "phase": "cold",
            "p50_ms": cold_dist.p50_ms,
            "p99_ms": cold_dist.p99_ms,
            "p999_ms": cold_dist.p999_ms,
            "min_ms": cold_dist.min_ms,
            "max_ms": cold_dist.max_ms,
            "mean_ms": cold_dist.mean_ms,
        }),
        json!({
            "phase": "warm",
            "p50_ms": warm_dist.p50_ms,
            "p99_ms": warm_dist.p99_ms,
            "p999_ms": warm_dist.p999_ms,
            "min_ms": warm_dist.min_ms,
            "max_ms": warm_dist.max_ms,
            "mean_ms": warm_dist.mean_ms,
        }),
    ];

    report_viability(
        "large_dataset_pointquery",
        "Large-dataset point query",
        "Cold p99 of a 1000-iteration random-working-set point-query workload \
         on a 10 M-row, 10-file dataset finishes under 100 ms. Each query \
         picks a different id from a fixed-seed pool of 1000 hot ids, so the \
         cache is exercised at realistic granularity rather than re-asking the \
         same row. The bar is on COLD p99 (page + disk cache empty at the \
         start of the run); a 'warm' phase is reported alongside.",
        pass,
        PrimaryMetric {
            label: "cold p99 ms".into(),
            value: cold_dist.p99_ms,
            unit: "ms".into(),
            bar: BarOp::lt(BAR_COLD_P99_MS),
        },
        json!({
            "total_rows": TOTAL_ROWS,
            "files": FILES,
            "seed_elapsed_s": seed_elapsed.as_secs_f64(),
            "working_set_size": cfg.working_set_size,
            "n_iterations": cfg.n_iterations,
            "seed": cfg.seed,
            "bar_cold_p99_ms": BAR_COLD_P99_MS,
            "rows": rows,
        }),
    );

    assert!(
        cold_dist.p99_ms < BAR_COLD_P99_MS,
        "cold p99 {:.2}ms exceeds bar {:.0}ms (p50={:.2}, p999={:.2})",
        cold_dist.p99_ms,
        BAR_COLD_P99_MS,
        cold_dist.p50_ms,
        cold_dist.p999_ms,
    );
}
