//! Viability test: analytical query speedup vs the OLTP engine.
//!
//! Claim: for a wide GROUP BY on a 1M-row table spread across ~10 Parquet
//! files, the analytical path (DuckDB on the underlying files) beats the
//! OLTP path (DataFusion-on-listing-table inside `basin-engine`) by at
//! least 1.5×. The two engines see the exact same files so this is a clean
//! engine-vs-engine comparison, not a storage one.
//!
//! The 1.5× bar is conservative on purpose. DuckDB's vectorised aggregator
//! is materially faster than DataFusion's on this shape; if we observe less
//! than 1.5× speedup, either DuckDB is mis-configured or DataFusion has
//! improved enough that we should stop maintaining a separate analytical
//! pool — both worth catching.
//!
//! What we explicitly do NOT test here:
//! - Snapshot pinning (`AS OF`) — v0.2.
//! - S3 path — v0.2.
//! - Tenant-isolation under load (covered by `viability_isolation_under_load`).

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_analytical::{AnalyticalConfig, AnalyticalEngine};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_engine::{Engine, EngineConfig, ExecResult, TenantSession};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

const ROWS: i64 = 1_000_000;
const FILES: i64 = 10;
const ROWS_PER_FILE: i64 = ROWS / FILES;
const GROUPS: i64 = 16;
// Iterations averaged in the timed window. v0.1 opens a fresh DuckDB
// connection per query, so a single-shot timing in release mode is
// dominated by ~25 ms of connection + view setup. Five iterations
// amortise that cost while still being a realistic shape: a dashboard
// polling every few seconds calls back-to-back. The brief recognises
// this with the v0.2 "cache sessions per tenant" plan.
const ITERATIONS: u32 = 5;
// Bar: DuckDB should win by >= 1.5x on this aggregate. See module doc above.
const SPEEDUP_BAR: f64 = 1.5;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("group_id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn build_batch(start: i64, len: i64) -> RecordBatch {
    let ids: Int64Array = (start..start + len).collect();
    let groups: Int64Array = (start..start + len).map(|i| i % GROUPS).collect();
    // Short, varying payload so Parquet doesn't dictionary-encode it down to
    // nothing — we want a representative columnar layout for the scan.
    let payloads: Vec<String> = (0..len)
        .map(|i| format!("p-{:08}", start + i))
        .collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(groups), Arc::new(payload_arr)])
        .unwrap()
}

#[derive(Debug, Clone)]
struct Aggregate {
    group_id: i64,
    count: i64,
    avg_id: f64,
    sum_len: i64,
}

fn collect_aggregate(batches: &[RecordBatch]) -> Vec<Aggregate> {
    let mut out = Vec::new();
    for b in batches {
        let groups = b
            .column_by_name("group_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = b
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // avg(BIGINT) = DOUBLE in both engines.
        let avgs = b
            .column_by_name("avg_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        let sums = b
            .column_by_name("s_len")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push(Aggregate {
                group_id: groups.value(i),
                count: counts.value(i),
                avg_id: avgs.value(i),
                sum_len: sums.value(i),
            });
        }
    }
    out.sort_by_key(|a| a.group_id);
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viability_analytical() {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    let tenant = TenantId::new();
    let table = TableName::new("t").unwrap();
    let part = PartitionKey::default_key();

    // Catalog-side: create the table so both engines can resolve it.
    catalog
        .create_table(&tenant, &table, schema().as_ref())
        .await
        .unwrap();

    // Storage-side: write FILES batches of ROWS_PER_FILE rows each. After
    // each write, commit the resulting DataFile to the catalog. The two
    // engines don't share state — each loads from the catalog/storage at
    // session-open time — so committing as we go is the realistic shape.
    for f in 0..FILES {
        let batch = build_batch(f * ROWS_PER_FILE, ROWS_PER_FILE);
        let df = storage
            .write_batch(&tenant, &table, &part, &batch)
            .await
            .unwrap();
        let meta = catalog.load_table(&tenant, &table).await.unwrap();
        catalog
            .append_data_files(
                &tenant,
                &table,
                meta.current_snapshot,
                vec![DataFileRef {
                    path: df.path.as_ref().to_string(),
                    size_bytes: df.size_bytes,
                    row_count: df.row_count,
                }],
            )
            .await
            .unwrap();
    }

    // Sanity check: file count actually matches our intent. If basin-storage
    // ever changes its writer to split across multiple files per batch the
    // bar would still be valid but the "10 files" claim in the report
    // wouldn't be, and we'd want to know.
    let files = storage.list_data_files(&tenant, &table).await.unwrap();
    assert_eq!(files.len(), FILES as usize, "expected {FILES} parquet files");

    // Aggregate that forces a real scan in both engines:
    // - GROUP BY group_id => 16 groups, hash-aggregate
    // - count(*)         => sanity check on group sizes
    // - avg(id)          => sum + count over an i64 column
    // - sum(length(payload)) => forces decoding the string payload column,
    //   which is where DuckDB's vectorised string ops dominate. Without it,
    //   DataFusion can compute count(*) from row-group stats and the
    //   comparison no longer reflects analytical work.
    let sql = "SELECT group_id, count(*) AS n, avg(id) AS avg_id, \
                      CAST(sum(length(payload)) AS BIGINT) AS s_len \
               FROM t GROUP BY group_id ORDER BY group_id";

    // ---- Engine path (DataFusion via basin-engine) -------------------------
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: None,
    });
    let sess: TenantSession = engine.open_session(tenant).await.unwrap();
    // One warmup to make the comparison fair: both engines pay listing /
    // metadata costs; we don't want the first-call cold cache to dominate.
    let _ = sess.execute(sql).await.unwrap();
    let mut engine_batches = Vec::new();
    let t0 = Instant::now();
    for _ in 0..ITERATIONS {
        let res = sess.execute(sql).await.unwrap();
        engine_batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            other => panic!("engine returned non-rows: {other:?}"),
        };
    }
    let engine_ms = t0.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;
    let engine_agg = collect_aggregate(&engine_batches);

    // ---- Analytical path (DuckDB via basin-analytical) ---------------------
    let analytical = AnalyticalEngine::new(AnalyticalConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        local_fs_root: Some(dir.path().to_path_buf()),
    })
    .unwrap();
    // Same warmup courtesy.
    let _ = analytical.query(&tenant, sql).await.unwrap();
    let mut analytical_batches = Vec::new();
    let t0 = Instant::now();
    for _ in 0..ITERATIONS {
        analytical_batches = analytical.query(&tenant, sql).await.unwrap();
    }
    let analytical_ms = t0.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;
    let analytical_agg = collect_aggregate(&analytical_batches);

    // ---- Correctness: same shape, same numbers -----------------------------
    assert_eq!(
        engine_agg.len(),
        GROUPS as usize,
        "engine returned {} groups, expected {GROUPS}",
        engine_agg.len()
    );
    assert_eq!(
        analytical_agg.len(),
        GROUPS as usize,
        "analytical returned {} groups, expected {GROUPS}",
        analytical_agg.len()
    );
    for (e, a) in engine_agg.iter().zip(analytical_agg.iter()) {
        assert_eq!(e.group_id, a.group_id, "group_id mismatch: {e:?} vs {a:?}");
        assert_eq!(e.count, a.count, "count mismatch: {e:?} vs {a:?}");
        assert_eq!(e.sum_len, a.sum_len, "sum_len mismatch: {e:?} vs {a:?}");
        // Both engines compute avg(id) on the same rows; tolerate a tiny
        // float wobble in the reduction order.
        let diff = (e.avg_id - a.avg_id).abs();
        assert!(
            diff < 1e-6 * e.avg_id.abs().max(1.0),
            "avg_id mismatch: {e:?} vs {a:?}"
        );
    }
    let total_count: i64 = engine_agg.iter().map(|a| a.count).sum();
    assert_eq!(total_count, ROWS, "row count drift: {total_count} != {ROWS}");

    // ---- Speedup -----------------------------------------------------------
    // Add 1ms floor on the divisor so a freakishly fast warm DuckDB run
    // doesn't blow this up to infinity.
    let analytical_ms_floor = analytical_ms.max(1.0);
    let speedup = engine_ms / analytical_ms_floor;
    let pass = speedup >= SPEEDUP_BAR;

    println!(
        "[VIABILITY analytical] rows={ROWS} files={FILES} groups={GROUPS} \
         engine_ms={engine_ms:.1} analytical_ms={analytical_ms:.1} \
         speedup={speedup:.2}x (bar >= {SPEEDUP_BAR}x) {status}",
        status = if pass { "PASS" } else { "FAIL" },
    );

    report_viability(
        "analytical",
        "Analytical path speedup vs OLTP engine",
        "On a 1M-row aggregate (count + avg + sum-of-lengths) with GROUP BY across \
         10 Parquet files, the DuckDB-backed analytical engine beats the \
         DataFusion-on-listing-table OLTP engine by at least 1.5x on the same dataset.",
        pass,
        PrimaryMetric {
            label: "analytical_speedup (engine_ms / analytical_ms)".into(),
            value: speedup,
            unit: "x".into(),
            bar: BarOp::ge(SPEEDUP_BAR),
        },
        json!({
            "rows": ROWS,
            "files": FILES,
            "groups": GROUPS,
            "engine_ms": engine_ms,
            "analytical_ms": analytical_ms,
            "speedup": speedup,
        }),
    );

    assert!(
        pass,
        "analytical_speedup {speedup:.2}x below bar {SPEEDUP_BAR}x \
         (engine={engine_ms:.1}ms analytical={analytical_ms:.1}ms)"
    );
}
