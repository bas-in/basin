//! S3 port of `viability_analytical.rs`.
//!
//! Writes the same 1M-row / 10-file analytical fixture to **real S3** through
//! `basin-storage`, then attempts the engine-vs-engine comparison. The
//! storage I/O is exercised against the configured S3-compatible service
//! (local MinIO or real cloud).
//!
//! Honest scope note: in v0.1, `basin-analytical` requires a `local_fs_root`
//! because DuckDB's `read_parquet` consumes absolute filesystem paths.
//! `object_store::AmazonS3` cannot satisfy that. The v0.1 brief calls out
//! that DuckDB `httpfs`-on-S3 is a v0.2 item. We therefore:
//!
//! 1. Write all 10 parquet files via `Storage` against real S3 — proving the
//!    write path holds at this dataset size.
//! 2. Time the OLTP path (DataFusion-on-listing-table) against those S3
//!    files, end-to-end, with the same SQL the LocalFS test runs.
//! 3. Report a viability card with `passed=false` and the reason text — the
//!    bar is the same 1.5x speedup the LocalFS test enforces, and the v0.1
//!    real-S3 analytical engine cannot beat itself by 1.5x because it
//!    cannot run at all.
//!
//! When v0.2 lands DuckDB-httpfs the card flips green automatically.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use serde_json::json;

const TEST_NAME: &str = "s3_viability_analytical";
const ROWS: i64 = 1_000_000;
const FILES: i64 = 10;
const ROWS_PER_FILE: i64 = ROWS / FILES;
const GROUPS: i64 = 16;
const ITERATIONS: u32 = 5;
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
    let payloads: Vec<String> = (0..len).map(|i| format!("p-{:08}", start + i)).collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(
        schema(),
        vec![Arc::new(ids), Arc::new(groups), Arc::new(payload_arr)],
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_viability_analytical() {
    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };

    let object_store = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
        store: object_store.clone(),
        prefix: run_prefix.clone(),
    };

    let storage = Storage::new(StorageConfig {
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    let tenant = TenantId::new();
    let table = TableName::new("t").unwrap();
    let part = PartitionKey::default_key();

    catalog
        .create_table(&tenant, &table, schema().as_ref())
        .await
        .unwrap();

    // Seed 1M rows across 10 files. This is the actual S3 PUT volume we
    // care about exercising — every file lands in the configured bucket.
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
    let files = storage.list_data_files(&tenant, &table).await.unwrap();
    assert_eq!(files.len(), FILES as usize, "expected {FILES} parquet files");

    let sql = "SELECT group_id, count(*) AS n, avg(id) AS avg_id, \
                      CAST(sum(length(payload)) AS BIGINT) AS s_len \
               FROM t GROUP BY group_id ORDER BY group_id";

    // ---- OLTP path on S3 (DataFusion via basin-engine) -----------------
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: None,
    });
    let sess = engine.open_session(tenant).await.unwrap();
    let _ = sess.execute(sql).await.unwrap(); // warmup
    let mut total_rows = 0usize;
    let t0 = Instant::now();
    for _ in 0..ITERATIONS {
        let res = sess.execute(sql).await.unwrap();
        total_rows = match res {
            ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
            other => panic!("engine returned non-rows: {other:?}"),
        };
    }
    let engine_ms = t0.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;
    assert_eq!(total_rows, GROUPS as usize, "expected {GROUPS} groups");

    // ---- Analytical path on S3: not implemented in v0.1 -------------------
    // basin-analytical requires a local_fs_root because DuckDB's
    // read_parquet consumes absolute filesystem paths. AmazonS3 doesn't
    // satisfy that contract. v0.2 wires DuckDB httpfs and the speedup
    // measurement becomes available; until then the speedup is reported
    // as 0.0x (no analytical run completed) and the card honestly fails
    // its bar.
    let analytical_ms = f64::INFINITY; // not run
    let speedup = 0.0;
    let pass = speedup >= SPEEDUP_BAR;

    println!(
        "[S3 viability_analytical] rows={ROWS} files={FILES} groups={GROUPS} \
         engine_ms={engine_ms:.1} analytical=v0.2 (DuckDB httpfs) \
         speedup={speedup:.2}x (bar >= {SPEEDUP_BAR}x) FAIL (v0.1 scope)",
    );

    report_real_viability(
        "analytical",
        "Analytical path speedup vs OLTP engine (real S3)",
        "On a 1M-row aggregate (count + avg + sum-of-lengths) with GROUP BY \
         across 10 Parquet files on real S3, DuckDB beats DataFusion by at \
         least 1.5x. v0.1 limitation: DuckDB requires a LocalFS root; \
         analytical-over-S3 needs httpfs wiring (v0.2). The card flips green \
         when that lands.",
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
            "engine_ms_oltp_on_s3": engine_ms,
            "analytical_ms": analytical_ms,
            "speedup": speedup,
            "v01_limitation": "basin-analytical requires local_fs_root; DuckDB httpfs is v0.2",
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    // No assertion: the gap is a documented v0.1 scope, not a regression.
    // The dashboard shows the card as failed-with-reason which IS the truth.
}
