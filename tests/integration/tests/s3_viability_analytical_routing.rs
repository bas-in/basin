//! S3 port of `viability_analytical_routing.rs`.
//!
//! Same hypothesis as the LocalFS test — engine routes GROUP BY to the
//! analytical engine — but the dataset lives on real S3 (configured
//! `[s3]` block). v0.1 limitation mirrors the sibling `s3_viability_analytical`
//! card: the analytical engine cannot read S3 directly because DuckDB needs
//! `local_fs_root`. We exercise the storage write path on S3 and the OLTP
//! routing baseline; the routed-engine timing requires v0.2 httpfs.
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

const TEST_NAME: &str = "s3_viability_analytical_routing";
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
async fn s3_viability_analytical_routing() {
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

    // OLTP-only baseline (no analytical engine attached).
    let oltp_engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: None,
    });
    let oltp_sess = oltp_engine.open_session(tenant).await.unwrap();
    let _ = oltp_sess.execute(sql).await.unwrap();
    let mut total_rows = 0usize;
    let t0 = Instant::now();
    for _ in 0..ITERATIONS {
        let res = oltp_sess.execute(sql).await.unwrap();
        total_rows = match res {
            ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
            other => panic!("OLTP engine returned non-rows: {other:?}"),
        };
    }
    let oltp_ms = t0.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;
    assert_eq!(total_rows, GROUPS as usize, "expected {GROUPS} groups");
    assert_eq!(
        oltp_engine.analytical_routing_count(),
        0,
        "OLTP-only engine should not route anything"
    );

    // Routed engine: in v0.1, attaching AnalyticalEngine to a real-S3
    // Storage is impossible (no local_fs_root). The card faithfully
    // reports the gap and the routed_ms / speedup as unavailable. v0.2
    // (DuckDB httpfs) flips this card green.
    let routed_ms = f64::INFINITY;
    let routings = 0u64;
    let speedup = 0.0;
    let pass = speedup >= SPEEDUP_BAR;

    println!(
        "[S3 viability_analytical_routing] rows={ROWS} files={FILES} groups={GROUPS} \
         oltp_ms={oltp_ms:.1} routed=v0.2 (DuckDB httpfs) \
         speedup={speedup:.2}x (bar >= {SPEEDUP_BAR}x) FAIL (v0.1 scope)",
    );

    report_real_viability(
        "analytical_routing",
        "Analytical routing speedup through basin-engine (real S3)",
        "Same SQL through the same engine entry point: with the analytical \
         engine attached, GROUP BY queries route to DuckDB and finish at \
         least 1.5x faster than the OLTP-only baseline on a 1M-row real-S3 \
         dataset. v0.1 limitation: DuckDB needs a LocalFS root; httpfs is \
         v0.2.",
        pass,
        PrimaryMetric {
            label: "analytical_routing_speedup (oltp_ms / routed_ms)".into(),
            value: speedup,
            unit: "x".into(),
            bar: BarOp::ge(SPEEDUP_BAR),
        },
        json!({
            "rows": ROWS,
            "files": FILES,
            "groups": GROUPS,
            "oltp_ms_on_s3": oltp_ms,
            "routed_ms": routed_ms,
            "speedup": speedup,
            "routings": routings,
            "v01_limitation": "basin-analytical requires local_fs_root; DuckDB httpfs is v0.2",
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );
}
