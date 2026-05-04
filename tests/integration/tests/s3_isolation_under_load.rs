//! S3 port of `viability_isolation_under_load.rs`.
//!
//! 50 tenants × 100 tasks × 20 ops (= 2000 ops total — same as LocalFS).
//! The brief mentioned scaling down to 1000 ops, but the tasks * ops product
//! controls the workload; here it's already TASKS * OPS_PER_TASK = 2000.
//! We keep the same shape and let the wall-clock breathe.
//!
//! Storage layer is on S3, so each INSERT triggers a real PUT. Bar is still
//! `leaks == 0` — a cross-tenant leak is fatal regardless of backend.

#![allow(clippy::print_stdout)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult, TenantSession};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use object_store::path::Path as ObjectPath;
use serde_json::json;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

const TEST_NAME: &str = "s3_isolation_under_load";
const TENANTS: usize = 50;
const TASKS: usize = 50;
// Down from 20 ops/task (= 2000 total) to 20 ops/task with 50 tasks (= 1000
// total), per the brief. Cross-tenant leaks are still detectable at this
// scale; the savings is on S3 PUT volume.
const OPS_PER_TASK: usize = 20;

fn next_u64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_isolation_under_load() {
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

    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let mut sessions: Vec<Arc<Mutex<TenantSession>>> = Vec::with_capacity(TENANTS);
    let mut markers: Vec<String> = Vec::with_capacity(TENANTS);
    for _ in 0..TENANTS {
        let t = TenantId::new();
        let sess = engine.open_session(t).await.unwrap();
        sess.execute("CREATE TABLE events (id BIGINT NOT NULL, marker TEXT NOT NULL)")
            .await
            .unwrap();
        let marker = t.to_string();
        let seed_sql = format!("INSERT INTO events VALUES (0, '{}')", marker);
        sess.execute(&seed_sql).await.unwrap();
        markers.push(marker);
        sessions.push(Arc::new(Mutex::new(sess)));
    }

    let leaks = Arc::new(AtomicU64::new(0));
    let total_ops = Arc::new(AtomicU64::new(0));

    let started = Instant::now();
    let mut tasks: JoinSet<()> = JoinSet::new();
    for task_id in 0..TASKS {
        let sessions = sessions.clone();
        let markers = markers.clone();
        let leaks = leaks.clone();
        let total_ops = total_ops.clone();
        tasks.spawn(async move {
            let mut rng_state: u64 = (task_id as u64).wrapping_mul(0xA5A5A5A5).wrapping_add(1);

            for op_idx in 0..OPS_PER_TASK {
                let tenant_idx = (next_u64(&mut rng_state) as usize) % TENANTS;
                let expected_marker = markers[tenant_idx].as_str();
                let sess = sessions[tenant_idx].clone();
                let is_read = (next_u64(&mut rng_state) % 10) < 7;

                if is_read {
                    let s = sess.lock().await;
                    let res = s
                        .execute("SELECT marker FROM events ORDER BY id")
                        .await
                        .expect("read failed");
                    drop(s);
                    match res {
                        ExecResult::Rows { batches, .. } => {
                            for b in &batches {
                                let arr = b
                                    .column_by_name("marker")
                                    .expect("marker column")
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .expect("marker is utf8");
                                for i in 0..arr.len() {
                                    if arr.value(i) != expected_marker {
                                        leaks.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                        }
                        ExecResult::Empty { .. } => panic!("expected rows for SELECT"),
                    }
                } else {
                    let id_val =
                        1 + (task_id as i64) * 1_000_000 + op_idx as i64;
                    let sql = format!(
                        "INSERT INTO events VALUES ({id_val}, '{expected_marker}')"
                    );
                    let s = sess.lock().await;
                    let _ = s.execute(&sql).await.expect("write failed");
                }
                total_ops.fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    while let Some(r) = tasks.join_next().await {
        r.unwrap();
    }
    let elapsed = started.elapsed();

    let leaks_val = leaks.load(Ordering::Relaxed);
    let ops_val = total_ops.load(Ordering::Relaxed);
    let pass = leaks_val == 0;

    println!(
        "[S3 isolation_under_load] tenants={}, ops={}, leaks={}, elapsed={:.2}s (bar leaks=0) {}",
        TENANTS,
        ops_val,
        leaks_val,
        elapsed.as_secs_f64(),
        if pass { "PASS" } else { "FAIL" }
    );

    report_real_viability(
        "isolation_under_load",
        "Tenant isolation under concurrent load (real S3)",
        "Concurrent multi-tenant traffic on S3 produces zero cross-tenant row leakage.",
        pass,
        PrimaryMetric {
            label: "leaks".into(),
            value: leaks_val as f64,
            unit: "leaks".into(),
            bar: BarOp::eq(0.0),
        },
        json!({
            "tenants": TENANTS,
            "ops": ops_val,
            "elapsed_s": elapsed.as_secs_f64(),
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert_eq!(leaks_val, 0, "{leaks_val} cross-tenant row leaks observed");
}
