//! S3 port of `viability_update_delete.rs`.
//!
//! Same copy-on-write UPDATE / DELETE shape but with `Storage` rooted on
//! real S3. Per-mutation cost on S3 grows because every CoW rewrite is a
//! sequence of S3 PUTs + DELETEs, not in-place inode work; we expect the
//! absolute number to be measurably higher than LocalFS but still well
//! under the 2 s budget for a 10K-row table.
//!
//! Bar: same 2 s total budget. Honest framing: this test surfaces the
//! S3-PUT-floor cost of CoW mutations on a small table.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use serde_json::json;

const TEST_NAME: &str = "s3_viability_update_delete";
const ROWS: usize = 10_000;
const BUDGET_SECS: f64 = 2.0;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_viability_update_delete() {
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
    let eng = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .unwrap();

    let mut sql = String::with_capacity(ROWS * 24);
    sql.push_str("INSERT INTO t VALUES ");
    for i in 0..ROWS {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i}, 'row-{i}')"));
    }
    sess.execute(&sql).await.unwrap();

    let t0 = Instant::now();
    let res = sess
        .execute("UPDATE t SET name = 'patched' WHERE id = 4242")
        .await
        .unwrap();
    let update_ms = t0.elapsed().as_secs_f64() * 1000.0;
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "UPDATE 1"),
        other => panic!("unexpected: {other:?}"),
    }

    let t0 = Instant::now();
    let res = sess.execute("DELETE FROM t WHERE id < 3").await.unwrap();
    let delete_ms = t0.elapsed().as_secs_f64() * 1000.0;
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "DELETE 3"),
        other => panic!("unexpected: {other:?}"),
    }

    let after = sess
        .execute("SELECT name FROM t WHERE id = 4242")
        .await
        .unwrap();
    let name = match after {
        ExecResult::Rows { batches, .. } => {
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
            let arr = batches[0]
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            arr.value(0).to_string()
        }
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(name, "patched");

    let row_count = sess.execute("SELECT id FROM t").await.unwrap();
    let total = match row_count {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(total, ROWS - 3);

    let total_ms = update_ms + delete_ms;
    let pass = total_ms < BUDGET_SECS * 1000.0;

    println!(
        "[S3 update_delete] rows={ROWS} update_ms={update_ms:.1} delete_ms={delete_ms:.1} total_ms={total_ms:.1} (bar < {budget:.0} ms) {status}",
        budget = BUDGET_SECS * 1000.0,
        status = if pass { "PASS" } else { "FAIL" },
    );

    report_real_viability(
        "update_delete",
        "UPDATE / DELETE via copy-on-write (real S3)",
        "An UPDATE and a DELETE on a 10K-row real-S3 table commit end-to-end (parser → executor → S3 rewrite → catalog Replace snapshot) in under 2 seconds.",
        pass,
        PrimaryMetric {
            label: "UPDATE+DELETE elapsed (ms)".into(),
            value: total_ms,
            unit: "ms".into(),
            bar: BarOp::lt(BUDGET_SECS * 1000.0),
        },
        json!({
            "rows": ROWS,
            "update_ms": update_ms,
            "delete_ms": delete_ms,
            "total_ms": total_ms,
            "remaining_rows": total,
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(
        pass,
        "UPDATE+DELETE took {total_ms:.1} ms on a {ROWS}-row table; budget {} ms",
        BUDGET_SECS * 1000.0
    );
}
