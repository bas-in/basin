//! S3 port of `viability_rls_isolation.rs`.
//!
//! Two projects, RLS toggled in every combination, asserting zero cross-project
//! row leakage. Same shape as the LocalFS variant; the storage backend is
//! real S3.
//!
//! Card id: `rls_isolation`. Bar: `cross_project_leak == 0`.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use serde_json::json;

const TEST_NAME: &str = "s3_viability_rls_isolation";

async fn count_forbidden_marker(sess: &ProjectSession, sql: &str, forbidden_marker: &str) -> usize {
    let res = sess.execute(sql).await.expect("query failed");
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return 0,
    };
    let mut leaks = 0;
    for b in &batches {
        let arr = b
            .column_by_name("marker")
            .expect("marker column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("marker is utf8");
        for i in 0..arr.len() {
            if arr.value(i) == forbidden_marker {
                leaks += 1;
            }
        }
    }
    leaks
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "live S3 / .basin-test.toml-gated; run with --ignored"]
async fn s3_viability_rls_isolation() {
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
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let project_a = ProjectId::new();
    let project_b = ProjectId::new();
    let marker_a = format!("A-{}", project_a);
    let marker_b = format!("B-{}", project_b);

    let admin_a = engine.open_session(project_a).await.unwrap();
    let admin_b = engine.open_session(project_b).await.unwrap();

    for s in [&admin_a, &admin_b] {
        s.execute(
            "CREATE TABLE events (id BIGINT NOT NULL, marker TEXT NOT NULL, owner TEXT NOT NULL)",
        )
        .await
        .unwrap();
    }

    let shared_a = engine.open_session_as(project_a, "shared").await.unwrap();
    let shared_b = engine.open_session_as(project_b, "shared").await.unwrap();

    for i in 0..5_i64 {
        admin_a
            .execute(&format!(
                "INSERT INTO events VALUES ({i}, '{marker_a}', 'shared')"
            ))
            .await
            .unwrap();
        admin_b
            .execute(&format!(
                "INSERT INTO events VALUES ({i}, '{marker_b}', 'shared')"
            ))
            .await
            .unwrap();
    }

    let mut leak_count: usize = 0;

    leak_count += count_forbidden_marker(&shared_a, "SELECT marker FROM events", &marker_b).await;
    leak_count += count_forbidden_marker(&shared_b, "SELECT marker FROM events", &marker_a).await;

    admin_a
        .execute("ALTER TABLE events ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin_a
        .execute("CREATE POLICY a_shared ON events FOR ALL TO PUBLIC USING (owner = current_user)")
        .await
        .unwrap();

    leak_count += count_forbidden_marker(&shared_a, "SELECT marker FROM events", &marker_b).await;
    leak_count += count_forbidden_marker(&shared_b, "SELECT marker FROM events", &marker_a).await;

    admin_b
        .execute("ALTER TABLE events ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin_b
        .execute(
            "CREATE POLICY b_shared ON events FOR SELECT TO PUBLIC USING (owner = current_user)",
        )
        .await
        .unwrap();

    leak_count += count_forbidden_marker(&shared_a, "SELECT marker FROM events", &marker_b).await;
    leak_count += count_forbidden_marker(&shared_b, "SELECT marker FROM events", &marker_a).await;

    admin_a
        .execute("DROP POLICY a_shared ON events")
        .await
        .unwrap();

    leak_count += count_forbidden_marker(&shared_a, "SELECT marker FROM events", &marker_b).await;
    leak_count += count_forbidden_marker(&shared_b, "SELECT marker FROM events", &marker_a).await;

    let a_post_drop = match shared_a.execute("SELECT marker FROM events").await.unwrap() {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        ExecResult::Empty { .. } => 0,
    };
    assert_eq!(
        a_post_drop, 0,
        "project A under RLS-on + no policy should see 0 rows; got {a_post_drop}"
    );
    let b_unaffected = match shared_b.execute("SELECT marker FROM events").await.unwrap() {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        ExecResult::Empty { .. } => 0,
    };
    assert_eq!(b_unaffected, 5);

    let pass = leak_count == 0;
    println!(
        "[S3 viability_rls_isolation] cross_project_leak={leak_count} (bar=0) {}",
        if pass { "PASS" } else { "FAIL" }
    );

    report_real_viability(
        "rls_isolation",
        "RLS preserves project prefix isolation (real S3)",
        "Cross-project row leakage is zero across every RLS configuration combination, with data hosted on real S3.",
        pass,
        PrimaryMetric {
            label: "cross_project_leak".into(),
            value: leak_count as f64,
            unit: "rows".into(),
            bar: BarOp::eq(0.0),
        },
        json!({
            "cross_project_leak": leak_count,
            "project_a": project_a.to_string(),
            "project_b": project_b.to_string(),
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert_eq!(
        leak_count, 0,
        "{leak_count} cross-project rows leaked under RLS"
    );
}
