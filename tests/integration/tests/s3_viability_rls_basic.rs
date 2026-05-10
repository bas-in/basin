//! S3 port of `viability_rls_basic.rs`.
//!
//! Per-principal RLS predicate injection, with the underlying object store
//! pointed at real S3. RLS is a metadata + plan-rewrite concern; the only
//! thing the storage backend changes is where the seed data lives. We keep
//! the same shape — alice + bob each insert 5 rows, RLS toggles, anon
//! deny-default — and assert per-principal visibility under the policy.
//!
//! Card id: `rls_basic`. Bar: `alice_rows_visible == 5`.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use serde_json::json;

const TEST_NAME: &str = "s3_viability_rls_basic";

fn rows_seen(res: ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty({tag})"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_viability_rls_basic() {
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

    let tenant = TenantId::new();

    let admin = engine.open_session(tenant).await.unwrap();
    admin
        .execute("CREATE TABLE orders (id BIGINT NOT NULL, owner_id TEXT NOT NULL, payload TEXT NOT NULL)")
        .await
        .unwrap();

    let alice = engine.open_session_as(tenant, "alice").await.unwrap();
    let bob = engine.open_session_as(tenant, "bob").await.unwrap();

    for i in 1..=5_i64 {
        alice
            .execute(&format!(
                "INSERT INTO orders VALUES ({i}, 'alice', 'a-payload-{i}')"
            ))
            .await
            .unwrap();
    }
    for i in 1..=5_i64 {
        bob.execute(&format!(
            "INSERT INTO orders VALUES ({}, 'bob', 'b-payload-{i}')",
            i + 100
        ))
        .await
        .unwrap();
    }

    let pre_rls = rows_seen(alice.execute("SELECT * FROM orders").await.unwrap());
    assert_eq!(pre_rls, 10, "before ENABLE RLS, alice sees all 10 rows");

    admin
        .execute("ALTER TABLE orders ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin
        .execute(
            "CREATE POLICY orders_owner_access ON orders FOR ALL TO PUBLIC \
             USING (owner_id = current_user)",
        )
        .await
        .unwrap();

    let alice_rows = rows_seen(alice.execute("SELECT * FROM orders").await.unwrap());
    let bob_rows = rows_seen(bob.execute("SELECT * FROM orders").await.unwrap());
    assert_eq!(
        alice_rows, 5,
        "alice sees her 5 rows under RLS, got {alice_rows}"
    );
    assert_eq!(bob_rows, 5, "bob sees his 5 rows under RLS, got {bob_rows}");

    let anon = engine.open_session(tenant).await.unwrap();
    let anon_rows = rows_seen(anon.execute("SELECT * FROM orders").await.unwrap());
    assert_eq!(
        anon_rows, 0,
        "anon sees zero rows under RLS, got {anon_rows}"
    );

    admin
        .execute("ALTER TABLE orders DISABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    let alice_after = rows_seen(alice.execute("SELECT * FROM orders").await.unwrap());
    let bob_after = rows_seen(bob.execute("SELECT * FROM orders").await.unwrap());
    let anon_after = rows_seen(anon.execute("SELECT * FROM orders").await.unwrap());
    assert_eq!(alice_after, 10);
    assert_eq!(bob_after, 10);
    assert_eq!(anon_after, 10);

    admin
        .execute("ALTER TABLE orders ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin
        .execute("DROP POLICY orders_owner_access ON orders")
        .await
        .unwrap();
    let alice_no_policy = rows_seen(alice.execute("SELECT * FROM orders").await.unwrap());
    assert_eq!(alice_no_policy, 0, "RLS on + no policy must deny-all");

    let pass = alice_rows == 5 && bob_rows == 5 && pre_rls == 10 && anon_rows == 0;
    println!(
        "[S3 viability_rls_basic] alice={alice_rows}, bob={bob_rows}, pre={pre_rls}, anon={anon_rows} {}",
        if pass { "PASS" } else { "FAIL" }
    );

    report_real_viability(
        "rls_basic",
        "Row-level security: per-principal predicate injection (real S3)",
        "RLS isolates rows per `current_user` while preserving full visibility when disabled. Object store: real S3.",
        pass,
        PrimaryMetric {
            label: "alice_rows_visible".into(),
            value: alice_rows as f64,
            unit: "rows".into(),
            bar: BarOp::eq(5.0),
        },
        json!({
            "alice_rows_visible": alice_rows,
            "bob_rows_visible": bob_rows,
            "anonymous_rows_visible": anon_rows,
            "all_rows_without_rls": pre_rls,
            "all_rows_after_drop_policy_with_rls_on": alice_no_policy,
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(pass);
}
