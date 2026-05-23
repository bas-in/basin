//! S3 port of `viability_alter_table.rs`.
//!
//! Card: `viability_alter_table` (real-cloud dashboard).
//! Bar: every ALTER form runs without error against a real S3-compatible
//! backend and the catalog reflects the change after — same shape as
//! the LocalFS card.
//!
//! The 8 ALTER variants exercised:
//!
//! ```sql
//! ALTER TABLE events ADD COLUMN device_id TEXT;
//! ALTER TABLE events SET cold_after = 7776000;
//! ALTER TABLE events SET cold_age_column = 'ts';
//! ALTER TABLE events SET BLOOM FILTERS ON (id, owner_id);
//! ALTER TABLE events ENABLE ROW LEVEL SECURITY;
//! ALTER TABLE events DISABLE ROW LEVEL SECURITY;
//! CREATE POLICY p_owner ON events FOR ALL TO PUBLIC USING (owner_id = current_user);
//! DROP POLICY p_owner ON events;
//! ```
//!
//! On real S3 the catalog round-trip is the same in-memory `InMemoryCatalog`
//! as the LocalFS card — the only difference is the underlying `Storage`
//! sits on a remote object store. The point of this card on the real-cloud
//! dashboard is to demonstrate that ALTER TABLE plumbing — parse, plan,
//! catalog mutation — is identical end-to-end across LocalFS and real S3.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::{InMemoryCatalog, PolicyCommand};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use serde_json::json;

const TEST_NAME: &str = "s3_viability_alter_table";

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "live S3 / .basin-test.toml-gated; run with --ignored"]
async fn s3_viability_alter_table() {
    basin_common::telemetry::try_init_for_tests();

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
        catalog: catalog.clone(),
        shard: None,
    });

    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute(
        "CREATE TABLE events (\
            id BIGINT NOT NULL, \
            owner_id TEXT NOT NULL, \
            ts BIGINT NOT NULL, \
            payload TEXT NOT NULL\
        )",
    )
    .await
    .unwrap();

    let table = TableName::new("events").unwrap();
    let mut variants_passed: Vec<&'static str> = Vec::new();

    // 1) ADD COLUMN
    sess.execute("ALTER TABLE events ADD COLUMN device_id TEXT")
        .await
        .expect("ADD COLUMN");
    let m = catalog.load_table(&project, &table).await.unwrap();
    assert!(
        m.schema.field_with_name("device_id").is_ok(),
        "device_id missing from schema after ADD COLUMN"
    );
    let n_cols_after_add = m.schema.fields().len();
    assert_eq!(
        n_cols_after_add, 5,
        "expected 5 cols, got {n_cols_after_add}"
    );
    variants_passed.push("ADD COLUMN");

    // 2) SET cold_after
    sess.execute("ALTER TABLE events SET cold_after = 7776000")
        .await
        .expect("SET cold_after");
    let m = catalog.load_table(&project, &table).await.unwrap();
    assert_eq!(m.cold_after_seconds, Some(7_776_000));
    variants_passed.push("SET cold_after");

    // 3) SET cold_age_column
    sess.execute("ALTER TABLE events SET cold_age_column = 'ts'")
        .await
        .expect("SET cold_age_column");
    let m = catalog.load_table(&project, &table).await.unwrap();
    assert_eq!(m.cold_age_column.as_deref(), Some("ts"));
    assert_eq!(m.cold_after_seconds, Some(7_776_000));
    variants_passed.push("SET cold_age_column");

    // 4) SET BLOOM FILTERS ON
    sess.execute("ALTER TABLE events SET BLOOM FILTERS ON (id, owner_id)")
        .await
        .expect("SET BLOOM FILTERS ON");
    let m = catalog.load_table(&project, &table).await.unwrap();
    assert_eq!(
        m.bloom_filter_columns,
        vec!["id".to_string(), "owner_id".to_string()]
    );
    variants_passed.push("SET BLOOM FILTERS ON");

    // 5) ENABLE ROW LEVEL SECURITY
    sess.execute("ALTER TABLE events ENABLE ROW LEVEL SECURITY")
        .await
        .expect("ENABLE ROW LEVEL SECURITY");
    let m = catalog.load_table(&project, &table).await.unwrap();
    assert!(m.rls_enabled, "rls_enabled should be true");
    variants_passed.push("ENABLE ROW LEVEL SECURITY");

    // 6) CREATE POLICY
    sess.execute(
        "CREATE POLICY p_owner ON events FOR ALL TO PUBLIC USING (owner_id = current_user)",
    )
    .await
    .expect("CREATE POLICY");
    let m = catalog.load_table(&project, &table).await.unwrap();
    assert_eq!(m.policies.len(), 1);
    assert_eq!(m.policies[0].name, "p_owner");
    assert!(matches!(m.policies[0].command, PolicyCommand::All));
    variants_passed.push("CREATE POLICY");

    // 7) DROP POLICY
    sess.execute("DROP POLICY p_owner ON events")
        .await
        .expect("DROP POLICY");
    let m = catalog.load_table(&project, &table).await.unwrap();
    assert_eq!(
        m.policies.len(),
        0,
        "policy list should be empty after DROP"
    );
    variants_passed.push("DROP POLICY");

    // 8) DISABLE ROW LEVEL SECURITY
    sess.execute("ALTER TABLE events DISABLE ROW LEVEL SECURITY")
        .await
        .expect("DISABLE ROW LEVEL SECURITY");
    let m = catalog.load_table(&project, &table).await.unwrap();
    assert!(!m.rls_enabled, "rls_enabled should be false after DISABLE");
    variants_passed.push("DISABLE ROW LEVEL SECURITY");

    // Sanity (matches LocalFS card): NOT NULL ADD COLUMN must still be rejected.
    let res = sess
        .execute("ALTER TABLE events ADD COLUMN required_col TEXT NOT NULL")
        .await;
    assert!(
        res.is_err(),
        "ADD COLUMN ... NOT NULL should be rejected in v0.1"
    );

    let pass = variants_passed.len() == 8;
    println!(
        "[S3 viability_alter_table] {} of 8 variants passed: {:?}",
        variants_passed.len(),
        variants_passed
    );

    report_real_viability(
        "alter_table",
        "ALTER TABLE SQL surface (real S3)",
        "Every ALTER form (ADD COLUMN, SET cold_after, SET cold_age_column, \
         SET BLOOM FILTERS ON, ENABLE/DISABLE ROW LEVEL SECURITY, \
         CREATE/DROP POLICY) parses, runs, and persists to the catalog \
         when Storage is backed by a real S3-compatible object store.",
        pass,
        PrimaryMetric {
            label: "alter_variants_passed".into(),
            value: variants_passed.len() as f64,
            unit: "variants".into(),
            bar: BarOp::eq(8.0),
        },
        json!({
            "variants_passed": variants_passed,
            "schema_cols_after_add_column": n_cols_after_add,
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(
        pass,
        "expected all 8 ALTER variants to pass; got {variants_passed:?}"
    );
}
