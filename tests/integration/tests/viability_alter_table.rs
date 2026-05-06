//! Viability test: `ALTER TABLE` SQL surface.
//!
//! Card: `viability_alter_table`
//! Bar: every ALTER form below runs without error and the catalog
//! reflects the change after.
//!
//! Forms exercised:
//!
//! ```sql
//! ALTER TABLE events ADD COLUMN device_id TEXT;
//! ALTER TABLE events SET cold_after = 7776000;          -- 90 days
//! ALTER TABLE events SET cold_age_column = 'ts';
//! ALTER TABLE events SET BLOOM FILTERS ON (id, owner_id);
//! ALTER TABLE events ENABLE ROW LEVEL SECURITY;
//! ALTER TABLE events DISABLE ROW LEVEL SECURITY;
//! CREATE POLICY p_owner ON events FOR ALL TO PUBLIC USING (owner_id = current_user);
//! DROP POLICY p_owner ON events;
//! ```
//!
//! After each statement we re-`load_table` and assert the catalog field
//! the statement was *supposed* to change has the expected value. This
//! is the "the catalog reflects the change" half of the bar; the
//! "without error" half is implicit in the unwraps.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::{InMemoryCatalog, PolicyCommand};
use basin_common::{TableName, TenantId};
use basin_engine::{Engine, EngineConfig};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn viability_alter_table() {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog: catalog.clone(),
        shard: None,
    });

    let tenant = TenantId::new();
    let sess = engine.open_session(tenant).await.unwrap();

    // Setup: a table with a TIMESTAMPTZ-ish age column (BIGINT-as-epoch
    // because the catalog accepts that for cold_age_column).
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

    // 1) ADD COLUMN — the table schema gains a new column.
    sess.execute("ALTER TABLE events ADD COLUMN device_id TEXT")
        .await
        .expect("ADD COLUMN");
    let m = catalog.load_table(&tenant, &table).await.unwrap();
    assert!(
        m.schema.field_with_name("device_id").is_ok(),
        "device_id missing from schema after ADD COLUMN"
    );
    let n_cols_after_add = m.schema.fields().len();
    assert_eq!(n_cols_after_add, 5, "expected 5 cols, got {n_cols_after_add}");
    variants_passed.push("ADD COLUMN");

    // 2) SET cold_after — tier policy gains a threshold.
    sess.execute("ALTER TABLE events SET cold_after = 7776000")
        .await
        .expect("SET cold_after");
    let m = catalog.load_table(&tenant, &table).await.unwrap();
    assert_eq!(
        m.cold_after_seconds,
        Some(7_776_000),
        "cold_after_seconds didn't take"
    );
    variants_passed.push("SET cold_after");

    // 3) SET cold_age_column — the column the tier policy keys off of.
    sess.execute("ALTER TABLE events SET cold_age_column = 'ts'")
        .await
        .expect("SET cold_age_column");
    let m = catalog.load_table(&tenant, &table).await.unwrap();
    assert_eq!(m.cold_age_column.as_deref(), Some("ts"));
    // And cold_after should still be set — the two SETs compose.
    assert_eq!(m.cold_after_seconds, Some(7_776_000));
    variants_passed.push("SET cold_age_column");

    // 4) SET BLOOM FILTERS ON (...) — bloom-filter column set.
    sess.execute("ALTER TABLE events SET BLOOM FILTERS ON (id, owner_id)")
        .await
        .expect("SET BLOOM FILTERS ON");
    let m = catalog.load_table(&tenant, &table).await.unwrap();
    assert_eq!(
        m.bloom_filter_columns,
        vec!["id".to_string(), "owner_id".to_string()]
    );
    variants_passed.push("SET BLOOM FILTERS ON");

    // 5) ENABLE ROW LEVEL SECURITY.
    sess.execute("ALTER TABLE events ENABLE ROW LEVEL SECURITY")
        .await
        .expect("ENABLE ROW LEVEL SECURITY");
    let m = catalog.load_table(&tenant, &table).await.unwrap();
    assert!(m.rls_enabled, "rls_enabled should be true");
    variants_passed.push("ENABLE ROW LEVEL SECURITY");

    // 6) CREATE POLICY (standalone). RLS DDL is wired through the same
    //    catalog mutator (`set_rls_state`) under the hood — confirm
    //    that, after CREATE POLICY, the policy turns up in the catalog.
    sess.execute(
        "CREATE POLICY p_owner ON events FOR ALL TO PUBLIC USING (owner_id = current_user)",
    )
    .await
    .expect("CREATE POLICY");
    let m = catalog.load_table(&tenant, &table).await.unwrap();
    assert_eq!(m.policies.len(), 1);
    assert_eq!(m.policies[0].name, "p_owner");
    assert!(matches!(m.policies[0].command, PolicyCommand::All));
    variants_passed.push("CREATE POLICY");

    // 7) DROP POLICY (standalone).
    sess.execute("DROP POLICY p_owner ON events")
        .await
        .expect("DROP POLICY");
    let m = catalog.load_table(&tenant, &table).await.unwrap();
    assert_eq!(m.policies.len(), 0, "policy list should be empty after DROP");
    variants_passed.push("DROP POLICY");

    // 8) DISABLE ROW LEVEL SECURITY — flip the bit off.
    sess.execute("ALTER TABLE events DISABLE ROW LEVEL SECURITY")
        .await
        .expect("DISABLE ROW LEVEL SECURITY");
    let m = catalog.load_table(&tenant, &table).await.unwrap();
    assert!(!m.rls_enabled, "rls_enabled should be false after DISABLE");
    variants_passed.push("DISABLE ROW LEVEL SECURITY");

    // Sanity: ADD COLUMN with NOT NULL is currently rejected (we'd need
    // a back-fill story for existing rows). Surface the rejection so
    // future contributors notice if the constraint relaxes.
    let res = sess
        .execute("ALTER TABLE events ADD COLUMN required_col TEXT NOT NULL")
        .await;
    assert!(res.is_err(), "ADD COLUMN ... NOT NULL should be rejected in v0.1");

    let pass = variants_passed.len() == 8;
    println!(
        "[VIABILITY ALTER TABLE] {} of 8 variants passed: {:?}",
        variants_passed.len(),
        variants_passed
    );

    report_viability(
        "alter_table",
        "ALTER TABLE SQL surface",
        "Every ALTER form (ADD COLUMN, SET cold_after, SET cold_age_column, \
         SET BLOOM FILTERS ON, ENABLE/DISABLE ROW LEVEL SECURITY, \
         CREATE/DROP POLICY) parses, runs, and persists to the catalog.",
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
        }),
    );

    assert!(pass, "expected all 8 ALTER variants to pass; got {variants_passed:?}");
}
