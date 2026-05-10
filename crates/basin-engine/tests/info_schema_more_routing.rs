//! Integration tests for engine-side routing of the Phase 5.11.M Tier 3
//! follow-up views: `pg_catalog.pg_index`, `pg_catalog.pg_constraint`,
//! `information_schema.views`, and `information_schema.schemata`.
//!
//! Each test opens a `TenantSession` and runs SQL through the same
//! `execute()` entry point a pgwire connection would hit. The asserts
//! cover:
//!
//! - SELECTs against pg_index / pg_constraint succeed and return zero rows
//!   (v0.1 has no user-defined indexes / constraint surfaces).
//! - SELECTs against information_schema.views surface continuous
//!   materialized views created via `CREATE MATERIALIZED VIEW ... WITH
//!   (basin.continuous, ...)`.
//! - SELECTs against information_schema.schemata return exactly one row
//!   per tenant with `schema_name = 'public'`.
//! - Cross-tenant isolation: A's matview is invisible to B.

use std::sync::Arc;

use arrow_array::{Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult, TenantSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

fn col_string(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            if arr.is_null(i) {
                out.push(String::new());
            } else {
                out.push(arr.value(i).to_string());
            }
        }
    }
    out
}

fn total_rows(batches: &[arrow_array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

async fn rows(sess: &TenantSession, sql: &str) -> Vec<arrow_array::RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

#[tokio::test]
async fn select_pg_index_routes() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (x BIGINT NOT NULL)")
        .await
        .unwrap();

    // v0.1: pg_index returns zero rows (no user-defined index surface yet).
    let batches = rows(&sess, "SELECT * FROM pg_catalog.pg_index").await;
    assert_eq!(total_rows(&batches), 0);
}

#[tokio::test]
async fn select_pg_constraint_routes() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (x BIGINT NOT NULL)")
        .await
        .unwrap();

    // pg_constraint emits one row per constraint surface. Here the only
    // declared constraint on `t` is `x NOT NULL`, so exactly one row
    // (contype='n') is expected. This is the routing smoke test —
    // populating-content invariants are pinned in
    // `crates/basin-engine/tests/constraints.rs`.
    let batches = rows(&sess, "SELECT * FROM pg_catalog.pg_constraint").await;
    assert_eq!(total_rows(&batches), 1);
}

#[tokio::test]
async fn select_information_schema_views_routes() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    // Create a source table + a continuous matview over it. The matview
    // must show up in information_schema.views.
    sess.execute("CREATE TABLE events (bucket BIGINT NOT NULL, n BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO events VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    sess.execute(
        "CREATE MATERIALIZED VIEW mv \
         WITH (basin.continuous, refresh_interval = '5m') \
         AS SELECT bucket, sum(n) AS total FROM events GROUP BY bucket",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "SELECT table_name, view_definition, check_option, is_updatable, \
                is_insertable_into FROM information_schema.views ORDER BY table_name",
    )
    .await;
    assert_eq!(total_rows(&batches), 1);
    let names = col_string(&batches, "table_name");
    let defs = col_string(&batches, "view_definition");
    let checks = col_string(&batches, "check_option");
    let updatables = col_string(&batches, "is_updatable");
    let insertables = col_string(&batches, "is_insertable_into");
    assert_eq!(names, vec!["mv".to_string()]);
    assert!(
        defs[0].to_ascii_lowercase().contains("from events"),
        "view_definition should expose the SELECT body, got: {:?}",
        defs[0]
    );
    assert_eq!(checks, vec!["NONE".to_string()]);
    assert_eq!(updatables, vec!["NO".to_string()]);
    assert_eq!(insertables, vec!["NO".to_string()]);
}

#[tokio::test]
async fn select_information_schema_schemata_routes() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    let batches = rows(
        &sess,
        "SELECT catalog_name, schema_name, schema_owner \
         FROM information_schema.schemata",
    )
    .await;
    assert_eq!(total_rows(&batches), 1);
    let cats = col_string(&batches, "catalog_name");
    let schemas = col_string(&batches, "schema_name");
    let owners = col_string(&batches, "schema_owner");
    assert_eq!(cats, vec!["basin".to_string()]);
    assert_eq!(schemas, vec!["public".to_string()]);
    // schema_owner is the empty placeholder string in v0.1.
    assert_eq!(owners, vec![String::new()]);
}

#[tokio::test]
async fn cross_tenant_isolation_views_at_select() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let a = TenantId::new();
    let b = TenantId::new();
    let sa = eng.open_session(a).await.unwrap();
    let sb = eng.open_session(b).await.unwrap();

    // Tenant A creates a continuous matview.
    sa.execute("CREATE TABLE events (bucket BIGINT NOT NULL, n BIGINT NOT NULL)")
        .await
        .unwrap();
    sa.execute("INSERT INTO events VALUES (1, 10)")
        .await
        .unwrap();
    sa.execute(
        "CREATE MATERIALIZED VIEW secret_mv \
         WITH (basin.continuous, refresh_interval = '5m') \
         AS SELECT bucket, sum(n) AS total FROM events GROUP BY bucket",
    )
    .await
    .unwrap();

    // Tenant B creates a plain table only — no matview.
    sb.execute("CREATE TABLE plain_b (x BIGINT NOT NULL)")
        .await
        .unwrap();

    let names_a = col_string(
        &rows(
            &sa,
            "SELECT table_name FROM information_schema.views ORDER BY table_name",
        )
        .await,
        "table_name",
    );
    let names_b = col_string(
        &rows(
            &sb,
            "SELECT table_name FROM information_schema.views ORDER BY table_name",
        )
        .await,
        "table_name",
    );
    assert_eq!(names_a, vec!["secret_mv".to_string()]);
    assert!(
        names_b.is_empty(),
        "tenant B leaked tenant A's matview row: {names_b:?}"
    );
    assert!(
        !names_a.contains(&"plain_b".to_string()),
        "tenant A leaked tenant B's row (which is a plain table, not a matview anyway): {names_a:?}"
    );
}
