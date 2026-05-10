//! Integration tests for engine-side routing of the Phase 5.11.M Tier 3
//! views: `pg_catalog.pg_proc` and `information_schema.routines`.
//!
//! Each test opens a `TenantSession` and runs SQL through the same
//! `execute()` entry point a pgwire connection would hit. The asserts
//! cover:
//!
//! - SELECTs against pg_proc surface registered functions (prokind='f')
//!   and procedures (prokind='p').
//! - SELECTs against information_schema.routines surface registered
//!   FUNCTIONs and PROCEDUREs with the SQL-standard column shape.
//! - WHERE-predicate filtering — `WHERE routine_type = 'FUNCTION'`.
//! - Cross-tenant isolation: A's routines are invisible to B.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
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

fn col_i64(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
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
async fn select_pg_proc_routes() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (x BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "CREATE FUNCTION add_one(x BIGINT) RETURNS BIGINT \
         LANGUAGE sql AS $$ SELECT x + 1 $$",
    )
    .await
    .unwrap();
    sess.execute(
        "CREATE PROCEDURE log_one(g TEXT) LANGUAGE sql AS $$ \
         INSERT INTO t VALUES (1) $$",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "SELECT proname, prokind FROM pg_catalog.pg_proc ORDER BY proname",
    )
    .await;
    assert_eq!(total_rows(&batches), 2);
    let names = col_string(&batches, "proname");
    let kinds = col_string(&batches, "prokind");
    assert_eq!(names, vec!["add_one".to_string(), "log_one".to_string()]);
    assert_eq!(kinds, vec!["f".to_string(), "p".to_string()]);
}

#[tokio::test]
async fn select_information_schema_routines_routes() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (x BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "CREATE FUNCTION add_one(x BIGINT) RETURNS BIGINT \
         LANGUAGE sql AS $$ SELECT x + 1 $$",
    )
    .await
    .unwrap();
    sess.execute(
        "CREATE PROCEDURE log_one(g TEXT) LANGUAGE sql AS $$ \
         INSERT INTO t VALUES (1) $$",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "SELECT routine_name, routine_type, routine_body, external_language \
         FROM information_schema.routines ORDER BY routine_name",
    )
    .await;
    assert_eq!(total_rows(&batches), 2);
    let names = col_string(&batches, "routine_name");
    let types = col_string(&batches, "routine_type");
    let bodies = col_string(&batches, "routine_body");
    let langs = col_string(&batches, "external_language");
    assert_eq!(names, vec!["add_one".to_string(), "log_one".to_string()]);
    assert_eq!(types, vec!["FUNCTION".to_string(), "PROCEDURE".to_string()]);
    assert_eq!(bodies, vec!["SQL".to_string(), "SQL".to_string()]);
    assert_eq!(langs, vec!["SQL".to_string(), "SQL".to_string()]);
}

#[tokio::test]
async fn where_predicate_filters_correctly() {
    // Pin that the DataFusion filter on top of our full-batch scan
    // returns only `WHERE routine_type = 'FUNCTION'` rows. Same shape as
    // the postgrest-predicate test for information_schema.columns.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (x BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "CREATE FUNCTION fn_a(x BIGINT) RETURNS BIGINT \
         LANGUAGE sql AS $$ SELECT x + 1 $$",
    )
    .await
    .unwrap();
    sess.execute(
        "CREATE FUNCTION fn_b(x BIGINT) RETURNS BIGINT \
         LANGUAGE sql AS $$ SELECT x * 2 $$",
    )
    .await
    .unwrap();
    sess.execute(
        "CREATE PROCEDURE pr_c() LANGUAGE sql AS $$ \
         INSERT INTO t VALUES (1) $$",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "SELECT routine_name FROM information_schema.routines \
         WHERE routine_type = 'FUNCTION' ORDER BY routine_name",
    )
    .await;
    let names = col_string(&batches, "routine_name");
    assert_eq!(names, vec!["fn_a".to_string(), "fn_b".to_string()]);
    assert!(!names.contains(&"pr_c".to_string()));
}

#[tokio::test]
async fn cross_tenant_isolation_pg_proc() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let a = TenantId::new();
    let b = TenantId::new();
    let sa = eng.open_session(a).await.unwrap();
    let sb = eng.open_session(b).await.unwrap();

    sa.execute("CREATE TABLE t (x BIGINT NOT NULL)")
        .await
        .unwrap();
    sa.execute(
        "CREATE FUNCTION secret_a(x BIGINT) RETURNS BIGINT \
         LANGUAGE sql AS $$ SELECT x + 1 $$",
    )
    .await
    .unwrap();
    sb.execute("CREATE TABLE t (x BIGINT NOT NULL)")
        .await
        .unwrap();
    sb.execute(
        "CREATE FUNCTION secret_b(x BIGINT) RETURNS BIGINT \
         LANGUAGE sql AS $$ SELECT x + 100 $$",
    )
    .await
    .unwrap();

    let names_a = col_string(
        &rows(
            &sa,
            "SELECT proname FROM pg_catalog.pg_proc ORDER BY proname",
        )
        .await,
        "proname",
    );
    let names_b = col_string(
        &rows(
            &sb,
            "SELECT proname FROM pg_catalog.pg_proc ORDER BY proname",
        )
        .await,
        "proname",
    );
    assert_eq!(names_a, vec!["secret_a".to_string()]);
    assert_eq!(names_b, vec!["secret_b".to_string()]);
    assert!(
        !names_a.contains(&"secret_b".to_string()),
        "tenant A leaked tenant B's pg_proc row: {names_a:?}"
    );
    assert!(
        !names_b.contains(&"secret_a".to_string()),
        "tenant B leaked tenant A's pg_proc row: {names_b:?}"
    );

    // Same isolation must hold for information_schema.routines.
    let rnames_a = col_string(
        &rows(
            &sa,
            "SELECT routine_name FROM information_schema.routines ORDER BY routine_name",
        )
        .await,
        "routine_name",
    );
    let rnames_b = col_string(
        &rows(
            &sb,
            "SELECT routine_name FROM information_schema.routines ORDER BY routine_name",
        )
        .await,
        "routine_name",
    );
    assert_eq!(rnames_a, vec!["secret_a".to_string()]);
    assert_eq!(rnames_b, vec!["secret_b".to_string()]);
}

#[tokio::test]
async fn pg_proc_prorettype_matches_router_oid() {
    // BIGINT (Arrow Int64) → PG OID 20 (int8). Pins the cross-layer
    // agreement between basin-router's RowDescription oids and pg_proc's
    // prorettype.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute(
        "CREATE FUNCTION add_one(x BIGINT) RETURNS BIGINT \
         LANGUAGE sql AS $$ SELECT x + 1 $$",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "SELECT prorettype FROM pg_catalog.pg_proc WHERE proname = 'add_one'",
    )
    .await;
    let oids = col_i64(&batches, "prorettype");
    assert_eq!(oids, vec![20_i64], "BIGINT must map to PG int8 OID 20");
}
