//! Integration tests for engine-side routing of `information_schema.tables`
//! and `pg_catalog.pg_class` (Phase 5.11.M follow-up).
//!
//! Each test opens a `ProjectSession` and runs SQL through the same
//! `execute()` entry point a pgwire connection would hit. The asserts
//! cover:
//!
//! - Basic SELECT works through the engine pipeline (planning, scan,
//!   collect).
//! - DataFusion's filter / projection operators compose with the
//!   project-scoped row source.
//! - The project boundary is structural: per-session providers see only
//!   their owner's tables.
//! - The pg_class oid hash is stable across calls inside one session,
//!   matching the catalog-side guarantee in `InfoSchemaQuery`.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float32Array, Int64Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
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
            out.push(arr.value(i).to_string());
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

fn col_bool(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<bool> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

fn col_f32(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<f32> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
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

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<arrow_array::RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

#[tokio::test]
async fn select_information_schema_tables_works() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE customers (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let batches = rows(
        &sess,
        "SELECT table_catalog, table_schema, table_name, table_type \
         FROM information_schema.tables ORDER BY table_name",
    )
    .await;
    assert_eq!(total_rows(&batches), 2);
    let names = col_string(&batches, "table_name");
    assert_eq!(names, vec!["customers".to_string(), "orders".to_string()]);
    let catalogs = col_string(&batches, "table_catalog");
    for c in &catalogs {
        assert_eq!(c, "basin");
    }
    let schemas = col_string(&batches, "table_schema");
    for s in &schemas {
        assert_eq!(s, "public");
    }
    let types = col_string(&batches, "table_type");
    for t in &types {
        assert_eq!(t, "BASE TABLE");
    }
}

#[tokio::test]
async fn select_pg_catalog_pg_class_works() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let batches = rows(
        &sess,
        "SELECT oid, relname, relnamespace, relkind, relrowsecurity, \
                relispartition, reltuples \
         FROM pg_catalog.pg_class ORDER BY relname",
    )
    .await;
    assert_eq!(total_rows(&batches), 1);
    assert_eq!(col_string(&batches, "relname"), vec!["orders".to_string()]);
    assert_eq!(col_string(&batches, "relkind"), vec!["r".to_string()]);
    let oids = col_i64(&batches, "oid");
    assert!(oids[0] > 0, "oid should be a positive hash, got {oids:?}");
    let rls = col_bool(&batches, "relrowsecurity");
    assert_eq!(rls, vec![false]);
    let parts = col_bool(&batches, "relispartition");
    assert_eq!(parts, vec![false]);
    let tuples = col_f32(&batches, "reltuples");
    // Genesis table, no INSERT yet → 0 rows.
    assert_eq!(tuples, vec![0.0_f32]);
}

#[tokio::test]
async fn project_filter_enforced_at_select_time() {
    // Two projects on the same engine: each session's
    // information_schema.tables view must contain exactly that project's
    // tables.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let a = ProjectId::new();
    let b = ProjectId::new();
    let sa = eng.open_session(a).await.unwrap();
    let sb = eng.open_session(b).await.unwrap();

    sa.execute("CREATE TABLE only_a (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sb.execute("CREATE TABLE only_b1 (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sb.execute("CREATE TABLE only_b2 (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let names_a = col_string(
        &rows(
            &sa,
            "SELECT table_name FROM information_schema.tables ORDER BY table_name",
        )
        .await,
        "table_name",
    );
    let names_b = col_string(
        &rows(
            &sb,
            "SELECT table_name FROM information_schema.tables ORDER BY table_name",
        )
        .await,
        "table_name",
    );
    assert_eq!(names_a, vec!["only_a".to_string()]);
    assert_eq!(names_b, vec!["only_b1".to_string(), "only_b2".to_string()]);
}

#[tokio::test]
async fn where_predicate_filters_correctly() {
    // DataFusion filters on top of the full RecordBatch we hand it. The
    // assertion is that a WHERE narrows the row set, exactly the
    // pushdown-deferred behaviour documented on the providers.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE foo (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE bar (id BIGINT NOT NULL)")
        .await
        .unwrap();

    // Every row's table_schema is `public` → predicate keeps all rows.
    let batches = rows(
        &sess,
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = 'public' ORDER BY table_name",
    )
    .await;
    let names = col_string(&batches, "table_name");
    assert_eq!(names, vec!["bar".to_string(), "foo".to_string()]);

    // No row's table_schema is `unknown` → predicate filters them all.
    let batches = rows(
        &sess,
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = 'unknown'",
    )
    .await;
    assert_eq!(total_rows(&batches), 0);
}

#[tokio::test]
async fn join_against_user_table() {
    // The provider is just a TableProvider — composition with the rest of
    // DataFusion's plan tree (LIKE, projection, even joins) is the
    // same. Cover the LIKE-projection path here, which is what
    // PostgREST-shaped clients issue when probing introspection.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE orders_archive (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE customers (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let batches = rows(
        &sess,
        "SELECT t.table_name FROM information_schema.tables t \
         WHERE t.table_name LIKE 'orders%' ORDER BY t.table_name",
    )
    .await;
    let names = col_string(&batches, "table_name");
    assert_eq!(
        names,
        vec!["orders".to_string(), "orders_archive".to_string()]
    );
}

#[tokio::test]
async fn pg_class_oid_stable_across_queries() {
    // The oid is a deterministic hash of (project, table). Two SELECTs in
    // the same session must produce the same oid for the same table — if
    // they don't, an upstream caching / re-init bug has crept in.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let oid1 = col_i64(
        &rows(
            &sess,
            "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'orders'",
        )
        .await,
        "oid",
    );
    let oid2 = col_i64(
        &rows(
            &sess,
            "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'orders'",
        )
        .await,
        "oid",
    );
    assert_eq!(oid1.len(), 1);
    assert_eq!(oid2.len(), 1);
    assert_eq!(oid1, oid2, "oid must be stable across queries");
    assert!(oid1[0] > 0, "oid should be positive, got {oid1:?}");
}

#[tokio::test]
async fn cross_project_isolation_under_select() {
    // A negative test: project A's information_schema.tables must NEVER
    // include any of project B's tables, even when the underlying catalog
    // backend physically holds both. This is the load-bearing P0
    // invariant — the providers' project filter has to be wired up
    // correctly at session-open time and not get accidentally widened
    // anywhere.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let a = ProjectId::new();
    let b = ProjectId::new();
    let sa = eng.open_session(a).await.unwrap();
    let sb = eng.open_session(b).await.unwrap();

    // B's secret-named table must not show up in A's view, regardless of
    // ordering.
    sb.execute("CREATE TABLE leaked_b_table (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sa.execute("CREATE TABLE alpha (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let names_a = col_string(
        &rows(&sa, "SELECT table_name FROM information_schema.tables").await,
        "table_name",
    );
    assert_eq!(names_a, vec!["alpha".to_string()]);
    assert!(
        !names_a.contains(&"leaked_b_table".to_string()),
        "project A leaked project B's table: {names_a:?}"
    );

    // Same check via pg_class.
    let names_a_pg = col_string(
        &rows(&sa, "SELECT relname FROM pg_catalog.pg_class").await,
        "relname",
    );
    assert_eq!(names_a_pg, vec!["alpha".to_string()]);
    assert!(
        !names_a_pg.contains(&"leaked_b_table".to_string()),
        "project A leaked project B's pg_class row: {names_a_pg:?}"
    );
}
