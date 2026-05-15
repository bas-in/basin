//! Integration tests for engine-side routing of `pg_catalog.pg_depend`
//! and `pg_catalog.pg_authid` (Phase 5.11.M tail).
//!
//! These views are lower-priority for tooling startup, but admin scripts
//! and `pg_dump` reach for them. The asserts cover:
//!
//! - SELECTs against pg_depend route end-to-end and surface function-type
//!   dep rows the catalog API produced.
//! - SELECTs against pg_authid surface the calling project's role row.
//! - pgAdmin-style "list relations + their owner" CROSS JOIN against
//!   `pg_class` works (Basin doesn't track per-object owner yet, so the
//!   pattern degrades to one role row × N relations).

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
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

fn col_string(batches: &[RecordBatch], name: &str) -> Vec<String> {
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

fn col_i64(batches: &[RecordBatch], name: &str) -> Vec<i64> {
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

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

#[tokio::test]
async fn select_pg_depend_routes() {
    // Register a function with two args + a return type; expect at least
    // 3 normal-deptype rows in pg_depend.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE FUNCTION fmt_id(label TEXT, n BIGINT) RETURNS BIGINT \
         LANGUAGE sql AS $$ SELECT n $$",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "SELECT classid, objid, refclassid, refobjid, deptype \
         FROM pg_catalog.pg_depend WHERE deptype = 'n'",
    )
    .await;
    assert!(
        total_rows(&batches) >= 3,
        "expected >= 3 normal-dep rows for fn(TEXT, BIGINT) RETURNS BIGINT, got {}",
        total_rows(&batches)
    );
    let refobjids = col_i64(&batches, "refobjid");
    assert!(
        refobjids.contains(&20),
        "missing BIGINT (20) in {refobjids:?}"
    );
    assert!(
        refobjids.contains(&25),
        "missing TEXT (25) in {refobjids:?}"
    );
}

#[tokio::test]
async fn select_pg_authid_routes() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();

    let batches = rows(&sess, "SELECT rolname FROM pg_catalog.pg_authid").await;
    assert_eq!(total_rows(&batches), 1, "exactly one role per session");
    let names = col_string(&batches, "rolname");
    assert_eq!(names, vec![project.to_string()]);
}

#[tokio::test]
async fn pg_authid_join_pg_class_owner_pattern() {
    // pgAdmin-style "list every relation alongside the owner role" pattern.
    // Basin v0.1 doesn't yet track owner per object, so the realistic
    // best-effort approximation is a CROSS JOIN: every relation gets
    // paired with the single project-as-role row. Tooling that displays
    // "owner" can still render a non-NULL value; the trade-off is that
    // every relation reports the same owner. This is documented in the
    // CAPABILITIES.md row for 5.11.M.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE accounts (id BIGINT NOT NULL, email TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL, account_id BIGINT NOT NULL)")
        .await
        .unwrap();

    let batches = rows(
        &sess,
        "SELECT c.relname, a.rolname \
         FROM pg_catalog.pg_class c \
         CROSS JOIN pg_catalog.pg_authid a \
         ORDER BY c.relname",
    )
    .await;
    assert_eq!(
        total_rows(&batches),
        2,
        "two relations × one role = two rows"
    );
    let relnames = col_string(&batches, "relname");
    let rolnames = col_string(&batches, "rolname");
    assert_eq!(relnames, vec!["accounts".to_string(), "orders".to_string()]);
    let expected = project.to_string();
    for r in &rolnames {
        assert_eq!(
            r, &expected,
            "every relation maps to the calling project role"
        );
    }
}

#[tokio::test]
async fn pg_depend_cv_source_table_pgdump_join() {
    // pg_dump's "find tables that views depend on" pattern, executed
    // end-to-end through the engine's pg_catalog routing. A continuous
    // matview is created over a base table; the JOIN
    //   pg_class t  ← pg_depend.refobjid
    //   pg_class v  ← pg_depend.objid
    // resolves the matview name back to its source table.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE events (bucket BIGINT NOT NULL, n BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO events VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    sess.execute(
        "CREATE MATERIALIZED VIEW my_mv \
         WITH (basin.continuous, refresh_interval = '5m') \
         AS SELECT bucket, sum(n) AS total FROM events GROUP BY bucket",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "SELECT t.relname AS src \
         FROM pg_catalog.pg_class t \
         JOIN pg_catalog.pg_depend d ON d.refobjid = t.oid \
         JOIN pg_catalog.pg_class v ON v.oid = d.objid \
         WHERE v.relname = 'my_mv' AND d.deptype = 'n'",
    )
    .await;
    assert_eq!(
        total_rows(&batches),
        1,
        "pg_dump-style JOIN must resolve my_mv → exactly one source"
    );
    let srcs = col_string(&batches, "src");
    assert_eq!(srcs, vec!["events".to_string()]);
}
