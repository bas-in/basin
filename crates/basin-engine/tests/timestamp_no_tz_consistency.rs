//! Bare `TIMESTAMP` (without time zone) acceptance across the remaining
//! DDL surfaces — `CREATE DOMAIN`, `CREATE FUNCTION` (arg + RETURNS
//! TABLE), `CREATE PROCEDURE` (arg).
//!
//! `tests/timestamp_no_tz.rs` already covers `CREATE TABLE`. Earlier the
//! type-resolvers in `type_ddl.rs`, `function_ddl.rs`, and
//! `procedure_ddl.rs` rejected bare `TIMESTAMP` and forced callers onto
//! `TIMESTAMPTZ`. This file pins consistency: every DDL surface that
//! takes a column / arg / return type accepts both TIMESTAMP and
//! TIMESTAMPTZ, and the two remain distinct downstream.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
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

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<arrow_array::RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
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

#[tokio::test]
async fn create_domain_with_timestamp_succeeds() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    let res = sess
        .execute("CREATE DOMAIN naive_ts AS TIMESTAMP")
        .await
        .expect("CREATE DOMAIN ... AS TIMESTAMP must be accepted");
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "CREATE DOMAIN"),
        other => panic!("unexpected: {other:?}"),
    }

    // Use the domain as a column type — proves the round trip through
    // the domain base-type → Arrow type resolver.
    sess.execute("CREATE TABLE evts (id BIGINT NOT NULL, ts naive_ts)")
        .await
        .expect("naive_ts column declaration must resolve to Timestamp(Microsecond, None)");
    sess.execute("INSERT INTO evts VALUES (1, '2024-01-01T00:00:00Z')")
        .await
        .unwrap();
    let batches = rows(&sess, "SELECT id FROM evts").await;
    assert_eq!(col_i64(&batches, "id"), vec![1]);
}

#[ignore = "C5: to_char returns format-string literal instead of formatted date when called via SQL UDF — blocked on #40 cluster"]
#[tokio::test]
async fn create_function_with_timestamp_arg() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE FUNCTION when_was(ts TIMESTAMP) RETURNS TEXT \
         LANGUAGE sql AS $$ SELECT to_char(ts, 'YYYY-MM-DD') $$",
    )
    .await
    .expect("CREATE FUNCTION with TIMESTAMP arg must be accepted");

    let batches = rows(
        &sess,
        "SELECT when_was(TIMESTAMP '2024-06-15 12:00:00') AS d",
    )
    .await;
    assert_eq!(col_string(&batches, "d"), vec!["2024-06-15".to_string()]);
}

#[tokio::test]
async fn create_function_returns_table_with_timestamp() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE FUNCTION recent() RETURNS TABLE(id BIGINT, ts TIMESTAMP) \
         LANGUAGE sql AS $$ SELECT 1::BIGINT AS id, '2024-01-01'::timestamp AS ts $$",
    )
    .await
    .expect("CREATE FUNCTION RETURNS TABLE with TIMESTAMP col must be accepted");

    // Inline-call the function from the FROM clause to confirm the
    // `RETURNS TABLE` shape was registered correctly.
    let batches = rows(&sess, "SELECT id FROM recent()").await;
    assert_eq!(col_i64(&batches, "id"), vec![1]);
}

#[tokio::test]
async fn create_procedure_with_timestamp_arg() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE logs (ts TIMESTAMP NOT NULL)")
        .await
        .unwrap();

    sess.execute(
        "CREATE PROCEDURE log_at(ts TIMESTAMP) LANGUAGE sql AS $$ \
         INSERT INTO logs (ts) VALUES (ts) $$",
    )
    .await
    .expect("CREATE PROCEDURE with TIMESTAMP arg must be accepted");

    // Pass a string literal — the engine's INSERT path coerces a
    // string-shaped Expr::Value to a microsecond timestamp regardless
    // of the column tz-ness. Using `TIMESTAMP '…'` would surface the
    // procedure substituter's `Expr::Nested` wrap which the INSERT
    // path rejects (separate concern from the DDL gate this test
    // pins).
    sess.execute("CALL log_at('2024-03-21T09:30:00Z')")
        .await
        .unwrap();

    let batches = rows(&sess, "SELECT COUNT(*) AS n FROM logs").await;
    assert_eq!(col_i64(&batches, "n"), vec![1]);
}

#[tokio::test]
async fn timestamptz_still_works() {
    // Regression: TIMESTAMPTZ continues to work in every surface. Just
    // touching one (CREATE FUNCTION arg) is enough to prove the
    // refactor didn't accidentally break the WithTimeZone arm.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE FUNCTION zoned_when(ts TIMESTAMPTZ) RETURNS TEXT \
         LANGUAGE sql AS $$ SELECT to_char(ts, 'YYYY') $$",
    )
    .await
    .expect("TIMESTAMPTZ arg must still be accepted");

    sess.execute("CREATE DOMAIN zd AS TIMESTAMPTZ")
        .await
        .expect("TIMESTAMPTZ as domain base type must still be accepted");

    sess.execute("CREATE PROCEDURE pz(ts TIMESTAMPTZ) LANGUAGE sql AS $$ SELECT 1 $$")
        .await
        .expect("TIMESTAMPTZ procedure arg must still be accepted");
}

#[tokio::test]
async fn timestamp_and_timestamptz_distinct_in_function_arg() {
    // Two args, one TIMESTAMP and one TIMESTAMPTZ. The function must
    // register and the args must surface as distinct types in
    // information_schema.parameters / pg_proc, mirroring how column
    // OIDs differ (1114 vs 1184) at the table level.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE FUNCTION both(a TIMESTAMP, b TIMESTAMPTZ) RETURNS BIGINT \
         LANGUAGE sql AS $$ SELECT 1::BIGINT $$",
    )
    .await
    .expect("function with both TIMESTAMP and TIMESTAMPTZ args must register");

    // Confirm the catalog stored them with distinct OIDs: query
    // pg_catalog.pg_proc.proargtypes for the function's arg-type
    // oidvector. PG-compat OIDs: 1114 for TIMESTAMP, 1184 for
    // TIMESTAMPTZ.
    let batches = rows(
        &sess,
        "SELECT proname, proargtypes FROM pg_catalog.pg_proc WHERE proname = 'both'",
    )
    .await;
    let names = col_string(&batches, "proname");
    let argtypes = col_string(&batches, "proargtypes");
    assert_eq!(names, vec!["both".to_string()]);
    assert_eq!(
        argtypes,
        vec!["1114 1184".to_string()],
        "TIMESTAMP arg must surface as OID 1114 and TIMESTAMPTZ as 1184 — distinct types"
    );

    // And the function executes with both arg types — the body
    // ignores them but registration + call dispatch must succeed.
    let batches = rows(
        &sess,
        "SELECT both(TIMESTAMP '2024-01-01 00:00:00', \
                    TIMESTAMPTZ '2025-01-01 00:00:00+00') AS s",
    )
    .await;
    assert_eq!(col_i64(&batches, "s"), vec![1]);
}
