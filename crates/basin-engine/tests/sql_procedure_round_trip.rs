//! Integration test for the `CREATE/DROP PROCEDURE … LANGUAGE sql`
//! and `CALL` SQL surface (Phase 5.11.F).
//!
//! Round-trips a user-defined `LANGUAGE sql` procedure end-to-end
//! through the engine's SQL entry point. The catalog API
//! (`Catalog::register_procedure` et al.) and the substitution helper
//! are exercised by the unit tests in
//! `basin_engine::procedure_ddl::tests`; this file proves the SQL
//! surface routes correctly to the same catalog calls and that
//! subsequent `CALL`s walk the body in order with arguments
//! substituted.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ExecResult};
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

fn shared_engine(dir: &TempDir, catalog: Arc<dyn basin_catalog::Catalog>) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
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

fn col_str(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<String> {
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

#[tokio::test]
async fn create_call_procedure_round_trip() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE log (msg TEXT NOT NULL)")
        .await
        .unwrap();

    let res = sess
        .execute(
            "CREATE PROCEDURE note() LANGUAGE sql AS $$ \
             INSERT INTO log VALUES ('called') \
             $$",
        )
        .await
        .unwrap();
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "CREATE PROCEDURE"),
        other => panic!("unexpected: {other:?}"),
    }

    let res = sess.execute("CALL note()").await.unwrap();
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "CALL 1"),
        other => panic!("unexpected: {other:?}"),
    }

    let res = sess.execute("SELECT msg FROM log").await.unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(col_str(&batches, "msg"), vec!["called".to_string()]);
}

#[tokio::test]
async fn multi_statement_body_executes_in_order() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE seen (msg TEXT NOT NULL, ord BIGINT NOT NULL)")
        .await
        .unwrap();

    sess.execute(
        "CREATE PROCEDURE three_steps() LANGUAGE sql AS $$ \
         INSERT INTO seen VALUES ('first', 1); \
         INSERT INTO seen VALUES ('second', 2); \
         INSERT INTO seen VALUES ('third', 3) \
         $$",
    )
    .await
    .unwrap();

    let res = sess.execute("CALL three_steps()").await.unwrap();
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "CALL 3"),
        other => panic!("unexpected: {other:?}"),
    }

    let res = sess
        .execute("SELECT msg, ord FROM seen ORDER BY ord")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(
        col_str(&batches, "msg"),
        vec!["first".to_string(), "second".into(), "third".into()]
    );
    assert_eq!(col_i64(&batches, "ord"), vec![1, 2, 3]);
}

#[tokio::test]
async fn call_with_arguments_substitutes() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE notes (greeting TEXT NOT NULL, n BIGINT NOT NULL)")
        .await
        .unwrap();

    sess.execute(
        "CREATE PROCEDURE add_note(g TEXT, x BIGINT) LANGUAGE sql AS $$ \
         INSERT INTO notes VALUES (g, x) \
         $$",
    )
    .await
    .unwrap();

    sess.execute("CALL add_note('hello', 42)").await.unwrap();
    sess.execute("CALL add_note('world', 7)").await.unwrap();

    let res = sess
        .execute("SELECT greeting, n FROM notes ORDER BY n")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(col_i64(&batches, "n"), vec![7, 42]);
    assert_eq!(
        col_str(&batches, "greeting"),
        vec!["world".to_string(), "hello".into()]
    );
}

#[tokio::test]
async fn call_failure_mid_procedure_persists_prior_statements() {
    // v0.1 contract: best-effort sequential. A mid-statement error
    // leaves earlier statements committed (until single-shard
    // transactions land per Phase 5). This test pins that contract.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE log (msg TEXT NOT NULL)")
        .await
        .unwrap();
    // No `missing_table` exists, so the second statement will error.
    sess.execute(
        "CREATE PROCEDURE half_fail() LANGUAGE sql AS $$ \
         INSERT INTO log VALUES ('first'); \
         INSERT INTO missing_table VALUES ('boom') \
         $$",
    )
    .await
    .unwrap();

    let err = sess.execute("CALL half_fail()").await.unwrap_err();
    // The error message mentions which statement failed; the prior
    // statement should still be visible in `log`.
    let msg = format!("{err}");
    assert!(
        msg.contains("statement #2") || msg.contains("missing_table") || msg.contains("not found"),
        "unexpected error shape: {msg}"
    );

    let res = sess.execute("SELECT msg FROM log").await.unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    // First statement persisted — that's the documented v0.1 behaviour.
    assert_eq!(col_str(&batches, "msg"), vec!["first".to_string()]);
}

#[tokio::test]
async fn drop_procedure_works() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (x BIGINT)").await.unwrap();
    sess.execute("CREATE PROCEDURE noop() LANGUAGE sql AS $$ INSERT INTO t VALUES (1) $$")
        .await
        .unwrap();

    let res = sess.execute("DROP PROCEDURE noop()").await.unwrap();
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "DROP PROCEDURE"),
        other => panic!("unexpected: {other:?}"),
    }

    let err = sess.execute("CALL noop()").await.unwrap_err();
    assert!(
        matches!(err, BasinError::NotFound(_)),
        "expected NotFound, got {err:?}"
    );

    // DROP PROCEDURE IF EXISTS on a missing procedure is a no-op.
    sess.execute("DROP PROCEDURE IF EXISTS noop()")
        .await
        .unwrap();
}

#[tokio::test]
async fn cross_project_isolation() {
    let dir = TempDir::new().unwrap();
    let cat: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let eng_a = shared_engine(&dir, cat.clone());
    let eng_b = shared_engine(&dir, cat.clone());

    let project_a = ProjectId::new();
    let project_b = ProjectId::new();
    let sess_a = eng_a.open_session(project_a).await.unwrap();
    let sess_b = eng_b.open_session(project_b).await.unwrap();

    sess_a
        .execute("CREATE TABLE log (msg TEXT NOT NULL)")
        .await
        .unwrap();
    sess_a
        .execute("CREATE PROCEDURE note() LANGUAGE sql AS $$ INSERT INTO log VALUES ('a') $$")
        .await
        .unwrap();

    // Project A's procedure must not be visible to project B.
    let err = sess_b.execute("CALL note()").await.unwrap_err();
    assert!(
        matches!(err, BasinError::NotFound(_)),
        "expected NotFound for project B, got {err:?}"
    );

    // Project A still sees its procedure.
    sess_a.execute("CALL note()").await.unwrap();
    let res = sess_a.execute("SELECT msg FROM log").await.unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(col_str(&batches, "msg"), vec!["a".to_string()]);
}

#[tokio::test]
async fn nested_call_rejected() {
    // Body containing `CALL other_proc()` must be rejected at
    // registration. v0.1 has no nested calls.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (x BIGINT)").await.unwrap();
    let err = sess
        .execute(
            "CREATE PROCEDURE outer() LANGUAGE sql AS $$ \
             INSERT INTO t VALUES (1); \
             CALL inner_proc() \
             $$",
        )
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("CALL") || msg.contains("not permitted") || msg.contains("disallow"),
        "expected nested-CALL rejection, got: {msg}"
    );
}

#[tokio::test]
async fn language_other_than_sql_rejected() {
    // SQLSTATE 0A000 per ADR 0012 — `LANGUAGE plpgsql` is out of scope.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    let err = sess
        .execute("CREATE PROCEDURE p() LANGUAGE plpgsql AS $$ BEGIN END $$")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("LANGUAGE sql") || msg.contains("0A000") || msg.contains("plpgsql"),
        "expected LANGUAGE-sql rejection, got: {msg}"
    );
}

#[tokio::test]
async fn procedure_with_user_function_call_works() {
    // A procedure body may reference a user-defined `LANGUAGE sql`
    // function (from 5.11.D). The function is inlined into the
    // body's statement at planning time when the procedure runs.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (x BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE summary (val BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (10), (20), (30)")
        .await
        .unwrap();

    sess.execute(
        "CREATE FUNCTION add_one(x BIGINT) RETURNS BIGINT \
         LANGUAGE sql AS $$ SELECT x + 1 $$",
    )
    .await
    .unwrap();

    sess.execute(
        "CREATE PROCEDURE record_max(thresh BIGINT) LANGUAGE sql AS $$ \
         INSERT INTO summary SELECT add_one(x) FROM t WHERE x > thresh \
         $$",
    )
    .await
    .unwrap();

    sess.execute("CALL record_max(15)").await.unwrap();

    let res = sess
        .execute("SELECT val FROM summary ORDER BY val")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    // Rows where x > 15: 20, 30. add_one expands to (x + 1) → 21, 31.
    assert_eq!(col_i64(&batches, "val"), vec![21, 31]);
}
