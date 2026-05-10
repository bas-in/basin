//! Integration test for the `CREATE/DROP/ALTER FUNCTION ... LANGUAGE sql`
//! SQL surface (Phase 5.11.D).
//!
//! Round-trips a user-defined `LANGUAGE sql` scalar function end-to-end
//! through the engine's SQL entry point. The catalog API
//! (`Catalog::register_sql_function` et al.) and the planning-time inliner
//! are exercised by their own unit tests; this file proves the SQL
//! surface routes correctly to the same catalog calls and that
//! subsequent SELECTs pick up the inlined function body.

use std::sync::Arc;

use arrow_array::{Array, Int64Array};
use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
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

#[tokio::test]
async fn create_function_round_trip() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (x BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1), (2), (3)")
        .await
        .unwrap();

    let res = sess
        .execute(
            "CREATE FUNCTION add_one(x BIGINT) RETURNS BIGINT \
             LANGUAGE sql AS $$ SELECT x + 1 $$",
        )
        .await
        .unwrap();
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "CREATE FUNCTION"),
        other => panic!("unexpected: {other:?}"),
    }

    let res = sess
        .execute("SELECT add_one(x) AS y FROM t ORDER BY y")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(col_i64(&batches, "y"), vec![2, 3, 4]);
}

#[tokio::test]
async fn create_or_replace_function_handled() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (x BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (10)").await.unwrap();

    sess.execute(
        "CREATE FUNCTION shift(x BIGINT) RETURNS BIGINT LANGUAGE sql AS $$ SELECT x + 1 $$",
    )
    .await
    .unwrap();

    // Without OR REPLACE the second CREATE must error.
    let err = sess
        .execute(
            "CREATE FUNCTION shift(x BIGINT) RETURNS BIGINT LANGUAGE sql \
             AS $$ SELECT x + 100 $$",
        )
        .await
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(msg.contains("already") || msg.contains("exist"), "got {err}");

    // OR REPLACE should re-register cleanly.
    sess.execute(
        "CREATE OR REPLACE FUNCTION shift(x BIGINT) RETURNS BIGINT LANGUAGE sql \
         AS $$ SELECT x + 100 $$",
    )
    .await
    .unwrap();

    let res = sess
        .execute("SELECT shift(x) AS y FROM t")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(col_i64(&batches, "y"), vec![110]);
}

#[tokio::test]
async fn drop_function_works() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (x BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (5)").await.unwrap();

    sess.execute(
        "CREATE FUNCTION add_one(x BIGINT) RETURNS BIGINT LANGUAGE sql \
         AS $$ SELECT x + 1 $$",
    )
    .await
    .unwrap();

    sess.execute("DROP FUNCTION add_one(BIGINT)").await.unwrap();

    let err = sess
        .execute("SELECT add_one(x) FROM t")
        .await
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("add_one") || msg.contains("not") || msg.contains("invalid"),
        "expected a 'function not found'-shaped error, got: {err}"
    );
}

#[tokio::test]
async fn drop_function_if_exists_idempotent() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    // DROP IF EXISTS on a non-existent function returns Ok.
    sess.execute("DROP FUNCTION IF EXISTS not_there(BIGINT)")
        .await
        .unwrap();
    sess.execute("DROP FUNCTION IF EXISTS not_there(BIGINT)")
        .await
        .unwrap();

    // Without IF EXISTS, the same DROP errors.
    let err = sess
        .execute("DROP FUNCTION not_there(BIGINT)")
        .await
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("not_there") || msg.contains("not"),
        "expected a 'function not found'-shaped error, got: {err}"
    );
}

#[tokio::test]
async fn alter_function_rename() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (x BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1), (2)").await.unwrap();

    sess.execute(
        "CREATE FUNCTION add_one(x BIGINT) RETURNS BIGINT LANGUAGE sql \
         AS $$ SELECT x + 1 $$",
    )
    .await
    .unwrap();

    sess.execute("ALTER FUNCTION add_one(BIGINT) RENAME TO add_1")
        .await
        .unwrap();

    let res = sess
        .execute("SELECT add_1(x) AS y FROM t ORDER BY y")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(col_i64(&batches, "y"), vec![2, 3]);

    // Old name no longer resolves.
    let err = sess
        .execute("SELECT add_one(x) FROM t")
        .await
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("add_one") || msg.contains("not") || msg.contains("invalid"),
        "old name should not resolve, got: {err}"
    );
}

#[tokio::test]
async fn language_other_than_sql_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    let err = sess
        .execute(
            "CREATE FUNCTION nope(x BIGINT) RETURNS BIGINT \
             LANGUAGE plpgsql AS $$ BEGIN RETURN x; END; $$",
        )
        .await
        .unwrap_err();
    let msg = format!("{err}");
    let lower = msg.to_ascii_lowercase();
    assert!(
        lower.contains("plpgsql") || lower.contains("language sql"),
        "error should mention LANGUAGE limitation, got: {err}"
    );
    assert!(
        msg.contains("0A000") || lower.contains("feature_not_supported"),
        "error should reference SQLSTATE 0A000, got: {err}"
    );
    assert!(
        lower.contains("0012") || lower.contains("adr"),
        "error should point at ADR 0012, got: {err}"
    );
}

#[tokio::test]
async fn recursive_function_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    let err = sess
        .execute(
            "CREATE FUNCTION rec(x BIGINT) RETURNS BIGINT LANGUAGE sql \
             AS $$ SELECT rec(x) $$",
        )
        .await
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("recursive") || msg.contains("recursion"),
        "expected a recursion-rejected error, got: {err}"
    );
}

#[tokio::test]
async fn mutual_recursion_caught_at_create_function() {
    // f → g → f. The first CREATE installs `f` calling `g` while the
    // catalog has no `g` registered (so `g` is treated as a built-in /
    // unknown name and the closure walk passes). The second CREATE
    // installs `g` calling `f` against a catalog that now contains
    // `f` — the cycle is detected at registration time, not at the
    // first SELECT that exercises the inliner.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute(
        "CREATE FUNCTION a(x BIGINT) RETURNS BIGINT LANGUAGE sql \
         AS $$ SELECT b(x) $$",
    )
    .await
    .unwrap();

    let err = sess
        .execute(
            "CREATE FUNCTION b(x BIGINT) RETURNS BIGINT LANGUAGE sql \
             AS $$ SELECT a(x) $$",
        )
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("mutual-recursion cycle"),
        "expected mutual-recursion cycle error, got: {err}"
    );
    assert!(
        msg.contains("a") && msg.contains("b"),
        "error must name both functions, got: {err}"
    );
}

#[tokio::test]
async fn body_must_be_select() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (x BIGINT NOT NULL)")
        .await
        .unwrap();

    let err = sess
        .execute(
            "CREATE FUNCTION nope(x BIGINT) RETURNS BIGINT LANGUAGE sql \
             AS $$ INSERT INTO t VALUES (x) $$",
        )
        .await
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("select") || msg.contains("body"),
        "expected a body-must-be-SELECT error, got: {err}"
    );
}

#[tokio::test]
async fn cross_tenant_isolation() {
    // Two tenants share the same catalog + storage backend; a function
    // registered for tenant A must be invisible to tenant B.
    let dir = TempDir::new().unwrap();
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let eng = shared_engine(&dir, catalog);
    let sess_a = eng.open_session(TenantId::new()).await.unwrap();
    let sess_b = eng.open_session(TenantId::new()).await.unwrap();

    sess_a
        .execute("CREATE TABLE t (x BIGINT NOT NULL)")
        .await
        .unwrap();
    sess_a.execute("INSERT INTO t VALUES (7)").await.unwrap();
    sess_a
        .execute(
            "CREATE FUNCTION add_one(x BIGINT) RETURNS BIGINT LANGUAGE sql \
             AS $$ SELECT x + 1 $$",
        )
        .await
        .unwrap();

    // Tenant A sees the inlined call.
    let res = sess_a
        .execute("SELECT add_one(x) AS y FROM t")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(col_i64(&batches, "y"), vec![8]);

    // Tenant B has its own table and no `add_one` function; the call
    // must surface as a planner error, not silently inline tenant A's
    // body.
    sess_b
        .execute("CREATE TABLE t (x BIGINT NOT NULL)")
        .await
        .unwrap();
    sess_b.execute("INSERT INTO t VALUES (7)").await.unwrap();
    let err = sess_b
        .execute("SELECT add_one(x) FROM t")
        .await
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("add_one") || msg.contains("not") || msg.contains("invalid"),
        "tenant B should not resolve add_one, got: {err}"
    );
}
