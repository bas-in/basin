//! Integration test for `GENERATED ALWAYS AS (<expr>) STORED` columns
//! (Phase 5.11.K).

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
use basin_catalog::{
    Catalog, InMemoryCatalog, SqlArgType, SqlFunctionArg, SqlFunctionDef, SqlFunctionLanguage,
    SqlReturnType,
};
use basin_common::{BasinError, TenantId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> (Engine, Arc<dyn Catalog>) {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog: catalog.clone(),
        shard: None,
    });
    (engine, catalog)
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
async fn generated_column_round_trip() {
    let dir = TempDir::new().unwrap();
    let (eng, _cat) = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE users (\
         first_name TEXT NOT NULL,\
         last_name TEXT NOT NULL,\
         full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED)",
    )
    .await
    .unwrap();

    sess.execute("INSERT INTO users (first_name, last_name) VALUES ('Ada', 'Lovelace')")
        .await
        .unwrap();

    let res = sess.execute("SELECT full_name FROM users").await.unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(col_string(&batches, "full_name"), vec!["Ada Lovelace"]);
}

#[tokio::test]
async fn generated_column_recomputes_on_update() {
    let dir = TempDir::new().unwrap();
    let (eng, _cat) = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE users (\
         id BIGINT NOT NULL,\
         first_name TEXT NOT NULL,\
         last_name TEXT NOT NULL,\
         full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED)",
    )
    .await
    .unwrap();
    sess.execute(
        "INSERT INTO users (id, first_name, last_name) VALUES \
         (1, 'Ada', 'Lovelace'), (2, 'Grace', 'Hopper')",
    )
    .await
    .unwrap();

    sess.execute("UPDATE users SET last_name = 'Byron' WHERE id = 1")
        .await
        .unwrap();

    let res = sess
        .execute("SELECT full_name FROM users ORDER BY id")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(
        col_string(&batches, "full_name"),
        vec!["Ada Byron", "Grace Hopper"]
    );
}

#[tokio::test]
async fn generated_column_with_builtin() {
    let dir = TempDir::new().unwrap();
    let (eng, _cat) = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    // Exercise `coalesce`, `lower`, and `||` together (Phase 5.11.A surface).
    sess.execute(
        "CREATE TABLE users (\
         first_name TEXT,\
         last_name TEXT,\
         display TEXT GENERATED ALWAYS AS \
         (lower(coalesce(first_name, '') || '_' || coalesce(last_name, ''))) STORED)",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO users (first_name, last_name) VALUES ('Ada', 'LOVELACE')")
        .await
        .unwrap();
    sess.execute("INSERT INTO users (first_name, last_name) VALUES (NULL, 'Hopper')")
        .await
        .unwrap();

    let res = sess
        .execute("SELECT display FROM users ORDER BY display")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(
        col_string(&batches, "display"),
        vec!["_hopper".to_string(), "ada_lovelace".to_string()]
    );
}

#[tokio::test]
async fn generated_column_with_user_function() {
    let dir = TempDir::new().unwrap();
    let (eng, catalog) = engine_in(&dir);
    let tenant = TenantId::new();

    // Register a `LANGUAGE sql` function directly on the catalog so this
    // test doesn't depend on the parallel CREATE FUNCTION SQL surface.
    let def = SqlFunctionDef {
        tenant,
        name: "double_it".to_string(),
        args: vec![SqlFunctionArg {
            name: "x".to_string(),
            data_type: SqlArgType::BigInt,
        }],
        return_type: SqlReturnType::Scalar(SqlArgType::BigInt),
        body: "SELECT x * 2".to_string(),
        language: SqlFunctionLanguage::Sql,
    };
    catalog.register_sql_function(def).await.unwrap();

    let sess = eng.open_session(tenant).await.unwrap();
    sess.execute(
        "CREATE TABLE t (\
         id BIGINT NOT NULL,\
         doubled BIGINT GENERATED ALWAYS AS (double_it(id)) STORED)",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO t (id) VALUES (3), (5), (7)")
        .await
        .unwrap();

    let res = sess
        .execute("SELECT doubled FROM t ORDER BY doubled")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(col_i64(&batches, "doubled"), vec![6, 10, 14]);
}

#[tokio::test]
async fn direct_insert_to_generated_col_rejected() {
    let dir = TempDir::new().unwrap();
    let (eng, _cat) = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();
    sess.execute(
        "CREATE TABLE users (\
         first_name TEXT NOT NULL,\
         last_name TEXT NOT NULL,\
         full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED)",
    )
    .await
    .unwrap();

    let err = sess
        .execute("INSERT INTO users (full_name) VALUES ('Ada Lovelace')")
        .await
        .unwrap_err();
    // `InvalidSchema` maps to SQLSTATE 42601 (syntax_error) at the router.
    assert!(matches!(err, BasinError::InvalidSchema(_)));
    let msg = err.to_string();
    assert!(
        msg.contains("cannot insert into generated column"),
        "expected PG-shaped message; got {msg}"
    );
    assert!(msg.contains("full_name"));
}

#[tokio::test]
async fn direct_update_to_generated_col_rejected() {
    let dir = TempDir::new().unwrap();
    let (eng, _cat) = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();
    sess.execute(
        "CREATE TABLE users (\
         first_name TEXT NOT NULL,\
         last_name TEXT NOT NULL,\
         full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED)",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO users (first_name, last_name) VALUES ('Ada', 'Lovelace')")
        .await
        .unwrap();

    let err = sess
        .execute("UPDATE users SET full_name = 'X' WHERE first_name = 'Ada'")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::InvalidSchema(_)));
    let msg = err.to_string();
    assert!(
        msg.contains("cannot insert into generated column"),
        "expected PG-shaped message; got {msg}"
    );
    assert!(msg.contains("full_name"));
}

#[tokio::test]
async fn virtual_rejected() {
    let dir = TempDir::new().unwrap();
    let (eng, _cat) = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    let err = sess
        .execute(
            "CREATE TABLE users (\
             first_name TEXT NOT NULL,\
             last_name TEXT NOT NULL,\
             full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) VIRTUAL)",
        )
        .await
        .unwrap_err();
    // VIRTUAL maps to SQLSTATE 0A000 (feature_not_supported) via the
    // FeatureNotSupported variant.
    assert!(
        matches!(err, BasinError::FeatureNotSupported(_)),
        "expected FeatureNotSupported, got {err:?}"
    );
}

#[tokio::test]
async fn missing_stored_keyword_rejected() {
    let dir = TempDir::new().unwrap();
    let (eng, _cat) = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    // GENERATED ALWAYS AS (...) without STORED isn't on the v0.1 surface
    // — sqlparser routes it through the same path with no expr-mode
    // tag, which we treat as VIRTUAL and reject.
    let err = sess
        .execute(
            "CREATE TABLE users (\
             first_name TEXT,\
             last_name TEXT,\
             full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name))",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, BasinError::FeatureNotSupported(_)),
        "expected FeatureNotSupported, got {err:?}"
    );
}

#[tokio::test]
async fn self_reference_rejected() {
    let dir = TempDir::new().unwrap();
    let (eng, _cat) = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();
    let err = sess
        .execute(
            "CREATE TABLE t (\
             full_name TEXT GENERATED ALWAYS AS (full_name || '!') STORED)",
        )
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::InvalidSchema(_)));
    assert!(err.to_string().contains("itself"));
}

#[tokio::test]
async fn composition_with_audit_to() {
    let dir = TempDir::new().unwrap();
    let (eng, _cat) = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();
    sess.execute(
        "CREATE TABLE users (\
         id BIGINT NOT NULL,\
         first_name TEXT NOT NULL,\
         last_name TEXT NOT NULL,\
         full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED\
         ) AUDIT TO users_audit",
    )
    .await
    .unwrap();
    sess.execute(
        "INSERT INTO users (id, first_name, last_name) VALUES (1, 'Ada', 'Lovelace')",
    )
    .await
    .unwrap();

    // Audit table's after_row should include the computed full_name.
    let res = sess
        .execute("SELECT after_row FROM users_audit ORDER BY audit_id")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    // after_row is JSONB serialised; check the canonical bytes for the
    // computed value.
    let arr = batches[0]
        .column_by_name("after_row")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::LargeBinaryArray>()
        .unwrap();
    let bytes = arr.value(0);
    let json = std::str::from_utf8(bytes).unwrap();
    assert!(
        json.contains("\"full_name\":\"Ada Lovelace\""),
        "expected computed full_name in audit row; got {json}"
    );
}

#[tokio::test]
async fn composition_with_soft_delete() {
    let dir = TempDir::new().unwrap();
    let (eng, _cat) = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();
    sess.execute(
        "CREATE TABLE users (\
         id BIGINT NOT NULL,\
         first_name TEXT NOT NULL,\
         last_name TEXT NOT NULL,\
         full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,\
         deleted_at TIMESTAMPTZ SOFT DELETE)",
    )
    .await
    .unwrap();
    sess.execute(
        "INSERT INTO users (id, first_name, last_name, deleted_at) VALUES \
         (1, 'Ada', 'Lovelace', NULL), (2, 'Grace', 'Hopper', NULL)",
    )
    .await
    .unwrap();

    // Soft-delete row id=1 — the underlying write is an UPDATE; the
    // generated column must be re-evaluated on the matched row but the
    // computed value is unchanged because the source columns haven't
    // changed.
    sess.execute("DELETE FROM users WHERE id = 1").await.unwrap();

    // Default SELECT hides the soft-deleted row.
    let res = sess
        .execute("SELECT id, full_name FROM users ORDER BY id")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(col_i64(&batches, "id"), vec![2]);
    assert_eq!(col_string(&batches, "full_name"), vec!["Grace Hopper"]);

    // INCLUDE DELETED reveals id=1 with its (still-valid) generated value.
    let res = sess
        .execute("SELECT id, full_name FROM users ORDER BY id INCLUDE DELETED")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(col_i64(&batches, "id"), vec![1, 2]);
    assert_eq!(
        col_string(&batches, "full_name"),
        vec!["Ada Lovelace", "Grace Hopper"]
    );
}

#[tokio::test]
async fn cross_tenant_isolation() {
    let dir = TempDir::new().unwrap();
    let (eng, _cat) = engine_in(&dir);
    let a = eng.open_session(TenantId::new()).await.unwrap();
    let b = eng.open_session(TenantId::new()).await.unwrap();

    for s in [&a, &b] {
        s.execute(
            "CREATE TABLE users (\
             first_name TEXT NOT NULL,\
             last_name TEXT NOT NULL,\
             full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED)",
        )
        .await
        .unwrap();
    }

    a.execute("INSERT INTO users (first_name, last_name) VALUES ('A', 'X')")
        .await
        .unwrap();
    b.execute(
        "INSERT INTO users (first_name, last_name) VALUES ('B', 'Y'), ('C', 'Z')",
    )
    .await
    .unwrap();

    let res_a = a.execute("SELECT full_name FROM users").await.unwrap();
    let res_b = b
        .execute("SELECT full_name FROM users ORDER BY full_name")
        .await
        .unwrap();
    let names_a = match res_a {
        ExecResult::Rows { batches, .. } => col_string(&batches, "full_name"),
        other => panic!("{other:?}"),
    };
    let names_b = match res_b {
        ExecResult::Rows { batches, .. } => col_string(&batches, "full_name"),
        other => panic!("{other:?}"),
    };
    assert_eq!(names_a, vec!["A X"]);
    assert_eq!(names_b, vec!["B Y", "C Z"]);
}

#[tokio::test]
async fn multi_column_arithmetic_expression() {
    let dir = TempDir::new().unwrap();
    let (eng, _cat) = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();
    sess.execute(
        "CREATE TABLE t (\
         a BIGINT NOT NULL,\
         b BIGINT NOT NULL,\
         c BIGINT GENERATED ALWAYS AS (a + b * 2) STORED)",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO t (a, b) VALUES (1, 10), (3, 7), (0, 0)")
        .await
        .unwrap();

    let res = sess.execute("SELECT c FROM t ORDER BY c").await.unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("{other:?}"),
    };
    assert_eq!(col_i64(&batches, "c"), vec![0, 17, 21]);
}
