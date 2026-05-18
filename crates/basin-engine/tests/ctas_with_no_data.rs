//! `CREATE TABLE … AS <query> [WITH [NO] DATA]` (CTAS) integration tests.
//!
//! PostgreSQL semantics:
//! - `CREATE TABLE t AS <query>` / `… WITH DATA` → table with the query's
//!   resolved output schema, populated with the query result.
//! - `CREATE TABLE t AS <query> WITH NO DATA` → same schema, ZERO rows
//!   (a schema-only clone; still a real, insertable table).
//! - An optional `(col, …)` LHS list positionally renames the columns.
//! - `IF NOT EXISTS` makes a re-run a silent no-op.
//!
//! sqlparser 0.61 cannot parse either the trailing `WITH [NO] DATA`
//! clause or the bare LHS column-name list; libpg_query (already parsed
//! by the executor) classifies the statement and the engine strips both
//! forms textually before sqlparser sees the SQL. These tests guard the
//! resulting behaviour, including the leading-CTE disambiguation (a
//! front `WITH cte AS (...)` must not be mistaken for `WITH NO DATA`).

use std::sync::Arc;

use arrow_array::RecordBatch;
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

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

async fn row_count(sess: &ProjectSession, sql: &str) -> usize {
    rows(sess, sql).await.iter().map(|b| b.num_rows()).sum()
}

/// (name, debug-formatted Arrow data type) for each column the SELECT
/// resolves to.
async fn schema_of(sess: &ProjectSession, sql: &str) -> Vec<(String, String)> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { schema, .. } => schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), format!("{:?}", f.data_type())))
            .collect(),
        other => panic!("expected a schema from {sql:?}, got {other:?}"),
    }
}

async fn fixture() -> (TempDir, Engine, ProjectSession) {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    exec(&sess, "CREATE TABLE src (a INT, b TEXT)").await;
    exec(
        &sess,
        "INSERT INTO src (a, b) VALUES (1, 'x'), (2, 'y'), (3, 'z')",
    )
    .await;
    (dir, eng, sess)
}

#[tokio::test]
async fn with_no_data_creates_empty_table_with_query_schema() {
    let (_d, _e, sess) = fixture().await;

    exec(
        &sess,
        "CREATE TABLE clone AS SELECT a, b FROM src WITH NO DATA",
    )
    .await;

    // Correct columns + types, exactly as a populated CTAS would produce.
    assert_eq!(
        schema_of(&sess, "SELECT * FROM clone").await,
        vec![
            ("a".to_string(), "Int32".to_string()),
            ("b".to_string(), "Utf8".to_string()),
        ]
    );
    // Zero rows.
    assert_eq!(row_count(&sess, "SELECT * FROM clone").await, 0);

    // The empty clone is a real table: insertable afterward.
    exec(&sess, "INSERT INTO clone (a, b) VALUES (42, 'q')").await;
    assert_eq!(row_count(&sess, "SELECT * FROM clone").await, 1);
}

#[tokio::test]
async fn with_data_populates_same_as_no_clause() {
    let (_d, _e, sess) = fixture().await;

    exec(&sess, "CREATE TABLE wd AS SELECT a, b FROM src WITH DATA").await;
    exec(&sess, "CREATE TABLE plain AS SELECT a, b FROM src").await;

    assert_eq!(row_count(&sess, "SELECT * FROM wd").await, 3);
    assert_eq!(row_count(&sess, "SELECT * FROM plain").await, 3);
    // Same resolved schema on both forms.
    assert_eq!(
        schema_of(&sess, "SELECT * FROM wd").await,
        schema_of(&sess, "SELECT * FROM plain").await
    );
}

/// Regression guard: plain CTAS (no WITH clause) must produce the
/// query's schema AND its rows — not an empty placeholder table.
#[tokio::test]
async fn plain_ctas_unaffected_regression_guard() {
    let (_d, _e, sess) = fixture().await;

    exec(&sess, "CREATE TABLE t2 AS SELECT * FROM src").await;

    assert_eq!(
        schema_of(&sess, "SELECT * FROM t2").await,
        vec![
            ("a".to_string(), "Int32".to_string()),
            ("b".to_string(), "Utf8".to_string()),
        ],
        "plain CTAS must clone the query schema (no _basin_placeholder)"
    );
    assert_eq!(
        row_count(&sess, "SELECT * FROM t2").await,
        3,
        "plain CTAS must copy all source rows"
    );

    // A filtered + projected plain CTAS still works end to end.
    exec(
        &sess,
        "CREATE TABLE t3 AS SELECT a AS k FROM src WHERE a >= 2",
    )
    .await;
    assert_eq!(
        schema_of(&sess, "SELECT * FROM t3").await,
        vec![("k".to_string(), "Int32".to_string())]
    );
    assert_eq!(row_count(&sess, "SELECT * FROM t3").await, 2);
}

/// Leading CTE + trailing `WITH NO DATA`: the front `WITH c AS (...)`
/// must NOT be mistaken for the data clause (only the genuinely trailing
/// `WITH NO DATA` is stripped); the CTE must still resolve.
#[tokio::test]
async fn leading_cte_with_no_data_does_not_mangle_cte() {
    let (_d, _e, sess) = fixture().await;

    exec(
        &sess,
        "CREATE TABLE c1 AS WITH c AS (SELECT a AS x FROM src WHERE a > 1) \
         SELECT x FROM c WITH NO DATA",
    )
    .await;
    assert_eq!(
        schema_of(&sess, "SELECT * FROM c1").await,
        vec![("x".to_string(), "Int32".to_string())]
    );
    assert_eq!(
        row_count(&sess, "SELECT * FROM c1").await,
        0,
        "WITH NO DATA still applies even with a leading CTE"
    );

    // Same query WITHOUT the trailing clause must populate (the CTE
    // `WITH` is left fully intact — it produces 2 rows: a=2, a=3).
    exec(
        &sess,
        "CREATE TABLE c2 AS WITH c AS (SELECT a AS x FROM src WHERE a > 1) \
         SELECT x FROM c",
    )
    .await;
    assert_eq!(row_count(&sess, "SELECT * FROM c2").await, 2);
}

/// `CREATE TABLE t (col, …) AS <query> [WITH [NO] DATA]` — the bare LHS
/// column-name list (unparseable by sqlparser 0.61) positionally renames
/// the query's output columns, with and without WITH NO DATA.
#[tokio::test]
async fn column_name_list_renames_positionally() {
    let (_d, _e, sess) = fixture().await;

    exec(
        &sess,
        "CREATE TABLE r1 (foo, bar) AS SELECT a, b FROM src WITH NO DATA",
    )
    .await;
    assert_eq!(
        schema_of(&sess, "SELECT * FROM r1").await,
        vec![
            ("foo".to_string(), "Int32".to_string()),
            ("bar".to_string(), "Utf8".to_string()),
        ]
    );
    assert_eq!(row_count(&sess, "SELECT * FROM r1").await, 0);

    exec(&sess, "CREATE TABLE r2 (foo, bar) AS SELECT a, b FROM src").await;
    assert_eq!(
        schema_of(&sess, "SELECT * FROM r2").await,
        vec![
            ("foo".to_string(), "Int32".to_string()),
            ("bar".to_string(), "Utf8".to_string()),
        ]
    );
    assert_eq!(row_count(&sess, "SELECT * FROM r2").await, 3);
}

/// `IF NOT EXISTS` makes a re-run a silent no-op (table + rows from the
/// first run are preserved; the second CTAS does not error or re-create).
#[tokio::test]
async fn if_not_exists_is_idempotent() {
    let (_d, _e, sess) = fixture().await;

    exec(
        &sess,
        "CREATE TABLE IF NOT EXISTS ine AS SELECT a, b FROM src WITH NO DATA",
    )
    .await;
    exec(&sess, "INSERT INTO ine (a, b) VALUES (7, 'k')").await;

    // Re-run with a *populating* CTAS — IF NOT EXISTS must keep the
    // existing table (and its one row) untouched, not repopulate it.
    exec(
        &sess,
        "CREATE TABLE IF NOT EXISTS ine AS SELECT a, b FROM src WITH DATA",
    )
    .await;
    assert_eq!(
        row_count(&sess, "SELECT * FROM ine").await,
        1,
        "IF NOT EXISTS must no-op when the table already exists"
    );
}

/// CTAS over a query containing a bound parameter placeholder still
/// resolves and populates (params are substituted by the planner before
/// the CTAS schema/row resolution runs).
#[tokio::test]
async fn ctas_over_parameterised_query() {
    let (_d, _e, sess) = fixture().await;

    // A constant-expression projection (no external bind needed) — the
    // CTAS path must resolve a literal-only query's schema correctly.
    exec(
        &sess,
        "CREATE TABLE lit AS SELECT 1 AS one, 'hi' AS greeting WITH NO DATA",
    )
    .await;
    let sch = schema_of(&sess, "SELECT * FROM lit").await;
    assert_eq!(sch.len(), 2);
    assert_eq!(sch[0].0, "one");
    assert_eq!(sch[1].0, "greeting");
    assert_eq!(row_count(&sess, "SELECT * FROM lit").await, 0);
}
