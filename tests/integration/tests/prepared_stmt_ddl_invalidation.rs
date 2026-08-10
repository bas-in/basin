//! Audit gap #3 — prepared-statement plan staleness across DDL.
//!
//! Drives the engine's extended-protocol seam directly (`prepare` → `bind` →
//! `execute_bound`, plus `describe_statement`) so the assertions are
//! deterministic and server-free. The vectors:
//!
//!   1. PREPARE `SELECT *` → ALTER TABLE ADD COLUMN → EXECUTE. PG re-plans and
//!      `SELECT *` returns the NEW column. Basin's `execute_bound` re-dispatches
//!      the `SELECT *` AST/text, which expands `*` against the CURRENT provider
//!      (the ALTER refreshed it), so the new column appears in the result. A
//!      stale OLD shape here is the bug.
//!   2. PREPARE a parameterized INSERT (`INSERT INTO t(id,v) VALUES ($1,$2)`),
//!      ALTER ADD COLUMN ... DEFAULT, EXECUTE. The bind-direct INSERT fast path
//!      (`try_insert_bind_direct`) re-loads `meta.schema` AFTER its own cache
//!      invalidation and re-resolves the plan's column names against it; when an
//!      untargeted column carries a DEFAULT it DECLINES (`try_bind_insert_batch`
//!      bails), so the slow path runs and stamps the default. The new column
//!      must therefore hold its default, not be dropped or mis-shaped.
//!   3. PREPARE an INSERT naming a column, DROP that column, EXECUTE. PG raises
//!      `0A000` ("cached plan must not change result type") / a column error.
//!      Basin must raise a TYPED error — never silently insert a stale shape.
//!   4. The same SELECT vector through the bind-direct/AST fast path
//!      (a parameterized `SELECT ... WHERE id = $1`) after ADD COLUMN: the
//!      result must carry the new column and the right values.
//!
//! What this pins: prepared plans in Basin are NOT cached as frozen physical
//! plans — `execute_bound` re-dispatches the (substituted) AST/text and the
//! INSERT fast path re-resolves against the live catalog schema, with the DDL's
//! dispatch-top cache invalidation (`executor.rs` `!stmt_keeps_cache`) clearing
//! the table-meta / provider / head-probe / PK-row caches first. The one
//! genuinely cached artifact is `describe_statement`'s prepare-time
//! `StatementSchema`; tests below document that the EXECUTE *result* schema is
//! authoritative (taken from real execution), so the data path is never stale.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession, ScalarParam};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn build_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
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

/// Names of the columns in the result schema, in order.
fn col_names(res: &ExecResult) -> Vec<String> {
    match res {
        ExecResult::Rows { schema, .. } => schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect(),
        ExecResult::Empty { .. } => vec![],
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Vector 1 — PREPARE SELECT *, ALTER ADD COLUMN, then EXECUTE the SAME handle.
// The executed result must carry the NEW column (PG re-plans `*`).
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn prepared_select_star_sees_added_column_after_alter() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE t (id BIGINT NOT NULL, v BIGINT)").await;
    exec(&sess, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)").await;

    // Prepare against the 2-column shape.
    let (handle, _schema) = sess
        .prepare("SELECT * FROM t WHERE id = $1")
        .await
        .expect("prepare select *");

    // Execute once on the OLD shape — two columns.
    {
        let bound = sess
            .bind(&handle, vec![ScalarParam::Int8(1)])
            .await
            .unwrap();
        let res = sess.execute_bound(bound).await.expect("execute pre-ALTER");
        assert_eq!(
            col_names(&res),
            vec!["id".to_string(), "v".to_string()],
            "pre-ALTER SELECT * must be (id, v)"
        );
    }

    // DDL: add a column. Dispatch invalidates the session caches + refreshes the
    // provider.
    exec(&sess, "ALTER TABLE t ADD COLUMN extra TEXT").await;

    // Execute the SAME prepared handle again. PG re-plans `*`; the result must
    // now include `extra`. A 2-column result here is the staleness bug.
    let bound = sess
        .bind(&handle, vec![ScalarParam::Int8(1)])
        .await
        .unwrap();
    let res = sess.execute_bound(bound).await.expect("execute post-ALTER");
    let names = col_names(&res);
    assert!(
        names.contains(&"extra".to_string()),
        "post-ALTER SELECT * on the reused prepared handle must include the new \
         column `extra`; got {names:?} — STALE prepared shape"
    );
    assert_eq!(
        names,
        vec!["id".to_string(), "v".to_string(), "extra".to_string()],
        "post-ALTER SELECT * must be exactly (id, v, extra)"
    );

    println!("[gap#3] reused PREPARE SELECT * re-plans across ADD COLUMN ✓");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Vector 2 — PREPARE parameterized INSERT, ALTER ADD COLUMN ... DEFAULT, EXECUTE.
// The new column must hold its DEFAULT (bind-direct declines → slow path stamps
// the default). Then SELECT it back to confirm.
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn prepared_insert_picks_up_default_of_added_column() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE t (id BIGINT NOT NULL, v BIGINT)").await;

    // Prepare a parameterized 2-column INSERT.
    let (handle, _schema) = sess
        .prepare("INSERT INTO t (id, v) VALUES ($1, $2)")
        .await
        .expect("prepare insert");

    // First execute on the OLD shape.
    {
        let bound = sess
            .bind(&handle, vec![ScalarParam::Int8(1), ScalarParam::Int8(10)])
            .await
            .unwrap();
        sess.execute_bound(bound).await.expect("insert pre-ALTER");
    }

    // Add a column WITH A DEFAULT.
    exec(
        &sess,
        "ALTER TABLE t ADD COLUMN status TEXT DEFAULT 'pending'",
    )
    .await;

    // Execute the SAME prepared INSERT (still targets id, v). The new column
    // must be filled with its DEFAULT, not dropped or NULL.
    let bound = sess
        .bind(&handle, vec![ScalarParam::Int8(2), ScalarParam::Int8(20)])
        .await
        .unwrap();
    sess.execute_bound(bound).await.expect("insert post-ALTER");

    // Verify: row id=2 has status='pending' (the default), v=20.
    let res = sess
        .execute("SELECT id, v, status FROM t WHERE id = 2")
        .await
        .expect("select back");
    match res {
        ExecResult::Rows { batches, .. } => {
            let b = batches.iter().find(|b| b.num_rows() > 0).expect("a row");
            let id = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            let v = b
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            let status = b
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("status utf8");
            assert_eq!(id, 2);
            assert_eq!(v, 20);
            assert!(!status.is_null(0), "status must be the default, not NULL");
            assert_eq!(
                status.value(0),
                "pending",
                "the prepared INSERT after ADD COLUMN ... DEFAULT must stamp the \
                 default on the new column"
            );
        }
        other => panic!("select got {other:?}"),
    }

    println!("[gap#3] prepared INSERT stamps the added column's DEFAULT ✓");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Vector 3 — PREPARE INSERT naming a column, DROP that column, EXECUTE.
// Must raise a TYPED error (never silently insert a stale shape).
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn prepared_insert_on_dropped_column_errors_not_silent() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE t (id BIGINT NOT NULL, v BIGINT, extra TEXT)",
    )
    .await;

    // Prepare an INSERT that names `extra`.
    let (handle, _schema) = sess
        .prepare("INSERT INTO t (id, v, extra) VALUES ($1, $2, $3)")
        .await
        .expect("prepare insert with extra");

    // Sanity: it works on the original shape.
    {
        let bound = sess
            .bind(
                &handle,
                vec![
                    ScalarParam::Int8(1),
                    ScalarParam::Int8(10),
                    ScalarParam::Text("hi".into()),
                ],
            )
            .await
            .unwrap();
        sess.execute_bound(bound).await.expect("insert pre-DROP");
    }

    // DROP the named column.
    exec(&sess, "ALTER TABLE t DROP COLUMN extra").await;

    // Execute the SAME prepared INSERT (still names `extra`). This must ERROR —
    // PG returns 0A000 / column-does-not-exist; Basin must surface a typed
    // error rather than silently inserting (or corrupting) the row.
    let bound = sess
        .bind(
            &handle,
            vec![
                ScalarParam::Int8(2),
                ScalarParam::Int8(20),
                ScalarParam::Text("bye".into()),
            ],
        )
        .await
        .unwrap();
    let res = sess.execute_bound(bound).await;
    assert!(
        res.is_err(),
        "prepared INSERT naming a dropped column must ERROR after DROP COLUMN, \
         not silently succeed with a stale shape; got Ok({:?})",
        res.ok().map(|r| col_names(&r))
    );

    // The error must mention the missing column (typed, not a panic/garbage).
    let msg = format!("{}", res.err().unwrap()).to_lowercase();
    assert!(
        msg.contains("extra") || msg.contains("column") || msg.contains("schema"),
        "the DROP-COLUMN prepared-INSERT error must be a typed column/schema \
         error; got: {msg}"
    );

    // And the table must still hold exactly the one pre-DROP row — the failed
    // EXECUTE must not have inserted anything.
    let total = match sess.execute("SELECT COUNT(*) FROM t").await.unwrap() {
        ExecResult::Rows { batches, .. } => batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        other => panic!("count got {other:?}"),
    };
    assert_eq!(
        total, 1,
        "the failed prepared INSERT must not have written a row"
    );

    println!("[gap#3] prepared INSERT on a dropped column errors (typed), no silent write ✓");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Vector 4 — parameterized SELECT (fast/bind path) after ADD COLUMN returns the
// new column with the right value.
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn prepared_param_select_sees_added_column_value() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE t (id BIGINT NOT NULL, v BIGINT)").await;
    exec(&sess, "INSERT INTO t (id, v) VALUES (1, 10)").await;

    // Prepare a column-projected SELECT that will later reference the new col.
    // (We prepare `SELECT *` so the projection follows the schema.)
    let (handle, _) = sess
        .prepare("SELECT * FROM t WHERE id = $1")
        .await
        .expect("prepare");

    exec(&sess, "ALTER TABLE t ADD COLUMN label TEXT").await;
    exec(&sess, "UPDATE t SET label = 'tagged' WHERE id = 1").await;

    let bound = sess
        .bind(&handle, vec![ScalarParam::Int8(1)])
        .await
        .unwrap();
    let res = sess
        .execute_bound(bound)
        .await
        .expect("execute post-ALTER+UPDATE");
    match res {
        ExecResult::Rows { schema, batches } => {
            assert!(
                schema.fields().iter().any(|f| f.name() == "label"),
                "reused prepared SELECT * must surface the new `label` column"
            );
            let b = batches.iter().find(|b| b.num_rows() > 0).expect("a row");
            let li = b.schema().index_of("label").expect("label col");
            let label = b
                .column(li)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("label utf8");
            assert_eq!(
                label.value(0),
                "tagged",
                "the reused prepared SELECT * must read the post-ALTER value"
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }

    println!("[gap#3] reused prepared param-SELECT reads the added column's value ✓");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Vector 5 — the EXECUTE result schema is authoritative even though
// `describe_statement` returns the prepare-time (now stale) StatementSchema.
// This documents the one cached artifact and proves the DATA path is correct.
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn execute_result_schema_is_authoritative_over_cached_describe() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE t (id BIGINT NOT NULL, v BIGINT)").await;
    exec(&sess, "INSERT INTO t (id, v) VALUES (1, 10)").await;

    let (handle, prepare_schema) = sess
        .prepare("SELECT * FROM t WHERE id = $1")
        .await
        .expect("prepare");
    let prepare_cols: Vec<String> = prepare_schema
        .columns
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    exec(&sess, "ALTER TABLE t ADD COLUMN extra TEXT").await;

    // The cached describe still reflects the prepare-time shape (documented).
    // We do NOT assert it equals the new shape — we assert that EXECUTE's result
    // schema reflects the CURRENT shape, which is what the wire/result actually
    // carries.
    let bound = sess
        .bind(&handle, vec![ScalarParam::Int8(1)])
        .await
        .unwrap();
    let res = sess.execute_bound(bound).await.expect("execute");
    let exec_cols = col_names(&res);

    assert!(
        exec_cols.contains(&"extra".to_string()),
        "EXECUTE result schema must reflect the post-ALTER shape (the data path \
         is authoritative); got {exec_cols:?}"
    );
    // Sanity: the prepare-time cache really was the smaller shape — proving this
    // test exercised the staleness boundary and not a no-op.
    assert!(
        !prepare_cols.contains(&"extra".to_string()),
        "prepare-time StatementSchema should pre-date the ADD COLUMN; got \
         {prepare_cols:?} (if this fails the test is not crossing the DDL boundary)"
    );

    println!("[gap#3] EXECUTE result schema authoritative over cached describe ✓");
}
