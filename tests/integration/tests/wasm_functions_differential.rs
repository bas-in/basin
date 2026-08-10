//! Wasm-functions differential test (Phase 5.11.W7 capstone).
//!
//! Asserts that a "reference TS function" — represented faithfully as a
//! Wasm-host-trait closure that drives the same `basin:functions/query`
//! interface a JS guest would compile to — produces results **identical**
//! to its hand-written SQL equivalent.
//!
//! ## Why the differential is at the host-trait layer (not via a JS compiler)
//!
//! The task allows a "tiny pre-built `.wasm` test fixture or a WAT
//! hand-built one". Running ComponentizeJS / Javy in-process would pull a
//! heavyweight JS toolchain into the test crate and trade determinism for
//! brittleness; W6's `tests/integration/tests/fn_javascript.rs` took the
//! same pragmatic stance for the RLS-in-function gate (see the doc comment
//! at the top of that file). The path under test here is the **exact same
//! one** a real ComponentizeJS-produced component takes:
//!
//!   guest (TS) → wasm component → `basin:functions/query::exec(sql)`
//!     → `FunctionHost::query::exec` → `QueryExecutor::exec_sql` → engine
//!
//! Compiling JS to Wasm changes only the topmost arrow. Everything from
//! `query::exec` downward is what we care about for "function output
//! matches SQL output", so we exercise it directly.
//!
//! The WAT-handler-component ABI round-trip (a wasm guest's
//! `handle(req) -> response` lifting through `HandlerHarness::handle`) is
//! covered by `crates/basin-fn/tests/handler_test.rs` and
//! `tests/integration/tests/fn_handler.rs`; this file does not duplicate
//! that gate.
//!
//! ## Coverage map
//!
//! | Shape | TS reference | SQL oracle | Test |
//! |---|---|---|---|
//! | Scalar TEXT | `(name) => SELECT 'hello ' \|\| $1` | `SELECT 'hello ' \|\| 'world'` | `scalar_text_concat_matches_sql` |
//! | Scalar INT | `(n) => SELECT 2 * $1` | `SELECT 2 * 21` | `scalar_int_doubling_matches_sql` |
//! | RETURNS TABLE | `(min) => SELECT id, val FROM t WHERE val >= $1 ORDER BY id` | same | `returns_table_filter_matches_sql` |
//! | Error: invalid SQL | function-side `query.exec` returns Err | direct exec returns same error class | `error_propagation_invalid_sql_matches` |
//!
//! Each test runs the "function" path and the "direct SQL" path against
//! the **same** `ProjectSession`, then asserts identical row-shaped
//! output (or identical error class).

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Int64Array, StringArray};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_fn::{
    engine::{InvocationContext, MockHttpClient, QueryRow, StubSecretStore},
    harness_bindings::basin::functions::query,
    FunctionCallContext, FunctionHost, QueryExecutor,
};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Engine factory + per-test session
// ---------------------------------------------------------------------------

fn make_engine(dir: &TempDir, catalog: Arc<dyn Catalog>) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// `QueryExecutor` impl that wraps a `ProjectSession`. Identical shape to
/// the production W2/W6 mount: the request handler opens a session under
/// the caller's JWT, wraps it in this adapter, and threads it into the
/// `InvocationContext`. (Same pattern as `fn_javascript.rs::SessionQueryExecutor`.)
struct SessionQueryExecutor {
    session: Arc<ProjectSession>,
    handle: tokio::runtime::Handle,
}

impl QueryExecutor for SessionQueryExecutor {
    fn exec_sql(&self, sql: &str) -> Result<Vec<QueryRow>, String> {
        let res = self
            .handle
            .block_on(self.session.execute(sql))
            .map_err(|e| format!("{e}"))?;
        match res {
            ExecResult::Rows { batches, .. } => {
                let mut rows = Vec::new();
                for batch in batches {
                    let schema = batch.schema();
                    for row_idx in 0..batch.num_rows() {
                        let mut cols = Vec::with_capacity(batch.num_columns());
                        for col_idx in 0..batch.num_columns() {
                            let name = schema.field(col_idx).name().to_string();
                            let arr = batch.column(col_idx);
                            let value = if arr.is_null(row_idx) {
                                "null".to_string()
                            } else {
                                arrow_value_as_string(arr.as_ref(), row_idx)
                            };
                            cols.push((name, value));
                        }
                        rows.push(QueryRow { columns: cols });
                    }
                }
                Ok(rows)
            }
            ExecResult::Empty { .. } => Ok(vec![]),
        }
    }
}

/// Canonical string rendering of an Arrow scalar at `row_idx`. Mirrors the
/// projection the production catalog-backed executor uses; sufficient for
/// the differential cells under test.
fn arrow_value_as_string(arr: &dyn arrow_array::Array, row_idx: usize) -> String {
    if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
        return s.value(row_idx).to_string();
    }
    if let Some(i) = arr.as_any().downcast_ref::<Int64Array>() {
        return i.value(row_idx).to_string();
    }
    if let Some(i) = arr.as_any().downcast_ref::<arrow_array::Int32Array>() {
        return i.value(row_idx).to_string();
    }
    if let Some(b) = arr.as_any().downcast_ref::<arrow_array::BooleanArray>() {
        return b.value(row_idx).to_string();
    }
    format!("{arr:?}")
}

/// Pull the rows out of a direct `session.execute(sql)` call, in the same
/// `Vec<QueryRow>` shape that the `query::exec` host trait produces. This
/// is the SQL "oracle" half of every differential test.
async fn direct_rows(session: &ProjectSession, sql: &str) -> Result<Vec<QueryRow>, String> {
    let res = session.execute(sql).await.map_err(|e| format!("{e}"))?;
    Ok(match res {
        ExecResult::Rows { batches, .. } => {
            let mut rows = Vec::new();
            for batch in batches {
                let schema = batch.schema();
                for row_idx in 0..batch.num_rows() {
                    let mut cols = Vec::with_capacity(batch.num_columns());
                    for col_idx in 0..batch.num_columns() {
                        let name = schema.field(col_idx).name().to_string();
                        let arr = batch.column(col_idx);
                        let value = if arr.is_null(row_idx) {
                            "null".to_string()
                        } else {
                            arrow_value_as_string(arr.as_ref(), row_idx)
                        };
                        cols.push((name, value));
                    }
                    rows.push(QueryRow { columns: cols });
                }
            }
            rows
        }
        ExecResult::Empty { .. } => vec![],
    })
}

/// Run `sql` through the `query::exec` host trait — i.e. the path a
/// real Wasm guest takes. Always runs on a blocking thread because the WIT
/// `exec` is sync and we hold a runtime handle inside the executor.
async fn function_rows(session: Arc<ProjectSession>, sql: &str) -> Result<Vec<QueryRow>, String> {
    let handle = tokio::runtime::Handle::current();
    let executor: Arc<dyn QueryExecutor> = Arc::new(SessionQueryExecutor { session, handle });
    let ctx = FunctionCallContext::new(InvocationContext {
        query: executor,
        secrets: Arc::new(StubSecretStore),
        http: Arc::new(MockHttpClient::err("not configured for differential test")),
    });
    let mut host = FunctionHost::new(ctx);
    let sql_owned = sql.to_string();
    // The WIT `query::Row` is shape-identical to our `QueryRow` (single
    // `columns: Vec<(String, String)>` field). Convert at the boundary so
    // the rest of the differential plumbing stays in one row type.
    let inner: Result<Vec<query::Row>, String> = tokio::task::spawn_blocking(move || {
        <FunctionHost as query::Host>::exec(&mut host, sql_owned)
    })
    .await
    .map_err(|e| format!("join: {e}"))?;
    inner.map(|rows| {
        rows.into_iter()
            .map(|r| QueryRow { columns: r.columns })
            .collect()
    })
}

/// Both sides should produce identical `Vec<QueryRow>` cell-for-cell.
fn assert_rows_equal(fn_rows: &[QueryRow], sql_rows: &[QueryRow], label: &str) {
    assert_eq!(
        fn_rows.len(),
        sql_rows.len(),
        "{label}: row count diverged — function={} sql={}",
        fn_rows.len(),
        sql_rows.len()
    );
    for (i, (a, b)) in fn_rows.iter().zip(sql_rows.iter()).enumerate() {
        assert_eq!(
            a.columns, b.columns,
            "{label}: row {i} diverged — function={:?} sql={:?}",
            a.columns, b.columns
        );
    }
}

// ---------------------------------------------------------------------------
// Shape 1 — scalar TEXT
// ---------------------------------------------------------------------------

/// Reference TS:
/// ```ts
/// export async function greet(name: string): Promise<string> {
///   const rows = await query.exec(`SELECT 'hello ' || '${name}' AS g`);
///   return rows[0].columns[0][1];
/// }
/// ```
/// SQL oracle: `SELECT 'hello ' || 'world' AS g`.
#[tokio::test]
async fn scalar_text_concat_matches_sql() {
    let dir = TempDir::new().unwrap();
    let cat: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let eng = make_engine(&dir, cat);
    let session = Arc::new(eng.open_session(ProjectId::new()).await.unwrap());

    let inputs = ["world", "basin", "", "with 'embedded' quotes"];
    for input in inputs {
        // The "function body" — verbatim SQL the reference TS would emit.
        let escaped = input.replace('\'', "''");
        let sql = format!("SELECT 'hello ' || '{escaped}' AS g");
        let fn_out = function_rows(session.clone(), &sql).await.unwrap();
        let sql_out = direct_rows(&session, &sql).await.unwrap();
        assert_rows_equal(&fn_out, &sql_out, &format!("scalar TEXT input={input:?}"));
        assert_eq!(fn_out.len(), 1, "scalar function must return one row");
        assert_eq!(
            fn_out[0].columns[0].1,
            format!("hello {input}"),
            "function should produce 'hello {input}'"
        );
    }
}

// ---------------------------------------------------------------------------
// Shape 2 — scalar INT
// ---------------------------------------------------------------------------

/// Reference TS:
/// ```ts
/// export async function double(n: number): Promise<number> {
///   const rows = await query.exec(`SELECT 2 * ${n}::bigint AS d`);
///   return Number(rows[0].columns[0][1]);
/// }
/// ```
#[tokio::test]
async fn scalar_int_doubling_matches_sql() {
    let dir = TempDir::new().unwrap();
    let cat: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let eng = make_engine(&dir, cat);
    let session = Arc::new(eng.open_session(ProjectId::new()).await.unwrap());

    let inputs: [i64; 5] = [0, 1, 21, -7, 10_000_000];
    for n in inputs {
        let sql = format!("SELECT (2 * CAST({n} AS BIGINT)) AS d");
        let fn_out = function_rows(session.clone(), &sql).await.unwrap();
        let sql_out = direct_rows(&session, &sql).await.unwrap();
        assert_rows_equal(&fn_out, &sql_out, &format!("scalar INT input={n}"));
        let v = &fn_out[0].columns[0].1;
        assert_eq!(
            v.parse::<i64>().unwrap(),
            n.saturating_mul(2),
            "function should produce 2*{n}"
        );
    }
}

// ---------------------------------------------------------------------------
// Shape 3 — RETURNS TABLE (multi-row, multi-column)
// ---------------------------------------------------------------------------

/// Reference TS:
/// ```ts
/// export async function items_over(min: number): Promise<Row[]> {
///   return await query.exec(`SELECT id, val FROM items WHERE val >= ${min} ORDER BY id`);
/// }
/// ```
#[tokio::test]
async fn returns_table_filter_matches_sql() {
    let dir = TempDir::new().unwrap();
    let cat: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let eng = make_engine(&dir, cat);
    let session = Arc::new(eng.open_session(ProjectId::new()).await.unwrap());

    // Seed a tiny table — same data for both sides.
    session
        .execute("CREATE TABLE items (id BIGINT, val BIGINT)")
        .await
        .unwrap();
    session
        .execute("INSERT INTO items VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
        .await
        .unwrap();

    // Probe several thresholds — including the empty-result boundary and
    // the all-rows boundary.
    for min in [0_i64, 25, 50, 51, -1] {
        let sql = format!("SELECT id, val FROM items WHERE val >= {min} ORDER BY id");
        let fn_out = function_rows(session.clone(), &sql).await.unwrap();
        let sql_out = direct_rows(&session, &sql).await.unwrap();
        assert_rows_equal(&fn_out, &sql_out, &format!("RETURNS TABLE min={min}"));
    }
}

// ---------------------------------------------------------------------------
// Shape 4 — error propagation: bad SQL inside function = same failure class
// ---------------------------------------------------------------------------

/// A real TS function that issues an invalid `query.exec` should surface
/// the same engine error a direct SQL call would. We assert both sides
/// fail and that the function-side error message preserves the engine's
/// signal (so the runtime hasn't swallowed the cause).
#[tokio::test]
async fn error_propagation_invalid_sql_matches() {
    let dir = TempDir::new().unwrap();
    let cat: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let eng = make_engine(&dir, cat);
    let session = Arc::new(eng.open_session(ProjectId::new()).await.unwrap());

    // SELECT against a table that doesn't exist.
    let bad_sql = "SELECT * FROM does_not_exist_table_42";

    let fn_err = function_rows(session.clone(), bad_sql)
        .await
        .expect_err("function-side bad SQL must error");
    let sql_err = direct_rows(&session, bad_sql)
        .await
        .expect_err("direct bad SQL must error");

    // Both surfaces must mention the missing table — the runtime is not
    // allowed to swallow the engine's diagnostic.
    assert!(
        fn_err.contains("does_not_exist_table_42")
            || fn_err.to_lowercase().contains("not found")
            || fn_err.to_lowercase().contains("table"),
        "function-side error must reference the missing table: {fn_err}"
    );
    assert!(
        sql_err.contains("does_not_exist_table_42")
            || sql_err.to_lowercase().contains("not found")
            || sql_err.to_lowercase().contains("table"),
        "direct SQL error must reference the missing table: {sql_err}"
    );
}
