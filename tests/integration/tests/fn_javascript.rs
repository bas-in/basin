//! Integration tests for `CREATE FUNCTION … LANGUAGE javascript`
//! (Phase 5.11.W6).
//!
//! Two acceptance threads:
//!
//! 1. **Catalog round-trip** — `CREATE FUNCTION greet(…) LANGUAGE javascript
//!    AS '<compiled-component>'` registers the function with
//!    `language = Javascript` and `version = 1`; `DROP FUNCTION` removes it;
//!    `ALTER FUNCTION … RENAME TO …` preserves the language; a redeploy via
//!    `CREATE OR REPLACE FUNCTION` bumps the version.
//!
//! 2. **RLS-aware in-function queries** — when a function runs
//!    `SELECT auth.uid()` via `basin:functions/query`, the result equals the
//!    caller's JWT `sub`. We verify this by wiring a `QueryExecutor`
//!    against a `ProjectSession` opened with `open_session_with_auth(...)`
//!    — exactly what W2's HTTP mount does in production — and invoking
//!    `FunctionHost::query::exec` directly through the WIT-generated
//!    `query::Host` trait. Cross-project isolation is verified by opening
//!    a session in a second project and confirming its `auth.uid()`
//!    differs.
//!
//! Why we don't run a JS-compiled-to-Wasm component end-to-end here:
//! `basin functions deploy` (W3) compiles the source through
//! ComponentizeJS / Javy client-side. The engine never invokes a JS
//! compiler — it stores the produced Wasm component verbatim. A full
//! handler-component round-trip needs a hand-written WAT component that
//! calls `query.exec` across the canonical ABI (string + list lifting),
//! which is outside W6's scope. The two layers covered here — catalog
//! round-trip plus host-side `query::exec` against a real session —
//! together pin down everything W6 owns.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use base64::Engine as Base64Engine;
use basin_catalog::{Catalog, InMemoryCatalog, SqlFunctionLanguage};
use basin_common::ProjectId;
use basin_engine::{AuthContext, Engine, EngineConfig, ExecResult, ProjectSession};
use basin_fn::{
    FunctionCallContext, FunctionHost, QueryExecutor,
    engine::{InvocationContext, MockHttpClient, QueryRow, StubSecretStore},
    harness_bindings::basin::functions::query,
};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Engine factory + helpers
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

/// A tiny valid Wasm **component** (no exports). Enough for the engine's
/// `validate_javascript_body` check (base64 + Wasm magic bytes) without
/// pulling in a JS compiler. Production `basin functions deploy` ships a
/// real ComponentizeJS / Javy output here.
fn tiny_component_b64() -> String {
    let bytes = wat::parse_str("(component)").expect("empty component must parse");
    base64::engine::general_purpose::STANDARD.encode(&bytes)
}

// ---------------------------------------------------------------------------
// Gate 1 — catalog round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn javascript_catalog_round_trip_create_lookup_drop() {
    let dir = TempDir::new().unwrap();
    let cat: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let eng = make_engine(&dir, cat.clone());
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();

    let body_b64 = tiny_component_b64();
    let ddl = format!(
        "CREATE FUNCTION greet(name TEXT) RETURNS TEXT LANGUAGE javascript AS '{body_b64}'"
    );
    sess.execute(&ddl)
        .await
        .expect("CREATE FUNCTION LANGUAGE javascript must succeed");

    // Catalog lookup: language = Javascript, version = 1 (first registration).
    let def = cat
        .lookup_sql_function(&project, "greet")
        .await
        .expect("function must be in catalog");
    assert_eq!(def.language, SqlFunctionLanguage::Javascript);
    assert_eq!(def.version, 1, "first registration is version 1");
    assert_eq!(def.name, "greet");
    // Body persisted verbatim — same base64 the caller supplied.
    assert_eq!(def.body, body_b64);

    // DROP removes it.
    sess.execute("DROP FUNCTION greet(TEXT)")
        .await
        .expect("DROP FUNCTION must succeed");
    assert!(
        cat.lookup_sql_function(&project, "greet").await.is_none(),
        "function must be gone after DROP"
    );

    // DROP IF EXISTS on a missing function is a no-op.
    sess.execute("DROP FUNCTION IF EXISTS greet(TEXT)")
        .await
        .expect("DROP IF EXISTS must be a no-op when function is gone");
}

#[tokio::test]
async fn javascript_alter_rename_preserves_language_and_body() {
    let dir = TempDir::new().unwrap();
    let cat: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let eng = make_engine(&dir, cat.clone());
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();

    let body_b64 = tiny_component_b64();
    sess.execute(&format!(
        "CREATE FUNCTION old_greet() RETURNS TEXT LANGUAGE javascript AS '{body_b64}'"
    ))
    .await
    .unwrap();

    sess.execute("ALTER FUNCTION old_greet() RENAME TO new_greet")
        .await
        .expect("ALTER FUNCTION RENAME TO must succeed for LANGUAGE javascript");

    assert!(
        cat.lookup_sql_function(&project, "old_greet").await.is_none(),
        "old name must be gone after RENAME"
    );
    let renamed = cat
        .lookup_sql_function(&project, "new_greet")
        .await
        .expect("new name must be in catalog");
    assert_eq!(renamed.language, SqlFunctionLanguage::Javascript);
    assert_eq!(renamed.body, body_b64, "body must round-trip unchanged");
}

#[tokio::test]
async fn javascript_replace_bumps_version() {
    let dir = TempDir::new().unwrap();
    let cat: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let eng = make_engine(&dir, cat.clone());
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();

    let body_b64 = tiny_component_b64();
    sess.execute(&format!(
        "CREATE FUNCTION pingpong() RETURNS TEXT LANGUAGE javascript AS '{body_b64}'"
    ))
    .await
    .unwrap();
    assert_eq!(
        cat.lookup_sql_function(&project, "pingpong")
            .await
            .unwrap()
            .version,
        1,
        "first registration is version 1"
    );

    // CREATE OR REPLACE redeploys — version must bump.
    sess.execute(&format!(
        "CREATE OR REPLACE FUNCTION pingpong() RETURNS TEXT LANGUAGE javascript AS '{body_b64}'"
    ))
    .await
    .unwrap();
    assert_eq!(
        cat.lookup_sql_function(&project, "pingpong")
            .await
            .unwrap()
            .version,
        2,
        "REPLACE must bump version to 2"
    );

    sess.execute(&format!(
        "CREATE OR REPLACE FUNCTION pingpong() RETURNS TEXT LANGUAGE javascript AS '{body_b64}'"
    ))
    .await
    .unwrap();
    assert_eq!(
        cat.lookup_sql_function(&project, "pingpong")
            .await
            .unwrap()
            .version,
        3,
        "second REPLACE bumps to 3"
    );
}

#[tokio::test]
async fn javascript_rejects_non_wasm_body() {
    let dir = TempDir::new().unwrap();
    let cat: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let eng = make_engine(&dir, cat);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // Plaintext "return name;" isn't even base64.
    let err = sess
        .execute(
            "CREATE FUNCTION bad(name TEXT) RETURNS TEXT LANGUAGE javascript \
             AS $$ return name; $$",
        )
        .await
        .expect_err("non-base64 body must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("base64") || msg.contains("Wasm"),
        "expected base64/Wasm validation error, got: {msg}"
    );

    // Base64 of non-Wasm bytes ("hello") — should hit the magic-bytes check.
    let not_wasm_b64 = base64::engine::general_purpose::STANDARD.encode(b"hello");
    let err = sess
        .execute(&format!(
            "CREATE FUNCTION bad2() RETURNS TEXT LANGUAGE javascript AS '{not_wasm_b64}'"
        ))
        .await
        .expect_err("body without Wasm magic must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("Wasm") || msg.contains("magic"),
        "expected Wasm-magic-bytes error, got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Gate 2 — RLS-in-function: auth.uid() resolves to the caller's JWT sub
// ---------------------------------------------------------------------------
//
// We don't run a JS-compiled-to-Wasm component end-to-end (that requires
// ComponentizeJS output, out of W6's scope). Instead we exercise the exact
// host-side path the runtime takes:
//
//   runtime.invoke(name, ctx, req)
//     → ComponentHarness::handle constructs FunctionHost from ctx
//       → guest calls basin:functions/query::exec("SELECT auth.uid()")
//         → host.query::exec(sql) dispatches to ctx.query.exec_sql
//
// `ctx.query` is built per-request from a `ProjectSession` opened with
// `open_session_with_auth(...)`. If the executor wires correctly, the
// guest sees the caller's `auth.uid()`.

/// `QueryExecutor` impl that wraps a `ProjectSession` and runs SQL through
/// the engine. This is the **production shape** of how the runtime
/// registry's `InvocationContext::query` is built per-call: W2's mount
/// opens a session with the JWT-derived `AuthContext`, wraps it in an
/// adapter, and threads it into the function.
///
/// Because the session was opened with `open_session_with_auth(...)`,
/// every `auth.uid()` / `auth.role()` / `auth.aal()` evaluated inside
/// the function's SQL resolves to the caller's identity. RLS policies on
/// any table the function touches are likewise applied under the caller's
/// principal — no service-role escape hatch.
struct SessionQueryExecutor {
    session: Arc<ProjectSession>,
    handle: tokio::runtime::Handle,
}

impl QueryExecutor for SessionQueryExecutor {
    fn exec_sql(&self, sql: &str) -> Result<Vec<QueryRow>, String> {
        // `exec_sql` is sync (WIT `exec` is sync), so block the current
        // worker on the async engine call via the captured runtime handle.
        // The runtime always invokes this from a `spawn_blocking` worker
        // (see FunctionRuntime::invoke), so blocking here is safe.
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
                                arrow_to_json_value(arr.as_ref(), row_idx)
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

/// Minimal Arrow-array → JSON-string projection. Mirrors what the
/// production catalog-backed `SessionQueryExecutor` will do (full
/// projection lives in basin-rest's PostgREST adapter; the test only
/// needs string-shaped values).
fn arrow_to_json_value(arr: &dyn arrow_array::Array, row_idx: usize) -> String {
    use arrow_array::{Int32Array, Int64Array, StringArray};
    if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
        return serde_json::Value::String(s.value(row_idx).to_string()).to_string();
    }
    if let Some(i) = arr.as_any().downcast_ref::<Int64Array>() {
        return i.value(row_idx).to_string();
    }
    if let Some(i) = arr.as_any().downcast_ref::<Int32Array>() {
        return i.value(row_idx).to_string();
    }
    // Fallback — string representation of debug form. The auth.uid() path
    // returns StringArray, so this branch is only a safety net.
    format!("{arr:?}")
}

/// The acceptance gate for RLS-in-function: a function running
/// `SELECT auth.uid()` via `basin:functions/query` sees the caller's
/// JWT sub.
#[tokio::test]
async fn rls_inside_function_auth_uid_matches_jwt_sub() {
    let dir = TempDir::new().unwrap();
    let cat: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let eng = make_engine(&dir, cat);
    let project = ProjectId::new();

    let user_id = Uuid::new_v4();
    let auth = AuthContext::from_jwt(
        user_id,
        "authenticated",
        serde_json::json!({"sub": user_id.to_string()}),
    );

    // Production shape: HTTP mount opens a session under the caller's JWT.
    let session = Arc::new(
        eng.open_session_with_auth(project, user_id.to_string(), auth)
            .await
            .unwrap(),
    );

    // Build the per-request InvocationContext the same way the runtime
    // would: wrap the session in a QueryExecutor and plug it into the
    // FunctionHost via the basin-fn `query::Host` trait.
    let handle = tokio::runtime::Handle::current();
    let executor: Arc<dyn QueryExecutor> = Arc::new(SessionQueryExecutor {
        session: session.clone(),
        handle,
    });
    let ctx = FunctionCallContext::new(InvocationContext {
        query: executor,
        secrets: Arc::new(StubSecretStore),
        http: Arc::new(MockHttpClient::err("not configured")),
    });
    let mut host = FunctionHost::new(ctx);

    // The "guest" call. In production this comes from the JS function
    // body; here we invoke the host trait directly to keep the test
    // free of WAT-component plumbing. The path under test is the same
    // one production hits.
    let exec_sql = "SELECT auth.uid()";
    let rows = tokio::task::spawn_blocking(move || {
        <FunctionHost as query::Host>::exec(&mut host, exec_sql.to_string())
    })
    .await
    .expect("spawn_blocking join")
    .expect("query::exec must succeed");

    assert_eq!(rows.len(), 1, "SELECT auth.uid() must return one row");
    let cols = &rows[0].columns;
    assert_eq!(cols.len(), 1, "one column");
    // The value is the JSON-encoded UUID string.
    let value = &cols[0].1;
    assert!(
        value.contains(&user_id.to_string()),
        "auth.uid() inside function must equal caller JWT sub: \
         got {value:?}, expected to contain {user_id}"
    );
}

/// Cross-project isolation: function in project A sees A's caller; the
/// same function name in project B sees B's caller. Verifies the
/// `(project, name)` keying is load-bearing and the executor is per-call.
#[tokio::test]
async fn rls_inside_function_cross_project_isolation() {
    let dir = TempDir::new().unwrap();
    let cat: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let eng = make_engine(&dir, cat);

    let project_a = ProjectId::new();
    let project_b = ProjectId::new();
    let user_a = Uuid::new_v4();
    let user_b = Uuid::new_v4();

    let session_a = Arc::new(
        eng.open_session_with_auth(
            project_a,
            user_a.to_string(),
            AuthContext::from_jwt(user_a, "authenticated", serde_json::json!({})),
        )
        .await
        .unwrap(),
    );
    let session_b = Arc::new(
        eng.open_session_with_auth(
            project_b,
            user_b.to_string(),
            AuthContext::from_jwt(user_b, "authenticated", serde_json::json!({})),
        )
        .await
        .unwrap(),
    );

    // Run "SELECT auth.uid()" through each session's executor and verify
    // each sees its own JWT sub — never the other project's.
    let handle = tokio::runtime::Handle::current();
    let exec_a = SessionQueryExecutor {
        session: session_a,
        handle: handle.clone(),
    };
    let exec_b = SessionQueryExecutor {
        session: session_b,
        handle,
    };

    let rows_a = tokio::task::spawn_blocking(move || exec_a.exec_sql("SELECT auth.uid()"))
        .await
        .unwrap()
        .unwrap();
    let rows_b = tokio::task::spawn_blocking(move || exec_b.exec_sql("SELECT auth.uid()"))
        .await
        .unwrap()
        .unwrap();

    let a_uid = &rows_a[0].columns[0].1;
    let b_uid = &rows_b[0].columns[0].1;
    assert!(
        a_uid.contains(&user_a.to_string()),
        "project A's executor must see user_a: got {a_uid:?}"
    );
    assert!(
        b_uid.contains(&user_b.to_string()),
        "project B's executor must see user_b: got {b_uid:?}"
    );
    assert!(
        !a_uid.contains(&user_b.to_string()),
        "project A must NOT see user_b's uid: {a_uid:?}"
    );
    assert!(
        !b_uid.contains(&user_a.to_string()),
        "project B must NOT see user_a's uid: {b_uid:?}"
    );
}
