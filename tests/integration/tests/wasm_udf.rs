//! Integration tests for `CREATE FUNCTION … LANGUAGE wasm` (Phase 5.11.J).
//!
//! Exercises the full engine round-trip:
//!   parse → catalog persistence → session registration → DataFusion dispatch → result.
//!
//! Acceptance gates per TASK.md 5.11.J:
//!
//! 1. `SELECT add(2, 3)` via a WASM UDF returns 5 (round-trip with WAT-compiled
//!    `(func add (param i32 i32) (result i32) …)`).
//! 2. CPU-cap sandbox: an infinite-loop WASM function returns an error rather
//!    than hanging forever (epoch interruption fires within ~200 ms).
//! 3. `DROP FUNCTION` removes the WASM UDF so subsequent `SELECT add(…)` fails.
//! 4. This test file (`cargo test -p basin-integration-tests --test wasm_udf`).

use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

fn shared_engine(dir: &TempDir, catalog: Arc<dyn Catalog>) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
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

/// Compile a WAT string to WASM bytes and base64-encode them.
fn wat_to_b64(wat_text: &str) -> String {
    use base64::Engine as Base64Engine;
    let bytes = wat::parse_str(wat_text).expect("WAT compilation failed");
    base64::engine::general_purpose::STANDARD.encode(&bytes)
}

/// WAT module that exports `add(i32, i32) -> i32`.
const ADD_WAT: &str = r#"
    (module
      (func $add (export "add") (param i32 i32) (result i32)
        local.get 0
        local.get 1
        i32.add)
    )
"#;

/// WAT module that exports `double_it(i64) -> i64`.
const DOUBLE_IT_WAT: &str = r#"
    (module
      (func $double_it (export "double_it") (param i64) (result i64)
        local.get 0
        i64.const 2
        i64.mul)
    )
"#;

/// WAT module that exports `scale(f64) -> f64` (multiply by 1.5).
const SCALE_WAT: &str = r#"
    (module
      (func $scale (export "scale") (param f64) (result f64)
        local.get 0
        f64.const 1.5
        f64.mul)
    )
"#;

/// WAT module with an infinite loop — used to verify the CPU cap.
const LOOP_FOREVER_WAT: &str = r#"
    (module
      (func $loop_forever (export "loop_forever") (result i32)
        block $break
          loop $top
            br $top
          end
        end
        i32.const 0)
    )
"#;

fn scalar_i64(batches: &[arrow_array::RecordBatch], col: usize) -> i64 {
    let b = &batches[0];
    b.column(col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0)
}

fn scalar_f64(batches: &[arrow_array::RecordBatch], col: usize) -> f64 {
    let b = &batches[0];
    b.column(col)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64Array")
        .value(0)
}

// ─────────────────────────────────────────────────────────────────────────────
// Gate 1 — `SELECT add(2, 3)` returns 5
// ─────────────────────────────────────────────────────────────────────────────

/// Full round-trip: CREATE FUNCTION (LANGUAGE wasm), open a new session (so
/// the function is re-loaded from catalog), SELECT it — result must be 5.
#[tokio::test]
async fn add_i32_round_trip() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();

    let body = wat_to_b64(ADD_WAT);
    let ddl = format!("CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE wasm AS '{body}'");
    sess.execute(&ddl)
        .await
        .expect("CREATE FUNCTION should succeed");

    // Open a fresh session so the UDF is loaded from catalog (not just held in memory).
    let sess2 = eng.open_session(project).await.unwrap();
    let res = sess2
        .execute("SELECT add(2, 3)")
        .await
        .expect("SELECT add(2, 3) should succeed");

    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected result: {other:?}"),
    };
    // INT WASM UDFs declare Int64 as their Arrow return type (DataFusion
    // integer literals are Int64), so the result column is Int64.
    let v = scalar_i64(&batches, 0);
    assert_eq!(v, 5, "add(2, 3) should return 5, got {v}");
}

// ─────────────────────────────────────────────────────────────────────────────
// Extra — `double_it` with BIGINT
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn double_it_bigint_round_trip() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();

    let body = wat_to_b64(DOUBLE_IT_WAT);
    let ddl =
        format!("CREATE FUNCTION double_it(n BIGINT) RETURNS BIGINT LANGUAGE wasm AS '{body}'");
    sess.execute(&ddl)
        .await
        .expect("CREATE FUNCTION should succeed");

    let sess2 = eng.open_session(project).await.unwrap();
    let res = sess2
        .execute("SELECT double_it(21)")
        .await
        .expect("SELECT double_it(21) should succeed");

    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected result: {other:?}"),
    };
    let v = scalar_i64(&batches, 0);
    assert_eq!(v, 42, "double_it(21) should return 42, got {v}");
}

// ─────────────────────────────────────────────────────────────────────────────
// Extra — `scale` with DOUBLE PRECISION
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn scale_f64_round_trip() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();

    let body = wat_to_b64(SCALE_WAT);
    let ddl = format!(
        "CREATE FUNCTION scale(x DOUBLE PRECISION) RETURNS DOUBLE PRECISION LANGUAGE wasm AS '{body}'"
    );
    sess.execute(&ddl)
        .await
        .expect("CREATE FUNCTION should succeed");

    let sess2 = eng.open_session(project).await.unwrap();
    let res = sess2
        .execute("SELECT scale(2.0)")
        .await
        .expect("SELECT scale(2.0) should succeed");

    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected result: {other:?}"),
    };
    let v = scalar_f64(&batches, 0);
    assert!(
        (v - 3.0).abs() < 1e-9,
        "scale(2.0) should return 3.0, got {v}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Gate 2 — CPU cap: infinite loop returns error, not hang
// ─────────────────────────────────────────────────────────────────────────────

/// A WASM function with an infinite loop must be interrupted by the epoch
/// deadline (default 100 ms) rather than hanging the process. We set a very
/// short epoch via `BASIN_WASM_EPOCH_MS=50` so the test finishes quickly.
#[tokio::test]
async fn cpu_cap_kills_runaway_loop() {
    // Set a very short epoch so the test finishes quickly.
    // Note: once the global engine is initialized this env var has no further
    // effect on the epoch interval — the ticker thread reads it only once.
    // Setting it before the first engine access (which happens inside
    // open_session → session wasm UDF registration) is sufficient.
    std::env::set_var("BASIN_WASM_EPOCH_MS", "50");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();

    let body = wat_to_b64(LOOP_FOREVER_WAT);
    let ddl = format!("CREATE FUNCTION loop_forever() RETURNS INT LANGUAGE wasm AS '{body}'");
    sess.execute(&ddl)
        .await
        .expect("CREATE FUNCTION should succeed");

    let sess2 = eng.open_session(project).await.unwrap();
    let err = sess2
        .execute("SELECT loop_forever()")
        .await
        .expect_err("infinite loop should be interrupted");

    let msg = format!("{err}");
    assert!(
        msg.contains("loop_forever") || msg.contains("interrupted") || msg.contains("trap"),
        "unexpected error shape: {msg}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Gate 3 — DROP FUNCTION removes the WASM UDF
// ─────────────────────────────────────────────────────────────────────────────

/// After `DROP FUNCTION add`, a new session must not find the UDF and
/// `SELECT add(1, 2)` must fail with "function not found" (or similar).
#[tokio::test]
async fn drop_function_removes_wasm_udf() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();

    let body = wat_to_b64(ADD_WAT);
    let ddl = format!("CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE wasm AS '{body}'");
    sess.execute(&ddl)
        .await
        .expect("CREATE FUNCTION should succeed");

    // Open a fresh session so the UDF is loaded from catalog and available.
    // (WASM UDFs are registered at session-open time, not when CREATE FUNCTION runs.)
    let sess2 = eng.open_session(project).await.unwrap();
    let res = sess2
        .execute("SELECT add(1, 2)")
        .await
        .expect("should work before DROP");
    match res {
        ExecResult::Rows { batches, .. } => {
            assert_eq!(
                scalar_i64(&batches, 0),
                3,
                "add(1,2) should be 3 before DROP"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }

    // DROP the function via the original session.
    sess.execute("DROP FUNCTION add(INT, INT)")
        .await
        .expect("DROP FUNCTION should succeed");

    // Open another new session — the UDF must be gone from the catalog.
    let sess3 = eng.open_session(project).await.unwrap();
    let err = sess3
        .execute("SELECT add(1, 2)")
        .await
        .expect_err("add should not exist after DROP FUNCTION");
    let msg = format!("{err}");
    // DataFusion surfaces "function not found" or similar when a UDF is missing.
    assert!(
        msg.contains("add") || msg.contains("not found") || msg.contains("No function"),
        "unexpected error after DROP: {msg}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Gate 4 — multi-project isolation
// ─────────────────────────────────────────────────────────────────────────────

/// Project A registers `add`; project B must NOT inherit it.
#[tokio::test]
async fn wasm_udf_multi_project_isolation() {
    let dir = TempDir::new().unwrap();
    let cat: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let eng_a = shared_engine(&dir, cat.clone());
    let eng_b = shared_engine(&dir, cat.clone());

    let project_a = ProjectId::new();
    let project_b = ProjectId::new();

    // Register `add` for project A.
    let sess_a_ddl = eng_a.open_session(project_a).await.unwrap();
    let body = wat_to_b64(ADD_WAT);
    let ddl = format!("CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE wasm AS '{body}'");
    sess_a_ddl
        .execute(&ddl)
        .await
        .expect("project A CREATE FUNCTION");

    // Project A can use the UDF — open a fresh session so the UDF is loaded from catalog.
    // (WASM UDFs are registered at session-open time, not when CREATE FUNCTION runs.)
    let sess_a = eng_a.open_session(project_a).await.unwrap();
    let res = sess_a.execute("SELECT add(10, 32)").await.unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("{other:?}"),
    };
    assert_eq!(scalar_i64(&batches, 0), 42, "add(10,32) should be 42");

    // Project B cannot see project A's UDF.
    let sess_b = eng_b.open_session(project_b).await.unwrap();
    let err = sess_b
        .execute("SELECT add(1, 2)")
        .await
        .expect_err("project B should not have access to project A's WASM UDF");
    let msg = format!("{err}");
    assert!(
        msg.contains("add") || msg.contains("not found") || msg.contains("No function"),
        "unexpected error: {msg}"
    );
}
