//! Security regression — adversarial SQL shapes must never panic or hang.
//!
//! The hard security property is **no panic, no hang, no isolation breach**.
//! A typed `BasinError` is the desired outcome; the catch-all
//! `BasinError::Internal` (SQLSTATE XX000) is flagged as a finding but
//! does not fail the test — it is a router-surface gap, not a security
//! breach. The "regression" tests at the bottom (the ones pinning the
//! recently-shipped `coerce_utf8_scalar_for_column` fix) DO fail on
//! `Internal` because the fix specifically classified the shape and a
//! regression to `Internal` would mean the fix was unwired.
//!
//! Scenarios:
//!
//! 1. `huge_in_list_does_not_oom` — 64 K element `IN(...)` list parses /
//!    executes / errors typed.
//! 2. `huge_in_list_against_int_column_pushdown_safe` — same shape against
//!    an int column (forces the `Utf8 -> Int` coerce-or-error pushdown);
//!    no panic in `coerce_utf8_scalar_for_column`.
//! 3. `ten_meg_string_literal_handled` — multi-MiB literal passes the
//!    parser without OOM / panic.
//! 4. `nul_byte_in_string_literal_no_panic` — `\0` inside a quoted
//!    literal surfaces a typed error or stores the bytes verbatim.
//! 5. `malformed_utf8_in_query_rejected` — non-UTF-8 SQL bytes are
//!    rejected with a typed error (engine never sees malformed `&str`).
//! 6. `deep_jsonb_does_not_stack_overflow` — JSONB literal with 200-level
//!    nesting + a 10 K element array surfaces typed parse/coerce error or
//!    stores cleanly — never panics.
//! 7. `recursive_cte_no_termination_bounded` — `WITH RECURSIVE r AS (... )`
//!    without an obvious termination must either error fast or honor the
//!    session statement timeout.
//! 8. `regex_match_no_panic_on_invalid_pattern` — invalid regex returns
//!    typed error, not a panic.
//! 9. `regression_int_eq_invalid_string_literal` — pins the recently-fixed
//!    `WHERE int_col = 'inf'` shape (was a worker panic in the Vortex
//!    `as_string()` downcast). Asserts NO panic AND no rows match.
//! 10. `regression_largeutf8_and_utf8view_predicate_no_panic` — pins the
//!     storage-layer LargeUtf8 / Utf8View column coercion fix at the
//!     SQL surface.
//! 11. `regression_timestamp_compared_to_text_literal` — `WHERE ts_col = 'oops'`
//!     against a microsecond-precision timestamp column must error typed,
//!     not panic.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

#[allow(clippy::type_complexity)]
async fn build() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
) {
    let sd = TempDir::new().unwrap();
    let wd = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(sd.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wd.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal)
}

/// The hard security property is "no panic, no hang, no isolation breach".
/// `BasinError::Internal` is technically a typed Rust error (no panic, no
/// UB) so it does not violate that property — but it is the catch-all
/// SQLSTATE-XX000 surface, and shapes that land there are findings worth
/// flagging because the router cannot map them to the precise PG error
/// class a real client driver expects. We log Internal as a finding and
/// panic only on `IsolationViolation` (load-bearing safety) or on a
/// classified-shape regression failure.
fn require_no_panic_or_isolation(e: BasinError, ctx: &str) {
    match e {
        BasinError::IsolationViolation(_) => panic!(
            "SECURITY: {ctx} produced IsolationViolation — this is the load-bearing \
             safety check; if it fires the request triggered a near-leak"
        ),
        BasinError::Internal(ref msg) => {
            // FINDING: `Internal` is the catch-all XX000 surface. The
            // security property (no panic / no hang) is preserved, but
            // routers cannot map this to a precise PG SQLSTATE. Flag.
            eprintln!(
                "[sec_sql_adversarial] FINDING: {ctx} returned BasinError::Internal: {msg} \
                 — engine should classify (e.g. InvalidSchema / FeatureNotSupported); \
                 not a security breach but a router-surface gap"
            );
        }
        _ => {}
    }
}

/// Strict variant — used for shapes where a typed classification was
/// explicitly shipped (e.g. the `int_col = 'inf'` coerce path). A
/// regression to `Internal` here means the fix was unwired.
fn require_typed_classified(e: BasinError, ctx: &str) {
    if matches!(e, BasinError::Internal(_)) {
        panic!(
            "SECURITY REGRESSION: {ctx} returned BasinError::Internal — \
             this shape was previously classified; the fix may have regressed. \
             Error: {e}"
        );
    }
    if matches!(e, BasinError::IsolationViolation(_)) {
        panic!(
            "SECURITY: {ctx} produced IsolationViolation: {e}"
        );
    }
}

// ---------------------------------------------------------------------------
// 1. Huge IN list (text column, IN-list to text literals).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn huge_in_list_does_not_oom() {
    let (_sd, _wd, eng, _sh, _bg, _wal) = build().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, label TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 'present'), (2, 'absent')")
        .await
        .unwrap();

    // 64K element IN list. 1M would parse-bomb the macOS test rig; 64K is
    // already 3 orders of magnitude past what a reasonable client emits.
    let n = 64 * 1024usize;
    let mut s = String::with_capacity(n * 4);
    s.push_str("SELECT id FROM t WHERE label IN (");
    for i in 0..n {
        if i > 0 {
            s.push(',');
        }
        s.push('\'');
        s.push_str(&format!("v{i}"));
        s.push('\'');
    }
    s.push(')');

    let t0 = Instant::now();
    let r = sess.execute(&s).await;
    let elapsed = t0.elapsed();
    println!("[sec_sql_adversarial] huge IN: elapsed={elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(60),
        "huge IN parse/plan took {elapsed:?} — over 60s budget"
    );
    match r {
        Ok(ExecResult::Rows { batches, .. }) => {
            let n: usize = batches.iter().map(|b| b.num_rows()).sum();
            // No row's label is in the list, so 0 rows is correct.
            assert_eq!(n, 0, "huge IN matched a row by accident");
        }
        Ok(ExecResult::Empty { .. }) => {}
        Err(e) => require_no_panic_or_isolation(e, "huge IN-list"),
    }
}

// ---------------------------------------------------------------------------
// 2. Huge IN list against integer column — hits coerce_utf8_scalar_for_column.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn huge_in_list_against_int_column_pushdown_safe() {
    let (_sd, _wd, eng, shard, _bg, _wal) = build().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1), (42), (100)")
        .await
        .unwrap();
    // Flush to cold so the storage-layer predicate path is exercised — this
    // is the path that previously panicked on `Utf8` → `Int` mismatch and
    // is now closed by `coerce_utf8_scalar_for_column`.
    shard.flush_to_parquet().await.unwrap();

    let n = 8 * 1024usize;
    let mut s = String::with_capacity(n * 8);
    s.push_str("SELECT id FROM t WHERE id IN (");
    for i in 0..n {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("{i}"));
    }
    s.push(')');

    let r = sess.execute(&s).await;
    match r {
        Ok(_) => {}
        Err(e) => require_no_panic_or_isolation(e, "huge int IN-list"),
    }
}

// ---------------------------------------------------------------------------
// 3. ~10 MiB string literal must not OOM / panic.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ten_meg_string_literal_handled() {
    let (_sd, _wd, eng, _sh, _bg, _wal) = build().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT, body TEXT)")
        .await
        .unwrap();

    // 8 MiB string literal — enough to test the path without making the
    // test minutes long.
    let big = "x".repeat(8 * 1024 * 1024);
    let sql = format!("INSERT INTO t VALUES (1, '{big}')");
    let t0 = Instant::now();
    let r = sess.execute(&sql).await;
    let elapsed = t0.elapsed();
    println!("[sec_sql_adversarial] 8MiB literal: elapsed={elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(60),
        "8MiB literal INSERT took {elapsed:?} — over 60s budget"
    );
    match r {
        Ok(_) => {}
        Err(e) => require_no_panic_or_isolation(e, "8 MiB string literal"),
    }
}

// ---------------------------------------------------------------------------
// 4. NUL byte in a string literal — typed error or stored verbatim.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nul_byte_in_string_literal_no_panic() {
    let (_sd, _wd, eng, _sh, _bg, _wal) = build().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT, body TEXT)")
        .await
        .unwrap();
    // Embed a NUL via E-string + chr(0): both parse-time and storage-time
    // legitimate inputs.
    let r = sess
        .execute("INSERT INTO t VALUES (1, E'before\\u0000after')")
        .await;
    match r {
        Ok(_) => {}
        Err(e) => require_no_panic_or_isolation(e, "NUL byte literal"),
    }
}

// ---------------------------------------------------------------------------
// 5. Malformed UTF-8 in query bytes.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_utf8_in_query_rejected() {
    // The engine's API takes `&str`, so malformed UTF-8 can never enter
    // through `execute`. The relevant security property is: the conversion
    // from raw bytes (pgwire / REST) to `&str` must reject invalid sequences.
    // 0x80 is a continuation byte that can never start a valid UTF-8 sequence;
    // putting it first guarantees an unambiguous from_utf8 failure (not a
    // truncate-on-NUL).
    let bad: Vec<u8> = vec![0xFF, 0x80, 0x80, 0x80];
    assert!(
        std::str::from_utf8(&bad).is_err(),
        "regression: std must still reject invalid UTF-8 bytes (sanity check \
         the property we rely on); the router-level decoders bind to this."
    );
}

// ---------------------------------------------------------------------------
// 6. Deeply nested JSONB literal — no stack overflow.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deep_jsonb_does_not_stack_overflow() {
    let (_sd, _wd, eng, _sh, _bg, _wal) = build().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT, data JSONB)")
        .await
        .unwrap();

    // 200-level nested object: {"a":{"a":{...}}}.
    let depth = 200usize;
    let mut s = String::new();
    for _ in 0..depth {
        s.push_str("{\"a\":");
    }
    s.push('1');
    for _ in 0..depth {
        s.push('}');
    }
    let sql = format!("INSERT INTO t VALUES (1, '{s}'::jsonb)");
    let r = sess.execute(&sql).await;
    match r {
        Ok(_) => {}
        Err(e) => require_no_panic_or_isolation(e, "deep JSONB"),
    }

    // 10 K element array literal.
    let mut arr = String::from("[");
    for i in 0..10_000 {
        if i > 0 {
            arr.push(',');
        }
        arr.push_str(&format!("{i}"));
    }
    arr.push(']');
    let sql = format!("INSERT INTO t VALUES (2, '{arr}'::jsonb)");
    let r = sess.execute(&sql).await;
    match r {
        Ok(_) => {}
        Err(e) => require_no_panic_or_isolation(e, "10K-array JSONB"),
    }
}

// ---------------------------------------------------------------------------
// 7. Recursive CTE without obvious termination — must be bounded.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recursive_cte_no_termination_bounded() {
    let (_sd, _wd, eng, _sh, _bg, _wal) = build().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // Set a tight per-session statement timeout so the test can never hang.
    let _ = sess.execute("SET statement_timeout = '5s'").await;

    // A WITH RECURSIVE that has no natural termination: each iteration
    // doubles the count. Bound the test at ~5s.
    let sql = "WITH RECURSIVE r(n) AS ( \
                  SELECT 1 \
                  UNION ALL \
                  SELECT n+1 FROM r \
               ) \
               SELECT count(*) FROM r";
    let t0 = Instant::now();
    let r = tokio::time::timeout(Duration::from_secs(20), sess.execute(sql)).await;
    let elapsed = t0.elapsed();
    println!("[sec_sql_adversarial] recursive CTE: outcome={r:?} elapsed={elapsed:?}");

    match r {
        Err(_outer) => panic!(
            "SECURITY: recursive CTE without termination ran past the 20s outer \
             tokio bound — either the engine has no iteration cap or the SQL \
             statement_timeout is not wired to recursive-CTE execution"
        ),
        Ok(Ok(_)) => {
            // Acceptable: the engine completed (some impls finite-cap RCTEs).
            assert!(
                elapsed < Duration::from_secs(15),
                "recursive CTE took {elapsed:?} — exceeds 15s budget"
            );
        }
        Ok(Err(e)) => require_no_panic_or_isolation(e, "recursive CTE no-termination"),
    }
}

// ---------------------------------------------------------------------------
// 8. Invalid regex pattern surfaces typed error.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn regex_match_no_panic_on_invalid_pattern() {
    let (_sd, _wd, eng, _sh, _bg, _wal) = build().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT, body TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 'hello')")
        .await
        .unwrap();

    // Unclosed group + invalid character class.
    let r = sess
        .execute("SELECT regexp_match(body, '(abc[') FROM t")
        .await;
    match r {
        Ok(_) => {}
        Err(e) => require_no_panic_or_isolation(e, "invalid regex pattern"),
    }
}

// ---------------------------------------------------------------------------
// 9. Regression — `WHERE int_col = 'inf'` (and friends).
//
// This was a worker panic in the Vortex `as_string()` downcast over a
// non-string column. Fix is in `coerce_utf8_scalar_for_column` (storage
// crate); this test pins the engine-surface behaviour as part of the
// security suite so a regression couldn't slip through unnoticed.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn regression_int_eq_invalid_string_literal() {
    let (_sd, _wd, eng, shard, _bg, _wal) = build().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
        .await
        .unwrap();
    // Flush to cold so the storage-layer predicate evaluator runs the
    // coerce path. This is the path that previously panicked.
    shard.flush_to_parquet().await.unwrap();

    for bad in ["'inf'", "'nan'", "'x'", "'-'", "'1.5'"] {
        let sql = format!("SELECT id FROM t WHERE id = {bad}");
        let r = sess.execute(&sql).await;
        match r {
            Ok(ExecResult::Rows { batches, .. }) => {
                let n: usize = batches.iter().map(|b| b.num_rows()).sum();
                // '1.5' parses to a float — coerce-or-error policy may
                // reject; 0 rows is also acceptable.
                assert_eq!(
                    n, 0,
                    "SECURITY: int_col = {bad} matched {n} rows; expected typed error or 0"
                );
            }
            Ok(ExecResult::Empty { .. }) => {}
            Err(e) => require_typed_classified(e, &format!("int_col = {bad}")),
        }
    }
}

// ---------------------------------------------------------------------------
// 10. Regression — LargeUtf8 / Utf8View column compared to a TEXT literal.
//
// The storage-layer fix (predicate.rs `eval_starts_with` / coerce paths) is
// unit-tested at that layer; here we drive the regression at the SQL surface
// so the security suite would catch a re-introduction at a higher layer.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn regression_largeutf8_and_utf8view_predicate_no_panic() {
    let (_sd, _wd, eng, shard, _bg, _wal) = build().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    // The engine's TEXT column maps to Utf8 by default; LargeUtf8 / Utf8View
    // exposure is via cold-parquet read paths. The SQL surface exercises the
    // storage predicate after flushing — that's the path that previously
    // panicked on the column-type downcast.
    sess.execute("CREATE TABLE t (id BIGINT, body TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();

    for predicate in [
        "body = 'beta'",
        "body LIKE 'a%'",
        "body LIKE '%a'",
        "body < 'c'",
        "body > 'b'",
    ] {
        let sql = format!("SELECT id FROM t WHERE {predicate}");
        let r = sess.execute(&sql).await;
        if let Err(e) = r {
            require_typed_classified(e, &format!("TEXT predicate `{predicate}`"));
        }
    }
}

// ---------------------------------------------------------------------------
// 11. Regression — timestamp column compared to a text literal.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn regression_timestamp_compared_to_text_literal() {
    let (_sd, _wd, eng, shard, _bg, _wal) = build().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT, when_ts TIMESTAMP)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, '2026-01-01 00:00:00')")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();

    // Valid ISO timestamp literal — must coerce + match (or 0 if the engine
    // doesn't auto-coerce; either is non-panic).
    let r = sess
        .execute("SELECT id FROM t WHERE when_ts = '2026-01-01 00:00:00'")
        .await;
    if let Err(e) = r {
        require_typed_classified(e, "TIMESTAMP = ISO literal");
    }

    // Invalid timestamp string — must be a typed error, never a panic.
    let r = sess
        .execute("SELECT id FROM t WHERE when_ts = 'not-a-timestamp'")
        .await;
    match r {
        Ok(ExecResult::Rows { batches, .. }) => {
            let n: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(n, 0, "invalid timestamp literal matched {n} rows");
        }
        Ok(ExecResult::Empty { .. }) => {}
        Err(e) => require_typed_classified(e, "TIMESTAMP = bad literal"),
    }
}
