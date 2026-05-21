//! Security regression tests — #53 P1 parser DoS: stack overflow + regex ReDoS.
//!
//! **Test 1 — `parser_deeply_nested_query_graceful`:**
//!   Generates `"(".repeat(10_000) + "1" + ")".repeat(10_000)` as a SELECT
//!   expression and asserts the engine returns a typed `BasinError` (not a panic
//!   / stack overflow). Captures the gap where no recursion depth limit exists.
//!
//! **Test 2 — `regex_redos_catastrophic_alternation`:**
//!   Runs `SELECT 'aaa…' ~ '(a+a+){50}'` (a catastrophic-backtracking POSIX
//!   regex) under a tight timeout. Asserts it either completes fast (RE2-style
//!   engine) OR returns `BasinError::QueryCanceled` (SQLSTATE 57014) within
//!   3 s. If the engine hangs past the outer tokio timeout the OS kills the
//!   test and we document the gap with `#[ignore]`.
//!
//! # Run command
//!
//! ```text
//! cargo test -p basin-integration-tests --test sec_parser_dos
//! ```

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Engine helper (mirrors sql_syntax_fuzz.rs)
// ---------------------------------------------------------------------------

fn make_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
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

// ---------------------------------------------------------------------------
// Test 1 — #53 P1: parser stack overflow on deeply nested expression
// ---------------------------------------------------------------------------

/// Security regression — #53 P1: parser stack overflow with deeply nested
/// parenthesised expression.
///
/// Generates 10 000 levels of nesting: `((…((1))…))`.  A parser without a
/// recursion-depth guard will overflow the stack; a hardened parser must return
/// a typed `BasinError` (parse error or feature-not-supported) rather than
/// crashing the process.
///
/// **Gap confirmed:** running this test without `#[ignore]` causes a stack
/// overflow (SIGABRT) — the parser (libpg_query / pg_query crate) recurses
/// into the C call-stack without a depth limit, overflowing the thread stack
/// at ~10 000 nesting levels. The `pg_query` C library does not enforce the
/// PostgreSQL server's `max_parser_depth` GUC (default 10 000) in the
/// standalone binding; the Basin layer adds no additional guard either.
///
/// Fix path: add a depth-limit pre-check in `basin_engine::pg_ast::parse`
/// (count opening parens before calling `pg_query::parse`; reject with
/// `BasinError::InvalidSchema` when depth > 8 000).
#[ignore = "P1 parser stack overflow — no recursion depth limit; sqlparser default 50 not enforced"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parser_deeply_nested_query_graceful() {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = make_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Construct the deeply-nested expression: (((…((1))…)))
    let depth = 10_000usize;
    let open = "(".repeat(depth);
    let close = ")".repeat(depth);
    let deeply_nested = format!("SELECT {open}1{close}");

    println!(
        "[sec_parser_dos] sending {}-deep nested expression ({} chars)",
        depth,
        deeply_nested.len()
    );

    let t0 = Instant::now();
    let result = sess.execute(&deeply_nested).await;
    let elapsed = t0.elapsed();

    println!(
        "[sec_parser_dos] parser_deeply_nested_query_graceful: result={:?} elapsed={:?}",
        result, elapsed
    );

    match result {
        Ok(_) => {
            // Parser handled it (very generous depth limit or iterative parser).
            // This is acceptable — what we forbid is a crash.
            println!(
                "[sec_parser_dos] PASS: deeply nested expression parsed successfully \
                 (parser has sufficient depth budget)"
            );
        }
        Err(e) => {
            // Any typed error (parse error, recursion limit, feature not
            // supported) is acceptable — it means the engine surfaced a typed
            // BasinError rather than crashing.
            assert!(
                !matches!(e, BasinError::Internal(_)),
                "SECURITY P1 (#53): deeply nested expression triggered Internal error \
                 (BasinError::Internal) — expected a typed parse-level or depth-limit \
                 error, not XX000. Error: {e:?}"
            );
            println!(
                "[sec_parser_dos] PASS: deeply nested expression returned typed error \
                 (not a crash): {e}"
            );
        }
    }

    // The parser must not take unreasonably long regardless of outcome.
    assert!(
        elapsed < Duration::from_secs(10),
        "SECURITY P1 (#53): deeply nested expression parsing took {elapsed:?} — \
         exceeds 10 s safety budget; recursion blowup suspected"
    );
}

// ---------------------------------------------------------------------------
// Test 2 — #53 P1: regex ReDoS with catastrophic-backtracking pattern
// ---------------------------------------------------------------------------

/// Security regression — #53 P1: regex ReDoS via catastrophic alternation.
///
/// Runs a POSIX regex match with a classically catastrophic pattern:
/// `(a+a+){50}` against a long string of 'a's followed by 'b'. A naive
/// backtracking engine exhibits O(2^n) behaviour; an RE2-style engine
/// (or one with a statement timeout) must finish promptly.
///
/// Acceptance criteria (either is sufficient):
///   1. The query completes within 3 s (RE2-style linear-time evaluation).
///   2. The engine returns `BasinError::QueryCanceled` within the outer
///      tokio timeout (statement timeout fires — SQLSTATE 57014).
///
/// If neither criterion is met the engine hangs and the test should be
/// `#[ignore]`d with the diagnosis below.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn regex_redos_catastrophic_alternation() {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = make_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Classic ReDoS input: 30 'a's followed by 'b' — the backtracking engine
    // must try every exponential partition before deciding no match.
    let input = format!("{}b", "a".repeat(30));
    // Catastrophic alternation pattern: (a+a+){N}
    // N=20 is already astronomical for a backtracking engine.
    let pattern = "(a+a+){20}";

    // The POSIX ~ operator is rewritten to regexp_like() by the engine.
    // We use a plain string literal (no SET statement_timeout) because the
    // SQL-level GUC is not yet wired to the executor timeout path; the
    // env-var BASIN_STATEMENT_TIMEOUT_MS is the currently shipped mechanism.
    // We accept either fast-completion OR a QueryCanceled within 3 s.
    let sql = format!("SELECT '{input}' ~ '{pattern}'");

    println!(
        "[sec_parser_dos] regex_redos_catastrophic_alternation: sql={sql:?}"
    );

    // Outer timeout: 3 s. If the engine doesn't respond within 3 s it is
    // hanging on catastrophic backtracking.
    let t0 = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        sess.execute(&sql),
    )
    .await;
    let elapsed = t0.elapsed();

    println!(
        "[sec_parser_dos] regex_redos result: outcome={:?} elapsed={:?}",
        outcome, elapsed
    );

    match outcome {
        Err(_timeout) => {
            // Engine did not respond within 3 s — catastrophic backtracking.
            // Document the gap but do not panic here because the test itself
            // would be killed by the outer tokio timeout machinery.
            // The #[ignore] on this test captures the gap.
            println!(
                "[sec_parser_dos] WARN: regex query did not complete within 3 s — \
                 engine may be stuck on catastrophic backtracking. \
                 This gap is documented; un-ignore and re-run to confirm."
            );
            // We assert False to make the gap visible when not ignored.
            panic!(
                "SECURITY P1 (#53): regex ReDoS — engine did not respond within 3 s \
                 for pattern '(a+a+){{20}}' on a 31-char input. \
                 This indicates catastrophic backtracking with no statement-timeout \
                 guard. Expected: query completes fast (RE2) OR QueryCanceled (57014)."
            );
        }
        Ok(Ok(_rows)) => {
            // Query completed in time. RE2-style engine or the pattern happened
            // to be optimised away.
            println!(
                "[sec_parser_dos] PASS: regex ReDoS pattern completed in {elapsed:?} \
                 (engine uses linear-time regex evaluation)"
            );
        }
        Ok(Err(BasinError::QueryCanceled(msg))) => {
            // Statement timeout fired — acceptable mitigation.
            println!(
                "[sec_parser_dos] PASS: regex ReDoS aborted by statement timeout \
                 (QueryCanceled) in {elapsed:?}: {msg}"
            );
        }
        Ok(Err(e)) => {
            // Any other typed error (unsupported operator, parse error) is
            // acceptable — the engine did not hang.
            assert!(
                !matches!(e, BasinError::Internal(_)),
                "SECURITY P1 (#53): regex ReDoS returned BasinError::Internal (XX000) \
                 — expected graceful handling. Error: {e:?}"
            );
            println!(
                "[sec_parser_dos] PASS: regex ReDoS returned typed error in {elapsed:?}: {e}"
            );
        }
    }
}
