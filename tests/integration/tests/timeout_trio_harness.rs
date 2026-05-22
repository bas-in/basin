//! Phase 5.28.A — lock_timeout + idle_in_transaction_session_timeout harness.
//!
//! **Task:** TASK.md Phase 5.28.A — Timeout-firing + tooling-compat test harness.
//! **Test-first convention:** this file lands BEFORE lock_timeout,
//! idle_in_transaction_session_timeout, and SQL-level SET statement_timeout are
//! implemented. Every test is `#[ignore]`-gated until 5.28.B/C/D close the
//! corresponding slices.
//!
//! ## Named slices
//!
//! | Test fn                                              | Slice                                              | Closed by       |
//! |------------------------------------------------------|----------------------------------------------------|-----------------|
//! | `lock_timeout_fires_and_returns_55P03`               | lock_timeout fires                                 | 5.28.B          |
//! | `idle_in_transaction_session_timeout_closes_session` | idle_in_transaction terminates                     | 5.28.C          |
//! | `statement_timeout_already_works_per_6_P0_A`         | statement_timeout (already shipped — assert stays) | 5.28.B (SET wire-up) + pg_sleep |
//! | `tooling_psql_dbeaver_pgcli_compat`                  | tooling compat                                     | 5.28.D          |
//!
//! ## How to flip a slice green
//!
//! * **Slice 1** (`lock_timeout fires`): implement the `lock_timeout` GUC +
//!   per-lock-acquisition enforcement in `basin-shard`; return SQLSTATE 55P03.
//!   Drop the `#[ignore]`. Closed by 5.28.B.
//! * **Slice 2** (`idle_in_transaction terminates`): implement the GUC + session
//!   reaper. Drop the `#[ignore]`. Closed by 5.28.C.
//! * **Slice 3** (`statement_timeout — canary`): currently RED because
//!   SQL-level `SET statement_timeout` is not yet wired to the executor and
//!   `pg_sleep` is not registered as a UDF. Drop the `#[ignore]` once both are
//!   available. The env-var path (`BASIN_STATEMENT_TIMEOUT_MS`) works at unit-
//!   test level within basin-engine (see `slow_join_is_canceled_and_connection_survives`).
//! * **Slice 4** (`tooling compat`): requires `BASIN_TOOLING_COMPAT_TEST=1` and
//!   `psql` in PATH. Drop the `#[ignore]`. Closed by 5.28.D.
//!
//! ## Harness design
//!
//! All four slices spin up an in-process Basin pgwire server and drive
//! `tokio-postgres` over the real TCP path (mirrors `pgwire_wire_compat.rs` /
//! `optimistic_lock_verify.rs`). This gives accurate SQLSTATE propagation
//! through the pgwire error envelope.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::NoTls;

// ─── shared pgwire server helpers ────────────────────────────────────────────

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _dir: TempDir,
}

/// Spin up an in-process Basin pgwire server bound to a random local port.
/// The single project is registered under the username `timeout_test`.
async fn start_server() -> TestServer {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let project = ProjectId::new();
    let mut map = HashMap::new();
    map.insert("timeout_test".to_owned(), project);
    let resolver = Arc::new(StaticProjectResolver::new(map));

    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        project_resolver: resolver,
        pool: None,
        shard_endpoints: None,
        tls: None,
        connection_limiter: None,
    })
    .await
    .expect("server failed to bind");

    TestServer {
        addr: running.local_addr,
        _shutdown: running.shutdown,
        _join: running.join,
        _dir: dir,
    }
}

/// Open a new tokio-postgres client connected to the test server.
async fn connect(addr: SocketAddr) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user=timeout_test password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect: {e}"));
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            // Connection closure is expected in idle_in_transaction tests.
            eprintln!("timeout_trio conn driver: {e}");
        }
    });
    client
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 1 — lock_timeout fires and returns SQLSTATE 55P03
// ─────────────────────────────────────────────────────────────────────────────
//
// Scenario:
//   Client A: BEGIN; UPDATE locks_test SET val = 1 WHERE id = 1; (holds row lock)
//   Client B: SET lock_timeout = '100ms'; BEGIN; UPDATE locks_test … (same row)
//     → must return SQLSTATE 55P03 (lock_not_available) within ~200 ms.
//
// RED until 5.28.B: Basin does not yet track SQL-level row locks between
// sessions, so client B either succeeds (no isolation) or blocks indefinitely,
// neither of which produces 55P03.

/// Phase 5.28.A — lock_timeout fires + 55P03 SQLSTATE.
///
/// Transaction 1 acquires a row lock on `locks_test.id = 1`. Transaction 2
/// sets `lock_timeout = '100ms'` and attempts the same row lock — the engine
/// must return SQLSTATE `55P03` (lock_not_available) within ~200 ms.
///
/// Closed by: 5.28.B (`lock_timeout` GUC + per-lock enforcement).
#[ignore = "5.28.B — requires real cross-session row-lock contention; Basin does not yet \
    track SQL-level row locks between sessions so client_b's UPDATE either succeeds \
    (no isolation) or sees a write-write conflict at the storage layer, neither of \
    which produces SQLSTATE 55P03; closes when per-lock acquisition tracking lands"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lock_timeout_fires_and_returns_55P03() {
    // Slice: "lock_timeout fires"
    //
    // IMPORTANT: every assertion reflects *correct PostgreSQL semantics*.
    // Do NOT weaken them to match Basin's current behaviour — that defeats the
    // test-first contract. Implement 5.28.B to flip this green.

    let server = start_server().await;
    let client_a = connect(server.addr).await;
    let client_b = connect(server.addr).await;

    // ── Setup: table + seed row ────────────────────────────────────────────────
    client_a
        .execute(
            "CREATE TABLE locks_test (id BIGINT NOT NULL, val BIGINT NOT NULL)",
            &[],
        )
        .await
        .expect("CREATE TABLE locks_test");
    client_a
        .execute("INSERT INTO locks_test (id, val) VALUES (1, 0)", &[])
        .await
        .expect("INSERT seed row");

    // ── Transaction A: acquire row lock and hold it ───────────────────────────
    client_a.execute("BEGIN", &[]).await.expect("client_a: BEGIN");
    client_a
        .execute("UPDATE locks_test SET val = 1 WHERE id = 1", &[])
        .await
        .expect("client_a: UPDATE (acquires row lock)");
    println!("[5.28.A lock_timeout] client_a holds row lock on id=1");

    // ── Transaction B: conflicting UPDATE with a 100 ms deadline ─────────────
    client_b
        .execute("SET lock_timeout = '100ms'", &[])
        .await
        .expect("client_b: SET lock_timeout");
    client_b.execute("BEGIN", &[]).await.expect("client_b: BEGIN");

    let t0 = Instant::now();
    let result = client_b
        .execute("UPDATE locks_test SET val = 2 WHERE id = 1", &[])
        .await;
    let elapsed = t0.elapsed();

    println!(
        "[5.28.A lock_timeout] client_b result: {:?}  elapsed: {:?}",
        result, elapsed
    );

    // Must fail — the row lock is held by client_a and lock_timeout is 100 ms.
    let err = result.expect_err(
        "LOCK_TIMEOUT: client_b's UPDATE must fail with SQLSTATE 55P03 \
         (lock_not_available) — the row is locked by client_a. \
         Closed by 5.28.B.",
    );
    let db_err = err.as_db_error().unwrap_or_else(|| {
        panic!(
            "LOCK_TIMEOUT: expected a pg DbError (SQLSTATE 55P03), \
             got a different error kind: {err:?}"
        )
    });
    assert_eq!(
        db_err.code().code(),
        "55P03",
        "LOCK_TIMEOUT: SQLSTATE must be 55P03 (lock_not_available); \
         got={:?}. Closed by 5.28.B.",
        db_err.code().code()
    );

    // Timeout must fire within a generous 500 ms window (100 ms deadline +
    // network + CI scheduling headroom).
    assert!(
        elapsed < Duration::from_millis(500),
        "LOCK_TIMEOUT: 55P03 must appear within ~200 ms; elapsed={elapsed:?} \
         (lock_timeout=100ms). Closed by 5.28.B."
    );

    println!(
        "[5.28.A lock_timeout] PASSED — 55P03 received within {:?}",
        elapsed
    );

    // Cleanup: rollback client_a's holding transaction.
    let _ = client_a.execute("ROLLBACK", &[]).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 2 — idle_in_transaction_session_timeout closes the session
// ─────────────────────────────────────────────────────────────────────────────
//
// Scenario:
//   BEGIN;
//   SET idle_in_transaction_session_timeout = '500ms';
//   <idle for 1 second — no SQL>
//   → engine terminates the session; next query gets a connection-closed error.
//
// RED until 5.28.C: Basin has no session reaper, so the session remains open
// indefinitely — `SELECT 1` succeeds instead of failing.

/// Phase 5.28.A — idle_in_transaction_session_timeout closes the session.
///
/// Opens a transaction, sets a 500 ms idle-in-transaction timeout, then idles
/// for 1 s. The server must terminate the session; the next query must return
/// a connection-closed or SQLSTATE 25P03 error.
///
/// Closed by: 5.28.C (`idle_in_transaction_session_timeout` GUC + reaper).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_in_transaction_session_timeout_closes_session() {
    // Slice: "idle_in_transaction terminates"
    //
    // IMPORTANT: do not weaken assertions to match current (no-op) behaviour.
    // Implement 5.28.C to flip this green.

    let server = start_server().await;
    let client = connect(server.addr).await;

    // ── Open transaction and set the idle-in-transaction timeout ─────────────
    client.execute("BEGIN", &[]).await.expect("idle_in_txn: BEGIN");
    client
        .execute(
            "SET idle_in_transaction_session_timeout = '500ms'",
            &[],
        )
        .await
        .expect("idle_in_txn: SET idle_in_transaction_session_timeout");

    println!(
        "[5.28.A idle_in_txn] transaction open; \
         idle_in_transaction_session_timeout=500ms; sleeping 1s…"
    );

    // ── Idle for 1 second (2× the 500 ms deadline) ────────────────────────────
    tokio::time::sleep(Duration::from_secs(1)).await;

    // ── Next query must fail — session was terminated by the reaper ───────────
    let result = client.execute("SELECT 1", &[]).await;
    println!(
        "[5.28.A idle_in_txn] post-idle SELECT result: {:?}",
        result
    );

    assert!(
        result.is_err(),
        "IDLE_IN_TXN: the session must be terminated after \
         idle_in_transaction_session_timeout fires (500ms; we waited 1s). \
         The next query must fail. Closed by 5.28.C."
    );

    let err = result.unwrap_err();
    // Accept either a PG-level error (SQLSTATE 25P03 / 57P01) or a
    // connection-level IO error — both confirm the session was terminated.
    let is_termination = match err.as_db_error() {
        Some(db_err) => {
            let code = db_err.code().code();
            println!(
                "[5.28.A idle_in_txn] SQLSTATE={code} message={:?}",
                db_err.message()
            );
            // 25P03 = idle_in_transaction_session_timeout
            // 57P01 = admin_shutdown (also acceptable)
            code == "25P03" || code == "57P01"
        }
        None => {
            // Connection-level error — server closed the TCP connection.
            println!("[5.28.A idle_in_txn] connection-level error (no SQLSTATE): {err:?}");
            true
        }
    };

    assert!(
        is_termination,
        "IDLE_IN_TXN: error must be a session-termination indicator \
         (SQLSTATE 25P03/57P01 or connection-closed). \
         Got: {err:?}. Closed by 5.28.C."
    );

    println!("[5.28.A idle_in_txn] PASSED — session terminated after idle timeout");
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 3 — statement_timeout canary (Phase 6.P0.A — already shipped)
// ─────────────────────────────────────────────────────────────────────────────
//
// This slice asserts that `SET statement_timeout = '100ms'` followed by
// `SELECT pg_sleep(1)` returns SQLSTATE `57014` (query_canceled).
//
// Current state (RED):
//   (a) SQL-level `SET statement_timeout` is silently accepted but NOT wired to
//       the executor's timeout path — that wiring is part of 5.28.B scope.
//       The env-var path (`BASIN_STATEMENT_TIMEOUT_MS`) DOES work and is
//       tested by `slow_join_is_canceled_and_connection_survives` inside
//       the basin-engine crate unit tests.
//   (b) `pg_sleep` is not yet registered as a UDF — the engine returns
//       `Invalid function 'pg_sleep'` instead of sleeping.
//
// How to flip green:
//   1. Register `pg_sleep(secs float8) → void` as a UDF that sleeps the
//      appropriate duration (via tokio::time::sleep or a blocking stub).
//   2. Wire `SET statement_timeout = '<interval>'` to the executor timeout
//      (currently `statement_timeout()` reads env-var once via OnceLock).
//   Both are within the 5.28.B scope; once done, drop the `#[ignore]` and
//   this test becomes the permanent canary for 6.P0.A non-regression.

/// Phase 5.28.A — statement_timeout canary (6.P0.A shipped; must not regress).
///
/// Sets `statement_timeout = '100ms'` via SQL and runs `SELECT pg_sleep(1)`.
/// The executor must cancel the query and return SQLSTATE `57014`.
///
/// Currently `#[ignore]`-d because SQL-level `SET statement_timeout` is not yet
/// wired to the executor timeout path, and `pg_sleep` is not registered.
/// Drop the `#[ignore]` once both prerequisites land (within 5.28.B scope).
#[ignore = "5.28.B — pg_sleep is registered but DataFusion invokes it synchronously (block_in_place); \
    the outer tokio::time::timeout wrapping df.collect() cannot preempt an in-progress sync UDF call, \
    so SELECT pg_sleep(1) still runs to completion instead of timing out at 100ms; \
    closes when DataFusion gains async-UDF support or a per-statement cancellation token is threaded \
    through the executor to interrupt blocking UDF polls"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn statement_timeout_already_works_per_6_P0_A() {
    // Slice: "statement_timeout (already shipped 6.P0.A — assert it stays)"
    //
    // IMPORTANT: every assertion reflects correct PG semantics. The slice is
    // currently RED because of the two prerequisites listed above. When both are
    // resolved, this test serves as the permanent canary for statement_timeout.
    //
    // The underlying env-var mechanism (BASIN_STATEMENT_TIMEOUT_MS) IS working
    // as verified by the basin-engine unit test:
    //   `statement_timeout_tests::slow_join_is_canceled_and_connection_survives`
    // This integration-level slice exercises the SQL surface (SET + pg_sleep)
    // rather than the env-var path so that ORMs and client drivers that issue
    // `SET statement_timeout` get first-class coverage.

    let server = start_server().await;
    let client = connect(server.addr).await;

    // ── Set statement_timeout via SQL ─────────────────────────────────────────
    // After 5.28.B: this SET must be wired to the executor deadline.
    // Currently: silently accepted, no effect on the executor.
    client
        .execute("SET statement_timeout = '100ms'", &[])
        .await
        .expect("SET statement_timeout");
    println!("[5.28.A canary] SET statement_timeout = '100ms'");

    // ── Run a 1-second sleep — must be canceled within ~100 ms ───────────────
    // After pg_sleep UDF lands: sleep(1) blocks for 1 second; the 100 ms
    // statement_timeout fires first and returns SQLSTATE 57014.
    // Currently: pg_sleep is not registered → PlannerRejected error instead
    // of QueryCanceled. This is still an error (so the Ok branch panics), but
    // the wrong error kind.
    let t0 = Instant::now();
    let result = client.simple_query("SELECT pg_sleep(1)").await;
    let elapsed = t0.elapsed();
    println!(
        "[5.28.A canary] SELECT pg_sleep(1) result: {:?}  elapsed: {:?}",
        result, elapsed
    );

    let err = result.expect_err(
        "STATEMENT_TIMEOUT CANARY: SELECT pg_sleep(1) must fail — either \
         query_canceled (57014) once fully implemented, or planner-rejected \
         currently. The query must NOT succeed. Closed by 5.28.B.",
    );

    // After both prerequisites land: the error must be 57014 (query_canceled).
    // For now we assert that some error is returned (not the happy path).
    let db_err_opt = err.as_db_error();
    let is_correct_error = match db_err_opt {
        Some(db_err) => {
            let code = db_err.code().code();
            println!(
                "[5.28.A canary] SQLSTATE={code} message={:?}",
                db_err.message()
            );
            // Correct final state: 57014 (query_canceled from statement_timeout).
            // Acceptable intermediate states:
            //   42883 = undefined_function (pg_sleep not yet registered)
            //   0A000 = feature_not_supported
            //   XX000 = internal (Basin's current error for unknown UDF)
            // Any DbError from the server is an acceptable intermediate — it proves
            // the server is alive and responding, just not yet implementing the
            // full timeout surface. The `57014` assertion below drives the slice green.
            !code.is_empty()
        }
        None => false,
    };

    assert!(
        is_correct_error,
        "STATEMENT_TIMEOUT CANARY: expected a db error (any SQLSTATE). \
         Got a non-db error: {err:?}. Closed by 5.28.B."
    );

    // The final bar: SQLSTATE must be exactly 57014 (query_canceled).
    // This assertion drives the slice green — it WILL FAIL until:
    //   1. pg_sleep UDF is registered
    //   2. SET statement_timeout is wired to the executor deadline
    // Both are within 5.28.B scope.
    let db_err = db_err_opt.unwrap(); // safe: is_correct_error checks Some
    assert_eq!(
        db_err.code().code(),
        "57014",
        "STATEMENT_TIMEOUT CANARY: SQLSTATE must be 57014 (query_canceled). \
         Currently getting {:?} — prerequisites not yet met. \
         Closed by 5.28.B (pg_sleep UDF + SET statement_timeout wiring).",
        db_err.code().code()
    );

    // The cancellation must fire within 500 ms (100 ms timeout + CI slack).
    assert!(
        elapsed < Duration::from_millis(500),
        "STATEMENT_TIMEOUT CANARY: cancellation must fire within 500 ms \
         (statement_timeout=100ms). elapsed={elapsed:?}"
    );

    println!(
        "[5.28.A canary] PASSED — SQLSTATE 57014 received within {elapsed:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 4 — tooling compat (psql / DBeaver / pgcli)
// ─────────────────────────────────────────────────────────────────────────────
//
// Scenario: invoke `psql` with `--variable=lock_timeout=100ms`; expect clean
// exit (exit 0). DBeaver and pgcli emit SHOW lock_timeout during preamble;
// those must return PG-shaped values rather than errors.
//
// Env-gated: only runs when `BASIN_TOOLING_COMPAT_TEST=1` AND `psql` is found
// in PATH.
//
// RED until 5.28.D: `SHOW lock_timeout` / `current_setting('lock_timeout')`
// currently returns an error or empty result rather than the PG-expected
// `'0'` default, so psql's session preamble may log errors.

/// Phase 5.28.A — tooling compat (psql / DBeaver / pgcli).
///
/// Runs `psql --variable=lock_timeout=100ms` against the test server and
/// asserts a clean exit (exit code 0). Requires `BASIN_TOOLING_COMPAT_TEST=1`
/// and `psql` in PATH; the test is skipped if either is absent.
///
/// Closed by: 5.28.D (GUC surface for `SHOW lock_timeout` + SET round-trip).
#[ignore = "5.28.D — tooling compat requires SHOW lock_timeout / current_setting('lock_timeout') \
    to return a PG-shaped value ('0' default); SET lock_timeout now stores the value but the SHOW \
    path returns an error rather than the expected string; also needs psql in PATH and \
    BASIN_TOOLING_COMPAT_TEST=1 env var set; closes when GUC SHOW surface for lock_timeout lands"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tooling_psql_dbeaver_pgcli_compat() {
    // Slice: "tooling compat"
    //
    // Env gate: skip silently unless BASIN_TOOLING_COMPAT_TEST=1.
    if std::env::var("BASIN_TOOLING_COMPAT_TEST").as_deref() != Ok("1") {
        println!(
            "[5.28.A tooling] SKIPPED — set BASIN_TOOLING_COMPAT_TEST=1 to run \
             (requires psql in PATH). Closed by 5.28.D."
        );
        return;
    }

    // Find psql in PATH.
    let psql_bin = match which_psql() {
        Some(p) => {
            println!("[5.28.A tooling] psql found at: {p}");
            p
        }
        None => {
            println!(
                "[5.28.A tooling] SKIPPED — psql not found in PATH. \
                 Install postgresql-client and retry. Closed by 5.28.D."
            );
            return;
        }
    };

    let server = start_server().await;
    let host = server.addr.ip().to_string();
    let port = server.addr.port().to_string();

    // ── psql: connect with --variable=lock_timeout=100ms ──────────────────────
    // psql sends a SHOW lock_timeout during its session preamble; the server
    // must return the PG-expected value (not an error). After 5.28.D this
    // returns '0' (the default) and psql exits cleanly.
    let output = tokio::process::Command::new(&psql_bin)
        .args([
            "--host",
            &host,
            "--port",
            &port,
            "--username",
            "timeout_test",
            "--dbname",
            "timeout_test",
            "--no-password",
            "--variable",
            "lock_timeout=100ms",
            "--command",
            "SELECT 1 AS tooling_compat_ok",
            "--tuples-only",
        ])
        .env("PGPASSWORD", "ignored")
        .output()
        .await
        .expect("psql invocation failed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!("[5.28.A tooling] psql exit: {:?}", output.status);
    println!("[5.28.A tooling] psql stdout: {stdout}");
    println!("[5.28.A tooling] psql stderr: {stderr}");

    // psql must exit 0 (clean exit).
    assert!(
        output.status.success(),
        "TOOLING COMPAT: psql must exit 0 when --variable=lock_timeout=100ms \
         is set. exit={:?}  stderr={stderr}. Closed by 5.28.D.",
        output.status
    );

    // The SELECT 1 result must appear in stdout.
    assert!(
        stdout.contains('1'),
        "TOOLING COMPAT: psql stdout must contain '1' (SELECT 1 output). \
         got: {stdout:?}. Closed by 5.28.D."
    );

    println!("[5.28.A tooling] PASSED — psql exited cleanly with lock_timeout=100ms");
}

/// Search for `psql` in the current PATH. Returns the first match or `None`.
fn which_psql() -> Option<String> {
    let path_var = std::env::var("PATH").unwrap_or_default();
    for dir in path_var.split(':') {
        let candidate = format!("{dir}/psql");
        if std::fs::metadata(&candidate)
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            return Some(candidate);
        }
    }
    None
}
