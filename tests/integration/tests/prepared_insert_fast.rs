//! Acceptance + parity gate for the bind-INSERT AST fast path
//! (perf-w-prepared).
//!
//! Background
//! ----------
//! Every extended-protocol `Execute` used to substitute the bind values into
//! the SQL TEXT and hand the freshly-built string to `executor::execute`,
//! which re-parses it (libpg_query for the noop/DDL pre-screens, then
//! sqlparser for dispatch). Because the literal values differ on every
//! `Execute`, the sqlparser parse-cache never hit — so a bulk-INSERT loop
//! paid two full parses per row.
//!
//! The fast path caches the template AST at `Parse` time and, at `Execute`,
//! deep-substitutes the decoded bind values into a clone of that AST and
//! dispatches the `Statement` directly via `executor::execute_statement`,
//! skipping both parses. It is gated to `INSERT` / plain `SELECT` templates
//! that trigger no text-rewrite pass; anything else (or any substitution that
//! leaves a placeholder behind) falls back to the unchanged text route.
//!
//! These tests assert end-to-end correctness through the real pgwire
//! extended-query path (`tokio_postgres`'s parameterised API), proving the
//! fast path is behaviour-identical to the text path for the in-scope shapes:
//!  * a prepared INSERT executed 100x with different binds (all rows land);
//!  * INSERT … RETURNING;
//!  * prepared SELECT point reads;
//!  * NULL binds;
//!  * text- and binary-format param round-trip parity;
//!  * a template that DOES need the rewrite pipeline (`payload->>'k'`) still
//!    works via the text fallback;
//!  * interleaved prepares on one connection.
//!
//! Plus an `#[ignore]` timing probe comparing prepared-INSERT vs
//! simple-protocol-INSERT p50 over 1000 iterations.
//!
//! The in-process-server harness mirrors `prepared_statements.rs`.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls};

// ---------------------------------------------------------------------------
// Test server helpers — same shape as prepared_statements.rs.
// ---------------------------------------------------------------------------

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _dir: TempDir,
}

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
    map.insert("alice".to_owned(), project);
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

async fn connect(addr: SocketAddr) -> Client {
    let conn_str = format!(
        "host={} port={} user=alice password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("prepared_insert_fast conn driver: {e}");
        }
    });
    client
}

/// Events fixture: BIGINT id + TEXT label + nullable BIGINT score.
async fn seed_events(client: &Client) {
    client
        .simple_query(
            "CREATE TABLE events (\
                 id BIGINT NOT NULL, \
                 label TEXT NOT NULL, \
                 score BIGINT\
             )",
        )
        .await
        .expect("create events");
}

// =============================================================================
// 1. Prepared INSERT executed 100x with different binds → all rows present.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_insert_100x_all_rows_present() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_events(&client).await;

    // `prepare` once; reuse the statement for every Execute. This is the exact
    // path the AST fast path optimises.
    let stmt = client
        .prepare("INSERT INTO events (id, label, score) VALUES ($1, $2, $3)")
        .await
        .expect("prepare insert");

    for i in 0..100_i64 {
        let n = client
            .execute(&stmt, &[&i, &format!("row-{i}"), &(i * 10)])
            .await
            .unwrap_or_else(|e| panic!("execute insert {i}: {e}"));
        assert_eq!(n, 1, "each INSERT affects exactly one row");
    }

    // Every row landed with the correct values.
    let rows = client
        .query("SELECT id, label, score FROM events ORDER BY id", &[])
        .await
        .expect("read back");
    assert_eq!(rows.len(), 100, "all 100 inserted rows present");
    for (i, row) in rows.iter().enumerate() {
        let i = i as i64;
        assert_eq!(row.get::<_, i64>(0), i);
        assert_eq!(row.get::<_, &str>(1), format!("row-{i}"));
        assert_eq!(row.get::<_, i64>(2), i * 10);
    }
}

// =============================================================================
// 2. Prepared INSERT ... RETURNING — fast path must still surface the row.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_insert_returning() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_events(&client).await;

    for i in 0..10_i64 {
        let row = client
            .query_one(
                "INSERT INTO events (id, label, score) VALUES ($1, $2, $3) RETURNING id, label, score",
                &[&i, &format!("ret-{i}"), &(i + 1)],
            )
            .await
            .unwrap_or_else(|e| panic!("insert returning {i}: {e}"));
        assert_eq!(row.get::<_, i64>(0), i);
        assert_eq!(row.get::<_, &str>(1), format!("ret-{i}"));
        assert_eq!(row.get::<_, i64>(2), i + 1);
    }

    let count = client
        .query_one("SELECT count(*) FROM events", &[])
        .await
        .expect("count");
    assert_eq!(count.get::<_, i64>(0), 10);
}

// =============================================================================
// 3. Prepared SELECT point reads — fast path applies to plain SELECT too.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_select_point_reads() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_events(&client).await;

    let insert = client
        .prepare("INSERT INTO events (id, label, score) VALUES ($1, $2, $3)")
        .await
        .expect("prepare insert");
    for i in 0..50_i64 {
        client
            .execute(&insert, &[&i, &format!("pt-{i}"), &i])
            .await
            .unwrap();
    }

    let select = client
        .prepare("SELECT label, score FROM events WHERE id = $1")
        .await
        .expect("prepare select");
    for i in 0..50_i64 {
        let row = client
            .query_one(&select, &[&i])
            .await
            .unwrap_or_else(|e| panic!("point read {i}: {e}"));
        assert_eq!(row.get::<_, &str>(0), format!("pt-{i}"));
        assert_eq!(row.get::<_, i64>(1), i);
    }

    // No-match must yield zero rows, not error.
    let rows = client.query(&select, &[&9999_i64]).await.expect("no-match");
    assert_eq!(rows.len(), 0);
}

// =============================================================================
// 4. NULL binds round-trip identically.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_insert_null_binds() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_events(&client).await;

    let none_score: Option<i64> = None;
    let stmt = client
        .prepare("INSERT INTO events (id, label, score) VALUES ($1, $2, $3)")
        .await
        .expect("prepare");

    client
        .execute(&stmt, &[&1_i64, &"has-null", &none_score])
        .await
        .expect("insert null score");
    client
        .execute(&stmt, &[&2_i64, &"has-val", &7_i64])
        .await
        .expect("insert non-null score");

    let null_row = client
        .query_one("SELECT score FROM events WHERE id = $1", &[&1_i64])
        .await
        .expect("read null row");
    assert_eq!(
        null_row.get::<_, Option<i64>>(0),
        None,
        "NULL bind preserved"
    );

    let val_row = client
        .query_one("SELECT score FROM events WHERE id = $1", &[&2_i64])
        .await
        .expect("read non-null row");
    assert_eq!(val_row.get::<_, Option<i64>>(0), Some(7));
}

// =============================================================================
// 5. Text- and binary-format params produce identical stored rows.
//
// `query`/`execute` send params in BINARY by default (extended protocol →
// AST fast path); `simple_query` carries the same logical values as TEXT
// literals (simple protocol → text route). Both must store byte-identical
// rows. We additionally assert that a mix of wire types — INT8 (binary),
// TEXT, FLOAT8, BOOL — all round-trip through the fast-path INSERT.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_insert_text_and_binary_formats_parity() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    client
        .simple_query(
            "CREATE TABLE wide (\
                 id BIGINT NOT NULL, \
                 label TEXT NOT NULL, \
                 ratio DOUBLE PRECISION NOT NULL, \
                 ok BOOLEAN NOT NULL\
             )",
        )
        .await
        .expect("create wide");

    // Binary-format params (tokio_postgres default), exercising four wire types.
    client
        .execute(
            "INSERT INTO wide (id, label, ratio, ok) VALUES ($1, $2, $3, $4)",
            &[&100_i64, &"mix", &2.5_f64, &true],
        )
        .await
        .expect("binary-format insert");

    // Same logical row via the TEXT (simple-protocol) path.
    client
        .simple_query("INSERT INTO wide (id, label, ratio, ok) VALUES (101, 'mix', 2.5, true)")
        .await
        .expect("text-format (simple) insert");

    // Both rows must agree on every non-id column.
    let rows = client
        .query(
            "SELECT id, label, ratio, ok FROM wide WHERE label = $1 ORDER BY id",
            &[&"mix"],
        )
        .await
        .expect("read both");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i64>(0), 100);
    assert_eq!(rows[1].get::<_, i64>(0), 101);
    assert_eq!(rows[0].get::<_, &str>(1), rows[1].get::<_, &str>(1));
    assert_eq!(rows[0].get::<_, f64>(2), rows[1].get::<_, f64>(2));
    assert_eq!(rows[0].get::<_, bool>(3), rows[1].get::<_, bool>(3));
    assert_eq!(rows[0].get::<_, f64>(2), 2.5);
    assert!(rows[0].get::<_, bool>(3));
}

// =============================================================================
// 6. A template that NEEDS the rewrite pipeline still works (text fallback).
//
// `payload->>'k'` lowers a JSON arrow operator — `needs_rewrite_pipeline` is
// true, so this prepared statement is NOT AST-fast-path eligible and must bind
// through the unchanged text route. The result must be correct.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_json_arrow_falls_back_to_text_path() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE docs (id BIGINT NOT NULL, payload JSONB NOT NULL)")
        .await
        .expect("create docs");
    client
        .simple_query(
            "INSERT INTO docs (id, payload) VALUES \
                 (1, '{\"k\":\"alpha\"}'), \
                 (2, '{\"k\":\"beta\"}')",
        )
        .await
        .expect("seed docs");

    // `payload->>'k' = $1` forces the JSON-operator rewrite pass; the bind
    // path must fall back to text and still return the right row.
    let rows = client
        .query(
            "SELECT id FROM docs WHERE payload->>'k' = $1 ORDER BY id",
            &[&"beta"],
        )
        .await
        .expect("json arrow filter");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 2);
}

// =============================================================================
// 7. Interleaved prepares on one connection don't cross-contaminate.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interleaved_prepares() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_events(&client).await;

    let ins = client
        .prepare("INSERT INTO events (id, label, score) VALUES ($1, $2, $3)")
        .await
        .expect("prepare ins");
    let sel = client
        .prepare("SELECT label FROM events WHERE id = $1")
        .await
        .expect("prepare sel");
    let ins_two = client
        .prepare("INSERT INTO events (id, label) VALUES ($1, $2)")
        .await
        .expect("prepare ins_two");

    // Interleave executes against the three distinct prepared statements.
    client
        .execute(&ins, &[&1_i64, &"three-col", &9_i64])
        .await
        .unwrap();
    client
        .execute(&ins_two, &[&2_i64, &"two-col"])
        .await
        .unwrap();
    client
        .execute(&ins, &[&3_i64, &"three-col-b", &11_i64])
        .await
        .unwrap();

    let r1 = client.query_one(&sel, &[&1_i64]).await.unwrap();
    assert_eq!(r1.get::<_, &str>(0), "three-col");
    let r2 = client.query_one(&sel, &[&2_i64]).await.unwrap();
    assert_eq!(r2.get::<_, &str>(0), "two-col");
    let r3 = client.query_one(&sel, &[&3_i64]).await.unwrap();
    assert_eq!(r3.get::<_, &str>(0), "three-col-b");

    // The two-column INSERT left score NULL (no $3 bound).
    let score = client
        .query_one("SELECT score FROM events WHERE id = $1", &[&2_i64])
        .await
        .unwrap();
    assert_eq!(score.get::<_, Option<i64>>(0), None);
}

// =============================================================================
// 8. Timing probe — prepared-INSERT (extended/AST fast path) vs simple-protocol
//    INSERT, p50 over 1000 iterations. Ignored by default (informational).
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "timing probe; run explicitly with --ignored"]
async fn timing_prepared_vs_simple_insert() {
    const ITERS: usize = 1000;

    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_events(&client).await;

    // Prepared / extended-protocol path (AST fast path applies).
    let stmt = client
        .prepare("INSERT INTO events (id, label, score) VALUES ($1, $2, $3)")
        .await
        .expect("prepare");
    let mut prepared_us: Vec<u128> = Vec::with_capacity(ITERS);
    for i in 0..ITERS as i64 {
        let t = Instant::now();
        client
            .execute(&stmt, &[&i, &"p", &i])
            .await
            .expect("prepared execute");
        prepared_us.push(t.elapsed().as_micros());
    }

    // Simple-protocol path (full text parse every time).
    let mut simple_us: Vec<u128> = Vec::with_capacity(ITERS);
    for i in 0..ITERS as i64 {
        let id = i + 1_000_000;
        let sql = format!("INSERT INTO events (id, label, score) VALUES ({id}, 's', {id})");
        let t = Instant::now();
        client.simple_query(&sql).await.expect("simple execute");
        simple_us.push(t.elapsed().as_micros());
    }

    let p50 = |v: &mut [u128]| {
        v.sort_unstable();
        v[v.len() / 2]
    };
    let prepared_p50 = p50(&mut prepared_us);
    let simple_p50 = p50(&mut simple_us);
    println!("[prepared-insert] prepared(extended/AST) p50 = {prepared_p50} us");
    println!("[prepared-insert] simple(text-parse)     p50 = {simple_p50} us");
    println!(
        "[prepared-insert] speedup (simple/prepared)  = {:.2}x",
        simple_p50 as f64 / prepared_p50.max(1) as f64
    );
}

// =============================================================================
// 9. Prepared LITERAL multi-row INSERT (zero parameters) — extended-protocol
//    shape 1. Some ORMs batch by interpolating literals into ONE statement and
//    then PREPARE/EXECUTE it. The 10k-row prepared Execute must produce rows
//    identical to the same statement through the simple protocol, and a
//    re-Execute of the same statement must hit the PK constraint exactly like
//    re-sending the simple statement.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_literal_10k_insert_matches_simple() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    for t in ["lit_prep", "lit_simple"] {
        client
            .simple_query(&format!(
                "CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, label TEXT, score BIGINT)"
            ))
            .await
            .unwrap_or_else(|e| panic!("create {t}: {e}"));
    }

    let n = 10_000usize;
    let tuples = (0..n)
        .map(|i| format!("({i}, 'it''s-{i}', {})", i * 3))
        .collect::<Vec<_>>()
        .join(", ");

    // Simple protocol.
    client
        .simple_query(&format!(
            "INSERT INTO lit_simple (id, label, score) VALUES {tuples}"
        ))
        .await
        .expect("simple literal insert");

    // Extended protocol: PREPARE once, EXECUTE once (zero params).
    let stmt = client
        .prepare(&format!(
            "INSERT INTO lit_prep (id, label, score) VALUES {tuples}"
        ))
        .await
        .expect("prepare literal insert");
    assert_eq!(
        stmt.params().len(),
        0,
        "literal statement has no parameters"
    );
    let affected = client
        .execute(&stmt, &[])
        .await
        .expect("execute prepared literal insert");
    assert_eq!(affected as usize, n);

    // Equivalence: both tables hold identical rows.
    let rows = client
        .query(
            "SELECT p.id, p.label, p.score, s.label, s.score \
             FROM lit_prep p JOIN lit_simple s ON p.id = s.id ORDER BY p.id",
            &[],
        )
        .await
        .expect("join read-back");
    assert_eq!(rows.len(), n, "every prepared row must match a simple row");
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<_, i64>(0), i as i64);
        assert_eq!(row.get::<_, &str>(1), row.get::<_, &str>(3));
        assert_eq!(row.get::<_, i64>(2), row.get::<_, i64>(4));
        assert_eq!(row.get::<_, &str>(1), format!("it's-{i}"));
    }

    // Re-Execute of the same prepared literal statement re-inserts identical
    // rows → PK violation, same as re-sending the simple statement.
    let err = client.execute(&stmt, &[]).await;
    assert!(err.is_err(), "duplicate PK re-execute must fail");
    let count = client
        .query_one("SELECT count(*) FROM lit_prep", &[])
        .await
        .expect("count");
    assert_eq!(count.get::<_, i64>(0), n as i64);
}

// =============================================================================
// 10. Parameterized INSERT with JSONB + TIMESTAMPTZ binary params — the
//     bind-direct batch builder must round-trip the binary wire formats the
//     real drivers send (JSONB v1 framing, PG-epoch timestamps), and the
//     stored rows must equal a simple-protocol literal control row.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_insert_jsonb_timestamptz_binary_params() {
    use std::time::{Duration, UNIX_EPOCH};

    let server = start_server().await;
    let client = connect(server.addr).await;
    client
        .simple_query(
            "CREATE TABLE docs2 (id BIGINT NOT NULL PRIMARY KEY, \
             doc JSONB NOT NULL, at TIMESTAMPTZ NOT NULL)",
        )
        .await
        .expect("create docs2");

    let stmt = client
        .prepare("INSERT INTO docs2 (id, doc, at) VALUES ($1, $2, $3)")
        .await
        .expect("prepare");

    // 2026-01-02 03:04:05.123456 UTC, microsecond precision.
    let at = UNIX_EPOCH + Duration::from_micros(1_767_323_045_123_456);
    let doc = serde_json::json!({"k": "v", "n": 42, "arr": [1, 2, 3], "obj": {"b": 2, "a": 1}});
    for i in 0..5_i64 {
        let n = client
            .execute(&stmt, &[&i, &doc, &at])
            .await
            .unwrap_or_else(|e| panic!("binary-param insert {i}: {e}"));
        assert_eq!(n, 1);
    }

    // Control row via the simple protocol with equivalent literals.
    client
        .simple_query(
            "INSERT INTO docs2 (id, doc, at) VALUES \
             (100, '{\"k\":\"v\",\"n\":42,\"arr\":[1,2,3],\"obj\":{\"b\":2,\"a\":1}}', \
              '2026-01-02 03:04:05.123456+00')",
        )
        .await
        .expect("simple control insert");

    let rows = client
        .query("SELECT id, doc, at FROM docs2 ORDER BY id", &[])
        .await
        .expect("read back");
    assert_eq!(rows.len(), 6);
    let control_doc: serde_json::Value = rows[5].get(1);
    let control_at: std::time::SystemTime = rows[5].get(2);
    for row in &rows[..5] {
        let got_doc: serde_json::Value = row.get(1);
        assert_eq!(got_doc, doc, "JSONB binary param round-trip");
        assert_eq!(got_doc, control_doc, "prepared JSONB equals simple literal");
        let got_at: std::time::SystemTime = row.get(2);
        assert_eq!(got_at, at, "TIMESTAMPTZ binary param round-trip");
        assert_eq!(got_at, control_at, "prepared ts equals simple literal");
    }
}

// =============================================================================
// 10b. TIMESTAMPTZ binds via the typed Timestamptz param — both ToSql shapes
//      real drivers send (std::time::SystemTime and chrono::DateTime<Utc>,
//      binary wire format) must round-trip exactly through the bind-direct /
//      AST / text routes, equal a simple-protocol literal control row, and be
//      usable as a prepared-SELECT predicate.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_timestamptz_systemtime_and_chrono_binds() {
    use std::time::{Duration, UNIX_EPOCH};

    let server = start_server().await;
    let client = connect(server.addr).await;
    client
        .simple_query(
            "CREATE TABLE ts_binds (id BIGINT NOT NULL PRIMARY KEY, at TIMESTAMPTZ NOT NULL)",
        )
        .await
        .expect("create ts_binds");

    // 2026-01-02 03:04:05.123456 UTC, microsecond precision.
    let micros = 1_767_323_045_123_456_i64;
    let at_sys = UNIX_EPOCH + Duration::from_micros(micros as u64);
    let at_chrono = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(micros).unwrap();

    let ins = client
        .prepare("INSERT INTO ts_binds (id, at) VALUES ($1, $2)")
        .await
        .expect("prepare insert");
    // SystemTime bind (tokio-postgres encodes binary timestamptz natively).
    client
        .execute(&ins, &[&1_i64, &at_sys])
        .await
        .expect("SystemTime bind insert");
    // chrono::DateTime<Utc> bind (with-chrono-0_4).
    client
        .execute(&ins, &[&2_i64, &at_chrono])
        .await
        .expect("chrono bind insert");
    // Control row via the simple protocol with the equivalent literal.
    client
        .simple_query("INSERT INTO ts_binds (id, at) VALUES (3, '2026-01-02 03:04:05.123456+00')")
        .await
        .expect("simple control insert");

    let rows = client
        .query("SELECT id, at FROM ts_binds ORDER BY id", &[])
        .await
        .expect("read back");
    assert_eq!(rows.len(), 3);
    let control_at: std::time::SystemTime = rows[2].get(1);
    for row in &rows[..2] {
        let got_sys: std::time::SystemTime = row.get(1);
        assert_eq!(got_sys, at_sys, "TIMESTAMPTZ bind round-trip");
        assert_eq!(got_sys, control_at, "prepared ts equals simple literal");
        let got_chrono: chrono::DateTime<chrono::Utc> = row.get(1);
        assert_eq!(got_chrono, at_chrono, "chrono read-back round-trip");
    }

    // Prepared SELECT with a timestamptz predicate param — exercises the
    // WHERE-clause inference (OID 1184) + AST/text substitution of the
    // typed param outside the bind-direct INSERT path.
    let sel = client
        .prepare("SELECT id FROM ts_binds WHERE at = $1 ORDER BY id")
        .await
        .expect("prepare select");
    let rows = client
        .query(&sel, &[&at_sys])
        .await
        .expect("timestamptz predicate query");
    let ids: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![1, 2, 3], "all three rows share the instant");
}

// =============================================================================
// 11. UUID-typed column: the bind-direct builder declines (UUID columns carry
//     validation outside the fast set), so the prepared INSERT must still work
//     via the AST/text fallback — with a native binary UUID param.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_insert_uuid_param_falls_back_and_round_trips() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    client
        .simple_query("CREATE TABLE keyed (id BIGINT NOT NULL PRIMARY KEY, u UUID NOT NULL)")
        .await
        .expect("create keyed");

    let stmt = client
        .prepare("INSERT INTO keyed (id, u) VALUES ($1, $2)")
        .await
        .expect("prepare");
    let ids: Vec<uuid::Uuid> = (0..3).map(|_| uuid::Uuid::new_v4()).collect();
    for (i, u) in ids.iter().enumerate() {
        client
            .execute(&stmt, &[&(i as i64), u])
            .await
            .unwrap_or_else(|e| panic!("uuid insert {i}: {e}"));
    }

    let rows = client
        .query("SELECT id, u FROM keyed ORDER BY id", &[])
        .await
        .expect("read back");
    assert_eq!(rows.len(), 3);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<_, i64>(0), i as i64);
        assert_eq!(row.get::<_, uuid::Uuid>(1), ids[i], "UUID round-trip");
    }
}

// =============================================================================
// 12. Multi-row parameterized template (one statement, many placeholder
//     tuples) — extended-protocol shape 2 with rows > 1.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_multi_row_param_template() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_events(&client).await;

    let stmt = client
        .prepare("INSERT INTO events (id, label, score) VALUES ($1, $2, $3), ($4, $5, $6)")
        .await
        .expect("prepare 2-row template");
    for batch in 0..10_i64 {
        let (a, b) = (batch * 2, batch * 2 + 1);
        let n = client
            .execute(
                &stmt,
                &[
                    &a,
                    &format!("m-{a}"),
                    &(a * 7),
                    &b,
                    &format!("m-{b}"),
                    &(b * 7),
                ],
            )
            .await
            .unwrap_or_else(|e| panic!("2-row execute {batch}: {e}"));
        assert_eq!(n, 2, "each Execute inserts both tuple rows");
    }

    let rows = client
        .query("SELECT id, label, score FROM events ORDER BY id", &[])
        .await
        .expect("read back");
    assert_eq!(rows.len(), 20);
    for (i, row) in rows.iter().enumerate() {
        let i = i as i64;
        assert_eq!(row.get::<_, i64>(0), i);
        assert_eq!(row.get::<_, &str>(1), format!("m-{i}"));
        assert_eq!(row.get::<_, i64>(2), i * 7);
    }
}

// =============================================================================
// 13. Decline shapes through the wire: ON CONFLICT keeps upsert semantics and
//     an explicit transaction keeps atomicity (bind-direct is auto-commit
//     only — a ROLLBACK must drop the prepared inserts).
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_insert_declines_still_correct() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_events(&client).await;

    // ON CONFLICT DO UPDATE: no bind-direct plan; upsert semantics preserved.
    let upsert = client
        .prepare(
            "INSERT INTO events (id, label, score) VALUES ($1, $2, $3) \
             ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label, score = EXCLUDED.score",
        )
        .await
        .expect("prepare upsert");
    client
        .execute(&upsert, &[&1_i64, &"first", &10_i64])
        .await
        .expect("insert");
    client
        .execute(&upsert, &[&1_i64, &"second", &20_i64])
        .await
        .expect("upsert");
    let row = client
        .query_one("SELECT label, score FROM events WHERE id = $1", &[&1_i64])
        .await
        .expect("read");
    assert_eq!(row.get::<_, &str>(0), "second");
    assert_eq!(row.get::<_, i64>(1), 20);

    // Explicit transaction: prepared inserts buffer in-tx; ROLLBACK drops them.
    let ins = client
        .prepare("INSERT INTO events (id, label, score) VALUES ($1, $2, $3)")
        .await
        .expect("prepare ins");
    client.simple_query("BEGIN").await.expect("begin");
    for i in 100..103_i64 {
        client
            .execute(&ins, &[&i, &"tx", &i])
            .await
            .unwrap_or_else(|e| panic!("in-tx insert {i}: {e}"));
    }
    client.simple_query("ROLLBACK").await.expect("rollback");
    let count = client
        .query_one("SELECT count(*) FROM events WHERE id >= $1", &[&100_i64])
        .await
        .expect("count");
    assert_eq!(
        count.get::<_, i64>(0),
        0,
        "ROLLBACK must drop in-tx prepared inserts (auto-commit-only gate)"
    );

    // And a COMMIT keeps them.
    client.simple_query("BEGIN").await.expect("begin");
    for i in 100..103_i64 {
        client
            .execute(&ins, &[&i, &"tx", &i])
            .await
            .unwrap_or_else(|e| panic!("in-tx insert {i}: {e}"));
    }
    client.simple_query("COMMIT").await.expect("commit");
    let count = client
        .query_one("SELECT count(*) FROM events WHERE id >= $1", &[&100_i64])
        .await
        .expect("count");
    assert_eq!(count.get::<_, i64>(0), 3);
}
