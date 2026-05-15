//! Additional `COPY` integration tests for the v0.2 extended option set.
//!
//! Covers:
//!
//! - `COPY t FROM STDIN WITH (FORMAT CSV, DELIMITER '|')` — pipe-delimited import.
//! - `COPY t FROM STDIN WITH (FORMAT CSV, DELIMITER '|', HEADER true)` — pipe + header.
//! - `COPY t FROM STDIN WITH (FORMAT CSV, NULL '\N')` — custom NULL string.
//! - `COPY t FROM STDIN WITH (FORMAT CSV, QUOTE '"', ESCAPE '\')` — explicit quote/escape.
//! - `COPY t TO STDOUT WITH (FORMAT CSV, DELIMITER '|')` — pipe-delimited export.
//! - `COPY t TO STDOUT WITH (FORMAT CSV, HEADER true, DELIMITER ',')` — all common flags.
//! - `COPY (SELECT id, name FROM t WHERE id > 2) TO STDOUT WITH (FORMAT CSV)` — query source.
//! - `COPY (SELECT * FROM t) TO STDOUT WITH (FORMAT CSV, HEADER true)` — query source + header.
//! - `COPY t FROM PROGRAM 'cmd'` — rejected with SQLSTATE 42501.
//! - `COPY t FROM STDIN WITH (FORMAT BINARY)` — BINARY still rejected with 42601.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::NoTls;

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

    let alice = ProjectId::new();
    let mut map = HashMap::new();
    map.insert("alice".to_owned(), alice);
    let resolver = Arc::new(StaticProjectResolver::new(map));

    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        project_resolver: resolver,
        pool: None,
        shard_endpoints: None,
        tls: None,
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

async fn connect(addr: SocketAddr) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user=alice password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect: {e}"));
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("conn driver: {e}");
        }
    });
    client
}

// ---------------------------------------------------------------------------
// Test 1: pipe-delimited COPY FROM STDIN
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_from_stdin_pipe_delimiter() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE");

    let sink = client
        .copy_in::<_, Bytes>("COPY t FROM STDIN WITH (FORMAT CSV, DELIMITER '|')")
        .await
        .expect("copy_in start");
    futures::pin_mut!(sink);

    sink.send(Bytes::from_static(b"1|alice\n2|bob\n3|charlie\n"))
        .await
        .expect("send");
    let n = sink.as_mut().finish().await.expect("finish");
    assert_eq!(n, 3, "3 rows imported");

    let row = client
        .query_one("SELECT name FROM t WHERE id = $1", &[&2i64])
        .await
        .expect("query");
    let name: &str = row.get(0);
    assert_eq!(name, "bob");
}

// ---------------------------------------------------------------------------
// Test 2: pipe-delimited COPY FROM STDIN with HEADER
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_from_stdin_pipe_delimiter_with_header() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL, val TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE");

    let sink = client
        .copy_in::<_, Bytes>("COPY t FROM STDIN WITH (FORMAT CSV, DELIMITER '|', HEADER true)")
        .await
        .expect("copy_in start");
    futures::pin_mut!(sink);

    sink.send(Bytes::from_static(b"id|val\n10|foo\n20|bar\n"))
        .await
        .expect("send");
    let n = sink.as_mut().finish().await.expect("finish");
    assert_eq!(n, 2, "2 data rows; header excluded from count");

    let row = client
        .query_one("SELECT count(*) FROM t", &[])
        .await
        .expect("count");
    let cnt: i64 = row.get(0);
    assert_eq!(cnt, 2);
}

// ---------------------------------------------------------------------------
// Test 3: custom NULL string import
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_from_stdin_custom_null_string() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL, label TEXT)")
        .await
        .expect("CREATE TABLE");

    // \N is the PostgreSQL default NULL representation in text mode.
    let sink = client
        .copy_in::<_, Bytes>(r"COPY t FROM STDIN WITH (FORMAT CSV, NULL '\N')")
        .await
        .expect("copy_in start");
    futures::pin_mut!(sink);

    sink.send(Bytes::from_static(b"1,hello\n2,\\N\n3,world\n"))
        .await
        .expect("send");
    let n = sink.as_mut().finish().await.expect("finish");
    assert_eq!(n, 3);

    let row = client
        .query_one("SELECT label FROM t WHERE id = $1", &[&2i64])
        .await
        .expect("query");
    let label: Option<&str> = row.get(0);
    assert!(label.is_none(), "row 2 label should be NULL, got: {label:?}");
}

// ---------------------------------------------------------------------------
// Test 4: explicit QUOTE + ESCAPE options are parsed and accepted
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_from_stdin_explicit_quote_escape() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL, body TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE");

    // The default quote char is " anyway — confirm the option is parsed fine.
    let sink = client
        .copy_in::<_, Bytes>("COPY t FROM STDIN WITH (FORMAT CSV, QUOTE '\"', ESCAPE '\\')")
        .await
        .expect("copy_in start");
    futures::pin_mut!(sink);

    sink.send(Bytes::from_static(b"1,plain\n2,\"with,comma\"\n"))
        .await
        .expect("send");
    let n = sink.as_mut().finish().await.expect("finish");
    assert_eq!(n, 2);

    let row = client
        .query_one("SELECT body FROM t WHERE id = $1", &[&2i64])
        .await
        .expect("query");
    let body: &str = row.get(0);
    assert_eq!(body, "with,comma");
}

// ---------------------------------------------------------------------------
// Test 5: pipe-delimited COPY TO STDOUT
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_to_stdout_pipe_delimiter() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE");
    for i in 0..3i64 {
        client
            .execute(
                "INSERT INTO t VALUES ($1, $2)",
                &[&i, &format!("name-{i}")],
            )
            .await
            .expect("insert");
    }

    let stream = client
        .copy_out("COPY t TO STDOUT WITH (FORMAT CSV, DELIMITER '|')")
        .await
        .expect("copy_out start");
    futures::pin_mut!(stream);
    let mut buf = Vec::<u8>::new();
    while let Some(c) = stream.next().await {
        buf.extend_from_slice(&c.expect("chunk"));
    }

    let text = std::str::from_utf8(&buf).expect("utf8");
    let rows: Vec<&str> = text.split('\n').filter(|s| !s.is_empty()).collect();
    assert_eq!(rows.len(), 3);
    // Each line should be pipe-separated.
    for line in &rows {
        assert!(line.contains('|'), "expected pipe in: {line:?}");
        assert!(!line.contains(','), "unexpected comma in: {line:?}");
    }
}

// ---------------------------------------------------------------------------
// Test 6: COPY TO STDOUT with HEADER and DELIMITER (all common flags)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_to_stdout_header_and_delimiter() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL, val TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE");
    for i in 0..2i64 {
        client
            .execute("INSERT INTO t VALUES ($1, $2)", &[&i, &format!("val-{i}")])
            .await
            .expect("insert");
    }

    let stream = client
        .copy_out("COPY t TO STDOUT WITH (FORMAT CSV, HEADER true, DELIMITER ',')")
        .await
        .expect("copy_out start");
    futures::pin_mut!(stream);
    let mut buf = Vec::<u8>::new();
    while let Some(c) = stream.next().await {
        buf.extend_from_slice(&c.expect("chunk"));
    }

    let text = std::str::from_utf8(&buf).expect("utf8");
    let lines: Vec<&str> = text.split('\n').filter(|s| !s.is_empty()).collect();
    // 1 header + 2 data rows
    assert_eq!(lines.len(), 3, "expected 1 header + 2 data rows");
    assert_eq!(lines[0], "id,val", "first line should be header");
}

// ---------------------------------------------------------------------------
// Test 7: COPY (SELECT …) TO STDOUT — query-source COPY
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_query_to_stdout() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE");
    for i in 0..5i64 {
        client
            .execute(
                "INSERT INTO t VALUES ($1, $2)",
                &[&i, &format!("name-{i}")],
            )
            .await
            .expect("insert");
    }

    // Only export rows where id > 2 (should give id=3 and id=4).
    let stream = client
        .copy_out("COPY (SELECT id, name FROM t WHERE id > 2) TO STDOUT WITH (FORMAT CSV)")
        .await
        .expect("copy_out start");
    futures::pin_mut!(stream);
    let mut buf = Vec::<u8>::new();
    while let Some(c) = stream.next().await {
        buf.extend_from_slice(&c.expect("chunk"));
    }

    let text = std::str::from_utf8(&buf).expect("utf8");
    let rows: Vec<&str> = text.split('\n').filter(|s| !s.is_empty()).collect();
    assert_eq!(rows.len(), 2, "expected 2 rows (id=3, id=4)");

    // Ensure we got the right ids.
    let mut ids: Vec<i64> = rows
        .iter()
        .map(|line| {
            line.split(',')
                .next()
                .expect("id col")
                .parse::<i64>()
                .expect("parse id")
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![3, 4]);
}

// ---------------------------------------------------------------------------
// Test 8: COPY (SELECT *) TO STDOUT WITH (FORMAT CSV, HEADER true)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_query_to_stdout_with_header() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL, label TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE");
    for i in 0..3i64 {
        client
            .execute(
                "INSERT INTO t VALUES ($1, $2)",
                &[&i, &format!("lbl-{i}")],
            )
            .await
            .expect("insert");
    }

    let stream = client
        .copy_out("COPY (SELECT * FROM t) TO STDOUT WITH (FORMAT CSV, HEADER true)")
        .await
        .expect("copy_out start");
    futures::pin_mut!(stream);
    let mut buf = Vec::<u8>::new();
    while let Some(c) = stream.next().await {
        buf.extend_from_slice(&c.expect("chunk"));
    }

    let text = std::str::from_utf8(&buf).expect("utf8");
    let lines: Vec<&str> = text.split('\n').filter(|s| !s.is_empty()).collect();
    assert_eq!(lines.len(), 4, "1 header + 3 data rows");
    // Header must name both columns.
    assert!(
        lines[0].contains("id") && lines[0].contains("label"),
        "header should name columns: {}",
        lines[0]
    );
}

// ---------------------------------------------------------------------------
// Test 9: COPY FROM PROGRAM rejected with SQLSTATE 42501
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_from_program_rejected() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL)")
        .await
        .expect("CREATE TABLE");

    let err = client
        .simple_query("COPY t FROM PROGRAM 'echo 1'")
        .await
        .expect_err("expected error");
    let dbe = err.as_db_error().expect("DbError");
    assert_eq!(
        dbe.code().code(),
        "42501",
        "expected SQLSTATE 42501 (insufficient_privilege), got: {:?}",
        dbe
    );
    assert!(
        dbe.message().to_ascii_lowercase().contains("program"),
        "error message should mention PROGRAM: {}",
        dbe.message()
    );

    // Connection must remain usable.
    let row = client
        .query_one("SELECT count(*) FROM t", &[])
        .await
        .expect("count after rejection");
    let cnt: i64 = row.get(0);
    assert_eq!(cnt, 0);
}

// ---------------------------------------------------------------------------
// Test 10: COPY TO PROGRAM rejected with SQLSTATE 42501
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_to_program_rejected() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL)")
        .await
        .expect("CREATE TABLE");

    let err = client
        .simple_query("COPY t TO PROGRAM 'cat'")
        .await
        .expect_err("expected error");
    let dbe = err.as_db_error().expect("DbError");
    assert_eq!(
        dbe.code().code(),
        "42501",
        "expected SQLSTATE 42501 (insufficient_privilege), got: {:?}",
        dbe
    );
    assert!(
        dbe.message().to_ascii_lowercase().contains("program"),
        "error message should mention PROGRAM: {}",
        dbe.message()
    );

    // Connection must remain usable.
    let row = client
        .query_one("SELECT count(*) FROM t", &[])
        .await
        .expect("count after rejection");
    let cnt: i64 = row.get(0);
    assert_eq!(cnt, 0);
}

// ---------------------------------------------------------------------------
// Test 11: BINARY format still rejected
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_binary_format_rejected() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL)")
        .await
        .expect("CREATE TABLE");

    let err = client
        .simple_query("COPY t FROM STDIN WITH (FORMAT BINARY)")
        .await
        .expect_err("expected error");
    let dbe = err.as_db_error().expect("DbError");
    assert_eq!(
        dbe.code().code(),
        "42601",
        "expected SQLSTATE 42601 for BINARY, got: {:?}",
        dbe
    );

    // Connection still usable.
    let row = client
        .query_one("SELECT count(*) FROM t", &[])
        .await
        .expect("count");
    let cnt: i64 = row.get(0);
    assert_eq!(cnt, 0);
}

// ---------------------------------------------------------------------------
// Test 12: column-list + delimiter round-trip (compound options)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_from_stdin_column_list_and_delimiter() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (a BIGINT NOT NULL, b TEXT NOT NULL, c BIGINT NOT NULL DEFAULT 99)")
        .await
        .expect("CREATE TABLE");

    // Import only (b, a) with pipe delimiter; c should default to 99.
    let sink = client
        .copy_in::<_, Bytes>("COPY t (b, a) FROM STDIN WITH (FORMAT CSV, DELIMITER '|')")
        .await
        .expect("copy_in start");
    futures::pin_mut!(sink);

    sink.send(Bytes::from_static(b"hello|1\nworld|2\n"))
        .await
        .expect("send");
    let n = sink.as_mut().finish().await.expect("finish");
    assert_eq!(n, 2);

    let row = client
        .query_one("SELECT b, c FROM t WHERE a = $1", &[&1i64])
        .await
        .expect("query");
    let b: &str = row.get(0);
    let c: i64 = row.get(1);
    assert_eq!(b, "hello");
    assert_eq!(c, 99, "c should use its DEFAULT");
}
