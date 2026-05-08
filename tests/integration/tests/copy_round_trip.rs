//! `COPY FROM STDIN` / `COPY TO STDOUT` end-to-end integration tests.
//!
//! Drive `tokio-postgres`'s `Client::copy_in` and `Client::copy_out` against
//! a real in-process Basin router (LocalFileSystem storage, no S3, no PG
//! dependency). Asserts that:
//!
//! 1. CSV import round-trips: 1000 rows in via `copy_in` → `SELECT count(*)`
//!    sees 1000.
//! 2. CSV export round-trips: 100 rows inserted the normal way → `copy_out`
//!    streams CSV bytes that parse back to 100 rows.
//! 3. The COPY parser rejects unsupported variants (DELIMITER, BINARY,
//!    column lists) cleanly without desyncing the connection.
//! 4. `COPY ... WITH (HEADER true)` round-trips.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::TenantId;
use basin_router::{ServerConfig, StaticTenantResolver};
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
    let catalog: Arc<dyn basin_catalog::Catalog> =
        Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let alice = TenantId::new();
    let mut map = HashMap::new();
    map.insert("alice".to_owned(), alice);
    let resolver = Arc::new(StaticTenantResolver::new(map));

    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        tenant_resolver: resolver,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_from_stdin_round_trip() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE events (id BIGINT NOT NULL, body TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE");

    // Push 1000 rows of CSV through copy_in.
    let sink = client
        .copy_in::<_, Bytes>("COPY events FROM STDIN WITH (FORMAT CSV)")
        .await
        .expect("copy_in start");
    futures::pin_mut!(sink);

    // Build the full payload up-front; tokio-postgres' CopyInSink will
    // chunk it onto the wire as needed.
    let mut payload = String::new();
    for i in 0..1000 {
        payload.push_str(&format!("{i},body-{i}\n"));
    }
    sink.send(Bytes::from(payload.into_bytes()))
        .await
        .expect("send copy data");
    let written = sink
        .as_mut()
        .finish()
        .await
        .expect("finish copy_in");
    assert_eq!(written, 1000, "CommandComplete tag count");

    // Confirm via SELECT count(*).
    let row = client
        .query_one("SELECT count(*) FROM events", &[])
        .await
        .expect("count");
    let n: i64 = row.get(0);
    assert_eq!(n, 1000, "table row count after COPY FROM STDIN");

    // Spot-check one specific row: id=42 / body='body-42'.
    let row = client
        .query_one(
            "SELECT body FROM events WHERE id = $1",
            &[&42i64],
        )
        .await
        .expect("spot check");
    let body: &str = row.get(0);
    assert_eq!(body, "body-42");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_to_stdout_round_trip() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE items (id BIGINT NOT NULL, label TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE");

    // Insert 100 rows the normal way.
    for i in 0..100i64 {
        client
            .execute(
                "INSERT INTO items VALUES ($1, $2)",
                &[&i, &format!("label-{i}")],
            )
            .await
            .expect("insert");
    }

    // Stream them out via copy_out.
    let stream = client
        .copy_out("COPY items TO STDOUT WITH (FORMAT CSV)")
        .await
        .expect("copy_out start");
    futures::pin_mut!(stream);

    let mut accumulated = Vec::<u8>::new();
    while let Some(chunk) = stream.next().await {
        accumulated.extend_from_slice(&chunk.expect("copy_out chunk"));
    }

    // Parse CSV back. We don't depend on the `csv` crate — a tiny ad-hoc
    // splitter is enough since we know the producer side never quotes.
    let text = std::str::from_utf8(&accumulated).expect("UTF-8 CSV");
    let rows: Vec<&str> = text.split('\n').filter(|s| !s.is_empty()).collect();
    assert_eq!(rows.len(), 100, "row count back");

    // Confirm contents are correct (order may differ after engine query).
    let mut seen = std::collections::HashMap::<i64, String>::new();
    for line in &rows {
        let mut parts = line.splitn(2, ',');
        let id: i64 = parts
            .next()
            .expect("id col")
            .parse()
            .expect("id parse");
        let label = parts.next().expect("label col").to_owned();
        seen.insert(id, label);
    }
    for i in 0..100i64 {
        let want = format!("label-{i}");
        assert_eq!(seen.get(&i), Some(&want), "row {i} mismatch");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_with_header_round_trips_skips_first_line() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE");

    let sink = client
        .copy_in::<_, Bytes>("COPY t FROM STDIN WITH (FORMAT CSV, HEADER true)")
        .await
        .expect("copy_in start");
    futures::pin_mut!(sink);

    let payload = "id,name\n1,alice\n2,bob\n3,charlie\n";
    sink.send(Bytes::from_static(payload.as_bytes()))
        .await
        .expect("send copy data");
    let n = sink.as_mut().finish().await.expect("finish");
    assert_eq!(n, 3, "header should not count toward COPY tag");

    let row = client
        .query_one("SELECT count(*) FROM t", &[])
        .await
        .expect("count");
    let cnt: i64 = row.get(0);
    assert_eq!(cnt, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_rejects_unsupported_variants_without_desync() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL)")
        .await
        .expect("CREATE TABLE");

    // Each rejected COPY should respond with a syntax error and the
    // connection must remain usable for further queries. We don't check
    // the exact wording — tokio-postgres' top-level `Display` only emits
    // "db error: <SQLSTATE> ..."; assert the SQLSTATE 42601.
    for sql in [
        "COPY t (id) FROM STDIN",
        "COPY t FROM STDIN WITH (FORMAT BINARY)",
        "COPY t FROM STDIN WITH (DELIMITER '|')",
        "COPY t FROM '/etc/passwd'",
    ] {
        let err = client
            .simple_query(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("expected error for: {sql}"));
        let dbe = err.as_db_error().unwrap_or_else(|| {
            panic!("expected DbError for {sql:?}, got: {err}")
        });
        assert_eq!(
            dbe.code().code(),
            "42601",
            "expected SQLSTATE 42601 for {sql:?}, got: {dbe:?}"
        );
    }

    // Connection must still be usable (no protocol desync).
    let row = client
        .query_one("SELECT count(*) FROM t", &[])
        .await
        .expect("count after rejected COPYs");
    let cnt: i64 = row.get(0);
    assert_eq!(cnt, 0);
}
