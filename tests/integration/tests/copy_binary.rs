//! `COPY … WITH (FORMAT BINARY)` end-to-end integration tests.
//!
//! Drives the PG binary COPY format (19-byte `PGCOPY` header, per-tuple
//! `i16` field count + `i32`-length-prefixed fields, `0xFFFF` trailer)
//! through `tokio-postgres`'s `copy_in` / `copy_out` against an in-process
//! Basin router. Asserts:
//!
//! 1. Binary COPY OUT of seeded data piped back through binary COPY IN to a
//!    second table reproduces identical rows (per-type coverage: int2/4/8,
//!    float4/8, bool, text with unicode, bytea, timestamp, uuid, jsonb, and
//!    a NULLs-everywhere row).
//! 2. A hand-rolled binary payload with a column list imports with
//!    reorder + DEFAULT fill, like INSERT with a column list.
//! 3. A mid-stream error (field-count mismatch) drains the remaining
//!    CopyData to CopyDone and surfaces one typed error; the connection
//!    stays usable — same guarantee as the CSV path.
//! 4. Binary COPY over a column type without a binary codec (vector) is
//!    rejected with a clean `0A000` naming the column, without desync.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

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
        .unwrap_or_else(|e| panic!("connect: {e}"));
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("conn driver: {e}");
        }
    });
    client
}

/// Collect a result set via `simple_query` as text cells (`None` = NULL),
/// so two tables can be compared through the engine's own canonical text
/// rendering.
async fn fetch_rows(client: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("query {sql:?}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(
                (0..r.len())
                    .map(|i| r.get(i).map(|s| s.to_string()))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

/// Stream `COPY <sql> TO STDOUT` and concatenate every CopyData chunk.
async fn copy_out_bytes(client: &Client, sql: &str) -> Vec<u8> {
    let stream = client.copy_out(sql).await.expect("copy_out start");
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk.expect("copy_out chunk"));
    }
    out
}

/// Send `payload` through `COPY … FROM STDIN` and return the reported row
/// count.
async fn copy_in_bytes(client: &Client, sql: &str, payload: Vec<u8>) -> u64 {
    let sink = client
        .copy_in::<_, Bytes>(sql)
        .await
        .expect("copy_in start");
    futures::pin_mut!(sink);
    sink.send(Bytes::from(payload)).await.expect("send");
    sink.as_mut().finish().await.expect("finish")
}

const WIDE_DDL_COLUMNS: &str = "id BIGINT NOT NULL, \
     i4 INT, \
     i2 SMALLINT, \
     f8 DOUBLE PRECISION, \
     f4 REAL, \
     flag BOOLEAN, \
     txt TEXT, \
     bin BYTEA, \
     ts TIMESTAMP, \
     uid UUID, \
     doc JSONB";

// ---------------------------------------------------------------------------
// Test 1: binary COPY OUT → binary COPY IN round-trips every supported type
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_binary_out_in_round_trip_all_types() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query(&format!("CREATE TABLE src ({WIDE_DDL_COLUMNS})"))
        .await
        .expect("CREATE TABLE src");
    client
        .simple_query(&format!("CREATE TABLE dst ({WIDE_DDL_COLUMNS})"))
        .await
        .expect("CREATE TABLE dst");

    client
        .simple_query(
            "INSERT INTO src VALUES \
             (1, -2147483648, 32767, 1.5, -0.25, true, 'héllo wörld 🚀 it''s', \
              '\\xdeadbeef00ff', '2024-03-14 15:09:26.535897', \
              'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', \
              '{\"k\": [1, 2, {\"nested\": true}], \"s\": \"v\"}'), \
             (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \
             (3, 42, -7, -1e10, 3.5, false, '', '\\x', \
              '1999-12-31 23:59:59', '00000000-0000-0000-0000-000000000000', \
              '[]')",
        )
        .await
        .expect("seed src");

    let payload = copy_out_bytes(&client, "COPY src TO STDOUT WITH (FORMAT BINARY)").await;
    assert!(
        payload.starts_with(b"PGCOPY\n\xFF\r\n\0"),
        "binary COPY OUT must start with the PGCOPY signature, got {:?}",
        &payload[..payload.len().min(11)]
    );
    assert_eq!(
        &payload[payload.len() - 2..],
        &[0xFFu8, 0xFF][..],
        "binary COPY OUT must end with the 0xFFFF trailer"
    );

    let n = copy_in_bytes(&client, "COPY dst FROM STDIN WITH (FORMAT BINARY)", payload).await;
    assert_eq!(n, 3, "COPY IN must report all three rows");

    let src_rows = fetch_rows(&client, "SELECT * FROM src ORDER BY id").await;
    let dst_rows = fetch_rows(&client, "SELECT * FROM dst ORDER BY id").await;
    assert_eq!(src_rows.len(), 3);
    assert_eq!(
        src_rows, dst_rows,
        "binary COPY OUT → COPY IN must reproduce identical rows"
    );
    // Spot-check a few cells so the comparison can't pass vacuously.
    assert_eq!(src_rows[0][6].as_deref(), Some("héllo wörld 🚀 it's"));
    assert_eq!(src_rows[1][6], None, "NULL text must stay NULL");
}

// ---------------------------------------------------------------------------
// Test 2: hand-rolled binary payload with column list → reorder + DEFAULT
// ---------------------------------------------------------------------------

/// Build a binary COPY payload from raw tuples (`None` = NULL field).
fn binary_payload(tuples: &[Vec<Option<Vec<u8>>>]) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(b"PGCOPY\n\xFF\r\n\0");
    p.extend_from_slice(&0u32.to_be_bytes()); // flags
    p.extend_from_slice(&0u32.to_be_bytes()); // extension length
    for tuple in tuples {
        p.extend_from_slice(&(tuple.len() as i16).to_be_bytes());
        for field in tuple {
            match field {
                None => p.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(bytes) => {
                    p.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    p.extend_from_slice(bytes);
                }
            }
        }
    }
    p.extend_from_slice(&(-1i16).to_be_bytes()); // trailer
    p
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_binary_column_subset_reorders_and_fills_default() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query(
            "CREATE TABLE t (a BIGINT NOT NULL, b TEXT NOT NULL, c BIGINT NOT NULL DEFAULT 99)",
        )
        .await
        .expect("CREATE TABLE");

    // Column list (b, a): text first, int second — reversed vs. table order.
    let payload = binary_payload(&[
        vec![
            Some("hello".as_bytes().to_vec()),
            Some(1i64.to_be_bytes().to_vec()),
        ],
        vec![
            Some("wörld🚀".as_bytes().to_vec()),
            Some(2i64.to_be_bytes().to_vec()),
        ],
    ]);
    let n = copy_in_bytes(
        &client,
        "COPY t (b, a) FROM STDIN WITH (FORMAT BINARY)",
        payload,
    )
    .await;
    assert_eq!(n, 2);

    let rows = fetch_rows(&client, "SELECT a, b, c FROM t ORDER BY a").await;
    assert_eq!(
        rows,
        vec![
            vec![
                Some("1".to_string()),
                Some("hello".to_string()),
                Some("99".to_string())
            ],
            vec![
                Some("2".to_string()),
                Some("wörld🚀".to_string()),
                Some("99".to_string())
            ],
        ],
        "column-subset binary COPY must reorder per the list and DEFAULT-fill c"
    );
}

// ---------------------------------------------------------------------------
// Test 3: mid-stream error drains to CopyDone; connection stays usable
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_binary_mid_stream_error_drains_to_copy_done() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL)")
        .await
        .expect("CREATE TABLE");

    let sink = client
        .copy_in::<_, Bytes>("COPY t FROM STDIN WITH (FORMAT BINARY)")
        .await
        .expect("copy_in start");
    futures::pin_mut!(sink);

    // Valid header, then a tuple claiming 3 fields against a 1-column table.
    let mut bad = Vec::new();
    bad.extend_from_slice(b"PGCOPY\n\xFF\r\n\0");
    bad.extend_from_slice(&0u32.to_be_bytes());
    bad.extend_from_slice(&0u32.to_be_bytes());
    bad.extend_from_slice(&3i16.to_be_bytes());
    sink.send(Bytes::from(bad)).await.expect("send bad tuple");
    // More garbage after the error — must be drained, not desync the wire.
    sink.send(Bytes::from_static(&[0u8; 64]))
        .await
        .expect("send post-error garbage");

    let err = sink
        .as_mut()
        .finish()
        .await
        .expect_err("field-count mismatch must fail the COPY");
    let dbe = err.as_db_error().expect("typed DbError after drain");
    assert!(
        dbe.message().contains("3 fields") && dbe.message().contains("expected 1"),
        "error should describe the field-count mismatch: {dbe:?}"
    );

    // Exactly the CSV-path guarantee: error only after CopyDone, connection
    // stays usable, nothing was inserted.
    let row = client
        .query_one("SELECT count(*) FROM t", &[])
        .await
        .expect("count after failed COPY");
    let cnt: i64 = row.get(0);
    assert_eq!(cnt, 0);
}

// ---------------------------------------------------------------------------
// Test 4: unsupported column type → clean 0A000 naming the column
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_binary_unsupported_column_type_rejected_cleanly() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE v (id BIGINT NOT NULL, emb VECTOR(3))")
        .await
        .expect("CREATE TABLE with vector column");
    client
        .simple_query("INSERT INTO v VALUES (1, '[1,2,3]')")
        .await
        .expect("seed");

    for sql in [
        "COPY v TO STDOUT WITH (FORMAT BINARY)",
        "COPY v FROM STDIN WITH (FORMAT BINARY)",
    ] {
        let err = client
            .simple_query(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("expected error for: {sql}"));
        let dbe = err
            .as_db_error()
            .unwrap_or_else(|| panic!("expected DbError for {sql:?}, got: {err}"));
        assert_eq!(
            dbe.code().code(),
            "0A000",
            "expected feature_not_supported for {sql:?}, got: {dbe:?}"
        );
        assert!(
            dbe.message().contains("emb"),
            "error must name the unsupported column: {dbe:?}"
        );
    }

    // CSV COPY of the same table still works, and the connection is intact.
    let csv = copy_out_bytes(&client, "COPY v (id) TO STDOUT WITH (FORMAT CSV)").await;
    assert_eq!(String::from_utf8(csv).unwrap().trim(), "1");
}
