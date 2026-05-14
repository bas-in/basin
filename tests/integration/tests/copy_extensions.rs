//! `COPY` column-list and server-side file-path integration tests.
//!
//! Drive `tokio-postgres`'s simple-query and `copy_in` / `copy_out` paths
//! against a real in-process Basin router. Covers:
//!
//! - `COPY t (b, a) FROM STDIN` — column-list import in non-schema order.
//! - `COPY t (a, b) FROM STDIN` against a table with a defaulted column —
//!   the unlisted column gets its DEFAULT.
//! - `COPY t (...) FROM STDIN` rejecting NOT NULL no-default omissions.
//! - `COPY t (unknown) FROM STDIN` rejected with 42601.
//! - `COPY t (b, a) TO STDOUT` — column-list export ordering.
//! - `COPY t TO '/tmp/...'` / `COPY t FROM '/tmp/...'` — server-side file
//!   paths gated by `BASIN_COPY_ALLOW_FILE_PATHS=1` + `BASIN_COPY_PATH_ALLOWLIST`.

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
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(basin_catalog::InMemoryCatalog::new());
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

/// Env vars for file-path COPY are process-global. Tests that mutate them must
/// serialise through this lock to avoid stomping each other.
fn copy_path_env_lock() -> std::sync::MutexGuard<'static, ()> {
    use std::sync::{Mutex, OnceLock};
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_from_stdin_with_column_list() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (a BIGINT NOT NULL, b TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE");

    // Column list reverses the schema order: CSV body lines are (b, a).
    let sink = client
        .copy_in::<_, Bytes>("COPY t (b, a) FROM STDIN WITH (FORMAT CSV)")
        .await
        .expect("copy_in start");
    futures::pin_mut!(sink);

    sink.send(Bytes::from_static(b"hello,1\nworld,2\n"))
        .await
        .expect("send copy data");
    let n = sink.as_mut().finish().await.expect("finish");
    assert_eq!(n, 2);

    let row = client
        .query_one("SELECT b FROM t WHERE a = $1", &[&1i64])
        .await
        .expect("spot-check");
    let body: &str = row.get(0);
    assert_eq!(body, "hello");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_from_stdin_column_subset_uses_default() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query(
            "CREATE TABLE t (a BIGINT NOT NULL, b TEXT NOT NULL, c BIGINT NOT NULL DEFAULT 42)",
        )
        .await
        .expect("CREATE TABLE");

    // List only a + b; c is unlisted but has a default — engine fills it.
    let sink = client
        .copy_in::<_, Bytes>("COPY t (a, b) FROM STDIN WITH (FORMAT CSV)")
        .await
        .expect("copy_in start");
    futures::pin_mut!(sink);
    sink.send(Bytes::from_static(b"1,one\n2,two\n"))
        .await
        .expect("send");
    let n = sink.as_mut().finish().await.expect("finish");
    assert_eq!(n, 2);

    let row = client
        .query_one("SELECT c FROM t WHERE a = $1", &[&1i64])
        .await
        .expect("read c");
    let c: i64 = row.get(0);
    assert_eq!(c, 42, "unlisted column must take its DEFAULT");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_from_stdin_missing_required_column_rejects() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (a BIGINT NOT NULL, b TEXT NOT NULL, c BIGINT NOT NULL)")
        .await
        .expect("CREATE TABLE");

    // c is NOT NULL with no default and not in the column list — must be
    // rejected at COPY start with SQLSTATE 42601, before any CopyData
    // bytes flow.
    let err = client
        .copy_in::<_, Bytes>("COPY t (a, b) FROM STDIN WITH (FORMAT CSV)")
        .await
        .err()
        .expect("expected error");
    let dbe = err.as_db_error().expect("DbError");
    assert_eq!(dbe.code().code(), "42601");
    assert!(
        dbe.message().contains('"') && dbe.message().contains('c'),
        "error should name column \"c\": {}",
        dbe.message()
    );

    // Connection still usable.
    let row = client
        .query_one("SELECT count(*) FROM t", &[])
        .await
        .expect("count");
    let cnt: i64 = row.get(0);
    assert_eq!(cnt, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_from_stdin_unknown_column_rejects() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (a BIGINT NOT NULL, b TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE");

    let err = client
        .copy_in::<_, Bytes>("COPY t (a, nope) FROM STDIN WITH (FORMAT CSV)")
        .await
        .err()
        .expect("expected error");
    let dbe = err.as_db_error().expect("DbError");
    assert_eq!(dbe.code().code(), "42601");
    assert!(
        dbe.message().contains("nope"),
        "error should name unknown column: {}",
        dbe.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_to_stdout_with_column_list() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (a BIGINT NOT NULL, b TEXT NOT NULL, c BIGINT NOT NULL)")
        .await
        .expect("CREATE TABLE");
    for i in 0..3i64 {
        client
            .execute(
                "INSERT INTO t VALUES ($1, $2, $3)",
                &[&i, &format!("name-{i}"), &(i * 10)],
            )
            .await
            .expect("insert");
    }

    // List (b, a) → output rows have only those columns in that order.
    let stream = client
        .copy_out("COPY t (b, a) TO STDOUT WITH (FORMAT CSV)")
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
    for line in &rows {
        let parts: Vec<&str> = line.split(',').collect();
        assert_eq!(parts.len(), 2, "expected exactly two columns: {line:?}");
        // First column is b (name-N), second is a (N) — the LIST order, not
        // schema order. If the schema order leaked through, parts[0] would
        // parse as an i64.
        assert!(
            parts[0].starts_with("name-"),
            "first column should be name-N (b), got {line:?}"
        );
        assert!(
            parts[1].parse::<i64>().is_ok(),
            "second column should be a numeric id (a), got {line:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_to_file_path_writes_csv() {
    let _g = copy_path_env_lock();
    let server = start_server().await;
    let client = connect(server.addr).await;

    let scratch = TempDir::new().unwrap();
    std::env::set_var("BASIN_COPY_ALLOW_FILE_PATHS", "1");
    std::env::set_var(
        "BASIN_COPY_PATH_ALLOWLIST",
        scratch.path().to_str().unwrap(),
    );

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE");
    for i in 0..5i64 {
        client
            .execute("INSERT INTO t VALUES ($1, $2)", &[&i, &format!("name-{i}")])
            .await
            .expect("insert");
    }

    let path = scratch.path().join("out.csv");
    let path_str = path.to_str().unwrap().to_owned();
    client
        .simple_query(&format!(
            "COPY t TO '{}' WITH (FORMAT CSV, HEADER true)",
            path_str
        ))
        .await
        .expect("COPY TO file");

    let body = std::fs::read_to_string(&path).expect("read written file");
    let lines: Vec<&str> = body.split('\n').filter(|s| !s.is_empty()).collect();
    assert_eq!(lines.len(), 6, "1 header + 5 rows");
    assert_eq!(lines[0], "id,name");

    std::env::remove_var("BASIN_COPY_ALLOW_FILE_PATHS");
    std::env::remove_var("BASIN_COPY_PATH_ALLOWLIST");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_to_file_path_relative_rejected() {
    let _g = copy_path_env_lock();
    let server = start_server().await;
    let client = connect(server.addr).await;

    let scratch = TempDir::new().unwrap();
    std::env::set_var("BASIN_COPY_ALLOW_FILE_PATHS", "1");
    std::env::set_var(
        "BASIN_COPY_PATH_ALLOWLIST",
        scratch.path().to_str().unwrap(),
    );

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL)")
        .await
        .expect("CREATE TABLE");

    let err = client
        .simple_query("COPY t TO 'relative.csv' WITH (FORMAT CSV)")
        .await
        .expect_err("expected error");
    let dbe = err.as_db_error().expect("DbError");
    assert_eq!(dbe.code().code(), "42601");
    assert!(
        dbe.message().contains("absolute"),
        "msg should mention 'absolute': {}",
        dbe.message()
    );

    std::env::remove_var("BASIN_COPY_ALLOW_FILE_PATHS");
    std::env::remove_var("BASIN_COPY_PATH_ALLOWLIST");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_to_file_path_outside_allowlist_rejected() {
    let _g = copy_path_env_lock();
    let server = start_server().await;
    let client = connect(server.addr).await;

    // Primary gate unset → SQLSTATE 42501 (insufficient_privilege).
    std::env::remove_var("BASIN_COPY_ALLOW_FILE_PATHS");
    std::env::remove_var("BASIN_COPY_PATH_ALLOWLIST");

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL)")
        .await
        .expect("CREATE TABLE");

    let err = client
        .simple_query("COPY t TO '/tmp/should-be-denied.csv' WITH (FORMAT CSV)")
        .await
        .expect_err("expected error");
    let dbe = err.as_db_error().expect("DbError");
    // 42501 = insufficient_privilege (primary gate disabled)
    assert_eq!(dbe.code().code(), "42501");
    assert!(
        dbe.message().contains("disabled"),
        "msg should mention default-deny: {}",
        dbe.message()
    );

    // Enable the primary gate, but only with allowlist pointing elsewhere;
    // path outside allowlist → still rejected (42601).
    let scratch = TempDir::new().unwrap();
    std::env::set_var("BASIN_COPY_ALLOW_FILE_PATHS", "1");
    std::env::set_var(
        "BASIN_COPY_PATH_ALLOWLIST",
        scratch.path().to_str().unwrap(),
    );
    let err = client
        .simple_query("COPY t TO '/tmp/also-denied.csv' WITH (FORMAT CSV)")
        .await
        .expect_err("expected error 2");
    let dbe = err.as_db_error().expect("DbError");
    assert_eq!(dbe.code().code(), "42601");
    assert!(
        dbe.message().contains("outside"),
        "msg should mention 'outside': {}",
        dbe.message()
    );
    std::env::remove_var("BASIN_COPY_ALLOW_FILE_PATHS");
    std::env::remove_var("BASIN_COPY_PATH_ALLOWLIST");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_from_file_path_round_trips() {
    let _g = copy_path_env_lock();
    let server = start_server().await;
    let client = connect(server.addr).await;

    let scratch = TempDir::new().unwrap();
    std::env::set_var("BASIN_COPY_ALLOW_FILE_PATHS", "1");
    std::env::set_var(
        "BASIN_COPY_PATH_ALLOWLIST",
        scratch.path().to_str().unwrap(),
    );

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE");
    for i in 0..4i64 {
        client
            .execute("INSERT INTO t VALUES ($1, $2)", &[&i, &format!("name-{i}")])
            .await
            .expect("insert");
    }

    let path = scratch.path().join("rt.csv");
    let path_str = path.to_str().unwrap().to_owned();
    client
        .simple_query(&format!("COPY t TO '{}' WITH (FORMAT CSV)", path_str))
        .await
        .expect("COPY TO");

    // Drop the table content; replay COPY FROM the file we just wrote.
    client
        .simple_query("CREATE TABLE u (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE u");
    client
        .simple_query(&format!("COPY u FROM '{}' WITH (FORMAT CSV)", path_str))
        .await
        .expect("COPY FROM");

    let row = client
        .query_one("SELECT count(*) FROM u", &[])
        .await
        .expect("count u");
    let cnt: i64 = row.get(0);
    assert_eq!(cnt, 4);

    let row = client
        .query_one("SELECT name FROM u WHERE id = $1", &[&2i64])
        .await
        .expect("spot-check");
    let n: &str = row.get(0);
    assert_eq!(n, "name-2");

    std::env::remove_var("BASIN_COPY_ALLOW_FILE_PATHS");
    std::env::remove_var("BASIN_COPY_PATH_ALLOWLIST");
}
