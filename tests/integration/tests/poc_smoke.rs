//! End-to-end PoC smoke test.
//!
//! Boots a real Basin server bound to an ephemeral port, connects via
//! `tokio-postgres`, runs CREATE / INSERT / SELECT through the simple-query
//! protocol (the extended protocol is intentionally not implemented yet —
//! see basin-router's module docs), and asserts:
//!
//! 1. The query results round-trip correctly.
//! 2. Two tenants writing to a same-named table see only their own rows.
//! 3. Parquet files actually land on disk under `tenants/{tenant}/...`.
//!
//! This is the test that proves the four-layer stack composes.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::TenantId;
use basin_router::{ServerConfig, StaticTenantResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{NoTls, SimpleQueryMessage};
use walkdir::WalkDir;

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    data_dir: TempDir,
    alice: TenantId,
    bob: TenantId,
}

async fn start_server() -> TestServer {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> =
        Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let alice = TenantId::new();
    let bob = TenantId::new();
    let mut map = HashMap::new();
    map.insert("alice".to_owned(), alice);
    map.insert("bob".to_owned(), bob);
    let resolver = Arc::new(StaticTenantResolver::new(map));

    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        tenant_resolver: resolver,
        pool: None,
    })
    .await
    .expect("server failed to bind");

    TestServer {
        addr: running.local_addr,
        _shutdown: running.shutdown,
        _join: running.join,
        data_dir: dir,
        alice,
        bob,
    }
}

async fn connect(addr: SocketAddr, user: &str) -> tokio_postgres::Client {
    let conn_str = format!("host={} port={} user={user} password=ignored", addr.ip(), addr.port());
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("conn driver: {e}");
        }
    });
    client
}

fn rows_of(msgs: &[SimpleQueryMessage]) -> Vec<Vec<Option<String>>> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                let mut row = Vec::with_capacity(r.len());
                for i in 0..r.len() {
                    row.push(r.get(i).map(|s| s.to_string()));
                }
                Some(row)
            }
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poc_pgwire_end_to_end_create_insert_select() {
    let server = start_server().await;
    let alice = connect(server.addr, "alice").await;

    // CREATE TABLE
    let res = alice
        .simple_query("CREATE TABLE events (id BIGINT NOT NULL, body TEXT NOT NULL)")
        .await
        .unwrap();
    assert!(matches!(
        res.last(),
        Some(SimpleQueryMessage::CommandComplete(_))
    ));

    // INSERT three rows
    let res = alice
        .simple_query("INSERT INTO events VALUES (1, 'first'), (2, 'second'), (3, 'third')")
        .await
        .unwrap();
    assert!(matches!(
        res.last(),
        Some(SimpleQueryMessage::CommandComplete(_))
    ));

    // SELECT them back, ordered.
    let res = alice
        .simple_query("SELECT id, body FROM events ORDER BY id")
        .await
        .unwrap();
    let rows = rows_of(&res);
    assert_eq!(rows.len(), 3, "expected 3 rows, got {}", rows.len());
    assert_eq!(rows[0], vec![Some("1".into()), Some("first".into())]);
    assert_eq!(rows[1], vec![Some("2".into()), Some("second".into())]);
    assert_eq!(rows[2], vec![Some("3".into()), Some("third".into())]);

    // Parquet must actually be on disk under alice's prefix, and only there.
    let alice_prefix = format!("tenants/{}", server.alice);
    let bob_prefix = format!("tenants/{}", server.bob);
    let mut alice_files = 0;
    let mut bob_files = 0;
    for entry in WalkDir::new(server.data_dir.path()).into_iter().flatten() {
        if entry.file_type().is_file() {
            let p = entry.path().to_string_lossy().to_string();
            if p.contains(&alice_prefix) && p.ends_with(".parquet") {
                alice_files += 1;
            }
            if p.contains(&bob_prefix) && p.ends_with(".parquet") {
                bob_files += 1;
            }
        }
    }
    assert_eq!(alice_files, 1, "expected 1 parquet file for alice");
    assert_eq!(bob_files, 0, "bob should have no files yet");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poc_pgwire_two_tenants_isolated() {
    let server = start_server().await;
    let alice = connect(server.addr, "alice").await;
    let bob = connect(server.addr, "bob").await;

    // Both tenants create the *same-named* table with different rows.
    for c in [&alice, &bob] {
        c.simple_query("CREATE TABLE shared (who TEXT NOT NULL, id BIGINT NOT NULL)")
            .await
            .unwrap();
    }
    alice
        .simple_query("INSERT INTO shared VALUES ('alice', 1), ('alice', 2)")
        .await
        .unwrap();
    bob.simple_query("INSERT INTO shared VALUES ('bob', 10), ('bob', 20), ('bob', 30)")
        .await
        .unwrap();

    let arows = rows_of(
        &alice
            .simple_query("SELECT who, id FROM shared ORDER BY id")
            .await
            .unwrap(),
    );
    assert_eq!(arows.len(), 2);
    for r in &arows {
        assert_eq!(r[0].as_deref(), Some("alice"), "row leaked: {r:?}");
    }

    let brows = rows_of(
        &bob.simple_query("SELECT who, id FROM shared ORDER BY id")
            .await
            .unwrap(),
    );
    assert_eq!(brows.len(), 3);
    for r in &brows {
        assert_eq!(r[0].as_deref(), Some("bob"), "row leaked: {r:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poc_pgwire_unknown_user_rejected() {
    let server = start_server().await;
    let conn_str = format!(
        "host={} port={} user=eve password=x",
        server.addr.ip(),
        server.addr.port()
    );
    let res = tokio_postgres::connect(&conn_str, NoTls).await;
    assert!(res.is_err(), "unknown user should fail to connect");
}
