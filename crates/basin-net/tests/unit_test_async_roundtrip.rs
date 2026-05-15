//! Unit test: async fire-and-forget queue persists a response row.
//!
//! Submit one request through the queue, step the runner once, then read
//! the response back from `_net_http_response`. Tests the full path:
//! queue → client → reqwest → store.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
use basin_net::{HttpClient, HttpRequest, RequestQueue, ResponseStore};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Tiny HTTP/1.1 server that returns 200 OK with body "ok" on any request.
async fn spawn_ok_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let _ = sock.read(&mut buf).await;
                let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nok";
                let _ = sock.write_all(resp).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_post_roundtrip_records_response_row() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let project = ProjectId::new();

    let addr = spawn_ok_server().await;
    let url = format!("http://{addr}/echo");

    let client = HttpClient::new();
    client.allow_host(&project, "127.0.0.1").await;
    let store = ResponseStore::new(engine.clone());
    let queue = RequestQueue::new(client, store.clone());

    let req = HttpRequest::post(&url, b"{\"x\":1}".to_vec(), "application/json");
    let id = queue.submit(&project, req);

    // Step the runner exactly once for determinism.
    let processed = queue.run_one().await.expect("queue had a request");
    assert_eq!(processed, id);

    let row = store
        .get(&project, id)
        .await
        .unwrap()
        .expect("response row missing");
    assert_eq!(row.id, id);
    assert_eq!(row.request_id, id);
    assert_eq!(row.status, 200);
    assert_eq!(row.body, "ok");
    assert!(row.error.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn allowlist_failure_is_recorded_as_terminal_error_row() {
    // pg_net stores DNS / TLS / blocked-host failures in the same response
    // table with an `error` column populated. Mirror that.
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let project = ProjectId::new();

    let client = HttpClient::new();
    // Note: deliberately not opting in to "127.0.0.1" so the allowlist fires.
    let store = ResponseStore::new(engine.clone());
    let queue = RequestQueue::new(client, store.clone());

    let req = HttpRequest::get("http://127.0.0.1/blocked");
    let id = queue.submit(&project, req);
    let processed = queue.run_one().await.expect("queue had a request");
    assert_eq!(processed, id);

    let row = store.get(&project, id).await.unwrap().expect("row missing");
    assert_eq!(row.status, 0);
    assert!(row
        .error
        .as_deref()
        .unwrap()
        .contains("host not on allowlist"));
}
