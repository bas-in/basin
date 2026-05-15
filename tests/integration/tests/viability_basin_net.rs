//! Viability test: `http`-extension + `pg_net`-style outbound HTTP surface.
//!
//! Card: `viability_basin_net`
//! Bar: `roundtrip_passed && allowlist_blocks_unknown_host && async_response_recorded`
//!
//! What this proves:
//!
//! 1. **Sync round-trip**: `HttpClient::http_get` against an allowlisted
//!    127.0.0.1 mock server returns status 200 and body "ok". This is the
//!    `SELECT * FROM http_get('http://...')` shape, just expressed through
//!    the v0.1 Rust API surface (same staging pattern as `basin-cron`'s
//!    `cron.schedule(...)` viability card).
//! 2. **Allowlist blocks unknown hosts**: a request to a host that was not
//!    explicitly opted in returns the documented `host not on allowlist`
//!    error — the SSRF-prevention boundary.
//! 3. **Async round-trip**: `RequestQueue::submit` returns a `RequestId`
//!    synchronously, the runner dispatches the request, and a row appears
//!    in `_net_http_response` with the matching id, status 200, body "ok",
//!    and no error. This is the `SELECT net.http_post(...) AS request_id`
//!    + `SELECT * FROM _net_http_response WHERE id = $1` shape.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_net::{HttpClient, HttpRequest, RequestQueue, ResponseStore};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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
async fn viability_basin_net() {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let project = ProjectId::new();
    let addr = spawn_ok_server().await;

    let client = HttpClient::new();
    // Equivalent to `INSERT INTO _net_allowed_hosts VALUES ('127.0.0.1')`.
    client.allow_host(&project, "127.0.0.1").await;

    // -- 1. Sync round-trip: `SELECT * FROM http_get('http://addr/ok')` --
    let url = format!("http://{addr}/ok");
    let resp = client
        .http_get(&project, &url)
        .await
        .expect("sync GET should succeed against allowlisted mock");
    let roundtrip_passed = resp.status == 200 && resp.body_as_text() == "ok";

    // -- 2. Allowlist blocks unknown hosts --
    let denied = client
        .http_get(&project, "http://192.0.2.1/secret")
        .await
        .err()
        .map(|e| format!("{e}"))
        .unwrap_or_default();
    let allowlist_blocks_unknown_host = denied.contains("host not on allowlist");

    // -- 3. Async round-trip via RequestQueue + ResponseStore --
    let store = ResponseStore::new(engine.clone());
    let queue = RequestQueue::new(client.clone(), store.clone());
    let req = HttpRequest::post(&url, b"{\"k\":1}".to_vec(), "application/json")
        .with_header("authorization", "Bearer test");
    let id = queue.submit(&project, req);
    // Step the runner deterministically.
    queue.run_one().await;
    let row = store
        .get(&project, id)
        .await
        .expect("get response row")
        .expect("row not yet present");
    let async_response_recorded =
        row.id == id && row.request_id == id && row.status == 200 && row.body == "ok";

    let pass = roundtrip_passed && allowlist_blocks_unknown_host && async_response_recorded;
    println!(
        "[VIABILITY basin_net] roundtrip_passed={roundtrip_passed} \
         allowlist_blocks_unknown_host={allowlist_blocks_unknown_host} \
         async_response_recorded={async_response_recorded} {}",
        if pass { "PASS" } else { "FAIL" }
    );

    let primary = PrimaryMetric {
        label: "checks_passed".into(),
        value: [
            roundtrip_passed,
            allowlist_blocks_unknown_host,
            async_response_recorded,
        ]
        .iter()
        .filter(|b| **b)
        .count() as f64,
        unit: "checks".into(),
        bar: BarOp::eq(3.0),
    };

    report_viability(
        "basin_net",
        "Outbound HTTP surface (Postgres `http` + Supabase `pg_net`)",
        "Sync GET round-trips, allowlist gate denies unknown hosts, async POST persists response row.",
        pass,
        primary,
        json!({
            "roundtrip_passed": roundtrip_passed,
            "allowlist_blocks_unknown_host": allowlist_blocks_unknown_host,
            "async_response_recorded": async_response_recorded,
            "sync_status": resp.status,
            "sync_body": resp.body_as_text(),
            "async_request_id": id.to_string(),
            "async_status": row.status,
        }),
    );

    assert!(pass);
}
