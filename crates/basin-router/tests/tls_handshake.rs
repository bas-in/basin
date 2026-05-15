//! End-to-end TLS negotiation test for the pgwire listener.
//!
//! Boots `basin-router` with a runtime-generated self-signed cert,
//! sends the 8-byte Postgres SSLRequest, and asserts the server replies
//! with `'S'` (TLS accepted). That proves the SSLRequest peek + acceptor
//! plumbing without dragging in a TLS client. Equivalent verification
//! that `tls = None` answers `'N'` is the second test below.

use std::collections::HashMap;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{run_until_bound, ServerConfig, StaticProjectResolver, TlsConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// SSLRequest packet bytes per the Postgres protocol: total length 8,
/// magic code 80877103 = 0x04D2162F.
const SSLREQUEST: [u8; 8] = [0x00, 0x00, 0x00, 0x08, 0x04, 0xD2, 0x16, 0x2F];

fn self_signed_pem() -> (Vec<u8>, Vec<u8>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    (
        cert.cert.pem().into_bytes(),
        cert.key_pair.serialize_pem().into_bytes(),
    )
}

async fn spawn_router(
    tls: Option<Arc<TlsConfig>>,
) -> (std::net::SocketAddr, TempDir, basin_router::RunningServer) {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let mut map = HashMap::new();
    map.insert("alice".to_owned(), ProjectId::new());
    let resolver = Arc::new(StaticProjectResolver::new(map));

    let running = run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        project_resolver: resolver,
        pool: None,
        shard_endpoints: None,
        tls,
    })
    .await
    .expect("server failed to bind");

    (running.local_addr, dir, running)
}

#[tokio::test]
async fn ssl_request_with_tls_configured_returns_s() {
    let (cert_pem, key_pem) = self_signed_pem();
    let tls = Some(Arc::new(TlsConfig { cert_pem, key_pem }));
    let (addr, _dir, running) = spawn_router(tls).await;

    let mut sock = TcpStream::connect(addr).await.expect("tcp connect");
    sock.write_all(&SSLREQUEST).await.expect("send SSLRequest");
    sock.flush().await.unwrap();

    let mut byte = [0u8; 1];
    sock.read_exact(&mut byte)
        .await
        .expect("read SSL response byte");
    assert_eq!(
        byte[0], b'S',
        "expected 'S' (TLS accepted), got {:?}",
        byte[0] as char
    );

    drop(sock);
    let _ = running.shutdown.send(());
    let _ = running.join.await;
}

#[tokio::test]
async fn ssl_request_without_tls_returns_n() {
    let (addr, _dir, running) = spawn_router(None).await;

    let mut sock = TcpStream::connect(addr).await.expect("tcp connect");
    sock.write_all(&SSLREQUEST).await.expect("send SSLRequest");
    sock.flush().await.unwrap();

    let mut byte = [0u8; 1];
    sock.read_exact(&mut byte)
        .await
        .expect("read SSL response byte");
    assert_eq!(
        byte[0], b'N',
        "expected 'N' (TLS refused), got {:?}",
        byte[0] as char
    );

    drop(sock);
    let _ = running.shutdown.send(());
    let _ = running.join.await;
}
