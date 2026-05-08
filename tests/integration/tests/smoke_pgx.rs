//! Boots Basin in-process and shells out to a tiny `pgx/v5` smoke script.
//!
//! Symmetric with `smoke_asyncpg`. Skips cleanly if `go` isn't installed; a
//! real failure means the wire protocol regressed against pgx, which is the
//! gate this test guards.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use basin_common::TenantId;
use basin_router::{ServerConfig, StaticTenantResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::process::Command;

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

    let mut map = HashMap::new();
    map.insert("alice".to_owned(), TenantId::new());
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

async fn binary_exists(name: &str) -> bool {
    Command::new("which")
        .arg(name)
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn smoke_pgx() {
    if !binary_exists("go").await {
        println!("[smoke_pgx] SKIP: go not on PATH");
        return;
    }

    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let go_dir = manifest.join("go");
    let script = go_dir.join("smoke.go");
    assert!(script.exists(), "smoke.go missing at {script:?}");

    // `go run` requires a populated go.sum. Run `go mod tidy` once to
    // resolve versions; if that fails (offline / GOPROXY blocked), skip
    // rather than misreport a wire-protocol failure.
    let tidy = Command::new("go")
        .args(["mod", "tidy"])
        .current_dir(&go_dir)
        .output()
        .await;
    match tidy {
        Ok(o) if o.status.success() => {}
        Ok(o) => {
            println!(
                "[smoke_pgx] SKIP: `go mod tidy` failed: status={} stderr={}",
                o.status,
                String::from_utf8_lossy(&o.stderr)
            );
            return;
        }
        Err(e) => {
            println!("[smoke_pgx] SKIP: `go mod tidy` spawn failed: {e}");
            return;
        }
    }

    let server = start_server().await;
    let url = format!(
        "postgres://alice:ignored@{}:{}/basin",
        server.addr.ip(),
        server.addr.port()
    );

    let out = Command::new("go")
        .args(["run", "./smoke.go", "--url", &url])
        .current_dir(&go_dir)
        .output()
        .await
        .expect("spawn go run failed");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    println!("--- pgx smoke stdout ---\n{stdout}");
    if !stderr.trim().is_empty() {
        println!("--- pgx smoke stderr ---\n{stderr}");
    }

    assert!(
        out.status.success(),
        "pgx smoke exited {:?}; stdout above",
        out.status.code()
    );
    assert!(stdout.contains("PASS"), "pgx smoke did not print PASS");
}
