//! Boots Basin in-process and shells out to a tiny `asyncpg` smoke script.
//!
//! Done = `cargo test ... --test smoke_asyncpg` either passes (subprocess
//! exited 0 with `PASS` on stdout) or skips cleanly when `python3` /
//! `asyncpg` aren't installed. Real failure means the wire protocol regressed
//! against a real Python driver, which is the gate this test guards.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
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
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let mut map = HashMap::new();
    map.insert("alice".to_owned(), ProjectId::new());
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

/// `which`-style probe. Empty status check so we don't spawn the real script
/// only to discover the interpreter is missing.
async fn binary_exists(name: &str) -> bool {
    Command::new("which")
        .arg(name)
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

async fn asyncpg_importable(python: &str) -> bool {
    Command::new(python)
        .args(["-c", "import asyncpg"])
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn smoke_asyncpg() {
    if !binary_exists("python3").await {
        println!("[smoke_asyncpg] SKIP: python3 not on PATH");
        return;
    }

    if !asyncpg_importable("python3").await {
        // Spec: try `pip install --user asyncpg` ONCE; if that fails, skip.
        println!("[smoke_asyncpg] asyncpg not importable; attempting pip install --user asyncpg");
        let installed = Command::new("python3")
            .args(["-m", "pip", "install", "--user", "--quiet", "asyncpg"])
            .output()
            .await;
        match installed {
            Ok(o) if o.status.success() => {
                if !asyncpg_importable("python3").await {
                    println!("[smoke_asyncpg] SKIP: asyncpg still not importable after install");
                    return;
                }
            }
            Ok(o) => {
                println!(
                    "[smoke_asyncpg] SKIP: pip install asyncpg failed: status={} stderr={}",
                    o.status,
                    String::from_utf8_lossy(&o.stderr)
                );
                return;
            }
            Err(e) => {
                println!("[smoke_asyncpg] SKIP: pip spawn failed: {e}");
                return;
            }
        }
    }

    let server = start_server().await;
    // libpq URL form, which asyncpg understands. `alice` is the project we
    // registered above; password is ignored by the test config.
    let url = format!(
        "postgres://alice:ignored@{}:{}/basin",
        server.addr.ip(),
        server.addr.port()
    );

    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let script = manifest.join("python").join("smoke.py");
    assert!(script.exists(), "smoke.py missing at {script:?}");

    let out = Command::new("python3")
        .arg(&script)
        .args(["--url", &url])
        .output()
        .await
        .expect("spawn python3 failed");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    println!("--- asyncpg smoke stdout ---\n{stdout}");
    if !stderr.trim().is_empty() {
        println!("--- asyncpg smoke stderr ---\n{stderr}");
    }

    assert!(
        out.status.success(),
        "asyncpg smoke exited {:?}; stdout above",
        out.status.code()
    );
    assert!(stdout.contains("PASS"), "asyncpg smoke did not print PASS");
}
