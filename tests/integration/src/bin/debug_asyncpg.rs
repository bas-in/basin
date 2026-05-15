//! Boot Basin in-process and idle so we can poke it with `psql`/`asyncpg`
//! while debugging the language-driver smoke tests. Writes the URL to
//! stdout and stays up until SIGINT.
//!
//! Run: `cargo run --release -p basin-integration-tests --bin debug_asyncpg`

use std::collections::HashMap;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

#[tokio::main]
async fn main() {
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
        connection_limiter: None,
    })
    .await
    .expect("server failed to bind");

    println!(
        "BASIN_URL=postgres://alice:ignored@{}:{}/basin",
        running.local_addr.ip(),
        running.local_addr.port()
    );
    println!("Press Ctrl-C to stop.");

    tokio::signal::ctrl_c().await.unwrap();
    let _ = running.shutdown.send(());
    let _ = running.join.await;
    drop(dir);
}
