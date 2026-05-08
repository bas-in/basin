//! S3 port of `viability_cold_start.rs`.
//!
//! Card: `viability_cold_start` (real-cloud dashboard).
//! Bar: from in-process router bring-up → first `SELECT 1` over pgwire is
//! < 5000 ms with real S3-backed storage.
//!
//! Why in-process here, when LocalFS spawns the binary?
//! `services/basin-server` is wired to LocalFS-only at the env-var surface
//! today; there's no `BASIN_S3_*` plumbing in `main.rs`. Spawning the
//! shipped binary against an S3 bucket isn't possible without changing the
//! binary, which the constraints forbid (only `tests/` and `benchmark/`).
//!
//! In-process via `basin_router::run_until_bound` measures the same thing
//! that matters for the wedge story: how long from "I want to talk to
//! Basin" to "Basin answered SELECT 1", with the storage layer pointed at
//! S3. The metric still excludes process spawn / dyld load / mimalloc init,
//! which the LocalFS variant captures — so this card is a slightly more
//! generous version of the cold-start claim.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use basin_common::TenantId;
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_router::{ServerConfig, StaticTenantResolver};
use object_store::path::Path as ObjectPath;
use serde_json::json;
use tokio_postgres::NoTls;

const TEST_NAME: &str = "s3_viability_cold_start";
const BAR_COLD_START_MS: f64 = 5_000.0;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_viability_cold_start() {
    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };

    let object_store = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
        store: object_store.clone(),
        prefix: run_prefix.clone(),
    };

    // Clock starts at the entry to setup. We're not measuring binary
    // launch (see module docs) — just the storage + catalog + engine +
    // router bring-up + first round-trip.
    let started = Instant::now();

    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
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
    .expect("basin router failed to bind");
    let basin_addr = running.local_addr;
    let conn_str = format!(
        "host=127.0.0.1 port={} user=alice password=ignored",
        basin_addr.port()
    );

    // First SELECT 1.
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("pgwire connect");
    let driver = tokio::spawn(async move {
        let _ = conn.await;
    });
    client
        .simple_query("SELECT 1")
        .await
        .expect("SELECT 1 failed");
    let cold_start_ms = started.elapsed().as_secs_f64() * 1000.0;
    drop(client);
    driver.abort();

    let pass = cold_start_ms < BAR_COLD_START_MS;
    println!(
        "[S3 viability_cold_start] cold_start_ms={:.1} (bar <{}ms) {} addr={basin_addr}",
        cold_start_ms,
        BAR_COLD_START_MS,
        if pass { "PASS" } else { "FAIL" }
    );

    report_real_viability(
        "cold_start",
        "Cold-start latency (real S3)",
        "From Storage / Catalog / Engine / Router construction to first `SELECT 1` returning Ok over pgwire is < 5 s with real S3-compatible backend.",
        pass,
        PrimaryMetric {
            label: "cold_start_ms".into(),
            value: cold_start_ms,
            unit: "ms".into(),
            bar: BarOp::lt(BAR_COLD_START_MS),
        },
        json!({
            "cold_start_ms": cold_start_ms,
            "bind_addr": basin_addr.to_string(),
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    drop(running.shutdown);
    let _ = running.join.await;

    assert!(
        pass,
        "cold_start_ms={cold_start_ms:.1} >= bar {BAR_COLD_START_MS}ms"
    );
}
