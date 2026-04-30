//! Viability test 2: idle-tenant RAM cost.
//!
//! Claim: Basin holds many idle tenants in one process for cheap. We
//! provision N=1000 tenants (namespace + table only — no data, no engine
//! sessions) and measure the RSS delta. Bar: <500 KiB / tenant.
//!
//! The bar is more honest than the original 100 KB pitch: tokio runtime,
//! allocator slack, and arrow_schema overhead are all real, and pretending
//! they aren't bites later. The print line surfaces the actual number so we
//! can see how close we are.

#![allow(clippy::print_stdout)]

use std::process::Command;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{TableName, TenantId};
use basin_integration_tests::dashboard::{report_viability, BarOp, PrimaryMetric};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

const TENANTS: usize = 1000;
const BAR_KIB_PER_TENANT: u64 = 500;

/// Read the current process's resident set size in KiB via `ps -o rss=`.
/// macOS and Linux both ship this. Returns KiB.
fn rss_kib() -> u64 {
    let pid = std::process::id().to_string();
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .expect("ps failed");
    let s = String::from_utf8_lossy(&out.stdout);
    s.trim()
        .parse::<u64>()
        .unwrap_or_else(|e| panic!("could not parse rss from {s:?}: {e}"))
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("created_at", DataType::Utf8, true),
    ])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn viability_2_idle_tenant_ram() {
    // Storage is a shared resource; provisioning a tenant is purely a catalog
    // operation in this test (no Parquet is written).
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let _storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let table = TableName::new("events").unwrap();
    let sch = schema();

    // Warm up the allocator a touch so the baseline RSS isn't suspiciously
    // low. A single dummy provision exercises the same code paths the loop
    // does and stabilizes lazy initialization in tokio + tracing.
    {
        let warm = TenantId::new();
        catalog.create_namespace(&warm).await.unwrap();
        catalog.create_table(&warm, &table, &sch).await.unwrap();
    }

    let rss_before = rss_kib();

    // Hold the tenant ids so the compiler doesn't drop them and so we model
    // a realistic "control plane has a list of all tenants in memory" world.
    let mut tenants: Vec<TenantId> = Vec::with_capacity(TENANTS);
    for _ in 0..TENANTS {
        let t = TenantId::new();
        catalog.create_namespace(&t).await.unwrap();
        catalog.create_table(&t, &table, &sch).await.unwrap();
        tenants.push(t);
    }

    let rss_after = rss_kib();

    let delta_kib = rss_after.saturating_sub(rss_before);
    let per_tenant_kib = delta_kib as f64 / TENANTS as f64;
    let bar_kib = BAR_KIB_PER_TENANT as f64;
    let pass = per_tenant_kib < bar_kib;

    // Keep `tenants` live across the measurement.
    assert_eq!(tenants.len(), TENANTS);

    println!(
        "[VIABILITY 2] idle tenants: tenants={}, rss_before={} KiB, rss_after={} KiB, per_tenant={:.1} KiB (bar <{} KiB/tenant) {}",
        TENANTS,
        rss_before,
        rss_after,
        per_tenant_kib,
        BAR_KIB_PER_TENANT,
        if pass { "PASS" } else { "FAIL" }
    );

    let per_tenant_bytes = (delta_kib as f64 * 1024.0) / TENANTS as f64;
    let bar_bytes = (BAR_KIB_PER_TENANT * 1024) as f64;

    report_viability(
        "idle_tenant_ram",
        "Idle-tenant RAM cost",
        "Basin holds many idle tenants in one process for under 500 KiB each.",
        pass,
        PrimaryMetric {
            label: "per_tenant_kib".into(),
            value: per_tenant_kib,
            unit: "KiB".into(),
            bar: BarOp::lt(BAR_KIB_PER_TENANT as f64),
        },
        json!({
            "tenants": TENANTS,
            "rss_before_kib": rss_before,
            "rss_after_kib": rss_after,
        }),
    );

    assert!(
        per_tenant_bytes < bar_bytes,
        "{per_tenant_bytes:.0} bytes/tenant >= {bar_bytes:.0} bytes/tenant bar"
    );
}
