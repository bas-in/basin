//! S3 port of `viability_idle_tenant_ram.rs`.
//!
//! Idle-tenant RAM cost is a control-plane metric: it measures namespace +
//! table catalog overhead, NOT storage I/O. We still wire a real-S3 `Storage`
//! to mirror the LocalFS test's shape (and to keep the dashboard story
//! consistent: the "real S3 backend" run actually touches the S3
//! `ObjectStore`), but no Parquet is written here, so the only I/O on S3 is
//! the implicit `Arc<dyn ObjectStore>` construction.
//!
//! Bar: <500 KiB / tenant, same as LocalFS.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::process::Command;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{TableName, TenantId};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use serde_json::json;

const TEST_NAME: &str = "s3_viability_idle_tenant_ram";
const TENANTS: usize = 1000;
const BAR_KIB_PER_TENANT: u64 = 500;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_viability_idle_tenant_ram() {
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

    let _storage = Storage::new(StorageConfig {
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let table = TableName::new("events").unwrap();
    let sch = schema();

    // Warm up the allocator.
    {
        let warm = TenantId::new();
        catalog.create_namespace(&warm).await.unwrap();
        catalog.create_table(&warm, &table, &sch).await.unwrap();
    }

    let rss_before = rss_kib();

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

    assert_eq!(tenants.len(), TENANTS);

    println!(
        "[S3 idle_tenant_ram] tenants={}, rss_before={} KiB, rss_after={} KiB, per_tenant={:.1} KiB (bar <{} KiB/tenant) {}",
        TENANTS,
        rss_before,
        rss_after,
        per_tenant_kib,
        BAR_KIB_PER_TENANT,
        if pass { "PASS" } else { "FAIL" }
    );

    let per_tenant_bytes = (delta_kib as f64 * 1024.0) / TENANTS as f64;
    let bar_bytes = (BAR_KIB_PER_TENANT * 1024) as f64;

    report_real_viability(
        "idle_tenant_ram",
        "Idle-tenant RAM cost (real S3)",
        "Basin holds many idle tenants in one process for under 500 KiB each, with a real-S3 Storage attached to each tenant's control-plane state.",
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
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(
        per_tenant_bytes < bar_bytes,
        "{per_tenant_bytes:.0} bytes/tenant >= {bar_bytes:.0} bytes/tenant bar"
    );
}
