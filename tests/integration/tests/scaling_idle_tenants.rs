//! Scaling test 1: idle-tenant cost curve (scale-down).
//!
//! Claim: RAM and provision time per *idle* tenant stay small as the tenant
//! count grows. The wedge depends on this — Basin's pricing assumes 90% of
//! tenants are idle 90% of the time, so per-idle-tenant cost is the load-
//! bearing constant in the unit economics.
//!
//! Curve shape: per_tenant_KiB roughly flat across 100 -> 10_000 tenants, and
//! per-tenant provision time within ~5x across the same range.
//!
//! Bars:
//! - per_tenant_KiB <= 5.0 at every scale point
//! - provision_ms / N at N=10_000 within 5x of N=100
//!
//! Notes:
//! - We use one shared `InMemoryCatalog` and `Storage` rooted in one
//!   `TempDir` for all four scale points. The goal is to measure the
//!   *additional* cost contributed by N tenants on top of an already-warm
//!   process; the brief explicitly accepts this.
//! - We do NOT open Engine sessions. Idle tenants have only catalog state.
//! - RSS via `ps -o rss=` matches the convention the viability suite uses
//!   (avoids pulling in `sysinfo` since the workspace doesn't have it).

#![allow(clippy::print_stdout)]

use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{TableName, TenantId};
use basin_integration_tests::benchmark::{
    report_scaling, AxisSpec, BarOp, PrimaryMetric, SeriesSpec,
};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

const SCALES: [usize; 4] = [100, 1_000, 5_000, 10_000];
const BAR_PER_TENANT_KIB: f64 = 5.0;
const BAR_PROVISION_RATIO: f64 = 5.0;

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
        Field::new("ts", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, true),
    ])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scaling_1_idle_tenants() {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let _storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let table = TableName::new("events").unwrap();
    let sch = schema();

    // Warm up the allocator and tokio + tracing lazy init so the very first
    // measurement isn't noisy.
    {
        let warm = TenantId::new();
        catalog.create_namespace(&warm).await.unwrap();
        catalog.create_table(&warm, &table, &sch).await.unwrap();
    }

    // Hold every tenant id across every scale so the measurement reflects a
    // realistic "control plane keeps the list of all live tenants" footprint.
    let mut tenants: Vec<TenantId> = Vec::with_capacity(SCALES.iter().sum::<usize>());

    struct Row {
        n: usize,
        rss_delta_kib: i64,
        per_tenant_kib: f64,
        provision_ms: f64,
        per_tenant_provision_us: f64,
    }
    let mut rows: Vec<Row> = Vec::new();

    for &n in SCALES.iter() {
        let rss_before = rss_kib();
        let started = Instant::now();
        for _ in 0..n {
            let t = TenantId::new();
            catalog.create_namespace(&t).await.unwrap();
            catalog.create_table(&t, &table, &sch).await.unwrap();
            tenants.push(t);
        }
        let provision_ms = started.elapsed().as_secs_f64() * 1000.0;
        let rss_after = rss_kib();

        let rss_delta_kib = rss_after as i64 - rss_before as i64;
        let per_tenant_kib = (rss_delta_kib as f64).max(0.0) / n as f64;
        let per_tenant_provision_us = (provision_ms * 1000.0) / n as f64;

        rows.push(Row {
            n,
            rss_delta_kib,
            per_tenant_kib,
            provision_ms,
            per_tenant_provision_us,
        });
    }

    // Sanity hold so the compiler can't drop the live tenants before we read
    // RSS.
    assert!(tenants.len() >= SCALES.iter().sum::<usize>());

    println!(
        "{:>10} {:>15} {:>15} {:>15}",
        "N", "rss_delta_KiB", "per_tenant_KiB", "provision_ms"
    );
    for r in &rows {
        println!(
            "{:>10} {:>15} {:>15.2} {:>15.1}",
            r.n, r.rss_delta_kib, r.per_tenant_kib, r.provision_ms
        );
    }

    let max_per_tenant = rows
        .iter()
        .map(|r| r.per_tenant_kib)
        .fold(0.0_f64, f64::max);
    let provision_us_first = rows.first().unwrap().per_tenant_provision_us;
    let provision_us_last = rows.last().unwrap().per_tenant_provision_us;
    let provision_ratio = provision_us_last / provision_us_first.max(1e-9);

    let pass_ram = max_per_tenant <= BAR_PER_TENANT_KIB;
    let pass_provision = provision_ratio <= BAR_PROVISION_RATIO;
    let pass = pass_ram && pass_provision;

    println!(
        "[SCALING 1] idle tenants: max_per_tenant={:.2} KiB, provision_ratio_10k_over_100={:.2}x (bar: per_tenant<={} KiB, provision_ratio<={}x) {}",
        max_per_tenant,
        provision_ratio,
        BAR_PER_TENANT_KIB,
        BAR_PROVISION_RATIO,
        if pass { "PASS" } else { "FAIL" }
    );

    let json_rows: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            json!({
                "tenants": r.n,
                "rss_delta_kib": r.rss_delta_kib,
                "per_tenant_kib": r.per_tenant_kib,
                "provision_ms": r.provision_ms,
            })
        })
        .collect();

    report_scaling(
        "idle_tenants",
        "Idle-tenant cost curve",
        "RAM cost stays small per idle tenant as the tenant count grows.",
        pass,
        AxisSpec {
            key: "tenants".into(),
            label: "tenants".into(),
        },
        vec![
            SeriesSpec {
                key: "per_tenant_kib".into(),
                label: "Per-tenant RSS".into(),
                unit: Some("KiB".into()),
            },
            SeriesSpec {
                key: "provision_ms".into(),
                label: "Provision time".into(),
                unit: Some("ms".into()),
            },
            SeriesSpec {
                key: "rss_delta_kib".into(),
                label: "RSS delta".into(),
                unit: Some("KiB".into()),
            },
        ],
        json_rows,
        Some(PrimaryMetric {
            label: "max per_tenant_kib across scales".into(),
            value: max_per_tenant,
            unit: "KiB".into(),
            bar: BarOp::lt(BAR_PER_TENANT_KIB),
        }),
    );

    if !pass {
        panic!(
            "FAIL: max_per_tenant_KiB={max_per_tenant:.2} (bar {BAR_PER_TENANT_KIB}), provision_ratio={provision_ratio:.2} (bar {BAR_PROVISION_RATIO})"
        );
    }
}
