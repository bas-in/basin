//! Viability test 2: idle-project RAM cost.
//!
//! Claim: Basin holds many idle projects in one process for cheap. We
//! provision N=1000 projects (namespace + table only — no data, no engine
//! sessions) and measure the RSS delta. Bar: <500 KiB / project.
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
use basin_common::{TableName, ProjectId};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

const PROJECTS: usize = 1000;
const BAR_KIB_PER_PROJECT: u64 = 500;

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
async fn viability_2_idle_project_ram() {
    // Storage is a shared resource; provisioning a project is purely a catalog
    // operation in this test (no Parquet is written).
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let _storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let table = TableName::new("events").unwrap();
    let sch = schema();

    // Warm up the allocator a touch so the baseline RSS isn't suspiciously
    // low. A single dummy provision exercises the same code paths the loop
    // does and stabilizes lazy initialization in tokio + tracing.
    {
        let warm = ProjectId::new();
        catalog.create_namespace(&warm).await.unwrap();
        catalog.create_table(&warm, &table, &sch).await.unwrap();
    }

    let rss_before = rss_kib();

    // Hold the project ids so the compiler doesn't drop them and so we model
    // a realistic "control plane has a list of all projects in memory" world.
    let mut projects: Vec<ProjectId> = Vec::with_capacity(PROJECTS);
    for _ in 0..PROJECTS {
        let t = ProjectId::new();
        catalog.create_namespace(&t).await.unwrap();
        catalog.create_table(&t, &table, &sch).await.unwrap();
        projects.push(t);
    }

    let rss_after = rss_kib();

    let delta_kib = rss_after.saturating_sub(rss_before);
    let per_project_kib = delta_kib as f64 / PROJECTS as f64;
    let bar_kib = BAR_KIB_PER_PROJECT as f64;
    let pass = per_project_kib < bar_kib;

    // Keep `projects` live across the measurement.
    assert_eq!(projects.len(), PROJECTS);

    println!(
        "[VIABILITY 2] idle projects: projects={}, rss_before={} KiB, rss_after={} KiB, per_project={:.1} KiB (bar <{} KiB/project) {}",
        PROJECTS,
        rss_before,
        rss_after,
        per_project_kib,
        BAR_KIB_PER_PROJECT,
        if pass { "PASS" } else { "FAIL" }
    );

    let per_project_bytes = (delta_kib as f64 * 1024.0) / PROJECTS as f64;
    let bar_bytes = (BAR_KIB_PER_PROJECT * 1024) as f64;

    report_viability(
        "idle_project_ram",
        "Idle-project RAM cost",
        "Basin holds many idle projects in one process for under 500 KiB each.",
        pass,
        PrimaryMetric {
            label: "per_project_kib".into(),
            value: per_project_kib,
            unit: "KiB".into(),
            bar: BarOp::lt(BAR_KIB_PER_PROJECT as f64),
        },
        json!({
            "projects": PROJECTS,
            "rss_before_kib": rss_before,
            "rss_after_kib": rss_after,
        }),
    );

    assert!(
        per_project_bytes < bar_bytes,
        "{per_project_bytes:.0} bytes/project >= {bar_bytes:.0} bytes/project bar"
    );
}
