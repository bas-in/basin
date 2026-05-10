//! S3 port of `scaling_tenant_count.rs`.
//!
//! Same shape — provision N tenants each with a one-row `events` table,
//! measure per-tenant RAM and the p50 of 5 quiet point-query samples — but
//! capped at SCALES = [1, 10, 100]. The 1000-tenant scale point on R2 takes
//! several minutes per provision pass and is not worth the wall time for
//! the dashboard story; the same trend is visible at 100.
//!
//! Card id: `tenant_count` (real-cloud dashboard).
//! Bar: same as LocalFS — `quiet_p50_at_max <= 5 * quiet_p50_at_1`.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{
    report_real_scaling, AxisSpec, BarOp, PrimaryMetric, SeriesSpec,
};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use serde_json::json;

const TEST_NAME: &str = "s3_scaling_tenant_count";
/// Capped at 100 — see module docs.
const SCALES: [usize; 3] = [1, 10, 100];
const QUIET_SAMPLES: usize = 5;
const BAR_LATENCY_RATIO: f64 = 5.0;

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

fn median(samples: &[f64]) -> f64 {
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[s.len() / 2]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_scaling_tenant_count() {
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

    // Caches OFF so the quiet point-query timing reflects the honest cold-ish
    // path. With caches enabled the second sample after a row-group hit
    // would dominate; we want to see the S3-class number.
    let storage = Storage::new(StorageConfig {
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog: catalog.clone(),
        shard: None,
    });

    {
        let warm = TenantId::new();
        let s = engine.open_session(warm).await.unwrap();
        s.execute("CREATE TABLE events (id BIGINT NOT NULL, payload TEXT NOT NULL)")
            .await
            .unwrap();
        s.execute("INSERT INTO events VALUES (0, 'warm')")
            .await
            .unwrap();
        let _ = s
            .execute("SELECT id FROM events WHERE id = 0")
            .await
            .unwrap();
    }

    struct Row {
        n: usize,
        rss_delta_kib: i64,
        per_tenant_kib: f64,
        quiet_p50_ms: f64,
    }
    let mut rows: Vec<Row> = Vec::new();
    let mut all_tenants: Vec<TenantId> = Vec::new();

    for &n in SCALES.iter() {
        let to_provision = n - all_tenants.len().min(n);
        let rss_before = rss_kib();
        for _ in 0..to_provision {
            let t = TenantId::new();
            let s = engine.open_session(t).await.unwrap();
            s.execute("CREATE TABLE events (id BIGINT NOT NULL, payload TEXT NOT NULL)")
                .await
                .unwrap();
            s.execute("INSERT INTO events VALUES (1, 'p')")
                .await
                .unwrap();
            drop(s);
            all_tenants.push(t);
        }
        let rss_after = rss_kib();
        let rss_delta_kib = rss_after as i64 - rss_before as i64;
        let per_tenant_kib = if to_provision == 0 {
            0.0
        } else {
            (rss_delta_kib as f64).max(0.0) / to_provision as f64
        };

        let quiet_tenant = all_tenants[0];
        let mut samples_ms: Vec<f64> = Vec::with_capacity(QUIET_SAMPLES);
        for _ in 0..QUIET_SAMPLES {
            let s = engine.open_session(quiet_tenant).await.unwrap();
            let started = Instant::now();
            let res = s
                .execute("SELECT id FROM events WHERE id = 1")
                .await
                .expect("quiet point query");
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
            match res {
                ExecResult::Rows { batches, .. } => {
                    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                    assert!(total >= 1, "quiet tenant should see at least one row");
                }
                ExecResult::Empty { .. } => panic!("expected Rows, got Empty"),
            }
            drop(s);
            samples_ms.push(elapsed_ms);
        }
        let quiet_p50_ms = median(&samples_ms);

        rows.push(Row {
            n,
            rss_delta_kib,
            per_tenant_kib,
            quiet_p50_ms,
        });
    }

    assert!(all_tenants.len() >= *SCALES.last().unwrap());

    println!(
        "{:>10} {:>15} {:>15} {:>15}",
        "N", "rss_delta_KiB", "per_tenant_KiB", "quiet_p50_ms"
    );
    for r in &rows {
        println!(
            "{:>10} {:>15} {:>15.2} {:>15.2}",
            r.n, r.rss_delta_kib, r.per_tenant_kib, r.quiet_p50_ms
        );
    }

    let p50_at_1 = rows.first().map(|r| r.quiet_p50_ms).unwrap_or(1.0);
    let p50_at_max = rows.last().map(|r| r.quiet_p50_ms).unwrap_or(1.0);
    let latency_ratio = p50_at_max / p50_at_1.max(1e-9);
    let pass = latency_ratio <= BAR_LATENCY_RATIO;

    println!(
        "[S3 scaling_tenant_count] quiet_p50@1={:.2}ms, quiet_p50@{}={:.2}ms, ratio={:.2}x (bar <={}x) {}",
        p50_at_1,
        SCALES.last().unwrap(),
        p50_at_max,
        latency_ratio,
        BAR_LATENCY_RATIO,
        if pass { "PASS" } else { "FAIL" }
    );

    let json_rows: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            json!({
                "tenant_count": r.n,
                "per_tenant_ram_kib": r.per_tenant_kib,
                "quiet_p50_ms": r.quiet_p50_ms,
                "rss_delta_kib": r.rss_delta_kib,
            })
        })
        .collect();

    report_real_scaling(
        "tenant_count",
        "Per-tenant cost vs tenant count (real S3)",
        "RAM and quiet point-query latency per tenant stay near-constant as tenant count grows on a real S3-compatible backend.",
        pass,
        AxisSpec {
            key: "tenant_count".into(),
            label: "tenants".into(),
        },
        vec![
            SeriesSpec {
                key: "per_tenant_ram_kib".into(),
                label: "Per-tenant RAM".into(),
                unit: Some("KiB".into()),
            },
            SeriesSpec {
                key: "quiet_p50_ms".into(),
                label: "Quiet point query p50".into(),
                unit: Some("ms".into()),
            },
        ],
        json_rows,
        Some(PrimaryMetric {
            label: "quiet_p50_at_max / quiet_p50_at_1".into(),
            value: latency_ratio,
            unit: "x".into(),
            bar: BarOp::lt(BAR_LATENCY_RATIO + f64::EPSILON),
        }),
    );

    if !pass {
        panic!(
            "FAIL: quiet_p50 ratio {:.2}x > {}x bar (p50@1={:.2}ms, p50@{}={:.2}ms)",
            latency_ratio,
            BAR_LATENCY_RATIO,
            p50_at_1,
            SCALES.last().unwrap(),
            p50_at_max,
        );
    }
}
