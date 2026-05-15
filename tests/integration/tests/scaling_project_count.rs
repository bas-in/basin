//! Scaling test: per-project cost as a function of total project count.
//!
//! Card: `scaling_project_count`
//! Claim: Basin's per-project overhead — both RAM and quiet-project point-query
//! latency — stays roughly constant as project count grows. The wedge depends
//! on this: pricing assumes 90% of projects are idle 90% of the time, so the
//! marginal cost per idle project must not balloon.
//!
//! Method: provision N projects, each with `events(id BIGINT, payload TEXT)`
//! seeded with one row. Measure two things at each scale:
//!   - per-project RAM (RSS delta / N).
//!   - p50 of 5 point-query samples on a single quiet project while the other
//!     N-1 sit idle.
//!
//! Bar: `quiet_p50_at_max <= 5 * quiet_p50_at_1`.
//!
//! Scale sweep: `[1, 10, 100, 1000]` on LocalFS. The S3 variant caps at 100
//! because 1000-project setup on R2 takes too long.
//!
//! Implementation notes:
//! - We use one shared `Engine` and a single `TempDir` for all scales so the
//!   measurement reflects *additional* per-project cost on a warm process.
//! - `ProjectSession`s are dropped between scale points to avoid accumulating
//!   per-session state — each scale's quiet point query opens a fresh
//!   session for the chosen project.
//! - We avoid the disk + page caches so the quiet point-query latency
//!   reflects the cold-ish path; otherwise the second sample after a warm-up
//!   cache hit would dominate.

#![allow(clippy::print_stdout)]

use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{
    report_scaling, AxisSpec, BarOp, PrimaryMetric, SeriesSpec,
};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

/// Scale sweep. The "1" point gives the per-project overhead with no
/// neighbours; the "1000" point amplifies any superlinear cost.
const SCALES: [usize; 4] = [1, 10, 100, 1000];
const QUIET_SAMPLES: usize = 5;
/// Bar: quiet p50 at the max scale must be within 5x of the quiet p50 at
/// the smallest scale. Generous enough that allocator drift / DataFusion
/// planner state doesn't trip a false fail; tight enough that any
/// per-project linear scan would blow it out.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scaling_project_count() {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    // Caches OFF so quiet-project p50 reflects the honest cold-ish path.
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog: catalog.clone(),
        shard: None,
    });

    // Warm the allocator + DataFusion planner before any measurement.
    {
        let warm = ProjectId::new();
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
        per_project_kib: f64,
        quiet_p50_ms: f64,
    }
    let mut rows: Vec<Row> = Vec::new();
    let mut all_projects: Vec<ProjectId> = Vec::new();

    for &n in SCALES.iter() {
        let to_provision = n - all_projects.len().min(n);
        let rss_before = rss_kib();
        for _ in 0..to_provision {
            let t = ProjectId::new();
            let s = engine.open_session(t).await.unwrap();
            s.execute("CREATE TABLE events (id BIGINT NOT NULL, payload TEXT NOT NULL)")
                .await
                .unwrap();
            // One row per project so the point query has something to find.
            s.execute("INSERT INTO events VALUES (1, 'p')")
                .await
                .unwrap();
            // Drop the session. Idle projects in this test should NOT hold a
            // live session; that would contaminate the per-project RAM signal.
            drop(s);
            all_projects.push(t);
        }
        let rss_after = rss_kib();
        let rss_delta_kib = rss_after as i64 - rss_before as i64;
        let per_project_kib = if to_provision == 0 {
            0.0
        } else {
            (rss_delta_kib as f64).max(0.0) / to_provision as f64
        };

        // Pick one quiet project (the first one provisioned) and run
        // QUIET_SAMPLES point queries. Open the session fresh each time so
        // we measure the cold-ish path through the engine.
        let quiet_project = all_projects[0];
        let mut samples_ms: Vec<f64> = Vec::with_capacity(QUIET_SAMPLES);
        for _ in 0..QUIET_SAMPLES {
            let s = engine.open_session(quiet_project).await.unwrap();
            let started = Instant::now();
            let res = s
                .execute("SELECT id FROM events WHERE id = 1")
                .await
                .expect("quiet point query");
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
            match res {
                ExecResult::Rows { batches, .. } => {
                    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                    assert!(total >= 1, "quiet project should see at least one row");
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
            per_project_kib,
            quiet_p50_ms,
        });
    }

    // Don't let the compiler drop the live projects before we read RSS.
    assert!(all_projects.len() >= *SCALES.last().unwrap());

    println!(
        "{:>10} {:>15} {:>15} {:>15}",
        "N", "rss_delta_KiB", "per_project_KiB", "quiet_p50_ms"
    );
    for r in &rows {
        println!(
            "{:>10} {:>15} {:>15.2} {:>15.2}",
            r.n, r.rss_delta_kib, r.per_project_kib, r.quiet_p50_ms
        );
    }

    let p50_at_1 = rows.first().map(|r| r.quiet_p50_ms).unwrap_or(1.0);
    let p50_at_max = rows.last().map(|r| r.quiet_p50_ms).unwrap_or(1.0);
    let latency_ratio = p50_at_max / p50_at_1.max(1e-9);
    let pass = latency_ratio <= BAR_LATENCY_RATIO;

    println!(
        "[SCALING project_count] quiet_p50@1={:.2}ms, quiet_p50@{}={:.2}ms, ratio={:.2}x (bar <={}x) {}",
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
                "project_count": r.n,
                "per_project_ram_kib": r.per_project_kib,
                "quiet_p50_ms": r.quiet_p50_ms,
                "rss_delta_kib": r.rss_delta_kib,
            })
        })
        .collect();

    report_scaling(
        "project_count",
        "Per-project cost vs project count",
        "RAM and quiet point-query latency per project stay near-constant as project count grows.",
        pass,
        AxisSpec {
            key: "project_count".into(),
            label: "projects".into(),
        },
        vec![
            SeriesSpec {
                key: "per_project_ram_kib".into(),
                label: "Per-project RAM".into(),
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

    assert!(
        pass,
        "FAIL: quiet_p50 ratio {:.2}x > {}x bar (p50@1={:.2}ms, p50@{}={:.2}ms)",
        latency_ratio,
        BAR_LATENCY_RATIO,
        p50_at_1,
        SCALES.last().unwrap(),
        p50_at_max,
    );
}
