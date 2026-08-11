//! Full-scan workload — `SELECT COUNT(*) FROM events` on `project[project_idx]`.
//!
//! The scan is the load-bearing "your column store actually compresses and
//! scans" measurement; it complements [`super::point_select`] by stressing
//! the IO + decode path rather than the planner fast-path.

use std::time::Instant;

use anyhow::Result;

use crate::config::BenchConfig;
use crate::harness::BenchHarness;
use crate::sample::{LatencySamples, SampleReport};

pub struct ScanOutcome {
    pub samples: SampleReport,
    pub wall_secs: f64,
    pub scans: u64,
}

/// Run `cfg.workload.iterations` `SELECT COUNT(*)` scans against
/// `project[project_idx].events`. Iterations are run serially in this
/// primitive — fan-out is configured by the profile by issuing the same
/// shape across multiple `project_idx` values in parallel (so per-project
/// IO permits actually fire).
pub async fn scan(h: &BenchHarness, cfg: &BenchConfig, project_idx: usize) -> Result<ScanOutcome> {
    let project = h
        .projects
        .get(project_idx)
        .copied()
        .ok_or_else(|| anyhow::anyhow!("project index out of range"))?;
    let session = h.engine.open_session(project).await?;
    let samples = LatencySamples::new();
    let started = Instant::now();
    let mut count: u64 = 0;
    for _ in 0..cfg.workload.iterations {
        let t0 = Instant::now();
        session
            .execute(&format!("SELECT COUNT(*) FROM {}", h.table))
            .await?;
        samples.record(t0.elapsed());
        count += 1;
    }
    Ok(ScanOutcome {
        samples: samples.report(),
        wall_secs: started.elapsed().as_secs_f64(),
        scans: count,
    })
}
