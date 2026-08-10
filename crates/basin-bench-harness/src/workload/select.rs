//! Point-select workload — random-id `SELECT * WHERE id = ?` against the
//! provisioned project's `events` table.
//!
//! Uses [`BenchHarness::engine`] directly (i.e. the in-process executor,
//! not pgwire). This keeps the harness target-agnostic; profiles that want
//! to measure the pgwire path can wrap a [`tokio_postgres::Client`] on
//! top of `basin-server` but that's outside this crate's scope.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::task::JoinSet;

use crate::config::BenchConfig;
use crate::harness::BenchHarness;
use crate::sample::{LatencySamples, SampleReport};

/// Default workload seed. Same constant the legacy
/// `tests/integration/src/workload.rs` uses — keeps replay bit-stable.
pub const DEFAULT_SEED: u64 = 0xBA51_F1ED_BA51_F1ED;

/// Outcome of one point-select workload run.
pub struct PointSelectOutcome {
    pub samples: SampleReport,
    pub wall_secs: f64,
    pub total_queries: u64,
    pub errors: u64,
}

/// Run `cfg.workload.iterations` point-selects fan-out across
/// `cfg.workload.concurrency` tokio tasks against project index `project_idx`.
///
/// Returns the merged latency distribution + wall-clock + (queries, errors).
pub async fn point_select(
    h: &BenchHarness,
    cfg: &BenchConfig,
    project_idx: usize,
) -> Result<PointSelectOutcome> {
    let project = h
        .projects
        .get(project_idx)
        .copied()
        .ok_or_else(|| anyhow::anyhow!("project index out of range"))?;
    let working_set = pick_working_set(
        cfg.workload.table_rows,
        cfg.workload.working_set,
        DEFAULT_SEED,
    );
    let working_set = Arc::new(working_set);

    let samples = LatencySamples::new();
    let deadline = Instant::now() + Duration::from_secs(cfg.workload.deadline_secs);
    let per_task = cfg.workload.iterations / cfg.workload.concurrency.max(1);

    let mut set = JoinSet::new();
    let started = Instant::now();
    for tid in 0..cfg.workload.concurrency {
        let engine = h.engine.clone();
        let table = h.table.clone();
        let working_set = working_set.clone();
        let s = samples.clone();
        set.spawn(async move {
            let mut rng = StdRng::seed_from_u64(DEFAULT_SEED ^ tid as u64);
            let session = engine.open_session(project).await.expect("open session");
            let mut errors: u64 = 0;
            let mut count: u64 = 0;
            for _ in 0..per_task {
                if Instant::now() >= deadline {
                    break;
                }
                let id = working_set[rng.gen_range(0..working_set.len())];
                let sql = format!("SELECT id, payload FROM {} WHERE id = {}", table, id);
                let t0 = Instant::now();
                match session.execute(&sql).await {
                    Ok(_) => s.record(t0.elapsed()),
                    Err(_) => errors += 1,
                }
                count += 1;
            }
            (count, errors)
        });
    }

    let mut total = 0u64;
    let mut errors = 0u64;
    while let Some(r) = set.join_next().await {
        let (c, e) = r.expect("task panic");
        total += c;
        errors += e;
    }
    let wall_secs = started.elapsed().as_secs_f64();
    Ok(PointSelectOutcome {
        samples: samples.report(),
        wall_secs,
        total_queries: total,
        errors,
    })
}

fn pick_working_set(table_size: u64, working_set_size: usize, seed: u64) -> Vec<u64> {
    assert!(table_size > 0);
    let mut rng = StdRng::seed_from_u64(seed);
    (0..working_set_size)
        .map(|_| rng.gen_range(0..table_size))
        .collect()
}
