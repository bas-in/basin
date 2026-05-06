//! Public data types for `basin-cron`.
//!
//! [`CronJob`] mirrors a row in `pg_cron`'s `cron.job` (jobname, schedule,
//! command, active, plus the bookkeeping fields `last_run` / `last_status`
//! that our v0.1 surface exposes). [`CronRunDetail`] mirrors `cron.job_run_details`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// One row in `cron.job`. The `(tenant, jobname)` pair is the natural key
/// (jobnames must be unique per tenant); `jobid` is a synthetic monotonically
/// increasing surrogate so result sets sort stably.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CronJob {
    /// Surrogate id, unique per tenant. Allocated at schedule time.
    pub jobid: i64,
    /// Human-readable name. Unique within a tenant. The pg_cron equivalent.
    pub jobname: String,
    /// Cron expression: `min hour dom mon dow` (5 fields).
    /// Note: the `cron` crate accepts a 6- or 7-field form too; we normalise
    /// to 5 fields by prepending `0` for seconds.
    pub schedule: String,
    /// Raw SQL that will be executed when due.
    pub command: String,
    /// Disabled jobs do not run. Default `true` at schedule time.
    pub active: bool,
    /// Wall-clock at the start of the most recent run. `None` until the
    /// first run completes.
    pub last_run: Option<DateTime<Utc>>,
    /// Postgres-style command tag (`"INSERT 0 1"`) or error message from
    /// the most recent run. `None` until the first run completes.
    pub last_status: Option<String>,
}

/// Status of one row in `cron.job_run_details`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Job ran and the engine returned success. `return_message` will be the
    /// command tag.
    Succeeded,
    /// Job ran and the engine returned an error. `return_message` will be
    /// the error string.
    Failed,
    /// Per-tenant rate limit was exhausted before this run could be issued.
    /// `return_message` is the rate-limit policy.
    RateLimited,
    /// Job was skipped because `active = false` at the time of the tick.
    Skipped,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Succeeded => "succeeded",
            JobStatus::Failed => "failed",
            JobStatus::RateLimited => "rate_limited",
            JobStatus::Skipped => "skipped",
        }
    }
}

/// Append-only audit row. One row per (job, tick) pair the runner attempted
/// to execute.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CronRunDetail {
    /// Auto-incrementing per-tenant id.
    pub id: i64,
    /// `CronJob::jobid` this run is for.
    pub jobid: i64,
    /// Same as `id` in pg_cron's table; we keep the column for compatibility.
    pub runid: i64,
    /// Always `None` in v0.1 — Basin does not expose engine-side OS pids.
    pub job_pid: Option<u32>,
    /// Logical database name. We use the tenant's prefix string.
    pub database: String,
    /// Principal `current_user` was stamped at schedule time.
    pub username: String,
    /// SQL the runner attempted to execute.
    pub command: String,
    /// Outcome.
    pub status: JobStatus,
    /// Free-form text. Command tag on success; error string on failure.
    pub return_message: String,
    /// Wall-clock at the start of the run.
    pub start_time: DateTime<Utc>,
    /// Wall-clock at the end of the run. Equal to `start_time` for skipped /
    /// rate-limited rows.
    pub end_time: DateTime<Utc>,
}
