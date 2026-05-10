//! Per-tenant cron-state store, backed by ordinary Basin tables.
//!
//! [`CronStore`] keeps two tables per tenant:
//!
//! - `cron_job` — current schedule. (We use underscore rather than the
//!   `cron.job` schema-qualified pg_cron name because Basin's table names
//!   are flat identifiers in v0.1; a future `cron` schema is a TODO.)
//! - `cron_job_run_details` — append-only audit log.
//!
//! Both are created lazily the first time the store is used for a tenant.
//! All reads and writes go through `Engine::open_session`, so:
//!
//! 1. Tenant isolation is the same as for user SQL.
//! 2. RLS policies on `cron_job` itself are honoured (you can shape who
//!    can schedule under a single tenant).
//! 3. Persistence is handled by the catalog and storage layers; no separate
//!    durability story.

use std::sync::Arc;

use arrow_array::Array;
use basin_common::{BasinError, Result, TenantId};
use basin_engine::{Engine, ExecResult};
use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::types::{CronJob, CronRunDetail, JobStatus};

/// Names of the two tables we keep per tenant. Underscored because Basin's
/// `Ident` validator rejects `.` characters in v0.1.
pub(crate) const JOB_TABLE: &str = "cron_job";
pub(crate) const RUN_DETAILS_TABLE: &str = "cron_job_run_details";

/// Errors specific to scheduling.
#[derive(Debug, Error)]
pub enum ScheduleError {
    #[error("invalid cron expression {expr:?}: {source}")]
    InvalidExpression {
        expr: String,
        #[source]
        source: cron::error::Error,
    },
    #[error("tenant has reached the per-tenant job cap of {cap}")]
    JobCapExceeded { cap: usize },
    #[error("job {0:?} already exists")]
    Duplicate(String),
    #[error("job {0:?} not found")]
    NotFound(String),
    #[error("engine error: {0}")]
    Engine(#[from] BasinError),
}

/// Store front for the per-tenant cron tables.
///
/// Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct CronStore {
    inner: Arc<CronStoreInner>,
}

struct CronStoreInner {
    engine: Engine,
    /// Per-tenant guard so two concurrent `schedule` calls into the same
    /// tenant cannot race past the cap or generate the same `jobid`. Held
    /// only over the read-then-write window.
    schedule_guard: Mutex<()>,
}

impl CronStore {
    /// Hard cap on jobs per tenant. Same default as pg_cron's deployments
    /// recommend; tunable if a deployment needs more.
    pub const MAX_JOBS_PER_TENANT: usize = 100;

    pub fn new(engine: Engine) -> Self {
        Self {
            inner: Arc::new(CronStoreInner {
                engine,
                schedule_guard: Mutex::new(()),
            }),
        }
    }

    /// Reference to the engine the store writes through.
    pub fn engine(&self) -> &Engine {
        &self.inner.engine
    }

    /// Idempotently create the two cron tables under `tenant`. Safe to call
    /// before every operation; the underlying `CREATE TABLE` is a no-op
    /// once the table exists.
    pub async fn ensure_tables(&self, tenant: &TenantId) -> Result<()> {
        let sess = self.inner.engine.open_session(*tenant).await?;
        // CREATE TABLE is currently not idempotent in basin-engine; we look
        // for the table first via SHOW TABLES so re-running is safe.
        let existing = list_table_names(&sess).await?;
        if !existing.contains(&JOB_TABLE.to_string()) {
            sess.execute(&format!(
                "CREATE TABLE {JOB_TABLE} (
                    jobid BIGINT NOT NULL,
                    jobname TEXT NOT NULL,
                    schedule TEXT NOT NULL,
                    command TEXT NOT NULL,
                    active BIGINT NOT NULL,
                    username TEXT NOT NULL,
                    last_run_unix_ms BIGINT,
                    last_status TEXT
                )"
            ))
            .await?;
        }
        if !existing.contains(&RUN_DETAILS_TABLE.to_string()) {
            sess.execute(&format!(
                "CREATE TABLE {RUN_DETAILS_TABLE} (
                    id BIGINT NOT NULL,
                    jobid BIGINT NOT NULL,
                    runid BIGINT NOT NULL,
                    database TEXT NOT NULL,
                    username TEXT NOT NULL,
                    command TEXT NOT NULL,
                    status TEXT NOT NULL,
                    return_message TEXT NOT NULL,
                    start_unix_ms BIGINT NOT NULL,
                    end_unix_ms BIGINT NOT NULL
                )"
            ))
            .await?;
        }
        Ok(())
    }

    /// Schedule a new job under `tenant`. Mirrors pg_cron's `cron.schedule`
    /// (3-arg form): `(jobname, schedule, command)`.
    ///
    /// Errors if the cron expression is invalid, the per-tenant cap is hit,
    /// or `jobname` already exists for this tenant.
    pub async fn schedule(
        &self,
        tenant: &TenantId,
        username: &str,
        jobname: &str,
        schedule: &str,
        command: &str,
    ) -> std::result::Result<i64, ScheduleError> {
        // Validate the cron expression up front so the failure surfaces at
        // schedule time, not at the next tick.
        parse_schedule(schedule)?;

        self.ensure_tables(tenant).await?;
        let _g = self.inner.schedule_guard.lock().await;

        let jobs = self.list_jobs(tenant).await?;
        if jobs.len() >= Self::MAX_JOBS_PER_TENANT {
            return Err(ScheduleError::JobCapExceeded {
                cap: Self::MAX_JOBS_PER_TENANT,
            });
        }
        if jobs.iter().any(|j| j.jobname == jobname) {
            return Err(ScheduleError::Duplicate(jobname.to_string()));
        }

        let jobid = jobs.iter().map(|j| j.jobid).max().unwrap_or(0) + 1;

        let sess = self.inner.engine.open_session(*tenant).await?;
        sess.execute(&format!(
            "INSERT INTO {JOB_TABLE} VALUES \
             ({jobid}, {jobname}, {schedule}, {command}, 1, {user}, NULL, NULL)",
            jobname = sql_text(jobname),
            schedule = sql_text(schedule),
            command = sql_text(command),
            user = sql_text(username),
        ))
        .await?;
        Ok(jobid)
    }

    /// Remove a job by name. Mirrors pg_cron's `cron.unschedule(name)`.
    /// Returns the removed jobid.
    pub async fn unschedule(
        &self,
        tenant: &TenantId,
        jobname: &str,
    ) -> std::result::Result<i64, ScheduleError> {
        self.ensure_tables(tenant).await?;
        let _g = self.inner.schedule_guard.lock().await;

        let jobs = self.list_jobs(tenant).await?;
        let job = jobs
            .iter()
            .find(|j| j.jobname == jobname)
            .ok_or_else(|| ScheduleError::NotFound(jobname.to_string()))?;
        let jobid = job.jobid;
        let sess = self.inner.engine.open_session(*tenant).await?;
        sess.execute(&format!("DELETE FROM {JOB_TABLE} WHERE jobid = {jobid}"))
            .await?;
        Ok(jobid)
    }

    /// Read every job for `tenant`. The Postgres equivalent is
    /// `SELECT * FROM cron.job` filtered to the current tenant by RLS; here
    /// the filter is structural.
    pub async fn list_jobs(&self, tenant: &TenantId) -> Result<Vec<CronJob>> {
        self.ensure_tables(tenant).await?;
        let sess = self.inner.engine.open_session(*tenant).await?;
        let res = sess
            .execute(&format!(
                "SELECT jobid, jobname, schedule, command, active, last_run_unix_ms, last_status \
                 FROM {JOB_TABLE} ORDER BY jobid"
            ))
            .await?;
        let batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            ExecResult::Empty { tag } => {
                return Err(BasinError::internal(format!(
                    "expected rows from cron_job, got Empty({tag})"
                )))
            }
        };
        let mut out = Vec::new();
        for b in batches {
            let jobid = downcast_i64(&b, "jobid")?;
            let jobname = downcast_str(&b, "jobname")?;
            let schedule = downcast_str(&b, "schedule")?;
            let command = downcast_str(&b, "command")?;
            let active = downcast_i64(&b, "active")?;
            let last_run = downcast_opt_i64(&b, "last_run_unix_ms")?;
            let last_status = downcast_opt_str(&b, "last_status")?;
            for i in 0..b.num_rows() {
                out.push(CronJob {
                    jobid: jobid[i],
                    jobname: jobname[i].clone(),
                    schedule: schedule[i].clone(),
                    command: command[i].clone(),
                    active: active[i] != 0,
                    last_run: last_run[i].map(unix_ms_to_dt),
                    last_status: last_status[i].clone(),
                });
            }
        }
        Ok(out)
    }

    /// Return the principal `current_user` recorded for `jobid`. Used by
    /// the runner so each scheduled SQL runs as the principal that scheduled
    /// it.
    pub async fn job_username(&self, tenant: &TenantId, jobid: i64) -> Result<Option<String>> {
        self.ensure_tables(tenant).await?;
        let sess = self.inner.engine.open_session(*tenant).await?;
        let res = sess
            .execute(&format!(
                "SELECT username FROM {JOB_TABLE} WHERE jobid = {jobid}"
            ))
            .await?;
        let batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            _ => return Ok(None),
        };
        for b in batches {
            let names = downcast_str(&b, "username")?;
            if let Some(n) = names.into_iter().next() {
                return Ok(Some(n));
            }
        }
        Ok(None)
    }

    /// Append one row to `cron_job_run_details`.
    pub async fn record_run(&self, tenant: &TenantId, detail: &CronRunDetail) -> Result<()> {
        self.ensure_tables(tenant).await?;
        let sess = self.inner.engine.open_session(*tenant).await?;
        sess.execute(&format!(
            "INSERT INTO {RUN_DETAILS_TABLE} VALUES \
             ({id}, {jobid}, {runid}, {database}, {username}, {command}, \
              {status}, {return_message}, {start}, {end})",
            id = detail.id,
            jobid = detail.jobid,
            runid = detail.runid,
            database = sql_text(&detail.database),
            username = sql_text(&detail.username),
            command = sql_text(&detail.command),
            status = sql_text(detail.status.as_str()),
            return_message = sql_text(&detail.return_message),
            start = detail.start_time.timestamp_millis(),
            end = detail.end_time.timestamp_millis(),
        ))
        .await?;
        Ok(())
    }

    /// Read every run-detail row for `tenant`. Equivalent to
    /// `SELECT * FROM cron.job_run_details`.
    pub async fn list_run_details(&self, tenant: &TenantId) -> Result<Vec<CronRunDetail>> {
        self.ensure_tables(tenant).await?;
        let sess = self.inner.engine.open_session(*tenant).await?;
        let res = sess
            .execute(&format!(
                "SELECT id, jobid, runid, database, username, command, \
                        status, return_message, start_unix_ms, end_unix_ms \
                 FROM {RUN_DETAILS_TABLE} ORDER BY id"
            ))
            .await?;
        let batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            ExecResult::Empty { tag } => {
                return Err(BasinError::internal(format!(
                    "expected rows from cron_job_run_details, got Empty({tag})"
                )))
            }
        };
        let mut out = Vec::new();
        for b in batches {
            let id = downcast_i64(&b, "id")?;
            let jobid = downcast_i64(&b, "jobid")?;
            let runid = downcast_i64(&b, "runid")?;
            let database = downcast_str(&b, "database")?;
            let username = downcast_str(&b, "username")?;
            let command = downcast_str(&b, "command")?;
            let status = downcast_str(&b, "status")?;
            let return_message = downcast_str(&b, "return_message")?;
            let start = downcast_i64(&b, "start_unix_ms")?;
            let end = downcast_i64(&b, "end_unix_ms")?;
            for i in 0..b.num_rows() {
                out.push(CronRunDetail {
                    id: id[i],
                    jobid: jobid[i],
                    runid: runid[i],
                    job_pid: None,
                    database: database[i].clone(),
                    username: username[i].clone(),
                    command: command[i].clone(),
                    status: status_from_str(&status[i]),
                    return_message: return_message[i].clone(),
                    start_time: unix_ms_to_dt(start[i]),
                    end_time: unix_ms_to_dt(end[i]),
                });
            }
        }
        Ok(out)
    }

    /// Stamp `last_run` / `last_status` on a job row. The implementation
    /// reads the job, deletes the old row, and inserts a fresh one — Basin
    /// does not yet expose UPDATE-of-non-key-cols on a `WHERE id =` shape
    /// without a roundabout copy-on-write rewrite, which is fine to lean on.
    pub async fn touch_last_run(
        &self,
        tenant: &TenantId,
        jobid: i64,
        last_run: DateTime<Utc>,
        last_status: &str,
    ) -> Result<()> {
        self.ensure_tables(tenant).await?;
        let sess = self.inner.engine.open_session(*tenant).await?;
        // Use a parameterised UPDATE — Basin engine supports compound WHERE
        // with `=` on a literal i64.
        sess.execute(&format!(
            "UPDATE {JOB_TABLE} SET last_run_unix_ms = {ts}, last_status = {status} \
             WHERE jobid = {jobid}",
            ts = last_run.timestamp_millis(),
            status = sql_text(last_status),
        ))
        .await?;
        Ok(())
    }

    /// Allocate the next id for `cron_job_run_details`.
    pub(crate) async fn next_run_detail_id(&self, tenant: &TenantId) -> Result<i64> {
        let details = self.list_run_details(tenant).await?;
        Ok(details.iter().map(|d| d.id).max().unwrap_or(0) + 1)
    }
}

/// Parse a 5- or 6-field cron expression. The `cron` crate wants a 6-field
/// form (`sec min hour dom mon dow`); pg_cron uses 5 (`min hour dom mon dow`).
/// We normalise to 6 fields with a `0` second prefix.
pub(crate) fn parse_schedule(expr: &str) -> std::result::Result<cron::Schedule, ScheduleError> {
    let trimmed = expr.trim();
    let field_count = trimmed.split_whitespace().count();
    let normalized = if field_count == 5 {
        format!("0 {trimmed}")
    } else {
        trimmed.to_string()
    };
    use std::str::FromStr;
    cron::Schedule::from_str(&normalized).map_err(|e| ScheduleError::InvalidExpression {
        expr: expr.to_string(),
        source: e,
    })
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

fn sql_text(s: &str) -> String {
    let escaped = s.replace('\'', "''");
    format!("'{escaped}'")
}

fn unix_ms_to_dt(ms: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_else(Utc::now)
}

fn status_from_str(s: &str) -> JobStatus {
    match s {
        "succeeded" => JobStatus::Succeeded,
        "failed" => JobStatus::Failed,
        "rate_limited" => JobStatus::RateLimited,
        "skipped" => JobStatus::Skipped,
        _ => JobStatus::Failed,
    }
}

async fn list_table_names(sess: &basin_engine::TenantSession) -> Result<Vec<String>> {
    let res = sess.execute("SHOW TABLES").await?;
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { tag } => {
            return Err(BasinError::internal(format!(
                "expected rows from SHOW TABLES, got Empty({tag})"
            )))
        }
    };
    let mut out = Vec::new();
    for b in batches {
        let names = downcast_str(&b, "table_name")?;
        out.extend(names);
    }
    Ok(out)
}

fn downcast_i64(b: &arrow_array::RecordBatch, col: &str) -> Result<Vec<i64>> {
    let arr = b
        .column_by_name(col)
        .ok_or_else(|| BasinError::internal(format!("missing col {col}")))?;
    let arr = arr
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .ok_or_else(|| BasinError::internal(format!("col {col} not Int64")))?;
    let mut v = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        v.push(arr.value(i));
    }
    Ok(v)
}

fn downcast_opt_i64(b: &arrow_array::RecordBatch, col: &str) -> Result<Vec<Option<i64>>> {
    let arr = b
        .column_by_name(col)
        .ok_or_else(|| BasinError::internal(format!("missing col {col}")))?;
    let arr = arr
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .ok_or_else(|| BasinError::internal(format!("col {col} not Int64")))?;
    let mut v = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        v.push(if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        });
    }
    Ok(v)
}

fn downcast_str(b: &arrow_array::RecordBatch, col: &str) -> Result<Vec<String>> {
    let arr = b
        .column_by_name(col)
        .ok_or_else(|| BasinError::internal(format!("missing col {col}")))?;
    let arr = arr
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or_else(|| BasinError::internal(format!("col {col} not Utf8")))?;
    let mut v = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        v.push(arr.value(i).to_string());
    }
    Ok(v)
}

fn downcast_opt_str(b: &arrow_array::RecordBatch, col: &str) -> Result<Vec<Option<String>>> {
    let arr = b
        .column_by_name(col)
        .ok_or_else(|| BasinError::internal(format!("missing col {col}")))?;
    let arr = arr
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or_else(|| BasinError::internal(format!("col {col} not Utf8")))?;
    let mut v = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        v.push(if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i).to_string())
        });
    }
    Ok(v)
}
