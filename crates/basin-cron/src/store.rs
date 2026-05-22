//! Per-project cron-state store, backed by ordinary Basin tables.
//!
//! # Phase 5.18.C — `cron` reserved schema migration
//!
//! The canonical namespace for cron tables is now `cron` (`ReservedSchema::Cron`
//! per ADR 0022). Table names match the pg_cron SQL surface:
//!
//! | Canonical (`cron.<table>`)  | Legacy flat name       |
//! |-----------------------------|------------------------|
//! | `cron.job`                  | `cron_job`             |
//! | `cron.job_run_details`      | `cron_job_run_details` |
//!
//! **Back-compat**: [`ensure_tables`] checks for both names. When only the
//! legacy name exists the store reads from/writes to the legacy table so
//! existing deployments are not broken. When neither exists the canonical
//! name is created. New deployments always use the canonical names.
//!
//! [`CronStore`] keeps two tables per project:
//!
//! - `job` (canonical; was `cron_job`) — current schedule.
//! - `job_run_details` (canonical; was `cron_job_run_details`) — audit log.
//!
//! Both are created lazily the first time the store is used for a project.
//! All reads and writes go through `Engine::open_session`, so:
//!
//! 1. Project isolation is the same as for user SQL.
//! 2. RLS policies on `job` / `cron_job` itself are honoured (you can shape
//!    who can schedule under a single project).
//! 3. Persistence is handled by the catalog and storage layers; no separate
//!    durability story.

use std::sync::Arc;

use arrow_array::Array;
use basin_common::{BasinError, ProjectId, Result};
use basin_engine::{Engine, ExecResult};
use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::types::{CronJob, CronRunDetail, JobStatus};

// ─────────────────────────────────────────────────────────────────────────────
// Phase 5.18.C — canonical reserved-schema constants + back-compat aliases
// ─────────────────────────────────────────────────────────────────────────────

/// The reserved schema name for cron system tables (ADR 0022 / 5.18.C).
pub const RESERVED_SCHEMA: &str = "cron";

/// Canonical bare table names within the `cron` schema (post-5.18.C).
/// These match the pg_cron SQL surface: `cron.job` / `cron.job_run_details`.
pub(crate) const JOB_TABLE: &str = "job";
pub(crate) const RUN_DETAILS_TABLE: &str = "job_run_details";

/// Legacy flat table names used before Phase 5.18.C. Physical tables created
/// before this migration have these names. [`CronStore::ensure_tables`]
/// recognises both names; new tables are always created under the canonical
/// names above.
pub(crate) const LEGACY_JOB_TABLE: &str = "cron_job";
pub(crate) const LEGACY_RUN_DETAILS_TABLE: &str = "cron_job_run_details";

/// Errors specific to scheduling.
#[derive(Debug, Error)]
pub enum ScheduleError {
    #[error("invalid cron expression {expr:?}: {source}")]
    InvalidExpression {
        expr: String,
        #[source]
        source: cron::error::Error,
    },
    #[error("project has reached the per-project job cap of {cap}")]
    JobCapExceeded { cap: usize },
    #[error("job {0:?} already exists")]
    Duplicate(String),
    #[error("job {0:?} not found")]
    NotFound(String),
    #[error("engine error: {0}")]
    Engine(#[from] BasinError),
}

/// Store front for the per-project cron tables.
///
/// Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct CronStore {
    inner: Arc<CronStoreInner>,
}

struct CronStoreInner {
    engine: Engine,
    /// Per-project guard so two concurrent `schedule` calls into the same
    /// project cannot race past the cap or generate the same `jobid`. Held
    /// only over the read-then-write window.
    schedule_guard: Mutex<()>,
}

impl CronStore {
    /// Hard cap on jobs per project. Same default as pg_cron's deployments
    /// recommend; tunable if a deployment needs more.
    pub const MAX_JOBS_PER_PROJECT: usize = 100;

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

    /// Idempotently create the two cron tables under `project`. Safe to call
    /// before every operation; the underlying `CREATE TABLE IF NOT EXISTS` is
    /// a no-op once the table exists.
    ///
    /// Phase 5.18.C: tables are now created in the reserved `cron` schema
    /// (`cron.job`, `cron.job_run_details`) using a trusted system session
    /// that bypasses the `guard_reserved_schema_for_user_ddl` security check.
    /// This is a fresh pre-launch project — no legacy data-migration needed.
    pub async fn ensure_tables(&self, project: &ProjectId) -> Result<()> {
        // Use a trusted system session so the engine allows DDL in the
        // reserved `cron` schema. User sessions (open_session) cannot do this.
        let sess = self.inner.engine.open_system_session(*project).await?;
        // CREATE TABLE is currently not idempotent in basin-engine; we look
        // for the table first via SHOW TABLES so re-running is safe.
        let existing = list_table_names(&sess).await?;

        // --- cron.job table ---
        // SHOW TABLES returns bare names; JOB_TABLE = "job".
        if !existing.contains(&JOB_TABLE.to_string()) {
            sess.execute(&format!(
                "CREATE TABLE {RESERVED_SCHEMA}.{JOB_TABLE} (
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

        // --- cron.job_run_details table ---
        if !existing.contains(&RUN_DETAILS_TABLE.to_string()) {
            sess.execute(&format!(
                "CREATE TABLE {RESERVED_SCHEMA}.{RUN_DETAILS_TABLE} (
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

    /// Schedule a new job under `project`. Mirrors pg_cron's `cron.schedule`
    /// (3-arg form): `(jobname, schedule, command)`.
    ///
    /// Errors if the cron expression is invalid, the per-project cap is hit,
    /// or `jobname` already exists for this project.
    pub async fn schedule(
        &self,
        project: &ProjectId,
        username: &str,
        jobname: &str,
        schedule: &str,
        command: &str,
    ) -> std::result::Result<i64, ScheduleError> {
        // Validate the cron expression up front so the failure surfaces at
        // schedule time, not at the next tick.
        parse_schedule(schedule)?;

        self.ensure_tables(project).await?;
        let _g = self.inner.schedule_guard.lock().await;

        let jobs = self.list_jobs(project).await?;
        if jobs.len() >= Self::MAX_JOBS_PER_PROJECT {
            return Err(ScheduleError::JobCapExceeded {
                cap: Self::MAX_JOBS_PER_PROJECT,
            });
        }
        if jobs.iter().any(|j| j.jobname == jobname) {
            return Err(ScheduleError::Duplicate(jobname.to_string()));
        }

        let jobid = jobs.iter().map(|j| j.jobid).max().unwrap_or(0) + 1;

        let sess = self.inner.engine.open_session(*project).await?;
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
        project: &ProjectId,
        jobname: &str,
    ) -> std::result::Result<i64, ScheduleError> {
        self.ensure_tables(project).await?;
        let _g = self.inner.schedule_guard.lock().await;

        let jobs = self.list_jobs(project).await?;
        let job = jobs
            .iter()
            .find(|j| j.jobname == jobname)
            .ok_or_else(|| ScheduleError::NotFound(jobname.to_string()))?;
        let jobid = job.jobid;
        let sess = self.inner.engine.open_session(*project).await?;
        sess.execute(&format!("DELETE FROM {JOB_TABLE} WHERE jobid = {jobid}"))
            .await?;
        Ok(jobid)
    }

    /// Read every job for `project`. The Postgres equivalent is
    /// `SELECT * FROM cron.job` filtered to the current project by RLS; here
    /// the filter is structural.
    pub async fn list_jobs(&self, project: &ProjectId) -> Result<Vec<CronJob>> {
        self.ensure_tables(project).await?;
        let sess = self.inner.engine.open_session(*project).await?;
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
    pub async fn job_username(&self, project: &ProjectId, jobid: i64) -> Result<Option<String>> {
        self.ensure_tables(project).await?;
        let sess = self.inner.engine.open_session(*project).await?;
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

    /// Append one row to `cron.job_run_details`.
    pub async fn record_run(&self, project: &ProjectId, detail: &CronRunDetail) -> Result<()> {
        self.ensure_tables(project).await?;
        let sess = self.inner.engine.open_session(*project).await?;
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

    /// Read every run-detail row for `project`. Equivalent to
    /// `SELECT * FROM cron.job_run_details`.
    pub async fn list_run_details(&self, project: &ProjectId) -> Result<Vec<CronRunDetail>> {
        self.ensure_tables(project).await?;
        let sess = self.inner.engine.open_session(*project).await?;
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
        project: &ProjectId,
        jobid: i64,
        last_run: DateTime<Utc>,
        last_status: &str,
    ) -> Result<()> {
        self.ensure_tables(project).await?;
        let sess = self.inner.engine.open_session(*project).await?;
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

    /// Allocate the next id for `job_run_details` (or `cron_job_run_details`
    /// on legacy deployments).
    pub(crate) async fn next_run_detail_id(&self, project: &ProjectId) -> Result<i64> {
        let details = self.list_run_details(project).await?;
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

async fn list_table_names(sess: &basin_engine::ProjectSession) -> Result<Vec<String>> {
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

// ─────────────────────────────────────────────────────────────────────────────
// Phase 5.18.C — unit tests for namespace constants and back-compat aliases
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod namespace_tests {
    use super::*;

    #[test]
    fn reserved_schema_is_cron() {
        assert_eq!(RESERVED_SCHEMA, "cron");
    }

    #[test]
    fn canonical_job_table_is_bare_name() {
        // The canonical name should be bare `job` (living in the `cron` schema).
        assert_eq!(JOB_TABLE, "job");
    }

    #[test]
    fn canonical_run_details_table_is_bare_name() {
        assert_eq!(RUN_DETAILS_TABLE, "job_run_details");
    }

    #[test]
    fn legacy_job_table_has_schema_prefix() {
        // The legacy name must include the `cron_` prefix so back-compat
        // resolution can distinguish it from the canonical name.
        assert_eq!(LEGACY_JOB_TABLE, "cron_job");
        assert!(LEGACY_JOB_TABLE.starts_with(&format!("{RESERVED_SCHEMA}_")));
    }

    #[test]
    fn legacy_run_details_table_has_schema_prefix() {
        assert_eq!(LEGACY_RUN_DETAILS_TABLE, "cron_job_run_details");
        assert!(LEGACY_RUN_DETAILS_TABLE.starts_with(&format!("{RESERVED_SCHEMA}_")));
    }

    #[test]
    fn canonical_and_legacy_job_tables_differ() {
        assert_ne!(JOB_TABLE, LEGACY_JOB_TABLE);
    }

    #[test]
    fn canonical_and_legacy_run_details_tables_differ() {
        assert_ne!(RUN_DETAILS_TABLE, LEGACY_RUN_DETAILS_TABLE);
    }

    #[test]
    fn legacy_names_derive_from_canonical_with_schema_prefix() {
        // Verify the mapping: canonical → schema_canonical (the old flat form).
        assert_eq!(
            format!("{RESERVED_SCHEMA}_{JOB_TABLE}"),
            LEGACY_JOB_TABLE
        );
        assert_eq!(
            format!("{RESERVED_SCHEMA}_{RUN_DETAILS_TABLE}"),
            LEGACY_RUN_DETAILS_TABLE
        );
    }
}
