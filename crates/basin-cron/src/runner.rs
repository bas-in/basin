//! Cron tick loop. Once every 60 seconds (in production) or once per
//! [`CronRunner::tick`] (in tests), walk every resident tenant's `cron_job`
//! table, compute due jobs since the last tick, and execute each one.
//!
//! Per-tenant per-minute rate limiting is enforced before the engine call:
//! a tenant that has issued [`CronRunner::PER_MINUTE_RATE_LIMIT`] runs in
//! the past 60-second window has additional jobs recorded as
//! `RateLimited` instead of executed. A future bucketed token-bucket would
//! sharpen this; the v0.1 sliding window is good enough for the smoke test.
//!
//! ## Clock injection
//!
//! [`CronRunner`] takes a [`Clock`] so tests can drive ticks at deterministic
//! times. Production code uses [`SystemClock`] which delegates to `Utc::now`.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use basin_common::{Result, TenantId};
use basin_engine::{Engine, ExecResult};
use chrono::{DateTime, Duration, Utc};
use tokio::sync::Mutex;

use crate::store::CronStore;
use crate::types::{CronRunDetail, JobStatus};

/// Source of time. Production uses [`SystemClock`]; tests use
/// [`TestClock`] to drive ticks at deterministic instants.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> DateTime<Utc>;
}

/// Real wall-clock, delegates to [`Utc::now`].
#[derive(Clone, Copy, Default, Debug)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Manually-advanced clock. `set` overrides the current time; the runner
/// reads it on every tick. Cheap to clone; the inner cell is an
/// [`AtomicI64`] of unix milliseconds so [`Clock::now`] is sync and
/// lock-free.
#[derive(Clone, Debug)]
pub struct TestClock {
    /// Unix epoch milliseconds. Monotonicity is the caller's responsibility.
    ms: Arc<AtomicI64>,
}

impl TestClock {
    pub fn new(t: DateTime<Utc>) -> Self {
        Self {
            ms: Arc::new(AtomicI64::new(t.timestamp_millis())),
        }
    }

    pub fn set(&self, t: DateTime<Utc>) {
        self.ms.store(t.timestamp_millis(), Ordering::Relaxed);
    }

    pub fn advance(&self, d: Duration) {
        self.ms
            .fetch_add(d.num_milliseconds(), Ordering::Relaxed);
    }
}

impl Clock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp_millis(self.ms.load(Ordering::Relaxed))
            .unwrap_or_else(|| Utc::now())
    }
}

/// Outcome of one job run. Surfaced from [`CronRunner::tick`] so tests can
/// assert without re-reading `cron_job_run_details`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunOutcome {
    pub tenant: TenantId,
    pub jobid: i64,
    pub jobname: String,
    pub status: JobStatus,
    pub return_message: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
}

/// Driver loop. Hold one of these for the lifetime of the process; call
/// [`CronRunner::tick`] from a 60-second timer in production, or directly
/// from tests at controlled instants.
#[derive(Clone)]
pub struct CronRunner {
    inner: Arc<RunnerInner>,
}

struct RunnerInner {
    store: CronStore,
    clock: Arc<dyn Clock>,
    /// Tenants the runner is responsible for. Cron is opt-in per tenant —
    /// the router (or test) registers each tenant once via
    /// [`CronRunner::register_tenant`].
    tenants: Mutex<Vec<TenantId>>,
    /// Per-tenant timestamp of the most recent tick. New runs only fire for
    /// schedule-times strictly after this anchor.
    last_tick: Mutex<HashMap<TenantId, DateTime<Utc>>>,
    /// Per-tenant rolling 60-second log of run-start timestamps. When the
    /// log has [`CronRunner::PER_MINUTE_RATE_LIMIT`] entries within 60s,
    /// further runs in that window are recorded as rate-limited.
    rate_log: Mutex<HashMap<TenantId, VecDeque<DateTime<Utc>>>>,
}

impl CronRunner {
    /// Soft cap on runs per tenant per 60-second window.
    pub const PER_MINUTE_RATE_LIMIT: usize = 60;

    /// Build a runner over `engine` with `clock`.
    pub fn new(engine: Engine, clock: Arc<dyn Clock>) -> Self {
        Self {
            inner: Arc::new(RunnerInner {
                store: CronStore::new(engine),
                clock,
                tenants: Mutex::new(Vec::new()),
                last_tick: Mutex::new(HashMap::new()),
                rate_log: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Convenience: build with [`SystemClock`].
    pub fn with_system_clock(engine: Engine) -> Self {
        Self::new(engine, Arc::new(SystemClock))
    }

    /// Reference to the underlying [`CronStore`]. Useful for tests that
    /// want to inspect or seed jobs directly.
    pub fn store(&self) -> &CronStore {
        &self.inner.store
    }

    /// Mark `tenant` as resident. Idempotent. The first registration also
    /// anchors the per-tenant `last_tick` to "now" so jobs don't fire for
    /// schedule-times in the past.
    pub async fn register_tenant(&self, tenant: TenantId) {
        let mut tenants = self.inner.tenants.lock().await;
        if !tenants.contains(&tenant) {
            tenants.push(tenant);
            let mut anchors = self.inner.last_tick.lock().await;
            anchors.entry(tenant).or_insert_with(|| self.inner.clock.now());
        }
    }

    /// Run one tick. Walks every registered tenant, computes due jobs, and
    /// executes each due job through the engine. Returns the per-job
    /// outcomes for direct test assertions; `cron_job_run_details` is
    /// also updated.
    ///
    /// "Due" means: there exists a schedule-time `t` such that
    /// `last_tick(tenant) < t <= now()`. The first matching `t` per job
    /// per tick fires once; multiple matches in one tick still fire once
    /// (Postgres pg_cron has the same coalescing behaviour for missed ticks).
    pub async fn tick(&self) -> Result<Vec<RunOutcome>> {
        let now = self.inner.clock.now();
        let tenants = self.inner.tenants.lock().await.clone();
        let mut outcomes = Vec::new();
        for tenant in tenants {
            let tenant_outcomes = self.tick_tenant(&tenant, now).await?;
            outcomes.extend(tenant_outcomes);
        }
        Ok(outcomes)
    }

    /// Test-only public alias for [`Self::tick`]. Documented in the SQL
    /// surface README so tests have a stable reference name.
    pub async fn tick_now(&self) -> Result<Vec<RunOutcome>> {
        self.tick().await
    }

    async fn tick_tenant(
        &self,
        tenant: &TenantId,
        now: DateTime<Utc>,
    ) -> Result<Vec<RunOutcome>> {
        let last = {
            let mut anchors = self.inner.last_tick.lock().await;
            let anchor = anchors.entry(*tenant).or_insert(now);
            let prev = *anchor;
            *anchor = now;
            prev
        };

        let jobs = self.inner.store.list_jobs(tenant).await?;
        let mut outcomes = Vec::new();
        for job in jobs {
            // Inactive jobs: record a Skipped row so the audit log is
            // honest, but don't run the SQL.
            if !job.active {
                let id = self.inner.store.next_run_detail_id(tenant).await?;
                let detail = CronRunDetail {
                    id,
                    jobid: job.jobid,
                    runid: id,
                    job_pid: None,
                    database: tenant.as_prefix(),
                    username: String::new(),
                    command: job.command.clone(),
                    status: JobStatus::Skipped,
                    return_message: "job inactive".into(),
                    start_time: now,
                    end_time: now,
                };
                self.inner.store.record_run(tenant, &detail).await?;
                outcomes.push(RunOutcome {
                    tenant: *tenant,
                    jobid: job.jobid,
                    jobname: job.jobname.clone(),
                    status: JobStatus::Skipped,
                    return_message: detail.return_message,
                    start_time: now,
                    end_time: now,
                });
                continue;
            }

            let schedule = match crate::store::parse_schedule(&job.schedule) {
                Ok(s) => s,
                Err(e) => {
                    let id = self.inner.store.next_run_detail_id(tenant).await?;
                    let detail = CronRunDetail {
                        id,
                        jobid: job.jobid,
                        runid: id,
                        job_pid: None,
                        database: tenant.as_prefix(),
                        username: String::new(),
                        command: job.command.clone(),
                        status: JobStatus::Failed,
                        return_message: format!("invalid schedule: {e}"),
                        start_time: now,
                        end_time: now,
                    };
                    self.inner.store.record_run(tenant, &detail).await?;
                    outcomes.push(RunOutcome {
                        tenant: *tenant,
                        jobid: job.jobid,
                        jobname: job.jobname.clone(),
                        status: JobStatus::Failed,
                        return_message: detail.return_message,
                        start_time: now,
                        end_time: now,
                    });
                    continue;
                }
            };

            // "Due" check: any upcoming run after `last` that is `<= now`.
            // The cron crate's `after(last)` iterator yields exactly that.
            let due = schedule.after(&last).next().map(|t| t <= now).unwrap_or(false);
            if !due {
                continue;
            }

            // Per-tenant rate limit. Window is 60s.
            let rate_ok = {
                let mut log = self.inner.rate_log.lock().await;
                let entry = log.entry(*tenant).or_default();
                let cutoff = now - Duration::seconds(60);
                while entry.front().is_some_and(|t| *t < cutoff) {
                    entry.pop_front();
                }
                if entry.len() >= Self::PER_MINUTE_RATE_LIMIT {
                    false
                } else {
                    entry.push_back(now);
                    true
                }
            };

            if !rate_ok {
                let id = self.inner.store.next_run_detail_id(tenant).await?;
                let detail = CronRunDetail {
                    id,
                    jobid: job.jobid,
                    runid: id,
                    job_pid: None,
                    database: tenant.as_prefix(),
                    username: String::new(),
                    command: job.command.clone(),
                    status: JobStatus::RateLimited,
                    return_message: format!(
                        "rate-limited: > {} runs/60s",
                        Self::PER_MINUTE_RATE_LIMIT
                    ),
                    start_time: now,
                    end_time: now,
                };
                self.inner.store.record_run(tenant, &detail).await?;
                outcomes.push(RunOutcome {
                    tenant: *tenant,
                    jobid: job.jobid,
                    jobname: job.jobname.clone(),
                    status: JobStatus::RateLimited,
                    return_message: detail.return_message,
                    start_time: now,
                    end_time: now,
                });
                continue;
            }

            // Execute the job's SQL as the principal that scheduled it.
            let username = self
                .inner
                .store
                .job_username(tenant, job.jobid)
                .await?
                .unwrap_or_else(|| basin_engine::ANONYMOUS_USER.to_string());
            let start = self.inner.clock.now();
            let res = self
                .run_one(*tenant, &username, &job.command)
                .await;
            let end = self.inner.clock.now();
            let (status, message) = match res {
                Ok(tag) => (JobStatus::Succeeded, tag),
                Err(e) => (JobStatus::Failed, format!("{e}")),
            };

            // Record run-detail audit row.
            let id = self.inner.store.next_run_detail_id(tenant).await?;
            let detail = CronRunDetail {
                id,
                jobid: job.jobid,
                runid: id,
                job_pid: None,
                database: tenant.as_prefix(),
                username: username.clone(),
                command: job.command.clone(),
                status,
                return_message: message.clone(),
                start_time: start,
                end_time: end,
            };
            self.inner.store.record_run(tenant, &detail).await?;
            // Touch last_run on the job row.
            let _ = self
                .inner
                .store
                .touch_last_run(tenant, job.jobid, start, status.as_str())
                .await;

            outcomes.push(RunOutcome {
                tenant: *tenant,
                jobid: job.jobid,
                jobname: job.jobname.clone(),
                status,
                return_message: message,
                start_time: start,
                end_time: end,
            });
        }
        Ok(outcomes)
    }

    /// Execute one statement under `tenant` as `username`. Returns the
    /// command tag on success.
    async fn run_one(
        &self,
        tenant: TenantId,
        username: &str,
        sql: &str,
    ) -> Result<String> {
        let sess = self
            .inner
            .store
            .engine()
            .open_session_as(tenant, username)
            .await?;
        match sess.execute(sql).await? {
            ExecResult::Empty { tag } => Ok(tag),
            ExecResult::Rows { batches, .. } => {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                Ok(format!("SELECT {rows}"))
            }
        }
    }

    /// Spawn the production tick loop on the current tokio runtime. Polls
    /// every 60 seconds. The returned [`tokio::task::JoinHandle`] can be
    /// used to abort the loop on shutdown.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Err(e) = self.tick().await {
                    tracing::warn!(error = %e, "cron tick failed");
                }
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        })
    }
}
