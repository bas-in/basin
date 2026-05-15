//! `basin-cron` — a `pg_cron`-compatible scheduler for Basin.
//!
//! Apps using PostgreSQL's [`pg_cron`](https://github.com/citusdata/pg_cron)
//! extension drop in here. The scheduler ticks once a minute, scans every
//! resident project's `cron.job` table for due rows, and runs each due job's
//! SQL through the same per-project `Engine::open_session` path a user query
//! would take. Job runs are recorded in the per-project `cron.job_run_details`
//! audit table.
//!
//! ## SQL surface (matches pg_cron)
//!
//! ```sql
//! SELECT cron.schedule('audit-cleanup', '0 3 * * *',
//!   $$ DELETE FROM events WHERE ts < now() - interval '90 days' $$);
//! SELECT cron.unschedule('audit-cleanup');
//! SELECT * FROM cron.job;
//! SELECT * FROM cron.job_run_details;
//! ```
//!
//! In v0.1 the SQL surface is exposed as Rust API on [`CronStore`] only;
//! [`CronStore::schedule`] / [`CronStore::unschedule`] under the hood lower
//! to plain `INSERT`/`DELETE` against the per-project `cron.job` table, so
//! they share the same RLS/project-isolation guarantees as user SQL. A SQL
//! call-site rewriter that intercepts `SELECT cron.schedule(...)` is the
//! follow-on work; see the crate-level TODOs.
//!
//! ## Per-project guardrails
//!
//! - Hard cap on jobs per project ([`CronStore::MAX_JOBS_PER_PROJECT`], 100).
//! - Per-project per-minute rate limit ([`CronRunner::PER_MINUTE_RATE_LIMIT`],
//!   60). Once the budget is exhausted in a 60-second window, additional due
//!   jobs are recorded with status `rate_limited` and skipped until the
//!   window slides.
//! - Job SQL runs as the same `current_user` recorded at schedule time, so
//!   RLS predicates apply identically to scheduled and ad-hoc queries.
//!
//! ## Isolation
//!
//! Per-project `cron.job` tables live inside the project's namespace. Project A
//! cannot see, schedule against, or run B's jobs. The runner walks projects
//! one at a time; each project's job set is read and executed under that
//! project's session.

#![forbid(unsafe_code)]

mod runner;
mod store;
mod types;

pub use runner::{Clock, CronRunner, RunOutcome, SystemClock, TestClock};
pub use store::{CronStore, ScheduleError};
pub use types::{CronJob, CronRunDetail, JobStatus};
