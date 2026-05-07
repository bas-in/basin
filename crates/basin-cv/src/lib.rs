//! `basin-cv` — TimescaleDB-style continuous aggregates for Basin.
//!
//! A "continuous aggregate" (CV) is a materialised view over a base table.
//! It pre-computes an aggregate query, stores the result as a regular table
//! in the catalog, and re-runs the query on a configurable cadence. Reads
//! of the CV go through the engine's normal `SELECT` path — from storage's
//! perspective the CV is just another Parquet-backed table.
//!
//! ## SQL surface (target shape, v0.2)
//!
//! ```sql
//! -- Define a CV: hourly request counts per tenant.
//! CREATE MATERIALIZED VIEW hourly_requests
//! WITH (basin.continuous, basin.refresh_interval = '1 hour') AS
//! SELECT
//!   date_trunc('hour', ts) AS bucket,
//!   count(*) AS requests
//! FROM events
//! GROUP BY bucket;
//!
//! -- Query the CV (just like any table).
//! SELECT bucket, requests FROM hourly_requests ORDER BY bucket DESC LIMIT 24;
//!
//! -- Manually refresh.
//! CALL basin.refresh_continuous_aggregate('hourly_requests', NULL, NULL);
//!
//! -- Drop.
//! DROP MATERIALIZED VIEW hourly_requests;
//! ```
//!
//! In v0.1 the SQL surface is exposed as a Rust API on [`CvStore`] /
//! [`CvRefresher`] only. The matching SQL interceptor lives in
//! `basin-engine`'s `cv_glue` module today as a deliberate no-op stub —
//! when the executor's pre-parse hook learns to recognise the
//! `WITH (basin.continuous, ...)` option-list shape and the
//! `basin.refresh_continuous_aggregate(name, start, end)` schema-qualified
//! procedure-call form, the registration will land there and `Engine::new`
//! will pick it up automatically. The same staging pattern is used by
//! `basin-cron` for `cron.schedule(...)` and `basin-net` for
//! `http_get(...)` / `net.http_post(...)`.
//!
//! ## Refresh model (v0.1)
//!
//! On each [`CvRefresher::tick`] (or [`basin_shard::Shard::run_cv_refresh`]):
//!
//! 1. For each registered CV under each resident tenant, check whether
//!    `now - last_refreshed_at >= refresh_interval`. Skip if not due.
//! 2. Re-execute the CV's source SQL against the tenant's session, in
//!    full. v0.2 will narrow this to an incremental scan over rows whose
//!    bucket key exceeds `last_bucket_max` (similar to TimescaleDB's
//!    "real-time" continuous aggregates).
//! 3. Write the result batches to a fresh Parquet file under the CV's
//!    table prefix.
//! 4. Atomically swap the catalog manifest (`replace_data_files`) so
//!    subsequent reads see the new materialisation.
//! 5. Stamp `last_refreshed_at_unix_ms` (and `last_bucket_max_unix_ms`,
//!    when the CV's bucket column is detectable) back into the catalog.
//!
//! ## Read model (v0.1)
//!
//! Reads of the CV go through `SELECT * FROM <cv_name>` against the engine,
//! which loads the CV exactly like a regular table — the materialised
//! Parquet file is the entire result. The "live tail" that TimescaleDB's
//! real-time aggregates merge in (raw rows newer than `last_bucket_max`)
//! is **not** included for v0.1: a SELECT issued between two refresh ticks
//! may return slightly stale data. v0.2 will merge in the tail at read
//! time. The integration test documents this gap explicitly.
//!
//! ## Per-tenant guardrails
//!
//! - The CV refresher walks tenants registered with
//!   [`CvRefresher::register_tenant`] only — a tenant that hasn't been
//!   registered never gets a refresh tick. Mirrors `basin_cron`'s
//!   `register_tenant` / `tick` surface.
//! - Each refresh runs through `Engine::open_session(tenant)` so the
//!   storage layer's per-tenant concurrency permits, RLS predicates,
//!   and the noisy-tenant detector all apply identically to scheduled
//!   and ad-hoc queries.
//! - There is no cross-tenant CV: tenant A cannot define a CV that
//!   reads from B's table. The check is structural — the source SQL
//!   only has access to A's namespace through A's session.
//!
//! ## Isolation
//!
//! Per-tenant CV state is keyed on [`TenantId`] inside [`CvStore`]'s
//! catalog. Tenant A's `register_cv` cannot affect B's CVs; A's
//! refresh tick never reads or writes B's data files. Documented and
//! tested by `tests/unit_test_isolation.rs`.

#![forbid(unsafe_code)]

mod refresher;
mod store;
mod types;

pub use refresher::{Clock, CvRefresher, RefreshOutcome, SystemClock, TestClock};
pub use store::{CvStore, RegisterError};
pub use types::{CvRefreshOutcome, CvRefreshState, CvSpec};
