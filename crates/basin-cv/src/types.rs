//! Public types for `basin-cv`.
//!
//! [`CvSpec`] is the in-memory representation of a continuous aggregate
//! definition — the "what" the refresher consumes. [`CvRefreshState`]
//! mirrors the bookkeeping fields the catalog persists alongside it.
//! [`CvRefreshOutcome`] is the discriminator returned from a single refresh
//! attempt so callers (and tests) can assert on whether a tick actually
//! re-materialised the CV.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// One continuous-aggregate definition. The natural key is `(tenant, name)`;
/// `name` is the same identifier used to read the CV via SQL
/// (`SELECT * FROM <name>`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CvSpec {
    /// CV name. Doubles as the catalog table name the materialised result
    /// is stored under, so it must satisfy the same identifier rules as a
    /// regular table.
    pub name: String,
    /// Source table the SQL reads from. Stored separately from the SQL
    /// itself so v0.2's incremental refresh planner can pull it out without
    /// re-parsing.
    pub source_table: String,
    /// Full SQL the refresher re-executes on each tick. Stored verbatim;
    /// the engine reparses at refresh time. When the body matches the
    /// supported time-bucket shape, the refresher rewrites it to inject a
    /// `WHERE <bucket_col> >= <last_bucket_max>` predicate and only
    /// re-aggregates the watermark-and-newer rows.
    pub query_sql: String,
    /// Refresh cadence. Ticks at intervals shorter than this are no-ops.
    /// Stored as seconds rather than `Duration` to keep the catalog
    /// payload a single `BIGINT`.
    pub refresh_interval_secs: u64,
    /// Wall-clock of the last successful refresh tick. `None` until the
    /// first refresh completes.
    pub last_refreshed_at: Option<DateTime<Utc>>,
    /// Watermark for the latest source-table bucket the materialised data
    /// covers. `None` after registration, before the first refresh. The
    /// incremental refresh path reads this as the lower bound on the next
    /// refresh's source-row scan (re-aggregates rows where
    /// `bucket_col >= last_bucket_max`); the full-rebuild path also writes
    /// it back so the next tick can do incremental.
    pub last_bucket_max: Option<DateTime<Utc>>,
}

/// Subset of [`CvSpec`] the catalog updates on every successful refresh.
/// Carved out so the refresher can write back the bookkeeping fields
/// without round-tripping the immutable definition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CvRefreshState {
    pub last_refreshed_at: DateTime<Utc>,
    pub last_bucket_max: Option<DateTime<Utc>>,
    /// Number of rows materialised in the latest refresh. Surfaced for
    /// observability; the catalog does not store this — it lives on the
    /// `RefreshOutcome` and the snapshot summary instead.
    pub last_row_count: u64,
}

/// Outcome of one refresh attempt. Returned from [`crate::CvRefresher::tick`]
/// (one per CV per tick) and consumed by tests / observability layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CvRefreshOutcome {
    /// The CV was re-materialised. `rows_written` is the row count of
    /// the new Parquet file.
    Refreshed { rows_written: u64 },
    /// The CV is registered but the interval since `last_refreshed_at`
    /// is below `refresh_interval_secs`. No work was done.
    NotDue,
    /// Refresh failed mid-flight. The CV's prior materialisation is
    /// untouched (the refresher writes a new file before swapping the
    /// catalog manifest, so a failed swap leaves the old file in place).
    /// The string is the human-readable error.
    Failed(String),
}
