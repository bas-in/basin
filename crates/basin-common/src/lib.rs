//! `basin-common` — types, errors, and telemetry shared by every Basin crate.
//!
//! Keep this crate small. Anything domain-specific (storage, catalog, SQL)
//! belongs in the relevant `basin-*` crate; only put something here if more
//! than one crate truly needs it. A bloated common crate is a sign that
//! abstraction boundaries are wrong.

#![forbid(unsafe_code)]

pub mod error;
pub mod events;
pub mod ids;
pub mod project_counters;
pub mod region;
pub mod telemetry;
pub mod types;

pub use error::{BasinError, Result};
pub use events::{ChangeEvent, ChangeEventSink, ChangeOp, EventSinkRegistry};
pub use ids::{
    Ident, PartitionKey, ProjectId, QualifiedTableName, SchemaName, TableName, MAX_IDENT_LEN,
};
pub use project_counters::{
    AcquireResult, CapLabel, HistogramSnapshot, LeaseMetrics, RenewResult, ReplicaLeaseSnapshot,
};
pub use region::{is_home_region, local_region, DEFAULT_REGION, REGION_ENV};
pub use telemetry::{ProjectCounterRegistry, ProjectCounters, ProjectCountersSnapshot};
