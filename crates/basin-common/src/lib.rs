//! `basin-common` — types, errors, and telemetry shared by every Basin crate.
//!
//! Keep this crate small. Anything domain-specific (storage, catalog, SQL)
//! belongs in the relevant `basin-*` crate; only put something here if more
//! than one crate truly needs it. A bloated common crate is a sign that
//! abstraction boundaries are wrong.

#![forbid(unsafe_code)]

pub mod error;
pub mod ids;
pub mod telemetry;

pub use error::{BasinError, Result};
pub use ids::{Ident, PartitionKey, TableName, TenantId, MAX_IDENT_LEN};
