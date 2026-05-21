//! Reusable workload primitives.
//!
//! Each primitive consumes `(&BenchHarness, &BenchConfig)` plus a small
//! shape-specific argument (project index, table size, …) and returns a
//! [`crate::sample::SampleReport`] plus shape-specific extras.
//!
//! The primitives intentionally do not call into the JSON emitter — that
//! lives in the profile layer so one workload can power multiple cards
//! (e.g. a `point_select` shape can be reported as both a viability bar and
//! a scaling-curve point).

pub mod insert;
pub mod scan;
pub mod select;
pub mod wasm;

pub use insert::bulk_insert;
pub use scan::scan;
pub use select::point_select;
pub use wasm::{wasm_invoke, wasm_invoke_concurrent};
