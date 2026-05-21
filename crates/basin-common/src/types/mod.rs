//! Shared type utilities for Basin logical types.
//!
//! Each sub-module provides the shared semantics for one Basin logical type
//! that rides on a standard Arrow physical type with a `BASIN_TYPE` metadata
//! marker. The metadata marker is defined in `basin-engine::types`; the
//! *behavioural* helpers (case-folding, comparisons, etc.) live here so they
//! can be shared across crates without pulling in the full engine dependency.

pub mod citext;
