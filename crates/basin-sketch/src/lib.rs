//! `basin-sketch` — probabilistic data-structure types shared across Basin
//! crates.
//!
//! Why this crate exists.  Two sketch types — HyperLogLog (cardinality) and
//! t-digest (quantiles) — are needed in two architectural positions:
//!
//! 1. **Writer-side computation** (`basin-storage`, Phase 5.14.B2): the
//!    storage writer iterates each `RecordBatch` and emits sketches per
//!    column at file-commit time.
//! 2. **Query-time merge** (`basin-engine`, Phase 5.14.B5): the
//!    `APPROX_COUNT_DISTINCT` and `APPROX_PERCENTILE` UDFs short-circuit
//!    the scan when every candidate file's catalog entry carries the
//!    sketch.
//!
//! The dependency graph constraint: `basin-storage` cannot depend on
//! `basin-engine` (the direction is engine → storage).  Sketches must
//! therefore live in a crate that BOTH can depend on.  Hence
//! `basin-sketch`.  See locked decision 1 in
//! `docs/roadmap/2026-05-19-decomposition.md` and ADR 0017.

#![forbid(unsafe_code)]

pub mod hll;
pub mod tdigest;

/// `BASIN_QUERY_SHAPE_SEED` — fixed 64-bit seed for the xxh3_64 hash
/// used by `QueryShape` (Phase 5.16.A).  Not a sketch, but lives here
/// because it is a project-wide constant referenced by both
/// `basin-engine` (recording) and (later) `basin-cloud` ingest
/// (correlation).  Documented in ADR 0017.
///
/// **Do not rotate.**  This seed is the join key across the entire
/// OSS → cloud → cross-project aggregate pipeline.  Changing it
/// invalidates every historical shape record in basin-cloud.
pub const QUERY_SHAPE_SEED: u64 = 0xBA51_4145_7E11_5A95;
