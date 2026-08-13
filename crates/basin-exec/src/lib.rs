//! Basin's physical execution layer.
//!
//! # Shape
//!
//! Pull-based: an [`Operator`] yields `Option<RecordBatch>` until exhausted.
//! Three properties follow from that choice, and each is a requirement rather
//! than a preference:
//!
//! - **Suspendable.** A pull-based operator can stop between batches and resume
//!   later, which is what `DECLARE CURSOR` / `FETCH` need. DataFusion's stream
//!   model makes this awkward.
//! - **Cancellable.** Cancellation is checked between batches, so a long query
//!   honours `statement_timeout`. DataFusion's UDFs are synchronous and cannot
//!   be preempted (`executor.rs:12`), which caps how well timeouts can work
//!   today.
//! - **Single-partition first.** Basin pins `target_partitions = 1`
//!   (`session.rs:2705`), and when the bulk-scan guard raises it, it
//!   simultaneously disables repartitioning of aggregates, joins and windows
//!   (`executor.rs:10984`). So no exchange operator, no Partial/Final aggregate
//!   split, and no partition-aware join is ever required. That is the single
//!   largest scope reduction in this migration.
//!
//! # Memory
//!
//! Operators account for their own memory against a budget. Basin wires a
//! bounded pool precisely so one heavy aggregate fails cleanly instead of
//! OOM-killing a shared multi-tenant node — but no `DiskManager` is configured
//! today, so current behaviour under pressure is *fail clean*, not spill.
//! [`ExecError::OutOfMemory`] preserves that. Real disk spill is deliberately
//! deferred; see `docs/migration/df-removal/08-ir-design.md` §7.

pub mod aggregate;
pub mod build;
pub mod correlated;
pub mod cte;
pub mod dml;
pub mod eval;
pub mod join;
pub mod lateral;
pub mod limit;
pub mod operator;
pub mod project;
pub mod recursive;
pub mod scan;
pub mod setop;
pub mod sort;
pub mod storage_source;
pub mod window;

pub use operator::{ExecError, Operator};
