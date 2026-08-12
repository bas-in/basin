//! The logical optimizer.
//!
//! Rules are applied to a fixpoint. DataFusion 53 runs its rule list up to
//! `max_passes = 3`, and the ablation in
//! `docs/migration/df-removal/05-optimizer-rules.md` found that plans are
//! genuine fixpoints rather than single-pass results — so the driver is
//! required in the first increment, not a later refinement.

pub mod driver;
pub mod projection;
pub mod pushdown;

pub use driver::{optimize, OptimizerRule};
