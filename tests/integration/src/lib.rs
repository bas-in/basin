//! Basin integration tests. Real tests live under `tests/`.
//!
//! `benchmark` is the JSON-emit helper every viability / scaling test calls
//! after computing its result. Each report writes `benchmark/data/<id>.json`
//! at the repo root, where `benchmark/index.html` picks them up to render
//! the live results UI and `benchmark/RESULTS.md` is regenerated as a
//! plain-text summary of the same data.

pub mod benchmark;
