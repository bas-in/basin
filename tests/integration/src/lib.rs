//! Basin integration tests. Real tests live under `tests/`.
//!
//! `dashboard` is the JSON-emit helper every viability / scaling test calls
//! after computing its result. Each report writes `dashboard/data/<id>.json`
//! at the repo root, where `dashboard/index.html` picks them up to render
//! the live results UI.

pub mod dashboard;
