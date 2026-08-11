//! Unified benchmark harness for Basin.
//!
//! ## Why this crate exists
//!
//! Before this crate, Basin's perf surface was three disjoint things:
//!
//! 1. ~30 integration "viability" / "scaling" / "smoke" tests under
//!    `tests/integration/tests/`, each rebuilding its own engine, its own
//!    workload, its own JSON emitter (via `basin_integration_tests::benchmark`).
//! 2. Three `criterion` benches (`memtable_bench`, `query_stats_overhead`,
//!    `filter_eval`) that didn't share any setup code with (1) and never
//!    populated the dashboard.
//! 3. A bespoke `benchmark/vortex_compare/` binary outside the workspace.
//!
//! That meant every threshold lived in a different file, the JSON shape
//! drifted, and adding a new "swap the engine config, rerun everything"
//! profile was a multi-day rewrite per dimension.
//!
//! This crate is the shared core: **one** [`BenchHarness`] that takes a
//! [`BenchConfig`] (storage backend, engine flags, workload knobs), builds
//! the engine + storage, and exposes a small set of workload primitives
//! ([`workload::point_select`], [`workload::bulk_insert`],
//! [`workload::scan`](fn@workload::scan), [`workload::wasm_invoke`], …) that record latencies
//! into an [`hdrhistogram::Histogram`] and emit a JSON sidecar matching the
//! shape already consumed by `benchmark/index_*.html` and `bundle.py`.
//!
//! ## JSON-shape contract — DO NOT BREAK
//!
//! The schema written by [`emit::report_viability`] / [`emit::report_scaling`]
//! is the same one `basin_integration_tests::benchmark` writes. The dashboard
//! viewer (`benchmark/assets/dashboard.js`) and the bundler
//! (`benchmark/bundle.py`) both depend on it. The integration tests crate is
//! still the authoritative emitter for the existing 80+ JSON files; this
//! crate emits the *same* shape into the *same* directories so a profile run
//! is indistinguishable from the legacy per-test reports. Adding a *new*
//! per-row field is safe (the dashboard ignores unknown keys); renaming or
//! removing one is not.
//!
//! ## Adding a profile
//!
//! 1. Define a [`BenchProfile`] (a [`BenchConfig`] builder + a list of
//!    shapes), put it under `src/profiles/`.
//! 2. Register it in [`profiles::all`] so `scripts/run-bench.sh all` picks
//!    it up.
//! 3. (Optional) Add a criterion bench under `benches/` that calls the same
//!    workload primitive — both surfaces are first-class.
//!
//! ## Scope discipline
//!
//! This crate does NOT:
//! - own engine config knob *meaning* (those live in `basin-engine`);
//! - own the dashboard renderer (that lives in `benchmark/`);
//! - reimplement the JSON emitter that integration tests already use — we
//!   either re-export or write directly to the same files.

pub mod config;
pub mod emit;
pub mod harness;
pub mod profiles;
pub mod runner;
pub mod sample;
pub mod workload;

pub use config::{BenchConfig, EngineFlags, StorageBackend, WorkloadKnobs};
pub use harness::BenchHarness;
pub use profiles::{BenchProfile, BenchShape, ShapeKind};
pub use runner::BenchSuite;
pub use sample::{LatencySamples, SampleReport};
