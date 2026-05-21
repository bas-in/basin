//! Profile registry.
//!
//! A profile = (name, list of shapes). Each shape is a (kind, id, runner)
//! triple: the runner takes a [`BenchHarness`] + [`BenchConfig`], produces
//! a [`crate::emit`] sidecar, and returns whether the bar passed.
//!
//! Profiles are intentionally thin: the heavy lifting is the workload
//! primitive. The profile is the "what bar do I assert + how do I label
//! the card" wrapper.

pub mod multi_instance;
pub mod noisy_neighbor;
pub mod vortex_vs_parquet;
pub mod wasm;

use std::path::PathBuf;
use std::pin::Pin;

use anyhow::Result;
use serde_json::Value;

use crate::config::BenchConfig;
use crate::harness::BenchHarness;

/// Card kind — mirrors the dashboard's three sections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShapeKind {
    Viability,
    Scaling,
    Compare,
}

/// One bench shape (= one dashboard card).
pub struct BenchShape {
    pub kind: ShapeKind,
    pub id: &'static str,
    pub name: &'static str,
    pub claim: &'static str,
    /// The runner. Takes the built harness + the live config; returns the
    /// path it wrote and `(passed, summary_json)` so the runner can render
    /// a Markdown summary.
    #[allow(clippy::type_complexity)]
    pub run: for<'a> fn(
        &'a BenchHarness,
        &'a BenchConfig,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Result<(PathBuf, bool, Value)>> + 'a>,
    >,
}

/// A bench profile: a config builder + the shapes that the profile owns.
///
/// The runner iterates `(profile, shape)` pairs. Profiles can override the
/// base config per shape via `shape_override`.
pub struct BenchProfile {
    pub name: &'static str,
    /// Build the baseline `BenchConfig` for this profile. The runner clones
    /// this and applies per-shape overrides on top.
    pub baseline: fn() -> BenchConfig,
    pub shapes: &'static [BenchShape],
}

/// Every registered profile. `scripts/run-bench.sh all` iterates this.
pub fn all() -> Vec<&'static BenchProfile> {
    vec![
        &vortex_vs_parquet::PROFILE,
        &noisy_neighbor::PROFILE,
        &multi_instance::PROFILE,
        &wasm::PROFILE,
    ]
}

/// Look up a profile by name. Returns `None` if no match.
pub fn by_name(name: &str) -> Option<&'static BenchProfile> {
    all().into_iter().find(|p| p.name == name)
}
