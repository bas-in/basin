//! [`BenchSuite`] — iterates `profiles × shapes`, builds the harness once
//! per profile, runs each shape, and renders a Markdown summary in the
//! `benchmark/RESULTS_*.md` style.
//!
//! The runner deliberately rebuilds the harness once per profile (not per
//! shape) so shapes share the seeded dataset. Shapes that need a different
//! engine flag clone the config and re-apply the flag inside the workload
//! (see `vortex_vs_parquet`) — the dataset is format-agnostic so this is
//! safe.

use std::fmt::Write as _;

use anyhow::Result;
use serde_json::Value;

use crate::config::BenchConfig;
use crate::harness::BenchHarness;
use crate::profiles::{self, BenchProfile};

/// One shape's result, for the Markdown render.
pub struct ShapeResult {
    pub profile: &'static str,
    pub shape_id: &'static str,
    pub shape_name: &'static str,
    pub passed: bool,
    pub summary: Value,
}

/// Suite runner.
pub struct BenchSuite;

impl BenchSuite {
    /// Run every shape of `profile`. Builds the harness from the profile's
    /// baseline config (the `override_cfg` closure can mutate it first, e.g.
    /// to shrink iterations for CI).
    pub async fn run_profile(
        profile: &BenchProfile,
        override_cfg: impl Fn(&mut BenchConfig),
    ) -> Result<Vec<ShapeResult>> {
        let mut cfg = (profile.baseline)();
        override_cfg(&mut cfg);
        let harness = BenchHarness::build(&cfg).await?;
        let mut results = Vec::new();
        for shape in profile.shapes {
            let (_, passed, summary) = (shape.run)(&harness, &cfg).await?;
            results.push(ShapeResult {
                profile: profile.name,
                shape_id: shape.id,
                shape_name: shape.name,
                passed,
                summary,
            });
        }
        Ok(results)
    }

    /// Run a named profile, or every profile when `name == "all"`.
    ///
    /// `override_cfg` is `Fn` (not `FnMut`/`FnOnce`) so it can be applied
    /// once per profile in the "all" path; pass `Clone`-able captures (e.g.
    /// a `PathBuf` output dir) by reference into the closure body.
    pub async fn run(
        name: &str,
        override_cfg: impl Fn(&mut BenchConfig),
    ) -> Result<Vec<ShapeResult>> {
        let mut all = Vec::new();
        if name == "all" {
            for p in profiles::all() {
                all.extend(Self::run_profile(p, &override_cfg).await?);
            }
        } else {
            let p = profiles::by_name(name)
                .ok_or_else(|| anyhow::anyhow!("unknown profile: {name}"))?;
            all.extend(Self::run_profile(p, &override_cfg).await?);
        }
        Ok(all)
    }

    /// Render results as a Markdown table, matching the prose style of
    /// `benchmark/RESULTS_localfs.md`.
    pub fn render_markdown(results: &[ShapeResult]) -> String {
        let mut s = String::new();
        let _ = writeln!(s, "# Basin benchmark results\n");
        let total = results.len();
        let passed = results.iter().filter(|r| r.passed).count();
        let _ = writeln!(s, "{passed}/{total} shapes passed.\n");
        let _ = writeln!(s, "| Profile | Shape | Pass | Summary |");
        let _ = writeln!(s, "|---|---|:--:|---|");
        for r in results {
            let mark = if r.passed { "PASS" } else { "FAIL" };
            let summary = serde_json::to_string(&r.summary).unwrap_or_default();
            let _ = writeln!(
                s,
                "| {} | {} ({}) | {} | `{}` |",
                r.profile, r.shape_name, r.shape_id, mark, summary
            );
        }
        s
    }
}
