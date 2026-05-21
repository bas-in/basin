//! JSON sidecar emission — mirrors the shape used by
//! `tests/integration/src/benchmark.rs` so the existing
//! `benchmark/index_*.html` dashboards render harness output identically.
//!
//! The schema is owned by the dashboard renderer at this point — see
//! `benchmark/assets/dashboard.js` and `benchmark/bundle.py`. Treat the
//! field names as part of the public interface; renames break the page.
//!
//! Per shape we write either `viability_<id>.json` (single fixed-bar card)
//! or `scaling_<id>.json` (multi-row curve). The integration tests crate
//! continues to write its own files unchanged — this module does NOT touch
//! `manifest.json` (the test runner / bundler still owns that).

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum BarOp {
    GreaterThanOrEqual { value: f64 },
    LessThan { value: f64 },
    Equal { value: f64 },
}

impl BarOp {
    pub fn ge(value: f64) -> Self {
        Self::GreaterThanOrEqual { value }
    }
    pub fn lt(value: f64) -> Self {
        Self::LessThan { value }
    }
    pub fn eq(value: f64) -> Self {
        Self::Equal { value }
    }
    pub fn evaluate(&self, measured: f64) -> bool {
        match self {
            BarOp::GreaterThanOrEqual { value } => measured >= *value,
            BarOp::LessThan { value } => measured < *value,
            BarOp::Equal { value } => (measured - *value).abs() < f64::EPSILON,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PrimaryMetric {
    pub label: String,
    pub value: f64,
    pub unit: String,
    pub bar: BarOp,
}

#[derive(Debug, Clone, Serialize)]
pub struct AxisSpec {
    pub key: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SeriesSpec {
    pub key: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ViabilityReport<'a> {
    pub kind: &'static str,
    pub id: &'a str,
    pub name: &'a str,
    pub claim: &'a str,
    pub passed: bool,
    pub primary: &'a PrimaryMetric,
    pub details: &'a serde_json::Value,
    pub generated_at: String,
}

#[derive(Debug, Serialize)]
pub struct ScalingReport<'a> {
    pub kind: &'static str,
    pub id: &'a str,
    pub name: &'a str,
    pub claim: &'a str,
    pub passed: bool,
    pub x_axis: &'a AxisSpec,
    pub series: &'a [SeriesSpec],
    pub rows: &'a [serde_json::Value],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary: Option<&'a PrimaryMetric>,
    pub generated_at: String,
}

fn now_iso() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("@{}", secs)
}

fn write_atomic(path: &Path, bytes: &[u8]) {
    if let Some(parent) = path.parent() {
        if let Err(e) = fs::create_dir_all(parent) {
            eprintln!("bench-harness: mkdir {}: {e}", parent.display());
            return;
        }
    }
    let tmp = path.with_extension("json.tmp");
    if let Err(e) = fs::write(&tmp, bytes) {
        eprintln!("bench-harness: write {}: {e}", tmp.display());
        return;
    }
    if let Err(e) = fs::rename(&tmp, path) {
        eprintln!("bench-harness: rename {}: {e}", path.display());
    }
}

pub fn report_viability(
    dir: &Path,
    id: &str,
    name: &str,
    claim: &str,
    passed: bool,
    primary: PrimaryMetric,
    details: serde_json::Value,
) -> PathBuf {
    let report = ViabilityReport {
        kind: "viability",
        id,
        name,
        claim,
        passed,
        primary: &primary,
        details: &details,
        generated_at: now_iso(),
    };
    let bytes = serde_json::to_vec_pretty(&report).expect("serialize viability");
    let path = dir.join(format!("viability_{id}.json"));
    write_atomic(&path, &bytes);
    path
}

#[allow(clippy::too_many_arguments)]
pub fn report_scaling(
    dir: &Path,
    id: &str,
    name: &str,
    claim: &str,
    passed: bool,
    x_axis: AxisSpec,
    series: Vec<SeriesSpec>,
    rows: Vec<serde_json::Value>,
    primary: Option<PrimaryMetric>,
) -> PathBuf {
    let report = ScalingReport {
        kind: "scaling",
        id,
        name,
        claim,
        passed,
        x_axis: &x_axis,
        series: &series,
        rows: &rows,
        primary: primary.as_ref(),
        generated_at: now_iso(),
    };
    let bytes = serde_json::to_vec_pretty(&report).expect("serialize scaling");
    let path = dir.join(format!("scaling_{id}.json"));
    write_atomic(&path, &bytes);
    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn viability_round_trips_shape() {
        let dir = TempDir::new().unwrap();
        let p = report_viability(
            dir.path(),
            "shape_x",
            "Shape X",
            "x is fast",
            true,
            PrimaryMetric {
                label: "p99".into(),
                value: 1.0,
                unit: "ms".into(),
                bar: BarOp::lt(10.0),
            },
            serde_json::json!({"extra": 1}),
        );
        let s = fs::read_to_string(&p).unwrap();
        // Same schema as `tests/integration/src/benchmark.rs::ViabilityReport`.
        assert!(s.contains("\"kind\": \"viability\""));
        assert!(s.contains("\"primary\""));
    }
}
