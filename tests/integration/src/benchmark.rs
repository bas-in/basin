//! JSON sidecar emitter for the live results dashboard.
//!
//! Every viability / scaling / comparison test calls one of the `report_*`
//! functions below at the end. Each writes a file to
//! `<repo_root>/dashboard/data/<id>.json`, atomically replacing any prior
//! result, with the schema the dashboard `index.html` expects.
//!
//! Files are independent — different tests can run in parallel without
//! racing on a shared file. The dashboard knows the list of expected IDs
//! from `dashboard/data/manifest.json` (also written by these helpers when
//! the test category is reported).
//!
//! The functions are infallible from the test's perspective: any IO failure
//! is logged via `eprintln!` but does not panic. The whole point is the
//! *test result* — the JSON is a side effect.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

const DASHBOARD_DIR: &str = "dashboard/data";

/// Path to `<repo_root>/dashboard/data/`. Resolved from this crate's
/// `CARGO_MANIFEST_DIR` (= `tests/integration`) walked up two parents.
fn data_dir() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(Path::parent)
        .map(|p| p.join(DASHBOARD_DIR))
        .unwrap_or_else(|| PathBuf::from(DASHBOARD_DIR))
}

fn now_iso() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("@{}", secs)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum BarOp {
    /// Measured ≥ value. (e.g. compression ratio ≥ 10×)
    GreaterThanOrEqual { value: f64 },
    /// Measured < value. (e.g. p99 latency < 1000ms)
    LessThan { value: f64 },
    /// Measured == value. (e.g. leak count == 0)
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimaryMetric {
    pub label: String,
    pub value: f64,
    pub unit: String,
    pub bar: BarOp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AxisSpec {
    pub key: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesSpec {
    pub key: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WhichWins {
    Basin,
    Postgres,
    Tie,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompareMetric {
    pub label: String,
    pub basin: f64,
    pub postgres: f64,
    pub unit: String,
    pub better: WhichWins,
    /// Optional human-readable ratio string ("pg / basin = 24.7×"). Lets the
    /// dashboard show whichever framing is most legible per metric without
    /// recomputing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ratio_text: Option<String>,
}

/// One viability-test record. Emitted to `dashboard/data/viability_<id>.json`.
#[derive(Debug, Clone, Serialize)]
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

/// One scaling-curve record. Emitted to `dashboard/data/scaling_<id>.json`.
#[derive(Debug, Clone, Serialize)]
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

/// One head-to-head comparison record.
#[derive(Debug, Clone, Serialize)]
pub struct CompareReport<'a> {
    pub kind: &'static str,
    pub id: &'a str,
    pub name: &'a str,
    pub claim: &'a str,
    pub available: bool,
    pub metrics: &'a [CompareMetric],
    pub generated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<&'a str>,
}

fn write_atomic(path: &Path, bytes: &[u8]) {
    if let Some(parent) = path.parent() {
        if let Err(e) = fs::create_dir_all(parent) {
            eprintln!("dashboard: mkdir {}: {e}", parent.display());
            return;
        }
    }
    let tmp = path.with_extension("json.tmp");
    if let Err(e) = fs::write(&tmp, bytes) {
        eprintln!("dashboard: write {}: {e}", tmp.display());
        return;
    }
    if let Err(e) = fs::rename(&tmp, path) {
        eprintln!("dashboard: rename {}: {e}", path.display());
    }
}

pub fn report_viability(
    id: &str,
    name: &str,
    claim: &str,
    passed: bool,
    primary: PrimaryMetric,
    details: serde_json::Value,
) {
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
    let path = data_dir().join(format!("viability_{id}.json"));
    write_atomic(&path, &bytes);
}

#[allow(clippy::too_many_arguments)]
pub fn report_scaling(
    id: &str,
    name: &str,
    claim: &str,
    passed: bool,
    x_axis: AxisSpec,
    series: Vec<SeriesSpec>,
    rows: Vec<serde_json::Value>,
    primary: Option<PrimaryMetric>,
) {
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
    let path = data_dir().join(format!("scaling_{id}.json"));
    write_atomic(&path, &bytes);
}

pub fn report_postgres_compare(
    id: &str,
    name: &str,
    claim: &str,
    available: bool,
    metrics: Vec<CompareMetric>,
    note: Option<&str>,
) {
    let report = CompareReport {
        kind: "compare",
        id,
        name,
        claim,
        available,
        metrics: &metrics,
        generated_at: now_iso(),
        note,
    };
    let bytes = serde_json::to_vec_pretty(&report).expect("serialize compare");
    let path = data_dir().join(format!("compare_{id}.json"));
    write_atomic(&path, &bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_dir_resolves_to_repo_root() {
        let p = data_dir();
        let s = p.to_string_lossy();
        assert!(
            s.ends_with("dashboard/data"),
            "data dir should end at dashboard/data, got {s}"
        );
    }

    #[test]
    fn bar_evaluates() {
        assert!(BarOp::ge(10.0).evaluate(15.0));
        assert!(!BarOp::ge(10.0).evaluate(5.0));
        assert!(BarOp::lt(1000.0).evaluate(184.0));
        assert!(!BarOp::lt(1000.0).evaluate(1500.0));
        assert!(BarOp::eq(0.0).evaluate(0.0));
    }
}
