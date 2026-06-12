//! Shared helpers for the `compare_postgres_ext_*` cards.
//!
//! Loaded via `#[path = "compare_postgres_ext_util.rs"] mod ext_util;` from
//! each ext card. Provides the env-knob reader, the wall-clock stamp, and the
//! atomic read-modify-write merge into the single shared artifact
//! `benchmark/data/compare_postgres_ext.json` (or
//! `benchmark/data_seaweedfs/compare_postgres_ext.json` when
//! `BASIN_BENCH_STORE=s3`).
//!
//! Why a merge (RMW) and not three separate files? The three ext cards
//! (`scalar`, `structural`, `dml`) are facets of one logical "extended
//! shapes" card; the dashboard renders them as one. Each card owns a named
//! top-level section and rewrites only its own section, preserving any
//! sibling sections already on disk so the file accumulates all three when the
//! cards are run in sequence (they cannot run concurrently — the workspace
//! serializes on a single `target/` and these are `#[ignore]`d, idle-box
//! cards). The write is atomic (`write tmp` + `rename`) per the artifact
//! convention used by `shape_sweeps.rs`.
//!
//! ## Store parameterization
//! Set `BASIN_BENCH_STORE=s3` before running the suite (via `extensions-suite.sh
//! STORE=seaweedfs`) to route both the Basin engine and the artifact file to the
//! S3-backed (SeaweedFS) path. Default (`local`) is byte-identical to the
//! pre-parameterization behaviour.

#![allow(dead_code, clippy::print_stderr)]

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn artifact_path() -> PathBuf {
    // Use the env-driven artifact dir so BASIN_BENCH_STORE=s3 routes artifacts
    // to benchmark/data_seaweedfs/ while the default is benchmark/data/.
    let dir = basin_integration_tests::ext_store_select::ext_artifact_dir();
    dir.join("compare_postgres_ext.json")
}

/// Read the existing `compare_postgres_ext.json` (if any), replace the named
/// section with `section_value`, and atomically write it back. Top-level
/// `card`/`generated_at` are refreshed on every write.
pub fn merge_artifact(section: &str, section_value: serde_json::Value) {
    let path = artifact_path();

    let mut root: serde_json::Value = std::fs::read(&path)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_else(|| {
            serde_json::json!({
                "card": "compare_postgres_ext",
                "description": "Extended PG head-to-head shapes not covered by \
                    the core compare_postgres cards: scalar (text/date/projection), \
                    structural (joins/set-ops/group-by cardinality/json_agg), and \
                    dml (expression UPDATE/subquery DELETE/INSERT…SELECT/RETURNING).",
                "sections": {}
            })
        });

    if !root.is_object() {
        root = serde_json::json!({ "card": "compare_postgres_ext", "sections": {} });
    }
    let obj = root.as_object_mut().unwrap();
    obj.insert(
        "card".to_string(),
        serde_json::Value::String("compare_postgres_ext".to_string()),
    );
    obj.insert(
        "generated_at".to_string(),
        serde_json::Value::String(format!("@{}", now_secs())),
    );
    let sections = obj
        .entry("sections".to_string())
        .or_insert_with(|| serde_json::json!({}));
    if !sections.is_object() {
        *sections = serde_json::json!({});
    }
    sections
        .as_object_mut()
        .unwrap()
        .insert(section.to_string(), section_value);

    let tmp = path.with_extension("json.tmp");
    if let Ok(bytes) = serde_json::to_vec_pretty(&root) {
        let _ = std::fs::write(&tmp, &bytes);
        let _ = std::fs::rename(&tmp, &path);
        eprintln!(
            "[ext] artifact merged section `{section}`: {}",
            path.display()
        );
    }
}
