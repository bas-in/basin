//! Swappable configuration for [`crate::BenchHarness`].
//!
//! A [`BenchConfig`] is the *single* knob bag the harness reads. Profiles
//! ([`crate::BenchProfile`]) are just builders over this struct, so swapping
//! "Parquet" for "Vortex" or "LocalFS" for "Tigris-S3" is a config edit, not
//! a code change. Every workload primitive in [`crate::workload`] is pure
//! over `(BenchConfig, BenchHarness)` — nothing in the workload modules
//! should reach for env vars or globals.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Storage substrate the harness should mount.
///
/// Matches the existing `benchmark/data_*/` directory layout so a `BenchSuite`
/// run with `backend = StorageBackend::SeaweedFs` writes to
/// `benchmark/data_seaweedfs/` and the rendered `index_seaweedfs.html` picks
/// the JSONs up unchanged.
///
/// The S3-flavoured backends carry their own configuration; the harness
/// passes them through to [`object_store`] without interpretation so a single
/// `BenchSuite` can target any object-store target without rebuilding.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StorageBackend {
    /// Local filesystem under a tempdir. The default for CI / fast iteration.
    LocalFs,
    /// AWS S3 (or any S3-API endpoint). Set `endpoint = None` for AWS proper.
    S3 {
        bucket: String,
        region: String,
        endpoint: Option<String>,
    },
    /// Tigris (S3-compatible). Carved out from `S3` so the dashboard slug
    /// stays separate — the legacy "real" config is just `S3 { endpoint: tigris }`.
    Tigris { bucket: String, region: String },
    /// Locally-hosted SeaweedFS S3 gateway. Started out-of-band; the harness
    /// only configures the client.
    SeaweedFs {
        bucket: String,
        endpoint: String,
    },
}

impl StorageBackend {
    /// Slug used to choose the `benchmark/data_<slug>/` directory.
    ///
    /// Keep this aligned with the slugs in `benchmark/dashboards.toml`.
    /// Mismatched slugs are *silently* wrong — the dashboard will render an
    /// empty page rather than show the missing-file error.
    pub fn data_slug(&self) -> &'static str {
        match self {
            StorageBackend::LocalFs => "data",
            StorageBackend::S3 { .. } => "data_real",
            StorageBackend::Tigris { .. } => "data_real",
            StorageBackend::SeaweedFs { .. } => "data_seaweedfs",
        }
    }
}

impl Default for StorageBackend {
    fn default() -> Self {
        StorageBackend::LocalFs
    }
}

/// Engine-side feature toggles a profile wants to flip without rebuilding.
///
/// These are the knobs that historically forced parallel test binaries
/// (`vortex_vs_parquet_*.rs`, `viability_bloom_filter_*.rs`,
/// `viability_disk_cache.rs` etc.). Pulling them into a single struct means
/// one profile can swap the matrix axes per shape.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EngineFlags {
    /// Vortex columnar format (true) vs Parquet (false).
    /// Default: false — Parquet is the production default today.
    pub vortex: bool,
    /// Point-query fast path in `basin-engine` (true). Default: true.
    /// Set false when measuring the planner-overhead delta.
    pub fast_path: bool,
    /// HTAP hot tier (memtable + WAL). Default: false; the writer-path is
    /// gated and most viability cards still measure the parquet-only path.
    pub htap_hot_tier: bool,
    /// Probe bloom filters during `fast_select`. Default: true (matches the
    /// production planner). Disable when measuring bloom-vs-no-bloom delta.
    pub blooms: bool,
    /// Page cache bytes in RAM. Default 64 MiB — small enough to force real
    /// IO under load, large enough that the warm path is meaningful.
    pub page_cache_bytes: u64,
    /// Disk cache bytes (per project). Default 256 MiB.
    pub disk_cache_bytes: u64,
}

impl Default for EngineFlags {
    fn default() -> Self {
        Self {
            vortex: false,
            fast_path: true,
            htap_hot_tier: false,
            blooms: true,
            page_cache_bytes: 64 * 1024 * 1024,
            disk_cache_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Workload-side knobs.
///
/// Each shape under [`crate::workload`] consumes whatever subset it cares
/// about; e.g. [`crate::workload::point_select`] reads `table_rows`,
/// `working_set`, `iterations`, and `concurrency` but ignores `wasm_*`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkloadKnobs {
    /// Number of projects the harness will provision before the workload runs.
    pub project_count: usize,
    /// Rows per table (per project).
    pub table_rows: u64,
    /// Hot-id pool size for point-query workloads.
    pub working_set: usize,
    /// Total query / call count to issue.
    pub iterations: usize,
    /// Concurrency factor (fan-out across tokio tasks).
    pub concurrency: usize,
    /// Hard wall-clock cap. The runner halts the shape if it exceeds this,
    /// reports `passed = false`, and continues to the next shape so a slow
    /// regression doesn't wedge a multi-profile run.
    pub deadline_secs: u64,
    /// Optional Wasm-bench knobs (None for non-wasm shapes).
    pub wasm: Option<WasmKnobs>,
    /// Optional noisy-neighbor knobs (None for single-project shapes).
    pub noisy: Option<NoisyKnobs>,
    /// Optional multi-instance knobs (None for single-instance shapes).
    pub multi_instance: Option<MultiInstanceKnobs>,
}

impl Default for WorkloadKnobs {
    fn default() -> Self {
        Self {
            project_count: 1,
            table_rows: 100_000,
            working_set: 1_000,
            iterations: 1_000,
            concurrency: 1,
            deadline_secs: 60,
            wasm: None,
            noisy: None,
            multi_instance: None,
        }
    }
}

/// Wasm-bench specific knobs. See `profiles/wasm.rs` for the seven shapes
/// (cold-start, host-roundtrip, concurrent invocation, memory cap, wall
/// timeout, JS via componentize-js, SQL/Wasm/raw differential).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WasmKnobs {
    /// Pre-compiled component bytes (component model format).
    /// `None` means "use the bundled tiny no-op component".
    #[serde(skip)]
    pub component_bytes: Option<Vec<u8>>,
    /// Memory cap in pages (64KiB each). Default 16 (= 1 MiB).
    pub memory_pages: u32,
    /// Per-invocation epoch deadline (matches the basin-fn 5-epoch default).
    pub epoch_deadline: u64,
    /// Try componentize-js (Node-toolchain produced JS components).
    /// Default false — the JS toolchain is gated behind `#[ignore]` and is
    /// only run when the operator has it installed.
    pub use_componentize_js: bool,
}

impl Default for WasmKnobs {
    fn default() -> Self {
        Self {
            component_bytes: None,
            memory_pages: 16,
            epoch_deadline: 5,
            use_componentize_js: false,
        }
    }
}

/// Noisy-neighbor knobs. The profile spins up one "noisy" project doing the
/// `noisy_shape` workload and N quiet projects doing the `quiet_shape`
/// workload, then measures the quiet projects' p99 degradation vs a baseline
/// run with the noisy project disabled.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NoisyKnobs {
    pub quiet_projects: usize,
    /// Concurrency of the noisy project's full-scan workload.
    pub noisy_concurrency: usize,
    /// Per-project query cost cap in rows (matches BASIN_QUERY_COST_LIMIT_ROWS).
    /// `None` means uncapped — surfaces the P0 gap from the noisy-neighbor
    /// audit (2026-05-21).
    pub query_cost_limit_rows: Option<u64>,
    /// Per-project I/O permit budget. None = unbounded (the current default).
    pub io_permits: Option<u32>,
}

impl Default for NoisyKnobs {
    fn default() -> Self {
        Self {
            quiet_projects: 8,
            noisy_concurrency: 4,
            query_cost_limit_rows: None,
            io_permits: None,
        }
    }
}

/// Multi-instance knobs. The profile spawns N basin-server-equivalent engine
/// instances in-process (sharing only the storage + catalog), round-robins
/// requests across them, and measures the cross-replica state cost (catalog
/// staleness, per-project counter divergence, idempotency-key drift).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MultiInstanceKnobs {
    pub replica_count: usize,
    /// Sticky-route fraction. 1.0 = always pin to the same replica per
    /// project (the optimistic case); 0.0 = pure round-robin (worst case).
    pub sticky_fraction: f64,
}

impl Default for MultiInstanceKnobs {
    fn default() -> Self {
        Self {
            replica_count: 3,
            sticky_fraction: 0.0,
        }
    }
}

/// The full config the harness reads.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchConfig {
    pub backend: StorageBackend,
    pub engine: EngineFlags,
    pub workload: WorkloadKnobs,
    /// Optional override of where the JSON sidecar lands.
    /// `None` resolves to `<repo_root>/benchmark/<backend.data_slug()>/`.
    #[serde(default)]
    pub output_dir: Option<PathBuf>,
    /// Profile slug for the dashboard card.
    #[serde(default)]
    pub profile_slug: String,
}

impl BenchConfig {
    /// Default LocalFS / Parquet / single-project configuration suitable for
    /// fast CI sanity (target: <30s per shape, <5min for the whole CI subset).
    pub fn ci_default() -> Self {
        Self {
            backend: StorageBackend::LocalFs,
            engine: EngineFlags::default(),
            workload: WorkloadKnobs::default(),
            output_dir: None,
            profile_slug: "ci".into(),
        }
    }

    /// Builder convenience.
    pub fn with_backend(mut self, backend: StorageBackend) -> Self {
        self.backend = backend;
        self
    }

    pub fn with_engine(mut self, engine: EngineFlags) -> Self {
        self.engine = engine;
        self
    }

    pub fn with_workload(mut self, workload: WorkloadKnobs) -> Self {
        self.workload = workload;
        self
    }

    pub fn with_profile_slug(mut self, slug: impl Into<String>) -> Self {
        self.profile_slug = slug.into();
        self
    }
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self::ci_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_slug_matches_dashboards_toml() {
        // The dashboard renderer reads `benchmark/dashboards.toml` and writes
        // to `benchmark/<data_dir>/`; if the slugs drift the JSONs land in
        // the wrong directory and silently disappear from the dashboard.
        assert_eq!(StorageBackend::LocalFs.data_slug(), "data");
        assert_eq!(
            StorageBackend::SeaweedFs {
                bucket: "b".into(),
                endpoint: "e".into()
            }
            .data_slug(),
            "data_seaweedfs"
        );
    }

    #[test]
    fn default_config_round_trips_json() {
        let cfg = BenchConfig::default();
        let s = serde_json::to_string(&cfg).unwrap();
        let back: BenchConfig = serde_json::from_str(&s).unwrap();
        assert_eq!(back.workload.iterations, cfg.workload.iterations);
    }
}
