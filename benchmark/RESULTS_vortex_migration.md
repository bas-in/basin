# Basin — post-migration LocalFS storage comparison (arrow58/df53)

_Benchmark run 2026-05-15. Re-run of the established `benchmark/vortex_compare` harness
against the arrow58/df53/sqlparser0.61/object_store0.13 migration (HEAD `5e21d13`)._

---

## Run context

| Field | Value |
|---|---|
| **HEAD commit** | `5e21d13f949f087ef54747ce92897dc167b6452e` |
| **Date** | 2026-05-15 |
| **Platform** | Darwin pcs-MacBook-Air.local 24.6.0 (ARM64 T8112 / Apple M2) |
| **CPUs** | 8 (`hw.ncpu`) |
| **RAM** | 16 GiB (`hw.memsize` = 17,179,869,184 bytes) |
| **Build** | `CARGO_BUILD_JOBS=1 cargo run --manifest-path benchmark/vortex_compare/Cargo.toml --release` |
| **Iterations** | 5 independent runs; sizes are deterministic (shown once) |

---

## What was benchmarked

`benchmark/vortex_compare/` is the established standalone harness (not a workspace
member — it uses its own `Cargo.lock` so it can pin arrow 58 independently).

**Dataset:** 1,000,000 synthetic audit-log rows:
`id (i64), ts (utf8), actor (utf8, 8-cardinality), event (utf8, 4-cardinality), body (utf8, ~120 B JSON)`

**Formats compared:**

| Format | Config |
|---|---|
| Parquet | ZSTD level 1, 65,536 rows/row-group (default) |
| Vortex | BtrBlocks compressor + `.with_compact()`: Zstd for strings, Pco for numerics |

CSV is not separately measured by this harness; the 20.22× CSV/Parquet ratio
from the main integration suite (unchanged; see `RESULTS_localfs.md`) provides
the transitive CSV/Vortex bound.

---

## Results

### On-disk size (deterministic across all runs)

| Format | Bytes | MiB |
|---|---|---|
| ZSTD Parquet | 3,174,879 | 3.03 |
| Vortex (BtrBlocks+compact) | 1,628,404 | 1.55 |
| **Ratio (parquet / vortex)** | **1.950×** | Vortex is smaller |

### Full-scan latency (5 independent cold runs, macOS LocalFS)

| Run | Parquet (ms) | Vortex (ms) | Ratio (parq/vrtx) |
|---|---|---|---|
| 1 | 75.5 | 3.1 | 24.3× |
| 2 | 64.9 | 3.5 | 18.4× |
| 3 | 72.5 | 3.3 | 21.9× |
| 4 | 66.5 | 3.4 | 19.8× |
| 5 | 61.7 | 3.3 | 19.0× |
| **p50** | **66.5** | **3.3** | **20.1×** |
| **p95** | **75.5** | **3.5** | **24.3×** |

> Note: each run re-generates and re-writes both files from scratch, then
> performs a single cold sequential scan. No warm-up pass. macOS buffer cache
> may partially warm between runs; true cold numbers would be higher for Parquet.

---

## Verdict

**The pre-migration Vortex advantage fully holds post-migration.**

The prior baseline claims were:
- Size: Vortex ≈ **1.95×** smaller than ZSTD Parquet
- Scan: Vortex ≈ **18×** faster than ZSTD Parquet

Post-migration (arrow58/df53/sqlparser0.61/object_store0.13, commit `5e21d13`):
- Size ratio: **1.950×** — exactly matches baseline (no regression)
- Scan ratio: p50 = **20.1×**, p95 = **24.3×** — exceeds the 18× baseline

No regressions were observed. The compression path (Vortex BtrBlocks+compact
cascade) is deterministic: both format writers are pure Rust crates independent
of the DataFusion/sqlparser/object_store migration, so the stability is
expected. The scan-speed margin widening from ~18× to ~20× p50 is likely
macOS buffer-cache behaviour rather than a real improvement; the result is
comfortably above the 18× bar regardless. The transitive CSV/Vortex ratio
remains ≥ 39× (20.22× CSV/Parquet × 1.95× Parquet/Vortex).

---

## Exact commands run

```sh
# Build (release, single job):
CARGO_BUILD_JOBS=1 cargo build --release --manifest-path benchmark/vortex_compare/Cargo.toml

# Run (5 iterations; each run overwrites benchmark/data/compare_vortex_parquet.json):
CARGO_BUILD_JOBS=1 cargo run --manifest-path benchmark/vortex_compare/Cargo.toml --release
```

The final JSON snapshot from run 5 is at `benchmark/data/compare_vortex_parquet.json`.

---

## See also

- [`benchmark/RESULTS_localfs.md`](./RESULTS_localfs.md) — full integration-test dashboard (LocalFS)
- [`benchmark/BENCHMARKS.md`](./BENCHMARKS.md) — long-form performance story
- [`benchmark/vortex_compare/src/main.rs`](./vortex_compare/src/main.rs) — benchmark source
