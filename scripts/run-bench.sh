#!/usr/bin/env bash
# run-bench.sh — unified benchmark entrypoint.
#
# Drives `basin_bench_harness::BenchSuite` (crate: basin-bench-harness) over
# a single profile or the whole matrix, writes per-shape JSON sidecars into
# `benchmark/<data_slug>/` (so the existing `index_*.html` dashboards render
# them unchanged), and prints a Markdown summary to stdout in the
# `benchmark/RESULTS_*.md` style.
#
# Usage:
#   scripts/run-bench.sh <profile|all> [--fast]
#
# Profiles (see `basin_bench_harness::profiles::all()`):
#   vortex-vs-parquet   Parquet vs Vortex point-select battery.
#   noisy-neighbor      Quiet-tenant degradation under a noisy tenant.
#   multi-instance      N in-process engine replicas, round-robin fanout.
#   wasm                The Wasm bench family (cold-start, host-roundtrip,
#                       concurrent, memory-cap, wall-timeout, componentize-js,
#                       differential).
#   all                 Every profile above.
#
# Flags:
#   --fast   Shrink iteration counts + dataset to fit the CI <5min budget.
#            CI runs `scripts/run-bench.sh all --fast`. Everything heavier
#            is run without --fast and is gated out of CI.
#
# After a run, re-bundle the dashboard so the HTML picks up the fresh JSON:
#   python3 benchmark/bundle.py            # all dashboards
#   open benchmark/index_localfs.html
#
# Examples:
#   scripts/run-bench.sh vortex-vs-parquet
#   scripts/run-bench.sh wasm --fast
#   scripts/run-bench.sh all --fast > benchmark/RESULTS_harness.md
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PROFILE="${1:-all}"
shift || true

# Release build: benchmark numbers must be measured with optimisations on.
# The first invocation compiles the (large) dependency tree; subsequent runs
# are warm.
echo "run-bench.sh: building basin-bench-harness (release) ..." >&2
cargo build -p basin-bench-harness --bin run-bench --release >&2

echo "run-bench.sh: running profile '$PROFILE' $* ..." >&2
exec cargo run -p basin-bench-harness --bin run-bench --release -- "$PROFILE" "$@"
