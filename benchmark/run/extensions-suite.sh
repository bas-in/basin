#!/usr/bin/env bash
# benchmark/run/extensions-suite.sh
#
# EXTENSIONS BENCHMARK SUITE — performance cards for every extension family,
# deliberately SEPARATE from the compare_postgres benchmarks. Own artifact
# family (benchmark/data/ext_bench_<family>.json), own runner.
#
# Runs each family SOLO (one cargo at a time, per the repo's cargo discipline):
#
#   1. ext_bench_trgm        (pg_trgm:     similarity / % / <-> / <% / GUC)
#   2. ext_bench_fts         (fts:         to_tsvector / @@ / ts_rank / headline)
#   3. ext_bench_vector      (pgvector:    <-> / HNSW / recall@10 / filtered kNN)
#   4. ext_bench_postgis     (postgis:     bbox && / ST_DWithin / <-> / PiP)
#   5. ext_bench_ranges      (range types: && / @> / range JOIN)
#   6. ext_bench_timescale   (timescaledb: time_bucket / first-last / retention)
#   7. ext_bench_jsonb_gin   (jsonb GIN:   @> / ? / path / build / write overhead)
#
# Each card is #[ignore]'d (sanctioned run-later pattern: timing-fragile + may
# need a live PG with the matching extension). PG head-to-head is opportunistic:
# every card probes `CREATE EXTENSION IF NOT EXISTS <ext>` at runtime and, on
# failure, records pg_available:false in its artifact and emits Basin-only
# timings — a card NEVER fails just because the local PG lacks an extension.
#
# Preflight refuses to start if:
#   * another cargo / rustc / leaked test binary is running (the timing cards
#     are wall-clock-sensitive and the workspace shares one target/);
#   * the git tree is dirty (provenance for the published numbers).
#     Regenerated benchmark outputs (benchmark/data*/, RESULTS_*.md,
#     index_*.html, results.js) are exempt — they stay unstaged by convention.
#     ALLOW_DIRTY=1 overrides (harness smoke only; never publish dirty numbers).
#
# RAW output is tee'd (never grep-filtered) to /tmp/basin_ext_bench_<date>.log
# so progress is peekable mid-run:  tail -f /tmp/basin_ext_bench_*.log
#
# Per-family PASS/FAIL ledger is printed at the end, plus which families ended
# up with pg_available:false (parsed from the emitted artifacts). Suite exits
# nonzero if any card fails so CI can gate on it.
#
# Env knobs (forwarded to the tests; see each test's module header):
#   BASIN_EXT_BENCH_ROWS              — shared row-count default for every card
#   BASIN_EXT_BENCH_TRGM_SAMPLES
#   BASIN_EXT_BENCH_FTS_SAMPLES
#   BASIN_EXT_BENCH_VEC_SAMPLES / _DIM / _BIGDIM / _BIGDIM_ROWS
#   BASIN_EXT_BENCH_GEO_SAMPLES
#   BASIN_EXT_BENCH_RANGE_SAMPLES / _RANGE_JOIN_N
#   BASIN_EXT_BENCH_TS_SAMPLES
#   BASIN_EXT_BENCH_JSONB_SAMPLES
#
#   ./benchmark/run/extensions-suite.sh
#
# Tiny-N harness smoke (fast; verifies the cards + artifacts without the full
# 100k/1M wall clock):
#   BASIN_EXT_BENCH_ROWS=5000 BASIN_EXT_BENCH_VEC_BIGDIM_ROWS=1000 \
#     ./benchmark/run/extensions-suite.sh

set -uo pipefail

RUN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "${RUN_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/.." && pwd)"
DATA_DIR="${BENCH_DIR}/data"

DATE_TAG="$(date +%Y%m%d_%H%M%S)"
LOG="/tmp/basin_ext_bench_${DATE_TAG}.log"

log() { printf '[ext-suite] %s\n' "$*" | tee -a "${LOG}" >&2; }

# ── preflight: box must be idle ───────────────────────────────────────────────
# One cargo at a time on this box; timing cards are meaningless under CPU
# contention. Refuse if any cargo/rustc process — or a leaked test binary from a
# previous run — is alive.
busy="$( { pgrep -lx cargo; pgrep -lx rustc; pgrep -lf 'target/[^/]*/deps/'; } 2>/dev/null | sort -u || true )"
if [[ -n "${busy}" ]]; then
  log "REFUSING to start: the box is not idle. Offending processes:"
  printf '%s\n' "${busy}" | tee -a "${LOG}" >&2
  log "(another cargo/rustc/test binary would skew timing and corrupt the shared target/)"
  exit 2
fi

# ── preflight: git tree must be clean ────────────────────────────────────────
# Benchmark outputs are regenerated automatically and stay unstaged.
dirty="$(git -C "${REPO_ROOT}" status --porcelain 2>/dev/null \
  | grep -v -E '^.. benchmark/(data|data_real|data_seaweedfs)/' \
  | grep -v -E '^.. benchmark/RESULTS_' \
  | grep -v -E '^.. benchmark/index_' \
  | grep -v -E 'results\.js$' || true)"
if [[ -n "${dirty}" && "${ALLOW_DIRTY:-0}" != "1" ]]; then
  log "REFUSING to start: git tree is dirty (numbers need a committed provenance):"
  printf '%s\n' "${dirty}" | tee -a "${LOG}" >&2
  log "commit/stash first, or ALLOW_DIRTY=1 for a harness smoke (never publish from dirty)"
  exit 2
fi

# ── provenance header ─────────────────────────────────────────────────────────
{
  echo "================ basin extensions-suite ================"
  echo "started   : $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "host      : $(hostname)"
  echo "commit    : $(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "dirty     : $([[ -n "${dirty}" ]] && echo "YES (ALLOW_DIRTY=1)" || echo no)"
  echo "log       : ${LOG}"
  echo "========================================================"
} | tee -a "${LOG}" >&2

# ── card table ────────────────────────────────────────────────────────────────
# Each entry: "<binary>/<test_fn_name>/<family>"
# The binary name is the --test argument and the function name is the filter.
declare -a CARDS=(
  "ext_bench_trgm/ext_bench_trgm/pg_trgm"
  "ext_bench_fts/ext_bench_fts/fts"
  "ext_bench_vector/ext_bench_vector/pgvector"
  "ext_bench_postgis/ext_bench_postgis/postgis"
  "ext_bench_ranges/ext_bench_ranges/range_types"
  "ext_bench_timescale/ext_bench_timescale/timescaledb"
  "ext_bench_jsonb_gin/ext_bench_jsonb_gin/jsonb_gin"
)

overall_rc=0
declare -a RESULTS=()
declare -a PG_UNAVAIL=()
SUITE_START="$(date +%s)"

for card_entry in "${CARDS[@]}"; do
  IFS='/' read -r binary fn_name family <<< "${card_entry}"

  log "=== ${family} (${binary}::${fn_name}): starting (solo) ==="
  t_start="$(date +%s)"

  # All cards are #[ignore]'d; filter to the specific test function so only this
  # card runs in its binary. RAW output goes through tee — never filtered.
  ( cd "${REPO_ROOT}" && \
      cargo test -p basin-integration-tests --test "${binary}" \
        -- --ignored --nocapture "${fn_name}" \
  ) 2>&1 | tee -a "${LOG}"
  rc="${PIPESTATUS[0]}"

  t_secs=$(( $(date +%s) - t_start ))

  if [[ "${rc}" == "0" ]]; then
    RESULTS+=( "${family} (${binary}): PASS (${t_secs}s)" )
    log "=== ${family}: PASS in ${t_secs}s ==="
  else
    RESULTS+=( "${family} (${binary}): FAIL rc=${rc} (${t_secs}s)" )
    log "=== ${family}: FAIL (rc=${rc}) in ${t_secs}s — continuing to next family ==="
    overall_rc=1
  fi

  # Parse the emitted artifact for the pg_available flag (head-to-head status).
  # write_artifact lands "<binary>.json" (e.g. ext_bench_trgm.json).
  artifact="${DATA_DIR}/${binary}.json"
  if [[ -f "${artifact}" ]]; then
    if grep -q '"pg_available": false' "${artifact}" 2>/dev/null; then
      PG_UNAVAIL+=( "${family}" )
    fi
  fi
done

# ── verdict ───────────────────────────────────────────────────────────────────
SUITE_SECS=$(( $(date +%s) - SUITE_START ))
{
  echo
  echo "================ extensions-suite verdict ================"
  for r in "${RESULTS[@]}"; do echo "  ${r}"; done
  echo
  if [[ ${#PG_UNAVAIL[@]} -gt 0 ]]; then
    echo "  pg_available:false (Basin-only) for: ${PG_UNAVAIL[*]}"
  else
    echo "  pg_available:true for every family (or no artifact parsed)"
  fi
  echo
  echo "  total wall-clock : ${SUITE_SECS}s ($(( SUITE_SECS / 60 ))m $(( SUITE_SECS % 60 ))s)"
  echo "  artifacts        : ${DATA_DIR}/ext_bench_{trgm,fts,vector,postgis,ranges,"
  echo "                       timescale,jsonb_gin}.json"
  echo "  raw log          : ${LOG}"
  echo "=========================================================="
} | tee -a "${LOG}"

exit "${overall_rc}"
