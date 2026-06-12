#!/usr/bin/env bash
# benchmark/run/scale-suite.sh
#
# Large-scale tier suite — runs, in order and SOLO (one cargo at a time,
# per the repo's cargo discipline):
#
#   1. scale_10m_smoke      (the 10M smoke, un-ignored)
#   2. scale_100m           (100M-row tier card; hours + ~100GB disk)
#   3. multi_project_fleet  (N-project noisy-neighbor card)
#
# Preflight refuses to start if:
#   * another cargo / rustc / leaked test binary is running (the timing
#     cards are wall-clock-sensitive and the workspace shares one target/);
#   * the git tree is dirty (code provenance for the published numbers).
#     Regenerated benchmark outputs (benchmark/data*/, RESULTS_*.md,
#     index_*.html, results.js) are exempt — per repo convention they stay
#     unstaged. ALLOW_DIRTY=1 overrides (smoke-testing the harness only;
#     never publish numbers from a dirty tree).
#
# RAW output is tee'd (not grep-filtered) to /tmp/basin_scale_suite_<date>.log
# so progress is peekable mid-run:  tail -f /tmp/basin_scale_suite_*.log
#
# Env knobs (forwarded to the tests; see each test's header):
#   BASIN_SCALE_100M_ROWS / _BATCH / _SAMPLES   — 100M card (set ROWS small
#                                                 to smoke-test the harness)
#   BASIN_FLEET_PROJECTS / _NOISY / _VICTIMS / _SEED_ROWS / _QUERIES /
#   BASIN_FLEET_NOISY_BATCH                     — fleet card
#
#   ./benchmark/run/scale-suite.sh
#   BASIN_SCALE_100M_ROWS=200000 BASIN_FLEET_PROJECTS=5 ./benchmark/run/scale-suite.sh   # tiny-N harness smoke

set -uo pipefail

RUN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "${RUN_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/.." && pwd)"

LOG="/tmp/basin_scale_suite_$(date +%Y%m%d_%H%M%S).log"

log() { printf '[scale-suite] %s\n' "$*" | tee -a "${LOG}" >&2; }

# --- preflight: the box must be idle ----------------------------------------
# One cargo at a time on this box, and the timing cards are meaningless under
# CPU contention. Refuse if any cargo/rustc process — or a leaked test binary
# from a previous run — is alive.
busy="$( { pgrep -lx cargo; pgrep -lx rustc; pgrep -lf 'target/[^/]*/deps/'; } 2>/dev/null | sort -u || true )"
if [[ -n "${busy}" ]]; then
  log "REFUSING to start: the box is not idle. Offending processes:"
  printf '%s\n' "${busy}" | tee -a "${LOG}" >&2
  log "(another cargo/rustc/test binary would corrupt the shared target/ and skew the timings)"
  exit 2
fi

# --- preflight: git tree must be clean (provenance) -------------------------
# Regenerated benchmark outputs are exempt — they are never committed and a
# previous run legitimately leaves them modified (see CLAUDE.md).
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

# --- provenance header into the log ------------------------------------------
{
  echo "================ basin scale suite ================"
  echo "started   : $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "host      : $(hostname)"
  echo "commit    : $(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "dirty     : $([[ -n "${dirty}" ]] && echo "YES (ALLOW_DIRTY=1)" || echo no)"
  echo "log       : ${LOG}"
  echo "==================================================="
} | tee -a "${LOG}" >&2

# --- run each tier solo, in order --------------------------------------------
# Sequential by construction: one cargo invocation at a time. Each test is
# the only #[test] in its binary; `--ignored` selects exactly the
# documented-ignore tier tests. RAW output goes through tee (peekable).
TESTS=( scale_10m_smoke scale_100m multi_project_fleet )

overall_rc=0
declare -a RESULTS=()
SUITE_START="$(date +%s)"

for t in "${TESTS[@]}"; do
  log "=== ${t}: starting (solo) ==="
  t_start="$(date +%s)"
  ( cd "${REPO_ROOT}" && \
      cargo test -p basin-integration-tests --test "${t}" -- --ignored --nocapture \
  ) 2>&1 | tee -a "${LOG}"
  rc=$?
  t_secs=$(( $(date +%s) - t_start ))
  if [[ "${rc}" == "0" ]]; then
    RESULTS+=( "${t}: PASS (${t_secs}s)" )
    log "=== ${t}: PASS in ${t_secs}s ==="
  else
    RESULTS+=( "${t}: FAIL rc=${rc} (${t_secs}s)" )
    log "=== ${t}: FAIL (rc=${rc}) in ${t_secs}s — continuing to next tier ==="
    overall_rc=1
  fi
done

# --- verdict ------------------------------------------------------------------
SUITE_SECS=$(( $(date +%s) - SUITE_START ))
{
  echo
  echo "================ scale-suite verdict ================"
  for r in "${RESULTS[@]}"; do echo "  ${r}"; done
  echo "  total wall-clock : ${SUITE_SECS}s ($(( SUITE_SECS / 60 ))m $(( SUITE_SECS % 60 ))s)"
  echo "  artifacts        : ${BENCH_DIR}/data/scale_100m.json, ${BENCH_DIR}/data/multi_project_fleet.json"
  echo "  raw log          : ${LOG}"
  echo "====================================================="
} | tee -a "${LOG}"

exit "${overall_rc}"
