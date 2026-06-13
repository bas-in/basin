#!/usr/bin/env bash
# benchmark/run/scale-suite.sh
#
# Large-scale TIER suite. Drives the core scale ladders — wired-and-ready,
# run-on-provisioned-hardware. Runs SOLO (one cargo at a time, per the repo's
# cargo discipline), in order:
#
#   ROW-SCALE ladder (engine proof, ClickBench/TimescaleDB-class):
#     scale_tier_default              @ each tier in BASIN_SCALE_TIERS
#     ts_ingest_default               @ each tier in BASIN_SCALE_TIERS
#       — point lookup / selective filter / keyset / GROUP BY / JSONB @> /
#         bulk UPDATE, plus sustained time-series ingest rate; the pruning
#         invariants (point lookup files_opened<=2) must hold AT EVERY TIER.
#
#   FLEET ladder (product proof — Basin's actual thesis):
#     fleet_scale_default             @ each tier in BASIN_FLEET_TIERS
#     saturation_default              (single adversarial tenant vs victim)
#       — victim p99 isolation as project count scales 50 -> 500 -> 5000.
#
#   Legacy standalone cards (kept for their sanctioned-ignore identity):
#     scale_10m_smoke, scale_100m, multi_project_fleet
#       — run only when BASIN_SCALE_LEGACY=1.
#
# ── BOX-CEILING vs PROVISIONED (read this before running) ───────────────────
# The tiers are NOT all runnable on a laptop. The ladder is:
#
#   ROW tiers:    1M  (dev/CI)
#                 10M (BOX-CEILING — the largest a dev box runs in ~minutes)
#                 100M (ClickBench-class — PROVISIONED hardware, ~100GB, hours)
#                 1B  (scale-proof — PROVISIONED hardware, tens-of-GB+, hours)
#
#   FLEET tiers:  50   (dev/CI)
#                 500  (BOX-STRESS — tens of minutes on a dev box)
#                 5000 (scale-proof — PROVISIONED hardware)
#
# To stop anyone running 1B / 5000 on a laptop by ACCIDENT, this runner REFUSES
# any tier above a ceiling unless you raise it explicitly:
#
#   BASIN_SCALE_MAX   — max ROW tier this box may run   (default 10000000  = 10M)
#   BASIN_FLEET_MAX   — max FLEET tier this box may run  (default 500)
#
# On provisioned hardware, raise the ceiling AND widen the ladder, e.g.:
#   BASIN_SCALE_MAX=1000000000 BASIN_SCALE_TIERS=1000000,10000000,100000000,1000000000 \
#   BASIN_FLEET_MAX=5000        BASIN_FLEET_TIERS=50,500,5000 \
#     ./benchmark/run/scale-suite.sh
#
# Preflight refuses to start if:
#   * another cargo / rustc / leaked test binary is running (the timing cards
#     are wall-clock-sensitive and the workspace shares one target/);
#   * the git tree is dirty (code provenance). Regenerated benchmark outputs
#     are exempt. ALLOW_DIRTY=1 overrides (harness smoke only).
#
# RAW output is tee'd (not grep-filtered) to /tmp/basin_scale_suite_<date>.log
# so progress is peekable mid-run:  tail -f /tmp/basin_scale_suite_*.log
#
# Env knobs (forwarded to the tests; see each test's header):
#   ROW ladder:   BASIN_SCALE_TIERS (default "1000000,10000000")
#                 BASIN_SCALE_BATCH / BASIN_SCALE_SAMPLES
#                 BASIN_TS_INGEST_BATCH
#   FLEET ladder: BASIN_FLEET_TIERS (default "50,500")
#                 BASIN_FLEET_NOISY_FRAC / _VICTIMS / _SEED_ROWS / _QUERIES /
#                 BASIN_FLEET_NOISY_BATCH
#   Saturation:   BASIN_SAT_CONNS / _SECS / _BATCH / _SEED_ROWS / _QUERIES
#
# Examples:
#   ./benchmark/run/scale-suite.sh                                  # box-safe: 1M+10M rows, 50+500 fleet
#   BASIN_SCALE_TIERS=5000 BASIN_FLEET_TIERS=5 ALLOW_DIRTY=1 \
#     ./benchmark/run/scale-suite.sh                                # tiny-N harness smoke
#   # provisioned hardware (full ladder to 1B / 5000):
#   BASIN_SCALE_MAX=1000000000 BASIN_SCALE_TIERS=1000000,10000000,100000000,1000000000 \
#   BASIN_FLEET_MAX=5000 BASIN_FLEET_TIERS=50,500,5000 ./benchmark/run/scale-suite.sh

set -uo pipefail

RUN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "${RUN_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/.." && pwd)"

LOG="/tmp/basin_scale_suite_$(date +%Y%m%d_%H%M%S).log"

log() { printf '[scale-suite] %s\n' "$*" | tee -a "${LOG}" >&2; }

# --- ladder config -----------------------------------------------------------
SCALE_TIERS="${BASIN_SCALE_TIERS:-1000000,10000000}"
FLEET_TIERS="${BASIN_FLEET_TIERS:-50,500}"
SCALE_MAX="${BASIN_SCALE_MAX:-10000000}"   # 10M box-ceiling by default
FLEET_MAX="${BASIN_FLEET_MAX:-500}"        # 500-project box-stress ceiling by default
RUN_LEGACY="${BASIN_SCALE_LEGACY:-0}"

# --- preflight: the box must be idle ----------------------------------------
busy="$( { pgrep -lx cargo; pgrep -lx rustc; pgrep -lf 'target/[^/]*/deps/'; } 2>/dev/null | sort -u || true )"
if [[ -n "${busy}" ]]; then
  log "REFUSING to start: the box is not idle. Offending processes:"
  printf '%s\n' "${busy}" | tee -a "${LOG}" >&2
  log "(another cargo/rustc/test binary would corrupt the shared target/ and skew the timings)"
  exit 2
fi

# --- preflight: git tree must be clean (provenance) -------------------------
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

# --- preflight: ladder tiers must be within the box ceiling -----------------
# Refuse a row tier above BASIN_SCALE_MAX or a fleet tier above BASIN_FLEET_MAX
# so nobody runs 1B / 5000 on a laptop by accident. Raise the ceiling
# explicitly on provisioned hardware.
ceiling_ok=1
IFS=',' read -r -a _scale_arr <<< "${SCALE_TIERS}"
for n in "${_scale_arr[@]}"; do
  n="${n//[[:space:]]/}"
  [[ -z "${n}" ]] && continue
  if (( n > SCALE_MAX )); then
    log "REFUSING row tier ${n}: exceeds BASIN_SCALE_MAX=${SCALE_MAX} (box-ceiling)."
    log "  100M/1B are PROVISIONED-hardware tiers. To run anyway: BASIN_SCALE_MAX=${n} (you confirm the box can take it)."
    ceiling_ok=0
  fi
done
IFS=',' read -r -a _fleet_arr <<< "${FLEET_TIERS}"
for n in "${_fleet_arr[@]}"; do
  n="${n//[[:space:]]/}"
  [[ -z "${n}" ]] && continue
  if (( n > FLEET_MAX )); then
    log "REFUSING fleet tier ${n}: exceeds BASIN_FLEET_MAX=${FLEET_MAX} (box-stress ceiling)."
    log "  5000 is a PROVISIONED-hardware tier. To run anyway: BASIN_FLEET_MAX=${n} (you confirm the box can take it)."
    ceiling_ok=0
  fi
done
if [[ "${ceiling_ok}" != "1" ]]; then
  log "Aborting: one or more requested tiers are above the box ceiling (see above)."
  exit 2
fi

# --- provenance header into the log ------------------------------------------
{
  echo "================ basin scale suite ================"
  echo "started     : $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "host        : $(hostname)"
  echo "commit      : $(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "dirty       : $([[ -n "${dirty}" ]] && echo "YES (ALLOW_DIRTY=1)" || echo no)"
  echo "row tiers   : ${SCALE_TIERS}   (ceiling BASIN_SCALE_MAX=${SCALE_MAX})"
  echo "fleet tiers : ${FLEET_TIERS}   (ceiling BASIN_FLEET_MAX=${FLEET_MAX})"
  echo "legacy cards: $([[ "${RUN_LEGACY}" == "1" ]] && echo "YES" || echo "no (BASIN_SCALE_LEGACY=1 to include)")"
  echo "log         : ${LOG}"
  echo "==================================================="
} | tee -a "${LOG}" >&2

overall_rc=0
declare -a RESULTS=()
SUITE_START="$(date +%s)"

# run_one <label> <test-binary> <filter> <env-prefix...>
# Each card is run SOLO (one cargo at a time); `--ignored <filter>` selects the
# documented-ignore tier test. RAW output goes through tee (peekable).
run_one() {
  local label="$1"; shift
  local test_bin="$1"; shift
  local filter="$1"; shift
  log "=== ${label}: starting (solo) ==="
  local t_start; t_start="$(date +%s)"
  ( cd "${REPO_ROOT}" && env "$@" \
      cargo test -p basin-integration-tests --test "${test_bin}" "${filter}" -- --ignored --nocapture \
  ) 2>&1 | tee -a "${LOG}"
  local rc=$?
  local t_secs=$(( $(date +%s) - t_start ))
  if [[ "${rc}" == "0" ]]; then
    RESULTS+=( "${label}: PASS (${t_secs}s)" )
    log "=== ${label}: PASS in ${t_secs}s ==="
  else
    RESULTS+=( "${label}: FAIL rc=${rc} (${t_secs}s)" )
    log "=== ${label}: FAIL (rc=${rc}) in ${t_secs}s — continuing ==="
    overall_rc=1
  fi
}

# --- ROW-SCALE ladder --------------------------------------------------------
for n in "${_scale_arr[@]}"; do
  n="${n//[[:space:]]/}"; [[ -z "${n}" ]] && continue
  run_one "scale_tier rows=${n}" scale_tier scale_tier_default BASIN_SCALE_ROWS="${n}"
  run_one "ts_ingest rows=${n}"  scale_timeseries_ingest ts_ingest_default BASIN_TS_INGEST_ROWS="${n}"
done

# --- FLEET ladder ------------------------------------------------------------
for n in "${_fleet_arr[@]}"; do
  n="${n//[[:space:]]/}"; [[ -z "${n}" ]] && continue
  run_one "fleet projects=${n}" multi_project_fleet_scale fleet_scale_default BASIN_FLEET_PROJECTS="${n}"
done

# Single-adversarial-tenant saturation (intensity axis, not fleet size).
run_one "noisy_tenant_saturation" noisy_tenant_saturation saturation_default

# --- legacy standalone cards (opt-in) ----------------------------------------
if [[ "${RUN_LEGACY}" == "1" ]]; then
  run_one "scale_10m_smoke (legacy)"     scale_10m_smoke     scale_10m_smoke
  run_one "scale_100m (legacy)"          scale_100m          scale_100m
  run_one "multi_project_fleet (legacy)" multi_project_fleet multi_project_fleet
fi

# --- verdict ------------------------------------------------------------------
SUITE_SECS=$(( $(date +%s) - SUITE_START ))
{
  echo
  echo "================ scale-suite verdict ================"
  for r in "${RESULTS[@]}"; do echo "  ${r}"; done
  echo "  total wall-clock : ${SUITE_SECS}s ($(( SUITE_SECS / 60 ))m $(( SUITE_SECS % 60 ))s)"
  echo "  artifacts        : ${BENCH_DIR}/data/scale_tier_<N>.json,"
  echo "                     ${BENCH_DIR}/data/scale_timeseries_ingest_<N>.json,"
  echo "                     ${BENCH_DIR}/data/fleet_<N>projects.json,"
  echo "                     ${BENCH_DIR}/data/noisy_tenant_saturation_<conns>conns.json"
  echo "  raw log          : ${LOG}"
  echo "====================================================="
} | tee -a "${LOG}"

exit "${overall_rc}"
