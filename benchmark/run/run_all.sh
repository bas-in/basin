#!/usr/bin/env bash
# benchmark/run/run_all.sh
#
# Umbrella: shared setup ONCE, then every config×category group launched
# with bounded concurrency, then one manifest rebuild per config dir, then
# teardown. Prints total wall-clock and FAILS LOUDLY if it exceeds the
# hard ceiling (10 min target + 2 min margin = 12 min) so a perf
# regression can never slip in silently.
#
#   ./benchmark/run/run_all.sh                  # localfs + seaweedfs
#   ONLY=localfs   ./benchmark/run/run_all.sh   # localfs groups only
#   ONLY=seaweedfs ./benchmark/run/run_all.sh   # seaweedfs groups only
#
# Does NOT touch the real-S3/cloud (data_real) config — that needs live
# cloud credentials and is operator-gated elsewhere.

set -uo pipefail

RUN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "${RUN_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/.." && pwd)"

HARD_CEILING_SECS="${HARD_CEILING_SECS:-900}"   # 15 min hard ceiling
TARGET_SECS="${TARGET_SECS:-600}"               # 10 min target
ONLY="${ONLY:-all}"                              # all | localfs | seaweedfs

# Max group invocations running at once. Each localfs group is itself
# multi-threaded; 4 concurrent cargo-test processes saturates the 8 cores
# without thrashing. seaweedfs groups are I/O bound on one SeaweedFS so
# they overlap localfs CPU work productively.
MAX_CONCURRENCY="${MAX_CONCURRENCY:-4}"

log() { printf '[run_all] %s\n' "$*" >&2; }

WALL_START="$(date +%s)"

# --- shared setup (build + SeaweedFS) — sourced so we inherit its EXIT
#     trap that tears SeaweedFS down no matter how this script ends. ------
setup_arg="all"
[[ "${ONLY}" == "localfs" ]] && setup_arg="localfs"
# shellcheck source=_setup.sh
. "${RUN_DIR}/_setup.sh" "${setup_arg}"

# _setup.sh runs `set -euo pipefail`; sourcing it leaks `errexit` into
# THIS shell. run_all.sh deliberately does NOT want errexit — it manages
# group exit codes itself (a failing benchmark group must not abort the
# umbrella or its sibling groups, and reap()'s `wait`/`kill -0` legitimately
# return non-zero). Restore our intended flags. We keep the EXIT trap
# _setup.sh installed (SeaweedFS teardown) — that survives `set +e`.
set +e
set -uo pipefail

# shellcheck source=_lib.sh
. "${RUN_DIR}/_lib.sh"

# --- crash-safe teardown ---------------------------------------------------
#
# CRITICAL: backgrounded group jobs spawn `cargo test` which spawns the
# test binaries (tokio multi-thread runtimes). If run_all.sh dies or is
# Ctrl-C'd, those orphan and KEEP RUNNING — a leaked scaling_* binary
# from a dead run wedges the next run (port / fixture / PG contention).
# So we record every launched group PID and, on ANY exit, kill each
# group PID *and its whole descendant tree*, then run _setup.sh's
# SeaweedFS teardown. We chain (not replace) the trap _setup.sh set.
LAUNCHED_PIDS=()

kill_tree() {
  # Recursively SIGKILL pid + all descendants (bash 3.2: pgrep -P walk).
  local p="$1" c
  for c in $(pgrep -P "${p}" 2>/dev/null); do
    kill_tree "${c}"
  done
  kill -9 "${p}" 2>/dev/null || true
}

_run_all_cleanup() {
  local code=$? p
  for p in ${LAUNCHED_PIDS[@]+"${LAUNCHED_PIDS[@]}"}; do
    if kill -0 "${p}" 2>/dev/null; then
      kill_tree "${p}"
    fi
  done
  # Belt-and-braces: nuke any of our test binaries still alive, regardless
  # of which profile dir they ran from (bench-fast / release / debug).
  pkill -9 -f "target/[^/]*/deps/.*-[0-9a-f]* --nocapture" 2>/dev/null || true
  # _setup.sh installed its own EXIT trap (SeaweedFS). Invoke it now so
  # SeaweedFS is also torn down on this path.
  if declare -f _setup_cleanup >/dev/null 2>&1; then
    _setup_cleanup || true
  fi
  return $code
}
trap _run_all_cleanup EXIT INT TERM

# --- select groups ----------------------------------------------------------
groups=()
for g in "${ALL_GROUPS[@]}"; do
  case "${ONLY}" in
    all) groups+=( "${g}" ) ;;
    localfs)   [[ "${g}" == localfs_*   ]] && groups+=( "${g}" ) ;;
    seaweedfs) [[ "${g}" == seaweedfs_* ]] && groups+=( "${g}" ) ;;
  esac
done
log "groups: ${groups[*]}"
log "concurrency: ${MAX_CONCURRENCY}"

LOG_DIR="${RUN_DIR}/.logs"
mkdir -p "${LOG_DIR}"
rm -f "${LOG_DIR}"/*.log "${LOG_DIR}"/*.rc 2>/dev/null || true

# --- bounded-concurrency job pool ------------------------------------------
#
# macOS ships bash 3.2 — no associative arrays, no `wait -n`. So the pool
# is two parallel indexed arrays (POOL_G[i]=group, POOL_P[i]=pid) reaped
# by a short poll loop. Each group's exit status is persisted to
# <LOG_DIR>/<group>.rc so the verdict survives subshells.
POOL_G=()
POOL_P=()

pool_size() { echo "${#POOL_P[@]}"; }

# Reap any finished jobs (non-blocking). Rebuilds the pool with the
# still-running entries.
reap() {
  local i new_g=() new_p=() pid g rc
  for (( i = 0; i < ${#POOL_P[@]}; i++ )); do
    pid="${POOL_P[$i]}"
    g="${POOL_G[$i]}"
    if kill -0 "${pid}" 2>/dev/null; then
      new_g+=( "${g}" )
      new_p+=( "${pid}" )
    else
      wait "${pid}"; rc=$?
      echo "${rc}" > "${LOG_DIR}/${g}.rc"
      log "group '${g}' done (rc=${rc})"
    fi
  done
  POOL_G=( "${new_g[@]:-}" )
  POOL_P=( "${new_p[@]:-}" )
  # Drop the empty-string placeholder ${arr[@]:-} leaves on an empty array.
  if [[ "${#POOL_P[@]}" -eq 1 && -z "${POOL_P[0]}" ]]; then
    POOL_G=()
    POOL_P=()
  fi
}

# Block until the pool has room (< MAX_CONCURRENCY) or is empty.
wait_for_slot() {
  while (( $(pool_size) >= MAX_CONCURRENCY )); do
    sleep 1
    reap
  done
}

drain() {
  while (( $(pool_size) > 0 )); do
    sleep 1
    reap
  done
}

for g in "${groups[@]}"; do
  wait_for_slot
  log "launching group '${g}'"
  ( bash "${RUN_DIR}/_group.sh" "${g}" ) > "${LOG_DIR}/${g}.log" 2>&1 &
  gpid=$!
  POOL_G+=( "${g}" )
  POOL_P+=( "${gpid}" )
  LAUNCHED_PIDS+=( "${gpid}" )
done

drain

# --- collect per-group results ---------------------------------------------
overall_rc=0
declare -a FAILED_GROUPS=()
for g in "${groups[@]}"; do
  rc="$(cat "${LOG_DIR}/${g}.rc" 2>/dev/null || echo 1)"
  if [[ "${rc}" != "0" ]]; then
    overall_rc=1
    FAILED_GROUPS+=( "${g}" )
  fi
done

# --- one manifest rebuild per config data dir ------------------------------
declare -a DIRS=()
[[ "${ONLY}" == "all" || "${ONLY}" == "localfs"   ]] && DIRS+=( "${BENCH_DIR}/data" )
[[ "${ONLY}" == "all" || "${ONLY}" == "seaweedfs" ]] && DIRS+=( "${BENCH_DIR}/data_seaweedfs" )

# --since the wall-clock start: a card whose generated_at predates this
# run did NOT regenerate (its test failed or was skipped) and is OMITTED
# from the manifest rather than carried forward with stale numbers.
log "rebuilding manifests from JSONs this run produced (--since ${WALL_START}) ..."
if ! python3 "${RUN_DIR}/merge_manifest.py" --since "${WALL_START}" "${DIRS[@]}"; then
  log "WARNING: manifest rebuild reported an error"
  overall_rc=1
fi

# --- re-bundle so RESULTS_*.md / results.js / HTML pick up fresh data ------
log "re-bundling dashboards ..."
( cd "${REPO_ROOT}" && python3 benchmark/bundle.py ) >&2 || \
  log "WARNING: bundle.py reported an error"

# --- teardown: explicitly tear SeaweedFS down now (the sourced EXIT trap
#     also covers crash paths). --------------------------------------------
stop_seaweed || true

# --- wall-clock verdict -----------------------------------------------------
WALL_END="$(date +%s)"
WALL_SECS=$(( WALL_END - WALL_START ))
WALL_MIN=$(( WALL_SECS / 60 ))
WALL_REM=$(( WALL_SECS % 60 ))

echo
echo "================ benchmark/run/run_all.sh ================"
echo "  groups run        : ${#groups[@]} (${groups[*]})"
if (( ${#FAILED_GROUPS[@]} > 0 )); then
  echo "  FAILED groups     : ${FAILED_GROUPS[*]}"
fi
echo "  total wall-clock  : ${WALL_SECS}s (${WALL_MIN}m ${WALL_REM}s)"
echo "  target            : ${TARGET_SECS}s (10m)"
echo "  hard ceiling      : ${HARD_CEILING_SECS}s (12m)"
echo "========================================================="

if (( WALL_SECS > HARD_CEILING_SECS )); then
  echo "FAIL: wall-clock ${WALL_SECS}s exceeded hard ceiling ${HARD_CEILING_SECS}s." >&2
  exit 1
fi
if (( WALL_SECS > TARGET_SECS )); then
  echo "WARN: wall-clock ${WALL_SECS}s over the ${TARGET_SECS}s target (within margin)." >&2
fi

exit "${overall_rc}"
