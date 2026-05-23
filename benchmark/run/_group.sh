#!/usr/bin/env bash
# benchmark/run/_group.sh <group-name>
#
# Run ONE config×category group of integration tests. Independently
# runnable: assumes _setup.sh already built the binaries (and, for
# seaweedfs groups, that a SeaweedFS S3 gateway is live). Writes only its
# own group's JSON sidecars into the config's data dir.
#
#   ./_group.sh localfs_viability
#   ./_group.sh seaweedfs_scaling
#
# Exit code: 0 if cargo reported all selected tests ok (or cleanly
# skipped), non-zero otherwise. Per-test JSON emission is a side effect of
# the tests themselves (see tests/integration/src/benchmark.rs).
#
# `bins` is populated by an indirect eval array deref; shellcheck's
# "referenced but not assigned" (SC2154) is a false positive here.
# shellcheck disable=SC2154

set -uo pipefail

GROUP="${1:?usage: _group.sh <group-name>}"

RUN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_lib.sh
. "${RUN_DIR}/_lib.sh"

spec="$(group_spec "${GROUP}")" || { echo "[group] unknown group: ${GROUP}" >&2; exit 2; }
read -r arrvar config threads ignored data_dir <<<"${spec}"

# Expand the binary list for this group (indirect array deref).
eval "bins=( \"\${${arrvar}[@]}\" )"
if [[ "${#bins[@]}" -eq 0 ]]; then
  echo "[group ${GROUP}] empty binary list — nothing to run" >&2
  exit 0
fi

log() { printf '[group %s] %s\n' "${GROUP}" "$*" >&2; }

# Per-binary wall-clock cap. A test that livelocks/hangs at HEAD (e.g. an
# engine bug unrelated to the benchmark) must NOT wedge the group or the
# whole run. On timeout the binary's process tree is killed; it simply
# does not re-emit its JSON, so merge_manifest.py omits that card with a
# logged reason. macOS has no `timeout(1)`, so we watchdog it ourselves.
#
# Was 240s when each binary's "run" actually re-invoked `cargo test --test X`,
# which paid a per-binary cargo lock-acquire + re-link cost (~30-120s) before
# the test even started. The fix below execs the prebuilt binary directly, so
# 240s is now an *all-test* cap — bumped to 600s for binaries that contain
# multiple #[test] fns (per_project_provisioning has 3 with N=100/50/5).
PER_TEST_TIMEOUT="${PER_TEST_TIMEOUT:-600}"   # 10 min/binary hard cap

# Per-config environment for the test binary.
declare -a env_prefix
env_prefix=()
if [[ "${config}" == "seaweedfs" ]]; then
  # Redirect the s3_* report_real_* sidecars into data_seaweedfs/, and
  # point the test-config loader at the SeaweedFS .toml.
  env_prefix+=(
    "BASIN_BENCHMARK_DIR=benchmark/${data_dir}"
    "BASIN_TEST_CONFIG=${SEAWEED_TEST_CONFIG:-${BENCH_REPO_ROOT}/.basin-test.seaweedfs.toml}"
  )
fi

# libtest runner args. The binary itself accepts these directly when invoked
# without cargo.
runner_args=( --nocapture --test-threads="${threads}" )
if [[ "${ignored}" == "1" ]]; then
  runner_args+=( --ignored )
fi

# bin → executable path map written by _setup.sh.
BIN_MAP="${BENCH_BIN_MAP:-${RUN_DIR}/.ready/bench_bins.tsv}"
if [[ ! -f "${BIN_MAP}" ]]; then
  echo "[group ${GROUP}] missing ${BIN_MAP} — run _setup.sh first" >&2
  exit 2
fi

# Resolve <bin_name> -> /path/to/executable (newest matching record, in case
# of stale entries from a previous run sharing the same .ready dir).
resolve_bin() {
  local name="$1"
  awk -F'\t' -v n="${name}" '$1==n {print $2}' "${BIN_MAP}" | tail -n 1
}

# Kill a pid and all its descendants (test binary -> tokio worker pool).
kill_tree() {
  local p="$1" c
  for c in $(pgrep -P "${p}" 2>/dev/null); do kill_tree "${c}"; done
  kill -9 "${p}" 2>/dev/null || true
}

# Run ONE test binary under the watchdog. Returns the binary's rc, or 124
# if it was killed for exceeding PER_TEST_TIMEOUT, or 125 if the executable
# is not on disk (build did not produce it).
run_one() {
  local bin="$1" exe b_start b_pid waited rc
  exe="$(resolve_bin "${bin}")"
  if [[ -z "${exe}" || ! -x "${exe}" ]]; then
    log "MISSING: no executable for '${bin}' in ${BIN_MAP} (rebuild needed)"
    return 125
  fi
  b_start="$(date +%s)"
  (
    cd "${BENCH_REPO_ROOT}" || exit 1
    env "${env_prefix[@]+"${env_prefix[@]}"}" \
      "${exe}" "${runner_args[@]}"
  ) &
  b_pid=$!
  waited=0
  while kill -0 "${b_pid}" 2>/dev/null; do
    if (( waited >= PER_TEST_TIMEOUT )); then
      log "TIMEOUT: '${bin}' exceeded ${PER_TEST_TIMEOUT}s — killing (card will be omitted)"
      kill_tree "${b_pid}"
      wait "${b_pid}" 2>/dev/null || true
      return 124
    fi
    sleep 2
    waited=$(( waited + 2 ))
  done
  wait "${b_pid}"; rc=$?
  log "  '${bin}' done in $(( $(date +%s) - b_start ))s (rc=${rc})"
  return "${rc}"
}

log "running ${#bins[@]} binaries, threads=${threads}, ignored=${ignored}, data_dir=${data_dir}, per-test-cap=${PER_TEST_TIMEOUT}s"
log "binaries: ${bins[*]}"

start_ts="$(date +%s)"
rc=0
timed_out=()
missing=()
for b in "${bins[@]}"; do
  run_one "${b}"
  brc=$?
  if [[ "${brc}" == "124" ]]; then
    timed_out+=( "${b}" )
    rc=1
  elif [[ "${brc}" == "125" ]]; then
    missing+=( "${b}" )
    rc=1
  elif [[ "${brc}" != "0" ]]; then
    rc=1
  fi
done
if (( ${#missing[@]} > 0 )); then
  log "missing binaries (build did not produce executable): ${missing[*]}"
fi
if (( ${#timed_out[@]} > 0 )); then
  log "timed-out binaries (omitted from manifest): ${timed_out[*]}"
fi
end_ts="$(date +%s)"
log "finished in $(( end_ts - start_ts ))s (cargo rc=${rc})"
exit "${rc}"
