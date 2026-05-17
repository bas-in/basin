#!/usr/bin/env bash
# benchmark/speed/run_all_speed.sh
#
# Top-level runner for all 5 Basin speed benchmark scenarios.
# Calls each scenario's run.sh in sequence; honors the shared
# .basin-three-way.toml config (same file used by benchmark/three_way/).
#
# USAGE
#   ./benchmark/speed/run_all_speed.sh [--dry-run] [--scenario SCENARIO]
#
# FLAGS
#   --dry-run               Pass --dry-run to every scenario (no connections).
#   --scenario SCENARIO     Run a single scenario only.
#                           Valid: point_read range_scan bulk_insert aggregate concurrent
#
# DSN CONFIG (any combination; missing targets are skipped gracefully):
#   NEON_DATABASE_URL      postgres://...
#   SUPABASE_DATABASE_URL  postgres://...
#   BASIN_DATABASE_URL     postgres://...
#   REGION_LABEL           provenance label (e.g. "fra")
#
# OPTIONAL ENV VARS:
#   SPEED_ITERATIONS    timed iterations (each scenario has its own default)
#   SPEED_WARMUP        warm-up iterations (each scenario has its own default)
#   SPEED_ROW_COUNT     preloaded rows (each scenario has its own default)
#   SPEED_OUT_DIR       output directory (default: benchmark/speed/out/)
#
# TOML CONFIG:
#   The .basin-three-way.toml at the repo root (gitignored) is automatically
#   loaded if present — same format as benchmark/three_way/.  Additional keys:
#     [endpoints]
#     speed_iterations = 100
#     speed_out_dir    = "/path/to/out"
#
# DEPENDENCIES:
#   psql (libpq) — must be on PATH.
#   python3      — stdlib only, used for percentile calc and TOML loading.
#
# SAFETY:
#   If ALL DSNs are absent, every scenario skips cleanly and the script exits 0.
#   If only some DSNs are present, only those targets are measured.
#   No connections are made without at least one DSN.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---------- CLI flags ---------------------------------------------------------

DRY_RUN=0
ONLY_SCENARIO=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        --scenario)
            shift
            ONLY_SCENARIO="${1:-}"
            [[ -z "${ONLY_SCENARIO}" ]] && { printf 'ERROR: --scenario requires an argument\n' >&2; exit 1; }
            shift
            ;;
        *)
            printf 'ERROR: unknown flag: %s\n' "$1" >&2
            printf 'Usage: %s [--dry-run] [--scenario SCENARIO]\n' "$0" >&2
            exit 1
            ;;
    esac
done

# Ordered scenario list.
ALL_SCENARIOS=(point_read range_scan bulk_insert aggregate concurrent)

# Validate --scenario if provided.
if [[ -n "${ONLY_SCENARIO}" ]]; then
    _valid=0
    for _s in "${ALL_SCENARIOS[@]}"; do
        [[ "${_s}" == "${ONLY_SCENARIO}" ]] && _valid=1 && break
    done
    if [[ "${_valid}" -eq 0 ]]; then
        printf 'ERROR: unknown scenario %q. Valid: %s\n' \
            "${ONLY_SCENARIO}" "${ALL_SCENARIOS[*]}" >&2
        exit 1
    fi
fi

# ---------- helpers -----------------------------------------------------------

log_all()  { printf '[speed/all] %s\n' "$*" >&2; }
info_all() { printf '[speed/all] %s\n' "$*"; }

# ---------- dependency check --------------------------------------------------

if ! command -v psql >/dev/null 2>&1; then
    log_all "ERROR: psql not found on PATH."
    log_all "  macOS:  brew install libpq && brew link --force libpq"
    log_all "  Ubuntu: apt-get install -y postgresql-client"
    exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
    log_all "WARNING: python3 not found — TOML config loading and percentile calc disabled."
    log_all "         Install python3 to enable .basin-three-way.toml support."
fi

# ---------- execution plan ----------------------------------------------------

if [[ "${DRY_RUN}" -eq 1 ]]; then
    info_all "=== DRY RUN — no connections will be made ==="
    info_all ""
    info_all "Scenarios to run:"
    if [[ -n "${ONLY_SCENARIO}" ]]; then
        info_all "  ${ONLY_SCENARIO} (--scenario filter)"
    else
        for _s in "${ALL_SCENARIOS[@]}"; do
            info_all "  ${_s}"
        done
    fi
    info_all ""
    info_all "Config:"
    info_all "  TOML:       ${REPO_ROOT:-<repo root>}/.basin-three-way.toml (if present)"
    info_all "  NEON:       ${NEON_DATABASE_URL:-(not set)}"
    info_all "  SUPABASE:   ${SUPABASE_DATABASE_URL:-(not set)}"
    info_all "  BASIN:      ${BASIN_DATABASE_URL:-(not set)}"
    info_all "  REGION:     ${REGION_LABEL:-(not set — will default to 'local')}"
    info_all "  OUT_DIR:    ${SPEED_OUT_DIR:-<benchmark/speed/out/>}"
    info_all ""
fi

# ---------- run scenarios -----------------------------------------------------

_scenarios_to_run=("${ALL_SCENARIOS[@]}")
if [[ -n "${ONLY_SCENARIO}" ]]; then
    _scenarios_to_run=("${ONLY_SCENARIO}")
fi

_flags=()
[[ "${DRY_RUN}" -eq 1 ]] && _flags+=(--dry-run)

_pass=0
_skip=0
_fail=0

for _scenario in "${_scenarios_to_run[@]}"; do
    _runner="${SCRIPT_DIR}/${_scenario}/run.sh"

    if [[ ! -f "${_runner}" ]]; then
        log_all "WARNING: runner not found for scenario '${_scenario}': ${_runner}"
        (( _skip++ )) || true
        continue
    fi

    info_all ""
    info_all ">>> Running scenario: ${_scenario}"
    info_all "    Runner: ${_runner}"

    if bash "${_runner}" "${_flags[@]+"${_flags[@]}"}"; then
        (( _pass++ )) || true
        info_all "<<< ${_scenario}: done"
    else
        _rc=$?
        log_all "<<< ${_scenario}: FAILED (exit ${_rc})"
        (( _fail++ )) || true
    fi
done

# ---------- summary -----------------------------------------------------------

info_all ""
info_all "=== run_all_speed.sh complete ==="
info_all "  passed:  ${_pass}"
info_all "  skipped: ${_skip}"
info_all "  failed:  ${_fail}"

if [[ "${_fail}" -gt 0 ]]; then
    log_all "One or more scenarios failed — see output above."
    exit 1
fi

exit 0
