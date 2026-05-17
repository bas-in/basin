#!/usr/bin/env bash
# benchmark/speed/point_read/run.sh
#
# Point-read latency benchmark: 1000 single-row lookups per target.
# Measures p50/p95/p99 round-trip latency for SELECT WHERE id = <random_id>.
#
# USAGE
#   ./benchmark/speed/point_read/run.sh [--dry-run]
#
# DSN CONFIG (any combination; missing targets are skipped gracefully):
#   NEON_DATABASE_URL      postgres://...
#   SUPABASE_DATABASE_URL  postgres://...
#   BASIN_DATABASE_URL     postgres://...
#   REGION_LABEL           provenance label stamped into the JSON (e.g. "fra")
#
# OPTIONAL:
#   SPEED_ITERATIONS   number of timed iterations per target (default: 1000)
#   SPEED_WARMUP       warm-up iterations discarded from stats (default: 100)
#   SPEED_ROW_COUNT    rows to preload (default: 100000)
#   SPEED_OUT_DIR      output directory (default: benchmark/speed/out/)

set -euo pipefail

SCENARIO="point_read"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPEED_SCENARIO="${SCENARIO}"
export SPEED_SCENARIO

# shellcheck source=benchmark/speed/_lib.sh
source "${SCRIPT_DIR}/../_lib.sh"

# ---------- CLI flags ---------------------------------------------------------

DRY_RUN=0
for arg in "$@"; do
    case "${arg}" in
        --dry-run) DRY_RUN=1 ;;
        *)
            log "unknown flag: ${arg}"
            printf 'Usage: %s [--dry-run]\n' "$0" >&2
            exit 1
            ;;
    esac
done

# ---------- config ------------------------------------------------------------

load_toml_config

REGION_LABEL="${REGION_LABEL:-local}"
ITERATIONS="${SPEED_ITERATIONS:-1000}"
WARMUP="${SPEED_WARMUP:-100}"
ROW_COUNT="${SPEED_ROW_COUNT:-100000}"
OUT_DIR="${SPEED_OUT_DIR:-${REPO_ROOT}/benchmark/speed/out}"
SCHEMA_SQL="${SCRIPT_DIR}/schema.sql"

# Target id in the middle of the range (avoids cold/hot edge effects).
TARGET_ID=$(( ROW_COUNT / 2 + 7 ))
BATCH_SIZE=1000

# ---------- dependency check --------------------------------------------------

if ! command -v psql >/dev/null 2>&1; then
    log "ERROR: psql not found on PATH."
    log "  macOS:  brew install libpq && brew link --force libpq"
    log "  Ubuntu: apt-get install -y postgresql-client"
    exit 1
fi

# ---------- target detection --------------------------------------------------

if ! check_targets "${SCENARIO}"; then
    exit 0  # no DSNs — skip cleanly
fi

# ---------- dry run -----------------------------------------------------------

if [[ "${DRY_RUN}" -eq 1 ]]; then
    info "=== DRY RUN — no connections will be made ==="
    info ""
    info "Plan:"
    info "  Scenario       : ${SCENARIO}"
    info "  Region label   : ${REGION_LABEL}"
    info "  Row count      : ${ROW_COUNT}"
    info "  Iterations     : ${ITERATIONS} (+ ${WARMUP} warm-up discarded)"
    info "  Target id      : ${TARGET_ID}"
    info "  Active targets : ${ACTIVE_TARGETS}"
    if [[ "${HAS_NEON}" -eq 1 ]];     then info "  NEON     : $(redact_url "${NEON_DATABASE_URL}")"; fi
    if [[ "${HAS_SUPABASE}" -eq 1 ]]; then info "  SUPABASE : $(redact_url "${SUPABASE_DATABASE_URL}")"; fi
    if [[ "${HAS_BASIN}" -eq 1 ]];    then info "  BASIN    : $(redact_url "${BASIN_DATABASE_URL}")"; fi
    info ""
    info "Output: ${OUT_DIR}/compare_speed_point_read_${REGION_LABEL}_<ts>.json"
    info "=== END DRY RUN ==="
    exit 0
fi

# ---------- setup -------------------------------------------------------------

mkdir -p "${OUT_DIR}"
TIMESTAMP="$(date +%Y%m%dT%H%M%S)"
OUT_JSON="${OUT_DIR}/compare_speed_point_read_${REGION_LABEL}_${TIMESTAMP}.json"

log "=== point_read benchmark (region=${REGION_LABEL}, rows=${ROW_COUNT}, iters=${ITERATIONS}) ==="

# ---------- teardown trap -----------------------------------------------------

NEON_PREPARED=0
SUPABASE_PREPARED=0
BASIN_PREPARED=0

cleanup() {
    local _ec=$?
    log "teardown..."
    if [[ "${NEON_PREPARED}" -eq 1 ]];     then drop_schema "${NEON_DATABASE_URL}"     "bench_speed_point_read"; fi
    if [[ "${SUPABASE_PREPARED}" -eq 1 ]]; then drop_schema "${SUPABASE_DATABASE_URL}" "bench_speed_point_read"; fi
    if [[ "${BASIN_PREPARED}" -eq 1 ]];    then drop_schema "${BASIN_DATABASE_URL}"    "bench_speed_point_read"; fi
    log "teardown complete."
    exit "${_ec}"
}
trap cleanup EXIT

# ---------- per-target benchmark function ------------------------------------

# benchmark_target LABEL URL PREPARED_FLAG_VAR
# Sets: tgt_p50, tgt_p95, tgt_p99 (ms floats or "null")
benchmark_target() {
    local _label="$1"
    local _url="$2"
    local _flag_var="$3"

    log ""
    log "--- ${_label} ---"

    # Schema + data load
    log "  creating schema..."
    if ! create_schema "${_url}" "${SCHEMA_SQL}"; then
        log "  ERROR: schema creation failed for ${_label}"
        tgt_p50="null"; tgt_p95="null"; tgt_p99="null"
        return
    fi
    printf -v "${_flag_var}" '%s' '1'

    log "  loading ${ROW_COUNT} rows..."
    local _tmpfile
    _tmpfile="$(mktemp /tmp/basin_speed_insert_XXXXXX.sql)"
    trap "rm -f ${_tmpfile}" RETURN

    {
        printf 'BEGIN;\n'
        local _i=0
        while [[ "${_i}" -lt "${ROW_COUNT}" ]]; do
            local _end=$(( _i + BATCH_SIZE ))
            [[ "${_end}" -gt "${ROW_COUNT}" ]] && _end="${ROW_COUNT}"
            printf 'INSERT INTO bench_speed_point_read.events (id, project_id, action, created_at, payload) VALUES\n'
            local _first=1
            local _j="${_i}"
            while [[ "${_j}" -lt "${_end}" ]]; do
                [[ "${_first}" -eq 0 ]] && printf ',\n'
                printf "(%d, gen_random_uuid(), 'action_%d', now(), 'payload-%040d')" \
                    "${_j}" "$(( _j % 10 ))" "${_j}"
                _first=0
                (( _j++ )) || true
            done
            printf ';\n'
            (( _i += BATCH_SIZE )) || true
        done
        printf 'COMMIT;\n'
    } > "${_tmpfile}"

    psql "${_url}" --no-psqlrc -f "${_tmpfile}" >/dev/null

    # Timed iterations
    log "  timing ${ITERATIONS} point reads (+ ${WARMUP} warm-up)..."
    local _sql="SELECT id, project_id, action, created_at, payload \
FROM bench_speed_point_read.events WHERE id = ${TARGET_ID}"

    time_query "${_url}" "${_sql}" "${ITERATIONS}" "${WARMUP}"

    if [[ "${TIME_QUERY_OK}" -eq 0 ]]; then
        log "  ERROR: all iterations failed for ${_label}"
        tgt_p50="null"; tgt_p95="null"; tgt_p99="null"
        return
    fi

    tgt_p50="$(p50_of "${TIME_QUERY_SAMPLES}")"
    tgt_p95="$(p95_of "${TIME_QUERY_SAMPLES}")"
    tgt_p99="$(p99_of "${TIME_QUERY_SAMPLES}")"
    log "  ${_label}: p50=${tgt_p50}ms  p95=${tgt_p95}ms  p99=${tgt_p99}ms"
}

# ---------- run all targets ---------------------------------------------------

n_p50="null"; n_p95="null"; n_p99="null"
s_p50="null"; s_p95="null"; s_p99="null"
b_p50="null"; b_p95="null"; b_p99="null"

tgt_p50="null"; tgt_p95="null"; tgt_p99="null"

if [[ "${HAS_NEON}" -eq 1 ]]; then
    benchmark_target "Neon"     "${NEON_DATABASE_URL}"     "NEON_PREPARED"
    n_p50="${tgt_p50}"; n_p95="${tgt_p95}"; n_p99="${tgt_p99}"
fi

if [[ "${HAS_SUPABASE}" -eq 1 ]]; then
    benchmark_target "Supabase" "${SUPABASE_DATABASE_URL}" "SUPABASE_PREPARED"
    s_p50="${tgt_p50}"; s_p95="${tgt_p95}"; s_p99="${tgt_p99}"
fi

if [[ "${HAS_BASIN}" -eq 1 ]]; then
    benchmark_target "Basin"    "${BASIN_DATABASE_URL}"    "BASIN_PREPARED"
    b_p50="${tgt_p50}"; b_p95="${tgt_p95}"; b_p99="${tgt_p99}"
fi

# ---------- stdout summary table ---------------------------------------------

log ""
log "=== Summary: point_read (${REGION_LABEL}) ==="
printf '\n%-30s %15s %15s %15s\n' "Metric" "Neon" "Supabase" "Basin"
printf '%-30s %15s %15s %15s\n' "$(printf '%0.s-' {1..30})" "$(printf '%0.s-' {1..15})" "$(printf '%0.s-' {1..15})" "$(printf '%0.s-' {1..15})"
printf '%-30s %15s %15s %15s\n' "Point read p50 (ms)" "${n_p50}" "${s_p50}" "${b_p50}"
printf '%-30s %15s %15s %15s\n' "Point read p95 (ms)" "${n_p95}" "${s_p95}" "${b_p95}"
printf '%-30s %15s %15s %15s\n' "Point read p99 (ms)" "${n_p99}" "${s_p99}" "${b_p99}"
printf '\n'

# ---------- JSON output -------------------------------------------------------

WINNER_P50="$(winner_of_3 "${n_p50}" "${s_p50}" "${b_p50}" "neon" "supabase" "basin")"
WINNER_P95="$(winner_of_3 "${n_p95}" "${s_p95}" "${b_p95}" "neon" "supabase" "basin")"
WINNER_P99="$(winner_of_3 "${n_p99}" "${s_p99}" "${b_p99}" "neon" "supabase" "basin")"

# Build ratio_text for p50 (compare basin vs the best competitor).
RATIO_P50="null"
if [[ "${b_p50}" != "null" && "${WINNER_P50}" == "basin" ]]; then
    # Basin won — report ratio vs best non-basin.
    _best_competitor="null"
    if [[ "${n_p50}" != "null" && "${s_p50}" != "null" ]]; then
        _best_competitor="$(python3 -c "print(min(${n_p50}, ${s_p50}))")"
        _best_label="$(python3 -c "print('Neon' if ${n_p50} <= ${s_p50} else 'Supabase')")"
    elif [[ "${n_p50}" != "null" ]]; then
        _best_competitor="${n_p50}"; _best_label="Neon"
    elif [[ "${s_p50}" != "null" ]]; then
        _best_competitor="${s_p50}"; _best_label="Supabase"
    fi
    [[ "${_best_competitor}" != "null" ]] && \
        RATIO_P50="$(ratio_text "${b_p50}" "${_best_competitor}" "${_best_label}")"
fi

UNIX_NOW="$(date +%s)"

cat > "${OUT_JSON}" <<JSONEOF
{
  "kind": "compare",
  "id": "speed_point_read_${REGION_LABEL}",
  "name": "Point-read latency (${REGION_LABEL}, ${ROW_COUNT} rows)",
  "claim": "1000 single-row lookups (SELECT WHERE id = N) against a ${ROW_COUNT}-row unindexed table. Warm-up iterations (${WARMUP}) are discarded before sampling. p50/p95/p99 computed via linear interpolation on sorted per-iteration millisecond timings.",
  "available": true,
  "region": $(jq_str "${REGION_LABEL}"),
  "row_count": ${ROW_COUNT},
  "iterations": ${ITERATIONS},
  "warmup": ${WARMUP},
  "generated_at": "@${UNIX_NOW}",
  "metrics": [
    {
      "label": "p50 (ms)",
      "neon":     $(jq_num "${n_p50}"),
      "supabase": $(jq_num "${s_p50}"),
      "basin":    $(jq_num "${b_p50}"),
      "unit": "ms",
      "better": $(jq_str "${WINNER_P50}"),
      "ratio_text": $(jq_str "${RATIO_P50}"),
      "note": "Median round-trip latency for SELECT WHERE id = ${TARGET_ID} (middle of ${ROW_COUNT}-row range, no index)."
    },
    {
      "label": "p95 (ms)",
      "neon":     $(jq_num "${n_p95}"),
      "supabase": $(jq_num "${s_p95}"),
      "basin":    $(jq_num "${b_p95}"),
      "unit": "ms",
      "better": $(jq_str "${WINNER_P95}"),
      "ratio_text": null,
      "note": null
    },
    {
      "label": "p99 (ms)",
      "neon":     $(jq_num "${n_p99}"),
      "supabase": $(jq_num "${s_p99}"),
      "basin":    $(jq_num "${b_p99}"),
      "unit": "ms",
      "better": $(jq_str "${WINNER_P99}"),
      "ratio_text": null,
      "note": null
    }
  ],
  "note": "Each target received ${ITERATIONS} point reads against the same ${ROW_COUNT}-row table. Random target id = ${TARGET_ID} drawn from the preloaded set. Targets run sequentially to avoid cross-endpoint interference."
}
JSONEOF

log ""
log "result JSON written: ${OUT_JSON}"

# Explicit teardown (trap will also fire but this surfaces errors earlier).
log "dropping schemas..."
if [[ "${NEON_PREPARED}" -eq 1 ]];     then drop_schema "${NEON_DATABASE_URL}"     "bench_speed_point_read"; NEON_PREPARED=0;     fi
if [[ "${SUPABASE_PREPARED}" -eq 1 ]]; then drop_schema "${SUPABASE_DATABASE_URL}" "bench_speed_point_read"; SUPABASE_PREPARED=0; fi
if [[ "${BASIN_PREPARED}" -eq 1 ]];    then drop_schema "${BASIN_DATABASE_URL}"    "bench_speed_point_read"; BASIN_PREPARED=0;    fi
log "done."
