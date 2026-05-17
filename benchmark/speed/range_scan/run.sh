#!/usr/bin/env bash
# benchmark/speed/range_scan/run.sh
#
# Range-scan latency benchmark: p50/p95/p99 at three window sizes
# (1k / 100k / 1M rows) against a ~10M-row table.
#
# USAGE
#   ./benchmark/speed/range_scan/run.sh [--dry-run]
#
# DSN CONFIG (any combination; missing targets are skipped gracefully):
#   NEON_DATABASE_URL / SUPABASE_DATABASE_URL / BASIN_DATABASE_URL / REGION_LABEL
#
# OPTIONAL:
#   SPEED_ITERATIONS    timed iterations per window per target (default: 50)
#   SPEED_WARMUP        warm-up iterations discarded from stats (default: 5)
#   SPEED_ROW_COUNT     rows to preload (default: 10000000)
#   SPEED_OUT_DIR       output directory (default: benchmark/speed/out/)

set -euo pipefail

SCENARIO="range_scan"
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
ITERATIONS="${SPEED_ITERATIONS:-50}"
WARMUP="${SPEED_WARMUP:-5}"
ROW_COUNT="${SPEED_ROW_COUNT:-10000000}"
OUT_DIR="${SPEED_OUT_DIR:-${REPO_ROOT}/benchmark/speed/out}"
SCHEMA_SQL="${SCRIPT_DIR}/schema.sql"
BATCH_SIZE=1000

# Window anchor at 1/4 of the range (avoids boundary effects).
ANCHOR=$(( ROW_COUNT / 4 ))

# Window boundaries.
LO_1K="${ANCHOR}";            HI_1K=$(( ANCHOR + 999 ))
LO_100K="${ANCHOR}";          HI_100K=$(( ANCHOR + 99999 ))
LO_1M="${ANCHOR}";            HI_1M=$(( ANCHOR + 999999 ))

# ---------- dependency check --------------------------------------------------

if ! command -v psql >/dev/null 2>&1; then
    log "ERROR: psql not found on PATH."
    exit 1
fi

# ---------- target detection --------------------------------------------------

if ! check_targets "${SCENARIO}"; then
    exit 0
fi

# ---------- dry run -----------------------------------------------------------

if [[ "${DRY_RUN}" -eq 1 ]]; then
    info "=== DRY RUN — no connections will be made ==="
    info "  Scenario       : ${SCENARIO}"
    info "  Region label   : ${REGION_LABEL}"
    info "  Row count      : ${ROW_COUNT}"
    info "  Iterations     : ${ITERATIONS} (+ ${WARMUP} warm-up) per window"
    info "  Windows        : 1k [${LO_1K}–${HI_1K}]  100k [${LO_100K}–${HI_100K}]  1M [${LO_1M}–${HI_1M}]"
    info "  Active targets : ${ACTIVE_TARGETS}"
    info "=== END DRY RUN ==="
    exit 0
fi

# ---------- setup -------------------------------------------------------------

mkdir -p "${OUT_DIR}"
TIMESTAMP="$(date +%Y%m%dT%H%M%S)"
OUT_JSON="${OUT_DIR}/compare_speed_range_scan_${REGION_LABEL}_${TIMESTAMP}.json"

log "=== range_scan benchmark (region=${REGION_LABEL}, rows=${ROW_COUNT}, iters=${ITERATIONS}) ==="

# ---------- teardown trap -----------------------------------------------------

NEON_PREPARED=0
SUPABASE_PREPARED=0
BASIN_PREPARED=0

cleanup() {
    local _ec=$?
    log "teardown..."
    if [[ "${NEON_PREPARED}" -eq 1 ]];     then drop_schema "${NEON_DATABASE_URL}"     "bench_speed_range_scan"; fi
    if [[ "${SUPABASE_PREPARED}" -eq 1 ]]; then drop_schema "${SUPABASE_DATABASE_URL}" "bench_speed_range_scan"; fi
    if [[ "${BASIN_PREPARED}" -eq 1 ]];    then drop_schema "${BASIN_DATABASE_URL}"    "bench_speed_range_scan"; fi
    log "teardown complete."
    exit "${_ec}"
}
trap cleanup EXIT

# ---------- per-target benchmark function ------------------------------------

# benchmark_target LABEL URL PREPARED_FLAG_VAR
# Sets: tgt_1k_p50/p95/p99  tgt_100k_p50/p95/p99  tgt_1m_p50/p95/p99
benchmark_target() {
    local _label="$1"
    local _url="$2"
    local _flag_var="$3"

    log ""
    log "--- ${_label} ---"

    log "  creating schema..."
    if ! create_schema "${_url}" "${SCHEMA_SQL}"; then
        log "  ERROR: schema creation failed for ${_label}"
        for _w in 1k 100k 1m; do
            for _p in p50 p95 p99; do
                eval "tgt_${_w}_${_p}=null"
            done
        done
        return
    fi
    printf -v "${_flag_var}" '%s' '1'

    log "  loading ${ROW_COUNT} rows (batch=${BATCH_SIZE})..."
    local _tmpfile
    _tmpfile="$(mktemp /tmp/basin_speed_range_insert_XXXXXX.sql)"
    trap "rm -f ${_tmpfile}" RETURN

    {
        printf 'BEGIN;\n'
        local _i=0
        while [[ "${_i}" -lt "${ROW_COUNT}" ]]; do
            local _end=$(( _i + BATCH_SIZE ))
            [[ "${_end}" -gt "${ROW_COUNT}" ]] && _end="${ROW_COUNT}"
            printf 'INSERT INTO bench_speed_range_scan.events (id, project_id, action, created_at, payload) VALUES\n'
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

    # ---- time each window size ----
    for _window_size in 1k 100k 1m; do
        local _lo _hi _label_w
        case "${_window_size}" in
            1k)   _lo="${LO_1K}";   _hi="${HI_1K}";   _label_w="1k" ;;
            100k) _lo="${LO_100K}"; _hi="${HI_100K}"; _label_w="100k" ;;
            1m)   _lo="${LO_1M}";   _hi="${HI_1M}";   _label_w="1M" ;;
        esac

        local _sql="SELECT COUNT(*) AS cnt FROM bench_speed_range_scan.events WHERE id BETWEEN ${_lo} AND ${_hi}"
        log "  timing ${ITERATIONS} range scans (window=${_label_w}, + ${WARMUP} warm-up)..."
        time_query "${_url}" "${_sql}" "${ITERATIONS}" "${WARMUP}"

        local _p50 _p95 _p99
        if [[ "${TIME_QUERY_OK}" -eq 0 ]]; then
            _p50="null"; _p95="null"; _p99="null"
        else
            _p50="$(p50_of "${TIME_QUERY_SAMPLES}")"
            _p95="$(p95_of "${TIME_QUERY_SAMPLES}")"
            _p99="$(p99_of "${TIME_QUERY_SAMPLES}")"
        fi

        eval "tgt_${_window_size}_p50=${_p50}"
        eval "tgt_${_window_size}_p95=${_p95}"
        eval "tgt_${_window_size}_p99=${_p99}"
        log "  ${_label} window=${_window_size}: p50=${_p50}ms p95=${_p95}ms p99=${_p99}ms"
    done
}

# ---------- run all targets ---------------------------------------------------

n_1k_p50="null"; n_1k_p95="null"; n_1k_p99="null"
n_100k_p50="null"; n_100k_p95="null"; n_100k_p99="null"
n_1m_p50="null"; n_1m_p95="null"; n_1m_p99="null"

s_1k_p50="null"; s_1k_p95="null"; s_1k_p99="null"
s_100k_p50="null"; s_100k_p95="null"; s_100k_p99="null"
s_1m_p50="null"; s_1m_p95="null"; s_1m_p99="null"

b_1k_p50="null"; b_1k_p95="null"; b_1k_p99="null"
b_100k_p50="null"; b_100k_p95="null"; b_100k_p99="null"
b_1m_p50="null"; b_1m_p95="null"; b_1m_p99="null"

if [[ "${HAS_NEON}" -eq 1 ]]; then
    benchmark_target "Neon"     "${NEON_DATABASE_URL}"     "NEON_PREPARED"
    n_1k_p50="${tgt_1k_p50}"; n_1k_p95="${tgt_1k_p95}"; n_1k_p99="${tgt_1k_p99}"
    n_100k_p50="${tgt_100k_p50}"; n_100k_p95="${tgt_100k_p95}"; n_100k_p99="${tgt_100k_p99}"
    n_1m_p50="${tgt_1m_p50}"; n_1m_p95="${tgt_1m_p95}"; n_1m_p99="${tgt_1m_p99}"
fi

if [[ "${HAS_SUPABASE}" -eq 1 ]]; then
    benchmark_target "Supabase" "${SUPABASE_DATABASE_URL}" "SUPABASE_PREPARED"
    s_1k_p50="${tgt_1k_p50}"; s_1k_p95="${tgt_1k_p95}"; s_1k_p99="${tgt_1k_p99}"
    s_100k_p50="${tgt_100k_p50}"; s_100k_p95="${tgt_100k_p95}"; s_100k_p99="${tgt_100k_p99}"
    s_1m_p50="${tgt_1m_p50}"; s_1m_p95="${tgt_1m_p95}"; s_1m_p99="${tgt_1m_p99}"
fi

if [[ "${HAS_BASIN}" -eq 1 ]]; then
    benchmark_target "Basin"    "${BASIN_DATABASE_URL}"    "BASIN_PREPARED"
    b_1k_p50="${tgt_1k_p50}"; b_1k_p95="${tgt_1k_p95}"; b_1k_p99="${tgt_1k_p99}"
    b_100k_p50="${tgt_100k_p50}"; b_100k_p95="${tgt_100k_p95}"; b_100k_p99="${tgt_100k_p99}"
    b_1m_p50="${tgt_1m_p50}"; b_1m_p95="${tgt_1m_p95}"; b_1m_p99="${tgt_1m_p99}"
fi

# ---------- stdout summary table ---------------------------------------------

log ""
log "=== Summary: range_scan (${REGION_LABEL}) ==="
printf '\n%-40s %15s %15s %15s\n' "Metric" "Neon" "Supabase" "Basin"
printf '%-40s %15s %15s %15s\n' "$(printf '%0.s-' {1..40})" "$(printf '%0.s-' {1..15})" "$(printf '%0.s-' {1..15})" "$(printf '%0.s-' {1..15})"
printf '%-40s %15s %15s %15s\n' "1k-row window p50 (ms)" "${n_1k_p50}" "${s_1k_p50}" "${b_1k_p50}"
printf '%-40s %15s %15s %15s\n' "1k-row window p99 (ms)" "${n_1k_p99}" "${s_1k_p99}" "${b_1k_p99}"
printf '%-40s %15s %15s %15s\n' "100k-row window p50 (ms)" "${n_100k_p50}" "${s_100k_p50}" "${b_100k_p50}"
printf '%-40s %15s %15s %15s\n' "100k-row window p99 (ms)" "${n_100k_p99}" "${s_100k_p99}" "${b_100k_p99}"
printf '%-40s %15s %15s %15s\n' "1M-row window p50 (ms)" "${n_1m_p50}" "${s_1m_p50}" "${b_1m_p50}"
printf '%-40s %15s %15s %15s\n' "1M-row window p99 (ms)" "${n_1m_p99}" "${s_1m_p99}" "${b_1m_p99}"
printf '\n'

# ---------- JSON output -------------------------------------------------------

UNIX_NOW="$(date +%s)"

cat > "${OUT_JSON}" <<JSONEOF
{
  "kind": "compare",
  "id": "speed_range_scan_${REGION_LABEL}",
  "name": "Range-scan latency (${REGION_LABEL}, ${ROW_COUNT} rows)",
  "claim": "SELECT COUNT(*) for 1k / 100k / 1M row windows against a ${ROW_COUNT}-row unindexed table. Tests storage-engine scan throughput at three granularities. Warm-up iterations (${WARMUP}) discarded; p50/p95/p99 computed via linear interpolation.",
  "available": true,
  "region": $(jq_str "${REGION_LABEL}"),
  "row_count": ${ROW_COUNT},
  "iterations": ${ITERATIONS},
  "warmup": ${WARMUP},
  "generated_at": "@${UNIX_NOW}",
  "metrics": [
    {
      "label": "1k-row window p50 (ms)",
      "neon":     $(jq_num "${n_1k_p50}"),
      "supabase": $(jq_num "${s_1k_p50}"),
      "basin":    $(jq_num "${b_1k_p50}"),
      "unit": "ms",
      "better": $(jq_str "$(winner_of_3 "${n_1k_p50}" "${s_1k_p50}" "${b_1k_p50}" "neon" "supabase" "basin")"),
      "ratio_text": null,
      "note": "COUNT(*) WHERE id BETWEEN ${LO_1K} AND ${HI_1K}"
    },
    {
      "label": "1k-row window p95 (ms)",
      "neon":     $(jq_num "${n_1k_p95}"),
      "supabase": $(jq_num "${s_1k_p95}"),
      "basin":    $(jq_num "${b_1k_p95}"),
      "unit": "ms",
      "better": $(jq_str "$(winner_of_3 "${n_1k_p95}" "${s_1k_p95}" "${b_1k_p95}" "neon" "supabase" "basin")"),
      "ratio_text": null,
      "note": null
    },
    {
      "label": "1k-row window p99 (ms)",
      "neon":     $(jq_num "${n_1k_p99}"),
      "supabase": $(jq_num "${s_1k_p99}"),
      "basin":    $(jq_num "${b_1k_p99}"),
      "unit": "ms",
      "better": $(jq_str "$(winner_of_3 "${n_1k_p99}" "${s_1k_p99}" "${b_1k_p99}" "neon" "supabase" "basin")"),
      "ratio_text": null,
      "note": null
    },
    {
      "label": "100k-row window p50 (ms)",
      "neon":     $(jq_num "${n_100k_p50}"),
      "supabase": $(jq_num "${s_100k_p50}"),
      "basin":    $(jq_num "${b_100k_p50}"),
      "unit": "ms",
      "better": $(jq_str "$(winner_of_3 "${n_100k_p50}" "${s_100k_p50}" "${b_100k_p50}" "neon" "supabase" "basin")"),
      "ratio_text": null,
      "note": "COUNT(*) WHERE id BETWEEN ${LO_100K} AND ${HI_100K}"
    },
    {
      "label": "100k-row window p95 (ms)",
      "neon":     $(jq_num "${n_100k_p95}"),
      "supabase": $(jq_num "${s_100k_p95}"),
      "basin":    $(jq_num "${b_100k_p95}"),
      "unit": "ms",
      "better": $(jq_str "$(winner_of_3 "${n_100k_p95}" "${s_100k_p95}" "${b_100k_p95}" "neon" "supabase" "basin")"),
      "ratio_text": null,
      "note": null
    },
    {
      "label": "100k-row window p99 (ms)",
      "neon":     $(jq_num "${n_100k_p99}"),
      "supabase": $(jq_num "${s_100k_p99}"),
      "basin":    $(jq_num "${b_100k_p99}"),
      "unit": "ms",
      "better": $(jq_str "$(winner_of_3 "${n_100k_p99}" "${s_100k_p99}" "${b_100k_p99}" "neon" "supabase" "basin")"),
      "ratio_text": null,
      "note": null
    },
    {
      "label": "1M-row window p50 (ms)",
      "neon":     $(jq_num "${n_1m_p50}"),
      "supabase": $(jq_num "${s_1m_p50}"),
      "basin":    $(jq_num "${b_1m_p50}"),
      "unit": "ms",
      "better": $(jq_str "$(winner_of_3 "${n_1m_p50}" "${s_1m_p50}" "${b_1m_p50}" "neon" "supabase" "basin")"),
      "ratio_text": null,
      "note": "COUNT(*) WHERE id BETWEEN ${LO_1M} AND ${HI_1M}"
    },
    {
      "label": "1M-row window p95 (ms)",
      "neon":     $(jq_num "${n_1m_p95}"),
      "supabase": $(jq_num "${s_1m_p95}"),
      "basin":    $(jq_num "${b_1m_p95}"),
      "unit": "ms",
      "better": $(jq_str "$(winner_of_3 "${n_1m_p95}" "${s_1m_p95}" "${b_1m_p95}" "neon" "supabase" "basin")"),
      "ratio_text": null,
      "note": null
    },
    {
      "label": "1M-row window p99 (ms)",
      "neon":     $(jq_num "${n_1m_p99}"),
      "supabase": $(jq_num "${s_1m_p99}"),
      "basin":    $(jq_num "${b_1m_p99}"),
      "unit": "ms",
      "better": $(jq_str "$(winner_of_3 "${n_1m_p99}" "${s_1m_p99}" "${b_1m_p99}" "neon" "supabase" "basin")"),
      "ratio_text": null,
      "note": null
    }
  ],
  "note": "Window anchor at id=${ANCHOR} (N/4 of ${ROW_COUNT}-row table). COUNT(*) eliminates network-transfer asymmetry; bottleneck is scan throughput and predicate evaluation. Targets run sequentially."
}
JSONEOF

log ""
log "result JSON written: ${OUT_JSON}"

log "dropping schemas..."
if [[ "${NEON_PREPARED}" -eq 1 ]];     then drop_schema "${NEON_DATABASE_URL}"     "bench_speed_range_scan"; NEON_PREPARED=0; fi
if [[ "${SUPABASE_PREPARED}" -eq 1 ]]; then drop_schema "${SUPABASE_DATABASE_URL}" "bench_speed_range_scan"; SUPABASE_PREPARED=0; fi
if [[ "${BASIN_PREPARED}" -eq 1 ]];    then drop_schema "${BASIN_DATABASE_URL}"    "bench_speed_range_scan"; BASIN_PREPARED=0; fi
log "done."
