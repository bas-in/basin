#!/usr/bin/env bash
# benchmark/speed/aggregate/run.sh
#
# Aggregate latency benchmark: p50/p95/p99 for four query shapes against
# a ~10M-row table:
#   A1 — full-table COUNT(*)
#   A2 — full-table SUM + AVG
#   A3 — GROUP BY id/1000 with COUNT(*) + SUM
#   A4 — windowed COUNT over ~10% of the table
#
# USAGE
#   ./benchmark/speed/aggregate/run.sh [--dry-run]
#
# DSN CONFIG (any combination; missing targets are skipped gracefully):
#   NEON_DATABASE_URL / SUPABASE_DATABASE_URL / BASIN_DATABASE_URL / REGION_LABEL
#
# OPTIONAL:
#   SPEED_ITERATIONS    timed iterations per query per target (default: 20)
#   SPEED_WARMUP        warm-up iterations discarded from stats (default: 3)
#   SPEED_ROW_COUNT     rows to preload (default: 10000000)
#   SPEED_OUT_DIR       output directory (default: benchmark/speed/out/)

set -euo pipefail

SCENARIO="aggregate"
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
ITERATIONS="${SPEED_ITERATIONS:-20}"
WARMUP="${SPEED_WARMUP:-3}"
ROW_COUNT="${SPEED_ROW_COUNT:-10000000}"
OUT_DIR="${SPEED_OUT_DIR:-${REPO_ROOT}/benchmark/speed/out}"
SCHEMA_SQL="${SCRIPT_DIR}/schema.sql"
BATCH_SIZE=1000

# Windowed aggregate covers ~10% of the table.
LO_WINDOW=$(( ROW_COUNT / 4 ))
HI_WINDOW=$(( LO_WINDOW + ROW_COUNT / 10 ))

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
    info "  Iterations     : ${ITERATIONS} (+ ${WARMUP} warm-up) per query"
    info "  Window         : [${LO_WINDOW}, ${HI_WINDOW}] (~10% of table)"
    info "  Active targets : ${ACTIVE_TARGETS}"
    info "=== END DRY RUN ==="
    exit 0
fi

# ---------- setup -------------------------------------------------------------

mkdir -p "${OUT_DIR}"
TIMESTAMP="$(date +%Y%m%dT%H%M%S)"
OUT_JSON="${OUT_DIR}/compare_speed_aggregate_${REGION_LABEL}_${TIMESTAMP}.json"

log "=== aggregate benchmark (region=${REGION_LABEL}, rows=${ROW_COUNT}, iters=${ITERATIONS}) ==="

# ---------- teardown trap -----------------------------------------------------

NEON_PREPARED=0
SUPABASE_PREPARED=0
BASIN_PREPARED=0

cleanup() {
    local _ec=$?
    log "teardown..."
    if [[ "${NEON_PREPARED}" -eq 1 ]];     then drop_schema "${NEON_DATABASE_URL}"     "bench_speed_aggregate"; fi
    if [[ "${SUPABASE_PREPARED}" -eq 1 ]]; then drop_schema "${SUPABASE_DATABASE_URL}" "bench_speed_aggregate"; fi
    if [[ "${BASIN_PREPARED}" -eq 1 ]];    then drop_schema "${BASIN_DATABASE_URL}"    "bench_speed_aggregate"; fi
    log "teardown complete."
    exit "${_ec}"
}
trap cleanup EXIT

# ---------- per-target benchmark function ------------------------------------

# benchmark_target LABEL URL PREPARED_FLAG_VAR
# Sets: tgt_a1_p50/p95/p99  tgt_a2_p50/p95/p99  tgt_a3_p50/p95/p99  tgt_a4_p50/p95/p99
benchmark_target() {
    local _label="$1"
    local _url="$2"
    local _flag_var="$3"

    log ""
    log "--- ${_label} ---"

    log "  creating schema..."
    if ! create_schema "${_url}" "${SCHEMA_SQL}"; then
        log "  ERROR: schema creation failed for ${_label}"
        for _q in a1 a2 a3 a4; do
            for _p in p50 p95 p99; do eval "tgt_${_q}_${_p}=null"; done
        done
        return
    fi
    printf -v "${_flag_var}" '%s' '1'

    log "  loading ${ROW_COUNT} rows..."
    local _tmpfile
    _tmpfile="$(mktemp /tmp/basin_speed_agg_insert_XXXXXX.sql)"
    trap "rm -f ${_tmpfile}" RETURN

    {
        printf 'BEGIN;\n'
        local _i=0
        while [[ "${_i}" -lt "${ROW_COUNT}" ]]; do
            local _end=$(( _i + BATCH_SIZE ))
            [[ "${_end}" -gt "${ROW_COUNT}" ]] && _end="${ROW_COUNT}"
            printf 'INSERT INTO bench_speed_aggregate.events (id, project_id, action, created_at, payload) VALUES\n'
            local _first=1 _j="${_i}"
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

    # ---- time each query ----
    local _sql_a1="SELECT COUNT(*) AS total_rows FROM bench_speed_aggregate.events"
    local _sql_a2="SELECT SUM(id) AS id_sum, AVG(id) AS id_avg FROM bench_speed_aggregate.events"
    local _sql_a3="SELECT id / 1000 AS bucket, COUNT(*) AS event_count, SUM(id) AS id_sum FROM bench_speed_aggregate.events GROUP BY bucket ORDER BY bucket"
    local _sql_a4="SELECT COUNT(*) AS windowed_count FROM bench_speed_aggregate.events WHERE id BETWEEN ${LO_WINDOW} AND ${HI_WINDOW}"

    for _qkey in a1 a2 a3 a4; do
        local _sql _desc
        case "${_qkey}" in
            a1) _sql="${_sql_a1}"; _desc="full COUNT(*)" ;;
            a2) _sql="${_sql_a2}"; _desc="full SUM+AVG" ;;
            a3) _sql="${_sql_a3}"; _desc="GROUP BY bucket" ;;
            a4) _sql="${_sql_a4}"; _desc="windowed COUNT" ;;
        esac

        log "  timing ${ITERATIONS} iterations of ${_desc} (+ ${WARMUP} warm-up)..."
        time_query "${_url}" "${_sql}" "${ITERATIONS}" "${WARMUP}"

        local _p50 _p95 _p99
        if [[ "${TIME_QUERY_OK}" -eq 0 ]]; then
            _p50="null"; _p95="null"; _p99="null"
        else
            _p50="$(p50_of "${TIME_QUERY_SAMPLES}")"
            _p95="$(p95_of "${TIME_QUERY_SAMPLES}")"
            _p99="$(p99_of "${TIME_QUERY_SAMPLES}")"
        fi

        eval "tgt_${_qkey}_p50=${_p50}"
        eval "tgt_${_qkey}_p95=${_p95}"
        eval "tgt_${_qkey}_p99=${_p99}"
        log "  ${_label} ${_desc}: p50=${_p50}ms p95=${_p95}ms p99=${_p99}ms"
    done
}

# ---------- run all targets ---------------------------------------------------

for _pfx in n s b; do
    for _q in a1 a2 a3 a4; do
        for _pct in p50 p95 p99; do
            eval "${_pfx}_${_q}_${_pct}=null"
        done
    done
done

if [[ "${HAS_NEON}" -eq 1 ]]; then
    benchmark_target "Neon"     "${NEON_DATABASE_URL}"     "NEON_PREPARED"
    for _q in a1 a2 a3 a4; do
        for _pct in p50 p95 p99; do eval "n_${_q}_${_pct}=\${tgt_${_q}_${_pct}}"; done
    done
fi

if [[ "${HAS_SUPABASE}" -eq 1 ]]; then
    benchmark_target "Supabase" "${SUPABASE_DATABASE_URL}" "SUPABASE_PREPARED"
    for _q in a1 a2 a3 a4; do
        for _pct in p50 p95 p99; do eval "s_${_q}_${_pct}=\${tgt_${_q}_${_pct}}"; done
    done
fi

if [[ "${HAS_BASIN}" -eq 1 ]]; then
    benchmark_target "Basin"    "${BASIN_DATABASE_URL}"    "BASIN_PREPARED"
    for _q in a1 a2 a3 a4; do
        for _pct in p50 p95 p99; do eval "b_${_q}_${_pct}=\${tgt_${_q}_${_pct}}"; done
    done
fi

# ---------- stdout summary table ---------------------------------------------

log ""
log "=== Summary: aggregate (${REGION_LABEL}) ==="
printf '\n%-44s %12s %12s %12s\n' "Metric" "Neon" "Supabase" "Basin"
printf '%-44s %12s %12s %12s\n' "$(printf '%0.s-' {1..44})" "$(printf '%0.s-' {1..12})" "$(printf '%0.s-' {1..12})" "$(printf '%0.s-' {1..12})"
for _q in a1 a2 a3 a4; do
    case "${_q}" in
        a1) _ql="Full COUNT(*)" ;;
        a2) _ql="Full SUM+AVG" ;;
        a3) _ql="GROUP BY bucket" ;;
        a4) _ql="Windowed COUNT" ;;
    esac
    for _pct in p50 p95 p99; do
        eval "_nv=\${n_${_q}_${_pct}}"; eval "_sv=\${s_${_q}_${_pct}}"; eval "_bv=\${b_${_q}_${_pct}}"
        printf '%-44s %12s %12s %12s\n' "${_ql} ${_pct} (ms)" "${_nv}" "${_sv}" "${_bv}"
    done
done
printf '\n'

# ---------- JSON output -------------------------------------------------------

UNIX_NOW="$(date +%s)"

# Helper to emit one metric object.
metric_json() {
    local _lbl="$1" _nv="$2" _sv="$3" _bv="$4" _note="${5:-null}"
    local _w
    _w="$(winner_of_3 "${_nv}" "${_sv}" "${_bv}" "neon" "supabase" "basin")"
    cat <<EOF
    {
      "label": ${_lbl},
      "neon":     $(jq_num "${_nv}"),
      "supabase": $(jq_num "${_sv}"),
      "basin":    $(jq_num "${_bv}"),
      "unit": "ms",
      "better": $(jq_str "${_w}"),
      "ratio_text": null,
      "note": $(jq_str "${_note}")
    }
EOF
}

{
cat <<JSONEOF
{
  "kind": "compare",
  "id": "speed_aggregate_${REGION_LABEL}",
  "name": "Aggregate latency (${REGION_LABEL}, ${ROW_COUNT} rows)",
  "claim": "COUNT / SUM / AVG / GROUP BY over a ${ROW_COUNT}-row table. Tests full-table scan + aggregation pipeline and predicate-pushdown efficiency. Warm-up iterations (${WARMUP}) discarded; p50/p95/p99 via linear interpolation.",
  "available": true,
  "region": $(jq_str "${REGION_LABEL}"),
  "row_count": ${ROW_COUNT},
  "iterations": ${ITERATIONS},
  "warmup": ${WARMUP},
  "generated_at": "@${UNIX_NOW}",
  "metrics": [
JSONEOF

metric_json '"Full COUNT(*) p50 (ms)"' "${n_a1_p50}" "${s_a1_p50}" "${b_a1_p50}" "SELECT COUNT(*) — full table scan."
printf ',\n'
metric_json '"Full COUNT(*) p95 (ms)"' "${n_a1_p95}" "${s_a1_p95}" "${b_a1_p95}" ""
printf ',\n'
metric_json '"Full COUNT(*) p99 (ms)"' "${n_a1_p99}" "${s_a1_p99}" "${b_a1_p99}" ""
printf ',\n'
metric_json '"Full SUM+AVG p50 (ms)"' "${n_a2_p50}" "${s_a2_p50}" "${b_a2_p50}" "SELECT SUM(id), AVG(id) — full table aggregation."
printf ',\n'
metric_json '"Full SUM+AVG p95 (ms)"' "${n_a2_p95}" "${s_a2_p95}" "${b_a2_p95}" ""
printf ',\n'
metric_json '"Full SUM+AVG p99 (ms)"' "${n_a2_p99}" "${s_a2_p99}" "${b_a2_p99}" ""
printf ',\n'
metric_json '"GROUP BY bucket p50 (ms)"' "${n_a3_p50}" "${s_a3_p50}" "${b_a3_p50}" "SELECT id/1000 AS bucket, COUNT(*), SUM(id) GROUP BY bucket — ~$(( ROW_COUNT / 1000 )) groups."
printf ',\n'
metric_json '"GROUP BY bucket p95 (ms)"' "${n_a3_p95}" "${s_a3_p95}" "${b_a3_p95}" ""
printf ',\n'
metric_json '"GROUP BY bucket p99 (ms)"' "${n_a3_p99}" "${s_a3_p99}" "${b_a3_p99}" ""
printf ',\n'
metric_json '"Windowed COUNT p50 (ms)"' "${n_a4_p50}" "${s_a4_p50}" "${b_a4_p50}" "COUNT(*) WHERE id BETWEEN ${LO_WINDOW} AND ${HI_WINDOW} (~10% of table)."
printf ',\n'
metric_json '"Windowed COUNT p95 (ms)"' "${n_a4_p95}" "${s_a4_p95}" "${b_a4_p95}" ""
printf ',\n'
metric_json '"Windowed COUNT p99 (ms)"' "${n_a4_p99}" "${s_a4_p99}" "${b_a4_p99}" ""

cat <<JSONEOF

  ],
  "note": "Full-table aggregates test the entire scan + aggregation pipeline. Windowed COUNT reveals predicate-pushdown interaction with column-level aggregation. Targets run sequentially."
}
JSONEOF
} > "${OUT_JSON}"

log ""
log "result JSON written: ${OUT_JSON}"

log "dropping schemas..."
if [[ "${NEON_PREPARED}" -eq 1 ]];     then drop_schema "${NEON_DATABASE_URL}"     "bench_speed_aggregate"; NEON_PREPARED=0; fi
if [[ "${SUPABASE_PREPARED}" -eq 1 ]]; then drop_schema "${SUPABASE_DATABASE_URL}" "bench_speed_aggregate"; SUPABASE_PREPARED=0; fi
if [[ "${BASIN_PREPARED}" -eq 1 ]];    then drop_schema "${BASIN_DATABASE_URL}"    "bench_speed_aggregate"; BASIN_PREPARED=0; fi
log "done."
