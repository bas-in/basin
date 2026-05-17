#!/usr/bin/env bash
# benchmark/speed/bulk_insert/run.sh
#
# Bulk-insert throughput benchmark: rows/sec for 10k / 100k / 1M batch sizes.
# Each batch is measured ITERATIONS times per target; warm-up iterations are
# discarded.  Metric: rows/second (higher is better).
#
# USAGE
#   ./benchmark/speed/bulk_insert/run.sh [--dry-run]
#
# DSN CONFIG (any combination; missing targets are skipped gracefully):
#   NEON_DATABASE_URL / SUPABASE_DATABASE_URL / BASIN_DATABASE_URL / REGION_LABEL
#
# OPTIONAL:
#   SPEED_ITERATIONS    timed repetitions per batch size per target (default: 5)
#   SPEED_WARMUP        warm-up iterations discarded from stats (default: 1)
#   SPEED_OUT_DIR       output directory (default: benchmark/speed/out/)

set -euo pipefail

SCENARIO="bulk_insert"
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
ITERATIONS="${SPEED_ITERATIONS:-5}"
WARMUP="${SPEED_WARMUP:-1}"
OUT_DIR="${SPEED_OUT_DIR:-${REPO_ROOT}/benchmark/speed/out}"
SCHEMA_SQL="${SCRIPT_DIR}/schema.sql"

# Batch sizes to measure.
BATCH_SIZES=(10000 100000 1000000)
# Internal INSERT chunk size (rows per VALUES list, avoids huge single SQL statements).
CHUNK_SIZE=1000

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
    info "  Batch sizes    : ${BATCH_SIZES[*]}"
    info "  Iterations     : ${ITERATIONS} (+ ${WARMUP} warm-up) per batch"
    info "  Active targets : ${ACTIVE_TARGETS}"
    info "=== END DRY RUN ==="
    exit 0
fi

# ---------- setup -------------------------------------------------------------

mkdir -p "${OUT_DIR}"
TIMESTAMP="$(date +%Y%m%dT%H%M%S)"
OUT_JSON="${OUT_DIR}/compare_speed_bulk_insert_${REGION_LABEL}_${TIMESTAMP}.json"

log "=== bulk_insert benchmark (region=${REGION_LABEL}, iters=${ITERATIONS}) ==="

# ---------- teardown trap -----------------------------------------------------

NEON_PREPARED=0
SUPABASE_PREPARED=0
BASIN_PREPARED=0

cleanup() {
    local _ec=$?
    log "teardown..."
    if [[ "${NEON_PREPARED}" -eq 1 ]];     then drop_schema "${NEON_DATABASE_URL}"     "bench_speed_bulk_insert"; fi
    if [[ "${SUPABASE_PREPARED}" -eq 1 ]]; then drop_schema "${SUPABASE_DATABASE_URL}" "bench_speed_bulk_insert"; fi
    if [[ "${BASIN_PREPARED}" -eq 1 ]];    then drop_schema "${BASIN_DATABASE_URL}"    "bench_speed_bulk_insert"; fi
    log "teardown complete."
    exit "${_ec}"
}
trap cleanup EXIT

# ---------- helpers -----------------------------------------------------------

# build_insert_sql TMPFILE BATCH_SIZE OFFSET
# Writes a BEGIN...COMMIT block for BATCH_SIZE rows starting at id=OFFSET.
build_insert_sql() {
    local _file="$1"
    local _batch="$2"
    local _offset="${3:-0}"
    {
        printf 'BEGIN;\n'
        local _i=0
        while [[ "${_i}" -lt "${_batch}" ]]; do
            local _end=$(( _i + CHUNK_SIZE ))
            [[ "${_end}" -gt "${_batch}" ]] && _end="${_batch}"
            printf 'INSERT INTO bench_speed_bulk_insert.events (id, project_id, action, created_at, payload) VALUES\n'
            local _first=1 _j="${_i}"
            while [[ "${_j}" -lt "${_end}" ]]; do
                [[ "${_first}" -eq 0 ]] && printf ',\n'
                printf "(%d, gen_random_uuid(), 'action_%d', now(), 'payload-%040d')" \
                    "$(( _offset + _j ))" "$(( _j % 10 ))" "$(( _offset + _j ))"
                _first=0
                (( _j++ )) || true
            done
            printf ';\n'
            (( _i += CHUNK_SIZE )) || true
        done
        printf 'COMMIT;\n'
    } > "${_file}"
}

# rows_per_sec BATCH_SIZE ELAPSED_MS
# Returns rows/sec as float.
rows_per_sec() {
    local _batch="$1"
    local _ms="$2"
    if [[ -z "${_ms}" || "${_ms}" == "null" || "${_ms}" -le 0 ]]; then
        echo "null"; return
    fi
    python3 -c "print(round(${_batch} / (${_ms} / 1000.0), 1))"
}

# ---------- per-target benchmark function ------------------------------------

# benchmark_target LABEL URL PREPARED_FLAG_VAR
# Sets: tgt_10k_rps / tgt_100k_rps / tgt_1m_rps
benchmark_target() {
    local _label="$1"
    local _url="$2"
    local _flag_var="$3"

    log ""
    log "--- ${_label} ---"

    log "  creating schema..."
    if ! create_schema "${_url}" "${SCHEMA_SQL}"; then
        log "  ERROR: schema creation failed for ${_label}"
        tgt_10k_rps="null"; tgt_100k_rps="null"; tgt_1m_rps="null"
        return
    fi
    printf -v "${_flag_var}" '%s' '1'

    for _batch in "${BATCH_SIZES[@]}"; do
        local _batch_label
        case "${_batch}" in
            10000)   _batch_label="10k"  ;;
            100000)  _batch_label="100k" ;;
            1000000) _batch_label="1M"   ;;
            *)       _batch_label="${_batch}" ;;
        esac

        log "  batch=${_batch_label}: ${ITERATIONS} trials (+ ${WARMUP} warm-up)..."

        # Pre-build the INSERT SQL file once (same for all trials).
        local _tmpfile
        _tmpfile="$(mktemp /tmp/basin_speed_bulk_XXXXXX.sql)"
        trap "rm -f ${_tmpfile}" RETURN
        build_insert_sql "${_tmpfile}" "${_batch}" 0

        local _samples=""
        local _total=$(( ITERATIONS + WARMUP ))
        local _k _ok=0

        for (( _k = 0; _k < _total; _k++ )); do
            # Truncate before each trial to start from empty table.
            psql "${_url}" --no-psqlrc -c \
                "TRUNCATE bench_speed_bulk_insert.events" >/dev/null 2>&1 || true

            local _t0 _t1 _elapsed
            _t0="$(epoch_ms)"
            if psql "${_url}" --no-psqlrc -f "${_tmpfile}" >/dev/null 2>&1; then
                _ok=1
            fi
            _t1="$(epoch_ms)"
            _elapsed=$(( _t1 - _t0 ))

            # Discard warm-up.
            if [[ "${_k}" -ge "${WARMUP}" ]]; then
                _samples="${_samples}${_elapsed}"$'\n'
            fi
        done

        local _p50_ms _rps
        if [[ "${_ok}" -eq 0 ]]; then
            _p50_ms="null"; _rps="null"
        else
            _p50_ms="$(p50_of "${_samples}")"
            _rps="$(rows_per_sec "${_batch}" "${_p50_ms}")"
        fi

        case "${_batch}" in
            10000)   tgt_10k_rps="${_rps}"  ;;
            100000)  tgt_100k_rps="${_rps}" ;;
            1000000) tgt_1m_rps="${_rps}"   ;;
        esac

        log "  ${_label} batch=${_batch_label}: p50_insert=${_p50_ms}ms  throughput=${_rps} rows/sec"
    done
}

# ---------- run all targets ---------------------------------------------------

n_10k_rps="null"; n_100k_rps="null"; n_1m_rps="null"
s_10k_rps="null"; s_100k_rps="null"; s_1m_rps="null"
b_10k_rps="null"; b_100k_rps="null"; b_1m_rps="null"

tgt_10k_rps="null"; tgt_100k_rps="null"; tgt_1m_rps="null"

if [[ "${HAS_NEON}" -eq 1 ]]; then
    benchmark_target "Neon"     "${NEON_DATABASE_URL}"     "NEON_PREPARED"
    n_10k_rps="${tgt_10k_rps}"; n_100k_rps="${tgt_100k_rps}"; n_1m_rps="${tgt_1m_rps}"
fi

if [[ "${HAS_SUPABASE}" -eq 1 ]]; then
    benchmark_target "Supabase" "${SUPABASE_DATABASE_URL}" "SUPABASE_PREPARED"
    s_10k_rps="${tgt_10k_rps}"; s_100k_rps="${tgt_100k_rps}"; s_1m_rps="${tgt_1m_rps}"
fi

if [[ "${HAS_BASIN}" -eq 1 ]]; then
    benchmark_target "Basin"    "${BASIN_DATABASE_URL}"    "BASIN_PREPARED"
    b_10k_rps="${tgt_10k_rps}"; b_100k_rps="${tgt_100k_rps}"; b_1m_rps="${tgt_1m_rps}"
fi

# ---------- stdout summary table ---------------------------------------------

log ""
log "=== Summary: bulk_insert (${REGION_LABEL}) ==="
printf '\n%-40s %15s %15s %15s\n' "Metric" "Neon" "Supabase" "Basin"
printf '%-40s %15s %15s %15s\n' "$(printf '%0.s-' {1..40})" "$(printf '%0.s-' {1..15})" "$(printf '%0.s-' {1..15})" "$(printf '%0.s-' {1..15})"
printf '%-40s %15s %15s %15s\n' "10k-row batch (rows/sec)" "${n_10k_rps}" "${s_10k_rps}" "${b_10k_rps}"
printf '%-40s %15s %15s %15s\n' "100k-row batch (rows/sec)" "${n_100k_rps}" "${s_100k_rps}" "${b_100k_rps}"
printf '%-40s %15s %15s %15s\n' "1M-row batch (rows/sec)" "${n_1m_rps}" "${s_1m_rps}" "${b_1m_rps}"
printf '\n'

# ---------- JSON output (higher rows/sec = better, so invert winner logic) ----
#
# winner_of_3 uses lower-is-better; for throughput we invert: pick the MAX.

winner_throughput() {
    local a="$1" b="$2" c="$3"
    local la="$4" lb="$5" lc="$6"
    if [[ "${a}" == "null" || "${b}" == "null" || "${c}" == "null" ]]; then
        echo "null"; return
    fi
    python3 - "${a}" "${b}" "${c}" "${la}" "${lb}" "${lc}" <<'PY'
import sys
a, b, c = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3])
la, lb, lc = sys.argv[4], sys.argv[5], sys.argv[6]
mx = max(a, b, c)
print(la if mx == a else (lb if mx == b else lc))
PY
}

WINNER_10K="$(winner_throughput  "${n_10k_rps}"  "${s_10k_rps}"  "${b_10k_rps}"  "neon" "supabase" "basin")"
WINNER_100K="$(winner_throughput "${n_100k_rps}" "${s_100k_rps}" "${b_100k_rps}" "neon" "supabase" "basin")"
WINNER_1M="$(winner_throughput   "${n_1m_rps}"   "${s_1m_rps}"   "${b_1m_rps}"   "neon" "supabase" "basin")"

UNIX_NOW="$(date +%s)"

cat > "${OUT_JSON}" <<JSONEOF
{
  "kind": "compare",
  "id": "speed_bulk_insert_${REGION_LABEL}",
  "name": "Bulk-insert throughput (${REGION_LABEL})",
  "claim": "rows/second for 10k / 100k / 1M row INSERT batches. Each batch is a single BEGIN...COMMIT block; table is TRUNCATED between trials. Warm-up trials (${WARMUP}) discarded; metric is rows/sec at p50 wall-clock latency.",
  "available": true,
  "region": $(jq_str "${REGION_LABEL}"),
  "row_count": 1000000,
  "iterations": ${ITERATIONS},
  "warmup": ${WARMUP},
  "generated_at": "@${UNIX_NOW}",
  "metrics": [
    {
      "label": "10k-row batch (rows/sec)",
      "neon":     $(jq_num "${n_10k_rps}"),
      "supabase": $(jq_num "${s_10k_rps}"),
      "basin":    $(jq_num "${b_10k_rps}"),
      "unit": "rows/sec",
      "better": $(jq_str "${WINNER_10K}"),
      "ratio_text": null,
      "note": "Higher is better. Table truncated between trials."
    },
    {
      "label": "100k-row batch (rows/sec)",
      "neon":     $(jq_num "${n_100k_rps}"),
      "supabase": $(jq_num "${s_100k_rps}"),
      "basin":    $(jq_num "${b_100k_rps}"),
      "unit": "rows/sec",
      "better": $(jq_str "${WINNER_100K}"),
      "ratio_text": null,
      "note": "Higher is better."
    },
    {
      "label": "1M-row batch (rows/sec)",
      "neon":     $(jq_num "${n_1m_rps}"),
      "supabase": $(jq_num "${s_1m_rps}"),
      "basin":    $(jq_num "${b_1m_rps}"),
      "unit": "rows/sec",
      "better": $(jq_str "${WINNER_1M}"),
      "ratio_text": null,
      "note": "Higher is better. Large single-transaction insert."
    }
  ],
  "note": "INSERT batches use multi-row VALUES lists in CHUNK_SIZE=${CHUNK_SIZE}-row chunks inside a single transaction. Metric: batch_size / (p50_wall_clock_ms / 1000). Targets run sequentially to avoid contention."
}
JSONEOF

log ""
log "result JSON written: ${OUT_JSON}"

log "dropping schemas..."
if [[ "${NEON_PREPARED}" -eq 1 ]];     then drop_schema "${NEON_DATABASE_URL}"     "bench_speed_bulk_insert"; NEON_PREPARED=0; fi
if [[ "${SUPABASE_PREPARED}" -eq 1 ]]; then drop_schema "${SUPABASE_DATABASE_URL}" "bench_speed_bulk_insert"; SUPABASE_PREPARED=0; fi
if [[ "${BASIN_PREPARED}" -eq 1 ]];    then drop_schema "${BASIN_DATABASE_URL}"    "bench_speed_bulk_insert"; BASIN_PREPARED=0; fi
log "done."
