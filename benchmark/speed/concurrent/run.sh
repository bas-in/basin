#!/usr/bin/env bash
# benchmark/speed/concurrent/run.sh
#
# Concurrent mixed-workload benchmark: N parallel workers against a single target.
# Worker counts: 1 / 8 / 32 / 64.
# Workload mix: 70% point reads / 20% range scans / 10% inserts.
# Reports: total throughput (ops/sec) and p50/p95/p99 latency per N value.
#
# Each target is benchmarked one at a time (sequential targets, parallel workers
# within each target run) to avoid cross-target interference on shared network.
#
# IMPLEMENTATION NOTE ON TIMING:
#   Each worker process is a sub-shell that writes per-iteration timings to a
#   temp file.  The parent collects all timing files and computes percentiles
#   across all workers for the given N.  This gives honest per-operation latency
#   distribution, not an average over the worker fan-out.
#
# USAGE
#   ./benchmark/speed/concurrent/run.sh [--dry-run]
#
# DSN CONFIG (any combination; missing targets are skipped gracefully):
#   NEON_DATABASE_URL / SUPABASE_DATABASE_URL / BASIN_DATABASE_URL / REGION_LABEL
#
# OPTIONAL:
#   SPEED_OPS_PER_WORKER  operations per worker (default: 100)
#   SPEED_WARMUP_OPS      warm-up ops per worker discarded from stats (default: 10)
#   SPEED_ROW_COUNT       rows to preload (default: 1000000)
#   SPEED_OUT_DIR         output directory (default: benchmark/speed/out/)
#   SPEED_CONCURRENCY     space-separated worker counts (default: "1 8 32 64")

set -euo pipefail

SCENARIO="concurrent"
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
OPS_PER_WORKER="${SPEED_OPS_PER_WORKER:-100}"
WARMUP_OPS="${SPEED_WARMUP_OPS:-10}"
ROW_COUNT="${SPEED_ROW_COUNT:-1000000}"
OUT_DIR="${SPEED_OUT_DIR:-${REPO_ROOT}/benchmark/speed/out}"
SCHEMA_SQL="${SCRIPT_DIR}/schema.sql"
BATCH_SIZE=1000

# Worker-count levels to test.
IFS=' ' read -ra CONCURRENCY_LEVELS <<< "${SPEED_CONCURRENCY:-1 8 32 64}"

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
    info "  Scenario         : ${SCENARIO}"
    info "  Region label     : ${REGION_LABEL}"
    info "  Row count        : ${ROW_COUNT}"
    info "  Concurrency      : ${CONCURRENCY_LEVELS[*]}"
    info "  Ops per worker   : ${OPS_PER_WORKER} (+ ${WARMUP_OPS} warm-up)"
    info "  Workload mix     : 70% point-read / 20% range-scan / 10% insert"
    info "  Active targets   : ${ACTIVE_TARGETS}"
    info "=== END DRY RUN ==="
    exit 0
fi

# ---------- setup -------------------------------------------------------------

mkdir -p "${OUT_DIR}"
TIMESTAMP="$(date +%Y%m%dT%H%M%S)"
OUT_JSON="${OUT_DIR}/compare_speed_concurrent_${REGION_LABEL}_${TIMESTAMP}.json"

log "=== concurrent benchmark (region=${REGION_LABEL}, rows=${ROW_COUNT}) ==="

# ---------- teardown trap -----------------------------------------------------

NEON_PREPARED=0
SUPABASE_PREPARED=0
BASIN_PREPARED=0

cleanup() {
    local _ec=$?
    log "teardown..."
    if [[ "${NEON_PREPARED}" -eq 1 ]];     then drop_schema "${NEON_DATABASE_URL}"     "bench_speed_concurrent"; fi
    if [[ "${SUPABASE_PREPARED}" -eq 1 ]]; then drop_schema "${SUPABASE_DATABASE_URL}" "bench_speed_concurrent"; fi
    if [[ "${BASIN_PREPARED}" -eq 1 ]];    then drop_schema "${BASIN_DATABASE_URL}"    "bench_speed_concurrent"; fi
    # Clean up any leftover timing temp dirs.
    rm -rf /tmp/basin_speed_concurrent_* 2>/dev/null || true
    log "teardown complete."
    exit "${_ec}"
}
trap cleanup EXIT

# ---------- single-worker process (runs as a sub-shell) ----------------------
#
# worker_process URL WORKER_ID OPS WARMUP ROW_COUNT TIMING_FILE
# Executes OPS+WARMUP operations (mixed workload) against URL.
# Writes per-timed-operation latency (ms) to TIMING_FILE, one per line.
# Discards first WARMUP results.
# Called via `bash -c "source _lib.sh; worker_process ..."` in background.

# We write this as an inline function that gets sourced by sub-shells.
run_worker() {
    local _url="$1"
    local _wid="$2"
    local _ops="$3"
    local _warmup="$4"
    local _row_count="$5"
    local _timing_file="$6"

    local _total=$(( _ops + _warmup ))
    local _k

    for (( _k = 0; _k < _total; _k++ )); do
        # Deterministic query selection: hash (wid * 1000 + k) mod 100.
        local _bucket
        _bucket=$(( ( _wid * 1337 + _k * 31 ) % 100 ))

        local _sql
        local _target_id=$(( ( _wid * 997 + _k * 13 ) % _row_count ))
        local _range_lo=$(( _target_id ))
        local _range_hi=$(( _range_lo + 999 ))
        local _new_id=$(( _row_count + _wid * 100000 + _k ))

        if [[ "${_bucket}" -lt 70 ]]; then
            # 70% point read
            _sql="SELECT id, action, created_at FROM bench_speed_concurrent.events WHERE id = ${_target_id}"
        elif [[ "${_bucket}" -lt 90 ]]; then
            # 20% range scan
            _sql="SELECT COUNT(*) FROM bench_speed_concurrent.events WHERE id BETWEEN ${_range_lo} AND ${_range_hi}"
        else
            # 10% insert
            _sql="INSERT INTO bench_speed_concurrent.events (id, project_id, action, created_at, payload) VALUES (${_new_id}, gen_random_uuid(), 'concurrent_insert', now(), 'concurrent-payload-${_new_id}')"
        fi

        local _t0 _t1 _elapsed
        _t0="$(epoch_ms)"
        psql "${_url}" --no-psqlrc --tuples-only --no-align -c "${_sql}" >/dev/null 2>&1 || true
        _t1="$(epoch_ms)"
        _elapsed=$(( _t1 - _t0 ))

        # Only record post-warmup samples.
        if [[ "${_k}" -ge "${_warmup}" ]]; then
            printf '%s\n' "${_elapsed}" >> "${_timing_file}"
        fi
    done
}

# Export the function and lib helpers for sub-shells.
export -f run_worker epoch_ms p50_of p95_of p99_of percentile_of log info

# ---------- per-target, per-concurrency-level measurement --------------------

# run_concurrency_level URL N TIMING_DIR
# Spawns N workers in background, waits for all, collects timings.
# Sets: conc_ops_sec  conc_p50  conc_p95  conc_p99
run_concurrency_level() {
    local _url="$1"
    local _n="$2"
    local _dir="$3"

    local _wstart _wend _wall_ms
    local _pids=()
    local _files=()

    mkdir -p "${_dir}"

    log "    spawning ${_n} worker(s) (${OPS_PER_WORKER} ops + ${WARMUP_OPS} warmup each)..."
    _wstart="$(epoch_ms)"

    for (( _w = 0; _w < _n; _w++ )); do
        local _tf="${_dir}/w${_w}.txt"
        _files+=("${_tf}")
        # Each worker is a sub-shell sourcing _lib.sh for epoch_ms, then calling run_worker.
        (
            # shellcheck source=benchmark/speed/_lib.sh
            source "${SCRIPT_DIR}/../_lib.sh"
            run_worker "${_url}" "${_w}" "${OPS_PER_WORKER}" "${WARMUP_OPS}" "${ROW_COUNT}" "${_tf}"
        ) &
        _pids+=($!)
    done

    # Wait for all workers.
    for _pid in "${_pids[@]}"; do
        wait "${_pid}" 2>/dev/null || true
    done

    _wend="$(epoch_ms)"
    _wall_ms=$(( _wend - _wstart ))

    # Aggregate all timing files.
    local _all_samples=""
    local _total_ops=0
    for _tf in "${_files[@]}"; do
        if [[ -f "${_tf}" ]]; then
            local _lines
            _lines="$(wc -l < "${_tf}" | tr -d ' ')"
            _total_ops=$(( _total_ops + _lines ))
            _all_samples="${_all_samples}$(cat "${_tf}")"$'\n'
        fi
    done

    if [[ "${_total_ops}" -eq 0 ]]; then
        conc_ops_sec="null"; conc_p50="null"; conc_p95="null"; conc_p99="null"
        return
    fi

    # Throughput: total completed ops / wall-clock seconds.
    conc_ops_sec="$(python3 -c "print(round(${_total_ops} / (${_wall_ms} / 1000.0), 1))")"
    conc_p50="$(p50_of "${_all_samples}")"
    conc_p95="$(p95_of "${_all_samples}")"
    conc_p99="$(p99_of "${_all_samples}")"

    log "    N=${_n}: ops_sec=${conc_ops_sec} p50=${conc_p50}ms p95=${conc_p95}ms p99=${conc_p99}ms"
}

# ---------- per-target benchmark function ------------------------------------

# benchmark_target LABEL URL PREPARED_FLAG_VAR
# For each N in CONCURRENCY_LEVELS, sets:
#   tgt_n<N>_ops_sec  tgt_n<N>_p50  tgt_n<N>_p95  tgt_n<N>_p99
benchmark_target() {
    local _label="$1"
    local _url="$2"
    local _flag_var="$3"
    local _safe_label
    _safe_label="$(printf '%s' "${_label}" | tr '[:upper:]' '[:lower:]')"

    log ""
    log "--- ${_label} ---"

    log "  creating schema..."
    if ! create_schema "${_url}" "${SCHEMA_SQL}"; then
        log "  ERROR: schema creation failed for ${_label}"
        for _n in "${CONCURRENCY_LEVELS[@]}"; do
            eval "tgt_n${_n}_ops_sec=null tgt_n${_n}_p50=null tgt_n${_n}_p95=null tgt_n${_n}_p99=null"
        done
        return
    fi
    printf -v "${_flag_var}" '%s' '1'

    log "  loading ${ROW_COUNT} rows..."
    local _tmpfile
    _tmpfile="$(mktemp /tmp/basin_speed_conc_insert_XXXXXX.sql)"
    trap "rm -f ${_tmpfile}" RETURN

    {
        printf 'BEGIN;\n'
        local _i=0
        while [[ "${_i}" -lt "${ROW_COUNT}" ]]; do
            local _end=$(( _i + BATCH_SIZE ))
            [[ "${_end}" -gt "${ROW_COUNT}" ]] && _end="${ROW_COUNT}"
            printf 'INSERT INTO bench_speed_concurrent.events (id, project_id, action, created_at, payload) VALUES\n'
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

    for _n in "${CONCURRENCY_LEVELS[@]}"; do
        local _tdir
        _tdir="$(mktemp -d /tmp/basin_speed_concurrent_${_safe_label}_n${_n}_XXXXXX)"
        log "  concurrency N=${_n}..."
        run_concurrency_level "${_url}" "${_n}" "${_tdir}"
        eval "tgt_n${_n}_ops_sec=${conc_ops_sec}"
        eval "tgt_n${_n}_p50=${conc_p50}"
        eval "tgt_n${_n}_p95=${conc_p95}"
        eval "tgt_n${_n}_p99=${conc_p99}"
        rm -rf "${_tdir}" 2>/dev/null || true
    done
}

# ---------- run all targets ---------------------------------------------------

# Initialize result vars for all concurrency levels.
for _n in "${CONCURRENCY_LEVELS[@]}"; do
    for _pfx in n s b; do
        eval "${_pfx}_n${_n}_ops_sec=null ${_pfx}_n${_n}_p50=null ${_pfx}_n${_n}_p95=null ${_pfx}_n${_n}_p99=null"
    done
done

conc_ops_sec="null"; conc_p50="null"; conc_p95="null"; conc_p99="null"

if [[ "${HAS_NEON}" -eq 1 ]]; then
    benchmark_target "Neon"     "${NEON_DATABASE_URL}"     "NEON_PREPARED"
    for _n in "${CONCURRENCY_LEVELS[@]}"; do
        eval "n_n${_n}_ops_sec=\${tgt_n${_n}_ops_sec} n_n${_n}_p50=\${tgt_n${_n}_p50} n_n${_n}_p95=\${tgt_n${_n}_p95} n_n${_n}_p99=\${tgt_n${_n}_p99}"
    done
fi

if [[ "${HAS_SUPABASE}" -eq 1 ]]; then
    benchmark_target "Supabase" "${SUPABASE_DATABASE_URL}" "SUPABASE_PREPARED"
    for _n in "${CONCURRENCY_LEVELS[@]}"; do
        eval "s_n${_n}_ops_sec=\${tgt_n${_n}_ops_sec} s_n${_n}_p50=\${tgt_n${_n}_p50} s_n${_n}_p95=\${tgt_n${_n}_p95} s_n${_n}_p99=\${tgt_n${_n}_p99}"
    done
fi

if [[ "${HAS_BASIN}" -eq 1 ]]; then
    benchmark_target "Basin"    "${BASIN_DATABASE_URL}"    "BASIN_PREPARED"
    for _n in "${CONCURRENCY_LEVELS[@]}"; do
        eval "b_n${_n}_ops_sec=\${tgt_n${_n}_ops_sec} b_n${_n}_p50=\${tgt_n${_n}_p50} b_n${_n}_p95=\${tgt_n${_n}_p95} b_n${_n}_p99=\${tgt_n${_n}_p99}"
    done
fi

# ---------- stdout summary table ---------------------------------------------

log ""
log "=== Summary: concurrent (${REGION_LABEL}) ==="
printf '\n%-40s %15s %15s %15s\n' "Metric" "Neon" "Supabase" "Basin"
printf '%-40s %15s %15s %15s\n' "$(printf '%0.s-' {1..40})" "$(printf '%0.s-' {1..15})" "$(printf '%0.s-' {1..15})" "$(printf '%0.s-' {1..15})"
for _n in "${CONCURRENCY_LEVELS[@]}"; do
    eval "_no=\${n_n${_n}_ops_sec}; _so=\${s_n${_n}_ops_sec}; _bo=\${b_n${_n}_ops_sec}"
    eval "_np=\${n_n${_n}_p50};     _sp=\${s_n${_n}_p50};     _bp=\${b_n${_n}_p50}"
    eval "_np99=\${n_n${_n}_p99};   _sp99=\${s_n${_n}_p99};   _bp99=\${b_n${_n}_p99}"
    printf '%-40s %15s %15s %15s\n' "N=${_n} throughput (ops/sec)" "${_no}" "${_so}" "${_bo}"
    printf '%-40s %15s %15s %15s\n' "N=${_n} p50 latency (ms)"   "${_np}" "${_sp}" "${_bp}"
    printf '%-40s %15s %15s %15s\n' "N=${_n} p99 latency (ms)"   "${_np99}" "${_sp99}" "${_bp99}"
done
printf '\n'

# ---------- JSON output -------------------------------------------------------

# For throughput metrics, higher is better (use winner_throughput logic).
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

UNIX_NOW="$(date +%s)"

# Build metrics array dynamically.
{
cat <<JSONEOF
{
  "kind": "compare",
  "id": "speed_concurrent_${REGION_LABEL}",
  "name": "Concurrent mixed-workload (${REGION_LABEL}, ${ROW_COUNT} rows)",
  "claim": "N = ${CONCURRENCY_LEVELS[*]} parallel workers, 70% point reads / 20% range scans / 10% inserts against a ${ROW_COUNT}-row table. Reports ops/sec throughput and p50/p95/p99 per-operation latency at each N. Each target is measured independently.",
  "available": true,
  "region": $(jq_str "${REGION_LABEL}"),
  "row_count": ${ROW_COUNT},
  "iterations": ${OPS_PER_WORKER},
  "warmup": ${WARMUP_OPS},
  "generated_at": "@${UNIX_NOW}",
  "metrics": [
JSONEOF

_first_metric=1
for _n in "${CONCURRENCY_LEVELS[@]}"; do
    eval "_no=\${n_n${_n}_ops_sec}; _so=\${s_n${_n}_ops_sec}; _bo=\${b_n${_n}_ops_sec}"
    eval "_np50=\${n_n${_n}_p50}; _sp50=\${s_n${_n}_p50}; _bp50=\${b_n${_n}_p50}"
    eval "_np95=\${n_n${_n}_p95}; _sp95=\${s_n${_n}_p95}; _bp95=\${b_n${_n}_p95}"
    eval "_np99=\${n_n${_n}_p99}; _sp99=\${s_n${_n}_p99}; _bp99=\${b_n${_n}_p99}"

    _wt="$(winner_throughput "${_no}" "${_so}" "${_bo}" "neon" "supabase" "basin")"
    _wl="$(winner_of_3 "${_np50}" "${_sp50}" "${_bp50}" "neon" "supabase" "basin")"
    _wl99="$(winner_of_3 "${_np99}" "${_sp99}" "${_bp99}" "neon" "supabase" "basin")"

    [[ "${_first_metric}" -eq 0 ]] && printf ',\n'
    cat <<EOF
    {
      "label": "N=${_n} throughput (ops/sec)",
      "neon":     $(jq_num "${_no}"),
      "supabase": $(jq_num "${_so}"),
      "basin":    $(jq_num "${_bo}"),
      "unit": "ops/sec",
      "better": $(jq_str "${_wt}"),
      "ratio_text": null,
      "note": "${_n} parallel workers, 70/20/10 read/range/insert mix. Higher is better."
    },
    {
      "label": "N=${_n} p50 latency (ms)",
      "neon":     $(jq_num "${_np50}"),
      "supabase": $(jq_num "${_sp50}"),
      "basin":    $(jq_num "${_bp50}"),
      "unit": "ms",
      "better": $(jq_str "${_wl}"),
      "ratio_text": null,
      "note": "Median per-operation round-trip across all ${_n} workers."
    },
    {
      "label": "N=${_n} p95 latency (ms)",
      "neon":     $(jq_num "${_np95}"),
      "supabase": $(jq_num "${_sp95}"),
      "basin":    $(jq_num "${_bp95}"),
      "unit": "ms",
      "better": null,
      "ratio_text": null,
      "note": null
    },
    {
      "label": "N=${_n} p99 latency (ms)",
      "neon":     $(jq_num "${_np99}"),
      "supabase": $(jq_num "${_sp99}"),
      "basin":    $(jq_num "${_bp99}"),
      "unit": "ms",
      "better": $(jq_str "${_wl99}"),
      "ratio_text": null,
      "note": "Tail latency under ${_n}-worker concurrency."
    }
EOF
    _first_metric=0
done

cat <<JSONEOF

  ],
  "note": "Workers are sub-processes each issuing ${OPS_PER_WORKER} ops (+ ${WARMUP_OPS} discarded warm-up). Throughput = total timed ops / wall-clock seconds across all workers. Latency percentiles computed over all per-op samples from all workers. Targets measured sequentially."
}
JSONEOF
} > "${OUT_JSON}"

log ""
log "result JSON written: ${OUT_JSON}"

log "dropping schemas..."
if [[ "${NEON_PREPARED}" -eq 1 ]];     then drop_schema "${NEON_DATABASE_URL}"     "bench_speed_concurrent"; NEON_PREPARED=0; fi
if [[ "${SUPABASE_PREPARED}" -eq 1 ]]; then drop_schema "${SUPABASE_DATABASE_URL}" "bench_speed_concurrent"; SUPABASE_PREPARED=0; fi
if [[ "${BASIN_PREPARED}" -eq 1 ]];    then drop_schema "${BASIN_DATABASE_URL}"    "bench_speed_concurrent"; BASIN_PREPARED=0; fi
log "done."
