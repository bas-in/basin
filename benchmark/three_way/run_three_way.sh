#!/usr/bin/env bash
# benchmark/three_way/run_three_way.sh
#
# 3-way Postgres-wire benchmark: Neon vs Supabase vs Basin Cloud (Frankfurt).
#
# USAGE
#   NEON_DATABASE_URL=postgres://... \
#   SUPABASE_DATABASE_URL=postgres://... \
#   BASIN_DATABASE_URL=postgres://... \
#   REGION_LABEL=fra \
#   ./benchmark/three_way/run_three_way.sh [--dry-run]
#
# FLAGS
#   --dry-run   Validate all required env vars and print the execution plan;
#               exit without making any network connections. Safe by default:
#               if you are unsure, run --dry-run first.
#
# REQUIRED ENV VARS (no .env file is ever read)
#   NEON_DATABASE_URL      Full libpq connection string for the Neon Frankfurt endpoint.
#   SUPABASE_DATABASE_URL  Full libpq connection string for the Supabase Frankfurt endpoint.
#   BASIN_DATABASE_URL     Full libpq connection string for the Basin Cloud Frankfurt endpoint.
#   REGION_LABEL           Stamped into the output JSON for provenance (e.g. "fra").
#                          The operator is responsible for ensuring all three endpoints
#                          are actually provisioned in the same region.
#
# OPTIONAL ENV VARS
#   ROW_COUNT    Number of rows to insert (default: 100000).
#   ITERATIONS   Number of timed query repetitions per query (default: 10).
#   OUT_DIR      Directory to write the result JSON (default: ./benchmark/three_way/out/).
#
# DEPENDENCIES
#   psql  (libpq) — must be on PATH. Install via:
#     macOS:  brew install libpq && brew link --force libpq
#     Ubuntu: apt-get install -y postgresql-client
#   jq    — optional; used for pretty-printing the summary. If absent, raw JSON
#           is printed instead.
#
# SAFETY GUARANTEES
#   1. All three URLs must be set; a partial/invalid run is impossible.
#   2. --dry-run prints the plan and exits 0 without connecting.
#   3. A trap-based teardown (DROP SCHEMA CASCADE) runs on EXIT (success, failure,
#      or Ctrl-C) for each endpoint that was successfully prepared.
#   4. No .env files, secrets files, or implicit credential sources are consulted.
#
# OUTPUT
#   benchmark/three_way/out/three_way_<REGION_LABEL>_<TIMESTAMP>.json
#
# JSON SCHEMA
#   See the "JSON schema" section below for the 3-column compare shape.

set -euo pipefail

# ---------- constants --------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEFAULT_OUT_DIR="${REPO_ROOT}/benchmark/three_way/out"

# ---------- CLI flags --------------------------------------------------------

DRY_RUN=0
for arg in "$@"; do
    case "${arg}" in
        --dry-run) DRY_RUN=1 ;;
        *)
            printf '[three_way] unknown flag: %s\n' "${arg}" >&2
            printf 'Usage: %s [--dry-run]\n' "$0" >&2
            exit 1
            ;;
    esac
done

# ---------- helpers ----------------------------------------------------------

log()  { printf '[three_way] %s\n' "$*" >&2; }
info() { printf '[three_way] %s\n' "$*"; }

# Elapsed milliseconds since a SECONDS snapshot.
# Usage: elapsed_ms <start_seconds_float_var>
# We use $SECONDS (integer) as a coarse timer and supplement with the
# finer-grained date +%s%3N where available.
epoch_ms() {
    if date +%s%3N >/dev/null 2>&1; then
        date +%s%3N
    else
        # Fallback: SECONDS * 1000 (1-second resolution).
        echo $(( SECONDS * 1000 ))
    fi
}

# Compute p50 (median) of a newline-separated list of millisecond values.
# Values are already integers (ms from \timing output).
p50_of() {
    local values="$1"
    # Sort numerically and pick the middle element.
    printf '%s\n' "${values}" \
        | sort -n \
        | awk 'BEGIN{lines=""} {lines=lines"\n"$0; n++} END{
            split(lines, a, "\n");
            # a[1] is empty because of leading \n
            mid=int((n+1)/2)+1;
            print a[mid]
          }'
}

# p95: 95th percentile of a newline-separated list of ms values.
p95_of() {
    local values="$1"
    printf '%s\n' "${values}" \
        | sort -n \
        | awk '{lines[NR]=$0; n=NR} END{
            idx=int(n*0.95+0.5);
            if(idx<1) idx=1;
            if(idx>n) idx=n;
            print lines[idx]
          }'
}

# Run a single SQL string against a URL via psql and capture stdout.
# Exits non-zero if psql itself fails.
run_sql() {
    local url="$1"
    local sql="$2"
    psql "${url}" --no-psqlrc --tuples-only --no-align -c "${sql}" 2>/dev/null
}

# Run a SQL file against a URL, substituting :token placeholders.
# Args: url target_id range_lo range_hi sql_file
run_sql_file_with_subs() {
    local url="$1"
    local target_id="$2"
    local range_lo="$3"
    local range_hi="$4"
    local sql_file="$5"
    # psql -v sets session variables; :token is then expanded by psql.
    psql "${url}" --no-psqlrc --tuples-only --no-align \
        -v "target_id=${target_id}" \
        -v "range_lo=${range_lo}" \
        -v "range_hi=${range_hi}" \
        -f "${sql_file}" 2>/dev/null
}

# ---------- TOML config loader ----------------------------------------------
#
# Load a TOML config if present and export the standard env-var names from it.
# Already-exported environment variables WIN — the TOML is fallback, so users
# can override one value via `NEON_DATABASE_URL=... ./run_three_way.sh` without
# editing the file.
#
# Lookup order:
#   1. $BASIN_THREE_WAY_CONFIG=<path>
#   2. ./.basin-three-way.toml         (relative to the script's repo root)
#   3. else: pure env-var path
#
# Expected TOML shape (mirrors .basin-three-way.example.toml):
#   [endpoints]
#   neon         = "postgres://..."
#   supabase     = "postgres://..."
#   basin        = "postgres://..."
#   region_label = "fra"
#   row_count    = 100000   # optional
#   iterations   = 10       # optional
#   out_dir      = "..."    # optional
#
# Parsed via python3 stdlib (tomllib on 3.11+, tomli on older). If python3
# is unavailable, we silently skip the TOML and rely on env vars.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
THREE_WAY_CFG="${BASIN_THREE_WAY_CONFIG:-${REPO_ROOT}/.basin-three-way.toml}"

if [[ -f "${THREE_WAY_CFG}" ]] && command -v python3 >/dev/null 2>&1; then
    log "Loading 3-way config from: ${THREE_WAY_CFG}"
    # Read each key; export ONLY if the corresponding env var is empty.
    # python prints `KEY\tVALUE` lines on stdout; empty value = skip.
    while IFS=$'\t' read -r key value; do
        [[ -z "${key}" ]] && continue
        # Respect pre-existing env var.
        if [[ -z "${!key:-}" ]]; then
            export "${key}=${value}"
        fi
    done < <(python3 - "${THREE_WAY_CFG}" <<'PY' 2>/dev/null
import sys
try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib  # type: ignore
    except ImportError:
        sys.exit(0)
try:
    with open(sys.argv[1], "rb") as f:
        cfg = tomllib.load(f)
except Exception:
    sys.exit(0)
ep = cfg.get("endpoints", {}) or {}
def emit(k, v):
    if v is None:
        return
    print(f"{k}\t{v}")
emit("NEON_DATABASE_URL",     ep.get("neon"))
emit("SUPABASE_DATABASE_URL", ep.get("supabase"))
emit("BASIN_DATABASE_URL",    ep.get("basin"))
emit("REGION_LABEL",          ep.get("region_label"))
emit("ROW_COUNT",             ep.get("row_count"))
emit("ITERATIONS",            ep.get("iterations"))
emit("OUT_DIR",               ep.get("out_dir"))
PY
    )
fi

# ---------- input validation -------------------------------------------------

MISSING_VARS=()
[[ -z "${NEON_DATABASE_URL:-}" ]]      && MISSING_VARS+=("NEON_DATABASE_URL")
[[ -z "${SUPABASE_DATABASE_URL:-}" ]]  && MISSING_VARS+=("SUPABASE_DATABASE_URL")
[[ -z "${BASIN_DATABASE_URL:-}" ]]     && MISSING_VARS+=("BASIN_DATABASE_URL")
[[ -z "${REGION_LABEL:-}" ]]           && MISSING_VARS+=("REGION_LABEL")

if [[ ${#MISSING_VARS[@]} -gt 0 ]]; then
    log "ERROR: the following required environment variables are not set:"
    for v in "${MISSING_VARS[@]}"; do
        log "  - ${v}"
    done
    log ""
    log "Set all four values before running.  Two options:"
    log ""
    log "  Option A — TOML config (preferred, persists across runs):"
    log "    cp .basin-three-way.example.toml .basin-three-way.toml"
    log "    \$EDITOR .basin-three-way.toml   # fill in the three DSNs"
    log "    ./benchmark/three_way/run_three_way.sh"
    log ""
    log "  Option B — environment variables:"
    log "    export NEON_DATABASE_URL='postgres://user:pass@host/db'"
    log "    export SUPABASE_DATABASE_URL='postgres://user:pass@host/db'"
    log "    export BASIN_DATABASE_URL='postgres://user:pass@host/db'"
    log "    export REGION_LABEL=fra"
    log "    ./benchmark/three_way/run_three_way.sh"
    exit 1
fi

ROW_COUNT="${ROW_COUNT:-100000}"
ITERATIONS="${ITERATIONS:-10}"
OUT_DIR="${OUT_DIR:-${DEFAULT_OUT_DIR}}"

# Validate numeric parameters.
if ! [[ "${ROW_COUNT}" =~ ^[0-9]+$ ]] || [[ "${ROW_COUNT}" -lt 1 ]]; then
    log "ERROR: ROW_COUNT must be a positive integer (got: ${ROW_COUNT})"
    exit 1
fi
if ! [[ "${ITERATIONS}" =~ ^[0-9]+$ ]] || [[ "${ITERATIONS}" -lt 1 ]]; then
    log "ERROR: ITERATIONS must be a positive integer (got: ${ITERATIONS})"
    exit 1
fi

# ---------- dry-run ----------------------------------------------------------

if [[ "${DRY_RUN}" -eq 1 ]]; then
    info "=== DRY RUN — no connections will be made ==="
    info ""
    info "Plan:"
    info "  Region label   : ${REGION_LABEL}"
    info "  Row count      : ${ROW_COUNT}"
    info "  Iterations     : ${ITERATIONS}"
    info "  Output dir     : ${OUT_DIR}"
    info ""
    info "Endpoints (from env, not validated for reachability in dry-run):"
    # Redact password from URLs for safe display.
    redact_url() {
        printf '%s\n' "$1" | sed 's|//[^:@]*:[^@]*@|//***:***@|'
    }
    info "  NEON     : $(redact_url "${NEON_DATABASE_URL}")"
    info "  SUPABASE : $(redact_url "${SUPABASE_DATABASE_URL}")"
    info "  BASIN    : $(redact_url "${BASIN_DATABASE_URL}")"
    info ""
    info "Workload:"
    info "  Schema  : benchmark/three_way/three_way_schema.sql"
    info "  Queries : benchmark/three_way/three_way_queries.sql"
    info "  Q1 Point query    WHERE id = (N/2+7)"
    info "  Q2 Range scan     WHERE id BETWEEN N/4 AND N/4+10000"
    info "  Q3 Aggregate      GROUP BY id/1000 — COUNT(*), SUM(id)"
    info "  Q4 On-disk size   pg_total_relation_size (null if unavailable)"
    info ""
    info "Safety:"
    info "  Teardown: DROP SCHEMA bench_three_way CASCADE on each endpoint (trap-based)"
    info "  Partial run impossible: all 3 URLs must be set before any connection is made"
    info ""
    info "Output JSON: ${OUT_DIR}/three_way_${REGION_LABEL}_<timestamp>.json"
    info ""
    info "=== END DRY RUN — exiting without connecting ==="
    exit 0
fi

# ---------- dependency check -------------------------------------------------

if ! command -v psql >/dev/null 2>&1; then
    log "ERROR: psql not found on PATH."
    log "  macOS:  brew install libpq && brew link --force libpq"
    log "  Ubuntu: apt-get install -y postgresql-client"
    exit 1
fi

# ---------- setup ------------------------------------------------------------

mkdir -p "${OUT_DIR}"

TIMESTAMP="$(date +%Y%m%dT%H%M%S)"
OUT_JSON="${OUT_DIR}/three_way_${REGION_LABEL}_${TIMESTAMP}.json"
SCHEMA_SQL="${SCRIPT_DIR}/three_way_schema.sql"
QUERIES_SQL="${SCRIPT_DIR}/three_way_queries.sql"

# Computed query parameters (mirrors the OSS harness).
TARGET_ID=$(( ROW_COUNT / 2 + 7 ))
RANGE_LO=$(( ROW_COUNT / 4 ))
RANGE_HI=$(( RANGE_LO + 10000 ))

log "3-way Frankfurt benchmark  (region: ${REGION_LABEL})"
log "  rows=${ROW_COUNT}  iterations=${ITERATIONS}"
log "  schema: ${SCHEMA_SQL}"
log "  queries: ${QUERIES_SQL}"
log "  output: ${OUT_JSON}"

# ---------- teardown trap ----------------------------------------------------
#
# Each flag is set once the corresponding schema has been created, so we only
# attempt teardown on endpoints that were actually prepared.

NEON_PREPARED=0
SUPABASE_PREPARED=0
BASIN_PREPARED=0

cleanup() {
    local exit_code=$?
    log "running teardown (trap)..."
    if [[ "${NEON_PREPARED}" -eq 1 ]]; then
        log "  dropping bench_three_way on Neon..."
        psql "${NEON_DATABASE_URL}" --no-psqlrc -c \
            "DROP SCHEMA IF EXISTS bench_three_way CASCADE" \
            >/dev/null 2>&1 || true
    fi
    if [[ "${SUPABASE_PREPARED}" -eq 1 ]]; then
        log "  dropping bench_three_way on Supabase..."
        psql "${SUPABASE_DATABASE_URL}" --no-psqlrc -c \
            "DROP SCHEMA IF EXISTS bench_three_way CASCADE" \
            >/dev/null 2>&1 || true
    fi
    if [[ "${BASIN_PREPARED}" -eq 1 ]]; then
        log "  dropping bench_three_way on Basin Cloud..."
        psql "${BASIN_DATABASE_URL}" --no-psqlrc -c \
            "DROP SCHEMA IF EXISTS bench_three_way CASCADE" \
            >/dev/null 2>&1 || true
    fi
    log "teardown complete."
    exit "${exit_code}"
}
trap cleanup EXIT

# ---------- per-endpoint benchmark function ----------------------------------

# benchmark_endpoint LABEL URL
# Sets result variables in the caller's scope:
#   result_insert_ms, result_disk_bytes, result_disk_null
#   result_q1_p50, result_q1_p95, result_q1_null
#   result_q2_p50, result_q2_p95
#   result_q3_p50, result_q3_p95
benchmark_endpoint() {
    local label="$1"
    local url="$2"

    log ""
    log "=== ${label} ==="

    # ---- schema creation ----
    log "  creating schema..."
    psql "${url}" --no-psqlrc -f "${SCHEMA_SQL}" >/dev/null

    # Mark as prepared so teardown runs even if we fail mid-benchmark.
    case "${label}" in
        Neon)    NEON_PREPARED=1 ;;
        Supabase) SUPABASE_PREPARED=1 ;;
        Basin)   BASIN_PREPARED=1 ;;
    esac

    # ---- bulk insert ----
    log "  inserting ${ROW_COUNT} rows..."

    local insert_start
    insert_start="$(epoch_ms)"

    # Build and execute bulk insert in batches of 1000 rows.
    # Using a single heredoc piped to psql avoids per-row round trips.
    local batch_size=1000
    local batch_sql=""
    local batch_count=0
    local i=0

    # We build SQL in a temp file to avoid shell argument limits.
    local tmpfile
    tmpfile="$(mktemp /tmp/basin_bench_insert_XXXXXX.sql)"
    # shellcheck disable=SC2064
    trap "rm -f ${tmpfile}" RETURN

    {
        printf 'BEGIN;\n'
        while [[ "${i}" -lt "${ROW_COUNT}" ]]; do
            local batch_end=$(( i + batch_size ))
            if [[ "${batch_end}" -gt "${ROW_COUNT}" ]]; then
                batch_end="${ROW_COUNT}"
            fi
            printf 'INSERT INTO bench_three_way.events (id, project_id, action, created_at, payload) VALUES\n'
            local first=1
            local j="${i}"
            while [[ "${j}" -lt "${batch_end}" ]]; do
                if [[ "${first}" -eq 0 ]]; then printf ',\n'; fi
                # payload is a fixed 50-char string varying by id.
                printf "(%d, gen_random_uuid(), 'action_%d', now(), 'payload-%040d')" \
                    "${j}" "$(( j % 10 ))" "${j}"
                first=0
                (( j++ )) || true
            done
            printf ';\n'
            (( i += batch_size )) || true
            (( batch_count++ )) || true
        done
        printf 'COMMIT;\n'
    } > "${tmpfile}"

    psql "${url}" --no-psqlrc -f "${tmpfile}" >/dev/null

    local insert_end
    insert_end="$(epoch_ms)"
    result_insert_ms=$(( insert_end - insert_start ))
    log "  insert done: ${result_insert_ms} ms"

    # ---- on-disk size ----
    log "  measuring on-disk size..."
    local disk_raw
    disk_raw="$(run_sql "${url}" \
        "SELECT pg_total_relation_size('bench_three_way.events')::bigint" 2>/dev/null || echo "NULL")"
    disk_raw="${disk_raw//[[:space:]]/}"  # strip whitespace
    result_disk_null=0
    if [[ -z "${disk_raw}" || "${disk_raw}" == "NULL" || "${disk_raw}" == "null" ]]; then
        result_disk_bytes="null"
        result_disk_null=1
        log "  on-disk size: unavailable (null)"
    else
        result_disk_bytes="${disk_raw}"
        log "  on-disk size: ${result_disk_bytes} bytes"
    fi

    # ---- timed queries ----

    # Helper: time a single SQL statement N times; store p50/p95 into caller vars.
    # Sets: _p50_ms, _p95_ms (integers, milliseconds), _null_flag (0 or 1)
    time_query() {
        local _url="$1"
        local _sql="$2"
        local _iters="$3"
        local _samples=""
        local _ok=0

        for (( _k=0; _k < _iters; _k++ )); do
            local _t0 _t1 _elapsed
            _t0="$(epoch_ms)"
            psql "${_url}" --no-psqlrc --tuples-only --no-align -c "${_sql}" >/dev/null 2>&1 && _ok=1
            _t1="$(epoch_ms)"
            _elapsed=$(( _t1 - _t0 ))
            _samples="${_samples}${_elapsed}"$'\n'
        done

        if [[ "${_ok}" -eq 0 ]]; then
            _p50_ms="null"
            _p95_ms="null"
            _null_flag=1
            return
        fi
        _null_flag=0
        _p50_ms="$(p50_of "${_samples}")"
        _p95_ms="$(p95_of "${_samples}")"
    }

    # Q1: point query
    log "  Q1 point query (${ITERATIONS} iterations)..."
    local Q1_SQL="SELECT id, project_id, action, created_at, payload FROM bench_three_way.events WHERE id = ${TARGET_ID}"
    local _p50_ms _p95_ms _null_flag
    time_query "${url}" "${Q1_SQL}" "${ITERATIONS}"
    result_q1_p50="${_p50_ms}"
    result_q1_p95="${_p95_ms}"
    result_q1_null="${_null_flag}"
    log "  Q1 p50=${result_q1_p50} ms  p95=${result_q1_p95} ms"

    # Q2: range scan
    log "  Q2 range scan (${ITERATIONS} iterations)..."
    local Q2_SQL="SELECT id, action, created_at FROM bench_three_way.events WHERE id BETWEEN ${RANGE_LO} AND ${RANGE_HI}"
    time_query "${url}" "${Q2_SQL}" "${ITERATIONS}"
    result_q2_p50="${_p50_ms}"
    result_q2_p95="${_p95_ms}"
    log "  Q2 p50=${result_q2_p50} ms  p95=${result_q2_p95} ms"

    # Q3: aggregate
    log "  Q3 aggregate GROUP BY (${ITERATIONS} iterations)..."
    local Q3_SQL="SELECT id/1000 AS bucket, COUNT(*) AS event_count, SUM(id) AS id_sum FROM bench_three_way.events GROUP BY bucket ORDER BY bucket"
    time_query "${url}" "${Q3_SQL}" "${ITERATIONS}"
    result_q3_p50="${_p50_ms}"
    result_q3_p95="${_p95_ms}"
    log "  Q3 p50=${result_q3_p50} ms  p95=${result_q3_p95} ms"

    log "  ${label} done."
}

# ---------- run all three endpoints ------------------------------------------

# We run sequentially to avoid cross-endpoint interference on shared network
# paths.  Each endpoint is benchmarked to completion before the next starts.

# Neon
n_insert_ms=0 n_disk_bytes="null" n_disk_null=0
n_q1_p50="null" n_q1_p95="null" n_q1_null=0
n_q2_p50="null" n_q2_p95="null"
n_q3_p50="null" n_q3_p95="null"

result_insert_ms=0 result_disk_bytes="null" result_disk_null=0
result_q1_p50="null" result_q1_p95="null" result_q1_null=0
result_q2_p50="null" result_q2_p95="null"
result_q3_p50="null" result_q3_p95="null"

benchmark_endpoint "Neon" "${NEON_DATABASE_URL}"
n_insert_ms="${result_insert_ms}"
n_disk_bytes="${result_disk_bytes}"
n_disk_null="${result_disk_null}"
n_q1_p50="${result_q1_p50}"; n_q1_p95="${result_q1_p95}"; n_q1_null="${result_q1_null}"
n_q2_p50="${result_q2_p50}"; n_q2_p95="${result_q2_p95}"
n_q3_p50="${result_q3_p50}"; n_q3_p95="${result_q3_p95}"

# Supabase
s_insert_ms=0 s_disk_bytes="null" s_disk_null=0
s_q1_p50="null" s_q1_p95="null" s_q1_null=0
s_q2_p50="null" s_q2_p95="null"
s_q3_p50="null" s_q3_p95="null"

result_insert_ms=0 result_disk_bytes="null" result_disk_null=0
result_q1_p50="null" result_q1_p95="null" result_q1_null=0
result_q2_p50="null" result_q2_p95="null"
result_q3_p50="null" result_q3_p95="null"

benchmark_endpoint "Supabase" "${SUPABASE_DATABASE_URL}"
s_insert_ms="${result_insert_ms}"
s_disk_bytes="${result_disk_bytes}"
s_disk_null="${result_disk_null}"
s_q1_p50="${result_q1_p50}"; s_q1_p95="${result_q1_p95}"; s_q1_null="${result_q1_null}"
s_q2_p50="${result_q2_p50}"; s_q2_p95="${result_q2_p95}"
s_q3_p50="${result_q3_p50}"; s_q3_p95="${result_q3_p95}"

# Basin Cloud
b_insert_ms=0 b_disk_bytes="null" b_disk_null=0
b_q1_p50="null" b_q1_p95="null" b_q1_null=0
b_q2_p50="null" b_q2_p95="null"
b_q3_p50="null" b_q3_p95="null"

result_insert_ms=0 result_disk_bytes="null" result_disk_null=0
result_q1_p50="null" result_q1_p95="null" result_q1_null=0
result_q2_p50="null" result_q2_p95="null"
result_q3_p50="null" result_q3_p95="null"

benchmark_endpoint "Basin" "${BASIN_DATABASE_URL}"
b_insert_ms="${result_insert_ms}"
b_disk_bytes="${result_disk_bytes}"
b_disk_null="${result_disk_null}"
b_q1_p50="${result_q1_p50}"; b_q1_p95="${result_q1_p95}"; b_q1_null="${result_q1_null}"
b_q2_p50="${result_q2_p50}"; b_q2_p95="${result_q2_p95}"
b_q3_p50="${result_q3_p50}"; b_q3_p95="${result_q3_p95}"

# ---------- stdout summary table ---------------------------------------------

log ""
log "=== Summary (${REGION_LABEL}) ==="
printf '\n%-40s %15s %15s %15s\n' "Metric" "Neon" "Supabase" "Basin Cloud"
printf '%-40s %15s %15s %15s\n' "$(printf '%0.s-' {1..40})" "$(printf '%0.s-' {1..15})" "$(printf '%0.s-' {1..15})" "$(printf '%0.s-' {1..15})"
printf '%-40s %15s %15s %15s\n' "Bulk insert ${ROW_COUNT} rows (ms)" "${n_insert_ms}" "${s_insert_ms}" "${b_insert_ms}"
printf '%-40s %15s %15s %15s\n' "On-disk bytes" "${n_disk_bytes}" "${s_disk_bytes}" "${b_disk_bytes}"
printf '%-40s %15s %15s %15s\n' "Point query p50 (ms)" "${n_q1_p50}" "${s_q1_p50}" "${b_q1_p50}"
printf '%-40s %15s %15s %15s\n' "Point query p95 (ms)" "${n_q1_p95}" "${s_q1_p95}" "${b_q1_p95}"
printf '%-40s %15s %15s %15s\n' "Range scan p50 (ms)" "${n_q2_p50}" "${s_q2_p50}" "${b_q2_p50}"
printf '%-40s %15s %15s %15s\n' "Range scan p95 (ms)" "${n_q2_p95}" "${s_q2_p95}" "${b_q2_p95}"
printf '%-40s %15s %15s %15s\n' "Aggregate p50 (ms)" "${n_q3_p50}" "${s_q3_p50}" "${b_q3_p50}"
printf '%-40s %15s %15s %15s\n' "Aggregate p95 (ms)" "${n_q3_p95}" "${s_q3_p95}" "${b_q3_p95}"

# ---------- JSON output ------------------------------------------------------
#
# JSON schema — 3-column compare shape
#
# The existing dashboard card renderer (assets/dashboard.js: compareCard)
# reads:
#   kind           "compare"
#   id             string key (loaded as compare_<id>.json)
#   name           string — card title
#   claim          string — subtitle
#   available      bool
#   metrics[]      array of metric objects
#     label        string  — row label
#     basin        number | null  — Basin Cloud value
#     neon         number | null  — Neon value
#     supabase     number | null  — Supabase value
#     unit         string  — "ms", "bytes", etc.
#     better       "basin" | "neon" | "supabase" | "tie" | null
#     ratio_text   string | null  — human-readable summary
#     note         string | null  — per-metric caveat (e.g. "pg_total_relation_size unavailable on Basin")
#   generated_at   "@<unix-secs>"
#   region         string — provenance label
#   row_count      number
#   iterations     number
#   note           string | null
#
# The existing compareCard reads m.basin and m.postgres only.  For the
# 3-way comparison this harness produces a SEPARATE JSON rendered by the
# new three_way_card() function documented in README.md.  The shape above
# is a strict superset: it adds neon/supabase alongside basin, and the
# legacy postgres field is omitted (no Postgres-only endpoint here).
#
# To surface these cards in the existing dashboard renderer without changes
# to dashboard.js, simply drop this file into a new data_three_way/ directory,
# add a [[dashboard]] row in dashboards.toml, and write a small dashboard.js
# extension (or fork compareCard) to handle the 3-column layout.  See README.md
# for the step-by-step.

note_disk_basin=""
if [[ "${b_disk_null}" -eq 1 ]]; then
    note_disk_basin="pg_total_relation_size unavailable on Basin Cloud; value is null"
fi
note_disk_neon=""
if [[ "${n_disk_null}" -eq 1 ]]; then
    note_disk_neon="pg_total_relation_size unavailable on Neon; value is null"
fi

# Determine winner for each metric (lowest value wins for latency; highest for bytes = lowest disk).
winner_of_3() {
    local a="$1" b="$2" c="$3"
    local la="$4" lb="$5" lc="$6"   # labels: "neon" "supabase" "basin"
    # If any is null, winner is unknown.
    if [[ "${a}" == "null" || "${b}" == "null" || "${c}" == "null" ]]; then
        echo "null"
        return
    fi
    # Lower is better for all our metrics (ms, bytes).
    if [[ "${a}" -le "${b}" && "${a}" -le "${c}" ]]; then
        echo "${la}"
    elif [[ "${b}" -le "${a}" && "${b}" -le "${c}" ]]; then
        echo "${lb}"
    else
        echo "${lc}"
    fi
}

WINNER_INSERT="$(winner_of_3 "${n_insert_ms}" "${s_insert_ms}" "${b_insert_ms}" "neon" "supabase" "basin")"
WINNER_DISK="$(winner_of_3 "${n_disk_bytes}" "${s_disk_bytes}" "${b_disk_bytes}" "neon" "supabase" "basin")"
WINNER_Q1_P50="$(winner_of_3 "${n_q1_p50}" "${s_q1_p50}" "${b_q1_p50}" "neon" "supabase" "basin")"
WINNER_Q2_P50="$(winner_of_3 "${n_q2_p50}" "${s_q2_p50}" "${b_q2_p50}" "neon" "supabase" "basin")"
WINNER_Q3_P50="$(winner_of_3 "${n_q3_p50}" "${s_q3_p50}" "${b_q3_p50}" "neon" "supabase" "basin")"

# Emit quoted string or JSON null.
jq_str() {
    if [[ -z "$1" || "$1" == "null" ]]; then
        echo "null"
    else
        # Minimal JSON string escaping (backslash and double-quote only).
        local s="${1//\\/\\\\}"
        s="${s//\"/\\\"}"
        printf '"%s"' "${s}"
    fi
}

# Emit numeric value or JSON null.
jq_num() {
    if [[ -z "$1" || "$1" == "null" ]]; then
        echo "null"
    else
        echo "$1"
    fi
}

UNIX_NOW="$(date +%s)"

cat > "${OUT_JSON}" <<JSONEOF
{
  "kind": "compare",
  "id": "three_way_${REGION_LABEL}",
  "name": "Neon vs Supabase vs Basin Cloud (${REGION_LABEL}, ${ROW_COUNT} rows)",
  "claim": "Identical audit-log workload — same schema, same queries, same region — measures Postgres-wire latency and disk cost across three managed cloud offerings.",
  "available": true,
  "region": "$(jq_str "${REGION_LABEL}")",
  "row_count": ${ROW_COUNT},
  "iterations": ${ITERATIONS},
  "generated_at": "@${UNIX_NOW}",
  "metrics": [
    {
      "label": "Bulk insert ${ROW_COUNT} rows",
      "neon":     $(jq_num "${n_insert_ms}"),
      "supabase": $(jq_num "${s_insert_ms}"),
      "basin":    $(jq_num "${b_insert_ms}"),
      "unit": "ms",
      "better": "$(jq_str "${WINNER_INSERT}")",
      "ratio_text": null,
      "note": "Wall-clock time including connection overhead for INSERT batches."
    },
    {
      "label": "On-disk bytes (pg_total_relation_size)",
      "neon":     $(jq_num "${n_disk_bytes}"),
      "supabase": $(jq_num "${s_disk_bytes}"),
      "basin":    $(jq_num "${b_disk_bytes}"),
      "unit": "bytes",
      "better": "$(jq_str "${WINNER_DISK}")",
      "ratio_text": null,
      "note": $(jq_str "${note_disk_neon}${note_disk_basin}")
    },
    {
      "label": "Point query p50",
      "neon":     $(jq_num "${n_q1_p50}"),
      "supabase": $(jq_num "${s_q1_p50}"),
      "basin":    $(jq_num "${b_q1_p50}"),
      "unit": "ms",
      "better": "$(jq_str "${WINNER_Q1_P50}")",
      "ratio_text": null,
      "note": "WHERE id = N. No index on any endpoint — substrate comparison."
    },
    {
      "label": "Point query p95",
      "neon":     $(jq_num "${n_q1_p95}"),
      "supabase": $(jq_num "${s_q1_p95}"),
      "basin":    $(jq_num "${b_q1_p95}"),
      "unit": "ms",
      "better": null,
      "ratio_text": null,
      "note": null
    },
    {
      "label": "Range scan p50 (~10k rows)",
      "neon":     $(jq_num "${n_q2_p50}"),
      "supabase": $(jq_num "${s_q2_p50}"),
      "basin":    $(jq_num "${b_q2_p50}"),
      "unit": "ms",
      "better": "$(jq_str "${WINNER_Q2_P50}")",
      "ratio_text": null,
      "note": "WHERE id BETWEEN N/4 AND N/4+10000."
    },
    {
      "label": "Range scan p95",
      "neon":     $(jq_num "${n_q2_p95}"),
      "supabase": $(jq_num "${s_q2_p95}"),
      "basin":    $(jq_num "${b_q2_p95}"),
      "unit": "ms",
      "better": null,
      "ratio_text": null,
      "note": null
    },
    {
      "label": "Aggregate (COUNT/SUM GROUP BY) p50",
      "neon":     $(jq_num "${n_q3_p50}"),
      "supabase": $(jq_num "${s_q3_p50}"),
      "basin":    $(jq_num "${b_q3_p50}"),
      "unit": "ms",
      "better": "$(jq_str "${WINNER_Q3_P50}")",
      "ratio_text": null,
      "note": "SELECT id/1000 AS bucket, COUNT(*), SUM(id) FROM events GROUP BY bucket."
    },
    {
      "label": "Aggregate p95",
      "neon":     $(jq_num "${n_q3_p95}"),
      "supabase": $(jq_num "${s_q3_p95}"),
      "basin":    $(jq_num "${b_q3_p95}"),
      "unit": "ms",
      "better": null,
      "ratio_text": null,
      "note": null
    }
  ],
  "note": "All three endpoints must be in Frankfurt (${REGION_LABEL}) — the operator is responsible for provisioning. Connection setup latency is included in insert timing; individual query iterations include only the SELECT round-trip."
}
JSONEOF

log ""
log "result JSON written: ${OUT_JSON}"

# Explicit teardown before the trap fires to surface any errors.
log ""
log "dropping bench_three_way schema on all three endpoints..."
psql "${NEON_DATABASE_URL}"     --no-psqlrc -c "DROP SCHEMA IF EXISTS bench_three_way CASCADE" >/dev/null 2>&1 && NEON_PREPARED=0     || true
psql "${SUPABASE_DATABASE_URL}" --no-psqlrc -c "DROP SCHEMA IF EXISTS bench_three_way CASCADE" >/dev/null 2>&1 && SUPABASE_PREPARED=0 || true
psql "${BASIN_DATABASE_URL}"    --no-psqlrc -c "DROP SCHEMA IF EXISTS bench_three_way CASCADE" >/dev/null 2>&1 && BASIN_PREPARED=0    || true
log "all schemas dropped."

log ""
log "=== done. result: ${OUT_JSON} ==="
