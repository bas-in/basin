#!/usr/bin/env bash
# benchmark/speed/_lib.sh
#
# Shared bash helpers for the Basin speed benchmark suite.
# Source this file from each scenario's run.sh:
#
#   SPEED_LIB="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/_lib.sh"
#   # shellcheck source=benchmark/speed/_lib.sh
#   source "${SPEED_LIB}"
#
# After sourcing, the following are available:
#   - log / info                    — stderr / stdout logging
#   - epoch_ms                      — current time in milliseconds
#   - percentile_of SORTED_NEWLINE N   — Nth percentile (0-100) of sorted list
#   - p50_of / p95_of / p99_of     — convenience wrappers
#   - run_sql URL SQL               — execute SQL, return stdout
#   - jq_str / jq_num               — safe JSON value helpers
#   - winner_of_3 A B C LA LB LC   — lowest-wins comparison ("neon"|"supabase"|"basin"|"null")
#   - ratio_text WINNER_VAL BEST    — "1.4x faster than Neon" style string
#   - load_toml_config              — load .basin-three-way.toml (same file as three_way/)
#   - NEON_DATABASE_URL / SUPABASE_DATABASE_URL / BASIN_DATABASE_URL / REGION_LABEL
#     These are exported after load_toml_config runs.
#   - check_targets SCENARIO        — detect which targets are available; set
#     HAS_NEON / HAS_SUPABASE / HAS_BASIN / ACTIVE_TARGETS (space-separated list)
#   - REPO_ROOT                     — absolute path to repo root

# ---------- repo root ---------------------------------------------------------

# _lib.sh lives at benchmark/speed/_lib.sh, so repo root is two levels up.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export REPO_ROOT

# ---------- logging -----------------------------------------------------------

# log: always to stderr (non-data output).
log()  { printf '[speed/%s] %s\n' "${SPEED_SCENARIO:-?}" "$*" >&2; }
# info: stdout (pipeline-safe, e.g. captured by run_all_speed.sh).
info() { printf '[speed/%s] %s\n' "${SPEED_SCENARIO:-?}" "$*"; }

# ---------- timing ------------------------------------------------------------

# epoch_ms: current Unix time in milliseconds.
# Prefers `date +%s%3N` (Linux / GNU date / macOS with coreutils).
# Falls back to Python if date lacks %3N support (BSD date on plain macOS).
# Last resort: $SECONDS * 1000 (1-second resolution).
epoch_ms() {
    local _t
    if _t="$(date +%s%3N 2>/dev/null)" && [[ "${_t}" =~ ^[0-9]+$ ]] && [[ "${#_t}" -gt 10 ]]; then
        echo "${_t}"
    elif command -v python3 >/dev/null 2>&1; then
        python3 -c "import time; print(int(time.time()*1000))"
    else
        echo $(( SECONDS * 1000 ))
    fi
}

# ---------- percentile calculation -------------------------------------------
#
# percentile_of VALUES N
#   VALUES  — newline-separated list of numeric values (need not be sorted).
#   N       — percentile to compute (0-100, e.g. 50, 95, 99).
#
# Uses the "nearest-rank" method with linear interpolation between adjacent
# sorted values when the index falls between two elements.
#
# Returns the result to stdout as a decimal number.
percentile_of() {
    local values="$1"
    local pct="$2"
    # Remove blank lines before sorting to avoid awk confusion.
    printf '%s\n' "${values}" \
        | grep -v '^[[:space:]]*$' \
        | sort -n \
        | awk -v pct="${pct}" '
            { lines[NR] = $0 + 0; n = NR }
            END {
                if (n == 0) { print "null"; exit }
                if (n == 1) { print lines[1]; exit }
                # Compute the fractional rank (1-based).
                rank = (pct / 100.0) * (n - 1) + 1
                lo = int(rank)
                hi = lo + 1
                if (hi > n) hi = n
                frac = rank - lo
                # Linear interpolation.
                val = lines[lo] + frac * (lines[hi] - lines[lo])
                printf "%.3f\n", val
            }
        '
}

p50_of() { percentile_of "$1" 50; }
p95_of() { percentile_of "$1" 95; }
p99_of() { percentile_of "$1" 99; }

# ---------- psql wrapper ------------------------------------------------------

# run_sql URL SQL
# Execute a single SQL statement via psql. Returns stdout, suppresses stderr.
run_sql() {
    local url="$1"
    local sql="$2"
    psql "${url}" --no-psqlrc --tuples-only --no-align -c "${sql}" 2>/dev/null
}

# run_sql_file URL FILE
# Execute a SQL file via psql.
run_sql_file() {
    local url="$1"
    local file="$2"
    psql "${url}" --no-psqlrc --tuples-only --no-align -f "${file}" 2>/dev/null
}

# ---------- time_query --------------------------------------------------------
#
# time_query URL SQL ITERATIONS WARMUP_COUNT
#
# Runs SQL against URL ITERATIONS+WARMUP_COUNT times.  The first WARMUP_COUNT
# samples are discarded (cold-cache / connection-establishment noise).
# The remaining ITERATIONS samples are returned in TIME_QUERY_SAMPLES
# (newline-separated, milliseconds as floats).
# Also sets TIME_QUERY_OK=1 if at least one iteration succeeded.
#
# Example:
#   time_query "${URL}" "SELECT 1" 100 10
#   p50="$(p50_of "${TIME_QUERY_SAMPLES}")"
#
time_query() {
    local _url="$1"
    local _sql="$2"
    local _iters="$3"
    local _warmup="${4:-10}"

    TIME_QUERY_SAMPLES=""
    TIME_QUERY_OK=0

    local _total=$(( _iters + _warmup ))
    local _k

    for (( _k = 0; _k < _total; _k++ )); do
        local _t0 _t1 _elapsed
        _t0="$(epoch_ms)"
        if psql "${_url}" --no-psqlrc --tuples-only --no-align -c "${_sql}" >/dev/null 2>&1; then
            TIME_QUERY_OK=1
        fi
        _t1="$(epoch_ms)"
        _elapsed=$(( _t1 - _t0 ))

        # Discard warm-up iterations.
        if [[ "${_k}" -ge "${_warmup}" ]]; then
            TIME_QUERY_SAMPLES="${TIME_QUERY_SAMPLES}${_elapsed}"$'\n'
        fi
    done
}

# ---------- JSON helpers ------------------------------------------------------

# jq_str VALUE — emit a JSON double-quoted string, or null.
jq_str() {
    if [[ -z "${1:-}" || "${1}" == "null" ]]; then
        echo "null"
    else
        local s="${1//\\/\\\\}"
        s="${s//\"/\\\"}"
        printf '"%s"' "${s}"
    fi
}

# jq_num VALUE — emit a JSON number, or null.
jq_num() {
    if [[ -z "${1:-}" || "${1}" == "null" ]]; then
        echo "null"
    else
        echo "$1"
    fi
}

# winner_of_3 A B C LA LB LC
# Lower-is-better.  A/B/C are numeric values (or "null").
# Returns the label (LA/LB/LC) of the smallest value, or "null" if any is null.
winner_of_3() {
    local a="$1" b="$2" c="$3"
    local la="$4" lb="$5" lc="$6"

    # Strip decimal for integer comparison; use Python for float comparison.
    if [[ "${a}" == "null" || "${b}" == "null" || "${c}" == "null" ]]; then
        echo "null"; return
    fi

    python3 - "${a}" "${b}" "${c}" "${la}" "${lb}" "${lc}" <<'PY'
import sys
a, b, c = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3])
la, lb, lc = sys.argv[4], sys.argv[5], sys.argv[6]
mn = min(a, b, c)
print(la if mn == a else (lb if mn == b else lc))
PY
}

# ratio_text WINNER_VAL LOSER_VAL LOSER_LABEL
# Returns a human-readable ratio string, e.g. "1.4x faster than Neon".
ratio_text() {
    local winner_val="$1"
    local loser_val="$2"
    local loser_label="$3"
    if [[ -z "${winner_val}" || "${winner_val}" == "null" || \
          -z "${loser_val}"  || "${loser_val}"  == "null" ]]; then
        echo "null"; return
    fi
    python3 - "${winner_val}" "${loser_val}" "${loser_label}" <<'PY'
import sys
w, l = float(sys.argv[1]), float(sys.argv[2])
label = sys.argv[3]
if w <= 0 or l <= 0:
    print("null")
else:
    ratio = l / w
    print(f"{ratio:.1f}x faster than {label.capitalize()}")
PY
}

# ---------- TOML config loader -----------------------------------------------
#
# Reads ${REPO_ROOT}/.basin-three-way.toml (the same file used by three_way/).
# Already-set environment variables WIN over TOML values.
# Exports: NEON_DATABASE_URL, SUPABASE_DATABASE_URL, BASIN_DATABASE_URL,
#          REGION_LABEL, and optionally SPEED_ITERATIONS, SPEED_OUT_DIR.

load_toml_config() {
    local _cfg="${BASIN_THREE_WAY_CONFIG:-${REPO_ROOT}/.basin-three-way.toml}"
    if [[ ! -f "${_cfg}" ]] || ! command -v python3 >/dev/null 2>&1; then
        return 0  # silent skip — env vars will be used directly
    fi

    while IFS=$'\t' read -r _key _value; do
        [[ -z "${_key}" ]] && continue
        if [[ -z "${!_key:-}" ]]; then
            export "${_key}=${_value}"
        fi
    done < <(python3 - "${_cfg}" <<'PY' 2>/dev/null
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
emit("SPEED_ITERATIONS",      ep.get("speed_iterations"))
emit("SPEED_OUT_DIR",         ep.get("speed_out_dir"))
PY
    )
}

# ---------- target detection --------------------------------------------------
#
# check_targets SCENARIO
# After load_toml_config has run, inspect which DSNs are available.
# Sets (does NOT export):
#   HAS_NEON / HAS_SUPABASE / HAS_BASIN  — "1" or "0"
#   ACTIVE_TARGETS                        — space-separated list of present targets
#   ACTIVE_COUNT                          — number of active targets
#
# If ACTIVE_COUNT == 0, prints an informative message and returns 1.
# Individual run.sh scripts should `exit 0` gracefully on return value 1.

check_targets() {
    local _scenario="${1:-${SPEED_SCENARIO:-speed}}"

    HAS_NEON=0
    HAS_SUPABASE=0
    HAS_BASIN=0
    ACTIVE_TARGETS=""
    ACTIVE_COUNT=0

    if [[ -n "${NEON_DATABASE_URL:-}" ]]; then
        HAS_NEON=1
        ACTIVE_TARGETS="${ACTIVE_TARGETS} neon"
        (( ACTIVE_COUNT++ )) || true
    fi
    if [[ -n "${SUPABASE_DATABASE_URL:-}" ]]; then
        HAS_SUPABASE=1
        ACTIVE_TARGETS="${ACTIVE_TARGETS} supabase"
        (( ACTIVE_COUNT++ )) || true
    fi
    if [[ -n "${BASIN_DATABASE_URL:-}" ]]; then
        HAS_BASIN=1
        ACTIVE_TARGETS="${ACTIVE_TARGETS} basin"
        (( ACTIVE_COUNT++ )) || true
    fi
    ACTIVE_TARGETS="${ACTIVE_TARGETS# }"  # strip leading space

    if [[ "${ACTIVE_COUNT}" -eq 0 ]]; then
        log "No DSNs configured — skipping ${_scenario}."
        log "Set NEON_DATABASE_URL / SUPABASE_DATABASE_URL / BASIN_DATABASE_URL"
        log "(or populate .basin-three-way.toml) to enable live measurement."
        return 1
    fi

    if [[ "${ACTIVE_COUNT}" -lt 3 ]]; then
        log "WARNING: only ${ACTIVE_COUNT}/3 targets available (${ACTIVE_TARGETS})."
        log "  Missing targets will appear as null in the JSON output."
    fi

    return 0
}

# ---------- URL redaction (for dry-run output) --------------------------------

redact_url() {
    printf '%s\n' "${1:-}" | sed 's|//[^:@]*:[^@]*@|//***:***@|'
}

# ---------- schema helpers ----------------------------------------------------

# create_schema URL SCHEMA_SQL_FILE SCHEMA_NAME
# Runs the given schema SQL file and marks SCHEMA_NAME as prepared.
# Sets: SCHEMA_CREATED_<UPPER_TARGET>=1 (caller uses this for trap cleanup).
create_schema() {
    local _url="$1"
    local _sql_file="$2"
    psql "${_url}" --no-psqlrc -f "${_sql_file}" >/dev/null 2>&1
}

# drop_schema URL SCHEMA_NAME
# Idempotent DROP SCHEMA IF EXISTS ... CASCADE.
drop_schema() {
    local _url="$1"
    local _schema="$2"
    psql "${_url}" --no-psqlrc -c \
        "DROP SCHEMA IF EXISTS ${_schema} CASCADE" >/dev/null 2>&1 || true
}
