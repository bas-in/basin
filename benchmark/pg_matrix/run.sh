#!/usr/bin/env bash
# benchmark/pg_matrix/run.sh
#
# Drives tests/integration/tests/compare_postgres_matrix.rs: a shape x size
# matrix of Basin (in-process, owned-engine bridge ON) vs a throwaway local
# Postgres 18.2, with per-query "which engine served" labeling.
#
# Prefers a native Homebrew postgresql@18 binary (fast, no Docker daemon
# dependency); falls back to a Docker postgres:18 container if the binary
# isn't on PATH. Either way the instance is scratch — a fresh PGDATA (or
# container) created here and torn down on exit, never touching any
# developer's real Postgres.
#
# ---------------------------------------------------------------------------
# BUILD PROFILE — read this before trusting a number out of here
# ---------------------------------------------------------------------------
# This runs the harness under `--profile bench-fast` (opt-level 2, no debug
# assertions), the same profile benchmark/run/_setup.sh uses for every other
# benchmark in this repo. A plain `cargo test` builds the dev profile, which
# is unoptimized: Basin would be measured at roughly an order of magnitude
# below its real speed while Postgres — a separately-compiled release binary —
# would not be handicapped at all. A dev-profile row in this matrix is not a
# slow Basin number, it is a meaningless one. Override with BENCH_PROFILE only
# if you know why; `BENCH_PROFILE=dev` prints a loud warning and stamps the
# output file so the numbers cannot be mistaken for benchmark-grade.
#
# ---------------------------------------------------------------------------
# KNOWN LIMITATION — the size list is compile-time
# ---------------------------------------------------------------------------
# CORE_SIZES / BOUNDARY_SIZES / CORE_SHAPES_10M live as `const`s in
# compare_postgres_matrix.rs, and the harness prints its report only after the
# LAST size finishes. That means:
#   * this script cannot trim the matrix to fit a time budget, and
#   * a run killed part-way produces NO table at all, only progress lines.
# BASIN_BENCH_MAX_SECS is therefore a blast-radius guard, not a way to get
# partial results. If you need a subset, the size lists have to change in the
# Rust harness.
#
# Usage:
#   ./benchmark/pg_matrix/run.sh                 # full matrix
#   ./benchmark/pg_matrix/run.sh --filter <name> # cargo test name filter
#   ./benchmark/pg_matrix/run.sh --no-build      # reuse an existing binary
#
# Output:
#   benchmark/pg_matrix/out/results_<timestamp>.md  (full stdout+stderr, incl.
#   the markdown table the harness prints at the end, prefixed with an
#   environment header — profile, git rev, host, Postgres version — because a
#   benchmark table without its provenance is not reproducible)
#
# Env overrides:
#   BENCH_PROFILE          (default bench-fast; `dev`/`release` also accepted)
#   BASIN_BENCH_PG_PORT    (default: first free port in 5455-5495; if set
#                            explicitly and busy, the run refuses to start)
#   BASIN_BENCH_MAX_SECS   (default 10800 = 3h; hard kill of the harness)
#   PG_BIN_DIR             (default /opt/homebrew/opt/postgresql@18/bin, then
#                            PATH's pg_ctl/initdb)
#   PG_SHARED_BUFFERS      (default 1GB — see "Postgres tuning" below)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="${REPO_ROOT}/benchmark/pg_matrix/out"
mkdir -p "$OUT_DIR"
TS="$(date +%Y%m%dT%H%M%S)"
OUT_FILE="${OUT_DIR}/results_${TS}.md"

log() { printf '[pg_matrix] %s\n' "$*" >&2; }

# ---- Pick a port nobody else is on -----------------------------------------
#
# A fixed port is a correctness hazard, not just an inconvenience. If some
# OTHER Postgres is already listening on it, `pg_ctl start` fails but
# `pg_isready` still answers yes — so a readiness probe passes, the harness
# connects, and the entire matrix is measured against a stranger's database
# with a stranger's data and settings. That is precisely the silently-wrong
# measurement this benchmark exists to avoid, so: scan for a genuinely free
# port, and refuse to run if an explicitly requested one is taken.
port_is_free() {
    ! (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

if [[ -n "${BASIN_BENCH_PG_PORT:-}" ]]; then
    PGPORT="$BASIN_BENCH_PG_PORT"
    if ! port_is_free "$PGPORT"; then
        log "ERROR: BASIN_BENCH_PG_PORT=${PGPORT} is already in use by another process."
        log "  Refusing to run: the harness would connect to THAT server, not ours."
        exit 1
    fi
else
    PGPORT=""
    for candidate in $(seq 5455 5495); do
        if port_is_free "$candidate"; then
            PGPORT="$candidate"
            break
        fi
    done
    if [[ -z "$PGPORT" ]]; then
        log "ERROR: no free TCP port in 5455-5495 for the scratch Postgres."
        exit 1
    fi
    [[ "$PGPORT" != "5455" ]] && log "port 5455 busy; using ${PGPORT} for the scratch Postgres"
fi

export BASIN_BENCH_PG_HOST=127.0.0.1
export BASIN_BENCH_PG_PORT="$PGPORT"
export BASIN_BENCH_PG_USER=postgres

BENCH_PROFILE="${BENCH_PROFILE:-bench-fast}"
MAX_SECS="${BASIN_BENCH_MAX_SECS:-10800}"
PG_SHARED_BUFFERS="${PG_SHARED_BUFFERS:-1GB}"

# ---- Args ------------------------------------------------------------------

FILTER="scaling_6_compare_postgres_matrix"
DO_BUILD=1
while [[ $# -gt 0 ]]; do
    case "$1" in
        --filter)
            [[ $# -ge 2 ]] || { log "ERROR: --filter needs an argument"; exit 2; }
            FILTER="$2"
            shift 2
            ;;
        --no-build)
            DO_BUILD=0
            shift
            ;;
        *)
            log "ERROR: unknown argument '$1'"
            exit 2
            ;;
    esac
done

case "$BENCH_PROFILE" in
    dev|debug)
        CARGO_PROFILE_FLAG=()          # cargo test's default is the dev profile
        PROFILE_NOTE="dev (UNOPTIMIZED — numbers are NOT benchmark-grade)"
        log "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        log "!! BENCH_PROFILE=dev: Basin runs unoptimized, Postgres does not."
        log "!! Every Basin number below is an artifact of the build profile."
        log "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        ;;
    release)
        CARGO_PROFILE_FLAG=(--release)
        PROFILE_NOTE="release"
        ;;
    *)
        CARGO_PROFILE_FLAG=(--profile "$BENCH_PROFILE")
        PROFILE_NOTE="$BENCH_PROFILE"
        ;;
esac

# ---- Locate a Postgres 18 binary set ---------------------------------------

PG_BIN_DIR="${PG_BIN_DIR:-}"
if [[ -z "$PG_BIN_DIR" ]]; then
    for candidate in /opt/homebrew/opt/postgresql@18/bin /usr/local/opt/postgresql@18/bin; do
        if [[ -x "${candidate}/pg_ctl" ]]; then
            PG_BIN_DIR="$candidate"
            break
        fi
    done
fi
if [[ -z "$PG_BIN_DIR" ]] && command -v pg_ctl >/dev/null 2>&1; then
    PG_BIN_DIR="$(dirname "$(command -v pg_ctl)")"
fi

USE_DOCKER=0
if [[ -z "$PG_BIN_DIR" ]]; then
    if command -v docker >/dev/null 2>&1; then
        USE_DOCKER=1
        log "no native pg_ctl found; falling back to Docker postgres:18"
    else
        log "ERROR: no native postgresql@18 (pg_ctl) and no docker on PATH."
        log "  Install: brew install postgresql@18"
        exit 1
    fi
fi

PGDATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/basin-pg-matrix.XXXXXX")"
CONTAINER_NAME="basin_pg_matrix_bench_${PGPORT}"
STARTED=0

cleanup() {
    local exit_code=$?
    if [[ "$USE_DOCKER" == "1" ]]; then
        docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
    elif [[ "$STARTED" == "1" ]]; then
        "${PG_BIN_DIR}/pg_ctl" -D "$PGDATA_DIR" stop -m fast >/dev/null 2>&1 || true
    fi
    rm -rf "$PGDATA_DIR"
    exit $exit_code
}
trap cleanup EXIT

# ---- Build BEFORE starting Postgres ----------------------------------------
#
# A cold optimized build of this workspace takes many minutes. Doing it while
# a Postgres instance sits idle just widens the window in which a crash leaks
# a PGDATA dir, and on a shared cargo target dir the build can block on the
# lock for a long time. Build first, start Postgres second.

# The harness is built ONCE here and then executed directly, rather than
# through `cargo test`. On a branch several people (or agents) are editing at
# the same time, `cargo test` at run time would re-resolve freshness against
# whatever crates/** looks like at that instant: the run could silently
# rebuild mid-benchmark, or measure a different tree than the one the header
# records, or simply fail because someone else's edit is half-saved. Resolving
# the binary path once and exec'ing it pins the measurement to one build.

BUILD_LOG="${OUT_DIR}/.build_${TS}.log"
if [[ "$DO_BUILD" == "1" ]]; then
    log "building harness (profile: ${PROFILE_NOTE}) — cold optimized builds of this workspace take ~1h"
    cargo test -p basin-integration-tests --test compare_postgres_matrix \
        "${CARGO_PROFILE_FLAG[@]}" --no-run 2>&1 | tee "$BUILD_LOG"
    BUILD_STATUS="${PIPESTATUS[0]}"
    if [[ "$BUILD_STATUS" != "0" ]]; then
        log "ERROR: build failed (exit ${BUILD_STATUS}); see ${BUILD_LOG}"
        exit "$BUILD_STATUS"
    fi
    # cargo prints:  Executable tests/compare_postgres_matrix.rs (target/<profile>/deps/compare_postgres_matrix-<hash>)
    HARNESS_BIN="$(sed -n 's/.*Executable tests\/compare_postgres_matrix\.rs (\(.*\))$/\1/p' "$BUILD_LOG" | tail -1)"
fi

if [[ -z "${HARNESS_BIN:-}" || ! -x "$HARNESS_BIN" ]]; then
    # --no-build, or cargo reported "Fresh" without an Executable line. Fall
    # back to the newest matching binary in the profile's deps dir.
    PROFILE_DIR="$BENCH_PROFILE"
    [[ "$BENCH_PROFILE" == "dev" || "$BENCH_PROFILE" == "debug" ]] && PROFILE_DIR="debug"
    HARNESS_BIN="$(ls -t "${REPO_ROOT}/target/${PROFILE_DIR}/deps/compare_postgres_matrix-"* 2>/dev/null \
        | grep -v '\.d$' | head -1)"
fi

if [[ -z "${HARNESS_BIN:-}" || ! -x "$HARNESS_BIN" ]]; then
    log "ERROR: no compare_postgres_matrix binary found for profile ${BENCH_PROFILE}."
    log "  Re-run without --no-build."
    exit 1
fi
log "harness binary: ${HARNESS_BIN}"

# ---- Postgres tuning -------------------------------------------------------
#
# Durability settings are left at Postgres defaults (fsync=on,
# synchronous_commit=on) — the harness times INSERTs via EXPLAIN ANALYZE,
# whose "Execution Time" excludes the post-statement commit, so relaxing them
# would change little and would make the comparison harder to defend.
#
# Memory settings are NOT left at defaults, deliberately. The stock 128MB
# shared_buffers against a multi-GB `events` table means every Postgres number
# at 1M/10M rows is partly a measurement of a cache too small for the working
# set, while Basin gets its own page cache plus the OS page cache. Raising
# shared_buffers and the checkpoint budget makes Postgres FASTER, i.e. it
# makes Basin's wins harder to claim — the honest direction to err in. The
# harness additionally sets work_mem=64MB per session.
#
# statement_timeout is NOT tuning — it is a correctness fix for the harness,
# and it has to live here because it is a server-side setting.
#
# compare_postgres_matrix.rs bounds every Postgres query with
# `tokio::time::timeout(...)`. That is a CLIENT-side timeout: it drops the
# future, but tokio-postgres sends no CancelRequest, so the backend keeps
# executing. The harness then issues its next statement on the same single
# connection, where it queues behind the query nobody is waiting for any
# more. Observed directly: the correlated-subquery shape at 1M rows blew its
# 20s client budget seven times, and the run then sat for 20+ minutes on the
# NEXT shape while one abandoned `EXPLAIN ANALYZE` ground away server-side
# (confirmed in pg_stat_activity). Without this setting the matrix does not
# hang at some sizes — it hangs at every size where any Postgres shape
# exceeds its budget, which is exactly the interesting ones.
#
# The value must sit ABOVE the harness's largest per-query budget
# (`budget_for` = 10s + rows/100k, clamped to 60s) so it can never abort a
# query the harness would have accepted and turn a real Postgres number into
# a spurious error. 70s clears the 60s ceiling with margin, and bounds an
# abandoned query's damage to ~70s instead of forever.
PG_TUNING=(
    -c "shared_buffers=${PG_SHARED_BUFFERS}"
    -c "max_wal_size=8GB"
    -c "checkpoint_timeout=30min"
    -c "maintenance_work_mem=512MB"
    -c "statement_timeout=70s"
)

if [[ "$USE_DOCKER" == "1" ]]; then
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
    log "starting postgres:18 container on 127.0.0.1:${PGPORT} ..."
    docker run --rm --detach --name "$CONTAINER_NAME" \
        -e POSTGRES_HOST_AUTH_METHOD=trust \
        -e POSTGRES_USER=postgres \
        -p "127.0.0.1:${PGPORT}:5432" \
        postgres:18 \
        postgres "${PG_TUNING[@]}" >/dev/null
    PG_READY=0
    for _ in $(seq 1 90); do
        if docker exec "$CONTAINER_NAME" pg_isready -U postgres -q 2>/dev/null; then
            PG_READY=1
            break
        fi
        sleep 1
    done
else
    log "initdb at ${PGDATA_DIR} ..."
    "${PG_BIN_DIR}/initdb" --auth=trust -U postgres -D "$PGDATA_DIR" -E UTF8 >/dev/null
    log "starting native postgres 18 on 127.0.0.1:${PGPORT} ..."
    "${PG_BIN_DIR}/pg_ctl" -D "$PGDATA_DIR" -l "${PGDATA_DIR}/pg.log" \
        -o "-p ${PGPORT} -c unix_socket_directories= -c listen_addresses=127.0.0.1 ${PG_TUNING[*]}" \
        start >/dev/null
    STARTED=1
    PG_READY=0
    for _ in $(seq 1 60); do
        if "${PG_BIN_DIR}/pg_isready" -h 127.0.0.1 -p "$PGPORT" -q 2>/dev/null; then
            PG_READY=1
            break
        fi
        sleep 1
    done
fi

# The harness SKIPS (prints a note, passes) when Postgres is unreachable. If
# this script let an unready server through, the run would "succeed" with an
# empty matrix — exactly the silent-wrong-measurement failure this benchmark
# exists to avoid. Fail loudly instead.
if [[ "$PG_READY" != "1" ]]; then
    log "ERROR: postgres never became ready on 127.0.0.1:${PGPORT}"
    if [[ "$USE_DOCKER" == "1" ]]; then
        docker logs "$CONTAINER_NAME" 2>&1 | tail -40 >&2 || true
    else
        tail -40 "${PGDATA_DIR}/pg.log" >&2 || true
    fi
    exit 1
fi

log "postgres ready on 127.0.0.1:${PGPORT}"

# ---- Provenance header -----------------------------------------------------

PG_VERSION="$(
    if [[ "$USE_DOCKER" == "1" ]]; then
        docker exec "$CONTAINER_NAME" psql -U postgres -tAc "select version()" 2>/dev/null
    else
        "${PG_BIN_DIR}/psql" "host=127.0.0.1 port=${PGPORT} user=postgres dbname=postgres" \
            -tAc "select version()" 2>/dev/null
    fi
)"

{
    echo "<!--"
    echo "pg_matrix run ${TS}"
    echo "  build profile : ${PROFILE_NOTE}"
    echo "  git rev       : $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
    echo "  git dirty     : $(git status --porcelain 2>/dev/null | wc -l | tr -d ' ') modified/untracked paths"
    # This branch is worked on by several agents at once, so 'git rev' alone
    # does not identify what was measured. Hash the working-tree diff too:
    # two runs with the same pair of values measured the same source.
    echo "  worktree diff : $(git diff HEAD 2>/dev/null | shasum | cut -c1-12)"
    echo "  host          : $(uname -mrs)"
    echo "  cpus          : $(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo '?')"
    echo "  memory        : $(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1024 / 1024 / 1024 )) GiB"
    echo "  postgres      : ${PG_VERSION}"
    echo "  pg tuning     : ${PG_TUNING[*]}"
    echo "  BASIN_OWNED_ENGINE=1 is set by the harness itself."
    echo "-->"
    echo
} > "$OUT_FILE"

# ---- Background-load sampler -----------------------------------------------
#
# Both engines are timed on this one machine, so anything else competing for
# its 8 cores inflates BOTH columns — but not equally, and not predictably.
# A matrix run that shared the box with a cargo build is not comparable to
# one that had it to itself, and there is no way to tell from the numbers
# alone. Sample the 1-minute load average throughout and print the range with
# the results, so a reader can see whether the machine was quiet.
LOAD_LOG="${PGDATA_DIR}/loadavg.txt"
(
    while :; do
        uptime | sed -n 's/.*load averages*: *\([0-9.]*\).*/\1/p' >> "$LOAD_LOG"
        sleep 15
    done
) &
LOAD_SAMPLER_PID=$!

log "running: ${HARNESS_BIN} ${FILTER} (profile ${PROFILE_NOTE})"
log "output -> ${OUT_FILE}"
log "wall-clock guard: ${MAX_SECS}s (a kill produces NO results table — see header comment)"

# Elapsed-time prefix on every line. The harness's own progress lines
# ("seeding rows=N", "rows=N shape=X") only say WHAT it is doing; a long run
# is undiagnosable without knowing WHEN, and the seed-vs-query time split is
# the first thing anyone asks when a matrix takes an hour.
stamp() {
    local t0="$SECONDS"
    while IFS= read -r line; do
        printf '[%6ss] %s\n' "$(( SECONDS - t0 ))" "$line"
    done
}

# macOS ships neither timeout(1) nor gtimeout(1) unless coreutils is
# installed, so the guard falls back to perl (which macOS does ship). The
# child gets its own process group and the whole group is signalled, because
# `cargo test` execs the test binary as a child — TERMing cargo alone would
# leave a multi-GB harness running and still holding the Postgres connection.
TIMEOUT_PL='my $t = shift @ARGV;
my $pid = fork(); die "fork: $!" unless defined $pid;
if ($pid == 0) { setpgrp(0,0); exec @ARGV or die "exec: $!"; }
$SIG{ALRM} = sub { kill("TERM", -$pid); sleep 5; kill("KILL", -$pid); exit 124; };
alarm $t; waitpid($pid, 0); my $rc = $?; alarm 0;
exit($rc & 127 ? 128 + ($rc & 127) : $rc >> 8);'

set +e
if command -v timeout >/dev/null 2>&1; then
    RUNNER=(timeout --foreground "$MAX_SECS")
elif command -v gtimeout >/dev/null 2>&1; then
    RUNNER=(gtimeout --foreground "$MAX_SECS")
elif command -v perl >/dev/null 2>&1; then
    RUNNER=(perl -e "$TIMEOUT_PL" "$MAX_SECS")
else
    RUNNER=()
    log "note: no timeout(1)/gtimeout(1)/perl on PATH — running without the wall-clock guard"
fi

"${RUNNER[@]}" "$HARNESS_BIN" "$FILTER" --nocapture --test-threads=1 \
    2>&1 | stamp | tee -a "$OUT_FILE"
STATUS="${PIPESTATUS[0]}"
kill "$LOAD_SAMPLER_PID" 2>/dev/null || true
set -e

if [[ -s "$LOAD_LOG" ]]; then
    LOAD_SUMMARY="$(sort -g "$LOAD_LOG" | awk '
        {a[NR]=$1}
        END {printf "min %.2f  median %.2f  max %.2f  (%d samples, 15s apart)",
             a[1], a[int((NR+1)/2)], a[NR], NR}')"
    log "1-min load average during run: ${LOAD_SUMMARY}"
    {
        echo
        echo "<!-- machine load during run (1-min avg): ${LOAD_SUMMARY}"
        echo "     On an 8-core box, a median well above ~2 means something else was"
        echo "     competing for the CPU and the absolute milliseconds are inflated. -->"
    } >> "$OUT_FILE"
fi

if [[ "$STATUS" == "124" ]]; then
    log "TIMED OUT after ${MAX_SECS}s — killed. No results table was produced."
    echo -e "\n> **RUN KILLED** after ${MAX_SECS}s by BASIN_BENCH_MAX_SECS. No results table." >> "$OUT_FILE"
elif [[ "$STATUS" != "0" ]]; then
    log "harness exited non-zero (${STATUS}); see ${OUT_FILE}"
fi

log "done (exit ${STATUS}). Results: ${OUT_FILE}"
exit "$STATUS"
