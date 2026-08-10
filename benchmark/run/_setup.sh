#!/usr/bin/env bash
# benchmark/run/_setup.sh
#
# Shared, expensive setup done ONCE for a full parallel benchmark run.
#
#   1. Build ONLY the ~80 bench group binaries (release + per-package
#      debug=0, CARGO_BUILD_JOBS=ncpu-2) once, so no group pays the compile
#      cost on its own and we never build the full ~150-binary suite.
#   2. (seaweedfs only) Start a single local SeaweedFS S3 gateway on
#      127.0.0.1:8333 with the basin-test bucket, shared by every
#      seaweedfs-config group. The s3_* tests namespace every write under
#      `<prefix>/<test_name>/<run_ulid>/` and delete it on teardown, so the
#      one shared instance is safe for concurrent groups (disjoint prefixes).
#   3. Write readiness markers other scripts source.
#
# Idempotent: re-running detects an already-built tree and an already-live
# SeaweedFS and skips that work. SeaweedFS teardown is trap-based and only
# kills an instance THIS script started (records its own PID file marker).
#
# Source it (`. _setup.sh`) to inherit the env exports + the EXIT trap, or
# run it standalone to just prime the build + service. run_all.sh sources it.
#
# ----------------------------------------------------------------------------
# Bench profile model (investigation #97)
# ----------------------------------------------------------------------------
# Three test-config TOMLs cover the realism / cost spectrum:
#
#   .basin-test.toml                  — pure LocalFS  (~0 ms/op,  fastest CI gate)
#   .basin-test.seaweedfs.toml        — SeaweedFS LB  (~1 ms/op,  structural-bug detector)
#   .basin-test.tigris-realistic.toml — LB + 9 ms     (~10 ms/op, Tigris same-region proxy)
#
# The tigris-realistic profile uses the SAME SeaweedFS gateway this script
# already launches; the only difference is `[latency_inject]` in its TOML,
# which makes `BasinTestConfig::build_object_store` wrap the store in
# `basin_storage::LatencyStore`. Per-op delay (9 ms) + loopback (~1 ms) ≈
# the empirical Fly-machine-to-Tigris same-region p50 (5–15 ms).
#
# Switch via BASIN_TEST_CONFIG (or SEAWEED_TEST_CONFIG below):
#
#   BASIN_TEST_CONFIG=./.basin-test.tigris-realistic.toml \
#     cargo test -p basin-integration-tests --test <bench> -- --ignored --nocapture
#
# Loopback SeaweedFS stays as the default `SEAWEED_TEST_CONFIG` because it's
# the cheapest harness; flip the env var to run the realistic-cost sweep.

set -euo pipefail

RUN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "${RUN_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/.." && pwd)"

# --- config knobs (overridable via env) -------------------------------------
SEAWEED_S3_ADDR="${SEAWEED_S3_ADDR:-127.0.0.1:8333}"
SEAWEED_S3_PORT="${SEAWEED_S3_ADDR##*:}"
SEAWEED_VOLUME_PORT="${SEAWEED_VOLUME_PORT:-8082}"
SEAWEED_DATA_DIR="${SEAWEED_DATA_DIR:-${REPO_ROOT}/.basin-seaweedfs-data}"
SEAWEED_S3_CONFIG="${SEAWEED_S3_CONFIG:-${REPO_ROOT}/.basin-seaweedfs-s3.json}"
SEAWEED_BUCKET="${SEAWEED_BUCKET:-basin-test}"
SEAWEED_TEST_CONFIG="${SEAWEED_TEST_CONFIG:-${REPO_ROOT}/.basin-test.seaweedfs.toml}"
SEAWEED_LOG="${SEAWEED_LOG:-${REPO_ROOT}/.basin-seaweedfs-bench.log}"
SEAWEED_PID_FILE="${SEAWEED_PID_FILE:-${REPO_ROOT}/.basin-seaweedfs-bench.pid}"

READY_MARKER_DIR="${RUN_DIR}/.ready"

log() { printf '[setup] %s\n' "$*" >&2; }

# Build profile for the bench/test binaries.
#
# Default: bench-fast (opt-level=2, codegen-units=256, debug=0, strip=symbols
# applied ONLY to the basin-integration-tests package; deps inherit the full
# release profile incl. opt-level=3, so the timed hot paths in engine/storage
# are byte-for-byte release-grade). bench-fast slashes clean build time vs.
# the previous opt-3 / codegen-units=16 release build by parallelising LLVM
# codegen across up to 256 modules per binary.
#
# Override options:
#   BENCH_PROFILE=release   — full opt-level=3 build (original behaviour,
#                             use when you need maximum precision on a
#                             micro-benchmark that is sensitive to opt-3 IR).
#   BENCH_PROFILE=dev       — unoptimised debug build (not recommended for
#                             the multi-million-row scaling tests).
#
# (Note: scaling_noisy_neighbor livelocks in basin_engine::fast_select at
# this HEAD in BOTH profiles — a pre-existing engine bug, unrelated to the
# harness; _group.sh's per-binary timeout isolates it and merge_manifest.py
# omits its card with a logged reason.)
BENCH_PROFILE="${BENCH_PROFILE:-bench-fast}"
case "${BENCH_PROFILE}" in
    release)
        CARGO_PROFILE_FLAG=( --release )
        ;;
    dev)
        CARGO_PROFILE_FLAG=()
        ;;
    *)
        # Named profiles (bench-fast, or any user-defined override) use the
        # --profile <name> flag instead of the --release shorthand.
        CARGO_PROFILE_FLAG=( --profile "${BENCH_PROFILE}" )
        ;;
esac
export BENCH_PROFILE

# --- 1. build the bench test binaries once ----------------------------------
# Build ONLY the binaries the groups will run (via --test), not the whole
# ~150-binary integration suite. Halves the link phase under
# CARGO_BUILD_JOBS=1.
build_test_binaries() {
    # _lib.sh defines the group binary arrays; source it to enumerate them.
    # shellcheck source=_lib.sh
    . "${RUN_DIR}/_lib.sh"
    local bins=(
        "${LOCALFS_VIABILITY[@]}" "${LOCALFS_SCALING[@]}"
        "${LOCALFS_COMPARE[@]}" "${LOCALFS_SERIAL[@]}"
        "${SEAWEEDFS_VIABILITY[@]}" "${SEAWEEDFS_SCALING[@]}"
        "${SEAWEEDFS_COMPARE[@]}" "${SEAWEEDFS_S3[@]}"
    )
    local test_args=() b
    for b in "${bins[@]}"; do
        test_args+=( --test "${b}" )
    done
    # Parallel build. The single biggest lever: at CARGO_BUILD_JOBS=1 the
    # ~80-binary link phase is the entire wall-clock. Default to ncpu-2
    # (≈6 on the 8-core box) — saturates codegen without RAM thrash. Deps
    # sccache-hit (per-package debug=0 in Cargo.toml keeps their cache warm,
    # so only basin-integration-tests itself recompiles).
    local jobs="${CARGO_BUILD_JOBS:-}"
    if [[ -z "${jobs}" ]]; then
        local ncpu; ncpu="$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)"
        jobs=$(( ncpu > 3 ? ncpu - 2 : 1 ))
    fi
    log "building ${#bins[@]} bench binaries (${BENCH_PROFILE}, CARGO_BUILD_JOBS=${jobs}) ..."
    # --message-format=json prints one Cargo build-message per artifact,
    # including the absolute `executable` path for each test binary. We
    # capture those into the bin-resolution map so _group.sh can exec the
    # binaries directly — no per-binary cargo invocation, no target-dir
    # file-lock contention (the real wall-clock killer at concurrency>1).
    local build_json="${READY_MARKER_DIR}/build.jsonl"
    (
        cd "${REPO_ROOT}"
        CARGO_BUILD_JOBS="${jobs}" cargo test -p basin-integration-tests \
            "${test_args[@]}" \
            ${CARGO_PROFILE_FLAG[@]+"${CARGO_PROFILE_FLAG[@]}"} --no-run \
            --message-format=json \
            > "${build_json}" 2> "${READY_MARKER_DIR}/build.err" || {
            log "ERROR: test-binary build failed — see ${READY_MARKER_DIR}/build.err"
            tail -30 "${READY_MARKER_DIR}/build.err" >&2 || true
            exit 1
        }
    )
    # Build the bin→executable map. compiler-artifact records have:
    #   target.kind:["test"], target.name:"<bin_name>", executable:"/path"
    local map="${READY_MARKER_DIR}/bench_bins.tsv"
    : > "${map}"
    if command -v jq >/dev/null 2>&1; then
        jq -r 'select(.reason=="compiler-artifact" and .executable!=null
                      and (.target.kind|index("test"))!=null)
               | "\(.target.name)\t\(.executable)"' \
            "${build_json}" > "${map}"
    else
        # No jq — pure-python fallback (always available on macOS).
        python3 - "${build_json}" "${map}" <<'PY'
import json, sys
src, dst = sys.argv[1], sys.argv[2]
with open(src) as f, open(dst, "w") as out:
    for line in f:
        try:
            r = json.loads(line)
        except Exception:
            continue
        if r.get("reason") != "compiler-artifact": continue
        if not r.get("executable"): continue
        t = r.get("target") or {}
        if "test" not in (t.get("kind") or []): continue
        out.write(f"{t.get('name')}\t{r['executable']}\n")
PY
    fi
    local nbins; nbins="$(wc -l < "${map}" | tr -d ' ')"
    if [[ "${nbins}" -lt "${#bins[@]}" ]]; then
        log "WARN: built ${nbins} executable records, expected ${#bins[@]} (some bins may run from cached binaries — that's fine)"
    fi
    log "build complete (${nbins} executable records in ${map})."
}

# --- 2. SeaweedFS lifecycle -------------------------------------------------
SEAWEED_STARTED_BY_US=0

seaweed_is_up() {
    # A live S3 gateway answers TCP on its port. weed has no cheap /healthz
    # so a plain connect is the most reliable liveness probe.
    nc -z "${SEAWEED_S3_ADDR%:*}" "${SEAWEED_S3_PORT}" >/dev/null 2>&1
}

start_seaweed() {
    if ! command -v weed >/dev/null 2>&1; then
        log "ERROR: 'weed' (SeaweedFS) not on PATH. Install: brew install seaweedfs"
        exit 1
    fi
    if seaweed_is_up; then
        log "SeaweedFS already listening on ${SEAWEED_S3_ADDR} — reusing it."
        return 0
    fi

    mkdir -p "${SEAWEED_DATA_DIR}"
    log "starting SeaweedFS S3 gateway on ${SEAWEED_S3_ADDR} (data: ${SEAWEED_DATA_DIR}) ..."
    # -volume.port overrides the SeaweedFS default of 8080 so it never
    # collides with the control-plane dev server, or anything else holding
    # that very common port (documented in .basin-test.seaweedfs.toml).
    nohup weed server \
        -s3 \
        -ip=127.0.0.1 \
        -dir="${SEAWEED_DATA_DIR}" \
        -s3.port="${SEAWEED_S3_PORT}" \
        -s3.config="${SEAWEED_S3_CONFIG}" \
        -volume.port="${SEAWEED_VOLUME_PORT}" \
        -volume.max=50 \
        > "${SEAWEED_LOG}" 2>&1 &
    local pid=$!
    echo "${pid}" > "${SEAWEED_PID_FILE}"
    SEAWEED_STARTED_BY_US=1

    log "waiting for SeaweedFS S3 to accept connections (up to 60s) ..."
    local waited=0
    until seaweed_is_up; do
        if ! kill -0 "${pid}" 2>/dev/null; then
            log "ERROR: SeaweedFS died at startup, see ${SEAWEED_LOG}"
            tail -20 "${SEAWEED_LOG}" >&2 || true
            exit 1
        fi
        if (( waited >= 60 )); then
            log "ERROR: SeaweedFS S3 not ready within 60s"
            exit 1
        fi
        sleep 1
        (( waited++ )) || true
    done
    # Give the S3 layer a moment to finish wiring up after the port opens.
    sleep 2
    log "SeaweedFS S3 ready (${waited}s)."

    # Ensure the bucket exists (idempotent — create is a no-op if present).
    log "ensuring bucket '${SEAWEED_BUCKET}' exists ..."
    printf 's3.bucket.create -name %s\n' "${SEAWEED_BUCKET}" \
        | weed shell >/dev/null 2>&1 || true
}

stop_seaweed() {
    # Only kill an instance THIS run started. A pre-existing dev SeaweedFS
    # the operator was using is left untouched.
    if [[ "${SEAWEED_STARTED_BY_US}" != "1" ]]; then
        return 0
    fi
    if [[ -f "${SEAWEED_PID_FILE}" ]]; then
        local pid
        pid="$(cat "${SEAWEED_PID_FILE}")"
        if kill -0 "${pid}" 2>/dev/null; then
            log "stopping SeaweedFS (pid ${pid}) ..."
            # weed runs master+volume+s3 in one process; a graceful TERM
            # can take a few seconds, so escalate to KILL if it lingers.
            kill "${pid}" 2>/dev/null || true
            pkill -P "${pid}" 2>/dev/null || true
            local g=0
            while kill -0 "${pid}" 2>/dev/null; do
                if (( g >= 5 )); then
                    kill -9 "${pid}" 2>/dev/null || true
                    break
                fi
                sleep 1
                (( g++ )) || true
            done
            wait "${pid}" 2>/dev/null || true
        fi
        rm -f "${SEAWEED_PID_FILE}"
    fi
    # Belt-and-braces: nothing of ours should still hold the S3 port.
    pkill -9 -f "weed server .*-s3.port=${SEAWEED_S3_PORT}" 2>/dev/null || true
    # Wait for the port to actually free so a re-run can re-bind it.
    local f=0
    while nc -z "${SEAWEED_S3_ADDR%:*}" "${SEAWEED_S3_PORT}" >/dev/null 2>&1; do
        if (( f >= 5 )); then break; fi
        sleep 1
        (( f++ )) || true
    done
}

# Trap so SeaweedFS is always torn down — success, failure, or Ctrl-C —
# but only if we were the ones who started it.
_setup_cleanup() {
    local code=$?
    stop_seaweed
    return $code
}
trap _setup_cleanup EXIT

# --- driver -----------------------------------------------------------------
mkdir -p "${READY_MARKER_DIR}"

do_build="${1:-all}"

build_test_binaries
: > "${READY_MARKER_DIR}/build.ready"

if [[ "${do_build}" == "all" || "${do_build}" == "seaweedfs" ]]; then
    start_seaweed
    : > "${READY_MARKER_DIR}/seaweedfs.ready"
fi

# Exported for the group runners.
export BENCH_REPO_ROOT="${REPO_ROOT}"
export BENCH_RUN_DIR="${RUN_DIR}"
export BENCH_DIR="${BENCH_DIR}"
export BENCH_BIN_MAP="${READY_MARKER_DIR}/bench_bins.tsv"
export SEAWEED_TEST_CONFIG
export SEAWEED_BUCKET

log "setup complete (build=ready$([[ -f "${READY_MARKER_DIR}/seaweedfs.ready" ]] && echo ', seaweedfs=ready'))."
