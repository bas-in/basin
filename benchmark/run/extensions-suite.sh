#!/usr/bin/env bash
# benchmark/run/extensions-suite.sh
#
# EXTENSIONS BENCHMARK SUITE — performance cards for every extension family,
# deliberately SEPARATE from the compare_postgres benchmarks. Own artifact
# family (benchmark/data/ext_bench_<family>.json), own runner.
#
# Runs each family SOLO (one cargo at a time, per the repo's cargo discipline):
#
#   1. ext_bench_trgm        (pg_trgm:     similarity / % / <-> / <% / GUC)
#   2. ext_bench_fts         (fts:         to_tsvector / @@ / ts_rank / headline)
#   3. ext_bench_vector      (pgvector:    <-> / HNSW / recall@10 / filtered kNN)
#   4. ext_bench_postgis     (postgis:     bbox && / ST_DWithin / <-> / PiP)
#   5. ext_bench_ranges      (range types: && / @> / range JOIN)
#   6. ext_bench_timescale   (timescaledb: time_bucket / first-last / retention)
#   7. ext_bench_jsonb_gin   (jsonb GIN:   @> / ? / path / build / write overhead)
#
# Each card is #[ignore]'d (sanctioned run-later pattern: timing-fragile + may
# need a live PG with the matching extension). PG head-to-head is opportunistic:
# every card probes `CREATE EXTENSION IF NOT EXISTS <ext>` at runtime and, on
# failure, records pg_available:false in its artifact and emits Basin-only
# timings — a card NEVER fails just because the local PG lacks an extension.
#
# ## SIZE LADDER (mirrors compare_postgres _10k/_100k/1M)
#
# The core compare_postgres benchmark publishes a 10k/100k/1M ladder so a reader
# can see how Basin-vs-PG scales. This suite now does the same: each family is
# invoked once per meaningful size point (via BASIN_EXT_BENCH_ROWS) and each run
# emits a SIZE-SUFFIXED artifact (ext_bench_<family>_<label>.json). The cards
# themselves are unchanged — they already read BASIN_EXT_BENCH_ROWS and write
# ext_bench_<family>.json; the runner sets the row count per point and routes
# the emitted file to the size-suffixed name (under data_seaweedfs/ in S3 mode).
#
# Sizes are DATA-DRIVEN per family (not a blind 10k/100k/1M everywhere):
#
#   trgm / fts / jsonb_gin / ranges   →  10k / 100k / 1M rows
#   postgis                           →  10k / 100k / 1M geometries
#   vector                            →  (10k / 100k / 1M rows) × (128d / 768d /
#                                        1536d) — the customer-real dims (OpenAI
#                                        text-embedding-3-small=1536, large-as-768
#                                        truncation, sentence-transformers=128/384).
#                                        Label is "<dim>d_<rows>" e.g. 768d_100k.
#   timescale                         →  100k / 1M / 10M time-series points
#                                        (10M behind EXT_BENCH_TS_10M=1, mirroring
#                                        the core 10M smoke opt-in).
#
# Each ladder is env-overridable (degrades to tiny-N for CI smoke):
#
#   BASIN_EXT_BENCH_SIZES        — shared default ladder "10000,100000,1000000"
#   EXT_BENCH_SIZES_TRGM         — per-family override (comma list of row counts)
#   EXT_BENCH_SIZES_FTS
#   EXT_BENCH_SIZES_VECTOR       — rows axis for the vector card
#   EXT_BENCH_VECTOR_DIMS        — dim axis for the vector card "128,768,1536"
#   EXT_BENCH_SIZES_POSTGIS
#   EXT_BENCH_SIZES_RANGES
#   EXT_BENCH_SIZES_TIMESCALE    — default "100000,1000000" (+10M if opt-in)
#   EXT_BENCH_TS_10M=1           — append 10000000 to the timescale ladder
#   EXT_BENCH_FAMILIES           — restrict to a subset (comma list of family
#                                  keys: trgm,fts,vector,postgis,ranges,
#                                  timescale,jsonb_gin). Default: all.
#
# CI smoke (fast; one tiny size point per family, no 1M wall clock):
#   BASIN_EXT_BENCH_SIZES=2000 EXT_BENCH_SIZES_TIMESCALE=2000 \
#     EXT_BENCH_VECTOR_DIMS=128 EXT_BENCH_SIZES_VECTOR=2000 \
#     ./benchmark/run/extensions-suite.sh
#
# Preflight refuses to start if:
#   * another cargo / rustc / leaked test binary is running (the timing cards
#     are wall-clock-sensitive and the workspace shares one target/);
#   * the git tree is dirty (provenance for the published numbers).
#     Regenerated benchmark outputs (benchmark/data*/, RESULTS_*.md,
#     index_*.html, results.js) are exempt — they stay unstaged by convention.
#     ALLOW_DIRTY=1 overrides (harness smoke only; never publish dirty numbers).
#
# RAW output is tee'd (never grep-filtered) to /tmp/basin_ext_bench_<date>.log
# so progress is peekable mid-run:  tail -f /tmp/basin_ext_bench_*.log
#
# Per-family/size PASS/FAIL ledger is printed at the end, plus which runs ended
# up with pg_available:false (parsed from the emitted artifacts). Suite exits
# nonzero if any card fails so CI can gate on it.
#
# ## Store selection (S3 / SeaweedFS mode)
#
# Set STORE=seaweedfs to run all cards against the local SeaweedFS-backed
# object store (reads config from .basin-test.seaweedfs.toml) and route
# size-suffixed artifacts to benchmark/data_seaweedfs/. Requires SeaweedFS to
# be running on 127.0.0.1:8333.
#
#   STORE=seaweedfs ./benchmark/run/extensions-suite.sh
#
# The STORE variable maps to BASIN_BENCH_STORE which the test harness reads.
# Default (STORE unset) is byte-identical to the original local-FS behaviour.

set -uo pipefail

RUN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "${RUN_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/.." && pwd)"

# ── store selection ───────────────────────────────────────────────────────────
# STORE=seaweedfs → BASIN_BENCH_STORE=s3 + artifacts go to benchmark/data_seaweedfs/
# Default (unset) → BASIN_BENCH_STORE=local (byte-identical original behaviour)
STORE="${STORE:-local}"
if [[ "${STORE}" == "seaweedfs" ]]; then
  export BASIN_BENCH_STORE=s3
  export BASIN_TEST_CONFIG="${BASIN_TEST_CONFIG:-${REPO_ROOT}/.basin-test.seaweedfs.toml}"
  DATA_DIR="${BENCH_DIR}/data_seaweedfs"
else
  export BASIN_BENCH_STORE=local
  DATA_DIR="${BENCH_DIR}/data"
fi
# The cards always WRITE to benchmark/data/ (their write_artifact hard-codes it).
# The runner moves the emitted file to the size-suffixed destination under
# DATA_DIR, so S3 mode composes with the size ladder.
EMIT_DIR="${BENCH_DIR}/data"
mkdir -p "${DATA_DIR}" "${EMIT_DIR}"

DATE_TAG="$(date +%Y%m%d_%H%M%S)"
LOG="/tmp/basin_ext_bench_${DATE_TAG}.log"

log() { printf '[ext-suite] %s\n' "$*" | tee -a "${LOG}" >&2; }

# ── size-ladder config ────────────────────────────────────────────────────────
# Shared default ladder; per-family overrides below. Comma-separated row counts.
DEFAULT_SIZES="${BASIN_EXT_BENCH_SIZES:-10000,100000,1000000}"

SIZES_TRGM="${EXT_BENCH_SIZES_TRGM:-${DEFAULT_SIZES}}"
SIZES_FTS="${EXT_BENCH_SIZES_FTS:-${DEFAULT_SIZES}}"
SIZES_POSTGIS="${EXT_BENCH_SIZES_POSTGIS:-${DEFAULT_SIZES}}"
SIZES_RANGES="${EXT_BENCH_SIZES_RANGES:-${DEFAULT_SIZES}}"
SIZES_JSONB="${EXT_BENCH_SIZES_JSONB_GIN:-${DEFAULT_SIZES}}"
SIZES_VECTOR="${EXT_BENCH_SIZES_VECTOR:-${DEFAULT_SIZES}}"
VECTOR_DIMS="${EXT_BENCH_VECTOR_DIMS:-128,768,1536}"

# timescale defaults to a heavier ladder (time-series points), 10M opt-in.
SIZES_TIMESCALE="${EXT_BENCH_SIZES_TIMESCALE:-100000,1000000}"
if [[ "${EXT_BENCH_TS_10M:-0}" == "1" ]]; then
  SIZES_TIMESCALE="${SIZES_TIMESCALE},10000000"
fi

# Optional family restriction (comma list of family keys). Empty → all.
FAMILY_FILTER="${EXT_BENCH_FAMILIES:-}"

# Human label for a row count: 10000→10k, 1000000→1m, else verbatim.
row_label() {
  local n="$1"
  if (( n % 1000000 == 0 )); then echo "$(( n / 1000000 ))m"
  elif (( n % 1000 == 0 )); then echo "$(( n / 1000 ))k"
  else echo "${n}"; fi
}

# family enabled? (respects EXT_BENCH_FAMILIES)
family_enabled() {
  [[ -z "${FAMILY_FILTER}" ]] && return 0
  local key="$1"
  IFS=',' read -ra wanted <<< "${FAMILY_FILTER}"
  for w in "${wanted[@]}"; do [[ "${w// /}" == "${key}" ]] && return 0; done
  return 1
}

# ── preflight: SeaweedFS must be reachable (S3/seaweedfs mode only) ───────────
if [[ "${STORE}" == "seaweedfs" ]]; then
  if ! nc -z 127.0.0.1 8333 2>/dev/null; then
    log "REFUSING to start: STORE=seaweedfs but SeaweedFS is not reachable on 127.0.0.1:8333"
    log "Start SeaweedFS first:  weed server -s3 -ip=127.0.0.1 -dir=./.basin-seaweedfs-data -s3.port=8333 ..."
    exit 2
  fi
  log "SeaweedFS reachable on 127.0.0.1:8333 — using BASIN_BENCH_STORE=s3"
fi

# ── preflight: box must be idle ───────────────────────────────────────────────
# One cargo at a time on this box; timing cards are meaningless under CPU
# contention. Refuse if any cargo/rustc process — or a leaked test binary from a
# previous run — is alive.
busy="$( { pgrep -lx cargo; pgrep -lx rustc; pgrep -lf 'target/[^/]*/deps/'; } 2>/dev/null | sort -u || true )"
if [[ -n "${busy}" ]]; then
  log "REFUSING to start: the box is not idle. Offending processes:"
  printf '%s\n' "${busy}" | tee -a "${LOG}" >&2
  log "(another cargo/rustc/test binary would skew timing and corrupt the shared target/)"
  exit 2
fi

# ── preflight: git tree must be clean ────────────────────────────────────────
# Benchmark outputs are regenerated automatically and stay unstaged.
dirty="$(git -C "${REPO_ROOT}" status --porcelain 2>/dev/null \
  | grep -v -E '^.. benchmark/(data|data_real|data_seaweedfs)/' \
  | grep -v -E '^.. benchmark/RESULTS_' \
  | grep -v -E '^.. benchmark/index_' \
  | grep -v -E 'results\.js$' || true)"
if [[ -n "${dirty}" && "${ALLOW_DIRTY:-0}" != "1" ]]; then
  log "REFUSING to start: git tree is dirty (numbers need a committed provenance):"
  printf '%s\n' "${dirty}" | tee -a "${LOG}" >&2
  log "commit/stash first, or ALLOW_DIRTY=1 for a harness smoke (never publish from dirty)"
  exit 2
fi

# ── provenance header ─────────────────────────────────────────────────────────
{
  echo "================ basin extensions-suite ================"
  echo "started   : $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "host      : $(hostname)"
  echo "commit    : $(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "dirty     : $([[ -n "${dirty}" ]] && echo "YES (ALLOW_DIRTY=1)" || echo no)"
  echo "store     : ${STORE} (BASIN_BENCH_STORE=${BASIN_BENCH_STORE})"
  echo "artifacts : ${DATA_DIR}/  (size-suffixed: ext_bench_<family>_<label>.json)"
  echo "ladders   : trgm=[${SIZES_TRGM}] fts=[${SIZES_FTS}] jsonb=[${SIZES_JSONB}]"
  echo "            ranges=[${SIZES_RANGES}] postgis=[${SIZES_POSTGIS}]"
  echo "            vector rows=[${SIZES_VECTOR}] dims=[${VECTOR_DIMS}]"
  echo "            timescale=[${SIZES_TIMESCALE}]"
  echo "families  : ${FAMILY_FILTER:-all}"
  echo "log       : ${LOG}"
  echo "========================================================"
} | tee -a "${LOG}" >&2

overall_rc=0
declare -a RESULTS=()
declare -a PG_UNAVAIL=()
SUITE_START="$(date +%s)"

# run_card <family-key> <binary> <fn_name> <label> [extra env assignments...]
#
# Runs ONE cargo invocation (solo), then moves the emitted
# benchmark/data/<binary>.json to DATA_DIR/<binary>_<label>.json. Extra args
# are `NAME=value` env assignments exported only for this invocation.
run_card() {
  local family="$1" binary="$2" fn_name="$3" label="$4"; shift 4
  local extra_env=( "$@" )

  log "=== ${family} [${label}] (${binary}::${fn_name}): starting (solo) ==="
  local t_start; t_start="$(date +%s)"

  # Build the per-invocation env prefix. BASIN_EXT_BENCH_SIZE_LABEL is also
  # forwarded so a card adopting ext_size_config picks the same label.
  ( cd "${REPO_ROOT}" && \
      env "${extra_env[@]}" BASIN_EXT_BENCH_SIZE_LABEL="${label}" \
        cargo test -p basin-integration-tests --test "${binary}" \
          -- --ignored --nocapture "${fn_name}" \
  ) 2>&1 | tee -a "${LOG}"
  local rc="${PIPESTATUS[0]}"

  local t_secs=$(( $(date +%s) - t_start ))

  # Route the emitted artifact to the size-suffixed destination (composes with
  # the S3-mode DATA_DIR). The card always writes EMIT_DIR/<binary>.json.
  local emitted="${EMIT_DIR}/${binary}.json"
  local dest="${DATA_DIR}/${binary}_${label}.json"
  if [[ -f "${emitted}" ]]; then
    mv -f "${emitted}" "${dest}"
    log "    artifact → ${dest}"
    if grep -q '"pg_available": false' "${dest}" 2>/dev/null; then
      PG_UNAVAIL+=( "${family}[${label}]" )
    fi
  else
    log "    WARNING: expected artifact ${emitted} not found"
  fi

  if [[ "${rc}" == "0" ]]; then
    RESULTS+=( "${family}[${label}] (${binary}): PASS (${t_secs}s)" )
    log "=== ${family} [${label}]: PASS in ${t_secs}s ==="
  else
    RESULTS+=( "${family}[${label}] (${binary}): FAIL rc=${rc} (${t_secs}s)" )
    log "=== ${family} [${label}]: FAIL (rc=${rc}) in ${t_secs}s — continuing ==="
    overall_rc=1
  fi
}

# ── ladder loops ──────────────────────────────────────────────────────────────
# Scalar-row families: one invocation per row-count point.
run_row_ladder() {
  local family="$1" binary="$2" fn_name="$3" sizes_csv="$4"
  family_enabled "${family}" || { log "=== ${family}: SKIPPED (not in EXT_BENCH_FAMILIES) ==="; return; }
  IFS=',' read -ra sizes <<< "${sizes_csv}"
  for n in "${sizes[@]}"; do
    n="${n// /}"; [[ -z "${n}" ]] && continue
    run_card "${family}" "${binary}" "${fn_name}" "$(row_label "${n}")" \
      "BASIN_EXT_BENCH_ROWS=${n}"
  done
}

run_row_ladder trgm      ext_bench_trgm      ext_bench_trgm      "${SIZES_TRGM}"
run_row_ladder fts       ext_bench_fts       ext_bench_fts       "${SIZES_FTS}"

# vector: dim × rows matrix. Label = "<dim>d_<rowlabel>" e.g. 768d_100k.
if family_enabled vector; then
  IFS=',' read -ra dims <<< "${VECTOR_DIMS}"
  IFS=',' read -ra vsizes <<< "${SIZES_VECTOR}"
  for d in "${dims[@]}"; do
    d="${d// /}"; [[ -z "${d}" ]] && continue
    for n in "${vsizes[@]}"; do
      n="${n// /}"; [[ -z "${n}" ]] && continue
      label="${d}d_$(row_label "${n}")"
      # Drive both the small-dim probe (VEC_DIM) and the big-dim ingest
      # (VEC_BIGDIM) to the same dim so the card measures THIS dim end-to-end;
      # big-dim rows track the row point too.
      run_card vector ext_bench_vector ext_bench_vector "${label}" \
        "BASIN_EXT_BENCH_ROWS=${n}" \
        "BASIN_EXT_BENCH_VEC_DIM=${d}" \
        "BASIN_EXT_BENCH_VEC_BIGDIM=${d}" \
        "BASIN_EXT_BENCH_VEC_BIGDIM_ROWS=${n}"
    done
  done
else
  log "=== vector: SKIPPED (not in EXT_BENCH_FAMILIES) ==="
fi

run_row_ladder postgis   ext_bench_postgis   ext_bench_postgis   "${SIZES_POSTGIS}"
run_row_ladder ranges    ext_bench_ranges    ext_bench_ranges    "${SIZES_RANGES}"
run_row_ladder timescale ext_bench_timescale ext_bench_timescale "${SIZES_TIMESCALE}"
run_row_ladder jsonb_gin ext_bench_jsonb_gin ext_bench_jsonb_gin "${SIZES_JSONB}"

# ── verdict ───────────────────────────────────────────────────────────────────
SUITE_SECS=$(( $(date +%s) - SUITE_START ))
{
  echo
  echo "================ extensions-suite verdict ================"
  for r in "${RESULTS[@]}"; do echo "  ${r}"; done
  echo
  if [[ ${#PG_UNAVAIL[@]} -gt 0 ]]; then
    echo "  pg_available:false (Basin-only) for: ${PG_UNAVAIL[*]}"
  else
    echo "  pg_available:true for every run (or no artifact parsed)"
  fi
  echo
  echo "  total wall-clock : ${SUITE_SECS}s ($(( SUITE_SECS / 60 ))m $(( SUITE_SECS % 60 ))s)"
  echo "  artifacts        : ${DATA_DIR}/ext_bench_<family>_<label>.json"
  echo "                     (e.g. ext_bench_fts_100k.json, ext_bench_vector_768d_1m.json)"
  echo "  raw log          : ${LOG}"
  echo "=========================================================="
} | tee -a "${LOG}"

exit "${overall_rc}"
