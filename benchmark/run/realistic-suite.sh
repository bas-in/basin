#!/usr/bin/env bash
# benchmark/run/realistic-suite.sh
#
# Realistic-conditions benchmark suite — runs the ten new PG-comparison cards
# in order, SOLO (one cargo at a time, per repo cargo discipline).
#
# ## Store selection (S3 / SeaweedFS mode) — compare_postgres_ext_* cards only
#
# The `compare_postgres_ext_*` cards (ext_scalar, ext_structural, ext_dml) support
# BASIN_BENCH_STORE=s3 to build the Basin engine over SeaweedFS and route artifacts
# to benchmark/data_seaweedfs/. Set the env var before invoking:
#
#   BASIN_BENCH_STORE=s3 \
#   BASIN_TEST_CONFIG=./.basin-test.seaweedfs.toml \
#     ./benchmark/run/realistic-suite.sh
#
# Check SeaweedFS is up first:  nc -z 127.0.0.1 8333
# The remaining cards (soak, interference, shape_sweeps, fts) are LocalFS-only
# and ignore BASIN_BENCH_STORE.
#
#   realistic_conditions:
#     1. soak_latency_stability        (2+ min sustained mixed RW)
#     2. background_interference       (quiesced vs compaction vs bulk-ingest)
#     8. n_plus_one_storm              (ORM lazy-load anti-pattern)
#     9. read_scaling_curve            (1/4/16/64 sessions, QPS curve)
#
#   ingest_compare:
#     3. copy_ingest_compare           (COPY text/csv/binary vs INSERT VALUES)
#     7. durability_modes              (sync vs async commit overhead)
#
#   shape_sweeps:
#     4. in_list_sweep                 (IN(10)/IN(100)/IN(1000) vs PG)
#     5. wide_row_sweep                (6-col vs 50-col; projection pushdown)
#    10. pagination_sweep              (OFFSET 100/10k/100k vs keyset at 1M)
#
#   fts_compare:
#     6. fts_head_to_head              (FTS @@ + ts_rank vs PG GIN)
#
# Preflight refuses to start if:
#   * another cargo / rustc / leaked test binary is running (timing-sensitive;
#     shared target/);
#   * the git tree is dirty (provenance for published numbers).
#     Regenerated benchmark outputs (benchmark/data*/, RESULTS_*.md,
#     index_*.html, results.js) are exempt — they stay unstaged by convention.
#     ALLOW_DIRTY=1 overrides (harness smoke only; never publish dirty numbers).
#
# RAW output is tee'd (never grep-filtered) to /tmp/basin_realistic_<date>.log
# so progress is peekable mid-run:  tail -f /tmp/basin_realistic_*.log
#
# Per-card PASS/FAIL ledger is printed at the end. Suite exits nonzero if any
# card fails so CI can gate on it.
#
# Env knobs (forwarded to the tests; see each test's module header):
#   BASIN_SOAK_SECS / BASIN_SOAK_SEED_ROWS
#   BASIN_INTERF_SEED_ROWS
#   BASIN_N1_PARENTS
#   BASIN_SCALE_WINDOW_SECS
#   BASIN_COPY_ROWS / BASIN_COPY_CHUNK
#   BASIN_DURABILITY_N
#   BASIN_IN_LIST_SAMPLES
#   BASIN_WIDE_ROWS / BASIN_WIDE_SAMPLES
#   BASIN_PAGI_SAMPLES
#   BASIN_FTS_ROWS / BASIN_FTS_SAMPLES
#
# Tiny-N harness smoke (fast, avoids full soak time):
#   BASIN_SOAK_SECS=30 BASIN_SOAK_SEED_ROWS=10000 \
#   BASIN_INTERF_SEED_ROWS=5000 \
#   BASIN_COPY_ROWS=50000 BASIN_COPY_CHUNK=10000 \
#   BASIN_WIDE_ROWS=5000 BASIN_FTS_ROWS=5000 \
#   ./benchmark/run/realistic-suite.sh

set -uo pipefail

RUN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "${RUN_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/.." && pwd)"

DATE_TAG="$(date +%Y%m%d_%H%M%S)"
LOG="/tmp/basin_realistic_${DATE_TAG}.log"

log() { printf '[realistic-suite] %s\n' "$*" | tee -a "${LOG}" >&2; }

# ── preflight: box must be idle ───────────────────────────────────────────────
# One cargo at a time on this box; timing cards are meaningless under CPU
# contention. Refuse if any cargo/rustc process — or a leaked test binary
# from a previous run — is alive.
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
  echo "================ basin realistic-suite ================"
  echo "started   : $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "host      : $(hostname)"
  echo "commit    : $(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "dirty     : $([[ -n "${dirty}" ]] && echo "YES (ALLOW_DIRTY=1)" || echo no)"
  echo "log       : ${LOG}"
  echo "======================================================="
} | tee -a "${LOG}" >&2

# ── card table ────────────────────────────────────────────────────────────────
# Each entry: "<binary>/<test_fn_name>/<card_number>"
# The binary name is the --test argument; the function name is passed to
# cargo test as a filter (last positional after --).
declare -a CARDS=(
  "realistic_conditions/soak_latency_stability/1"
  "realistic_conditions/background_interference/2"
  "realistic_conditions/n_plus_one_storm/8"
  "realistic_conditions/read_scaling_curve/9"
  "ingest_compare/copy_ingest_compare/3"
  "ingest_compare/durability_modes/7"
  "shape_sweeps/in_list_sweep/4"
  "shape_sweeps/wide_row_sweep/5"
  "shape_sweeps/pagination_sweep/10"
  "fts_compare/fts_head_to_head/6"
)

overall_rc=0
declare -a RESULTS=()
SUITE_START="$(date +%s)"

for card_entry in "${CARDS[@]}"; do
  # Parse "<binary>/<fn>/<num>"
  IFS='/' read -r binary fn_name card_num <<< "${card_entry}"

  log "=== Card #${card_num} (${binary}::${fn_name}): starting (solo) ==="
  t_start="$(date +%s)"

  # Run with --ignored (all cards are #[ignore]'d) and filter to the specific
  # test function so only this card runs in its binary.
  # RAW output goes through tee — never filtered — so mid-run progress is
  # peekable with:  tail -f "${LOG}"
  ( cd "${REPO_ROOT}" && \
      cargo test -p basin-integration-tests --test "${binary}" \
        -- --ignored --nocapture "${fn_name}" \
  ) 2>&1 | tee -a "${LOG}"
  rc="${PIPESTATUS[0]}"

  t_secs=$(( $(date +%s) - t_start ))

  if [[ "${rc}" == "0" ]]; then
    RESULTS+=( "Card #${card_num} ${binary}::${fn_name}: PASS (${t_secs}s)" )
    log "=== Card #${card_num}: PASS in ${t_secs}s ==="
  else
    RESULTS+=( "Card #${card_num} ${binary}::${fn_name}: FAIL rc=${rc} (${t_secs}s)" )
    log "=== Card #${card_num}: FAIL (rc=${rc}) in ${t_secs}s — continuing to next card ==="
    overall_rc=1
  fi
done

# ── verdict ───────────────────────────────────────────────────────────────────
SUITE_SECS=$(( $(date +%s) - SUITE_START ))
{
  echo
  echo "================ realistic-suite verdict ================"
  for r in "${RESULTS[@]}"; do echo "  ${r}"; done
  echo
  echo "  total wall-clock : ${SUITE_SECS}s ($(( SUITE_SECS / 60 ))m $(( SUITE_SECS % 60 ))s)"
  echo "  artifacts        : ${BENCH_DIR}/data/{soak_latency_stability,background_interference,"
  echo "                       n_plus_one_storm,read_scaling_curve,copy_ingest_compare,"
  echo "                       durability_modes,in_list_sweep,wide_row_sweep,"
  echo "                       pagination_sweep,fts_head_to_head}.json"
  echo "  raw log          : ${LOG}"
  echo "=========================================================="
} | tee -a "${LOG}"

exit "${overall_rc}"
