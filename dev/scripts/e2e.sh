#!/usr/bin/env bash
# dev/scripts/e2e.sh — run the full E2E suite via basin-e2e-runner.
#
# Builds the runner (if needed), then runs all three workload modes:
#   perf            point/range/insert/update latency measurements
#   noisy-neighbor  2-tenant adversarial fairness check
#   handoff         kill replica-0, assert replica-1 picks up within budget
#
# Usage:
#   bash dev/scripts/e2e.sh [--workload perf|noisy-neighbor|handoff|all]
#                           [--replicas N]
#                           [--host H] [--port P] [--user U]
#                           [--out-dir DIR]
#
# Defaults:
#   workload = all
#   replicas = 1  (handoff requires --replicas 2)
#   host     = 127.0.0.1
#   port     = $BASIN_PORT_BASE (5533)
#   out-dir  = dev/results

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$DEV_DIR/.." && pwd)"

WORKLOAD="all"
REPLICAS=1
HOST="127.0.0.1"
PORT="${BASIN_PORT_BASE:-5533}"
PORT_REPLICA1="${BASIN_PORT_REPLICA1:-5534}"
USER_ALICE="alice"
USER_BOB="bob"
OUT_DIR="$DEV_DIR/results"

while [[ $# -gt 0 ]]; do
  case $1 in
    --workload)  WORKLOAD="$2";  shift 2 ;;
    --replicas)  REPLICAS="$2";  shift 2 ;;
    --host)      HOST="$2";      shift 2 ;;
    --port)      PORT="$2";      shift 2 ;;
    --user)      USER_ALICE="$2"; shift 2 ;;
    --out-dir)   OUT_DIR="$2";   shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ── Build basin-e2e-runner if not already built ───────────────────────────────
RUNNER_BIN="$REPO_ROOT/target/release/basin-e2e-runner"
if [[ ! -x "$RUNNER_BIN" ]] || [[ "$REPO_ROOT/services/basin-e2e-runner/src/main.rs" -nt "$RUNNER_BIN" ]]; then
  echo "==> Building basin-e2e-runner..."
  cargo build --release -p basin-e2e-runner --manifest-path "$REPO_ROOT/Cargo.toml"
fi

mkdir -p "$OUT_DIR"

TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT_FILE="$OUT_DIR/${TIMESTAMP}.json"

echo "==> E2E runner starting (workload=$WORKLOAD, replicas=$REPLICAS)"
echo "    output: $OUT_FILE"
echo ""

run_workload() {
  local w="$1"
  echo "--> workload: $w"
  "$RUNNER_BIN" \
    --workload "$w" \
    --host "$HOST" \
    --port "$PORT" \
    --port-replica1 "$PORT_REPLICA1" \
    --user-alice "$USER_ALICE" \
    --user-bob "$USER_BOB" \
    --replicas "$REPLICAS" \
    --out "$OUT_FILE"
  echo ""
}

if [[ "$WORKLOAD" == "all" ]]; then
  run_workload "perf"
  run_workload "noisy-neighbor"
  if [[ "$REPLICAS" -ge 2 ]]; then
    run_workload "handoff"
  else
    echo "[SKIP] handoff workload requires --replicas 2 (stack only has $REPLICAS replica)"
  fi
else
  run_workload "$WORKLOAD"
fi

echo "==> E2E complete. Results written to: $OUT_FILE"

# Print a human-readable pass/fail summary from the JSON output.
if command -v jq >/dev/null 2>&1; then
  echo ""
  echo "── Summary ─────────────────────────────────────────────────────────────"
  jq -r '
    .workloads[] |
    "  [\(if .passed then "PASS" else "FAIL" end)] \(.name)" +
    (if .error then " — \(.error)" else "" end)
  ' "$OUT_FILE" 2>/dev/null || true

  TOTAL=$(jq '[.workloads[]] | length' "$OUT_FILE" 2>/dev/null || echo "?")
  PASSED=$(jq '[.workloads[] | select(.passed)] | length' "$OUT_FILE" 2>/dev/null || echo "?")
  echo "  $PASSED/$TOTAL workloads passed"
  echo "────────────────────────────────────────────────────────────────────────"
fi
