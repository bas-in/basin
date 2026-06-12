#!/usr/bin/env bash
# Live-ORM compatibility harness runner.
#
# Boots a real basin-server (unless BASIN_DSN is already exported), waits for
# pgwire, runs each ORM suite (its OWN migration engine / query builder / tx
# manager — no recorded SQL), parses the TAP output into a scorecard, and
# compares against baseline.json:
#
#   * baseline "pass"  + test fails  -> REGRESSION (exit nonzero)
#   * baseline "xfail" + test fails  -> expected (known gap, named)
#   * baseline "xfail" + test passes -> XPASS (progress!  re-baseline)
#   * not in baseline  + test fails  -> NEW-FAIL (exit nonzero — add a
#                                       baseline entry naming the gap)
#
# Usage:
#   ./run-all.sh                      # full run, all available toolchains
#   ./run-all.sh --only=django        # one suite
#   ./run-all.sh --update-baseline    # rewrite baseline.json from observed
#   BASIN_SERVER_BIN=/path/basin-server ./run-all.sh
#   BASIN_DSN=postgres://... ./run-all.sh   # use an already-running server
#
# Suites whose toolchain (node / python3 / go) is missing are SKIPPED, never
# silently passed.

set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
RESULTS_DIR="$ROOT/results"
BASELINE="$ROOT/baseline.json"
PORT="${BASIN_PORT:-54329}"
UPDATE_BASELINE=0
ONLY=""

for arg in "$@"; do
  case "$arg" in
    --update-baseline) UPDATE_BASELINE=1 ;;
    --only=*) ONLY="${arg#--only=}" ;;
    -h|--help) sed -n '2,24p' "$0"; exit 0 ;;
    *) echo "unknown arg: $arg (try --help)" >&2; exit 2 ;;
  esac
done

command -v python3 >/dev/null 2>&1 || {
  echo "[run-all] python3 is required for the scorecard" >&2; exit 2; }

mkdir -p "$RESULTS_DIR"
rm -f "$RESULTS_DIR"/*.tap "$RESULTS_DIR"/*.log "$RESULTS_DIR"/*.skipped 2>/dev/null

# ── Server ───────────────────────────────────────────────────────────────────
SERVER_PID=""
if [ -z "${BASIN_DSN:-}" ]; then
  BIN="${BASIN_SERVER_BIN:-$REPO_ROOT/target/release/basin-server}"
  if [ ! -x "$BIN" ]; then
    echo "[run-all] basin-server not found at $BIN" >&2
    echo "          build it:  cargo build --release -p basin-server" >&2
    echo "          or point:  BASIN_SERVER_BIN=/path/to/basin-server" >&2
    exit 2
  fi
  DATA_DIR="$(mktemp -d -t basin-orm-live.XXXXXX)"
  echo "[run-all] starting $BIN on 127.0.0.1:$PORT (data: $DATA_DIR)"
  BASIN_DATA_DIR="$DATA_DIR" \
  BASIN_BIND="127.0.0.1:$PORT" \
  BASIN_PROJECTS="basin=*" \
    "$BIN" >"$RESULTS_DIR/server.log" 2>&1 &
  SERVER_PID=$!
  # shellcheck disable=SC2064
  trap "kill $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null" EXIT

  READY=0
  for _ in $(seq 1 120); do                      # up to 60 s
    if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
      exec 3>&- 3<&- 2>/dev/null
      READY=1; break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      echo "[run-all] server exited before binding; last log lines:" >&2
      tail -n 20 "$RESULTS_DIR/server.log" >&2
      exit 2
    fi
    sleep 0.5
  done
  if [ "$READY" != 1 ]; then
    echo "[run-all] pgwire never became ready on :$PORT" >&2
    tail -n 20 "$RESULTS_DIR/server.log" >&2
    exit 2
  fi
  export BASIN_DSN="postgres://basin:basin@127.0.0.1:$PORT/basin?sslmode=disable"
  echo "[run-all] pgwire ready"
else
  echo "[run-all] using externally provided BASIN_DSN (no server launched)"
fi
echo "[run-all] BASIN_DSN=$BASIN_DSN"

# ── Suites ───────────────────────────────────────────────────────────────────
run_suite() {
  local name="$1" tool="$2"
  if [ -n "$ONLY" ] && [ "$name" != "$ONLY" ]; then
    echo "excluded by --only=$ONLY" > "$RESULTS_DIR/$name.skipped"; return
  fi
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "[run-all] SKIP $name — $tool not installed"
    echo "$tool not installed" > "$RESULTS_DIR/$name.skipped"; return
  fi
  echo ""
  echo "[run-all] ── $name ──────────────────────────────────────────────"
  # TAP to stdout AND the .tap file (peekable mid-run); tool noise to .log.
  ( cd "$ROOT/$name" && bash run.sh 2>"$RESULTS_DIR/$name.log" ) | tee "$RESULTS_DIR/$name.tap"
}

run_suite prisma     node
run_suite drizzle    node
run_suite typeorm    node
run_suite django     python3
run_suite sqlalchemy python3
run_suite gorm       go

# ── Scorecard ────────────────────────────────────────────────────────────────
echo ""
SC_ARGS=(--results "$RESULTS_DIR" --baseline "$BASELINE")
[ "$UPDATE_BASELINE" = 1 ] && SC_ARGS+=(--update-baseline)
python3 "$ROOT/scorecard.py" "${SC_ARGS[@]}"
