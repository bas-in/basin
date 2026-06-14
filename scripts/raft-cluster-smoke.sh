#!/usr/bin/env bash
#
# raft-cluster-smoke.sh — bring up a real 3-node Basin raft cluster on one box
# (three basin-server PROCESSES on distinct ports + data dirs), wait for it to
# be ready, run an end-to-end smoke (write to the leader, read from a follower,
# kill the leader, verify a new leader keeps serving), and tear it down.
#
# This is the "real multi-machine" story compressed onto localhost: each node
# is a separate OS process with its own pgwire port, raft bind port, data dir,
# and WAL dir, talking to its peers over the tonic gRPC raft transport — the
# SAME wiring a 3-machine deployment uses, just with 127.0.0.1 instead of
# private-net IPs. The only thing localhost cannot exercise is a real network
# partition between machines (use the in-process failover drills for that:
# `cargo test -p basin-wal --test raft_failover_drills`).
#
# Usage:
#   scripts/raft-cluster-smoke.sh                 # build (debug) + run
#   BASIN_BIN=/path/to/basin-server scripts/raft-cluster-smoke.sh   # use a prebuilt binary
#   KEEP_UP=1 scripts/raft-cluster-smoke.sh       # leave the cluster running after the smoke
#
# Requires: a `basin-server` built with the `raft-net` feature, and `basinctl`.
# Catalog defaults to in-memory (volatile) — this is a smoke, not a durable
# deployment. mTLS is OFF here (localhost private); see the runbook for the
# BASIN_RAFT_TLS_* setup.
#
# Exit non-zero on any failed step. set -e + explicit checks.

set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/basin-raft-smoke.XXXXXX")"
PG_BASE_PORT="${PG_BASE_PORT:-5433}"      # node N pgwire on PG_BASE_PORT + (N-1)
RAFT_BASE_PORT="${RAFT_BASE_PORT:-6010}"  # node N raft  on RAFT_BASE_PORT + (N-1)
READY_TIMEOUT="${READY_TIMEOUT:-60}"      # seconds to wait for a leader
PIDS=()

log()  { printf '\033[1;34m[smoke]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[smoke FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

cleanup() {
  local code=$?
  if [[ "${KEEP_UP:-0}" == "1" && $code -eq 0 ]]; then
    log "KEEP_UP=1 — leaving cluster running. Data under $WORK_DIR"
    log "pgwire ports: $(seq -s, "$PG_BASE_PORT" "$((PG_BASE_PORT + 2))")"
    return
  fi
  log "tearing down…"
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

pg_port()   { echo "$((PG_BASE_PORT + $1 - 1))"; }
raft_port() { echo "$((RAFT_BASE_PORT + $1 - 1))"; }
pg_url()    { echo "postgres://basin@127.0.0.1:$(pg_port "$1")/basin"; }

PEERS=""
for n in 1 2 3; do
  PEERS+="${n}@127.0.0.1:$(raft_port "$n"),"
done
PEERS="${PEERS%,}"   # strip trailing comma

# ---------------------------------------------------------------------------
# Resolve binaries
# ---------------------------------------------------------------------------
if [[ -z "${BASIN_BIN:-}" ]]; then
  log "building basin-server (--features raft-net) + basinctl…"
  ( cd "$REPO_ROOT" && cargo build -p basin-server --features raft-net -p basinctl )
  BASIN_BIN="$REPO_ROOT/target/debug/basin-server"
fi
BASINCTL_BIN="${BASINCTL_BIN:-$REPO_ROOT/target/debug/basinctl}"
[[ -x "$BASIN_BIN" ]]    || fail "basin-server binary not found/executable: $BASIN_BIN"
[[ -x "$BASINCTL_BIN" ]] || fail "basinctl binary not found/executable: $BASINCTL_BIN"

# ---------------------------------------------------------------------------
# Launch a node. $1 = node id, $2 = "1" to bootstrap.
# ---------------------------------------------------------------------------
start_node() {
  local id="$1" bootstrap="${2:-0}"
  local data_dir="$WORK_DIR/node-$id"
  mkdir -p "$data_dir"
  local logf="$WORK_DIR/node-$id.log"

  log "starting node $id (pgwire $(pg_port "$id"), raft $(raft_port "$id"), bootstrap=$bootstrap)"
  env \
    BASIN_BIND="127.0.0.1:$(pg_port "$id")" \
    BASIN_DATA_DIR="$data_dir/data" \
    BASIN_WAL_DIR="$data_dir/wal" \
    BASIN_CATALOG="memory" \
    BASIN_SHARD_ENABLED=1 \
    BASIN_WAL_MODE=raft \
    BASIN_NODE_ID="$id" \
    BASIN_RAFT_BIND="127.0.0.1:$(raft_port "$id")" \
    BASIN_RAFT_PEERS="$PEERS" \
    BASIN_RAFT_BOOTSTRAP="$([[ "$bootstrap" == "1" ]] && echo 1 || echo 0)" \
    "$BASIN_BIN" > "$logf" 2>&1 &
  PIDS+=("$!")
}

# ---------------------------------------------------------------------------
# Wait until a pgwire endpoint answers SELECT 1.
# ---------------------------------------------------------------------------
wait_ready() {
  local url="$1" deadline=$(( $(date +%s) + READY_TIMEOUT ))
  while (( $(date +%s) < deadline )); do
    if "$BASINCTL_BIN" ping "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# Find the node id that currently accepts a write (the leader). Returns id on
# stdout, or empty if none within the deadline.
find_writable() {
  local deadline=$(( $(date +%s) + READY_TIMEOUT ))
  while (( $(date +%s) < deadline )); do
    for id in 1 2 3; do
      if "$BASINCTL_BIN" query --url "$(pg_url "$id")" \
           "CREATE TABLE IF NOT EXISTS smoke_probe (id INT)" >/dev/null 2>&1; then
        echo "$id"; return 0
      fi
    done
    sleep 1
  done
  return 1
}

# ---------------------------------------------------------------------------
# Bring the cluster up: node 1 bootstraps, nodes 2/3 join.
# ---------------------------------------------------------------------------
start_node 1 1
start_node 2 0
start_node 3 0

log "waiting for all three pgwire listeners…"
for id in 1 2 3; do
  wait_ready "$(pg_url "$id")" || fail "node $id never became ready (see $WORK_DIR/node-$id.log)"
done
log "all three nodes are accept-ready"

# ---------------------------------------------------------------------------
# Smoke: write to the leader, read from a follower.
# ---------------------------------------------------------------------------
leader="$(find_writable)" || fail "no node accepted a write (no leader elected?)"
log "leader is node $leader; writing through it"
"$BASINCTL_BIN" query --url "$(pg_url "$leader")" \
  "CREATE TABLE IF NOT EXISTS smoke (id INT, v TEXT)" >/dev/null
"$BASINCTL_BIN" query --url "$(pg_url "$leader")" \
  "INSERT INTO smoke (id, v) VALUES (1, 'before-failover')" >/dev/null

# Read from a DIFFERENT node (a follower) — replication must have carried it.
follower=$(( leader == 1 ? 2 : 1 ))
log "reading from follower node $follower (expect the replicated row)"
read_deadline=$(( $(date +%s) + 15 ))
got=""
while (( $(date +%s) < read_deadline )); do
  got="$("$BASINCTL_BIN" query --url "$(pg_url "$follower")" \
        "SELECT v FROM smoke WHERE id = 1" 2>/dev/null || true)"
  [[ "$got" == *"before-failover"* ]] && break
  sleep 1
done
[[ "$got" == *"before-failover"* ]] || fail "follower node $follower did not replicate the row (got: $got)"
log "follower replicated the write ✔"

# ---------------------------------------------------------------------------
# Kill the leader; a new leader must take over and keep serving.
# ---------------------------------------------------------------------------
log "killing leader node $leader to force failover"
kill "${PIDS[leader - 1]}" 2>/dev/null || true
# Mark it dead in PIDS so cleanup doesn't double-kill.
PIDS[leader - 1]=""

new_leader="$(find_writable)" || fail "no new leader after killing node $leader (cluster lost quorum?)"
[[ "$new_leader" != "$leader" ]] || fail "leader id unchanged after kill ($new_leader)"
log "new leader is node $new_leader; writing through it"
"$BASINCTL_BIN" query --url "$(pg_url "$new_leader")" \
  "INSERT INTO smoke (id, v) VALUES (2, 'after-failover')" >/dev/null

# Verify both rows are present (the pre-failover row survived the leader loss).
rows="$("$BASINCTL_BIN" query --url "$(pg_url "$new_leader")" \
        "SELECT v FROM smoke ORDER BY id" 2>/dev/null || true)"
[[ "$rows" == *"before-failover"* ]] || fail "pre-failover write LOST across leader loss (rows: $rows)"
[[ "$rows" == *"after-failover"*  ]] || fail "post-failover write not committed (rows: $rows)"

log "PASS — write/replicate/failover/recover all verified."
log "  pre-failover write survived the leader kill (zero acked-write loss)"
log "  new leader (node $new_leader) accepted a fresh write"
