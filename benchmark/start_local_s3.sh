#!/usr/bin/env bash
# Start a local S3-compatible server for Basin's "real" benchmark suite.
#
# Detects whichever S3-compatible binary is on $PATH (MinIO preferred for
# maturity, RustFS supported when available) and runs it bound to
# 127.0.0.1:9000 with a fixed access-key pair that matches `.basin-test.toml`.
#
# The data directory and access keys are deliberately fixed (not random) so
# the .basin-test.toml config can live unchanged across restarts. Data persists
# across runs in `./.basin-local-s3-data/` so you can iterate on benchmarks
# without re-seeding.
#
# Usage:
#   ./benchmark/start_local_s3.sh           # foreground; Ctrl-C to stop
#   ./benchmark/start_local_s3.sh --bg      # background, write PID file
#   ./benchmark/start_local_s3.sh --stop    # stop background instance
#
# Install one of:
#   brew install minio                                 # macOS, recommended
#   cargo install --git https://github.com/rustfs/rustfs   # if you want pure Rust

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${ROOT}/.basin-local-s3-data"
CERTS_DIR="${ROOT}/.basin-local-s3-certs"
PID_FILE="${ROOT}/.basin-local-s3.pid"
ADDR="127.0.0.1:9000"
CONSOLE_ADDR="127.0.0.1:9001"   # MinIO web console
ACCESS_KEY="basinminioadmin"
SECRET_KEY="basinminiosecret123"
BUCKET_NAME="basin-bucket"
# Optional: enable TLS (HTTP/2) for h2 multiplex test. MinIO speaks h2
# only over HTTPS — over plain HTTP it falls back to HTTP/1.1 even
# when the client requests h2 (verified empirically). Toggle by
# exporting BASIN_LOCAL_S3_TLS=1 before invoking this script.
TLS="${BASIN_LOCAL_S3_TLS:-0}"

cmd="${1:-fg}"

case "$cmd" in
  --stop|stop)
    if [[ -f "$PID_FILE" ]]; then
      pid="$(cat "$PID_FILE")"
      if kill -0 "$pid" 2>/dev/null; then
        echo "stopping local S3 (pid $pid)"
        kill "$pid"
        rm -f "$PID_FILE"
      else
        echo "stale pid file (process $pid is gone), removing"
        rm -f "$PID_FILE"
      fi
    else
      echo "no pid file at $PID_FILE — nothing to stop"
    fi
    exit 0
    ;;
esac

mkdir -p "$DATA_DIR"
# MinIO single-drive mode treats top-level subdirs as buckets at startup.
# Pre-create the test bucket so the smoke test doesn't need CreateBucket.
mkdir -p "$DATA_DIR/$BUCKET_NAME"

# Detect server binary. MinIO first because it's mature.
SERVER=""
if command -v minio >/dev/null 2>&1; then
  SERVER="minio"
elif command -v rustfs >/dev/null 2>&1; then
  SERVER="rustfs"
fi

if [[ -z "$SERVER" ]]; then
  cat <<'EOF' >&2
ERROR: no local S3 server found on PATH.

Install one of:
  brew install minio                                          # macOS, fast path
  cargo install --git https://github.com/rustfs/rustfs        # pure Rust alternative

Then re-run this script.
EOF
  exit 1
fi

echo "starting $SERVER on $ADDR (data: $DATA_DIR)"

# Both servers honor MINIO_*-style env vars for the credentials when
# launched in their server modes. We export them generically.
export MINIO_ROOT_USER="$ACCESS_KEY"
export MINIO_ROOT_PASSWORD="$SECRET_KEY"
# RustFS uses RUSTFS_ROOT_USER / RUSTFS_ROOT_PASSWORD per recent releases.
export RUSTFS_ROOT_USER="$ACCESS_KEY"
export RUSTFS_ROOT_PASSWORD="$SECRET_KEY"

run_server() {
  case "$SERVER" in
    minio)
      if [[ "$TLS" == "1" ]]; then
        if [[ ! -f "$CERTS_DIR/public.crt" || ! -f "$CERTS_DIR/private.key" ]]; then
          echo "ERROR: BASIN_LOCAL_S3_TLS=1 but no cert at $CERTS_DIR/{public.crt,private.key}." >&2
          echo "       Generate one with: cd $CERTS_DIR && openssl req -x509 -nodes -days 365 -newkey rsa:2048 \\" >&2
          echo "         -keyout private.key -out public.crt -subj /CN=127.0.0.1 \\" >&2
          echo "         -addext 'subjectAltName=IP:127.0.0.1,DNS:localhost'" >&2
          exit 1
        fi
        exec minio server "$DATA_DIR" \
          --address "$ADDR" \
          --console-address "$CONSOLE_ADDR" \
          --certs-dir "$CERTS_DIR"
      else
        exec minio server "$DATA_DIR" \
          --address "$ADDR" \
          --console-address "$CONSOLE_ADDR"
      fi
      ;;
    rustfs)
      # RustFS CLI surface is in flux; this is the most common shape.
      # If your version differs, edit this line.
      exec rustfs server --address "$ADDR" --data-dir "$DATA_DIR"
      ;;
  esac
}

case "$cmd" in
  --bg|bg)
    if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
      echo "already running (pid $(cat "$PID_FILE"))"
      exit 0
    fi
    nohup bash -c "$(declare -f run_server); run_server" \
      > "${ROOT}/.basin-local-s3.log" 2>&1 &
    echo $! > "$PID_FILE"
    sleep 1
    if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
      echo "ERROR: server died at startup, see ${ROOT}/.basin-local-s3.log" >&2
      rm -f "$PID_FILE"
      exit 1
    fi
    echo "started in background (pid $(cat "$PID_FILE"), log .basin-local-s3.log)"
    echo "endpoint:    http://$ADDR"
    echo "console:     http://$CONSOLE_ADDR"
    echo "access_key:  $ACCESS_KEY"
    echo "secret_key:  $SECRET_KEY"
    echo "bucket:      $BUCKET_NAME (you must create this on first run)"
    ;;
  *)
    echo "endpoint:    http://$ADDR"
    echo "console:     http://$CONSOLE_ADDR"
    echo "access_key:  $ACCESS_KEY"
    echo "secret_key:  $SECRET_KEY"
    echo "bucket:      $BUCKET_NAME (create via console or 'mc mb' on first run)"
    echo "Ctrl-C to stop."
    run_server
    ;;
esac
