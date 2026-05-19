#!/usr/bin/env bash
set -euo pipefail

# Rough local HTTP latency check for mica-daemon's in-process web host.

PORT=8082
LOG_FILE="${LOG_FILE:-scratch/http_latency_results.log}"
DAEMON_LOG="${DAEMON_LOG:-scratch/http_latency_daemon.log}"
CONCURRENCY="${CONCURRENCY:-50}"
REQUESTS="${REQUESTS:-500}"

cleanup() {
    if [[ -n "${DAEMON_PID:-}" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
        kill "$DAEMON_PID"
        wait "$DAEMON_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "Starting mica-daemon on port $PORT..."
cargo build --bin mica-daemon >/dev/null
target/debug/mica-daemon --web-bind 127.0.0.1:"$PORT" > "$DAEMON_LOG" 2>&1 &
DAEMON_PID=$!

for _ in $(seq 1 100); do
    if curl -fs -o /dev/null "http://127.0.0.1:$PORT/healthz"; then
        break
    fi
    sleep 0.05
done

curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz"

echo "Running $REQUESTS requests with concurrency $CONCURRENCY..."
rm -f "$LOG_FILE"
seq "$REQUESTS" |
    xargs -P "$CONCURRENCY" -I {} \
        curl -o /dev/null -s -w "%{time_total}\n" "http://127.0.0.1:$PORT/hello" \
        >> "$LOG_FILE"

echo "Average latency under load:"
awk '{ sum += $1; n++ } END { if (n > 0) printf "%.9f seconds (%.6f ms)\n", sum / n, (sum / n) * 1000; }' "$LOG_FILE"
