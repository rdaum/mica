#!/usr/bin/env bash
set -euo pipefail

# Reproducible local HTTP latency comparison for mica-daemon.
#
# Defaults compare the commit before the async driver refactor with the current
# checkout. The benchmark uses isolated git worktrees so the working tree being
# edited is not checked out back and forth.

ROOT="$(git rev-parse --show-toplevel)"
BEFORE_REF="${BEFORE_REF:-335e13a93be6ec17611d88f38066cfd172b02d5b}"
AFTER_REF="${AFTER_REF:-HEAD}"
REQUESTS="${REQUESTS:-200}"
CONCURRENCY="${CONCURRENCY:-20}"
SEQ_REQUESTS="${SEQ_REQUESTS:-50}"
TARGET_PROFILE="${TARGET_PROFILE:-release}"
BASE_PORT="${BASE_PORT:-18180}"
RESULT_DIR="${RESULT_DIR:-$ROOT/scratch/http_baseline}"
WORK_DIR="${WORK_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/mica-http-baseline.XXXXXX")}"
TARGET_DIR="${TARGET_DIR:-$ROOT/target/http-baseline}"

COMMON_FILEINS=(
    examples/string.mica
    examples/events.mica
    examples/mud-core.mica
    examples/event-substitutions.mica
    examples/mud-command-parser.mica
)

cleanup_pids=()
DAEMON_PID=""
cleanup() {
    for pid in "${cleanup_pids[@]:-}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            wait "$pid" 2>/dev/null || true
        fi
    done
    if [[ -d "$WORK_DIR/before" ]]; then
        git -C "$ROOT" worktree remove --force "$WORK_DIR/before" >/dev/null 2>&1 || true
    fi
    if [[ -d "$WORK_DIR/after" ]]; then
        git -C "$ROOT" worktree remove --force "$WORK_DIR/after" >/dev/null 2>&1 || true
    fi
    rmdir "$WORK_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p "$RESULT_DIR"

echo "Preparing worktrees in $WORK_DIR"
git -C "$ROOT" worktree add --detach "$WORK_DIR/before" "$BEFORE_REF" >/dev/null
git -C "$ROOT" worktree add --detach "$WORK_DIR/after" "$AFTER_REF" >/dev/null

build_daemon() {
    local tree="$1"
    local profile_flag=()
    if [[ "$TARGET_PROFILE" == "release" ]]; then
        profile_flag=(--release)
    fi
    echo "Building mica-daemon in $tree ($TARGET_PROFILE)"
    CARGO_TARGET_DIR="$TARGET_DIR" cargo build --manifest-path "$tree/Cargo.toml" --bin mica-daemon "${profile_flag[@]}" >/dev/null
}

daemon_bin() {
    local profile_dir="debug"
    if [[ "$TARGET_PROFILE" == "release" ]]; then
        profile_dir="release"
    fi
    printf '%s/%s/mica-daemon\n' "$TARGET_DIR" "$profile_dir"
}

start_daemon() {
    local tree="$1"
    local port="$2"
    local mode="$3"
    local log_file="$4"
    local filein_args=()

    for filein in "${COMMON_FILEINS[@]}"; do
        filein_args+=(--filein "$tree/$filein")
    done
    case "$mode" in
        core)
            filein_args+=(--filein "$tree/examples/http-core.mica")
            ;;
        router)
            filein_args+=(--filein "$tree/examples/relational-router.mica")
            ;;
        *)
            echo "unknown daemon mode: $mode" >&2
            exit 1
            ;;
    esac

    "$(daemon_bin)" "${filein_args[@]}" --web-bind "127.0.0.1:$port" > "$log_file" 2>&1 &
    local pid=$!
    cleanup_pids+=("$pid")
    for _ in $(seq 1 200); do
        if curl -fs -o /dev/null "http://127.0.0.1:$port/healthz"; then
            DAEMON_PID="$pid"
            return
        fi
        sleep 0.05
    done
    echo "daemon failed to become ready on port $port" >&2
    cat "$log_file" >&2
    exit 1
}

stop_daemon() {
    local pid="$1"
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid"
        wait "$pid" 2>/dev/null || true
    fi
}

measure_url() {
    local label="$1"
    local url="$2"
    local requests="$3"
    local concurrency="$4"
    local log_file="$RESULT_DIR/$label.times"

    rm -f "$log_file"
    if [[ "$concurrency" == "1" ]]; then
        for _ in $(seq 1 "$requests"); do
            curl -o /dev/null -s -w "%{time_total}\n" "$url" >> "$log_file"
        done
    else
        seq "$requests" |
            xargs -P "$concurrency" -I {} \
                curl -o /dev/null -s -w "%{time_total}\n" "$url" \
                >> "$log_file"
    fi
    awk -v label="$label" -v requests="$requests" -v concurrency="$concurrency" '
        {
            sum += $1
            if (n == 0 || $1 < min) min = $1
            if ($1 > max) max = $1
            n++
        }
        END {
            if (n == 0) exit 1
            printf "%s requests=%d concurrency=%d avg_ms=%.3f min_ms=%.3f max_ms=%.3f\n",
                label, requests, concurrency, (sum / n) * 1000, min * 1000, max * 1000
        }
    ' "$log_file"
}

run_case() {
    local label="$1"
    local tree="$2"
    local mode="$3"
    local port="$4"
    start_daemon "$tree" "$port" "$mode" "$RESULT_DIR/$label.daemon.log"
    local pid="$DAEMON_PID"
    measure_url "$label-seq" "http://127.0.0.1:$port/hello" "$SEQ_REQUESTS" 1 | tee "$RESULT_DIR/$label-seq.summary"
    measure_url "$label-load" "http://127.0.0.1:$port/hello" "$REQUESTS" "$CONCURRENCY" | tee "$RESULT_DIR/$label-load.summary"
    if [[ "$mode" == "router" ]]; then
        measure_url "$label-denied" "http://127.0.0.1:$port/admin" "$SEQ_REQUESTS" 1 | tee "$RESULT_DIR/$label-denied.summary"
        measure_url "$label-missing" "http://127.0.0.1:$port/missing" "$SEQ_REQUESTS" 1 | tee "$RESULT_DIR/$label-missing.summary"
    fi
    stop_daemon "$pid"
}

build_daemon "$WORK_DIR/before"
build_daemon "$WORK_DIR/after"

run_case before-core "$WORK_DIR/before" core "$BASE_PORT"
run_case after-core "$WORK_DIR/after" core "$((BASE_PORT + 1))"
if [[ -f "$WORK_DIR/after/examples/relational-router.mica" ]]; then
    run_case after-router "$WORK_DIR/after" router "$((BASE_PORT + 2))"
fi

cat > "$RESULT_DIR/summary.txt" <<EOF
before_ref=$BEFORE_REF
after_ref=$AFTER_REF
profile=$TARGET_PROFILE
sequential_requests=$SEQ_REQUESTS
loaded_requests=$REQUESTS
loaded_concurrency=$CONCURRENCY

$(cat "$RESULT_DIR"/*.summary)
EOF

echo "Summary written to $RESULT_DIR/summary.txt"
