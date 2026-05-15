#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rpc_sock="${MICA_RPC_SOCK:-/tmp/mica-host-console-demo.sock}"
rpc_endpoint="ipc://${rpc_sock}"

daemon_pid=""

cleanup() {
  if [[ -n "${daemon_pid}" ]] && kill -0 "${daemon_pid}" 2>/dev/null; then
    kill "${daemon_pid}" 2>/dev/null || true
    wait "${daemon_pid}" 2>/dev/null || true
  fi
  rm -f "${rpc_sock}"
}

trap cleanup EXIT INT TERM

cd "${repo_root}"
rm -f "${rpc_sock}"

cargo run --bin mica-daemon -- --rpc-bind "${rpc_endpoint}" &
daemon_pid=$!

for _ in {1..50}; do
  if [[ -S "${rpc_sock}" ]]; then
    break
  fi
  if ! kill -0 "${daemon_pid}" 2>/dev/null; then
    wait "${daemon_pid}"
  fi
  sleep 0.1
done

if [[ ! -S "${rpc_sock}" ]]; then
  echo "daemon did not create ${rpc_sock}" >&2
  exit 1
fi

cargo run --bin mica-host-console -- --rpc "${rpc_endpoint}" "$@"
