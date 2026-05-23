#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

wt_bind="${MICA_SOURCE_WT_BIND:-127.0.0.1:4433}"
wt_url="${MICA_SOURCE_WT_URL:-https://127.0.0.1:4433/view}"
http_host="${MICA_SOURCE_HTTP_HOST:-127.0.0.1}"
http_port="${MICA_SOURCE_HTTP_PORT:-8008}"
cert_path="${MICA_SOURCE_WT_CERT:-/tmp/mica-source-wt-cert.pem}"
key_path="${MICA_SOURCE_WT_KEY:-/tmp/mica-source-wt-key.pem}"
poll_ms="${MICA_SOURCE_POLL_MS:-5000}"
embedding_provider="${MICA_SOURCE_EMBEDDING_PROVIDER:-disabled}"

daemon_pid=""

cleanup() {
  if [[ -n "${daemon_pid}" ]] && kill -0 "${daemon_pid}" 2>/dev/null; then
    kill "${daemon_pid}" 2>/dev/null || true
    wait "${daemon_pid}" 2>/dev/null || true
  fi
}

trap cleanup EXIT INT TERM

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

require_command cargo
require_command openssl
require_command xxd

if [[ "${MICA_SOURCE_TRACE:-}" == "1" ]]; then
  export MICA_WT_TRACE_SYNC=1
  export MICA_DRIVER_TRACE=1
  export MICA_TASK_TRACE=1
  export MICA_VM_HOST_TRACE=1
fi

if [[ ! -f "${cert_path}" || ! -f "${key_path}" ]]; then
  openssl ecparam -name prime256v1 -genkey -noout -out "${key_path}"
  openssl req -new -x509 \
    -key "${key_path}" \
    -out "${cert_path}" \
    -days 7 \
    -subj "/CN=127.0.0.1" \
    -addext "subjectAltName=IP:127.0.0.1"
fi

cert_hash="$(
  openssl x509 -in "${cert_path}" -outform DER \
    | openssl dgst -sha256 -binary \
    | xxd -p -c 256
)"

cd "${repo_root}"

cargo run ${MICA_SOURCE_BUILD_FLAGS:-} --bin mica-daemon -- \
  --filein apps/shared/sync-host.mica \
  --filein apps/shared/sync-dom.mica \
  --filein apps/source/core.mica \
  --filein apps/source/ui-session.mica \
  --filein apps/source/ui-compose.mica \
  --filein apps/source/http.mica \
  --embedding-provider "${embedding_provider}" \
  --web-bind "${http_host}:${http_port}" \
  --webtransport-bind "${wt_bind}" \
  --webtransport-cert "${cert_path}" \
  --webtransport-key "${key_path}" &
daemon_pid=$!

encoded_url="${wt_url//:/%3A}"
encoded_url="${encoded_url//\//%2F}"
default_url="http://${http_host}:${http_port}/source"
webtransport_url="http://${http_host}:${http_port}/source?transport=webtransport&url=${encoded_url}&certHash=${cert_hash}&pollMs=${poll_ms}"

cat <<EOF
Mica source viewer is starting.

Default browser URL (SSE):
  ${default_url}

WebTransport override URL:
  ${webtransport_url}

Manual values:
  SSE sync base: http://${http_host}:${http_port}/sync
  URL: ${wt_url}
  Certificate SHA-256: ${cert_hash}

Press Ctrl-C to stop the daemon.
EOF

wait "${daemon_pid}"
