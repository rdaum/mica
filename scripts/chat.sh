#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

default_public_host() {
  if command -v tailscale >/dev/null 2>&1; then
    local tailscale_ip
    tailscale_ip="$(tailscale ip -4 2>/dev/null | head -n 1 || true)"
    if [[ -n "${tailscale_ip}" ]]; then
      printf '%s\n' "${tailscale_ip}"
      return
    fi
  fi
  hostname -f 2>/dev/null || hostname 2>/dev/null || printf '127.0.0.1\n'
}

public_host="${MICA_WT_PUBLIC_HOST:-$(default_public_host)}"
wt_bind="${MICA_WT_BIND:-0.0.0.0:4433}"
wt_port="${MICA_WT_PORT:-${wt_bind##*:}}"
wt_url="${MICA_WT_URL:-https://${public_host}:${wt_port}/view}"
http_host="${MICA_WT_HTTP_HOST:-0.0.0.0}"
http_port="${MICA_WT_HTTP_PORT:-8008}"
if [[ -n "${MICA_WT_FILEINS:-}" ]]; then
  read -r -a fileins <<< "${MICA_WT_FILEINS}"
elif [[ -n "${MICA_WT_FILEIN:-}" ]]; then
  fileins=("${MICA_WT_FILEIN}")
else
  fileins=(
    apps/shared/sync-host.mica
    apps/chat/sync.mica
    apps/shared/sync-dom.mica
    apps/chat/http.mica
  )
fi
cert_path="${MICA_WT_CERT:-/tmp/mica-wt-cert.pem}"
key_path="${MICA_WT_KEY:-/tmp/mica-wt-key.pem}"
page="${MICA_WT_PAGE:-chat}"
poll_ms="${MICA_WT_POLL_MS:-1000}"
embedding_provider="${MICA_WT_EMBEDDING_PROVIDER:-deterministic}"
log_filter="${MICA_WT_LOG_FILTER:-info}"

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

filein_args=()
for filein in "${fileins[@]}"; do
  filein_args+=(--filein "${filein}")
done
startup_source_args=()
if [[ -n "${MICA_WT_STARTUP_SOURCE:-}" ]]; then
  startup_source_args+=(--startup-source "${MICA_WT_STARTUP_SOURCE}")
fi

cargo run ${MICA_WT_BUILD_FLAGS:-} --bin mica-daemon -- \
  "${filein_args[@]}" \
  "${startup_source_args[@]}" \
  --embedding-provider "${embedding_provider}" \
  --web-bind "${http_host}:${http_port}" \
  --webtransport-bind "${wt_bind}" \
  --webtransport-cert "${cert_path}" \
  --webtransport-key "${key_path}" \
  --log-filter "${log_filter}" &
daemon_pid=$!

encoded_url="${wt_url//:/%3A}"
encoded_url="${encoded_url//\//%2F}"
default_url="http://${public_host}:${http_port}/${page}"
webtransport_url="http://${public_host}:${http_port}/${page}?transport=webtransport&url=${encoded_url}&certHash=${cert_hash}&pollMs=${poll_ms}"

cat <<EOF
Mica browser sync fixture is starting.

Default browser URL (SSE):
  ${default_url}

WebTransport override URL:
  ${webtransport_url}

Manual values:
  HTTP bind: ${http_host}:${http_port}
  WebTransport bind: ${wt_bind}
  SSE sync base: http://${public_host}:${http_port}/sync
  URL: ${wt_url}
  Certificate SHA-256: ${cert_hash}
  Log filter: ${log_filter}

Press Ctrl-C to stop the daemon.
EOF

wait "${daemon_pid}"
