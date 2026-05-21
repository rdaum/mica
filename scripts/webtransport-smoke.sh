#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

wt_bind="${MICA_WT_BIND:-127.0.0.1:4433}"
wt_url="${MICA_WT_URL:-https://127.0.0.1:4433/view}"
http_host="${MICA_WT_HTTP_HOST:-127.0.0.1}"
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

cargo run ${MICA_WT_BUILD_FLAGS:-} --bin mica-daemon -- \
  "${filein_args[@]}" \
  --web-bind "${http_host}:${http_port}" \
  --webtransport-bind "${wt_bind}" \
  --webtransport-cert "${cert_path}" \
  --webtransport-key "${key_path}" &
daemon_pid=$!

encoded_url="${wt_url//:/%3A}"
encoded_url="${encoded_url//\//%2F}"
smoke_url="http://${http_host}:${http_port}/${page}?url=${encoded_url}&certHash=${cert_hash}"

cat <<EOF
Mica WebTransport sync fixture is starting.

Browser URL:
  ${smoke_url}

Manual values:
  URL: ${wt_url}
  Certificate SHA-256: ${cert_hash}

Press Ctrl-C to stop the daemon.
EOF

wait "${daemon_pid}"
