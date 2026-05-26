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

public_host="${MICA_SOURCE_PUBLIC_HOST:-$(default_public_host)}"
wt_bind="${MICA_SOURCE_WT_BIND:-0.0.0.0:4433}"
wt_port="${MICA_SOURCE_WT_PORT:-${wt_bind##*:}}"
wt_url="${MICA_SOURCE_WT_URL:-https://${public_host}:${wt_port}/view}"
http_host="${MICA_SOURCE_HTTP_HOST:-0.0.0.0}"
http_port="${MICA_SOURCE_HTTP_PORT:-8008}"
cert_path="${MICA_SOURCE_WT_CERT:-/tmp/mica-source-wt-cert.pem}"
key_path="${MICA_SOURCE_WT_KEY:-/tmp/mica-source-wt-key.pem}"
poll_ms="${MICA_SOURCE_POLL_MS:-5000}"
embedding_provider="${MICA_SOURCE_EMBEDDING_PROVIDER:-deterministic}"
dogstatsd_endpoint="${MICA_SOURCE_DOGSTATSD_ENDPOINT-127.0.0.1:8125}"
dogstatsd_interval_secs="${MICA_SOURCE_DOGSTATSD_INTERVAL_SECS:-10}"
build_source_index="${MICA_SOURCE_BUILD_INDEX:-1}"
prewarm_retrieval_index="${MICA_SOURCE_PREWARM_RETRIEVAL_INDEX:-1}"
source_agent_file_context_lines="${MICA_SOURCE_AGENT_FILE_CONTEXT_LINES:-2000}"
source_agent_model="${MICA_SOURCE_AGENT_MODEL:-deepseek/deepseek-v4-pro}"
source_generation_provider="${MICA_SOURCE_GENERATION_PROVIDER:-openrouter}"
source_retrieval_limit="${MICA_SOURCE_RETRIEVAL_LIMIT:-8}"
source_retrieval_model="${MICA_SOURCE_RETRIEVAL_MODEL:-${MICA_VLLM_SERVED_MODEL_NAME:-source-workspace}}"
export MICA_SOURCE_ROOT="${MICA_SOURCE_ROOT:-${repo_root}}"
export MICA_SOURCE_INDEX="${MICA_SOURCE_INDEX:-${repo_root}/.cache/source-index/mica-worktree.json}"

if [[ "${MICA_SOURCE_TRACE:-}" == "1" ]]; then
  log_filter="${MICA_SOURCE_LOG_FILTER:-info,mica_driver=debug,mica_runtime::task=trace,mica_vm::host=trace,mica_web_host::sync=trace,mica_webtransport_host::sync=trace}"
else
  log_filter="${MICA_SOURCE_LOG_FILTER:-info}"
fi

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

mica_string_literal() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '"%s"' "${value}"
}

if [[ ! "${source_retrieval_limit}" =~ ^[0-9]+$ ]]; then
  echo "MICA_SOURCE_RETRIEVAL_LIMIT must be a non-negative integer" >&2
  exit 1
fi

if [[ ! "${source_agent_file_context_lines}" =~ ^[1-9][0-9]*$ ]]; then
  echo "MICA_SOURCE_AGENT_FILE_CONTEXT_LINES must be a positive integer" >&2
  exit 1
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

if [[ "${build_source_index}" != "0" ]]; then
  echo "Building source index: ${MICA_SOURCE_INDEX}"
  cargo run ${MICA_SOURCE_BUILD_FLAGS:---release} --bin mica -- \
    source-index \
    --root "${MICA_SOURCE_ROOT}" \
    --output "${MICA_SOURCE_INDEX}"
fi

daemon_args=(
  --filein apps/shared/sync-host.mica
  --filein apps/shared/sync-dom.mica
  --filein apps/shared/retrieval.mica
  --filein apps/shared/openai.mica
  --filein apps/source/core.mica
  --filein apps/source/retrieval.mica
  --filein apps/source/ui-session.mica
  --filein apps/source/ui-policy.mica
  --filein apps/source/ui-state.mica
  --filein apps/source/ui-actions.mica
  --filein apps/source/ui-sync.mica
  --filein apps/source/ui-compose.mica
  --filein apps/source/ui-navigator.mica
  --filein apps/source/ui-retrieval-panel.mica
  --filein apps/source/ui-agent-panel.mica
  --filein apps/source/ui-code-panel.mica
  --filein apps/source/http.mica
  --embedding-provider "${embedding_provider}"
  --web-bind "${http_host}:${http_port}"
  --webtransport-bind "${wt_bind}"
  --webtransport-cert "${cert_path}"
  --webtransport-key "${key_path}"
  --log-filter "${log_filter}"
)

source_agent_model_literal="$(mica_string_literal "${source_agent_model}")"
source_generation_provider_literal="$(mica_string_literal "${source_generation_provider}")"
source_retrieval_model_literal="$(mica_string_literal "${source_retrieval_model}")"
daemon_args+=(
  --startup-source "retract source/RuntimeConfig(#source/config_agent_model, _)
assert source/RuntimeConfig(#source/config_agent_model, ${source_agent_model_literal})
retract source/RuntimeConfig(#source/config_agent_file_context_line_limit, _)
assert source/RuntimeConfig(#source/config_agent_file_context_line_limit, ${source_agent_file_context_lines})
retract source/RuntimeConfig(#source/config_generation_provider, _)
assert source/RuntimeConfig(#source/config_generation_provider, ${source_generation_provider_literal})
retract source/RuntimeConfig(#source/config_retrieval_limit, _)
assert source/RuntimeConfig(#source/config_retrieval_limit, ${source_retrieval_limit})
retract source/RuntimeConfig(#source/config_retrieval_model, _)
assert source/RuntimeConfig(#source/config_retrieval_model, ${source_retrieval_model_literal})
return {:agent_file_context_lines -> ${source_agent_file_context_lines}, :agent_model -> ${source_agent_model_literal}, :generation_provider -> ${source_generation_provider_literal}, :retrieval_limit -> ${source_retrieval_limit}, :retrieval_model -> ${source_retrieval_model_literal}}"
)

if [[ "${MICA_SOURCE_NO_LOG_ANSI:-}" == "1" ]]; then
  daemon_args+=(--no-log-ansi)
fi

if [[ "${prewarm_retrieval_index}" != "0" ]]; then
  daemon_args+=(
    --startup-source $'let prewarm = spawn :source/run_retrieval_prewarm(#web) after 0\nreturn prewarm'
  )
fi

if [[ -n "${dogstatsd_endpoint}" ]]; then
  daemon_args+=(
    --dogstatsd-endpoint "${dogstatsd_endpoint}"
    --dogstatsd-interval-secs "${dogstatsd_interval_secs}"
  )
fi

cargo run ${MICA_SOURCE_BUILD_FLAGS:---release} --bin mica-daemon -- "${daemon_args[@]}" &
daemon_pid=$!

encoded_url="${wt_url//:/%3A}"
encoded_url="${encoded_url//\//%2F}"
default_url="http://${public_host}:${http_port}/source"
webtransport_url="http://${public_host}:${http_port}/source?transport=webtransport&url=${encoded_url}&certHash=${cert_hash}&pollMs=${poll_ms}"

cat <<EOF
Mica source viewer is starting.

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
  Embedding provider: ${embedding_provider}
  Retrieval model: ${source_retrieval_model}
  Retrieval limit: ${source_retrieval_limit}
  Source index: ${MICA_SOURCE_INDEX}
  Retrieval prewarm: ${prewarm_retrieval_index}
  Source agent file context lines: ${source_agent_file_context_lines}
  Source agent model: ${source_agent_model}
  Source generation provider: ${source_generation_provider}
  DogStatsD endpoint: ${dogstatsd_endpoint:-disabled}
  DogStatsD interval: ${dogstatsd_interval_secs}s
  Log filter: ${log_filter}

Press Ctrl-C to stop the daemon.
EOF

wait "${daemon_pid}"
