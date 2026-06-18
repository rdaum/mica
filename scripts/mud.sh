#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

default_local_users_json='[{"login":"alice","password":"alice-pass","display_name":"Alice","roles":["operator"]},{"login":"bob","password":"bob-pass","display_name":"Bob","roles":["operator"]}]'

if [[ "${MICA_MUD_SMOKE_TRACE:-}" == "1" ]]; then
  export MICA_WT_LOG_FILTER="${MICA_WT_LOG_FILTER:-info,mica_driver=debug,mica_runtime::task=trace,mica_vm::host=trace,mica_web_host::sync=trace,mica_webtransport_host::sync=trace}"
fi
export MICA_AUTH_SCHEMA_NAMESPACE="${MICA_AUTH_SCHEMA_NAMESPACE:-mud}"
export MICA_AUTH_LOGIN_ACTOR="${MICA_AUTH_LOGIN_ACTOR:-mud/auth_guest}"
export MICA_AUTH_LOCAL_LOGIN_RETURN="${MICA_AUTH_LOCAL_LOGIN_RETURN:-/mud}"
export MICA_AUTH_COOKIE_NAME="${MICA_AUTH_COOKIE_NAME:-mica_session}"
export MICA_AUTH_COOKIE_SECURE="${MICA_AUTH_COOKIE_SECURE:-0}"
export MICA_AUTH_LOCAL_PASSWORD="${MICA_AUTH_LOCAL_PASSWORD:-1}"
if [[ "${MICA_AUTH_LOCAL_PASSWORD}" =~ ^(1|true|TRUE|yes|YES|on|ON)$ && -z "${MICA_AUTH_LOCAL_USERS_JSON:-}" ]]; then
  export MICA_AUTH_LOCAL_USERS_JSON="${default_local_users_json}"
fi

auth_requested="false"
if [[ "${MICA_AUTH_LOCAL_PASSWORD}" =~ ^(1|true|TRUE|yes|YES|on|ON)$ || -n "${MICA_AUTH_GITHUB_CLIENT_ID:-}" ]]; then
  auth_requested="true"
fi
if [[ "${auth_requested}" == "true" && -z "${MICA_AUTH_PASETO_KEY:-}" ]]; then
  if ! command -v openssl >/dev/null 2>&1; then
    echo "missing required command: openssl" >&2
    exit 1
  fi
  auth_state_dir="${MICA_AUTH_STATE_DIR:-${repo_root}/.cache/auth}"
  paseto_key_file="${MICA_AUTH_PASETO_KEY_FILE:-${auth_state_dir}/paseto-v4-local.hex}"
  mkdir -p "$(dirname "${paseto_key_file}")"
  chmod 700 "$(dirname "${paseto_key_file}")"
  if [[ ! -f "${paseto_key_file}" ]]; then
    umask 077
    openssl rand -hex 32 >"${paseto_key_file}"
  fi
  chmod 600 "${paseto_key_file}"
  export MICA_AUTH_PASETO_KEY="$(tr -d '[:space:]' <"${paseto_key_file}")"
fi
if [[ "${auth_requested}" == "true" && ! "${MICA_AUTH_PASETO_KEY}" =~ ^[0-9a-fA-F]{64}$ ]]; then
  echo "MICA_AUTH_PASETO_KEY must be 64 hex characters (32 bytes)" >&2
  exit 1
fi

github_auth_enabled=false
if [[ -n "${MICA_AUTH_GITHUB_CLIENT_ID:-}" ]]; then
  github_auth_enabled=true
fi
local_auth_enabled=false
if [[ "${MICA_AUTH_LOCAL_PASSWORD}" =~ ^(1|true|TRUE|yes|YES|on|ON)$ ]]; then
  local_auth_enabled=true
fi
export MICA_WT_STARTUP_SOURCE="${MICA_WT_STARTUP_SOURCE:-retract mud/RuntimeConfig(#mud/config_local_password_auth, _)
assert mud/RuntimeConfig(#mud/config_local_password_auth, ${local_auth_enabled})
retract mud/RuntimeConfig(#mud/config_github_auth, _)
assert mud/RuntimeConfig(#mud/config_github_auth, ${github_auth_enabled})}"

export MICA_WT_BUILD_FLAGS="--release"
export MICA_WT_PAGE="${MICA_WT_PAGE:-mud}"
export MICA_WT_POLL_MS="${MICA_WT_POLL_MS:-5000}"
export MICA_WT_EMBEDDING_PROVIDER="${MICA_WT_EMBEDDING_PROVIDER:-deterministic}"
export MICA_WT_FILEINS="${MICA_WT_FILEINS:-apps/shared/sync-host.mica apps/shared/string.mica apps/shared/events.mica apps/mud/core.mica apps/mud/auth.mica apps/mud/event-substitutions.mica apps/mud/command-parser.mica apps/shared/retrieval.mica apps/shared/sync-dom.mica apps/mud/ui-session.mica apps/mud/ui-retrieval.mica apps/mud/ui-mica-inspect.mica apps/mud/ui-compose.mica apps/mud/ui-narrative.mica apps/mud/ui-actions.mica apps/mud/http.mica}"

exec "${repo_root}/scripts/chat.sh"
