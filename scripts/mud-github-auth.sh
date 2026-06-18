#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
Usage: scripts/mud-github-auth.sh

Starts the MUD browser fixture with GitHub OAuth enabled.

Required environment:
  MICA_AUTH_ADMIN_GITHUB_LOGIN   GitHub login to grant admin on login
  MICA_AUTH_GITHUB_CLIENT_ID     GitHub OAuth app client id
  MICA_AUTH_GITHUB_CLIENT_SECRET GitHub OAuth app client secret

Access restriction:
  MICA_AUTH_GITHUB_ORG           Optional GitHub org required for login
  MICA_AUTH_GITHUB_ALLOWED_LOGINS
                                  Optional comma/space separated GitHub login allowlist
  MICA_AUTH_GITHUB_DEFAULT_ROLE  Default role for GitHub users, default operator

At least one of MICA_AUTH_GITHUB_ORG or MICA_AUTH_GITHUB_ALLOWED_LOGINS is required.

Optional environment:
  MICA_AUTH_PUBLIC_BASE_URL      Public base URL, default http://localhost:$MICA_WT_HTTP_PORT
  MICA_AUTH_GITHUB_REDIRECT_URI  Overrides OAuth callback URL
  MICA_AUTH_PASETO_KEY           32-byte hex session key
  MICA_AUTH_PASETO_KEY_FILE      Persistent key file, default .cache/auth/paseto-v4-local.hex
  MICA_AUTH_STATE_DIR            Persistent auth state dir, default .cache/auth

Normal MICA_WT_* variables accepted by scripts/mud.sh still apply.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "missing required environment: ${name}" >&2
    exit 1
  fi
}

validate_github_slug() {
  local name="$1"
  local value="$2"
  if [[ ! "${value}" =~ ^[A-Za-z0-9][A-Za-z0-9-]{0,38}$ || "${value}" == *- ]]; then
    echo "${name} must be a GitHub login/org slug" >&2
    exit 1
  fi
}

require_env MICA_AUTH_GITHUB_CLIENT_ID
require_env MICA_AUTH_GITHUB_CLIENT_SECRET
require_env MICA_AUTH_ADMIN_GITHUB_LOGIN

validate_github_slug MICA_AUTH_ADMIN_GITHUB_LOGIN "${MICA_AUTH_ADMIN_GITHUB_LOGIN}"
if [[ -n "${MICA_AUTH_GITHUB_ORG:-}" ]]; then
  validate_github_slug MICA_AUTH_GITHUB_ORG "${MICA_AUTH_GITHUB_ORG}"
fi
if [[ -z "${MICA_AUTH_GITHUB_ORG:-}" && -z "${MICA_AUTH_GITHUB_ALLOWED_LOGINS:-}" ]]; then
  echo "set MICA_AUTH_GITHUB_ORG or MICA_AUTH_GITHUB_ALLOWED_LOGINS" >&2
  exit 1
fi
for login in ${MICA_AUTH_GITHUB_ALLOWED_LOGINS//,/ }; do
  validate_github_slug MICA_AUTH_GITHUB_ALLOWED_LOGINS "${login}"
done

export MICA_WT_HTTP_HOST="${MICA_WT_HTTP_HOST:-127.0.0.1}"
export MICA_WT_HTTP_PORT="${MICA_WT_HTTP_PORT:-8008}"
export MICA_WT_PUBLIC_HOST="${MICA_WT_PUBLIC_HOST:-localhost}"

auth_base_url="${MICA_AUTH_PUBLIC_BASE_URL:-http://${MICA_WT_PUBLIC_HOST}:${MICA_WT_HTTP_PORT}}"
auth_base_url="${auth_base_url%/}"
if [[ ! "${auth_base_url}" =~ ^https?:// ]]; then
  echo "MICA_AUTH_PUBLIC_BASE_URL must start with http:// or https://" >&2
  exit 1
fi
export MICA_AUTH_GITHUB_REDIRECT_URI="${MICA_AUTH_GITHUB_REDIRECT_URI:-${auth_base_url}/auth/callback}"

cat <<EOF
Starting Mica MUD with GitHub OAuth enabled.

GitHub org restriction:
  ${MICA_AUTH_GITHUB_ORG:-none}

GitHub login allowlist:
  ${MICA_AUTH_GITHUB_ALLOWED_LOGINS:-none}

Admin bootstrap login:
  ${MICA_AUTH_ADMIN_GITHUB_LOGIN}

Default GitHub role:
  ${MICA_AUTH_GITHUB_DEFAULT_ROLE:-operator}

OAuth callback URL:
  ${MICA_AUTH_GITHUB_REDIRECT_URI}

EOF

exec "${repo_root}/scripts/mud.sh"
