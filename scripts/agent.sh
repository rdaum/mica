#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export MICA_WT_BUILD_FLAGS="${MICA_WT_BUILD_FLAGS:---release}"
export MICA_WT_PAGE="${MICA_WT_PAGE:-agent}"
export MICA_WT_POLL_MS="${MICA_WT_POLL_MS:-5000}"
export MICA_WT_EMBEDDING_PROVIDER="${MICA_WT_EMBEDDING_PROVIDER:-deterministic}"
export MICA_WT_FILEINS="${MICA_WT_FILEINS:-apps/shared/sync-host.mica apps/shared/string.mica apps/shared/events.mica apps/shared/llm.mica apps/agent/core.mica apps/agent/workspaces.mica apps/shared/sync-dom.mica apps/agent/ui-session.mica apps/agent/transcript.mica apps/agent/ui-compose.mica apps/agent/ui-actions.mica apps/agent/http.mica}"
export MICA_WT_LOG_FILTER="${MICA_WT_LOG_FILTER:-info}"
export MICA_SOURCE_ROOTS="${MICA_SOURCE_ROOTS:-${repo_root}}"

exec "${repo_root}/scripts/chat.sh"