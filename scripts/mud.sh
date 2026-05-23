#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "${MICA_MUD_SMOKE_TRACE:-}" == "1" ]]; then
  export MICA_WT_TRACE_SYNC=1
  export MICA_DRIVER_TRACE=1
  export MICA_TASK_TRACE=1
  export MICA_VM_HOST_TRACE=1
else
  unset MICA_WT_TRACE_SYNC
  unset MICA_DRIVER_TRACE
  unset MICA_TASK_TRACE
  unset MICA_VM_HOST_TRACE
fi
export MICA_WT_BUILD_FLAGS="--release"
export MICA_WT_PAGE="${MICA_WT_PAGE:-mud}"
export MICA_WT_POLL_MS="${MICA_WT_POLL_MS:-5000}"
export MICA_WT_EMBEDDING_PROVIDER="${MICA_WT_EMBEDDING_PROVIDER:-deterministic}"
export MICA_WT_FILEINS="${MICA_WT_FILEINS:-apps/shared/sync-host.mica apps/shared/string.mica apps/shared/events.mica apps/mud/core.mica apps/mud/event-substitutions.mica apps/mud/command-parser.mica apps/shared/retrieval.mica apps/shared/sync-dom.mica apps/mud/ui-session.mica apps/mud/ui-retrieval.mica apps/mud/ui-mica-inspect.mica apps/mud/ui-compose.mica apps/mud/ui-narrative.mica apps/mud/ui-actions.mica apps/mud/http.mica}"

exec "${repo_root}/scripts/chat.sh"
