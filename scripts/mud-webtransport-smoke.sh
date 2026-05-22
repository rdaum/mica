#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export MICA_WT_TRACE_SYNC=1
export MICA_DRIVER_TRACE=1
export MICA_TASK_TRACE=1
export MICA_VM_HOST_TRACE=1
export MICA_WT_BUILD_FLAGS="--release"
export MICA_WT_PAGE="${MICA_WT_PAGE:-mud}"
export MICA_WT_FILEINS="${MICA_WT_FILEINS:-apps/shared/sync-host.mica apps/shared/string.mica apps/shared/events.mica apps/mud/core.mica apps/mud/event-substitutions.mica apps/mud/command-parser.mica apps/shared/sync-dom.mica apps/mud/ui-session.mica apps/mud/ui-compose.mica apps/mud/ui-narrative.mica apps/mud/ui-actions.mica apps/mud/http.mica}"

exec "${repo_root}/scripts/webtransport-smoke.sh"
