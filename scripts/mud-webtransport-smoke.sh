#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export MICA_WT_PAGE="${MICA_WT_PAGE:-mud}"
export MICA_WT_FILEINS="${MICA_WT_FILEINS:-apps/shared/sync-host.mica apps/shared/string.mica apps/shared/events.mica apps/mud/core.mica apps/mud/event-substitutions.mica apps/mud/command-parser.mica apps/shared/sync-dom.mica apps/mud/sync.mica apps/mud/http.mica}"

exec "${repo_root}/scripts/webtransport-smoke.sh"
