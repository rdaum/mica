#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output="${MICA_SOURCE_INDEX:-${repo_root}/.cache/source-index/mica-worktree.json}"

cd "${repo_root}"

cargo run ${MICA_SOURCE_BUILD_FLAGS:-} --bin mica -- \
  source-index \
  --root "${MICA_SOURCE_ROOT:-${repo_root}}" \
  --output "${output}"
