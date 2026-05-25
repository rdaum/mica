#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export MICA_SOURCE_EMBEDDING_PROVIDER="${MICA_SOURCE_EMBEDDING_PROVIDER:-vllm}"
export MICA_VLLM_BASE_URL="${MICA_VLLM_BASE_URL:-http://127.0.0.1:8000/v1}"
export MICA_VLLM_MODEL="${MICA_VLLM_MODEL:-Qwen/Qwen3-Embedding-0.6B}"
export MICA_VLLM_SERVED_MODEL_NAME="${MICA_VLLM_SERVED_MODEL_NAME:-source-workspace}"

cat <<EOF
Source viewer retrieval will use the vLLM embedding provider.

Expected vLLM base URL:
  ${MICA_VLLM_BASE_URL}

Expected served model name:
  ${MICA_VLLM_SERVED_MODEL_NAME}

If needed, start the local embedding server first:
  scripts/vllm-spark.sh --served-model-name "${MICA_VLLM_SERVED_MODEL_NAME}" --model "${MICA_VLLM_MODEL}"
EOF

exec "${repo_root}/scripts/source.sh"
