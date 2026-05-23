#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

model="${MICA_VLLM_MODEL:-Qwen/Qwen3-Embedding-0.6B}"
served_model_name="${MICA_VLLM_SERVED_MODEL_NAME:-mud-world}"
port="${MICA_VLLM_PORT:-8000}"
gpu_mem="${MICA_VLLM_GPU_MEM:-0.08}"
max_model_len="${MICA_VLLM_MAX_MODEL_LEN:-512}"
image="${MICA_VLLM_IMAGE:-vllm-spark}"
spark_vllm_dir="${MICA_VLLM_SPARK_DIR:-${repo_root}/.cache/spark-vllm-docker}"
use_fastsafetensors="${MICA_VLLM_USE_FASTSAFETENSORS:-1}"
extra_vllm_args=()

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --model) model="$2"; shift ;;
    --served-model-name) served_model_name="$2"; shift ;;
    --port) port="$2"; shift ;;
    --gpu-mem) gpu_mem="$2"; shift ;;
    --max-model-len) max_model_len="$2"; shift ;;
    --image) image="$2"; shift ;;
    --spark-dir) spark_vllm_dir="$2"; shift ;;
    --no-fastsafetensors) use_fastsafetensors=0 ;;
    --) shift; extra_vllm_args=("$@"); break ;;
    *)
      echo "unknown option: $1" >&2
      echo "usage: $0 [--model MODEL] [--served-model-name NAME] [--port PORT] [--gpu-mem FRAC] [--max-model-len LEN] [--image IMAGE] [--spark-dir DIR] [--no-fastsafetensors] [-- EXTRA_VLLM_ARGS...]" >&2
      exit 1
      ;;
  esac
  shift
done

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

require_command docker
require_command git

if ! docker image inspect "$image" >/dev/null 2>&1; then
  echo "image '$image' not found; building Spark vLLM image"
  if [[ ! -d "$spark_vllm_dir" ]]; then
    git clone https://github.com/eugr/spark-vllm-docker.git "$spark_vllm_dir"
  fi
  (
    cd "$spark_vllm_dir"
    ./build-and-copy.sh --tag "$image"
  )
fi

load_format_flag=()
if [[ "$use_fastsafetensors" == "1" ]]; then
  load_format_flag=(--load-format fastsafetensors)
fi

mount_args=()
serve_model="$model"
if [[ -d "$model" ]]; then
  model_path="$(realpath "$model")"
  mount_args=(-v "$model_path:/model")
  serve_model="/model"
fi

echo "Starting Spark vLLM embeddings server"
echo "  model: $model"
echo "  served model name: $served_model_name"
echo "  port: $port"
echo "  base URL: http://127.0.0.1:${port}/v1"
echo
echo "To point Mica at this server:"
echo "  export MICA_WT_EMBEDDING_PROVIDER=vllm"
echo "  export MICA_VLLM_BASE_URL=http://127.0.0.1:${port}/v1"
echo

docker run --privileged --gpus all -it --rm \
  --network host \
  --ipc=host \
  -e TORCH_FLOAT32_MATMUL_PRECISION=high \
  -v "${HOME}/.cache/huggingface:/root/.cache/huggingface" \
  "${mount_args[@]}" \
  "$image" \
  bash -lc "pip install --quiet git+https://github.com/huggingface/transformers.git && \
    vllm serve '$serve_model' \
      --port '$port' \
      --served-model-name '$served_model_name' \
      ${load_format_flag[*]} \
      --gpu-memory-utilization '$gpu_mem' \
      --max-model-len '$max_model_len' \
      --trust-remote-code \
      ${extra_vllm_args[*]}"
