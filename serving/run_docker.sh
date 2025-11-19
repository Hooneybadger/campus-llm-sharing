#!/usr/bin/env bash
# 소형 다중 모델 Pod를 k8s 밖에서(docker) 띄운다 — P2 반복 실험용. k8s 배포와 같은 이미지·슈퍼바이저·설정을 쓴다.
# 사용: serving/run_docker.sh <profile.yaml> <GPU-UUID> <container-name> [host-port-base]
set -euo pipefail
PROJ=$(cd "$(dirname "$0")/.." && pwd)
PROFILE=$(realpath "$1"); GPU=$2; NAME=$3; BASE=${4:-18100}
UUID=$GPU
IMAGE=${IMAGE_OVERRIDE:-$(python3 -c "import yaml; print(yaml.safe_load(open('$PROJ/models/registry.yaml'))['vllm_image'])")}
CPU_ARGS=()
if [[ -n ${CPUSET:-} ]]; then CPU_ARGS=(--cpuset-cpus "$CPUSET"); fi
docker run -d --name "$NAME" --gpus "device=$UUID" \
  "${CPU_ARGS[@]}" --memory "${MEMORY_LIMIT:-64g}" --ipc=host \
  -p 127.0.0.1:$BASE:8000 -p 127.0.0.1:$((BASE+1)):8001 -p 127.0.0.1:$((BASE+2)):8002 -p 127.0.0.1:$((BASE+3)):8003 \
  -v "${MODELS:-/data/llm/models}":/models:ro -v "${CACHE_DIR:-/data/llm/cache}":/root/.cache \
  -v "$PROJ/serving/multi_vllm.py":/opt/pknu/multi_vllm.py:ro \
  -v "$PROFILE":/etc/pknu/profile.yaml:ro -v "$PROJ/models/registry.yaml":/etc/pknu/registry.yaml:ro \
  --entrypoint python3 "$IMAGE" /opt/pknu/multi_vllm.py /etc/pknu/profile.yaml /etc/pknu/registry.yaml
