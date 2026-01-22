#!/usr/bin/env bash
# 알림 규칙 단위 테스트(promtool). PrometheusRule CRD에서 groups만 뽑아 검증한다.
set -euo pipefail
cd "$(dirname "$0")/.."
"${PYTHON:-python3}" -c "
import yaml; d=yaml.safe_load(open('monitoring/rules/llm-alerts.yaml'))
yaml.safe_dump({'groups': d['spec']['groups']}, open('monitoring/rules/rules.flat.yaml','w'), allow_unicode=True, sort_keys=False)"
docker run --rm -v "$PWD/monitoring/rules:/rules" -w /rules --entrypoint promtool quay.io/prometheus/prometheus:v3.7.3 check rules rules.flat.yaml
docker run --rm -v "$PWD/monitoring/rules:/rules" -w /rules --entrypoint promtool quay.io/prometheus/prometheus:v3.7.3 test rules tests.yaml
