#!/usr/bin/env bash
# 프로젝트 공통 환경. 호출자가 지정한 클러스터 설정을 유지한다.
export PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export MODELS="${MODELS:-/data/llm/models}"
