#!/usr/bin/env bash
# scripts/deploy.sh target [build|diff|apply]
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh
if [[ ${1:-} != target || ! ${2:-build} =~ ^(build|diff|apply)$ ]]; then
  echo "Usage: $0 target [build|diff|apply]" >&2
  exit 2
fi
render() { kubectl kustomize --load-restrictor LoadRestrictionsNone infra/k8s/overlays/target; }
case "${2:-build}" in
  build) render ;;
  diff) render | kubectl diff -f - ;;
  apply) render | kubectl apply -f - ;;
esac
