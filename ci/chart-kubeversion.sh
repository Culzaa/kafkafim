#!/usr/bin/env bash
# Job "Helm chart": the chart refuses Kubernetes older than 1.27. helm-unittest does not enforce
# Chart.yaml kubeVersion, so assert it with helm itself. Needs helm on the PATH.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

v=charts/kafkafim/ci/minimal-values.yaml
if out=$(helm template kafkafim charts/kafkafim -f "$v" --kube-version 1.26.9 2>&1); then
  echo "kubeVersion is not enforced: helm template succeeded on Kubernetes 1.26.9" >&2
  exit 1
fi
grep -q 'incompatible with Kubernetes' <<<"$out" || {
  echo "unexpected failure on 1.26.9: $out" >&2
  exit 1
}
helm template kafkafim charts/kafkafim -f "$v" --kube-version 1.27.0 >/dev/null
