#!/usr/bin/env bash
# Job "Helm chart": renders the chart with every charts/kafkafim/ci/*-values.yaml and validates
# the manifests against the Kubernetes schemas. Needs helm and kubeconform on the PATH.
#
# KUBECONFORM_CACHE (optional): a directory of schemas that kubeconform reads before it asks the
# network. The GitLab job image carries the cache that the development bundle filled, so the
# job needs no connection; GitHub fetches the schemas.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

shopt -s nullglob
files=(charts/kafkafim/ci/*-values.yaml)
if [[ ${#files[@]} -eq 0 ]]; then
  echo "no charts/kafkafim/ci/*-values.yaml; add at least one" >&2
  exit 1
fi
kc=(kubeconform -strict -summary)
[[ -z "${KUBECONFORM_CACHE:-}" ]] || kc+=(-cache "$KUBECONFORM_CACHE")
kubeconform -v >/dev/null # smoke test only: a go-install build reports "development"
for v in "${files[@]}"; do
  helm template kafkafim charts/kafkafim -f "$v" | "${kc[@]}"
done
