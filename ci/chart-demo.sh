#!/usr/bin/env bash
# Job "Helm chart": the local demo's umbrella chart (deploy/local/kafkafim-demo) is
# charts/kafkafim as a subchart plus Kafka, Elasticsearch and Grafana LGTM. Its symlinks into
# charts/ and deploy/grafana must resolve in a plain checkout; the template fails without the
# dashboards. Lints, renders and validates it. Needs helm and kubeconform on the PATH;
# KUBECONFORM_CACHE as in ci/chart-validate.sh.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=ci/lib.sh
source ci/lib.sh

out="$(ci_tmp)/demo.yaml"
kc=(kubeconform -strict -summary)
[[ -z "${KUBECONFORM_CACHE:-}" ]] || kc+=(-cache "$KUBECONFORM_CACHE")
helm lint deploy/local/kafkafim-demo
helm template kafkafim deploy/local/kafkafim-demo -n kafkafim-demo >"$out"
"${kc[@]}" "$out"
for f in deploy/grafana/dashboards/*.json; do
  grep -q "^  $(basename "$f"): " "$out" || {
    echo "$f is not in the demo's dashboards ConfigMap" >&2
    exit 1
  }
done
grep -q 'value: "http://apm-server:8200"' "$out" || {
  echo "kafkafim's OTLP endpoint does not point at the apm-server Service" >&2
  exit 1
}
