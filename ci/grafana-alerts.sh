#!/usr/bin/env bash
# Job "Helm chart": the Grafana dashboards parse, and the Prometheus alert rules pass promtool's
# rule check and the rule tests next to them. Needs jq and promtool on the PATH.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

shopt -s nullglob
dashboards=(deploy/grafana/dashboards/*.json)
cd deploy/grafana/alerts
tests=(*.test.yaml)
rules=()
for f in *.yaml; do [[ "$f" == *.test.yaml ]] || rules+=("$f"); done
cd - >/dev/null
if [[ ${#dashboards[@]} -eq 0 || ${#rules[@]} -eq 0 || ${#tests[@]} -eq 0 ]]; then
  echo "expected deploy/grafana/dashboards/*.json, deploy/grafana/alerts/*.yaml and *.test.yaml" >&2
  exit 1
fi
jq empty "${dashboards[@]}"
echo "dashboards: ${dashboards[*]} parse"
promtool --version
cd deploy/grafana/alerts # the tests name their rule_files relative to themselves
promtool check rules "${rules[@]}"
promtool test rules "${tests[@]}"
