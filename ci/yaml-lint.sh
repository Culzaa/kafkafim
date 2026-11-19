#!/usr/bin/env bash
# Job "YAML format": yamllint with .yamllint.yaml (Google has no YAML guide) on the chart
# metadata and values, the Grafana files, the workflows and pipeline, and the compose files.
# charts/*/templates are Helm templates, not YAML. Needs yamllint on the PATH.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

paths=(
  charts/kafkafim/Chart.yaml charts/kafkafim/values.yaml charts/kafkafim/ci
  charts/kafkafim/tests deploy/grafana .github compose.yaml loadtest/compose.yaml
  src/main/resources
)
[[ ! -f .gitlab-ci.yml ]] || paths+=(.gitlab-ci.yml ci/gitlab)
yamllint -s -c .yamllint.yaml "${paths[@]}"
