#!/usr/bin/env bash
# Release pipeline (release.yml: "Stage alert rules and dashboards"): first, so a missing file
# fails the release before anything is published. runbook_url points at this tag's RUNBOOK, not
# main; the copy of the RUNBOOK travels with the rules.
#
#   ci/release-stage-grafana.sh VERSION [DIST_DIR]
#   RUNBOOK_BLOB_BASE (optional): the repository's "blob" URL prefix up to and including the
#   ref, default /blob/main, rewritten to /blob/v<VERSION>
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

v="${1:?usage: ci/release-stage-grafana.sh VERSION [DIST_DIR]}"
dist="${2:-dist}"
out="$dist/kafkafim-${v}-grafana"
mkdir -p "$out/alerts" "$out/dashboards"
cp deploy/grafana/alerts/kafkafim-alerts.yaml "$out/alerts/"
cp deploy/grafana/dashboards/*.json "$out/dashboards/"
sed "s#/blob/main/docs/RUNBOOK.md#/blob/v${v}/docs/RUNBOOK.md#" "$out/alerts/kafkafim-alerts.yaml" >"$out/alerts/kafkafim-alerts.yaml.new"
mv "$out/alerts/kafkafim-alerts.yaml.new" "$out/alerts/kafkafim-alerts.yaml"
cp docs/RUNBOOK.md "$out/"
