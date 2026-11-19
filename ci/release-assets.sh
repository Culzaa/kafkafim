#!/usr/bin/env bash
# Release pipeline (release.yml: softprops/action-gh-release): uploads the SBOM, the chart
# package, the alert rules, the dashboards and, when signing is on, the public key to the
# project's generic package registry, and creates the GitLab release for the tag with a link to
# each. Uses the job's own token (CI_JOB_TOKEN). Needs python3.
#
#   ci/release-assets.sh vMAJOR.MINOR.PATCH[-prerelease]
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

tag="${1:?usage: ci/release-assets.sh TAG}"
v="${tag#v}"
files=(
  "dist/kafkafim-$v.sbom.spdx.json"
  "dist/kafkafim-$v.tgz"
  "dist/kafkafim-$v-grafana/alerts/kafkafim-alerts.yaml"
)
for f in "dist/kafkafim-$v-grafana/dashboards/"*.json; do files+=("$f"); done
if [[ "${KAFKAFIM_SIGNING:-cosign}" == cosign ]]; then
  printf '%s\n' "${COSIGN_PUBLIC_KEY:?COSIGN_PUBLIC_KEY is not set}" >"dist/kafkafim-$v.cosign.pub"
  files+=("dist/kafkafim-$v.cosign.pub")
fi
for f in "${files[@]}"; do
  [[ -s "$f" ]] || {
    echo "release asset $f is missing or empty" >&2
    exit 1
  }
done
python3 ci/gitlab-release.py "$tag" "$(cat dist/image.ref)" "$(cat dist/chart.ref)" "${files[@]}"
