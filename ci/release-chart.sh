#!/usr/bin/env bash
# Release pipeline (release.yml: "Package, push and sign chart"): packages charts/kafkafim at
# the tag's version, pushes it to the OCI chart repository and records the pushed reference,
# repository@digest, in dist/chart.ref for ci/release-sign.sh chart. Needs helm.
#
#   ci/release-chart.sh VERSION
# KAFKAFIM_RELEASE_CHART_REPO: oci://host/path (default oci://$CI_REGISTRY_IMAGE/charts);
# KAFKAFIM_RELEASE_INSECURE=true: the registry speaks plain HTTP.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

v="${1:?usage: ci/release-chart.sh VERSION}"
repo="${KAFKAFIM_RELEASE_CHART_REPO:?KAFKAFIM_RELEASE_CHART_REPO is not set}"
[[ -f charts/kafkafim/Chart.yaml ]] || {
  echo "no charts/kafkafim/Chart.yaml; nothing to package" >&2
  exit 1
}
args=()
[[ "${KAFKAFIM_RELEASE_INSECURE:-}" != true ]] || args+=(--plain-http)
mkdir -p dist
helm package charts/kafkafim --version "$v" --app-version "$v" -d dist
helm push "dist/kafkafim-$v.tgz" "$repo" "${args[@]}" 2>&1 | tee dist/chart-push.log
digest="$(grep -o 'sha256:[a-f0-9]*' dist/chart-push.log | head -n 1)"
[[ -n "$digest" ]] || {
  echo "no digest in the helm push output" >&2
  exit 1
}
echo "${repo#oci://}/kafkafim@$digest" >dist/chart.ref
