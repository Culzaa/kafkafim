#!/usr/bin/env bash
# Release pipeline (release.yml: anchore/sbom-action): the SPDX JSON SBOM of the pushed image,
# read from the registry by digest, with the bundled syft. Needs syft.
#
#   ci/release-sbom.sh VERSION
# KAFKAFIM_RELEASE_INSECURE=true: the registry speaks plain HTTP.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

v="${1:?usage: ci/release-sbom.sh VERSION}"
ref="$(cat dist/image.ref)"
[[ "${KAFKAFIM_RELEASE_INSECURE:-}" != true ]] || export SYFT_REGISTRY_INSECURE_USE_HTTP=true
syft "registry:$ref" -o "spdx-json=dist/kafkafim-$v.sbom.spdx.json"
