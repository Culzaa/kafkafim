#!/usr/bin/env bash
# Release pipeline (release.yml: "Build and test at the tag"): the full build with the tests,
# integration tests included, at the tag. The tag becomes the Maven revision.
#
#   ci/release-verify.sh vMAJOR.MINOR.PATCH[-prerelease]
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

tag="${1:?usage: ci/release-verify.sh TAG}"
./mvnw -B -ntp -Drevision="${tag#v}" verify
