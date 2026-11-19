#!/bin/sh
# Release pipeline, first job (release.yml: "Validate the tag"): a bad tag fails before any test
# runs. The tag becomes the Maven revision, the image tags and the chart version, and releases
# are cut from main only. POSIX sh and git only: the job runs in the bundled docker image.
#
#   ci/release-validate-tag.sh TAG COMMIT DEFAULT_BRANCH
set -eu

tag="${1:?usage: ci/release-validate-tag.sh TAG COMMIT DEFAULT_BRANCH}"
sha="${2:?missing COMMIT}"
branch="${3:?missing DEFAULT_BRANCH}"
if ! printf '%s\n' "$tag" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$'; then
  echo "ERROR: tag '$tag' is not vMAJOR.MINOR.PATCH[-prerelease]" >&2
  exit 1
fi
if ! git merge-base --is-ancestor "$sha" "origin/$branch"; then
  echo "ERROR: tagged commit $sha is not reachable from $branch; releases are cut from main only" >&2
  exit 1
fi
