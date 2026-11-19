#!/usr/bin/env bash
# Release pipeline (release.yml: "Push multi-arch image"): builds the image from the jar that
# release-verify tested and pushes it with the tags 1.2.3, 1.2 (not for a pre-release) and
# sha-<7 characters>. Never `latest`. Writes the pushed reference, repository@digest, to
# dist/image.ref for the signing and SBOM steps. Needs docker (buildx), jq.
#
#   ci/release-image.sh vMAJOR.MINOR.PATCH[-prerelease]
#
# Variables: KAFKAFIM_RELEASE_IMAGE (default $CI_REGISTRY_IMAGE), KAFKAFIM_RELEASE_PLATFORMS
# (default linux/amd64; a second platform needs that platform's base image in the registry and
# the containerd image store), and those of ci/image-build.sh.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

tag="${1:?usage: ci/release-image.sh TAG}"
image="${KAFKAFIM_RELEASE_IMAGE:?KAFKAFIM_RELEASE_IMAGE is not set}"
sha="${CI_COMMIT_SHA:?CI_COMMIT_SHA is not set}"
version="${tag#v}"
tags=("$image:$version" "$image:sha-${sha:0:7}")
if [[ "$version" != *-* ]]; then
  tags+=("$image:$(cut -d. -f1,2 <<<"$version")")
fi
# A separate name from KAFKAFIM_IMAGE_PLATFORMS, so a project variable for the release cannot reach
# the CI image job.
export KAFKAFIM_IMAGE_PLATFORMS="${KAFKAFIM_RELEASE_PLATFORMS:-linux/amd64}"
mkdir -p dist
rm -f dist/image-build.json
labels=(
  --label "org.opencontainers.image.version=$version"
  --label "org.opencontainers.image.revision=$sha"
  --label "org.opencontainers.image.created=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  --label "org.opencontainers.image.source=${CI_PROJECT_URL:-}"
)
# image-build.sh passes extra buildx arguments through KAFKAFIM_BUILD_ARGS.
KAFKAFIM_BUILD_ARGS="${labels[*]}" ci/image-build.sh --push --metadata-file dist/image-build.json "${tags[@]}"
digest="$(jq -r '."containerimage.digest"' dist/image-build.json)"
[[ "$digest" == sha256:* ]] || {
  echo "no image digest in dist/image-build.json" >&2
  exit 1
}
echo "$image@$digest" >dist/image.ref
echo "pushed ${tags[*]} -> $image@$digest"
