#!/usr/bin/env bash
# Job "Container image", GitLab only (GitHub builds with docker/build-push-action): builds the
# image from target/*.jar and the Dockerfile, with the base images read from a registry prefix.
#
#   ci/image-build.sh [--push [--metadata-file FILE]] TAG...
#
# The Dockerfile names its base image as eclipse-temurin:27-jre@sha256:<index digest>. This
# script writes a copy of the Dockerfile whose FROM lines read
# <KAFKAFIM_IMAGE_PREFIX>/eclipse-temurin:27-jre[@sha256:<digest>] and builds that, so the
# repository's Dockerfile (and Dependabot's bumps of it) stay untouched and GitHub builds it as
# is.
#
# Digests (KAFKAFIM_BASE_DIGEST, issue 486): `tools/airgap-dev-bundle.sh publish` pushes one
# platform of each image, and a push of a single platform produces another manifest, so the
# digest the Dockerfile pins does not exist in that registry ("not found", shown on a local
# registry). Hence the default, `drop`: build from the tag, which the mirror's operator controls.
# Use `keep` when the registry holds a digest-preserving copy of the bundled index (skopeo copy
# --all, crane copy, Artifactory replication): the FROM keeps the @sha256 pin, exactly as on GitHub.
#
# The `# syntax=docker/dockerfile:1` line is dropped from the copy: BuildKit would fetch that
# frontend image from Docker Hub, and the Dockerfile uses nothing beyond the built-in syntax.
#
# KAFKAFIM_BUILD_ARGS: extra `docker build` arguments, word split (labels, for the release).
#
# Variables: KAFKAFIM_IMAGE_PREFIX (default: none, base images from where the Dockerfile names
# them), KAFKAFIM_BASE_DIGEST (drop, default, or keep), KAFKAFIM_IMAGE_PLATFORMS (for example
# linux/amd64,linux/arm64; default: the daemon's platform; several need the containerd image
# store, Docker 29's default).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

mode=load
meta=""
if [[ "${1:-}" == --push ]]; then
  mode=push
  shift
  if [[ "${1:-}" == --metadata-file ]]; then
    meta="${2:?--metadata-file needs a file}"
    shift 2
  fi
fi
[[ $# -gt 0 ]] || {
  echo "usage: ci/image-build.sh [--push [--metadata-file FILE]] TAG..." >&2
  exit 2
}
tags=()
for t in "$@"; do tags+=(-t "$t"); done
prefix="${KAFKAFIM_IMAGE_PREFIX:-}"
prefix="${prefix%/}"
digest="${KAFKAFIM_BASE_DIGEST:-drop}"
[[ "$digest" == keep || "$digest" == drop ]] || {
  echo "KAFKAFIM_BASE_DIGEST must be keep or drop, not '$digest'" >&2
  exit 2
}

dockerfile="$(mktemp "${TMPDIR:-/tmp}/Dockerfile.XXXXXX")"
trap 'rm -f "$dockerfile"' EXIT
# FROM [--platform=...] image[@digest] [AS name]: prefix the image, optionally drop the digest.
sed -E \
  -e '/^# syntax=/d' \
  -e "s#^(FROM[[:space:]]+(--[a-z]+=[^[:space:]]+[[:space:]]+)?)([^[:space:]]+)#\\1${prefix:+$prefix/}\\3#" \
  Dockerfile >"$dockerfile"
if [[ "$digest" == drop ]]; then sed -E '/^FROM/ s/@sha256:[0-9a-f]+//' "$dockerfile" >"$dockerfile.new" && mv "$dockerfile.new" "$dockerfile"; fi
grep '^FROM' "$dockerfile"

args=(build -f "$dockerfile" "${tags[@]}")
# shellcheck disable=SC2206  # a word list on purpose
[[ -z "${KAFKAFIM_BUILD_ARGS:-}" ]] || args+=(${KAFKAFIM_BUILD_ARGS})
[[ -z "${KAFKAFIM_IMAGE_PLATFORMS:-}" ]] || args+=(--platform "$KAFKAFIM_IMAGE_PLATFORMS")
if [[ "$mode" == push ]]; then
  # buildx records the provenance attestation that the GitHub release also pushes.
  [[ -z "$meta" ]] || args+=(--metadata-file "$meta")
  docker buildx build --push --provenance=mode=max "${args[@]:1}" .
else
  docker "${args[@]}" .
fi
