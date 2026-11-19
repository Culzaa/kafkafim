#!/usr/bin/env bash
# Builds the GitLab job image (ci/tools-image/Dockerfile) from the air-gap development bundle
# on the far side, and optionally pushes it. Run once per bundle, or whenever a tool pin of
# ci.yml changes; no network is needed. The bundled job images must already be in the registry
# (`tools/airgap-dev-bundle.sh publish`), or on the local Docker with PREFIX empty.
#
#   ci/build-tools-image.sh BUNDLE_DIR PREFIX TAG [--push]
#
# Result: PREFIX/kafkafim-ci:TAG, the default of KAFKAFIM_CI_IMAGE in .gitlab-ci.yml. Needs
# docker, gpg, tar (with xz) on the machine that runs it, not in CI. HADOLINT_IMAGE overrides
# the hadolint image taken from the bundle's images.lock.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

bundle="$(cd "${1:?usage: ci/build-tools-image.sh BUNDLE_DIR PREFIX TAG [--push]}" && pwd)"
prefix="${2-}"
prefix="${prefix%/}"
tag="${3:?missing TAG}"
push="${4:-}"
pre="${prefix:+$prefix/}"

case "$(docker info --format '{{.Architecture}}')" in
  aarch64 | arm64) plat=linux-arm64 ;;
  *) plat=linux-amd64 ;;
esac
tools="$bundle/tools/$plat"
lock="$bundle/tools/tools.lock"
[[ -d "$tools" && -f "$lock" ]] || {
  echo "no $tools or $lock: is $bundle a development bundle with its tools phase?" >&2
  exit 1
}

# The file the lock lists for a tool on this platform.
file_of() { awk -v t="$1" -v p="$plat" '$1 == t && $3 == p {print $4}' "$lock" | head -n 1; }
version_of() { awk -v t="$1" '$1 == t {print $2}' "$lock" | head -n 1; }
pinned_image() { sed -n "s/^image=//p" tools/airgap-dev/pins.txt | sed -E 's/ *#.*//' | grep -E "$1" | head -n 1; }

ctx="$(mktemp -d)"
trap 'rm -rf "$ctx"' EXIT
mkdir -p "$ctx/context/bin" "$ctx/context/helm-unittest"
cp ci/tools-image/Dockerfile "$ctx/"
cp ci/tools-image/unzip "$ctx/context/unzip"

extract() { # tool, archive member (empty: the archive is the binary), name in bin/
  local f
  f="$(file_of "$1")"
  [[ -n "$f" ]] || {
    echo "tools.lock has no $1 for $plat" >&2
    exit 1
  }
  if [[ -z "$2" ]]; then
    cp "$tools/$f" "$ctx/context/bin/$3"
  else
    tar -xf "$tools/$f" -C "$ctx/context/bin" --strip-components="$(awk -F/ '{print NF-1}' <<<"$2")" "$2"
    [[ "$(basename "$2")" == "$3" ]] || mv "$ctx/context/bin/$(basename "$2")" "$ctx/context/bin/$3"
  fi
  chmod +x "$ctx/context/bin/$3"
}
os_arch="${plat#linux-}"
extract helm "linux-$os_arch/helm" helm
extract kubeconform kubeconform kubeconform
extract shellcheck "shellcheck-$(version_of shellcheck)/shellcheck" shellcheck
extract shfmt "" shfmt
cp "$tools/promtool" "$ctx/context/bin/promtool" # the build keeps the binary, not the tarball
extract cosign "" cosign
extract syft syft syft
extract jq "" jq

ut="unittest-$(version_of helm-unittest).tgz"
cp "$bundle/tools/helm-unittest/$ut" "$bundle/tools/helm-unittest/$ut.prov" "$ctx/context/helm-unittest/"
fpr="$(sed -n "s/^[[:space:]]*KEY_FPR: *\\([0-9A-F]*\\).*/\\1/p" .github/workflows/ci.yml | head -n 1)"
ci/helm-unittest-keyring.sh "$bundle/tools/helm-unittest/helm-unittest-public-key.asc" "$fpr" \
  "$ctx/context/helm-unittest/keyring.gpg"
cp -R "$bundle/tools/kubeconform-schemas" "$ctx/context/kubeconform-schemas"
cp -R "$bundle/tools/yamllint-wheels" "$ctx/context/wheels"

image="${pre}kafkafim-ci:$tag"
hadolint="${HADOLINT_IMAGE:-}"
[[ -n "$hadolint" ]] || hadolint="$(awk '{print $1}' "$bundle/images/images.lock" 2>/dev/null | sed 's/@.*//' | grep hadolint | head -n 1)"
[[ -n "$hadolint" ]] || {
  echo "no hadolint image in $bundle/images/images.lock; set HADOLINT_IMAGE" >&2
  exit 1
}
docker build -t "$image" \
  --build-arg "JDK_IMAGE=${pre}$(pinned_image '^eclipse-temurin:')" \
  --build-arg "DOCKER_IMAGE=${pre}$(pinned_image '^docker:[0-9.]+-cli$')" \
  --build-arg "PYTHON_IMAGE=${pre}$(pinned_image '^python:')" \
  --build-arg "HADOLINT_IMAGE=${pre}${hadolint}" \
  --build-arg "YAMLLINT_VERSION=$(version_of yamllint)" \
  "$ctx"
echo "built $image"
if [[ "$push" == --push ]]; then docker push "$image"; fi
