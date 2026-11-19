#!/usr/bin/env bash
# Tool downloads of the air-gap development bundle (ADR-0056). Sourced by
# tools/airgap-dev-bundle.sh; not run on its own.
#
# Versions come from .github/workflows/*.yml (the pins CI uses) and, for the few tools CI does
# not pin, from tools/airgap-dev/pins.txt. Every download is checked against a SHA-256: the
# one pinned in ci.yml where CI pins one, else the checksum file of the upstream release, else
# (shellcheck publishes none) recorded on first download and marked "recorded" in tools.lock.

# Platforms the tools are fetched for: name os arch (arch spelling differs per project).
readonly -a TOOL_PLATFORMS=(linux-amd64 linux-arm64 darwin-arm64)

#######################################
# Prints the value of KEY: in a workflow file, first occurrence, quotes and comments removed.
# Arguments:
#   Repository root, key, optional grep pattern for the line (default: the key)
#######################################
wf_value() {
  local root="$1" key="$2"
  cat "$root"/.github/workflows/*.yml |
    sed -n "s/^[[:space:]]*$key: *//p" | head -n 1 | sed -E "s/ *#.*//; s/['\"]//g"
}

#######################################
# Prints the value of name=... in tools/airgap-dev/pins.txt.
# Arguments:
#   Repository root, name
#######################################
pin() {
  sed -n "s/^$2=//p" "$1/tools/airgap-dev/pins.txt" | head -n 1 | sed -E 's/ *#.*//'
}

#######################################
# Prints "name version" for every tool, one per line, from the workflows and the pins file.
# An empty version is an error (the check fails on it).
# Arguments:
#   Repository root
#######################################
tool_versions() {
  local r="$1" wf
  wf="$(cat "$r"/.github/workflows/*.yml)"
  echo "helm $(grep -A3 'azure/setup-helm' <<<"$wf" | sed -n 's/.*version: *\(v[0-9.]*\).*/\1/p' | head -n 1)"
  echo "helm-unittest $(grep -E '^[[:space:]]+TGZ: unittest-' <<<"$wf" | sed -E 's/.*unittest-([0-9.]+)\.tgz.*/\1/' | head -n 1)"
  echo "kubeconform $(sed -n 's#.*kubeconform@\(v[0-9.]*\).*#\1#p' <<<"$wf" | head -n 1)"
  echo "shellcheck $(wf_value "$r" SHELLCHECK_VERSION)"
  echo "shfmt $(wf_value "$r" SHFMT_VERSION)"
  echo "actionlint $(wf_value "$r" ACTIONLINT_VERSION)"
  echo "promtool $(grep -E '^[[:space:]]+TGZ: prometheus-' <<<"$wf" | sed -E 's/.*prometheus-([0-9.]+)\.linux.*/\1/' | head -n 1)"
  echo "yamllint $(sed -n 's/.*yamllint==\([0-9.]*\).*/\1/p' <<<"$wf" | head -n 1)"
  echo "cosign $(wf_value "$r" cosign-release)"
  echo "hadolint $(sed -n 's#.*hadolint/hadolint-action@\([0-9a-f]*\).*#\1#p' <<<"$wf" | head -n 1)"
  echo "syft $(sed -n 's#.*anchore/sbom-action@\([0-9a-f]*\).*#\1#p' <<<"$wf" | head -n 1)"
  local p
  for p in kind kubectl jq markdownlint-cli2; do echo "$p $(pin "$r" "$p")"; done
}

#######################################
# Prints the image references of the pins file (job images), one per line.
# Arguments:
#   Repository root
#######################################
pinned_images() {
  sed -n 's/^image=//p' "$1/tools/airgap-dev/pins.txt" | sed -E 's/ *#.*//'
}

#######################################
# Prints the value of a version from tool_versions.
# Arguments:
#   Repository root, tool name
#######################################
tool_version() {
  tool_versions "$1" | sed -n "s/^$2 //p"
}

#######################################
# The tool part of the drift check: every version parses, every pin has a value, every
# download in the workflows is one the bundle knows.
# Arguments:
#   Repository root
# Returns:
#   1 when something disagrees
#######################################
check_tools() {
  local root="$1" bad=0 name ver
  while read -r name ver; do
    if [[ -z "$ver" ]]; then
      echo "::error::no version found for tool $name (workflows or tools/airgap-dev/pins.txt); update tools/airgap-dev-tools.sh" >&2
      bad=1
    fi
  done < <(tool_versions "$root")
  [[ -n "$(pinned_images "$root")" ]] || {
    echo "::error::tools/airgap-dev/pins.txt names no image" >&2
    bad=1
  }
  # The markdownlint action bundles a markdownlint-cli2: the pin must say the same version.
  tr -s '\n# ' ' ' <"$root/.github/workflows/ci.yml" | grep -q "markdownlint-cli2 $(pin "$root" markdownlint-cli2)" || {
    echo "::error::ci.yml no longer says it uses markdownlint-cli2 $(pin "$root" markdownlint-cli2); update the pin in tools/airgap-dev/pins.txt" >&2
    bad=1
  }
  # Downloads in the workflows: github.com release URLs, pip, go install, third-party actions.
  local url act
  while IFS= read -r url; do
    case "$url" in
      */koalaman/shellcheck/* | */mvdan/sh/* | */rhysd/actionlint/*) ;;
      *)
        echo "::error::a workflow downloads $url, which the air-gap development bundle does not fetch; teach tools/airgap-dev-tools.sh" >&2
        bad=1
        ;;
    esac
  done < <(cat "$root"/.github/workflows/*.yml | grep -oE 'https://github\.com/[^"$ ]+/releases/download' | sort -u | sed 's#/releases/download##; s#$#/#')
  while IFS= read -r act; do
    case "$act" in
      actions/* | docker/* | softprops/action-gh-release | azure/setup-helm | hadolint/hadolint-action | sigstore/cosign-installer | anchore/sbom-action | DavidAnson/markdownlint-cli2-action) ;;
      *)
        echo "::error::a workflow uses the action $act; decide whether the air-gap bundle needs its tool and update tools/airgap-dev-tools.sh" >&2
        bad=1
        ;;
    esac
  done < <(cat "$root"/.github/workflows/*.yml | sed -n 's/^[[:space:]-]*uses: *\([^@ ]*\)@.*/\1/p' | sort -u)
  grep -hoE '(pip install [^ ]+ [^ ]+|go install [a-z]+\.[a-z.]+/[^ ]+)' "$root"/.github/workflows/*.yml | sort -u | while read -r l; do
    case "$l" in
      *yamllint* | *kubeconform*) ;;
      *) echo "::error::a workflow runs '$l', which the bundle does not fetch" >&2 && exit 1 ;;
    esac
  done || bad=1
  return "$bad"
}

#######################################
# Sets URL, FILE, CK (checksum file URL or empty) and CKFMT (name: "hash  file" lines in CK,
# bare: CK holds only the hash) for a tool on a platform. Exits 0 with FILE empty when the tool
# has no build for that platform.
# Arguments:
#   Repository root, tool, platform (os-arch)
#######################################
tool_asset() {
  local r="$1" t="$2" os="${3%-*}" arch="${3#*-}" v aarch
  v="$(tool_version "$r" "$t")"
  URL="" FILE="" CK="" CKFMT=name
  case "$arch" in arm64) aarch=aarch64 ;; *) aarch=x86_64 ;; esac
  case "$t" in
    helm)
      FILE="helm-$v-$os-$arch.tar.gz"
      URL="https://get.helm.sh/$FILE"
      CK="$URL.sha256sum"
      ;;
    kubeconform)
      FILE="kubeconform-$os-$arch.tar.gz"
      URL="https://github.com/yannh/kubeconform/releases/download/$v/$FILE"
      CK="https://github.com/yannh/kubeconform/releases/download/$v/CHECKSUMS"
      ;;
    shellcheck)
      FILE="shellcheck-$v.$os.$aarch.tar.xz"
      URL="https://github.com/koalaman/shellcheck/releases/download/$v/$FILE"
      CK=""
      ;;
    shfmt)
      FILE="shfmt_${v}_${os}_$arch"
      URL="https://github.com/mvdan/sh/releases/download/$v/$FILE"
      CK="" # v3.14.1 publishes no checksum file; linux-amd64 is checked against ci.yml
      ;;
    actionlint)
      FILE="actionlint_${v}_${os}_$arch.tar.gz"
      URL="https://github.com/rhysd/actionlint/releases/download/v$v/$FILE"
      CK="https://github.com/rhysd/actionlint/releases/download/v$v/actionlint_${v}_checksums.txt"
      ;;
    promtool)
      FILE="prometheus-$v.$os-$arch.tar.gz"
      URL="https://github.com/prometheus/prometheus/releases/download/v$v/$FILE"
      CK="https://github.com/prometheus/prometheus/releases/download/v$v/sha256sums.txt"
      ;;
    cosign)
      FILE="cosign-$os-$arch"
      URL="https://github.com/sigstore/cosign/releases/download/$v/$FILE"
      CK="https://github.com/sigstore/cosign/releases/download/$v/cosign_checksums.txt"
      ;;
    syft)
      local sv
      sv="$(syft_version "$r")"
      FILE="syft_${sv#v}_${os}_$arch.tar.gz"
      URL="https://github.com/anchore/syft/releases/download/$sv/$FILE"
      CK="https://github.com/anchore/syft/releases/download/$sv/syft_${sv#v}_checksums.txt"
      ;;
    kind)
      FILE="kind-$os-$arch"
      URL="https://github.com/kubernetes-sigs/kind/releases/download/$v/$FILE"
      CK="$URL.sha256sum"
      ;;
    kubectl)
      FILE="kubectl"
      URL="https://dl.k8s.io/release/$v/bin/$os/$arch/kubectl"
      CK="$URL.sha256"
      CKFMT=bare
      ;;
    jq)
      local jos="$os"
      [[ "$os" != darwin ]] || jos=macos
      FILE="jq-$jos-$arch"
      URL="https://github.com/jqlang/jq/releases/download/$v/$FILE"
      CK="https://github.com/jqlang/jq/releases/download/$v/sha256sum.txt"
      ;;
    *) FILE="" ;;
  esac
}

#######################################
# Prints the syft version the pinned sbom-action downloads.
# Arguments:
#   Repository root
#######################################
syft_version() {
  curl -fsSL "https://raw.githubusercontent.com/anchore/sbom-action/$(tool_version "$1" syft)/src/SyftVersion.ts" |
    sed -n 's/.*VERSION *= *"\(v[0-9.]*\)".*/\1/p' | head -n 1
}

#######################################
# Prints the image the pinned hadolint action runs.
# Arguments:
#   Repository root
#######################################
hadolint_image() {
  curl -fsSL "https://raw.githubusercontent.com/hadolint/hadolint-action/$(tool_version "$1" hadolint)/Dockerfile" |
    sed -n 's/^FROM *\([^ ]*\).*/\1/p' | head -n 1
}

#######################################
# Prints the node image the pinned kind release uses by default.
# Arguments:
#   Repository root
#######################################
kind_node_image() {
  curl -fsSL "https://raw.githubusercontent.com/kubernetes-sigs/kind/$(tool_version "$1" kind)/pkg/apis/config/defaults/image.go" |
    sed -n 's/^const Image = "\(.*\)"/\1/p'
}

#######################################
# Downloads one tool for one platform into a directory and appends a line to tools.lock:
# name version platform file sha256 how-checked.
# Arguments:
#   Repository root, tool, platform, output directory (tools/<platform>), lock file
#######################################
fetch_tool() {
  local r="$1" t="$2" plat="$3" out="$4" lock="$5" want="" how got ck
  tool_asset "$r" "$t" "$plat"
  [[ -n "$FILE" ]] || return 0
  mkdir -p "$out"
  curl -fsSL --retry 3 --connect-timeout 20 -o "$out/$FILE" "$URL" || die "download failed: $URL"
  got="$(sha256_of "$out/$FILE")"
  how=upstream
  if [[ -n "$CK" ]]; then
    ck="$(curl -fsSL --retry 3 "$CK")" || die "checksum file failed: $CK"
    if [[ "$CKFMT" == bare ]]; then
      want="$(tr -d ' \n' <<<"$ck")"
    else
      want="$(grep -E "(^|[[:space:]*])${FILE//./\\.}\$" <<<"$ck" | head -n 1 | cut -d' ' -f1)"
    fi
    [[ -n "$want" ]] || die "no checksum for $FILE in $CK"
  else
    how=recorded
  fi
  # A SHA-256 pinned in ci.yml (linux amd64) is the stronger reference.
  local pinned=""
  case "$t/$plat" in
    shellcheck/linux-amd64) pinned="$(wf_value "$r" SHELLCHECK_SHA256)" ;;
    shfmt/linux-amd64) pinned="$(wf_value "$r" SHFMT_SHA256)" ;;
    actionlint/linux-amd64) pinned="$(wf_value "$r" ACTIONLINT_SHA256)" ;;
  esac
  if [[ -n "$pinned" ]]; then
    [[ "$got" == "$pinned" ]] || die "$FILE does not match the SHA-256 pinned in ci.yml"
    how=ci-pin
  fi
  if [[ -n "$want" && "$got" != "$want" ]]; then die "$FILE does not match the upstream checksum"; fi
  local ver
  ver="$(tool_version "$r" "$t")"
  [[ "$t" != syft ]] || ver="$(syft_version "$r")"
  printf '%s %s %s %s %s %s\n' "$t" "$ver" "$plat" "$FILE" "$got" "$how" >>"$lock"
  if [[ "$t" == promtool ]]; then
    # Only promtool is used; keep the binary, not the 100 MB Prometheus tarball.
    tar -xzf "$out/$FILE" -C "$out" --strip-components=1 "${FILE%.tar.gz}/promtool"
    rm -f "$out/$FILE"
  fi
}

#######################################
# helm-unittest's signed plugin tarball and its provenance file, checked against the SHA-256
# values pinned in ci.yml, plus the public key from the repository.
# Arguments:
#   Repository root, output directory, lock file
#######################################
fetch_helm_unittest() {
  local r="$1" out="$2" lock="$3" v tgz tsha psha
  v="$(tool_version "$r" helm-unittest)"
  tgz="unittest-$v.tgz"
  tsha="$(grep -E '^[[:space:]]+TGZ_SHA256:' "$r"/.github/workflows/ci.yml | head -n 1 | sed -E "s/.*: *//; s/ *#.*//; s/['\"]//g")"
  psha="$(grep -E '^[[:space:]]+PROV_SHA256:' "$r"/.github/workflows/ci.yml | head -n 1 | sed -E "s/.*: *//; s/ *#.*//; s/['\"]//g")"
  mkdir -p "$out"
  local base="https://github.com/helm-unittest/helm-unittest/releases/download/v$v"
  curl -fsSL --retry 3 -o "$out/$tgz" "$base/$tgz" || die "download failed: $base/$tgz"
  curl -fsSL --retry 3 -o "$out/$tgz.prov" "$base/$tgz.prov" || die "download failed: $base/$tgz.prov"
  [[ "$(sha256_of "$out/$tgz")" == "$tsha" ]] || die "$tgz does not match the SHA-256 pinned in ci.yml"
  [[ "$(sha256_of "$out/$tgz.prov")" == "$psha" ]] || die "$tgz.prov does not match the SHA-256 pinned in ci.yml"
  cp "$r/.github/keys/helm-unittest-public-key.asc" "$out/"
  printf 'helm-unittest %s all %s %s ci-pin\n' "$v" "$tgz" "$tsha" >>"$lock"
}

#######################################
# yamllint with its dependencies as wheels for the platforms and Python versions developers
# use, so `pip install --no-index --find-links` works offline.
# Arguments:
#   Repository root, output directory, lock file
#######################################
fetch_yamllint() {
  local r="$1" out="$2" lock="$3" v pv plat tag
  v="$(tool_version "$r" yamllint)"
  mkdir -p "$out"
  need python3
  python3 -m pip --version >/dev/null 2>&1 || die "python3 -m pip is needed to download the yamllint wheels"
  for plat in "${TOOL_PLATFORMS[@]}"; do
    case "$plat" in
      linux-amd64) tag=manylinux2014_x86_64 ;;
      linux-arm64) tag=manylinux2014_aarch64 ;;
      darwin-arm64) tag=macosx_11_0_arm64 ;;
    esac
    for pv in 3.10 3.11 3.12 3.13; do
      python3 -m pip download --disable-pip-version-check -q --only-binary=:all: \
        --platform "$tag" --python-version "$pv" --implementation cp -d "$out" "yamllint==$v" ||
        die "pip download of yamllint failed ($tag, Python $pv)"
    done
  done
  printf 'yamllint %s all wheels %s pip\n' "$v" "$(find "$out" -type f | wc -l | tr -d ' ')" >>"$lock"
}

#######################################
# Fills the schema cache of kubeconform by validating what CI validates: every chart values
# file and the demo chart, with the host's helm and kubeconform from the bundle. Offline use:
# kubeconform -cache <dir> (the cache is read before any request).
# Arguments:
#   Source checkout, bundle directory, host platform
#######################################
fill_kubeconform_cache() {
  local src="$1" dir="$2" plat="$3" f helm kc hv t cache
  t="$dir/tools/$plat"
  cache="$dir/tools/kubeconform-schemas"
  hv="$(tool_version "$src" helm)"
  mkdir -p "$cache" "$dir/.tmp-tools"
  tar -xzf "$t/helm-$hv-${plat%-*}-${plat#*-}.tar.gz" -C "$dir/.tmp-tools" --strip-components=1 "${plat%-*}-${plat#*-}/helm"
  tar -xzf "$t/kubeconform-${plat%-*}-${plat#*-}.tar.gz" -C "$dir/.tmp-tools" kubeconform
  helm="$dir/.tmp-tools/helm"
  kc="$dir/.tmp-tools/kubeconform"
  for f in "$src"/charts/kafkafim/ci/*-values.yaml; do
    "$helm" template kafkafim "$src/charts/kafkafim" -f "$f" | "$kc" -strict -summary -cache "$cache" ||
      die "kubeconform failed on $f"
  done
  (cd "$src" && "$helm" template kafkafim deploy/local/kafkafim-demo -n kafkafim-demo) |
    "$kc" -strict -summary -cache "$cache" || die "kubeconform failed on the demo chart"
  rm -rf "$dir/.tmp-tools"
}

#######################################
# The tools phase: every tool for every platform, helm-unittest, yamllint wheels, the
# kubeconform schema cache, and tools.lock.
# Arguments:
#   Source checkout, bundle directory, host platform (os-arch of this machine)
#######################################
build_tools() {
  local src="$1" dir="$2" host="$3" t p lock="$2/tools/tools.lock"
  mkdir -p "$dir/tools"
  : >"$lock"
  for p in "${TOOL_PLATFORMS[@]}"; do
    for t in helm kubeconform shellcheck shfmt actionlint promtool cosign syft kind kubectl jq; do
      echo "  $t $p"
      fetch_tool "$src" "$t" "$p" "$dir/tools/$p" "$lock"
    done
  done
  fetch_helm_unittest "$src" "$dir/tools/helm-unittest" "$lock"
  fetch_yamllint "$src" "$dir/tools/yamllint-wheels" "$lock"
  fill_kubeconform_cache "$src" "$dir" "$host"
}
