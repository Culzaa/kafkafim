#!/usr/bin/env bash
# Builds, checks and verifies the air-gap DEVELOPMENT bundle (ADR-0056, docs/DELIVERY.md,
# "Air-gap development bundle").
#
#   tools/airgap-dev-bundle.sh inventory   print what the bundle takes from this checkout
#   tools/airgap-dev-bundle.sh check       offline drift check (CI): the bundle's inputs and the
#                                          repository agree
#   tools/airgap-dev-bundle.sh build REF   build the bundle from a tag or branch (needs network,
#                                          Docker, a JDK of the pom's java.version, git, curl, jq)
#   tools/airgap-dev-bundle.sh verify DIR  the far-side proof: checksums, git bundle, then
#                                          `./mvnw -o verify` against the bundled repository only
#   tools/airgap-dev-bundle.sh publish DIR --maven-url URL --registry HOST/PATH
#                                          upload the Maven repository (HTTP PUT, for Artifactory,
#                                          Nexus or the GitLab package registry) and push the
#                                          images; see the publish options below
#
# build options: --out DIR (default ./airgap-dev-out), --repo URL_OR_PATH (default: origin),
# --platforms "linux/amd64 linux/arm64" (default both), --split-mib N (also write the bundle as
# tar parts of N MiB, for example 1900; default none), --skip-maven, --skip-images, --skip-jdk,
# --skip-sources (partial builds; the manifest says what is missing), --resume (continue in an existing directory; a
# finished phase is not repeated).
# publish options: --maven-url URL (the repository to PUT into, for example
# https://artifactory.example/artifactory/kafkafim-maven), --registry PREFIX (images are pushed
# as PREFIX/<original name:tag>), --platform linux/amd64|linux/arm64 (default: this Docker's),
# --jobs N (parallel uploads, default 8), --skip-maven, --skip-images, --dry-run. Credentials:
# PUBLISH_USER and PUBLISH_PASSWORD (a token works as the password); run `docker login` first.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly HERE
ROOT="$(cd "$HERE/.." && pwd)"
readonly ROOT

# Maven goals the project runs, in the order that fills one repository. They mirror ci.yml
# (build, integration, coverage, mutation) and release.yml; `check` fails when a workflow runs a
# plugin goal that is not covered here.
readonly -a GOAL_SETS=(
  "verify"
  "test-compile org.pitest:pitest-maven:mutationCoverage -Djacoco.skip=true"
  "-DskipTests -Dspotless.check.skip=true -Dcheckstyle.skip=true compile jacoco:merge@merge jacoco:report@report jacoco:check@check"
)

#######################################
# Prints a message to stderr and exits 1.
# Arguments:
#   Message words
#######################################
die() {
  echo "airgap-dev-bundle: $*" >&2
  exit 1
}

#######################################
# Exits 1 unless every named tool is on the PATH.
# Arguments:
#   Tool names
#######################################
need() {
  local tool
  for tool in "$@"; do
    command -v "$tool" >/dev/null || die "missing tool: $tool"
  done
}

#######################################
# Prints the SHA-256 of a file, whichever of sha256sum or shasum exists.
# Arguments:
#   File
#######################################
sha256_of() {
  if command -v sha256sum >/dev/null; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

#######################################
# Runs sha256sum, or shasum -a 256 where sha256sum does not exist (macOS), with the arguments.
#######################################
sums() {
  if command -v sha256sum >/dev/null; then sha256sum "$@"; else shasum -a 256 "$@"; fi
}

#######################################
# Image references named by Dockerfile FROM lines (stage aliases and scratch excluded).
# Arguments:
#   Repository root
#######################################
images_from_dockerfile() {
  sed -nE 's/^FROM[[:space:]]+(--[a-z]+=[^[:space:]]+[[:space:]]+)*([^[:space:]]+).*/\2/p' "$1/Dockerfile" |
    grep -vx scratch || true
}

#######################################
# Image references named by `image: X` lines of the compose files and the demo chart's values
# (the same lines the CI check "Container image versions agree" reads).
# Arguments:
#   Repository root
#######################################
images_from_yaml() {
  local root="$1"
  grep -hE '^[[:space:]]+image: [^[:space:]]' "$root/compose.yaml" "$root/loadtest/compose.yaml" \
    "$root/deploy/local/kafkafim-demo/values.yaml" |
    sed -E "s/^[[:space:]]+image: *['\"]?([^'\" #]+).*/\1/"
}

#######################################
# Image references the Testcontainers tests name.
# Arguments:
#   Repository root
#######################################
images_from_tests() {
  grep -rhoE 'DockerImageName\.parse\("[^"]+"\)' "$1/src/test" |
    sed -E 's/^.*\("([^"]+)"\)$/\1/'
}

#######################################
# Prints every image reference derived from the repository, sorted, without duplicates. The
# Testcontainers helper (Ryuk) is not in the repository: `ryuk_from_repo` reads it from the
# bundled Maven repository.
# Arguments:
#   Repository root
#######################################
derived_images() {
  {
    images_from_dockerfile "$1"
    images_from_yaml "$1"
    images_from_tests "$1"
  } | sort -u
}

#######################################
# Prints the Ryuk image the Testcontainers jar in a Maven repository would pull.
# Arguments:
#   Maven repository directory
#######################################
ryuk_from_repo() {
  local jar
  jar="$(find "$1/org/testcontainers/testcontainers" -name 'testcontainers-[0-9]*.jar' | sort | tail -n 1)"
  [[ -n "$jar" ]] || die "no Testcontainers jar in $1"
  unzip -p "$jar" 'org/testcontainers/utility/*' | strings | grep -oE 'testcontainers/ryuk:[0-9][0-9.]*' | sort -u | head -n 1
}

#######################################
# Prints the Maven wrapper's distribution URL.
# Arguments:
#   Repository root
#######################################
wrapper_url() {
  sed -n 's/^distributionUrl=//p' "$1/.mvn/wrapper/maven-wrapper.properties"
}

#######################################
# Prints the pom's java.version.
# Arguments:
#   Repository root
#######################################
java_major() {
  sed -n 's#.*<java.version>\([0-9]*\)</java.version>.*#\1#p' "$1/pom.xml" | head -n 1
}

#######################################
# Prints the inputs the bundle takes from a checkout.
# Arguments:
#   Repository root
#######################################
inventory() {
  local root="$1"
  echo "jdk: temurin $(java_major "$root")"
  echo "maven wrapper: $(wrapper_url "$root")"
  derived_images "$root" | sed 's/^/image: /'
  echo "image (from the Testcontainers jar, at build time): testcontainers/ryuk"
  local g
  for g in "${GOAL_SETS[@]}"; do echo "maven: ./mvnw $g"; done
}

#######################################
# The drift check: fails when what the bundle takes and what the repository holds disagree.
#   1. Each image source yields at least one image (a moved file or a changed pattern is not
#      silently an empty list).
#   2. Every image named anywhere in tracked files in one of the known forms is in the derived
#      list: a new compose file, Dockerfile or Testcontainers constant fails here until the
#      derivation reads it.
#   3. Every Maven plugin goal that the workflows run is covered by GOAL_SETS.
#   4. The wrapper URL and the java version parse.
# Needs no network and no Docker.
# Arguments:
#   Repository root
#######################################
check() {
  local root="$1" bad=0 src n
  for src in images_from_dockerfile images_from_yaml images_from_tests; do
    n="$("$src" "$root" 2>/dev/null | wc -l | tr -d ' ')" || n=0
    if [[ "$n" -eq 0 ]]; then
      echo "::error::$src found no images; the repository layout changed, update $0" >&2
      bad=1
    fi
  done

  local derived tracked ref
  derived="$(derived_images "$root" 2>/dev/null)" || derived=""
  # Broad scan over every tracked file: YAML image keys, Dockerfile FROM lines, Testcontainers
  # constants. A file the derivation does not read shows up as an image it does not know.
  tracked="$(
    cd "$root"
    {
      git grep -hE '^[[:space:]]+image: [^[:space:]{]' -- '*.yaml' '*.yml' ':!.github' ':!charts/*/templates' ':!deploy/local/kafkafim-demo/templates' ':!deploy/local/kafkafim-demo/charts' |
        sed -E "s/^[[:space:]]+image: *['\"]?([^'\" #]+).*/\1/" || true
      git grep -hE '^FROM[[:space:]]' -- 'Dockerfile*' '*/Dockerfile*' |
        sed -nE 's/^FROM[[:space:]]+(--[a-z]+=[^[:space:]]+[[:space:]]+)*([^[:space:]]+).*/\2/p' | grep -vx scratch || true
      git grep -hoE 'DockerImageName\.parse\("[^"]+"\)' -- src | sed -E 's/^.*\("([^"]+)"\)$/\1/' || true
      git grep -hoE '(withImage|new GenericContainer<>)\("[^"]+"' -- src | sed -E 's/^.*\("([^"]+)"$/\1/' || true
    } | sort -u
  )"
  while IFS= read -r ref; do
    [[ -n "$ref" ]] || continue
    if ! grep -qxF "$ref" <<<"$derived"; then
      echo "::error::image $ref is named in the repository but the air-gap development bundle does not take it; teach derived_images in $0 about it" >&2
      bad=1
    fi
  done <<<"$tracked"

  # Maven goals: every `prefix:goal` or fully qualified goal a workflow runs after ./mvnw.
  local goals goal
  goals="$(
    cd "$root"
    git grep -h -A2 '\./mvnw ' -- .github/workflows |
      tr -s '[:space:]' '\n' | grep -E '^([a-z.-]+:)+[a-zA-Z-]+(@[a-z-]+)?$' | sort -u || true
  )"
  if [[ -z "$goals" ]]; then
    echo "::error::found no plugin goals after ./mvnw in .github/workflows; update the goal scan in $0" >&2
    bad=1
  fi
  while IFS= read -r goal; do
    [[ -n "$goal" ]] || continue
    if ! printf '%s\n' "${GOAL_SETS[@]}" | grep -qF "${goal%%@*}"; then
      echo "::error::a workflow runs Maven goal $goal that GOAL_SETS in $0 does not run; add it" >&2
      bad=1
    fi
  done <<<"$goals"

  [[ "$(java_major "$root")" =~ ^[0-9]+$ ]] || {
    echo "::error::no java.version in pom.xml" >&2
    bad=1
  }
  [[ "$(wrapper_url "$root")" == https://* ]] || {
    echo "::error::no distributionUrl in .mvn/wrapper/maven-wrapper.properties" >&2
    bad=1
  }
  [[ "$bad" -eq 0 ]] || exit 1
  echo "airgap-dev-bundle: inputs agree with the repository ($(wc -l <<<"$derived" | tr -d ' ') images, ${#GOAL_SETS[@]} Maven goal sets)"
}

#######################################
# Writes the source as a git bundle with full history: the main branch and every tag.
# Arguments:
#   Repository URL or path, ref, output file
#######################################
bundle_source() {
  local url="$1" ref="$2" out="$3" tmp
  tmp="$(mktemp -d)"
  git init -q --bare "$tmp/src.git"
  git -C "$tmp/src.git" fetch -q --no-tags "$url" '+refs/heads/main:refs/heads/main' '+refs/tags/*:refs/tags/*'
  if ! git -C "$tmp/src.git" rev-parse -q --verify "$ref^{commit}" >/dev/null; then
    git -C "$tmp/src.git" fetch -q --no-tags "$url" "+$ref:refs/heads/$ref" || die "cannot resolve $ref in $url"
  fi
  git -C "$tmp/src.git" bundle create "$out" --all 2>/dev/null
  git bundle verify "$out" >/dev/null
  rm -rf "$tmp"
}

#######################################
# Runs one Maven goal set against a repository directory.
# Arguments:
#   Source checkout, Maven repository, MAVEN_USER_HOME, then the Maven arguments
#######################################
run_mvn() {
  local src="$1" repo="$2" home="$3"
  shift 3
  (
    cd "$src"
    # shellcheck disable=SC2086  # a goal set is a word list on purpose
    MAVEN_USER_HOME="$home" ./mvnw -B -ntp -Dmaven.repo.local="$repo" "$@"
  )
}

#######################################
# Fills a Maven repository by running every goal the project runs, from a clean one.
# Arguments:
#   Source checkout, Maven repository directory
#######################################
fill_maven() {
  local src="$1" repo="$2" home g
  home="$(mktemp -d)"
  mkdir -p "$repo"
  for g in "${GOAL_SETS[@]}"; do
    echo "== ./mvnw $g"
    # shellcheck disable=SC2086
    run_mvn "$src" "$repo" "$home" $g
  done
  rm -rf "$home"
  # Resolver bookkeeping ties an artifact to the repository it came from and records failed
  # lookups: neither belongs in a repository that is moved or served.
  rm -rf "$repo/.locks"
  find "$repo" \( -name '_remote.repositories' -o -name '*.lastUpdated' -o -name 'resolver-status.properties' \) -delete
}

#######################################
# Adds the -sources and -javadoc jars that Maven Central has for every jar in the repository,
# plugins and transitive dependencies included, each checked against Central's SHA-1. Maven
# itself resolves these only on request (IDE downloads, dependency:sources), so the build does
# not fetch them. A 404 means Central has none (normal); any other failure is fatal, so a
# network error cannot leave a bundle quietly incomplete.
# Arguments:
#   Maven repository directory
#######################################
fetch_extra_jars() {
  local repo="$1" total src jav
  export CENTRAL="https://repo.maven.apache.org/maven2"
  find "$repo" -name '*.part' -delete
  # shellcheck disable=SC2016  # the worker script is expanded by the child bash
  find "$repo" -name '*.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' -print0 |
    xargs -0 -n 1 -P 8 bash -c '
      jar="$1"
      rel="${jar#"$0"/}"
      for c in sources javadoc; do
        dest="${jar%.jar}-$c.jar"
        [[ -e "$dest" ]] && continue
        url="$CENTRAL/${rel%.jar}-$c.jar"
        code="$(curl -sSL --retry 3 --connect-timeout 20 --max-time 300 -o "$dest.part" -w "%{http_code}" "$url")" || code=000
        case "$code" in
          200)
            want="$(curl -fsSL --retry 3 --max-time 60 "$url.sha1" | cut -c1-40)" || want=""
            [[ -n "$want" ]] || { echo "sha1 fetch failed: $url" >&2; rm -f "$dest.part"; exit 255; }
            if command -v sha1sum >/dev/null; then got="$(sha1sum "$dest.part" | cut -c1-40)"; else got="$(shasum -a 1 "$dest.part" | cut -c1-40)"; fi
            [[ "$got" == "$want" ]] || { echo "checksum mismatch: $url" >&2; rm -f "$dest.part"; exit 255; }
            mv "$dest.part" "$dest"
            ;;
          404) rm -f "$dest.part" ;;
          *) echo "HTTP $code for $url" >&2; rm -f "$dest.part"; exit 255 ;;
        esac
      done' "$repo"
  total="$(find "$repo" -name '*.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' | wc -l | tr -d ' ')"
  src="$(find "$repo" -name '*-sources.jar' | wc -l | tr -d ' ')"
  jav="$(find "$repo" -name '*-javadoc.jar' | wc -l | tr -d ' ')"
  echo "sources: $src of $total jars, javadoc: $jav of $total (the rest have none on Central)"
}

#######################################
# Downloads the Maven distribution the wrapper fetches, into the repository layout, checked
# against Maven Central's published SHA-512.
# Arguments:
#   Source checkout, Maven repository directory
#######################################
fetch_maven_dist() {
  local url dest sum
  url="$(wrapper_url "$1")"
  dest="$2/${url#https://repo.maven.apache.org/maven2/}"
  [[ "$dest" != "$url" ]] || die "wrapper distributionUrl is not on repo.maven.apache.org: $url"
  mkdir -p "$(dirname "$dest")"
  curl -fsSL -o "$dest" "$url"
  curl -fsSL -o "$dest.sha512" "$url.sha512"
  sum="$(cut -d' ' -f1 "$dest.sha512")"
  if command -v sha512sum >/dev/null; then
    echo "$sum  $dest" | sha512sum -c - >/dev/null
  else
    echo "$sum  $dest" | shasum -a 512 -c - >/dev/null
  fi
}

#######################################
# Downloads the Temurin JDK for the platforms the bundle covers, checked against the SHA-256
# that Adoptium publishes with each release, and writes jdk/jdk.lock.
# Arguments:
#   JDK major version, output directory
#######################################
fetch_jdk() {
  local major="$1" out="$2" p os arch json link want name
  mkdir -p "$out"
  : >"$out/jdk.lock"
  for p in linux/x64 linux/aarch64 mac/aarch64; do
    os="${p%/*}"
    arch="${p#*/}"
    json="$(curl -fsSL "https://api.adoptium.net/v3/assets/latest/$major/hotspot?architecture=$arch&image_type=jdk&os=$os&vendor=eclipse")"
    link="$(jq -r '.[0].binary.package.link' <<<"$json")"
    want="$(jq -r '.[0].binary.package.checksum' <<<"$json")"
    name="$(jq -r '.[0].binary.package.name' <<<"$json")"
    curl -fsSL -o "$out/$name" "$link"
    [[ "$(sha256_of "$out/$name")" == "$want" ]] || die "checksum mismatch for $name"
    printf '%s  %s  %s\n' "$want" "$name" "$(jq -r '.[0].release_name' <<<"$json")" >>"$out/jdk.lock"
  done
}

#######################################
# Prints a reference as a file name.
# Arguments:
#   Image reference
#######################################
safe_name() {
  echo "${1%%@*}" | tr '/:' '__'
}

#######################################
# Resolves an image reference to name:tag@digest (the multi-arch index digest). A reference
# that already carries a digest is kept.
# Arguments:
#   Image reference
#######################################
pin_ref() {
  local ref="$1" digest
  if [[ "$ref" == *@sha256:* ]]; then
    echo "$ref"
    return
  fi
  digest="$(docker buildx imagetools inspect "$ref" --format '{{.Manifest.Digest}}')"
  [[ "$digest" == sha256:* ]] || die "no digest for $ref"
  echo "$ref@$digest"
}

#######################################
# Pulls every image by digest for each platform, tags it with its original name and saves one
# tar per platform and image. Writes images/images.lock (pinned reference, platform, file,
# SHA-256 of the file). Images present before the run are not removed afterwards.
# Arguments:
#   Output directory, platforms (space separated), then the image references
#######################################
save_images() {
  local out="$1" platforms="$2" ref pinned plat dir file had
  shift 2
  mkdir -p "$out"
  : >"$out/images.lock"
  for ref in "$@"; do
    pinned="$(pin_ref "$ref")"
    for plat in $platforms; do
      dir="$out/${plat//\//-}"
      mkdir -p "$dir"
      file="$dir/$(safe_name "$ref").tar"
      had=0
      docker image inspect "${ref%%@*}" >/dev/null 2>&1 && had=1
      docker pull -q --platform "$plat" "$pinned" >/dev/null
      docker tag "$pinned" "${ref%%@*}"
      docker save -o "$file" "${ref%%@*}"
      [[ "$had" -eq 1 ]] || docker rmi -f "${ref%%@*}" "$pinned" >/dev/null 2>&1 || true
      printf '%s  %s  %s  %s\n' "$pinned" "$plat" "${file#"$out"/}" "$(sha256_of "$file")" >>"$out/images.lock"
    done
  done
}

#######################################
# Builds the bundle.
# Arguments:
#   The build arguments (see the header)
#######################################
build() {
  local ref="" out="$PWD/airgap-dev-out" url platforms="linux/amd64 linux/arm64" split=0
  local skip_maven=0 skip_images=0 skip_jdk=0 skip_sources=0 resume=0
  url="$(git -C "$ROOT" remote get-url origin)"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --out)
        out="$2"
        shift 2
        ;;
      --repo)
        url="$2"
        shift 2
        ;;
      --platforms)
        platforms="$2"
        shift 2
        ;;
      --split-mib)
        split="$2"
        shift 2
        ;;
      --skip-maven)
        skip_maven=1
        shift
        ;;
      --skip-images)
        skip_images=1
        shift
        ;;
      --skip-jdk)
        skip_jdk=1
        shift
        ;;
      --skip-sources)
        skip_sources=1
        shift
        ;;
      --resume)
        resume=1
        shift
        ;;
      -*) die "unknown option $1" ;;
      *)
        ref="$1"
        shift
        ;;
    esac
  done
  [[ -n "$ref" ]] || die "build needs a tag or branch"
  mkdir -p "$out"
  out="$(cd "$out" && pwd)"
  case "$out" in "$ROOT"/*) die "--out must be outside the repository" ;; esac
  need git curl jq docker unzip strings tar

  local name dir work src
  name="kafkafim-dev-${ref//\//-}-airgap-dev"
  dir="$out/$name"
  [[ ! -e "$dir" || "$resume" -eq 1 ]] || die "$dir exists; remove it first or pass --resume"
  mkdir -p "$dir/source"
  work="$(mktemp -d)"

  echo "== source: git bundle"
  [[ -e "$dir/.phase-source" ]] || bundle_source "$url" "$ref" "$dir/source/kafkafim.bundle"
  touch "$dir/.phase-source"
  git clone -q "$dir/source/kafkafim.bundle" "$work/src"
  src="$work/src"
  git -C "$src" checkout -q "$ref"
  local commit
  commit="$(git -C "$src" rev-parse HEAD)"

  local major
  major="$(java_major "$src")"
  if [[ "$skip_maven" -eq 0 ]]; then
    local have
    have="$(java -XshowSettings:properties -version 2>&1 | sed -n 's/.*java.specification.version = //p')"
    [[ "$have" == "$major" ]] || die "build needs JDK $major on the PATH (java.specification.version is $have)"
    if [[ ! -e "$dir/.phase-maven" ]]; then
      echo "== maven repository"
      fill_maven "$src" "$dir/maven-repo"
      fetch_maven_dist "$src" "$dir/maven-repo"
      touch "$dir/.phase-maven"
    fi
    if [[ "$skip_sources" -eq 0 && ! -e "$dir/.phase-sources" ]]; then
      echo "== sources and javadoc jars"
      fetch_extra_jars "$dir/maven-repo"
      touch "$dir/.phase-sources"
    fi
  fi

  if [[ "$skip_jdk" -eq 0 && ! -e "$dir/.phase-jdk" ]]; then
    echo "== jdk"
    fetch_jdk "$major" "$dir/jdk"
    touch "$dir/.phase-jdk"
  fi

  local -a refs=()
  local line
  while IFS= read -r line; do refs+=("$line"); done < <(derived_images "$src")
  [[ ! -d "$dir/maven-repo" ]] || refs+=("$(ryuk_from_repo "$dir/maven-repo")")
  if [[ "$skip_images" -eq 0 && ! -e "$dir/.phase-images" ]]; then
    echo "== images (${#refs[@]})"
    save_images "$dir/images" "$platforms" "${refs[@]}"
    touch "$dir/.phase-images"
  fi

  write_manifest "$dir" "$ref" "$commit" "$platforms" "${refs[@]}"
  cp "$ROOT/tools/airgap-dev-bundle.sh" "$dir/airgap-dev-bundle.sh"
  (cd "$dir" && find . -type f ! -name SHA256SUMS ! -name '.phase-*' | LC_ALL=C sort | while IFS= read -r f; do sums "$f"; done) >"$dir/SHA256SUMS"
  rm -rf "$work"
  du -sh "$dir"

  if [[ "$split" -gt 0 ]]; then
    echo "== split archive ($split MiB parts)"
    tar -cf - -C "$out" "$name" | split -b "${split}m" - "$out/$name.tar.part-"
    (cd "$out" && sums "$name".tar.part-*) >"$out/$name.parts.SHA256SUMS"
  fi
}

#######################################
# Writes manifest.json: what the bundle was built from and what it holds.
# Arguments:
#   Bundle directory, ref, commit, platforms, image refs
#######################################
write_manifest() {
  local dir="$1" ref="$2" commit="$3" platforms="$4" phases
  shift 4
  phases="$(cd "$dir" && find . -maxdepth 1 -name '.phase-*' | sed 's|^\./\.phase-||' | tr '\n' ' ')"
  jq -n --arg ref "$ref" --arg commit "$commit" --arg built "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg platforms "$platforms" --arg phases "$phases" \
    --arg images "$(printf '%s\n' "$@")" \
    '{ref: $ref, commit: $commit, built: $built, platforms: ($platforms | split(" ")),
      phases: ($phases | split(" ") | map(select(length > 0))), images: ($images | split("\n") | map(select(length > 0)))}' \
    >"$dir/manifest.json"
}

#######################################
# The far-side proof, run inside the air gap: checksums, the git bundle, a clone, and a full
# offline build against the bundled Maven repository only.
# Globals:
#   VERIFY_GOALS (read): Maven goals, default "verify"
# Arguments:
#   Bundle directory
#######################################
verify() {
  local dir work
  dir="$(cd "$1" && pwd)"
  need git jq
  echo "== checksums"
  (cd "$dir" && sums -c SHA256SUMS >/dev/null) || die "SHA256SUMS does not match"
  echo "== git bundle"
  git bundle verify "$dir/source/kafkafim.bundle" >/dev/null
  work="$(mktemp -d)"
  git clone -q "$dir/source/kafkafim.bundle" "$work/src"
  git -C "$work/src" checkout -q "$(jq -r .commit "$dir/manifest.json")"
  echo "== offline build"
  # The wrapper fetches Maven from the bundle (MVNW_REPOURL; a user name and password make it use
  # its Java downloader, because wget cannot read file:// URLs); Maven runs with -o and the
  # bundled repository as its only local repository.
  cp -R "$dir/maven-repo" "$work/repo" # Maven writes bookkeeping files; the bundle stays untouched
  (
    cd "$work/src"
    # shellcheck disable=SC2086
    MAVEN_USER_HOME="$work/mavenhome" MVNW_REPOURL="file://$dir/maven-repo" \
      MVNW_USERNAME=bundle MVNW_PASSWORD=bundle \
      ./mvnw -B -ntp -o -Dmaven.repo.local="$work/repo" ${VERIFY_GOALS:-verify}
  ) || die "offline build failed"
  rm -rf "$work"
  echo "airgap-dev-bundle: verified $dir"
}

#######################################
# Uploads the bundle's Maven repository with HTTP PUT and pushes its images to a registry.
# Resolver metadata files named maven-metadata-*.xml are not uploaded: they only describe a
# local repository, and the repository manager writes its own.
# Globals:
#   PUBLISH_USER, PUBLISH_PASSWORD (read, optional)
# Arguments:
#   Bundle directory, then the options in the header
#######################################
publish() {
  local dir="" murl="" registry="" plat="" jobs=8 skip_maven=0 skip_images=0 dry=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --maven-url | --registry | --platform | --jobs)
        [[ $# -ge 2 ]] || die "$1 needs a value"
        case "$1" in
          --maven-url) murl="${2%/}" ;;
          --registry) registry="${2%/}" ;;
          --platform) plat="$2" ;;
          --jobs) jobs="$2" ;;
        esac
        shift 2
        ;;
      --skip-maven)
        skip_maven=1
        shift
        ;;
      --skip-images)
        skip_images=1
        shift
        ;;
      --dry-run)
        dry=1
        shift
        ;;
      -*) die "unknown option $1" ;;
      *)
        dir="$(cd "$1" && pwd)"
        shift
        ;;
    esac
  done
  [[ -n "$dir" ]] || die "publish needs the bundle directory"
  [[ "$skip_maven" -eq 1 || -n "$murl" ]] || die "publish needs --maven-url (or --skip-maven)"
  [[ "$skip_images" -eq 1 || -n "$registry" ]] || die "publish needs --registry (or --skip-images)"
  [[ "$jobs" =~ ^[1-9][0-9]*$ ]] || die "--jobs needs a positive integer"
  need curl
  (cd "$dir" && sums -c SHA256SUMS >/dev/null) || die "SHA256SUMS does not match; not publishing"

  if [[ "$skip_maven" -eq 0 ]]; then
    local n
    n="$(find "$dir/maven-repo" -type f ! -name 'maven-metadata-*.xml' | wc -l | tr -d ' ')"
    [[ "$n" -gt 0 ]] || die "no files in $dir/maven-repo"
    echo "== maven: $n files to $murl"
    if [[ "$dry" -eq 0 ]]; then
      # curl reads the credentials from a config file where backslash and quote are escapes.
      local u="${PUBLISH_USER:-}" w="${PUBLISH_PASSWORD:-}"
      u="${u//\\/\\\\}"
      u="${u//\"/\\\"}"
      w="${w//\\/\\\\}"
      w="${w//\"/\\\"}"
      export PUB_URL="$murl" PUBLISH_USER="$u" PUBLISH_PASSWORD="$w"
      # shellcheck disable=SC2016  # the worker script is expanded by the child bash
      find "$dir/maven-repo" -type f ! -name 'maven-metadata-*.xml' -print0 |
        xargs -0 -n 1 -P "$jobs" bash -c '
          rel="${1#"$0"/}"
          if [[ -n "$PUBLISH_USER" ]]; then
            printf "user = \"%s:%s\"\n" "$PUBLISH_USER" "$PUBLISH_PASSWORD"
          fi | curl -g -fsS -K - -o /dev/null -T "$1" "$PUB_URL/$rel" || {
            echo "upload failed: $rel" >&2
            exit 255
          }' "$dir/maven-repo"
    fi
  fi

  if [[ "$skip_images" -eq 0 ]]; then
    need docker
    if [[ -z "$plat" ]]; then
      case "$(docker info --format '{{.Architecture}}')" in
        aarch64 | arm64) plat=linux/arm64 ;;
        *) plat=linux/amd64 ;;
      esac
    fi
    [[ -f "$dir/images/images.lock" ]] || die "the bundle has no images/images.lock (built with --skip-images?)"
    echo "== images ($plat) to $registry"
    local pinned p file sum ref target got
    while read -r pinned p file sum; do
      [[ "$p" == "$plat" ]] || continue
      ref="${pinned%%@*}"
      target="$registry/$ref"
      echo "$ref -> $target"
      [[ "$dry" -eq 0 ]] || continue
      [[ "$(sha256_of "$dir/images/$file")" == "$sum" ]] || die "$file does not match images.lock"
      docker load -q -i "$dir/images/$file" >/dev/null
      docker tag "$ref" "$target"
      docker push -q "$target" >/dev/null
      got="$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$target" 2>/dev/null | grep -F "$target@" | head -n 1 || true)"
      if [[ "${got##*@}" != "${pinned##*@}" ]]; then
        echo "note: $target has digest ${got##*@}, the pin was ${pinned##*@}; a FROM that pins a digest must be rewritten" >&2
      fi
    done <"$dir/images/images.lock"
  fi
  echo "airgap-dev-bundle: published $dir$([[ "$dry" -eq 0 ]] || echo " (dry run, nothing uploaded)")"
}

#######################################
# Entry point.
# Arguments:
#   Subcommand and its arguments
#######################################
main() {
  local cmd="${1:-}"
  [[ $# -gt 0 ]] && shift
  case "$cmd" in
    inventory) inventory "$ROOT" ;;
    check) check "$ROOT" ;;
    build) build "$@" ;;
    verify)
      [[ $# -eq 1 ]] || die "verify needs the bundle directory"
      verify "$1"
      ;;
    publish) publish "$@" ;;
    *) die "usage: $0 inventory | check | build REF [options] | verify DIR | publish DIR [options]" ;;
  esac
}

main "$@"
