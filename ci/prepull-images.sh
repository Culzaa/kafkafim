#!/usr/bin/env bash
# Job "Integration tests", first step: starts pulling every Testcontainers image named in src/test
# while Java is set up and Maven compiles. No wait: a container start that needs an image still
# being pulled joins the daemon's in-flight pull. A failed pull only warns; Testcontainers pulls
# again. Testcontainers' own ryuk image (2 MB) is not named in src/test and is left to it.
#
# KAFKAFIM_IMAGE_PREFIX (optional): pull PREFIX/<name> instead of <name>, the names that
# RegistryPrefixImageNameSubstitutor makes Testcontainers use.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=ci/lib.sh
source ci/lib.sh

images=$(grep -rhoE 'DockerImageName\.parse\("[^"]+"\)' src/test | sed -E 's/^.*\("([^"]+)"\)$/\1/' | sort -u) || true # no match: warn below
if [[ -z "$images" ]]; then
  ci_warning 'no DockerImageName.parse("...") found in src/test'
  exit 0
fi
prefix="${KAFKAFIM_IMAGE_PREFIX:-}"
prefix="${prefix%/}"
logs="$(ci_tmp)/pulls"
mkdir -p "$logs"
for i in $images; do
  [[ -z "$prefix" ]] || i="$prefix/$i"
  log="$logs/$(echo "$i" | tr '/:' '__').log"
  echo "pulling $i"
  # Detached from the step's output, so the step ends while the pulls go on.
  (
    start=$(date +%s)
    if docker pull -q "$i"; then echo "ok $i $(($(date +%s) - start))s"; else echo "FAILED $i"; fi
  ) >"$log" 2>&1 </dev/null &
done
