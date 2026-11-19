#!/usr/bin/env bash
# Job "YAML format": container image versions agree. Dependabot's docker-compose entries bump
# the two compose files; the demo chart's values and the Testcontainers constants name the same
# images and are bumped by hand (#386). Each image must have one tag everywhere, so a half-done
# bump fails here.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=ci/lib.sh
source ci/lib.sh

yaml_imgs=$(grep -hE '^\s+image: ' compose.yaml loadtest/compose.yaml deploy/local/kafkafim-demo/values.yaml |
  sed -E "s/^[[:space:]]+image: *['\"]?([^'\" #]+).*/\1/")
test_imgs=$(grep -rhoE 'DockerImageName\.parse\("[^"]+"\)' src/test | sed -E 's/^.*\("([^"]+)"\)$/\1/')
if [[ -z "$yaml_imgs" || -z "$test_imgs" ]]; then
  ci_error "found no images in the compose files/demo values or in src/test; update this check"
  exit 1
fi
imgs=$(printf '%s\n%s\n' "$yaml_imgs" "$test_imgs" | sort -u)
echo "$imgs"
bad=$(echo "$imgs" | awk -F: 'NF == 2 { n[$1]++ } END { for (r in n) if (n[r] > 1) print r }')
if [[ -n "$bad" ]]; then
  ci_error "image(s) with more than one tag across compose, demo values and src/test: $bad"
  exit 1
fi
