#!/usr/bin/env bash
# Writes a Docker Compose override file that reads every image of a compose file through the
# registry prefix KAFKAFIM_IMAGE_PREFIX (the names that `tools/airgap-dev-bundle.sh publish
# --registry` pushes: PREFIX/<name as written>), so a compose file needs no edit. Prints the
# override's path. Needs docker compose and jq.
#
#   export KAFKAFIM_IMAGE_PREFIX=registry.example/mirror
#   COMPOSE_FILE="compose.yaml:$(deploy/local/compose-prefix.sh compose.yaml)" docker compose up -d
#   KAFKAFIM_COMPOSE_OVERRIDE="$(deploy/local/compose-prefix.sh loadtest/compose.yaml)" \
#     loadtest/run.sh up
#
# `./mvnw spring-boot:run` starts compose.yaml itself and ignores COMPOSE_FILE; give it both
# files: -Dspring-boot.run.arguments="--spring.docker.compose.file=compose.yaml,<override>".
set -euo pipefail

file="${1:?usage: deploy/local/compose-prefix.sh COMPOSE_FILE}"
prefix="${KAFKAFIM_IMAGE_PREFIX:-}"
prefix="${prefix%/}"
[[ -n "$prefix" ]] || {
  echo "KAFKAFIM_IMAGE_PREFIX is not set" >&2
  exit 2
}
command -v jq >/dev/null || {
  echo "missing tool: jq" >&2
  exit 2
}
out="${TMPDIR:-/tmp}/kafkafim-compose-prefix-$(cksum <<<"$PWD/$file" | cut -d' ' -f1).json"
docker compose -f "$file" config --format json |
  jq --arg p "$prefix" '{services: (.services | map_values(select(.image != null) | {image: ($p + "/" + .image)}))}' >"$out"
echo "$out"
