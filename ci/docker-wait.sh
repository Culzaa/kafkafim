#!/usr/bin/env bash
# GitLab only: waits up to 60 s for the Docker daemon the job talks to (the docker:dind service
# or the runner's mounted socket), prints where it is, and logs it in to the registry of
# KAFKAFIM_IMAGE_PREFIX, which the daemon pulls base and Testcontainers images from.
#
# Credentials: KAFKAFIM_REGISTRY_USER and KAFKAFIM_REGISTRY_PASSWORD (a masked variable) for
# any registry; unset, the job's CI_REGISTRY_USER and CI_REGISTRY_PASSWORD are used when the
# registry is this GitLab's. No credentials, no login: a public mirror needs none.
set -euo pipefail

echo "DOCKER_HOST=${DOCKER_HOST:-<default socket>} KAFKAFIM_DOCKER_MODE=${KAFKAFIM_DOCKER_MODE:-dind}"
up=0
for _ in $(seq 1 30); do
  if docker info >/dev/null 2>&1; then
    up=1
    break
  fi
  sleep 2
done
if [[ "$up" -ne 1 ]]; then
  echo "no Docker daemon answered within 60 s; see docs/DELIVERY.md, CI on GitLab, runner requirements" >&2
  docker info 2>&1 | tail -n 5 >&2 || true
  exit 1
fi
docker info --format 'Docker {{.ServerVersion}} on {{.OperatingSystem}}, storage driver {{.Driver}}'

host="${KAFKAFIM_IMAGE_PREFIX:-}"
host="${host%%/*}"
user="${KAFKAFIM_REGISTRY_USER:-}"
password="${KAFKAFIM_REGISTRY_PASSWORD:-}"
if [[ -z "$user" && -n "$host" && "$host" == "${CI_REGISTRY:-}" ]]; then
  user="${CI_REGISTRY_USER:-}"
  password="${CI_REGISTRY_PASSWORD:-}"
fi
if [[ -n "$host" && -n "$user" && -n "$password" ]]; then
  echo "$password" | docker login "$host" -u "$user" --password-stdin
fi
