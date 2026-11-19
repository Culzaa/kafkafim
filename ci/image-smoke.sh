#!/usr/bin/env bash
# Job "Container image": starts the image read-only with the telemetry exporters off and waits
# up to 60 s for the liveness probe.
#
#   ci/image-smoke.sh IMAGE
#
# DOCKER_HOST_NAME (optional): the host the container's published port is reachable on. Empty
# (default) is localhost, right for a local Docker daemon; with Docker-in-Docker it is the
# service's host name, "docker".
set -euo pipefail

image="${1:?usage: ci/image-smoke.sh IMAGE}"
host="${DOCKER_HOST_NAME:-localhost}"
cid=$(docker run -d --read-only --tmpfs /tmp -p 8080:8080 "$image" \
  --spring.docker.compose.enabled=false \
  --management.otlp.metrics.export.enabled=false \
  --management.tracing.export.otlp.enabled=false \
  --management.logging.export.otlp.enabled=false)
probe() {
  if command -v curl >/dev/null 2>&1; then
    curl -sf "http://$host:8080/actuator/health/liveness"
  else
    # The GitLab job image has no curl; Python is there for yamllint.
    python3 -c 'import sys, urllib.request; sys.stdout.write(urllib.request.urlopen(sys.argv[1], timeout=5).read().decode())' \
      "http://$host:8080/actuator/health/liveness"
  fi
}
for _ in $(seq 1 30); do
  if probe; then
    echo
    docker rm -f "$cid" >/dev/null
    exit 0
  fi
  sleep 2
done
echo "container did not become live"
docker logs "$cid"
docker rm -f "$cid" >/dev/null
exit 1
