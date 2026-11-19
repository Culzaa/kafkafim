#!/usr/bin/env bash
# Release pipeline: logs Docker (and Helm, for the chart) in to the registries that the image
# and the chart are pushed to. Needs docker and helm.
#
#   KAFKAFIM_RELEASE_IMAGE            image repository, default $CI_REGISTRY_IMAGE
#   KAFKAFIM_RELEASE_CHART_REPO       oci://host/path of the charts, default $CI_REGISTRY_IMAGE/charts
#   KAFKAFIM_RELEASE_REGISTRY_USER    user for both, default the job's CI_REGISTRY_USER
#   KAFKAFIM_RELEASE_REGISTRY_PASSWORD  masked variable, default CI_REGISTRY_PASSWORD
#   KAFKAFIM_RELEASE_INSECURE=true    the registries speak plain HTTP (a lab registry)
set -euo pipefail

image="${KAFKAFIM_RELEASE_IMAGE:?KAFKAFIM_RELEASE_IMAGE is not set}"
chart="${KAFKAFIM_RELEASE_CHART_REPO:?KAFKAFIM_RELEASE_CHART_REPO is not set}"
user="${KAFKAFIM_RELEASE_REGISTRY_USER:-}"
password="${KAFKAFIM_RELEASE_REGISTRY_PASSWORD:-}"
if [[ -z "$user" || -z "$password" ]]; then
  echo "KAFKAFIM_RELEASE_REGISTRY_USER and KAFKAFIM_RELEASE_REGISTRY_PASSWORD are not set; the release cannot push" >&2
  exit 1
fi
chart_ref="${chart#oci://}"
helm_args=()
[[ "${KAFKAFIM_RELEASE_INSECURE:-}" != true ]] || helm_args+=(--plain-http)
image_host="${image%%/*}"
chart_host="${chart_ref%%/*}"
for host in $(printf '%s\n%s\n' "$image_host" "$chart_host" | sort -u); do
  echo "$password" | docker login "$host" -u "$user" --password-stdin
  echo "$password" | helm registry login "$host" -u "$user" --password-stdin ${helm_args[@]+"${helm_args[@]}"}
done
