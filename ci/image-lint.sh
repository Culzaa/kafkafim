#!/usr/bin/env bash
# Job "Container image", GitLab only: hadolint on the Dockerfile. On GitHub the pinned
# hadolint action runs the same tool with the same default rules.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

hadolint Dockerfile
