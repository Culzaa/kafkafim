#!/usr/bin/env bash
# Job "Helm chart": helm lint of the production chart. Needs helm on the PATH.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

helm lint charts/kafkafim
