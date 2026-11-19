#!/usr/bin/env bash
# Job "Helm chart": installs the signed helm-unittest plugin tarball (Helm 4 checks it against
# the .prov file next to it, with the keyring) and runs the chart's unit tests. The download and
# the SHA-256 and fingerprint checks come before this, from ci.yml on GitHub or at the build of
# the tools image on GitLab. Needs helm on the PATH.
#
#   ci/chart-unittest.sh unittest-<version>.tgz KEYRING.gpg
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

tgz="${1:?usage: ci/chart-unittest.sh unittest-VERSION.tgz KEYRING.gpg}"
keyring="${2:?missing KEYRING.gpg}"
version="$(basename "$tgz" .tgz)"
version="${version#unittest-}"
helm plugin install "$tgz" --keyring "$keyring"
helm plugin list | grep -E "^unittest[[:space:]]+${version//./\\.}[[:space:]].*[[:space:]]signed[[:space:]]"
helm unittest charts/kafkafim
