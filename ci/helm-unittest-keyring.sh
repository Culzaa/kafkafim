#!/usr/bin/env bash
# Turns the helm-unittest release key into the legacy keyring file that Helm reads, after
# checking that the key file holds exactly the expected fingerprint. Used by the GitHub chart
# job and by the build of the GitLab tools image. Needs gpg.
#
#   ci/helm-unittest-keyring.sh KEY.asc FINGERPRINT OUT.gpg
set -euo pipefail

key="${1:?usage: ci/helm-unittest-keyring.sh KEY.asc FINGERPRINT OUT.gpg}"
want="${2:?missing FINGERPRINT}"
out="${3:?missing OUT.gpg}"
GNUPGHOME=$(mktemp -d) # short path: gpg's socket paths have a length limit
export GNUPGHOME
gpg --batch --no-autostart --quiet --import "$key"
fprs=$(gpg --batch --no-autostart --with-colons --list-keys | awk -F: '$1 == "pub" {p = 1; next} p && $1 == "fpr" {print $10; p = 0}')
if [[ "$fprs" != "$want" ]]; then
  echo "unexpected key(s) in keyring: $fprs" >&2
  exit 1
fi
gpg --batch --no-autostart --export >"$out" # Helm reads the legacy keyring format
rm -rf "$GNUPGHOME"
