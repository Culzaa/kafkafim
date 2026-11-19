#!/usr/bin/env bash
# Release pipeline (release.yml: "Sign image", "Package, push and sign chart"): signs the pushed
# image or chart with a Cosign key pair and checks the signature with the public key right away.
# Keyless signing needs Sigstore, which an air gap cannot reach (ADR-0057): the signature is
# made with a key, goes to the registry as a .sig tag (the legacy format, like the GitHub
# release, which `cosign save` and `cosign load` carry), and nothing is uploaded to a
# transparency log. Needs cosign.
#
#   ci/release-sign.sh check|image|chart
#
# `check` signs nothing: it fails at once, before anything is pushed, when signing is on and a
# variable is missing, so a release never leaves an unsigned image behind.
#
#   KAFKAFIM_SIGNING=cosign (default) | off   off signs nothing and says so; it is the only way
#                                              to run a release without a key
#   COSIGN_PRIVATE_KEY_B64   base64 (one line) of the encrypted private key file from
#                            `cosign generate-key-pair`; masked and protected. A multi-line key
#                            cannot be a masked variable, hence the encoding
#   COSIGN_PASSWORD          the key's password; masked and protected
#   COSIGN_PUBLIC_KEY        the public key (cosign.pub, the PEM text); not secret
#   KAFKAFIM_RELEASE_INSECURE=true   the registry speaks plain HTTP
#
# With signing on, a missing variable fails the job. The private key is passed to cosign through
# the environment of that one process (env://), never written to a file or logged.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

what="${1:?usage: ci/release-sign.sh check|image|chart}"
case "$what" in
  check | image | chart) ;;
  *)
    echo "usage: ci/release-sign.sh check|image|chart" >&2
    exit 2
    ;;
esac
mode="${KAFKAFIM_SIGNING:-cosign}"
case "$mode" in
  off)
    echo "KAFKAFIM_SIGNING=off: the $what is NOT signed"
    exit 0
    ;;
  cosign) ;;
  *)
    echo "KAFKAFIM_SIGNING must be cosign or off, not '$mode'" >&2
    exit 2
    ;;
esac
for v in COSIGN_PRIVATE_KEY_B64 COSIGN_PASSWORD COSIGN_PUBLIC_KEY; do
  if [[ -z "${!v:-}" ]]; then
    echo "ERROR: $v is not set. Signing is on (KAFKAFIM_SIGNING=cosign); set the variable (docs/DELIVERY.md, CI on GitLab, Signing) or set KAFKAFIM_SIGNING=off to release unsigned." >&2
    exit 1
  fi
done
if [[ "$what" == check ]]; then
  echo "signing is on and its variables are set"
  exit 0
fi
ref="$(cat "dist/$what.ref")"
pub="$(mktemp)"
trap 'rm -f "$pub"' EXIT
printf '%s\n' "$COSIGN_PUBLIC_KEY" >"$pub"
extra=()
[[ "${KAFKAFIM_RELEASE_INSECURE:-}" != true ]] || extra+=(--allow-http-registry)
COSIGN_PRIVATE_KEY="$(printf '%s' "$COSIGN_PRIVATE_KEY_B64" | base64 -d)" \
  cosign sign --yes --key env://COSIGN_PRIVATE_KEY --tlog-upload=false \
  --use-signing-config=false --new-bundle-format=false ${extra[@]+"${extra[@]}"} "$ref"
cosign verify --key "$pub" --insecure-ignore-tlog=true ${extra[@]+"${extra[@]}"} "$ref" >/dev/null
echo "signed and verified $ref with the key in COSIGN_PUBLIC_KEY"
