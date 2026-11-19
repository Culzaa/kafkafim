#!/usr/bin/env bash
# Release workflow (release.yml, jobs `bundle-publish` and `release`): signs the checksums of the
# air-gap development bundle's parts and attaches them to a DRAFT release; `finish` makes the
# release public once everything else is attached (ADR-0058). GitHub only: the GitLab pipeline
# has no bundle step (ADR-0057, ci/parity.map). Needs gh and cosign; GH_TOKEN, and for `draft`
# the job's OIDC token (permissions id-token: write).
#
#   ci/bundle-publish.sh draft vMAJOR.MINOR.PATCH[-prerelease] DIR   DIR holds what bundle-build.sh left
#   ci/bundle-publish.sh finish vMAJOR.MINOR.PATCH[-prerelease]
#
# `draft` checks the parts against their checksums file, signs that file with Cosign keyless (a
# Sigstore bundle next to it), verifies the signature with the identity of this repository's
# workflows, then creates the draft release (or re-uploads to it, `--clobber`, when a rerun finds
# one) and uploads the parts, the checksums and the signature. A draft is visible only to people
# with write access, so a failure here leaves nothing public.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

mode="${1:?usage: ci/bundle-publish.sh draft|finish TAG [DIR]}"
tag="${2:?missing TAG}"
repo="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is not set}"
name="kafkafim-dev-${tag}-airgap-dev"

case "$mode" in
  draft)
    dir="${3:?usage: ci/bundle-publish.sh draft TAG DIR}"
    sums="$dir/$name.parts.SHA256SUMS"
    (cd "$dir" && sha256sum -c "$(basename "$sums")")
    [[ "$(grep -c . "$sums")" -eq "$(find "$dir" -name "$name.tar.part-*" | wc -l)" ]] ||
      {
        echo "ERROR: $sums does not list exactly the parts in $dir" >&2
        exit 1
      }
    cosign sign-blob --yes --bundle "$sums.sigstore.json" "$sums"
    cosign verify-blob --bundle "$sums.sigstore.json" \
      --certificate-identity-regexp "^https://github\\.com/${repo//./\\.}/\\.github/workflows/release\\.yml@refs/tags/v" \
      --certificate-oidc-issuer https://token.actions.githubusercontent.com "$sums"
    assets=("$dir/$name".tar.part-* "$sums" "$sums.sigstore.json")
    if gh release view "$tag" --repo "$repo" >/dev/null 2>&1; then
      gh release upload "$tag" "${assets[@]}" --clobber --repo "$repo"
    else
      flags=(--draft --verify-tag --title "$tag" --generate-notes)
      [[ "$tag" != *-* ]] || flags+=(--prerelease)
      gh release create "$tag" "${assets[@]}" "${flags[@]}" --repo "$repo"
    fi
    ;;
  finish)
    gh release edit "$tag" --draft=false --repo "$repo"
    ;;
  *)
    echo "usage: ci/bundle-publish.sh draft|finish TAG [DIR]" >&2
    exit 2
    ;;
esac
