#!/usr/bin/env bash
# Release workflow (release.yml, job `bundle`): builds the air-gap development bundle at the tag,
# splits it into parts that fit a release asset (2 GiB each) and proves it with the far-side
# check, a full offline build, before anything is published (ADR-0058). GitHub only: the GitLab
# pipeline has no bundle step (ADR-0057, ci/parity.map). Needs Docker, git, curl, jq, Python 3
# and the project's JDK on the PATH (the job's setup-java step).
#
#   ci/bundle-build.sh REF OUT_DIR
#
# REF is the tag (or, in a dry run, the branch). The checkout is the source, through a bare copy
# of its remote branches and tags: the bundle holds the commit that is being released and needs
# no GitHub token. The checkout must have the full history (actions/checkout fetch-depth: 0).
# Leaves in OUT_DIR the parts (<name>.tar.part-aa ...), <name>.parts.SHA256SUMS and the unpacked
# directory. Parts are 1900 MiB (BUNDLE_SPLIT_MIB overrides).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

ref="${1:?usage: ci/bundle-build.sh REF OUT_DIR}"
out="${2:?missing OUT_DIR}"
split="${BUNDLE_SPLIT_MIB:-1900}"
name="kafkafim-dev-${ref//\//-}-airgap-dev"

src="$(mktemp -d)"
git init -q --bare "$src/src.git"
git -C "$src/src.git" fetch -q --no-tags "$PWD" '+refs/remotes/origin/*:refs/heads/*' '+refs/tags/*:refs/tags/*'
tools/airgap-dev-bundle.sh build "$ref" --repo "$src/src.git" --out "$out" --split-mib "$split"
tools/airgap-dev-bundle.sh verify "$out/$name"
# The parts are what is published: prove that they join into the bundle that was verified.
work="$(mktemp -d "$out/rejoin.XXXXXX")"
(cd "$out" && sha256sum -c "$name.parts.SHA256SUMS")
cat "$out/$name".tar.part-* | tar -xf - -C "$work"
(cd "$work/$name" && sha256sum -c SHA256SUMS >/dev/null)
rm -rf "$work" "$src"
ls -l "$out"/"$name".tar.part-*
