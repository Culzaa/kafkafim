#!/usr/bin/env bash
# Release workflow (release.yml, jobs `bundle` and `bundle-publish`): makes room on a GitHub
# hosted runner for the air-gap development bundle (ADR-0058). The runner's root disk has about
# 14 GB free; the bundle is 10 GB or more and is written twice (the directory and its parts).
# Removes toolchains the job never uses (.NET, Android SDK, GHC, CodeQL, cached tool versions) and
# creates a work directory on the runner's second disk (/mnt, about 60 GB free). Does nothing
# unless it runs on a GitHub runner, so it is harmless on a developer machine.
#
#   ci/bundle-free-disk.sh WORK_DIR
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=ci/lib.sh
source ci/lib.sh

work="${1:?usage: ci/bundle-free-disk.sh WORK_DIR}"
if [[ "${GITHUB_ACTIONS:-}" != true ]]; then
  mkdir -p "$work"
  echo "not on a GitHub runner: nothing removed, work directory $work"
  exit 0
fi
df -h / /mnt | ci_summary
sudo rm -rf /usr/share/dotnet /usr/local/lib/android /opt/ghc /opt/hostedtoolcache/CodeQL \
  /usr/local/share/boost /usr/local/julia* /usr/share/swift
sudo mkdir -p "$work"
sudo chown "$(id -u):$(id -g)" "$work"
df -h / /mnt | ci_summary
