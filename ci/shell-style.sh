#!/usr/bin/env bash
# Job "Shell style" (ADR-0046; Google Shell Style Guide): shellcheck and shfmt on every
# tracked *.sh. Needs shellcheck and shfmt on the PATH.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=ci/lib.sh
source ci/lib.sh

mapfile -d '' scripts < <(ci_files '*.sh')
[[ ${#scripts[@]} -gt 0 ]] || {
  echo "no tracked *.sh files" >&2
  exit 1
}
shellcheck -x "${scripts[@]}"
shfmt -d "${scripts[@]}" # indent from .editorconfig ([*.sh]: 2 spaces)
