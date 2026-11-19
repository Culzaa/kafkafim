#!/usr/bin/env bash
# Job "Integration tests", last step (always): prints how each image pre-pull ended and warns
# about the ones that failed.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=ci/lib.sh
source ci/lib.sh

for f in "$(ci_tmp)"/pulls/*.log; do
  [[ -e "$f" ]] || continue
  tail -n 1 "$f"
  if grep -q '^FAILED' "$f"; then ci_warning "pre-pull failed: $(cat "$f")"; fi
done
