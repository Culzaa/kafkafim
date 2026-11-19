#!/usr/bin/env bash
# Job "Coverage" (always): prints line and branch coverage from the JaCoCo report.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=ci/lib.sh
source ci/lib.sh

[[ -f target/site/jacoco/jacoco.csv ]] || exit 0
awk -F, 'NR>1 {lm+=$8; lc+=$9; bm+=$6; bc+=$7}
  END {printf "Line coverage: %.1f %%, branch coverage: %.1f %%\n", 100*lc/(lm+lc), 100*bc/(bm+bc)}' \
  target/site/jacoco/jacoco.csv | ci_summary
