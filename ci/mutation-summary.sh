#!/usr/bin/env bash
# Job "Mutation tests" (always): counts the mutants by status.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=ci/lib.sh
source ci/lib.sh

[[ -f target/pit-reports/mutations.xml ]] || exit 0
# Every <mutation> element has a status attribute: KILLED, SURVIVED, NO_COVERAGE, ...
{
  echo 'Mutants by status:'
  { grep -o '<mutation [^>]*status=.[A-Z_]*' target/pit-reports/mutations.xml || true; } |
    sed 's/.*status=.//' | sort | uniq -c | sort -rn
} | ci_summary
