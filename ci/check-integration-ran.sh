#!/usr/bin/env bash
# Job "Integration tests": Failsafe exits 0 when its **/*IntegrationTest.java include matches
# nothing, so a discovery break (rename, pom edit, a swallowed fork crash) would turn this
# required check green with nothing run. Every *IntegrationTest source file must have produced a
# report.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=ci/lib.sh
source ci/lib.sh

expected=$(find src/test -name '*IntegrationTest.java' | wc -l)
ran=$(find target/failsafe-reports -name 'TEST-*IntegrationTest.xml' 2>/dev/null | wc -l)
echo "integration test classes in src/test: $expected, with a Failsafe report: $ran"
if [[ "$expected" -eq 0 || "$ran" -lt "$expected" ]]; then
  ci_error "expected a Failsafe report for each of $expected *IntegrationTest classes, found $ran"
  exit 1
fi
