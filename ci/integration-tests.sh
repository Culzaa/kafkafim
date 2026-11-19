#!/usr/bin/env bash
# Job "Integration tests": Failsafe only (*IntegrationTest, Testcontainers). The build job
# already runs Spotless, Checkstyle and Surefire on the same commit, so they are skipped here.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

./mvnw -B -ntp verify \
  -Dtest=NONE -Dsurefire.failIfNoSpecifiedTests=false \
  -Dspotless.check.skip=true -Dcheckstyle.skip=true -Djacoco.check.skip=true
