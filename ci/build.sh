#!/usr/bin/env bash
# Job "Build and test": Spotless, Checkstyle, unit and architecture tests, the jar. The
# integration tests run in their own job, and the JaCoCo floor in the coverage job.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

./mvnw -B -ntp verify -DskipITs -Djacoco.check.skip=true
