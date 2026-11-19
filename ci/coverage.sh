#!/usr/bin/env bash
# Job "Coverage": merges the unit and integration coverage data, writes the report and enforces
# the floor from pom.xml.
#
#   ci/coverage.sh DATA_DIR    DATA_DIR holds jacoco.exec and jacoco-it.exec (it may be target/)
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

data="${1:?usage: ci/coverage.sh DATA_DIR}"
mkdir -p target
# GitHub downloads both files into one directory; GitLab restores them into target/ already.
if [[ "$(cd "$data" && pwd)" != "$PWD/target" ]]; then cp "$data/jacoco.exec" "$data/jacoco-it.exec" target/; fi
./mvnw -B -ntp -DskipTests -Dspotless.check.skip=true -Dcheckstyle.skip=true compile \
  jacoco:merge@merge jacoco:report@report jacoco:check@check
