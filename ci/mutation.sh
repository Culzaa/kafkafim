#!/usr/bin/env bash
# Job "Mutation tests": PIT on the pure-logic route and decode packages, with the floor from
# pom.xml.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

./mvnw -B -ntp test-compile org.pitest:pitest-maven:mutationCoverage -Djacoco.skip=true
