#!/usr/bin/env bash
# Job "YAML format": XML indents with 2 spaces (.editorconfig); pom.xml is the one declared tab
# exception (#280).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=ci/lib.sh
source ci/lib.sh

mapfile -d '' xml < <(ci_files '*.xml')
files=()
for f in "${xml[@]}"; do [[ "$f" == pom.xml ]] || files+=("$f"); done
if [[ ${#files[@]} -gt 0 ]] && grep -nP '\t' -- "${files[@]}"; then
  ci_error "tab indentation in XML; use 2 spaces (.editorconfig)"
  exit 1
fi
