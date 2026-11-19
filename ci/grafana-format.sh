#!/usr/bin/env bash
# Job "Grafana format": dashboards must be canonical JSON (the YAML is checked by "YAML format").
# Google JSON Style Guide where Grafana's schema allows: valid JSON, 2-space indent, stable
# (sorted) key order, no comments, no trailing commas. Grafana fixes its own field names, so the
# guide's naming rules do not apply. Canonical form is `jq -S --indent 2 .`; fix a file with
# `jq -S --indent 2 . f.json > t && mv t f.json`. Needs jq on the PATH.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=ci/lib.sh
source ci/lib.sh

mapfile -t files < <(find deploy/grafana -name '*.json' | sort)
[[ ${#files[@]} -gt 0 ]] || {
  echo "no deploy/grafana JSON found" >&2
  exit 1
}
jq --version
bad=0
for f in "${files[@]}"; do
  jq empty "$f" || {
    bad=1
    continue
  }
  if ! jq -S --indent 2 . "$f" | diff -u "$f" -; then
    ci_error "$f is not canonical; run: jq -S --indent 2 . $f > t && mv t $f"
    bad=1
  fi
done
exit "$bad"
