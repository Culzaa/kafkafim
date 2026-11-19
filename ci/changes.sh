#!/usr/bin/env bash
# Job "Detect changes", GitHub only (GitLab skips jobs with `rules: changes`): decides which gate
# jobs a pull request needs (docs/DELIVERY.md, "Docs-only pull requests"). Skipped jobs report
# success, so the check names stay usable as required checks; a workflow-level paths-ignore
# would leave them pending instead. Fails open: if this job fails, its outputs are empty and
# every job runs.
#
# Runs for pull requests only (the workflow step decides pushes and merge queue runs, which run
# everything, before a checkout). GITHUB_OUTPUT: the file the outputs are appended to.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=ci/lib.sh
source ci/lib.sh

# What merging the PR would change. --no-renames lists both sides of a rename, so renaming code
# to *.md is not docs-only. quotePath=false keeps non-ASCII paths unquoted; git still quotes a
# path containing a control character (tab, newline, ...), " or \ ("docs/a\tb.md"). It then
# matches no docs pattern and counts as non-docs (the safe side: build, integration and image
# run), but it matches no helm or grafana pattern.
changed="$(ci_tmp)/changed"
git -c core.quotePath=false diff --no-renames --name-only HEAD^1 HEAD >"$changed"
sed 's/^/  /' "$changed"
docs_only=false helm=false grafana=false
[[ -s "$changed" ]] && docs_only=true
while IFS= read -r f; do
  case "$f" in
    src/*) docs_only=false ;; # Maven packages src/main/resources, *.md included
    *.md | docs/* | LICENSE) ;;
    *) docs_only=false ;;
  esac
  case "$f" in
    # deploy/grafana too: the demo chart renders the dashboards and their provider. The CI
    # definitions and scripts are inputs of the chart jobs too.
    charts/* | deploy/local/* | deploy/grafana/* | .github/workflows/ci.yml | .github/keys/* | ci/*) helm=true ;;
  esac
  case "$f" in
    deploy/grafana/* | .github/workflows/ci.yml | ci/*) grafana=true ;;
  esac
done <"$changed"
echo "docs_only=$docs_only helm=$helm grafana=$grafana"
printf 'docs_only=%s\nhelm=%s\ngrafana=%s\n' "$docs_only" "$helm" "$grafana" >>"${GITHUB_OUTPUT:?}"
