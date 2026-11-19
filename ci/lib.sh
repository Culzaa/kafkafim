#!/usr/bin/env bash
# Helpers shared by the scripts in ci/. Sourced, not run. The scripts are the one place where
# the check commands live: .github/workflows/ci.yml and .gitlab-ci.yml both call them
# (docs/DELIVERY.md, "CI on GitLab"; ADR-0057). Only what depends on the CI system (action
# pins, caches, tool downloads, annotations) differs, and that stays out of the scripts.

#######################################
# Prints an error annotation: a GitHub workflow command on GitHub, plain text elsewhere.
# Arguments:
#   Message words
#######################################
ci_error() {
  if [[ "${GITHUB_ACTIONS:-}" == true ]]; then echo "::error::$*" >&2; else echo "ERROR: $*" >&2; fi
}

#######################################
# Prints a warning annotation: a GitHub workflow command on GitHub, plain text elsewhere.
# Arguments:
#   Message words
#######################################
ci_warning() {
  if [[ "${GITHUB_ACTIONS:-}" == true ]]; then echo "::warning::$*" >&2; else echo "WARNING: $*" >&2; fi
}

#######################################
# Prints the directory for scratch files of a job: the runner's temp directory on GitHub, else
# TMPDIR. Created if missing.
# Outputs:
#   The directory
#######################################
ci_tmp() {
  local d="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
  mkdir -p "$d"
  echo "${d%/}"
}

#######################################
# Copies stdin to stdout and, on GitHub, to the job summary.
#######################################
ci_summary() {
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then tee -a "$GITHUB_STEP_SUMMARY"; else cat; fi
}

#######################################
# Lists the repository's files matching shell globs, NUL separated, relative to the current
# directory. Tracked files when git can say (GitHub, a developer checkout); else every file
# below the current directory except .git, target and node_modules, which is the same set in
# the fresh clone of a GitLab job (the job images carry no git).
# Arguments:
#   One or more globs such as '*.sh'
#######################################
ci_files() {
  if command -v git >/dev/null 2>&1 && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git ls-files -z -- "$@"
  else
    local g args=()
    for g in "$@"; do args+=(-o -name "$g"); done
    find . \( -name .git -o -name target -o -name node_modules \) -prune -o -type f \
      \( "${args[@]:1}" \) -print0 | sed -z 's#^\./##' | sort -z
  fi
}

#######################################
# Exits 1 with a message unless every named tool is on the PATH.
# Arguments:
#   Tool names
#######################################
ci_need() {
  local t
  for t in "$@"; do
    command -v "$t" >/dev/null 2>&1 || {
      ci_error "$t is not on the PATH (the job image or the workflow step must provide it)"
      exit 1
    }
  done
}
