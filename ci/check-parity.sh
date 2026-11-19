#!/usr/bin/env bash
# Fails when the two CI definitions drift apart (ADR-0057). Runs in both: the "YAML format" job
# of ci.yml and of .gitlab-ci.yml call it. Reads ci/parity.map and compares it with
# .github/workflows/ci.yml, .github/workflows/release.yml and .gitlab-ci.yml. It fails when
#   - a job of ci.yml or .gitlab-ci.yml has no row in the map, or a row names a job that does
#     not exist;
#   - the scripts a job calls differ from the row (so a job on one side calls a script, or a
#     different script, that its counterpart does not);
#   - a ci/*.sh is in no row, or a row names a script that does not exist;
#   - an image the GitLab pipeline names through KAFKAFIM_IMAGE_PREFIX is not one the
#     development bundle takes (tools/airgap-dev/pins.txt);
#   - release.yml and the GitLab release disagree on the tag format (once the map has a release row);
#   - .gitlab-ci.yml or a ci/release-* script builds an air-gap bundle (not wanted in GitLab).
# Needs only bash, awk, grep and sed.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=ci/lib.sh
source ci/lib.sh

readonly GH=.github/workflows/ci.yml REL=.github/workflows/release.yml GL=.gitlab-ci.yml MAP=ci/parity.map
bad=0
fail() {
  ci_error "$*"
  bad=1
}

#######################################
# Prints "job<TAB>script script ..." for every job of a workflow or pipeline file, scripts
# sorted without the ci/ prefix.
# Arguments:
#   File, then "github" (jobs are the 2-space keys under jobs:) or "gitlab" (column-0 keys that
#   are not reserved words or hidden templates)
#######################################
jobs_of() {
  awk -v style="$2" '
    function flush(   s, n, i, j, t, a, out) {
      if (job != "") {
        n = 0
        for (s in refs) a[++n] = s
        for (i = 2; i <= n; i++) {
          t = a[i]
          for (j = i - 1; j >= 1 && a[j] > t; j--) a[j + 1] = a[j]
          a[j + 1] = t
        }
        out = ""
        for (i = 1; i <= n; i++) out = out (i > 1 ? " " : "") a[i]
        print job "\t" out
      }
      job = ""; delete refs
    }
    BEGIN { split("stages variables workflow include default image services cache before_script after_script", r, " "); for (i in r) reserved[r[i]] = 1 }
    style == "github" && /^jobs:/ { in_jobs = 1; next }
    style == "github" && /^[^ #]/ { flush(); in_jobs = 0 }
    {
      if (style == "github" && in_jobs && match($0, /^  [A-Za-z][A-Za-z0-9_-]*:[ ]*$/)) {
        flush(); job = $0; sub(/^  /, "", job); sub(/:.*/, "", job); next
      }
      if (style == "gitlab" && match($0, /^[a-z][A-Za-z0-9_-]*:/)) {
        flush(); k = $0; sub(/:.*/, "", k)
        if (!(k in reserved)) job = k
        next
      }
      if (style == "gitlab" && /^[^ #\t]/) flush()
    }
    job != "" {
      line = $0
      while (match(line, /ci\/[a-z][a-z0-9-]*\.sh/)) {
        refs[substr(line, RSTART + 3, RLENGTH - 3)] = 1
        line = substr(line, RSTART + RLENGTH)
      }
    }
    END { flush() }
  ' "$1"
}

gh_jobs="$(jobs_of "$GH" github)"
gl_jobs="$(jobs_of "$GL" gitlab)"
rel_jobs="$(jobs_of "$REL" github | cut -f1)"
[[ -n "$gh_jobs" ]] || fail "found no jobs in $GH"
[[ -n "$gl_jobs" ]] || fail "found no jobs in $GL"

# Scripts and jobs of a file, by job name; empty when the job is absent.
scripts_of() { awk -F'\t' -v j="$2" '$1 == j {print $2}' <<<"$1"; }
has_job() { awk -F'\t' -v j="$2" '$1 == j {f = 1} END {exit !f}' <<<"$1"; }
sorted_words() { tr ', ' '\n' <<<"$1" | { grep -v '^$' || true; } | sort -u | tr '\n' ' ' | sed 's/ $//'; }

mapped_gh="" mapped_gl="" mapped_scripts=""
while IFS='|' read -r kind ghj glj shared ghonly glonly _; do
  kind="${kind// /}"
  [[ -n "$kind" && "$kind" != \#* ]] || continue
  ghj="${ghj// /}" glj="${glj// /}"
  shared="${shared//[[:space:]]/}"
  ghonly="${ghonly//[[:space:]]/}"
  glonly="${glonly//[[:space:]]/}"
  glonly="${glonly%%#*}"
  mapped_scripts+=",$shared,$ghonly,$glonly"
  case "$kind" in
    ci)
      mapped_gh+=" $ghj"
      mapped_gl+=" $glj"
      if [[ "$ghj" != - ]]; then
        has_job "$gh_jobs" "$ghj" || fail "$MAP: job '$ghj' is not in $GH"
        want="$(sorted_words "$shared,$ghonly")"
        got="$(scripts_of "$gh_jobs" "$ghj")"
        [[ "$got" == "$want" ]] || fail "$GH job '$ghj' calls [$got], the map says [$want]"
      fi
      if [[ "$glj" != - ]]; then
        has_job "$gl_jobs" "$glj" || fail "$MAP: job '$glj' is not in $GL"
        want="$(sorted_words "$shared,$glonly")"
        got="$(scripts_of "$gl_jobs" "$glj")"
        [[ "$got" == "$want" ]] || fail "$GL job '$glj' calls [$got], the map says [$want]"
      fi
      ;;
    release)
      grep -qxF "$ghj" <<<"$rel_jobs" || fail "$MAP: job '$ghj' is not in $REL"
      mapped_gl+=" ${glj//,/ }"
      got=""
      for j in ${glj//,/ }; do
        has_job "$gl_jobs" "$j" || fail "$MAP: job '$j' is not in $GL"
        got+=" $(scripts_of "$gl_jobs" "$j")"
      done
      got="$(sorted_words "$got")"
      want="$(sorted_words "$glonly")"
      [[ "$got" == "$want" ]] || fail "$GL jobs '$glj' call [$got], the map says [$want]"
      ;;
    support) ;;
    *) fail "$MAP: unknown kind '$kind'" ;;
  esac
done <"$MAP"

# Every job of both files is mapped.
while IFS=$'\t' read -r j _; do
  [[ -z "$j" ]] || [[ " $mapped_gh " == *" $j "* ]] || fail "$GH job '$j' has no row in $MAP"
done <<<"$gh_jobs"
while IFS=$'\t' read -r j _; do
  [[ -z "$j" ]] || [[ " $mapped_gl " == *" $j "* ]] || fail "$GL job '$j' has no row in $MAP"
done <<<"$gl_jobs"

# Every script is mapped, and every mapped script exists.
for f in ci/*.sh; do
  s="${f#ci/}"
  [[ ",$mapped_scripts," == *",$s,"* ]] || fail "$f is in no row of $MAP"
done
for s in $(tr ',' ' ' <<<"$mapped_scripts"); do
  [[ -f "ci/$s" ]] || fail "$MAP names ci/$s, which does not exist"
  [[ -x "ci/$s" ]] || fail "ci/$s is not executable"
done

# The images the GitLab pipeline pulls through the prefix are images the bundle carries.
pinned="$(sed -n 's/^image=//p' tools/airgap-dev/pins.txt | sed -E 's/ *#.*//')"
while IFS= read -r img; do
  [[ -n "$img" && "$img" != kafkafim-ci:* ]] || continue # kafkafim-ci is built from the bundle by ci/build-tools-image.sh
  grep -qxF "$img" <<<"$pinned" || fail "$GL names image $img, which tools/airgap-dev/pins.txt does not list (the bundle would not carry it)"
done < <(grep -hoE '\$\{?KAFKAFIM_IMAGE_PREFIX\}?/[^"'"'"' ]+' "$GL" ci/gitlab/*.yml 2>/dev/null | sed -E 's#^\$\{?KAFKAFIM_IMAGE_PREFIX\}?/##' | sort -u)

# The release: the tag format is the same on both sides, and nothing builds an air-gap bundle
# (owner's decision, issue 480).
if grep -q '^release|' "$MAP"; then
  gh_tag="$(grep -oE '\^v\[0-9\]\+[^"]*\$' "$REL" | head -n 1 || true)"
  gl_tag="$(grep -oE '\^v\[0-9\]\+[^'"'"' ]*\$' ci/release-validate-tag.sh | head -n 1 || true)"
  if [[ -z "$gh_tag" || "$gh_tag" != "$gl_tag" ]]; then
    fail "release tag format differs: $REL has [$gh_tag], ci/release-validate-tag.sh has [$gl_tag]"
  fi
fi
shopt -s nullglob
if grep -nE 'airgap-dev-bundle\.sh (build|verify)|airgap\.tar|SHA256SUMS' "$GL" ci/release-*.sh >&2; then
  fail "the GitLab pipeline must not build air-gap bundles"
fi

if [[ "$bad" -ne 0 ]]; then exit 1; fi
echo "CI definitions agree: $(grep -c . <<<"$gh_jobs") GitHub jobs, $(grep -c . <<<"$gl_jobs") GitLab jobs, $(find ci -name '*.sh' | wc -l | tr -d ' ') scripts"
