# ADR-0046: Google style guides for shell, Markdown, documentation prose and JSON/YAML

Status: accepted · Date: 2026-11-16 · Extends [ADR-0024](0024-google-java-style.md)

## Decision
ADR-0024 puts Java under the Google Java Style Guide, enforced in the build. The rest of the
repository follows the matching Google guides too. Where a tool can enforce a rule, CI fails on a
violation; where none can, the guide is the review standard.

| Area | Guide | Enforcement |
|---|---|---|
| Shell (`*.sh`) | [Shell Style Guide](https://google.github.io/styleguide/shellguide.html) | `shellcheck` and `shfmt` on every tracked `*.sh`, CI job "Shell style" (this ADR) |
| Markdown | [Markdown style guide](https://google.github.io/styleguide/docguide/style.html) | Tracked in #266 |
| Documentation prose | [Documentation best practices](https://google.github.io/styleguide/docguide/best_practices.html) and the [developer documentation style guide](https://developers.google.com/style) | Review standard; tracked in #267 |
| JSON | [JSON Style Guide](https://google.github.io/styleguide/jsoncstyleguide.xml) | Tracked in #268 |
| YAML | no Google guide exists; `.editorconfig` (2 spaces) and the repository's existing conventions | Tracked in #268 |

Each tracking issue lands its own check in its own pull request and links this ADR. Where Google
publishes no guide (YAML), nothing is invented: the existing conventions stay.

### Shell
- `shellcheck` (pinned, default severity, `-x`) and `shfmt` (pinned, `-d`) run on every file
  `git ls-files '*.sh'` lists. A new script is covered without touching CI.
- `shfmt` reads `.editorconfig` (`[*.sh]`: 2 spaces, `switch_case_indent = true`), so editors and
  CI agree. `shfmt -w` rewrites; `shfmt -d` is the CI check.
- Scripts start with `#!/usr/bin/env bash` and a header comment (purpose and usage), use
  `set -euo pipefail`, `[[ ]]`, quoted expansions, `local` variables, lowercase function names,
  uppercase for constants and environment variables, and a `main "$@"` at the bottom.
- Not enforced: the guide's 80-column limit (command lines with long URLs, `awk` programs and
  messages exceed it, and the guide itself allows long strings) and Bash-only constructs, which the
  guide permits.
- Both tools are pinned by version and, in CI, by SHA-256 of the downloaded release asset.

## Alternatives
- Enforce only `shellcheck`: misses layout, and layout is what reviews argue about.
- `shfmt -i 2 -ci` flags in CI instead of `.editorconfig`: duplicates the setting, and editors
  would not follow it.
- A pre-commit framework: another tool for contributors to install; CI plus documented commands
  is enough for three scripts.

## Consequences
A badly formatted or unquoted script fails "Shell style". The check is not yet in the branch
protection's required list; adding it is Matan's call. The three existing scripts were reformatted
once and a few shellcheck findings were fixed with no behaviour change.
