# Contributing

## Branches

- `main` is always releasable. Nobody commits to it directly; every change arrives
  through a pull request from a short-lived branch.
- One branch, one concern. Name the branch `<type>/<short-kebab-description>`, where `type`
  is one of the commit types below: `feat/route-header-conditions`,
  `fix/offset-tracker-revoke`, `docs/runbook`, `ci/helm-unittest`.
- Rebase on `main` before you open or update a pull request. Merges into `main`
  are **squash merges** only: a pull request becomes one commit whose subject is
  the PR title and whose body is the PR description, so write both in
  Conventional Commit form. Keep commits inside the branch atomic for review.

## Commits

[Conventional Commits](https://www.conventionalcommits.org/):

```text
<type>(<scope>): <imperative summary, ≤ 72 chars>

<why, not what; wrap at 72>
```

- Types: `feat`, `fix`, `docs`, `build`, `ci`, `test`, `refactor`, `perf`, `chore`.
- Scopes: `kafka`, `es`, `config`, `route`, `decode`, `dlq`, `supervisor`,
  `telemetry`, `helm`, `docker`, `ci`, `deps`, `style`, `docs`. Omit when global.
- One commit, one responsibility. A commit builds and passes `./mvnw verify` on its
  own. A commit that changes only documentation (`*.md` outside `src/`, `docs/**`, `LICENSE`)
  passes `./mvnw -DskipTests verify`, matching CI, which skips the Java and image jobs for
  docs-only pull requests (see [Delivery](docs/DELIVERY.md)). Put formatting-only changes in
  their own commit (`style:`).
- Breaking changes carry `!` after the type and a `BREAKING CHANGE:` footer.

## Before you push

```bash
./mvnw spotless:apply
```

```bash
./mvnw verify
```

`verify` runs Google Java Style checks (Spotless + Checkstyle), unit tests,
Testcontainers integration tests, and ArchUnit rules. CI runs the same checks, split into
"Build and test" (`-DskipITs`) and a parallel "Integration tests" job, plus the
Dockerfile build and Helm checks. All must pass before merge. `verify` also measures
JaCoCo coverage of unit and integration tests together and enforces a floor on it (about 94 %
of lines, 87 % of branches); the report is in `target/site/jacoco/index.html`. CI checks the
floor in a separate "Coverage" job (see [DELIVERY.md](docs/DELIVERY.md#coverage-floor)).
A run with `-DskipITs` has only unit coverage, so add `-Djacoco.check.skip=true` to it.
A separate "Mutation tests" job runs PIT on the `route` and `decode` packages and fails below a
95 % mutation score; it is not part of `verify` (see
[DELIVERY.md](docs/DELIVERY.md#mutation-testing)). To run it locally:
`./mvnw test-compile org.pitest:pitest-maven:mutationCoverage`.
A docs-only change needs only `./mvnw -DskipTests verify`; CI skips the Java and image jobs for it.

Shell scripts follow the [Google Shell Style Guide](https://google.github.io/styleguide/shellguide.html)
(ADR-0046). If you change a `*.sh` file, run `shellcheck` and `shfmt` (2-space indent, read from
`.editorconfig`); CI's "Shell style" job runs the same pinned versions on every tracked script:

```bash
shfmt -w $(git ls-files '*.sh')            # rewrite in place
shellcheck -x $(git ls-files '*.sh') && shfmt -d $(git ls-files '*.sh')   # what CI checks
```

If you don't have them installed, use `nix shell nixpkgs#shellcheck nixpkgs#shfmt -c sh -c '...'`, or
`brew install shellcheck shfmt`.

Markdown (`*.md`) follows the [Google Markdown Style Guide](https://google.github.io/styleguide/docguide/style.html)
(ADR-0046), checked by markdownlint-cli2; the rules and their exceptions (tables, code blocks, and
headings are exempt from the 100-column limit) are in `.markdownlint-cli2.jsonc`. CI's "Markdown
style" job runs the pinned version on every tracked `*.md`:

```bash
markdownlint-cli2          # what CI checks (globs come from the config)
markdownlint-cli2 --fix    # fixes blank lines around headings and lists, trailing spaces, ...
```

If you don't have it installed, use `nix shell nixpkgs#markdownlint-cli2 -c markdownlint-cli2`, or
`npx markdownlint-cli2@0.24.0`.

### Prose checklist for documentation changes

The linter checks Markdown syntax, not wording. Documentation follows the
[Google developer documentation style guide](https://developers.google.com/style). Authors and
reviewers (including the reviewer agent) check every change to a `*.md` file against this list:

- Address the reader as "you" and use the imperative for instructions ("Run `helm upgrade`"),
  not "we", "the user", or "one".
- Use the active voice and the present tense: "The supervisor restarts the pipeline", not "The
  pipeline will be restarted".
- Write headings in sentence case ("Rotate a secret", not "Rotate A Secret").
- Use descriptive link text that names the target ("see the [Runbook](docs/RUNBOOK.md)"), never
  "here" or "this page".
- Use a numbered list for ordered steps and a bulleted list for everything else. Start each step
  with a verb, and keep one action per step.
- Cut filler such as "simply", "just", "easily", "please", and "obviously".
- Use the Oxford comma in lists of three or more.
- Put commands, file names, keys, and values in code font, and keep them exact.
- A pure wording change must not alter a command, value, number, or behaviour. Behaviour changes
  update the docs in the same pull request.

The `*IntegrationTest` classes run in three parallel Failsafe JVMs (each class starts
its own containers, so up to three sets run at once). On a smaller machine, or
to read one class's log on its own, run fewer forks:

```bash
./mvnw -Dit.forkCount=1 verify
```

Integration tests must be safe to run alongside each other: no fixed host ports (pass
`--server.port=0` as a command-line argument when starting the app, since
`application.yaml`'s 8080 beats default properties), no shared files outside a
`@TempDir`, and no containers shared between classes.

## Pull requests

- Write the title in Conventional Commit form, and use the body to say why you made the change and
  how you verified it. The title and body become the squash commit on `main`, so write them for
  the git log, not for chat.
- Every pull request gets an independent review before merge (a reviewer agent or a
  teammate). Fix findings on the branch. Approval plus green CI merges the pull request, and
  the branch is deleted.
- Keep pull requests small enough to review in one sitting. If a change needs two concerns,
  open two pull requests.
- Design changes come with an ADR in `docs/decisions/` in the same pull request.
