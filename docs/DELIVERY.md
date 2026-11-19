# Delivery: CI, CD, registries, air gap

Development happens on the internet (GitHub). Production is air-gapped: nothing is
deployed from CI. CI proves the application works; CD publishes a versioned image and
chart that the team transfers into the air-gapped environment and deploys there.

## Principles

1. **The jar that passed tests is the jar in the image.** CI builds once, tests, then
   the Dockerfile only wraps `target/*.jar`. No second compile inside Docker.
2. **Everything that leaves CI is versioned, signed and self-describing.** Image,
   chart, SBOM, and checksums share one semver, derived from a git tag.
3. **Nothing in the image or chart reaches the internet at runtime.** No init
   containers, no subcharts, no downloads. The chart's image reference is fully
   overridable for the internal registry.
4. **Publishing is cheap and GitHub-native.** GitHub Container Registry (GHCR) for
   both images and OCI Helm charts, authenticated with the workflow token.

## Versioning

- Semantic versions, driven by git tags `vMAJOR.MINOR.PATCH`. No version-bump
  commits: the pom uses the CI-friendly `${revision}` property (default
  `0.1.0-SNAPSHOT`) and the release workflow passes `-Drevision=1.2.3`.
- Chart `version` and `appVersion` are both the tag, so one number identifies the
  whole deliverable when it is carried into the air gap.
- Image tags per release: `1.2.3`, `1.2`, `sha-<7>`. Nothing is published on pushes
  to `main`; only `v*` tags publish. `latest` is never published.

## Registries and names

| Artifact | Location |
|---|---|
| Container image | `ghcr.io/culzaa/kafkafim` |
| Helm chart (OCI) | `oci://ghcr.io/culzaa/charts/kafkafim` |
| Release bundle for the air gap | GitHub Release assets on the tag |

Alternatives considered: Docker Hub (anonymous pull rate limits hurt air-gap
transfer scripts), a GitHub Pages `index.yaml` Helm repo (legacy; OCI is the current
Helm distribution model and is what `helm pull`/`helm push` and `skopeo` understand).

## CI (`.github/workflows/ci.yml`) on pull requests and `main`

The command of each job below is a script in `ci/` that `.gitlab-ci.yml` calls too
([CI and release on GitLab](#ci-and-release-on-gitlab-the-air-gap)); `ci.yml` keeps the setup that
depends on GitHub (action pins, caches, tool downloads).

| Job | What it proves |
|---|---|
| `changes` (check "Detect changes") | Nothing; it classifies the pull request's changed paths for the jobs below (see "Docs-only pull requests"). On pushes to `main` and merge queue runs it selects every job. |
| `build` (check "Build and test") | `./mvnw verify -DskipITs`: Spotless, Checkstyle, compile, unit tests (Surefire defaults minus `*IntegrationTest`), ArchUnit package-boundary tests. Passes `-Djacoco.check.skip=true`, because this job has only unit coverage. Uploads the unit test reports, the unit coverage data and the jar. |
| `integration` (check "Integration tests") | Failsafe only (`*IntegrationTest`, Testcontainers Kafka + Elasticsearch + LGTM): `./mvnw verify -Dtest=NONE -Dsurefire.failIfNoSpecifiedTests=false -Dspotless.check.skip=true -Dcheckstyle.skip=true -Djacoco.check.skip=true`; the classes run in three parallel JVMs (`it.forkCount` in `pom.xml`). Runs in parallel with `build`, needing only `changes`; one job, not a matrix of shards, to stay well inside the Free plan's 20 concurrent jobs. Its first step starts pulling, in the background, every image named by a `DockerImageName.parse("...")` in `src/test` (so the list cannot drift from the tests), overlapping the pulls with Java setup and compilation; a test that starts a container before its pull finishes joins the daemon's in-flight pull. A step after Maven fails the job unless every `*IntegrationTest` source file produced a Failsafe report (Failsafe alone exits 0 when its include matches nothing). Uploads the Failsafe reports and the integration coverage data. Also skips the coverage floor. |
| `coverage` (check "Coverage") | Needs `build` and `integration`. Downloads both jobs' JaCoCo data, merges it, writes the HTML and CSV report (uploaded as `coverage-report`, with line and branch totals in the job summary) and enforces the coverage floor ([Coverage floor](#coverage-floor)). Not a required check. |
| `mutation` (check "Mutation tests") | Needs only `changes`; skipped on docs-only pull requests. Runs PIT (`./mvnw test-compile org.pitest:pitest-maven:mutationCoverage`) on the `route` and `decode` packages, uploads the HTML and XML report as `mutation-report`, with the count of mutants per status in the job summary, and fails when the mutation score drops below the floor ([Mutation testing](#mutation-testing)). Not a required check. |
| `image` (check "Container image") | Needs only `build`, so it runs while `integration` is still running. The Dockerfile builds from `build`'s jar (no push); `hadolint` on the Dockerfile; the container starts read-only and answers `/actuator/health/liveness` within 60 s. |
| `helm` (check "Helm chart") | `helm lint`, `helm template` with every `charts/kafkafim/ci/*-values.yaml`, `kubeconform -strict` (v0.8.0) against Kubernetes schemas, `helm unittest` (helm-unittest 1.2.1) for the group-rendering logic and every template-time `fail`. Helm is pinned (`azure/setup-helm` `version:`, currently v4.3.1, the same in `release.yml`); helm-unittest is installed from its signed release tarball: the tarball and its `.prov` are fetched through the GitHub API asset endpoint (not `releases/download/`), checked against pinned SHA-256s, and Helm verifies the provenance with the release key committed at `.github/keys/helm-unittest-public-key.asc` (fingerprint pinned in `ci.yml`). Both tools are kept in an `actions/cache` keyed on their pins, so once a passing run has warmed it (caches unused for 7 days are evicted) an outage of github.com release downloads or of the Go module proxy cannot fail the job. Checkout, the Actions cache service, `azure/setup-helm` (get.helm.sh), and kubeconform's schemas (raw.githubusercontent.com) are still needed on every run. The local demo's chart ([`deploy/local/kafkafim-demo`](../deploy/local/README.md)) gets `helm lint`, `helm template`, and `kubeconform -strict` too, plus a check that every `deploy/grafana/dashboards/*.json` is in its dashboards ConfigMap and that kafkafim's OTLP endpoint is the `apm-server` Service. The chart steps are skipped until the chart exists, and on pull requests that change none of `charts/**`, `deploy/local/**`, `deploy/grafana/**` (the demo chart renders the dashboards), `.github/workflows/ci.yml`, `.github/keys/**` or `ci/**`. The same job also checks [`deploy/grafana/`](../deploy/grafana/README.md), so the required checks stay the same: `jq empty` on every `dashboards/*.json`, `promtool check rules` on every `alerts/*.yaml` and `promtool test rules` on every `alerts/*.test.yaml` (run from `alerts/`, since the tests name their rule files relative to themselves); it fails if any of the three globs is empty. promtool comes from the Prometheus 3.15.1 `linux-amd64` release tarball, fetched through the GitHub API asset endpoint, checked against a pinned SHA-256 on every run and kept in its own `actions/cache`. These steps run on pull requests that change `deploy/grafana/**`, `ci/**` or `ci.yml`, and on every push to `main`. The job is skipped on pull requests that change none of `charts/**`, `deploy/local/**`, `.github/keys/**`, `deploy/grafana/**`, `ci/**`, and `ci.yml`. |
| `shell` (check "Shell style") | `shellcheck` v0.11.0 and `shfmt` v3.14.1 (release assets checked against pinned SHA-256s) on every `git ls-files '*.sh'`, and `actionlint` 1.7.12 (also SHA-256 pinned), which runs that shellcheck on the inline `run:` blocks of every workflow; `shfmt` takes its 2-space indent from `.editorconfig` ([ADR-0046](decisions/0046-google-style-guides-for-shell-markdown-prose-and-json-yaml.md)). No `needs`: runs on every pull request and push, docs-only included. Not a required check yet. |
| `yaml` (check "YAML format") | `yamllint` 1.37.1 with the repo-root `.yamllint.yaml` over the chart's `Chart.yaml`, `values.yaml`, `ci/` and `tests/`, `deploy/grafana`, `.github`, both compose files, and `src/main/resources` (not `charts/*/templates`, which are Helm templates); plus a check that no tracked XML except `pom.xml` contains a tab. No `needs`. Not a required check. |
| `markdown` (check "Markdown style") | `markdownlint-cli2` (action v25.0.0, bundling markdownlint-cli2 0.24.0) on every tracked `*.md`; rules and exceptions in `.markdownlint-cli2.jsonc` ([ADR-0046](decisions/0046-google-style-guides-for-shell-markdown-prose-and-json-yaml.md)). No `needs`: runs on every pull request and push, docs-only included. Not a required check yet. |

A new push to a pull request cancels the pull request's run still in progress. Runs on
`main` are never cancelled or replaced: each pushed commit has its own concurrency group
(keyed on the commit SHA), so every merged commit gets a complete run, including the
image build, even when several merges land within minutes.

### Coverage floor

JaCoCo measures unit tests (Surefire, `target/jacoco.exec`) and integration tests (Failsafe,
`target/jacoco-it.exec`). In the `verify` phase `pom.xml` merges the two files, writes the report to
`target/site/jacoco/`, and fails the build when the merged line coverage drops below 94 % or the
branch coverage below 87 %. The floors sit just below the values measured on 2026-11-17 (94.5 %
and 87.3 %). Raise them when coverage rises; never lower them to make a change pass.

CI's `build` and `integration` jobs each see only half of the data, so they pass
`-Djacoco.check.skip=true`. The `coverage` job checks the merged data of both. To reproduce it
locally, run `./mvnw verify` (needs Docker) and open `target/site/jacoco/index.html`.

### Mutation testing

PIT changes the compiled `route` and `decode` classes one small step at a time (a flipped
condition, a removed call, a changed return value) and runs the unit tests of those two packages
against each change. A change no test notices is a surviving mutant, and points at a missing or
weak assertion. Line coverage cannot show that: the code runs, but nothing checks the result.

The `pitest-maven` plugin (1.30.1) and `pitest-junit5-plugin` (1.2.3) are pinned in `pom.xml`. PIT
is not part of `verify`; the `mutation` job runs it. The scope is only these two packages, because
they are pure logic and the unit tests are fast; the other packages need containers to show
anything. The run measured on 2026-11-17 generated 511 mutants and killed 489 of them (95.7 %) in
about 30 seconds on a developer machine, with the same result in two runs. The job fails when the
score drops below 95 %. Raise the floor when the score rises; never lower it to make a change pass.

New code in `route` and `decode` is mutated from the day it lands, so a pull request that adds a
transform op (for example `remove`, `rename` or `set`) must kill its own mutants in the op's tests
and keep the score at or above the floor. After merging `remove` the score was 96.4 % (507 of 526
mutants). If a change dips below the floor, add tests for the surviving mutants in the same pull
request; do not lower the floor.

The surviving mutants that are left are mostly equivalent or not worth a test: boundary changes
in checks whose two sides behave the same, and calls to the validation helper that a second check
repeats further down. Read them in the report before adding a test.

To reproduce it locally, run the command below and open `target/pit-reports/index.html`.

```bash
./mvnw test-compile org.pitest:pitest-maven:mutationCoverage
```

### Docs-only pull requests

A pull request whose every changed path (both sides of a rename) is `*.md` outside
`src/` (Maven packages `src/main/resources`), under `docs/`, or `LICENSE` skips `build`,
`integration` and therefore `image`; `helm` runs only if the
chart, `deploy/local/`, `ci.yml`, `.github/keys/` or `deploy/grafana/` changed (a pull request that
changes
only `deploy/grafana/README.md` is docs-only but still gets the Grafana checks). Any other
file under `deploy/grafana/` is not docs, so the Java and image jobs run for it too. The paths are
what merging would change
(`git diff` between the PR merge commit and the base tip). The skip is a job-level `if:`,
not a workflow-level `paths-ignore`: GitHub reports a job skipped by `if:` as a passing
check, while a workflow that never starts leaves required checks pending. If `changes`
itself fails, its outputs are empty and every job runs. Pushes to `main`, merge queue runs and
`release.yml` always run everything. Locally, docs-only changes still run
`./mvnw -DskipTests verify` (see CONTRIBUTING.md).

Branch protection on `main` (a classic branch protection rule, in force since
2026-11-09):

- Four required status checks: "Build and test", "Integration tests", "Container
  image", and "Helm chart". "Detect changes" is not required. `image` does not wait
  for the integration tests, so "Integration tests" and "Container image" are both
  required: both must pass before a pull request merges. A job skipped by its `if:`
  (docs-only pull requests, or `helm` when none of `charts/**`, `deploy/local/**`, `ci.yml`,
  `.github/keys/**`, and `deploy/grafana/**` changed) reports as passing, so it does not block the merge.
- Branches must be up to date with `main` before merging (strict status checks).
- Administrators are included: the rule applies to them too.
- Force-pushes to `main` and deleting `main` are blocked.
- The repository allows squash merges only (merge commits and rebase merges are
  off), so every pull request lands as one commit.

The rule requires no approving review; reviews are a process rule (CONTRIBUTING.md).

Merges go through the GitHub merge queue (once its ruleset is enabled). `ci.yml` also triggers
on `merge_group` (`checks_requested`), so the four required checks run on the queue's temporary
`gh-readonly-queue/...` branch, which is `main` plus the queued pull requests. A queue run always
runs everything: `changes` takes the same path as a push to `main`, so docs-only pull requests are
not skipped there. Each queue run has its own concurrency group (keyed on the commit SHA) and is
never cancelled. It builds the image but pushes nothing; `ci.yml` never publishes, and
`release.yml` runs only on its own triggers.

Dependabot runs weekly, with grouped updates, for Maven, GitHub Actions, the Docker
base image, and the images in `compose.yaml` and `loadtest/compose.yaml` (one group). The
same images appear in `deploy/local/kafkafim-demo/values.yaml` and as `DockerImageName.parse`
constants in `src/test`, which Dependabot cannot read; a compose bump PR must update those by
hand, and the "YAML format" job's "Container image versions agree" step fails until every
place names one tag per image.
Every `uses:` in `.github/workflows` is pinned to a full commit SHA with the released version
in a trailing comment (a retagged action cannot change what runs); Dependabot's
`github-actions` ecosystem keeps both current. Checkouts set `persist-credentials: false`.
Dependabot ignores `io.opentelemetry.proto:opentelemetry-proto`. No BOM manages it, and
`pom.xml` declares it at the version `micrometer-registry-otlp` depends on. A
`maven-enforcer-plugin` `dependencyConvergence` rule fails `validate` when the two
differ. A Boot or Micrometer upgrade that moves Micrometer's proto version therefore fails
the build until `opentelemetry-proto.version` is bumped to match. The rule cannot notice
Micrometer dropping the dependency: `micrometer-registry-otlp` 1.18 (Boot 4.2) shades it,
which leaves only the direct path, so the rule passes. #127 tracks moving the artifact to
test scope and removing these guards in that upgrade.
Dependabot also ignores `io.opentelemetry.instrumentation:opentelemetry-logback-appender-1.0`
(ADR-0044). No BOM manages it either; `pom.xml` pins the instrumentation release built
against the Spring Boot BOM's OpenTelemetry SDK (instrumentation 2.N pairs with SDK
1.(N+34)), and it is bumped by hand in the Boot upgrade that moves the SDK.

## CD (`.github/workflows/release.yml`) on tag `v*`

0. Jobs `verify` and `bundle` start together on the tag, both with a `contents: read` token (no
   write, no `id-token`). Job `bundle` builds the air-gap development bundle
   ([below](#air-gap-development-bundle),
   [ADR-0058](decisions/0058-release-builds-and-publishes-the-air-gap-development-bundle.md)).
   Job `bundle-publish` needs both: it signs the bundle's checksums and creates the **draft**
   GitHub release with the parts. Job `release` (steps 2 onward) needs `bundle-publish`, so the
   image is pushed only after the development bundle was built, verified and attached. The release
   stays a draft until the last step of `release`.
1. Job `verify` (token: `contents: read` only) first validates the tag: it must match
   `^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$` (so `v1.0` or `vfoo` publishes nothing) and the
   tagged commit must be an ancestor of `origin/main` (a tag on an unmerged branch commit publishes
   nothing). No tag ruleset and no other gate: the release does not wait for CI on the commit
   (Matan, 2026-11-18, #404). Then it runs `./mvnw -Drevision=<ver> verify` (tests run again on the
   exact tagged commit). Job `release` needs it, so nothing below runs or publishes unless it
   passes, and the write, `packages` and `id-token` permissions are held by a job that runs no
   Maven; steps 2 onward are that job. `verify` uploads the tested jar as an artifact and `release`
   downloads it into `target/`, where the Dockerfile's `COPY target/*.jar` expects it, so the image
   contains the jar that passed the tests.
2. Multi-arch image (`linux/amd64`, `linux/arm64`) built with Buildx and pushed to
   GHCR with the tags above and BuildKit provenance attestations.
3. Image signed with Cosign (keyless, GitHub OIDC) in the classic `.sig`-tag format
   (`--new-bundle-format=false --use-signing-config=false`; cosign 3.1 rejects the first flag
   without the second), because that is the format `cosign save` and
   `cosign load` carry into and out of an OCI layout; `cosign verify` accepts it
   without flags. The signature is stored in GHCR next to the image.
4. SBOM (SPDX JSON, via Syft) generated from the pushed digest and attached to the
   release.
5. The **exact pushed digest** is saved with `cosign save` into an OCI layout
   directory. This carries the image index (every platform manifest, plus the
   BuildKit provenance stored inside the index) and the Cosign signature, so the
   bundle contains precisely the bytes that were signed. The release attaches no
   Cosign attestation (`cosign attest`), so the layout holds none. There is no
   second build.
6. Chart packaged with `--version <ver> --app-version <ver>`, pushed to
   `oci://ghcr.io/culzaa/charts`, signed with Cosign (keyless, the same classic
   format and flags as the image, on the pushed chart digest). The signature lives
   in GHCR only: the air-gap bundle carries the chart `.tgz` and its `SHA256SUMS`
   entry, not the chart's signature. The chart is verified on the internet side
   before transfer, and by checksum inside the air gap (see "Importing into the
   air-gapped environment").
7. **Air-gap bundle** `kafkafim-<ver>-airgap.tar.gz` attached to the GitHub Release:
   - `kafkafim-<ver>-image/` (OCI layout from `cosign save`: image index with its BuildKit
     provenance, signature)
   - `kafkafim-<ver>-image.digest` (the `ghcr.io/...@sha256:...` reference that was signed)
   - `kafkafim-<ver>.tgz` (the chart)
   - `kafkafim-<ver>.sbom.spdx.json`
   - `kafkafim-<ver>-grafana/` (the Prometheus alert rules and Grafana dashboards of this
     version, copied from [`deploy/grafana/`](../deploy/grafana/README.md):
     `alerts/kafkafim-alerts.yaml` and `dashboards/*.json`, plus `RUNBOOK.md` that the
     alerts' `runbook_url` annotations point to). The workflow copies them
     first, so a missing file fails the release before anything is published.
   - `SHA256SUMS` (every file in the bundle)
   - `IMPORT.md` (this document with the exact version filled in; the procedure is
     "Importing into the air-gapped environment" below)
8. GitHub Release (a draft since `bundle-publish`, made public by the last step) with auto-generated
   notes (a version with a `-`, such as `0.3.0-rc.1`, is marked a pre-release). Besides the bundle
   it carries the SBOM, the chart, the alert rules, and the dashboards as separate assets
   (`kafkafim-alerts.yaml`, `kafkafim-overview.json`, `kafkafim-elasticsearch.json`), so whoever
   runs Prometheus and Grafana can fetch them without the image. The release also carries the
   development bundle's parts, checksums and signature (see below).

The chart installs no alert rules and no dashboards (no `PrometheusRule`, no dashboard
ConfigMap): the production clusters have no Prometheus Operator CRDs. They travel in the
bundle instead, versioned with the image (decided on #91, 2026-11-13).

## Importing into the air-gapped environment

On a machine with internet access:

```bash
gh release download v1.2.3 --repo culzaa/kafkafim --pattern 'kafkafim-1.2.3-airgap.tar.gz'
```

```bash
cosign verify ghcr.io/culzaa/kafkafim:1.2.3 --certificate-identity-regexp 'https://github.com/culzaa/kafkafim/' --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

Releases signed before the repository moved to the `culzaa` organization (2026-11-18) carry
the old signing identity. They verify only with the old pattern
`--certificate-identity-regexp 'https://github.com/matabar/kafkafim/'`, and their image and
chart live under `ghcr.io/matabar`, not `ghcr.io/culzaa`.

The chart's signature is checked the same way, on this internet-side machine, before
you carry anything across (`cosign verify` accepts the classic format the release
signs with, without flags). The reference is the chart's GHCR OCI reference, tag
included:

```bash
cosign verify ghcr.io/culzaa/charts/kafkafim:1.2.3 --certificate-identity-regexp 'https://github.com/culzaa/kafkafim/' --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

The bundle's `kafkafim-1.2.3.tgz` is the chart that was pushed. To tie it to the
signed GHCR artifact, extract the bundle, pull the chart into a separate directory
(not the bundle's, or the pull overwrites the file being compared) and compare its
checksum with the bundle's `SHA256SUMS` (the two hashes must be identical):

```bash
helm pull oci://ghcr.io/culzaa/charts/kafkafim --version 1.2.3 --destination "$(mktemp -d)"
```

```bash
sha256sum <that directory>/kafkafim-1.2.3.tgz
```

```bash
grep ' ./kafkafim-1.2.3.tgz$' SHA256SUMS
```

The chart's signature is not in the bundle (only the image's is), so it cannot be
re-verified inside the air gap. There the chart's integrity check is `SHA256SUMS`,
which covers `kafkafim-1.2.3.tgz` (see the `sha256sum -c` below); carry it on
the trusted medium. `SHA256SUMS` is only as trustworthy as that
medium, which is why the signature is verified before transfer. (Checked with cosign
3.1.3 against the 0.3.0-rc.2 chart: `cosign verify` passes, and `helm pull` returns
a `.tgz` byte-identical to the release asset.)

The image check is the authoritative one for the image: online, `cosign` refreshes the Sigstore
trusted root
(Fulcio and Rekor keys) from Sigstore's TUF repository. To re-check the signature
inside the air gap later, take that trusted root across with the bundle. `cosign
initialize` refreshes the TUF cache; copy the `trusted_root.json` it verified out of
the cache (under `$TUF_ROOT` instead of `~/.sigstore/root` when that is set):

```bash
cosign initialize
```

```bash
cp ~/.sigstore/root/tuf-repo-cdn.sigstore.dev/targets/trusted_root.json .
```

Sigstore rotates these keys, so fetch a fresh copy for each import. `SHA256SUMS`
does not cover `trusted_root.json`, and it is the trust anchor for the check inside
the air gap, so carry it on the same trusted medium as the bundle (or note its
`sha256sum` here and compare it there). Then carry both across and, inside the air
gap, run the following commands in the directory that holds the extracted bundle and
`trusted_root.json`:

```bash
sha256sum -c SHA256SUMS
```

```bash
cosign load --dir kafkafim-1.2.3-image registry.internal/kafkafim/kafkafim:1.2.3
```

`cosign load` pushes the image index and its signature together, so the internal
registry holds the same signed artifact as GHCR. To re-check the signature inside
the air gap, pass the trusted root you carried across:

```bash
cosign verify registry.internal/kafkafim/kafkafim:1.2.3 --trusted-root trusted_root.json --certificate-identity-regexp 'https://github.com/culzaa/kafkafim/' --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

This needs no network access: the signature manifest carries the signing
certificate and its Rekor inclusion proof, which `cosign` checks against the keys in
`trusted_root.json` (it reports the transparency-log claim as "verified offline").
`--trusted-root` is what makes it work: without it, `cosign` tries to refresh TUF
metadata and fails, even with a `~/.sigstore` cache carried across. Don't add
`--offline`, which cosign 3.1 deprecates; it only turns a missing inclusion proof
into an error instead of an online lookup. The same command with `--local-image
kafkafim-1.2.3-image` in place of the registry reference checks the layout before
it is pushed. (Checked with cosign 3.1.3 against a local registry with outbound
network blocked, on a public image signed keyless in the same classic format.)
Without `cosign` inside the air gap, the checksum file is the integrity check there.
The SBOM describes the `linux/amd64` image, which is what production runs.

```bash
helm push kafkafim-1.2.3.tgz oci://registry.internal/charts
```

The infrastructure repository's CI applies the release to each Kubernetes cluster with
plain `helm upgrade --install` inside the air gap, using that cluster's values file;
there is no Argo CD or Flux. At import time, record the digest from the bundle in each
per-cluster values file, next to the internal registry:

```bash
cut -d@ -f2 kafkafim-1.2.3-image.digest
```

```yaml
image:
  registry: registry.internal/kafkafim
  digest: sha256:...   # the output above
```

```bash
helm upgrade --install kafkafim oci://registry.internal/charts/kafkafim --version 1.2.3 -f values-<cluster>.yaml
```

For a manual install from the extracted bundle, pass the same values as
`--set image.registry=registry.internal/kafkafim --set image.digest="$(cut -d@ -f2 kafkafim-1.2.3-image.digest)"`.

`image.digest` pins the pods to the exact image that was signed and checksummed,
so a tag moved or overwritten in the internal registry cannot change what runs.
`kafkafim-1.2.3-image.digest` holds the full reference
(`ghcr.io/culzaa/kafkafim@sha256:...`); the chart takes only the `sha256:<64 hex>`
part after `@` (the registry comes from `image.registry`), and its schema rejects
anything else. `cosign load` and `skopeo copy --all --preserve-digests` push the same
bytes, so the digest is the same in the internal registry. To check it, run the following
command, which prints `OK` or `MISMATCH`:

```bash
[ "sha256:$(skopeo inspect --raw docker://registry.internal/kafkafim/kafkafim:1.2.3 | sha256sum | cut -d' ' -f1)" = "$(cut -d@ -f2 kafkafim-1.2.3-image.digest)" ] && echo OK || echo MISMATCH
```

The pod image is then `registry.internal/kafkafim/kafkafim@sha256:...`;
the `app.kubernetes.io/version` label still reads `1.2.3` (the chart's appVersion).
Leave `image.tag` empty, or set it to `1.2.3` for a `:1.2.3@sha256:...` reference; never put
`1.2.3@sha256:...` in `image.tag` (the chart fails and points at `image.digest`).

If `cosign` is unavailable inside the air gap, use skopeo (1.18 or later, for the
`@<index>` source syntax) to push the image, then its signature in a second copy:

```bash
skopeo copy --all --preserve-digests oci:kafkafim-1.2.3-image:@0 docker://registry.internal/kafkafim/kafkafim:1.2.3
```

```bash
skopeo copy --preserve-digests oci:kafkafim-1.2.3-image:@1 "docker://registry.internal/kafkafim/kafkafim:$(cut -d@ -f2 kafkafim-1.2.3-image.digest | tr : -).sig"
```

The layout `cosign save` writes holds more than one manifest (the image index, then
the signature manifest, plus Cosign attestation manifests if any were attached; each
annotated `kind: dev.cosignproject.cosign/...`) and no
`org.opencontainers.image.ref.name`, so plain `oci:kafkafim-1.2.3-image` fails with
"more than one image in oci, choose an image" and a `:<tag>` reference matches
nothing; `:@0` selects the first entry of `index.json`, the image index. Check that
`jq -r '.manifests[0].annotations.kind' kafkafim-1.2.3-image/index.json` prints
`dev.cosignproject.cosign/imageIndex` (or read `index.json`), and run the digest
check above afterwards. `--preserve-digests` makes skopeo fail rather than convert
the manifests, which would change the digest. (Checked with cosign 3.1.3 and
skopeo 1.24.1.)

The second copy recreates what `cosign load` writes: Cosign finds a classic signature
under the tag `sha256-<hex>.sig` next to the image, where `sha256:<hex>` is the signed
digest from `kafkafim-1.2.3-image.digest`. `:@1` is the second entry of `index.json`,
the signature manifest; check that
`jq -r '.manifests[1].annotations.kind' kafkafim-1.2.3-image/index.json` prints
`dev.cosignproject.cosign/sigs`. If you skip the second copy, the internal registry holds
the image unsigned: `cosign verify` there fails with `no signatures found`. With both
copies, `cosign verify --trusted-root trusted_root.json` works on the internal
reference as after `cosign load`. A release has no Cosign
attestation; a layout that has one (a third entry, kind
`dev.cosignproject.cosign/atts`) takes a third copy, `:@2` to `sha256-<hex>.att`.
(Checked with cosign 3.1.3 and skopeo 1.24.1 against a local registry, signing with
a local key in the release's `--new-bundle-format=false` format: both copies keep
their digests, and `cosign verify` and `cosign verify-attestation` pass on the copy.)
Keyless verification of the copy was not tested; the release's certificate and
transparency-log entry are part of the signature manifest, which skopeo copies
unchanged.

### Alert rules and dashboards

The bundle carries the Prometheus alert rules and the Grafana dashboards that match
this version in `kafkafim-1.2.3-grafana/`, and `SHA256SUMS` covers them. `helm upgrade`
does not install them. Load them with each import, because metric names, labels,
and thresholds can change between versions:

- `alerts/kafkafim-alerts.yaml`: copy it to a path that the `rule_files:` of the
  Prometheus inside the air gap names, then reload Prometheus (`SIGHUP`, or
  `POST /-/reload` where `--web.enable-lifecycle` is set). For a Prometheus Operator,
  put the file's `groups:` list under the `spec:` of a `PrometheusRule` that carries
  the labels the Prometheus `ruleSelector` matches (without them the operator ignores
  it silently). Where
  `promtool` is available, check the file first:

  ```bash
  promtool check rules kafkafim-1.2.3-grafana/alerts/kafkafim-alerts.yaml
  ```

- `dashboards/kafkafim-overview.json` and `dashboards/kafkafim-elasticsearch.json`:
  import each in Grafana (*Dashboards → New → Import*), or copy them into the directory
  of a file provider. The UIDs are fixed (`kafkafim-overview`,
  `kafkafim-elasticsearch`), so importing a newer version replaces the old dashboard
  and links to it keep working. Both have a `datasource` variable; pick the
  Prometheus-compatible data source that holds kafkafim's OTLP metrics.

Every alert has a `runbook_url` annotation. In the release's copy it names the
`RUNBOOK.md` of this tag
(`https://github.com/culzaa/kafkafim/blob/v1.2.3/docs/RUNBOOK.md#...`, so section
anchors match the version that is running), which the air gap cannot open. The bundle
therefore carries `kafkafim-1.2.3-grafana/RUNBOOK.md`: host it inside the air gap (a
wiki, a static web server) and rewrite the prefix in the rules file before loading it,
keeping the `#...` anchors:

```bash
sed -i 's#https://github.com/culzaa/kafkafim/blob/v1.2.3/docs/RUNBOOK.md#https://docs.internal/kafkafim/RUNBOOK#' \
  kafkafim-1.2.3-grafana/alerts/kafkafim-alerts.yaml
```

Repeat it as part of each import, like the thresholds below. Anchors follow GitHub's
heading slugs, so a host that renders Markdown differently may need its own. The
runbook's relative links to other documents in `docs/` do not resolve from the lone copy.

Thresholds marked "tune" in the rules file are starting points. Keep local changes to
them as a patch applied after each import, since the next bundle's file replaces them.
`deploy/grafana/README.md` in the repository lists every alert with its runbook section.

## Air-gap development bundle

The release bundle above deploys a release. To keep *developing* inside the air gap there is a
second, separate bundle
([ADR-0056](decisions/0056-air-gap-development-bundle-is-built-by-a-script-and-not-published.md),
published with each release by
[ADR-0058](decisions/0058-release-builds-and-publishes-the-air-gap-development-bundle.md)).
The release workflow builds it with `tools/airgap-dev-bundle.sh` (jobs `bundle` and
`bundle-publish`),
proves it with the offline `verify`, and attaches it to the release as parts, because the bundle
is about 10 GB and a release asset is capped at 2 GiB per file:

| Release asset | Content |
|---|---|
| `kafkafim-dev-v1.2.3-airgap-dev.tar.part-aa`, `-ab`, ... | The bundle as one tar stream, cut into parts of 1900 MiB |
| `kafkafim-dev-v1.2.3-airgap-dev.parts.SHA256SUMS` | SHA-256 of every part |
| `kafkafim-dev-v1.2.3-airgap-dev.parts.SHA256SUMS.sigstore.json` | Cosign keyless signature of that checksums file, same identity as the image and chart |

Do not confuse it with `kafkafim-1.2.3-airgap.tar.gz`, the deployment bundle. Fetching,
verifying and reassembling it: [RUNBOOK § 7.1](RUNBOOK.md#71-get-the-bundle-and-carry-it-across).
GitLab has no bundle step (`ci/parity.map`); the bundle is how the GitLab side is fed. The job
frees runner disk (`ci/bundle-free-disk.sh`) and works on the runner's second disk; a manual run
of `release.yml` (`workflow_dispatch`) is a dry run that builds and verifies the bundle on a
branch and attaches nothing to a release. Contents:

| Directory | Content |
|---|---|
| `source/kafkafim.bundle` | The repository as a `git bundle`: `main` and every tag, full history |
| `maven-repo/` | Every dependency and Maven plugin of `verify`, PIT and the coverage goals, in the standard repository layout (`org/springframework/...`) with their `-sources.jar` and `-javadoc.jar` where Maven Central has them, and the Maven distribution the wrapper downloads |
| `jdk/` | Temurin of the pom's `java.version`: Linux x64, Linux aarch64, macOS aarch64, with `jdk.lock` |
| `images/linux-amd64/`, `images/linux-arm64/` | One `docker save` tar per image: the Dockerfile base image, the Testcontainers images and Ryuk, both compose files, the demo chart, kind's node image, hadolint, markdownlint-cli2 and the GitLab job images (`docker`, `docker:dind`, a JDK, Python); `images.lock` has the pinned digests |
| `tools/<platform>/` | The CI tools of `ci.yml` and `release.yml` for `linux-amd64`, `linux-arm64`, `darwin-arm64` (Helm, kubeconform, shellcheck, shfmt, actionlint, promtool, cosign, syft, kind, kubectl, jq), `tools/helm-unittest/` (signed plugin and key), `tools/yamllint-wheels/`, `tools/kubeconform-schemas/` (offline schema cache) and `tools/tools.lock` (version, SHA-256 and how it was checked) |
| `manifest.json` | The ref, the commit, the build time, the phases and every image |
| `SHA256SUMS` | Every file above, same format as the release bundle |
| `airgap-dev-bundle.sh`, `airgap-dev-tools.sh` | The scripts that built it, with the `verify` and `publish` subcommands (keep them together) |

The bundle expects the operating system to provide `git`, `curl`, `bash`, `python3`, `gpg`,
`docker` (with the compose plugin) and a JDK from `jdk/`. It has no `gh`, `go`, `npm` or Node:
CI uses `gh` and `go install` only to download tools, which the bundle ships as binaries, and
markdownlint-cli2 runs from its image. The `tools.lock` column "how" says how each download was
checked: `ci-pin` (the SHA-256 pinned in `ci.yml`), `upstream` (the release's checksum file) or
`recorded` (no reference exists: the hash of what arrived). The GitLab jobs run in one image,
`kafkafim-ci`, that the far side builds from the bundle (`ci/build-tools-image.sh`, no network:
the JDK, Python, Docker CLI and hadolint images plus the static tools) and in the `docker`,
`docker:dind` and markdownlint-cli2 images ([CI and release on
GitLab](#ci-and-release-on-gitlab-the-air-gap)).

You can also build it by hand, for any tag or branch, on a machine with a network, Docker, git,
curl, jq, Python 3 with pip (for the yamllint wheels) and the project's JDK on the `PATH`:

```bash
tools/airgap-dev-bundle.sh build v1.2.3 --out /mnt/transfer --split-mib 1900
```

`v1.2.3` is any tag or branch. The script derives the images from the repository (`FROM`,
`image:` and `DockerImageName.parse` lines), fills the Maven repository by running the goals
that `ci.yml` and `release.yml` run against an empty local repository (so `dependency:go-offline`
and its blind spots are not relied on), and pins each image to the digest of its multi-arch
index. `--platforms "linux/arm64"` limits the images, `--resume` continues an interrupted
build, and `--split-mib` also writes the bundle as parts that fit a release asset or a small
medium. The `YAML format` job of `ci.yml` runs `tools/airgap-dev-bundle.sh check`: it fails when the
repository names an image or a workflow runs a Maven goal that the script does not take.

`tools/airgap-dev-bundle.sh publish` uploads the Maven repository and the images to a
repository manager (Artifactory; Nexus works the same, HTTP PUT)
([RUNBOOK § 7.5](RUNBOOK.md#75-publish-to-artifactory-or-nexus)).

Set up a developer machine in the air gap from the bundle:
[RUNBOOK § 7](RUNBOOK.md#7-developing-inside-the-air-gap). What the build proves, and what it
cannot, is described there too.

## CI and release on GitLab (the air gap)

The air gap runs GitLab, so everything that works on GitHub has a counterpart there
([ADR-0057](decisions/0057-gitlab-pipeline-shares-the-ci-scripts-with-github-actions.md)),
except air-gap bundles: there is nowhere further to carry one. GitHub stays the upstream and its
workflows behave as before. The far side needs GitLab 17 or later, **Free tier**, and a runner.
First-time setup from an empty GitLab is in
[RUNBOOK § 8](RUNBOOK.md#8-ci-and-release-in-gitlab-inside-the-air-gap).

**One source of truth.** The check commands are scripts in `ci/` (`ci/build.sh`,
`ci/integration-tests.sh`, `ci/chart-validate.sh`, and so on). `ci.yml` and `.gitlab-ci.yml` call
the same ones; only what depends on the CI system differs. `ci/parity.map` lists the job pairs, and
`ci/check-parity.sh` (a step of the `YAML format` job on both sides) fails when a job exists on one
side only, when the two jobs of a pair call different scripts, when a script is in no row, when
the GitLab pipeline names an image that the development bundle does not carry, or when it builds
an air-gap bundle. Run any script locally: `ci/chart-lint.sh` needs `helm` on the `PATH`, and so on
(each script's header says what it needs).

### Variables

Everything environment-specific is a CI/CD variable (Settings, CI/CD, Variables); the defaults are
in the `variables:` block of `.gitlab-ci.yml`, so the repository needs no edit.

| Variable | Default | Meaning |
|---|---|---|
| `KAFKAFIM_IMAGE_PREFIX` | `$CI_REGISTRY/$CI_PROJECT_NAMESPACE/mirror` | Registry path the job and service images, the Dockerfile's base images and the Testcontainers images come from. The images are `PREFIX/<name as written>`, as `publish --registry` pushes them |
| `KAFKAFIM_CI_IMAGE` | `$KAFKAFIM_IMAGE_PREFIX/kafkafim-ci:1` | The job image, built from the bundle by `ci/build-tools-image.sh` |
| `KAFKAFIM_DOCKER_MODE` | `dind` | `dind` (the `docker:dind` service, no TLS), `dind-tls`, or `socket` (the runner's mounted socket) |
| `KAFKAFIM_DOCKER_HOST_ADDRESS` | `host.docker.internal` | `socket` mode: the name the job reaches the host's published ports at |
| `MAVEN_REPO_URL` | the project's Maven package registry | The Maven repository to resolve from (the bundle's `maven-repo/`); a `settings.xml` mirror is generated from it, no `-o` |
| `MAVEN_REPO_USER`, `MAVEN_REPO_PASSWORD` | `gitlab-ci-token`, the job token | Credentials for it (HTTP Basic); masked for a real repository manager |
| `KAFKAFIM_REGISTRY_USER`, `KAFKAFIM_REGISTRY_PASSWORD` | empty | `docker login` of the jobs to the registry of `KAFKAFIM_IMAGE_PREFIX`; empty uses the job's own credentials when that registry is this GitLab's |
| `KAFKAFIM_BASE_DIGEST` | `drop` | The Dockerfile's base image: `drop` builds from the tag in the prefix registry, `keep` keeps its `@sha256` pin (see below) |
| `KAFKAFIM_RELEASE_PLATFORMS` | `linux/amd64` | Platforms of the release image |
| `KAFKAFIM_RELEASE_IMAGE` | `$CI_REGISTRY_IMAGE` | Repository the release image is pushed to |
| `KAFKAFIM_RELEASE_CHART_REPO` | `oci://$CI_REGISTRY_IMAGE/charts` | OCI repository the chart is pushed to |
| `KAFKAFIM_RELEASE_REGISTRY_USER`, `KAFKAFIM_RELEASE_REGISTRY_PASSWORD` | the job's registry credentials | Credentials for both of the above (masked for Artifactory) |
| `KAFKAFIM_RELEASE_INSECURE` | unset | `true` when those registries speak plain HTTP |
| `KAFKAFIM_SIGNING` | `cosign` | `cosign`, or `off` to release unsigned |
| `COSIGN_PRIVATE_KEY_B64`, `COSIGN_PASSWORD`, `COSIGN_PUBLIC_KEY` | none | The signing key (see Signing) |

### Runner requirements

A runner with the Docker executor. The Integration tests and Container image jobs need container
access:

- **`dind` (default).** A privileged runner (`privileged = true` in `config.toml`); the jobs start
  a `docker:dind` service named `docker` and talk to it on port 2375. Testcontainers starts its
  containers inside that daemon and reaches them at the host `docker`
  (`TESTCONTAINERS_HOST_OVERRIDE`). No other runner setting is needed.
- **`dind-tls`.** The same, with TLS on port 2376; the runner must mount the client certificates:
  `volumes = ["/certs/client", "/cache"]`. Not exercised by the validation of this change.
- **`socket`.** The runner mounts the host's socket
  (`volumes = ["/var/run/docker.sock:/var/run/docker.sock", "/cache"]`) and resolves
  `host.docker.internal` for jobs (`extra_hosts = ["host.docker.internal:host-gateway"]`). The
  containers a job starts are siblings on the host, and anyone who can run a job controls that
  host's Docker: use it only on a runner that is not shared.

The runner pulls the job image and the service image from `KAFKAFIM_IMAGE_PREFIX`: the registry
must allow it (a public or internal mirror project, the mirror's job token allowlist, or
`DOCKER_AUTH_CONFIG`; [RUNBOOK § 8.5](RUNBOOK.md#85-runner-and-registry-access)). On Kubernetes
and shell executors the pipeline is not designed to run: it needs a privileged Docker sidecar, or
Docker, the JDK and the tools on the runner host.

### Jobs

| GitLab job | Runs | Skipped when |
|---|---|---|
| `build`, `integration`, `coverage`, `mutation`, `container-image` | merge requests that change a non-documentation path, and every push to the default branch | a merge request changes only `docs/`, `*.md` and `LICENSE` |
| `helm` | merge requests that change `charts/`, `deploy/`, `.github/keys/`, `ci/` or `.gitlab-ci.yml`, and every push to the default branch | |
| `grafana-format` | merge requests that change `deploy/grafana/`, `ci/` or `.gitlab-ci.yml`, and every push to the default branch | |
| `shell`, `markdown`, `yaml` | every merge request and push to the default branch | |
| `release-validate`, `release-verify`, `release-publish` | a tag `v*` | |

`rules:changes` cannot negate a pattern, so the list of non-documentation paths is explicit
(`.code-paths` in `.gitlab-ci.yml`): a new top-level path that is not documentation must be added
to it. A newer pipeline of the same merge request cancels the older one (GitLab's default
"auto-cancel redundant pipelines" also applies to the default branch; GitHub never cancels runs on
`main`: turn that setting off in Settings, CI/CD, General pipelines if every commit needs a run).

### Release on a tag

Push a tag `vMAJOR.MINOR.PATCH[-prerelease]` from the default branch (protect `v*` tags: the
signing variables are protected). `release-validate` checks the tag format and that the commit is
on the default branch, `release-verify` runs `./mvnw verify` at the tag, `release-publish` pushes:

- the image `KAFKAFIM_RELEASE_IMAGE` with the tags `1.2.3`, `1.2` (not for a pre-release) and
  `sha-<7>`, never `latest`, with BuildKit provenance;
- the chart `kafkafim-1.2.3` to `KAFKAFIM_RELEASE_CHART_REPO`;
- the SBOM (SPDX JSON, from the bundled syft), the chart package, the alert rules, the dashboards
  and, with signing, the public key, to the project's generic package registry, linked from a
  GitLab release for the tag.

There is no air-gap bundle step and no `SHA256SUMS`: the artifacts are in the registries and the
release already.

### Signing

Keyless Cosign needs Sigstore, which an air gap cannot reach, so the GitLab release signs with a
**Cosign key pair** generated on the far side (the private key never exists elsewhere). Generate
it once:

```bash
COSIGN_PASSWORD='<a long password>' cosign generate-key-pair   # writes cosign.key and cosign.pub
base64 < cosign.key | tr -d '\n'                               # the value of COSIGN_PRIVATE_KEY_B64
```

Set `COSIGN_PRIVATE_KEY_B64` and `COSIGN_PASSWORD` as **masked and protected** variables (the key
is base64 on one line because a multi-line value cannot be masked; GitLab masks only values of at
least 8 characters on one line made of letters, digits and `+/=@:.~-_`, so choose the password
accordingly) and `COSIGN_PUBLIC_KEY` to the
text of `cosign.pub` (not secret). Keep `cosign.key` offline. With `KAFKAFIM_SIGNING=cosign` (the
default), a missing variable fails `release-publish`; `KAFKAFIM_SIGNING=off` releases unsigned.

The image and the chart are signed in the legacy `.sig` format, like the GitHub release, with no
transparency log upload, and checked with the public key in the same job. Anyone verifies with
the public key from the release assets:

```bash
cosign verify --key cosign.pub --insecure-ignore-tlog=true "$IMAGE@sha256:<digest>"
cosign verify --key cosign.pub --insecure-ignore-tlog=true "<registry>/<path>/charts/kafkafim@sha256:<digest>"
```

### Base image digests

The Dockerfile pins its base image by the digest of the multi-arch index. `tools/airgap-dev-bundle.sh
publish` pushes one platform, which makes another manifest, so that digest does not exist in the
mirror ("not found"). The GitLab image build therefore reads the base image from the prefix
registry by tag (`KAFKAFIM_BASE_DIGEST=drop`); what the tag points to is the mirror operator's
decision. A mirror that holds a digest-preserving copy of the bundled index (`skopeo copy --all`,
`crane copy`, Artifactory replication) can set `keep` and gets the same pin as on GitHub. The
repository's Dockerfile is never edited: `ci/image-build.sh` builds a copy with the prefix.

### Local demo, Dependabot and the merge queue

- **Local demo and compose.** With `KAFKAFIM_IMAGE_PREFIX` set, `deploy/local/demo.sh up` reads the
  demo chart's images, the Dockerfile's base image and, with `KIND_NODE_IMAGE`, the kind node
  image from the registry ([deploy/local/README.md](../deploy/local/README.md#from-a-registry-mirror)).
  `deploy/local/compose-prefix.sh` writes a compose override for `compose.yaml` and
  `loadtest/compose.yaml`.
- **Dependabot has no counterpart** inside the air gap: dependency and image updates arrive with
  the next development bundle, built on the connected side from GitHub, where Dependabot keeps
  working.
- **The merge queue** becomes merge requests with *Pipelines must succeed* and the merge method
  *Fast-forward merge* (or *Merge commit with semi-linear history*) on a protected default
  branch. Merge trains are Premium and left out: a merge request can race another that landed
  first, and the fast-forward rule makes the second rebase and rerun.

### Parity: each GitHub job and feature, and its GitLab counterpart

| GitHub | GitLab | Gap and reason |
|---|---|---|
| `Detect changes` | `rules:changes` | The path list is static (see Jobs) |
| `Build and test` | `build` | |
| `Integration tests` (Testcontainers) | `integration`, with `docker:dind` | |
| `Coverage` (floor from `pom.xml`) | `coverage` | |
| `Mutation tests` | `mutation` | |
| `Container image` (hadolint, build, smoke run) | `container-image` | No layer cache (the GitHub job uses the Actions cache) |
| `Helm chart`: lint, `kubeVersion`, kubeconform, demo chart, helm-unittest, dashboards and alert rules | `helm` | kubeconform reads the bundled schema cache, so it needs no network |
| `Shell style` | `shell` | `actionlint` is GitHub-only: it checks workflow files |
| `Markdown style` | `markdown` | |
| `Grafana format` | `grafana-format` | |
| `YAML format`: yamllint, image versions agree, XML tabs | `yaml` | `tools/airgap-dev-bundle.sh check` is GitHub-only: nothing builds bundles in GitLab |
| CI definitions agree | `yaml` | |
| Newer push cancels the run | `interruptible: true` | |
| Required checks and merge queue | Protected branch, pipelines must succeed, semi-linear merge | No merge train (Premium) |
| Dependabot | none | No offline equivalent; updates arrive with the bundle |
| `release.yml`: verify, jar, image, chart | `release-validate`, `release-verify`, `release-publish` | Image is single-platform by default |
| Cosign keyless signature | Cosign key-based signature | No transparency log |
| SBOM (anchore) | syft from the bundle | |
| GitHub release with generated notes, assets | GitLab release with the same assets | Notes are not generated |
| Air-gap bundle step | none | Nowhere further to carry it |
| Local demo | `deploy/local/demo.sh` with `KAFKAFIM_IMAGE_PREFIX` | |
| Step summaries | job log; the `coverage` job's pattern reads the line coverage from it | |

## Local developer loop

```bash
./mvnw verify                                # CI build + integration jobs together
```

```bash
docker build -t kafkafim:dev .               # same Dockerfile as CI, from target/*.jar
```

```bash
helm template charts/kafkafim -f charts/kafkafim/ci/two-groups-values.yaml
```

```bash
deploy/local/demo.sh up                      # the chart on a local kind cluster (deploy/local/README.md)
```

## Base image

`eclipse-temurin:27-jre` (Ubuntu), pinned in both Dockerfile stages by the digest of the
multi-arch index (linux/amd64 and linux/arm64) so builds are reproducible. Dependabot's
`docker` ecosystem updates the tag and digest together. Distroless does not ship a Java 27
runtime yet; revisit when it does. The image runs as UID 65532 with a read-only root filesystem
and `JAVA_TOOL_OPTIONS=-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError` as the default
for running the image without the chart. The chart sets the same variable (with equivalent
flags) from `jvm.options` in `values.yaml`; a pod env entry replaces the image's `ENV` (it
does not append), so the flags are never defined twice. Change the flags for chart deployments in `jvm.options`.
Class-data sharing (AppCDS) is a later startup optimisation, not needed for the MVP.
