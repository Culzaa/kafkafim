# ADR-0056: The air-gap development bundle is built by a script and not published

Status: accepted · Date: 2026-11-19 · Extends [ADR-0022](0022-delivery-github-actions-ghcr-airgap.md)

The "not published" part is superseded by
[ADR-0058](0058-release-builds-and-publishes-the-air-gap-development-bundle.md).

## Context

The release bundle of ADR-0022 (`kafkafim-<ver>-airgap.tar.gz`) lets a team deploy a signed
release inside the air gap. It does not let anyone develop there: no source history, no Maven
dependencies, no JDK, none of the images that the build, the integration tests and the demo
use, none of the tools CI runs (issue 480). The owner asked for one bundle that holds "all the
source code, as a git repo, all java dependencies in their structure, docker images used for
demo, ci, and everything I need to continue development in the airgap".

Measured on the current tree (v0.4.x line), built for linux/arm64 images: the Maven repository
for every goal the project runs is 323 MB (912 MB with sources and Javadoc jars); the images,
runtime and job images together, are 3.2 GB per platform; the CI tools for three platforms are
1.3 GB (helm-unittest alone 208 MB); a JDK is about 145 MB per platform. The bundle is 5.8 GB
with one image platform and about 9 GB with both Linux platforms. A GitHub release asset is
capped at 2 GiB per file.

## Decision

1. **A script builds it on a connected machine; the release workflow does not publish it.**
   `tools/airgap-dev-bundle.sh build <tag>` clones the source at a tag, fills the bundle and
   writes it to a directory (optionally also as tar parts of at most 2 GiB). The operator
   carries it across on their own media. Nothing is uploaded to GitHub.
2. **Everything is derived from the repository** at the tag being bundled, not listed a second
   time:
   - *Images*: `FROM` lines of the `Dockerfile`, `image:` lines of `compose.yaml`,
     `loadtest/compose.yaml` and the demo chart's `values.yaml`, and
     `DockerImageName.parse("...")` in `src/test`. Testcontainers' helper image (Ryuk) is read
     from the Testcontainers jar the build resolved. Each is pinned to the digest of its
     multi-arch index when the bundle is built, and the pin is recorded in `images/images.lock`.
   - *Maven repository*: the build itself fills it. The script runs, with an empty
     `-Dmaven.repo.local`, the goal sets of `ci.yml` and `release.yml`: `verify` (Spotless,
     Checkstyle, Surefire, Failsafe, JaCoCo, packaging), PIT, and the JaCoCo merge, report and
     check. `dependency:go-offline` is not used: it misses artifacts that plugins resolve
     while they run (Spotless' google-java-format, Checkstyle, PIT's JUnit plugin, Surefire and
     Failsafe providers).
   - *CI tools*: the versions pinned in `ci.yml` and `release.yml` (Helm, helm-unittest with its
     signed tarball and key, kubeconform, shellcheck, shfmt, actionlint, promtool, cosign, syft,
     yamllint as wheels), as binaries for linux-amd64, linux-arm64 and darwin-arm64, plus kind,
     kubectl and jq (pinned in `tools/airgap-dev/pins.txt`, the only hand-kept list: CI does not
     pin them). Each is checked against the SHA-256 pinned in `ci.yml` where CI pins one, else
     against the upstream checksum file, else (shellcheck, shfmt on arm64 and macOS) recorded
     on download and marked `recorded` in `tools/tools.lock`. The Kubernetes schemas kubeconform
     fetches are cached by running it on the charts, so `kubeconform -cache` works offline.
     hadolint and syft versions are not in the workflows: they come from the pinned actions'
     own files (the hadolint action's Dockerfile, the sbom action's `SyftVersion.ts`).
   - *Job images* for the GitLab runner (generic: tool-specific job images come with the pipeline
     change, issue 482) (`docker`, `docker:dind`, a JDK, Python, the
     markdownlint-cli2 image, hadolint) and kind's node image, pinned by digest like the rest.
   - *Sources and Javadoc*: for every jar in the repository, plugins and transitive
     dependencies included, the `-sources.jar` and `-javadoc.jar` that Maven Central has, each
     checked against Central's SHA-1 (448 of 455 jars have sources, 435 have Javadoc; the
     others have none on Central). Maven does not fetch them during a build.
   - *Maven distribution*: the file the wrapper's `distributionUrl` names, stored in the same
     repository layout, so `MVNW_REPOURL=file://<bundle>/maven-repo ./mvnw` works offline.
   - *JDK*: Temurin of the pom's `java.version`, from Adoptium, checked against the SHA-256
     Adoptium publishes.
3. **A CI check keeps the derivation honest** (`tools/airgap-dev-bundle.sh check`, offline, a few
   seconds). It fails when an image named anywhere in tracked files (a new compose file,
   Dockerfile or Testcontainers constant) is not in the derived list, when a Maven plugin goal
   that a workflow runs is not in the script's goal sets, or when a source of images yields
   nothing because a file moved. A new file in one of those forms that the derivation does not read
   fails the check until the script reads it. Images named in other ways (shell scripts,
   `docker run` lines, chart templates) are not caught.
4. **Integrity.** The bundle carries `SHA256SUMS` over every file, in the format of the release
   bundle (`sha256sum -c SHA256SUMS`), and `manifest.json` with the commit, the ref and every
   image digest. `tools/airgap-dev-bundle.sh verify <dir>` checks the sums, the git bundle and
   then runs a full offline build. Each downloaded component is checked at build time: Maven
   distribution against Central's SHA-512, the JDK against the SHA-256 that Adoptium's API
   publishes next to the file (this guards against transport damage, not against a
   compromised Adoptium), images by digest. Tie to the signed release: when the bundled ref is
   a release tag, the source commit
   in the manifest is the commit the release workflow built and signed, and the bundle
   contains no image that claims to be kafkafim. The deployment bundle remains the only
   artifact that carries the signed kafkafim image. Signing the development bundle itself is
   left out: it is built by a person on a machine of their choice, so a signature would only
   attest to that person.
5. **Platforms.** Images: `linux/amd64` and `linux/arm64` (the platforms of the release image),
   one tar per platform. JDK: Linux x64, Linux aarch64, macOS aarch64. Left out on purpose:
   Windows and macOS x64, because the project and its scripts are Bash and the demo needs a
   Linux Docker; Docker Desktop on macOS runs the arm64 images. The job images and tools of CI follow
   the same platform set in a later change of this series.

## The air gap runs GitLab (owner's decision, issue 480)

GitHub stays the upstream. Inside the air gap there is a GitLab instance and a GitLab Runner;
nothing of or for kafkafim is there yet (no repository, no Maven mirror, no proxied images, no
tools). Everything that works on GitHub must work there, except that GitLab does not build
air-gap bundles (there is nowhere further to carry them). The development bundle is built on
the connected side and is how the repository, dependencies, images and tools arrive. This
splits the work into the bundle (this change) and the GitLab pipelines (later changes):

- **GitLab edition and version.** Self-managed GitLab 17 or later, **Free tier** assumed. Every
  feature used is in Free; nothing here needs Premium or Ultimate.
- **One source of truth for the checks.** The check commands move from inline `run:` blocks to
  scripts under `ci/` that `ci.yml` and `.gitlab-ci.yml` both call, and a check fails when a
  job exists on one side without its counterpart (a map file lists the pairs). The GitHub
  workflows keep their behaviour.
- **Job images and tools** are in the bundle and are loaded into the GitLab container registry;
  jobs name them through variables (`KAFKAFIM_IMAGE_PREFIX`, default
  `$CI_REGISTRY/$CI_PROJECT_NAMESPACE/mirror`), so the far side points them elsewhere without
  editing the repository. The Maven repository is published to the project's Maven package
  registry or mounted, selected by `MAVEN_REPO_URL`. The release image and chart go to
  `$CI_REGISTRY_IMAGE`.
- **Runner requirements.** A runner exists with the Docker executor; container access is
  believed (not confirmed) to be Docker-in-Docker. The Integration tests and Container image
  jobs are written for a `docker:dind` service on a privileged runner (`DOCKER_HOST`, the TLS
  variables and the Testcontainers host override are set so that containers Testcontainers
  starts are reachable from the job). One CI variable, `KAFKAFIM_DOCKER_MODE` (`dind` by
  default, or `socket`), switches those jobs to the host's mounted Docker socket. The `docker`
  and `docker:dind` images are job images, so the bundle carries them pinned by digest.
  Kubernetes and shell executors are not designed for: they need a privileged Docker sidecar,
  or Docker, the JDK and the tools on the runner host.
- **Signing.** Keyless Cosign needs Sigstore, which the air gap cannot reach. Options: (a) Cosign
  with a key pair, the private key in a masked, protected CI variable and the public key in the
  repository (recommended: same Cosign signature format, offline verification with `cosign
  verify --key`); (b) a private Sigstore stack (Fulcio, Rekor, TUF), keyless as on GitHub but a
  service to run and keep; (c) no signature, checksums only. This is the owner's call; the
  release pipeline change waits for it.
- **Replacements for repository hygiene.** Dependabot has no offline counterpart: updates keep
  coming from GitHub and reach the air gap with the next bundle. The merge queue becomes
  merge requests with "pipelines must succeed" and semi-linear or fast-forward merge (Free); a
  merge train is Premium and is left out, so a merge request can race another one that landed
  first. Required checks become protected-branch rules plus "pipelines must succeed".

| GitHub | GitLab counterpart | Delivered by |
|---|---|---|
| Job Build and test, Integration tests, Coverage, Mutation tests | jobs of `.gitlab-ci.yml` calling the same `ci/` scripts | GitLab pipeline change |
| Job Container image (hadolint, build, smoke run) | build with the runner's Docker or a builder; push to `$CI_REGISTRY_IMAGE` | GitLab pipeline change |
| Jobs Helm chart, Shell style, Markdown style, YAML format, Grafana format | same scripts, tools from bundle images | GitLab pipeline change |
| Job Detect changes (docs-only skip) | `rules:changes` | GitLab pipeline change |
| `release.yml` on tag `v*` | `.gitlab-ci.yml` tag pipeline: jar, image, chart, SBOM, dashboards and alerts as release assets | release pipeline change, after the signing decision |
| Air-gap bundle step of `release.yml` | none: no further air gap to carry to | left out on purpose |
| Local demo (`deploy/local/demo.sh`) | unchanged; images from the registry through `KAFKAFIM_IMAGE_PREFIX` | demo change |
| Dependabot | none (upstream updates arrive with the next bundle) | gap, no offline equivalent |
| Merge queue | MR with pipelines must succeed and semi-linear merge | gap: no merge train on Free |
| Cosign keyless signature | Cosign key-based (recommended) | owner's decision |

## Alternatives considered

- **Release assets, split into 1.9 GiB parts.** Five or more assets per release, a publish step
  that downloads and saves 10 GB of third-party images on every tag, and a public redistribution
  of the JDK and of Elastic, Grafana and Kibana images that the owner has not decided on. The
  script can be used by a release manager at any time, and the workflow can call the same
  script later if that decision is made.
- **A workflow artifact.** Capped by retention (90 days) and by the repository's storage, not
  attached to the release, and it cannot be reproduced by a person without the CI runner.
- **A container image or registry mirror of everything.** Needs a registry inside the air gap
  before the first build; the directory needs only a disk.
- **`dependency:go-offline` or `dependency:resolve-plugins` for the Maven repository.** Both
  miss artifacts a plugin resolves lazily; running the real goals cannot, and the offline
  verify proves it.
- **Hand-maintained lists of images and tools.** They go stale on every Dependabot bump; the
  derivation plus the check cannot.

## Consequences

- The development bundle is reproducible by anyone with the repository, a network, Docker and
  the JDK. It is not a release asset and not signed; `SHA256SUMS` protects against corruption in
  transit, not against a malicious builder (the same trust as the transfer medium).
- Resolver bookkeeping (`_remote.repositories`, `*.lastUpdated`, `resolver-status.properties`)
  is removed from the Maven repository, so the same tree works as `-Dmaven.repo.local` and as
  the content of an internal repository manager.
- A change to a goal set in a workflow, or a new image source, needs a matching change in the
  script; the check says so in the pull request that makes the change.
- Loading the images by digest needs Docker's containerd image store (the default from
  Docker 29); the classic store drops the digest on `docker load`, and a digest-pinned `FROM`
  would then try to pull. This was shown on the containerd store only.
- What `verify` does not prove: it runs on a machine that has a network. It uses `-o`, a
  Maven repository copied from the bundle and a wrapper served from the bundle, not a network
  namespace without routes. The runbook states this.
