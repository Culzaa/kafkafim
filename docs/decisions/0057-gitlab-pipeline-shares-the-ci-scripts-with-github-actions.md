# ADR-0057: The GitLab pipeline calls the same `ci/` scripts as GitHub Actions

Status: accepted · Date: 2026-11-19 · Extends [ADR-0056](0056-air-gap-development-bundle-is-built-by-a-script-and-not-published.md)
and [ADR-0022](0022-delivery-github-actions-ghcr-airgap.md)

## Context

The air gap runs GitLab, not GitHub (issue 480). The owner's bar is parity: everything that works
on GitHub must work there, except air-gap bundles, which have nowhere further to go. Nothing of
kafkafim is in that GitLab yet: no repository, no Maven mirror, no images, no tools. Its runner
uses the Docker executor and is believed to offer Docker-in-Docker. ADR-0056 fixed the GitLab
edition (Free), the variables and the replacements for Dependabot and the merge queue, and left
the pipeline, the release and the signing for later changes. This ADR is that change (issues
482, 483 and 484).

## Decision

1. **The check commands live in `ci/` scripts that both CI systems call.** Every inline `run:`
   block of `ci.yml` became a script (`ci/build.sh`, `ci/integration-tests.sh`, `ci/chart-*.sh`,
   and so on); `ci.yml` and `.gitlab-ci.yml` call them. What depends on the CI system stays
   out of the scripts: GitHub's action pins, caches, tool downloads and annotations, and
   GitLab's job images, variables and `rules`. GitHub's job names, required check names and
   behaviour do not change (the one addition is `ci/*` in the path filters that decide whether
   the chart and Grafana halves run, because their commands now live there).
2. **A drift check runs in both systems.** `ci/parity.map` lists the job pairs and the scripts
   each side calls; `ci/check-parity.sh` fails when a job exists on one side only, when two jobs
   of a pair call different scripts, when a script is in no row, when the GitLab pipeline names an
   image that the development bundle does not carry, when the release tag formats differ, or when
   the GitLab pipeline builds an air-gap bundle. It runs in the `YAML format` job of both
   (`ci.yml`, so the required check names stay as they are). `release.yml` stays inline: only a
   tag can run it, so refactoring it would trade a tested file for an untestable one. Its tag
   format is compared with the GitLab one instead.
3. **One job image, built from the bundle.** The bundled images are bare: `docker:29-cli` has git
   and no bash; the JDK, Python and hadolint images have neither git, curl, unzip nor gpg. The
   far side builds `kafkafim-ci` once (`ci/build-tools-image.sh`, from the bundle, no network):
   the Python image as the base (bash, `pip` for yamllint's wheels), the JDK, the Docker CLI and
   buildx copied from their images, hadolint copied from its image, the static tools from the
   bundle's `tools/linux-<arch>/` (Helm, kubeconform, shellcheck, shfmt, promtool, cosign, syft,
   jq), the signed helm-unittest plugin and its keyring, kubeconform's schema cache, and a
   30-line `unzip` for the Maven wrapper. Markdown style runs in the markdownlint-cli2 image and
   the release's tag check in `docker:29-cli` (it has git), both from the bundle. Because the jobs
   have no git, `ci/lib.sh` lists files with `find` when git is absent; the job checkout is clean,
   so that is the tracked set.
4. **Docker access: `dind` by default.** `KAFKAFIM_DOCKER_MODE` is `dind` (default), `dind-tls` or
   `socket`; one of three include files defines the `.docker` template. `dind` runs the
   `docker:dind` service without TLS (port 2375): only the job's own network reaches it, and it
   needs no runner configuration, which is unknown. `dind-tls` is GitLab's recommended form and
   needs `/certs/client` mounted by the runner. `socket` needs the runner to mount the host's
   socket and a name for the host (`host.docker.internal`). `TESTCONTAINERS_HOST_OVERRIDE` and
   `DOCKER_HOST_NAME` name the host that published ports are reachable at (`docker` for dind).
   Two integration tests that use TLS failed behind dind (hostname verification: "No subject
   alternative DNS name matching docker found"); the container is now named after the Docker
   host when that is not `localhost` (`DockerHostNames`), which puts the name in Elasticsearch's
   generated certificate (checked with `curl --resolve`). A local daemon is unaffected.
5. **Images come through `KAFKAFIM_IMAGE_PREFIX`.** Testcontainers' own
   `TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX` does not do it: its substitutor returns an image that
   names a registry unchanged ("is not a Docker Hub image", read from `PrefixingImageNameSubstitutor`
   in testcontainers 2.0.5), so the Elasticsearch image on `docker.elastic.co` stayed on the
   internet. `RegistryPrefixImageNameSubstitutor` (test sources) prefixes every image, as
   `<prefix>/<name as written>`, the names `publish` pushes; the integration job activates it with
   `TESTCONTAINERS_IMAGE_SUBSTITUTOR`, so GitHub never loads it. The Dockerfile's base image
   goes through `ci/image-build.sh`, which builds a copy of the Dockerfile with the prefix in the
   `FROM` lines (the repository's file and Dependabot's bumps of it stay as they are) and without
   the `# syntax=` line, whose BuildKit frontend would be fetched from Docker Hub.
6. **Digests (issue 486).** The Dockerfile pins the base image by the digest of its multi-arch
   index. `publish` pushes one platform, which makes another manifest: the pinned digest then
   fails with "not found" (shown against a local registry). So `KAFKAFIM_BASE_DIGEST` defaults to
   `drop` (build from the tag in the prefix registry, which the mirror's operator controls) and
   offers `keep` for a registry that holds a digest-preserving copy (`skopeo copy --all`,
   `crane copy`, Artifactory replication), where the pin applies exactly as on GitHub.
7. **Maven goes through one repository, `MAVEN_REPO_URL`.** `ci/maven-settings.sh` writes a
   `settings.xml` with a `mirrorOf *` for it, so no `-o` is needed, and the wrapper downloads
   Maven from the same place (`MVNW_REPOURL`, with the same credentials). Defaults: the project's
   Maven package registry, authenticated as `gitlab-ci-token` with the job token; Artifactory and
   Nexus work with a user and password.
8. **Docs-only merge requests skip the Java, image and chart jobs with `rules:changes`.** GitLab
   cannot negate a pattern, so the non-documentation paths are listed; a new top-level path must be
   added to that list. Pushes to the default branch run everything, like `main` on GitHub.
9. **The release is a tag pipeline in the same file.** `release-validate` (tag format, reachable
   from the default branch), `release-verify` (`./mvnw verify` at the tag) and `release-publish`:
   image with the tags `1.2.3`, `1.2` (not for a pre-release) and `sha-<7>`, never `latest`; the
   chart pushed to an OCI repository; the SBOM with the bundled syft; dashboards, alert rules, the
   chart package and the SBOM uploaded to the generic package registry and linked from a GitLab
   release created with the job token. The registries are variables (`KAFKAFIM_RELEASE_IMAGE`,
   `KAFKAFIM_RELEASE_CHART_REPO`, credentials), defaulting to the project's registry; Artifactory is
   the real target. The image is single-platform by default (`KAFKAFIM_RELEASE_PLATFORMS`): the
   Dockerfile needs no emulation for a second platform, but the registry needs that platform's
   base image, which `publish` pushes for one platform at a time. No air-gap bundle step.
10. **Signing: a Cosign key pair** (option (a) of issue 483, accepted by the owner's delegate). The
    private key is `COSIGN_PRIVATE_KEY_B64` (base64 on one line, because a multi-line key cannot be
    a masked variable) with `COSIGN_PASSWORD`, both masked and protected, and the public key is
    `COSIGN_PUBLIC_KEY`; the far side generates the pair (`cosign generate-key-pair`), so the
    private key never exists outside. The image and the chart are signed in the legacy `.sig`
    format with `--tlog-upload=false` (nothing is uploaded; there is no Sigstore), and verified
    with the public key in the same job, which catches a key that does not match. With
    `KAFKAFIM_SIGNING=cosign` (default) a missing variable fails the job; `KAFKAFIM_SIGNING=off`
    releases unsigned, visibly.
11. **Local demo and repository hygiene (issue 484).** `deploy/local/demo.sh` reads the demo
    chart's images, the Dockerfile's base image and optionally the kind node image through
    `KAFKAFIM_IMAGE_PREFIX`, with no edit of the repository (it passes `--set` values derived
    from `values.yaml` and loads the images into kind from the local Docker).
    `deploy/local/compose-prefix.sh` writes a compose override for the two compose files.
    Dependabot has no offline counterpart (updates arrive with the next bundle). The merge queue
    becomes merge requests with "pipelines must succeed" and the semi-linear merge method; merge
    trains are Premium and left out.

## Alternatives considered

- **Hand-written `.gitlab-ci.yml` commands next to `ci.yml`.** Two copies drift; the bundle's
  goal-set check already showed how often a workflow changes.
- **TLS on by default for dind.** It needs `/certs/client` in the runner's configuration, which
  nobody knows yet; a job that cannot read the certificate fails with a confusing TLS error. It
  stays one variable away.
- **`TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX`.** Proven not to cover `docker.elastic.co` and
  `ghcr.io` (decision 5).
- **Editing the Dockerfile with a registry build argument.** It changes the file Dependabot and
  the GitHub build read; the generated copy leaves both alone.
- **Sigstore (a private Fulcio, Rekor and TUF) or checksums only.** A service to run, or no
  signature. Both stay open as later decisions.
- **A masked, protected file variable for the private key.** A file variable cannot be masked.

## Consequences

- A change to a check is a change to one script; the parity check says when a side was missed.
- The far side builds one image from the bundle before the first pipeline, and again when the tool
  pins change (the pipeline names it by tag).
- The base image of the release is as trustworthy as the mirror registry, not as the digest pin,
  unless the mirror keeps digests.
- `rules:changes` and `ci/changes.sh` implement the same idea twice; they can disagree for a new
  path.
- What the validation of this change did not exercise: the `dind-tls` mode, a real Artifactory,
  GitLab's own container registry (the release was run against a plain-HTTP registry, and the
  GitLab release and package calls against a real GitLab), the integration job on a real GitLab
  runner (it ran in a simulation of the Docker executor with `docker:dind` and no route out, and
  on a local Docker with the socket), and `release-publish` as a whole on a real GitLab. The pull
  request lists what ran where.
