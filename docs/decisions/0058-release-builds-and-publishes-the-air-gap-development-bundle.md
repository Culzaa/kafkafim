# ADR-0058: The release builds and publishes the air-gap development bundle

Status: accepted · Date: 2026-11-19

Supersedes the "not published" part of
[ADR-0056](0056-air-gap-development-bundle-is-built-by-a-script-and-not-published.md) (decision 1
and the signing sentence of decision 4); the rest of ADR-0056 stands.

## Context

ADR-0056 had the development bundle built by hand with `tools/airgap-dev-bundle.sh` and not
published. After v0.5.0 the owner asked: "I need the whole bundle to be created by the release"
(issue 488). A release must now contain what the air gap needs to keep developing, with no manual
step. GitLab keeps having no bundle step ("we dont need airgap bundles there", issue 480).

Constraints: the bundle is well over 6 GB with `linux/amd64` and `linux/arm64` images (ADR-0056
measured about 9 GB); a release asset is capped at 2 GiB per file; a hosted runner has about 14 GB
free on its root disk; building the bundle runs Maven (`verify`, PIT, coverage) and so runs the
project's plugin and dependency code, which the release workflow keeps away from write and
`id-token` permissions (job `verify`, ADR-0022).

## Decision

1. **The release workflow builds the bundle and attaches it to the release**, as split tar parts.
   `release.yml` gains two jobs: `bundle` and `bundle-publish`. Nothing else in ADR-0056 changes:
   the script, its derivation from the repository and the offline `verify` are what the release
   runs.
2. **Assets, named so that they cannot be mistaken for the deployment bundle**
   (`kafkafim-<ver>-airgap.tar.gz`, unchanged):
   - `kafkafim-dev-v<ver>-airgap-dev.tar.part-aa`, `-ab`, ... : the bundle as one tar stream cut
     into parts of 1900 MiB (`--split-mib 1900`, under the 2 GiB cap);
   - `kafkafim-dev-v<ver>-airgap-dev.parts.SHA256SUMS`: the SHA-256 of every part;
   - `kafkafim-dev-v<ver>-airgap-dev.parts.SHA256SUMS.sigstore.json`: the Cosign signature.
   One reassembly command: `cat kafkafim-dev-v<ver>-airgap-dev.tar.part-* | tar -xf -`, then
   `sha256sum -c SHA256SUMS` inside the directory and `airgap-dev-bundle.sh verify`. The
   deployment bundle stays as it is: it is small, signed content a cluster operator needs, and the
   development bundle is for developers.
3. **Parts, not an OCI artifact.** An OCI artifact in GHCR would need `packages: write` in a job
   that built from third-party code, a registry client (`oras`, `crane`) on the connected machine,
   and layers within GHCR's size limits, and it would not appear on the release page. Release assets
   are fetched with `gh release download` or a browser, sit next to the version they belong to, and
   reassemble with `cat`. The cost is a list of assets (about six parts of 1900 MiB at the measured
   size, plus two files) and that a part missing in transit is found by the checksums file.
4. **Two jobs, so the write permissions never meet Maven.**
   - `bundle` (`contents: read`, no `id-token`): checks out the full history, validates the tag,
     frees disk (below), runs `ci/bundle-build.sh`: build the bundle from the checkout (a bare copy
     of its branches and tags, no token), `airgap-dev-bundle.sh verify` (checksums, git bundle, full
     offline `./mvnw verify` from the bundled Maven repository), and a rejoin of the parts to prove
     that what is published unpacks to the bundle that was verified. The parts and checksums go to a
     one-day workflow artifact, uncompressed.
   - `bundle-publish` (`contents: write`, `id-token: write`, no Maven): downloads the artifact,
     checks the parts, signs the checksums file with Cosign keyless (`sign-blob --bundle`), verifies
     the signature at once, and uploads parts, checksums and signature to a **draft** release.
5. **Integrity.** The checksums file is signed with the identity of the workflow, the same
   repository and issuer as the image and the chart, but anchored to this workflow file and to tags:
   `--certificate-identity-regexp '^https://github\.com/culzaa/kafkafim/\.github/workflows/release\.yml@refs/tags/v'
   --certificate-oidc-issuer https://token.actions.githubusercontent.com`, checked with `cosign
   verify-blob --bundle`. That signs the parts, and the bundle's own `SHA256SUMS` (inside the parts)
   covers every file. The signature binds the bundle to the release workflow of this repository, not
   to the contents' third-party downloads, which ADR-0056 already checks (digest pins, upstream
   checksums, `recorded` marks). The runbook has the verification before carrying across.
6. **Failure order: nothing public until everything is attached.** The bundle is the slowest and
   most failure-prone step (network downloads of about 10 GB, a full offline build), and the image
   push is the one that cannot be undone. So `release` (image, chart, SBOM, signing) `needs:
   [verify, bundle-publish]`: the image is pushed only after the bundle is built, verified and
   signed. The GitHub release is created as a draft by `bundle-publish`, filled by `release`
   (`draft: true`), and made public by the last step (`gh release edit --draft=false`). A failure
   anywhere leaves at worst a draft (visible to writers) and, if `release` fails after the push,
   the same state a failure there has always left. `bundle` runs beside `verify` rather than after
   it to save the test time; a bad tag fails both within seconds (`bundle` validates the tag first).
7. **Runner disk and time.** `ci/bundle-free-disk.sh` removes the .NET, Android, GHC and CodeQL
   toolchains (they are never used here) and puts the work directory on the runner's second disk
   (`/mnt`). Peak need is the unpacked bundle, its parts and the rejoin check, about three times 10 GB on `/mnt`; the build's temporary files go to `/mnt` too. `bundle` has a
   five-hour timeout (hosted limit: six) and `bundle-publish` two. Ordinary pull-request CI is
   untouched: both jobs exist only in `release.yml`, which runs on tags and manual dispatch.
8. **Dry run.** `release.yml` has `workflow_dispatch`: it runs only `bundle`, on the selected
   branch or tag, ending in the one-day artifact. No release, signature or tag is involved.
9. **GitLab keeps no bundle step.** `ci/parity.map` records both jobs as GitHub-only release
   jobs (GitLab job `-`), `ci/check-parity.sh` accepts that row kind and fails if a GitLab job or
   another script calls `ci/bundle-*.sh`.

## Alternatives considered

- **An OCI artifact in GHCR**: see 3.
- **Build and attach in the `release` job**: one job less, but Maven would then run in a job with
  `contents: write`, `packages: write` and `id-token: write`, which `verify` exists to avoid, and
  the disk would be shared with the image build.
- **Upload parts to the release from `bundle` with a token, no artifact**: the same permission
  problem, and a failed build would leave a public release without assets.
- **Publish the release first, add the bundle later**: a failed bundle would leave a public
  release without it, and the owner wants the bundle to be part of the release.
- **A release asset per image or per directory**: many more assets and no single checksum story;
  the offline `verify` needs the whole directory anyway.

## Consequences

- Every release tag costs a bundle: several GB of downloads from Maven Central, Adoptium, Docker
  Hub, GHCR and other registries, and a runner for most of an hour or more. A failure of any of
  them (rate limits, a moved tool URL) blocks the release. This is deliberate (decision 6); a
  rerun of the failed jobs continues, because the draft is reused (`--clobber`).
- The release redistributes third-party images (Elastic, Grafana, Kibana, Temurin and the like)
  in the parts. ADR-0056 left this to the owner; the owner has asked for it (issue 488).
- A workflow artifact of about 10 GB exists for a day. If the repository's Actions storage quota
  cannot hold it, `bundle` fails at the upload with nothing public.
- What only a real tag proves: the draft release flow (`gh release create --draft`, the
  `softprops/action-gh-release` update of a draft, `gh release edit --draft=false`), Cosign
  `sign-blob` on the hosted identity and its verification, the artifact size limits, and the
  disk and time on a hosted runner. The dry run covers the build, the verification and the rejoin.
