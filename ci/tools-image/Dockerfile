# The GitLab job image: every tool the ci/ scripts need, assembled from the air-gap development
# bundle (ADR-0057). Built once on the far side by ci/build-tools-image.sh; nothing is downloaded.
# The bundled job images carry no git, curl, unzip or gpg, so the tools come as static binaries
# (the bundle's tools/linux-<arch>/) and the JDK, the Docker CLI and hadolint are copied from the
# images that hold them.
ARG JDK_IMAGE=eclipse-temurin:27-jdk
ARG DOCKER_IMAGE=docker:29-cli
ARG HADOLINT_IMAGE=hadolint/hadolint:latest-debian
ARG PYTHON_IMAGE=python:3.13-slim

FROM ${JDK_IMAGE} AS jdk
FROM ${DOCKER_IMAGE} AS docker
FROM ${HADOLINT_IMAGE} AS hadolint

FROM ${PYTHON_IMAGE}
ARG YAMLLINT_VERSION
COPY --from=jdk /opt/java/openjdk /opt/java/openjdk
COPY --from=docker /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker /usr/local/libexec/docker/cli-plugins/docker-buildx /usr/local/libexec/docker/cli-plugins/docker-buildx
COPY --from=hadolint /bin/hadolint /usr/local/bin/hadolint
COPY context/bin/ /usr/local/bin/
COPY context/unzip /usr/local/bin/unzip
COPY context/helm-unittest/ /opt/helm-unittest/
COPY context/kubeconform-schemas/ /opt/kubeconform-schemas/
COPY context/wheels/ /tmp/wheels/
RUN pip install --no-index --no-cache-dir --find-links /tmp/wheels "yamllint==${YAMLLINT_VERSION}" \
  && rm -rf /tmp/wheels
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin \
    KUBECONFORM_CACHE=/opt/kubeconform-schemas \
    KAFKAFIM_HELM_UNITTEST_KEYRING=/opt/helm-unittest/keyring.gpg \
    SKIP_BIN_INSTALL=1
# SKIP_BIN_INSTALL: helm-unittest's install hook would download a binary from GitHub; the signed
# tarball already carries one per platform (plugin.yaml: platformCommand).
LABEL org.opencontainers.image.title="kafkafim-ci" \
      org.opencontainers.image.description="Job image of the kafkafim GitLab pipeline (ci/tools-image)"
