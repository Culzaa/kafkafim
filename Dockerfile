# syntax=docker/dockerfile:1
# Runtime-only image. The jar is built and tested by Maven first (CI: the `build`
# job); this file only layers it. See docs/DELIVERY.md.
ARG JAR_FILE=target/*.jar

# Both stages pin the multi-arch index digest (amd64 + arm64); Dependabot's docker
# ecosystem bumps the tag and digest together. Keep the two image references identical.

# Extraction output is platform-independent: run it natively instead of under QEMU.
FROM --platform=$BUILDPLATFORM eclipse-temurin:27-jre@sha256:acfb350b50e707addf3dcbb47f7052d1882d1e5f1670ce838a9c7838d82d4fe5 AS extract
WORKDIR /build
ARG JAR_FILE
COPY ${JAR_FILE} application.jar
RUN java -Djarmode=tools -jar application.jar extract --layers --destination extracted

FROM eclipse-temurin:27-jre@sha256:acfb350b50e707addf3dcbb47f7052d1882d1e5f1670ce838a9c7838d82d4fe5
LABEL org.opencontainers.image.source="https://github.com/matabar/kafkafim" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.title="kafkafim" \
      org.opencontainers.image.description="Kafka to Elasticsearch ingest for Kubernetes"
WORKDIR /app
# Layer order: least to most frequently changing, so image pushes/pulls are small.
COPY --from=extract /build/extracted/dependencies/ ./
COPY --from=extract /build/extracted/spring-boot-loader/ ./
COPY --from=extract /build/extracted/snapshot-dependencies/ ./
COPY --from=extract /build/extracted/application/ ./
# Do NOT attach the OpenTelemetry Java agent (no -javaagent in JAVA_TOOL_OPTIONS) and do not
# enable Spring Kafka observation: the opentelemetry-kafka-clients library inside the jar is
# the one tracing mechanism for the Kafka clients, and a second one would trace each twice.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError" \
    SPRING_CONFIG_ADDITIONAL_LOCATION=optional:/etc/kafkafim/config/
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/application.jar"]
