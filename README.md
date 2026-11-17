# kafkafim

Production-grade Kafka → Elasticsearch ingest for Kubernetes. kafkafim replaces a fleet of
Logstash pipelines with one service that routes JSON records from many Kafka
clusters to many Elasticsearch clusters. It provides at-least-once delivery, declarative
per-event routing, a real DLQ, hot-reloaded Spring configuration rendered by Helm,
and built-in OpenTelemetry. MVP scope: a shipper, not a pipeline engine (only per-route
`remove`, `rename`, and `set` transformations, no expression language, no CLI).

Status: released (see [GitHub Releases](https://github.com/matabar/kafkafim/releases) for the
latest version). Each release publishes a signed multi-arch image, a signed Helm chart, an SBOM,
and an air-gap bundle ([Delivery](docs/DELIVERY.md)). Throughput and sizing are measured in the
[load test](docs/LOADTEST.md).

## Install

Pull the image and the chart from GHCR, then install one release per Kubernetes cluster
with your own values file (clusters, pipelines, and secret names). The
[Helm chart guide](docs/HELM.md) describes the values and the secrets layout; to verify the
signatures and import into an air-gapped registry, see
[Delivery](docs/DELIVERY.md#importing-into-the-air-gapped-environment).

```bash
helm upgrade --install kafkafim oci://ghcr.io/matabar/charts/kafkafim \
  --version <release> -f values-<cluster>.yaml     # <release>: a tag without the leading v
docker pull ghcr.io/matabar/kafkafim:<release>     # the chart already references this image
```

## Documents

- [Design](docs/DESIGN.md): goals, architecture, configuration model, failure
  handling, observability, migration, roadmap.
- [Decision records](docs/decisions/README.md): one ADR per decision.
- [Tech stack](docs/TECH_STACK.md): every library, its version, and the reason.
- [Delivery](docs/DELIVERY.md): CI, releases, registries, importing into the
  air-gapped environment.
- [Helm chart](docs/HELM.md): one release per Kubernetes cluster, groups, secrets.
- [Runbook](docs/RUNBOOK.md): deploy, observe, and recover kafkafim in production;
  incidents as symptom → check → action.
- [Load test](docs/LOADTEST.md): measured throughput, latency, memory, outage, and rebalance
  behaviour, and the sizing that follows; the harness is in [loadtest/](loadtest/README.md).

## Develop

You need Docker (for the local stack and tests) and a JDK 27 on `PATH`. You don't need a
local Maven install.

```bash
./mvnw spring-boot:run      # starts Kafka, kafka-init, Elasticsearch, and Grafana via compose.yaml
```

The one-shot `kafka-init` service creates the topics the `local`
profile's `demo` pipeline needs (`demo.events`, `demo.events.kafkafim-dlq`; 3 partitions,
replication 1). kafkafim never creates topics ([ADR-0021](docs/decisions/0021-no-resource-provisioning.md)),
and the local broker has topic auto-creation disabled to mirror production. Once
the `demo` pipeline is `running`, run the smoke test
(`curl -s localhost:8080/actuator/prometheus | grep 'kafkafim_pipeline_state{pipeline="demo",state="running"}'`):

```bash
echo '{"id":"smoke-1","region":"eu","@timestamp":"2026-11-08T10:00:00Z"}' | \
  docker compose run --rm -T --no-deps --entrypoint /opt/kafka/bin/kafka-console-producer.sh \
  kafka-init --bootstrap-server kafka:19092 --topic demo.events
curl -s 'localhost:9200/demo-*/_search?pretty'     # the document appears in demo-eu-<date>
docker compose down -v                             # reset the stack and its data
```

The native broker image ships no Kafka CLI scripts, so the `kafka-init` service uses the JVM
`apache/kafka` image for topic and producer commands.

To run the real Helm chart instead, use a local kind or Docker Desktop cluster with
in-cluster Kafka, Elasticsearch, and Grafana LGTM (one `helm install`) and secrets mounted
as files like production. For details, see the [local demo guide](deploy/local/README.md):

```bash
deploy/local/demo.sh up && deploy/local/demo.sh smoke
```

```bash
./mvnw verify               # unit + integration tests (Testcontainers)
```

```bash
docker build -t kafkafim:dev .   # runtime image from target/*.jar
```

```bash
./mvnw spotless:apply       # format to Google Java Style before committing
```

The code follows the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html);
`./mvnw verify` fails on unformatted code or Checkstyle violations.

Actuator: `http://localhost:8080/actuator/health`, `/actuator/prometheus`.
Grafana (local stack): `http://localhost:3000`, with the kafkafim dashboards in the *kafkafim*
folder. The [Grafana guide](deploy/grafana/README.md) covers those dashboards and the Prometheus
alert rules for production. The local stack doesn't load the rules, and its latency panels
stay empty unless you export seconds (see the *Compose demo* section of that guide).

## License

[MIT](LICENSE).
