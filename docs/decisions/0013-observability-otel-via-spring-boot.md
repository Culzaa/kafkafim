# ADR-0013: Observability through Spring Boot's OpenTelemetry starter, OTLP to Elastic APM Server, plus Prometheus endpoint

Status: accepted · Date: 2026-11-07 · Refined by [ADR-0049](0049-kafka-clients-are-instrumented-with-the-otel-library-and-micrometer.md)

## Decision

- Metrics: Micrometer → OTLP (APM Server) and `/actuator/prometheus`. The Kafka
  clients' own metrics come from Micrometer's `KafkaClientMetrics`, the one binder
  (ADR-0049).
- Traces: Micrometer Tracing with the OpenTelemetry bridge → OTLP. One span per bulk
  request under a batch span; incoming `traceparent` Kafka headers become **span
  links**, not parents, except the first record's Kafka process span, which parents
  the batch (ADR-0049). Head sampling 1% of batches without a Kafka parent, errors
  always. The Kafka clients' spans come from the OpenTelemetry `kafka-clients`
  library, not an agent and not Spring Kafka observation (ADR-0049).
- Logs: ECS-structured JSON on stdout (Spring Boot structured logging), OTLP log
  export optional.
- Label discipline: `route` is the route name or position, never the resolved index.
  Partition labels are opt-in.
- Health: `/actuator/health/liveness` = process alive; `/readiness` = config valid
  and Kafka clients connected, deliberately **not** gated on ES health.
- Repo ships Grafana dashboards and alert rules (`deploy/grafana/`).

## Alternatives

OpenTelemetry Java agent (zero-code). Rejected as the primary path: the Boot starter
gives the same OTLP output with explicit, testable instrumentation and no agent to
manage in the image; the agent remains compatible if ever wanted.
