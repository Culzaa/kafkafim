# ADR-0055: The OTLP export drops NaN and infinite points

Status: accepted · Date: 2026-11-18 · Refines [ADR-0054](0054-metrics-leave-over-otlp-only.md)

## Context

A Micrometer gauge reports `NaN` for "no value". The Kafka clients' `*-avg` and `*-max`
metrics are `NaN` until their first sample: on an idle producer, 18 of them
(`kafka.producer.request.latency.avg`, `kafka.producer.record.size.max`, and so on). The
OTLP registry exports them as they are. Elastic APM Server writes the token `NaN` into a
JSON document, Elasticsearch rejects it (`Non-standard token 'NaN'`), and the points of
that bulk are lost. kafkafim's own gauges are always finite (issue 475).

## Decision

- **A decorator on the OTLP metrics sender drops every non-finite point** before the request
  leaves (`FiniteMetricsSender`): `NaN` and infinite gauge and sum points, and non-finite
  `sum`, `min` and `max` of a histogram point. A metric left with no point is omitted. The
  body is decoded and encoded again only when something is dropped; a body that does not
  parse is sent unchanged.
- **Skip, do not substitute.** "No value" stays "no value": a zero would claim a measurement
  that never happened and pull averages down in dashboards.
- **At the sender, not per meter.** A `MeterFilter` sees only a meter's identity at
  registration, never its value, so it cannot tell a gauge that is `NaN` now from one that
  will not be. The sender covers every meter (Kafka clients, JVM, Spring Boot, kafkafim)
  with one rule and needs no list of meter names that goes stale on a client upgrade.
  Spring Boot's sender is wrapped by a `BeanPostProcessor`, so its connection details and
  TLS stay in use.

## Consequences

- `micrometer-registry-otlp` and `opentelemetry-proto` are compile-scope dependencies now
  (they were runtime); no new artifact.
- A gauge appears in the backend from its first finite sample on, not from startup.
- Cost: one protobuf decode per export interval (about a minute).
