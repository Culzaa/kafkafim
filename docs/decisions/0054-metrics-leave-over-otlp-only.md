# ADR-0054: Metrics leave over OTLP only; kafkafim serves no Prometheus endpoint

Status: accepted · Date: 2026-11-18 · Supersedes the Prometheus part of
[ADR-0013](0013-observability-otel-via-spring-boot.md) (metrics: "and `/actuator/prometheus`")

## Context

ADR-0013 exported metrics two ways: Micrometer → OTLP, and a Prometheus registry served
at `/actuator/prometheus`. Production never scraped kafkafim: the clusters have no
Prometheus Operator CRDs and no one wired a scrape job (ADR-0015, ADR-0023). Two paths meant
two sets of tests and two unit conventions, and the Kafka clients' metrics had to be bound
to both. Issues 343 to 346 removed the registry, the endpoint and the chart's scrape
support.

## Decision

- **OTLP is the only metrics path.** Micrometer exports to
  `${OTEL_EXPORTER_OTLP_ENDPOINT}/v1/metrics`, together with traces and logs. There is no
  Prometheus registry and no `/actuator/prometheus` (it answers 404). The actuator
  exposes `health`, `info`, `metrics` and `reload`; `/actuator/metrics/<name>` serves
  the same meters for a quick look at one pod, with no histogram buckets.
- **A collector or a backend with OTLP ingestion is required.** Elastic APM Server,
  an OpenTelemetry Collector, or any Prometheus-compatible backend that accepts OTLP
  all work; a kafkafim pod with no reachable endpoint exports nothing, and there is no
  scrape fallback.
- **Metric names stay the same.** OTLP metrics use seconds as the base time unit, so
  Prometheus-compatible backends show the names DESIGN §10 lists. The dashboards and
  alert rules in `deploy/grafana/` are unchanged.
- **The chart has no scrape support.** No `ServiceMonitor`, no `prometheus.io/*` pod
  annotations; a values file that still sets `serviceMonitor` fails the schema.
- **"Metrics missing" means checking the export.** The RUNBOOK §3.3 says so: the
  collector, the endpoint value and the pod's logs, not a port-forward to a scrape
  endpoint.

## Consequences

- Anyone who scraped `/actuator/prometheus` (a hand-made job, the old default pod
  annotations) loses that source and must receive OTLP instead.
- One metrics pipeline to test and to reason about; the Kafka clients' metrics are
  bound once (ADR-0049).
- A local look at a pod is `/actuator/metrics/<name>`; percentiles need the backend.

## Alternatives

Keep the endpoint as an opt-in. Rejected: nothing used it, and an unused path rots (the
load test already moved to `/actuator/metrics`).
