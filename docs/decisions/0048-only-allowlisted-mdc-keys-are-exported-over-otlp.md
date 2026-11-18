# ADR-0048: Only allowlisted MDC keys are exported over OTLP

Status: accepted · Date: 2026-11-17 · Refines
[ADR-0044](0044-logs-over-otlp-stdout-is-readable-text.md)

## Context

ADR-0044 configured the OpenTelemetry Logback appender with `captureMdcAttributes=*`, so
every MDC key, including any added later, became a log attribute in the backend. RUNBOOK
says logs never contain document bodies or secret values, and nothing enforced that for
MDC (#327).

## Decision

`logback-spring.xml` lists the exported MDC keys by name (`captureMdcAttributes`), never
`*`. A new key is exported only when a reviewed change adds it to the list.
`MdcAllowlistTest` loads the real file and checks that a listed key is exported and an
unlisted one is not. The list names `pipeline` (#399). Threads that work for one
pipeline set it with `telemetry/PipelineMdc` for the duration of a task and clear it
afterwards: the consumer thread's listener, rebalance callbacks, commit hook and commit callback, the bulk
sink's sender tasks and the DLQ writer's send tasks. Logs written on those threads carry
the pipeline name as an attribute, so a backend can filter logs by pipeline as it does
metrics and spans. Threads owned by client libraries (the Kafka producer's I/O thread, the
Elasticsearch HTTP client's) and the supervisor's own threads are not covered; their
lines still carry the pipeline name in the message text.

## Consequences

- Micrometer Tracing's `traceId` and `spanId` MDC entries are no longer repeated as
  attributes. The record's native trace and span id come from the span context and are
  unchanged.
- This replaces the ADR-0044 consequence that any MDC entry added later is exported
  automatically. DESIGN §10 (Logs) states the rule.
