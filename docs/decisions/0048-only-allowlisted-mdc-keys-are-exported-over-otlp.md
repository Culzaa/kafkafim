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
unlisted one is not. The list names `pipeline`; no code sets an MDC key yet, so it is a
reservation.

## Consequences

- Micrometer Tracing's `traceId` and `spanId` MDC entries are no longer repeated as
  attributes. The record's native trace and span id come from the span context and are
  unchanged.
- This replaces the ADR-0044 consequence that any MDC entry added later is exported
  automatically. DESIGN §10 (Logs) states the rule.
- SLF4J fluent key-value pairs (`addKeyValue`) have no allowlist in the appender, so
  `captureKeyValuePairAttributes` is `false` (#354): a call site that attached a document
  field or a secret would otherwise export it. kafkafim uses none today. Exporting one
  later needs a reviewed change that turns the option on, or an MDC key on the list.
  `MdcAllowlistTest` checks that an unlisted pair is not exported. This also replaces the
  ADR-0044 consequence that key-value pairs added later are exported automatically.
