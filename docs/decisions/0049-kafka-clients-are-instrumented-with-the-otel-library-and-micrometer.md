# ADR-0049: Kafka clients are traced with the OpenTelemetry kafka-clients library and measured with Micrometer

Status: accepted · Date: 2026-11-17 · Refines [ADR-0013](0013-observability-otel-via-spring-boot.md)
and [ADR-0029](0029-batch-spans-recorded-at-batch-end-failed-batches-always-sampled.md)

## Context

ADR-0013 left the Kafka clients uninstrumented: the consumer containers and the DLQ
producer are built by hand, so Spring Boot's Kafka observation never applies, and no
agent runs. No Kafka span reached Elastic APM, and a `pipeline.batch` span had no parent
in the trace that produced its records, only `traceparent` links. Issues 321 to
326 add them. The spike in issue 321 and the metrics check in issue 324 settled what the
real libraries do.

## Decision

- **One tracing mechanism: `opentelemetry-kafka-clients-2.6`.** The OpenTelemetry
  Java agent is not attached (the Dockerfile says so) and Spring Kafka observation is
  never enabled (`setObservationEnabled` is never called). Two mechanisms would trace
  each client twice.
- **Instrument each client once.** One `KafkaTelemetry` bean
  (`telemetry/KafkaTelemetryConfiguration`, absent under the `validate` profile) is
  built from Boot's `OpenTelemetry`. Consumers are wrapped with a
  `DefaultKafkaConsumerFactory` post-processor in `ConsumerFactories`; the DLQ
  producer is wrapped in `KafkaDlqClient`, each replacement producer included. The
  library's interceptor config properties are not used: with the wrapper they would
  double every span.
- **Spans.** One `process` span (kind consumer) per delivered record, with
  `messaging.destination.name`, `messaging.destination.partition.id`,
  `messaging.kafka.consumer.group`, `messaging.client_id` and
  `messaging.kafka.message.offset`; one `publish` span (kind producer) per DLQ send,
  which also injects `traceparent` (it replaces one copied from the source record).
  Receive spans stay off (the library default): kafkafim processes records after the
  poll, so a receive span adds one span per poll and no information. A process span is
  microseconds long, because it ends as Spring's iteration over the polled records
  ends: it marks delivery, not processing time.
- **Parenting (refines ADR-0029).** The process span's context is recorded per record
  when the library ends the span (`telemetry/ProcessSpanContexts`, an attribute
  extractor on the `KafkaTelemetry` builder: Spring iterates the records itself, so no
  span is current in the listener) and travels with the record. `pipeline.batch` is a
  child of the first record of the batch that has one; the other records'
  `traceparent` headers stay span links. A batch whose records have no context is a
  root span as before. The DLQ publish span is a child of the record's process span.
  The trace reads: Kafka process span → `pipeline.batch` → `es.bulk`. ADR-0029's
  "links, never parents" now holds for all records but the first.
- **Sampling is parent-based.** A `pipeline.batch` span with `error.type` is always
  sampled, whatever its parent decided; every other span follows its parent's sampled
  flag, and only a root span, a batch without a Kafka parent, uses the ratio
  (`management.tracing.sampling.probability`, 1% by default). A healthy batch is kept
  exactly when its Kafka process span is, which is decided by the producer's
  sampler, not by kafkafim. A failed batch under an unsampled parent is exported
  without its Kafka parent.
- **One metrics binder: Micrometer `KafkaClientMetrics`.** The library's own metrics
  reporter (`metricConfigProperties()`) is rejected and is not configured. Boot's
  `OpenTelemetry` bean has no exporting `MeterProvider` (no `SdkMeterProvider` bean in
  any Boot 4.1.2 jar; `KafkaClientMetersTest` pins it), so the reporter would register
  instruments that are exported nowhere. `KafkaClientMetrics` binds to the registry
  that carries the `kafkafim_*` series, so `kafka.consumer.*` and `kafka.producer.*`
  reach OTLP with them. Binding lives as long as the client (ADR-0028): no `kafka.*`
  series is left after a pipeline stops or a DLQ producer closes. Per-partition and
  client-wide lag and lead series are denied (ADR-0034). Tags and cardinality are in
  DESIGN §10.
- **No new log output.** The library adds none; the Logback appender stays the one
  log path (ADR-0044). A test asserts that nothing from `io.opentelemetry` is logged.
- **Version and internal API.** The artifact is pinned through the
  `opentelemetry-logback-appender.version` property, so it follows Boot's OTel BOM
  (instrumentation 2.N pairs with SDK 1.(N+34)), under the same convergence check and
  Dependabot rule. It is an alpha artifact, like the appender. The process-span hook
  uses the library's `internal` package (`KafkaProcessRequest`, from
  `opentelemetry-kafka-clients-common-0.11`, pinned with it), which is not API: a
  version bump can break it. `ConsumerTracingIntegrationTest` fails if it does;
  the fallback is links only, as before.

## Alternatives

- **OpenTelemetry Java agent.** Rejected again (ADR-0013): an agent to manage in the
  image, and it would double the spans of a wrapper.
- **Spring Kafka observation.** The hand-built containers and producer do not use
  Spring's templates, and its spans would differ from the OTel library's.
- **Interceptor properties instead of wrapping.** Also yield one span per record, but
  the wrapper keeps the mechanism in one place and leaves `ConsumerSettings` (the
  user's passthrough properties) alone.
- **The library's metrics reporter.** Needs an exporting meter provider that Boot does
  not build; adding one would be a second metrics pipeline next to Micrometer's.
- **Receive spans on.** One span per poll for no extra information.

## Consequences

- Span volume grows by one `process` span per record, sampled by the producer's
  decision: at high throughput, watch the APM span count (RUNBOOK §3.4). A batch under
  an unsampled parent is dropped, so a 1% producer sampler means about 1% of healthy
  batches, not the 1% of batches that kafkafim's ratio alone would keep.
- The library is alpha and its hook is internal: a bump needs the integration tests.
- Series count grows by the clients' metric set (consumer threads plus one producer
  per cluster), a function of the configuration, not of traffic.
