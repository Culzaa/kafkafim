package io.github.culzaa.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.culzaa.kafkafim.sink.es.BatchReport;
import io.github.culzaa.kafkafim.sink.es.FlushReason;
import io.github.culzaa.kafkafim.sink.es.SinkKey;
import io.github.culzaa.kafkafim.telemetry.BatchTracer;
import io.github.culzaa.kafkafim.telemetry.KafkafimMeters;
import io.github.culzaa.kafkafim.telemetry.PipelineSpec;
import io.github.culzaa.kafkafim.telemetry.SourceProbe;
import io.github.culzaa.kafkafim.telemetry.TelemetryNames;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.tracing.Tracer;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Sink and DLQ events into the §10 meters and batch traces (DESIGN §10). */
class TelemetryListenerTest {

  private static final SinkKey KEY = new SinkKey("orders", "search-eu", "writer");
  private static final Instant T0 = Instant.parse("2026-11-08T10:00:00Z");

  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final KafkafimMeters meters = new KafkafimMeters(registry);
  private final TelemetryListener listener =
      new TelemetryListener(meters, new BatchTracer(Tracer.NOOP));

  @AfterEach
  void close() {
    meters.close();
  }

  private void configure() {
    meters.sync(
        Map.of(
            "orders",
            new PipelineSpec(
                "prod-a", List.of("t"), List.of("0"), Set.of("search-eu"), SourceProbe.NONE)),
        Set.of("search-eu"),
        cluster -> null);
  }

  private double counter(String name, String... tags) {
    return registry
        .get(name)
        .tags("pipeline", "orders", "kafka_cluster", "prod-a")
        .tags(tags)
        .counter()
        .count();
  }

  @Test
  void sinkEventsBecomeMeters() {
    configure();

    listener.onFlush(KEY, FlushReason.BYTES, 10, 1000);
    listener.onBulkResponse(KEY, 200, 10, 1000, Duration.ofMillis(20));
    listener.onRequestRetry(KEY, 0, new ConnectException("refused"), 1, Duration.ofMillis(100));
    listener.onRequestRetry(KEY, 503, null, 2, Duration.ofMillis(200));
    listener.onItemRetry(KEY, 429, "es_rejected_execution_exception", 1);
    listener.onItemError(KEY, 400, "mapper_parsing_exception");
    listener.onAuthFailure(KEY, 401, "{}");

    assertThat(counter(TelemetryNames.BATCH_FLUSH, "es_cluster", "search-eu", "reason", "bytes"))
        .isOne();
    assertThat(counter(TelemetryNames.BULK_REQUESTS, "es_cluster", "search-eu", "status", "200"))
        .isOne();
    assertThat(
            counter(TelemetryNames.BULK_REQUESTS, "es_cluster", "search-eu", "status", "io_error"))
        .as("a transport error has no response, so it is counted from the retry")
        .isOne();
    assertThat(counter(TelemetryNames.RETRIES, "es_cluster", "search-eu", "scope", "request"))
        .isEqualTo(2);
    assertThat(counter(TelemetryNames.RETRIES, "es_cluster", "search-eu", "scope", "item")).isOne();
    assertThat(
            counter(
                TelemetryNames.ITEM_ERRORS,
                "es_cluster",
                "search-eu",
                "error_type",
                "es_rejected_execution_exception",
                "retryable",
                "true"))
        .isOne();
    assertThat(
            counter(
                TelemetryNames.ITEM_ERRORS,
                "es_cluster",
                "search-eu",
                "error_type",
                "mapper_parsing_exception",
                "retryable",
                "false"))
        .isOne();
    assertThat(
            counter(TelemetryNames.AUTH_FAILURES, "es_cluster", "search-eu", "identity", "writer"))
        .isOne();
  }

  @Test
  void eventsOfUnknownPipelinesAreDropped() {
    listener.onFlush(KEY, FlushReason.AGE, 1, 1);
    listener.onBatchSent(KEY, report(List.of()));
    assertThat(registry.getMeters()).isEmpty();
  }

  @Test
  void traceParentsComeFromTheSourceRecordsHeadersDistinctAndBounded() {
    List<Object> handles = new ArrayList<>();
    handles.add(ref("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"));
    handles.add(ref("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"));
    handles.add(ref(null));
    for (int i = 0; i < 300; i++) {
      handles.add(ref(String.format("00-%032x-%016x-01", i + 1, i + 1)));
    }

    List<String> parents = TelemetryListener.traceParents(handles);

    assertThat(parents).hasSize(BatchTracer.MAX_LINKS).doesNotHaveDuplicates();
    assertThat(parents.getFirst())
        .isEqualTo("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
  }

  @Test
  void theBatchParentIsTheFirstRecordsProcessSpanContext() {
    String first = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    String second = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    // Untraced records are skipped; the first record that has a context wins.
    assertThat(
            TelemetryListener.parentTraceParent(
                List.of(ref(null, null), ref(null, first), ref(null, second))))
        .isEqualTo(first);
    assertThat(TelemetryListener.parentTraceParent(List.of(ref(null, null)))).isNull();
  }

  private static BatchReport report(List<?> handles) {
    return new BatchReport(
        FlushReason.DOCS,
        1,
        10,
        T0,
        T0,
        T0.plusMillis(5),
        List.of(new BatchReport.Request(T0, T0.plusMillis(5), 1, 10, 200, null, 0, 0)),
        handles);
  }

  private static RecordProcessor.DocumentRef ref(String traceparent) {
    return ref(traceparent, null);
  }

  private static RecordProcessor.DocumentRef ref(String traceparent, String processTraceParent) {
    RecordHeaders headers = new RecordHeaders();
    if (traceparent != null) {
      headers.add(new RecordHeader("traceparent", traceparent.getBytes(StandardCharsets.UTF_8)));
    }
    ConsumerRecord<byte[], byte[]> record =
        new ConsumerRecord<>(
            "t",
            0,
            1,
            T0.toEpochMilli(),
            TimestampType.CREATE_TIME,
            0,
            2,
            null,
            "{}".getBytes(StandardCharsets.UTF_8),
            headers,
            Optional.empty());
    InboundRecord inbound =
        new InboundRecord() {
          @Override
          public ConsumerRecord<byte[], byte[]> record() {
            return record;
          }

          @Override
          public boolean oversize() {
            return false;
          }

          @Override
          public void expectParts(int parts) {}

          @Override
          public void complete() {}

          @Override
          public boolean isAbandoned() {
            return false;
          }

          @Override
          public String processTraceParent() {
            return processTraceParent;
          }
        };
    return new RecordProcessor.DocumentRef(
        inbound, null, new RouteTarget("search-eu", "writer", null, "0"));
  }
}
