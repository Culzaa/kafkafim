package io.github.matabar.kafkafim.dlq;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.instrumentation.kafkaclients.v2_6.KafkaTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The OTel producer wrapper in {@link KafkaDlqClient}: one span per attempt, also across a
 * replacement.
 */
class KafkaDlqClientTelemetryTest {

  private final List<SpanData> spans = new CopyOnWriteArrayList<>();
  private final SpanExporter exporter =
      new SpanExporter() {
        @Override
        public CompletableResultCode export(Collection<SpanData> batch) {
          spans.addAll(batch);
          return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
          return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
          return CompletableResultCode.ofSuccess();
        }
      };
  private final OpenTelemetrySdk sdk =
      OpenTelemetrySdk.builder()
          .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
          .setTracerProvider(
              SdkTracerProvider.builder()
                  .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                  .build())
          .build();
  private final List<MockProducer<byte[], byte[]>> created = new ArrayList<>();

  @AfterEach
  void closeSdk() {
    sdk.close();
  }

  private KafkaDlqClient client() {
    return KafkaDlqClient.instrumented(
        Map.of("bootstrap.servers", "localhost:9092", "client.id", "kafkafim-local-dlq"),
        settings -> {
          MockProducer<byte[], byte[]> producer =
              new MockProducer<>(false, null, new ByteArraySerializer(), new ByteArraySerializer());
          created.add(producer);
          return producer;
        },
        KafkaTelemetry.create(sdk)::wrap);
  }

  private static ProducerRecord<byte[], byte[]> record() {
    return new ProducerRecord<>("dlq", new byte[] {1}, new byte[] {2});
  }

  @Test
  void oneSendIsOnePublishSpanAndInjectsTheTraceContext() {
    KafkaDlqClient client = client();
    ProducerRecord<byte[], byte[]> record = record();
    record.headers().add("kafkafim.error.type", "decode".getBytes());

    client.send(record, (m, e) -> {});
    created.get(0).completeNext();

    assertThat(spans)
        .singleElement()
        .satisfies(span -> assertThat(span.getKind()).isEqualTo(SpanKind.PRODUCER));
    assertThat(created.get(0).history().get(0).headers().headers("traceparent")).hasSize(1);
    assertThat(created.get(0).history().get(0).headers().headers("kafkafim.error.type")).hasSize(1);
  }

  @Test
  void replacementAddsOneAttemptNotDuplicateSpan() {
    KafkaDlqClient client = client();
    ProducerRecord<byte[], byte[]> record = record();
    List<Exception> failures = new ArrayList<>();

    client.send(record, (m, e) -> failures.add(e));
    created.get(0).errorNext(new SaslAuthenticationException("bad password"));

    // The fatal failure closed the raw producer and created a wrapped replacement.
    assertThat(created).hasSize(2);
    assertThat(created.get(0).closed()).isTrue();
    assertThat(failures).hasSize(1);
    assertThat(spans).hasSize(1);

    // The retry is the same record again: a second attempt, one more span, one traceparent.
    client.send(record, (m, e) -> failures.add(e));
    created.get(1).completeNext();

    assertThat(spans).hasSize(2);
    assertThat(failures).hasSize(2).containsNull();
    assertThat(created.get(1).history().get(0).headers().headers("traceparent")).hasSize(1);
  }
}
