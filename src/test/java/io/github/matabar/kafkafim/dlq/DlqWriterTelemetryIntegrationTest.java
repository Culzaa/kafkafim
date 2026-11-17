package io.github.matabar.kafkafim.dlq;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.matabar.kafkafim.KafkaTestContainers;
import io.github.matabar.kafkafim.KafkaTopics;
import io.github.matabar.kafkafim.config.KafkaCluster;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.config.Pipeline;
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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The DLQ producer instrumented with the OpenTelemetry {@code kafka-clients} wrapper (ADR-0013),
 * against a real broker: one publish span per send, the trace context in the record's headers next
 * to the {@code kafkafim.*} ones, and the acknowledgement-before-completion contract unchanged.
 */
@Testcontainers
class DlqWriterTelemetryIntegrationTest {

  @Container
  static final KafkaContainer KAFKA =
      KafkaTestContainers.kafka().withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static Admin admin;

  private final List<SpanData> spans = new CopyOnWriteArrayList<>();
  private OpenTelemetrySdk sdk;
  private DlqProducers producers;

  @BeforeAll
  static void connect() {
    admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
  }

  @AfterAll
  static void disconnect() {
    admin.close();
  }

  @AfterEach
  void close() {
    if (producers != null) {
      producers.close();
    }
    if (sdk != null) {
      sdk.close();
    }
  }

  private DlqWriter writer(String sourceTopic) {
    sdk =
        OpenTelemetrySdk.builder()
            // A bare SDK has a no-op propagator; Boot's OpenTelemetry bean has the W3C one.
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .setTracerProvider(
                SdkTracerProvider.builder()
                    .addSpanProcessor(SimpleSpanProcessor.create(new Capture()))
                    .build())
            .build();
    KafkaCluster local =
        new KafkaCluster(List.of(KAFKA.getBootstrapServers()), null, null, null, null, null);
    Pipeline pipeline =
        new Pipeline(
            new Pipeline.Source(
                "local", List.of(sourceTopic), null, null, null, null, null, null, null, null),
            null,
            null,
            null,
            null,
            null);
    producers =
        new DlqProducers(
            new KafkafimProperties(
                false,
                new KafkafimProperties.Kafka(Map.of("local", local)),
                null,
                Map.of("orders", pipeline)),
            DlqListener.NONE,
            DlqOptions.DEFAULTS,
            KafkaTelemetry.create(sdk));
    return producers.writer("orders");
  }

  @Test
  void publishSpanPerSendAndTraceContextNextToTheKafkafimHeaders() throws Exception {
    DlqWriter writer = writer("orders-" + UUID.randomUUID());
    KafkaTopics.create(admin, List.of(new NewTopic(writer.topic(), 1, (short) 1)));
    String staleTrace = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    DlqRequest request =
        new DlqRequest(
            "orders.v2",
            0,
            1,
            null,
            "k".getBytes(StandardCharsets.UTF_8),
            "{\"broken\"".getBytes(StandardCharsets.UTF_8),
            List.<Header>of(
                new RecordHeader("traceparent", staleTrace.getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("app", "x".getBytes(StandardCharsets.UTF_8))),
            "decode",
            "unexpected end of input",
            null,
            null,
            1,
            Instant.now(),
            null);

    RecordMetadata metadata = writer.send(request).get(60, TimeUnit.SECONDS);

    // The span ends when the broker acknowledged, before the send completed.
    assertThat(spans)
        .singleElement()
        .satisfies(s -> assertThat(s.getKind()).isEqualTo(SpanKind.PRODUCER));
    ConsumerRecord<byte[], byte[]> record = readOne(writer.topic(), metadata.offset());
    assertThat(record.headers().headers("traceparent")).hasSize(1);
    String injected =
        new String(record.headers().lastHeader("traceparent").value(), StandardCharsets.UTF_8);
    assertThat(injected)
        .contains(spans.get(0).getTraceId(), spans.get(0).getSpanId())
        .isNotEqualTo(staleTrace);
    assertThat(record.headers().lastHeader("app")).isNotNull();
    assertThat(new String(record.headers().lastHeader("kafkafim.error.type").value()))
        .isEqualTo("decode");
  }

  @Test
  void publishSpanIsParentedOnTheSourceRecordsProcessSpan() throws Exception {
    DlqWriter writer = writer("orders-" + UUID.randomUUID());
    KafkaTopics.create(admin, List.of(new NewTopic(writer.topic(), 1, (short) 1)));
    String processSpan = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    DlqRequest request =
        new DlqRequest(
            "orders.v2",
            0,
            2,
            null,
            null,
            "{\"broken\"".getBytes(StandardCharsets.UTF_8),
            List.of(),
            "decode",
            null,
            null,
            null,
            1,
            Instant.now(),
            null,
            processSpan);

    writer.send(request).get(60, TimeUnit.SECONDS);

    assertThat(spans)
        .singleElement()
        .satisfies(s -> {
          assertThat(s.getTraceId()).isEqualTo("4bf92f3577b34da6a3ce929d0e0e4736");
          assertThat(s.getParentSpanId()).isEqualTo("00f067aa0ba902b7");
          assertThat(s.getKind()).isEqualTo(SpanKind.PRODUCER);
        });
  }

  private static ConsumerRecord<byte[], byte[]> readOne(String topic, long offset) {
    try (KafkaConsumer<byte[], byte[]> consumer =
        new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class))) {
      TopicPartition partition = new TopicPartition(topic, 0);
      consumer.assign(List.of(partition));
      consumer.seek(partition, offset);
      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (System.nanoTime() < deadline) {
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(500))) {
          if (record.offset() == offset) {
            return record;
          }
        }
      }
      throw new AssertionError("no record at " + topic + "@" + offset);
    }
  }

  private final class Capture implements SpanExporter {
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
  }
}
