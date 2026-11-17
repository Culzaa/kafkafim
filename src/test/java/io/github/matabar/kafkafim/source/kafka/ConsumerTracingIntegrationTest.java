package io.github.matabar.kafkafim.source.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.util.unit.DataSize;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The consumers' OpenTelemetry instrumentation (ADR-0013) through {@link ConsumerFactories}, with
 * {@link KafkaPipelineSource} against a real broker and an in-memory span exporter: one {@code
 * process} span per record, never a duplicate, across pause, resume, a rebalance and the commits;
 * and the library writes no log output of its own.
 */
@Testcontainers
class ConsumerTracingIntegrationTest {

  @Container static final KafkaContainer KAFKA = KafkaTestContainers.kafka();

  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  private static Admin admin;
  private static KafkaProducer<byte[], byte[]> producer;

  private final List<SpanData> spans = new CopyOnWriteArrayList<>();
  private final List<KafkaPipelineSource> started = new ArrayList<>();
  private final ListAppender<ILoggingEvent> logback = new ListAppender<>();
  private final List<LogRecord> jul = new CopyOnWriteArrayList<>();
  private final Handler julHandler =
      new Handler() {
        @Override
        public void publish(LogRecord record) {
          jul.add(record);
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
      };
  private final Logger otelLogback = (Logger) LoggerFactory.getLogger("io.opentelemetry");
  private final java.util.logging.Logger otelJul =
      java.util.logging.Logger.getLogger("io.opentelemetry");
  private Level previousLevel;
  private java.util.logging.Level previousJulLevel;
  private OpenTelemetrySdk sdk;
  private ConsumerFactories factories;

  @BeforeAll
  static void connect() {
    admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
    producer = new KafkaProducer<>(KafkaTopics.seedingProducerConfig(KAFKA.getBootstrapServers()));
  }

  @AfterAll
  static void disconnect() {
    producer.close();
    admin.close();
  }

  @BeforeEach
  void instrument() {
    // Everything the library or the SDK could log, at the most verbose level, on both frameworks.
    previousLevel = otelLogback.getLevel();
    otelLogback.setLevel(Level.ALL);
    logback.start();
    otelLogback.addAppender(logback);
    previousJulLevel = otelJul.getLevel();
    otelJul.setLevel(java.util.logging.Level.ALL);
    otelJul.addHandler(julHandler);

    sdk =
        OpenTelemetrySdk.builder()
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .setTracerProvider(
                SdkTracerProvider.builder()
                    .addSpanProcessor(SimpleSpanProcessor.create(new Capture()))
                    .build())
            .build();
    KafkaCluster local =
        new KafkaCluster(List.of(KAFKA.getBootstrapServers()), null, null, null, null, null);
    factories =
        new ConsumerFactories(
            new KafkafimProperties(
                false, new KafkafimProperties.Kafka(Map.of("local", local)), null, null),
            KafkaTelemetry.create(sdk));
  }

  @AfterEach
  void tearDown() {
    started.forEach(KafkaPipelineSource::stop);
    sdk.close();
    otelLogback.detachAppender(logback);
    otelLogback.setLevel(previousLevel);
    otelJul.removeHandler(julHandler);
    otelJul.setLevel(previousJulLevel);
  }

  @Test
  void oneProcessSpanPerRecordAcrossPauseResumeRebalanceAndCommits() throws Exception {
    String topic = createTopic(2);
    produce(topic, 0, 10);
    produce(topic, 1, 10);
    Recorder recorder = new Recorder();
    KafkaPipelineSource first = start("trace-a", topic, recorder, 2);
    await().atMost(TIMEOUT).until(() -> recorder.records.size() == 20);

    // Pause: records produced meanwhile are not fetched, so no span appears for them.
    first.pause("breaker:test");
    await().atMost(TIMEOUT).until(() -> first.pauseReasons().contains("breaker:test"));
    final int before = spans.size();
    produce(topic, 0, 5);
    Thread.sleep(1_500);
    assertThat(recorder.records).hasSize(20);
    assertThat(spans).hasSize(before);
    first.resume("breaker:test");
    await().atMost(TIMEOUT).until(() -> recorder.records.size() == 25);

    // Rebalance: a second member of the same group takes over a partition.
    final KafkaPipelineSource second = start("trace-b", topic, recorder, 1);
    await().atMost(TIMEOUT).until(() -> groupMembers(topic) == 3);
    produce(topic, 0, 5);
    produce(topic, 1, 5);
    // 35 distinct records in all; commitSync / CommitBeforePoll still reach the end offsets.
    await().atMost(TIMEOUT).until(() -> distinct(recorder) == 35 && committedTotal(topic) == 35);

    first.stop();
    second.stop();
    // At-least-once delivery may re-deliver a record after a rebalance; each delivery is one span.
    await().atMost(TIMEOUT).until(() -> spansOf("process").size() == recorder.records.size());
    List<SpanData> process = spansOf("process");
    assertThat(spansOf("receive")).as("receive spans are off by default").isEmpty();
    assertThat(process).allMatch(s -> s.getKind() == SpanKind.CONSUMER);
    assertThat(process)
        .allSatisfy(span -> {
          Map<String, Object> attributes = new HashMap<>();
          span.getAttributes().forEach((k, v) -> attributes.put(k.getKey(), v));
          assertThat(attributes)
              .containsEntry("messaging.destination.name", topic)
              .containsEntry("messaging.kafka.consumer.group", "kafkafim." + topic)
              .containsKeys("messaging.destination.partition.id", "messaging.client_id");
        });
  }

  @Test
  void theInstrumentationAddsNoLogOutput() throws Exception {
    String topic = createTopic(1);
    produce(topic, 0, 10);
    Recorder recorder = new Recorder();
    start("quiet", topic, recorder, 1);
    await().atMost(TIMEOUT).until(() -> recorder.records.size() == 10);
    await().atMost(TIMEOUT).until(() -> spansOf("process").size() == 10);
    started.forEach(KafkaPipelineSource::stop);

    assertThat(logback.list).as("logback events of io.opentelemetry").isEmpty();
    assertThat(jul).as("java.util.logging records of io.opentelemetry").isEmpty();
  }

  private List<SpanData> spansOf(String operation) {
    return spans.stream()
        .filter(s -> s.getName().toLowerCase(Locale.ROOT).contains(operation))
        .toList();
  }

  private static long distinct(Recorder recorder) {
    return recorder.records.stream()
        .map(r -> r.topicPartition() + "@" + r.offset())
        .distinct()
        .count();
  }

  private static int groupMembers(String topic) throws Exception {
    try {
      return admin
          .describeConsumerGroups(List.of("kafkafim." + topic))
          .all()
          .get()
          .get("kafkafim." + topic)
          .members()
          .stream()
          .mapToInt(m -> 1)
          .sum();
    } catch (ExecutionException e) {
      return 0;
    }
  }

  private static long committedTotal(String topic) throws Exception {
    Map<TopicPartition, OffsetAndMetadata> offsets =
        admin.listConsumerGroupOffsets("kafkafim." + topic).partitionsToOffsetAndMetadata().get();
    return offsets.values().stream().mapToLong(OffsetAndMetadata::offset).sum();
  }

  private KafkaPipelineSource start(
      String pipeline, String topic, Recorder recorder, int concurrency) {
    Pipeline.Source config =
        new Pipeline.Source(
            "local",
            List.of(topic),
            "kafkafim." + topic,
            null,
            Pipeline.OffsetReset.EARLIEST,
            100,
            DataSize.ofMegabytes(1),
            Duration.ofMillis(200),
            DataSize.ofBytes(1_000),
            null);
    KafkaPipelineSource source =
        new KafkaPipelineSource(
            pipeline,
            config,
            factories.get("local", null),
            recorder,
            SourceOptions.defaults()
                .withDesiredConcurrency(concurrency)
                .withDrainTimeout(Duration.ofSeconds(5)));
    source.start();
    started.add(source);
    return source;
  }

  private static String createTopic(int partitions) throws Exception {
    String topic = "t-" + UUID.randomUUID().toString().substring(0, 8);
    KafkaTopics.create(admin, List.of(new NewTopic(topic, partitions, (short) 1)));
    return topic;
  }

  private static void produce(String topic, int partition, int count) throws Exception {
    for (int i = 0; i < count; i++) {
      producer.send(new ProducerRecord<>(topic, partition, null, new byte[10]));
    }
    producer.flush();
  }

  private static final class Recorder implements SourceRecordHandler {
    final List<SourceRecord> records = new CopyOnWriteArrayList<>();

    @Override
    public void onBatch(List<SourceRecord> batch) {
      records.addAll(batch);
      batch.forEach(SourceRecord::complete);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

    @Override
    public void onSourceFailed(String cause) {}
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
