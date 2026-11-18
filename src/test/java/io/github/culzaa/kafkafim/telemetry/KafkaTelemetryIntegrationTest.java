package io.github.culzaa.kafkafim.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.culzaa.kafkafim.KafkaTestContainers;
import io.github.culzaa.kafkafim.KafkaTopics;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.instrumentation.kafkaclients.v2_6.KafkaTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The spike of the OTel Kafka client instrumentation series: what {@code
 * opentelemetry-kafka-clients-2.6} really does with kafkafim's client shapes, against a real broker
 * and in-memory exporters. It pins the facts the series' later issues build on: the consumer
 * wrapper works inside Spring's {@code ConcurrentMessageListenerContainer} (wrapping through a
 * consumer post-processor), the interceptor config properties are the alternative, a wrapped
 * producer injects the trace context, and the library has its own metrics reporter.
 */
class KafkaTelemetryIntegrationTest {

  private static final KafkaContainer KAFKA =
      KafkaTestContainers.kafka().withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  private static Admin admin;

  private final List<SpanData> spans = new CopyOnWriteArrayList<>();
  private final Map<String, MetricData> metrics = new ConcurrentHashMap<>();
  private OpenTelemetrySdk sdk;

  @BeforeAll
  static void startKafka() {
    KAFKA.start();
    admin = KafkaTopics.awaitedAdmin(KAFKA.getBootstrapServers());
  }

  @AfterAll
  static void stopKafka() {
    if (admin != null) {
      admin.close();
    }
    KAFKA.stop();
  }

  @AfterEach
  void closeSdk() {
    if (sdk != null) {
      sdk.close();
    }
  }

  private KafkaTelemetry telemetry() {
    sdk =
        OpenTelemetrySdk.builder()
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .setTracerProvider(
                SdkTracerProvider.builder()
                    .addSpanProcessor(SimpleSpanProcessor.create(new Capture()))
                    .build())
            .setMeterProvider(
                SdkMeterProvider.builder()
                    .registerMetricReader(
                        PeriodicMetricReader.builder(new MetricCapture())
                            .setInterval(Duration.ofMillis(500))
                            .build())
                    .build())
            .build();
    return KafkaTelemetry.create(sdk);
  }

  private String topic(String name, int partitions) throws Exception {
    admin.createTopics(List.of(new NewTopic(name, partitions, (short) 1))).all().get();
    return name;
  }

  private Map<String, Object> consumerConfig(String group) {
    Map<String, Object> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    config.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    return config;
  }

  private Map<String, Object> producerConfig() {
    return Map.of(
        "bootstrap.servers", KAFKA.getBootstrapServers(),
        "key.serializer", ByteArraySerializer.class,
        "value.serializer", ByteArraySerializer.class);
  }

  private void send(Producer<byte[], byte[]> producer, String topic, int count) throws Exception {
    for (int i = 0; i < count; i++) {
      producer.send(new ProducerRecord<>(topic, ("v" + i).getBytes())).get();
    }
  }

  /** Runs a Spring batch-listener container over {@code factory}; returns the records handled. */
  private int consume(DefaultKafkaConsumerFactory<byte[], byte[]> factory, String topic, int n) {
    List<Object> handled = new CopyOnWriteArrayList<>();
    ContainerProperties properties = new ContainerProperties(topic);
    properties.setAckMode(ContainerProperties.AckMode.MANUAL);
    properties.setMessageListener(
        (BatchAcknowledgingMessageListener<byte[], byte[]>)
            (records, ack) -> {
              handled.addAll(records);
              ack.acknowledge();
            });
    ConcurrentMessageListenerContainer<byte[], byte[]> container =
        new ConcurrentMessageListenerContainer<>(factory, properties);
    container.setConcurrency(2);
    container.start();
    try {
      await().atMost(TIMEOUT).until(() -> handled.size() >= n);
      // Receive and process spans are exported as the container's next poll completes.
      await().atMost(TIMEOUT).until(() -> spansOf("process").size() >= n);
    } finally {
      container.stop();
    }
    // A duplicate span would be exported by now: the container has stopped and the processor is
    // synchronous, so the counts the tests assert are final.
    return handled.size();
  }

  private List<SpanData> spansOf(String operation) {
    return spans.stream()
        .filter(s -> s.getName().toLowerCase(Locale.ROOT).contains(operation))
        .collect(Collectors.toList());
  }

  @Test
  void consumerWrappedByPostProcessorInsideSpringContainerTracesOncePerRecord() throws Exception {
    KafkaTelemetry telemetry = telemetry();
    String topic = topic("spike.wrap", 2);
    try (Producer<byte[], byte[]> producer = new KafkaProducer<>(producerConfig())) {
      send(producer, topic, 6);
    }
    DefaultKafkaConsumerFactory<byte[], byte[]> factory =
        new DefaultKafkaConsumerFactory<>(consumerConfig("g-wrap"));
    factory.addPostProcessor(telemetry::wrap);

    assertThat(consume(factory, topic, 6)).isEqualTo(6);

    List<SpanData> process = spansOf("process");
    assertThat(process).hasSize(6);
    assertThat(process).allMatch(s -> s.getKind() == SpanKind.CONSUMER);
    assertThat(process.get(0).getAttributes().asMap().keySet().stream().map(Object::toString))
        .contains(
            "messaging.destination.name",
            "messaging.destination.partition.id",
            "messaging.kafka.consumer.group",
            "messaging.client_id");
    // Receive spans are off by default (setMessagingReceiveTelemetryEnabled): one process span per
    // record is the whole default output.
    assertThat(spansOf("receive")).isEmpty();
  }

  @Test
  void consumerInterceptorPropertiesTraceTheSameWayWithoutWrapping() throws Exception {
    KafkaTelemetry telemetry = telemetry();
    String topic = topic("spike.interceptor", 2);
    try (Producer<byte[], byte[]> producer = new KafkaProducer<>(producerConfig())) {
      send(producer, topic, 6);
    }
    Map<String, Object> config = consumerConfig("g-interceptor");
    config.putAll(telemetry.consumerInterceptorConfigProperties());

    assertThat(consume(new DefaultKafkaConsumerFactory<>(config), topic, 6)).isEqualTo(6);

    assertThat(spansOf("process")).hasSize(6);
  }

  @Test
  void wrappedProducerPublishesOneSpanAndInjectsTraceContext() throws Exception {
    KafkaTelemetry telemetry = telemetry();
    String topic = topic("spike.producer", 1);
    try (Producer<byte[], byte[]> producer =
        telemetry.wrap(new KafkaProducer<byte[], byte[]>(producerConfig()))) {
      send(producer, topic, 3);
    }
    List<SpanData> publish = spansOf("publish");
    assertThat(publish).hasSize(3);

    // A consumer of those records continues the producer's trace.
    DefaultKafkaConsumerFactory<byte[], byte[]> factory =
        new DefaultKafkaConsumerFactory<>(consumerConfig("g-producer"));
    factory.addPostProcessor(telemetry::wrap);
    consume(factory, topic, 3);
    Set<String> publishSpanIds =
        publish.stream().map(SpanData::getSpanId).collect(Collectors.toSet());
    // With receive spans off, the producer's span is the parent of each process span.
    assertThat(spansOf("process"))
        .allMatch(s -> publishSpanIds.contains(s.getParentSpanContext().getSpanId()));
  }

  @Test
  void metricsReporterPropertiesExportTheClientMetrics() throws Exception {
    KafkaTelemetry telemetry = telemetry();
    String topic = topic("spike.metrics", 1);
    Map<String, Object> producerConfig = new HashMap<>(producerConfig());
    producerConfig.putAll(telemetry.metricConfigProperties());
    try (Producer<byte[], byte[]> producer = new KafkaProducer<>(producerConfig)) {
      send(producer, topic, 3);
      await()
          .atMost(TIMEOUT)
          .until(() -> metrics.keySet().stream().anyMatch(n -> n.startsWith("kafka.producer.")));
    }
    List<String> names = new ArrayList<>(metrics.keySet());
    assertThat(names).anyMatch(n -> n.startsWith("kafka.producer."));
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

  private final class MetricCapture implements MetricExporter {
    @Override
    public CompletableResultCode export(Collection<MetricData> batch) {
      batch.forEach(m -> metrics.put(m.getName(), m));
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType type) {
      return AggregationTemporality.CUMULATIVE;
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
