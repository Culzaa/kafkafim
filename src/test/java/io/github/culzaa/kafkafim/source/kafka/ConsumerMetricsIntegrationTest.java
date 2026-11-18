package io.github.culzaa.kafkafim.source.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.culzaa.kafkafim.KafkaTestContainers;
import io.github.culzaa.kafkafim.KafkaTopics;
import io.github.culzaa.kafkafim.config.KafkaCluster;
import io.github.culzaa.kafkafim.config.KafkafimProperties;
import io.github.culzaa.kafkafim.config.Pipeline;
import io.github.culzaa.kafkafim.telemetry.KafkaClientMeters;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The Kafka clients' metrics (ADR-0013, ADR-0028) against a real broker: a running pipeline's
 * consumers show {@code kafka.consumer.*} series tagged {@code pipeline} and {@code kafka_cluster},
 * with no lag or lead series, one series per metric and client, and a stopped pipeline leaves none.
 */
@Testcontainers
class ConsumerMetricsIntegrationTest {

  @Container static final KafkaContainer KAFKA = KafkaTestContainers.kafka();

  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  private static Admin admin;

  @BeforeAll
  static void connect() {
    admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
  }

  @AfterAll
  static void disconnect() {
    admin.close();
  }

  @Test
  void consumerSeriesCarryPipelineAndClusterAndLeaveWithTheStoppedPipeline() throws Exception {
    String topic = "t-" + UUID.randomUUID().toString().substring(0, 8);
    KafkaTopics.create(admin, List.of(new NewTopic(topic, 2, (short) 1)));
    try (KafkaProducer<byte[], byte[]> producer =
        new KafkaProducer<>(KafkaTopics.seedingProducerConfig(KAFKA.getBootstrapServers()))) {
      for (int i = 0; i < 10; i++) {
        producer.send(new ProducerRecord<>(topic, i % 2, null, new byte[10]));
      }
      producer.flush();
    }
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
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
    KafkaCluster local =
        new KafkaCluster(List.of(KAFKA.getBootstrapServers()), null, null, null, null, null);
    KafkafimProperties properties =
        new KafkafimProperties(
            false,
            new KafkafimProperties.Kafka(Map.of("local", local)),
            null,
            Map.of("orders", new Pipeline(config, null, null, null, null, null)));
    ConsumerFactories factories =
        new ConsumerFactories(properties, null, new KafkaClientMeters(registry));
    List<Object> received = new CopyOnWriteArrayList<>();
    KafkaPipelineSource source =
        new KafkaPipelineSource(
            "orders",
            config,
            factories.get("local", null),
            new SourceRecordHandler() {
              @Override
              public void onBatch(List<SourceRecord> batch) {
                received.addAll(batch);
                batch.forEach(SourceRecord::complete);
              }

              @Override
              public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

              @Override
              public void onSourceFailed(String cause) {}
            },
            SourceOptions.defaults()
                .withDesiredConcurrency(2)
                .withDrainTimeout(Duration.ofSeconds(5)));
    source.start();
    try {
      await().atMost(TIMEOUT).until(() -> received.size() == 10);
      List<Meter> meters = kafkaMeters(registry);
      assertThat(meters).isNotEmpty();
      assertThat(meters)
          .allSatisfy(m -> {
            assertThat(m.getId().getTag("pipeline")).isEqualTo("orders");
            assertThat(m.getId().getTag("kafka_cluster")).isEqualTo("local");
            assertThat(m.getId().getName()).doesNotStartWith("kafka.producer.");
          });
      assertThat(meters.stream().map(m -> m.getId().getName()))
          .as("no lag or lead series: consumer lag is monitored outside kafkafim")
          .noneMatch(n -> n.contains("records.lag") || n.contains("records.lead"))
          .contains("kafka.consumer.coordinator.commit.latency.avg");
      assertThat(meters.stream().map(m -> m.getId().getTag("client.id")).distinct())
          .as("one client per container thread")
          .hasSize(2);
      // One series per metric and client: the same id is never registered twice.
      assertThat(meters.stream().map(m -> m.getId().toString()).distinct()).hasSameSizeAs(meters);
      assertThat(
              meters.stream()
                  .map(m -> m.getId().getName().replace('.', '_'))
                  .noneMatch(n -> n.startsWith("kafka_consumer_kafka_consumer")))
          .isTrue();
    } finally {
      source.stop();
    }
    assertThat(kafkaMeters(registry)).as("stopping the pipeline removes its series").isEmpty();
  }

  private static List<Meter> kafkaMeters(SimpleMeterRegistry registry) {
    return registry.getMeters().stream()
        .filter(m -> m.getId().getName().startsWith("kafka."))
        .collect(Collectors.toList());
  }
}
