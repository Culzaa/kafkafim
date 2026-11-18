package io.github.culzaa.kafkafim.dlq;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.culzaa.kafkafim.KafkaTestContainers;
import io.github.culzaa.kafkafim.config.KafkaCluster;
import io.github.culzaa.kafkafim.config.KafkafimProperties;
import io.github.culzaa.kafkafim.config.Pipeline;
import io.github.culzaa.kafkafim.telemetry.KafkaClientMeters;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.kafkaclients.v2_6.KafkaTelemetry;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The DLQ producer's Kafka metrics (ADR-0013, ADR-0028) against a real broker, on the registry
 * shape the application has (a composite registry): {@code kafka.producer.*} series tagged {@code
 * kafka_cluster} only (the producer is shared by the cluster's pipelines), next to the consumers'
 * series, and gone once the producers close.
 */
@Testcontainers
class DlqProducerMetricsIntegrationTest {

  @Container
  static final KafkaContainer KAFKA =
      KafkaTestContainers.kafka().withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

  @Test
  void producerSeriesCarryTheClusterAndLeaveWithTheProducer() {
    SimpleMeterRegistry simple = new SimpleMeterRegistry();
    CompositeMeterRegistry registry = new CompositeMeterRegistry();
    registry.add(simple);
    KafkaClientMeters meters = new KafkaClientMeters(registry);
    KafkaCluster local =
        new KafkaCluster(List.of(KAFKA.getBootstrapServers()), null, null, null, null, null);
    Pipeline pipeline =
        new Pipeline(
            new Pipeline.Source(
                "local", List.of("orders"), null, null, null, null, null, null, null, null),
            null,
            null,
            null,
            null,
            null);
    DlqProducers producers =
        new DlqProducers(
            new KafkafimProperties(
                false,
                new KafkafimProperties.Kafka(Map.of("local", local)),
                null,
                Map.of("orders", pipeline, "billing", pipeline)),
            DlqListener.NONE,
            DlqOptions.DEFAULTS,
            KafkaTelemetry.create(OpenTelemetry.noop()),
            meters);
    producers.writer("orders");
    producers.writer("billing"); // the same shared producer

    // A consumer's series next to it, with its own tag keys.
    KafkaConsumer<byte[], byte[]> consumer =
        new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,
                "g",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class));
    final KafkaClientMeters.Binding consumerBinding =
        meters.bind(consumer, Tags.of("pipeline", "orders", "kafka_cluster", "local"));

    List<Meter> producerMeters = producerMeters(simple);
    assertThat(producerMeters).isNotEmpty();
    assertThat(producerMeters)
        .allSatisfy(m -> {
          assertThat(m.getId().getTag("kafka_cluster")).isEqualTo("local");
          assertThat(m.getId().getTag("pipeline")).isNull();
        });
    assertThat(producerMeters.stream().map(m -> m.getId().getTag("client.id")).distinct())
        .as("one shared producer for the cluster")
        .hasSize(1);
    assertThat(producerMeters.stream().map(m -> m.getId().toString()).distinct())
        .hasSameSizeAs(producerMeters);
    assertThat(simple.getMeters().stream().map(m -> m.getId().getName()))
        .anyMatch(name -> name.startsWith("kafka.consumer."));

    consumerBinding.close();
    consumer.close();
    producers.close();
    assertThat(simple.getMeters().stream().filter(m -> m.getId().getName().startsWith("kafka.")))
        .as("closing the producers removes their series")
        .isEmpty();
  }

  private static List<Meter> producerMeters(SimpleMeterRegistry registry) {
    return registry.getMeters().stream()
        .filter(m -> m.getId().getName().startsWith("kafka.producer."))
        .collect(Collectors.toList());
  }
}
