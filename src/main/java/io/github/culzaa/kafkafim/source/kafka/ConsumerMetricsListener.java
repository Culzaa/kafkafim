package io.github.culzaa.kafkafim.source.kafka;

import io.github.culzaa.kafkafim.config.KafkafimProperties;
import io.github.culzaa.kafkafim.telemetry.KafkaClientMeters;
import io.micrometer.core.instrument.Tags;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.jspecify.annotations.Nullable;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Binds the Kafka metrics of every consumer a {@link ConsumerFactories} factory creates, tagged
 * {@code pipeline} and {@code kafka_cluster}, and removes them when the consumer closes (ADR-0028).
 * A factory is shared by the pipelines of a cluster, so the pipeline comes from the consumer's
 * client id, {@code kafkafim-<pipeline>-<n>} (one per container thread); consumers of no configured
 * pipeline (the metadata consumer of {@code partitionCount()}, which closes at once) get no series.
 */
final class ConsumerMetricsListener implements ConsumerFactory.Listener<byte[], byte[]> {

  private static final String CLIENT_ID_PREFIX = "kafkafim-";
  private static final Pattern CONCURRENCY_SUFFIX = Pattern.compile("-\\d+$");

  private final KafkaClientMeters meters;
  private final KafkafimProperties properties;
  private final String cluster;
  private final Map<Consumer<?, ?>, KafkaClientMeters.Binding> bindings = new ConcurrentHashMap<>();

  ConsumerMetricsListener(KafkaClientMeters meters, KafkafimProperties properties, String cluster) {
    this.meters = meters;
    this.properties = properties;
    this.cluster = cluster;
  }

  @Override
  public void consumerAdded(@Nullable String id, Consumer<byte[], byte[]> consumer) {
    String pipeline = pipelineOf(id, properties.pipelines().keySet());
    if (pipeline != null) {
      bindings.computeIfAbsent(
          consumer, c -> meters.bind(c, Tags.of("pipeline", pipeline, "kafka_cluster", cluster)));
    }
  }

  @Override
  public void consumerRemoved(@Nullable String id, Consumer<byte[], byte[]> consumer) {
    KafkaClientMeters.Binding binding = bindings.remove(consumer);
    if (binding != null) {
      binding.close();
    }
  }

  /**
   * The pipeline whose consumer {@code id} is, or {@code null}. Spring Kafka prefixes the client id
   * with the group id (or a placeholder when the factory is asked for none) and the container
   * always appends {@code -<n>} per consumer thread, so the id must end in {@code
   * kafkafim-<pipeline>-<n>}. Requiring the suffix makes the match unambiguous even for pipelines
   * named {@code orders} and {@code orders-1}: {@code kafkafim-orders-1} is thread 1 of the first,
   * {@code kafkafim-orders-1-0} thread 0 of the second.
   */
  static @Nullable String pipelineOf(@Nullable String id, Set<String> pipelines) {
    if (id == null || !CONCURRENCY_SUFFIX.matcher(id).find()) {
      return null;
    }
    String withoutSuffix = CONCURRENCY_SUFFIX.matcher(id).replaceFirst("");
    for (String name : pipelines) {
      String clientId = CLIENT_ID_PREFIX + name;
      if (withoutSuffix.endsWith(clientId)
          && (withoutSuffix.length() == clientId.length()
              || withoutSuffix.charAt(withoutSuffix.length() - clientId.length() - 1) == '.')) {
        return name;
      }
    }
    return null;
  }
}
