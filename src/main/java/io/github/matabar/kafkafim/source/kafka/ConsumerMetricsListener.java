package io.github.matabar.kafkafim.source.kafka;

import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.telemetry.KafkaClientMeters;
import io.micrometer.core.instrument.Tags;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.jspecify.annotations.Nullable;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Binds the Kafka metrics of every consumer a {@link ConsumerFactories} factory creates, tagged
 * {@code pipeline} and {@code kafka_cluster}, and removes them when the consumer closes (ADR-0028).
 * A factory is shared by the pipelines of a cluster, so the pipeline comes from the consumer's
 * client id, {@code kafkafim-<pipeline>} with an optional {@code -<n>} suffix of the container;
 * consumers of no configured pipeline (the metadata consumer of {@code partitionCount()}, which
 * closes at once) get no series.
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
    String pipeline = pipelineOf(id);
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
   * with the group id (or a placeholder when the factory is asked for none), so the id is matched
   * by its end; of several matches the longest pipeline name wins, and an exact {@code
   * kafkafim-<pipeline>} wins over a {@code -<n>} suffix.
   */
  private @Nullable String pipelineOf(@Nullable String id) {
    if (id == null) {
      return null;
    }
    String best = null;
    for (String name : properties.pipelines().keySet()) {
      String clientId = CLIENT_ID_PREFIX + name;
      if (endsAtBoundary(id, clientId)
          || endsAtBoundary(CONCURRENCY_SUFFIX.matcher(id).replaceFirst(""), clientId)) {
        if (best == null || name.length() > best.length()) {
          best = name;
        }
      }
    }
    return best;
  }

  private static boolean endsAtBoundary(String id, String clientId) {
    return id.endsWith(clientId)
        && (id.length() == clientId.length()
            || id.charAt(id.length() - clientId.length() - 1) == '.');
  }
}
