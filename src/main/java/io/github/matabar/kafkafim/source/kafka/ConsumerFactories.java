package io.github.matabar.kafkafim.source.kafka;

import io.github.matabar.kafkafim.config.KafkaCluster;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.opentelemetry.instrumentation.kafkaclients.v2_6.KafkaTelemetry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

/**
 * One {@link ConsumerFactory} per {@code (Kafka cluster, identity)} for one bound configuration
 * (DESIGN §5.2.1). A factory holds settings only; every container still creates and owns its own
 * {@code KafkaConsumer}s. A reload builds a new instance from the new configuration, so nothing
 * here is ever invalidated in place.
 *
 * <p>Tracing (ADR-0013): when a {@link KafkaTelemetry} is given, every factory registers it as a
 * consumer post-processor, so each consumer the factory creates is wrapped exactly once, in this
 * one place, and records one {@code process} span per record polled. The wrapper is the only
 * tracing mechanism for consumers; the interceptor config properties of the library are never
 * merged into the consumer config, since both together would double every span. Receive spans stay
 * at the library default (off): one span per poll would add volume without telling more than the
 * process spans do. The metadata consumer of {@code partitionCount()} is created by the same
 * factory and is therefore wrapped too, but it never polls, so it exports nothing. The process
 * spans are microseconds long: kafkafim hands records off after the poll and processes them
 * asynchronously, so a span marks delivery from the broker, not processing time.
 */
public final class ConsumerFactories {

  private final KafkafimProperties properties;
  private final @Nullable KafkaTelemetry telemetry;
  private final Map<Key, ConsumerFactory<byte[], byte[]>> factories = new ConcurrentHashMap<>();

  /**
   * Creates an empty cache over a bound configuration.
   *
   * @param properties the validated configuration
   */
  public ConsumerFactories(KafkafimProperties properties) {
    this(properties, null);
  }

  /**
   * Creates an empty cache whose consumers are traced.
   *
   * @param properties the validated configuration
   * @param telemetry wraps every consumer, or {@code null} for untraced consumers
   */
  public ConsumerFactories(KafkafimProperties properties, @Nullable KafkaTelemetry telemetry) {
    this.properties = properties;
    this.telemetry = telemetry;
  }

  /**
   * The factory for a cluster and identity, built on first use.
   *
   * @param cluster name of a {@code kafkafim.kafka.clusters} entry
   * @param identity name of one of its identities, or {@code null} for none
   * @return the cached factory
   * @throws IllegalArgumentException if the cluster or identity is unknown, or the cluster's
   *     passthrough sets a key kafkafim owns
   */
  public ConsumerFactory<byte[], byte[]> get(String cluster, @Nullable String identity) {
    return factories.computeIfAbsent(new Key(cluster, identity), this::create);
  }

  /** Number of distinct factories built so far. */
  int size() {
    return factories.size();
  }

  private ConsumerFactory<byte[], byte[]> create(Key key) {
    KafkaCluster cluster = properties.kafka().clusters().get(key.cluster());
    if (cluster == null) {
      throw new IllegalArgumentException("Unknown Kafka cluster '" + key.cluster() + "'");
    }
    KafkaCluster.KafkaIdentity identity = null;
    if (key.identity() != null) {
      identity = cluster.identities().get(key.identity());
      if (identity == null) {
        throw new IllegalArgumentException(
            "Unknown identity '" + key.identity() + "' on Kafka cluster '" + key.cluster() + "'");
      }
    }
    DefaultKafkaConsumerFactory<byte[], byte[]> factory =
        new DefaultKafkaConsumerFactory<>(ConsumerSettings.forIdentity(cluster, identity));
    if (telemetry != null) {
      factory.addPostProcessor(telemetry::wrap);
    }
    return factory;
  }

  private record Key(String cluster, @Nullable String identity) {}
}
