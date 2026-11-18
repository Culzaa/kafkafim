package io.github.culzaa.kafkafim.telemetry;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

/**
 * The Kafka clients' own metrics (ADR-0013): Micrometer's {@link KafkaClientMetrics}, the one and
 * only binder of them, on the registry that also carries the {@code kafkafim_*} series, so they
 * reach OTLP through the same exporter. The OpenTelemetry {@code kafka-clients} library's own
 * metrics reporter is not used: Spring Boot's OpenTelemetry bean has no exporting meter provider,
 * so it would export nothing, and binding both would duplicate every series.
 *
 * <p>A binding lives as long as its client: {@link Binding#close()} removes every series of the
 * client from the registry (ADR-0028: a stopped pipeline or a replaced DLQ producer leaves none
 * behind). Per-partition lag and lead series are denied (see {@link #denyLagAndLead}): consumer lag
 * is monitored outside kafkafim.
 *
 * <p>The metric names are Micrometer's ({@code kafka.consumer.*}, {@code kafka.producer.*}); the
 * existing {@code kafkafim_kafka_connected} keeps reading {@code connection-count} itself and is
 * unchanged.
 */
public final class KafkaClientMeters {

  private final MeterRegistry registry;

  /**
   * Creates the binder and denies the series kafkafim does not export.
   *
   * @param registry the application's registry
   */
  public KafkaClientMeters(MeterRegistry registry) {
    this.registry = registry;
    registry.config().meterFilter(denyLagAndLead());
  }

  /**
   * Binds the metrics of a consumer.
   *
   * @param consumer a live consumer
   * @param tags added to every series ({@code pipeline}, {@code kafka_cluster})
   * @return the binding; close it when the consumer closes
   */
  public Binding bind(Consumer<?, ?> consumer, Tags tags) {
    return register(new KafkaClientMetrics(consumer, tags));
  }

  /**
   * Binds the metrics of a producer.
   *
   * @param producer a live producer
   * @param tags added to every series ({@code kafka_cluster})
   * @return the binding; close it when the producer closes
   */
  public Binding bind(Producer<?, ?> producer, Tags tags) {
    return register(new KafkaClientMetrics(producer, tags));
  }

  /**
   * Binds the metrics of a producer for as long as it lives: the returned producer delegates
   * everything, and closing it removes the series after closing the delegate.
   *
   * @param producer a live producer
   * @param tags added to every series
   * @return a producer to use instead of {@code producer}
   */
  @SuppressWarnings("unchecked")
  public <K, V> Producer<K, V> bound(Producer<K, V> producer, Tags tags) {
    Binding binding = bind(producer, tags);
    return (Producer<K, V>)
        Proxy.newProxyInstance(
            Producer.class.getClassLoader(),
            new Class<?>[] {Producer.class},
            (proxy, method, args) -> {
              try {
                return method.invoke(producer, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              } finally {
                if (method.getName().equals("close")) {
                  binding.close();
                }
              }
            });
  }

  private Binding register(KafkaClientMetrics metrics) {
    metrics.bindTo(registry);
    return metrics::close;
  }

  /** Denies the per-partition series of consumer lag and lead; everything else stays. */
  static MeterFilter denyLagAndLead() {
    return new MeterFilter() {
      @Override
      public MeterFilterReply accept(Meter.Id id) {
        String name = id.getName();
        boolean lagOrLead =
            name.startsWith("kafka.consumer.fetch.manager.records.lag")
                || name.startsWith("kafka.consumer.fetch.manager.records.lead");
        return lagOrLead ? MeterFilterReply.DENY : MeterFilterReply.NEUTRAL;
      }
    };
  }

  /** The metrics of one client on the registry. */
  @FunctionalInterface
  public interface Binding extends AutoCloseable {
    /** Removes the client's series; safe to call more than once. */
    @Override
    void close();
  }
}
