package io.github.matabar.kafkafim.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import java.util.List;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.opentelemetry.autoconfigure.OpenTelemetrySdkAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class KafkaClientMetersTest {

  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final KafkaClientMeters meters = new KafkaClientMeters(registry);

  @Test
  void boundProducerClosesItsDelegateAndCloseIsIdempotent() {
    MockProducer<byte[], byte[]> raw =
        new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
    Producer<byte[], byte[]> producer = meters.bound(raw, Tags.of("kafka_cluster", "local"));

    // MockProducer has no metrics, so the series themselves are checked against a real broker
    // (DlqProducerMetricsIntegrationTest); here: the proxy closes the delegate, once or twice.
    producer.close();
    assertThat(raw.closed()).isTrue();
    producer.close(); // idempotent
  }

  @Test
  void failureOfTheDelegateReachesTheCallerUnwrapped() {
    MockProducer<byte[], byte[]> raw =
        new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
    Producer<byte[], byte[]> producer = meters.bound(raw, Tags.empty());
    raw.close();

    assertThatThrownBy(() ->
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>("t", new byte[0])))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void consumerBindingAddsTheTagsAndCloseIsIdempotent() {
    MockConsumer<byte[], byte[]> consumer =
        new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());
    KafkaClientMeters.Binding binding =
        meters.bind(consumer, Tags.of("pipeline", "orders", "kafka_cluster", "local"));

    assertThat(kafkaMeters())
        .allSatisfy(m -> assertThat(m.getId().getTag("pipeline")).isEqualTo("orders"));
    binding.close();
    binding.close();
    assertThat(kafkaMeters()).isEmpty();
  }

  @Test
  void lagAndLeadSeriesAreDenied() {
    registry.gauge("kafka.consumer.fetch.manager.records.lag", Tags.of("partition", "0"), 1);
    registry.gauge("kafka.consumer.fetch.manager.records.lag.max", 1);
    registry.gauge("kafka.consumer.fetch.manager.records.lead.min", 1);
    registry.gauge("kafka.consumer.fetch.manager.fetch.size.avg", 1);

    assertThat(kafkaMeters().stream().map(m -> m.getId().getName()))
        .containsExactly("kafka.consumer.fetch.manager.fetch.size.avg");
  }

  /**
   * Why Micrometer's binder and not the library's metrics reporter (#324): Spring Boot's
   * auto-configured OpenTelemetry has no meter provider bean, so the SDK it builds has no metric
   * reader and the reporter's instruments would be exported nowhere.
   */
  @Test
  void bootsOpenTelemetryBeanHasNoExportingMeterProvider() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(OpenTelemetrySdkAutoConfiguration.class))
        .run(context -> {
          assertThat(context).hasSingleBean(OpenTelemetrySdk.class);
          assertThat(context).doesNotHaveBean(SdkMeterProvider.class);
          // A reader-less provider collects nothing, whatever is registered on it.
          OpenTelemetrySdk sdk = context.getBean(OpenTelemetrySdk.class);
          sdk.getMeter("probe").counterBuilder("probe").build().add(1);
          assertThat(
                  sdk.getSdkMeterProvider()
                      .forceFlush()
                      .join(5, java.util.concurrent.TimeUnit.SECONDS)
                      .isSuccess())
              .isTrue();
        });
  }

  private List<Meter> kafkaMeters() {
    return registry.getMeters().stream()
        .filter(m -> m.getId().getName().startsWith("kafka."))
        .toList();
  }
}
