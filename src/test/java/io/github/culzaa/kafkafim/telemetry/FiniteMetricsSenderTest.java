package io.github.culzaa.kafkafim.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.micrometer.registry.otlp.OtlpMetricsSender;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

/**
 * #475: Kafka client gauges are {@code NaN} until their first sample, the OTLP registry exports
 * them as they are, and the APM Server cannot index {@code NaN}.
 */
class FiniteMetricsSenderTest {

  private final List<byte[]> raw = new ArrayList<>();
  private final List<byte[]> sent = new ArrayList<>();

  private OtlpMeterRegistry registry(OtlpMetricsSender sender) {
    OtlpConfig config =
        new OtlpConfig() {
          @Override
          public String get(String key) {
            return null;
          }

          @Override
          public java.time.Duration step() {
            return java.time.Duration.ofDays(1); // exported by close(), not by a timer
          }
        };
    return OtlpMeterRegistry.builder(config).metricsSender(sender).build();
  }

  /** What a real, still idle Kafka producer exports. */
  private void exportIdleProducerMetrics(OtlpMetricsSender sender) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:1");
    props.put("max.block.ms", "100");
    OtlpMeterRegistry registry = registry(sender);
    try (KafkaProducer<byte[], byte[]> producer =
            new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
        KafkaClientMetrics binder = new KafkaClientMetrics(producer)) {
      binder.bindTo(registry);
      Gauge.builder("kafkafim.test.nan", () -> Double.NaN).register(registry);
      Gauge.builder("kafkafim.test.inf", () -> Double.POSITIVE_INFINITY).register(registry);
      Gauge.builder("kafkafim.test.one", () -> 1).register(registry);
      registry.close(); // publishes the final export
    }
  }

  private static List<NumberDataPoint> gaugePoints(List<byte[]> bodies) throws Exception {
    List<NumberDataPoint> points = new ArrayList<>();
    for (byte[] body : bodies) {
      ExportMetricsServiceRequest request = ExportMetricsServiceRequest.parseFrom(body);
      request.getResourceMetricsList().stream()
          .flatMap(r -> r.getScopeMetricsList().stream())
          .flatMap(s -> s.getMetricsList().stream())
          .filter(Metric::hasGauge)
          .forEach(m -> points.addAll(m.getGauge().getDataPointsList()));
    }
    return points;
  }

  /** Names of the Kafka client gauges that carry a non-finite point. */
  private static List<String> nonFiniteKafkaGauges(List<byte[]> bodies) throws Exception {
    List<String> names = new ArrayList<>();
    for (byte[] body : bodies) {
      ExportMetricsServiceRequest.parseFrom(body).getResourceMetricsList().stream()
          .flatMap(r -> r.getScopeMetricsList().stream())
          .flatMap(s -> s.getMetricsList().stream())
          .filter(m -> m.hasGauge() && m.getName().startsWith("kafka."))
          .filter(m ->
              m.getGauge().getDataPointsList().stream()
                  .anyMatch(p -> p.hasAsDouble() && !Double.isFinite(p.getAsDouble())))
          .forEach(m -> names.add(m.getName()));
    }
    return names;
  }

  @Test
  void stockOtlpExportCarriesNonFinitePoints() throws Exception {
    exportIdleProducerMetrics(request -> raw.add(request.getMetricsData()));

    assertThat(nonFiniteKafkaGauges(raw))
        .as("the premise of #475: an idle Kafka client's gauges export NaN")
        .isNotEmpty();
  }

  @Test
  void noExportedPointIsNonFiniteAndFiniteOnesStay() throws Exception {
    exportIdleProducerMetrics(
        new FiniteMetricsSender(request -> sent.add(request.getMetricsData())));

    assertThat(nonFiniteKafkaGauges(sent)).isEmpty();
    List<NumberDataPoint> points = gaugePoints(sent);
    assertThat(points).isNotEmpty();
    assertThat(points)
        .allSatisfy(p -> {
          if (p.hasAsDouble()) {
            assertThat(Double.isFinite(p.getAsDouble())).isTrue();
          }
        });
    assertThat(
            sent.stream()
                .map(b -> {
                  try {
                    return ExportMetricsServiceRequest.parseFrom(b);
                  } catch (Exception e) {
                    throw new IllegalStateException(e);
                  }
                })
                .flatMap(r -> r.getResourceMetricsList().stream())
                .flatMap(r -> r.getScopeMetricsList().stream())
                .flatMap(s -> s.getMetricsList().stream())
                .map(Metric::getName))
        .contains("kafkafim.test.one")
        .doesNotContain("kafkafim.test.nan", "kafkafim.test.inf");
  }

  @Test
  void unparsableAndCleanBodiesAreSentUnchanged() {
    byte[] garbage = {1, 2, 3, 4, 5, 6, 7};
    assertThat(FiniteMetricsSender.filter(garbage)).isSameAs(garbage);

    byte[] clean = ExportMetricsServiceRequest.getDefaultInstance().toByteArray();
    assertThat(FiniteMetricsSender.filter(clean)).isSameAs(clean);
  }

  @Test
  void theConfigurationWrapsSpringBootsSender() {
    new org.springframework.boot.test.context.runner.ApplicationContextRunner()
        .withUserConfiguration(TelemetryConfiguration.class)
        .withBean(OtlpMetricsSender.class, () -> request -> {})
        .run(context ->
            assertThat(context.getBean(OtlpMetricsSender.class))
                .isInstanceOf(FiniteMetricsSender.class));
  }
}
